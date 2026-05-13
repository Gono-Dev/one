use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::broadcast, time};
use tracing::{debug, info, warn};

use crate::{auth::Principal, notify_push::runtime::SubscribeError, permissions, state::AppState};

use super::{NotifyClientInfo, NotifyRuntime, PushMessage, UpdatedFiles};

const CLIENT_INFO_PREFIX: &str = "gono_client_info ";

pub async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    runtime: Arc<NotifyRuntime>,
    peer_addr: String,
) {
    let principal = match time::timeout(
        runtime.auth_timeout(),
        authenticate(&mut socket, &state, &runtime),
    )
    .await
    {
        Ok(Ok(user)) => user,
        Ok(Err(message)) => {
            runtime.auth_failed();
            warn!(%peer_addr, message, "notify_push websocket authentication failed");
            let _ = socket.send(Message::text(format!("err: {message}"))).await;
            return;
        }
        Err(_) => {
            runtime.auth_failed();
            warn!(%peer_addr, "notify_push websocket authentication timed out");
            let _ = socket
                .send(Message::text("err: Authentication timeout"))
                .await;
            return;
        }
    };

    let user = principal.username.clone();
    let mut receiver = match runtime.subscribe(&user) {
        Ok(receiver) => receiver,
        Err(SubscribeError::LimitExceeded) => {
            runtime.auth_failed();
            warn!(%peer_addr, user, "notify_push websocket connection limit exceeded");
            let _ = socket
                .send(Message::text("err: Too many connections"))
                .await;
            return;
        }
    };

    if socket.send(Message::text("authenticated")).await.is_err() {
        runtime.disconnect(&user, None);
        return;
    }

    let connection_id = runtime.register_connection(&user, &peer_addr);
    info!(%peer_addr, user, "notify_push websocket authenticated");
    debug!(user, "notify_push websocket authenticated");
    let (mut sender, mut inbound) = socket.split();
    let mut ping = time::interval(runtime.ping_interval());
    ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut flush = time::interval(Duration::from_millis(100));
    flush.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut max_connection = runtime
        .max_connection_time()
        .map(|duration| Box::pin(time::sleep(duration)));
    let mut listen_file_id = false;
    let mut gono_client_info_received = false;
    let mut pending: Option<PushMessage> = None;

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Ok(message) => {
                        if let Some(message) = filter_message_for_principal(&state, &principal, message).await {
                            merge_pending(&mut pending, message);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        merge_pending(&mut pending, PushMessage::file(None));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = flush.tick() => {
                let send_file_ids = listen_file_id && gono_client_info_received;
                if !send_pending(&runtime, &mut sender, &mut pending, send_file_ids, &user, &peer_addr).await {
                    break;
                }
            }
            _ = ping.tick() => {
                if sender.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
            _ = async {
                if let Some(sleep) = max_connection.as_mut() {
                    sleep.as_mut().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => break,
            inbound_msg = inbound.next() => {
                match inbound_msg {
                    Some(Ok(message)) => {
                        match message {
                            Message::Text(text) if text.as_str() == "listen notify_file_id" => {
                                listen_file_id = true;
                                runtime.set_connection_listen_file_id(connection_id, true);
                            }
                            Message::Text(text) => {
                                if let Some(client_info) = parse_client_info_message(text.as_str()) {
                                    runtime.update_connection_client_info(connection_id, client_info);
                                    gono_client_info_received = true;
                                }
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    Some(Err(err)) => {
                        warn!(?err, %peer_addr, user, "notify_push websocket receive error");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = send_pending(
        &runtime,
        &mut sender,
        &mut pending,
        listen_file_id && gono_client_info_received,
        &user,
        &peer_addr,
    )
    .await;
    let _ = sender.close().await;
    runtime.disconnect(&user, Some(connection_id));
    info!(%peer_addr, user, "notify_push websocket disconnected");
    debug!(user, "notify_push websocket disconnected");
}

fn parse_client_info_message(text: &str) -> Option<NotifyClientInfo> {
    let payload = text.strip_prefix(CLIENT_INFO_PREFIX)?;
    let client_info = match NotifyClientInfo::from_json(payload) {
        Ok(client_info) => client_info,
        Err(err) => {
            debug!(?err, "ignoring invalid notify_push client info message");
            return None;
        }
    };
    (!client_info.is_empty()).then_some(client_info)
}

async fn authenticate(
    socket: &mut WebSocket,
    state: &AppState,
    runtime: &NotifyRuntime,
) -> Result<Principal, &'static str> {
    let username = read_text(socket).await.ok_or("Invalid auth message")?;
    let password = read_text(socket).await.ok_or("Invalid auth message")?;

    if username.is_empty() {
        runtime
            .take_pre_auth(&password)
            .ok_or("Invalid pre-auth token")
    } else {
        match state.user_store.verify_cached(&username, &password).await {
            Ok(Some(principal)) => Ok(principal),
            Ok(None) => Err("Invalid credentials"),
            Err(_) => Err("Authentication backend error"),
        }
    }
}

async fn read_text(socket: &mut WebSocket) -> Option<String> {
    loop {
        let message = socket.next().await?.ok()?;
        match message {
            Message::Text(text) => return Some(text.to_string()),
            Message::Close(_) => return None,
            _ => {}
        }
    }
}

async fn filter_message_for_principal(
    state: &AppState,
    principal: &Principal,
    message: PushMessage,
) -> Option<PushMessage> {
    let PushMessage::File(files) = message else {
        return Some(message);
    };
    let UpdatedFiles::Known(ids) = files else {
        return principal_sees_entire_storage(principal).then_some(PushMessage::File(files));
    };

    let mut visible = BTreeSet::new();
    for file_id in ids {
        let rel_path =
            match crate::db::file_rel_path_by_id(&state.db, &principal.username, file_id).await {
                Ok(Some(rel_path)) => rel_path,
                Ok(None) => continue,
                Err(err) => {
                    warn!(?err, file_id, "failed to resolve notify_push file id");
                    continue;
                }
            };
        if permissions::resolve_scope_for_storage_path(principal, Path::new(&rel_path))
            .ok()
            .flatten()
            .is_some()
        {
            visible.insert(file_id);
        }
    }

    if visible.is_empty() {
        None
    } else {
        Some(PushMessage::File(UpdatedFiles::Known(visible)))
    }
}

fn principal_sees_entire_storage(principal: &Principal) -> bool {
    principal.scopes.iter().any(|scope| {
        scope.mount_path.as_os_str().is_empty() && scope.storage_path.as_os_str().is_empty()
    })
}

fn merge_pending(pending: &mut Option<PushMessage>, message: PushMessage) {
    match pending {
        Some(existing) => {
            if !existing.merge(&message) {
                *pending = Some(message);
            }
        }
        None => *pending = Some(message),
    }
}

async fn send_pending(
    runtime: &NotifyRuntime,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    pending: &mut Option<PushMessage>,
    send_file_ids: bool,
    user: &str,
    peer_addr: &str,
) -> bool {
    let Some(message) = pending.take() else {
        return true;
    };
    let ty = message.message_type();
    let text = message.to_wire_text(send_file_ids);
    if sender.send(Message::text(text)).await.is_ok() {
        runtime.message_sent(ty);
        info!(
            %peer_addr,
            user,
            send_file_ids,
            message = %message.to_wire_text(send_file_ids),
            "notify_push websocket message sent"
        );
        true
    } else {
        warn!(
            %peer_addr,
            user,
            send_file_ids,
            "failed to send notify_push websocket message"
        );
        false
    }
}
