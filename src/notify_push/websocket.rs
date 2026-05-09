use std::{sync::Arc, time::Duration};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::broadcast, time};
use tracing::{debug, warn};

use crate::{notify_push::runtime::SubscribeError, state::AppState};

use super::{NotifyRuntime, PushMessage};

pub async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    runtime: Arc<NotifyRuntime>,
) {
    let user = match time::timeout(
        runtime.auth_timeout(),
        authenticate(&mut socket, &state, &runtime),
    )
    .await
    {
        Ok(Ok(user)) => user,
        Ok(Err(message)) => {
            runtime.auth_failed();
            let _ = socket.send(Message::text(format!("err: {message}"))).await;
            return;
        }
        Err(_) => {
            runtime.auth_failed();
            let _ = socket
                .send(Message::text("err: Authentication timeout"))
                .await;
            return;
        }
    };

    let mut receiver = match runtime.subscribe(&user) {
        Ok(receiver) => receiver,
        Err(SubscribeError::LimitExceeded) => {
            runtime.auth_failed();
            let _ = socket
                .send(Message::text("err: Too many connections"))
                .await;
            return;
        }
    };

    if socket.send(Message::text("authenticated")).await.is_err() {
        runtime.disconnect(&user);
        return;
    }

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
    let mut pending: Option<PushMessage> = None;

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Ok(message) => merge_pending(&mut pending, message),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        merge_pending(&mut pending, PushMessage::file(None));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = flush.tick() => {
                if !send_pending(&runtime, &mut sender, &mut pending, listen_file_id).await {
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
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    Some(Err(err)) => {
                        warn!(?err, user, "notify_push websocket receive error");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = send_pending(&runtime, &mut sender, &mut pending, listen_file_id).await;
    let _ = sender.close().await;
    runtime.disconnect(&user);
    debug!(user, "notify_push websocket disconnected");
}

async fn authenticate(
    socket: &mut WebSocket,
    state: &AppState,
    runtime: &NotifyRuntime,
) -> Result<String, &'static str> {
    let username = read_text(socket).await.ok_or("Invalid auth message")?;
    let password = read_text(socket).await.ok_or("Invalid auth message")?;

    if username.is_empty() {
        runtime
            .take_pre_auth(&password)
            .ok_or("Invalid pre-auth token")
    } else {
        match state.user_store.verify(&username, &password).await {
            Ok(Some(_)) => Ok(username),
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
    listen_file_id: bool,
) -> bool {
    let Some(message) = pending.take() else {
        return true;
    };
    let ty = message.message_type();
    let text = message.to_wire_text(listen_file_id);
    if sender.send(Message::text(text)).await.is_ok() {
        runtime.message_sent(ty);
        true
    } else {
        false
    }
}
