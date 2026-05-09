use std::{net::IpAddr, sync::Arc};

use axum::{
    extract::{Path, State, WebSocketUpgrade},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};

use crate::{auth::parse_basic_auth, state::AppState};

use super::{websocket, NotifyRuntime};

pub fn router(path: &str) -> Router<Arc<AppState>> {
    let ws_route = format!("{}/ws", normalize_push_path(path));

    Router::new()
        .route(&ws_route, get(ws_handler))
        .route("/apps/notify_push/pre_auth", post(pre_auth))
        .route("/index.php/apps/notify_push/pre_auth", post(pre_auth))
        .route("/apps/notify_push/uid", get(uid))
        .route("/index.php/apps/notify_push/uid", get(uid))
        .route("/push/test/cookie", get(test_cookie))
        .route("/push/test/reverse_cookie", get(test_reverse_cookie))
        .route("/push/test/mapping/{id}", get(test_mapping))
        .route("/push/test/remote/{ip}", get(test_remote))
        .route("/push/test/version", post(test_version))
        .route("/push/test/trigger/{kind}", post(test_trigger))
}

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    let Some(runtime) = state.notify_push.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    ws.on_upgrade(move |socket| websocket::handle_socket(socket, state, runtime))
}

async fn pre_auth(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(runtime) = state.notify_push.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(username) = verify_basic(&state, &headers).await else {
        return unauthorized(&state.auth_realm);
    };
    runtime.issue_pre_auth(username).into_response()
}

async fn uid(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(_runtime) = state.notify_push.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(username) = verify_basic(&state, &headers).await else {
        return unauthorized(&state.auth_realm);
    };
    username.into_response()
}

async fn test_cookie(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    with_valid_test_token(&state, &headers, |runtime| {
        runtime.test_cookie().to_string().into_response()
    })
}

async fn test_reverse_cookie(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    with_valid_test_token(&state, &headers, |runtime| {
        runtime.test_cookie().to_string().into_response()
    })
}

async fn test_mapping(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(_id): Path<String>,
) -> Response {
    with_valid_test_token(&state, &headers, |_runtime| "1".into_response())
}

async fn test_remote(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(ip): Path<IpAddr>,
) -> Response {
    with_valid_test_token(&state, &headers, |_runtime| ip.to_string().into_response())
}

async fn test_version(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    with_valid_test_token(&state, &headers, |runtime| {
        runtime.set_version(env!("CARGO_PKG_VERSION"));
        "set".into_response()
    })
}

async fn test_trigger(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(kind): Path<String>,
) -> Response {
    with_valid_test_token(&state, &headers, |runtime| {
        match kind.as_str() {
            "activity" => runtime.notify_activity(&state.owner),
            "notification" => runtime.notify_notification(&state.owner),
            "custom" => {
                runtime.notify_custom(&state.owner, "notify_custom", Some("test".to_owned()))
            }
            _ => return (StatusCode::BAD_REQUEST, "unknown trigger").into_response(),
        }
        "sent".into_response()
    })
}

async fn verify_basic(state: &AppState, headers: &HeaderMap) -> Option<String> {
    let (username, password) = parse_basic_auth(headers.get(AUTHORIZATION))?;
    state
        .user_store
        .verify(&username, &password)
        .await
        .ok()
        .flatten()
        .map(|principal| principal.username)
}

fn with_valid_test_token(
    state: &AppState,
    headers: &HeaderMap,
    render: impl FnOnce(&Arc<NotifyRuntime>) -> Response,
) -> Response {
    let Some(runtime) = state.notify_push.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    runtime.test_endpoint_hit();
    let token = headers.get("token").and_then(|value| value.to_str().ok());
    if runtime.validate_test_token(token) {
        render(runtime)
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

fn unauthorized(realm: &str) -> Response {
    let challenge = format!("Basic realm=\"{realm}\"");
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, challenge)],
        "Unauthorized",
    )
        .into_response()
}

pub fn websocket_endpoint(base_url: &str, path: &str) -> String {
    let endpoint = join_url(base_url, &format!("{}/ws", normalize_push_path(path)));
    endpoint
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| {
            endpoint
                .strip_prefix("http://")
                .map(|rest| format!("ws://{rest}"))
        })
        .unwrap_or(endpoint)
}

pub fn pre_auth_endpoint(base_url: &str) -> String {
    join_url(base_url, "/apps/notify_push/pre_auth")
}

fn join_url(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

fn normalize_push_path(path: &str) -> String {
    let path = path.trim();
    if path.is_empty() {
        return "/push".to_owned();
    }
    format!("/{}", path.trim_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::{normalize_push_path, websocket_endpoint};

    #[test]
    fn websocket_endpoint_rewrites_http_scheme() {
        assert_eq!(
            websocket_endpoint("https://files.example.com", "/push"),
            "wss://files.example.com/push/ws"
        );
        assert_eq!(
            websocket_endpoint("http://127.0.0.1:3000/", "/push/"),
            "ws://127.0.0.1:3000/push/ws"
        );
        assert_eq!(
            websocket_endpoint("https://files.example.com", "push"),
            "wss://files.example.com/push/ws"
        );
    }

    #[test]
    fn push_path_is_normalized_for_routing() {
        assert_eq!(normalize_push_path(""), "/push");
        assert_eq!(normalize_push_path("push"), "/push");
        assert_eq!(normalize_push_path("/custom/"), "/custom");
    }
}
