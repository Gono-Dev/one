use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Form, Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use dashmap::DashMap;
use rand::{RngCore, rngs::OsRng};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::{auth::parse_basic_auth, db::unix_timestamp, origin, state::AppState};

const LOGIN_FLOW_TTL_SECS: i64 = 20 * 60;

#[derive(Debug)]
pub struct LoginFlowStore {
    flows: DashMap<String, LoginFlowEntry>,
}

impl Default for LoginFlowStore {
    fn default() -> Self {
        Self {
            flows: DashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct LoginFlowEntry {
    flow_token: String,
    created_at: i64,
    credentials: Option<LoginFlowCredentials>,
}

#[derive(Debug, Clone)]
struct LoginFlowCredentials {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct PollForm {
    token: String,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/login/v2", post(init))
        .route("/index.php/login/v2", post(init))
        .route("/login/v2/poll", post(poll))
        .route("/index.php/login/v2/poll", post(poll))
        .route("/login/v2/flow/{token}", get(flow))
        .route("/index.php/login/v2/flow/{token}", get(flow))
}

async fn init(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Json<Value> {
    state.login_flows.prune_expired();

    let poll_token = generate_token();
    let flow_token = generate_token();
    state.login_flows.flows.insert(
        poll_token.clone(),
        LoginFlowEntry {
            flow_token: flow_token.clone(),
            created_at: unix_timestamp(),
            credentials: None,
        },
    );

    let base_url = login_flow_base_url(&state, &headers);
    Json(json!({
        "poll": {
            "token": poll_token,
            "endpoint": join_url(&base_url, "/login/v2/poll")
        },
        "login": join_url(&base_url, &format!("/login/v2/flow/{flow_token}"))
    }))
}

async fn poll(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<PollForm>,
) -> Response {
    state.login_flows.prune_expired();

    let Some(entry) = state.login_flows.flows.get(&form.token) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let Some(credentials) = entry.credentials.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    drop(entry);

    state.login_flows.flows.remove(&form.token);
    Json(json!({
        "server": login_flow_base_url(&state, &headers),
        "loginName": credentials.username,
        "appPassword": credentials.password
    }))
    .into_response()
}

async fn flow(
    State(state): State<Arc<AppState>>,
    Path(flow_token): Path<String>,
    headers: HeaderMap,
) -> Response {
    state.login_flows.prune_expired();

    let Some(poll_token) = state.login_flows.poll_token_for_flow(&flow_token) else {
        return (
            StatusCode::NOT_FOUND,
            Html("<!doctype html><title>Login expired</title><h1>Login expired</h1>"),
        )
            .into_response();
    };

    let Some((username, password)) = parse_basic_auth(headers.get(AUTHORIZATION)) else {
        return unauthorized(&state.auth_realm);
    };

    match state.user_store.verify(&username, &password).await {
        Ok(Some(_principal)) => {
            if let Some(mut entry) = state.login_flows.flows.get_mut(&poll_token) {
                entry.credentials = Some(LoginFlowCredentials { username, password });
            }
            Html(
                r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Gono Cloud connected</title></head>
<body><h1>Account connected</h1><p>You can close this window and return to the desktop client.</p></body>
</html>"#,
            )
            .into_response()
        }
        Ok(None) => unauthorized(&state.auth_realm),
        Err(err) => {
            error!(?err, "Login Flow v2 authentication failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Authentication backend error",
            )
                .into_response()
        }
    }
}

impl LoginFlowStore {
    fn prune_expired(&self) {
        let cutoff = unix_timestamp() - LOGIN_FLOW_TTL_SECS;
        self.flows.retain(|_, entry| entry.created_at >= cutoff);
    }

    fn poll_token_for_flow(&self, flow_token: &str) -> Option<String> {
        self.flows
            .iter()
            .find_map(|entry| (entry.flow_token == flow_token).then(|| entry.key().clone()))
    }
}

fn login_flow_base_url(state: &AppState, headers: &HeaderMap) -> String {
    if state.base_url_explicit {
        return state.base_url.clone();
    }
    origin::runtime_base_url(headers, &state.config.server.bind)
}

fn join_url(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn generate_token() -> String {
    let mut bytes = [0u8; 64];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn unauthorized(realm: &str) -> Response {
    let challenge = format!("Basic realm=\"{realm}\"");
    (
        StatusCode::UNAUTHORIZED,
        [(WWW_AUTHENTICATE, HeaderValue::from_str(&challenge).unwrap())],
        "Unauthorized",
    )
        .into_response()
}
