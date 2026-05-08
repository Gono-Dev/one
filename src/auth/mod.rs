use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        HeaderValue, StatusCode,
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct Principal {
    pub username: String,
}

pub async fn require_basic_auth(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    match parse_basic_auth(request.headers().get(AUTHORIZATION)) {
        Some((username, password))
            if username == state.auth.username && password == state.auth.password =>
        {
            request.extensions_mut().insert(Principal { username });
            next.run(request).await
        }
        _ => unauthorized(&state.auth.realm),
    }
}

fn parse_basic_auth(header: Option<&HeaderValue>) -> Option<(String, String)> {
    let header = header?.to_str().ok()?;
    let encoded = header.strip_prefix("Basic ")?;
    let decoded = STANDARD.decode(encoded).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_owned(), password.to_owned()))
}

fn unauthorized(realm: &str) -> Response {
    let challenge = format!("Basic realm=\"{realm}\"");
    (
        StatusCode::UNAUTHORIZED,
        [(WWW_AUTHENTICATE, challenge)],
        "Unauthorized",
    )
        .into_response()
}
