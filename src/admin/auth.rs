use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        StatusCode,
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::error;

use crate::{auth::parse_basic_auth, state::AppState};

pub async fn require_admin(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some((username, password)) = parse_basic_auth(request.headers().get(AUTHORIZATION)) else {
        return unauthorized(&state.auth_realm);
    };

    match state.user_store.verify(&username, &password).await {
        Ok(Some(principal)) if is_admin(&state, &principal.username) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Ok(Some(_)) => (StatusCode::FORBIDDEN, "Forbidden").into_response(),
        Ok(None) => unauthorized(&state.auth_realm),
        Err(err) => {
            error!(?err, "Admin Basic Auth verification failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Authentication backend error",
            )
                .into_response()
        }
    }
}

fn is_admin(state: &AppState, username: &str) -> bool {
    state.admin_config.enabled
        && state
            .admin_config
            .users
            .iter()
            .any(|admin_user| admin_user == username)
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
