use std::sync::Arc;

use anyhow::{anyhow, Context};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::{
    body::Body,
    extract::connect_info::ConnectInfo,
    extract::{Request, State},
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use sqlx::{Row, SqlitePool};
use tracing::error;

mod rate_limit;
pub use rate_limit::{AuthRateLimitStats, AuthRateLimiter};

use crate::{
    db::unix_timestamp,
    permissions::{self, AppPasswordScope, PermissionLevel},
    state::AppState,
};

#[derive(Debug, Clone)]
pub struct Principal {
    pub username: String,
    pub app_password_id: i64,
    pub app_password_label: String,
    pub expires_at: Option<i64>,
    pub scopes: Vec<AppPasswordScope>,
}

#[derive(Debug, Clone)]
pub struct SqliteUserStore {
    pool: SqlitePool,
}

impl SqliteUserStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn verify(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<Principal>> {
        let rows = sqlx::query(
            r#"
            SELECT app_passwords.id, app_passwords.label, app_passwords.password_hash, app_passwords.expires_at
            FROM app_passwords
            JOIN users ON users.username = app_passwords.username
            WHERE users.username = ?1 AND users.enabled = 1
            "#,
        )
        .bind(username)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("load app passwords for {username}"))?;

        for row in rows {
            let password_hash: String = row.try_get("password_hash")?;
            let parsed_hash = PasswordHash::new(&password_hash)
                .map_err(|err| anyhow!("parse password hash: {err}"))?;
            if Argon2::default()
                .verify_password(password.as_bytes(), &parsed_hash)
                .is_ok()
            {
                let id: i64 = row.try_get("id")?;
                let label: String = row.try_get("label")?;
                let expires_at: Option<i64> = row.try_get("expires_at")?;
                let now = unix_timestamp();
                if expires_at.is_some_and(|expires_at| expires_at <= now) {
                    return Ok(None);
                }
                let scopes = load_app_password_scopes(&self.pool, id).await?;
                sqlx::query("UPDATE app_passwords SET last_used_at = ?1 WHERE id = ?2")
                    .bind(now)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .context("update app password last_used_at")?;

                return Ok(Some(Principal {
                    username: username.to_owned(),
                    app_password_id: id,
                    app_password_label: label,
                    expires_at,
                    scopes,
                }));
            }
        }

        Ok(None)
    }
}

async fn load_app_password_scopes(
    pool: &SqlitePool,
    app_password_id: i64,
) -> anyhow::Result<Vec<AppPasswordScope>> {
    let rows = sqlx::query(
        r#"
        SELECT id, mount_path, storage_path, permission
        FROM app_password_scopes
        WHERE app_password_id = ?1
        ORDER BY mount_path
        "#,
    )
    .bind(app_password_id)
    .fetch_all(pool)
    .await
    .context("load app password scopes")?;

    if rows.is_empty() {
        return Ok(vec![permissions::default_scope()]);
    }

    let scopes = rows
        .into_iter()
        .map(|row| {
            Ok(AppPasswordScope {
                id: row.try_get("id")?,
                mount_path: permissions::normalize_scope_path(
                    &row.try_get::<String, _>("mount_path")?,
                )?,
                storage_path: permissions::normalize_scope_path(
                    &row.try_get::<String, _>("storage_path")?,
                )?,
                permission: PermissionLevel::parse(&row.try_get::<String, _>("permission")?)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    permissions::validate_scope_set(&scopes)?;
    Ok(scopes)
}

pub async fn require_basic_auth(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let client_ip = client_ip(&request);
    let Some((username, password)) = parse_basic_auth(request.headers().get(AUTHORIZATION)) else {
        return unauthorized(&state.auth_realm);
    };

    match state.user_store.verify(&username, &password).await {
        Ok(Some(principal)) => {
            state.auth_rate_limiter.clear(&client_ip, &username);
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Ok(None) => {
            let delay = state
                .auth_rate_limiter
                .register_failure(&client_ip, &username);
            tokio::time::sleep(delay).await;
            unauthorized(&state.auth_realm)
        }
        Err(err) => {
            error!(?err, "Basic Auth verification failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Authentication backend error",
            )
                .into_response()
        }
    }
}

fn client_ip(request: &Request<Body>) -> String {
    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
    {
        return addr.ip().to_string();
    }
    client_ip_from_headers(request.headers()).unwrap_or_else(|| "unknown".to_owned())
}

fn client_ip_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
}

pub(crate) fn parse_basic_auth(header: Option<&HeaderValue>) -> Option<(String, String)> {
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
