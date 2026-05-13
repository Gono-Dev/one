use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordVerifier},
};
use axum::{
    body::Body,
    extract::connect_info::ConnectInfo,
    extract::{Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use sha2::{Digest, Sha256};
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

const DEFAULT_SUCCESS_AUTH_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct SqliteUserStore {
    pool: SqlitePool,
    success_cache: Arc<DashMap<AuthCacheKey, AuthCacheEntry>>,
    success_cache_ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AuthCacheKey([u8; 32]);

#[derive(Debug, Clone)]
struct AuthCacheEntry {
    principal: Principal,
    cached_at: Instant,
}

impl SqliteUserStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self::new_with_cache_ttl(pool, DEFAULT_SUCCESS_AUTH_CACHE_TTL)
    }

    pub fn new_with_cache_ttl(pool: SqlitePool, success_cache_ttl: Duration) -> Self {
        Self {
            pool,
            success_cache: Arc::new(DashMap::new()),
            success_cache_ttl,
        }
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

    pub async fn verify_cached(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<Principal>> {
        let key = AuthCacheKey::new(username, password);
        if let Some(entry) = self.success_cache.get(&key) {
            if entry.cached_at.elapsed() < self.success_cache_ttl {
                let principal = entry.principal.clone();
                if principal
                    .expires_at
                    .is_none_or(|expires_at| expires_at > unix_timestamp())
                {
                    return Ok(Some(principal));
                }
            }
            drop(entry);
            self.success_cache.remove(&key);
        }

        let principal = self.verify(username, password).await?;
        if let Some(principal) = &principal {
            self.success_cache.insert(
                key,
                AuthCacheEntry {
                    principal: principal.clone(),
                    cached_at: Instant::now(),
                },
            );
            self.prune_success_cache();
        }
        Ok(principal)
    }

    pub fn clear_success_cache(&self) {
        self.success_cache.clear();
    }

    pub fn clear_success_cache_for_user(&self, username: &str) {
        self.success_cache
            .retain(|_, entry| entry.principal.username != username);
    }

    fn prune_success_cache(&self) {
        let ttl = self.success_cache_ttl;
        self.success_cache
            .retain(|_, entry| entry.cached_at.elapsed() < ttl);
    }
}

impl AuthCacheKey {
    fn new(username: &str, password: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update((username.len() as u64).to_be_bytes());
        hasher.update(username.as_bytes());
        hasher.update((password.len() as u64).to_be_bytes());
        hasher.update(password.as_bytes());
        Self(hasher.finalize().into())
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

    match state.user_store.verify_cached(&username, &password).await {
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

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration};

    use sqlx::SqlitePool;

    use crate::{
        config::DbConfig,
        db::{self, AppPasswordScopeInput},
        permissions::PermissionLevel,
    };

    use super::SqliteUserStore;

    async fn temp_pool(temp: &tempfile::TempDir) -> SqlitePool {
        let config = DbConfig {
            path: temp
                .path()
                .join("gono-cloud.db")
                .to_string_lossy()
                .into_owned(),
            max_connections: 1,
        };
        let pool = db::connect(&config).await.expect("connect sqlite");
        db::migrate(&pool).await.expect("migrate sqlite");
        pool
    }

    #[tokio::test]
    async fn successful_auth_cache_reuses_principal_until_ttl_expires() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let created = db::create_local_user(&pool, "alice", None)
            .await
            .expect("create alice");
        let store = SqliteUserStore::new_with_cache_ttl(pool.clone(), Duration::from_secs(60));

        let first = store
            .verify_cached("alice", &created.app_password)
            .await
            .expect("verify first")
            .expect("first principal");
        assert_eq!(first.username, "alice");

        assert!(
            db::delete_local_user(&pool, "alice")
                .await
                .expect("delete alice")
        );
        let cached = store
            .verify_cached("alice", &created.app_password)
            .await
            .expect("verify cached")
            .expect("cached principal");
        assert_eq!(cached.username, "alice");

        store.clear_success_cache_for_user("alice");
        assert!(
            store
                .verify_cached("alice", &created.app_password)
                .await
                .expect("verify after clear")
                .is_none()
        );
    }

    #[tokio::test]
    async fn successful_auth_cache_refreshes_after_ttl() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let created = db::create_local_user(&pool, "alice", None)
            .await
            .expect("create alice");
        let store = SqliteUserStore::new_with_cache_ttl(pool.clone(), Duration::ZERO);

        let first = store
            .verify_cached("alice", &created.app_password)
            .await
            .expect("verify first")
            .expect("first principal");
        assert_eq!(first.scopes[0].storage_path, Path::new(""));

        assert!(
            db::replace_local_app_password_scopes(
                &pool,
                "alice",
                db::DEFAULT_APP_PASSWORD_LABEL,
                &[AppPasswordScopeInput {
                    mount_path: "/Docs".to_owned(),
                    storage_path: "/Projects".to_owned(),
                    permission: PermissionLevel::View,
                }],
            )
            .await
            .expect("replace scopes")
        );

        let refreshed = store
            .verify_cached("alice", &created.app_password)
            .await
            .expect("verify refreshed")
            .expect("refreshed principal");
        assert_eq!(refreshed.scopes[0].storage_path, Path::new("Projects"));
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
