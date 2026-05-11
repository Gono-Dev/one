use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqliteConnection, SqlitePool,
};

use crate::{
    config::DbConfig,
    permissions::{self, PermissionLevel},
    storage,
};

pub const BOOTSTRAP_USER: &str = "gono";
const BOOTSTRAP_LABEL: &str = "bootstrap";
pub const DEFAULT_APP_PASSWORD_LABEL: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapOutcome {
    pub generated_password: Option<String>,
    pub generated_admin_users: Vec<CreatedLocalUser>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalUser {
    pub username: String,
    pub display_name: String,
    pub enabled: bool,
    pub created_at: i64,
    pub app_password_count: i64,
    pub app_password_labels: Vec<String>,
    pub app_passwords: Vec<LocalAppPassword>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAppPassword {
    pub id: i64,
    pub label: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub scopes: Vec<LocalAppPasswordScope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAppPasswordScope {
    pub id: i64,
    pub mount_path: String,
    pub storage_path: String,
    pub permission: PermissionLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPasswordScopeInput {
    pub mount_path: String,
    pub storage_path: String,
    pub permission: PermissionLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedLocalAppPassword {
    pub username: String,
    pub password_label: String,
    pub app_password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedLocalUser {
    pub username: String,
    pub display_name: String,
    pub password_label: String,
    pub app_password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetLocalUserPassword {
    pub username: String,
    pub password_label: String,
    pub app_password: String,
}

pub async fn connect(config: &DbConfig) -> anyhow::Result<SqlitePool> {
    let db_path = Path::new(&config.path);
    if let Some(parent) = db_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create database directory {}", parent.display()))?;
    }

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);

    SqlitePoolOptions::new()
        .max_connections(config.max_connections.max(1))
        .connect_with(options)
        .await
        .with_context(|| format!("open SQLite database {}", db_path.display()))
}

pub async fn migrate(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("run SQLite migrations")?;
    ensure_runtime_schema(pool).await
}

async fn ensure_runtime_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS webdav_locks (
            token        TEXT PRIMARY KEY,
            path         TEXT    NOT NULL,
            principal    TEXT,
            owner_xml    TEXT,
            timeout_at   INTEGER,
            timeout_secs INTEGER,
            shared       INTEGER NOT NULL,
            deep         INTEGER NOT NULL,
            created_at   INTEGER NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create webdav_locks table")?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_webdav_locks_path ON webdav_locks(path)")
        .execute(pool)
        .await
        .context("create webdav_locks path index")?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_webdav_locks_timeout ON webdav_locks(timeout_at)")
        .execute(pool)
        .await
        .context("create webdav_locks timeout index")?;
    Ok(())
}

pub async fn ensure_bootstrap_user(pool: &SqlitePool) -> anyhow::Result<BootstrapOutcome> {
    let mut tx = pool
        .begin()
        .await
        .context("begin bootstrap user transaction")?;
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT INTO users(username, display_name, enabled, created_at)
        VALUES(?1, ?1, 1, ?2)
        ON CONFLICT(username) DO UPDATE SET
            display_name = excluded.display_name,
            enabled = 1
        "#,
    )
    .bind(BOOTSTRAP_USER)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("ensure bootstrap user")?;

    let existing: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT id
        FROM app_passwords
        WHERE username = ?1 AND label = ?2
        "#,
    )
    .bind(BOOTSTRAP_USER)
    .bind(BOOTSTRAP_LABEL)
    .fetch_optional(&mut *tx)
    .await
    .context("lookup bootstrap app password")?;

    let generated_password = if existing.is_none() {
        let password = generate_app_password();
        let password_hash = hash_password(&password).context("hash bootstrap app password")?;

        let result = sqlx::query(
            r#"
            INSERT INTO app_passwords(username, label, password_hash, created_at)
            VALUES(?1, ?2, ?3, ?4)
            "#,
        )
        .bind(BOOTSTRAP_USER)
        .bind(BOOTSTRAP_LABEL)
        .bind(password_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("insert bootstrap app password")?;
        insert_default_scope(&mut tx, result.last_insert_rowid(), now).await?;

        Some(password)
    } else {
        None
    };

    tx.commit()
        .await
        .context("commit bootstrap user transaction")?;
    Ok(BootstrapOutcome {
        generated_password,
        generated_admin_users: Vec::new(),
    })
}

pub async fn ensure_configured_admin_users(
    pool: &SqlitePool,
    admin_users: &[String],
) -> anyhow::Result<Vec<CreatedLocalUser>> {
    let mut generated = Vec::new();
    for username in admin_users {
        if username == BOOTSTRAP_USER {
            continue;
        }
        if let Some(created) = ensure_local_user_with_default_password(pool, username).await? {
            generated.push(created);
        }
    }
    Ok(generated)
}

async fn ensure_local_user_with_default_password(
    pool: &SqlitePool,
    username: &str,
) -> anyhow::Result<Option<CreatedLocalUser>> {
    validate_username(username)?;
    let now = unix_timestamp();
    let mut tx = pool
        .begin()
        .await
        .context("begin ensure local admin user transaction")?;

    let existing_display_name: Option<Option<String>> =
        sqlx::query_scalar("SELECT display_name FROM users WHERE username = ?1")
            .bind(username)
            .fetch_optional(&mut *tx)
            .await
            .with_context(|| format!("lookup local admin user {username}"))?;

    let display_name = existing_display_name
        .clone()
        .flatten()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| username.to_owned());

    if existing_display_name.is_some() {
        sqlx::query("UPDATE users SET enabled = 1 WHERE username = ?1")
            .bind(username)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("enable local admin user {username}"))?;
    } else {
        sqlx::query(
            r#"
            INSERT INTO users(username, display_name, enabled, created_at)
            VALUES(?1, ?2, 1, ?3)
            "#,
        )
        .bind(username)
        .bind(&display_name)
        .bind(now)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert local admin user {username}"))?;
    }

    let existing_password: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT id
        FROM app_passwords
        WHERE username = ?1 AND label = ?2
        "#,
    )
    .bind(username)
    .bind(DEFAULT_APP_PASSWORD_LABEL)
    .fetch_optional(&mut *tx)
    .await
    .with_context(|| format!("lookup default app password for admin user {username}"))?;

    let created = if existing_password.is_none() {
        let password = generate_app_password();
        let password_hash = hash_password(&password).context("hash admin app password")?;
        let result = sqlx::query(
            r#"
            INSERT INTO app_passwords(username, label, password_hash, created_at)
            VALUES(?1, ?2, ?3, ?4)
            "#,
        )
        .bind(username)
        .bind(DEFAULT_APP_PASSWORD_LABEL)
        .bind(password_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert admin app password for {username}"))?;
        insert_default_scope(&mut tx, result.last_insert_rowid(), now).await?;

        Some(CreatedLocalUser {
            username: username.to_owned(),
            display_name,
            password_label: DEFAULT_APP_PASSWORD_LABEL.to_owned(),
            app_password: password,
        })
    } else {
        None
    };

    tx.commit()
        .await
        .context("commit ensure local admin user transaction")?;
    Ok(created)
}

pub async fn list_local_users(pool: &SqlitePool) -> anyhow::Result<Vec<LocalUser>> {
    let rows = sqlx::query(
        r#"
        SELECT
            users.username,
            COALESCE(users.display_name, users.username) AS display_name,
            users.enabled,
            users.created_at,
            (
                SELECT COUNT(*)
                FROM app_passwords
                WHERE app_passwords.username = users.username
            ) AS app_password_count,
            COALESCE((
                SELECT GROUP_CONCAT(label, ', ')
                FROM (
                    SELECT label
                    FROM app_passwords
                    WHERE app_passwords.username = users.username
                    ORDER BY label
                )
            ), '') AS app_password_labels
        FROM users
        ORDER BY users.username
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list local users")?;

    let mut users = rows
        .into_iter()
        .map(|row| {
            Ok(LocalUser {
                username: row.try_get("username")?,
                display_name: row.try_get("display_name")?,
                enabled: row.try_get::<i64, _>("enabled")? != 0,
                created_at: row.try_get("created_at")?,
                app_password_count: row.try_get("app_password_count")?,
                app_password_labels: row
                    .try_get::<String, _>("app_password_labels")?
                    .split(", ")
                    .filter(|label| !label.is_empty())
                    .map(str::to_owned)
                    .collect(),
                app_passwords: Vec::new(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    for user in &mut users {
        user.app_passwords = list_local_app_passwords(pool, &user.username).await?;
    }

    Ok(users)
}

pub async fn local_user_exists(pool: &SqlitePool, username: &str) -> anyhow::Result<bool> {
    validate_username(username)?;
    let exists: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM users
            WHERE username = ?1 AND enabled = 1
        )
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
    .with_context(|| format!("lookup local user {username}"))?;
    Ok(exists != 0)
}

pub async fn create_local_user(
    pool: &SqlitePool,
    username: &str,
    display_name: Option<&str>,
) -> anyhow::Result<CreatedLocalUser> {
    validate_username(username)?;
    let display_name = display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(username);
    let now = unix_timestamp();
    let password = generate_app_password();
    let password_hash = hash_password(&password).context("hash app password")?;
    let mut tx = pool
        .begin()
        .await
        .context("begin create local user transaction")?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT username FROM users WHERE username = ?1")
            .bind(username)
            .fetch_optional(&mut *tx)
            .await
            .with_context(|| format!("lookup local user {username}"))?;
    if existing.is_some() {
        anyhow::bail!("local user {username:?} already exists");
    }

    sqlx::query(
        r#"
        INSERT INTO users(username, display_name, enabled, created_at)
        VALUES(?1, ?2, 1, ?3)
        "#,
    )
    .bind(username)
    .bind(display_name)
    .bind(now)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("insert local user {username}"))?;

    let result = sqlx::query(
        r#"
        INSERT INTO app_passwords(username, label, password_hash, created_at)
        VALUES(?1, ?2, ?3, ?4)
        "#,
    )
    .bind(username)
    .bind(DEFAULT_APP_PASSWORD_LABEL)
    .bind(password_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("insert app password for {username}"))?;
    insert_default_scope(&mut tx, result.last_insert_rowid(), now).await?;

    tx.commit()
        .await
        .context("commit create local user transaction")?;

    Ok(CreatedLocalUser {
        username: username.to_owned(),
        display_name: display_name.to_owned(),
        password_label: DEFAULT_APP_PASSWORD_LABEL.to_owned(),
        app_password: password,
    })
}

pub async fn list_local_app_passwords(
    pool: &SqlitePool,
    username: &str,
) -> anyhow::Result<Vec<LocalAppPassword>> {
    validate_username(username)?;
    let rows = sqlx::query(
        r#"
        SELECT id, label, created_at, last_used_at, expires_at
        FROM app_passwords
        WHERE username = ?1
        ORDER BY label
        "#,
    )
    .bind(username)
    .fetch_all(pool)
    .await
    .with_context(|| format!("list app passwords for {username}"))?;

    let mut passwords = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id")?;
        passwords.push(LocalAppPassword {
            id,
            label: row.try_get("label")?,
            created_at: row.try_get("created_at")?,
            last_used_at: row.try_get("last_used_at")?,
            expires_at: row.try_get("expires_at")?,
            scopes: list_local_app_password_scopes(pool, id).await?,
        });
    }
    Ok(passwords)
}

pub async fn create_local_app_password(
    pool: &SqlitePool,
    username: &str,
    label: &str,
    expires_at: Option<i64>,
    scopes: &[AppPasswordScopeInput],
) -> anyhow::Result<CreatedLocalAppPassword> {
    validate_username(username)?;
    let label = validate_app_password_label(label)?;
    let normalized_scopes = normalize_scope_inputs(scopes)?;
    let now = unix_timestamp();
    let password = generate_app_password();
    let password_hash = hash_password(&password).context("hash app password")?;
    let mut tx = pool
        .begin()
        .await
        .context("begin create app password transaction")?;

    ensure_user_exists_tx(&mut tx, username).await?;
    let result = sqlx::query(
        r#"
        INSERT INTO app_passwords(username, label, password_hash, created_at, expires_at)
        VALUES(?1, ?2, ?3, ?4, ?5)
        "#,
    )
    .bind(username)
    .bind(label)
    .bind(password_hash)
    .bind(now)
    .bind(expires_at)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("insert app password {label:?} for {username}"))?;
    insert_scope_inputs(&mut tx, result.last_insert_rowid(), now, &normalized_scopes).await?;

    tx.commit()
        .await
        .context("commit create app password transaction")?;

    Ok(CreatedLocalAppPassword {
        username: username.to_owned(),
        password_label: label.to_owned(),
        app_password: password,
    })
}

pub async fn reset_local_app_password(
    pool: &SqlitePool,
    username: &str,
    label: &str,
) -> anyhow::Result<ResetLocalUserPassword> {
    validate_username(username)?;
    let label = validate_app_password_label(label)?;
    let password = generate_app_password();
    let password_hash = hash_password(&password).context("hash reset app password")?;
    let result = sqlx::query(
        r#"
        UPDATE app_passwords
        SET password_hash = ?1, last_used_at = NULL
        WHERE username = ?2 AND label = ?3
        "#,
    )
    .bind(password_hash)
    .bind(username)
    .bind(label)
    .execute(pool)
    .await
    .with_context(|| format!("reset app password {label:?} for {username}"))?;
    if result.rows_affected() == 0 {
        anyhow::bail!("app password {label:?} for {username:?} does not exist");
    }
    Ok(ResetLocalUserPassword {
        username: username.to_owned(),
        password_label: label.to_owned(),
        app_password: password,
    })
}

pub async fn delete_local_app_password(
    pool: &SqlitePool,
    username: &str,
    label: &str,
) -> anyhow::Result<bool> {
    validate_username(username)?;
    let label = validate_app_password_label(label)?;
    let result = sqlx::query("DELETE FROM app_passwords WHERE username = ?1 AND label = ?2")
        .bind(username)
        .bind(label)
        .execute(pool)
        .await
        .with_context(|| format!("delete app password {label:?} for {username}"))?;
    Ok(result.rows_affected() > 0)
}

pub async fn update_local_app_password_expiry(
    pool: &SqlitePool,
    username: &str,
    label: &str,
    expires_at: Option<i64>,
) -> anyhow::Result<bool> {
    validate_username(username)?;
    let label = validate_app_password_label(label)?;
    let result = sqlx::query(
        r#"
        UPDATE app_passwords
        SET expires_at = ?1
        WHERE username = ?2 AND label = ?3
        "#,
    )
    .bind(expires_at)
    .bind(username)
    .bind(label)
    .execute(pool)
    .await
    .with_context(|| format!("update app password expiry {label:?} for {username}"))?;
    Ok(result.rows_affected() > 0)
}

pub async fn replace_local_app_password_scopes(
    pool: &SqlitePool,
    username: &str,
    label: &str,
    scopes: &[AppPasswordScopeInput],
) -> anyhow::Result<bool> {
    validate_username(username)?;
    let label = validate_app_password_label(label)?;
    let normalized_scopes = normalize_scope_inputs(scopes)?;
    let now = unix_timestamp();
    let mut tx = pool
        .begin()
        .await
        .context("begin replace app password scopes transaction")?;
    let app_password_id: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT id
        FROM app_passwords
        WHERE username = ?1 AND label = ?2
        "#,
    )
    .bind(username)
    .bind(label)
    .fetch_optional(&mut *tx)
    .await
    .with_context(|| format!("lookup app password {label:?} for {username}"))?;
    let Some(app_password_id) = app_password_id else {
        return Ok(false);
    };
    sqlx::query("DELETE FROM app_password_scopes WHERE app_password_id = ?1")
        .bind(app_password_id)
        .execute(&mut *tx)
        .await
        .context("delete old app password scopes")?;
    insert_scope_inputs(&mut tx, app_password_id, now, &normalized_scopes).await?;
    tx.commit()
        .await
        .context("commit replace app password scopes transaction")?;
    Ok(true)
}

pub async fn delete_local_user(pool: &SqlitePool, username: &str) -> anyhow::Result<bool> {
    validate_username(username)?;
    if username == BOOTSTRAP_USER {
        anyhow::bail!("refusing to delete bootstrap user {BOOTSTRAP_USER:?}");
    }

    let result = sqlx::query("DELETE FROM users WHERE username = ?1")
        .bind(username)
        .execute(pool)
        .await
        .with_context(|| format!("delete local user {username}"))?;
    Ok(result.rows_affected() > 0)
}

pub async fn update_local_user_display_name(
    pool: &SqlitePool,
    username: &str,
    display_name: &str,
) -> anyhow::Result<bool> {
    validate_username(username)?;
    let display_name = display_name.trim();
    if display_name.is_empty() {
        anyhow::bail!("display name cannot be empty");
    }

    let result = sqlx::query("UPDATE users SET display_name = ?1 WHERE username = ?2")
        .bind(display_name)
        .bind(username)
        .execute(pool)
        .await
        .with_context(|| format!("update display name for {username}"))?;
    Ok(result.rows_affected() > 0)
}

pub async fn set_local_user_enabled(
    pool: &SqlitePool,
    username: &str,
    enabled: bool,
) -> anyhow::Result<bool> {
    validate_username(username)?;
    let result = sqlx::query("UPDATE users SET enabled = ?1 WHERE username = ?2")
        .bind(if enabled { 1 } else { 0 })
        .bind(username)
        .execute(pool)
        .await
        .with_context(|| format!("set enabled={enabled} for {username}"))?;
    Ok(result.rows_affected() > 0)
}

pub async fn reset_local_user_app_password(
    pool: &SqlitePool,
    username: &str,
) -> anyhow::Result<ResetLocalUserPassword> {
    validate_username(username)?;
    let user_exists: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM users
            WHERE username = ?1
        )
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
    .with_context(|| format!("lookup local user {username}"))?;
    if user_exists == 0 {
        anyhow::bail!("local user {username:?} does not exist");
    }

    let now = unix_timestamp();
    let password = generate_app_password();
    let password_hash = hash_password(&password).context("hash reset app password")?;
    let mut tx = pool
        .begin()
        .await
        .context("begin reset app password transaction")?;

    sqlx::query("DELETE FROM app_passwords WHERE username = ?1")
        .bind(username)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("delete old app passwords for {username}"))?;
    let result = sqlx::query(
        r#"
        INSERT INTO app_passwords(username, label, password_hash, created_at)
        VALUES(?1, ?2, ?3, ?4)
        "#,
    )
    .bind(username)
    .bind(DEFAULT_APP_PASSWORD_LABEL)
    .bind(password_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("insert reset app password for {username}"))?;
    insert_default_scope(&mut tx, result.last_insert_rowid(), now).await?;

    tx.commit()
        .await
        .context("commit reset app password transaction")?;

    Ok(ResetLocalUserPassword {
        username: username.to_owned(),
        password_label: DEFAULT_APP_PASSWORD_LABEL.to_owned(),
        app_password: password,
    })
}

pub async fn enabled_admin_count(pool: &SqlitePool, admin_users: &[String]) -> anyhow::Result<i64> {
    let mut count = 0;
    for username in admin_users {
        validate_username(username)?;
        let enabled: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM users
                WHERE username = ?1 AND enabled = 1
            )
            "#,
        )
        .bind(username)
        .fetch_one(pool)
        .await
        .with_context(|| format!("lookup enabled admin user {username}"))?;
        count += enabled;
    }
    Ok(count)
}

async fn list_local_app_password_scopes(
    pool: &SqlitePool,
    app_password_id: i64,
) -> anyhow::Result<Vec<LocalAppPasswordScope>> {
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
    .context("list app password scopes")?;

    if rows.is_empty() {
        let default = permissions::default_scope();
        return Ok(vec![LocalAppPasswordScope {
            id: default.id,
            mount_path: permissions::scope_path_to_db(&default.mount_path)?,
            storage_path: permissions::scope_path_to_db(&default.storage_path)?,
            permission: default.permission,
        }]);
    }

    rows.into_iter()
        .map(|row| {
            Ok(LocalAppPasswordScope {
                id: row.try_get("id")?,
                mount_path: row.try_get("mount_path")?,
                storage_path: row.try_get("storage_path")?,
                permission: PermissionLevel::parse(&row.try_get::<String, _>("permission")?)?,
            })
        })
        .collect()
}

async fn insert_default_scope(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    app_password_id: i64,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO app_password_scopes(app_password_id, mount_path, storage_path, permission, created_at)
        VALUES(?1, '/', '/', 'full', ?2)
        "#,
    )
    .bind(app_password_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .context("insert default app password scope")?;
    Ok(())
}

#[derive(Debug, Clone)]
struct NormalizedScopeInput {
    mount_path: String,
    storage_path: String,
    permission: PermissionLevel,
}

fn normalize_scope_inputs(
    scopes: &[AppPasswordScopeInput],
) -> anyhow::Result<Vec<NormalizedScopeInput>> {
    let fallback;
    let scopes = if scopes.is_empty() {
        fallback = vec![AppPasswordScopeInput {
            mount_path: "/".to_owned(),
            storage_path: "/".to_owned(),
            permission: PermissionLevel::Full,
        }];
        fallback.as_slice()
    } else {
        scopes
    };

    let mut normalized = Vec::with_capacity(scopes.len());
    let mut scope_defs = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let mount_path = permissions::normalize_scope_path(scope.mount_path.trim())?;
        let storage_path = permissions::normalize_scope_path(scope.storage_path.trim())?;
        scope_defs.push(permissions::AppPasswordScope {
            id: 0,
            mount_path: mount_path.clone(),
            storage_path: storage_path.clone(),
            permission: scope.permission,
        });
        normalized.push(NormalizedScopeInput {
            mount_path: permissions::scope_path_to_db(&mount_path)?,
            storage_path: permissions::scope_path_to_db(&storage_path)?,
            permission: scope.permission,
        });
    }
    permissions::validate_scope_set(&scope_defs)?;
    Ok(normalized)
}

async fn insert_scope_inputs(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    app_password_id: i64,
    now: i64,
    scopes: &[NormalizedScopeInput],
) -> anyhow::Result<()> {
    for scope in scopes {
        sqlx::query(
            r#"
            INSERT INTO app_password_scopes(app_password_id, mount_path, storage_path, permission, created_at)
            VALUES(?1, ?2, ?3, ?4, ?5)
            "#,
        )
        .bind(app_password_id)
        .bind(&scope.mount_path)
        .bind(&scope.storage_path)
        .bind(scope.permission.as_str())
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("insert app password scope")?;
    }
    Ok(())
}

async fn ensure_user_exists_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    username: &str,
) -> anyhow::Result<()> {
    let exists: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM users
            WHERE username = ?1
        )
        "#,
    )
    .bind(username)
    .fetch_one(&mut **tx)
    .await
    .with_context(|| format!("lookup local user {username}"))?;
    if exists == 0 {
        anyhow::bail!("local user {username:?} does not exist");
    }
    Ok(())
}

pub fn validate_username(username: &str) -> anyhow::Result<()> {
    if username.is_empty() {
        anyhow::bail!("username cannot be empty");
    }
    if username.len() > 64 {
        anyhow::bail!("username is too long");
    }
    if username == "." || username == ".." {
        anyhow::bail!("username cannot be {username:?}");
    }
    if !username
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
    {
        anyhow::bail!(
            "username may only contain ASCII letters, numbers, dot, underscore, dash, or @"
        );
    }
    Ok(())
}

fn validate_app_password_label(label: &str) -> anyhow::Result<&str> {
    let label = label.trim();
    if label.is_empty() {
        anyhow::bail!("app password label cannot be empty");
    }
    if label.len() > 64 {
        anyhow::bail!("app password label is too long");
    }
    if !label
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        anyhow::bail!(
            "app password label may only contain ASCII letters, numbers, dot, underscore, or dash"
        );
    }
    Ok(label)
}

fn generate_app_password() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash_password(password: &str) -> anyhow::Result<String> {
    let mut salt_bytes = [0u8; 16];
    OsRng.fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|err| anyhow!("encode password salt: {err}"))?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|err| anyhow!("hash password: {err}"))?
        .to_string())
}

pub fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    pub id: i64,
    pub oc_file_id: String,
    pub etag: String,
    pub permissions: i64,
    pub favorite: bool,
    pub mtime_ns: i64,
    pub file_size: i64,
}

#[derive(Debug, Clone)]
pub struct FileRecordInput<'a> {
    pub owner: &'a str,
    pub rel_path: &'a Path,
    pub abs_path: &'a Path,
    pub instance_id: &'a str,
    pub xattr_ns: &'a str,
}

pub async fn ensure_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
) -> anyhow::Result<FileRecord> {
    upsert_file_record(pool, input, false).await
}

pub async fn assign_new_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
) -> anyhow::Result<FileRecord> {
    upsert_file_record(pool, input, true).await
}

pub async fn move_file_record(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;

    sqlx::query(
        r#"
        UPDATE file_ids
        SET rel_path = CASE
            WHEN rel_path = ?1 THEN ?2
            ELSE ?2 || substr(rel_path, length(?1) + 1)
        END
        WHERE owner = ?3 AND (rel_path = ?1 OR rel_path LIKE ?4)
        "#,
    )
    .bind(&from_rel)
    .bind(&to_rel)
    .bind(owner)
    .bind(format!("{from_rel}/%"))
    .execute(pool)
    .await
    .context("move file_id cache row")?;

    move_dead_props(pool, owner, from_rel_path, to_rel_path).await?;
    Ok(())
}

pub async fn delete_file_records(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let prefix = if rel_path.is_empty() {
        "%".to_owned()
    } else {
        format!("{rel_path}/%")
    };

    sqlx::query(
        r#"
        DELETE FROM file_ids
        WHERE owner = ?1 AND (rel_path = ?2 OR rel_path LIKE ?3)
        "#,
    )
    .bind(owner)
    .bind(&rel_path)
    .bind(&prefix)
    .execute(pool)
    .await
    .context("delete file_id cache rows")?;

    sqlx::query(
        r#"
        DELETE FROM dead_props
        WHERE owner = ?1 AND (rel_path = ?2 OR rel_path LIKE ?3)
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(prefix)
    .execute(pool)
    .await
    .context("delete dead property rows")?;

    Ok(())
}

pub async fn file_rel_path_by_id(
    pool: &SqlitePool,
    owner: &str,
    file_id: i64,
) -> anyhow::Result<Option<String>> {
    sqlx::query_scalar(
        r#"
        SELECT rel_path
        FROM file_ids
        WHERE owner = ?1 AND id = ?2
        "#,
    )
    .bind(owner)
    .bind(file_id)
    .fetch_optional(pool)
    .await
    .context("lookup file path by id")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadProp {
    pub namespace: String,
    pub name: String,
    pub xml: Vec<u8>,
}

pub async fn list_dead_props(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
) -> anyhow::Result<Vec<DeadProp>> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let rows = sqlx::query(
        r#"
        SELECT namespace, name, xml
        FROM dead_props
        WHERE owner = ?1 AND rel_path = ?2
        ORDER BY namespace, name
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .fetch_all(pool)
    .await
    .context("list dead props")?;

    rows.into_iter()
        .map(|row| {
            Ok(DeadProp {
                namespace: row.try_get("namespace")?,
                name: row.try_get("name")?,
                xml: row.try_get("xml")?,
            })
        })
        .collect()
}

pub async fn get_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");
    let xml = sqlx::query("SELECT xml FROM dead_props WHERE owner = ?1 AND rel_path = ?2 AND namespace = ?3 AND name = ?4")
        .bind(owner)
        .bind(rel_path)
        .bind(namespace)
        .bind(name)
        .fetch_optional(pool)
        .await
        .context("get dead prop")?
        .map(|row| row.try_get::<Vec<u8>, _>("xml"))
        .transpose()?;
    Ok(xml)
}

pub async fn set_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
    xml: &[u8],
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT INTO dead_props(owner, rel_path, namespace, name, xml, updated_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(owner, rel_path, namespace, name) DO UPDATE SET
            xml = excluded.xml,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(namespace)
    .bind(name)
    .bind(xml)
    .bind(now)
    .execute(pool)
    .await
    .context("set dead prop")?;
    Ok(())
}

pub async fn remove_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");

    sqlx::query("DELETE FROM dead_props WHERE owner = ?1 AND rel_path = ?2 AND namespace = ?3 AND name = ?4")
        .bind(owner)
        .bind(rel_path)
        .bind(namespace)
        .bind(name)
        .execute(pool)
        .await
        .context("remove dead prop")?;
    Ok(())
}

pub async fn copy_dead_props(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT OR REPLACE INTO dead_props(owner, rel_path, namespace, name, xml, updated_at)
        SELECT owner, ?1, namespace, name, xml, ?2
        FROM dead_props
        WHERE owner = ?3 AND rel_path = ?4
        "#,
    )
    .bind(to_rel)
    .bind(now)
    .bind(owner)
    .bind(from_rel)
    .execute(pool)
    .await
    .context("copy dead props")?;
    Ok(())
}

async fn move_dead_props(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;
    let prefix = format!("{from_rel}/%");

    sqlx::query(
        r#"
        UPDATE dead_props
        SET rel_path = CASE
            WHEN rel_path = ?1 THEN ?2
            ELSE ?2 || substr(rel_path, length(?1) + 1)
        END
        WHERE owner = ?3 AND (rel_path = ?1 OR rel_path LIKE ?4)
        "#,
    )
    .bind(from_rel)
    .bind(to_rel)
    .bind(owner)
    .bind(prefix)
    .execute(pool)
    .await
    .context("move dead props")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogEntry {
    pub file_id: i64,
    pub rel_path: String,
    pub operation: String,
    pub sync_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeLogPruneOutcome {
    pub deleted_rows: u64,
    pub floor_token: i64,
    pub current_token: i64,
}

pub async fn record_change(
    pool: &SqlitePool,
    owner: &str,
    file_id: i64,
    rel_path: &Path,
    operation: &str,
) -> anyhow::Result<i64> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let now = unix_timestamp();
    let mut tx = pool
        .begin()
        .await
        .context("begin record_change transaction")?;

    let (token,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO sync_tokens(owner, token) VALUES(?1, 1)
        ON CONFLICT(owner) DO UPDATE SET token = token + 1
        RETURNING token
        "#,
    )
    .bind(owner)
    .fetch_one(&mut *tx)
    .await
    .context("allocate sync token")?;

    sqlx::query(
        r#"
        INSERT INTO change_log(owner, file_id, rel_path, operation, sync_token, changed_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        "#,
    )
    .bind(owner)
    .bind(file_id)
    .bind(rel_path)
    .bind(operation)
    .bind(token)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("insert change_log row")?;

    tx.commit()
        .await
        .context("commit record_change transaction")?;
    Ok(token)
}

pub async fn current_sync_token(pool: &SqlitePool, owner: &str) -> anyhow::Result<i64> {
    let token = sqlx::query("SELECT token FROM sync_tokens WHERE owner = ?1")
        .bind(owner)
        .fetch_optional(pool)
        .await
        .context("load current sync token")?
        .map(|row| row.try_get::<i64, _>("token"))
        .transpose()?
        .unwrap_or(0);
    Ok(token)
}

pub async fn change_log_floor_token(
    pool: &SqlitePool,
    owner: &str,
    current_token: i64,
) -> anyhow::Result<i64> {
    let min_retained: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MIN(sync_token)
        FROM change_log
        WHERE owner = ?1 AND sync_token <= ?2
        "#,
    )
    .bind(owner)
    .bind(current_token)
    .fetch_one(pool)
    .await
    .context("load change_log floor token")?;

    Ok(min_retained
        .map(|token| token.saturating_sub(1))
        .unwrap_or(current_token))
}

pub async fn prune_change_log(
    pool: &SqlitePool,
    owner: &str,
    retention_days: u64,
    min_entries: usize,
) -> anyhow::Result<ChangeLogPruneOutcome> {
    let current_token = current_sync_token(pool, owner).await?;
    let retention_secs = retention_days
        .saturating_mul(24 * 60 * 60)
        .min(i64::MAX as u64) as i64;
    let cutoff = unix_timestamp().saturating_sub(retention_secs);
    let min_entries = i64::try_from(min_entries.max(1)).unwrap_or(i64::MAX);
    let keep_floor: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MIN(sync_token)
        FROM (
            SELECT sync_token
            FROM change_log
            WHERE owner = ?1
            ORDER BY sync_token DESC
            LIMIT ?2
        )
        "#,
    )
    .bind(owner)
    .bind(min_entries)
    .fetch_one(pool)
    .await
    .context("load change_log prune floor")?;

    let deleted_rows = if let Some(keep_floor) = keep_floor {
        sqlx::query(
            r#"
            DELETE FROM change_log
            WHERE owner = ?1 AND changed_at < ?2 AND sync_token < ?3
            "#,
        )
        .bind(owner)
        .bind(cutoff)
        .bind(keep_floor)
        .execute(pool)
        .await
        .context("prune change_log rows")?
        .rows_affected()
    } else {
        0
    };

    let floor_token = change_log_floor_token(pool, owner, current_token).await?;
    Ok(ChangeLogPruneOutcome {
        deleted_rows,
        floor_token,
        current_token,
    })
}

pub async fn list_change_log(
    pool: &SqlitePool,
    owner: &str,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    list_change_log_since(pool, owner, 0).await
}

pub async fn list_change_log_since(
    pool: &SqlitePool,
    owner: &str,
    since_token: i64,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    list_change_log_range(pool, owner, since_token, i64::MAX).await
}

pub async fn list_change_log_range(
    pool: &SqlitePool,
    owner: &str,
    since_token: i64,
    until_token: i64,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT file_id, rel_path, operation, sync_token
        FROM change_log
        WHERE owner = ?1 AND sync_token > ?2 AND sync_token <= ?3
        ORDER BY sync_token ASC
        "#,
    )
    .bind(owner)
    .bind(since_token)
    .bind(until_token)
    .fetch_all(pool)
    .await
    .context("list change_log rows")?;

    rows.into_iter()
        .map(|row| {
            Ok(ChangeLogEntry {
                file_id: row.try_get("file_id")?,
                rel_path: row.try_get("rel_path")?,
                operation: row.try_get("operation")?,
                sync_token: row.try_get("sync_token")?,
            })
        })
        .collect()
}

const UPLOAD_SESSION_TTL_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSession {
    pub upload_id: String,
    pub owner: String,
    pub target_path: String,
    pub total_size: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: i64,
}

pub async fn upsert_upload_session(
    pool: &SqlitePool,
    upload_id: &str,
    owner: &str,
    target_path: &Path,
    total_size: i64,
) -> anyhow::Result<UploadSession> {
    let target_path = storage::rel_path_string(target_path)?;
    let now = unix_timestamp();
    let expires_at = now + UPLOAD_SESSION_TTL_SECS;

    sqlx::query(
        r#"
        INSERT INTO upload_sessions(upload_id, owner, target_path, total_size, created_at, updated_at, expires_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?5, ?6)
        ON CONFLICT(upload_id) DO UPDATE SET
            owner = excluded.owner,
            target_path = excluded.target_path,
            total_size = excluded.total_size,
            updated_at = excluded.updated_at,
            expires_at = excluded.expires_at
        "#,
    )
    .bind(upload_id)
    .bind(owner)
    .bind(&target_path)
    .bind(total_size)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await
    .context("upsert upload session")?;

    load_upload_session(pool, owner, upload_id)
        .await?
        .context("upload session missing after upsert")
}

pub async fn load_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
) -> anyhow::Result<Option<UploadSession>> {
    let row = sqlx::query(
        r#"
        SELECT upload_id, owner, target_path, total_size, created_at, updated_at, expires_at
        FROM upload_sessions
        WHERE owner = ?1 AND upload_id = ?2
        "#,
    )
    .bind(owner)
    .bind(upload_id)
    .fetch_optional(pool)
    .await
    .context("load upload session")?;

    row.map(upload_session_from_row).transpose()
}

pub async fn touch_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
    total_size: Option<i64>,
) -> anyhow::Result<()> {
    let now = unix_timestamp();
    let expires_at = now + UPLOAD_SESSION_TTL_SECS;

    sqlx::query(
        r#"
        UPDATE upload_sessions
        SET total_size = COALESCE(?1, total_size),
            updated_at = ?2,
            expires_at = ?3
        WHERE owner = ?4 AND upload_id = ?5
        "#,
    )
    .bind(total_size)
    .bind(now)
    .bind(expires_at)
    .bind(owner)
    .bind(upload_id)
    .execute(pool)
    .await
    .context("touch upload session")?;

    Ok(())
}

pub async fn delete_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM upload_sessions WHERE owner = ?1 AND upload_id = ?2")
        .bind(owner)
        .bind(upload_id)
        .execute(pool)
        .await
        .context("delete upload session")?;
    Ok(())
}

pub async fn list_expired_upload_sessions(
    pool: &SqlitePool,
    cutoff: i64,
) -> anyhow::Result<Vec<UploadSession>> {
    let rows = sqlx::query(
        r#"
        SELECT upload_id, owner, target_path, total_size, created_at, updated_at, expires_at
        FROM upload_sessions
        WHERE expires_at < ?1
        ORDER BY expires_at ASC
        "#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .context("list expired upload sessions")?;

    rows.into_iter().map(upload_session_from_row).collect()
}

fn upload_session_from_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<UploadSession> {
    Ok(UploadSession {
        upload_id: row.try_get("upload_id")?,
        owner: row.try_get("owner")?,
        target_path: row.try_get("target_path")?,
        total_size: row.try_get("total_size")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

async fn upsert_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
    force_new_id: bool,
) -> anyhow::Result<FileRecord> {
    let rel_path = storage::rel_path_string(input.rel_path)?;
    let (mtime_ns, file_size) = storage::metadata_fingerprint(input.abs_path)?;
    let etag = derive_etag(mtime_ns, file_size);
    let existing = load_by_rel_path(pool, input.owner, &rel_path).await?;

    if !force_new_id {
        if let Some(existing) = existing.as_ref() {
            if existing.is_fresh(mtime_ns, file_size) {
                return Ok(existing.to_file_record(input.instance_id));
            }
        }
    }

    let mut tx = pool
        .begin()
        .await
        .context("begin file metadata transaction")?;
    let id = if force_new_id {
        delete_by_rel_path(&mut *tx, input.owner, &rel_path).await?;
        insert_file_record(&mut *tx, input.owner, &rel_path).await?
    } else if let Some(existing) = existing.as_ref() {
        existing.id
    } else if let Some(xattr_id) = read_i64_xattr(input.abs_path, input.xattr_ns, "fileid")? {
        attach_xattr_file_id(&mut *tx, input.owner, &rel_path, xattr_id).await?
    } else {
        insert_file_record(&mut *tx, input.owner, &rel_path).await?
    };

    let favorite = if force_new_id {
        false
    } else {
        read_bool_xattr(input.abs_path, input.xattr_ns, "favorite")?
            .or_else(|| existing.as_ref().map(|row| row.favorite))
            .unwrap_or(false)
    };
    let permissions = if force_new_id {
        0x3f
    } else {
        read_i64_xattr(input.abs_path, input.xattr_ns, "perms")?
            .or_else(|| existing.as_ref().and_then(|row| row.permissions))
            .unwrap_or(0x3f)
    };

    write_file_metadata_xattrs(
        input.abs_path,
        input.xattr_ns,
        FileMetadataXattrs {
            file_id: id,
            etag: &etag,
            favorite,
            permissions,
        },
    )?;

    sqlx::query(
        r#"
        UPDATE file_ids
        SET etag = ?1,
            permissions = ?2,
            favorite = ?3,
            mtime_ns = ?4,
            file_size = ?5
        WHERE owner = ?6 AND rel_path = ?7
        "#,
    )
    .bind(&etag)
    .bind(permissions)
    .bind(if favorite { 1 } else { 0 })
    .bind(mtime_ns)
    .bind(file_size)
    .bind(input.owner)
    .bind(&rel_path)
    .execute(&mut *tx)
    .await
    .context("update file metadata cache")?;

    tx.commit()
        .await
        .context("commit file metadata transaction")?;

    Ok(FileRecord {
        id,
        oc_file_id: format!("{id}{}", input.instance_id),
        etag,
        permissions,
        favorite,
        mtime_ns,
        file_size,
    })
}

pub async fn set_favorite(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    abs_path: &Path,
    instance_id: &str,
    xattr_ns: &str,
    favorite: bool,
) -> anyhow::Result<FileRecord> {
    let mut record = ensure_file_record(
        pool,
        FileRecordInput {
            owner,
            rel_path,
            abs_path,
            instance_id,
            xattr_ns,
        },
    )
    .await?;
    let rel_path = storage::rel_path_string(rel_path)?;

    write_favorite_xattr(abs_path, xattr_ns, favorite)?;
    sqlx::query(
        r#"
        UPDATE file_ids
        SET favorite = ?1
        WHERE owner = ?2 AND rel_path = ?3
        "#,
    )
    .bind(if favorite { 1 } else { 0 })
    .bind(owner)
    .bind(rel_path)
    .execute(pool)
    .await
    .context("update favorite")?;

    record.favorite = favorite;
    Ok(record)
}

async fn load_by_rel_path(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &str,
) -> anyhow::Result<Option<FileRecordRow>> {
    let row = sqlx::query(
        r#"
        SELECT id, etag, permissions, favorite, mtime_ns, file_size
        FROM file_ids
        WHERE owner = ?1 AND rel_path = ?2
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .fetch_optional(pool)
    .await
    .context("load file_id row")?;

    row.map(FileRecordRow::try_from_row).transpose()
}

async fn delete_by_rel_path(
    conn: &mut SqliteConnection,
    owner: &str,
    rel_path: &str,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM file_ids WHERE owner = ?1 AND rel_path = ?2")
        .bind(owner)
        .bind(rel_path)
        .execute(&mut *conn)
        .await
        .context("delete existing file_id row")?;
    Ok(())
}

async fn insert_file_record(
    conn: &mut SqliteConnection,
    owner: &str,
    rel_path: &str,
) -> anyhow::Result<i64> {
    let now = unix_timestamp();
    let result = sqlx::query(
        r#"
        INSERT INTO file_ids(owner, rel_path, permissions, favorite, created_at)
        VALUES(?1, ?2, ?3, 0, ?4)
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(0x3f_i64)
    .bind(now)
    .execute(&mut *conn)
    .await
    .context("insert file_id row")?;

    Ok(result.last_insert_rowid())
}

async fn attach_xattr_file_id(
    conn: &mut SqliteConnection,
    owner: &str,
    rel_path: &str,
    id: i64,
) -> anyhow::Result<i64> {
    let now = unix_timestamp();
    let updated = sqlx::query(
        r#"
        UPDATE file_ids
        SET rel_path = ?1
        WHERE owner = ?2 AND id = ?3
        "#,
    )
    .bind(rel_path)
    .bind(owner)
    .bind(id)
    .execute(&mut *conn)
    .await
    .context("reattach xattr file_id row")?
    .rows_affected();

    if updated == 0 {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO file_ids(id, owner, rel_path, permissions, favorite, created_at)
            VALUES(?1, ?2, ?3, ?4, 0, ?5)
            "#,
        )
        .bind(id)
        .bind(owner)
        .bind(rel_path)
        .bind(0x3f_i64)
        .bind(now)
        .execute(&mut *conn)
        .await
        .context("insert xattr file_id row")?;
    }

    Ok(id)
}

#[derive(Debug, Clone)]
struct FileRecordRow {
    id: i64,
    etag: Option<String>,
    permissions: Option<i64>,
    favorite: bool,
    mtime_ns: Option<i64>,
    file_size: Option<i64>,
}

impl FileRecordRow {
    fn try_from_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            etag: row.try_get("etag")?,
            permissions: row.try_get("permissions")?,
            favorite: row.try_get::<i64, _>("favorite")? != 0,
            mtime_ns: row.try_get("mtime_ns")?,
            file_size: row.try_get("file_size")?,
        })
    }

    fn is_fresh(&self, mtime_ns: i64, file_size: i64) -> bool {
        self.etag.is_some() && self.mtime_ns == Some(mtime_ns) && self.file_size == Some(file_size)
    }

    fn to_file_record(&self, instance_id: &str) -> FileRecord {
        let mtime_ns = self.mtime_ns.unwrap_or_default();
        let file_size = self.file_size.unwrap_or_default();
        FileRecord {
            id: self.id,
            oc_file_id: format!("{}{}", self.id, instance_id),
            etag: self
                .etag
                .clone()
                .unwrap_or_else(|| derive_etag(mtime_ns, file_size)),
            permissions: self.permissions.unwrap_or(0x3f),
            favorite: self.favorite,
            mtime_ns,
            file_size,
        }
    }
}

fn derive_etag(mtime_ns: i64, file_size: i64) -> String {
    format!("{file_size:x}-{mtime_ns:x}")
}

struct FileMetadataXattrs<'a> {
    file_id: i64,
    etag: &'a str,
    favorite: bool,
    permissions: i64,
}

fn write_file_metadata_xattrs(
    path: &Path,
    namespace: &str,
    values: FileMetadataXattrs<'_>,
) -> anyhow::Result<()> {
    write_xattr(path, namespace, "fileid", &values.file_id.to_string())?;
    write_xattr(path, namespace, "etag", values.etag)?;
    write_favorite_xattr(path, namespace, values.favorite)?;
    write_xattr(path, namespace, "perms", &values.permissions.to_string())?;
    Ok(())
}

fn write_favorite_xattr(path: &Path, namespace: &str, favorite: bool) -> anyhow::Result<()> {
    write_xattr(
        path,
        namespace,
        "favorite",
        if favorite { "1" } else { "0" },
    )
}

fn xattr_key(namespace: &str, name: &str) -> String {
    format!("{namespace}.{name}")
}

fn read_i64_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<i64>> {
    let Some(raw) = read_xattr(path, namespace, name)? else {
        return Ok(None);
    };
    let value = String::from_utf8(raw).context("xattr value is not UTF-8")?;
    value
        .parse::<i64>()
        .map(Some)
        .with_context(|| format!("parse xattr {name} as integer"))
}

fn read_bool_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<bool>> {
    let Some(raw) = read_xattr(path, namespace, name)? else {
        return Ok(None);
    };
    let value = String::from_utf8(raw).context("xattr value is not UTF-8")?;
    Ok(Some(value == "1" || value.eq_ignore_ascii_case("true")))
}

fn read_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
    xattr::get(path, xattr_key(namespace, name))
        .with_context(|| format!("read xattr {namespace}.{name} from {}", path.display()))
}

#[cfg(test)]
static FAIL_NEXT_XATTR_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn write_xattr(path: &Path, namespace: &str, name: &str, value: &str) -> anyhow::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_XATTR_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst) {
        anyhow::bail!("injected xattr write failure");
    }

    xattr::set(path, xattr_key(namespace, name), value.as_bytes())
        .with_context(|| format!("write xattr {namespace}.{name} to {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::auth::SqliteUserStore;

    use super::*;

    async fn temp_pool(temp: &tempfile::TempDir) -> SqlitePool {
        let config = DbConfig {
            path: temp
                .path()
                .join("gono-cloud.db")
                .to_string_lossy()
                .into_owned(),
            max_connections: 1,
        };
        let pool = connect(&config).await.expect("connect sqlite");
        migrate(&pool).await.expect("migrate sqlite");
        pool
    }

    #[tokio::test]
    async fn local_user_admin_creates_lists_and_deletes_users() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;

        let created = create_local_user(&pool, "alice", Some("Alice Example"))
            .await
            .expect("create local user");
        assert_eq!(created.username, "alice");
        assert_eq!(created.display_name, "Alice Example");
        assert_eq!(created.password_label, DEFAULT_APP_PASSWORD_LABEL);
        assert!(created.app_password.len() >= 40);

        let users = list_local_users(&pool).await.expect("list local users");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "alice");
        assert_eq!(users[0].display_name, "Alice Example");
        assert_eq!(users[0].app_password_count, 1);
        assert_eq!(
            users[0].app_password_labels,
            vec![DEFAULT_APP_PASSWORD_LABEL.to_owned()]
        );
        assert_eq!(users[0].app_passwords[0].scopes[0].mount_path, "/");
        assert_eq!(users[0].app_passwords[0].scopes[0].storage_path, "/");
        assert_eq!(
            users[0].app_passwords[0].scopes[0].permission,
            PermissionLevel::Full
        );

        let store = SqliteUserStore::new(pool.clone());
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("verify local user")
            .is_some());

        assert!(delete_local_user(&pool, "alice")
            .await
            .expect("delete local user"));
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("verify deleted local user")
            .is_none());
    }

    #[tokio::test]
    async fn app_password_admin_manages_scopes_expiry_and_single_resets() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let created = create_local_user(&pool, "alice", None)
            .await
            .expect("create alice");
        let expires_at = unix_timestamp() + 3600;
        let readonly = create_local_app_password(
            &pool,
            "alice",
            "readonly",
            Some(expires_at),
            &[AppPasswordScopeInput {
                mount_path: "/Docs".to_owned(),
                storage_path: "/Projects".to_owned(),
                permission: PermissionLevel::View,
            }],
        )
        .await
        .expect("create readonly app password");

        let store = SqliteUserStore::new(pool.clone());
        let readonly_principal = store
            .verify("alice", &readonly.app_password)
            .await
            .expect("verify readonly")
            .expect("readonly principal");
        assert_eq!(readonly_principal.app_password_label, "readonly");
        assert_eq!(readonly_principal.expires_at, Some(expires_at));
        assert_eq!(readonly_principal.scopes[0].mount_path, Path::new("Docs"));
        assert_eq!(
            readonly_principal.scopes[0].storage_path,
            Path::new("Projects")
        );
        assert_eq!(
            readonly_principal.scopes[0].permission,
            PermissionLevel::View
        );

        assert!(replace_local_app_password_scopes(
            &pool,
            "alice",
            "readonly",
            &[AppPasswordScopeInput {
                mount_path: "/Upload".to_owned(),
                storage_path: "/Inbox/Upload".to_owned(),
                permission: PermissionLevel::Full,
            }],
        )
        .await
        .expect("replace scopes"));
        assert!(
            update_local_app_password_expiry(&pool, "alice", "readonly", None)
                .await
                .expect("clear expiry")
        );

        let reset = reset_local_app_password(&pool, "alice", "readonly")
            .await
            .expect("reset single password");
        assert!(store
            .verify("alice", &readonly.app_password)
            .await
            .expect("old readonly invalid")
            .is_none());
        let reset_principal = store
            .verify("alice", &reset.app_password)
            .await
            .expect("reset readonly valid")
            .expect("reset principal");
        assert_eq!(
            reset_principal.scopes[0].storage_path,
            Path::new("Inbox/Upload")
        );
        assert_eq!(reset_principal.expires_at, None);
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("default password still valid")
            .is_some());

        assert!(delete_local_app_password(&pool, "alice", "readonly")
            .await
            .expect("delete readonly"));
        let labels = list_local_app_passwords(&pool, "alice")
            .await
            .expect("list passwords")
            .into_iter()
            .map(|password| password.label)
            .collect::<Vec<_>>();
        assert_eq!(labels, vec![DEFAULT_APP_PASSWORD_LABEL.to_owned()]);
    }

    #[tokio::test]
    async fn local_user_admin_rejects_unsafe_and_bootstrap_deletes() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        ensure_bootstrap_user(&pool)
            .await
            .expect("ensure bootstrap user");

        assert!(validate_username("alice").is_ok());
        assert!(validate_username("alice@example").is_ok());
        assert!(validate_username("").is_err());
        assert!(validate_username("../alice").is_err());
        assert!(validate_username("alice/bob").is_err());
        assert!(delete_local_user(&pool, BOOTSTRAP_USER).await.is_err());
    }

    #[tokio::test]
    async fn local_user_admin_updates_state_and_resets_passwords() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let created = create_local_user(&pool, "alice", Some("Alice Example"))
            .await
            .expect("create local user");
        let store = SqliteUserStore::new(pool.clone());

        assert!(
            update_local_user_display_name(&pool, "alice", "Alice Renamed")
                .await
                .expect("update display name")
        );
        let users = list_local_users(&pool).await.expect("list local users");
        assert_eq!(users[0].display_name, "Alice Renamed");

        assert!(set_local_user_enabled(&pool, "alice", false)
            .await
            .expect("disable user"));
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("verify disabled user")
            .is_none());

        assert!(set_local_user_enabled(&pool, "alice", true)
            .await
            .expect("enable user"));
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("verify reenabled user")
            .is_some());

        let reset = reset_local_user_app_password(&pool, "alice")
            .await
            .expect("reset app password");
        assert_eq!(reset.password_label, DEFAULT_APP_PASSWORD_LABEL);
        assert!(store
            .verify("alice", &created.app_password)
            .await
            .expect("old password invalid")
            .is_none());
        assert!(store
            .verify("alice", &reset.app_password)
            .await
            .expect("new password valid")
            .is_some());
    }

    #[tokio::test]
    async fn enabled_admin_count_counts_only_enabled_configured_users() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        create_local_user(&pool, "alice", None)
            .await
            .expect("create alice");
        create_local_user(&pool, "bob", None)
            .await
            .expect("create bob");
        set_local_user_enabled(&pool, "bob", false)
            .await
            .expect("disable bob");

        let admins = vec!["alice".to_owned(), "bob".to_owned(), "missing".to_owned()];
        assert_eq!(
            enabled_admin_count(&pool, &admins)
                .await
                .expect("count enabled admins"),
            1
        );
    }

    #[tokio::test]
    async fn configured_admin_users_are_created_once() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        ensure_bootstrap_user(&pool)
            .await
            .expect("ensure bootstrap user");

        let admins = vec![BOOTSTRAP_USER.to_owned(), "kimi".to_owned()];
        let generated = ensure_configured_admin_users(&pool, &admins)
            .await
            .expect("create configured admin users");
        assert_eq!(generated.len(), 1);
        assert_eq!(generated[0].username, "kimi");
        assert_eq!(generated[0].password_label, DEFAULT_APP_PASSWORD_LABEL);

        let store = SqliteUserStore::new(pool.clone());
        assert!(store
            .verify("kimi", &generated[0].app_password)
            .await
            .expect("verify generated admin user")
            .is_some());

        let regenerated = ensure_configured_admin_users(&pool, &admins)
            .await
            .expect("ensure existing admin users");
        assert!(regenerated.is_empty());
        assert!(store
            .verify("kimi", &generated[0].app_password)
            .await
            .expect("old admin password still valid")
            .is_some());
    }

    #[tokio::test]
    async fn xattr_write_failure_rolls_back_new_file_record() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let abs_path = temp.path().join("failed-xattr.txt");
        std::fs::write(&abs_path, "body").expect("write test file");

        FAIL_NEXT_XATTR_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = ensure_file_record(
            &pool,
            FileRecordInput {
                owner: BOOTSTRAP_USER,
                rel_path: Path::new("failed-xattr.txt"),
                abs_path: &abs_path,
                instance_id: "test",
                xattr_ns: "user.nc",
            },
        )
        .await
        .expect_err("injected xattr failure");
        assert!(format!("{err:#}").contains("injected xattr write failure"));

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM file_ids WHERE owner = ?1 AND rel_path = ?2")
                .bind(BOOTSTRAP_USER)
                .bind("failed-xattr.txt")
                .fetch_one(&pool)
                .await
                .expect("count file rows");
        assert_eq!(count, 0);
    }
}
