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
    SqlitePool,
};

use crate::config::DbConfig;

pub const BOOTSTRAP_USER: &str = "gono";
const BOOTSTRAP_LABEL: &str = "bootstrap";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapOutcome {
    pub generated_password: Option<String>,
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
        .context("run SQLite migrations")
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

        sqlx::query(
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

        Some(password)
    } else {
        None
    };

    tx.commit()
        .await
        .context("commit bootstrap user transaction")?;
    Ok(BootstrapOutcome { generated_password })
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
