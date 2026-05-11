use anyhow::{bail, Context};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use serde_json::Value;
use sqlx::{Row, SqlitePool};

use crate::{
    config::Config,
    db::{self, unix_timestamp},
};

const EDITABLE_KEYS: &[&str] = &[
    "server.base_url",
    "auth.realm",
    "sync.change_log_retention_days",
    "sync.change_log_min_entries",
    "notify_push.enabled",
    "notify_push.path",
    "notify_push.advertised_types",
    "notify_push.pre_auth_ttl_secs",
    "notify_push.user_connection_limit",
    "notify_push.max_debounce_secs",
    "notify_push.ping_interval_secs",
    "notify_push.auth_timeout_secs",
    "notify_push.max_connection_secs",
    "admin.enabled",
    "admin.users",
];
const INSTANCE_ID_KEY: &str = "instance.id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsUpdate {
    pub server_base_url: String,
    pub auth_realm: String,
    pub sync_change_log_retention_days: u64,
    pub sync_change_log_min_entries: usize,
    pub notify_push_enabled: bool,
    pub notify_push_path: String,
    pub notify_push_advertised_types: Vec<String>,
    pub notify_push_pre_auth_ttl_secs: u64,
    pub notify_push_user_connection_limit: usize,
    pub notify_push_max_debounce_secs: u64,
    pub notify_push_ping_interval_secs: u64,
    pub notify_push_auth_timeout_secs: u64,
    pub notify_push_max_connection_secs: u64,
    pub admin_enabled: bool,
    pub admin_users: Vec<String>,
}

pub async fn load_effective_config(pool: &SqlitePool, base: Config) -> anyhow::Result<Config> {
    seed_missing_settings(pool, &base).await?;
    apply_saved_settings(pool, base).await
}

pub async fn get_or_create_instance_id(pool: &SqlitePool) -> anyhow::Result<String> {
    if let Some(instance_id) = load_instance_id(pool).await? {
        return Ok(instance_id);
    }

    let instance_id = generate_instance_id();
    let now = unix_timestamp();
    sqlx::query(
        r#"
        INSERT INTO settings(key, value_json, updated_at, updated_by)
        VALUES(?1, ?2, ?3, 'system')
        ON CONFLICT(key) DO NOTHING
        "#,
    )
    .bind(INSTANCE_ID_KEY)
    .bind(serde_json::to_string(&instance_id)?)
    .bind(now)
    .execute(pool)
    .await
    .context("persist instance id")?;

    load_instance_id(pool)
        .await?
        .context("instance id missing after creation")
}

pub async fn apply_saved_settings(pool: &SqlitePool, mut config: Config) -> anyhow::Result<Config> {
    let rows = sqlx::query(
        r#"
        SELECT key, value_json
        FROM settings
        ORDER BY key
        "#,
    )
    .fetch_all(pool)
    .await
    .context("load settings")?;

    for row in rows {
        let key: String = row.try_get("key")?;
        let value_json: String = row.try_get("value_json")?;
        if !EDITABLE_KEYS.contains(&key.as_str()) {
            continue;
        }
        let value: Value = serde_json::from_str(&value_json)
            .with_context(|| format!("parse setting {key} JSON"))?;
        apply_setting_value(&mut config, &key, value)?;
    }

    validate_config_subset(&config)?;
    Ok(config)
}

pub async fn save_settings_update(
    pool: &SqlitePool,
    update: &SettingsUpdate,
    updated_by: &str,
) -> anyhow::Result<()> {
    validate_update(pool, update).await?;
    let values = update_values(update)?;
    let now = unix_timestamp();
    let mut tx = pool.begin().await.context("begin settings save")?;
    for (key, value) in values {
        sqlx::query(
            r#"
            INSERT INTO settings(key, value_json, updated_at, updated_by)
            VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by
            "#,
        )
        .bind(key)
        .bind(serde_json::to_string(&value)?)
        .bind(now)
        .bind(updated_by)
        .execute(&mut *tx)
        .await
        .context("save setting")?;
    }
    tx.commit().await.context("commit settings save")?;
    Ok(())
}

async fn seed_missing_settings(pool: &SqlitePool, config: &Config) -> anyhow::Result<()> {
    let values = config_values(config)?;
    let now = unix_timestamp();
    let mut tx = pool.begin().await.context("begin settings seed")?;
    for (key, value) in values {
        sqlx::query(
            r#"
            INSERT INTO settings(key, value_json, updated_at, updated_by)
            VALUES(?1, ?2, ?3, 'config.toml')
            ON CONFLICT(key) DO NOTHING
            "#,
        )
        .bind(key)
        .bind(serde_json::to_string(&value)?)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("seed setting")?;
    }
    tx.commit().await.context("commit settings seed")?;
    Ok(())
}

async fn validate_update(pool: &SqlitePool, update: &SettingsUpdate) -> anyhow::Result<()> {
    validate_base_url(&update.server_base_url)?;
    validate_notify_path(&update.notify_push_path)?;
    if update.auth_realm.trim().is_empty() {
        bail!("auth realm cannot be empty");
    }
    if update
        .notify_push_advertised_types
        .iter()
        .any(|item| item.is_empty())
    {
        bail!("notify_push advertised types cannot contain empty values");
    }
    for username in &update.admin_users {
        db::validate_username(username)?;
    }
    if update.admin_enabled {
        let enabled = db::enabled_admin_count(pool, &update.admin_users).await?;
        if enabled == 0 {
            bail!("admin.users must contain at least one enabled local user when admin is enabled");
        }
    }
    Ok(())
}

fn validate_config_subset(config: &Config) -> anyhow::Result<()> {
    validate_base_url(&config.server.base_url)?;
    validate_notify_path(&config.notify_push.path)?;
    if config.auth.realm.trim().is_empty() {
        bail!("auth realm cannot be empty");
    }
    for username in &config.admin.users {
        db::validate_username(username)?;
    }
    Ok(())
}

async fn load_instance_id(pool: &SqlitePool) -> anyhow::Result<Option<String>> {
    let Some(value_json) = sqlx::query_scalar::<_, String>(
        r#"
        SELECT value_json
        FROM settings
        WHERE key = ?1
        "#,
    )
    .bind(INSTANCE_ID_KEY)
    .fetch_optional(pool)
    .await
    .context("load instance id")?
    else {
        return Ok(None);
    };

    let instance_id: String =
        serde_json::from_str(&value_json).context("parse instance id setting JSON")?;
    validate_instance_id(&instance_id)?;
    Ok(Some(instance_id))
}

fn generate_instance_id() -> String {
    let mut bytes = [0_u8; 12];
    OsRng.fill_bytes(&mut bytes);
    format!("i{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn validate_instance_id(value: &str) -> anyhow::Result<()> {
    if value.is_empty() {
        bail!("instance id cannot be empty");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("instance id contains unsupported characters");
    }
    if !value.as_bytes()[0].is_ascii_alphabetic() {
        bail!("instance id must start with an ASCII letter");
    }
    Ok(())
}

fn config_values(config: &Config) -> anyhow::Result<Vec<(&'static str, Value)>> {
    update_values(&SettingsUpdate {
        server_base_url: config.server.base_url.clone(),
        auth_realm: config.auth.realm.clone(),
        sync_change_log_retention_days: config.sync.change_log_retention_days,
        sync_change_log_min_entries: config.sync.change_log_min_entries,
        notify_push_enabled: config.notify_push.enabled,
        notify_push_path: config.notify_push.path.clone(),
        notify_push_advertised_types: config.notify_push.advertised_types.clone(),
        notify_push_pre_auth_ttl_secs: config.notify_push.pre_auth_ttl_secs,
        notify_push_user_connection_limit: config.notify_push.user_connection_limit,
        notify_push_max_debounce_secs: config.notify_push.max_debounce_secs,
        notify_push_ping_interval_secs: config.notify_push.ping_interval_secs,
        notify_push_auth_timeout_secs: config.notify_push.auth_timeout_secs,
        notify_push_max_connection_secs: config.notify_push.max_connection_secs,
        admin_enabled: config.admin.enabled,
        admin_users: config.admin.users.clone(),
    })
}

fn update_values(update: &SettingsUpdate) -> anyhow::Result<Vec<(&'static str, Value)>> {
    Ok(vec![
        (
            "server.base_url",
            Value::String(update.server_base_url.clone()),
        ),
        ("auth.realm", Value::String(update.auth_realm.clone())),
        (
            "sync.change_log_retention_days",
            Value::from(update.sync_change_log_retention_days),
        ),
        (
            "sync.change_log_min_entries",
            Value::from(update.sync_change_log_min_entries),
        ),
        (
            "notify_push.enabled",
            Value::Bool(update.notify_push_enabled),
        ),
        (
            "notify_push.path",
            Value::String(update.notify_push_path.clone()),
        ),
        (
            "notify_push.advertised_types",
            Value::Array(
                update
                    .notify_push_advertised_types
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        (
            "notify_push.pre_auth_ttl_secs",
            Value::from(update.notify_push_pre_auth_ttl_secs),
        ),
        (
            "notify_push.user_connection_limit",
            Value::from(update.notify_push_user_connection_limit),
        ),
        (
            "notify_push.max_debounce_secs",
            Value::from(update.notify_push_max_debounce_secs),
        ),
        (
            "notify_push.ping_interval_secs",
            Value::from(update.notify_push_ping_interval_secs),
        ),
        (
            "notify_push.auth_timeout_secs",
            Value::from(update.notify_push_auth_timeout_secs),
        ),
        (
            "notify_push.max_connection_secs",
            Value::from(update.notify_push_max_connection_secs),
        ),
        ("admin.enabled", Value::Bool(update.admin_enabled)),
        (
            "admin.users",
            Value::Array(
                update
                    .admin_users
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
    ])
}

fn apply_setting_value(config: &mut Config, key: &str, value: Value) -> anyhow::Result<()> {
    match key {
        "server.base_url" => config.server.base_url = string_value(key, value)?,
        "auth.realm" => config.auth.realm = string_value(key, value)?,
        "sync.change_log_retention_days" => {
            config.sync.change_log_retention_days = u64_value(key, value)?
        }
        "sync.change_log_min_entries" => {
            config.sync.change_log_min_entries = usize_value(key, value)?
        }
        "notify_push.enabled" => config.notify_push.enabled = bool_value(key, value)?,
        "notify_push.path" => config.notify_push.path = string_value(key, value)?,
        "notify_push.advertised_types" => {
            config.notify_push.advertised_types = string_array_value(key, value)?
        }
        "notify_push.pre_auth_ttl_secs" => {
            config.notify_push.pre_auth_ttl_secs = u64_value(key, value)?
        }
        "notify_push.user_connection_limit" => {
            config.notify_push.user_connection_limit = usize_value(key, value)?
        }
        "notify_push.max_debounce_secs" => {
            config.notify_push.max_debounce_secs = u64_value(key, value)?
        }
        "notify_push.ping_interval_secs" => {
            config.notify_push.ping_interval_secs = u64_value(key, value)?
        }
        "notify_push.auth_timeout_secs" => {
            config.notify_push.auth_timeout_secs = u64_value(key, value)?
        }
        "notify_push.max_connection_secs" => {
            config.notify_push.max_connection_secs = u64_value(key, value)?
        }
        "admin.enabled" => config.admin.enabled = bool_value(key, value)?,
        "admin.users" => config.admin.users = string_array_value(key, value)?,
        _ => bail!("unknown editable setting key {key:?}"),
    }
    Ok(())
}

fn validate_base_url(value: &str) -> anyhow::Result<()> {
    let value = value.trim();
    if !(value.starts_with("http://") || value.starts_with("https://")) {
        bail!("server.base_url must start with http:// or https://");
    }
    let uri = value
        .parse::<http::Uri>()
        .context("parse server.base_url")?;
    if uri.scheme().is_none() || uri.authority().is_none() {
        bail!("server.base_url must be an absolute URL");
    }
    Ok(())
}

fn validate_notify_path(value: &str) -> anyhow::Result<()> {
    if !value.starts_with('/') {
        bail!("notify_push.path must start with /");
    }
    if value.contains("//") {
        bail!("notify_push.path must not contain duplicate slashes");
    }
    Ok(())
}

fn string_value(key: &str, value: Value) -> anyhow::Result<String> {
    value
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("setting {key} must be a string"))
}

fn bool_value(key: &str, value: Value) -> anyhow::Result<bool> {
    value
        .as_bool()
        .with_context(|| format!("setting {key} must be a boolean"))
}

fn u64_value(key: &str, value: Value) -> anyhow::Result<u64> {
    value
        .as_u64()
        .with_context(|| format!("setting {key} must be a non-negative integer"))
}

fn usize_value(key: &str, value: Value) -> anyhow::Result<usize> {
    let value = u64_value(key, value)?;
    usize::try_from(value).with_context(|| format!("setting {key} is too large"))
}

fn string_array_value(key: &str, value: Value) -> anyhow::Result<Vec<String>> {
    value
        .as_array()
        .with_context(|| format!("setting {key} must be a string array"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .with_context(|| format!("setting {key} must contain only strings"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use crate::{config::DbConfig, db};

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
        let pool = db::connect(&config).await.expect("connect sqlite");
        db::migrate(&pool).await.expect("migrate sqlite");
        pool
    }

    #[tokio::test]
    async fn settings_seed_once_and_apply_overrides() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let mut config = Config::dev_default();
        config.server.base_url = "https://from-config.example".to_owned();

        let effective = load_effective_config(&pool, config.clone())
            .await
            .expect("seed settings");
        assert_eq!(effective.server.base_url, "https://from-config.example");

        sqlx::query("UPDATE settings SET value_json = ?1 WHERE key = 'server.base_url'")
            .bind("\"https://from-db.example\"")
            .execute(&pool)
            .await
            .expect("update setting");

        let mut changed_config = config;
        changed_config.server.base_url = "https://new-config.example".to_owned();
        let effective = load_effective_config(&pool, changed_config)
            .await
            .expect("load existing settings");
        assert_eq!(effective.server.base_url, "https://from-db.example");
    }
}
