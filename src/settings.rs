use anyhow::{bail, Context};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use sqlx::SqlitePool;

use crate::{
    config::Config,
    db::{self, unix_timestamp},
};

const INSTANCE_ID_KEY: &str = "instance.id";

pub async fn load_effective_config(_pool: &SqlitePool, base: Config) -> anyhow::Result<Config> {
    validate_config_subset(&base)?;
    Ok(base)
}

pub fn has_explicit_base_url(config: &Config) -> bool {
    !config.server.base_url.trim().is_empty()
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

fn validate_config_subset(config: &Config) -> anyhow::Result<()> {
    if has_explicit_base_url(config) {
        validate_base_url(&config.server.base_url)?;
    }
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
    async fn config_file_overrides_legacy_saved_settings() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let mut config = Config::dev_default();
        config.server.base_url = "https://from-config.example".to_owned();

        let effective = load_effective_config(&pool, config.clone())
            .await
            .expect("load config");
        assert_eq!(effective.server.base_url, "https://from-config.example");

        sqlx::query(
            r#"
            INSERT INTO settings(key, value_json, updated_at, updated_by)
            VALUES('server.base_url', ?1, 1, 'admin')
            "#,
        )
        .bind("\"https://from-db.example\"")
        .execute(&pool)
        .await
        .expect("insert legacy setting");

        let mut changed_config = config;
        changed_config.server.base_url = "https://new-config.example".to_owned();
        let effective = load_effective_config(&pool, changed_config)
            .await
            .expect("load changed config");
        assert_eq!(effective.server.base_url, "https://new-config.example");
    }

    #[tokio::test]
    async fn missing_base_url_stays_empty() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let mut config = Config::dev_default();
        config.server.bind = "127.0.0.1:16102".to_owned();
        config.server.base_url.clear();

        let effective = load_effective_config(&pool, config)
            .await
            .expect("load config");
        assert_eq!(effective.server.base_url, "");
    }

    #[tokio::test]
    async fn whitespace_base_url_stays_empty_for_runtime_inference() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let mut config = Config::dev_default();
        config.server.bind = "0.0.0.0:16102".to_owned();
        config.server.base_url = "  ".to_owned();

        let effective = load_effective_config(&pool, config)
            .await
            .expect("load config");
        assert_eq!(effective.server.base_url, "  ");
    }
}
