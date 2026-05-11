use std::{fs, path::Path};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub db: DbConfig,
    pub auth: AuthConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub sync: SyncConfig,
    #[serde(default)]
    pub notify_push: NotifyPushConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub cert_file: String,
    pub key_file: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub xattr_ns: String,
    #[serde(default = "StorageConfig::default_upload_min_free_bytes")]
    pub upload_min_free_bytes: u64,
    #[serde(default)]
    pub upload_min_free_percent: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DbConfig {
    pub path: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub realm: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdminConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub users: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            users: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    #[serde(default = "SyncConfig::default_change_log_retention_days")]
    pub change_log_retention_days: u64,
    #[serde(default = "SyncConfig::default_change_log_min_entries")]
    pub change_log_min_entries: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            change_log_retention_days: Self::default_change_log_retention_days(),
            change_log_min_entries: Self::default_change_log_min_entries(),
        }
    }
}

impl SyncConfig {
    fn default_change_log_retention_days() -> u64 {
        30
    }

    fn default_change_log_min_entries() -> usize {
        10_000
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotifyPushConfig {
    #[serde(default = "NotifyPushConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "NotifyPushConfig::default_path")]
    pub path: String,
    #[serde(default = "NotifyPushConfig::default_advertised_types")]
    pub advertised_types: Vec<String>,
    #[serde(default = "NotifyPushConfig::default_pre_auth_ttl_secs")]
    pub pre_auth_ttl_secs: u64,
    #[serde(default = "NotifyPushConfig::default_user_connection_limit")]
    pub user_connection_limit: usize,
    #[serde(default = "NotifyPushConfig::default_max_debounce_secs")]
    pub max_debounce_secs: u64,
    #[serde(default = "NotifyPushConfig::default_ping_interval_secs")]
    pub ping_interval_secs: u64,
    #[serde(default = "NotifyPushConfig::default_auth_timeout_secs")]
    pub auth_timeout_secs: u64,
    #[serde(default = "NotifyPushConfig::default_max_connection_secs")]
    pub max_connection_secs: u64,
}

impl Default for NotifyPushConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: "/push".to_owned(),
            advertised_types: vec![
                "files".to_owned(),
                "activities".to_owned(),
                "notifications".to_owned(),
            ],
            pre_auth_ttl_secs: 15,
            user_connection_limit: 64,
            max_debounce_secs: 15,
            ping_interval_secs: 30,
            auth_timeout_secs: 15,
            max_connection_secs: 0,
        }
    }
}

impl NotifyPushConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_path() -> String {
        "/push".to_owned()
    }

    fn default_advertised_types() -> Vec<String> {
        vec![
            "files".to_owned(),
            "activities".to_owned(),
            "notifications".to_owned(),
        ]
    }

    fn default_pre_auth_ttl_secs() -> u64 {
        15
    }

    fn default_user_connection_limit() -> usize {
        64
    }

    fn default_max_debounce_secs() -> u64 {
        15
    }

    fn default_ping_interval_secs() -> u64 {
        30
    }

    fn default_auth_timeout_secs() -> u64 {
        15
    }

    fn default_max_connection_secs() -> u64 {
        0
    }
}

impl StorageConfig {
    pub fn default_upload_min_free_bytes() -> u64 {
        1024 * 1024 * 1024
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse config file {}", path.display()))
    }

    pub fn load_or_dev_default(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            Self::load(path)
        } else {
            Ok(Self::dev_default())
        }
    }

    pub fn dev_default() -> Self {
        Self {
            server: ServerConfig {
                bind: "127.0.0.1:3000".to_owned(),
                cert_file: "certs/cert.pem".to_owned(),
                key_file: "certs/key.pem".to_owned(),
                base_url: "https://127.0.0.1:3000".to_owned(),
            },
            storage: StorageConfig {
                data_dir: "data".to_owned(),
                xattr_ns: "user.nc".to_owned(),
                upload_min_free_bytes: StorageConfig::default_upload_min_free_bytes(),
                upload_min_free_percent: 0,
            },
            db: DbConfig {
                path: "data/gono-cloud.db".to_owned(),
                max_connections: 5,
            },
            auth: AuthConfig {
                realm: "Gono Cloud".to_owned(),
            },
            admin: AdminConfig::default(),
            sync: SyncConfig::default(),
            notify_push: NotifyPushConfig::default(),
        }
    }
}
