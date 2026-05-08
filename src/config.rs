use std::{fs, path::Path};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub db: DbConfig,
    pub auth: AuthConfig,
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
            },
            db: DbConfig {
                path: "data/nc-dav.db".to_owned(),
                max_connections: 5,
            },
            auth: AuthConfig {
                realm: "Nextcloud".to_owned(),
            },
        }
    }
}
