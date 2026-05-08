use std::path::{Path, PathBuf};

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
    pub realm: String,
}

impl BasicAuthConfig {
    pub fn phase0() -> Self {
        Self {
            username: "gono".to_owned(),
            password: "app-password".to_owned(),
            realm: "Nextcloud".to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct AppState {
    pub files_root: PathBuf,
    pub auth: BasicAuthConfig,
    pub instance_id: String,
}

impl AppState {
    pub fn phase0(data_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create data directory {}", data_dir.display()))?;
        let files_root = data_dir
            .canonicalize()
            .with_context(|| format!("canonicalize data directory {}", data_dir.display()))?;

        Ok(Self {
            files_root,
            auth: BasicAuthConfig::phase0(),
            instance_id: "phase0".to_owned(),
        })
    }
}
