use std::{path::PathBuf, sync::Arc};

use sqlx::SqlitePool;

use crate::{
    auth::SqliteUserStore,
    config::Config,
    db::{self, BootstrapOutcome, BOOTSTRAP_USER},
    storage::StorageLayout,
};

#[derive(Debug)]
pub struct AppState {
    pub data_root: PathBuf,
    pub files_root: PathBuf,
    pub uploads_root: PathBuf,
    pub db: SqlitePool,
    pub user_store: Arc<SqliteUserStore>,
    pub auth_realm: String,
    pub owner: String,
    pub base_url: String,
    pub instance_id: String,
}

#[derive(Debug)]
pub struct InitializedApp {
    pub state: Arc<AppState>,
    pub bootstrap: BootstrapOutcome,
}

impl AppState {
    pub async fn initialize(config: Config) -> anyhow::Result<InitializedApp> {
        let storage = StorageLayout::prepare(&config.storage)?;
        let db = db::connect(&config.db).await?;
        db::migrate(&db).await?;
        let bootstrap = db::ensure_bootstrap_user(&db).await?;
        let user_store = Arc::new(SqliteUserStore::new(db.clone()));

        Ok(InitializedApp {
            state: Arc::new(Self {
                data_root: storage.data_root,
                files_root: storage.files_root,
                uploads_root: storage.uploads_root,
                db,
                user_store,
                auth_realm: config.auth.realm,
                owner: BOOTSTRAP_USER.to_owned(),
                base_url: config.server.base_url,
                instance_id: "phase1".to_owned(),
            }),
            bootstrap,
        })
    }
}
