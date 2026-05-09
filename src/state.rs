use std::{path::PathBuf, sync::Arc};

use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::{
    auth::SqliteUserStore,
    config::{Config, NotifyPushConfig, SyncConfig},
    db::{self, BootstrapOutcome, BOOTSTRAP_USER},
    notify_push::NotifyRuntime,
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
    pub xattr_ns: String,
    pub sync_config: SyncConfig,
    pub notify_push_config: NotifyPushConfig,
    pub notify_push: Option<Arc<NotifyRuntime>>,
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
        let sync_config = config.sync.clone();
        let prune = db::prune_change_log(
            &db,
            BOOTSTRAP_USER,
            sync_config.change_log_retention_days,
            sync_config.change_log_min_entries,
        )
        .await?;
        if prune.deleted_rows > 0 {
            info!(
                deleted_rows = prune.deleted_rows,
                floor_token = prune.floor_token,
                current_token = prune.current_token,
                "pruned change_log during startup"
            );
        }
        let user_store = Arc::new(SqliteUserStore::new(db.clone()));
        let notify_push = config
            .notify_push
            .enabled
            .then(|| NotifyRuntime::new(config.notify_push.clone()));

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
                xattr_ns: config.storage.xattr_ns,
                sync_config,
                notify_push_config: config.notify_push,
                notify_push,
            }),
            bootstrap,
        })
    }

    pub fn notify_file_changed(&self, file_id: Option<i64>) {
        if let Some(notify_push) = &self.notify_push {
            notify_push.notify_file(&self.owner, file_id);
        }
    }

    pub async fn compact_change_log(&self) {
        match db::prune_change_log(
            &self.db,
            &self.owner,
            self.sync_config.change_log_retention_days,
            self.sync_config.change_log_min_entries,
        )
        .await
        {
            Ok(outcome) if outcome.deleted_rows > 0 => {
                info!(
                    deleted_rows = outcome.deleted_rows,
                    floor_token = outcome.floor_token,
                    current_token = outcome.current_token,
                    "pruned change_log"
                );
            }
            Ok(_) => {}
            Err(err) => warn!(?err, "failed to prune change_log"),
        }
    }
}
