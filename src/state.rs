use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::{
    auth::{AuthRateLimiter, SqliteUserStore},
    config::{AdminConfig, Config, NotifyPushConfig, SyncConfig},
    db::{self, BootstrapOutcome, BOOTSTRAP_USER},
    notify_push::NotifyRuntime,
    settings,
    storage::{self, StorageLayout},
    webdav_clients::WebDavClientRegistry,
};

#[derive(Debug)]
pub struct AppState {
    pub data_root: PathBuf,
    pub files_root: PathBuf,
    pub uploads_root: PathBuf,
    pub db: SqlitePool,
    pub user_store: Arc<SqliteUserStore>,
    pub auth_rate_limiter: AuthRateLimiter,
    pub auth_realm: String,
    pub admin_config: AdminConfig,
    pub admin_csrf_token: String,
    pub owner: String,
    pub base_url: String,
    pub instance_id: String,
    pub xattr_ns: String,
    pub sync_config: SyncConfig,
    pub notify_push_config: NotifyPushConfig,
    pub notify_push: Option<Arc<NotifyRuntime>>,
    pub webdav_clients: WebDavClientRegistry,
    pub config: Config,
}

#[derive(Debug)]
pub struct InitializedApp {
    pub state: Arc<AppState>,
    pub bootstrap: BootstrapOutcome,
}

impl AppState {
    pub async fn initialize(config: Config) -> anyhow::Result<InitializedApp> {
        let db = db::connect(&config.db).await?;
        db::migrate(&db).await?;
        let config = settings::load_effective_config(&db, config).await?;
        let storage = StorageLayout::prepare(&config.storage)?;
        for username in &config.admin.users {
            db::validate_username(username)?;
        }
        if config.admin.enabled && config.admin.users.is_empty() {
            warn!("admin.enabled=true but admin.users is empty; no user can access /admin");
        }
        let mut bootstrap = db::ensure_bootstrap_user(&db).await?;
        if config.admin.enabled {
            bootstrap.generated_admin_users =
                db::ensure_configured_admin_users(&db, &config.admin.users).await?;
        }
        let instance_id = settings::get_or_create_instance_id(&db).await?;
        let sync_config = config.sync.clone();
        prune_startup_change_logs(&db, &sync_config).await?;
        let user_store = Arc::new(SqliteUserStore::new(db.clone()));
        let notify_push = config
            .notify_push
            .enabled
            .then(|| NotifyRuntime::new(config.notify_push.clone()));
        let state_config = config.clone();

        Ok(InitializedApp {
            state: Arc::new(Self {
                data_root: storage.data_root,
                files_root: storage.files_root,
                uploads_root: storage.uploads_root,
                db,
                user_store,
                auth_rate_limiter: AuthRateLimiter::new(),
                auth_realm: config.auth.realm,
                admin_config: config.admin,
                admin_csrf_token: generate_csrf_token(),
                owner: BOOTSTRAP_USER.to_owned(),
                base_url: config.server.base_url,
                instance_id,
                xattr_ns: config.storage.xattr_ns,
                sync_config,
                notify_push_config: config.notify_push,
                notify_push,
                webdav_clients: WebDavClientRegistry::default(),
                config: state_config,
            }),
            bootstrap,
        })
    }

    pub fn files_root_for_owner(&self, owner: &str) -> anyhow::Result<PathBuf> {
        db::validate_username(owner)?;
        Ok(self.data_root.join("users").join(owner).join("files"))
    }

    pub async fn ensure_files_root_for_owner(&self, owner: &str) -> anyhow::Result<PathBuf> {
        db::validate_username(owner)?;
        let rel_root = Path::new("users").join(owner).join("files");
        let files_root = self.data_root.join(&rel_root);
        tokio::fs::create_dir_all(&files_root).await?;
        storage::safe_existing_path(&self.data_root, &rel_root)
    }

    pub fn notify_file_changed(&self, file_id: Option<i64>) {
        self.notify_file_changed_for_owner(&self.owner, file_id);
    }

    pub fn notify_file_changed_for_owner(&self, owner: &str, file_id: Option<i64>) {
        if let Some(notify_push) = &self.notify_push {
            notify_push.notify_file(owner, file_id);
        }
    }

    pub async fn compact_change_log(&self) {
        self.compact_change_log_for_owner(&self.owner).await;
    }

    pub async fn compact_change_log_for_owner(&self, owner: &str) {
        match db::prune_change_log(
            &self.db,
            owner,
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

fn generate_csrf_token() -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use rand::{rngs::OsRng, RngCore};

    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

async fn prune_startup_change_logs(
    pool: &SqlitePool,
    sync_config: &SyncConfig,
) -> anyhow::Result<()> {
    for user in db::list_local_users(pool)
        .await?
        .into_iter()
        .filter(|user| user.enabled)
    {
        let prune = db::prune_change_log(
            pool,
            &user.username,
            sync_config.change_log_retention_days,
            sync_config.change_log_min_entries,
        )
        .await?;
        if prune.deleted_rows > 0 {
            info!(
                owner = %user.username,
                deleted_rows = prune.deleted_rows,
                floor_token = prune.floor_token,
                current_token = prune.current_token,
                "pruned change_log during startup"
            );
        }
    }
    Ok(())
}
