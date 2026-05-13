use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::{
    auth::{AuthRateLimiter, SqliteUserStore},
    config::{AdminConfig, Config, NotifyPushConfig, SyncConfig},
    db::{self, BOOTSTRAP_USER, BootstrapOutcome},
    nextcloud_proto::login_flow::LoginFlowStore,
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
    pub base_url_explicit: bool,
    pub instance_id: String,
    pub xattr_ns: String,
    pub sync_config: SyncConfig,
    pub notify_push_config: NotifyPushConfig,
    pub notify_push: Option<Arc<NotifyRuntime>>,
    pub login_flows: LoginFlowStore,
    pub webdav_clients: WebDavClientRegistry,
    pub config: Config,
    change_log_prune_gate: ChangeLogPruneGate,
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
        let base_url_explicit = settings::has_explicit_base_url(&config);
        let config = settings::load_effective_config(&db, config).await?;
        let storage = StorageLayout::prepare(&config.storage)?;
        for username in &config.admin.users {
            db::validate_username(username)?;
        }
        if config.admin.enabled && config.admin.users.is_empty() {
            warn!("admin.enabled=true but admin.users is empty; no user can access /gono-admin");
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
                base_url_explicit,
                instance_id,
                xattr_ns: config.storage.xattr_ns,
                sync_config,
                notify_push_config: config.notify_push,
                notify_push,
                login_flows: LoginFlowStore::default(),
                webdav_clients: WebDavClientRegistry::default(),
                config: state_config,
                change_log_prune_gate: ChangeLogPruneGate::default(),
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

    pub async fn compact_change_log_for_owner_throttled(&self, owner: &str) {
        if self.change_log_prune_gate.should_prune(owner) {
            self.compact_change_log_for_owner(owner).await;
        }
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

const CHANGE_LOG_PRUNE_MIN_INTERVAL: Duration = Duration::from_secs(30);
const CHANGE_LOG_PRUNE_CHANGE_INTERVAL: u64 = 512;

#[derive(Debug, Default)]
struct ChangeLogPruneGate {
    owners: Mutex<HashMap<String, ChangeLogPruneState>>,
}

#[derive(Debug)]
struct ChangeLogPruneState {
    last_pruned: Instant,
    pending_changes: u64,
}

impl ChangeLogPruneGate {
    fn should_prune(&self, owner: &str) -> bool {
        self.should_prune_at(owner, Instant::now())
    }

    fn should_prune_at(&self, owner: &str, now: Instant) -> bool {
        let mut owners = self.owners.lock().expect("change_log prune gate poisoned");
        let state = owners
            .entry(owner.to_owned())
            .or_insert_with(|| ChangeLogPruneState {
                last_pruned: now,
                pending_changes: 0,
            });
        state.pending_changes = state.pending_changes.saturating_add(1);

        let due = state.pending_changes >= CHANGE_LOG_PRUNE_CHANGE_INTERVAL
            || now.duration_since(state.last_pruned) >= CHANGE_LOG_PRUNE_MIN_INTERVAL;
        if due {
            state.last_pruned = now;
            state.pending_changes = 0;
        }
        due
    }
}

fn generate_csrf_token() -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use rand::{RngCore, rngs::OsRng};

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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        CHANGE_LOG_PRUNE_CHANGE_INTERVAL, CHANGE_LOG_PRUNE_MIN_INTERVAL, ChangeLogPruneGate,
    };

    #[test]
    fn change_log_prune_gate_batches_write_bursts() {
        let gate = ChangeLogPruneGate::default();
        let start = Instant::now();

        for _ in 1..CHANGE_LOG_PRUNE_CHANGE_INTERVAL {
            assert!(!gate.should_prune_at("gono", start));
        }
        assert!(gate.should_prune_at("gono", start));
        assert!(!gate.should_prune_at("gono", start));
    }

    #[test]
    fn change_log_prune_gate_runs_after_interval() {
        let gate = ChangeLogPruneGate::default();
        let start = Instant::now();

        assert!(!gate.should_prune_at("gono", start));
        assert!(!gate.should_prune_at(
            "gono",
            start + CHANGE_LOG_PRUNE_MIN_INTERVAL - Duration::from_millis(1),
        ));
        assert!(gate.should_prune_at("gono", start + CHANGE_LOG_PRUNE_MIN_INTERVAL,));
    }
}
