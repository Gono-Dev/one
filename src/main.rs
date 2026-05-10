use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{bail, Context};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use gono_cloud::{
    build_router,
    consistency::{self, RepairMode},
    dav_handler::chunked_upload,
    db::{self, AppPasswordScopeInput},
    permissions::PermissionLevel,
    AppState, Config,
};
use sqlx::SqlitePool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let command = Command::parse(std::env::args().skip(1))?;
    if command == Command::Help {
        print_help();
        return Ok(());
    }

    let config_path = std::env::var("NC_DAV_CONFIG").unwrap_or_else(|_| "config.toml".to_owned());
    let config = Config::load_or_dev_default(&config_path)?;
    match command {
        Command::Serve => run_server(&config_path, config).await,
        Command::ConsistencyCheck => run_consistency_check(config).await,
        Command::ConsistencyRepair(mode) => run_consistency_repair(config, mode).await,
        Command::UserList => run_user_list(config).await,
        Command::UserAdd {
            username,
            display_name,
        } => run_user_add(config, &username, display_name.as_deref()).await,
        Command::UserDelete { username } => run_user_delete(config, &username).await,
        Command::AppPasswordList { username } => run_app_password_list(config, &username).await,
        Command::AppPasswordAdd {
            username,
            label,
            mounts,
            expires_at,
        } => run_app_password_add(config, &username, &label, &mounts, expires_at).await,
        Command::AppPasswordReset { username, label } => {
            run_app_password_reset(config, &username, &label).await
        }
        Command::AppPasswordDelete { username, label } => {
            run_app_password_delete(config, &username, &label).await
        }
        Command::AppPasswordScope {
            username,
            label,
            mounts,
            expires_at,
        } => run_app_password_scope(config, &username, &label, &mounts, expires_at).await,
        Command::Help => unreachable!("handled before config load"),
    }
}

async fn run_server(config_path: &str, config: Config) -> anyhow::Result<()> {
    let addr: SocketAddr = config.server.bind.parse().context("parse server.bind")?;
    let cert_file = config.server.cert_file.clone();
    let key_file = config.server.key_file.clone();

    if !Path::new(&config_path).exists() {
        tracing::warn!(
            "config file {config_path} not found; using development defaults, see config.example.toml"
        );
    }

    let tls = if Path::new(&cert_file).exists() && Path::new(&key_file).exists() {
        Some(
            RustlsConfig::from_pem_file(&cert_file, &key_file)
                .await
                .with_context(|| format!("load TLS certificate {cert_file} and key {key_file}"))?,
        )
    } else if std::env::var("NC_DAV_INSECURE_HTTP").as_deref() == Ok("1") {
        tracing::warn!("NC_DAV_INSECURE_HTTP=1 set; serving Basic Auth over plain HTTP");
        None
    } else {
        bail!(
            "TLS certificate or key missing: cert_file={cert_file}, key_file={key_file}. \
             Create them or set NC_DAV_INSECURE_HTTP=1 for local-only smoke testing."
        );
    };

    let initialized = AppState::initialize(config).await?;
    if let Some(password) = &initialized.bootstrap.generated_password {
        tracing::warn!("Generated app password for gono: {password}");
    }

    let upload_cleanup = chunked_upload::spawn_cleanup_task(initialized.state.clone());
    let app = build_router(initialized.state);

    let server_result = if let Some(tls) = tls {
        tracing::info!("gono-cloud listening on https://{addr}");
        let handle = Handle::<SocketAddr>::new();
        let shutdown_task = spawn_axum_server_shutdown(handle.clone());
        let result = axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("serve HTTPS");
        shutdown_task.abort();
        result
    } else {
        tracing::info!("gono-cloud listening on http://{addr}");
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .context("serve HTTP"),
            Err(err) => Err(err).context("bind HTTP listener"),
        }
    };

    stop_upload_cleanup(upload_cleanup).await;
    server_result?;

    Ok(())
}

async fn run_consistency_check(config: Config) -> anyhow::Result<()> {
    let report = consistency::check(&config).await?;
    print!("{}", report.render_text());
    if report.is_clean() {
        Ok(())
    } else {
        std::process::exit(2);
    }
}

async fn run_consistency_repair(config: Config, mode: RepairMode) -> anyhow::Result<()> {
    let report = consistency::repair(&config, mode).await?;
    print!("{}", report.render_text());
    match mode {
        RepairMode::DryRun => {
            if report.actions.is_empty() {
                Ok(())
            } else {
                std::process::exit(2);
            }
        }
        RepairMode::Apply => {
            if report.after.as_ref().is_some_and(|after| after.is_clean()) {
                Ok(())
            } else {
                std::process::exit(2);
            }
        }
    }
}

async fn user_admin_pool(config: &Config) -> anyhow::Result<SqlitePool> {
    let pool = db::connect(&config.db).await?;
    db::migrate(&pool).await?;
    Ok(pool)
}

async fn run_user_list(config: Config) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    let users = db::list_local_users(&pool).await?;
    if users.is_empty() {
        println!("No local users found.");
        return Ok(());
    }

    println!(
        "{:<24} {:<24} {:<8} DISPLAY_NAME",
        "USERNAME", "APP_PASSWORDS", "ENABLED"
    );
    for user in users {
        let app_passwords = if user.app_password_labels.is_empty() {
            "- (0)".to_owned()
        } else {
            format!(
                "{} ({})",
                user.app_password_labels.join(","),
                user.app_password_count
            )
        };
        println!(
            "{:<24} {:<24} {:<8} {}",
            user.username,
            app_passwords,
            if user.enabled { "yes" } else { "no" },
            user.display_name
        );
    }
    Ok(())
}

async fn run_user_add(
    config: Config,
    username: &str,
    display_name: Option<&str>,
) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    let created = db::create_local_user(&pool, username, display_name).await?;
    println!("Created local user: {}", created.username);
    println!("Display name: {}", created.display_name);
    println!("App password label: {}", created.password_label);
    println!("App password: {}", created.app_password);
    println!("Save this password now; it cannot be shown again.");
    Ok(())
}

async fn run_user_delete(config: Config, username: &str) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    if db::delete_local_user(&pool, username).await? {
        println!("Deleted local user: {username}");
    } else {
        println!("Local user not found: {username}");
    }
    Ok(())
}

async fn run_app_password_list(config: Config, username: &str) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    let passwords = db::list_local_app_passwords(&pool, username).await?;
    if passwords.is_empty() {
        println!("No app passwords found for {username}.");
        return Ok(());
    }
    println!(
        "{:<24} {:<20} {:<20} SCOPES",
        "LABEL", "LAST_USED", "EXPIRES"
    );
    for password in passwords {
        let scopes = password
            .scopes
            .iter()
            .map(|scope| {
                format!(
                    "{}={}:{}",
                    scope.mount_path,
                    scope.storage_path,
                    scope.permission.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<24} {:<20} {:<20} {}",
            password.label,
            password
                .last_used_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            password
                .expires_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "never".to_owned()),
            scopes
        );
    }
    Ok(())
}

async fn run_app_password_add(
    config: Config,
    username: &str,
    label: &str,
    mounts: &[String],
    expires_at: Option<i64>,
) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    let scopes = parse_mount_options(mounts)?;
    let created =
        db::create_local_app_password(&pool, username, label, expires_at, &scopes).await?;
    println!("Created app password: {}", created.password_label);
    println!("Username: {}", created.username);
    println!("App password: {}", created.app_password);
    println!("Save this password now; it cannot be shown again.");
    Ok(())
}

async fn run_app_password_reset(config: Config, username: &str, label: &str) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    let reset = db::reset_local_app_password(&pool, username, label).await?;
    println!("Reset app password: {}", reset.password_label);
    println!("Username: {}", reset.username);
    println!("App password: {}", reset.app_password);
    println!("Save this password now; it cannot be shown again.");
    Ok(())
}

async fn run_app_password_delete(
    config: Config,
    username: &str,
    label: &str,
) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    if db::delete_local_app_password(&pool, username, label).await? {
        println!("Deleted app password {label} for {username}.");
    } else {
        println!("App password not found: {username}/{label}");
    }
    Ok(())
}

async fn run_app_password_scope(
    config: Config,
    username: &str,
    label: &str,
    mounts: &[String],
    expires_at: ExpiryUpdate,
) -> anyhow::Result<()> {
    let pool = user_admin_pool(&config).await?;
    if !mounts.is_empty() {
        let scopes = parse_mount_options(mounts)?;
        if db::replace_local_app_password_scopes(&pool, username, label, &scopes).await? {
            println!("Updated scopes for app password {label}.");
        } else {
            println!("App password not found: {username}/{label}");
        }
    }
    match expires_at {
        ExpiryUpdate::Unchanged => {}
        ExpiryUpdate::Never => {
            db::update_local_app_password_expiry(&pool, username, label, None).await?;
            println!("Updated expiry for app password {label}: never.");
        }
        ExpiryUpdate::At(timestamp) => {
            db::update_local_app_password_expiry(&pool, username, label, Some(timestamp)).await?;
            println!("Updated expiry for app password {label}: {timestamp}.");
        }
    }
    if mounts.is_empty() && expires_at == ExpiryUpdate::Unchanged {
        println!("No app password changes requested.");
    }
    Ok(())
}

fn print_help() {
    println!(concat!(
        "Usage:\n",
        "  gono-cloud [serve]\n",
        "  gono-cloud consistency-check\n",
        "  gono-cloud consistency-repair [--dry-run|--apply]\n",
        "  gono-cloud user-list\n",
        "  gono-cloud user-add <username> [display-name]\n",
        "  gono-cloud user-delete <username>\n",
        "  gono-cloud app-password-list <username>\n",
        "  gono-cloud app-password-add <username> <label> [--mount /client=/storage:permission]... [--expires-at TIMESTAMP]\n",
        "  gono-cloud app-password-reset <username> <label>\n",
        "  gono-cloud app-password-delete <username> <label>\n",
        "  gono-cloud app-password-scope <username> <label> [--mount /client=/storage:permission]... [--expires-at TIMESTAMP|--no-expiry]\n",
        "\n",
        "Environment:\n",
        "  NC_DAV_CONFIG    Path to config.toml (default: config.toml)\n",
    ));
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Serve,
    ConsistencyCheck,
    ConsistencyRepair(RepairMode),
    UserList,
    UserAdd {
        username: String,
        display_name: Option<String>,
    },
    UserDelete {
        username: String,
    },
    AppPasswordList {
        username: String,
    },
    AppPasswordAdd {
        username: String,
        label: String,
        mounts: Vec<String>,
        expires_at: Option<i64>,
    },
    AppPasswordReset {
        username: String,
        label: String,
    },
    AppPasswordDelete {
        username: String,
        label: String,
    },
    AppPasswordScope {
        username: String,
        label: String,
        mounts: Vec<String>,
        expires_at: ExpiryUpdate,
    },
    Help,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpiryUpdate {
    Unchanged,
    Never,
    At(i64),
}

impl Command {
    fn parse(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let args: Vec<_> = args.into_iter().collect();
        match args.as_slice() {
            [] => Ok(Self::Serve),
            [command] if command == "serve" => Ok(Self::Serve),
            [command] if command == "consistency-check" => Ok(Self::ConsistencyCheck),
            [command] if command == "consistency-repair" => {
                Ok(Self::ConsistencyRepair(RepairMode::DryRun))
            }
            [command, mode] if command == "consistency-repair" && mode == "--dry-run" => {
                Ok(Self::ConsistencyRepair(RepairMode::DryRun))
            }
            [command, mode] if command == "consistency-repair" && mode == "--apply" => {
                Ok(Self::ConsistencyRepair(RepairMode::Apply))
            }
            [command] if command == "user-list" => Ok(Self::UserList),
            [command, username] if command == "user-add" => Ok(Self::UserAdd {
                username: username.to_owned(),
                display_name: None,
            }),
            [command, username, display_name] if command == "user-add" => Ok(Self::UserAdd {
                username: username.to_owned(),
                display_name: Some(display_name.to_owned()),
            }),
            [command, username] if command == "user-delete" => Ok(Self::UserDelete {
                username: username.to_owned(),
            }),
            [command, username] if command == "app-password-list" => Ok(Self::AppPasswordList {
                username: username.to_owned(),
            }),
            [command, username, label, rest @ ..] if command == "app-password-add" => {
                let (mounts, expires_at) = parse_app_password_add_options(rest)?;
                Ok(Self::AppPasswordAdd {
                    username: username.to_owned(),
                    label: label.to_owned(),
                    mounts,
                    expires_at,
                })
            }
            [command, username, label] if command == "app-password-reset" => {
                Ok(Self::AppPasswordReset {
                    username: username.to_owned(),
                    label: label.to_owned(),
                })
            }
            [command, username, label] if command == "app-password-delete" => {
                Ok(Self::AppPasswordDelete {
                    username: username.to_owned(),
                    label: label.to_owned(),
                })
            }
            [command, username, label, rest @ ..] if command == "app-password-scope" => {
                let (mounts, expires_at) = parse_app_password_scope_options(rest)?;
                Ok(Self::AppPasswordScope {
                    username: username.to_owned(),
                    label: label.to_owned(),
                    mounts,
                    expires_at,
                })
            }
            [command] if command == "--help" || command == "-h" || command == "help" => {
                Ok(Self::Help)
            }
            [unknown] => bail!("unknown command {unknown:?}; run gono-cloud --help"),
            _ => bail!("too many arguments; run gono-cloud --help"),
        }
    }
}

fn parse_app_password_add_options(args: &[String]) -> anyhow::Result<(Vec<String>, Option<i64>)> {
    let (mounts, expiry) = parse_app_password_options(args, false)?;
    let expires_at = match expiry {
        ExpiryUpdate::Unchanged | ExpiryUpdate::Never => None,
        ExpiryUpdate::At(timestamp) => Some(timestamp),
    };
    Ok((mounts, expires_at))
}

fn parse_app_password_scope_options(
    args: &[String],
) -> anyhow::Result<(Vec<String>, ExpiryUpdate)> {
    parse_app_password_options(args, true)
}

fn parse_app_password_options(
    args: &[String],
    allow_no_expiry: bool,
) -> anyhow::Result<(Vec<String>, ExpiryUpdate)> {
    let mut mounts = Vec::new();
    let mut expiry = ExpiryUpdate::Unchanged;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--mount" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| anyhow::anyhow!("--mount requires a value"))?;
                mounts.push(value.to_owned());
            }
            "--expires-at" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| anyhow::anyhow!("--expires-at requires a value"))?;
                expiry = ExpiryUpdate::At(parse_cli_timestamp(value)?);
            }
            "--no-expiry" if allow_no_expiry => expiry = ExpiryUpdate::Never,
            unknown => anyhow::bail!("unknown option {unknown:?}; run gono-cloud --help"),
        }
        index += 1;
    }
    Ok((mounts, expiry))
}

fn parse_mount_options(values: &[String]) -> anyhow::Result<Vec<AppPasswordScopeInput>> {
    values
        .iter()
        .map(|value| parse_mount_option(value))
        .collect()
}

fn parse_mount_option(value: &str) -> anyhow::Result<AppPasswordScopeInput> {
    let (paths, permission) = value
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("--mount must use /client=/storage:permission"))?;
    let (mount_path, storage_path) = paths
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("--mount must use /client=/storage:permission"))?;
    Ok(AppPasswordScopeInput {
        mount_path: mount_path.to_owned(),
        storage_path: storage_path.to_owned(),
        permission: match permission {
            "view" => PermissionLevel::View,
            "full" => PermissionLevel::Full,
            _ => anyhow::bail!("mount permission must be view or full"),
        },
    })
}

fn parse_cli_timestamp(value: &str) -> anyhow::Result<i64> {
    value
        .parse::<i64>()
        .with_context(|| format!("parse UNIX timestamp {value:?}"))
}

fn init_tracing() {
    let env_filter = EnvFilter::from_default_env();

    match log_format_from_env() {
        LogFormat::Text => tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init(),
        LogFormat::Compact => tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .init(),
        LogFormat::Json => tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }
}

fn log_format_from_env() -> LogFormat {
    std::env::var("NC_DAV_LOG_FORMAT")
        .ok()
        .as_deref()
        .map(LogFormat::parse)
        .unwrap_or(LogFormat::Text)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Compact,
    Json,
}

impl LogFormat {
    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            "compact" => Self::Compact,
            _ => Self::Text,
        }
    }
}

fn spawn_axum_server_shutdown(handle: Handle<SocketAddr>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(
            timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT.as_secs(),
            "starting graceful shutdown"
        );
        handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(?err, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(?err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

async fn stop_upload_cleanup(upload_cleanup: tokio::task::JoinHandle<()>) {
    upload_cleanup.abort();
    match upload_cleanup.await {
        Ok(()) => tracing::info!("upload cleanup task stopped"),
        Err(err) if err.is_cancelled() => tracing::debug!("upload cleanup task cancelled"),
        Err(err) => tracing::warn!(?err, "upload cleanup task ended unexpectedly"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, ExpiryUpdate, LogFormat, RepairMode};

    #[test]
    fn log_format_parser_accepts_supported_formats() {
        assert_eq!(LogFormat::parse("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse(" JSON "), LogFormat::Json);
        assert_eq!(LogFormat::parse("compact"), LogFormat::Compact);
        assert_eq!(LogFormat::parse("text"), LogFormat::Text);
        assert_eq!(LogFormat::parse("unknown"), LogFormat::Text);
    }

    #[test]
    fn command_parser_accepts_server_and_check_modes() {
        assert_eq!(Command::parse([]).unwrap(), Command::Serve);
        assert_eq!(
            Command::parse(["serve".to_owned()]).unwrap(),
            Command::Serve
        );
        assert_eq!(
            Command::parse(["consistency-check".to_owned()]).unwrap(),
            Command::ConsistencyCheck
        );
        assert_eq!(
            Command::parse(["consistency-repair".to_owned()]).unwrap(),
            Command::ConsistencyRepair(RepairMode::DryRun)
        );
        assert_eq!(
            Command::parse(["consistency-repair".to_owned(), "--apply".to_owned()]).unwrap(),
            Command::ConsistencyRepair(RepairMode::Apply)
        );
        assert_eq!(
            Command::parse(["user-list".to_owned()]).unwrap(),
            Command::UserList
        );
        assert_eq!(
            Command::parse(["user-add".to_owned(), "alice".to_owned()]).unwrap(),
            Command::UserAdd {
                username: "alice".to_owned(),
                display_name: None
            }
        );
        assert_eq!(
            Command::parse([
                "user-add".to_owned(),
                "alice".to_owned(),
                "Alice Example".to_owned()
            ])
            .unwrap(),
            Command::UserAdd {
                username: "alice".to_owned(),
                display_name: Some("Alice Example".to_owned())
            }
        );
        assert_eq!(
            Command::parse(["user-delete".to_owned(), "alice".to_owned()]).unwrap(),
            Command::UserDelete {
                username: "alice".to_owned()
            }
        );
        assert_eq!(
            Command::parse(["app-password-list".to_owned(), "alice".to_owned()]).unwrap(),
            Command::AppPasswordList {
                username: "alice".to_owned()
            }
        );
        assert_eq!(
            Command::parse([
                "app-password-add".to_owned(),
                "alice".to_owned(),
                "mobile".to_owned(),
                "--mount".to_owned(),
                "/Docs=/Projects:view".to_owned(),
                "--expires-at".to_owned(),
                "2000".to_owned(),
            ])
            .unwrap(),
            Command::AppPasswordAdd {
                username: "alice".to_owned(),
                label: "mobile".to_owned(),
                mounts: vec!["/Docs=/Projects:view".to_owned()],
                expires_at: Some(2000),
            }
        );
        assert_eq!(
            Command::parse([
                "app-password-scope".to_owned(),
                "alice".to_owned(),
                "mobile".to_owned(),
                "--no-expiry".to_owned(),
            ])
            .unwrap(),
            Command::AppPasswordScope {
                username: "alice".to_owned(),
                label: "mobile".to_owned(),
                mounts: Vec::new(),
                expires_at: ExpiryUpdate::Never,
            }
        );
        assert_eq!(
            Command::parse(["--help".to_owned()]).unwrap(),
            Command::Help
        );
        assert!(Command::parse(["unknown".to_owned()]).is_err());
    }
}
