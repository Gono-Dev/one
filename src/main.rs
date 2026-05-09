use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{bail, Context};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use gono_cloud::{
    build_router,
    consistency::{self, RepairMode},
    dav_handler::chunked_upload,
    db, AppState, Config,
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

fn print_help() {
    println!(concat!(
        "Usage:\n",
        "  gono-cloud [serve]\n",
        "  gono-cloud consistency-check\n",
        "  gono-cloud consistency-repair [--dry-run|--apply]\n",
        "  gono-cloud user-list\n",
        "  gono-cloud user-add <username> [display-name]\n",
        "  gono-cloud user-delete <username>\n",
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
    Help,
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
            [command] if command == "--help" || command == "-h" || command == "help" => {
                Ok(Self::Help)
            }
            [unknown] => bail!("unknown command {unknown:?}; run gono-cloud --help"),
            _ => bail!("too many arguments; run gono-cloud --help"),
        }
    }
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
    use super::{Command, LogFormat, RepairMode};

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
            Command::parse(["--help".to_owned()]).unwrap(),
            Command::Help
        );
        assert!(Command::parse(["unknown".to_owned()]).is_err());
    }
}
