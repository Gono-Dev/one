use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{bail, Context};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use gono_one::{build_router, dav_handler::chunked_upload, AppState, Config};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::var("NC_DAV_CONFIG").unwrap_or_else(|_| "config.toml".to_owned());
    let config = Config::load_or_dev_default(&config_path)?;
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
        tracing::info!("gono-one listening on https://{addr}");
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
        tracing::info!("gono-one listening on http://{addr}");
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
    use super::LogFormat;

    #[test]
    fn log_format_parser_accepts_supported_formats() {
        assert_eq!(LogFormat::parse("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse(" JSON "), LogFormat::Json);
        assert_eq!(LogFormat::parse("compact"), LogFormat::Compact);
        assert_eq!(LogFormat::parse("text"), LogFormat::Text);
        assert_eq!(LogFormat::parse("unknown"), LogFormat::Text);
    }
}
