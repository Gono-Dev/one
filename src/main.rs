use std::{net::SocketAddr, path::Path};

use anyhow::{bail, Context};
use axum_server::tls_rustls::RustlsConfig;
use gono_one::{build_router, dav_handler::chunked_upload, AppState, Config};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

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

    let _upload_cleanup = chunked_upload::spawn_cleanup_task(initialized.state.clone());
    let app = build_router(initialized.state);

    if let Some(tls) = tls {
        tracing::info!("gono-one listening on https://{addr}");
        axum_server::bind_rustls(addr, tls)
            .serve(app.into_make_service())
            .await
            .context("serve HTTPS")?;
    } else {
        tracing::info!("gono-one listening on http://{addr}");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await.context("serve HTTP")?;
    }

    Ok(())
}
