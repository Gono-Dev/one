use std::{net::SocketAddr, sync::Arc};

use nc_dav::{build_router, AppState};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_dir = std::env::var("NC_DAV_DATA_DIR").unwrap_or_else(|_| "data/files".to_owned());
    let bind = std::env::var("NC_DAV_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let addr: SocketAddr = bind.parse()?;
    let state = Arc::new(AppState::phase0(data_dir)?);
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!("nc-dav phase0 listening on http://{addr}");
    tracing::info!("phase0 Basic Auth user=gono password=app-password");

    axum::serve(listener, app).await?;
    Ok(())
}
