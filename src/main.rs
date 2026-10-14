use anyhow::Result;
use n8n_rust_v5::{build_app, config::ServerConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug".into()),
        )
        .init();

    let config = ServerConfig::default();
    let addr = config.socket_addr()?;

    info!(
        "Initializing n8n-rust-v5 native engine & UI server on {}",
        addr
    );
    info!(
        "Serving frontend assets from: {}",
        config.ui_dist_path.display()
    );

    let app = build_app(&config.ui_dist_path);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!(
        "Editor UI is accessible at: http://127.0.0.1:{}/",
        config.port
    );

    axum::serve(listener, app).await?;

    Ok(())
}
