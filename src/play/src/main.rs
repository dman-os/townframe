//! PGLite CLI - embedded PostgreSQL via WebAssembly

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use play::{start_pglite, Config, PgResponse};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting pglite");

    // Use default XDG paths
    let config = Config::new()?;
    info!("pgroot: {:?}", config.pgroot);
    info!("pgdata: {:?}", config.pgdata);

    // Start pglite
    let handle = start_pglite(config).await?;

    // Run a simple query
    info!("Running SELECT 1...");
    let (tx, mut rx) = mpsc::channel(16);
    handle.query("SELECT 1 AS result".into(), tx).await?;

    while let Some(response) = rx.recv().await {
        match response {
            PgResponse::Data(data) => {
                info!("Received {} bytes of data", data.len());
            }
            PgResponse::Done => {
                info!("Query complete");
                break;
            }
            PgResponse::Error(e) => {
                tracing::error!("Query error: {}", e);
                break;
            }
        }
    }

    // Shutdown
    handle.shutdown().await?;
    info!("Done");

    Ok(())
}
