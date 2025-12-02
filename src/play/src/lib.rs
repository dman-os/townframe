//! PGLite embedded PostgreSQL via WebAssembly
//!
//! A simplified implementation for running PostgreSQL in-process via pglite WASM.

use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::thread;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::{debug, info};

mod install;
mod protocol;
mod wire;

// Re-export for convenience
pub use install::install_runtime;

/// Configuration for the pglite instance
#[derive(Debug, Clone)]
pub struct Config {
    /// Root directory for runtime files (pglite binaries, share, lib)
    pub pgroot: PathBuf,
    /// Database cluster directory
    pub pgdata: PathBuf,
    /// Optional extension tar.gz paths to install
    pub extensions: Vec<PathBuf>,
}

impl Config {
    /// Create a new config with platform-specific XDG defaults
    pub fn new() -> Result<Self> {
        let dirs = directories::ProjectDirs::from("org", "pglite", "pglite")
            .context("failed to get XDG directories")?;

        let data_dir = dirs.data_dir().to_path_buf();
        Ok(Self {
            pgroot: data_dir.join("runtime"),
            pgdata: data_dir.join("data"),
            extensions: vec![],
        })
    }

    /// Create config with explicit paths
    pub fn with_paths(pgroot: impl Into<PathBuf>, pgdata: impl Into<PathBuf>) -> Self {
        Self {
            pgroot: pgroot.into(),
            pgdata: pgdata.into(),
            extensions: vec![],
        }
    }

    /// Add an extension archive to install
    pub fn with_extension(mut self, path: impl Into<PathBuf>) -> Self {
        self.extensions.push(path.into());
        self
    }

    /// Path to the pglite WASM module
    pub fn wasm_path(&self) -> PathBuf {
        self.pgroot.join("pglite").join("bin").join("pglite.wasi")
    }

    /// Path to the pre-compiled module cache
    pub fn cwasm_path(&self) -> PathBuf {
        self.pgroot.join("pglite.cwasm")
    }

    /// Path to the dev directory (for urandom)
    pub fn dev_path(&self) -> PathBuf {
        self.pgroot.join("dev")
    }

    /// Check if the cluster has been initialized
    pub fn cluster_exists(&self) -> bool {
        self.pgdata.join("PG_VERSION").exists()
    }

    /// Check if the runtime is installed
    pub fn runtime_exists(&self) -> bool {
        self.wasm_path().exists()
    }
}

/// Request messages sent to the pglite worker
#[derive(Debug)]
pub enum PgRequest {
    /// Execute a simple SQL query (we build wire format internally)
    Query(String),
    /// Send pre-encoded wire protocol bytes
    RawWire(Vec<u8>),
    /// Shutdown the worker
    Shutdown,
}

/// Response messages from the pglite worker
#[derive(Debug)]
pub enum PgResponse {
    /// Wire protocol data chunk
    Data(Vec<u8>),
    /// Query complete (ReadyForQuery received)
    Done,
    /// Fatal error occurred
    Error(String),
}

/// Internal message type for the worker thread
struct WorkerRequest {
    request: PgRequest,
    response_tx: std_mpsc::Sender<PgResponse>,
}

/// Handle to communicate with a running pglite instance
#[derive(Clone)]
pub struct PgliteHandle {
    tx: mpsc::Sender<WorkerRequest>,
}

impl PgliteHandle {
    /// Execute a SQL query, streaming responses to the provided channel
    pub async fn query(
        &self,
        sql: String,
        response_tx: mpsc::Sender<PgResponse>,
    ) -> Result<()> {
        let (sync_tx, sync_rx) = std_mpsc::channel();

        self.tx
            .send(WorkerRequest {
                request: PgRequest::Query(sql),
                response_tx: sync_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("worker channel closed"))?;

        // Forward responses from sync channel to async channel
        tokio::task::spawn_blocking(move || {
            while let Ok(response) = sync_rx.recv() {
                let is_done = matches!(response, PgResponse::Done | PgResponse::Error(_));
                if response_tx.blocking_send(response).is_err() {
                    break;
                }
                if is_done {
                    break;
                }
            }
        });

        Ok(())
    }

    /// Send raw wire protocol bytes, streaming responses to the provided channel
    pub async fn raw_wire(
        &self,
        data: Vec<u8>,
        response_tx: mpsc::Sender<PgResponse>,
    ) -> Result<()> {
        let (sync_tx, sync_rx) = std_mpsc::channel();

        self.tx
            .send(WorkerRequest {
                request: PgRequest::RawWire(data),
                response_tx: sync_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("worker channel closed"))?;

        // Forward responses from sync channel to async channel
        tokio::task::spawn_blocking(move || {
            while let Ok(response) = sync_rx.recv() {
                let is_done = matches!(response, PgResponse::Done | PgResponse::Error(_));
                if response_tx.blocking_send(response).is_err() {
                    break;
                }
                if is_done {
                    break;
                }
            }
        });

        Ok(())
    }

    /// Request worker shutdown
    pub async fn shutdown(&self) -> Result<()> {
        let (sync_tx, _) = std_mpsc::channel();
        self.tx
            .send(WorkerRequest {
                request: PgRequest::Shutdown,
                response_tx: sync_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("worker channel closed"))
    }
}

/// Start a pglite instance with the given configuration
///
/// This will:
/// 1. Install the runtime if needed
/// 2. Initialize the database cluster if needed
/// 3. Spawn a dedicated thread running the PostgreSQL backend
/// 4. Return a handle for communication
pub async fn start_pglite(config: Config) -> Result<PgliteHandle> {
    // Ensure runtime is installed
    if !config.runtime_exists() {
        info!("Installing pglite runtime...");
        install::install_runtime(&config).await?;
    }

    // Create communication channel (async -> sync bridge)
    let (tx, mut rx) = mpsc::channel::<WorkerRequest>(32);

    // Spawn the worker in a dedicated thread to avoid async runtime conflicts
    let config_clone = config.clone();
    thread::spawn(move || {
        if let Err(e) = worker_thread(config_clone, &mut rx) {
            tracing::error!("pglite worker error: {}", e);
        }
    });

    debug!("pglite worker started");
    Ok(PgliteHandle { tx })
}

/// Worker thread - runs outside the async runtime
fn worker_thread(
    config: Config,
    rx: &mut mpsc::Receiver<WorkerRequest>,
) -> Result<()> {
    // Load the WASM module (with pre-compilation caching)
    let engine = wire::create_engine()?;
    let module = wire::load_module(&engine, &config)?;

    // Initialize cluster if needed (runs initdb)
    if !config.cluster_exists() {
        info!("Initializing database cluster...");
        install::init_cluster(&config, &engine, &module)?;
    }

    // Create wire session
    let mut wire = wire::WireSession::new(&config, &engine, &module)?;

    // Perform initial handshake
    wire.handshake()?;
    debug!("handshake complete");

    // Process requests (blocking recv from async channel)
    while let Some(WorkerRequest { request, response_tx }) = rx.blocking_recv() {
        match request {
            PgRequest::Query(sql) => {
                let payload = protocol::build_simple_query(&sql);
                process_wire_request(&mut wire, &payload, &response_tx);
            }
            PgRequest::RawWire(data) => {
                process_wire_request(&mut wire, &data, &response_tx);
            }
            PgRequest::Shutdown => {
                debug!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

/// Process a wire protocol request and stream responses (sync version)
fn process_wire_request(
    wire: &mut wire::WireSession,
    payload: &[u8],
    response_tx: &std_mpsc::Sender<PgResponse>,
) {
    if let Err(e) = wire.send(payload) {
        let _ = response_tx.send(PgResponse::Error(e.to_string()));
        return;
    }

    // Stream responses until we see ReadyForQuery
    loop {
        if let Err(e) = wire.tick() {
            let _ = response_tx.send(PgResponse::Error(e.to_string()));
            return;
        }

        match wire.try_recv() {
            Ok(Some(data)) => {
                let has_ready = protocol::contains_ready_for_query(&data);
                let has_error = protocol::contains_error(&data);

                if has_error {
                    let msg = protocol::extract_error_message(&data);
                    let _ = response_tx.send(PgResponse::Error(msg));
                    return;
                }

                let _ = response_tx.send(PgResponse::Data(data));

                if has_ready {
                    let _ = response_tx.send(PgResponse::Done);
                    return;
                }
            }
            Ok(None) => {
                // No data yet, continue ticking
            }
            Err(e) => {
                let _ = response_tx.send(PgResponse::Error(e.to_string()));
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_select_1() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

        // Use temp directory for test isolation
        let temp = tempfile::tempdir()?;
        let config = Config::with_paths(temp.path().join("runtime"), temp.path().join("data"));

        let handle = start_pglite(config).await?;

        let (tx, mut rx) = mpsc::channel(16);
        handle.query("SELECT 1 AS result".into(), tx).await?;

        let mut got_data = false;
        let mut got_done = false;

        while let Some(response) = rx.recv().await {
            match response {
                PgResponse::Data(data) => {
                    got_data = true;
                    debug!("received {} bytes", data.len());
                }
                PgResponse::Done => {
                    got_done = true;
                    break;
                }
                PgResponse::Error(e) => {
                    panic!("query error: {}", e);
                }
            }
        }

        assert!(got_data, "should have received data");
        assert!(got_done, "should have received done");

        handle.shutdown().await?;
        Ok(())
    }
}
