// based on https://github.com/electric-sql/pglite-bindings and https://github.com/f0rr0/pglite-oxide

mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use std::path::PathBuf;

mod install;
mod plugin;
mod protocol;
mod wire;

// Re-export plugin
pub use plugin::PglitePlugin;

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
    pub fn new() -> Res<Self> {
        let dirs = directories::ProjectDirs::from("org", "pglite", "pglite")
            .ok_or_else(|| ferr!("failed to get XDG directories"))?;

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
    /// Execute a simple SQL query
    Query(String),
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

/// Handle to communicate with a running pglite instance
#[derive(Clone)]
pub struct PgliteHandle {
    tx: tokio::sync::mpsc::Sender<(PgRequest, tokio::sync::mpsc::Sender<PgResponse>)>,
}

impl PgliteHandle {
    /// Execute a SQL query, streaming responses to the provided channel
    pub async fn query(
        &self,
        sql: String,
        response_tx: tokio::sync::mpsc::Sender<PgResponse>,
    ) -> Res<()> {
        self.tx
            .send((PgRequest::Query(sql), response_tx))
            .await
            .map_err(|_| ferr!("worker channel closed"))
    }

    /// Request worker shutdown
    pub async fn shutdown(&self) -> Res<()> {
        let (response_tx, _) = tokio::sync::mpsc::channel(1);
        self.tx
            .send((PgRequest::Shutdown, response_tx))
            .await
            .map_err(|_| ferr!("worker channel closed"))
    }
}

/// Start a pglite instance with the given configuration
///
/// This will:
/// 1. Install the runtime if needed
/// 2. Initialize the database cluster if needed
/// 3. Spawn a tokio task running the PostgreSQL backend
/// 4. Return a handle for communication
pub async fn start_pglite(config: Config) -> Res<PgliteHandle> {
    // Ensure runtime is installed
    if !config.runtime_exists() {
        info!("Installing pglite runtime...");
        install::install_runtime(&config).await?;
    }

    // Load the WASM module (with pre-compilation caching)
    let engine = wire::create_engine()?;
    let module = wire::load_module(&engine, &config)?;

    // Initialize cluster if needed (runs initdb)
    if !config.cluster_exists() {
        info!("Initializing database cluster...");
        install::init_cluster(&config, &engine, &module).await?;
    }

    // Create communication channel
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    // Create wire session and spawn worker task
    let wire_session = wire::WireSession::new(&config, &engine, &module).await?;

    tokio::spawn(async move {
        if let Err(e) = worker_loop(wire_session, rx).await {
            tracing::error!("pglite worker error: {}", e);
        }
    });

    debug!("pglite worker started");
    Ok(PgliteHandle { tx })
}

/// Main worker loop - processes requests and streams responses
async fn worker_loop(
    mut wire: wire::WireSession,
    mut rx: tokio::sync::mpsc::Receiver<(PgRequest, tokio::sync::mpsc::Sender<PgResponse>)>,
) -> Res<()> {
    // Perform initial handshake
    wire.handshake().await?;
    debug!("handshake complete");

    while let Some((request, response_tx)) = rx.recv().await {
        match request {
            PgRequest::Query(sql) => {
                let payload = protocol::build_simple_query(&sql);
                if let Err(e) = process_wire_request(&mut wire, &payload, &response_tx).await {
                    let _ = response_tx.send(PgResponse::Error(e.to_string())).await;
                }
            }
            PgRequest::Shutdown => {
                debug!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

/// Process a wire protocol request and stream responses
async fn process_wire_request(
    wire: &mut wire::WireSession,
    payload: &[u8],
    response_tx: &tokio::sync::mpsc::Sender<PgResponse>,
) -> Res<()> {
    wire.send(payload).await?;

    // Stream responses until we see ReadyForQuery
    loop {
        wire.tick().await?;

        if let Some(data) = wire.try_recv().await? {
            let has_ready = protocol::contains_ready_for_query(&data);
            let has_error = protocol::contains_error(&data);

            if has_error {
                let msg = protocol::extract_error_message(&data);
                response_tx.send(PgResponse::Error(msg)).await?;
                return Ok(());
            }

            response_tx.send(PgResponse::Data(data)).await?;

            if has_ready {
                response_tx.send(PgResponse::Done).await?;
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_select_1() -> Res<()> {
        let _ = tracing_subscriber::fmt::try_init();

        // Use temp directory for test isolation
        let temp = tempfile::tempdir()?;
        let config = Config::with_paths(temp.path().join("runtime"), temp.path().join("data"));

        let handle = start_pglite(config).await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
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
