//! The node directory and everything the CLI keeps in it, plus the booted in-process node.
//!
//! Layout of a node directory (ADR 032 section 1: the node owns its data, the CLI only knows
//! where it is):
//!
//! ```text
//! <node>/node.json        identity seed and operator policy, written once by `day init` (0600)
//! <node>/node.lock        flock held for as long as a process has the node booted; holds its pid
//! <node>/run/node.sock    the daemon's Unix socket (the node creates `run/` as 0700)
//! <node>/run/daemon.log   the background daemon's stderr
//! <node>/node.sqlite, repo/, blobs/   the node's own storage, opened by `big_node::Bindings`
//! ```

use crate::interlude::*;

use big_dispatch_core::DispatchScope;
use big_node::BigNode;
use big_node::Listeners;
use secrets_rs::SecretStore;
use sqlite_rs::SqliteRuntime;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;

/// `sockaddr_un.sun_path` holds 108 bytes on Linux (104 on the BSDs and macOS) including the
/// terminator; refuse a node directory whose socket path cannot fit instead of failing in `bind`.
const MAX_SOCKET_PATH_BYTES: usize = 100;

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
    pub node_dir: NodeDir,
}

impl Config {
    pub fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let node_dir = NodeDir::new(cli_config.node_path.clone())?;
        Ok(Self {
            cli_config,
            node_dir,
        })
    }
}

/// A node directory and the paths inside it.
#[derive(Debug, Clone)]
pub struct NodeDir {
    root: PathBuf,
}

/// What `day init` records. The identity seed is the node's keyhive identity and, through it, its
/// node id (ADR 032 section 6: one node key).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSettings {
    version: u32,
    /// 32 bytes, lowercase hex.
    identity_seed: String,
    /// The operator's grant of dispatch to the nodule service: the ceiling of what any nodule
    /// can be granted to invoke. Empty (the default) lets nodules invoke nothing.
    #[serde(default)]
    pub nodule_operations: DispatchScope,
}

impl NodeSettings {
    const VERSION: u32 = 1;

    fn seed(&self) -> [u8; 32] {
        let bytes = data_encoding::HEXLOWER
            .decode(self.identity_seed.as_bytes())
            .expect("node.json holds a hex identity seed");
        <[u8; 32]>::try_from(bytes.as_slice()).expect("node.json holds a 32 byte identity seed")
    }

    /// The node's id: the public half of the identity seed, as the repo derives its own peer id.
    pub fn node_id(&self) -> String {
        let key = ed25519_dalek::SigningKey::from_bytes(&self.seed());
        data_encoding::HEXLOWER.encode(key.verifying_key().as_bytes())
    }
}

/// The state of the daemon's socket file, learned by connecting to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SocketState {
    /// Something accepted the connection.
    Live,
    /// The file is there and nothing listens on it: a daemon died without removing it.
    Stale,
    Absent,
}

impl NodeDir {
    pub fn new(root: PathBuf) -> Res<Self> {
        let this = Self { root };
        let socket = this.socket_path();
        eyre::ensure!(
            socket.as_os_str().len() <= MAX_SOCKET_PATH_BYTES,
            "the node socket path {} is longer than a Unix socket path can be ({MAX_SOCKET_PATH_BYTES} bytes); use a shorter --node path",
            socket.display()
        );
        Ok(this)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn settings_path(&self) -> PathBuf {
        self.root.join("node.json")
    }

    fn lock_path(&self) -> PathBuf {
        self.root.join("node.lock")
    }

    pub fn run_dir(&self) -> PathBuf {
        self.root.join("run")
    }

    pub fn socket_path(&self) -> PathBuf {
        self.run_dir().join("node.sock")
    }

    pub fn log_path(&self) -> PathBuf {
        self.run_dir().join("daemon.log")
    }

    pub fn is_initialized(&self) -> bool {
        self.settings_path().is_file()
    }

    /// Creates the directory and `node.json` with a fresh identity. The settings file is created
    /// exclusively, so an existing node is refused here and never overwritten, whoever else is
    /// initializing the same directory at the same time.
    pub fn init(&self) -> Res<NodeSettings> {
        use rand::RngCore;
        std::fs::create_dir_all(&self.root)
            .wrap_err_with(|| format!("creating {}", self.root.display()))?;
        let mut seed = [0_u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let settings = NodeSettings {
            version: NodeSettings::VERSION,
            identity_seed: data_encoding::HEXLOWER.encode(&seed),
            nodule_operations: DispatchScope::default(),
        };
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.settings_path())
            .wrap_err_with(|| format!("creating {}", self.settings_path().display()))?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&settings)?)?;
        Ok(settings)
    }

    pub fn settings(&self) -> Res<NodeSettings> {
        let path = self.settings_path();
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "reading {}; is there a node here? (`day init`)",
                path.display()
            )
        })?;
        let settings: NodeSettings = serde_json::from_slice(&bytes)
            .wrap_err_with(|| format!("parsing {}", path.display()))?;
        eyre::ensure!(
            settings.version == NodeSettings::VERSION,
            "{} is version {}, this day understands version {}",
            path.display(),
            settings.version,
            NodeSettings::VERSION
        );
        Ok(settings)
    }

    /// Creates `run/` owner-only. The node's listener accepts an existing parent only if it is.
    pub fn ensure_run_dir(&self) -> Res<()> {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(self.run_dir())?;
        let run_dir = self.run_dir();
        let mode = std::fs::metadata(&run_dir)?.permissions().mode();
        eyre::ensure!(
            mode & 0o077 == 0,
            "{} is accessible to other users (mode {:o})",
            run_dir.display(),
            mode & 0o7777
        );
        Ok(())
    }

    /// Learns whether a daemon answers on the socket by connecting to it, as the node's own
    /// listener does before it replaces a stale socket.
    pub async fn probe_socket(&self) -> Res<SocketState> {
        use std::os::unix::fs::FileTypeExt;
        let path = self.socket_path();
        match std::fs::symlink_metadata(&path) {
            Ok(found) if found.file_type().is_socket() => {}
            Ok(_) => eyre::bail!("{} exists and is not a socket", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SocketState::Absent);
            }
            Err(error) => {
                return Err(error).wrap_err_with(|| format!("inspecting {}", path.display()));
            }
        }
        match tokio::net::UnixStream::connect(&path).await {
            Ok(_) => Ok(SocketState::Live),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                Ok(SocketState::Stale)
            }
            Err(error) => Err(error).wrap_err_with(|| format!("probing {}", path.display())),
        }
    }

    /// Takes the node lock, or reports the pid of whoever holds it. The lock is what makes a
    /// node's storage single-process (the node opens SQLite and keyhive archives that a second
    /// process must not): a booting daemon holds it before its socket exists, so a command that
    /// would otherwise boot its own in-process node sees it.
    pub fn try_lock(&self) -> Res<Result<NodeLock, LockHeld>> {
        let path = self.lock_path();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .wrap_err_with(|| format!("opening {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {
                file.set_len(0)?;
                std::io::Write::write_all(&mut &file, std::process::id().to_string().as_bytes())?;
                Ok(Ok(NodeLock { _file: file }))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(Err(LockHeld {
                pid: std::fs::read_to_string(&path)?.trim().parse().ok(),
            })),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(error).wrap_err_with(|| format!("locking {}", path.display()))
            }
        }
    }

    /// Blocks until the holder of the node lock has exited. Run on a blocking thread.
    pub fn wait_for_lock_release(&self) -> Res<()> {
        let path = self.lock_path();
        let file =
            std::fs::File::open(&path).wrap_err_with(|| format!("opening {}", path.display()))?;
        file.lock_shared()
            .wrap_err_with(|| format!("waiting on {}", path.display()))
    }
}

/// The node lock, released when dropped (or when the process exits, however it exits).
#[derive(Debug)]
pub struct NodeLock {
    _file: std::fs::File,
}

/// Another process holds the node lock. `pid` is what it recorded, absent if it has not written
/// it yet.
#[derive(Debug, Clone, Copy)]
pub struct LockHeld {
    pub pid: Option<u32>,
}

impl std::fmt::Display for LockHeld {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.pid {
            Some(pid) => write!(formatter, "process {pid}"),
            None => write!(formatter, "another process"),
        }
    }
}

/// A node booted in this process, with everything it must stop in order.
pub struct Booted {
    pub node: BigNode,
    stop: big_node::StopToken,
    secrets: Arc<SecretStore>,
    // Held to the end: the node's storage is this process's alone while it is booted.
    _lock: NodeLock,
}

impl Booted {
    /// Boots the node in `dir` on the lock the caller holds. `listeners` is how the caller
    /// attaches it to the outside: none for a command's own node, the socket for the daemon.
    pub async fn boot(dir: &NodeDir, lock: NodeLock, listeners: Listeners) -> Res<Self> {
        let settings = dir.settings()?;
        let runtime = Arc::new(SqliteRuntime::new(default()));
        let bindings = big_node::Bindings::in_directory(&runtime, dir.root(), None).await?;
        let secrets = Arc::new(SecretStore::boot().await?);
        let config = big_node::Config {
            repo: big_repo::Config {
                node_identity_seed: settings.seed(),
                storage: bindings.repo,
                scope_key: "day".into(),
                hidden_parts: default(),
                automerge_frontier_group_scope: default(),
                causal_checkpoint_group_scope: default(),
                group_part_group_scope: default(),
            },
            sql: bindings.sql,
            schemas: Arc::new(big_cabinet::SchemaRepo::open(std::iter::empty())?),
            secrets: Arc::clone(&secrets),
            index_concurrency: std::num::NonZeroUsize::new(2).expect("two is not zero"),
            blob_storage: bindings.blobs,
            nodule_operations: settings.nodule_operations,
            listeners,
            peers: None,
        };
        let (node, stop) = BigNode::boot(config).await?;
        Ok(Self {
            node,
            stop,
            secrets,
            _lock: lock,
        })
    }

    pub async fn stop(self) -> Res<()> {
        let Self {
            node,
            stop,
            secrets,
            _lock,
        } = self;
        drop(node);
        stop.stop().await?;
        // The node's config held the other reference and went with the boot.
        Arc::try_unwrap(secrets)
            .unwrap_or_else(|_| panic!("the stopped node still holds the secret store"))
            .stop()
            .await
    }
}
