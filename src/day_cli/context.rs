//! The node directory and everything the CLI keeps in it, plus the booted in-process node.
//!
//! Layout of a node directory (ADR 033 section 1: the node owns its data, the CLI only knows
//! where it is):
//!
//! ```text
//! <node>/node.json        identity seed, operator policy and peers, created by `day node init` (0600)
//! <node>/node.lock        flock held for as long as a process has the node booted; holds its pid
//! <node>/run/daemon.log   the background daemon's stderr
//! <node>/node.sqlite, repo/, blobs/   the node's own storage, opened by `big_node::Bindings`
//! ```
//!
//! The daemon's Unix socket is not in the node directory but in the per-user runtime directory,
//! named by the node id (see [`NodeDir::socket_path`]), so a node directory can be anywhere,
//! however long its path, and two nodes on a machine never share a socket. Every other secret a
//! node keeps is in the platform keyring under entries that carry the node id too.

use crate::interlude::*;

use big_node::BigNode;
use big_node::Listeners;
use secrets_rs::SecretStore;
use sqlite_rs::SqliteRuntime;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;

/// `sockaddr_un.sun_path` holds 108 bytes on Linux (104 on the BSDs and macOS) including the
/// terminator; refuse a socket path that cannot fit instead of failing in `bind`.
const MAX_SOCKET_PATH_BYTES: usize = 100;

/// The directory the daemon sockets live in: the one place the socket location is decided, so
/// making it configurable is a change to this function and its caller [`NodeDir::new`].
///
/// - **Normally** `$XDG_RUNTIME_DIR/day`: the per-user runtime directory the login session
///   creates as `0700` and removes at logout, which is where a local daemon's socket belongs
///   (ADR 033 section 5). `directories` yields it only where the platform has one (Linux).
/// - **Without one** (macOS, a session with no `XDG_RUNTIME_DIR`, a cron job) `/tmp/day-<euid>`:
///   a directory of the user's own under `/tmp` rather than `std::env::temp_dir()`, because
///   macOS's per-user temp dir is already ~50 bytes and a socket named by a 64 character node id
///   would not fit in `sun_path`. A shared `/tmp` lets another user pre-create the directory, so
///   [`NodeDir::ensure_socket_dir`] only accepts one that this uid owns and nobody else can enter.
fn default_socket_dir() -> PathBuf {
    let runtime =
        directories::BaseDirs::new().and_then(|dirs| dirs.runtime_dir().map(Path::to_path_buf));
    // SAFETY: geteuid(2) takes no arguments and cannot fail.
    socket_dir_in(runtime.as_deref(), unsafe { libc::geteuid() })
}

/// [`default_socket_dir`] for a given runtime directory and uid.
pub(crate) fn socket_dir_in(runtime_dir: Option<&Path>, uid: u32) -> PathBuf {
    match runtime_dir {
        Some(runtime) => runtime.join("day"),
        None => PathBuf::from(format!("/tmp/day-{uid}")),
    }
}

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
    pub node_dir: NodeDir,
}

impl Config {
    pub fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let node_dir = NodeDir::new(cli_config.node_path.clone());
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
    /// Where this node's socket lives; [`default_socket_dir`] unless a test places it.
    socket_dir: PathBuf,
}

/// What `day node init` records. The identity seed is the node's keyhive identity and, through it, its
/// node id (ADR 033 section 6: one node key).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSettings {
    version: u32,
    /// 32 bytes, lowercase hex.
    identity_seed: String,
    /// The node's network attachment. Absent: the daemon attaches none, so the node neither
    /// dials nor can be dialed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peers: Option<PeerSettings>,
    /// Run the node as a relay (ADR 038 section 3): accept any peer and mirror every drawer the
    /// node is a member of, which a user grants with `day drawers add-member`. A relay needs the
    /// network attachment, so this requires a `peers` section.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub relay: bool,
}

/// The `peers` section of `node.json`: its presence is what makes the daemon attach the peers
/// service (the Iroh endpoint, keyed by the node's identity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PeerSettings {
    /// Peers dialed as soon as the daemon starts and redialed with backoff when the connection
    /// drops, as endpoint tickets (the `address` a node's `getNodeInfo` reports). Which nodes
    /// belong here is the composition's policy, not the node's: a user node lists its own other
    /// devices and a relay, a relay lists none.
    #[serde(default)]
    pub bootstrap: Vec<String>,
    /// Bind the loopback interface only, with relays off (see
    /// `big_node::peers::PeersConfig::loopback_only`).
    #[serde(default)]
    pub loopback_only: bool,
}

impl PeerSettings {
    /// The id (lowercase hex) of the endpoint a bootstrap ticket names.
    fn ticket_node_id(ticket: &str) -> Res<String> {
        let addr = big_node::peers::parse_ticket(ticket)?;
        Ok(data_encoding::HEXLOWER.encode(addr.id.as_bytes()))
    }

    /// Adds `ticket`, replacing an entry for the same endpoint (so a node that changed its
    /// addresses is re-added, not duplicated). Returns whether the list changed.
    pub fn add_bootstrap(&mut self, ticket: &str) -> Res<bool> {
        let id = Self::ticket_node_id(ticket)?;
        for existing in &mut self.bootstrap {
            if Self::ticket_node_id(existing)? == id {
                let changed = existing != ticket;
                ticket.clone_into(existing);
                return Ok(changed);
            }
        }
        self.bootstrap.push(ticket.to_owned());
        Ok(true)
    }

    /// The peers config the daemon boots with: dial the bootstrap list under the default
    /// mirroring policy, or under the relay policy when the node is a relay.
    fn peers_config(&self, relay: bool) -> Res<big_node::peers::PeersConfig> {
        let dial = self
            .bootstrap
            .iter()
            .map(|ticket| big_node::peers::parse_ticket(ticket))
            .collect::<Res<Vec<_>>>()?;
        Ok(big_node::peers::PeersConfig {
            loopback_only: self.loopback_only,
            policy: if relay {
                big_node::peers::PolicyChoice::Custom(Arc::new(big_node::peers::RelayPolicy::new(
                    dial,
                )))
            } else {
                big_node::peers::PolicyChoice::Mirror(big_node::peers::MirrorPolicy { dial })
            },
            ..default()
        })
    }
}

impl NodeSettings {
    const VERSION: u32 = 1;

    pub fn seed(&self) -> [u8; 32] {
        let bytes = data_encoding::HEXLOWER
            .decode(self.identity_seed.as_bytes())
            .expect("node.json holds a hex identity seed");
        <[u8; 32]>::try_from(bytes.as_slice()).expect("node.json holds a 32 byte identity seed")
    }

    /// The node's id: the public half of the identity seed, as the repo derives its own peer id.
    pub fn node_id(&self) -> String {
        big_node::node_id(&self.seed())
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
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            socket_dir: default_socket_dir(),
        }
    }

    /// A node directory whose socket lives in `socket_dir`, so a test does not touch the real
    /// runtime directory.
    #[cfg(test)]
    pub fn with_socket_dir(root: PathBuf, socket_dir: PathBuf) -> Self {
        Self { root, socket_dir }
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

    /// The daemon's socket: `<socket dir>/<node id>.sock`. The node must be initialized, as the
    /// id is read from `node.json`. It is refused when it cannot fit in `sun_path`, which a
    /// 64 character id leaves ~35 bytes of directory for on macOS.
    pub fn socket_path(&self) -> Res<PathBuf> {
        let socket = self
            .socket_dir
            .join(format!("{}.sock", self.settings()?.node_id()));
        eyre::ensure!(
            socket.as_os_str().len() <= MAX_SOCKET_PATH_BYTES,
            "the node socket path {} is longer than a Unix socket path can be ({MAX_SOCKET_PATH_BYTES} bytes); use a shorter XDG_RUNTIME_DIR",
            socket.display()
        );
        Ok(socket)
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
            peers: None,
            relay: false,
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
                "reading {}; is there a node here? (`day node init`)",
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
        eyre::ensure!(
            !settings.relay || settings.peers.is_some(),
            "{}: a relay needs a `peers` section (it is reached over the network)",
            path.display()
        );
        if let Some(peers) = &settings.peers {
            // Fail on a bad ticket whichever command reads the settings, not when a daemon
            // happens to boot it.
            peers
                .peers_config(settings.relay)
                .wrap_err_with(|| format!("{}: bootstrap list", path.display()))?;
        }
        Ok(settings)
    }

    /// Rewrites `node.json` with `change` applied. The new file is written beside it and renamed
    /// over it, so a reader (a daemon booting) sees the old settings or the new, never half.
    /// Whether the node is serving does not matter: settings are read once, at boot.
    pub fn update_settings<T>(&self, change: impl FnOnce(&mut NodeSettings) -> Res<T>) -> Res<T> {
        let mut settings = self.settings()?;
        let out = change(&mut settings)?;
        let staging = self.root.join("node.json.new");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&staging)
            .wrap_err_with(|| format!("creating {}", staging.display()))?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&settings)?)?;
        file.sync_all()?;
        std::fs::rename(&staging, self.settings_path())
            .wrap_err_with(|| format!("replacing {}", self.settings_path().display()))?;
        Ok(out)
    }

    /// Creates `run/` (the daemon's log) and the socket directory, each owner-only.
    pub fn ensure_run_dir(&self) -> Res<()> {
        Self::ensure_private_dir(&self.run_dir())?;
        Self::ensure_private_dir(&self.socket_dir)
    }

    /// Creates `dir` as `0700` if it is missing and refuses it unless this uid owns it and no
    /// other user can enter it: the node's listener checks the mode of an existing parent too, but
    /// a directory in a shared `/tmp` could also have been created by someone else.
    fn ensure_private_dir(dir: &Path) -> Res<()> {
        use std::os::unix::fs::DirBuilderExt;
        use std::os::unix::fs::MetadataExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        let found = std::fs::metadata(dir)?;
        let mode = found.permissions().mode();
        eyre::ensure!(
            mode & 0o077 == 0,
            "{} is accessible to other users (mode {:o})",
            dir.display(),
            mode & 0o7777
        );
        // SAFETY: geteuid(2) takes no arguments and cannot fail.
        let uid = unsafe { libc::geteuid() };
        eyre::ensure!(
            found.uid() == uid,
            "{} is owned by uid {}, not by this user (uid {uid})",
            dir.display(),
            found.uid()
        );
        Ok(())
    }

    /// Learns whether a daemon answers on the socket by connecting to it, as the node's own
    /// listener does before it replaces a stale socket.
    pub async fn probe_socket(&self) -> Res<SocketState> {
        use std::os::unix::fs::FileTypeExt;
        let path = self.socket_path()?;
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

    fn boot_lock_path(&self) -> PathBuf {
        self.root.join("node.boot")
    }

    /// Takes the node lock as `role`, or reports who holds it. The lock is what makes a node's
    /// storage single-process (the node opens SQLite and keyhive archives that a second process
    /// must not), and whoever holds it serves the node's socket.
    ///
    /// Two files carry that. `node.lock` is the holder's identity, held for as long as it lives,
    /// and records the holder's pid, its socket and its role. `node.boot` is held from before
    /// `node.lock` is taken until the holder's socket accepts connections ([`NodeLock::serving`]),
    /// so a process that finds the node held can block on it ([`Self::wait_until_serving`])
    /// instead of polling for the socket. A process that only probes holds `node.boot` briefly
    /// too, which can make a booting holder see the node as held for that moment; it then waits
    /// out the probe and takes the lock on its next attempt.
    pub fn try_lock(&self, role: LockRole) -> Res<Result<NodeLock, LockHeld>> {
        let path = self.lock_path();
        let boot = Self::open_lock_file(&self.boot_lock_path())?;
        match boot.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(Err(LockHeld::read(&path)?)),
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error).wrap_err("locking the node's boot lock");
            }
        }
        let file = Self::open_lock_file(&path)?;
        match file.try_lock() {
            Ok(()) => {
                file.set_len(0)?;
                std::io::Write::write_all(
                    &mut &file,
                    format!(
                        "{}
{}
{}",
                        std::process::id(),
                        self.socket_path()?.display(),
                        role.as_str()
                    )
                    .as_bytes(),
                )?;
                Ok(Ok(NodeLock {
                    _file: file,
                    boot: Some(boot),
                }))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(Err(LockHeld::read(&path)?)),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(error).wrap_err_with(|| format!("locking {}", path.display()))
            }
        }
    }

    fn open_lock_file(path: &Path) -> Res<std::fs::File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .wrap_err_with(|| format!("opening {}", path.display()))
    }

    /// Blocks until the holder of the node lock has exited. Run on a blocking thread.
    pub fn wait_for_lock_release(&self) -> Res<()> {
        let path = self.lock_path();
        Self::open_lock_file(&path)?
            .lock_shared()
            .wrap_err_with(|| format!("waiting on {}", path.display()))
    }

    /// Blocks until whoever is booting the node has its socket up (or has given up, which
    /// leaves the node free). Run on a blocking thread.
    pub fn wait_until_serving(&self) -> Res<()> {
        let path = self.boot_lock_path();
        Self::open_lock_file(&path)?
            .lock_shared()
            .wrap_err_with(|| format!("waiting on {}", path.display()))
    }
}

/// What the holder of the node lock is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockRole {
    /// `day node run`: it serves until stopped, and attaches the peers service.
    Daemon,
    /// A command that opened the node for itself: it serves its socket while it runs and for as
    /// long as clients remain connected after, and attaches no peers service.
    Command,
}

impl LockRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::Command => "command",
        }
    }
}

/// The node lock, released when dropped (or when the process exits, however it exits).
#[derive(Debug)]
pub struct NodeLock {
    _file: std::fs::File,
    /// Held until the socket accepts connections.
    boot: Option<std::fs::File>,
}

impl NodeLock {
    /// Announces that the holder's socket accepts connections: processes waiting on it proceed.
    pub fn serving(&mut self) -> Res<()> {
        self.boot
            .take()
            .expect("a node announces it is serving once")
            .unlock()
            .wrap_err("releasing the node's boot lock")
    }
}

/// Another process holds the node lock, and what it recorded there. Each field is absent if the
/// holder has not written it yet.
#[derive(Debug, Clone)]
pub struct LockHeld {
    pub pid: Option<u32>,
    pub socket: Option<PathBuf>,
    pub role: Option<LockRole>,
}

impl LockHeld {
    fn read(path: &Path) -> Res<Self> {
        let text = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("reading {}", path.display()))?;
        let mut lines = text.lines();
        Ok(Self {
            pid: lines.next().and_then(|line| line.parse().ok()),
            socket: lines.next().map(PathBuf::from),
            role: lines.next().and_then(|line| match line {
                "daemon" => Some(LockRole::Daemon),
                "command" => Some(LockRole::Command),
                _ => None,
            }),
        })
    }
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
    /// Boots the node in `dir` on the lock the caller holds, serving its socket. `role` is how
    /// it attaches to the network: a daemon attaches the peers service when `node.json` has a
    /// `peers` section; a command's own node attaches none, since it lives for one command and
    /// must not dial or hold the node's endpoint. The lock announces it is serving once the
    /// socket accepts connections.
    pub async fn boot(dir: &NodeDir, mut lock: NodeLock, role: LockRole) -> Res<Self> {
        let settings = dir.settings()?;
        let listeners = Listeners {
            unix_socket: Some(dir.socket_path()?),
            ..default()
        };
        let peers = match role {
            LockRole::Daemon => settings
                .peers
                .as_ref()
                .map(|peers| peers.peers_config(settings.relay))
                .transpose()?,
            LockRole::Command => None,
        };
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
            listeners,
            peers,
        };
        let (node, stop) = BigNode::boot(config).await?;
        lock.serving()?;
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
