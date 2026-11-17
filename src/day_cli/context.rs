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
use day_node::HeldNode;
pub use day_node::NodeSettings;
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

    /// The native descriptor socket paired with [`Self::socket_path`].
    #[cfg(unix)]
    pub fn fd_socket_path(&self) -> Res<PathBuf> {
        let socket = self.socket_path()?;
        let stem = socket
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| eyre::eyre!("the node socket has no UTF-8 stem"))?;
        let fd_socket = socket.with_file_name(format!("{stem}.fd.sock"));
        eyre::ensure!(
            fd_socket.as_os_str().len() <= MAX_SOCKET_PATH_BYTES,
            "the descriptor socket path {} is longer than a Unix socket path can be ({MAX_SOCKET_PATH_BYTES} bytes); use a shorter XDG_RUNTIME_DIR",
            fd_socket.display()
        );
        Ok(fd_socket)
    }

    pub fn log_path(&self) -> PathBuf {
        self.run_dir().join("daemon.log")
    }

    pub fn is_initialized(&self) -> bool {
        NodeSettings::exists(&self.root)
    }

    /// Creates the directory and `node.json` with a fresh identity; an existing node is refused.
    pub fn init(&self) -> Res<NodeSettings> {
        NodeSettings::create(&self.root)
    }

    pub fn settings(&self) -> Res<NodeSettings> {
        NodeSettings::load(&self.root)
    }

    /// Rewrites `node.json` with `change` applied (see [`NodeSettings::update`]).
    pub fn update_settings<T>(&self, change: impl FnOnce(&mut NodeSettings) -> Res<T>) -> Res<T> {
        NodeSettings::update(&self.root, change)
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
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            // The holder has the boot lock but has not created its lock file yet.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(error).wrap_err_with(|| format!("reading {}", path.display()));
            }
        };
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
    held: HeldNode,
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
        let listeners = Listeners {
            unix_socket: Some(dir.socket_path()?),
            ..default()
        };
        let held = HeldNode::boot(
            dir.root(),
            &dir.settings()?,
            listeners,
            role == LockRole::Daemon,
        )
        .await?;
        lock.serving()?;
        Ok(Self { held, _lock: lock })
    }

    pub fn node(&self) -> &BigNode {
        &self.held.node
    }

    pub async fn stop(self) -> Res<()> {
        let Self { held, _lock } = self;
        held.stop().await
    }
}
