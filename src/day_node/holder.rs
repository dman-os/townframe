//! The node directory's runtime half: its lock and socket, and finding the node that holds them
//! or holding it oneself (ADR 033 sections 1 and 5; clay-on-the-node section 1).
//!
//! `day` and the app hosts both come here: [`NodeDir::find_or_hold`] either reaches the process
//! that holds the node through its socket, or boots the node in this process as its holder.
//!
//! Layout of a node directory (ADR 033 section 1: the node owns its data, the CLI only knows
//! where it is):
//!
//! ```text
//! <node>/node.json        node id, operator policy and peers, created by `day node init` (0600)
//! <node>/node.lock        flock held for as long as a process has the node booted; holds its pid
//! <node>/run/serve.log   the background server's stderr
//! <node>/node.sqlite, repo/, blobs/   the node's own storage, opened by `big_node::Bindings`
//! ```
//!
//! The server's Unix socket is not in the node directory but in the per-user runtime directory,
//! named by the node id (see [`NodeDir::socket_path`]), so a node directory can be anywhere,
//! however long its path, and two nodes on a machine never share a socket. Every other secret a
//! node keeps is in the platform keyring under entries that carry the node id too.

use crate::HeldNode;
use crate::NodeSettings;
use big_node::BigNode;
use big_node::Listeners;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use utils_rs::prelude::Arc;
use utils_rs::prelude::ERROR_TOKIO;
use utils_rs::prelude::Path;
use utils_rs::prelude::PathBuf;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::default;
use utils_rs::prelude::eyre;

/// `sockaddr_un.sun_path` holds 108 bytes on Linux (104 on the BSDs and macOS) including the
/// terminator; refuse a socket path that cannot fit instead of failing in `bind`.
const MAX_SOCKET_PATH_BYTES: usize = 100;

/// The directory the server sockets live in: the one place the socket location is decided, so
/// making it configurable is a change to this function and its caller [`NodeDir::new`].
///
/// - **Normally** `$XDG_RUNTIME_DIR/day`: the per-user runtime directory the login session
///   creates as `0700` and removes at logout, which is where a local server's socket belongs
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
fn socket_dir_in(runtime_dir: Option<&Path>, uid: u32) -> PathBuf {
    match runtime_dir {
        Some(runtime) => runtime.join("day"),
        None => PathBuf::from(format!("/tmp/day-{uid}")),
    }
}

/// A node directory and the paths inside it.
#[derive(Debug, Clone)]
pub struct NodeDir {
    root: PathBuf,
    /// Where this node's socket lives; [`default_socket_dir`] unless a test places it.
    socket_dir: PathBuf,
}

/// The state of the server's socket file, learned by connecting to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SocketState {
    /// Something accepted the connection.
    Live,
    /// The file is there and nothing listens on it: a server died without removing it.
    Stale,
    Absent,
}

impl NodeDir {
    /// The node directory a host uses when it is not told one: the platform's per-user data
    /// directory for `org.townframe.day`. `day` (after its `--node` flag and `DAY_NODE`) and the
    /// desktop app both land here, so they share one node.
    pub fn default_root() -> Res<PathBuf> {
        let dirs = directories::ProjectDirs::from("org", "townframe", "day")
            .ok_or_else(|| eyre::eyre!("failed to get xdg directories"))?;
        Ok(dirs.data_dir().to_path_buf())
    }

    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            socket_dir: default_socket_dir(),
        }
    }

    /// A node directory whose socket lives in `socket_dir`, so a test does not touch the real
    /// runtime directory.
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

    /// The server's socket: `<socket dir>/<node id>.sock`. The node must be initialized, as the
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
        self.run_dir().join("serve.log")
    }

    pub fn is_initialized(&self) -> bool {
        NodeSettings::exists(&self.root)
    }

    /// Creates the directory and `node.json` with a fresh identity; an existing node is refused.
    pub async fn init(&self) -> Res<NodeSettings> {
        NodeSettings::create(&self.root).await
    }

    /// Moves a version 1 node's identity seed into the keyring on the node lock the caller holds
    /// (see [`NodeSettings::migrate`]).
    pub async fn migrate(&self, lock: &NodeLock) -> Res<()> {
        NodeSettings::migrate(&self.root, lock).await
    }

    pub fn settings(&self) -> Res<NodeSettings> {
        NodeSettings::load(&self.root)
    }

    /// Rewrites `node.json` with `change` applied (see [`NodeSettings::update`]). A version 1
    /// node is migrated first, under the node lock taken for that alone, as a version 2 file
    /// written without the seed in the keyring would lose the node's identity.
    pub async fn update_settings<T>(
        &self,
        change: impl FnOnce(&mut NodeSettings) -> Res<T>,
    ) -> Res<T> {
        let on_disk: serde_json::Value = serde_json::from_slice(
            &std::fs::read(self.root.join("node.json"))
                .wrap_err_with(|| format!("reading {}", self.root.join("node.json").display()))?,
        )?;
        if on_disk.get("identity_seed").is_some() {
            match self.try_lock(LockRole::Command)? {
                Ok(lock) => self.migrate(&lock).await?,
                Err(held) => eyre::bail!(
                    "{} is version 1 and {held} has the node open; its seed moves to the keyring once the node is booted again",
                    self.root.join("node.json").display()
                ),
            }
        }
        NodeSettings::update(&self.root, change)
    }

    /// Creates `run/` (the server's log) and the socket directory, each owner-only.
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

    /// Learns whether a server answers on the socket by connecting to it, as the node's own
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
        self.lock_as(role, Some(self.socket_path()?))
    }

    /// [`Self::try_lock`] for a holder that serves no socket (a host with no second process, whose
    /// node data directory may be too deep for a socket path). It records no socket in the lock
    /// file and has no boot to wait out, so `node.boot` is released at once: a process that
    /// finds this node held sees no socket to wait for, and waits for the lock instead.
    pub fn try_lock_unserved(&self, role: LockRole) -> Res<Result<NodeLock, LockHeld>> {
        let mut lock = self.lock_as(role, None)?;
        if let Ok(lock) = &mut lock {
            lock.serving()?;
        }
        Ok(lock)
    }

    fn lock_as(&self, role: LockRole, socket: Option<PathBuf>) -> Res<Result<NodeLock, LockHeld>> {
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
                        // An empty line for a holder with no socket, so the role stays third.
                        socket.as_deref().unwrap_or(Path::new("")).display(),
                        role.as_str()
                    )
                    .as_bytes(),
                )?;
                Ok(Ok(NodeLock {
                    _file: file,
                    boot: Some(boot),
                    socket,
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
    /// `day node start`: it serves until stopped, and attaches the peers service.
    Serve,
    /// A command that opened the node for itself: it serves its socket while it runs and for as
    /// long as clients remain connected after, and attaches no peers service.
    Command,
}

impl LockRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Serve => "serve",
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
    /// The socket the holder serves; `None` for a lock taken by [`NodeDir::try_lock_unserved`].
    socket: Option<PathBuf>,
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
            socket: lines
                .next()
                .filter(|line| !line.is_empty())
                .map(PathBuf::from),
            role: lines.next().and_then(|line| match line {
                "serve" => Some(LockRole::Serve),
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
    /// Boots the node in `dir` on the lock the caller holds, serving its socket, after moving a
    /// version 1 node's seed into the keyring (the one place a node is opened). `role` is how
    /// it attaches to the network: a server attaches the peers service when `node.json` has a
    /// `peers` section; a command's own node attaches none, since it lives for one command and
    /// must not dial or hold the node's endpoint. `schemas` is the datum vocabulary the node
    /// admits. A lock from [`NodeDir::try_lock`] has the node serve its socket and announces it
    /// is serving once the socket accepts connections; one from [`NodeDir::try_lock_unserved`]
    /// boots it with no listener.
    pub async fn boot(
        dir: &NodeDir,
        mut lock: NodeLock,
        role: LockRole,
        schemas: Arc<big_cabinet::SchemaRepo>,
    ) -> Res<Self> {
        dir.migrate(&lock).await?;
        let listeners = Listeners {
            unix_socket: lock.socket.clone(),
            ..default()
        };
        let held = HeldNode::boot(
            dir.root(),
            &dir.settings()?,
            listeners,
            role == LockRole::Serve,
            schemas,
        )
        .await?;
        if lock.socket.is_some() {
            lock.serving()?;
        }
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

/// How long a client waits for a node another process is booting, or is shutting down, before it
/// gives up.
pub const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// How [`NodeDir::find_or_hold`] reached the node.
pub enum Reached {
    /// Nothing held the node, so this process booted it and is its holder. `found` is what the
    /// socket looked like before the boot: absent, or a stale file a dead holder left.
    Held {
        booted: Box<Booted>,
        found: SocketState,
    },
    /// Another process holds the node.
    Attached(Attached),
}

/// One HTTP/1 connection to another process's node, over its socket.
pub struct Attached {
    /// Opened by [`NodeDir::find_or_hold`] and never idled out. The holder counts its
    /// connections, and a holder that has finished its own command accepts no new one, so every
    /// call must go over this one. A call made while another is in flight would open a second
    /// connection, which a draining holder refuses (an error, not a hang).
    pub http: reqwest::Client,
    /// What the holder booted as, if it had recorded it by the time it was found.
    pub role: Option<LockRole>,
    /// The holder's native descriptor socket (see [`NodeDir::fd_socket_path`]).
    pub fd_socket: PathBuf,
}

impl NodeDir {
    /// Reaches the node in this directory: through the socket of the process that holds it, or,
    /// if none does, by booting it here as `role` over `schemas`. Finding the node held by a
    /// process that is still booting waits for its socket, and finding it held by one that is
    /// shutting down waits for the lock, both bounded by `deadline`.
    pub async fn find_or_hold(
        &self,
        role: LockRole,
        schemas: Arc<big_cabinet::SchemaRepo>,
        deadline: std::time::Duration,
    ) -> Res<Reached> {
        let found = self.probe_socket().await?;
        tokio::time::timeout(deadline, self.acquire(role, schemas, found))
            .await
            .map_err(|_| {
                eyre::eyre!(
                    "the node at {} stayed held by another process for {deadline:?}",
                    self.root().display()
                )
            })?
    }

    /// Opens the one connection to the holder `held`, once its boot lock says its socket is up.
    /// The inner `Err` is a failed connect (the holder shutting down, or only probing the lock);
    /// any other failure is the outer error.
    ///
    /// The holder must answer a request on it, which proves it accepted: a connect alone can sit
    /// in the backlog of a listener about to close. A holder that has finished its own command
    /// closes its socket and exits once the connections it accepted have ended, so every call
    /// goes over this one connection, and a refusal or a reset means the holder is on its way out.
    async fn attach_held(&self, held: &LockHeld) -> Res<Result<Attached, reqwest::Error>> {
        let waiting = self.clone();
        tokio::task::spawn_blocking(move || waiting.wait_until_serving())
            .await
            .expect(ERROR_TOKIO)?;
        let socket = match &held.socket {
            Some(socket) => socket.clone(),
            None => self.socket_path()?,
        };
        let http = reqwest::Client::builder()
            .unix_socket(socket.as_path())
            .pool_max_idle_per_host(1)
            .pool_idle_timeout(None)
            .build()?;
        match http
            .get(format!("http://localhost/xrpc/{}", big_node::GET_NODE_INFO))
            .send()
            .await
        {
            Ok(_) => Ok(Ok(Attached {
                http,
                role: held.role,
                fd_socket: self.fd_socket_path()?,
            })),
            Err(error) if error.is_connect() => Ok(Err(error)),
            Err(error) => {
                Err(error).wrap_err_with(|| format!("connecting to {}", socket.display()))
            }
        }
    }

    /// Attaches to the process holding the node without ever booting it here, for a caller that
    /// must not become the holder (a capture hook, which starts a server instead). `None` is no
    /// server to attach to: nobody holds the node, or its socket is absent or refuses
    /// connections, the holder's idle exit having closed its listener. Those are the I/O kinds
    /// `NotFound` and `ConnectionRefused`, read off the error's `source` chain; any other
    /// failure is an error. A holder that is still shutting down is waited out first, so a
    /// `None` leaves the node free to start a server on.
    pub async fn attach_server(&self) -> Res<Option<Attached>> {
        let held = match self.try_lock(LockRole::Command)? {
            Ok(_free) => return Ok(None),
            Err(held) => held,
        };
        let error = match self.attach_held(&held).await? {
            Ok(attached) => return Ok(Some(attached)),
            Err(error) => error,
        };
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let absent_or_refused = std::iter::from_fn(|| {
            let current = source?;
            source = current.source();
            Some(current)
        })
        .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            )
        });
        if !absent_or_refused {
            return Err(error)
                .wrap_err_with(|| format!("connecting to the node at {}", self.root().display()));
        }
        let waiting = self.clone();
        tokio::task::spawn_blocking(move || waiting.wait_for_lock_release())
            .await
            .expect(ERROR_TOKIO)?;
        Ok(None)
    }

    async fn acquire(
        &self,
        role: LockRole,
        schemas: Arc<big_cabinet::SchemaRepo>,
        found: SocketState,
    ) -> Res<Reached> {
        loop {
            let held = match self.try_lock(role)? {
                Ok(lock) => {
                    let booted = Booted::boot(self, lock, role, schemas).await?;
                    return Ok(Reached::Held {
                        booted: Box::new(booted),
                        found,
                    });
                }
                Err(held) => held,
            };
            if let Ok(attached) = self.attach_held(&held).await? {
                return Ok(Reached::Attached(attached));
            }
            // No socket: the holder is shutting down, or it was only a process probing the lock.
            // Either way the lock is the signal, so wait for it to be free and try again.
            let waiting = self.clone();
            tokio::task::spawn_blocking(move || waiting.wait_for_lock_release())
                .await
                .expect(ERROR_TOKIO)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrets_rs::SecretStore;

    /// A holder with no socket takes the lock without a socket path, records no
    /// socket and its role, and leaves no boot lock for another process to wait on.
    #[tokio::test]
    async fn an_unserved_lock_records_its_role_but_no_socket_and_holds_no_boot_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = NodeDir::new(tmp.path().to_path_buf());
        // Initialized only so the probing `try_lock` below can name its own socket.
        dir.init().await.unwrap();
        let _lock = dir.try_lock_unserved(LockRole::Serve).unwrap().unwrap();
        let held = dir.try_lock(LockRole::Command).unwrap().unwrap_err();
        assert_eq!(held.pid, Some(std::process::id()));
        assert_eq!(held.socket, None);
        assert_eq!(held.role, Some(LockRole::Serve));
        // Returns at once instead of blocking: nothing is booting.
        dir.wait_until_serving().unwrap();
    }

    /// `attach_server` never boots: with nobody holding the node, and with a holder whose
    /// listener is gone (a socket file that refuses connections, as after an idle exit), it
    /// answers `None` once the holder lets go, leaving the node free to start a server on. The
    /// holder drops its lock concurrently with the attach, and both orders end in `None`: the
    /// lock already free, or waited out after the refusal.
    #[tokio::test]
    async fn attach_server_finds_no_server_when_nobody_holds_the_node_or_its_listener_is_gone() {
        let tmp = short_tempdir();
        let dir = NodeDir::new(tmp.path().to_path_buf());
        dir.init().await.unwrap();
        assert!(dir.attach_server().await.unwrap().is_none());

        let mut lock = dir.try_lock(LockRole::Serve).unwrap().unwrap();
        lock.serving().unwrap();
        drop(std::os::unix::net::UnixListener::bind(dir.socket_path().unwrap()).unwrap());
        let release = std::thread::spawn(move || drop(lock));
        assert!(dir.attach_server().await.unwrap().is_none());
        release.join().unwrap();
    }

    #[test]
    fn the_default_socket_directory_is_the_runtime_directory_or_a_private_one_in_tmp() {
        assert_eq!(
            socket_dir_in(Some(Path::new("/run/user/1000")), 1000),
            Path::new("/run/user/1000/day")
        );
        assert_eq!(socket_dir_in(None, 1000), Path::new("/tmp/day-1000"));
    }

    #[tokio::test]
    async fn init_round_trips_identity_seed_through_keyring_without_putting_it_in_node_json() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = NodeDir::new(tmp.path().join("node"));
        let settings = dir.init().await.unwrap();
        let secrets = SecretStore::boot().await.unwrap();

        let seed = settings.seed(&secrets).await.unwrap();
        assert_eq!(big_node::node_id(&seed), settings.node_id());
        let node_json = std::fs::read_to_string(dir.root().join("node.json")).unwrap();
        assert!(!node_json.contains("identity_seed"));
        assert!(node_json.contains("node_id"));

        assert_eq!(big_node::delete_secrets(&secrets, &seed).await.unwrap(), 1);
        secrets.stop().await.unwrap();
    }

    /// Under `/tmp` rather than `$TMPDIR`: a socket named by a 64 character node id has little
    /// room in `sun_path`, and `$TMPDIR` can be long.
    fn short_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("d")
            .tempdir_in("/tmp")
            .unwrap()
    }

    fn write_v1(dir: &NodeDir, seed: &[u8; 32]) -> String {
        std::fs::create_dir_all(dir.root()).unwrap();
        let seed_hex = data_encoding::HEXLOWER.encode(seed);
        std::fs::write(
            dir.root().join("node.json"),
            format!(
                r#"{{"version": 1, "identity_seed": "{seed_hex}", "relay": false, "dev": {{}}}}"#
            ),
        )
        .unwrap();
        seed_hex
    }

    #[tokio::test]
    async fn a_version_one_node_migrates_its_seed_into_the_keyring() {
        let tmp = short_tempdir();
        let dir = NodeDir::with_socket_dir(tmp.path().join("node"), tmp.path().join("run"));
        let seed = [9_u8; 32];
        let seed_hex = write_v1(&dir, &seed);
        // Before the migration the file reads as the node it becomes.
        assert_eq!(dir.settings().unwrap().node_id(), big_node::node_id(&seed));

        let lock = dir.try_lock(LockRole::Command).unwrap().unwrap();
        dir.migrate(&lock).await.unwrap();

        let node_json = std::fs::read_to_string(dir.root().join("node.json")).unwrap();
        assert!(!node_json.contains(&seed_hex));
        assert!(!node_json.contains("identity_seed"));
        let mode = std::fs::metadata(dir.root().join("node.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let settings = dir.settings().unwrap();
        let secrets = SecretStore::boot().await.unwrap();
        assert_eq!(settings.seed(&secrets).await.unwrap(), seed);

        // Opening again is a no-op, and the node still opens.
        dir.migrate(&lock).await.unwrap();
        assert_eq!(settings.seed(&secrets).await.unwrap(), seed);
        assert_eq!(big_node::delete_secrets(&secrets, &seed).await.unwrap(), 1);
        secrets.stop().await.unwrap();
    }

    /// A crash after the keyring store and before the rename leaves the version 1 file with the
    /// entry already present; migrating again completes it.
    #[tokio::test]
    async fn a_migration_interrupted_after_the_store_completes_on_the_next_open() {
        let tmp = short_tempdir();
        let dir = NodeDir::with_socket_dir(tmp.path().join("node"), tmp.path().join("run"));
        let seed = [11_u8; 32];
        let seed_hex = write_v1(&dir, &seed);
        let secrets = SecretStore::boot().await.unwrap();
        big_node::IdentityKeyring(&secrets)
            .store(&seed)
            .await
            .unwrap();

        let lock = dir.try_lock(LockRole::Command).unwrap().unwrap();
        dir.migrate(&lock).await.unwrap();

        assert!(
            !std::fs::read_to_string(dir.root().join("node.json"))
                .unwrap()
                .contains(&seed_hex)
        );
        assert_eq!(dir.settings().unwrap().seed(&secrets).await.unwrap(), seed);
        big_node::delete_secrets(&secrets, &seed).await.unwrap();
        secrets.stop().await.unwrap();
    }

    /// Updating the settings of a version 1 node moves its seed first, so the rewritten file
    /// does not strand the identity.
    #[tokio::test]
    async fn updating_a_version_one_node_keeps_its_seed() {
        let tmp = short_tempdir();
        let dir = NodeDir::with_socket_dir(tmp.path().join("node"), tmp.path().join("run"));
        let seed = [13_u8; 32];
        write_v1(&dir, &seed);

        dir.update_settings(|settings| {
            settings.peers = Some(default());
            Ok(())
        })
        .await
        .unwrap();

        let secrets = SecretStore::boot().await.unwrap();
        let settings = dir.settings().unwrap();
        assert!(settings.peers.is_some());
        assert_eq!(settings.seed(&secrets).await.unwrap(), seed);
        big_node::delete_secrets(&secrets, &seed).await.unwrap();
        secrets.stop().await.unwrap();
    }
}
