//! The node directory and everything the CLI keeps in it, plus the booted in-process node.
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

/// What `day node init` records. The node id is the public half of the identity seed, which remains
/// in the OS keyring (ADR 033 section 6: one node key).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSettings {
    version: u32,
    /// The public half of the identity seed, in lowercase hex.
    node_id: String,
    /// The node's network attachment. Absent: the server attaches none, so the node neither
    /// dials nor can be dialed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peers: Option<PeerSettings>,
    /// Run the node as a relay (ADR 038 section 3): accept any peer and mirror every drawer the
    /// node is a member of, which a user grants with `day drawers add-member`. A relay needs the
    /// network attachment, so this requires a `peers` section.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub relay: bool,
    /// Development hooks, all off unless set here by hand (a command can't turn one on, since
    /// the node reads this file at boot).
    #[serde(default, skip_serializing_if = "DevSettings::is_default")]
    pub dev: DevSettings,
    /// What a nodule's `#net` permbinds may reach beyond the public internet.
    #[serde(default, skip_serializing_if = "NetSettings::is_default")]
    pub net: NetSettings,
    /// The blob store's tuning.
    #[serde(default, skip_serializing_if = "BlobSettings::is_default")]
    pub blobs: BlobSettings,
}

/// The `blobs` section of `node.json`. Keys are snake_case, as the file's other top-level keys are.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobSettings {
    /// How long an upload nothing references keeps its bytes (ADR 042 §6). Absent: 24 hours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_hold_grace_secs: Option<u64>,
}

impl BlobSettings {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }

    fn upload_hold_grace(&self) -> Option<std::time::Duration> {
        self.upload_hold_grace_secs
            .map(std::time::Duration::from_secs)
    }
}

/// The `net` section of `node.json`. Keys are snake_case, as the file's other top-level keys are.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetSettings {
    /// Lets a nodule that holds a `grant/net` for `host.daybook.internal:<port>` reach that
    /// port on this machine's own loopback (a local server, a dev tool). Off by default, and
    /// read at boot, so no nodule or command can turn it on. A guest's own `127.0.0.1` and
    /// `localhost` never reach the machine either way.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_host_loopback: bool,
}

impl NetSettings {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// The `dev` section of `node.json`. Keys are snake_case, as the file's other top-level keys are.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevSettings {
    /// Lets `day carts dev-takeover` run an installed dev build as another installed cartridge,
    /// with its actor, data and grants (ADR 045 section 5). Never a side effect of anything
    /// else: with it off the node refuses to set a takeover and does not honor one found in
    /// its config document.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_slot_takeover: bool,
}

impl DevSettings {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// The `peers` section of `node.json`: its presence is what makes the server attach the peers
/// service (the Iroh endpoint, keyed by the node's identity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PeerSettings {
    /// Peers dialed as soon as the server starts and redialed with backoff when the connection
    /// drops, as endpoint tickets (the `address` a node's `getNodeInfo` reports). Which nodes
    /// belong here is the composition's policy, not the node's: a user node lists its own other
    /// devices and a relay, a relay lists none.
    #[serde(default)]
    pub bootstrap: Vec<String>,
    /// Bind the loopback interface only, with relays off (see
    /// `big_node::peers::PeersConfig::loopback_only`).
    #[serde(default)]
    pub loopback_only: bool,
    /// The UDP port the node listens on (see `big_node::peers::PeersConfig::listen_port`). Unset,
    /// a free port is taken at each start and the node's ticket changes with every restart; a
    /// node that others dial by its ticket sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_port: Option<u16>,
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

    /// The peers config the server boots with: dial the bootstrap list under the default
    /// mirroring policy, or under the relay policy when the node is a relay.
    fn peers_config(&self, relay: bool) -> Res<big_node::peers::PeersConfig> {
        let dial = self
            .bootstrap
            .iter()
            .map(|ticket| big_node::peers::parse_ticket(ticket))
            .collect::<Res<Vec<_>>>()?;
        Ok(big_node::peers::PeersConfig {
            loopback_only: self.loopback_only,
            listen_port: self.listen_port,
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
    const VERSION: u32 = 2;

    pub async fn seed(&self, secrets: &SecretStore) -> Res<[u8; 32]> {
        big_node::IdentityKeyring(secrets).load(&self.node_id).await
    }

    /// The node's id: the public half of the identity seed, as the repo derives its own peer id.
    pub fn node_id(&self) -> String {
        self.node_id.clone()
    }
}

/// The state of the server's socket file, learned by connecting to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SocketState {
    /// Something accepted the connection.
    Live,
    /// The file is there and nothing listens on it: a server died without removing it.
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
        self.settings_path().is_file()
    }

    /// Creates the directory and `node.json` with a fresh identity. The settings file is created
    /// exclusively, so an existing node is refused here and never overwritten, whoever else is
    /// initializing the same directory at the same time.
    pub async fn init(&self) -> Res<NodeSettings> {
        use rand::RngCore;
        std::fs::create_dir_all(&self.root)
            .wrap_err_with(|| format!("creating {}", self.root.display()))?;
        let mut seed = [0_u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let settings = NodeSettings {
            version: NodeSettings::VERSION,
            node_id: big_node::node_id(&seed),
            peers: None,
            relay: false,
            dev: DevSettings::default(),
            net: NetSettings::default(),
            blobs: BlobSettings::default(),
        };
        let bytes = serde_json::to_vec_pretty(&settings)?;
        let secrets = SecretStore::boot().await?;
        big_node::IdentityKeyring(&secrets).store(&seed).await?;
        let result = (|| -> Res<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(self.settings_path())
                .wrap_err_with(|| format!("creating {}", self.settings_path().display()))?;
            std::io::Write::write_all(&mut file, &bytes)?;
            Ok(())
        })();
        if let Err(error) = result {
            // The node never came to be, so its keyring entries go with the failed init.
            if let Err(cleanup) = big_node::delete_secrets(&secrets, &seed).await {
                return Err(error.wrap_err(cleanup));
            }
            secrets.stop().await?;
            return Err(error);
        }
        secrets.stop().await?;
        Ok(settings)
    }

    /// Writes `settings` beside `node.json` and renames it over, so a reader sees the old file
    /// or the new, never half, and a crash leaves the old one. The file is `0600` and synced
    /// before the rename.
    fn write_settings(&self, settings: &NodeSettings) -> Res<()> {
        let staging = self.root.join("node.json.new");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&staging)
            .wrap_err_with(|| format!("creating {}", staging.display()))?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(settings)?)?;
        file.sync_all()?;
        std::fs::rename(&staging, self.settings_path())
            .wrap_err_with(|| format!("replacing {}", self.settings_path().display()))?;
        Ok(())
    }

    /// Rewrites a version 1 `node.json` value (identity seed in the file) into the version 2
    /// shape (its node id) and returns the seed it held; any other value is left alone.
    fn upgrade_v1(value: &mut serde_json::Value) -> Res<Option<[u8; 32]>> {
        if value.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Ok(None);
        }
        let object = value
            .as_object_mut()
            .ok_or_else(|| eyre::eyre!("node.json is not a JSON object"))?;
        let Some(hex) = object.remove("identity_seed") else {
            return Ok(None);
        };
        let hex = hex
            .as_str()
            .ok_or_else(|| eyre::eyre!("node.json identity_seed is not a string"))?;
        let seed = <[u8; 32]>::try_from(
            data_encoding::HEXLOWER
                .decode(hex.as_bytes())
                .wrap_err("node.json holds a hex identity seed")?
                .as_slice(),
        )
        .map_err(|_| eyre::eyre!("node.json holds a 32 byte identity seed"))?;
        object.insert("version".into(), NodeSettings::VERSION.into());
        object.insert("node_id".into(), big_node::node_id(&seed).into());
        Ok(Some(seed))
    }

    /// Moves a version 1 node's identity seed out of `node.json` into the keyring, then rewrites
    /// `node.json` as version 2 (the node id only). Run by whoever opens the node, on the node
    /// lock they hold (the proof is the `lock` argument), so no two processes migrate at once.
    /// The seed is stored and read back equal before the rewrite, and the plaintext is gone only
    /// once the rename lands: a crash in between leaves the version 1 file, which migrates again
    /// (storing the equal seed twice changes nothing). Any other node is left alone.
    pub async fn migrate(&self, _lock: &NodeLock) -> Res<()> {
        let path = self.settings_path();
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "reading {}; is there a node here? (`day node init`)",
                path.display()
            )
        })?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)
            .wrap_err_with(|| format!("parsing {}", path.display()))?;
        let Some(seed) = Self::upgrade_v1(&mut value)? else {
            return Ok(());
        };
        let settings: NodeSettings = serde_json::from_value(value)
            .wrap_err_with(|| format!("parsing {} as a version 2 node", path.display()))?;
        let secrets = SecretStore::boot().await?;
        let migrated = async {
            let identity = big_node::IdentityKeyring(&secrets);
            identity.store(&seed).await?;
            eyre::ensure!(
                identity.load(&settings.node_id).await? == seed,
                "the keyring did not return the identity seed just stored for node {}",
                settings.node_id
            );
            self.write_settings(&settings)
        }
        .await;
        secrets.stop().await?;
        migrated.wrap_err_with(|| format!("migrating {}", path.display()))
    }

    pub fn settings(&self) -> Res<NodeSettings> {
        let path = self.settings_path();
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "reading {}; is there a node here? (`day node init`)",
                path.display()
            )
        })?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)
            .wrap_err_with(|| format!("parsing {}", path.display()))?;
        // A version 1 node reads as the version 2 it becomes: the seed in the file names the node
        // id. Only booting the node moves the seed into the keyring ([`Self::migrate`]).
        Self::upgrade_v1(&mut value)?;
        let settings: NodeSettings = serde_json::from_value(value)
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
            // Fail on a bad ticket whichever command reads the settings, not when a server
            // happens to boot it.
            peers
                .peers_config(settings.relay)
                .wrap_err_with(|| format!("{}: bootstrap list", path.display()))?;
        }
        Ok(settings)
    }

    /// Rewrites `node.json` with `change` applied. The new file is written beside it and renamed
    /// over it, so a reader (a server booting) sees the old settings or the new, never half.
    /// Whether the node is serving does not matter: settings are read once, at boot. A version 1
    /// node is migrated first, under the node lock taken for that alone, as a version 2 file
    /// written without the seed in the keyring would lose the node's identity.
    pub async fn update_settings<T>(
        &self,
        change: impl FnOnce(&mut NodeSettings) -> Res<T>,
    ) -> Res<T> {
        let on_disk: serde_json::Value = serde_json::from_slice(
            &std::fs::read(self.settings_path())
                .wrap_err_with(|| format!("reading {}", self.settings_path().display()))?,
        )?;
        if on_disk.get("identity_seed").is_some() {
            match self.try_lock(LockRole::Command)? {
                Ok(lock) => self.migrate(&lock).await?,
                Err(held) => eyre::bail!(
                    "{} is version 1 and {held} has the node open; its seed moves to the keyring once the node is booted again",
                    self.settings_path().display()
                ),
            }
        }
        let mut settings = self.settings()?;
        let out = change(&mut settings)?;
        self.write_settings(&settings)?;
        Ok(out)
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
    pub node: BigNode,
    stop: big_node::StopToken,
    secrets: Arc<SecretStore>,
    // Held to the end: the node's storage is this process's alone while it is booted.
    _lock: NodeLock,
}

impl Booted {
    /// Boots the node in `dir` on the lock the caller holds, serving its socket, after moving a
    /// version 1 node's seed into the keyring (the one place a node is opened). `role` is how
    /// it attaches to the network: a server attaches the peers service when `node.json` has a
    /// `peers` section; a command's own node attaches none, since it lives for one command and
    /// must not dial or hold the node's endpoint. The lock announces it is serving once the
    /// socket accepts connections.
    pub async fn boot(dir: &NodeDir, mut lock: NodeLock, role: LockRole) -> Res<Self> {
        dir.migrate(&lock).await?;
        let settings = dir.settings()?;
        let listeners = Listeners {
            unix_socket: Some(dir.socket_path()?),
            ..default()
        };
        let peers = match role {
            LockRole::Serve => settings
                .peers
                .as_ref()
                .map(|peers| peers.peers_config(settings.relay))
                .transpose()?,
            LockRole::Command => None,
        };
        let runtime = Arc::new(SqliteRuntime::new(default()));
        let bindings = big_node::Bindings::in_directory(&runtime, dir.root(), None).await?;
        let secrets = Arc::new(SecretStore::boot().await?);
        let seed = settings.seed(&secrets).await?;
        let config = big_node::Config {
            repo: big_repo::Config {
                node_identity_seed: seed,
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
            allow_slot_takeover: settings.dev.allow_slot_takeover,
            allow_host_loopback: settings.net.allow_host_loopback,
            upload_hold_grace: settings.blobs.upload_hold_grace(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_round_trips_identity_seed_through_keyring_without_putting_it_in_node_json() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = NodeDir::new(tmp.path().join("node"));
        let settings = dir.init().await.unwrap();
        let secrets = SecretStore::boot().await.unwrap();

        let seed = settings.seed(&secrets).await.unwrap();
        assert_eq!(big_node::node_id(&seed), settings.node_id());
        let node_json = std::fs::read_to_string(dir.settings_path()).unwrap();
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
            dir.settings_path(),
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

        let node_json = std::fs::read_to_string(dir.settings_path()).unwrap();
        assert!(!node_json.contains(&seed_hex));
        assert!(!node_json.contains("identity_seed"));
        let mode = std::fs::metadata(dir.settings_path())
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
            !std::fs::read_to_string(dir.settings_path())
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
