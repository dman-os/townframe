//! Topology builder: boots nodes, wires connections, exchanges contact cards,
//! and owns RAII teardown.
//!
//! Tier 1 needs only [`Pair`] (a direct A↔B link). The [`Topo`] enum + builder
//! is the seam for Tier 3 (relay/line/star/mesh): each variant boots the same
//! [`Node`] fixtures and wires a different connection graph. Adding a topology
//! is additive here, not a rewrite of every test.
//!
//! # RAII teardown
//! [`Pair`] (and future [`Topo`]s) hold a [`ShutdownGuard`]. Drop shuts every
//! node down — even on assertion failure / panic — which is the leak-flake fix
//! called out in `play.big_repo.test2.md`. Tests do not call `.stop()` by hand.

use crate::interlude::*;

use super::log_nickname;

/// The live-lane replay hold the harness asks for, in milliseconds.
///
/// Production parks a caught-up live lane for `ReplayPageTask::HOLD_MS` (15s). A test's
/// settling waits (`wait_for_quiescence`, `wait_for_keyhive_reconciliation`) cannot see a
/// parked lane as in-flight work, so an assertion made after quiescence would fire while
/// the lane is still parked and the cluster is not converged. The harness therefore asks
/// for a short hold of its own — a client-side pacing choice the responder honours —
/// instead of having a test double ignore the request.
const REPLAY_HOLD_MS: u32 = 200;
use crate::{
    BigRepo, BigRepoConnection, BigRepoStopToken, Config, DocumentId, PeerKey, SqliteBigRepoStore,
    StorageConfig, WorkerGroupScope,
};
use big_sync::{HostPartStore, stress_support};
use sqlx_utils_rs::SqlCtx;
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

/// A single booted BigRepo node with an Iroh endpoint + big-sync worker.
///
/// Mirrors the fixtures the old `test.rs` set up inline. Held by [`Pair`] /
/// [`Topo`]; callers rarely construct this directly.
pub(crate) struct Node {
    pub repo: Arc<BigRepo>,
    pub store: Arc<SqliteBigRepoStore>,
    pub(crate) worker: big_sync::BigSyncWorkerHandle,
    big_sync_stop: big_sync::StopToken,
    repo_stop: BigRepoStopToken,
    pub(crate) endpoint: iroh::Endpoint,
    _router: iroh::protocol::Router,
    repo_rpc_stop: crate::rpc::BigRepoRpcStopToken,
    big_sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    /// Kept alive for the whole node: the handle owns the local sender that keeps
    /// `spawn_big_sync_rpc`'s dispatch loop open. Dropping it closes `rpc_rx`, which
    /// tears the loop down and makes every accepted connection close unhandled.
    _big_sync_rpc: big_sync::rpc::BigSyncRpcHandle,
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
    connections: Arc<Mutex<HashMap<PeerKey, BigRepoConnection>>>,
    /// Human label for diagnostics ("Alice"). Registered in [`log_nickname`].
    pub label: &'static str,
    pub(crate) identity_seed: [u8; 32],
    /// Parts this node opts out of entirely (hidden parts). Persisted across
    /// restarts so a restarted node keeps its hidden-parts config — a
    /// restart that drops it silently re-advertises GLOBAL to peers that
    /// still hide it, and those routes never establish.
    hidden_parts: HashSet<big_sync_core::PartKey>,
    /// Frontier-worker group scope. Disabled by default (see boot_with_store);
    /// persisted across restarts like hidden_parts.
    pub(crate) frontier_scope: WorkerGroupScope,
}

#[derive(Clone, Debug)]
struct AcceptHandler {
    repo: Arc<BigRepo>,
    endpoint: iroh::Endpoint,
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
}

impl iroh::protocol::ProtocolHandler for AcceptHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(conn, self.endpoint.clone(), None)
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_boxed(err.into()))?;
        *self.accepted.lock().await = Some(connection);
        self.accepts.notify_waiters();
        Ok(())
    }
}

impl Node {
    /// Boot a node with in-memory BigRepo storage.
    pub(crate) async fn boot(seed: u8, label: &'static str) -> crate::Res<Self> {
        Self::boot_with_config(seed, label, StorageConfig::Memory).await
    }

    /// Boot a node with the keyhive change-notification subscription
    /// unwired: peers only learn keyhive changes through explicit
    /// `sync_keyhive_with_peer` rounds, so a peer's membership view stays
    /// stale by construction (late-keyhive-sync properties).
    pub(crate) async fn boot_without_keyhive_notifs(
        seed: u8,
        label: &'static str,
    ) -> crate::Res<Self> {
        Self::boot_with_scopes_impl(
            seed,
            label,
            StorageConfig::Memory,
            Default::default(),
            WorkerGroupScope::disabled(),
            false,
        )
        .await
    }

    /// Boot a node with a selectable persistent BigRepo storage configuration.
    pub(crate) async fn boot_with_config(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
    ) -> crate::Res<Self> {
        Self::boot_with_config_and_hidden(seed, label, storage, Default::default()).await
    }

    pub(crate) async fn boot_with_config_and_hidden(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        hidden_parts: HashSet<big_sync_core::PartKey>,
    ) -> crate::Res<Self> {
        Self::boot_with_scopes(
            seed,
            label,
            storage,
            hidden_parts,
            WorkerGroupScope::disabled(),
        )
        .await
    }

    /// Boot a node with an explicit frontier-worker group scope. Tests that
    /// exercise the AutomergeFrontierWorker must opt in here — the default is
    /// disabled so relays/read-only peers never materialize forwarded docs.
    pub(crate) async fn boot_with_scopes(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        hidden_parts: HashSet<big_sync_core::PartKey>,
        frontier_scope: WorkerGroupScope,
    ) -> crate::Res<Self> {
        Self::boot_with_scopes_impl(seed, label, storage, hidden_parts, frontier_scope, true).await
    }

    async fn boot_with_scopes_impl(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        hidden_parts: HashSet<big_sync_core::PartKey>,
        frontier_scope: WorkerGroupScope,
        keyhive_change_notifs: bool,
    ) -> crate::Res<Self> {
        let sql = match &storage {
            StorageConfig::Memory => SqlCtx::memory().await?,
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(path)?;
                let db_path = path.join("big_repo.sqlite");
                ::tracing::info!(seed, label, path = %path.display(), "booted node with disk storage");
                SqlCtx::url(&format!("sqlite://{}", db_path.display())).await?
            }
        };
        let store = Arc::new(
            SqliteBigRepoStore::new_with_config(
                sql,
                "big-repo-test",
                big_sync_core::BuckId::MAX_LEVEL,
                big_sync::HostPartStoreConfig {
                    hidden_parts: hidden_parts.clone(),
                    ..Default::default()
                },
            )
            .await?,
        );
        // The stress test part must exist before membership is written to it. It
        // must not be seeded with a synthetic object payload: every object in the
        // document scope has to be a document, which the frontier worker asserts
        // in test builds.
        store.ensure_part(stress_support::test_part()).await?;
        store.ensure_part(crate::global_part_id()).await?;
        Self::boot_with_store(
            seed,
            label,
            storage,
            store,
            hidden_parts,
            frontier_scope,
            keyhive_change_notifs,
        )
        .await
    }

    async fn boot_with_store(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        store: Arc<SqliteBigRepoStore>,
        hidden_parts: HashSet<big_sync_core::PartKey>,
        frontier_scope: WorkerGroupScope,
        keyhive_change_notifs: bool,
    ) -> crate::Res<Self> {
        let (repo, repo_stop) = BigRepo::boot_with_store(
            Config {
                node_identity_seed: [seed; 32],
                storage,
                scope_key: Arc::from("big-repo-test"),
                hidden_parts: Default::default(),
                // Workers are opt-in per test: the default scope disables the
                // AutomergeFrontierWorker entirely so relays/read-only peers
                // never acquire + materialize docs they only forward. Tests
                // exercising a worker must enable it explicitly via
                // boot_with_scopes. Watching GLOBAL here made every node
                // materialize every doc marker it synced, which is not
                // normal-role behavior.
                automerge_frontier_group_scope: frontier_scope.clone(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
                keyhive_change_notifs,
            },
            (*store).clone(),
        )
        .await?;

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            // The iroh identity is the node identity, as in production: the big-sync RPC
            // attributes a request to `conn.remote_id()`, and that key has to be the same
            // `PeerKey` the part grants name. A random endpoint key would make every
            // peer-facing read answer `UnkownParts` for a peer that is in fact a member.
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let accepted = Arc::new(Mutex::new(None));
        let accepts = Arc::new(Notify::new());
        let (repo_rpc, repo_rpc_stop) = crate::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
        let (big_sync_rpc, big_sync_rpc_stop) =
            big_sync::rpc::spawn_big_sync_rpc(HashMap::from([(
                Arc::from("big-repo-test"),
                Arc::clone(&store) as crate::SharedPartStore,
            )]))
            .await?;
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                AcceptHandler {
                    repo: Arc::clone(&repo),
                    endpoint: endpoint.clone(),
                    accepted: Arc::clone(&accepted),
                    accepts: Arc::clone(&accepts),
                },
            )
            .accept(crate::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
            .accept(
                big_sync::rpc::BIG_SYNC_RPC_ALPN,
                big_sync_rpc.protocol_handler(),
            )
            .spawn();

        let sync_backend = Arc::new(crate::BigRepoSyncBackend::boot(Arc::downgrade(&repo)).await?);
        let mut backends = HashMap::new();
        backends.insert(BigRepo::BACKEND_ID.into(), sync_backend as _);
        let shared_store: crate::SharedPartStore = Arc::clone(&store) as _;
        let (worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
            shared_store,
            backends,
            label,
            Some(Duration::from_secs(5)),
            // Bucket-diff, explicitly: this harness runs the band the embedder ships, so big_repo's
            // tests exercise it; `bucket_band_reconciles_after_offline_reopen` covers the
            // offline-reopen path that used to be the reason to stay on cursor replay.
            Some(big_sync::SyncMode::Bucket),
            Arc::from("big-repo-test"),
        )?;
        log_nickname::register(repo.local_peer_id(), label);
        worker.set_replay_hold_ms(REPLAY_HOLD_MS).await?;
        Ok(Self {
            big_sync_rpc_stop,
            _big_sync_rpc: big_sync_rpc,
            repo,
            store,
            worker,
            big_sync_stop,
            repo_stop,
            endpoint,
            _router: router,
            repo_rpc_stop,
            accepted,
            accepts,
            connections: Arc::new(Mutex::new(HashMap::new())),
            label,
            identity_seed: [seed; 32],
            hidden_parts,
            frontier_scope,
        })
    }

    pub(crate) async fn restart(self, storage: StorageConfig) -> crate::Res<Self> {
        let seed = self.identity_seed;
        let label = self.label;
        let hidden_parts = self.hidden_parts.clone();
        let frontier_scope = self.frontier_scope.clone();
        let retained_memory_store =
            matches!(&storage, StorageConfig::Memory).then(|| Arc::clone(&self.store));
        self.shutdown().await;

        let restarted = if let Some(store) = retained_memory_store {
            // Memory restarts intentionally retain the store for tests that
            // isolate Keyhive loss from part-store persistence.
            Self::boot_with_store(
                seed[0],
                label,
                storage,
                store,
                hidden_parts,
                frontier_scope,
                true,
            )
            .await?
        } else {
            // Disk restarts reopen the SQLite file, modeling a new process
            // rather than reusing the old pool/Arc. Keep the hidden-parts
            // config: a restart that drops it silently re-advertises parts
            // peers still hide.
            Self::boot_with_scopes(seed[0], label, storage, hidden_parts, frontier_scope).await?
        };
        restarted.repo.wait_for_keyhive_reconciliation().await?;
        Ok(restarted)
    }

    pub fn peer_id(&self) -> PeerKey {
        self.repo.local_peer_id()
    }

    pub(crate) async fn obj_parts_contains(
        &self,
        doc_id: DocumentId,
        part_id: big_sync_core::PartKey,
    ) -> crate::Res<bool> {
        Ok(self.store.obj_parts(doc_id).await?.contains(&part_id))
    }

    /// Update the subscribed parts for an already-connected peer.
    /// part replication between the two nodes.
    pub(crate) async fn set_peer_parts(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartKey>,
    ) -> crate::Res<()> {
        let parts = subscribed_parts
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect::<HashMap<_, _>>();
        self.worker
            .set_peer(
                remote.peer_id(),
                Arc::new(big_sync::rpc::IrohBigSyncRpcClient::new(
                    self.endpoint.clone(),
                    remote.endpoint.addr(),
                )),
                parts,
                HashMap::new(),
            )
            .await
    }

    /// Authorize this node to pull `parts` from `remote`.
    ///
    /// Part access is explicit: nothing derives it from a document-level grant, and
    /// `/seds` in particular is a mirror-grade grant that a document grant must never
    /// imply. A topology whose peers are `/seds` readers therefore has to say so — and
    /// must say so *before* the routes are registered, because a page denied at
    /// registration backs off rather than retrying once the grant lands.
    pub(crate) async fn allow_part_pull(
        &self,
        remote: &Self,
        parts: &[big_sync_core::PartKey],
    ) -> crate::Res<()> {
        for part in parts {
            remote
                .store
                .add_part_member(
                    part.clone(),
                    self.peer_id(),
                    keyhive_core::access::Access::Read,
                )
                .await?;
        }
        Ok(())
    }
    /// Open an outbound connection to `remote` and wire bidirectional big-sync
    /// part replication between the two nodes.
    async fn connect_with_keyhive_notifications(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartKey>,
        part_access: bool,
    ) -> crate::Res<BigRepoConnection> {
        let connection = self
            .repo
            .open_connection_iroh(
                self.endpoint.clone(),
                remote.endpoint.addr(),
                remote.peer_id(),
                None,
            )
            .await?;
        // Part access is explicit on the serving side (see `allow_part_pull`), so the
        // fixture grants the parts each side subscribes for, in both directions, before
        // the routes are registered. A page denied at registration backs off for the whole
        // unauthorized window instead of retrying once the grant lands, so the order here
        // is not interchangeable.
        //
        // `part_access: false` is for tests that assert what a *document* grant does and
        // does not authorize: with the mirror part already granted, a permitted answer
        // legitimately includes `/seds` and the assertion stops being about the document
        // grant.
        if part_access {
            self.allow_part_pull(remote, &subscribed_parts).await?;
            remote.allow_part_pull(self, &subscribed_parts).await?;
        }
        self.set_peer_parts(remote, subscribed_parts.clone())
            .await?;
        remote.set_peer_parts(self, subscribed_parts).await?;
        Ok(connection)
    }
    pub(crate) async fn connect(&self, remote: &Self) -> crate::Res<BigRepoConnection> {
        self.connect_with_parts(remote, vec![crate::global_part_id()])
            .await
    }
    pub(crate) async fn connect_with_parts(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartKey>,
    ) -> crate::Res<BigRepoConnection> {
        self.connect_with_parts_inner(remote, subscribed_parts, true)
            .await
    }

    /// [`Self::connect_with_parts`] without the part grants.
    ///
    /// Part read is explicit on the serving side, so the normal path grants it. Tests
    /// that assert what a *document* grant does and does not authorize need it left
    /// ungranted.
    pub(crate) async fn connect_with_parts_ungranted(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartKey>,
    ) -> crate::Res<BigRepoConnection> {
        self.connect_with_parts_inner(remote, subscribed_parts, false)
            .await
    }

    async fn connect_with_parts_inner(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartKey>,
        part_access: bool,
    ) -> crate::Res<BigRepoConnection> {
        let connection = self
            .connect_with_keyhive_notifications(remote, subscribed_parts, part_access)
            .await?;
        self.connections
            .lock()
            .await
            .insert(remote.peer_id(), connection.clone());
        Ok(connection)
    }
    pub(crate) async fn connected_peer_ids(&self) -> Vec<PeerKey> {
        self.connections.lock().await.keys().cloned().collect()
    }
    pub(crate) async fn disconnect_peer(&self, peer_id: PeerKey) -> crate::Res<()> {
        self.worker.remove_peer(peer_id.clone()).await?;
        if let Some(connection) = self.connections.lock().await.remove(&peer_id) {
            connection.stop().await?;
        }
        Ok(())
    }
    /// Take the next inbound connection accepted by this node's endpoint.
    pub(crate) async fn accepted_connection(&self) -> BigRepoConnection {
        let connection = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(connection) = self.accepted.lock().await.take() {
                    return connection;
                }
                self.accepts.notified().await;
            }
        })
        .await
        .expect("timed out waiting for accepted connection");
        self.connections
            .lock()
            .await
            .insert(connection.peer_id(), connection.clone());
        connection
    }

    pub(crate) async fn shutdown(self) {
        for connection in self
            .connections
            .lock()
            .await
            .drain()
            .map(|(_, connection)| connection)
        {
            if !connection.is_closed() {
                connection
                    .stop()
                    .await
                    .expect("failed stopping node connection during shutdown");
            }
        }
        self.endpoint.close().await;
        self.repo_rpc_stop
            .stop()
            .await
            .inspect_err(|err| error!("shutdown err: {err}"))
            .ok();
        self.repo_stop
            .stop()
            .await
            .expect("repo_stop failed during shutdown");
        self.big_sync_stop
            .stop()
            .await
            .expect("big_sync_stop failed during shutdown");
        self.big_sync_rpc_stop
            .stop()
            .await
            .inspect_err(|err| error!("shutdown err: {err}"))
            .ok();
    }
}

/// RAII guard that shuts down a set of nodes on drop — even on panic.
///
/// Shutdown is async; in the multi-threaded tokio test runtime we perform it
/// with [`tokio::task::block_in_place`] + [`block_on`](tokio::runtime::Handle::block_on).
/// Nodes already removed via [`Self::take`] are not shut down again.
pub(crate) struct ShutdownGuard {
    nodes: Vec<Node>,
}

impl ShutdownGuard {
    pub(crate) fn from(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }

    /// Boot N disconnected nodes managed under this RAII shutdown guard.
    #[expect(unused)]
    pub(crate) async fn boot(specs: &[(u8, &'static str)]) -> crate::Res<Self> {
        let mut nodes = Vec::with_capacity(specs.len());
        for &(seed, label) in specs {
            nodes.push(Node::boot(seed, label).await?);
        }
        Ok(Self { nodes })
    }

    /// Boot N nodes with per-node control over the keyhive change-notification
    /// subscription. Nodes booted with `false` only learn keyhive changes
    /// through explicit `sync_keyhive_with_peer` rounds, so their membership
    /// view is stale by construction until a test explicitly syncs.
    pub(crate) async fn boot_mixed(specs: &[(u8, &'static str, bool)]) -> crate::Res<Self> {
        let mut nodes = Vec::with_capacity(specs.len());
        for &(seed, label, keyhive_change_notifs) in specs {
            nodes.push(if keyhive_change_notifs {
                Node::boot(seed, label).await?
            } else {
                Node::boot_without_keyhive_notifs(seed, label).await?
            });
        }
        Ok(Self { nodes })
    }

    /// Return a reference to a node by index.
    pub(crate) fn node(&self, idx: usize) -> &Node {
        &self.nodes[idx]
    }

    /// Remove and return all nodes, deferring their shutdown to the caller.
    /// Used when a test wants orderly explicit teardown.
    #[expect(dead_code)]
    pub(crate) fn take(&mut self) -> Vec<Node> {
        std::mem::take(&mut self.nodes)
    }

    async fn shutdown_all(mut nodes: Vec<Node>) {
        while let Some(node) = nodes.pop() {
            node.shutdown().await;
        }
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        // We are inside a `#[tokio::test(flavor = "multi_thread")]` runtime, on
        // a worker thread. `block_in_place` moves us off the scheduler so the
        // nested `block_on` can drive the async shutdown without deadlocking.
        // If no runtime is present (shouldn't happen in tests), we leak
        // teardown rather than panic-during-unwind.
        let _handle = tokio::runtime::Handle::try_current()
            .expect("ShutdownGuard dropped outside active tokio runtime");
        let nodes = std::mem::take(&mut self.nodes);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(Self::shutdown_all(nodes))
        });
    }
}

/// A direct two-node topology: `left` ↔ `right`, both connections established
/// and the initial contact-card (keyhive) exchange completed.
///
/// This is the Tier-1 fixture. `left`/`right` are the owner/reader roles by
/// convention in the ladder rungs, but the pair is symmetric.
pub(crate) struct Pair {
    guard: ShutdownGuard,
    pub left_idx: usize,
    pub right_idx: usize,
    pub left_conn: Option<BigRepoConnection>,
    pub right_conn: Option<BigRepoConnection>,
}

impl Pair {
    /// Boot two nodes without connecting them. The RAII guard is active
    /// immediately, so setup failures still tear down both nodes.
    pub(crate) async fn boot_disconnected(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
    ) -> crate::Res<Self> {
        let left = Node::boot(left_seed, left_label).await?;
        let mut guard = ShutdownGuard::from(vec![left]);
        let right = Node::boot(right_seed, right_label).await?;
        guard.nodes.push(right);
        Ok(Self {
            guard,
            left_idx: 0,
            right_idx: 1,
            left_conn: None,
            right_conn: None,
        })
    }

    /// Boot a connected pair whose nodes have the keyhive change-notification
    /// subscription unwired: membership views only move on explicit
    /// `sync_keyhive_with_peer` rounds, so late-keyhive-sync tests are
    /// deterministic instead of racing the notification fan-out.
    pub(crate) async fn boot_without_keyhive_notifs(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
    ) -> crate::Res<Self> {
        let left = Node::boot_without_keyhive_notifs(left_seed, left_label).await?;
        let mut guard = ShutdownGuard::from(vec![left]);
        let right = Node::boot_without_keyhive_notifs(right_seed, right_label).await?;
        guard.nodes.push(right);
        let mut pair = Self {
            guard,
            left_idx: 0,
            right_idx: 1,
            left_conn: None,
            right_conn: None,
        };
        // These peers read `/seds`, the store-wide enumeration part. Part access is
        // explicit and a page denied at registration backs off for the whole
        // unauthorized window, so the grant has to precede the routes.
        pair.left()
            .allow_part_pull(pair.right(), &[crate::global_part_id()])
            .await?;
        pair.right()
            .allow_part_pull(pair.left(), &[crate::global_part_id()])
            .await?;
        pair.connect().await?;
        // The contact-card exchange rides the first keyhive protocol round;
        // with the notification subscription unwired nothing starts one
        // automatically, so run one round per direction up front.
        pair.left_conn().sync_keyhive_with_peer().await?;
        pair.right_conn().sync_keyhive_with_peer().await?;
        Ok(pair)
    }

    /// Connect an already-booted pair leaving part read ungranted on both sides.
    pub(crate) async fn connect_ungranted(&mut self) -> crate::Res<()> {
        assert!(self.left_conn.is_none());
        assert!(self.right_conn.is_none());
        let left_conn = self
            .left()
            .connect_with_parts_ungranted(self.right(), vec![crate::global_part_id()])
            .await?;
        let right_conn = self.right().accepted_connection().await;
        self.left_conn = Some(left_conn);
        self.right_conn = Some(right_conn);
        Ok(())
    }

    /// Boot a connected pair with persistent per-node BigRepo storage.
    pub(crate) async fn boot_persistent(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
        left_path: std::path::PathBuf,
        right_path: std::path::PathBuf,
    ) -> crate::Res<Self> {
        let left = Node::boot_with_config(
            left_seed,
            left_label,
            StorageConfig::Disk { path: left_path },
        )
        .await?;
        let mut guard = ShutdownGuard::from(vec![left]);
        let right = Node::boot_with_config(
            right_seed,
            right_label,
            StorageConfig::Disk { path: right_path },
        )
        .await?;
        guard.nodes.push(right);
        let left_conn = guard.node(0).connect(guard.node(1)).await?;
        let right_conn = guard.node(1).accepted_connection().await;
        left_conn.sync_keyhive_with_peer().await?;
        Ok(Self {
            guard,
            left_idx: 0,
            right_idx: 1,
            left_conn: Some(left_conn),
            right_conn: Some(right_conn),
        })
    }

    /// Connect an already-booted pair without performing a Keyhive sync.
    pub(crate) async fn connect(&mut self) -> crate::Res<()> {
        self.connect_with_keyhive_notifications().await
    }

    async fn connect_with_keyhive_notifications(&mut self) -> crate::Res<()> {
        assert!(self.left_conn.is_none());
        assert!(self.right_conn.is_none());
        let left_conn = self.left().connect(self.right()).await?;
        let right_conn = self.right().accepted_connection().await;
        self.left_conn = Some(left_conn);
        self.right_conn = Some(right_conn);
        Ok(())
    }

    /// Remove the big-sync peer routes as well as the transport connections.
    /// This makes offline ladder rungs genuinely offline instead of merely
    /// suppressing the Iroh connection.
    pub(crate) async fn disconnect(&self) -> crate::Res<()> {
        self.left()
            .worker
            .remove_peer(self.right().peer_id())
            .await?;
        self.right()
            .worker
            .remove_peer(self.left().peer_id())
            .await?;
        Ok(())
    }

    pub(crate) async fn restart_right(&mut self, storage: StorageConfig) -> crate::Res<()> {
        self.left_conn.take();
        self.right_conn.take();
        let node = self.guard.nodes.remove(self.right_idx);
        let restarted = node.restart(storage).await?;
        self.guard.nodes.insert(self.right_idx, restarted);
        Ok(())
    }

    /// Shut down and remove the right node, handing back its store handle so
    /// the caller can inspect/manipulate the persistent SQLite state directly
    /// while the runtime is down. A fresh node over the same disk path
    /// ([`Node::boot_with_config`]) models the subsequent process restart.
    pub(crate) async fn shutdown_take_right(&mut self) -> std::sync::Arc<SqliteBigRepoStore> {
        self.left_conn.take();
        self.right_conn.take();
        let node = self.guard.nodes.remove(self.right_idx);
        let store = std::sync::Arc::clone(&node.store);
        node.shutdown().await;
        store
    }

    /// Re-attach a freshly booted node at the right slot. Callers boot the
    /// node themselves with [`Node::boot_with_config`], mirroring a process
    /// restart over the same persistent store.
    pub(crate) fn put_right(&mut self, node: Node) {
        self.guard.nodes.insert(self.right_idx, node);
    }

    pub(crate) async fn restart_left(&mut self, storage: StorageConfig) -> crate::Res<()> {
        self.left_conn.take();
        self.right_conn.take();
        let node = self.guard.nodes.remove(self.left_idx);
        let restarted = node.restart(storage).await?;
        self.guard.nodes.insert(self.left_idx, restarted);
        Ok(())
    }

    /// Boot two nodes, connect them, and run one keyhive sync so each side
    /// knows the other's agent.
    pub(crate) async fn boot(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
    ) -> crate::Res<Self> {
        let mut pair =
            Self::boot_disconnected(left_seed, right_seed, left_label, right_label).await?;
        pair.connect().await?;
        pair.left_conn().sync_keyhive_with_peer().await?;
        Ok(pair)
    }

    /// Boot a connected pair whose parts are **not** granted to each other.
    ///
    /// [`Self::boot`] grants the parts each side subscribes (part read is explicit on the
    /// serving side; see [`Node::connect_with_parts`]). Tests that assert what a document
    /// grant does and does not authorize need the ungranted state instead, because a
    /// granted mirror part legitimately appears in a permitted answer.
    pub(crate) async fn boot_ungranted(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
    ) -> crate::Res<Self> {
        let mut pair =
            Self::boot_disconnected(left_seed, right_seed, left_label, right_label).await?;
        pair.connect_ungranted().await?;
        pair.left_conn().sync_keyhive_with_peer().await?;
        Ok(pair)
    }

    /// Boot a connected pair with admission-driven Automerge frontier workers.
    pub(crate) async fn boot_with_frontier_workers(
        left_seed: u8,
        right_seed: u8,
        left_label: &'static str,
        right_label: &'static str,
    ) -> crate::Res<Self> {
        let left = Node::boot_with_scopes(
            left_seed,
            left_label,
            StorageConfig::Memory,
            Default::default(),
            WorkerGroupScope::All,
        )
        .await?;
        let mut guard = ShutdownGuard::from(vec![left]);
        let right = Node::boot_with_scopes(
            right_seed,
            right_label,
            StorageConfig::Memory,
            Default::default(),
            WorkerGroupScope::All,
        )
        .await?;
        guard.nodes.push(right);
        let mut pair = Self {
            guard,
            left_idx: 0,
            right_idx: 1,
            left_conn: None,
            right_conn: None,
        };
        // The frontier peers read `/seds` (store-wide enumeration): grant it before
        // the routes are registered, or the reader's first page is denied and backs off.
        pair.left()
            .allow_part_pull(pair.right(), &[crate::global_part_id()])
            .await?;
        pair.right()
            .allow_part_pull(pair.left(), &[crate::global_part_id()])
            .await?;
        pair.connect().await?;
        pair.left_conn().sync_keyhive_with_peer().await?;
        Ok(pair)
    }

    pub fn left(&self) -> &Node {
        &self.guard.nodes[self.left_idx]
    }

    pub fn right(&self) -> &Node {
        &self.guard.nodes[self.right_idx]
    }

    pub fn left_conn(&self) -> &BigRepoConnection {
        self.left_conn.as_ref().expect("Pair connection consumed")
    }

    pub fn right_conn(&self) -> &BigRepoConnection {
        self.right_conn.as_ref().expect("Pair connection consumed")
    }

    /// Borrow both nodes mutably for orderly teardown (unused; kept for
    /// future explicit-teardown rungs). Split via index to satisfy the borrow
    // checker.
    #[expect(dead_code)]
    pub(crate) fn nodes_mut(&mut self) -> (&mut Node, &mut Node) {
        let (l, r) = (self.left_idx, self.right_idx);
        let (left_part, right_part) = {
            let nodes = &mut self.guard.nodes;
            if l <= r {
                let (a, b) = nodes.split_at_mut(r);
                (&mut a[l], &mut b[0])
            } else {
                let (a, b) = nodes.split_at_mut(l);
                (&mut b[0], &mut a[r])
            }
        };
        (left_part, right_part)
    }
}

// ─── Multi-node topology support (Tier 3+) ──────────────────────────────────

/// A generic multi-node topology built from the same [`Node`] and
/// [`ShutdownGuard`] primitives used by [`Pair`]. Each variant holds
/// a guard for RAII teardown, the node vector, labelled connections for
/// keyhive and document sync operations, and indexing metadata.
pub(crate) enum Topo {
    /// Relay A↔R↔B where R has Relay-only capability (stores encrypted parts
    Relay(TopoData3),
    /// Line A↔B↔C.
    Line(TopoData3),
    /// Star hub↔leaf1, hub↔leaf2.
    Star(TopoData3),
    /// Triangle A↔B↔C↔A (full 3-node mesh).
    Triangle(TopoData3),
}

/// Shared internals for 3-node topologies (relay, line, triangle).
pub(crate) struct TopoData3 {
    pub(crate) guard: ShutdownGuard,
    /// (initiator_idx, initiator_conn, acceptor_idx, acceptor_conn) for
    /// each edge in the topology.
    pub(crate) edges: Vec<(usize, BigRepoConnection, usize, BigRepoConnection)>,
}

impl TopoData3 {
    #[expect(dead_code)]
    fn from(
        nodes: Vec<Node>,
        edges: Vec<(usize, BigRepoConnection, usize, BigRepoConnection)>,
    ) -> Self {
        let guard = ShutdownGuard::from(nodes);
        Self { guard, edges }
    }

    /// Return the connection from `from_idx` to `to_idx` (the initiator's
    /// connection). Panics if the edge does not exist in that direction.
    pub(crate) fn conn(&self, from_idx: usize, to_idx: usize) -> &BigRepoConnection {
        self.edges
            .iter()
            .find(|(i, _, j, _)| *i == from_idx && *j == to_idx)
            .map(|(_, conn, _, _)| conn)
            .or_else(|| {
                self.edges
                    .iter()
                    .find(|(i, _, j, _)| *i == to_idx && *j == from_idx)
                    .map(|(_, _, _, conn)| conn)
            })
            .expect("edge not found")
    }
}

impl Topo {
    /// Build a relay topology: A ↔ R ↔ B. R is a no-grant relay.
    pub(crate) async fn boot_relay(
        seed_a: u8,
        seed_r: u8,
        seed_b: u8,
        label_a: &'static str,
        label_r: &'static str,
        label_b: &'static str,
    ) -> crate::Res<Self> {
        let a = Node::boot(seed_a, label_a).await?;
        let mut guard = ShutdownGuard::from(vec![a]);
        let r = Node::boot(seed_r, label_r).await?;
        guard.nodes.push(r);
        let b = Node::boot(seed_b, label_b).await?;
        guard.nodes.push(b);

        // A → R
        let a_r_conn = guard.node(0).connect(guard.node(1)).await?;
        let r_a_conn = guard.node(1).accepted_connection().await;
        // R → B
        let r_b_conn = guard.node(1).connect(guard.node(2)).await?;
        let b_r_conn = guard.node(2).accepted_connection().await;

        let edges = vec![(0, a_r_conn, 1, r_a_conn), (1, r_b_conn, 2, b_r_conn)];
        // Sync from the far end inward so A learns B's contact identity
        // through the relay before topology tests issue grants.
        edges[1].1.sync_keyhive_with_peer().await?;
        edges[0].1.sync_keyhive_with_peer().await?;
        Ok(Self::Relay(TopoData3 { guard, edges }))
    }

    /// Build a line topology: A ↔ B ↔ C.
    pub(crate) async fn boot_line(
        seed_a: u8,
        seed_b: u8,
        seed_c: u8,
        label_a: &'static str,
        label_b: &'static str,
        label_c: &'static str,
    ) -> crate::Res<Self> {
        let a = Node::boot(seed_a, label_a).await?;
        let mut guard = ShutdownGuard::from(vec![a]);
        let b = Node::boot(seed_b, label_b).await?;
        guard.nodes.push(b);
        let c = Node::boot(seed_c, label_c).await?;
        guard.nodes.push(c);

        let a_b_conn = guard.node(0).connect(guard.node(1)).await?;
        let b_a_conn = guard.node(1).accepted_connection().await;
        let b_c_conn = guard.node(1).connect(guard.node(2)).await?;
        let c_b_conn = guard.node(2).accepted_connection().await;

        let edges = vec![(0, a_b_conn, 1, b_a_conn), (1, b_c_conn, 2, c_b_conn)];
        // Sync from the far end inward so A learns C through B.
        edges[1].1.sync_keyhive_with_peer().await?;
        edges[0].1.sync_keyhive_with_peer().await?;
        Ok(Self::Line(TopoData3 { guard, edges }))
    }

    /// Build a star topology: hub ↔ leaf1, hub ↔ leaf2.
    /// Returns nodes as [hub, leaf1, leaf2].
    pub(crate) async fn boot_star(
        seed_h: u8,
        seed_l1: u8,
        seed_l2: u8,
        label_h: &'static str,
        label_l1: &'static str,
        label_l2: &'static str,
    ) -> crate::Res<Self> {
        let hub = Node::boot(seed_h, label_h).await?;
        let mut guard = ShutdownGuard::from(vec![hub]);
        let leaf1 = Node::boot(seed_l1, label_l1).await?;
        guard.nodes.push(leaf1);
        let leaf2 = Node::boot(seed_l2, label_l2).await?;
        guard.nodes.push(leaf2);

        // hub ↔ leaf1
        let h_l1_conn = guard.node(0).connect(guard.node(1)).await?;
        let l1_h_conn = guard.node(1).accepted_connection().await;
        // hub ↔ leaf2
        let h_l2_conn = guard.node(0).connect(guard.node(2)).await?;
        let l2_h_conn = guard.node(2).accepted_connection().await;

        let edges = vec![(0, h_l1_conn, 1, l1_h_conn), (0, h_l2_conn, 2, l2_h_conn)];
        edges[0].1.sync_keyhive_with_peer().await?;
        edges[1].1.sync_keyhive_with_peer().await?;
        Ok(Self::Star(TopoData3 { guard, edges }))
    }

    /// Build a partial-mesh (triangle) topology: A↔B, B↔C, C↔A (full
    /// triangle, the densest 3-node mesh).
    pub(crate) async fn boot_triangle(
        seed_a: u8,
        seed_b: u8,
        seed_c: u8,
        label_a: &'static str,
        label_b: &'static str,
        label_c: &'static str,
    ) -> crate::Res<Self> {
        let a = Node::boot(seed_a, label_a).await?;
        let mut guard = ShutdownGuard::from(vec![a]);
        let b = Node::boot(seed_b, label_b).await?;
        guard.nodes.push(b);
        let c = Node::boot(seed_c, label_c).await?;
        guard.nodes.push(c);

        let a_b_conn = guard.node(0).connect(guard.node(1)).await?;
        let b_a_conn = guard.node(1).accepted_connection().await;
        let b_c_conn = guard.node(1).connect(guard.node(2)).await?;
        let c_b_conn = guard.node(2).accepted_connection().await;
        let c_a_conn = guard.node(2).connect(guard.node(0)).await?;
        let a_c_conn = guard.node(0).accepted_connection().await;

        let edges = vec![
            (0, a_b_conn, 1, b_a_conn),
            (1, b_c_conn, 2, c_b_conn),
            (2, c_a_conn, 0, a_c_conn),
        ];
        edges[0].1.sync_keyhive_with_peer().await?;
        edges[1].1.sync_keyhive_with_peer().await?;
        edges[2].1.sync_keyhive_with_peer().await?;
        Ok(Self::Triangle(TopoData3 { guard, edges }))
    }

    /// Return a reference to a node by index.
    pub(crate) fn topo_node(&self, idx: usize) -> &Node {
        match self {
            Topo::Relay(d) | Topo::Line(d) | Topo::Star(d) | Topo::Triangle(d) => d.guard.node(idx),
        }
    }

    /// Return the connection for a directional edge.
    pub(crate) fn topo_conn(&self, from: usize, to: usize) -> &BigRepoConnection {
        match self {
            Topo::Relay(d) | Topo::Line(d) | Topo::Star(d) | Topo::Triangle(d) => d.conn(from, to),
        }
    }
}
