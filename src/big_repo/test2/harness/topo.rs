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

use super::log_nickname;
use crate::test::StressBigSyncRpcClient;
use crate::{
    BigRepo, BigRepoConnection, BigRepoStopToken, Config, DocumentId, PeerId, SqliteBigRepoStore,
    StorageConfig,
};
use big_sync::{stress_support, HostPartStore};
use sqlx_utils_rs::SqlCtx;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{timeout, Duration};

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
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
    connections: Arc<Mutex<HashMap<PeerId, BigRepoConnection>>>,
    /// Human label for diagnostics ("Alice"). Registered in [`log_nickname`].
    pub label: &'static str,
    pub(crate) identity_seed: [u8; 32],
    /// Parts this node opts out of entirely (hidden parts). Persisted across
    /// restarts so a restarted node keeps its hidden-parts config — a
    /// restart that drops it silently re-advertises GLOBAL to peers that
    /// still hide it, and those routes never establish.
    hidden_parts: HashSet<big_sync_core::PartId>,
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
        hidden_parts: HashSet<big_sync_core::PartId>,
    ) -> crate::Res<Self> {
        let sql = match &storage {
            StorageConfig::Memory => SqlCtx::memory().await?,
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(path)?;
                let db_path = path.join("big_repo.sqlite");
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
                },
                Arc::new(crate::access_policy::KeyhiveMembershipPolicy::new()),
            )
            .await?,
        );
        let part_init_obj = big_sync_core::ObjId(big_sync_core::Byte32Id::new(
            [255_u8.wrapping_sub(seed); 32],
        ));
        store
            .set_obj_payload(
                part_init_obj,
                serde_json::json!({ "heads": Vec::<String>::new() }),
            )
            .await?;
        store
            .remove_obj_from_part(part_init_obj, stress_support::test_part())
            .await?;
        store.ensure_part(crate::GLOBAL_PART_ID).await?;
        Self::boot_with_store(seed, label, storage, store, hidden_parts).await
    }

    async fn boot_with_store(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        store: Arc<SqliteBigRepoStore>,
        hidden_parts: HashSet<big_sync_core::PartId>,
    ) -> crate::Res<Self> {
        let (repo, repo_stop) = BigRepo::boot_with_store(
            Config {
                node_identity_seed: [seed; 32],
                storage,
                scope_key: Arc::from("big-repo-test"),
                hidden_parts: Default::default(),
            },
            (*store).clone(),
        )
        .await?;

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let accepted = Arc::new(Mutex::new(None));
        let accepts = Arc::new(Notify::new());
        let (repo_rpc, repo_rpc_stop) = crate::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
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
        )?;
        log_nickname::register(repo.local_peer_id(), label);
        Ok(Self {
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
        })
    }

    pub(crate) async fn restart(self, storage: StorageConfig) -> crate::Res<Self> {
        let seed = self.identity_seed;
        let label = self.label;
        let hidden_parts = self.hidden_parts.clone();
        let retained_memory_store =
            matches!(&storage, StorageConfig::Memory).then(|| Arc::clone(&self.store));
        self.shutdown().await;

        if let Some(store) = retained_memory_store {
            // Memory restarts intentionally retain the store for tests that
            // isolate Keyhive loss from part-store persistence.
            Self::boot_with_store(seed[0], label, storage, store, hidden_parts).await
        } else {
            // Disk restarts reopen the SQLite file, modeling a new process
            // rather than reusing the old pool/Arc. Keep the hidden-parts
            // config: a restart that drops it silently re-advertises parts
            // peers still hide.
            Self::boot_with_config_and_hidden(seed[0], label, storage, hidden_parts).await
        }
    }

    pub fn peer_id(&self) -> PeerId {
        self.repo.local_peer_id()
    }

    pub(crate) async fn obj_parts_contains(
        &self,
        doc_id: DocumentId,
        part_id: big_sync_core::PartId,
    ) -> crate::Res<bool> {
        Ok(self.store.obj_parts(doc_id).await?.contains(&part_id))
    }

    /// Update the subscribed parts for an already-connected peer.
    /// part replication between the two nodes.
    pub(crate) async fn set_peer_parts(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartId>,
    ) -> crate::Res<()> {
        let parts = subscribed_parts
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect::<HashMap<_, _>>();
        self.worker
            .set_peer(
                remote.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&remote.store) as crate::SharedPartStore,
                    subscriber: self.peer_id(),
                }),
                parts,
                HashMap::new(),
            )
            .await
    }
    /// Open an outbound connection to `remote` and wire bidirectional big-sync
    /// part replication between the two nodes.
    async fn connect_with_keyhive_notifications(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartId>,
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
        self.set_peer_parts(remote, subscribed_parts.clone())
            .await?;
        remote.set_peer_parts(self, subscribed_parts).await?;
        Ok(connection)
    }
    pub(crate) async fn connect(&self, remote: &Self) -> crate::Res<BigRepoConnection> {
        self.connect_with_parts(remote, vec![crate::GLOBAL_PART_ID])
            .await
    }
    pub(crate) async fn connect_with_parts(
        &self,
        remote: &Self,
        subscribed_parts: Vec<big_sync_core::PartId>,
    ) -> crate::Res<BigRepoConnection> {
        let connection = self
            .connect_with_keyhive_notifications(remote, subscribed_parts)
            .await?;
        self.connections
            .lock()
            .await
            .insert(remote.peer_id(), connection.clone());
        Ok(connection)
    }
    pub(crate) async fn connected_peer_ids(&self) -> Vec<PeerId> {
        self.connections.lock().await.keys().copied().collect()
    }
    pub(crate) async fn disconnect_peer(&self, peer_id: PeerId) -> crate::Res<()> {
        self.worker.remove_peer(peer_id).await?;
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
        let _ = self.repo_rpc_stop.stop().await;
        let _ = self.repo_stop.stop().await;
        let _ = self.big_sync_stop.stop().await;
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

    /// Return a reference to a node by index.
    pub(crate) fn node(&self, idx: usize) -> &Node {
        &self.nodes[idx]
    }

    /// Remove and return all nodes, deferring their shutdown to the caller.
    /// Used when a test wants orderly explicit teardown.
    #[allow(dead_code)]
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
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::warn!("no tokio runtime at ShutdownGuard drop; leaking node teardown");
            return;
        }
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
        let right = Node::boot(right_seed, right_label).await?;
        Ok(Self {
            guard: ShutdownGuard::from(vec![left, right]),
            left_idx: 0,
            right_idx: 1,
            left_conn: None,
            right_conn: None,
        })
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
        let right = Node::boot_with_config(
            right_seed,
            right_label,
            StorageConfig::Disk { path: right_path },
        )
        .await?;
        let left_conn = left.connect(&right).await?;
        let right_conn = right.accepted_connection().await;
        left_conn.sync_keyhive_with_peer(None).await?;
        Ok(Self {
            guard: ShutdownGuard::from(vec![left, right]),
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

    /// Restart the left node while preserving its persistent store and identity.
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
        pair.left_conn().sync_keyhive_with_peer(None).await?;
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
    #[allow(dead_code)]
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
#[allow(clippy::large_enum_variant)]
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
        let r = Node::boot(seed_r, label_r).await?;
        let b = Node::boot(seed_b, label_b).await?;

        // A → R
        let a_r_conn = a.connect(&r).await?;
        let r_a_conn = r.accepted_connection().await;
        // R → B
        let r_b_conn = r.connect(&b).await?;
        let b_r_conn = b.accepted_connection().await;

        let nodes = vec![a, r, b];
        let edges = vec![(0, a_r_conn, 1, r_a_conn), (1, r_b_conn, 2, b_r_conn)];
        // Sync from the far end inward so A learns B's contact identity
        // through the relay before topology tests issue grants.
        edges[1].1.sync_keyhive_with_peer(None).await?;
        edges[0].1.sync_keyhive_with_peer(None).await?;
        Ok(Self::Relay(TopoData3::from(nodes, edges)))
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
        let b = Node::boot(seed_b, label_b).await?;
        let c = Node::boot(seed_c, label_c).await?;

        let a_b_conn = a.connect(&b).await?;
        let b_a_conn = b.accepted_connection().await;
        let b_c_conn = b.connect(&c).await?;
        let c_b_conn = c.accepted_connection().await;

        let nodes = vec![a, b, c];
        let edges = vec![(0, a_b_conn, 1, b_a_conn), (1, b_c_conn, 2, c_b_conn)];
        // Sync from the far end inward so A learns C through B.
        edges[1].1.sync_keyhive_with_peer(None).await?;
        edges[0].1.sync_keyhive_with_peer(None).await?;
        Ok(Self::Line(TopoData3::from(nodes, edges)))
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
        let leaf1 = Node::boot(seed_l1, label_l1).await?;
        let leaf2 = Node::boot(seed_l2, label_l2).await?;

        // hub ↔ leaf1
        let h_l1_conn = hub.connect(&leaf1).await?;
        let l1_h_conn = leaf1.accepted_connection().await;
        // hub ↔ leaf2
        let h_l2_conn = hub.connect(&leaf2).await?;
        let l2_h_conn = leaf2.accepted_connection().await;

        let nodes = vec![hub, leaf1, leaf2];
        let edges = vec![(0, h_l1_conn, 1, l1_h_conn), (0, h_l2_conn, 2, l2_h_conn)];
        edges[0].1.sync_keyhive_with_peer(None).await?;
        edges[1].1.sync_keyhive_with_peer(None).await?;
        Ok(Self::Star(TopoData3::from(nodes, edges)))
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
        let b = Node::boot(seed_b, label_b).await?;
        let c = Node::boot(seed_c, label_c).await?;

        let a_b_conn = a.connect(&b).await?;
        let b_a_conn = b.accepted_connection().await;
        let b_c_conn = b.connect(&c).await?;
        let c_b_conn = c.accepted_connection().await;
        let c_a_conn = c.connect(&a).await?;
        let a_c_conn = a.accepted_connection().await;

        let nodes = vec![a, b, c];
        let edges = vec![
            (0, a_b_conn, 1, b_a_conn),
            (1, b_c_conn, 2, c_b_conn),
            (2, c_a_conn, 0, a_c_conn),
        ];
        edges[0].1.sync_keyhive_with_peer(None).await?;
        edges[1].1.sync_keyhive_with_peer(None).await?;
        edges[2].1.sync_keyhive_with_peer(None).await?;
        Ok(Self::Triangle(TopoData3::from(nodes, edges)))
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
