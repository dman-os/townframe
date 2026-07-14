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
use crate::test::{StressBigSyncRpcClient, boot_part_store};
use crate::{
    BigRepo, BigRepoConnection, BigRepoStopToken, Config, DocumentId, PeerId, SharedPartStore,
    StorageConfig,
};
use big_sync::stress_support;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, timeout};

/// A single booted BigRepo node with an Iroh endpoint + big-sync worker.
///
/// Mirrors the fixtures the old `test.rs` set up inline. Held by [`Pair`] /
/// [`Topo`]; callers rarely construct this directly.
pub(crate) struct Node {
    pub repo: Arc<BigRepo>,
    pub store: SharedPartStore,
    worker: big_sync::BigSyncWorkerHandle,
    big_sync_stop: big_sync::StopToken,
    repo_stop: BigRepoStopToken,
    endpoint: iroh::Endpoint,
    _router: iroh::protocol::Router,
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
    /// Human label for diagnostics ("Alice"). Registered in [`log_nickname`].
    pub label: &'static str,
    identity_seed: [u8; 32],
}

#[derive(Clone, Debug)]
struct AcceptHandler {
    repo: Arc<BigRepo>,
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
            .accept_connection_iroh(conn, None)
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
        let (host, initial_stop) = boot_part_store("sqlite::memory:").await?;
        let part_init_obj = big_sync_core::ObjId(big_sync_core::Byte32Id::new(
            [255_u8.wrapping_sub(seed); 32],
        ));
        host.store
            .set_obj_payload(
                part_init_obj,
                serde_json::json!({ "heads": Vec::<String>::new() }),
            )
            .await?;
        host.store
            .remove_obj_from_part(part_init_obj, stress_support::test_part())
            .await?;
        initial_stop.stop().await?;
        Self::boot_with_store(seed, label, storage, Arc::clone(&host.store)).await
    }

    async fn boot_with_store(
        seed: u8,
        label: &'static str,
        storage: StorageConfig,
        store: SharedPartStore,
    ) -> crate::Res<Self> {
        let (repo, repo_stop) = BigRepo::boot(
            Config {
                node_identity_seed: [seed; 32],
                storage,
            },
            Arc::clone(&store),
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
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                AcceptHandler {
                    repo: Arc::clone(&repo),
                    accepted: Arc::clone(&accepted),
                    accepts: Arc::clone(&accepts),
                },
            )
            .spawn();

        let sync_backend = Arc::new(crate::BigRepoSyncBackend::boot(Arc::downgrade(&repo)).await?);
        let mut backends = HashMap::new();
        backends.insert(BigRepo::BACKEND_ID.into(), sync_backend as _);
        let (worker, big_sync_stop) =
            big_sync::spawn_big_sync_worker(Arc::clone(&store), backends)?;

        log_nickname::register(repo.local_peer_id(), label);
        Ok(Self {
            repo,
            store,
            worker,
            big_sync_stop,
            repo_stop,
            endpoint,
            _router: router,
            accepted,
            accepts,
            label,
            identity_seed: [seed; 32],
        })
    }

    async fn restart(self, storage: StorageConfig) -> crate::Res<Self> {
        let store = Arc::clone(&self.store);
        let seed = self.identity_seed;
        let label = self.label;
        self.shutdown().await;
        Self::boot_with_store(seed[0], label, storage, store).await
    }

    pub fn peer_id(&self) -> PeerId {
        self.repo.local_peer_id()
    }

    /// Open an outbound connection to `remote` and wire bidirectional big-sync
    /// part replication between the two nodes.
    pub(crate) async fn connect(&self, remote: &Self) -> crate::Res<BigRepoConnection> {
        let connection = self
            .repo
            .open_connection_iroh(
                self.endpoint.clone(),
                remote.endpoint.addr(),
                remote.peer_id(),
                None,
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect();
        self.worker
            .set_peer(
                remote.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&remote.store),
                }),
                parts,
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect();
        remote
            .worker
            .set_peer(
                self.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&self.store),
                }),
                parts,
            )
            .await?;
        Ok(connection)
    }

    /// Take the next inbound connection accepted by this node's endpoint.
    pub(crate) async fn accepted_connection(&self) -> BigRepoConnection {
        timeout(Duration::from_secs(10), async {
            loop {
                if let Some(connection) = self.accepted.lock().await.take() {
                    return connection;
                }
                self.accepts.notified().await;
            }
        })
        .await
        .expect("timed out waiting for accepted connection")
    }

    async fn shutdown(self) {
        self.endpoint.close().await;
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
        let _ = tokio::task::block_in_place(|| {
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
        let node = self.guard.nodes.remove(self.right_idx);
        let restarted = node.restart(storage).await?;
        self.guard.nodes.insert(self.right_idx, restarted);
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
        let mut pair = Self::boot_disconnected(left_seed, right_seed, left_label, right_label)
            .await?;
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
        self.left_conn
            .as_ref()
            .expect("Pair connection consumed")
    }

    pub fn right_conn(&self) -> &BigRepoConnection {
        self.right_conn
            .as_ref()
            .expect("Pair connection consumed")
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

// A `DocumentId` is just newtype-wrapped bytes; allow it in this module's API.
#[allow(dead_code)]
fn _doc_id_typecheck(_id: DocumentId) {}

/// Placeholder seam for Tier 3 topologies (relay/line/star/mesh).
///
/// Today only [`Pair`] (direct) exists. Tier 3 expands this enum; the
/// [`Node`] + [`ShutdownGuard`] primitives above are reused unchanged.
#[allow(dead_code)]
pub(crate) enum Topo {
    Direct(Pair),
    // Relay, Line, Star, Mesh land with Tier 3.
}
