//! The peers service (ADR 033 section 6): the node's attachment to other nodes, and the successor
//! of the old `IrohSyncRepo`.
//!
//! It owns the Iroh endpoint and the ALPN router (subduction, big_sync RPC, repo sync, iroh-blobs),
//! the two big_sync workers (documents and blobs), and one actor that is the only writer of the
//! per-peer state below. A node without a [`PeersConfig`] has none of this.
//!
//! **One node key.** The endpoint's secret key is `big_repo::Config::node_identity_seed`, the
//! 32 bytes that are already the Keyhive individual's and the Subduction signer's Ed25519 key. An
//! Iroh endpoint id is that key's public half, so "who is this connection" (the transport) and
//! "what may it have" (Keyhive) name the same `PeerKey` with no mapping table between them.
//!
//! **Per-peer state machine** (the actor's `slots`):
//!
//! ```text
//!             dial (policy target, connect())                incoming, policy.accept
//!                  |                                                |
//!                  v                                                v
//!   (absent) -> Connecting { closed: None }          (absent) -> Connecting { closed: Some }
//!                  | connection opened                                |
//!                  v                                                  |
//!             Connecting { closed: Some } <--------------------------+
//!                  | registered: rpc peer map, blob address, doc + blob set_peer
//!                  v
//!              Connected { conn, rpc, addr } --- policy changed / drawer descriptor arrived
//!                  |                                -> set_peer again with the new parts
//!                  | the connection's own end signal (stale ones are ignored by end flag)
//!                  v
//!   (absent), teardown: unregister, remove_peer; a policy target is redialled after a backoff
//! ```
//!
//! A failed dial doubles that target's delay up to a cap and a registered connection resets it.
//! Rescheduled sync tasks inside the workers are not this machine's business: big_sync races
//! keyhive membership by retrying on purpose.

mod access;
mod policy;

pub use policy::MirrorPolicy;
pub use policy::PeerPolicy;
pub use policy::PolicyContext;

use big_blobs::sync::BlobSyncBackend;
use big_repo::AccessSubject;
use big_repo::BigRepo;
use big_repo::BigRepoConnection;
use big_repo::BigRepoSyncBackend;
use big_repo::ConnFinishSignal;
use big_repo::KeyhiveAccessDelta;
use big_sync::BackendId;
use big_sync::rpc::WireBigSyncRpcClient;
use big_sync_core::ObjKey;
use big_sync_core::PartKey;
use big_sync_core::PeerKey;
use big_sync_core::SyncStatEvent;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre;
use utils_rs::prelude::eyre::WrapErr;

/// The scope names of the two big_sync workers and the RPC stores that serve them. Every
/// `big_node` uses these, so two nodes' workers and servers always agree on what a scope is.
const DOCS_SCOPE: &str = "big-node-docs";
const BLOBS_SCOPE: &str = "big-node-blobs";
const BLOBS_BACKEND: &str = "blob-inventory";

#[derive(Clone)]
pub struct PeersConfig {
    /// Bind the endpoint to the loopback interface only, with relays off. For tests and for
    /// nodes that are only ever reached by an address handed to them.
    pub loopback_only: bool,
    pub policy: PolicyChoice,
    /// The first redial delay of a dial target; each consecutive failure doubles it.
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// How often the policy's dial targets are re-read when nothing else prompts it. It only
    /// drives dialing: a peer's parts are recomputed when the policy or keyhive access changes
    /// (see [`access`]), never on this timer.
    pub sweep_interval: Duration,
}

impl Default for PeersConfig {
    fn default() -> Self {
        Self {
            loopback_only: false,
            policy: PolicyChoice::default(),
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(60),
            sweep_interval: Duration::from_secs(15),
        }
    }
}

/// The embedder's peer policy: the built-in mirroring policy over a fixed dial list, or its own.
#[derive(Clone)]
pub enum PolicyChoice {
    Mirror(MirrorPolicy),
    Custom(Arc<dyn PeerPolicy>),
}

impl Default for PolicyChoice {
    fn default() -> Self {
        Self::Mirror(MirrorPolicy::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Incoming,
    Outgoing,
}

/// What the peers service reports; the successor of `IrohSyncEvent`'s transport and progress
/// variants.
#[derive(Debug, Clone)]
pub enum PeersEvent {
    Connected {
        peer: PeerKey,
        direction: Direction,
    },
    Disconnected {
        peer: PeerKey,
        reason: String,
    },
    /// A document this node subscribes to was synced with `peer`.
    DocSynced {
        peer: PeerKey,
        doc: ObjKey,
    },
    /// Everything `peer` has in `part` has been synced.
    PartSynced {
        peer: PeerKey,
        part: PartKey,
    },
    /// `peer` has changes this node has not caught up with.
    Stale {
        peer: PeerKey,
    },
}

enum Command {
    Connect {
        addr: iroh::EndpointAddr,
        resp: oneshot::Sender<Res<PeerKey>>,
    },
    Connected {
        resp: oneshot::Sender<Vec<PeerKey>>,
    },
}

/// Parses an endpoint ticket (the text form `iroh_tickets` gives an address: the endpoint's id,
/// its relay and its direct addresses) into the address a dial list takes. This is the one
/// spelling of a peer's address outside the process, as the ticket in `daybook_core`'s sync
/// bootstrap is, and what [`Peers::ticket`] prints.
pub fn parse_ticket(ticket: &str) -> Res<iroh::EndpointAddr> {
    Ok(ticket
        .parse::<iroh_tickets::endpoint::EndpointTicket>()
        .wrap_err_with(|| format!("`{ticket}` is not an endpoint ticket"))?
        .endpoint_addr()
        .clone())
}

/// The handle to a running peers service.
#[derive(Clone)]
pub struct Peers {
    endpoint: iroh::Endpoint,
    events: broadcast::Sender<PeersEvent>,
    commands: mpsc::UnboundedSender<Command>,
}

impl Peers {
    /// This node's address: its key and the sockets it is reachable on. Handing it to another
    /// node's dial list (or [`Peers::connect`]) is how two nodes meet.
    pub fn addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }

    /// [`Peers::addr`] as a ticket, the text [`parse_ticket`] reads back.
    pub fn ticket(&self) -> String {
        iroh_tickets::endpoint::EndpointTicket::from(self.addr()).to_string()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PeersEvent> {
        self.events.subscribe()
    }

    /// Dials `addr` once, outside the policy's dial list (no redial), and answers when the peer
    /// is registered. A peer that is already connected or being connected answers at once.
    pub async fn connect(&self, addr: iroh::EndpointAddr) -> Res<PeerKey> {
        let (resp, answer) = oneshot::channel();
        self.commands
            .send(Command::Connect { addr, resp })
            .map_err(|_| eyre::eyre!("the peers service is stopped"))?;
        answer
            .await
            .map_err(|_| eyre::eyre!("the peers service stopped while connecting"))?
    }

    /// The peers with a registered connection now.
    pub async fn connected(&self) -> Res<Vec<PeerKey>> {
        let (resp, answer) = oneshot::channel();
        self.commands
            .send(Command::Connected { resp })
            .map_err(|_| eyre::eyre!("the peers service is stopped"))?;
        answer
            .await
            .map_err(|_| eyre::eyre!("the peers service stopped while listing peers"))
    }

    /// Waits until `peer` has no registered connection (it may already have none).
    pub async fn wait_disconnected(&self, peer: &PeerKey) {
        let mut events = self.subscribe();
        let mut connected = self
            .connected()
            .await
            .expect("peers are running")
            .contains(peer);
        while connected {
            match events.recv().await {
                Ok(PeersEvent::Disconnected { peer: found, .. }) if &found == peer => {
                    connected = false;
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    unreachable!("the service keeps its event sender while a handle exists")
                }
            }
        }
    }
}

/// Stops the service in reverse construction order. Built by [`boot`], run by the node's stop.
pub(crate) struct PeersStop {
    tasks: service_utils_rs::StopToken,
    blob_worker: big_sync::StopToken,
    doc_worker: big_sync::StopToken,
    router: iroh::protocol::Router,
    sync_rpc: big_sync::rpc::BigSyncRpcStopToken,
    repo_rpc: big_repo::rpc::BigRepoRpcStopToken,
    _sync_rpc_handle: big_sync::rpc::BigSyncRpcHandle,
}

impl PeersStop {
    pub(crate) async fn stop(self) -> Res<()> {
        // The actor and the drivers were started last; the actor closes its connections and
        // removes its peers from the workers before the workers go.
        self.tasks.stop().await;
        self.blob_worker.stop().await?;
        self.doc_worker.stop().await?;
        let endpoint = self.router.endpoint().clone();
        self.router.shutdown().await?;
        self.sync_rpc.stop().await?;
        self.repo_rpc.stop().await?;
        endpoint.close().await;
        Ok(())
    }
}

/// The parts of a node the peers service is built over.
pub(crate) struct Node<'a> {
    pub repo: &'a big_repo::SharedBigRepo,
    /// The repo's `scope_key`: the service's access-stream cursor shares the repo's database with
    /// other repos' and is namespaced by it.
    pub scope_key: &'a str,
    pub drawers: &'a big_cabinet::Drawers,
    pub blobs: &'a big_blobs::Blobs,
    pub blob_workers: &'a big_blobs::workers::BlobWorkers,
}

/// Refuses a connection the policy does not accept, and hands an accepted one to the actor.
#[derive(Clone)]
struct Accept {
    repo: big_repo::SharedBigRepo,
    endpoint: iroh::Endpoint,
    ctx: PolicyContext,
    policy: Arc<dyn PeerPolicy>,
    incoming: mpsc::UnboundedSender<BigRepoConnection>,
    ends: mpsc::UnboundedSender<ConnFinishSignal>,
}

impl std::fmt::Debug for Accept {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Accept").finish_non_exhaustive()
    }
}

impl ProtocolHandler for Accept {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let peer = PeerKey::new(*connection.remote_id().as_bytes());
        if !self.policy.accept(&self.ctx, &peer).await {
            return Err(AcceptError::from_boxed(
                eyre::eyre!("the peer policy does not accept {peer}").into(),
            ));
        }
        let connection = self
            .repo
            .accept_connection_iroh(connection, self.endpoint.clone(), Some(self.ends.clone()))
            .await
            .map_err(|error| AcceptError::from_boxed(error.into()))?;
        // The actor owns the receiver for as long as the router runs; a closed channel is the
        // service stopping, which refuses the connection.
        self.incoming.send(connection).map_err(|_| {
            AcceptError::from_boxed(eyre::eyre!("the peers service is stopping").into())
        })
    }
}

#[derive(Debug)]
struct ServeBlobs(iroh_blobs::BlobsProtocol);

impl ProtocolHandler for ServeBlobs {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        ProtocolHandler::accept(&self.0, connection).await
    }
}

pub(crate) async fn boot(
    node: Node<'_>,
    config: PeersConfig,
    seed: [u8; 32],
) -> Res<(Peers, PeersStop)> {
    let lookup = iroh::address_lookup::MemoryLookup::new();
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .address_lookup(lookup.clone())
        .secret_key(iroh::SecretKey::from_bytes(&seed));
    if config.loopback_only {
        builder = builder
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled);
    }
    let endpoint = builder.bind().await?;
    debug_assert_eq!(
        endpoint.id().as_bytes(),
        &node.repo.local_peer_id().to_bytes32()?,
        "the endpoint key and the keyhive identity are one key"
    );
    let ctx = PolicyContext {
        drawers: node.drawers.clone(),
        repo: Arc::clone(node.repo),
    };
    let policy: Arc<dyn PeerPolicy> = match config.policy {
        PolicyChoice::Mirror(policy) => Arc::new(policy),
        PolicyChoice::Custom(policy) => policy,
    };

    let blob_backend = Arc::new(BlobSyncBackend::new(
        node.blob_workers.mirrors.clone(),
        endpoint.clone(),
        lookup.clone(),
    ));
    let (repo_rpc, repo_rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(node.repo)).await?;
    let (sync_rpc, sync_rpc_stop) = big_sync::rpc::spawn_big_sync_rpc(HashMap::from([
        (Arc::from(DOCS_SCOPE), node.repo.shared_part_store()),
        (Arc::from(BLOBS_SCOPE), Arc::clone(&node.blob_workers.parts)),
    ]))
    .await?;
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
    let (ends_tx, ends_rx) = mpsc::unbounded_channel();
    let router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            subduction_iroh::ALPN,
            Accept {
                repo: Arc::clone(node.repo),
                endpoint: endpoint.clone(),
                ctx: ctx.clone(),
                policy: Arc::clone(&policy),
                incoming: incoming_tx,
                ends: ends_tx.clone(),
            },
        )
        .accept(big_repo::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
        .accept(
            big_sync::rpc::BIG_SYNC_RPC_ALPN,
            sync_rpc.protocol_handler(),
        )
        .accept(
            iroh_blobs::ALPN,
            ServeBlobs(iroh_blobs::BlobsProtocol::new(
                node.blobs.service.store(),
                None,
            )),
        )
        .spawn();

    let repo_backend = Arc::new(
        BigRepoSyncBackend::boot(Arc::downgrade(node.repo))
            .await
            .wrap_err("booting the repo sync backend")?,
    );
    let (docs, doc_worker) = big_sync::spawn_big_sync_worker_with_options(
        node.repo.shared_part_store(),
        HashMap::from([(BigRepo::BACKEND_ID.into(), repo_backend as _)]),
        DOCS_SCOPE,
        None,
        Some(big_sync::SyncMode::Bucket),
        Arc::from(DOCS_SCOPE),
    )?;
    let (blobs, blob_worker) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&node.blob_workers.parts),
        HashMap::from([(
            BackendId::from(BLOBS_BACKEND),
            Arc::clone(&blob_backend) as _,
        )]),
        BLOBS_SCOPE,
        None,
        Some(big_sync::SyncMode::CursorOnly),
        Arc::from(BLOBS_SCOPE),
    )?;

    let (peers_tx, peers_rx) = watch::channel(HashSet::new());
    let (stop, tasks) = service_utils_rs::StopToken::new();
    let demand = node.repo.drive_sync_demand(&docs, peers_rx);
    let cancel = tasks.clone();
    tasks
        .spawn(async move {
            tokio::select! { biased; _ = cancel.cancelled() => (), result = demand => result.unwrap() }
        })
        .expect("a fresh stop token accepts tasks");
    let mirrors = node.blob_workers.mirrors.clone();
    let source = blobs.subscription_source();
    let cancel = tasks.clone();
    tasks
        .spawn(async move {
            tokio::select! { biased; _ = cancel.cancelled() => (), result = mirrors.drive_subscriptions(source, Arc::from(BLOBS_BACKEND)) => result.unwrap() }
        })
        .expect("a fresh stop token accepts tasks");

    let access_rx = access::spawn(&tasks, Arc::clone(node.repo), node.scope_key);

    let (events, _) = broadcast::channel(1024);
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();
    let actor = Actor {
        stats: docs.subscribe_stats(),
        changes: policy.changes(&ctx),
        ctx,
        policy,
        endpoint: endpoint.clone(),
        lookup,
        blob_backend,
        repo_rpc,
        docs,
        blobs,
        events: events.clone(),
        ends_tx,
        internal_tx,
        tasks: tasks.clone(),
        peers: peers_tx,
        slots: HashMap::new(),
        redial: HashMap::new(),
        watched_docs: HashSet::new(),
        backoff: (config.initial_backoff, config.max_backoff),
        sweep_interval: config.sweep_interval,
    };
    tasks
        .spawn(async move {
            actor
                .run(commands_rx, incoming_rx, ends_rx, internal_rx, access_rx)
                .await
                .unwrap()
        })
        .expect("a fresh stop token accepts tasks");
    Ok((
        Peers {
            endpoint,
            events,
            commands: commands_tx,
        },
        PeersStop {
            tasks: stop,
            blob_worker,
            doc_worker,
            router,
            sync_rpc: sync_rpc_stop,
            repo_rpc: repo_rpc_stop,
            _sync_rpc_handle: sync_rpc,
        },
    ))
}

enum Slot {
    /// Reserved, with no connection yet (`closed: None`: a dial is in flight) or with one that is
    /// being registered. Its end flag, once it exists, identifies its end signal.
    Connecting {
        closed: Option<Arc<AtomicBool>>,
    },
    Connected(Link),
}

struct Link {
    conn: BigRepoConnection,
    addr: iroh::EndpointAddr,
    rpc: Arc<dyn WireBigSyncRpcClient>,
    /// The groups the last policy answer wanted from the peer, readable or not. An access delta
    /// naming one of them (or the peer itself) is what makes the answer worth recomputing:
    /// whether the peer is a member of a wanted group is keyhive state that arrives on its own
    /// schedule, and the access stream says when it did.
    watch: HashSet<[u8; 32]>,
}

struct Redial {
    addr: iroh::EndpointAddr,
    failures: u32,
    due: Instant,
}

enum Internal {
    Dialed {
        addr: iroh::EndpointAddr,
        resp: Option<oneshot::Sender<Res<PeerKey>>>,
        result: Res<BigRepoConnection>,
    },
}

struct Actor {
    ctx: PolicyContext,
    policy: Arc<dyn PeerPolicy>,
    endpoint: iroh::Endpoint,
    lookup: iroh::address_lookup::MemoryLookup,
    blob_backend: Arc<BlobSyncBackend>,
    repo_rpc: big_repo::rpc::BigRepoRpcHandle,
    docs: big_sync::BigSyncWorkerHandle,
    blobs: big_sync::BigSyncWorkerHandle,
    events: broadcast::Sender<PeersEvent>,
    stats: broadcast::Receiver<SyncStatEvent>,
    changes: watch::Receiver<u64>,
    ends_tx: mpsc::UnboundedSender<ConnFinishSignal>,
    internal_tx: mpsc::UnboundedSender<Internal>,
    tasks: service_utils_rs::Spawner,
    /// The connected peers, as big_repo's sync demand driver wants them.
    peers: watch::Sender<HashSet<PeerKey>>,
    slots: HashMap<PeerKey, Slot>,
    redial: HashMap<PeerKey, Redial>,
    /// Docs the last policy answer asked for by id. Their arrival can change the answer (a
    /// drawer descriptor names groups), so their sync re-applies the parts.
    watched_docs: HashSet<ObjKey>,
    /// Initial and cap of the redial delay.
    backoff: (Duration, Duration),
    sweep_interval: Duration,
}

impl Actor {
    fn local(&self) -> PeerKey {
        PeerKey::new(*self.endpoint.id().as_bytes())
    }

    fn emit(&self, event: PeersEvent) {
        // A broadcast channel has 0..N listeners; no receiver is not a failure.
        drop(self.events.send(event));
    }

    async fn run(
        mut self,
        mut commands: mpsc::UnboundedReceiver<Command>,
        mut incoming: mpsc::UnboundedReceiver<BigRepoConnection>,
        mut ends: mpsc::UnboundedReceiver<ConnFinishSignal>,
        mut internal: mpsc::UnboundedReceiver<Internal>,
        mut access: mpsc::UnboundedReceiver<KeyhiveAccessDelta>,
    ) -> Res<()> {
        let cancel = self.tasks.clone();
        let mut sweep_tick = tokio::time::interval(self.sweep_interval);
        sweep_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_due = self.sweep().await;
        loop {
            let due = async {
                match next_due {
                    Some(due) => tokio::time::sleep_until(due).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(command) = commands.recv() => self.command(command).await?,
                Some(connection) = incoming.recv() => self.incoming(connection).await?,
                Some(signal) = ends.recv() => {
                    self.ended(signal).await?;
                    next_due = self.sweep().await;
                }
                Some(message) = internal.recv() => {
                    self.internal(message).await?;
                    next_due = self.sweep().await;
                }
                event = self.stats.recv() => self.stat(event).await?,
                changed = self.changes.changed() => {
                    changed.wrap_err("the peer policy dropped its change signal")?;
                    self.reapply(|_, _| true).await?;
                    next_due = self.sweep().await;
                }
                Some(delta) = access.recv() => {
                    // A grant is several admissions; recompute once for the burst.
                    let mut burst = vec![delta];
                    while let Ok(more) = access.try_recv() {
                        burst.push(more);
                    }
                    self.access_changed(&burst).await?;
                }
                _ = due => next_due = self.sweep().await,
                _ = sweep_tick.tick() => next_due = self.sweep().await,
            }
        }
        for (peer, slot) in std::mem::take(&mut self.slots) {
            if let Slot::Connected(link) = slot {
                self.unregister(&peer).await?;
                link.conn.stop().await?;
            }
        }
        Ok(())
    }

    /// Dials every policy target that is neither connected nor backing off. Answers when the
    /// earliest backing-off target is due, if any.
    async fn sweep(&mut self) -> Option<Instant> {
        let now = Instant::now();
        let targets = self.policy.dial(&self.ctx).await;
        let local = self.local();
        let ids: HashSet<PeerKey> = targets
            .iter()
            .map(|addr| PeerKey::new(*addr.id.as_bytes()))
            .collect();
        self.redial.retain(|peer, _| ids.contains(peer));
        let mut next: Option<Instant> = None;
        for addr in targets {
            let peer = PeerKey::new(*addr.id.as_bytes());
            if peer == local || self.slots.contains_key(&peer) {
                continue;
            }
            let state = self.redial.entry(peer).or_insert_with(|| Redial {
                addr: addr.clone(),
                failures: 0,
                due: now,
            });
            state.addr = addr.clone();
            if state.due <= now {
                self.dial(addr, None);
            } else {
                next = Some(next.map_or(state.due, |earliest| earliest.min(state.due)));
            }
        }
        next
    }

    async fn command(&mut self, command: Command) -> Res<()> {
        match command {
            Command::Connect { addr, resp } => self.dial(addr, Some(resp)),
            Command::Connected { resp } => {
                let connected = self
                    .slots
                    .iter()
                    .filter(|(_, slot)| matches!(slot, Slot::Connected(_)))
                    .map(|(peer, _)| peer.clone())
                    .collect();
                // The asker may have gone; the answer is for nobody then.
                drop(resp.send(connected));
            }
        }
        Ok(())
    }

    /// Reserves the peer's slot and opens the connection off the actor, so a dial that takes
    /// its time never holds up the other peers.
    fn dial(&mut self, addr: iroh::EndpointAddr, resp: Option<oneshot::Sender<Res<PeerKey>>>) {
        let peer = PeerKey::new(*addr.id.as_bytes());
        if peer == self.local() {
            if let Some(resp) = resp {
                drop(resp.send(Err(eyre::eyre!("a node does not connect to itself"))));
            }
            return;
        }
        if self.slots.contains_key(&peer) {
            if let Some(resp) = resp {
                drop(resp.send(Ok(peer)));
            }
            return;
        }
        self.slots
            .insert(peer.clone(), Slot::Connecting { closed: None });
        let repo = Arc::clone(&self.ctx.repo);
        let endpoint = self.endpoint.clone();
        let ends = self.ends_tx.clone();
        let internal = self.internal_tx.clone();
        let cancel = self.tasks.clone();
        let key = peer.clone();
        let spawned = self.tasks.spawn(async move {
            let open = repo.open_connection_iroh(endpoint, addr.clone(), peer, Some(ends));
            let result = tokio::select! { biased; _ = cancel.cancelled() => return, result = open => result };
            if let Err(unsent) = internal.send(Internal::Dialed { addr, resp, result }) {
                // The actor is gone, so the service is stopping: do not leave a connection
                // nobody owns.
                let Internal::Dialed { result, .. } = unsent.0;
                if let Ok(connection) = result {
                    connection.stop().await.unwrap();
                }
            }
        });
        if spawned.is_none() {
            // Stopping: the stop token is closed, the actor is about to leave its loop.
            self.slots.remove(&key);
        }
    }

    async fn internal(&mut self, message: Internal) -> Res<()> {
        let Internal::Dialed { addr, resp, result } = message;
        let peer = PeerKey::new(*addr.id.as_bytes());
        let answer = match result {
            Ok(conn) => {
                self.slots.insert(
                    peer.clone(),
                    Slot::Connecting {
                        closed: Some(conn.closed_flag()),
                    },
                );
                self.register(conn, addr, Direction::Outgoing).await?;
                self.redial.remove(&peer);
                Ok(peer)
            }
            Err(error) => {
                // The network said no: a failed dial is expected and is retried on a backoff.
                tracing::warn!(%peer, ?error, "dialing the peer failed");
                self.slots.remove(&peer);
                let (initial, max) = self.backoff;
                let state = self.redial.entry(peer).or_insert_with(|| Redial {
                    addr,
                    failures: 0,
                    due: Instant::now(),
                });
                state.failures += 1;
                let delay = initial
                    .saturating_mul(2u32.saturating_pow(state.failures - 1))
                    .min(max);
                state.due = Instant::now() + delay;
                Err(error)
            }
        };
        if let Some(resp) = resp {
            drop(resp.send(answer));
        }
        Ok(())
    }

    async fn incoming(&mut self, conn: BigRepoConnection) -> Res<()> {
        let peer = conn.peer_id.clone();
        match self.slots.get(&peer) {
            Some(Slot::Connected(_)) => {
                tracing::warn!(%peer, "new connection replaces the registered one");
                self.unregister(&peer).await?;
                self.slots.remove(&peer);
            }
            Some(Slot::Connecting { .. }) => {
                tracing::warn!(%peer, "ignoring an incoming connection while setting up");
                return Ok(());
            }
            None => {}
        }
        self.slots.insert(
            peer.clone(),
            Slot::Connecting {
                closed: Some(conn.closed_flag()),
            },
        );
        // The transport knows where the connection came from; with no record (the connection
        // already died) the bare id is all there is, and dialing it later is the lookup's job.
        let id = iroh::EndpointId::from_bytes(&peer.to_bytes32()?)?;
        let addr = match self.endpoint.remote_info(id).await {
            Some(info) => iroh::EndpointAddr::from_parts(
                info.id(),
                info.into_addrs().map(|info| info.into_addr()),
            ),
            None => iroh::EndpointAddr::new(id),
        };
        self.register(conn, addr, Direction::Incoming).await
    }

    /// Makes `conn`'s peer known to every service: the repo RPC peer map, the blob downloader's
    /// address book, and the two workers.
    async fn register(
        &mut self,
        conn: BigRepoConnection,
        addr: iroh::EndpointAddr,
        direction: Direction,
    ) -> Res<()> {
        let peer = conn.peer_id.clone();
        self.lookup.add_endpoint_info(addr.clone());
        self.blob_backend
            .register_peer_addr(peer.clone(), addr.clone());
        self.repo_rpc.register_peer(addr.id, peer.clone());
        let rpc: Arc<dyn WireBigSyncRpcClient> = Arc::new(
            big_sync::rpc::BigSyncRpcClient::over_iroh(self.endpoint.clone(), addr.clone()),
        );
        // Blob parts are subscribed per inventory by the blob mirrors' driver, so the blob
        // worker only needs to know how to reach the peer.
        self.blobs
            .set_peer(
                peer.clone(),
                Arc::clone(&rpc),
                HashMap::new(),
                HashMap::new(),
            )
            .await?;
        let watch = self.apply(&peer, &rpc).await?;
        let link = Link {
            conn,
            addr,
            rpc,
            watch,
        };
        self.slots.insert(peer.clone(), Slot::Connected(link));
        self.peers.send_modify(|peers| {
            peers.insert(peer.clone());
        });
        self.emit(PeersEvent::Connected { peer, direction });
        Ok(())
    }

    /// Asks the policy what it wants from `peer`, bounds that by what the peer may have (keyhive
    /// access, as BigRepo projects it) and sets the document worker's routes to the result.
    ///
    /// Answers the wanted groups, which is what a later access delta is matched against.
    async fn apply(
        &mut self,
        peer: &PeerKey,
        rpc: &Arc<dyn WireBigSyncRpcClient>,
    ) -> Res<HashSet<[u8; 32]>> {
        let wanted = self.policy.wanted(&self.ctx, peer).await?;
        let groups: Vec<[u8; 32]> = wanted.groups.iter().map(|group| group.0).collect();
        let readable = big_repo::groups_with_member(self.ctx.repo.keyhive(), peer, &groups).await?;
        tracing::debug!(local = %self.local(), %peer, wanted_groups = groups.len(), readable = readable.len(), wanted_docs = wanted.docs.len(), "applying peer parts");
        let parts = readable
            .into_iter()
            .map(|group| (big_repo::group_part_id(group), BigRepo::BACKEND_ID.into()))
            .collect();
        let objects = wanted
            .docs
            .iter()
            .map(|doc| (doc.clone(), BigRepo::BACKEND_ID.into()))
            .collect();
        self.watched_docs.extend(wanted.docs);
        self.docs
            .set_peer(peer.clone(), Arc::clone(rpc), parts, objects)
            .await?;
        Ok(groups.into_iter().collect())
    }

    /// Re-asks the policy for every connected peer `touched` selects and sets its parts again.
    async fn reapply(&mut self, touched: impl Fn(&PeerKey, &Link) -> bool) -> Res<()> {
        let peers: Vec<PeerKey> = self
            .slots
            .iter()
            .filter(|(peer, slot)| matches!(slot, Slot::Connected(link) if touched(peer, link)))
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in peers {
            // Taken out for the duration so `apply` can borrow the actor mutably; a connection
            // end signal cannot interleave because this runs inside the actor's own loop.
            let Some(Slot::Connected(mut link)) = self.slots.remove(&peer) else {
                unreachable!("filtered as connected above");
            };
            let applied = self.apply(&peer, &link.rpc).await;
            if let Ok(watch) = &applied {
                link.watch.clone_from(watch);
            }
            self.slots.insert(peer, Slot::Connected(link));
            applied?;
        }
        Ok(())
    }

    /// Keyhive access deltas (each the closure of one subject, as it now is) recompute the parts
    /// of the connected peers they can change. A document subject is a drawer or a document: it
    /// matters to a peer that wanted it as a group, or that is in its closure. A group subject
    /// can sit anywhere in a membership chain, so every peer is recomputed.
    async fn access_changed(&mut self, deltas: &[KeyhiveAccessDelta]) -> Res<()> {
        self.reapply(|peer, link| {
            deltas.iter().any(|delta| match delta.subject {
                AccessSubject::Group(_) => true,
                AccessSubject::Document(id) => {
                    link.watch.contains(&id.to_bytes())
                        || delta
                            .agents
                            .keys()
                            .any(|agent| PeerKey::new(agent.to_bytes()) == *peer)
                }
            })
        })
        .await
    }

    async fn unregister(&mut self, peer: &PeerKey) -> Res<()> {
        self.repo_rpc.unregister_peer(peer.clone());
        self.blob_backend.unregister_peer_addr(peer.clone());
        self.docs.remove_peer(peer.clone()).await?;
        self.blobs.remove_peer(peer.clone()).await?;
        self.peers.send_modify(|peers| {
            peers.remove(peer);
        });
        Ok(())
    }

    async fn ended(&mut self, signal: ConnFinishSignal) -> Res<()> {
        // Every tracked connection end is signalled, including a superseded connection's: only
        // the end of the peer's current connection tears the registration down.
        let current = match self.slots.get(&signal.peer_id) {
            Some(Slot::Connected(link)) => Arc::ptr_eq(&link.conn.closed_flag(), &signal.closed),
            Some(Slot::Connecting {
                closed: Some(closed),
            }) => Arc::ptr_eq(closed, &signal.closed),
            Some(Slot::Connecting { closed: None }) | None => false,
        };
        if !current {
            return Ok(());
        }
        let peer = signal.peer_id;
        tracing::info!(%peer, error = ?signal.err, "connection ended; tearing the peer down");
        self.unregister(&peer).await?;
        let Some(slot) = self.slots.remove(&peer) else {
            unreachable!("checked above");
        };
        if let Slot::Connected(link) = slot {
            self.redial.insert(
                peer.clone(),
                Redial {
                    addr: link.addr,
                    failures: 0,
                    due: Instant::now() + self.backoff.0,
                },
            );
            self.emit(PeersEvent::Disconnected {
                peer,
                reason: signal
                    .err
                    .map_or_else(|| "closed".to_owned(), |error| format!("{error:#}")),
            });
        }
        Ok(())
    }

    async fn stat(&mut self, event: Result<SyncStatEvent, broadcast::error::RecvError>) -> Res<()> {
        tracing::debug!(local = %self.local(), ?event, "sync stat");
        match event {
            Ok(SyncStatEvent::ObjectSynced { peer_id, obj_id }) => {
                let descriptor_arrived = self.watched_docs.contains(&obj_id);
                self.emit(PeersEvent::DocSynced {
                    peer: peer_id,
                    doc: obj_id,
                });
                if descriptor_arrived {
                    self.reapply(|_, _| true).await?;
                }
            }
            Ok(SyncStatEvent::PeerPartFullySynced { peer_id, part_id }) => {
                self.emit(PeersEvent::PartSynced {
                    peer: peer_id,
                    part: part_id,
                });
            }
            Ok(SyncStatEvent::PeerStale { peer_id }) => {
                self.emit(PeersEvent::Stale { peer: peer_id });
            }
            // The other stats are reports about retries and part sets that the workers act on
            // themselves.
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(skipped, "the sync stats observer lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                eyre::bail!("the document sync worker stopped under the peers service");
            }
        }
        Ok(())
    }
}
