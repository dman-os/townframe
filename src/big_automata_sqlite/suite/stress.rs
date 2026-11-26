//! A whole-machine stress run over SQLite: the port of big_sync's `stress.rs`.
//!
//! Several nodes, each a SQLite file holding its part store and its peer-cursor table, serve
//! their store through the real responders and run one real [`PeerDriver`] per other node
//! (so every node pulls from every other). The run writes, churns and restarts, then judges:
//!
//! - **convergence**: every node's part store holds exactly the members and payloads the
//!   model says, which makes all nodes equal; their root bucket summaries (live count and
//!   fingerprint) and member counts agree too;
//! - **quiet**: once converged, nothing commits and the backends are not called any more.
//!
//! # Membership is each node's own
//!
//! A peer's job never changes a node's membership (`SyncBackend`'s contract): the replay of a
//! part is only a notification that an object changed. Who is a member of which part comes
//! from a simulated keyhive stream the script owns. The model is the truth; an object's owner
//! changes its own store's memberships as it writes, and tells every other node's stream, which
//! delivers its pending changes in order at random moments, so that a membership reaches a
//! node before, with or after the notification of its object. A notification that arrives
//! first is parked in the node's held wants and fetched when the membership is delivered. A
//! removal is delivered the same way while the sync is running, and a peer's stale echo of an
//! old add can only be parked, never resurrect the member.
//!
//! The workload partitions authority, as an embedder's policy would:
//!
//! - an object's **owner** (`obj % nodes`) is the only node that adds or removes its
//!   memberships, and overwrites its payload;
//! - a **stable** object is never removed, so any node that holds it also overwrites its
//!   payload: concurrent last-writer-wins payloads, the old suite's `LwwPayload` workload;
//! - a **churn** object (one in three) is removed and re-added by its owner alone, at any
//!   point of the run.
//!
//! Every payload is stamped from one counter in the order the script issues the writes, so
//! the greatest stamp of an object is the last write issued and the model is exact.
//!
//! # Churn
//!
//! Connections drop and come back, transport calls fail, and nodes restart: the node's drivers
//! are aborted wherever they are (a transaction in flight is dropped, which rolls it back), its
//! database handle is dropped and the same file reopened, and the other nodes' drivers
//! reconnect.
//!
//! # Determinism
//!
//! The workload (which node writes what, when connections flap and nodes restart) is a pure
//! function of the seed, and time is tokio's paused clock injected into the stores, the
//! responders and the drivers: nothing sleeps on the wall clock. What the seed does not fix is
//! the interleaving of the drivers with the writes: SQLite runs on `sqlite_rs`'s worker
//! threads, and a paused tokio clock advances whenever the runtime has nothing ready to poll,
//! including while it waits on a worker. So a backoff can expire earlier or later in
//! virtual time between two runs of one seed. The assertions are on the converged state and
//! the quiet window, which do not depend on that interleaving. The waits count looks (one look
//! is a nudge of the virtual clock and a read of every node), never wall time, so a stall
//! fails the test instead of hanging it.
//!
//! # Running
//!
//! `AUTOMATA_SQLITE_STRESS_SEED` replays one seed of the default run. The heavy variant is
//! `#[ignore]`d: `cargo nextest run -p big_automata_sqlite --run-ignored only stress_heavy`
//! (it takes the same environment variable, and `AUTOMATA_SQLITE_STRESS_SEEDS` is how many
//! consecutive seeds it sweeps from it).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use big_automata::ByteKey;
use big_automata::driver::DriverConfig;
use big_automata::driver::Facts;
use big_automata::driver::ObjJob;
use big_automata::driver::PeerCursors;
use big_automata::driver::PeerDriver;
use big_automata::driver::RouteUpdate;
use big_automata::driver::Settle;
use big_automata::driver::SyncBackend;
use big_automata::driver::memory::LwwPayload;
use big_automata::durable_outbox::Advert;
use big_automata::durable_outbox::HeldConfig;
use big_automata::durable_outbox::HeldTable;
use big_automata::long_poll::Timer;
use big_automata::part_store::AccessLevel;
use big_automata::part_store::ObjPayload;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::PartStoreWrite;
use big_automata::part_store::laws::stage;
use big_automata::part_store::oracle::PartWrite;
use big_automata::replay::responder::Responder;
use big_automata::replay::server::ReplayServer;
use big_automata::replay::wire::CloseSession;
use big_automata::replay::wire::PageSession;
use big_automata::replay::wire::SessionError;
use big_automata::replay::wire::SessionPage;
use big_automata::replay::wire::SessionUpdated;
use big_automata::replay::wire::UpdateSession;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata::sync::rpc::BigSyncRpcClient;
use big_automata::sync::rpc::BigSyncRpcResult;
use big_automata::sync::rpc::RpcError;
use big_automata::sync::rpc::bucket::BucketsError;
use big_automata::sync::rpc::bucket::GetChangedBucketsRequest;
use big_automata::sync::rpc::bucket::GetChangedBucketsResponse;
use big_automata::sync::rpc::bucket::LeafBucketsRequest;
use big_automata::sync::rpc::bucket::LeafBucketsResponse;
use big_automata::sync::rpc::responder::RpcResponder;
use big_automata::sync::rpc::summary::PartSummariesError;
use big_automata::sync::rpc::summary::PartSummariesRequest;
use big_automata::sync::rpc::summary::PartSummariesResponse;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use sqlite_rs::Database;
use tokio::task::JoinHandle;

use super::RUNTIME;
use super::Store;
use crate::interlude::*;
use crate::*;

const BACKEND: &str = "sqlite";

fn obj(index: usize) -> ObjKey {
    ObjKey::new(&(index as u32).to_be_bytes())
}

fn part(index: usize) -> PartKey {
    PartKey::new(&[b'p', index as u8])
}

fn node_key(index: usize) -> PeerKey {
    PeerKey::new(&[10 + index as u8])
}

/// The principal every driver acts as at every responder: each node grants it every part.
fn reader() -> PeerKey {
    PeerKey::new(&[0])
}

// # The node's serving half

/// What a node serves from: its store over its file, the peer cursors of its own drivers in
/// the same file, and the responders' state.
struct Serving {
    store: Store,
    cursors: SqlitePeerCursors<Database>,
    rpc: RpcResponder<Store>,
    server: Mutex<ReplayServer>,
}

impl Serving {
    /// Open (or reopen) the node's file.
    async fn open(path: &Path, time: &Time) -> Res<Arc<Self>> {
        let db = RUNTIME.database(path, MIGRATIONS, MIGRATION_TABLE);
        let store = Store::open(db.clone(), time.clock()).await?;
        Ok(Arc::new(Self {
            rpc: RpcResponder::new(store.clone()),
            cursors: SqlitePeerCursors::new(db),
            store,
            server: default(),
        }))
    }
}

/// The run's time, moved only by the script.
///
/// The stores' long-poll hubs, the responders and the drivers all read this clock, and a
/// responder's hold sleeps on it. It is not tokio's paused clock, because that one advances by
/// itself whenever the runtime has nothing ready to poll, and that includes every wait on a
/// SQLite worker thread: a 15 second hold would then end within the milliseconds a query
/// takes, and every driver would re-poll in a hot loop that starves the writers. Here a hold
/// ends once its deadline has passed on this clock and the script has taken a step since the
/// hold began, so a parked poll wakes at most once per step.
///
/// The drivers' own backoff sleeps are still tokio's, so [`Time::advance`] moves tokio's
/// paused clock along, and a backoff that tokio ends early is re-armed by its machine against
/// this clock.
#[derive(Clone)]
struct Time(Arc<TimeState>);

struct TimeState {
    start: web_time::Instant,
    elapsed_ms: AtomicU64,
    steps: tokio::sync::watch::Sender<u64>,
}

impl Time {
    fn new() -> Self {
        Self(Arc::new(TimeState {
            start: web_time::Instant::now(),
            elapsed_ms: AtomicU64::new(0),
            steps: tokio::sync::watch::channel(0).0,
        }))
    }

    fn now(&self) -> web_time::Instant {
        self.0.start + Duration::from_millis(self.0.elapsed_ms.load(Ordering::SeqCst))
    }

    fn now_ms(&self) -> u64 {
        self.0.elapsed_ms.load(Ordering::SeqCst)
    }

    fn clock(&self) -> big_automata::Clock {
        let time = self.clone();
        Arc::new(move || time.now())
    }

    async fn advance(&self, by: Duration) {
        let ms = u64::try_from(by.as_millis()).expect("a short step");
        self.0.elapsed_ms.fetch_add(ms, Ordering::SeqCst);
        tokio::time::advance(by).await;
        self.0.steps.send_modify(|steps| *steps += 1);
    }
}

impl Timer for Time {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        let (time, mut steps) = (self.clone(), self.0.steps.subscribe());
        let deadline = self.now() + duration;
        async move {
            while time.now() < deadline {
                steps
                    .changed()
                    .await
                    .expect("the script outlives its holds");
            }
        }
    }
}

/// A node's place on the network: the serving half while it is up, and transport failures to
/// inject. Everything that talks to a node goes through its link and holds the `Arc<Serving>`
/// only for the length of one call, so a restart can wait for the old one to be unreferenced.
#[derive(Default)]
struct Link {
    serving: Mutex<Option<Arc<Serving>>>,
    /// The next this many calls fail as transport failures.
    fail_next: AtomicUsize,
}

impl Link {
    fn serving(&self) -> Option<Arc<Serving>> {
        self.serving.lock().expect("link lock").clone()
    }

    fn set(&self, serving: Option<Arc<Serving>>) {
        *self.serving.lock().expect("link lock") = serving;
    }

    fn gate(&self) -> BigSyncRpcResult<Arc<Serving>> {
        if self
            .fail_next
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(RpcError::TransportError("injected failure".into()));
        }
        self.serving()
            .ok_or_else(|| RpcError::TransportError("the node is down".into()))
    }
}

/// A driver's connection to a node: the real responders, called in process.
#[derive(Clone)]
struct Wire {
    time: Time,
    link: Arc<Link>,
}

impl BigSyncRpcClient for Wire {
    async fn part_summaries(
        &self,
        req: PartSummariesRequest,
    ) -> BigSyncRpcResult<Result<PartSummariesResponse, PartSummariesError>> {
        let serving = self.link.gate()?;
        Ok(serving
            .rpc
            .part_summaries(&reader(), req)
            .await
            .expect("store"))
    }

    async fn update_replay_session(
        &self,
        req: UpdateSession,
    ) -> BigSyncRpcResult<Result<SessionUpdated, SessionError>> {
        let serving = self.link.gate()?;
        let mut responder = Responder::new(serving.store.clone(), self.time.clone());
        let admits = responder.admission(&reader(), &req).await.expect("store");
        Ok(serving.server.lock().expect("server lock").update(
            &reader(),
            &req,
            self.time.now(),
            admits,
        ))
    }

    async fn page_replay_session(
        &self,
        req: PageSession,
    ) -> BigSyncRpcResult<Result<SessionPage, SessionError>> {
        let serving = self.link.gate()?;
        let mut responder = Responder::new(serving.store.clone(), self.time.clone());
        Ok(responder
            .page(&reader(), |round| {
                serving.server.lock().expect("server lock").resolve_round(
                    &reader(),
                    &req,
                    self.time.now(),
                    round,
                )
            })
            .await
            .expect("store")
            .cloned())
    }

    async fn close_replay_session(
        &self,
        req: CloseSession,
    ) -> BigSyncRpcResult<Result<(), SessionError>> {
        let serving = self.link.gate()?;
        serving
            .server
            .lock()
            .expect("server lock")
            .close(&reader(), &req);
        Ok(Ok(()))
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> BigSyncRpcResult<Result<GetChangedBucketsResponse, BucketsError>> {
        let serving = self.link.gate()?;
        Ok(serving
            .rpc
            .changed_buckets(&reader(), req)
            .await
            .expect("store"))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> BigSyncRpcResult<Result<LeafBucketsResponse, BucketsError>> {
        let serving = self.link.gate()?;
        Ok(serving
            .rpc
            .leaf_buckets(&reader(), req)
            .await
            .expect("store"))
    }
}

// # The backend

type Held = HeldTable<SqliteOkv<Database>>;

/// What one node's backends share: its store, its held wants (an okv table of the same
/// database, so a membership, a payload and a release commit together), the links to the peers
/// a parked want is fetched from, and its clock.
///
/// Membership is the node's own (`SyncBackend`'s contract): a peer's job never changes it. The
/// script stands in for the node's keyhive stream by calling [`member_added`](Self::member_added)
/// and [`member_removed`](Self::member_removed).
struct NodeBackend {
    local: Store,
    held: Held,
    links: BTreeMap<PeerKey, Arc<Link>>,
    time: Time,
}

impl NodeBackend {
    async fn new(local: Store, links: BTreeMap<PeerKey, Arc<Link>>, time: Time) -> Res<Self> {
        let held = HeldTable::new(
            SqliteOkv::open(&local, "held").await?,
            HeldConfig::default(),
        );
        Ok(Self {
            local,
            held,
            links,
            time,
        })
    }

    async fn write(&self, writes: Vec<PartWrite>) -> Res<()> {
        let mut tx = self.local.begin().await?;
        for write in writes {
            stage(&self.local, &mut tx, write).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// The membership source says `obj` is now a member of `part`: the want parked for the
    /// object, if any, is fetched from every peer that advertised it and released.
    async fn member_added(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::AddMembers(vec![(
            obj.clone(),
            part.clone(),
        )])])
        .await?;
        self.fetch_parked(obj, part).await
    }

    async fn member_removed(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::RemoveMembers(vec![(
            obj.clone(),
            part.clone(),
        )])])
        .await
    }

    /// Merge the payload every provider of the parked want holds, then release the want in the
    /// transaction of the last landing. One provider that cannot answer leaves the want
    /// parked: the error reaches the source, which retries.
    async fn fetch_parked(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        let mut tx = self.local.begin().await?;
        let want = self.held.get(&mut tx, obj).await?;
        tx.rollback().await?;
        // a want that no notification named `part` for stays parked
        let Some(want) = want.filter(|want| want.named.values().any(|named| named.contains(part)))
        else {
            return Ok(());
        };
        for provider in &want.providers {
            let link = self.links.get(provider).expect("a provider is a peer");
            self.land(obj, Self::payload_of(link, obj).await?).await?;
        }
        self.land_release(obj, part).await
    }

    /// `obj`'s payload at the peer behind `link`. A peer that is down fails the call, which
    /// the machine retries on its backoff.
    async fn payload_of(link: &Link, obj: &ObjKey) -> Res<ObjPayload> {
        let source = link
            .serving()
            .ok_or_else(|| eyre::eyre!("the peer is down"))?;
        source
            .store
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .and_then(|state| state.content)
            .map(|(payload, _)| payload)
            .ok_or_else(|| eyre::eyre!("the peer no longer holds the payload of {obj:?}"))
    }

    /// Merge `payload` into `obj`. The comparison and the write are one transaction: the
    /// writer lease is taken first, so no payload can land between the read of the held one
    /// and the write that replaces it (a newer local overwrite would otherwise be replaced by
    /// the older peer payload).
    async fn land(&self, obj: &ObjKey, payload: ObjPayload) -> Res<()> {
        let mut tx = self.local.begin().await?;
        let held = self
            .local
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .and_then(|state| state.content);
        if held.is_none_or(|(held, _)| LwwPayload::wins(&payload, &held)) {
            stage(
                &self.local,
                &mut tx,
                PartWrite::SetPayloads(vec![(obj.clone(), payload)]),
            )
            .await?;
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(())
    }

    async fn land_release(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        let mut tx = self.local.begin().await?;
        self.held.release(&mut tx, obj, part).await?;
        tx.commit().await?;
        Ok(())
    }

    /// The first of `parts` that `obj` is a member of, pending memberships included.
    async fn member_part(&self, obj: &ObjKey, parts: &[PartKey]) -> Res<Option<PartKey>> {
        Ok(self
            .local
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .and_then(|state| {
                parts
                    .iter()
                    .find(|part| state.parts.contains(*part))
                    .cloned()
            }))
    }
}

/// The driver's backend over a node's SQLite store, for the pull from one peer: downloads the
/// payload from the peer's store, merging last-writer-wins (the memory backend's download mode,
/// over SQLite). A peer that is down fails the fetch, which the machine retries on its backoff.
/// A job whose object is not yet a member is parked in the node's held wants (the node's
/// [`NodeBackend::member_added`] fetches it).
struct LwwBackend {
    node: Arc<NodeBackend>,
    peer: PeerKey,
    source: Arc<Link>,
    /// Calls made, to tell a quiet cluster from a busy one.
    calls: Arc<AtomicU64>,
}

impl SyncBackend for LwwBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // the job's parts are a notification: nothing here changes membership
        if job.add.is_empty() {
            // a removal moves no payload, and a job with no parts is content
            return Ok(if job.remove.is_empty() {
                Settle::Fetch
            } else {
                Settle::Done
            });
        }
        if self.node.member_part(&job.obj, &job.add).await?.is_some() {
            return Ok(Settle::Fetch);
        }
        let advert =
            Advert::new(self.peer.clone(), 0, ByteKey::new(&[])).naming(job.add.iter().cloned());
        let mut tx = self.node.local.begin().await?;
        let now_ms = self.node.time.now_ms();
        self.node
            .held
            .hold(&mut tx, &job.obj, advert, now_ms)
            .await?;
        tx.commit().await?;
        // the source may have added the object between the check and the park, finding
        // nothing to release: look again, now that the want is visible to it
        if let Some(part) = self.node.member_part(&job.obj, &job.add).await? {
            self.node.fetch_parked(&job.obj, &part).await?;
        }
        Ok(Settle::Done)
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if job.add.is_empty() && !job.remove.is_empty() {
            return Ok(());
        }
        let payload = NodeBackend::payload_of(&self.source, &job.obj).await?;
        self.node.land(&job.obj, payload).await
    }
}

// # The workload

#[derive(Debug, Clone, Copy)]
struct Plan {
    nodes: usize,
    parts: usize,
    objects: usize,
    rounds: usize,
    /// Writes per node per round, in one transaction.
    writes: usize,
    /// A node restarts every this many rounds (0: never).
    restart_every: usize,
    seed: u64,
}

/// What every node must hold once the cluster has converged.
#[derive(Clone, Default)]
struct Model {
    members: BTreeSet<(usize, usize)>,
    payloads: BTreeMap<usize, LwwPayload>,
    /// The last stamp issued: stamps are unique and increase in the order writes are issued.
    stamp: u64,
}

impl Model {
    fn expected(&self) -> BTreeMap<(ObjKey, PartKey), ObjPayload> {
        self.members
            .iter()
            .map(|&(at, of)| {
                (
                    (obj(at), part(of)),
                    self.payloads[&at].clone().into_payload(),
                )
            })
            .collect()
    }

    fn stamped(&mut self, at: usize, writer: usize, payload: &mut Vec<PartWrite>) {
        self.stamp += 1;
        let lww = LwwPayload {
            value: serde_json::json!(self.stamp),
            written_at: self.stamp,
            writer_id: node_key(writer),
        };
        payload.push(PartWrite::SetPayloads(vec![(
            obj(at),
            lww.clone().into_payload(),
        )]));
        self.payloads.insert(at, lww);
    }
}

struct Pull {
    driver: PeerDriver<Wire>,
    task: JoinHandle<()>,
}

struct Node {
    path: PathBuf,
    link: Arc<Link>,
    calls: Arc<AtomicU64>,
    /// By the node pulled from.
    pulls: BTreeMap<usize, Pull>,
    /// What the node's drivers share, rebuilt by a restart.
    backend: Option<Arc<NodeBackend>>,
    /// The membership changes the node's keyhive stream has yet to deliver, in order.
    inbox: VecDeque<Member>,
}

/// One change of the simulated keyhive stream: `(object, part, added)`.
type Member = (usize, usize, bool);

struct Cluster {
    plan: Plan,
    rng: StdRng,
    nodes: Vec<Node>,
    model: Model,
    facts: Facts,
    time: Time,
    /// Pulls `(from, at)` disconnected by the script.
    flapped: BTreeSet<(usize, usize)>,
    /// Keeps the files alive.
    _dir: tempfile::TempDir,
}

/// A look's nudge of the virtual clock.
const LOOK: Duration = Duration::from_millis(50);
/// The most looks a wait takes before the run calls the cluster stalled.
const MAX_LOOKS: usize = 5_000;

impl Cluster {
    async fn start(plan: Plan) -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let time = Time::new();
        let mut nodes = vec![];
        for at in 0..plan.nodes {
            let path = dir.path().join(format!("node-{at}.sqlite"));
            let serving = Serving::open(&path, &time).await?;
            let mut tx = serving.store.begin().await?;
            let parts = (0..plan.parts).map(part).collect::<Vec<_>>();
            serving.store.ensure_parts(&mut tx, &parts).await?;
            serving
                .store
                .grant_access(
                    &mut tx,
                    &parts
                        .iter()
                        .map(|part| (part.clone(), reader(), AccessLevel::Read))
                        .collect::<Vec<_>>(),
                )
                .await?;
            tx.commit().await?;
            let link = Arc::new(Link::default());
            link.set(Some(serving));
            nodes.push(Node {
                path,
                link,
                calls: default(),
                pulls: default(),
                backend: None,
                inbox: default(),
            });
        }
        let mut cluster = Self {
            plan,
            rng: StdRng::seed_from_u64(plan.seed),
            nodes,
            model: default(),
            facts: Facts::new(),
            time,
            flapped: default(),
            _dir: dir,
        };
        for at in 0..plan.nodes {
            cluster.start_pulls(at).await?;
            cluster.connect_pulls(at);
        }
        Ok(cluster)
    }

    /// One driver per other node, each routed every part. They connect when asked to.
    async fn start_pulls(&mut self, at: usize) -> Res<()> {
        let node = &self.nodes[at];
        let serving = node.link.serving().expect("the node is up");
        let links = (0..self.plan.nodes)
            .filter(|&from| from != at)
            .map(|from| (node_key(from), Arc::clone(&self.nodes[from].link)))
            .collect();
        let backend =
            Arc::new(NodeBackend::new(serving.store.clone(), links, self.time.clone()).await?);
        let mut pulls = BTreeMap::new();
        for from in (0..self.plan.nodes).filter(|&from| from != at) {
            let mut config = DriverConfig::new(node_key(from), BACKEND);
            config.min_backoff = Duration::from_millis(100);
            config.max_backoff = Duration::from_secs(2);
            let (driver, driver_loop) = PeerDriver::start(
                config,
                self.time.clock(),
                self.facts.clone(),
                LwwBackend {
                    node: Arc::clone(&backend),
                    peer: node_key(from),
                    source: Arc::clone(&self.nodes[from].link),
                    calls: Arc::clone(&node.calls),
                },
                serving.cursors.clone(),
                serving.store.clone(),
            );
            let task = tokio::spawn(driver_loop);
            driver
                .update_routes(RouteUpdate {
                    added_parts: (0..self.plan.parts).map(part).collect(),
                    ..default()
                })
                .await;
            pulls.insert(from, Pull { driver, task });
        }
        self.nodes[at].pulls = pulls;
        self.nodes[at].backend = Some(backend);
        Ok(())
    }

    fn connect_pull(&self, at: usize, from: usize) {
        self.nodes[at].pulls[&from].driver.connect(Wire {
            time: self.time.clone(),
            link: Arc::clone(&self.nodes[from].link),
        });
    }

    fn connect_pulls(&self, at: usize) {
        for &from in self.nodes[at].pulls.keys() {
            self.connect_pull(at, from);
        }
    }

    /// Let the drivers run: nudge the virtual clock and give the loops turns, and fail if a
    /// loop has ended (a driver loop only ends by panicking).
    async fn look(&self) {
        self.time.advance(LOOK).await;
        tokio::task::yield_now().await;
        for node in &self.nodes {
            for pull in node.pulls.values() {
                assert!(!pull.task.is_finished(), "a driver loop ended: it panicked");
            }
        }
    }

    async fn serving(&self, at: usize) -> Arc<Serving> {
        self.nodes[at].link.serving().expect("the node is up")
    }

    // # Writes

    /// One node's transaction of `plan.writes` writes, then the model's account of it. The
    /// node's own store takes the memberships it changes (its own keyhive), and every other
    /// node's keyhive stream is told, to deliver whenever it gets to it.
    async fn write_round(&mut self, at: usize, round: usize) -> Res<()> {
        let Plan {
            nodes,
            parts,
            objects,
            ..
        } = self.plan;
        let owned = (objects - at).div_ceil(nodes);
        // a churn object is removed and re-added by its owner alone
        let churn = |obj: usize| (obj / nodes).is_multiple_of(3);
        let serving = self.serving(at).await;
        let mut next = self.model.clone();
        let mut writes = vec![];
        let mut events = vec![];
        let ops = if round == 0 { owned } else { self.plan.writes };
        for op in 0..ops {
            // the first round adds every owned object to one part, in order
            let kind = if round == 0 {
                0
            } else {
                self.rng.random_range(0..12)
            };
            let own = if round == 0 {
                at + nodes * op
            } else {
                at + nodes * self.rng.random_range(0..owned)
            };
            match kind {
                // add (or re-add) to a part, with a fresh payload
                0..=3 => {
                    let of = self.rng.random_range(0..parts);
                    next.stamped(own, at, &mut writes);
                    writes.push(PartWrite::AddMembers(vec![(obj(own), part(of))]));
                    next.members.insert((own, of));
                    events.push((own, of, true));
                }
                // remove a churn object from every part, while the others sync it
                10..=11 => {
                    if churn(own) {
                        let held = next
                            .members
                            .range((own, 0)..(own + 1, 0))
                            .map(|&(_, of)| of)
                            .collect::<Vec<_>>();
                        for of in held {
                            writes.push(PartWrite::RemoveMembers(vec![(obj(own), part(of))]));
                            next.members.remove(&(own, of));
                            events.push((own, of, false));
                        }
                    }
                }
                // overwrite one's own member
                4..=5 => {
                    if next.members.range((own, 0)..(own + 1, 0)).next().is_some() {
                        next.stamped(own, at, &mut writes);
                    }
                }
                // overwrite somebody else's stable object, if this node has it
                _ => {
                    let other = self.rng.random_range(0..objects);
                    if other % nodes != at && !churn(other) {
                        let state = serving.store.obj_states(&[obj(other)]).await?.remove(0);
                        if state.is_some_and(|state| !state.parts.is_empty()) {
                            next.stamped(other, at, &mut writes);
                        }
                    }
                }
            }
        }
        let mut tx = serving.store.begin().await?;
        for write in writes {
            stage(&serving.store, &mut tx, write).await?;
        }
        tx.commit().await?;
        self.model = next;
        for (other, node) in self.nodes.iter_mut().enumerate() {
            if other != at {
                node.inbox.extend(events.iter().copied());
            }
        }
        Ok(())
    }

    /// The keyhive stream of node `at` delivers up to `limit` of its pending changes, in order.
    /// A change that fails (the peer a parked want is fetched from is down) stays first in line
    /// and is retried by the next delivery.
    async fn deliver(&mut self, at: usize, limit: usize) -> Res<()> {
        let backend = Arc::clone(self.nodes[at].backend.as_ref().expect("the node is up"));
        for _ in 0..limit {
            let Some(&(own, of, added)) = self.nodes[at].inbox.front() else {
                break;
            };
            let delivered = if added {
                backend.member_added(&obj(own), &part(of)).await
            } else {
                backend.member_removed(&obj(own), &part(of)).await
            };
            if delivered.is_err() {
                break;
            }
            self.nodes[at].inbox.pop_front();
        }
        Ok(())
    }

    /// Every node's keyhive stream delivers a random share of what it is behind on, so a
    /// membership reaches a node before, with or after the notification of its object.
    async fn deliver_some(&mut self) -> Res<()> {
        for at in 0..self.plan.nodes {
            let limit = self.rng.random_range(0..=self.nodes[at].inbox.len());
            self.deliver(at, limit).await?;
        }
        Ok(())
    }

    // # Churn

    /// Drop the connection of one pull, or bring a dropped one back.
    fn flap(&mut self) {
        let Plan { nodes, .. } = self.plan;
        let at = self.rng.random_range(0..nodes);
        let from = (at + 1 + self.rng.random_range(0..nodes - 1)) % nodes;
        if self.flapped.remove(&(from, at)) {
            self.connect_pull(at, from);
        } else {
            self.nodes[at].pulls[&from].driver.disconnect();
            self.flapped.insert((from, at));
        }
    }

    /// Kill a node mid-flight and bring it back from the same file.
    async fn restart(&mut self, at: usize) -> Res<()> {
        let nodes = self.plan.nodes;
        // Everybody stops using the node first: its callers' connections drop, and the node's
        // own drivers are aborted wherever they are, a transaction in flight included.
        for from in (0..nodes).filter(|&from| from != at) {
            self.nodes[from].pulls[&at].driver.disconnect();
        }
        for pull in std::mem::take(&mut self.nodes[at].pulls).into_values() {
            pull.task.abort();
            assert!(
                pull.task.await.expect_err("aborted").is_cancelled(),
                "a driver loop panicked"
            );
        }
        let old = Arc::downgrade(&self.serving(at).await);
        self.nodes[at].link.set(None);
        // the file is reopened only once nothing holds the old handle: a call in flight at a
        // responder or a fetch holds it until it ends
        for _ in 0..MAX_LOOKS {
            if old.strong_count() == 0 {
                break;
            }
            self.look().await;
        }
        assert_eq!(
            old.strong_count(),
            0,
            "the stopped node is still referenced"
        );

        let serving = Serving::open(&self.nodes[at].path, &self.time).await?;
        self.nodes[at].link.set(Some(serving));
        self.start_pulls(at).await?;
        self.connect_pulls(at);
        self.flapped.retain(|&(from, to)| from != at && to != at);
        for from in (0..nodes).filter(|&from| from != at) {
            self.connect_pull(from, at);
        }
        Ok(())
    }

    // # Judging

    async fn observe(&self, at: usize) -> Res<BTreeMap<(ObjKey, PartKey), ObjPayload>> {
        let serving = self.serving(at).await;
        let objs = (0..self.plan.objects).map(obj).collect::<Vec<_>>();
        let states = serving.store.obj_states(&objs).await?;
        let mut held = BTreeMap::new();
        for (obj, state) in objs.into_iter().zip(states) {
            // a payload with no live member is what a removal leaves behind
            if let Some(state) = state
                && let Some((payload, _)) = state.content
            {
                for part in state.parts {
                    held.insert((obj.clone(), part), payload.clone());
                }
            }
        }
        Ok(held)
    }

    async fn heads(&self) -> Res<Vec<Rev>> {
        let mut heads = vec![];
        for at in 0..self.plan.nodes {
            heads.push(RevSource::latest_rev(&self.serving(at).await.store).await?);
        }
        Ok(heads)
    }

    fn calls(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| node.calls.load(Ordering::SeqCst))
            .sum()
    }

    /// Bring every connection back and wait for every node to hold the model.
    async fn converge(&mut self) -> Res<()> {
        for (from, at) in std::mem::take(&mut self.flapped) {
            self.connect_pull(at, from);
        }
        // failures injected and not yet consumed would turn up as retried calls later
        for node in &self.nodes {
            node.link.fail_next.store(0, Ordering::SeqCst);
        }
        let expected = self.model.expected();
        let mut last = vec![];
        for _ in 0..MAX_LOOKS {
            last.clear();
            for at in 0..self.plan.nodes {
                self.deliver(at, usize::MAX).await?;
                last.push(self.observe(at).await?);
            }
            if last.iter().all(|held| *held == expected) {
                // The state has to hold through the echoes of the last writes (rows replayed
                // and found already applied), not just be seen once.
                for _ in 0..200 {
                    self.look().await;
                }
                let mut settled = true;
                for at in 0..self.plan.nodes {
                    settled &= self.observe(at).await? == expected;
                }
                if settled {
                    return Ok(());
                }
                continue;
            }
            self.look().await;
        }
        for (at, held) in last.iter().enumerate() {
            let missing = expected
                .keys()
                .filter(|key| !held.contains_key(*key))
                .count();
            let extra = held
                .keys()
                .filter(|key| !expected.contains_key(*key))
                .count();
            let stale = expected
                .iter()
                .filter(|(key, payload)| held.get(*key).is_some_and(|held| held != *payload))
                .count();
            eprintln!(
                "node {at}: {} members of {} expected; {missing} missing, {extra} extra, {stale} with another payload",
                held.len(),
                expected.len()
            );
        }
        for (at, held) in last.iter().enumerate() {
            let missing = expected
                .keys()
                .filter(|key| !held.contains_key(*key))
                .count();
            let extra = held
                .keys()
                .filter(|key| !expected.contains_key(*key))
                .count();
            let stale = expected
                .iter()
                .filter(|(key, payload)| held.get(*key).is_some_and(|held| held != *payload))
                .count();
            eprintln!(
                "node {at}: {} members of {} expected; {missing} missing, {extra} extra, {stale} with another payload",
                held.len(),
                expected.len()
            );
        }
        panic!("the cluster did not converge in {MAX_LOOKS} looks");
    }

    /// The stores agree beyond their members: each part's member count and root bucket (live
    /// count and fingerprint) are the same everywhere.
    async fn assert_aligned(&self) -> Res<()> {
        let mut per_node = vec![];
        for at in 0..self.plan.nodes {
            let store = self.serving(at).await.store.clone();
            let mut parts = vec![];
            for of in 0..self.plan.parts {
                let counted = store.member_count(&part(of)).await?;
                let root = store
                    .bucket_summaries(&part(of), &[BuckId::ROOT])
                    .await?
                    .remove(0);
                assert_eq!(counted, u64::from(root.live_count), "node {at} part {of}");
                parts.push((counted, root.fp));
            }
            per_node.push(parts);
        }
        for (at, parts) in per_node.iter().enumerate() {
            assert_eq!(parts, &per_node[0], "node {at} disagrees with node 0");
        }
        let expected = self.model.expected();
        for (of, (counted, _)) in per_node[0].iter().enumerate() {
            let want = expected
                .keys()
                .filter(|(_, part_key)| *part_key == part(of))
                .count();
            assert_eq!(*counted, want as u64, "part {of}");
        }
        Ok(())
    }

    /// Nothing commits and no backend is called from now on, over far more virtual time than
    /// any hold or backoff lasts.
    async fn assert_quiet(&self) -> Res<()> {
        // echoes of the last writes (rows replayed and found already applied) settle first
        for _ in 0..200 {
            self.look().await;
        }
        let (heads, calls) = (self.heads().await?, self.calls());
        for _ in 0..600 {
            self.time.advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(self.heads().await?, heads, "the nodes keep committing");
        }
        assert_eq!(
            self.calls(),
            calls,
            "the drivers keep calling their backends"
        );
        Ok(())
    }

    /// The peer cursors of one node are in its file: after a final restart of every node the
    /// drivers resume from them and find nothing to do.
    async fn assert_cursors_persisted(&self) -> Res<()> {
        for at in 0..self.plan.nodes {
            let serving = self.serving(at).await;
            for from in (0..self.plan.nodes).filter(|&from| from != at) {
                let parts = (0..self.plan.parts).map(part).collect::<Vec<_>>();
                let cursors = serving
                    .cursors
                    .read(&node_key(from), BACKEND, &parts)
                    .await?;
                assert!(
                    cursors.iter().all(Option::is_some),
                    "node {at} never persisted its cursor on node {from}"
                );
            }
        }
        Ok(())
    }
}

async fn run(plan: Plan) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut cluster = Cluster::start(plan).await?;
    for round in 0..=plan.rounds {
        for at in 0..plan.nodes {
            cluster.write_round(at, round).await?;
        }
        if round == 0 {
            continue;
        }
        if cluster.rng.random_range(0..3) == 0 {
            cluster.flap();
        }
        if cluster.rng.random_range(0..4) == 0 {
            let at = cluster.rng.random_range(0..plan.nodes);
            cluster.nodes[at].link.fail_next.store(3, Ordering::SeqCst);
        }
        if plan.restart_every != 0 && round % plan.restart_every == 0 {
            let at = cluster.rng.random_range(0..plan.nodes);
            cluster.restart(at).await?;
        }
        for _ in 0..cluster.rng.random_range(1..8) {
            cluster.look().await;
            cluster.deliver_some().await?;
        }
    }
    // Removals and re-adds interleave with the sync all along: a peer's stale echo of an old
    // add is only a notification, and cannot resurrect what the node's own keyhive removed.
    cluster.converge().await?;
    cluster.assert_aligned().await?;
    cluster.assert_cursors_persisted().await?;
    cluster.assert_quiet().await?;

    // one more restart of every node, with everything settled: they reopen their files, resume
    // from their persisted cursors, and the cluster stays put
    for at in 0..plan.nodes {
        cluster.restart(at).await?;
    }
    cluster.converge().await?;
    cluster.assert_aligned().await?;
    cluster.assert_quiet().await?;
    Ok(())
}

fn seeds(default_count: u64) -> std::ops::Range<u64> {
    let first = std::env::var("AUTOMATA_SQLITE_STRESS_SEED")
        .ok()
        .map(|seed| seed.parse::<u64>().expect("the seed is an integer"));
    match first {
        Some(seed) => {
            let count = std::env::var("AUTOMATA_SQLITE_STRESS_SEEDS")
                .ok()
                .map_or(1, |count| {
                    count.parse::<u64>().expect("the count is an integer")
                });
            seed..seed + count
        }
        None => 0..default_count,
    }
}

/// Two nodes pulling from each other, with writes, flapping connections, failing calls and
/// restarts: sized for every CI run.
#[tokio::test(start_paused = true)]
async fn stress_two_nodes_converge_and_go_quiet() -> Res<()> {
    for seed in seeds(2) {
        run(Plan {
            nodes: 2,
            parts: 3,
            objects: 60,
            rounds: 10,
            writes: 12,
            restart_every: 4,
            seed,
        })
        .await?;
    }
    Ok(())
}

/// Four nodes, every one pulling from every other, with the same churn: sized for every CI run.
#[tokio::test(start_paused = true)]
async fn stress_four_nodes_converge_and_go_quiet() -> Res<()> {
    for seed in seeds(2) {
        run(Plan {
            nodes: 4,
            parts: 3,
            objects: 80,
            rounds: 10,
            writes: 12,
            restart_every: 4,
            seed,
        })
        .await?;
    }
    Ok(())
}

/// The same run, larger and longer: six nodes, thousands of objects, many seeds.
///
/// Not part of the default run. Run it with
/// `cargo nextest run -p big_automata_sqlite --run-ignored only stress_heavy`; set
/// `AUTOMATA_SQLITE_STRESS_SEED` (and `AUTOMATA_SQLITE_STRESS_SEEDS`, the count) to replay
/// or sweep seeds.
#[tokio::test(start_paused = true)]
#[ignore = "heavy: run with --run-ignored only stress_heavy"]
async fn stress_heavy_six_nodes_many_seeds() -> Res<()> {
    for seed in seeds(8) {
        run(Plan {
            nodes: 6,
            parts: 4,
            objects: 3000,
            rounds: 60,
            writes: 100,
            restart_every: 5,
            seed,
        })
        .await?;
    }
    Ok(())
}
