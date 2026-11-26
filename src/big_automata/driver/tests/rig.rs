//! The test rig: a scripted peer (the real responders over a memory store, behind the
//! `BigSyncRpcClient` the driver calls), a scripted backend, and a driver started over them.
//!
//! Everything runs on tokio's paused clock: sleeps (backoffs, held rounds) cost no real time,
//! and the machine's clock is the same virtual one.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::interlude::*;

use crate::Clock;
use crate::driver::DriverConfig;
use crate::driver::Fact;
use crate::driver::Facts;
use crate::driver::ObjJob;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::Settle;
use crate::driver::SyncBackend;
use crate::driver::memory::LwwPayload;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::long_poll::RuntimeTimer;
use crate::part_store::PartStoreRead;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::laws::stage;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::replay::responder::Responder;
use crate::replay::server::ReplayServer;
use crate::replay::wire::CloseSession;
use crate::replay::wire::PageSession;
use crate::replay::wire::Route;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::UpdateSession;
use crate::sync::rpc::BigSyncRpcClient;
use crate::sync::rpc::BigSyncRpcResult;
use crate::sync::rpc::RpcError;
use crate::sync::rpc::bucket::BucketsError;
use crate::sync::rpc::bucket::GetChangedBucketsRequest;
use crate::sync::rpc::bucket::GetChangedBucketsResponse;
use crate::sync::rpc::bucket::LeafBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsResponse;
use crate::sync::rpc::responder::RpcResponder;
use crate::sync::rpc::summary::PartSummariesError;
use crate::sync::rpc::summary::PartSummariesRequest;
use crate::sync::rpc::summary::PartSummariesResponse;

/// The principal our driver acts as at every scripted peer.
fn me() -> PeerKey {
    principal(0)
}

pub(super) fn clock() -> Clock {
    Arc::new(|| tokio::time::Instant::now().into_std())
}

/// Wait, in virtual time, until `cond` holds.
pub(super) async fn until(mut cond: impl FnMut() -> bool) {
    within(async {
        while !cond() {
            n0_future::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// A virtual-time bound on a wait, so a stalled driver fails the test instead of hanging it. It
/// costs no real time and is not a latency claim.
pub(super) async fn within<T>(wait: impl Future<Output = T>) -> T {
    n0_future::time::timeout(Duration::from_secs(3600), wait)
        .await
        .expect("the driver stalled: the wait did not end in an hour of virtual time")
}

#[derive(Default)]
struct PeerLog {
    updates: Vec<UpdateSession>,
    rounds: Vec<PageSession>,
}

struct PeerInner {
    store: MemoryPartStore,
    rpc: RpcResponder<MemoryPartStore>,
    server: Mutex<ReplayServer>,
    /// While set, every call fails as a transport failure.
    down: AtomicBool,
    /// The next this many calls fail as transport failures.
    fail_next: AtomicUsize,
    log: Mutex<PeerLog>,
    /// What `publish` made members, in order.
    published: Mutex<Vec<(u8, u8)>>,
    /// The local stores whose membership source follows this peer's: the simulated local
    /// keyhive stream. A peer's job never changes membership, so a test that expects an
    /// object to land says so here, by making the local store a follower.
    followers: Mutex<Vec<MemoryPartStore>>,
}

/// A peer's side of the transport: the real replay and sync responders over a memory store, as
/// `big_automata_iroh` binds them, called in process. Cloning shares the peer.
#[derive(Clone)]
pub(super) struct ScriptedPeer {
    inner: Arc<PeerInner>,
}

impl ScriptedPeer {
    pub(super) fn new() -> Self {
        let store = MemoryPartStore::default();
        Self {
            inner: Arc::new(PeerInner {
                rpc: RpcResponder::new(store.clone()),
                store,
                server: default(),
                down: AtomicBool::new(false),
                fail_next: AtomicUsize::new(0),
                log: default(),
                published: default(),
                followers: default(),
            }),
        }
    }

    pub(super) fn store(&self) -> &MemoryPartStore {
        &self.inner.store
    }

    /// `local`'s membership follows what this peer publishes, from now on and for everything
    /// published so far: the local keyhive stream agrees with the group.
    pub(super) fn follow(&self, local: &MemoryPartStore) {
        let published = self.inner.published.lock().unwrap().clone();
        futures::executor::block_on(Self::deliver(local, &published));
        self.inner.followers.lock().unwrap().push(local.clone());
    }

    async fn deliver(local: &MemoryPartStore, members: &[(u8, u8)]) {
        let mut tx = local.begin();
        for &(at, index) in members {
            stage(
                local,
                &mut tx,
                PartWrite::AddMembers(vec![(obj(at), part(index))]),
            )
            .await
            .expect("stage");
        }
        tx.commit();
    }

    pub(super) async fn commit(&self, writes: Vec<PartWrite>) -> Option<u64> {
        let store = &self.inner.store;
        let mut tx = store.begin();
        for write in writes {
            stage(store, &mut tx, write).await.expect("stage");
        }
        tx.commit()
    }

    /// Part `index` readable by us, with objects `objs` as members at payload `value`,
    /// all committed in one transaction so they share one rev.
    pub(super) async fn publish(&self, index: u8, objs: &[u8], value: u8) {
        use crate::part_store::laws::add;
        use crate::part_store::laws::grant;
        self.commit(vec![grant(&[(index, 0)])]).await;
        let sets: Vec<_> = objs.iter().map(|obj| (*obj, value)).collect();
        let adds: Vec<_> = objs.iter().map(|obj| (*obj, index)).collect();
        self.commit(vec![LwwPayload::set(&sets), add(&adds)]).await;
        self.inner
            .published
            .lock()
            .unwrap()
            .extend(adds.iter().copied());
        let followers = self.inner.followers.lock().unwrap().clone();
        for local in followers {
            Self::deliver(&local, &adds).await;
        }
    }

    pub(super) fn set_down(&self, down: bool) {
        self.inner.down.store(down, Ordering::SeqCst);
    }

    pub(super) fn fail_next(&self, calls: usize) {
        self.inner.fail_next.store(calls, Ordering::SeqCst);
    }

    pub(super) fn updates(&self) -> Vec<UpdateSession> {
        self.inner.log.lock().unwrap().updates.clone()
    }

    /// The cursor each part's route was first asked from in `session`'s rounds.
    pub(super) fn first_round_cursors(
        &self,
        session: crate::replay::wire::SessionId,
    ) -> BTreeMap<PartKey, u64> {
        let log = self.inner.log.lock().unwrap();
        let mut routes = BTreeMap::new();
        for update in log.updates.iter().filter(|u| u.session_id == session) {
            routes.extend(update.additions.iter().cloned());
        }
        let mut first = BTreeMap::new();
        for round in log.rounds.iter().filter(|r| r.session_id == session) {
            for (id, cursor) in &round.targets {
                if let Some(Route::Part { part_key }) = routes.get(id) {
                    first.entry(part_key.clone()).or_insert(*cursor);
                }
            }
        }
        first
    }

    fn gate(&self) -> BigSyncRpcResult<()> {
        let failing = self.inner.down.load(Ordering::SeqCst)
            || self
                .inner
                .fail_next
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
        if failing {
            return Err(RpcError::TransportError("scripted failure".into()));
        }
        Ok(())
    }

    fn responder(&self) -> Responder<MemoryPartStore, RuntimeTimer> {
        Responder::new(self.inner.store.clone(), RuntimeTimer)
    }

    fn server(&self) -> std::sync::MutexGuard<'_, ReplayServer> {
        self.inner.server.lock().unwrap()
    }
}

impl BigSyncRpcClient for ScriptedPeer {
    async fn part_summaries(
        &self,
        req: PartSummariesRequest,
    ) -> BigSyncRpcResult<Result<PartSummariesResponse, PartSummariesError>> {
        self.gate()?;
        Ok(self
            .inner
            .rpc
            .part_summaries(&me(), req)
            .await
            .expect("store"))
    }

    async fn update_replay_session(
        &self,
        req: UpdateSession,
    ) -> BigSyncRpcResult<Result<SessionUpdated, SessionError>> {
        self.gate()?;
        self.inner.log.lock().unwrap().updates.push(req.clone());
        let mut responder = self.responder();
        let admits = responder.admission(&me(), &req).await.expect("store");
        Ok(self
            .server()
            .update(&me(), &req, std::time::Instant::now(), admits))
    }

    async fn page_replay_session(
        &self,
        req: PageSession,
    ) -> BigSyncRpcResult<Result<SessionPage, SessionError>> {
        self.gate()?;
        self.inner.log.lock().unwrap().rounds.push(req.clone());
        let mut responder = self.responder();
        let page = responder
            .page(&me(), |round| {
                self.server()
                    .resolve_round(&me(), &req, std::time::Instant::now(), round)
            })
            .await
            .expect("store")
            .cloned();
        Ok(page)
    }

    async fn close_replay_session(
        &self,
        req: CloseSession,
    ) -> BigSyncRpcResult<Result<(), SessionError>> {
        self.gate()?;
        self.server().close(&me(), &req);
        Ok(Ok(()))
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> BigSyncRpcResult<Result<GetChangedBucketsResponse, BucketsError>> {
        self.gate()?;
        Ok(self
            .inner
            .rpc
            .changed_buckets(&me(), req)
            .await
            .expect("store"))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> BigSyncRpcResult<Result<LeafBucketsResponse, BucketsError>> {
        self.gate()?;
        Ok(self
            .inner
            .rpc
            .leaf_buckets(&me(), req)
            .await
            .expect("store"))
    }
}

// # The backend

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Record,
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BackendEvent {
    Started(Kind, ObjKey),
    Finished(Kind, ObjKey),
    /// The future was dropped before it finished: the driver cancelled it.
    Dropped(Kind, ObjKey),
}

#[derive(Default)]
struct BackendState {
    events: Vec<BackendEvent>,
    /// Calls running now, and the most there ever were at once, per kind.
    running: [usize; 2],
    peak: [usize; 2],
    /// Objects with a call running now, and the most there ever were at once.
    running_objs: BTreeMap<ObjKey, usize>,
    peak_objs: usize,
}

struct BackendInner {
    memory: MemoryBackend,
    state: Mutex<BackendState>,
    /// While set, every fetch waits.
    hold_fetches: tokio::sync::watch::Sender<bool>,
    fail_records: AtomicUsize,
}

/// A [`MemoryBackend`] that logs every call it gets and how it ended, and can be told to hold
/// its fetches and to fail its next records. Cloning shares it.
#[derive(Clone)]
pub(super) struct ScriptedBackend {
    inner: Arc<BackendInner>,
}

/// One call's presence in the log: dropping it unfinished is a cancel.
struct Running<'a> {
    backend: &'a BackendInner,
    kind: Kind,
    obj: ObjKey,
    finished: bool,
}

impl BackendInner {
    fn start(&self, kind: Kind, obj: ObjKey) -> Running<'_> {
        let mut state = self.state.lock().unwrap();
        state.events.push(BackendEvent::Started(kind, obj.clone()));
        state.running[kind as usize] += 1;
        state.peak[kind as usize] = state.peak[kind as usize].max(state.running[kind as usize]);
        *state.running_objs.entry(obj.clone()).or_default() += 1;
        state.peak_objs = state.peak_objs.max(state.running_objs.len());
        Running {
            backend: self,
            kind,
            obj,
            finished: false,
        }
    }
}

impl Running<'_> {
    fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        let mut state = self.backend.state.lock().unwrap();
        state.events.push(if self.finished {
            BackendEvent::Finished(self.kind, self.obj.clone())
        } else {
            BackendEvent::Dropped(self.kind, self.obj.clone())
        });
        state.running[self.kind as usize] -= 1;
        let count = state.running_objs.get_mut(&self.obj).expect("counted");
        *count -= 1;
        if *count == 0 {
            state.running_objs.remove(&self.obj);
        }
    }
}

impl ScriptedBackend {
    pub(super) fn new(local: MemoryPartStore, source: MemoryPartStore) -> Self {
        Self {
            inner: Arc::new(BackendInner {
                memory: MemoryBackend::new(PeerKey::new(&[1]), local, source),
                state: default(),
                hold_fetches: tokio::sync::watch::channel(false).0,
                fail_records: AtomicUsize::new(0),
            }),
        }
    }

    pub(super) fn hold_fetches(&self, hold: bool) {
        self.inner.hold_fetches.send_replace(hold);
    }

    pub(super) fn fail_records(&self, calls: usize) {
        self.inner.fail_records.store(calls, Ordering::SeqCst);
    }

    pub(super) fn events(&self) -> Vec<BackendEvent> {
        self.inner.state.lock().unwrap().events.clone()
    }

    pub(super) fn count(&self, wanted: &BackendEvent) -> usize {
        self.events()
            .iter()
            .filter(|event| *event == wanted)
            .count()
    }

    /// The most calls of `kind` that ran at once.
    pub(super) fn peak(&self, kind: Kind) -> usize {
        self.inner.state.lock().unwrap().peak[kind as usize]
    }

    /// The most objects with a call running at once.
    pub(super) fn peak_objs(&self) -> usize {
        self.inner.state.lock().unwrap().peak_objs
    }

    /// Wait until a call of `kind` for `obj` is running.
    pub(super) async fn running(&self, kind: Kind, obj: &ObjKey) {
        while !self
            .events()
            .contains(&BackendEvent::Started(kind, obj.clone()))
        {
            tokio::task::yield_now().await;
        }
    }
}

impl SyncBackend for ScriptedBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        let running = self.inner.start(Kind::Record, job.obj.clone());
        // let the loop poll something else first, as a real write would
        tokio::task::yield_now().await;
        let failed = self
            .inner
            .fail_records
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if failed {
            return Err(eyre::eyre!("scripted record failure"));
        }
        let settle = self.inner.memory.record(job).await?;
        running.finish();
        Ok(settle)
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        let running = self.inner.start(Kind::Fetch, job.obj.clone());
        self.inner
            .hold_fetches
            .subscribe()
            .wait_for(|held| !*held)
            .await
            .expect("the backend holds its sender");
        self.inner.memory.fetch(job).await?;
        running.finish();
        Ok(())
    }
}

// # The rig

pub(super) const LOCAL_PEER: u8 = 9;

/// One driver over one scripted peer, with everything a test reads back.
pub(super) struct Rig {
    pub peer: ScriptedPeer,
    pub local: MemoryPartStore,
    pub backend: ScriptedBackend,
    pub cursors: MemoryPeerCursors,
    pub facts: Facts,
    pub driver: PeerDriver<ScriptedPeer>,
    pub config: DriverConfig,
}

impl Rig {
    pub(super) fn new() -> Self {
        Self::with(Facts::new(), LOCAL_PEER, |_| {})
    }

    /// A rig whose peer is key `peer`, publishing to `facts`.
    pub(super) fn with(facts: Facts, peer: u8, tune: impl FnOnce(&mut DriverConfig)) -> Self {
        let scripted = ScriptedPeer::new();
        let local = MemoryPartStore::default();
        let backend = ScriptedBackend::new(local.clone(), scripted.store().clone());
        scripted.follow(&local);
        let cursors = MemoryPeerCursors::default();
        let mut config = DriverConfig::new(principal(peer), "memory");
        config.window = 8;
        config.fetch_cap = 2;
        config.min_backoff = Duration::from_secs(2);
        config.max_backoff = Duration::from_secs(8);
        tune(&mut config);
        Self::start(facts, scripted, local, backend, cursors, config)
    }

    pub(super) fn start(
        facts: Facts,
        peer: ScriptedPeer,
        local: MemoryPartStore,
        backend: ScriptedBackend,
        cursors: MemoryPeerCursors,
        config: DriverConfig,
    ) -> Self {
        let (driver, driver_loop) = PeerDriver::start(
            config.clone(),
            clock(),
            facts.clone(),
            backend.clone(),
            cursors.clone(),
            local.clone(),
        );
        tokio::spawn(driver_loop);
        Self {
            peer,
            local,
            backend,
            cursors,
            facts,
            driver,
            config,
        }
    }

    pub(super) fn peer_key(&self) -> PeerKey {
        self.config.peer.clone()
    }

    pub(super) async fn want(&self, parts: &[u8]) {
        self.driver
            .update_routes(RouteUpdate {
                added_parts: parts.iter().map(|p| part(*p)).collect(),
                ..default()
            })
            .await;
    }

    pub(super) async fn unwant(&self, parts: &[u8]) {
        self.driver
            .update_routes(RouteUpdate {
                removed_parts: parts.iter().map(|p| part(*p)).collect(),
                ..default()
            })
            .await;
    }

    pub(super) fn connect(&self) {
        self.driver.connect(self.peer.clone());
    }

    /// Wait until this peer's `part` has `fact`, judged on a fresh subscription.
    pub(super) async fn fact(&self, index: u8, fact: Fact) {
        let mut stream = self.facts.subscribe();
        loop {
            let event = within(stream.next()).await;
            if event.peer == self.config.peer && event.part == part(index) && event.fact == fact {
                return;
            }
        }
    }

    pub(super) async fn synced(&self, index: u8) {
        self.fact(index, Fact::Synced).await;
    }

    /// The live members of `index` in `store` among objects `1..=max`, with their payloads.
    pub(super) async fn members(
        store: &MemoryPartStore,
        index: u8,
        max: u8,
    ) -> BTreeMap<u8, serde_json::Value> {
        let objs: Vec<_> = (1..=max).map(obj).collect();
        let states = store.obj_states(&objs).await.expect("states");
        (1..=max)
            .zip(states)
            .filter_map(|(at, state)| {
                let (payload, _) = state.as_ref()?.content.clone()?;
                state?.parts.contains(&part(index)).then_some((at, payload))
            })
            .collect()
    }

    /// The local store holds exactly what the peer does for `index`.
    pub(super) async fn assert_converged(&self, index: u8, max: u8) {
        let local = Self::members(&self.local, index, max).await;
        let remote = Self::members(self.peer.store(), index, max).await;
        assert_eq!(
            local, remote,
            "part {index}: local view differs from the peer's"
        );
    }
}
