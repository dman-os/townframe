//! The loop: one task per (peer, backend) that owns the machine and runs its commands.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::AbortHandle;
use futures::future::abortable;
use futures::stream::FuturesUnordered;
use n0_future::boxed::BoxFuture;

use super::Fact;
use super::Facts;
use super::PeerCursors;
use super::SyncBackend;
use crate::AllowedStrat;
use crate::Clock;
use crate::fingerprint::FingerprintSeed;
use crate::part_store::PartStoreRead;
use crate::part_store::bucket_summary::BucketSummary;
use crate::replay::server::MAX_HOLD;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::rev::Rev;
use crate::sync::bucket::BucketMachine;
use crate::sync::bucket::BucketObjLeafPage;
use crate::sync::bucket::FilteredBuckets;
use crate::sync::bucket::filter_buckets;
use crate::sync::bucket::filter_objects;
use crate::sync::lifecycle;
use crate::sync::object_jobs;
use crate::sync::object_jobs::Outcome;
use crate::sync::object_jobs::Settle;
use crate::sync::peer::CallId;
use crate::sync::peer::PeerCommand;
use crate::sync::peer::PeerSync;
use crate::sync::peer::Reply;
use crate::sync::peer::SummaryReply;
use crate::sync::rpc::BigSyncRpcClient;
use crate::sync::rpc::MAX_SUMMARY_PARTS;
use crate::sync::rpc::bucket::GetChangedBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsRequest;
use crate::sync::rpc::summary::PartSummariesError;
use crate::sync::rpc::summary::PartSummariesRequest;

/// What one driver is: whose parts it pulls, for which backend, and how hard.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// The peer pulled from; keys its cursors and its facts.
    pub peer: PeerKey,
    /// Names the backend in the peer-cursor table: a peer with two backends has two drivers
    /// whose cursors must not meet.
    pub backend: String,
    /// The receive window: unsettled objects admitted before the next replay round waits. At
    /// least 3 (the machine needs room for a `Live` and a `Bulk` round beside a walk).
    pub window: usize,
    /// Fetches in flight at once.
    pub fetch_cap: usize,
    pub strat: AllowedStrat,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

impl DriverConfig {
    pub fn new(peer: PeerKey, backend: impl Into<String>) -> Self {
        Self {
            peer,
            backend: backend.into(),
            window: 64,
            fetch_cap: 4,
            strat: AllowedStrat::default(),
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }

    fn machine(&self) -> crate::sync::peer::Config {
        let backoffs = (self.min_backoff, self.max_backoff);
        crate::sync::peer::Config {
            lifecycle: lifecycle::Config {
                max_parts: MAX_SUMMARY_PARTS,
                min_backoff: backoffs.0,
                max_backoff: backoffs.1,
            },
            jobs: object_jobs::Config {
                window: self.window,
                fetch_cap: self.fetch_cap,
                min_backoff: backoffs.0,
                max_backoff: backoffs.1,
            },
            lanes: crate::replay::lanes::Config {
                hold_ms: u32::try_from(MAX_HOLD.as_millis()).expect(ERROR_IMPOSSIBLE),
                // a held `Live` round takes at most a quarter of the window, which leaves the
                // machine's requirement that the window exceed twice that
                live_limit: (self.window / 4).max(1),
                min_backoff: backoffs.0,
                max_backoff: backoffs.1,
            },
            strat: self.strat,
            min_backoff: backoffs.0,
            max_backoff: backoffs.1,
        }
    }
}

/// A change of the routes wanted from the peer, incremental as `PeerSync` law 1: nothing is
/// diffed, adding a wanted route or removing an unwanted one is a no-op, and removals apply
/// before additions.
#[derive(Debug, Clone, Default)]
pub struct RouteUpdate {
    pub added_parts: Vec<PartKey>,
    pub removed_parts: Vec<PartKey>,
    pub added_objects: Vec<ObjKey>,
    pub removed_objects: Vec<ObjKey>,
}

enum Input<C> {
    /// With the channel that answers once the update is applied.
    Routes(RouteUpdate, async_channel::Sender<()>),
    Connect(C),
    Disconnect,
}

/// The handle of a running driver. Clones share the driver; it ends, with a `Removed` fact for
/// each of its parts, when the last one is dropped.
pub struct PeerDriver<C> {
    inputs: async_channel::Sender<Input<C>>,
}

impl<C> Clone for PeerDriver<C> {
    fn clone(&self) -> Self {
        Self {
            inputs: self.inputs.clone(),
        }
    }
}

impl<C> PeerDriver<C>
where
    C: BigSyncRpcClient + Send + Sync + 'static,
{
    /// Start the loop. Spawns through [`n0_future::task::spawn`]: on a native host this runs
    /// inside a tokio runtime, in the browser it is a `wasm-bindgen-futures` task.
    ///
    /// The driver is idle until [`connect`](Self::connect). `local` is the part store the
    /// backend writes: bucket walks filter the peer's listings against it.
    pub fn spawn<B, K, S>(
        config: DriverConfig,
        clock: Clock,
        facts: Facts,
        backend: B,
        cursors: K,
        local: S,
    ) -> Self
    where
        B: SyncBackend,
        K: PeerCursors,
        S: PartStoreRead + Send + Sync + 'static,
    {
        let (inputs, receiver) = async_channel::unbounded();
        let driver = Driver {
            config,
            clock,
            facts,
            backend: Arc::new(backend),
            cursors,
            local: Arc::new(local),
            routes: default(),
            conn: None,
        };
        // Detached on purpose: the loop ends when the last handle drops its sender. Its only
        // failures are the ones that crash it (a failed table or local read, a bug), and the
        // process panic hook surfaces a panic.
        drop(n0_future::task::spawn(driver.run(receiver)));
        Self { inputs }
    }

    fn send(&self, input: Input<C>) {
        // This handle keeps the channel open, so the loop only goes away by panicking.
        self.inputs
            .try_send(input)
            .expect("the driver loop outlives its handles");
    }

    /// Resolves once the loop has applied the update: its parts are facts from then on
    /// (`Unsynced`, or `Removed`), so a wait asked for right after sees them. The update
    /// applies even if this future is dropped first.
    pub async fn update_routes(&self, update: RouteUpdate) {
        let (applied, done) = async_channel::bounded(1);
        self.send(Input::Routes(update, applied));
        done.recv()
            .await
            .expect("the driver loop answers every update it takes");
    }

    /// A connection to the peer is up: a fresh machine pulls the current routes from the
    /// cursors persisted for them. A connection still up is ended first.
    pub fn connect(&self, client: C) {
        self.send(Input::Connect(client));
    }

    /// The connection is lost. Every call in flight is dropped and every part is `Unsynced`;
    /// only the persisted cursors survive.
    pub fn disconnect(&self) {
        self.send(Input::Disconnect);
    }
}

/// The wanted routes, kept to be re-added on a new connection (law 7).
#[derive(Default)]
struct Routes {
    parts: BTreeSet<PartKey>,
    objects: BTreeSet<ObjKey>,
}

/// What a call came back with, for the machine input that names its kind.
enum Done {
    Summary(SummaryReply),
    Session(Reply<Result<SessionUpdated, SessionError>>),
    Round(Reply<Result<SessionPage, SessionError>>),
    List(Reply<Vec<BucketSummary>>),
    Leaf(Reply<Map<BuckId, BucketObjLeafPage>>),
    Record(Outcome<Settle>),
    Fetch(Outcome<()>),
}

impl Done {
    /// What an aborted call reports (law 2): `Cancelled` for backend work, `Lost` for the
    /// rest. Only backend calls are aborted while their machine lives.
    fn aborted(cmd: &PeerCommand) -> Self {
        match cmd {
            PeerCommand::Summary { .. } => Self::Summary(SummaryReply::Lost),
            PeerCommand::Session { .. } => Self::Session(Reply::Lost),
            PeerCommand::Round { .. } => Self::Round(Reply::Lost),
            PeerCommand::ListBuckets { .. } => Self::List(Reply::Lost),
            PeerCommand::LeafBuckets { .. } => Self::Leaf(Reply::Lost),
            PeerCommand::Record { .. } => Self::Record(Outcome::Cancelled),
            PeerCommand::Fetch { .. } => Self::Fetch(Outcome::Cancelled),
            _ => unreachable!("not a call: {cmd:?}"),
        }
    }
}

/// What the calls of one connection run against.
struct Env<C, B, S> {
    client: C,
    backend: Arc<B>,
    local: Arc<S>,
}

/// One connection: the machine and the calls it has in flight. The calls are futures in this
/// loop's own set, so nothing runs while the loop is not polled and dropping the connection
/// cancels them all.
struct Conn<C, B, S> {
    sync: PeerSync,
    env: Arc<Env<C, B, S>>,
    calls: FuturesUnordered<BoxFuture<(CallId, Done)>>,
    aborts: BTreeMap<CallId, AbortHandle>,
}

enum Event<C> {
    Input(Option<Input<C>>),
    Done(CallId, Done),
    Timer,
}

struct Driver<C, B, K, S> {
    config: DriverConfig,
    clock: Clock,
    facts: Facts,
    backend: Arc<B>,
    cursors: K,
    local: Arc<S>,
    routes: Routes,
    conn: Option<Conn<C, B, S>>,
}

impl<C, B, K, S> Driver<C, B, K, S>
where
    C: BigSyncRpcClient + Send + Sync + 'static,
    B: SyncBackend,
    K: PeerCursors,
    S: PartStoreRead + Send + Sync + 'static,
{
    /// Law 1: every input reaches the machine from here, one at a time, and the machine is
    /// never borrowed across an await.
    async fn run(mut self, inputs: async_channel::Receiver<Input<C>>) {
        loop {
            self.flush().await;
            match self.next_event(&inputs).await {
                Event::Input(Some(input)) => self.input(input).await,
                // every handle dropped
                Event::Input(None) => break,
                Event::Done(call, done) => self.feed(call, done),
                Event::Timer => {}
            }
        }
        self.disconnect();
        for part in std::mem::take(&mut self.routes.parts) {
            self.facts.publish(&self.config.peer, &part, Fact::Removed);
        }
    }

    /// The first of: a call's outcome, an input, the machine's `next_due` (law 4).
    async fn next_event(&mut self, inputs: &async_channel::Receiver<Input<C>>) -> Event<C> {
        let now = (self.clock)();
        let due = self.conn.as_ref().and_then(|conn| conn.sync.next_due());
        let call = async {
            match self.conn.as_mut() {
                Some(conn) if !conn.calls.is_empty() => {
                    conn.calls.next().await.expect("a non-empty set yields")
                }
                _ => std::future::pending().await,
            }
        }
        .fuse();
        let input = inputs.recv().fuse();
        let timer = async {
            match due {
                Some(at) => n0_future::time::sleep(at.saturating_duration_since(now)).await,
                None => std::future::pending().await,
            }
        }
        .fuse();
        futures::pin_mut!(call, input, timer);
        futures::select_biased! {
            (call, done) = call => Event::Done(call, done),
            input = input => Event::Input(input.ok()),
            () = timer => Event::Timer,
        }
    }

    async fn input(&mut self, input: Input<C>) {
        match input {
            Input::Routes(update, applied) => {
                self.update_routes(update).await;
                // the caller may have stopped waiting: the update stands either way
                applied.try_send(()).ok();
            }
            Input::Connect(client) => self.connect(client).await,
            Input::Disconnect => self.disconnect(),
        }
    }

    /// The cursor persisted for each part. A table that answers a different number of
    /// cursors than asked is broken.
    /// Borrows the table and the config alone: the whole driver is not `Sync`.
    async fn cursors_of(
        cursors: &K,
        config: &DriverConfig,
        parts: &[PartKey],
    ) -> Vec<(PartKey, Rev)> {
        if parts.is_empty() {
            return vec![];
        }
        let cursors = cursors
            .read(&config.peer, &config.backend, parts)
            .await
            .expect("the peer-cursor table failed a read");
        assert_eq!(cursors.len(), parts.len(), "the table answers every part");
        parts.iter().cloned().zip(cursors).collect()
    }

    async fn update_routes(&mut self, update: RouteUpdate) {
        let removed_parts: Vec<_> = update
            .removed_parts
            .into_iter()
            .filter(|part| self.routes.parts.remove(part))
            .collect();
        let removed_objects: Vec<_> = update
            .removed_objects
            .into_iter()
            .filter(|obj| self.routes.objects.remove(obj))
            .collect();
        let added_parts: Vec<_> = update
            .added_parts
            .into_iter()
            .filter(|part| self.routes.parts.insert(part.clone()))
            .collect();
        let added_objects: Vec<_> = update
            .added_objects
            .into_iter()
            .filter(|obj| self.routes.objects.insert(obj.clone()))
            .collect();
        for part in &removed_parts {
            self.facts.publish(&self.config.peer, part, Fact::Removed);
        }
        for part in &added_parts {
            self.facts.publish(&self.config.peer, part, Fact::Unsynced);
        }
        if self.conn.is_none() {
            return;
        }
        let added = Self::cursors_of(&self.cursors, &self.config, &added_parts).await;
        if let Some(conn) = self.conn.as_mut() {
            conn.sync
                .update(&added, &removed_parts, &added_objects, &removed_objects);
        }
    }

    /// Law 6: a fresh machine and session, given the current routes with their persisted
    /// cursors.
    async fn connect(&mut self, client: C) {
        self.disconnect();
        let parts: Vec<_> = self.routes.parts.iter().cloned().collect();
        let added = Self::cursors_of(&self.cursors, &self.config, &parts).await;
        let objects: Vec<_> = self.routes.objects.iter().cloned().collect();
        let mut sync = PeerSync::new(self.config.machine(), Uuid::from_u128(rand::random()));
        sync.update(&added, &[], &objects, &[]);
        self.conn = Some(Conn {
            sync,
            env: Arc::new(Env {
                client,
                backend: Arc::clone(&self.backend),
                local: Arc::clone(&self.local),
            }),
            calls: FuturesUnordered::new(),
            aborts: default(),
        });
    }

    /// Law 6: the machine says what to cancel; dropping the connection drops those calls, and
    /// nothing is reported to a machine that is gone (ObjectJobs law 10).
    fn disconnect(&mut self) {
        let Some(Conn { sync, aborts, .. }) = self.conn.take() else {
            return;
        };
        let mut cmds = vec![];
        sync.disconnect(&mut cmds);
        for cmd in cmds {
            let PeerCommand::Cancel { call } = cmd else {
                unreachable!("a disconnect only cancels: {cmd:?}");
            };
            aborts[&call].abort();
        }
        for part in &self.routes.parts {
            self.facts.publish(&self.config.peer, part, Fact::Unsynced);
        }
    }

    /// Law 2: an outcome goes to the machine once, by the input that names its kind.
    fn feed(&mut self, call: CallId, done: Done) {
        let conn = self.conn.as_mut().expect("calls live in a connection");
        conn.aborts
            .remove(&call)
            .expect("a call's outcome is reported once");
        let sync = &mut conn.sync;
        match done {
            Done::Summary(reply) => sync.summary_replied(call, reply),
            Done::Session(reply) => sync.update_replied(call, reply),
            Done::Round(reply) => sync.round_replied(call, reply),
            Done::List(reply) => sync.list_replied(call, reply),
            Done::Leaf(reply) => sync.leaf_replied(call, reply),
            Done::Record(outcome) => sync.record_ended(call, outcome),
            Done::Fetch(outcome) => sync.fetch_ended(call, outcome),
        }
    }

    /// The flush the machine is owed after every input (law 4), and its commands.
    async fn flush(&mut self) {
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        let mut cmds = vec![];
        conn.sync.flush((self.clock)(), &mut cmds);
        let mut persists = vec![];
        for cmd in cmds {
            match cmd {
                PeerCommand::Cancel { call } => conn.aborts[&call].abort(),
                PeerCommand::PersistCursor { part, cursor } => persists.push((part, cursor)),
                // An edge of a part whose route was removed since is stale: the removal
                // already published `Removed`.
                PeerCommand::Synced { part } => {
                    if self.routes.parts.contains(&part) {
                        self.facts.publish(&self.config.peer, &part, Fact::Synced);
                    }
                }
                PeerCommand::Unsynced { part } => {
                    if self.routes.parts.contains(&part) {
                        self.facts.publish(&self.config.peer, &part, Fact::Unsynced);
                    }
                }
                call => {
                    let id = call_id(&call);
                    let aborted = Done::aborted(&call);
                    let (task, handle) = abortable(run_call(Arc::clone(&conn.env), call));
                    conn.aborts.insert(id, handle);
                    conn.calls
                        .push(Box::pin(async move { (id, task.await.unwrap_or(aborted)) }));
                }
            }
        }
        // Law 5: one write per flush, its entries in the order the machine issued them, and the
        // loop waits for it: the next batch and a re-added part's read both see it.
        if !persists.is_empty() {
            self.cursors
                .write(&self.config.peer, &self.config.backend, &persists)
                .await
                .expect("the peer-cursor table failed a write");
        }
    }
}

fn call_id(cmd: &PeerCommand) -> CallId {
    match cmd {
        PeerCommand::Summary { call, .. }
        | PeerCommand::Session { call, .. }
        | PeerCommand::Round { call, .. }
        | PeerCommand::ListBuckets { call, .. }
        | PeerCommand::LeafBuckets { call, .. }
        | PeerCommand::Record { call, .. }
        | PeerCommand::Fetch { call, .. } => *call,
        _ => unreachable!("not a call: {cmd:?}"),
    }
}

/// Run one call to its outcome. A transport failure, or an answer the machine cannot take
/// from a correct peer, is `Lost`; a backend failure is `Failed`. Both are retried by the
/// machine (law 9); the warning is the only trace they leave.
async fn run_call<C, B, S>(env: Arc<Env<C, B, S>>, cmd: PeerCommand) -> Done
where
    C: BigSyncRpcClient,
    B: SyncBackend,
    S: PartStoreRead,
{
    match cmd {
        PeerCommand::Summary { parts, .. } => Done::Summary(
            match env
                .client
                .part_summaries(PartSummariesRequest { parts })
                .await
            {
                Ok(Ok(summaries)) => SummaryReply::Summaries(summaries),
                Ok(Err(PartSummariesError::TooManyParts { cap })) => {
                    SummaryReply::TooManyParts { cap }
                }
                Ok(Err(error)) => lost(&error, SummaryReply::Lost),
                Err(error) => lost(&error, SummaryReply::Lost),
            },
        ),
        PeerCommand::Session { update, .. } => {
            Done::Session(answered(env.client.update_replay_session(update).await))
        }
        PeerCommand::Round { page, .. } => {
            Done::Round(answered(env.client.page_replay_session(page).await))
        }
        PeerCommand::ListBuckets {
            part_key,
            offset,
            since,
            working_level,
            ..
        } => Done::List(list_buckets(&env, part_key, offset, since, working_level).await),
        PeerCommand::LeafBuckets {
            part_key,
            since,
            buckets,
            ..
        } => {
            let seed = FingerprintSeed::new(rand::random(), rand::random());
            let request = LeafBucketsRequest {
                part_key: part_key.clone(),
                since,
                buckets,
                seed,
                limit_hint: Some(BucketMachine::LEAF_BUCKET_LIMIT_HINT),
            };
            Done::Leaf(match env.client.leaf_buckets(request).await {
                Ok(Ok(pages)) => Reply::Answer(
                    filter_objects(part_key, pages, seed, &*env.local)
                        .await
                        .expect("the local part store failed a read"),
                ),
                Ok(Err(error)) => lost(&error, Reply::Lost),
                Err(error) => lost(&error, Reply::Lost),
            })
        }
        PeerCommand::Record { job, .. } => Done::Record(match env.backend.record(job).await {
            Ok(settle) => Outcome::Succeeded(settle),
            Err(error) => lost(&error, Outcome::Failed),
        }),
        PeerCommand::Fetch { job, .. } => Done::Fetch(match env.backend.fetch(job).await {
            Ok(()) => Outcome::Succeeded(()),
            Err(error) => lost(&error, Outcome::Failed),
        }),
        _ => unreachable!("not a call: {cmd:?}"),
    }
}

fn lost<T>(error: &dyn std::fmt::Debug, outcome: T) -> T {
    tracing::warn!(?error, "a call failed; the machine retries it");
    outcome
}

/// A method's own error is the call's answer; a transport failure is a lost call.
fn answered<T, E>(result: Result<Result<T, E>, crate::sync::rpc::RpcError>) -> Reply<Result<T, E>> {
    match result {
        Ok(answer) => Reply::Answer(answer),
        Err(error) => lost(&error, Reply::Lost),
    }
}

/// The filtered listing the machine asks for: list from `offset`, filter against the local
/// part, and relist past a page that filtered to nothing; an empty answer ends the listing.
async fn list_buckets<C, B, S>(
    env: &Env<C, B, S>,
    part_key: PartKey,
    mut offset: BuckId,
    since: Rev,
    working_level: BuckLevel,
) -> Reply<Vec<BucketSummary>>
where
    C: BigSyncRpcClient,
    S: PartStoreRead,
{
    loop {
        let request = GetChangedBucketsRequest {
            part_key: part_key.clone(),
            offset,
            to_level: working_level,
            since,
            limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
        };
        let listed = match env.client.get_changed_buckets(request).await {
            Ok(Ok(listed)) => listed,
            Ok(Err(error)) => return lost(&error, Reply::Lost),
            Err(error) => return lost(&error, Reply::Lost),
        };
        match filter_buckets(part_key.clone(), working_level, listed, &*env.local)
            .await
            .expect("the local part store failed a read")
        {
            FilteredBuckets::Done => return Reply::Answer(vec![]),
            FilteredBuckets::Handoff(buckets) => return Reply::Answer(buckets),
            FilteredBuckets::Relist(next) => offset = next,
        }
    }
}
