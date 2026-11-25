//! The whole-sync workload, runnable anywhere the crate builds: two in-memory nodes, one
//! serving `n` objects of one part and one pulling them through the real driver over an
//! in-process transport that counts its calls and its bytes.
//!
//! It is plain library code (behind `test-support`) so that the same run serves three hosts:
//! a test on tokio's paused clock (deterministic simulated time, gated), a native wall-clock
//! bench, and the browser host (`big_automata_web`, timed with `performance.now()`). It spawns
//! nothing: the driver's loop and the waiting side are polled together, so it needs only a
//! clock and `n0_future`'s sleep, which each runtime has.

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use futures::future::Either;

use crate::Clock;
use crate::driver::DriverConfig;
use crate::driver::Facts;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::kit::scheduler::Instant;
use crate::part_store::AccessLevel;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreWrite;
use crate::part_store::memory::MemoryPartStore;
use crate::replay::responder::Responder;
use crate::replay::responder::Timer;
use crate::replay::server::ReplayServer;
use crate::replay::wire::CloseSession;
use crate::replay::wire::PageSession;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::UpdateSession;
use crate::sync::rpc::BigSyncRpcClient;
use crate::sync::rpc::BigSyncRpcResult;
use crate::sync::rpc::bucket::BucketsError;
use crate::sync::rpc::bucket::GetChangedBucketsRequest;
use crate::sync::rpc::bucket::GetChangedBucketsResponse;
use crate::sync::rpc::bucket::LeafBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsResponse;
use crate::sync::rpc::responder::RpcResponder;
use crate::sync::rpc::summary::PartSummariesError;
use crate::sync::rpc::summary::PartSummariesRequest;
use crate::sync::rpc::summary::PartSummariesResponse;

/// Members per transaction on the serving node, so `n` objects are `n / WRITE_BATCH` revs.
const WRITE_BATCH: usize = 100;

/// What one whole sync cost.
#[derive(Debug, Clone, Copy)]
pub struct SyncReport {
    pub objects: usize,
    /// The round trip injected before every call.
    pub rtt: Duration,
    /// Time on the driver's clock: virtual under tokio's paused clock, real elsewhere.
    pub elapsed: Duration,
    /// Real time, however the driver's clock runs.
    pub wall: Duration,
    /// Calls of every kind.
    pub calls: u64,
    /// `page_replay_session` calls: replay rounds.
    pub rounds: u64,
    /// Request and response bytes as CBOR, which is what a codec would carry.
    pub bytes_up: u64,
    pub bytes_down: u64,
    /// What the pulling side and the serving responders allocated while syncing (zero unless the
    /// host installed `utils_rs::testing::alloc_count::CountingAllocator`), setup excluded.
    pub allocations: u64,
    pub alloc_bytes: u64,
}

#[derive(Default)]
struct Meter {
    calls: AtomicU64,
    rounds: AtomicU64,
    up: AtomicU64,
    down: AtomicU64,
}

/// A `Write` that only counts.
struct Tally(u64);

impl std::io::Write for Tally {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encoded_len(value: &impl Serialize) -> u64 {
    let mut tally = Tally(0);
    ciborium::into_writer(value, &mut tally).expect(ERROR_IMPOSSIBLE);
    tally.0
}

struct NeverTimer;

impl Timer for NeverTimer {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        n0_future::time::sleep(duration)
    }
}

struct PeerInner {
    rpc: RpcResponder<MemoryPartStore>,
    responder: futures::lock::Mutex<Responder<MemoryPartStore, NeverTimer>>,
    server: Mutex<ReplayServer>,
    meter: Meter,
    rtt: Duration,
}

/// The serving node as the driver's transport: the real responders over a memory store, called
/// in process, each call delayed by the injected round trip and metered.
#[derive(Clone)]
struct MeteredPeer {
    inner: Arc<PeerInner>,
}

fn me() -> PeerKey {
    PeerKey::new(&[0])
}

fn part() -> PartKey {
    PartKey::new(&[1])
}

impl MeteredPeer {
    fn new(store: MemoryPartStore, rtt: Duration) -> Self {
        Self {
            inner: Arc::new(PeerInner {
                rpc: RpcResponder::new(store.clone()),
                responder: futures::lock::Mutex::new(Responder::new(store, NeverTimer)),
                server: default(),
                meter: default(),
                rtt,
            }),
        }
    }

    /// One call: the round trip, then the answer, counting both directions.
    async fn call<Req: Serialize, Resp: Serialize>(
        &self,
        request: &Req,
        answer: impl Future<Output = Resp>,
    ) -> BigSyncRpcResult<Resp> {
        let meter = &self.inner.meter;
        meter.calls.fetch_add(1, Ordering::Relaxed);
        meter.up.fetch_add(encoded_len(request), Ordering::Relaxed);
        if !self.inner.rtt.is_zero() {
            n0_future::time::sleep(self.inner.rtt).await;
        }
        let response = answer.await;
        meter
            .down
            .fetch_add(encoded_len(&response), Ordering::Relaxed);
        Ok(response)
    }
}

impl BigSyncRpcClient for MeteredPeer {
    async fn part_summaries(
        &self,
        req: PartSummariesRequest,
    ) -> BigSyncRpcResult<Result<PartSummariesResponse, PartSummariesError>> {
        self.call(&req.clone(), async {
            self.inner
                .rpc
                .part_summaries(&me(), req)
                .await
                .expect("store")
        })
        .await
    }

    async fn update_replay_session(
        &self,
        req: UpdateSession,
    ) -> BigSyncRpcResult<Result<SessionUpdated, SessionError>> {
        self.call(&req.clone(), async {
            let mut responder = self.inner.responder.lock().await;
            let admits = responder.admission(&me(), &req).await.expect("store");
            self.inner.server.lock().expect("server lock").update(
                &me(),
                &req,
                Instant::now(),
                admits,
            )
        })
        .await
    }

    async fn page_replay_session(
        &self,
        req: PageSession,
    ) -> BigSyncRpcResult<Result<SessionPage, SessionError>> {
        self.inner.meter.rounds.fetch_add(1, Ordering::Relaxed);
        self.call(&req.clone(), async {
            let mut responder = self.inner.responder.lock().await;
            responder
                .page(&me(), |round| {
                    self.inner
                        .server
                        .lock()
                        .expect("server lock")
                        .resolve_round(&me(), &req, Instant::now(), round)
                })
                .await
                .expect("store")
                .cloned()
        })
        .await
    }

    async fn close_replay_session(
        &self,
        req: CloseSession,
    ) -> BigSyncRpcResult<Result<(), SessionError>> {
        self.call(&req.clone(), async {
            self.inner
                .server
                .lock()
                .expect("server lock")
                .close(&me(), &req);
            Ok(())
        })
        .await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> BigSyncRpcResult<Result<GetChangedBucketsResponse, BucketsError>> {
        self.call(&req.clone(), async {
            self.inner
                .rpc
                .changed_buckets(&me(), req)
                .await
                .expect("store")
        })
        .await
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> BigSyncRpcResult<Result<LeafBucketsResponse, BucketsError>> {
        self.call(&req.clone(), async {
            self.inner
                .rpc
                .leaf_buckets(&me(), req)
                .await
                .expect("store")
        })
        .await
    }
}

/// A serving node holding `objects` live members of one part, readable by the pulling node.
async fn serving_store(objects: usize) -> MemoryPartStore {
    let store = MemoryPartStore::default();
    let mut tx = store.begin();
    store
        .grant_access(&mut tx, &[(part(), me(), AccessLevel::Read)])
        .await
        .expect("grant");
    tx.commit();
    for start in (0..objects).step_by(WRITE_BATCH) {
        let chunk = start..(start + WRITE_BATCH).min(objects);
        let obj =
            |row: usize| ObjKey::new(&u32::try_from(row).expect("rows fit u32").to_be_bytes());
        let mut tx = store.begin();
        store
            .set_payloads(
                &mut tx,
                chunk
                    .clone()
                    .map(|row| {
                        (
                            obj(row),
                            serde_json::json!({ "row": row, "text": "x".repeat(48) }),
                        )
                    })
                    .collect(),
            )
            .await
            .expect("payloads");
        store
            .add_members(
                &mut tx,
                &chunk.map(|row| (obj(row), part())).collect::<Vec<_>>(),
            )
            .await
            .expect("members");
        tx.commit();
    }
    store
}

/// Sync `objects` objects from one in-memory node to another through the real driver and
/// report what it cost. `clock` is the driver's injected clock (tokio's paused one in
/// simulation, [`web_time::Instant::now`] in a browser); `wall` is always the real one.
pub async fn sync_once(
    objects: usize,
    rtt: Duration,
    config: impl FnOnce(&mut DriverConfig),
    clock: Clock,
) -> SyncReport {
    let source = serving_store(objects).await;
    let peer = MeteredPeer::new(source.clone(), rtt);
    let local = MemoryPartStore::default();
    let pulled_into = local.clone();
    let facts = Facts::new();
    let mut driver_config = DriverConfig::new(PeerKey::new(&[9]), "bench");
    config(&mut driver_config);
    let peer_key = driver_config.peer.clone();
    let (driver, driver_loop) = PeerDriver::start(
        driver_config,
        Arc::clone(&clock),
        facts.clone(),
        MemoryBackend::new(local.clone(), source),
        MemoryPeerCursors::default(),
        local,
    );
    let wall_start = web_time::Instant::now();
    let started = clock();
    let pulled = async {
        driver
            .update_routes(RouteUpdate {
                added_parts: vec![part()],
                ..default()
            })
            .await;
        driver.connect(peer.clone());
        facts
            .wait_for_full_sync([(peer_key, part())])
            .await
            .expect("the part stays wanted");
    };
    utils_rs::testing::alloc_count::start();
    // The loop never ends while the handle lives, so the sync finishing is what ends the run.
    match futures::future::select(std::pin::pin!(driver_loop), std::pin::pin!(pulled)).await {
        Either::Left(((), _)) => panic!("the driver loop ended before the part synced"),
        Either::Right(((), _)) => {}
    }
    let counted = utils_rs::testing::alloc_count::finish();
    assert_eq!(
        pulled_into.member_count(&part()).await.expect("count"),
        objects as u64,
        "the part is synced when every object is local"
    );
    let elapsed = clock().saturating_duration_since(started);
    let wall = wall_start.elapsed();
    let meter = &peer.inner.meter;
    SyncReport {
        objects,
        rtt,
        elapsed,
        wall,
        calls: meter.calls.load(Ordering::Relaxed),
        rounds: meter.rounds.load(Ordering::Relaxed),
        bytes_up: meter.up.load(Ordering::Relaxed),
        bytes_down: meter.down.load(Ordering::Relaxed),
        allocations: counted.allocations,
        alloc_bytes: counted.bytes,
    }
}

/// A line of the repo's benchmark record stream (`x/bench.ts` reads these from a run's
/// output), for one measurement of the bench `name`.
pub fn record_line(
    name: &str,
    metric: &str,
    unit: &str,
    value: f64,
    params: &[(&str, String)],
) -> String {
    let params: serde_json::Map<_, _> = params
        .iter()
        .map(|(key, value)| ((*key).to_owned(), serde_json::Value::String(value.clone())))
        .collect();
    format!(
        "BENCH_RECORD {}",
        serde_json::json!({
            "bench": name,
            "metric": metric,
            "unit": unit,
            "value": value,
            "params": params,
        })
    )
}

impl SyncReport {
    /// This run's records, one line per measurement.
    pub fn record_lines(&self, bench: &str) -> Vec<String> {
        let params = [
            ("n", self.objects.to_string()),
            ("rtt_ms", self.rtt.as_millis().to_string()),
        ];
        let secs = |duration: Duration| duration.as_secs_f64();
        [
            ("sim_time", "s", secs(self.elapsed)),
            ("wall_ns", "ns", self.wall.as_nanos() as f64),
            ("calls", "count", self.calls as f64),
            ("rounds", "count", self.rounds as f64),
            ("wire_bytes_up", "bytes", self.bytes_up as f64),
            ("wire_bytes_down", "bytes", self.bytes_down as f64),
            ("allocations", "count", self.allocations as f64),
            ("alloc_bytes", "bytes", self.alloc_bytes as f64),
        ]
        .into_iter()
        .map(|(metric, unit, value)| record_line(bench, metric, unit, value, &params))
        .collect()
    }
}
