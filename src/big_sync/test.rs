use crate::{SyncBackend, interlude::*};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use big_sync_core::rpc::{
    BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PeerSummaryRequest, PeerSummaryResult, SubPartsRequest,
};
use big_sync_core::{
    BuckId, ByteKey, FingerprintSeed, ObjKey, PartKey, PeerKey, SyncMode, SyncStatEvent,
    SyncTaskCompletion,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, seq::SliceRandom};
use serde::{Deserialize, Serialize};

use crate::backend::contract::{self, SyncBackendHarness, SyncBackendScenario};
use crate::part_store::HostPartStore;
use crate::part_store::memory::MemoryPartStore;
use crate::test_support::{ObservedStore, ObservedStoreSnapshot};
use crate::{Ctx, SyncTaskRunOutcome};

const TEST_BACKEND_ID: &str = "MemorySyncBackend";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LwwPayload {
    value: serde_json::Value,
    #[serde(rename = "writtenAt")]
    written_at: u64,
    #[serde(rename = "writerId")]
    writer_id: PeerKey,
}

impl LwwPayload {
    fn into_value(self) -> serde_json::Value {
        serde_json::to_value(self).expect(ERROR_JSON)
    }
}

fn lww_payload(
    value: impl Into<serde_json::Value>,
    written_at: u64,
    writer_id: PeerKey,
) -> serde_json::Value {
    LwwPayload {
        value: value.into(),
        written_at,
        writer_id,
    }
    .into_value()
}

fn compare_lww_payloads(left: &serde_json::Value, right: &serde_json::Value) -> Ordering {
    let left: LwwPayload = serde_json::from_value(left.clone()).expect(ERROR_JSON);
    let right: LwwPayload = serde_json::from_value(right.clone()).expect(ERROR_JSON);
    match left.written_at.cmp(&right.written_at) {
        Ordering::Equal => match left.writer_id.cmp(&right.writer_id) {
            Ordering::Equal => {
                assert_eq!(
                    left.value, right.value,
                    "equal LWW metadata must not diverge in payload value"
                );
                Ordering::Equal
            }
            ordering => ordering,
        },
        ordering => ordering,
    }
}

pub(crate) fn test_part() -> PartKey {
    PartKey(ByteKey::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

pub(crate) fn test_parts() -> Vec<PartKey> {
    vec![test_part()]
}

/// Work counters for comparing strategies. These count units of work rather than time:
/// on a shared runner, wall-clock measures contention more reliably than it measures
/// the strategy under test.
#[derive(Default)]
pub(crate) struct WorkCounters {
    /// `SyncBackend::sync_obj` calls. Each one is an object-level sync attempt, and so
    /// either a payload comparison or a remote payload fetch — the cost that dominates.
    obj_syncs: AtomicU64,
    /// Object entries the peer handed back in `leaf_buckets` pages.
    leaf_objects_returned: AtomicU64,
    /// Bucket summaries the peer handed back in `get_changed_buckets` pages.
    bucket_summaries_returned: AtomicU64,
    rpc_peer_summary: AtomicU64,
    rpc_replay_pages: AtomicU64,
    rpc_get_changed_buckets: AtomicU64,
    rpc_leaf_buckets: AtomicU64,
}

/// A point-in-time copy of [`WorkCounters`], for a test to assert on and log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkSnapshot {
    pub obj_syncs: u64,
    pub leaf_objects_returned: u64,
    pub bucket_summaries_returned: u64,
    pub rpc_peer_summary: u64,
    pub rpc_replay_pages: u64,
    pub rpc_get_changed_buckets: u64,
    pub rpc_leaf_buckets: u64,
}

impl WorkCounters {
    fn bump(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, AtomicOrdering::Relaxed);
    }

    fn snapshot(&self) -> WorkSnapshot {
        WorkSnapshot {
            obj_syncs: self.obj_syncs.load(AtomicOrdering::Relaxed),
            leaf_objects_returned: self.leaf_objects_returned.load(AtomicOrdering::Relaxed),
            bucket_summaries_returned: self.bucket_summaries_returned.load(AtomicOrdering::Relaxed),
            rpc_peer_summary: self.rpc_peer_summary.load(AtomicOrdering::Relaxed),
            rpc_replay_pages: self.rpc_replay_pages.load(AtomicOrdering::Relaxed),
            rpc_get_changed_buckets: self.rpc_get_changed_buckets.load(AtomicOrdering::Relaxed),
            rpc_leaf_buckets: self.rpc_leaf_buckets.load(AtomicOrdering::Relaxed),
        }
    }
}

#[derive(Default)]
pub(crate) struct TestWorld {
    stores: Mutex<HashMap<PeerKey, Arc<dyn HostPartStore>>>,
    online: Mutex<HashSet<PeerKey>>,
    work: WorkCounters,
}

impl TestWorld {
    /// Work done in this world so far, in counted units rather than seconds.
    fn work(&self) -> WorkSnapshot {
        self.work.snapshot()
    }

    fn register_store<S>(&self, peer_id: PeerKey, store: Arc<S>)
    where
        S: HostPartStore + 'static,
    {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let store: Arc<dyn HostPartStore> = store;
        let old = stores.insert(peer_id.clone(), store);
        assert!(old.is_none(), "fishy");
        self.set_online(peer_id, true);
    }

    fn store_for_peer(&self, peer_id: PeerKey) -> Arc<dyn HostPartStore> {
        self.stores
            .lock()
            .expect(ERROR_MUTEX)
            .get(&peer_id)
            .cloned()
            .expect(ERROR_IMPOSSIBLE)
    }

    fn remove_store(&self, peer_id: PeerKey) {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let old = stores.remove(&peer_id);
        assert!(old.is_some(), "fishy");
    }

    fn set_online(&self, peer_id: PeerKey, online: bool) {
        let mut online_state = self.online.lock().expect(ERROR_MUTEX);
        if online {
            online_state.insert(peer_id);
        } else {
            online_state.remove(&peer_id);
        }
    }

    fn is_online(&self, peer_id: PeerKey) -> bool {
        self.online.lock().expect(ERROR_MUTEX).contains(&peer_id)
    }
}

#[derive(Clone)]
pub(crate) struct MemoryRpcClient {
    world: Arc<TestWorld>,
    _source_part_store: Arc<dyn HostPartStore>,
    source_peer_id: PeerKey,
    target_peer_id: PeerKey,
    target_part_store: Arc<dyn HostPartStore>,
}

impl MemoryRpcClient {
    fn new(
        world: Arc<TestWorld>,
        source_part_store: Arc<dyn HostPartStore>,
        source_peer_id: PeerKey,
        target_peer_id: PeerKey,
        target_part_store: Arc<dyn HostPartStore>,
    ) -> Self {
        Self {
            world,
            _source_part_store: source_part_store,
            source_peer_id,
            target_peer_id,
            target_part_store,
        }
    }
}

#[async_trait]
impl crate::rpc::WireBigSyncRpcClient for MemoryRpcClient {
    async fn peer_summary(
        &self,
        req: crate::rpc::ScopedRequest<PeerSummaryRequest>,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        WorkCounters::bump(&self.world.work.rpc_peer_summary, 1);
        let req = req.inner;
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_count = req.parts.len(),
            "memory rpc peer summary"
        );
        if !self.world.is_online(self.target_peer_id.clone()) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let asker = Some(self.source_peer_id.clone());
        let parts = self.target_part_store.summarize_parts(req.parts).await??;
        let mut summaries = HashMap::new();
        for (part_id, summary) in parts {
            let since = req.asker_part_cursors.get(&part_id).copied().unwrap_or(0);
            let dirty = self
                .target_part_store
                .part_dirty_count(part_id.clone(), asker.clone(), since)
                .await?;
            summaries.insert(part_id, summary.into_strat_summaries(dirty));
        }
        Ok(Ok(Ok(PeerSummaryResult { parts: summaries })))
    }

    async fn replay_page(
        &self,
        req: crate::rpc::ScopedRequest<big_sync_core::rpc::ReplayPageRequest>,
    ) -> Res<BigSyncRpcResult<big_sync_core::rpc::ReplayPageOutcome>> {
        WorkCounters::bump(&self.world.work.rpc_replay_pages, 1);
        let req = req.inner;
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            target = ?req.target,
            "memory rpc replay page"
        );
        if !self.world.is_online(self.target_peer_id.clone()) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        // A short hold keeps a caught-up client from spinning while still letting
        // it observe new events soon after they land. The double caps the
        // caller's request rather than honouring the long production hold, so a
        // test does not sit on the long poll.
        let hold = Duration::from_millis(u64::from(req.hold_ms)).min(Duration::from_millis(50));
        let outcome = self
            .target_part_store
            .replay_page(req.target, req.limit, self.source_peer_id.clone(), hold)
            .await?;
        Ok(Ok(outcome))
    }

    async fn get_changed_buckets(
        &self,
        req: crate::rpc::ScopedRequest<GetChangedBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        let req = req.inner;
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_id = %req.part_id,
            offset = ?req.offset,
            since = req.since,
            limit_hint = req.limit_hint,
            "memory rpc get changed buckets"
        );
        if !self.world.is_online(self.target_peer_id.clone()) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        WorkCounters::bump(&self.world.work.rpc_get_changed_buckets, 1);
        let response = self
            .target_part_store
            .get_changed_buckets(req, self.source_peer_id.clone())
            .await?;
        if let Ok(summaries) = &response {
            WorkCounters::bump(
                &self.world.work.bucket_summaries_returned,
                summaries.len() as u64,
            );
        }
        Ok(Ok(response))
    }

    async fn leaf_buckets(
        &self,
        req: crate::rpc::ScopedRequest<LeafBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        let req = req.inner;
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_id = %req.part_id,
            bucket_count = req.buckets.len(),
            since = req.since,
            "memory rpc leaf buckets"
        );
        if !self.world.is_online(self.target_peer_id.clone()) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        WorkCounters::bump(&self.world.work.rpc_leaf_buckets, 1);
        let res = self
            .target_part_store
            .leaf_buckets(req, self.source_peer_id.clone())
            .await??;
        let returned = res
            .bucks
            .values()
            .map(|page| page.entries.len() as u64)
            .sum::<u64>();
        WorkCounters::bump(&self.world.work.leaf_objects_returned, returned);
        Ok(Ok(Ok(res)))
    }
}

pub(crate) struct MemorySyncBackend {
    _local_peer_id: PeerKey,
    local_part_store: Arc<dyn HostPartStore>,
    world: Arc<TestWorld>,
}

impl MemorySyncBackend {
    pub(crate) fn new(
        local_peer_id: PeerKey,
        local_part_store: Arc<dyn HostPartStore>,
        world: Arc<TestWorld>,
    ) -> Self {
        Self {
            _local_peer_id: local_peer_id,
            local_part_store,
            world,
        }
    }
}

#[async_trait]
impl SyncBackend for MemorySyncBackend {
    #[tracing::instrument(skip(self))]
    async fn sync_obj(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<PartKey>,
        remote_payload: Option<serde_json::Value>,
    ) -> Res<SyncTaskRunOutcome> {
        WorkCounters::bump(&self.world.work.obj_syncs, 1);
        let local_payload = self.local_part_store.obj_payload(obj_id.clone()).await?;
        let remote_payload = match remote_payload {
            Some(remote_payload) => Some(remote_payload),
            None => {
                if !self.world.is_online(peer_id.clone()) {
                    eyre::bail!("peer is offline");
                }
                let remote_part_store = self.world.store_for_peer(peer_id);
                remote_part_store.obj_payload(obj_id.clone()).await?
            }
        };
        let outcome = match (local_payload, remote_payload) {
            (Some(local), Some(remote)) => match compare_lww_payloads(&local, &remote) {
                Ordering::Less => {
                    self.local_part_store
                        .set_obj_payload(obj_id.clone(), remote)
                        .await?;
                    SyncTaskCompletion {
                        obj_id: obj_id.clone(),
                        deets: big_sync_core::SyncCompletionDeets::ChangedObject,
                    }
                }
                Ordering::Equal | Ordering::Greater => SyncTaskCompletion {
                    obj_id: obj_id.clone(),
                    deets: big_sync_core::SyncCompletionDeets::Noop,
                },
            },
            (None, Some(payload)) => {
                self.local_part_store
                    .set_obj_payload(obj_id.clone(), payload)
                    .await?;
                SyncTaskCompletion {
                    obj_id: obj_id.clone(),
                    deets: big_sync_core::SyncCompletionDeets::AddedMember,
                }
            }
            (Some(_), None) | (None, None) => {
                eyre::bail!("missing on remote");
            }
        };
        // The memory backend wants synced objs re-advertised to other peers:
        // adopt the hinted parts so bucket replay short-circuits on them.
        if !parts.is_empty() {
            self.local_part_store
                .add_obj_to_parts(obj_id, parts.clone())
                .await?;
        }
        Ok(SyncTaskRunOutcome::Completion(outcome))
    }

    async fn remove_obj_from_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        for part_id in parts {
            self.local_part_store
                .remove_obj_from_part(obj_id.clone(), part_id)
                .await?;
        }
        Ok(())
    }
}

struct MemorySyncBackendContractHarness {
    world: Arc<TestWorld>,
    backend: Arc<dyn SyncBackend>,
    store: Arc<dyn HostPartStore>,
}

#[async_trait]
impl SyncBackendHarness for MemorySyncBackendContractHarness {
    fn backend(&self) -> &dyn SyncBackend {
        self.backend.as_ref()
    }

    fn store(&self) -> &dyn HostPartStore {
        self.store.as_ref()
    }

    async fn prepare_case(&self, case: &SyncBackendScenario) -> Res<()> {
        if case.remote_payload.is_none() {
            let remote_store = Arc::new(MemoryPartStore::new());
            if let Some(payload) = &case.initial_payload {
                remote_store
                    .set_obj_payload(case.obj_id.clone(), payload.clone())
                    .await?;
            }
            self.world
                .register_store(case.peer_id.clone(), Arc::clone(&remote_store));
        }
        Ok(())
    }

    async fn assert_case(&self, case: &SyncBackendScenario) -> Res<()> {
        if case.remote_payload.is_none() {
            self.world.remove_store(case.peer_id.clone());
        }
        Ok(())
    }
}

fn memory_sync_backend_cases() -> Vec<SyncBackendScenario> {
    let part = test_part();
    let extra_part = PartKey(ByteKey::new([7; 32]));
    vec![
        SyncBackendScenario::noop(
            "noop_when_payloads_match",
            peer_id(2),
            gen_obj_id(10),
            payload(serde_json::json!({"kind": "noop"}), 1, peer_id(2)),
            vec![part.clone()],
        ),
        SyncBackendScenario::changed_object(
            "changed_object_applies_remote",
            peer_id(2),
            gen_obj_id(11),
            payload(serde_json::json!({"kind": "old"}), 1, peer_id(1)),
            payload(serde_json::json!({"kind": "new"}), 2, peer_id(2)),
            vec![part.clone()],
        ),
        SyncBackendScenario::changed_object(
            "changed_object_with_empty_part_hints",
            peer_id(2),
            gen_obj_id(1101),
            payload(serde_json::json!({"kind": "old-empty"}), 1, peer_id(1)),
            payload(serde_json::json!({"kind": "new-empty"}), 2, peer_id(2)),
            vec![],
        ),
        SyncBackendScenario::changed_object(
            "changed_object_with_multiple_part_hints",
            peer_id(2),
            gen_obj_id(1102),
            payload(serde_json::json!({"kind": "old-multi"}), 1, peer_id(1)),
            payload(serde_json::json!({"kind": "new-multi"}), 2, peer_id(2)),
            vec![part.clone(), extra_part],
        ),
        SyncBackendScenario::added_member(
            "added_member_materializes_missing_obj",
            peer_id(2),
            gen_obj_id(12),
            payload(serde_json::json!({"kind": "new"}), 2, peer_id(2)),
            vec![part],
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_backend_contract() -> Res<()> {
    let world = Arc::new(TestWorld::default());
    let local = Arc::new(MemoryPartStore::new());
    let local_part_store: Arc<dyn HostPartStore> = Arc::clone(&local) as _;
    let backend: Arc<dyn SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id(1),
        Arc::clone(&local_part_store),
        Arc::clone(&world),
    ));
    let harness = MemorySyncBackendContractHarness {
        world,
        backend,
        store: local_part_store,
    };

    contract::assert_sync_backend_scenarios(&harness, &memory_sync_backend_cases()).await
}

struct NodeHarness {
    world: Arc<TestWorld>,
    peer_id: PeerKey,
    host: Ctx,
    handle: crate::BigSyncWorkerHandle,
    stop: crate::StopToken,
    store: Arc<dyn HostPartStore>,
    observed_store: Arc<dyn ObservedStore>,
    restart_memory_store: Option<Arc<MemoryPartStore>>,
    sqlite_temp_dir: Option<tempfile::TempDir>,
    /// The hint this node was booted with, so a restart hands back the same band instead
    /// of silently dropping to the default mid-scenario.
    sync_mode: Option<SyncMode>,
}

impl NodeHarness {
    async fn connect_to(&self, remote: &NodeHarness) -> Res<()> {
        let client = Arc::new(MemoryRpcClient::new(
            Arc::clone(&self.world),
            Arc::clone(&self.store),
            self.peer_id.clone(),
            remote.peer_id.clone(),
            Arc::clone(&remote.store),
        ));
        self.host
            .worker
            .set_peer(
                remote.peer_id.clone(),
                client,
                test_parts()
                    .iter()
                    .map(|part| (part.clone(), TEST_BACKEND_ID.into()))
                    .collect(),
                std::collections::HashMap::new(),
            )
            .await
    }

    async fn seed_obj(&self, obj: ObjKey, payload: serde_json::Value) -> Res<()> {
        let (peer_ids, stores): (Vec<_>, Vec<_>) = {
            let stores = self.world.stores.lock().expect(ERROR_MUTEX);
            (
                stores.keys().cloned().collect(),
                stores.values().cloned().collect(),
            )
        };
        for store in stores {
            let agents = peer_ids
                .iter()
                .cloned()
                .map(|peer_id| (peer_id, keyhive_core::access::Access::Read))
                .collect::<HashMap<_, _>>();
            for part in test_parts() {
                store.set_part_members(part, agents.clone()).await?;
            }
        }
        self.host
            .store
            .set_obj_payload(obj.clone(), payload)
            .await?;
        self.host.store.add_obj_to_parts(obj, test_parts()).await?;
        Ok(())
    }
    async fn remove_obj(&self, obj: ObjKey) -> Res<()> {
        self.host
            .store
            .remove_obj_from_part(obj, test_part())
            .await?;
        Ok(())
    }

    async fn wait_for_full_sync(
        &self,
        peer_ids: impl IntoIterator<Item = PeerKey>,
        part_ids: impl IntoIterator<Item = PartKey>,
    ) -> Res<()> {
        self.host
            .worker
            .wait_for_full_sync(peer_ids, part_ids)
            .await
    }

    async fn snapshot(&self) -> Res<ObservedStoreSnapshot> {
        self.observed_store.observed_snapshot().await
    }

    async fn stop(self) -> Res<()> {
        self.world.set_online(self.peer_id.clone(), false);
        self.stop.stop().await?;
        self.world.remove_store(self.peer_id.clone());
        Ok(())
    }
}

pub(crate) fn peer_id(seed: u8) -> PeerKey {
    PeerKey(ByteKey::new([seed; 32]))
}

pub(crate) fn payload(
    value: impl Into<serde_json::Value>,
    written_at: u64,
    writer_id: PeerKey,
) -> serde_json::Value {
    lww_payload(value, written_at, writer_id)
}

pub(crate) fn gen_obj_id(seed: usize) -> ObjKey {
    ObjKey(ByteKey::new(
        *blake3::hash(format!("test.{seed}").as_bytes()).as_bytes(),
    ))
}

async fn seed_objects(node: &NodeHarness, prefix: &str, count: usize) -> Res<Vec<ObjKey>> {
    let mut objs = Vec::with_capacity(count);
    for ii in 0..count {
        let obj = ObjKey(ByteKey::new(
            *blake3::hash(format!("{prefix}.{ii}").as_bytes()).as_bytes(),
        ));
        node.seed_obj(
            obj.clone(),
            payload(
                serde_json::json!({ "ii": ii, "prefix": prefix }),
                ii as u64,
                node.peer_id.clone(),
            ),
        )
        .await?;
        objs.push(obj);
    }
    Ok(objs)
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_part_store_root_bucket_contract() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new();
    let part_id = test_part();
    let seed = FingerprintSeed::new(1, 2);
    let mut obj_ids = Vec::new();
    for ii in 0..5u8 {
        let obj_id = gen_obj_id((90 + ii) as usize);
        store
            .set_obj_payload(
                obj_id.clone(),
                payload(
                    serde_json::json!({"phase": "present", "ii": ii}),
                    ii as u64,
                    peer_id(1),
                ),
            )
            .await?;
        store
            .add_obj_to_parts(obj_id.clone(), vec![part_id.clone()])
            .await?;
        obj_ids.push(obj_id);
    }

    crate::part_store::contract::assert_root_bucket_contract(
        &store,
        part_id.clone(),
        seed,
        &obj_ids,
        &[],
        2,
    )
    .await?;

    let removed_obj_id = obj_ids[1].clone();
    store
        .remove_obj_from_part(removed_obj_id.clone(), part_id.clone())
        .await?;
    let live_ids: Vec<_> = obj_ids
        .iter()
        .filter(|&obj_id| *obj_id != removed_obj_id)
        .cloned()
        .collect();
    crate::part_store::contract::assert_root_bucket_contract(
        &store,
        part_id,
        seed,
        &live_ids,
        &[removed_obj_id],
        2,
    )
    .await?;
    Ok(())
}

// #[tokio::test(flavor = "multi_thread")]
// async fn memory_part_store_gen_obj_id_id_distribution() -> Res<()> {
//     let store = crate::part_store::memory::MemoryPartStore::new(peer_id(1));
//     let objs: Vec<_> = (0..64u8).map(gen_obj_id).collect();
//     crate::part_store::contract::assert_gen_obj_id_id_distribution(&store, &objs).await
// }

#[test]
fn memory_part_store_terminal_bucket_bounds_do_not_wrap() {
    let terminal = BuckId::new(1, 15);
    let (start, end) = crate::part_store::bucket_index_bounds(terminal);
    assert!(end.is_none(), "terminal bucket must not wrap");
    assert_eq!(start, 15 << 12);
    assert_eq!(start, crate::part_store::bucket_index_bounds(terminal).0);
    let non_terminal = BuckId::new(2, 0);
    let (start, end) = crate::part_store::bucket_index_bounds(non_terminal);
    assert_eq!((start, end), (0, Some(1 << 8)));
    assert!(
        end.is_some(),
        "non-terminal bucket should still have an upper bound"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_part_store_bucket_summary_is_order_independent() -> Res<()> {
    let store_a = MemoryPartStore::new();
    let store_b = MemoryPartStore::new();

    let objs = [
        (
            gen_obj_id(1),
            payload(serde_json::json!({"obj": 1}), 1, peer_id(1)),
        ),
        (
            gen_obj_id(2),
            payload(serde_json::json!({"obj": 2}), 2, peer_id(1)),
        ),
        (
            gen_obj_id(3),
            payload(serde_json::json!({"obj": 3}), 3, peer_id(1)),
        ),
    ];
    let mut obj_ids_a = Vec::new();
    let mut obj_ids_b = Vec::new();
    for (obj, _) in &objs {
        obj_ids_a.push(obj.clone());
        obj_ids_b.push(obj.clone());
    }
    for ((_, payload), obj_id) in objs.iter().zip(obj_ids_a.iter()) {
        store_a
            .set_obj_payload(obj_id.clone(), payload.clone())
            .await?;
        store_a
            .add_obj_to_parts(obj_id.clone(), vec![test_part()])
            .await?;
    }
    for ((_, payload), obj_id) in objs.iter().rev().zip(obj_ids_b.iter().rev()) {
        store_b
            .set_obj_payload(obj_id.clone(), payload.clone())
            .await?;
        store_b
            .add_obj_to_parts(obj_id.clone(), vec![test_part()])
            .await?;
    }
    // A bucket walk is a peer-facing read, so both stores are walked as a granted
    // peer rather than as a local caller.
    let subscriber_a =
        crate::part_store::contract::grant_bucket_read(&store_a, [test_part()]).await?;
    let subscriber_b =
        crate::part_store::contract::grant_bucket_read(&store_b, [test_part()]).await?;

    let a_initial = store_a
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: test_part(),
                offset: BuckId::ROOT,
                to_level: BuckId::ROOT.level(),
                since: 0,
                limit_hint: 8 * u32::from(BuckId::ARITY),
            },
            subscriber_a.clone(),
        )
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    let b_initial = store_b
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: test_part(),
                offset: BuckId::ROOT,
                to_level: BuckId::ROOT.level(),
                since: 0,
                limit_hint: 8 * u32::from(BuckId::ARITY),
            },
            subscriber_b.clone(),
        )
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(a_initial.id, b_initial.id);
    assert_eq!(a_initial.len, b_initial.len);
    assert_eq!(a_initial.live_count, b_initial.live_count);
    assert_eq!(a_initial.fp, b_initial.fp);
    assert_eq!(a_initial.changed_at, b_initial.changed_at);

    store_a
        .remove_obj_from_part(obj_ids_a[1].clone(), test_part())
        .await?;
    store_b
        .remove_obj_from_part(obj_ids_b[1].clone(), test_part())
        .await?;

    let a_final = store_a
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: test_part(),
                offset: BuckId::ROOT,
                to_level: BuckId::ROOT.level(),
                since: 0,
                limit_hint: 8 * u32::from(BuckId::ARITY),
            },
            subscriber_a,
        )
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    let b_final = store_b
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id: test_part(),
                offset: BuckId::ROOT,
                to_level: BuckId::ROOT.level(),
                since: 0,
                limit_hint: 8 * u32::from(BuckId::ARITY),
            },
            subscriber_b,
        )
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);

    assert_eq!(a_final, b_final);
    Ok(())
}

async fn boot_node_with_store<S>(
    world: Arc<TestWorld>,
    peer_id: PeerKey,
    store: Arc<S>,
    restart_memory_store: Option<Arc<MemoryPartStore>>,
    sync_mode: Option<SyncMode>,
) -> Res<NodeHarness>
where
    S: ObservedStore + HostPartStore + 'static,
{
    store.ensure_part(test_part()).await?;
    world.set_online(peer_id.clone(), true);
    world.register_store(peer_id.clone(), Arc::clone(&store));
    let store_for_worker: Arc<dyn HostPartStore> = Arc::clone(&store) as _;
    let observed_store: Arc<dyn ObservedStore> = Arc::clone(&store) as _;
    let backend: Arc<dyn SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id.clone(),
        Arc::clone(&store_for_worker),
        Arc::clone(&world),
    ));
    let (handle, stop) = crate::spawn_big_sync_worker_with_options(
        Arc::clone(&store_for_worker),
        [(TEST_BACKEND_ID.into(), backend)].into(),
        "big-sync-test",
        None,
        sync_mode,
        Arc::from("big-sync-test"),
    )?;
    let host = Ctx {
        store: Arc::clone(&store_for_worker),
        worker: handle.clone(),
    };

    Ok(NodeHarness {
        world,
        peer_id,
        store: store_for_worker,
        observed_store,
        restart_memory_store,
        sqlite_temp_dir: None,
        sync_mode,
        host,
        handle,
        stop,
    })
}

async fn boot_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(MemoryPartStore::new());
    boot_node_with_store(world, peer_id, Arc::clone(&store), Some(store), None).await
}

async fn boot_policy_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(MemoryPartStore::new());
    boot_node_with_store(world, peer_id, Arc::clone(&store), Some(store), None).await
}

/// Boot a node with an explicit strategy hint. This is the same embedder knob
/// production uses: without it the picker short-circuits to `CursorOnly` and the
/// bucket machine never starts, no matter what the scenario looks like.
async fn boot_node_with_mode(
    world: Arc<TestWorld>,
    peer_seed: u8,
    sync_mode: SyncMode,
) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(MemoryPartStore::new());
    boot_node_with_store(
        world,
        peer_id,
        Arc::clone(&store),
        Some(store),
        Some(sync_mode),
    )
    .await
}

async fn restart_node(world: Arc<TestWorld>, node: NodeHarness) -> Res<NodeHarness> {
    let NodeHarness {
        world: node_world,
        peer_id,
        restart_memory_store,
        sync_mode,
        host: _host,
        handle: _handle,
        stop,
        sqlite_temp_dir: _sqlite_temp_dir,
        ..
    } = node;
    node_world.set_online(peer_id.clone(), false);
    stop.stop().await?;
    node_world.remove_store(peer_id.clone());
    let Some(memory_store) = restart_memory_store else {
        eyre::bail!("node is not restartable with a memory store");
    };
    boot_node_with_store(
        world,
        peer_id,
        Arc::clone(&memory_store),
        Some(memory_store),
        sync_mode,
    )
    .await
}

async fn assert_two_node_alignment(
    left: &NodeHarness,
    right: &NodeHarness,
    expected_obj_count: usize,
) -> Res<(ObservedStoreSnapshot, ObservedStoreSnapshot)> {
    let worker_left = left.handle.snapshot().await?;
    let worker_right = right.handle.snapshot().await?;
    let part_id = test_part();
    let expected_left_parts = [(part_id.clone(), TEST_BACKEND_ID.into())]
        .into_iter()
        .collect();
    let expected_right_parts = [(part_id, TEST_BACKEND_ID.into())].into_iter().collect();
    assert_eq!(worker_left.peer_parts.len(), 1);
    assert_eq!(worker_right.peer_parts.len(), 1);
    assert_eq!(
        worker_left.peer_parts.get(&right.peer_id),
        Some(&expected_left_parts)
    );
    assert_eq!(
        worker_right.peer_parts.get(&left.peer_id),
        Some(&expected_right_parts)
    );

    let snapshot_left = left.snapshot().await?;
    let snapshot_right = right.snapshot().await?;
    assert_eq!(snapshot_left.objs, snapshot_right.objs);
    assert_eq!(snapshot_left.objs.len(), expected_obj_count);

    Ok((snapshot_left, snapshot_right))
}

/// Wait until every node's store and worker snapshots agree, and stay agreeing for 8 rounds.
///
/// No deadline of its own: the harness class timeout is the deadline, and the periodic line
/// names what still differs. The worker breakdown carries the task counts by kind and the
/// peer/part flags, which is where a fence that is still held shows up.
async fn wait_for_convergence(nodes: &[&NodeHarness]) -> Res<()> {
    /// How often a still-diverged wait reports what it is waiting on.
    const REPORT_INTERVAL: Duration = Duration::from_secs(5);
    let started = std::time::Instant::now();
    let mut next_report = started + REPORT_INTERVAL;
    let mut last_snapshot = None;
    let mut stable_rounds = 0usize;

    loop {
        let mut current = Vec::with_capacity(nodes.len());
        for node in nodes {
            current.push((node.handle.snapshot().await?, node.snapshot().await?));
        }

        let stores_equal = current
            .iter()
            .map(|(_, snapshot)| snapshot)
            .all(|snapshot| snapshot == &current[0].1);

        if stores_equal && last_snapshot.as_ref().is_some_and(|prev| prev == &current) {
            stable_rounds += 1;
            if stable_rounds >= 8 {
                return Ok(());
            }
        } else {
            stable_rounds = if stores_equal { 1 } else { 0 };
        }

        let now = std::time::Instant::now();
        if now >= next_report {
            let breakdown = current
                .iter()
                .enumerate()
                .map(|(idx, (worker_snapshot, _))| {
                    format!("node{idx}:{}", worker_snapshot.idle_breakdown())
                })
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!(
                elapsed_secs = started.elapsed().as_secs(),
                stores_equal,
                stable_rounds,
                breakdown = %breakdown,
                "convergence wait still diverged",
            );
            next_report = now + REPORT_INTERVAL;
        }

        last_snapshot = Some(current);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_same_observed_state(
    left: &NodeHarness,
    right: &NodeHarness,
) -> Res<(ObservedStoreSnapshot, ObservedStoreSnapshot)> {
    let left_snapshot = left.snapshot().await?;
    let right_snapshot = right.snapshot().await?;
    assert_eq!(left_snapshot, right_snapshot);
    Ok((left_snapshot, right_snapshot))
}

/// Wait until every node reports its worker idle.
///
/// No deadline of its own: the harness class timeout is the deadline. The breakdown is logged
/// whenever it changes and re-reported every [`REPORT_INTERVAL`], so a stalled wait leaves a
/// trajectory even when the counters stop moving.
async fn wait_for_idle(nodes: &[&NodeHarness]) -> Res<()> {
    /// How often a still-busy wait re-reports the breakdown it is stuck on.
    const REPORT_INTERVAL: Duration = Duration::from_secs(5);
    let started = std::time::Instant::now();
    let mut next_report = started + REPORT_INTERVAL;
    let mut last_breakdown = String::new();

    loop {
        let mut current = Vec::with_capacity(nodes.len());
        for node in nodes {
            current.push(node.handle.snapshot().await?);
        }
        let breakdown = current
            .iter()
            .map(|worker_snapshot| worker_snapshot.idle_breakdown())
            .collect::<Vec<_>>()
            .join(" || ");
        if current
            .iter()
            .all(|worker_snapshot| worker_snapshot.is_idle())
        {
            tracing::debug!(breakdown = %breakdown, "idle wait satisfied");
            return Ok(());
        }

        // Log the counter breakdown whenever it changes while stalled, so a
        // timeout shows the whole trajectory rather than only the final state.
        if breakdown != last_breakdown {
            tracing::debug!(breakdown = %breakdown, "idle wait still stalled");
            last_breakdown = breakdown.clone();
        }

        let now = std::time::Instant::now();
        if now >= next_report {
            tracing::info!(
                elapsed_secs = started.elapsed().as_secs(),
                breakdown = %breakdown,
                "idle wait still stalled",
            );
            next_report = now + REPORT_INTERVAL;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn drain_stats(stats_rx: &mut tokio::sync::broadcast::Receiver<SyncStatEvent>) {
    while stats_rx.try_recv().is_ok() {}
}

async fn collect_stats(
    stats_rx: &mut tokio::sync::broadcast::Receiver<SyncStatEvent>,
    timeout: Duration,
) -> Vec<SyncStatEvent> {
    let mut out = Vec::new();
    loop {
        match tokio::time::timeout(timeout, stats_rx.recv()).await {
            Ok(Ok(evt)) => out.push(evt),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
            Err(_) => break,
        }
    }
    out
}

/// Make work the machine has paced for later due now, on every node.
///
/// A denied replay route is retried on production pacing, and the only thing that
/// moves a due retry is the worker's real-time tick. A test that waits the pacing
/// out has to pick a deadline larger than the pacing, and picking one equal to it
/// turns the test into a race with the tick.
///
/// `Tasks` doubles a retry's backoff per attempt, capped at the task frame's
/// `max_backoff` (one minute by default), so the delta clears the pacing constant
/// with margin rather than landing on its boundary.
async fn advance_past_backoff(nodes: &[&NodeHarness]) -> Res<()> {
    let delta = big_sync_core::unauthorized_backoff() * 3;
    for node in nodes {
        node.handle.advance_clock(delta).await?;
    }
    Ok(())
}

/// Collect stats until `done` holds, driving each node's machine clock while
/// waiting so work paced for later becomes due instead of being slept out.
///
/// A fixed drain window has to outlast whatever pacing the machine applies to the
/// work producing the event under assertion, which is how
/// [`memory_sync_single_obj_created_while_connected_replicates`] came to wait
/// exactly as long as the pacing constant. This waits on the event instead, so the
/// window stops being a race with the pacing.
///
/// The wait has no deadline of its own: the harness class timeout is the
/// deadline, and a route that stays denied is the failure this test wants to see.
async fn collect_stats_until(
    stats_rx: &mut tokio::sync::broadcast::Receiver<SyncStatEvent>,
    nodes: &[&NodeHarness],
    done: impl Fn(&[SyncStatEvent]) -> bool,
) -> Res<Vec<SyncStatEvent>> {
    // A denied route is re-paced after each attempt, so the clock is advanced
    // on every idle tick for as long as the wait lasts.
    let mut out = Vec::new();
    loop {
        if done(&out) {
            return Ok(out);
        }
        tokio::select! {
            biased;
            msg = stats_rx.recv() => match msg {
                Ok(evt) => out.push(evt),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(out),
            },
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                advance_past_backoff(nodes).await?;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_preconnected_seeds_converge() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let left_obj = gen_obj_id(10);
    let right_obj = gen_obj_id(11);
    let left_payload = payload("left-a", 1, node_a.peer_id.clone());
    let right_payload = payload("right-b", 1, node_b.peer_id.clone());

    node_a
        .seed_obj(left_obj.clone(), left_payload.clone())
        .await?;
    node_b
        .seed_obj(right_obj.clone(), right_payload.clone())
        .await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    wait_for_convergence(&[&node_a, &node_b]).await?;
    let (snapshot_a, _) = assert_two_node_alignment(&node_a, &node_b, 2).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&left_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(left_payload)
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&right_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(right_payload)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_single_obj_created_while_connected_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let part_id = test_part();
    let mut stats_rx = node_a.handle.subscribe_stats();

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a]).await?;
    drain_stats(&mut stats_rx);

    let obj = gen_obj_id(20);
    let created_payload = payload("connected-create", 1, node_b.peer_id.clone());
    node_b
        .seed_obj(obj.clone(), created_payload.clone())
        .await?;

    // Wait on the events this test asserts, not on the idle proxy: `is_idle` omits
    // machine tasks and `all`, so it can report idle before the sync task spawns.
    // The wait drives the machine clock, because the replay route that carries this
    // object was denied once before the peer's grant landed (fail-closed recipient
    // filtering, step 1) and its retry is paced by production backoff. There is no
    // backstop: the clock is driven, so a denied-forever route hangs here instead of
    // failing on a wall-clock guess.
    let stats = collect_stats_until(&mut stats_rx, &[&node_a, &node_b], |stats| {
        stats.iter().any(|evt| {
            matches!(
                evt,
                SyncStatEvent::PartFullySynced { part_id: synced, .. } if *synced == part_id
            )
        }) && stats
            .iter()
            .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. }))
    })
    .await?;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(
        stats
            .iter()
            .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. }))
    );
    let (snapshot_a, snapshot_b) = assert_same_observed_state(&node_a, &node_b).await?;
    assert_eq!(snapshot_a.objs.len(), 1);
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(created_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(created_payload)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_wait_for_full_sync_resolves_for_connected_peer_pair() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let part_id = test_part();
    let created_payload = payload("wait-for-full-sync", 1, node_b.peer_id.clone());

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let obj = gen_obj_id(21);
    node_b
        .seed_obj(obj.clone(), created_payload.clone())
        .await?;

    node_a
        .wait_for_full_sync([node_b.peer_id.clone()], [part_id])
        .await?;

    let (snapshot_a, snapshot_b) = assert_same_observed_state(&node_a, &node_b).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(created_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(created_payload)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_higher_peer_update_propagates_after_convergence() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 2).await?;
    let node_b = boot_node(Arc::clone(&world), 1).await?;

    let obj = gen_obj_id(30);
    let base_payload = payload("base", 1, node_b.peer_id.clone());
    let update_payload = payload("higher-update", 2, node_a.peer_id.clone());
    node_b.seed_obj(obj.clone(), base_payload).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    node_a.seed_obj(obj.clone(), update_payload.clone()).await?;

    wait_for_convergence(&[&node_a, &node_b]).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(update_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(update_payload)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_connected_cursor_replay_handles_mutation_burst() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj_a = gen_obj_id(41);
    let obj_b = gen_obj_id(42);
    let obj_a_payload = payload("cursor-a-0", 1, node_a.peer_id.clone());
    let obj_b_payload = payload("cursor-b-0", 1, node_b.peer_id.clone());
    node_a
        .seed_obj(obj_a.clone(), obj_a_payload.clone())
        .await?;
    node_b
        .seed_obj(obj_b.clone(), obj_b_payload.clone())
        .await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let rounds = 24;
    for round in 0..rounds {
        node_a
            .seed_obj(
                obj_a.clone(),
                payload(
                    format!("cursor-a-{round}"),
                    round as u64 + 2,
                    node_a.peer_id.clone(),
                ),
            )
            .await?;
        node_b
            .seed_obj(
                obj_b.clone(),
                payload(
                    format!("cursor-b-{round}"),
                    round as u64 + 2,
                    node_b.peer_id.clone(),
                ),
            )
            .await?;
        wait_for_convergence(&[&node_a, &node_b]).await?;
    }

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 2).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj_a)
            .and_then(|obj| obj.payload.clone()),
        Some(payload(
            format!("cursor-a-{}", rounds - 1),
            rounds as u64 + 1,
            node_a.peer_id.clone(),
        ))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(payload(
            format!("cursor-b-{}", rounds - 1),
            rounds as u64 + 1,
            node_b.peer_id.clone(),
        ))
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_concurrent_conflicting_updates_converge_to_higher_peer_value() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = gen_obj_id(40);
    let base_payload = payload("base", 1, node_a.peer_id.clone());
    let lower_payload = payload("lower-conflict", 2, node_a.peer_id.clone());
    let higher_payload = payload("higher-conflict", 2, node_b.peer_id.clone());
    node_a.seed_obj(obj.clone(), base_payload).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    for part in test_parts() {
        node_a
            .store
            .set_part_members(
                part.clone(),
                HashMap::from([(node_b.peer_id.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
        node_b
            .store
            .set_part_members(
                part,
                HashMap::from([(node_a.peer_id.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
    }
    wait_for_convergence(&[&node_a, &node_b]).await?;

    tokio::try_join!(
        node_a.seed_obj(obj.clone(), lower_payload.clone()),
        node_b.seed_obj(obj.clone(), higher_payload.clone()),
    )?;

    wait_for_convergence(&[&node_a, &node_b]).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(higher_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(higher_payload)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_delete_propagates_to_both_nodes() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = gen_obj_id(50);
    node_a
        .seed_obj(obj.clone(), payload("delete-me", 1, node_a.peer_id.clone()))
        .await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    node_a.remove_obj(obj.clone()).await?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 0).await?;
    assert!(!snapshot_a.objs.contains_key(&obj));
    assert!(!snapshot_b.objs.contains_key(&obj));

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_direct_backend_errors_on_collected_object() -> Res<()> {
    let world = Arc::new(TestWorld::default());
    let peer_a = peer_id(1);
    let peer_b = peer_id(2);
    let store_a = Arc::new(MemoryPartStore::new());
    let store_b = Arc::new(MemoryPartStore::new());
    let store_b_dyn: Arc<dyn HostPartStore> = Arc::clone(&store_b) as _;

    world.register_store(peer_a.clone(), Arc::clone(&store_a));
    world.register_store(peer_b.clone(), Arc::clone(&store_b));

    let part = test_part();
    let obj = gen_obj_id(51);
    let live_payload = payload("live", 1, peer_a.clone());

    store_a
        .set_obj_payload(obj.clone(), live_payload.clone())
        .await?;
    store_a
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;
    store_b.set_obj_payload(obj.clone(), live_payload).await?;
    store_b
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;
    store_a
        // Removing the membership keeps the payload, so a remote's absence only arises from
        // collection: `remove_obj_payload` is the one path that clears content, and it leaves no
        // membership behind either.
        .remove_obj_payload(obj.clone())
        .await?;

    let backend = MemorySyncBackend::new(peer_b, Arc::clone(&store_b_dyn), Arc::clone(&world));

    let err = backend
        .sync_obj(peer_a.clone(), obj.clone(), Vec::new(), None)
        .await
        .expect_err("remote absence should be treated as a hard error for now");

    assert!(format!("{err:?}").contains("missing on remote"));
    assert_eq!(
        store_b.obj_payload(obj.clone()).await?,
        Some(payload("live", 1, peer_a))
    );
    assert_eq!(store_b.obj_parts(obj).await?, vec![part]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_direct_backend_cross_replication_is_symmetric() -> Res<()> {
    let world = Arc::new(TestWorld::default());
    let peer_a = peer_id(1);
    let peer_b = peer_id(2);
    let store_a = Arc::new(MemoryPartStore::new());
    let store_b = Arc::new(MemoryPartStore::new());
    let store_a_dyn: Arc<dyn HostPartStore> = Arc::clone(&store_a) as _;
    let store_b_dyn: Arc<dyn HostPartStore> = Arc::clone(&store_b) as _;

    world.register_store(peer_a.clone(), Arc::clone(&store_a));
    world.register_store(peer_b.clone(), Arc::clone(&store_b));

    let part = test_part();
    let obj_a = gen_obj_id(52);
    let obj_b = gen_obj_id(53);
    let left_payload = payload("left-a", 1, peer_a.clone());
    let right_payload = payload("right-b", 1, peer_b.clone());

    store_a
        .set_obj_payload(obj_a.clone(), left_payload.clone())
        .await?;
    store_a
        .add_obj_to_parts(obj_a.clone(), vec![part.clone()])
        .await?;
    store_b
        .set_obj_payload(obj_b.clone(), right_payload.clone())
        .await?;
    store_b.add_obj_to_parts(obj_b.clone(), vec![part]).await?;

    let backend_a =
        MemorySyncBackend::new(peer_a.clone(), Arc::clone(&store_a_dyn), Arc::clone(&world));
    let backend_b =
        MemorySyncBackend::new(peer_b.clone(), Arc::clone(&store_b_dyn), Arc::clone(&world));

    backend_a
        .sync_obj(
            peer_b,
            obj_b.clone(),
            Vec::new(),
            Some(right_payload.clone()),
        )
        .await?;
    backend_b
        .sync_obj(
            peer_a,
            obj_a.clone(),
            Vec::new(),
            Some(left_payload.clone()),
        )
        .await?;

    let snapshot_a = store_a.snapshot().await?;
    let snapshot_b = store_b.snapshot().await?;
    assert!(snapshot_a.objs.contains_key(&obj_a));
    assert!(snapshot_a.objs.contains_key(&obj_b));
    assert!(snapshot_b.objs.contains_key(&obj_a));
    assert!(snapshot_b.objs.contains_key(&obj_b));
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj_a)
            .and_then(|obj| obj.payload.clone()),
        Some(left_payload.clone())
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(right_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_a)
            .and_then(|obj| obj.payload.clone()),
        Some(left_payload.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(right_payload)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_two_node_bidirectional_connect_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let left_obj = gen_obj_id(90);
    let right_obj = gen_obj_id(91);
    let left_payload = payload("left", 1, node_a.peer_id.clone());
    let right_payload = payload("right", 1, node_b.peer_id.clone());

    node_a
        .seed_obj(left_obj.clone(), left_payload.clone())
        .await?;
    node_b
        .seed_obj(right_obj.clone(), right_payload.clone())
        .await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let (snapshot_a, snapshot_b) = assert_same_observed_state(&node_a, &node_b).await?;
    assert_eq!(snapshot_a.objs.len(), 2);
    assert_eq!(
        snapshot_a
            .objs
            .get(&left_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(left_payload.clone())
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&right_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(right_payload.clone())
    );
    assert_eq!(snapshot_a, snapshot_b);

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_two_node_sync_is_idempotent_when_idle() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let obj = gen_obj_id(92);
    let payload_value = payload("idempotent", 1, node_a.peer_id.clone());

    node_a.seed_obj(obj, payload_value.clone()).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    let snapshot_before = node_a.snapshot().await?;

    wait_for_convergence(&[&node_a, &node_b]).await?;
    let snapshot_after = node_a.snapshot().await?;
    assert_eq!(snapshot_before, snapshot_after);

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

async fn memory_sync_connect_order_snapshot(
    connect_left_first: bool,
) -> Res<ObservedStoreSnapshot> {
    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let left_obj = gen_obj_id(93);
    let right_obj = gen_obj_id(94);
    let left_payload = payload("order-left", 1, node_a.peer_id.clone());
    let right_payload = payload("order-right", 1, node_b.peer_id.clone());

    node_a.seed_obj(left_obj, left_payload).await?;
    node_b.seed_obj(right_obj, right_payload).await?;

    if connect_left_first {
        node_a.connect_to(&node_b).await?;
        node_b.connect_to(&node_a).await?;
    } else {
        node_b.connect_to(&node_a).await?;
        node_a.connect_to(&node_b).await?;
    }
    wait_for_convergence(&[&node_a, &node_b]).await?;
    let snapshot = node_a.snapshot().await?;
    node_a.stop().await?;
    node_b.stop().await?;
    Ok(snapshot)
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_two_node_connect_order_does_not_change_final_state() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let left_first = memory_sync_connect_order_snapshot(true).await?;
    let right_first = memory_sync_connect_order_snapshot(false).await?;
    assert_eq!(left_first, right_first);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_large_gap_uses_bucket_catchup() -> Res<()> {
    memory_sync_large_gap_for_count(300, SyncMode::Bucket)
        .await
        .map(drop)
}

/// The measured cost of one strategy on one scenario. Work is counted rather than timed:
/// on a shared runner wall-clock reports contention more reliably than it reports
/// strategy, so durations are logged for the reader and never asserted on.
struct BandRun {
    mode: SyncMode,
    elapsed: Duration,
    work: WorkSnapshot,
}

impl BandRun {
    fn finish(
        scenario: &str,
        mode: SyncMode,
        started: std::time::Instant,
        world: &TestWorld,
    ) -> Self {
        let run = Self {
            mode,
            elapsed: started.elapsed(),
            work: world.work(),
        };
        tracing::info!(
            scenario,
            mode = ?run.mode,
            elapsed_ms = run.elapsed.as_millis() as u64,
            obj_syncs = run.work.obj_syncs,
            leaf_objects_returned = run.work.leaf_objects_returned,
            bucket_summaries_returned = run.work.bucket_summaries_returned,
            rpc_peer_summary = run.work.rpc_peer_summary,
            rpc_replay_pages = run.work.rpc_replay_pages,
            rpc_get_changed_buckets = run.work.rpc_get_changed_buckets,
            rpc_leaf_buckets = run.work.rpc_leaf_buckets,
            "band work measured",
        );
        run
    }

    /// Which band ran, taken from counted RPC rather than from a log line: only the bucket
    /// walk asks for bucket summaries or leaf pages. This is what lets the tests named for
    /// bucket catchup check the thing they are named after.
    fn bucket_walk_ran(&self) -> bool {
        self.work.rpc_get_changed_buckets > 0 || self.work.rpc_leaf_buckets > 0
    }

    fn assert_band(&self) {
        match self.mode {
            SyncMode::Bucket => assert!(
                self.bucket_walk_ran(),
                "bucket band requested but no bucket walk ran: {:?}",
                self.work
            ),
            SyncMode::CursorOnly => assert!(
                !self.bucket_walk_ran(),
                "cursor band requested but a bucket walk ran: {:?}",
                self.work
            ),
        }
    }
}

/// Wait until `node` holds every expected object with its payload and has consumed the
/// peer's stream for the part, then check the payloads and the fully-synced stats.
///
/// The cursor target is the peer's own latest cursor for the part — where a reader that has
/// consumed the whole stream sits. A literal derived from the object count would instead
/// encode how many cursor values each write happens to consume, which is not a property:
/// interleaving any other stamped write moves every later cursor value.
async fn await_catchup_and_verify(
    node: &NodeHarness,
    peer: &NodeHarness,
    part_id: PartKey,
    expected: usize,
    stats_rx: &mut tokio::sync::broadcast::Receiver<SyncStatEvent>,
) -> Res<()> {
    /// How often a still-waiting catchup reports progress. The wait has no deadline of its
    /// own: the harness class timeout is the deadline, and this line — which names the objects
    /// still missing and the rate they are arriving at — is what a kill leaves behind.
    const REPORT_INTERVAL: Duration = Duration::from_secs(5);
    let started = std::time::Instant::now();
    let mut last_report = started;
    let mut next_report = started + REPORT_INTERVAL;
    let mut seen_at_last_report = 0usize;
    let snapshot = loop {
        let snapshot = node.snapshot().await?;
        if snapshot.objs.len() == expected
            && (0..expected).all(|ii| {
                snapshot
                    .objs
                    .get(&gen_obj_id(ii))
                    .and_then(|obj| obj.payload.as_ref())
                    .is_some()
            })
        {
            break snapshot;
        }
        let now = std::time::Instant::now();
        // Only the report walks all of `expected`; the completion check above short-circuits on
        // the first object still missing, and this poll runs every 50ms, so the expensive walk
        // must stay off the common iteration.
        if now >= next_report {
            let missing: Vec<usize> = (0..expected)
                .filter(|ii| {
                    snapshot
                        .objs
                        .get(&gen_obj_id(*ii))
                        .and_then(|obj| obj.payload.as_ref())
                        .is_none()
                })
                .collect();
            let missing_head = missing
                .iter()
                .take(8)
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let seen = snapshot.objs.len();
            tracing::info!(
                part = ?part_id,
                elapsed_secs = started.elapsed().as_secs(),
                seen,
                expected,
                payload_ready = expected - missing.len(),
                added_since_last_report = seen.saturating_sub(seen_at_last_report),
                per_sec = seen.saturating_sub(seen_at_last_report) as f64
                    / now.duration_since(last_report).as_secs_f64(),
                missing_first = %missing_head,
                "catchup still waiting",
            );
            last_report = now;
            seen_at_last_report = seen;
            next_report = now + REPORT_INTERVAL;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(snapshot.objs.len(), expected);
    for ii in 0..expected {
        let value = snapshot
            .objs
            .get(&gen_obj_id(ii))
            .and_then(|obj| obj.payload.clone())
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            value,
            payload(
                serde_json::json!({ "ii": ii }),
                ii as u64,
                peer.peer_id.clone()
            )
        );
    }

    let cursor_started = std::time::Instant::now();
    let mut cursor_next_report = cursor_started + REPORT_INTERVAL;
    loop {
        let peer_part_cursor = peer
            .host
            .store
            .summarize_parts(HashSet::from([part_id.clone()]))
            .await?
            .map_err(|err| ferr!("failed to summarize the peer's part: {err:?}"))?
            .get(&part_id)
            .map(|summary| summary.latest_cursor)
            .ok_or_else(|| ferr!("the peer does not advertise the part it just seeded"))?;
        let snapshot = node.snapshot().await?;
        let observed = snapshot
            .peer_part_cursors
            .get(&(peer.peer_id.clone(), part_id.clone()))
            .copied();
        if observed == Some(peer_part_cursor) {
            break;
        }
        let now = std::time::Instant::now();
        if now >= cursor_next_report {
            tracing::info!(
                part = ?part_id,
                elapsed_secs = cursor_started.elapsed().as_secs(),
                peer_cursor_observed = ?observed,
                peer_cursor_target = peer_part_cursor,
                "catchup cursor still behind",
            );
            cursor_next_report = now + REPORT_INTERVAL;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let stats = collect_stats(stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(
        stats
            .iter()
            .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. }))
    );
    Ok(())
}

/// Run one band on one scenario shape and measure it.
///
/// `shared` objects are seeded on both nodes first — as if `node_a` had got them from
/// somewhere else, which is decision 7's restored device or second relay — then the rest go
/// to `node_b` alone. `shared == 0` is a cold peer; `shared` close to `total` is the sparse
/// dirt the bucket tree exists for.
async fn run_band_scenario(
    scenario: &str,
    total: usize,
    shared: usize,
    sync_mode: SyncMode,
) -> Res<BandRun> {
    utils_rs::testing::setup_tracing_once();
    let world = Arc::new(TestWorld::default());
    let node_a = boot_node_with_mode(Arc::clone(&world), 1, sync_mode).await?;
    let node_b = boot_node_with_mode(Arc::clone(&world), 2, sync_mode).await?;
    let part_id = test_part();
    let mut stats_rx = node_a.handle.subscribe_stats();

    for ii in 0..total {
        let obj_id = gen_obj_id(ii);
        let value = payload(
            serde_json::json!({ "ii": ii }),
            ii as u64,
            node_b.peer_id.clone(),
        );
        if ii < shared {
            node_a.seed_obj(obj_id.clone(), value.clone()).await?;
        }
        node_b.seed_obj(obj_id, value).await?;
    }

    let started = std::time::Instant::now();
    node_a.connect_to(&node_b).await?;
    await_catchup_and_verify(&node_a, &node_b, part_id, total, &mut stats_rx).await?;
    let run = BandRun::finish(scenario, sync_mode, started, &world);
    run.assert_band();

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(run)
}

async fn memory_sync_large_gap_for_count(obj_count: usize, sync_mode: SyncMode) -> Res<BandRun> {
    run_band_scenario("cold", obj_count, 0, sync_mode).await
}

/// The regime the bucket tree exists for, and the one no cold case can create: the puller
/// already holds almost all of the content, so its view of the part is dense while the real
/// difference is a handful of objects.
///
/// The size is not arbitrary. `calc_working_level` only leaves level 0 once a level-0 bucket
/// would hold more than `ACTIVE_SYNC_JOB_TARGET` objects, which needs more than
/// `ACTIVE_SYNC_JOB_TARGET * ARITY` = 16384 members. Below that the whole part is a single
/// root bucket and there are no ranges to prune: the band still wins by not moving payloads,
/// but it is not exercising the tree. Above it, clean ranges are skipped outright.
const SPARSE_TOTAL: usize = 20_000;

#[tokio::test(flavor = "multi_thread")]
async fn long_af_test_memory_sync_sparse_dirt_uses_bucket_work() -> Res<()> {
    const SHARED: usize = SPARSE_TOTAL - 3;
    let bucket = run_band_scenario("sparse", SPARSE_TOTAL, SHARED, SyncMode::Bucket).await?;
    let cursor = run_band_scenario("sparse", SPARSE_TOTAL, SHARED, SyncMode::CursorOnly).await?;
    // Counted work, never wall-clock. The tree prunes every range whose fingerprint agrees,
    // so the bucket band touches object work proportional to the difference, while cursor
    // replay pays for every event in the gap. One order of magnitude is the claim; the
    // exact numbers are in the log line above.
    assert!(
        bucket.work.obj_syncs * 10 <= cursor.work.obj_syncs,
        "sparse dirt: bucket moved {} object syncs against cursor's {}",
        bucket.work.obj_syncs,
        cursor.work.obj_syncs
    );
    Ok(())
}

/// The cold case, both bands, measured rather than assumed. Decision 7 says cursor replay
/// *is* enumeration for a peer with no prior state, so this is the scenario where the tree
/// has nothing to prune. The assertion is that the two bands stay within an order of
/// magnitude of each other on object work: a test claiming a bucket advantage here would be
/// claiming something false.
#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_cold_bands_move_comparable_work() -> Res<()> {
    const COUNT: usize = 1_000;
    let bucket = run_band_scenario("cold", COUNT, 0, SyncMode::Bucket).await?;
    let cursor = run_band_scenario("cold", COUNT, 0, SyncMode::CursorOnly).await?;
    let (bucket_work, cursor_work) = (bucket.work.obj_syncs, cursor.work.obj_syncs);
    assert!(
        bucket_work <= cursor_work * 4 && cursor_work <= bucket_work * 4,
        "cold: bucket moved {bucket_work} object syncs against cursor's {cursor_work}, which \
         is not the comparable work decision 7 predicts"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_large_gap_uses_bucket_catchup_1k() -> Res<()> {
    memory_sync_large_gap_for_count(1_000, SyncMode::Bucket)
        .await
        .map(drop)
}

/// The same scenario on the cursor band: the contrast the cutoff decision has to
/// be measured against. Identical workload, different strategy.
#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_large_gap_uses_cursor_replay_1k() -> Res<()> {
    memory_sync_large_gap_for_count(1_000, SyncMode::CursorOnly)
        .await
        .map(drop)
}

/// Bucket-shaped in the sense the bucket path exists for: the peer is *not* cold.
/// A first round leaves node A holding a non-zero stored peer cursor, then a burst
/// big enough to select the bucket band arrives, so the walk enters mid-stream and
/// has to honour the since-filter rather than enumerate the part from the start.
/// The four cases above are all cold peers, where cursor replay is the cheaper band.
#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_bucket_catchup_with_prior_state() -> Res<()> {
    mid_stream_backlog_for_mode(SyncMode::Bucket).await
}

/// The same mid-stream backlog on the cursor band, so the two strategies are
/// compared on identical work rather than on the cold-peer cases above.
#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_cursor_replay_with_prior_state() -> Res<()> {
    mid_stream_backlog_for_mode(SyncMode::CursorOnly).await
}

async fn mid_stream_backlog_for_mode(sync_mode: SyncMode) -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    const FIRST_ROUND: usize = 50;
    const BURST: usize = 600;

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node_with_mode(Arc::clone(&world), 1, sync_mode).await?;
    let node_b = boot_node_with_mode(Arc::clone(&world), 2, sync_mode).await?;
    let part_id = test_part();

    for ii in 0..FIRST_ROUND {
        node_b
            .seed_obj(
                gen_obj_id(ii),
                payload(
                    serde_json::json!({ "ii": ii }),
                    ii as u64,
                    node_b.peer_id.clone(),
                ),
            )
            .await?;
    }
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    assert_two_node_alignment(&node_a, &node_b, FIRST_ROUND).await?;

    // If A never stored a cursor for the part this is just another cold-peer case
    // and proves nothing about the mid-stream walk.
    let after_first = node_a.snapshot().await?;
    let stored = after_first
        .peer_part_cursors
        .get(&(node_b.peer_id.clone(), part_id))
        .copied()
        .unwrap_or(0);
    assert!(
        stored > 0,
        "the first round must leave a non-zero stored peer cursor, got {stored}"
    );

    // Detach the peers before the burst so it accumulates as a backlog instead of
    // streaming to A incrementally: while attached, A tracks B live and the gap
    // never grows enough to select the bucket band.
    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id.clone()),
        node_b.host.worker.remove_peer(node_a.peer_id.clone()),
    )?;
    world.set_online(node_b.peer_id.clone(), false);

    for ii in FIRST_ROUND..(FIRST_ROUND + BURST) {
        node_b
            .seed_obj(
                gen_obj_id(ii),
                payload(
                    serde_json::json!({ "ii": ii }),
                    ii as u64,
                    node_b.peer_id.clone(),
                ),
            )
            .await?;
    }

    world.set_online(node_b.peer_id.clone(), true);
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    assert_two_node_alignment(&node_a, &node_b, FIRST_ROUND + BURST).await?;

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_memory_sync_large_gap_uses_bucket_catchup_10k() -> Res<()> {
    memory_sync_large_gap_for_count(10_000, SyncMode::Bucket)
        .await
        .map(drop)
}

#[tokio::test(flavor = "multi_thread")]
async fn long_af_test_memory_sync_large_gap_uses_bucket_catchup_100k() -> Res<()> {
    memory_sync_large_gap_for_count(100_000, SyncMode::Bucket)
        .await
        .map(drop)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow bucket catchup case"]
async fn memory_sync_large_gap_uses_bucket_catchup_1m() -> Res<()> {
    memory_sync_large_gap_for_count(1_000_000, SyncMode::Bucket)
        .await
        .map(drop)
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_peer_restart_reconnects_cleanly() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = gen_obj_id(60);
    let before_restart = payload("before-restart", 1, node_a.peer_id.clone());
    node_a.seed_obj(obj.clone(), before_restart.clone()).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id.clone()),
        node_b.host.worker.remove_peer(node_a.peer_id.clone()),
    )?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(before_restart.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(before_restart)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_offline_edits_catch_up_after_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    let obj = gen_obj_id(70usize);
    let online_base = payload("online-base", 1, node_a.peer_id.clone());
    let offline_a = payload("offline-a", 2, node_a.peer_id.clone());
    node_a.seed_obj(obj.clone(), online_base).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id.clone()),
        node_b.host.worker.remove_peer(node_a.peer_id.clone()),
    )?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    node_a.seed_obj(obj.clone(), offline_a.clone()).await?;
    wait_for_convergence(&[&node_a]).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    let _stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(offline_a.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(offline_a)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_same_state_via_third_peer_stays_quiet() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let node_c = boot_node(Arc::clone(&world), 3).await?;

    let obj = gen_obj_id(80);
    let shared_from_third = payload("shared-from-third", 1, node_c.peer_id.clone());
    node_c
        .seed_obj(obj.clone(), shared_from_third.clone())
        .await?;

    tokio::try_join!(node_a.connect_to(&node_c), node_c.connect_to(&node_a))?;
    tokio::try_join!(node_b.connect_to(&node_c), node_c.connect_to(&node_b))?;
    wait_for_convergence(&[&node_a, &node_b, &node_c]).await?;

    let mut stats_rx = node_a.handle.subscribe_stats();
    drain_stats(&mut stats_rx);

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b, &node_c]).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(
        stats
            .iter()
            .any(|evt| matches!(evt, SyncStatEvent::PartStale { .. }))
    );

    let (snapshot_a, snapshot_b, snapshot_c) = (
        node_a.snapshot().await?,
        node_b.snapshot().await?,
        node_c.snapshot().await?,
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(shared_from_third.clone())
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(shared_from_third.clone())
    );
    assert_eq!(
        snapshot_c
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(shared_from_third)
    );

    node_a.stop().await?;
    node_b.stop().await?;
    node_c.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_random_half_deleted_before_reconnect_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    let objs = seed_objects(&node_a, "half-delete", 32).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id.clone()),
        node_b.host.worker.remove_peer(node_a.peer_id.clone()),
    )?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let mut rng = StdRng::seed_from_u64(0x3b1a_5eed);
    let mut deleted_mask = vec![false; objs.len()];
    let mut delete_idxs: Vec<_> = (0..objs.len()).collect();
    delete_idxs.shuffle(&mut rng);
    for ii in delete_idxs.into_iter().take(objs.len() / 2) {
        deleted_mask[ii] = true;
        node_a.remove_obj(objs[ii].clone()).await?;
    }

    wait_for_idle(&[&node_a]).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let _stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;

    let (snapshot_a, snapshot_b) =
        assert_two_node_alignment(&node_a, &node_b, objs.len() / 2).await?;
    for (ii, obj) in objs.iter().enumerate() {
        let expected = if deleted_mask[ii] {
            None
        } else {
            Some(payload(
                serde_json::json!({ "ii": ii, "prefix": "half-delete" }),
                ii as u64,
                node_a.peer_id.clone(),
            ))
        };
        assert_eq!(
            snapshot_a.objs.get(obj).and_then(|obj| obj.payload.clone()),
            expected
        );
        assert_eq!(
            snapshot_b.objs.get(obj).and_then(|obj| obj.payload.clone()),
            expected
        );
    }

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_offline_evolution_reconnects_cleanly() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let objs = seed_objects(&node_a, "offline-evolve", 16).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id.clone()),
        node_b.host.worker.remove_peer(node_a.peer_id.clone()),
    )?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let mut rng = StdRng::seed_from_u64(0x5eed_face);
    let mut expected_payloads = Vec::with_capacity(objs.len());
    for (ii, obj) in objs.iter().enumerate() {
        if rng.random_bool(0.50) {
            let expected = payload(
                serde_json::json!({ "ii": ii, "prefix": "offline-evolve.a" }),
                1_000 + ii as u64,
                node_a.peer_id.clone(),
            );
            node_a.seed_obj(obj.clone(), expected.clone()).await?;
            expected_payloads.push(Some(expected));
        } else {
            expected_payloads.push(Some(payload(
                serde_json::json!({ "ii": ii, "prefix": "offline-evolve" }),
                ii as u64,
                node_a.peer_id.clone(),
            )));
        }
    }

    wait_for_idle(&[&node_a]).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b]).await?;

    let expected_obj_count = node_a.snapshot().await?.objs.len();
    let (snapshot_a, snapshot_b) =
        assert_two_node_alignment(&node_a, &node_b, expected_obj_count).await?;
    assert_eq!(snapshot_a.objs, snapshot_b.objs);
    for (obj, expected) in objs.iter().zip(expected_payloads.iter()) {
        assert_eq!(
            snapshot_a.objs.get(obj).and_then(|obj| obj.payload.clone()),
            *expected
        );
    }

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hidden_part_subscription_returns_unknown_parts() -> Res<()> {
    use crate::HostPartStoreConfig;
    use big_sync_core::rpc::SubscriptionTarget;

    let part = test_part();
    let hidden = PartKey(ByteKey::new([99u8; 32]));
    let store = MemoryPartStore::with_config(HostPartStoreConfig {
        hidden_parts: HashSet::from([hidden.clone()]),
        ..Default::default()
    });
    let peer = PeerKey::new([1u8; 32]);

    // Both parts exist in the store.
    store.ensure_part(part.clone()).await?;
    store.ensure_part(hidden.clone()).await?;

    // Subscribing to a visible part succeeds.
    let rx = store
        .subscribe(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([SubscriptionTarget::Part {
                    part_id: part,
                    cursor: 0,
                }]),
            },
            peer.clone(),
        )
        .await?;
    assert!(rx.is_ok(), "visible part must subscribe ok");

    // Subscribing to a hidden part returns UnkownParts.
    let err = store
        .subscribe(
            SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([SubscriptionTarget::Part {
                    part_id: hidden.clone(),
                    cursor: 0,
                }]),
            },
            peer,
        )
        .await?;
    match err {
        Err(ListPartsError::UnkownParts { unkown_parts }) => {
            assert_eq!(unkown_parts, vec![hidden]);
        }
        other => panic!("expected UnkownParts for hidden part, got {other:?}"),
    }

    Ok(())
}

#[cfg(test)]
mod stress;
