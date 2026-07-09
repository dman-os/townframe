use crate::{interlude::*, SyncBackend};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

use big_sync_core::rpc::{
    BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PeerSummaryRequest, PeerSummaryResult, SubEvent,
    SubPartsRequest,
};
use big_sync_core::{
    BuckId, Byte32Id, FingerprintSeed, ObjId, PartId, PeerId, SyncStatEvent, SyncTaskCompletion,
};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::backend::contract::{self, SyncBackendHarness, SyncBackendScenario};
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::HostPartStore;
use crate::test_support::{ObservedStore, ObservedStoreSnapshot};
use crate::{Ctx, SyncTaskRunOutcome};

const TEST_BACKEND_ID: &str = "MemorySyncBackend";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LwwPayload {
    value: serde_json::Value,
    #[serde(rename = "writtenAt")]
    written_at: u64,
    #[serde(rename = "writerId")]
    writer_id: PeerId,
}

impl LwwPayload {
    fn into_value(self) -> serde_json::Value {
        serde_json::to_value(self).expect(ERROR_JSON)
    }
}

fn lww_payload(
    value: impl Into<serde_json::Value>,
    written_at: u64,
    writer_id: PeerId,
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

pub(crate) fn test_part() -> PartId {
    PartId(Byte32Id::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

pub(crate) fn test_parts() -> Vec<PartId> {
    vec![test_part()]
}

#[derive(Default)]
pub(crate) struct TestWorld {
    stores: Mutex<HashMap<PeerId, Arc<dyn HostPartStore>>>,
    online: Mutex<HashSet<PeerId>>,
}

impl TestWorld {
    fn register_store<S>(&self, peer_id: PeerId, store: Arc<S>)
    where
        S: HostPartStore + 'static,
    {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let store: Arc<dyn HostPartStore> = store;
        let old = stores.insert(peer_id, store);
        assert!(old.is_none(), "fishy");
        self.set_online(peer_id, true);
    }

    fn store_for_peer(&self, peer_id: PeerId) -> Arc<dyn HostPartStore> {
        self.stores
            .lock()
            .expect(ERROR_MUTEX)
            .get(&peer_id)
            .cloned()
            .expect(ERROR_IMPOSSIBLE)
    }

    fn remove_store(&self, peer_id: PeerId) {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let old = stores.remove(&peer_id);
        assert!(old.is_some(), "fishy");
    }

    fn set_online(&self, peer_id: PeerId, online: bool) {
        let mut online_state = self.online.lock().expect(ERROR_MUTEX);
        if online {
            online_state.insert(peer_id);
        } else {
            online_state.remove(&peer_id);
        }
    }

    fn is_online(&self, peer_id: PeerId) -> bool {
        self.online.lock().expect(ERROR_MUTEX).contains(&peer_id)
    }
}

#[derive(Clone)]
pub(crate) struct MemoryRpcClient {
    world: Arc<TestWorld>,
    _source_part_store: Arc<dyn HostPartStore>,
    target_peer_id: PeerId,
    target_part_store: Arc<dyn HostPartStore>,
}

impl MemoryRpcClient {
    fn new(
        world: Arc<TestWorld>,
        source_part_store: Arc<dyn HostPartStore>,
        target_peer_id: PeerId,
        target_part_store: Arc<dyn HostPartStore>,
    ) -> Self {
        Self {
            world,
            _source_part_store: source_part_store,
            target_peer_id,
            target_part_store,
        }
    }
}

#[async_trait]
impl crate::rpc::HostBigRpcClient for MemoryRpcClient {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_count = req.parts.len(),
            "memory rpc peer summary"
        );
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let parts = self.target_part_store.summarize_parts(req.parts).await??;
        Ok(Ok(Ok(PeerSummaryResult {
            parts,
            deepest_bucket_level: BuckId::MAX_LEVEL,
        })))
    }

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<big_sync_core::mpsc::Receiver<SubEvent>, ListPartsError>>>
    {
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_count = req.parts.len(),
            "memory rpc sub parts"
        );
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let receiver = self
            .target_part_store
            .subscribe(req, PeerId::new([0u8; 32]))
            .await??;
        Ok(Ok(Ok(receiver)))
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_id = %req.part_id,
            offset = ?req.offset,
            since = req.since,
            limit_hint = req.limit_hint,
            "memory rpc get changed buckets"
        );
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let response = self.target_part_store.get_changed_buckets(req).await?;
        Ok(Ok(response))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        tracing::debug!(
            target_peer_id = %self.target_peer_id,
            part_id = %req.part_id,
            bucket_count = req.buckets.len(),
            since = req.since,
            "memory rpc leaf buckets"
        );
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let res = self.target_part_store.leaf_buckets(req).await??;
        Ok(Ok(Ok(res)))
    }
}

pub(crate) struct MemorySyncBackend {
    _local_peer_id: PeerId,
    local_part_store: Arc<dyn HostPartStore>,
    world: Arc<TestWorld>,
}

impl MemorySyncBackend {
    pub(crate) fn new(
        local_peer_id: PeerId,
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
        peer_id: PeerId,
        obj_id: ObjId,
        remote_payload: Option<serde_json::Value>,
    ) -> Res<SyncTaskRunOutcome> {
        let local_payload = self.local_part_store.obj_payload(obj_id).await?;
        let remote_payload = match remote_payload {
            Some(remote_payload) => Some(remote_payload),
            None => {
                if !self.world.is_online(peer_id) {
                    eyre::bail!("peer is offline");
                }
                let remote_part_store = self.world.store_for_peer(peer_id);
                remote_part_store.obj_payload(obj_id).await?
            }
        };
        match (local_payload, remote_payload) {
            (Some(local), Some(remote)) => match compare_lww_payloads(&local, &remote) {
                Ordering::Less => {
                    self.local_part_store
                        .set_obj_payload(obj_id, remote)
                        .await?;
                    Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::ChangedObject,
                    }))
                }
                Ordering::Equal | Ordering::Greater => {
                    Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    }))
                }
            },
            (None, Some(payload)) => {
                self.local_part_store
                    .set_obj_payload(obj_id, payload)
                    .await?;
                Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                    obj_id,
                    deets: big_sync_core::SyncCompletionDeets::AddedMember,
                }))
            }
            (Some(_), None) | (None, None) => eyre::bail!("missing on remote"),
        }
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
            if let Some(payload) = &case.expected_payload {
                remote_store
                    .set_obj_payload(case.obj_id, payload.clone())
                    .await?;
            }
            self.world
                .register_store(case.peer_id, Arc::clone(&remote_store));
        }
        Ok(())
    }

    async fn assert_case(&self, case: &SyncBackendScenario) -> Res<()> {
        if case.remote_payload.is_none() {
            self.world.remove_store(case.peer_id);
        }
        Ok(())
    }
}

fn memory_sync_backend_cases() -> Vec<SyncBackendScenario> {
    let part = test_part();
    let extra_part = PartId(Byte32Id::new([7; 32]));
    vec![
        SyncBackendScenario::noop(
            "noop_when_payloads_match",
            peer_id(2),
            gen_obj_id(10),
            payload(serde_json::json!({"kind": "noop"}), 1, peer_id(2)),
            vec![part],
        ),
        SyncBackendScenario::noop(
            "noop_when_remote_payload_is_missing",
            peer_id(2),
            gen_obj_id(1010),
            payload(serde_json::json!({"kind": "noop-none"}), 1, peer_id(2)),
            vec![part],
        )
        .with_remote_payload(None),
        SyncBackendScenario::changed_object(
            "changed_object_when_remote_payload_is_missing",
            peer_id(2),
            gen_obj_id(1011),
            payload(serde_json::json!({"kind": "old-none"}), 1, peer_id(1)),
            payload(serde_json::json!({"kind": "new-none"}), 2, peer_id(2)),
            vec![part],
        )
        .with_remote_payload(None),
        SyncBackendScenario::added_member(
            "added_member_when_remote_payload_is_missing",
            peer_id(2),
            gen_obj_id(1012),
            payload(serde_json::json!({"kind": "new-added-none"}), 2, peer_id(2)),
            vec![part],
        )
        .with_remote_payload(None),
        SyncBackendScenario::changed_object(
            "changed_object_applies_remote",
            peer_id(2),
            gen_obj_id(11),
            payload(serde_json::json!({"kind": "old"}), 1, peer_id(1)),
            payload(serde_json::json!({"kind": "new"}), 2, peer_id(2)),
            vec![part],
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
            vec![part, extra_part],
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
    peer_id: PeerId,
    host: Ctx,
    handle: crate::BigSyncWorkerHandle,
    stop: crate::StopToken,
    store: Arc<dyn HostPartStore>,
    observed_store: Arc<dyn ObservedStore>,
    restart_memory_store: Option<Arc<MemoryPartStore>>,
    sqlite_temp_dir: Option<tempfile::TempDir>,
}

impl NodeHarness {
    async fn connect_to(&self, remote: &NodeHarness) -> Res<()> {
        let client = Arc::new(MemoryRpcClient::new(
            Arc::clone(&self.world),
            Arc::clone(&self.store),
            remote.peer_id,
            Arc::clone(&remote.store),
        ));
        self.host
            .worker
            .set_peer(
                remote.peer_id,
                client,
                test_parts()
                    .iter()
                    .map(|&part| (part, TEST_BACKEND_ID.into()))
                    .collect(),
            )
            .await
    }

    async fn seed_obj(&self, obj: ObjId, payload: serde_json::Value) -> Res<()> {
        self.host.store.set_obj_payload(obj, payload).await?;
        self.host.store.add_obj_to_parts(obj, test_parts()).await?;
        Ok(())
    }

    async fn remove_obj(&self, obj: ObjId) -> Res<()> {
        self.host
            .store
            .remove_obj_from_part(obj, test_part())
            .await?;
        Ok(())
    }

    async fn wait_for_full_sync(
        &self,
        peer_ids: impl IntoIterator<Item = PeerId>,
        part_ids: impl IntoIterator<Item = PartId>,
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
        self.world.set_online(self.peer_id, false);
        self.stop.stop().await?;
        self.world.remove_store(self.peer_id);
        Ok(())
    }
}

pub(crate) fn peer_id(seed: u8) -> PeerId {
    PeerId(Byte32Id::new([seed; 32]))
}

pub(crate) fn payload(
    value: impl Into<serde_json::Value>,
    written_at: u64,
    writer_id: PeerId,
) -> serde_json::Value {
    lww_payload(value, written_at, writer_id)
}

pub(crate) fn gen_obj_id(seed: usize) -> ObjId {
    ObjId(Byte32Id::new(
        *blake3::hash(format!("test.{seed}").as_bytes()).as_bytes(),
    ))
}

async fn seed_objects(node: &NodeHarness, prefix: &str, count: usize) -> Res<Vec<ObjId>> {
    let mut objs = Vec::with_capacity(count);
    for ii in 0..count {
        let obj = ObjId(Byte32Id::new(
            *blake3::hash(format!("{prefix}.{ii}").as_bytes()).as_bytes(),
        ));
        node.seed_obj(
            obj,
            payload(
                serde_json::json!({ "ii": ii, "prefix": prefix }),
                ii as u64,
                node.peer_id,
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
                obj_id,
                payload(
                    serde_json::json!({"phase": "present", "ii": ii}),
                    ii as u64,
                    peer_id(1),
                ),
            )
            .await?;
        store.add_obj_to_parts(obj_id, vec![part_id]).await?;
        obj_ids.push(obj_id);
    }

    crate::part_store::contract::assert_root_bucket_contract(
        &store,
        part_id,
        seed,
        &obj_ids,
        &[],
        2,
    )
    .await?;

    let removed_obj_id = obj_ids[1];
    store.remove_obj_from_part(removed_obj_id, part_id).await?;
    let live_ids: Vec<_> = obj_ids
        .iter()
        .copied()
        .filter(|obj_id| *obj_id != removed_obj_id)
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
    let (start, end) = crate::part_store::obj_id_bounds_for_bucket(terminal);
    assert!(end.is_none(), "terminal bucket must not wrap");
    assert_eq!(
        start,
        crate::part_store::obj_id_bounds_for_bucket(terminal).0
    );
    let non_terminal = BuckId::new(2, 0);
    let (_, end) = crate::part_store::obj_id_bounds_for_bucket(non_terminal);
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
        obj_ids_a.push(*obj);
        obj_ids_b.push(*obj);
    }
    for ((_, payload), &obj_id) in objs.iter().zip(obj_ids_a.iter()) {
        store_a.set_obj_payload(obj_id, payload.clone()).await?;
        store_a.add_obj_to_parts(obj_id, vec![test_part()]).await?;
    }
    for ((_, payload), &obj_id) in objs.iter().rev().zip(obj_ids_b.iter().rev()) {
        store_b.set_obj_payload(obj_id, payload.clone()).await?;
        store_b.add_obj_to_parts(obj_id, vec![test_part()]).await?;
    }

    let a_initial = store_a
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id: test_part(),
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    let b_initial = store_b
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id: test_part(),
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
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
        .remove_obj_from_part(obj_ids_a[1], test_part())
        .await?;
    store_b
        .remove_obj_from_part(obj_ids_b[1], test_part())
        .await?;

    let a_final = store_a
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id: test_part(),
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    let b_final = store_b
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id: test_part(),
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);

    assert_eq!(a_final, b_final);
    Ok(())
}

async fn boot_node_with_store<S>(
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<S>,
    restart_memory_store: Option<Arc<MemoryPartStore>>,
) -> Res<NodeHarness>
where
    S: ObservedStore + HostPartStore + 'static,
{
    store.ensure_part(test_part()).await?;
    world.set_online(peer_id, true);
    world.register_store(peer_id, Arc::clone(&store));
    let store_for_worker: Arc<dyn HostPartStore> = Arc::clone(&store) as _;
    let observed_store: Arc<dyn ObservedStore> = Arc::clone(&store) as _;
    let backend: Arc<dyn SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id,
        Arc::clone(&store_for_worker),
        Arc::clone(&world),
    ));
    let (handle, stop) = crate::spawn_big_sync_worker(
        Arc::clone(&store_for_worker),
        [(TEST_BACKEND_ID.into(), backend)].into(),
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
        host,
        handle,
        stop,
    })
}

async fn boot_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(MemoryPartStore::new());
    boot_node_with_store(world, peer_id, Arc::clone(&store), Some(store)).await
}

async fn restart_node(world: Arc<TestWorld>, node: NodeHarness) -> Res<NodeHarness> {
    let NodeHarness {
        world: node_world,
        peer_id,
        restart_memory_store,
        host: _host,
        handle: _handle,
        stop,
        sqlite_temp_dir: _sqlite_temp_dir,
        ..
    } = node;
    node_world.set_online(peer_id, false);
    stop.stop().await?;
    node_world.remove_store(peer_id);
    let Some(memory_store) = restart_memory_store else {
        eyre::bail!("node is not restartable with a memory store");
    };
    boot_node_with_store(
        world,
        peer_id,
        Arc::clone(&memory_store),
        Some(memory_store),
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
    let expected_left_parts = [(part_id, TEST_BACKEND_ID.into())].into_iter().collect();
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

async fn wait_for_convergence(nodes: &[&NodeHarness], timeout: Duration) -> Res<()> {
    let deadline = std::time::Instant::now() + timeout;
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

        last_snapshot = Some(current);
        if std::time::Instant::now() >= deadline {
            return Err(ferr!(
                "timed out waiting for test nodes to converge: last_snapshot={last_snapshot:?}"
            ));
        }

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

async fn wait_for_idle(nodes: &[&NodeHarness], timeout: Duration) -> Res<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let mut current = Vec::with_capacity(nodes.len());
        for node in nodes {
            current.push(node.handle.snapshot().await?);
        }
        if current
            .iter()
            .all(|worker_snapshot| worker_snapshot.is_idle())
        {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(ferr!("timed out waiting for test nodes to become idle"));
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

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_preconnected_seeds_converge() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let left_obj = gen_obj_id(10);
    let right_obj = gen_obj_id(11);
    let left_payload = payload("left-a", 1, node_a.peer_id);
    let right_payload = payload("right-b", 1, node_b.peer_id);

    node_a.seed_obj(left_obj, left_payload.clone()).await?;
    node_b.seed_obj(right_obj, right_payload.clone()).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
    wait_for_convergence(&[&node_a], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    let obj = gen_obj_id(20);
    let created_payload = payload("connected-create", 1, node_b.peer_id);
    node_b.seed_obj(obj, created_payload.clone()).await?;

    wait_for_idle(&[&node_a], Duration::from_secs(30)).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));
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
    let created_payload = payload("wait-for-full-sync", 1, node_b.peer_id);

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let obj = gen_obj_id(21);
    node_b.seed_obj(obj, created_payload.clone()).await?;

    tokio::time::timeout(
        Duration::from_secs(30),
        node_a.wait_for_full_sync([node_b.peer_id], [part_id]),
    )
    .await
    .wrap_err(ERROR_CHANNEL)??;

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
    let base_payload = payload("base", 1, node_b.peer_id);
    let update_payload = payload("higher-update", 2, node_a.peer_id);
    node_b.seed_obj(obj, base_payload).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    node_a.seed_obj(obj, update_payload.clone()).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
    let obj_a_payload = payload("cursor-a-0", 1, node_a.peer_id);
    let obj_b_payload = payload("cursor-b-0", 1, node_b.peer_id);
    node_a.seed_obj(obj_a, obj_a_payload.clone()).await?;
    node_b.seed_obj(obj_b, obj_b_payload.clone()).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let rounds = 24;
    for round in 0..rounds {
        node_a
            .seed_obj(
                obj_a,
                payload(
                    format!("cursor-a-{round}"),
                    round as u64 + 2,
                    node_a.peer_id,
                ),
            )
            .await?;
        node_b
            .seed_obj(
                obj_b,
                payload(
                    format!("cursor-b-{round}"),
                    round as u64 + 2,
                    node_b.peer_id,
                ),
            )
            .await?;
        wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
            node_a.peer_id,
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
            node_b.peer_id,
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
    let base_payload = payload("base", 1, node_a.peer_id);
    let lower_payload = payload("lower-conflict", 2, node_a.peer_id);
    let higher_payload = payload("higher-conflict", 2, node_b.peer_id);
    node_a.seed_obj(obj, base_payload).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.seed_obj(obj, lower_payload.clone()),
        node_b.seed_obj(obj, higher_payload.clone()),
    )?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
        .seed_obj(obj, payload("delete-me", 1, node_a.peer_id))
        .await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    node_a.remove_obj(obj).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 0).await?;
    assert!(!snapshot_a.objs.contains_key(&obj));
    assert!(!snapshot_b.objs.contains_key(&obj));

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_direct_backend_adopts_remote_tombstone() -> Res<()> {
    let world = Arc::new(TestWorld::default());
    let peer_a = peer_id(1);
    let peer_b = peer_id(2);
    let store_a = Arc::new(MemoryPartStore::new());
    let store_b = Arc::new(MemoryPartStore::new());
    let store_b_dyn: Arc<dyn HostPartStore> = Arc::clone(&store_b) as _;

    world.register_store(peer_a, Arc::clone(&store_a));
    world.register_store(peer_b, Arc::clone(&store_b));

    let part = test_part();
    let obj = gen_obj_id(51);
    let live_payload = payload("live", 1, peer_a);

    store_a.set_obj_payload(obj, live_payload.clone()).await?;
    store_a.add_obj_to_parts(obj, vec![part]).await?;
    store_b.set_obj_payload(obj, live_payload).await?;
    store_b.add_obj_to_parts(obj, vec![part]).await?;
    store_a.remove_obj_from_part(obj, part).await?;

    let backend = MemorySyncBackend::new(peer_b, Arc::clone(&store_b_dyn), Arc::clone(&world));

    let err = backend
        .sync_obj(peer_a, obj, None)
        .await
        .expect_err("remote absence should be treated as a hard error for now");

    assert!(format!("{err:?}").contains("missing on remote"));
    assert_eq!(
        store_b.obj_payload(obj).await?,
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

    world.register_store(peer_a, Arc::clone(&store_a));
    world.register_store(peer_b, Arc::clone(&store_b));

    let part = test_part();
    let obj_a = gen_obj_id(52);
    let obj_b = gen_obj_id(53);
    let left_payload = payload("left-a", 1, peer_a);
    let right_payload = payload("right-b", 1, peer_b);

    store_a.set_obj_payload(obj_a, left_payload.clone()).await?;
    store_a.add_obj_to_parts(obj_a, vec![part]).await?;
    store_b
        .set_obj_payload(obj_b, right_payload.clone())
        .await?;
    store_b.add_obj_to_parts(obj_b, vec![part]).await?;

    let backend_a = MemorySyncBackend::new(peer_a, Arc::clone(&store_a_dyn), Arc::clone(&world));
    let backend_b = MemorySyncBackend::new(peer_b, Arc::clone(&store_b_dyn), Arc::clone(&world));

    let _ = backend_a
        .sync_obj(peer_b, obj_b, Some(right_payload.clone()))
        .await?;
    let _ = backend_b
        .sync_obj(peer_a, obj_a, Some(left_payload.clone()))
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
    let left_payload = payload("left", 1, node_a.peer_id);
    let right_payload = payload("right", 1, node_b.peer_id);

    node_a.seed_obj(left_obj, left_payload.clone()).await?;
    node_b.seed_obj(right_obj, right_payload.clone()).await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

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
    let payload_value = payload("idempotent", 1, node_a.peer_id);

    node_a.seed_obj(obj, payload_value.clone()).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let snapshot_before = node_a.snapshot().await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
    let left_payload = payload("order-left", 1, node_a.peer_id);
    let right_payload = payload("order-right", 1, node_b.peer_id);

    node_a.seed_obj(left_obj, left_payload).await?;
    node_b.seed_obj(right_obj, right_payload).await?;

    if connect_left_first {
        node_a.connect_to(&node_b).await?;
        node_b.connect_to(&node_a).await?;
    } else {
        node_b.connect_to(&node_a).await?;
        node_a.connect_to(&node_b).await?;
    }
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
async fn memory_sync_large_gap_uses_bucket_catchup() -> Res<()> {
    memory_sync_large_gap_uses_bucket_catchup_for_count(300, Duration::from_secs(15)).await
}

async fn memory_sync_large_gap_uses_bucket_catchup_for_count(
    obj_count: usize,
    timeout: Duration,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;
    let part_id = test_part();
    let mut stats_rx = node_a.handle.subscribe_stats();

    for ii in 0..obj_count {
        let obj_id = gen_obj_id(ii);
        node_b
            .seed_obj(
                obj_id,
                payload(serde_json::json!({ "ii": ii }), ii as u64, node_b.peer_id),
            )
            .await?;
    }

    node_a.connect_to(&node_b).await?;
    let deadline = std::time::Instant::now() + timeout;
    let snapshot = loop {
        let snapshot = node_a.snapshot().await?;
        if snapshot.objs.len() == obj_count
            && (0..obj_count).all(|ii| {
                let obj = gen_obj_id(ii);
                snapshot
                    .objs
                    .get(&obj)
                    .and_then(|obj| obj.payload.as_ref())
                    .is_some()
            })
        {
            break snapshot;
        }
        if std::time::Instant::now() >= deadline {
            return Err(ferr!(
                "timed out waiting for bucket catchup, saw {} objects",
                snapshot.objs.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(snapshot.objs.len(), obj_count);
    for ii in 0..obj_count {
        let obj = gen_obj_id(ii);
        let value = snapshot
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone())
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            value,
            payload(serde_json::json!({ "ii": ii }), ii as u64, node_b.peer_id)
        );
    }

    let cursor_deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = node_a.snapshot().await?;
        if snapshot
            .peer_part_cursors
            .get(&(node_b.peer_id, part_id))
            .copied()
            == Some(obj_count as u64)
        {
            break;
        }
        if std::time::Instant::now() >= cursor_deadline {
            return Err(ferr!(
                "timed out waiting for bucket cursor advance to {}",
                obj_count
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_large_gap_uses_bucket_catchup_1k() -> Res<()> {
    memory_sync_large_gap_uses_bucket_catchup_for_count(1_000, Duration::from_secs(30)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_large_gap_uses_bucket_catchup_10k() -> Res<()> {
    memory_sync_large_gap_uses_bucket_catchup_for_count(10_000, Duration::from_secs(90)).await
}

#[tokio::test(flavor = "multi_thread")]
// #[ignore = "slow bucket catchup case"]
async fn memory_sync_large_gap_uses_bucket_catchup_100k() -> Res<()> {
    memory_sync_large_gap_uses_bucket_catchup_for_count(100_000, Duration::from_secs(300)).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow bucket catchup case"]
async fn memory_sync_large_gap_uses_bucket_catchup_1m() -> Res<()> {
    memory_sync_large_gap_uses_bucket_catchup_for_count(1_000_000, Duration::from_secs(900)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_peer_restart_reconnects_cleanly() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = gen_obj_id(60);
    let before_restart = payload("before-restart", 1, node_a.peer_id);
    node_a.seed_obj(obj, before_restart.clone()).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id),
        node_b.host.worker.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

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
    let online_base = payload("online-base", 1, node_a.peer_id);
    let offline_a = payload("offline-a", 2, node_a.peer_id);
    node_a.seed_obj(obj, online_base).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id),
        node_b.host.worker.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    node_a.seed_obj(obj, offline_a.clone()).await?;
    wait_for_convergence(&[&node_a], Duration::from_secs(30)).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
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
    let shared_from_third = payload("shared-from-third", 1, node_c.peer_id);
    node_c.seed_obj(obj, shared_from_third.clone()).await?;

    tokio::try_join!(node_a.connect_to(&node_c), node_c.connect_to(&node_a))?;
    tokio::try_join!(node_b.connect_to(&node_c), node_c.connect_to(&node_b))?;
    wait_for_convergence(&[&node_a, &node_b, &node_c], Duration::from_secs(30)).await?;

    let mut stats_rx = node_a.handle.subscribe_stats();
    drain_stats(&mut stats_rx);

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b, &node_c], Duration::from_secs(30)).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PartStale { .. })));

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
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id),
        node_b.host.worker.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let mut rng = StdRng::seed_from_u64(0x3b1a_5eed);
    let mut deleted_mask = vec![false; objs.len()];
    let mut delete_idxs: Vec<_> = (0..objs.len()).collect();
    delete_idxs.shuffle(&mut rng);
    for ii in delete_idxs.into_iter().take(objs.len() / 2) {
        deleted_mask[ii] = true;
        node_a.remove_obj(objs[ii]).await?;
    }

    wait_for_idle(&[&node_a], Duration::from_secs(30)).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

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
                node_a.peer_id,
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
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id),
        node_b.host.worker.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let mut rng = StdRng::seed_from_u64(0x5eed_face);
    let mut expected_payloads = Vec::with_capacity(objs.len());
    for (ii, &obj) in objs.iter().enumerate() {
        if rng.random_bool(0.50) {
            let expected = payload(
                serde_json::json!({ "ii": ii, "prefix": "offline-evolve.a" }),
                1_000 + ii as u64,
                node_a.peer_id,
            );
            node_a.seed_obj(obj, expected.clone()).await?;
            expected_payloads.push(Some(expected));
        } else {
            expected_payloads.push(Some(payload(
                serde_json::json!({ "ii": ii, "prefix": "offline-evolve" }),
                ii as u64,
                node_a.peer_id,
            )));
        }
    }

    wait_for_idle(&[&node_a], Duration::from_secs(30)).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

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

#[cfg(test)]
mod stress;
