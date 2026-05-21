use crate::{interlude::*, SyncBackend};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use big_sync_core::part_store::CursorIndex;
use big_sync_core::part_store::ObjPayload;
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

use crate::part_store::memory::{MemoryPartStore, MemoryPartStoreSnapshot};
use crate::part_store::{HostPartitionStore, ObjStoreLease, StoreMutationOutcome};
use crate::{BackendId, Ctx, SyncTaskRunOutcome};

const TEST_BACKEND_ID: BackendId = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedObjSnapshot {
    payload: Option<ObjPayload>,
    parts: BTreeSet<PartId>,
}

#[derive(Debug, Clone)]
struct ObservedStoreSnapshot {
    objs: BTreeMap<ObjId, ObservedObjSnapshot>,
    peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

impl PartialEq for ObservedStoreSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.objs == other.objs
    }
}

impl Eq for ObservedStoreSnapshot {}

impl From<MemoryPartStoreSnapshot> for ObservedStoreSnapshot {
    fn from(value: MemoryPartStoreSnapshot) -> Self {
        Self {
            objs: value
                .objs
                .into_iter()
                .map(|(obj, snapshot)| {
                    (
                        obj,
                        ObservedObjSnapshot {
                            payload: snapshot.payload,
                            parts: snapshot.parts,
                        },
                    )
                })
                .collect(),
            peer_part_cursors: value.peer_part_cursors,
        }
    }
}

fn test_part() -> PartId {
    PartId(Byte32Id::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

fn test_parts() -> Vec<PartId> {
    vec![test_part()]
}

#[derive(Default)]
struct TestWorld {
    stores: Mutex<HashMap<PeerId, Arc<MemoryPartStore>>>,
    online: Mutex<HashSet<PeerId>>,
}

impl TestWorld {
    fn register_store(&self, peer_id: PeerId, store: Arc<MemoryPartStore>) {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let old = stores.insert(peer_id, store);
        assert!(old.is_none(), "fishy");
        self.set_online(peer_id, true);
    }

    fn store_for_peer(&self, peer_id: PeerId) -> Arc<MemoryPartStore> {
        self.stores
            .lock()
            .expect(ERROR_MUTEX)
            .get(&peer_id)
            .cloned()
            .expect(ERROR_IMPOSSIBLE)
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
    source_part_store: Arc<MemoryPartStore>,
    target_peer_id: PeerId,
    target_part_store: Arc<MemoryPartStore>,
}

impl MemoryRpcClient {
    fn new(
        world: Arc<TestWorld>,
        source_part_store: Arc<MemoryPartStore>,
        target_peer_id: PeerId,
        target_part_store: Arc<MemoryPartStore>,
    ) -> Self {
        Self {
            world,
            source_part_store,
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
        let receiver = self.target_part_store.subscribe(req).await??;
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

struct MemorySyncBackend {
    local_peer_id: PeerId,
    local_part_store: Arc<MemoryPartStore>,
    world: Arc<TestWorld>,
}

impl MemorySyncBackend {
    fn new(
        local_peer_id: PeerId,
        local_part_store: Arc<MemoryPartStore>,
        world: Arc<TestWorld>,
    ) -> Self {
        Self {
            local_peer_id,
            local_part_store,
            world,
        }
    }
}

#[async_trait]
impl SyncBackend for MemorySyncBackend {
    #[tracing::instrument(
        skip(self, part_hints, lease),
        fields(
            part_hint_count = part_hints.len()
        )
    )]
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        lease: ObjStoreLease,
        obj_id: ObjId,
        part_hints: Vec<PartId>,
    ) -> Res<SyncTaskRunOutcome> {
        if !self.world.is_online(peer_id) {
            eyre::bail!("peer is offline");
        }
        loop {
            let remote_part_store = self.world.store_for_peer(peer_id);
            let (local_payload, remote_payload) = tokio::try_join!(
                self.local_part_store.obj_payload(obj_id),
                remote_part_store.obj_payload(obj_id)
            )?;
            let (local_payload, remote_payload) = match (local_payload, remote_payload) {
                (Some(_), None) | (None, None) => eyre::bail!("missing on remote"),
                (None, Some(payload)) => {
                    match self
                        .local_part_store
                        .upsert_obj(obj_id, payload, part_hints, Some(lease))
                        .await?
                    {
                        StoreMutationOutcome::Applied => {
                            return Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                                obj_id,
                                deets: big_sync_core::SyncCompletionDeets::AddedMember,
                            }))
                        }
                        StoreMutationOutcome::Stale => return Ok(SyncTaskRunOutcome::Stale),
                    }
                }
                (Some(local), Some(remote)) => (local, remote),
            };
        }
        Ok(SyncTaskRunOutcome::Stale)
    }
}

struct NodeHarness {
    world: Arc<TestWorld>,
    peer_id: PeerId,
    host: Ctx,
    handle: crate::BigSyncWorkerHandle,
    stop: crate::StopToken,
    store: Arc<MemoryPartStore>,
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
                    .map(|&part| (part, TEST_BACKEND_ID))
                    .collect(),
            )
            .await
    }

    async fn seed_obj(&self, obj: ObjId, payload: serde_json::Value) -> Res<()> {
        self.host
            .store
            .upsert_obj(obj, payload, test_parts(), None)
            .await
            .map(|outcome| assert_eq!(outcome, StoreMutationOutcome::Applied))
    }

    async fn remove_obj(&self, obj: ObjId) -> Res<()> {
        self.host
            .store
            .remove_obj_from_part(obj, test_part(), None)
            .await
            .map(|outcome| assert_eq!(outcome, StoreMutationOutcome::Applied))
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
        Ok(self.store.snapshot().await?.into())
    }

    async fn stop(self) -> Res<()> {
        self.world.set_online(self.peer_id, false);
        self.stop.stop().await?;
        Ok(())
    }
}

fn peer_id(seed: u8) -> PeerId {
    PeerId(Byte32Id::new([seed; 32]))
}

fn payload(label: &str) -> serde_json::Value {
    serde_json::Value::from(label)
}

fn gen_obj_id(seed: usize) -> ObjId {
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
        node.seed_obj(obj, serde_json::json!({ "ii": ii, "prefix": prefix }))
            .await?;
        objs.push(obj);
    }
    Ok(objs)
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_part_store_root_bucket_contract() -> Res<()> {
    let store = crate::part_store::memory::MemoryPartStore::new(peer_id(1));
    let part_id = test_part();
    let seed = FingerprintSeed::new(1, 2);
    let mut obj_ids = Vec::new();
    for ii in 0..5u8 {
        let obj_id = gen_obj_id((90 + ii) as usize);
        store
            .upsert_obj(
                obj_id,
                serde_json::json!({"phase": "present", "ii": ii}),
                vec![part_id],
                None,
            )
            .await?;
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
    store
        .remove_obj_from_part(removed_obj_id, part_id, None)
        .await?;
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
    let store_a = MemoryPartStore::new(peer_id(1));
    let store_b = MemoryPartStore::new(peer_id(2));

    let objs = [
        (gen_obj_id(1), serde_json::json!({"obj": 1})),
        (gen_obj_id(2), serde_json::json!({"obj": 2})),
        (gen_obj_id(3), serde_json::json!({"obj": 3})),
    ];
    let mut obj_ids_a = Vec::new();
    let mut obj_ids_b = Vec::new();
    for (obj, _) in &objs {
        obj_ids_a.push(*obj);
        obj_ids_b.push(*obj);
    }
    for ((_, payload), &obj_id) in objs.iter().zip(obj_ids_a.iter()) {
        store_a
            .upsert_obj(obj_id, payload.clone(), vec![test_part()], None)
            .await?;
    }
    for ((_, payload), &obj_id) in objs.iter().rev().zip(obj_ids_b.iter().rev()) {
        store_b
            .upsert_obj(obj_id, payload.clone(), vec![test_part()], None)
            .await?;
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
        .remove_obj_from_part(obj_ids_a[1], test_part(), None)
        .await?;
    store_b
        .remove_obj_from_part(obj_ids_b[1], test_part(), None)
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

async fn boot_node_with_store(
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<MemoryPartStore>,
) -> Res<NodeHarness> {
    world.set_online(peer_id, true);
    let store_for_worker = Arc::clone(&store) as _;
    let backend: Arc<dyn SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id,
        Arc::clone(&store),
        Arc::clone(&world),
    ));
    let (handle, stop) =
        crate::spawn_big_sync_worker(store_for_worker, [(TEST_BACKEND_ID, backend)].into())?;
    let host = Ctx {
        store: Arc::clone(&store) as _,
        worker: handle.clone(),
    };

    Ok(NodeHarness {
        world,
        peer_id,
        store,
        host,
        handle,
        stop,
    })
}

async fn boot_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(MemoryPartStore::new(peer_id));
    world.register_store(peer_id, Arc::clone(&store));
    boot_node_with_store(world, peer_id, store).await
}

async fn restart_node(world: Arc<TestWorld>, node: NodeHarness) -> Res<NodeHarness> {
    let NodeHarness {
        world: node_world,
        peer_id,
        store,
        host: _host,
        handle,
        stop,
    } = node;
    node_world.set_online(peer_id, false);
    stop.stop().await?;
    drop(handle);
    boot_node_with_store(world, peer_id, store).await
}

async fn assert_two_node_alignment(
    left: &NodeHarness,
    right: &NodeHarness,
    expected_obj_count: usize,
) -> Res<(ObservedStoreSnapshot, ObservedStoreSnapshot)> {
    let worker_left = left.handle.snapshot().await?;
    let worker_right = right.handle.snapshot().await?;
    let part_id = test_part();
    let expected_left_parts = [(part_id, TEST_BACKEND_ID)].into_iter().collect();
    let expected_right_parts = [(part_id, TEST_BACKEND_ID)].into_iter().collect();
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

        if last_snapshot.as_ref().is_some_and(|prev| prev == &current) {
            stable_rounds += 1;
            if stable_rounds >= 8 {
                return Ok(());
            }
        } else {
            stable_rounds = 1;
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

    node_a.seed_obj(left_obj, payload("left-a")).await?;
    node_b.seed_obj(right_obj, payload("right-b")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, _) = assert_two_node_alignment(&node_a, &node_b, 2).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&left_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("left-a"))
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&right_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("right-b"))
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
    node_b.seed_obj(obj, payload("connected-create")).await?;

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
        Some(payload("connected-create"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("connected-create"))
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

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let obj = gen_obj_id(21);
    node_b.seed_obj(obj, payload("wait-for-full-sync")).await?;

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
        Some(payload("wait-for-full-sync"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("wait-for-full-sync"))
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
    node_b.seed_obj(obj, payload("base")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    node_a.seed_obj(obj, payload("higher-update")).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-update"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-update"))
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
    node_a.seed_obj(obj_a, payload("cursor-a-0")).await?;
    node_b.seed_obj(obj_b, payload("cursor-b-0")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let rounds = 24;
    for round in 0..rounds {
        node_a
            .seed_obj(obj_a, payload(&format!("cursor-a-{round}")))
            .await?;
        node_b
            .seed_obj(obj_b, payload(&format!("cursor-b-{round}")))
            .await?;
        wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    }

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 2).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj_a)
            .and_then(|obj| obj.payload.clone()),
        Some(payload(&format!("cursor-a-{}", rounds - 1)))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(payload(&format!("cursor-b-{}", rounds - 1)))
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
    node_a.seed_obj(obj, payload("base")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.seed_obj(obj, payload("lower-conflict")),
        node_b.seed_obj(obj, payload("higher-conflict")),
    )?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-conflict"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-conflict"))
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
    node_a.seed_obj(obj, payload("delete-me")).await?;

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
    let store_a = Arc::new(MemoryPartStore::new(peer_a));
    let store_b = Arc::new(MemoryPartStore::new(peer_b));

    world.register_store(peer_a, Arc::clone(&store_a));
    world.register_store(peer_b, Arc::clone(&store_b));

    let part = test_part();
    let obj = gen_obj_id(51);

    store_a
        .upsert_obj(obj, payload("live"), vec![part], None)
        .await?;
    store_b
        .upsert_obj(obj, payload("live"), vec![part], None)
        .await?;
    store_a.remove_obj_from_part(obj, part, None).await?;

    let backend = MemorySyncBackend::new(peer_b, Arc::clone(&store_b), Arc::clone(&world));

    let _ = backend
        .sync_obj(
            peer_a,
            store_b.get_obj_lease(obj).await?,
            obj,
            [part].into(),
        )
        .await?;

    assert_eq!(store_b.obj_payload(obj).await?, None);
    assert!(store_b.obj_parts(obj).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_direct_backend_cross_replication_is_symmetric() -> Res<()> {
    let world = Arc::new(TestWorld::default());
    let peer_a = peer_id(1);
    let peer_b = peer_id(2);
    let store_a = Arc::new(MemoryPartStore::new(peer_a));
    let store_b = Arc::new(MemoryPartStore::new(peer_b));

    world.register_store(peer_a, Arc::clone(&store_a));
    world.register_store(peer_b, Arc::clone(&store_b));

    let part = test_part();
    let obj_a = gen_obj_id(52);
    let obj_b = gen_obj_id(53);

    store_a
        .upsert_obj(obj_a, payload("left-a"), vec![part], None)
        .await?;
    store_b
        .upsert_obj(obj_b, payload("right-b"), vec![part], None)
        .await?;

    let backend_a = MemorySyncBackend::new(peer_a, Arc::clone(&store_a), Arc::clone(&world));
    let backend_b = MemorySyncBackend::new(peer_b, Arc::clone(&store_b), Arc::clone(&world));

    let _ = backend_a
        .sync_obj(
            peer_b,
            store_a.get_obj_lease(obj_b).await?,
            obj_b,
            [part].into_iter().collect(),
        )
        .await?;
    let _ = backend_b
        .sync_obj(
            peer_a,
            store_b.get_obj_lease(obj_b).await?,
            obj_b,
            [part].into_iter().collect(),
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
        Some(payload("left-a"))
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("right-b"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_a)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("left-a"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj_b)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("right-b"))
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

    node_a.seed_obj(left_obj, payload("left")).await?;
    node_b.seed_obj(right_obj, payload("right")).await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let (snapshot_a, snapshot_b) = assert_same_observed_state(&node_a, &node_b).await?;
    assert_eq!(snapshot_a.objs.len(), 2);
    assert_eq!(
        snapshot_a
            .objs
            .get(&left_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("left"))
    );
    assert_eq!(
        snapshot_a
            .objs
            .get(&right_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("right"))
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

    node_a.seed_obj(obj, payload("idempotent")).await?;
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

    node_a.seed_obj(left_obj, payload("order-left")).await?;
    node_b.seed_obj(right_obj, payload("order-right")).await?;

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
            .seed_obj(obj_id, serde_json::json!({ "ii": ii }))
            .await?;
    }

    node_a.connect_to(&node_b).await?;
    let deadline = std::time::Instant::now() + timeout;
    let snapshot = loop {
        let snapshot = node_a.snapshot().await?;
        if snapshot.objs.len() == obj_count {
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
        assert_eq!(value, serde_json::json!({ "ii": ii }));
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
    node_a.seed_obj(obj, payload("before-restart")).await?;
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
        Some(payload("before-restart"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("before-restart"))
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
    node_a.seed_obj(obj, payload("online-base")).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.worker.remove_peer(node_b.peer_id),
        node_b.host.worker.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let NodeHarness {
        world: node_b_world,
        peer_id,
        store,
        host: _host,
        handle,
        stop,
    } = node_b;
    node_b_world.set_online(peer_id, false);
    stop.stop().await?;
    drop(handle);
    node_a.seed_obj(obj, payload("offline-a")).await?;
    wait_for_convergence(&[&node_a], Duration::from_secs(30)).await?;

    let node_b = boot_node_with_store(Arc::clone(&world), peer_id, store).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let _stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("offline-a"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("offline-a"))
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
    node_c.seed_obj(obj, payload("shared-from-third")).await?;

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
        Some(payload("shared-from-third"))
    );
    assert_eq!(
        snapshot_b
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("shared-from-third"))
    );
    assert_eq!(
        snapshot_c
            .objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("shared-from-third"))
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

    let NodeHarness {
        world: node_b_world,
        peer_id,
        store,
        host: _host,
        handle,
        stop,
    } = node_b;
    node_b_world.set_online(peer_id, false);
    stop.stop().await?;
    drop(handle);

    let mut rng = StdRng::seed_from_u64(0x3b1a_5eed);
    let mut deleted_mask = vec![false; objs.len()];
    let mut delete_idxs: Vec<_> = (0..objs.len()).collect();
    delete_idxs.shuffle(&mut rng);
    for ii in delete_idxs.into_iter().take(objs.len() / 2) {
        deleted_mask[ii] = true;
        node_a.remove_obj(objs[ii]).await?;
    }

    wait_for_idle(&[&node_a], Duration::from_secs(30)).await?;
    let node_b = boot_node_with_store(Arc::clone(&world), peer_id, store).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let _stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;

    let (snapshot_a, snapshot_b) =
        assert_two_node_alignment(&node_a, &node_b, objs.len() / 2).await?;
    for (ii, obj) in objs.iter().enumerate() {
        let expected = if deleted_mask[ii] {
            None
        } else {
            Some(serde_json::json!({ "ii": ii, "prefix": "half-delete" }))
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
            let expected = serde_json::json!({ "ii": ii, "prefix": "offline-evolve.a" });
            node_a.seed_obj(obj, expected.clone()).await?;
            expected_payloads.push(Some(expected));
        } else {
            expected_payloads.push(Some(
                serde_json::json!({ "ii": ii, "prefix": "offline-evolve" }),
            ));
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
