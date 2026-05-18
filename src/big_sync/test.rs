use crate::interlude::*;

use std::collections::HashMap;
use std::sync::Mutex;

use big_sync_core::mpsc::{self, Receiver};
use big_sync_core::rpc::{
    BigSyncRpcResult, BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest,
    LeafBucketsError, LeafBucketsRequest, LeafBucketResult, ListPartsError, PeerSummaryRequest,
    PeerSummaryResult, SubEvent, SubPartsRequest,
};
use big_sync_core::{
    BuckId, Byte32Id, FingerprintSeed, ObjId, PartId, PeerId, SyncCompletionDeets,
    SyncStatEvent, SyncTaskCompletion, SyncTaskDeets,
};
use big_sync_core::part_store::ObjPayload;

use crate::part_store::HostPartitionStore;
use crate::part_store::MemoryPartStoreSnapshot;
use crate::{
    BackendId, BigSyncHost, ScopeRef, ScopedIdResolver, ScopedObjRef, ScopedPartRef,
    SharedPartitionStore,
};

const TEST_BACKEND_ID: BackendId = 0;

fn test_scope() -> ScopeRef {
    ScopeRef::new(Url::parse("big-sync-test://drepo_test").expect(ERROR_IMPOSSIBLE))
}

fn test_part() -> ScopedPartRef {
    ScopedPartRef::new(test_scope(), "core.docs")
}

fn test_parts() -> Vec<ScopedPartRef> {
    vec![test_part()]
}

#[derive(Default)]
struct TestWorld {
    stores: Mutex<HashMap<PeerId, Arc<crate::part_store::MemoryPartStore>>>,
    online: Mutex<HashSet<PeerId>>,
}

impl TestWorld {
    fn register_store(&self, peer_id: PeerId, store: Arc<crate::part_store::MemoryPartStore>) {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let old = stores.insert(peer_id, store);
        assert!(old.is_none(), "fishy");
        self.set_online(peer_id, true);
    }

    fn store_for_peer(&self, peer_id: PeerId) -> Arc<crate::part_store::MemoryPartStore> {
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
    source_part_store: Arc<crate::part_store::MemoryPartStore>,
    target_peer_id: PeerId,
    target_part_store: Arc<crate::part_store::MemoryPartStore>,
}

impl MemoryRpcClient {
    fn new(
        world: Arc<TestWorld>,
        source_part_store: Arc<crate::part_store::MemoryPartStore>,
        target_peer_id: PeerId,
        target_part_store: Arc<crate::part_store::MemoryPartStore>,
    ) -> Self {
        Self {
            world,
            source_part_store,
            target_peer_id,
            target_part_store,
        }
    }

    async fn map_local_summary_req(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<(HashSet<PartId>, HashMap<PartId, PartId>)> {
        let mut remote_parts = HashSet::new();
        let mut remote_to_local = HashMap::new();
        for local_part_id in parts {
            let scoped_part = self.source_part_store.scoped_part(local_part_id).await?;
            let remote_part_id = self.target_part_store.resolve_part(&scoped_part).await?;
            remote_parts.insert(remote_part_id);
            let old = remote_to_local.insert(remote_part_id, local_part_id);
            assert!(old.is_none(), "fishy");
        }
        Ok((remote_parts, remote_to_local))
    }

    async fn map_remote_event(&self, evt: SubEvent) -> Res<SubEvent> {
        let transition = match evt {
            SubEvent::Upserted(transition) => {
                let scoped_part = self
                    .target_part_store
                    .scoped_part(transition.part_id)
                    .await?;
                let scoped_obj = self.target_part_store.scoped_obj(transition.obj_id).await?;
                let part_id = self.source_part_store.resolve_part(&scoped_part).await?;
                let obj_id = self.source_part_store.resolve_obj(&scoped_obj).await?;
                return Ok(SubEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                    cursor: transition.cursor,
                    part_id,
                    obj_id,
                    payload: transition.payload,
                }));
            }
            SubEvent::Deleted(transition) => transition,
            SubEvent::ReplayComplete => return Ok(SubEvent::ReplayComplete),
        };
        let scoped_part = self
            .target_part_store
            .scoped_part(transition.part_id)
            .await?;
        let scoped_obj = self.target_part_store.scoped_obj(transition.obj_id).await?;
        let part_id = self.source_part_store.resolve_part(&scoped_part).await?;
        let obj_id = self.source_part_store.resolve_obj(&scoped_obj).await?;
        Ok(SubEvent::Deleted(big_sync_core::rpc::ObjRemoved {
            cursor: transition.cursor,
            part_id,
            obj_id,
        }))
    }

    async fn map_remote_leaf_bucket_result(
        &self,
        result: LeafBucketResult,
    ) -> Res<LeafBucketResult> {
        let mut bucks = HashMap::new();
        for (buck_id, items) in result.bucks {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let scoped_obj = self.target_part_store.scoped_obj(item.obj_id).await?;
                out.push(BucketObjPageEntry {
                    obj_id: self.source_part_store.resolve_obj(&scoped_obj).await?,
                    dead: item.dead,
                    fp: item.fp,
                });
            }
            bucks.insert(buck_id, out);
        }
        Ok(LeafBucketResult {
            seed: result.seed,
            bucks,
        })
    }
}

#[async_trait]
impl crate::rpc::HostBigRpcClient for MemoryRpcClient {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let (remote_parts, remote_to_local) = self.map_local_summary_req(req.parts).await?;
        let remote_parts = self
            .target_part_store
            .summarize_parts(remote_parts)
            .await??;
        let parts = remote_parts
            .into_iter()
            .map(|(remote_part_id, summary)| {
                let local_part_id = remote_to_local
                    .get(&remote_part_id)
                    .copied()
                    .expect(ERROR_IMPOSSIBLE);
                (local_part_id, summary)
            })
            .collect();
        Ok(Ok(Ok(PeerSummaryResult {
            parts,
            deepest_bucket_level: BuckId::MAX_LEVEL,
        })))
    }

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>> {
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let mut remote_reqs = Vec::new();
        for req in req.parts {
            let scoped_part = self.source_part_store.scoped_part(req.part_id).await?;
            let remote_part_id = self.target_part_store.resolve_part(&scoped_part).await?;
            remote_reqs.push(big_sync_core::rpc::PartStreamCursorRequest {
                part_id: remote_part_id,
                cursor: req.cursor,
            });
        }
        let receiver = self
            .target_part_store
            .subscribe(SubPartsRequest { parts: remote_reqs })
            .await??;
        let (forward_tx, forward_rx) =
            mpsc::unbounded("MemoryRpcClient".into(), "BigSyncMachine".into());
        let client = self.clone();
        tokio::spawn(async move {
            let mut receiver = receiver;
            while let Some(evt) = receiver.recv().await {
                let evt = client.map_remote_event(evt).await.expect(ERROR_IMPOSSIBLE);
                if forward_tx.send(evt).await.is_err() {
                    break;
                }
            }
        });
        Ok(Ok(Ok(forward_rx)))
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let scoped_part = self.source_part_store.scoped_part(req.part_id).await?;
        let remote_part_id = self.target_part_store.resolve_part(&scoped_part).await?;
        let response = self
            .target_part_store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: remote_part_id,
                offset: req.offset,
                since: req.since,
                limit_hint: req.limit_hint,
            })
            .await?;
        Ok(Ok(response))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let scoped_part = self.source_part_store.scoped_part(req.part_id).await?;
        let remote_part_id = self.target_part_store.resolve_part(&scoped_part).await?;
        let items = self
            .target_part_store
            .leaf_buckets(LeafBucketsRequest {
                part_id: remote_part_id,
                since: req.since,
                buckets: req.buckets,
                seed: req.seed,
            })
            .await??;
        Ok(Ok(Ok(self.map_remote_leaf_bucket_result(items).await?)))
    }
}

struct MemorySyncBackend {
    local_peer_id: PeerId,
    local_part_store: Arc<crate::part_store::MemoryPartStore>,
    world: Arc<TestWorld>,
}

impl MemorySyncBackend {
    fn new(
        local_peer_id: PeerId,
        local_part_store: Arc<crate::part_store::MemoryPartStore>,
        world: Arc<TestWorld>,
    ) -> Self {
        Self {
            local_peer_id,
            local_part_store,
            world,
        }
    }

    async fn last_synced_remote_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
    ) -> Res<Option<Option<ObjPayload>>> {
        self.local_part_store
            .get_peer_obj_payload(peer_id, obj_id)
            .await
    }

    async fn set_last_synced_remote_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        payload: Option<ObjPayload>,
    ) -> Res<()> {
        self.local_part_store
            .set_peer_obj_payload(peer_id, obj_id, payload)
            .await
    }
}

#[async_trait]
impl crate::SyncBackend for MemorySyncBackend {
    async fn run(&self, task: SyncTaskDeets) -> Res<Vec<SyncTaskCompletion>> {
        let remote_part_store = self.world.store_for_peer(task.peer_id);
        let local_part_id = task.part_hints.first().copied().expect(ERROR_IMPOSSIBLE);
        let scoped_part = self.local_part_store.scoped_part(local_part_id).await?;
        let remote_part_id = remote_part_store.resolve_part(&scoped_part).await?;
        let scoped_obj = self.local_part_store.scoped_obj(task.obj_id).await?;
        let remote_obj_id = remote_part_store.resolve_obj(&scoped_obj).await?;
        let local_payload = self.local_part_store.obj_payload(task.obj_id).await?;
        let remote_payload = remote_part_store.obj_payload(remote_obj_id).await?;
        let local_parts = self.local_part_store.obj_parts(task.obj_id).await?;
        let remote_parts = remote_part_store.obj_parts(remote_obj_id).await?;
        let local_has_part = local_parts.contains(&local_part_id);
        let remote_has_part = remote_parts.contains(&remote_part_id);
        let last_synced = self
            .last_synced_remote_payload(task.peer_id, task.obj_id)
            .await?;
        let local_is_synced = last_synced.as_ref() == Some(&local_payload);
        let remote_is_synced = last_synced.as_ref() == Some(&remote_payload);
        let completion = match (local_payload.clone(), remote_payload.clone()) {
            (Some(local), Some(remote)) if local == remote => SyncCompletionDeets::Noop,
            (_, remote) if local_is_synced && !remote_is_synced => {
                if let Some(remote) = remote {
                    self.local_part_store
                        .sync_upsert_obj(task.obj_id, remote, task.part_hints.clone())
                        .await?;
                    if local_has_part {
                        SyncCompletionDeets::ChangedObject
                    } else {
                        SyncCompletionDeets::AddedMember
                    }
                } else {
                    self.local_part_store
                        .remove_obj_from_part(task.obj_id, local_part_id)
                        .await?;
                    SyncCompletionDeets::Noop
                }
            }
            (_, remote) if remote_is_synced && !local_is_synced => {
                let _ = remote;
                if local_has_part {
                    SyncCompletionDeets::ChangedObject
                } else if remote_has_part {
                    SyncCompletionDeets::AddedMember
                } else {
                    SyncCompletionDeets::Noop
                }
            }
            (Some(_local), Some(remote)) => {
                if self.local_peer_id < task.peer_id {
                    self.local_part_store
                        .sync_upsert_obj(task.obj_id, remote.clone(), task.part_hints.clone())
                        .await?;
                    if local_has_part {
                        SyncCompletionDeets::ChangedObject
                    } else if remote_has_part {
                        SyncCompletionDeets::AddedMember
                    } else {
                        SyncCompletionDeets::Noop
                    }
                } else {
                    let _ = remote;
                    if local_has_part {
                        SyncCompletionDeets::ChangedObject
                    } else if remote_has_part {
                        SyncCompletionDeets::AddedMember
                    } else {
                        SyncCompletionDeets::Noop
                    }
                }
            }
            (Some(_local), None) => SyncCompletionDeets::ChangedObject,
            (None, Some(remote)) => {
                self.local_part_store
                    .sync_upsert_obj(task.obj_id, remote, task.part_hints.clone())
                    .await?;
                SyncCompletionDeets::AddedMember
            }
            (None, None) => SyncCompletionDeets::Noop,
        };

        let new_local_payload = self.local_part_store.obj_payload(task.obj_id).await?;
        self.set_last_synced_remote_payload(task.peer_id, task.obj_id, new_local_payload.clone())
            .await?;
        Ok(vec![SyncTaskCompletion {
            obj_id: task.obj_id,
            deets: completion,
        }])
    }
}

struct NodeHarness {
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<crate::part_store::MemoryPartStore>,
    host: BigSyncHost<crate::part_store::MemoryPartStore>,
    handle: crate::BigSyncWorkerHandle,
    stop: crate::StopToken,
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
            .set_peer(remote.peer_id, client, test_parts())
            .await
    }

    async fn seed_obj(&self, obj: &ScopedObjRef, payload: serde_json::Value) -> Res<()> {
        self.host.upsert_obj(obj, payload, test_parts()).await
    }

    async fn remove_obj(&self, obj: &ScopedObjRef) -> Res<()> {
        self.host.remove_obj_from_part(obj, &test_part()).await
    }

    async fn snapshot(&self) -> Res<MemoryPartStoreSnapshot> {
        self.store.snapshot().await
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

fn scoped_obj(seed: u8) -> ScopedObjRef {
    ScopedObjRef::new(test_scope(), format!("obj.{seed}"))
}

fn payload(label: &str) -> serde_json::Value {
    serde_json::Value::from(label)
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_part_store_bucket_includes_removed_members() -> Res<()> {
    let store = crate::part_store::MemoryPartStore::default();
    let part_id = store.resolve_part(&test_part()).await?;
    let obj = scoped_obj(99);
    let obj_id = store.resolve_obj(&obj).await?;
    store
        .upsert_obj(
            obj_id,
            serde_json::json!({"phase": "present"}),
            vec![part_id],
        )
        .await?;
    let present_root = store
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id,
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    assert_eq!(present_root.id, BuckId::ROOT);
    assert_eq!(present_root.len, 1);
    assert_eq!(present_root.live_count, 1);

    store.remove_obj_from_part(obj_id, part_id).await?;
    let removed_root = store
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id,
            offset: BuckId::ROOT,
            since: 0,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??
        .into_iter()
        .next()
        .expect(ERROR_IMPOSSIBLE);
    let removed_items = store
        .leaf_buckets(LeafBucketsRequest {
            part_id,
            since: 0,
            buckets: vec![BuckId::ROOT],
            seed: FingerprintSeed::new(1, 2),
        })
        .await??;
    let root_items = removed_items.bucks.get(&BuckId::ROOT).expect(ERROR_IMPOSSIBLE);
    assert_eq!(root_items.len(), 1);
    assert_eq!(root_items[0].obj_id, obj_id);
    assert!(root_items[0].dead);

    let removed_since = store
        .get_changed_buckets(GetChangedBucketsRequest {
            part_id,
            offset: BuckId::ROOT,
            since: removed_root.changed_at,
            limit_hint: 8 * u32::from(BuckId::ARITY),
        })
        .await??;

    assert_ne!(present_root.fp, removed_root.fp);
    assert_eq!(removed_root.len, 1);
    assert_eq!(removed_root.live_count, 0);
    assert!(removed_since.is_empty());
    Ok(())
}

async fn boot_node_with_store(
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<crate::part_store::MemoryPartStore>,
) -> Res<NodeHarness> {
    world.set_online(peer_id, true);
    store.resolve_part(&test_part()).await?;
    let store_for_worker: SharedPartitionStore = Arc::clone(&store) as _;
    let backend: Arc<dyn crate::SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id,
        Arc::clone(&store),
        Arc::clone(&world),
    ));
    let (handle, stop) =
        crate::spawn_big_sync_worker(store_for_worker, [(TEST_BACKEND_ID, backend)].into())?;
    let host = BigSyncHost::new(
        Arc::clone(&store) as _,
        Arc::clone(&store),
        handle.clone(),
        TEST_BACKEND_ID,
    );

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
    let store = Arc::new(crate::part_store::MemoryPartStore::default());
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
) -> Res<(MemoryPartStoreSnapshot, MemoryPartStoreSnapshot)> {
    let worker_left = left.handle.snapshot().await?;
    let worker_right = right.handle.snapshot().await?;
    let left_part_id = left.host.resolve_part(&test_part()).await?;
    let right_part_id = right.host.resolve_part(&test_part()).await?;
    let expected_left_parts = [(left_part_id, TEST_BACKEND_ID)].into_iter().collect();
    let expected_right_parts = [(right_part_id, TEST_BACKEND_ID)].into_iter().collect();
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
    assert_eq!(snapshot_left.scoped_objs, snapshot_right.scoped_objs);
    assert_eq!(snapshot_left.scoped_objs.len(), expected_obj_count);
    assert_eq!(snapshot_left.peer_part_cursors.len(), 1);
    assert_eq!(snapshot_right.peer_part_cursors.len(), 1);
    assert!(snapshot_left
        .peer_part_cursors
        .contains_key(&(right.peer_id, left_part_id)));
    assert!(snapshot_right
        .peer_part_cursors
        .contains_key(&(left.peer_id, right_part_id)));

    Ok((snapshot_left, snapshot_right))
}

async fn wait_for_convergence(nodes: &[&NodeHarness], timeout: Duration) -> Res<()> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_snapshot = None;

    loop {
        let mut current = Vec::with_capacity(nodes.len());
        for node in nodes {
            current.push((node.handle.snapshot().await?, node.snapshot().await?));
        }

        let all_idle = current
            .iter()
            .all(|(worker_snapshot, _)| worker_snapshot.is_idle());
        if all_idle && last_snapshot.as_ref().is_some_and(|prev| prev == &current) {
            return Ok(());
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

    let left_obj = scoped_obj(10);
    let right_obj = scoped_obj(11);

    node_a.seed_obj(&left_obj, payload("left-a")).await?;
    node_b.seed_obj(&right_obj, payload("right-b")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, _) = assert_two_node_alignment(&node_a, &node_b, 2).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&left_obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("left-a"))
    );
    assert_eq!(
        snapshot_a
            .scoped_objs
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
    let part_id = node_a.store.resolve_part(&test_part()).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    let obj = scoped_obj(20);
    node_b.seed_obj(&obj, payload("connected-create")).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("connected-create"))
    );
    assert_eq!(
        snapshot_b
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("connected-create"))
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

    let obj = scoped_obj(30);
    node_b.seed_obj(&obj, payload("base")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    node_a.seed_obj(&obj, payload("higher-update")).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-update"))
    );
    assert_eq!(
        snapshot_b
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-update"))
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
    let part_id = node_a.store.resolve_part(&test_part()).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    let obj = scoped_obj(40);
    node_a.seed_obj(&obj, payload("base")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.seed_obj(&obj, payload("lower-conflict")),
        node_b.seed_obj(&obj, payload("higher-conflict")),
    )?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartStale { part_id: stale_part_id, .. }
            if *stale_part_id == part_id
    )));
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PeerStale { .. }
    )));
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("higher-conflict"))
    );
    assert_eq!(
        snapshot_b
            .scoped_objs
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

    let obj = scoped_obj(50);
    node_a.seed_obj(&obj, payload("delete-me")).await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    node_a.remove_obj(&obj).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 0).await?;
    assert!(!snapshot_a.scoped_objs.contains_key(&obj));
    assert!(!snapshot_b.scoped_objs.contains_key(&obj));

    node_a.stop().await?;
    node_b.stop().await?;
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
    let part_id = node_a.store.resolve_part(&test_part()).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    for ii in 0..obj_count {
        let obj = ScopedObjRef::new(test_scope(), format!("bucket.obj.{ii}"));
        node_b
            .seed_obj(&obj, serde_json::json!({ "ii": ii }))
            .await?;
    }

    node_a.connect_to(&node_b).await?;
    let deadline = std::time::Instant::now() + timeout;
    let snapshot = loop {
        let snapshot = node_a.snapshot().await?;
        if snapshot.scoped_objs.len() == obj_count {
            break snapshot;
        }
        if std::time::Instant::now() >= deadline {
            return Err(ferr!(
                "timed out waiting for bucket catchup, saw {} objects",
                snapshot.scoped_objs.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(snapshot.scoped_objs.len(), obj_count);
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));
    for ii in 0..obj_count {
        let obj = ScopedObjRef::new(test_scope(), format!("bucket.obj.{ii}"));
        let value = snapshot
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone())
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(value, serde_json::json!({ "ii": ii }));
    }

    let part_id = node_a.store.resolve_part(&test_part()).await?;
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
#[ignore = "slow bucket catchup case"]
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

    let obj = scoped_obj(60);
    node_a.seed_obj(&obj, payload("before-restart")).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.host.remove_peer(node_b.peer_id),
        node_b.host.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let node_b = restart_node(Arc::clone(&world), node_b).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("before-restart"))
    );
    assert_eq!(
        snapshot_b
            .scoped_objs
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
    let part_id = node_a.store.resolve_part(&test_part()).await?;
    let mut stats_rx = node_a.handle.subscribe_stats();

    let obj = scoped_obj(70);
    node_a.seed_obj(&obj, payload("online-base")).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    drain_stats(&mut stats_rx);

    tokio::try_join!(
        node_a.host.remove_peer(node_b.peer_id),
        node_b.host.remove_peer(node_a.peer_id),
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
    node_a.seed_obj(&obj, payload("offline-a")).await?;
    wait_for_convergence(&[&node_a], Duration::from_secs(30)).await?;

    let node_b = boot_node_with_store(Arc::clone(&world), peer_id, store).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let stats = collect_stats(&mut stats_rx, Duration::from_millis(200)).await;
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartStale { part_id: stale_part_id, .. }
            if *stale_part_id == part_id
    )));
    assert!(stats.iter().any(|evt| matches!(evt, SyncStatEvent::PeerStale { .. })));
    assert!(stats.iter().any(|evt| matches!(
        evt,
        SyncStatEvent::PartFullySynced { part_id: synced_part_id, .. }
            if *synced_part_id == part_id
    )));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
    assert_eq!(
        snapshot_a
            .scoped_objs
            .get(&obj)
            .and_then(|obj| obj.payload.clone()),
        Some(payload("offline-a"))
    );
    assert_eq!(
        snapshot_b
            .scoped_objs
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

    let obj = scoped_obj(80);
    node_c.seed_obj(&obj, payload("shared-from-third")).await?;

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
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerStale { .. })));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PartFullySynced { .. })));
    assert!(stats
        .iter()
        .any(|evt| matches!(evt, SyncStatEvent::PeerFullySynced { .. })));

    let (snapshot_a, snapshot_b, snapshot_c) = (
        node_a.snapshot().await?,
        node_b.snapshot().await?,
        node_c.snapshot().await?,
    );
    assert_eq!(
        snapshot_a.scoped_objs.get(&obj).and_then(|obj| obj.payload.clone()),
        Some(payload("shared-from-third"))
    );
    assert_eq!(
        snapshot_b.scoped_objs.get(&obj).and_then(|obj| obj.payload.clone()),
        Some(payload("shared-from-third"))
    );
    assert_eq!(
        snapshot_c.scoped_objs.get(&obj).and_then(|obj| obj.payload.clone()),
        Some(payload("shared-from-third"))
    );

    node_a.stop().await?;
    node_b.stop().await?;
    node_c.stop().await?;
    Ok(())
}

#[cfg(test)]
mod stress;
