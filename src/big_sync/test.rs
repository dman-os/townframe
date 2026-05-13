use crate::interlude::*;

use std::sync::Mutex;

use big_sync_core::mpsc::{self, Receiver};
use big_sync_core::rpc::{
    BigSyncRpcResult, ListPartsError, PeerSummaryRequest, PeerSummaryResult, SubEvent,
    SubPartsRequest,
};
use big_sync_core::{Byte32Id, ObjId, PartId, PeerId, SyncCompletion, SyncTaskDeets};

use crate::part_store::HostPartitionStore;
use crate::part_store::MemoryPartStoreSnapshot;
use crate::{BackendId, SharedPartitionStore};

const TEST_BACKEND_ID: BackendId = 0;
const TEST_PART_ID: PartId = PartId(Byte32Id::new([9; 32]));

fn test_parts() -> HashMap<PartId, BackendId> {
    [(TEST_PART_ID, TEST_BACKEND_ID)].into_iter().collect()
}

#[derive(Default)]
struct TestWorld {
    stores: Mutex<HashMap<PeerId, SharedPartitionStore>>,
    online: Mutex<HashSet<PeerId>>,
}

impl TestWorld {
    fn register_store(&self, peer_id: PeerId, store: SharedPartitionStore) {
        let mut stores = self.stores.lock().expect(ERROR_MUTEX);
        let old = stores.insert(peer_id, store);
        assert!(old.is_none(), "fishy");
        self.set_online(peer_id, true);
    }

    fn store_for_peer(&self, peer_id: PeerId) -> SharedPartitionStore {
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
    target_peer_id: PeerId,
    target_part_store: SharedPartitionStore,
}

impl MemoryRpcClient {
    fn new(
        world: Arc<TestWorld>,
        target_peer_id: PeerId,
        target_part_store: SharedPartitionStore,
    ) -> Self {
        Self {
            world,
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
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let parts = self.target_part_store.summarize_parts(req.parts).await??;
        Ok(Ok(Ok(PeerSummaryResult { parts })))
    }

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>> {
        if !self.world.is_online(self.target_peer_id) {
            return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
        }
        let receiver = self.target_part_store.subscribe(req).await??;
        let (forward_tx, forward_rx) =
            mpsc::unbounded("MemoryRpcClient".into(), "BigSyncMachine".into());
        tokio::spawn(async move {
            let mut receiver = receiver;
            while let Some(evt) = receiver.recv().await {
                if forward_tx.send(evt).await.is_err() {
                    break;
                }
            }
        });
        Ok(Ok(Ok(forward_rx)))
    }
}

struct MemorySyncBackend {
    local_peer_id: PeerId,
    local_part_store: SharedPartitionStore,
    world: Arc<TestWorld>,
}

impl MemorySyncBackend {
    fn new(
        local_peer_id: PeerId,
        local_part_store: SharedPartitionStore,
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
impl crate::SyncBackend for MemorySyncBackend {
    async fn run(&self, task: SyncTaskDeets) -> Res<Vec<SyncCompletion>> {
        let remote_part_store = self.world.store_for_peer(task.peer_id);
        let mut requested_part = HashSet::new();
        requested_part.insert(TEST_PART_ID);
        let local_summary = self
            .local_part_store
            .summarize_parts(requested_part.clone())
            .await??;
        let remote_summary = remote_part_store.summarize_parts(requested_part).await??;
        let local_payload = self.local_part_store.obj_payload(task.obj_id).await?;
        let remote_payload = remote_part_store.obj_payload(task.obj_id).await?;
        let local_parts = self.local_part_store.obj_parts(task.obj_id).await?;
        let remote_parts = remote_part_store.obj_parts(task.obj_id).await?;
        let local_has_part = local_parts.contains(&TEST_PART_ID);
        let remote_has_part = remote_parts.contains(&TEST_PART_ID);
        let local_latest = local_summary
            .get(&TEST_PART_ID)
            .expect(ERROR_IMPOSSIBLE)
            .latest_cursor;
        let remote_latest = remote_summary
            .get(&TEST_PART_ID)
            .expect(ERROR_IMPOSSIBLE)
            .latest_cursor;

        let completion = if local_has_part && !remote_has_part {
            SyncCompletion::DeletedMember {
                peer: task.peer_id,
                obj_id: task.obj_id,
            }
        } else if !local_has_part && remote_has_part {
            let obj_payload = remote_payload.expect(ERROR_IMPOSSIBLE);
            SyncCompletion::AddedMember {
                peer: task.peer_id,
                obj_id: task.obj_id,
                obj_payload,
            }
        } else if local_has_part && remote_has_part {
            match (local_payload, remote_payload) {
                (Some(local), Some(remote)) if local == remote => SyncCompletion::Noop {
                    peer: task.peer_id,
                    obj_id: task.obj_id,
                },
                (Some(local), Some(_)) if local_latest > remote_latest => {
                    SyncCompletion::AddedMember {
                        peer: task.peer_id,
                        obj_id: task.obj_id,
                        obj_payload: local,
                    }
                }
                (Some(_), Some(remote)) if remote_latest > local_latest => {
                    SyncCompletion::AddedMember {
                        peer: task.peer_id,
                        obj_id: task.obj_id,
                        obj_payload: remote,
                    }
                }
                (Some(local), Some(remote)) => {
                    // Deterministic tie-breaker for concurrent same-depth updates.
                    let obj_payload = if task.peer_id > self.local_peer_id {
                        remote
                    } else {
                        local
                    };
                    SyncCompletion::AddedMember {
                        peer: task.peer_id,
                        obj_id: task.obj_id,
                        obj_payload,
                    }
                }
                (None, Some(payload)) => SyncCompletion::AddedMember {
                    peer: task.peer_id,
                    obj_id: task.obj_id,
                    obj_payload: payload,
                },
                _ => SyncCompletion::Noop {
                    peer: task.peer_id,
                    obj_id: task.obj_id,
                },
            }
        } else {
            SyncCompletion::Noop {
                peer: task.peer_id,
                obj_id: task.obj_id,
            }
        };
        Ok(vec![completion])
    }
}

struct NodeHarness {
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<crate::part_store::MemoryPartStore>,
    handle: crate::BigSyncWorkerHandle,
    stop: crate::StopToken,
}

impl NodeHarness {
    async fn connect_to(&self, remote: &NodeHarness) -> Res<()> {
        let client = Arc::new(MemoryRpcClient::new(
            Arc::clone(&remote.world),
            remote.peer_id,
            Arc::clone(&remote.store) as _,
        ));
        self.handle
            .set_peer(remote.peer_id, client, test_parts())
            .await
    }

    async fn seed_obj(&self, obj_id: ObjId, payload: serde_json::Value) -> Res<()> {
        self.store
            .upsert_obj(obj_id, payload, vec![TEST_PART_ID])
            .await
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

fn obj_id(seed: u8) -> ObjId {
    ObjId(Byte32Id::new([seed; 32]))
}

fn payload(label: &str) -> serde_json::Value {
    serde_json::Value::from(label)
}

async fn boot_node_with_store(
    world: Arc<TestWorld>,
    peer_id: PeerId,
    store: Arc<crate::part_store::MemoryPartStore>,
) -> Res<NodeHarness> {
    world.set_online(peer_id, true);
    store.ensure_part(TEST_PART_ID).await?;
    let store_for_worker: SharedPartitionStore = Arc::clone(&store) as _;
    let backend: Arc<dyn crate::SyncBackend> = Arc::new(MemorySyncBackend::new(
        peer_id,
        Arc::clone(&store) as _,
        Arc::clone(&world),
    ));
    let (handle, stop) =
        crate::spawn_big_sync_worker(store_for_worker, [(TEST_BACKEND_ID, backend)].into())?;

    Ok(NodeHarness {
        world,
        peer_id,
        store,
        handle,
        stop,
    })
}

async fn boot_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let store = Arc::new(crate::part_store::MemoryPartStore::default());
    world.register_store(peer_id, Arc::clone(&store) as _);
    boot_node_with_store(world, peer_id, store).await
}

async fn restart_node(world: Arc<TestWorld>, node: NodeHarness) -> Res<NodeHarness> {
    let NodeHarness {
        world: node_world,
        peer_id,
        store,
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
    let expected_parts = test_parts();
    assert_eq!(worker_left.peer_parts.len(), 1);
    assert_eq!(worker_right.peer_parts.len(), 1);
    assert_eq!(
        worker_left.peer_parts.get(&right.peer_id),
        Some(&expected_parts)
    );
    assert_eq!(
        worker_right.peer_parts.get(&left.peer_id),
        Some(&expected_parts)
    );

    let snapshot_left = left.snapshot().await?;
    let snapshot_right = right.snapshot().await?;
    assert_eq!(snapshot_left.objs, snapshot_right.objs);
    assert_eq!(snapshot_left.objs.len(), expected_obj_count);
    assert_eq!(snapshot_left.peer_part_cursors.len(), 1);
    assert_eq!(snapshot_right.peer_part_cursors.len(), 1);
    assert!(snapshot_left
        .peer_part_cursors
        .contains_key(&(right.peer_id, TEST_PART_ID)));
    assert!(snapshot_right
        .peer_part_cursors
        .contains_key(&(left.peer_id, TEST_PART_ID)));

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

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_preconnected_seeds_converge() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let left_obj = obj_id(10);
    let right_obj = obj_id(11);

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

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let obj = obj_id(20);
    node_b.seed_obj(obj, payload("connected-create")).await?;

    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 1).await?;
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
async fn memory_sync_higher_peer_update_propagates_after_convergence() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 2).await?;
    let node_b = boot_node(Arc::clone(&world), 1).await?;

    let obj = obj_id(30);
    node_b.seed_obj(obj, payload("base")).await?;

    node_a.connect_to(&node_b).await?;
    node_b.connect_to(&node_a).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

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
async fn memory_sync_concurrent_conflicting_updates_converge_to_higher_peer_value() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = obj_id(40);
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

    let obj = obj_id(50);
    node_a.seed_obj(obj, payload("delete-me")).await?;

    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    node_a.store.remove_obj_from_part(obj, TEST_PART_ID).await?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    let (snapshot_a, snapshot_b) = assert_two_node_alignment(&node_a, &node_b, 0).await?;
    assert!(!snapshot_a.objs.contains_key(&obj));
    assert!(!snapshot_b.objs.contains_key(&obj));

    node_a.stop().await?;
    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_peer_restart_reconnects_cleanly() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let world = Arc::new(TestWorld::default());
    let node_a = boot_node(Arc::clone(&world), 1).await?;
    let node_b = boot_node(Arc::clone(&world), 2).await?;

    let obj = obj_id(60);
    node_a.seed_obj(obj, payload("before-restart")).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.handle.remove_peer(node_b.peer_id),
        node_b.handle.remove_peer(node_a.peer_id),
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

    let obj = obj_id(70);
    node_a.seed_obj(obj, payload("online-base")).await?;
    tokio::try_join!(node_a.connect_to(&node_b), node_b.connect_to(&node_a))?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;

    tokio::try_join!(
        node_a.handle.remove_peer(node_b.peer_id),
        node_b.handle.remove_peer(node_a.peer_id),
    )?;
    wait_for_convergence(&[&node_a, &node_b], Duration::from_secs(30)).await?;
    let NodeHarness {
        world: node_b_world,
        peer_id,
        store,
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

#[cfg(test)]
mod stress;
