use super::*;

use crate::part_store::sqlite::SqlitePartStore;
use crate::stress_support::{self, StressFixture};
use crate::worker::WorkerSnapshot;
use std::fmt::Write as _;

#[derive(Clone, Copy, Debug)]
enum StressBackend {
    Memory,
    Sqlite,
}

impl StressBackend {
    fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Clone, Copy)]
struct LwwStressFixture {
    backend: StressBackend,
}

impl LwwStressFixture {
    fn new(backend: StressBackend) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl StressFixture for LwwStressFixture {
    type World = TestWorld;
    type Node = NodeHarness;
    type StressObj = ObjId;
    type Observation = (WorkerSnapshot, ObservedStoreSnapshot);

    fn label(&self) -> &'static str {
        self.backend.label()
    }

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj {
        stress_support::stress_obj(rng)
    }

    async fn boot_node(&self, world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
        match self.backend {
            StressBackend::Memory => boot_node(world, peer_seed).await,
            StressBackend::Sqlite => boot_sqlite_node(world, peer_seed).await,
        }
    }

    async fn stop_node(&self, node: Self::Node) -> Res<()> {
        node.stop().await
    }

    async fn restart_node(
        &self,
        world: Arc<Self::World>,
        _peer_seed: u8,
        node: Self::Node,
    ) -> Res<Self::Node> {
        match self.backend {
            StressBackend::Memory => super::restart_node(world, node).await,
            StressBackend::Sqlite => restart_sqlite_node(world, node).await,
        }
    }

    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        tokio::try_join!(left.connect_to(right), right.connect_to(left))?;
        Ok(())
    }

    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        tokio::try_join!(
            left.host.worker.remove_peer(right.peer_id),
            right.host.worker.remove_peer(left.peer_id),
        )?;
        Ok(())
    }

    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        self.seed_obj(node, obj, payload).await
    }

    async fn seed_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        node.seed_obj(*obj, payload).await
    }

    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation> {
        tokio::try_join!(node.handle.snapshot(), node.snapshot())
    }

    fn peer_id(&self, node: &Self::Node) -> PeerId {
        node.peer_id
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        assert_cluster_alignment_lww(nodes).await
    }
}

fn diff_scoped_obj_snapshots(
    left_peer: PeerId,
    left: &ObservedStoreSnapshot,
    right_peer: PeerId,
    right: &ObservedStoreSnapshot,
) -> String {
    let mut out = String::new();
    writeln!(
        out,
        "scoped_objs differ: left_peer={left_peer:?} left_count={} right_peer={right_peer:?} right_count={}",
        left.objs.len(),
        right.objs.len()
    )
    .ok();

    let mut only_left = Vec::new();
    let mut only_right = Vec::new();
    let mut differing = Vec::new();

    for (obj, snapshot) in &left.objs {
        match right.objs.get(obj) {
            None => only_left.push((obj, snapshot)),
            Some(other) if other != snapshot => differing.push((obj, snapshot, other)),
            Some(_) => {}
        }
    }
    for (obj, snapshot) in &right.objs {
        if !left.objs.contains_key(obj) {
            only_right.push((obj, snapshot));
        }
    }

    let max_items = 20usize;

    let only_left_count = only_left.len();
    let only_right_count = only_right.len();
    let differing_count = differing.len();

    if only_left_count > 0 {
        writeln!(out, "only in left (showing up to {max_items}):").ok();
        for (obj, snapshot) in only_left.into_iter().take(max_items) {
            writeln!(
                out,
                "  - {obj:?} => payload={:?} parts={:?}",
                snapshot.payload, snapshot.parts
            )
            .ok();
        }
    }
    if only_right_count > 0 {
        writeln!(out, "only in right (showing up to {max_items}):").ok();
        for (obj, snapshot) in only_right.into_iter().take(max_items) {
            writeln!(
                out,
                "  - {obj:?} => payload={:?} parts={:?}",
                snapshot.payload, snapshot.parts
            )
            .ok();
        }
    }
    if differing_count > 0 {
        writeln!(out, "differing entries (showing up to {max_items}):").ok();
        for (obj, left_snapshot, right_snapshot) in differing.into_iter().take(max_items) {
            writeln!(out, "  - {obj:?}:").ok();
            writeln!(
                out,
                "      left : payload={:?} parts={:?}",
                left_snapshot.payload, left_snapshot.parts
            )
            .ok();
            writeln!(
                out,
                "      right: payload={:?} parts={:?}",
                right_snapshot.payload, right_snapshot.parts
            )
            .ok();
        }
    }

    if only_left_count == 0 && only_right_count == 0 && differing_count == 0 {
        writeln!(out, "snapshots differ for an unknown reason").ok();
    }

    out
}

async fn assert_cluster_alignment_lww(nodes: &[&NodeHarness]) -> Res<()> {
    if nodes.is_empty() {
        return Ok(());
    }
    let part_ids = stress_support::test_parts();
    for node in nodes {
        let connected_peers = node.handle.snapshot().await?.peer_parts.into_keys();
        node.wait_for_full_sync(connected_peers, part_ids.iter().copied())
            .await?;
    }
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(30));
    let mut last_diff = None;

    loop {
        let mut store_snaps = Vec::with_capacity(nodes.len());
        for node in nodes {
            let snapshot = node.snapshot().await?;
            for &(_, part_id) in snapshot.peer_part_cursors.keys() {
                assert_eq!(part_id, stress_support::test_part());
            }
            store_snaps.push((node.peer_id, snapshot));
        }

        let mut converged = true;
        for snapshot in store_snaps.iter().skip(1) {
            if store_snaps[0].1.objs != snapshot.1.objs {
                converged = false;
                last_diff = Some(diff_scoped_obj_snapshots(
                    store_snaps[0].0,
                    &store_snaps[0].1,
                    snapshot.0,
                    &snapshot.1,
                ));
                break;
            }
        }

        if converged {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            let diff = last_diff.unwrap_or_else(|| "cluster failed to converge".to_string());
            panic!("{diff}");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn boot_sqlite_node(world: Arc<TestWorld>, peer_seed: u8) -> Res<NodeHarness> {
    let temp_dir = tempfile::tempdir()?;
    boot_sqlite_node_at(world, peer_seed, temp_dir).await
}

async fn boot_sqlite_node_at(
    world: Arc<TestWorld>,
    peer_seed: u8,
    temp_dir: tempfile::TempDir,
) -> Res<NodeHarness> {
    let peer_id = peer_id(peer_seed);
    let db_path = temp_dir.path().join("big_sync.sqlite");
    let sqlite_url = format!("sqlite://{}", db_path.display());
    let sql = sqlx_utils_rs::SqlCtx::url(&sqlite_url).await?;
    let store = Arc::new(
        SqlitePartStore::new(
            sql,
            format!("big-sync-stress://peer/{peer_seed}"),
            BuckId::MAX_LEVEL,
        )
        .await?,
    );
    let node = boot_node_with_store(world, peer_id, store, None).await?;
    Ok(NodeHarness {
        sqlite_temp_dir: Some(temp_dir),
        ..node
    })
}

async fn restart_sqlite_node(world: Arc<TestWorld>, node: NodeHarness) -> Res<NodeHarness> {
    let NodeHarness {
        world: node_world,
        peer_id,
        stop,
        sqlite_temp_dir,
        ..
    } = node;
    node_world.set_online(peer_id, false);
    stop.stop().await?;
    node_world.remove_store(peer_id);
    let temp_dir = sqlite_temp_dir.ok_or_eyre("sqlite stress node is missing its temp dir")?;
    let peer_seed = peer_id.as_bytes()[0];
    boot_sqlite_node_at(world, peer_seed, temp_dir).await
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_randomized_four_node_stress_converges() -> Res<()> {
    stress_support::run_randomized_four_node_stress(
        LwwStressFixture::new(StressBackend::Memory),
        Arc::new(TestWorld::default()),
        stress_support::PHASE1_MUTATIONS,
        stress_support::PHASE2_MUTATIONS,
        stress_support::PHASE3_MUTATIONS,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_sync_randomized_four_node_stress_converges() -> Res<()> {
    stress_support::run_randomized_four_node_stress(
        LwwStressFixture::new(StressBackend::Sqlite),
        Arc::new(TestWorld::default()),
        stress_support::PHASE1_MUTATIONS,
        stress_support::PHASE2_MUTATIONS,
        stress_support::PHASE3_MUTATIONS,
    )
    .await
}

#[derive(Clone, Default)]
struct PolicyMembershipFixture;

#[async_trait]
impl StressFixture for PolicyMembershipFixture {
    type World = TestWorld;
    type Node = NodeHarness;
    type StressObj = ObjId;
    type Observation = (WorkerSnapshot, ObservedStoreSnapshot);

    fn label(&self) -> &'static str {
        "policy_membership"
    }

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj {
        stress_support::stress_obj(rng)
    }

    async fn boot_node(&self, world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
        boot_policy_node(world, peer_seed).await
    }

    async fn stop_node(&self, node: Self::Node) -> Res<()> {
        node.stop().await
    }

    async fn restart_node(
        &self,
        world: Arc<Self::World>,
        _peer_seed: u8,
        node: Self::Node,
    ) -> Res<Self::Node> {
        super::restart_node(world, node).await
    }

    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        tokio::try_join!(left.connect_to(right), right.connect_to(left))?;
        Ok(())
    }

    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        tokio::try_join!(
            left.host.worker.remove_peer(right.peer_id),
            right.host.worker.remove_peer(left.peer_id),
        )?;
        Ok(())
    }

    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        node.seed_obj(*obj, payload).await
    }

    async fn seed_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        node.seed_obj(*obj, payload).await
    }

    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation> {
        tokio::try_join!(node.handle.snapshot(), node.snapshot())
    }

    fn peer_id(&self, node: &Self::Node) -> PeerId {
        node.peer_id
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        assert_cluster_alignment_lww(nodes).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_sync_randomized_four_node_stress_converges() -> Res<()> {
    stress_support::run_randomized_four_node_stress(
        PolicyMembershipFixture,
        Arc::new(TestWorld::default()),
        stress_support::PHASE1_MUTATIONS,
        stress_support::PHASE2_MUTATIONS,
        stress_support::PHASE3_MUTATIONS,
    )
    .await
}
