use super::*;

use crate::part_store::sqlite::SqlitePartStore;
use crate::stress_support::{self, StressFixture};
use crate::worker::WorkerSnapshot;
use std::fmt::Write as _;
use std::str::FromStr;

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
        _nodes: &[Option<Self::Node>],
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
    let _ = writeln!(
        out,
        "scoped_objs differ: left_peer={left_peer:?} left_count={} right_peer={right_peer:?} right_count={}",
        left.objs.len(),
        right.objs.len()
    );

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
        let _ = writeln!(out, "only in left (showing up to {max_items}):");
        for (obj, snapshot) in only_left.into_iter().take(max_items) {
            let _ = writeln!(
                out,
                "  - {obj:?} => payload={:?} parts={:?}",
                snapshot.payload, snapshot.parts
            );
        }
    }
    if only_right_count > 0 {
        let _ = writeln!(out, "only in right (showing up to {max_items}):");
        for (obj, snapshot) in only_right.into_iter().take(max_items) {
            let _ = writeln!(
                out,
                "  - {obj:?} => payload={:?} parts={:?}",
                snapshot.payload, snapshot.parts
            );
        }
    }
    if differing_count > 0 {
        let _ = writeln!(out, "differing entries (showing up to {max_items}):");
        for (obj, left_snapshot, right_snapshot) in differing.into_iter().take(max_items) {
            let _ = writeln!(out, "  - {obj:?}:");
            let _ = writeln!(
                out,
                "      left : payload={:?} parts={:?}",
                left_snapshot.payload, left_snapshot.parts
            );
            let _ = writeln!(
                out,
                "      right: payload={:?} parts={:?}",
                right_snapshot.payload, right_snapshot.parts
            );
        }
    }

    if only_left_count == 0 && only_right_count == 0 && differing_count == 0 {
        let _ = writeln!(out, "snapshots differ for an unknown reason");
    }

    out
}

async fn assert_cluster_alignment_lww(nodes: &[&NodeHarness]) -> Res<()> {
    if nodes.is_empty() {
        return Ok(());
    }
    let peer_ids: Vec<PeerId> = nodes.iter().map(|node| node.peer_id).collect();
    let part_ids = stress_support::test_parts();
    for node in nodes {
        node.wait_for_full_sync(
            peer_ids
                .iter()
                .copied()
                .filter(|peer_id| *peer_id != node.peer_id),
            part_ids.iter().copied(),
        )
        .await?;
    }
    let mut worker_snaps = Vec::with_capacity(nodes.len());
    let mut store_snaps = Vec::with_capacity(nodes.len());

    for node in nodes {
        let worker_snapshot = node.handle.snapshot().await?;
        let _part_id = stress_support::test_part();
        worker_snaps.push(worker_snapshot);
        let snapshot = node.snapshot().await?;
        for &(_, part_id) in snapshot.peer_part_cursors.keys() {
            assert_eq!(part_id, stress_support::test_part());
        }
        store_snaps.push((node.peer_id, snapshot));
    }

    for snapshot in store_snaps.iter().skip(1) {
        if store_snaps[0].1.objs != snapshot.1.objs {
            panic!(
                "{}",
                diff_scoped_obj_snapshots(
                    store_snaps[0].0,
                    &store_snaps[0].1,
                    snapshot.0,
                    &snapshot.1
                )
            );
        }
    }

    let _ = worker_snaps;
    Ok(())
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
    let db_url = format!("sqlite://{}", db_path.display());
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .create_if_missing(true);
    let read_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options.clone())
        .await?;
    let write_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let store = Arc::new(
        SqlitePartStore::new(
            read_pool,
            write_pool,
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
