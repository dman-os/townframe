use super::*;

use rand::rngs::StdRng;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Mutex;

const STRESS_NODE_COUNT: usize = 4;
const PHASE1_MUTATIONS: usize = 48 * 1;
const PHASE2_MUTATIONS: usize = 24 * 1;
const PHASE3_MUTATIONS: usize = 32 * 1;
const DEFAULT_STRESS_SEED: u64 = 0xB1A0_5EED_5EED_0001;

#[derive(Default)]
struct StressJournal {
    entries: Mutex<Vec<String>>,
}

impl StressJournal {
    fn record(&self, entry: impl Into<String>) {
        self.entries.lock().expect(ERROR_MUTEX).push(entry.into());
    }

    fn snapshot(&self) -> Vec<String> {
        self.entries.lock().expect(ERROR_MUTEX).clone()
    }
}

fn stress_obj(seed: u32) -> ScopedObjRef {
    ScopedObjRef::new(test_scope(), format!("stress.obj.{seed}"))
}

fn stress_payload(
    phase: &str,
    step: usize,
    node_idx: usize,
    obj_id: &ScopedObjRef,
    nonce: u64,
) -> serde_json::Value {
    serde_json::Value::from(format!(
        "{phase}:step={step}:node={node_idx}:obj={obj_id:?}:nonce={nonce}"
    ))
}

#[derive(Default)]
struct StressState {
    next_obj_seed: u32,
    live_objs: Vec<ScopedObjRef>,
    retired_objs: HashSet<ScopedObjRef>,
}

impl StressState {
    fn fresh_obj(&mut self) -> ScopedObjRef {
        let obj = stress_obj(self.next_obj_seed);
        self.next_obj_seed = self.next_obj_seed.wrapping_add(1);
        obj
    }

    fn choose_live_obj(&self, rng: &mut StdRng) -> Option<ScopedObjRef> {
        if self.live_objs.is_empty() {
            return None;
        }
        Some(self.live_objs[rng.random_range(0..self.live_objs.len())].clone())
    }

    fn publish_new_obj(&mut self) -> ScopedObjRef {
        let obj = self.fresh_obj();
        self.live_objs.push(obj.clone());
        obj
    }

    fn retire_obj(&mut self, obj: ScopedObjRef) {
        self.retired_objs.insert(obj.clone());
        self.live_objs.retain(|candidate| candidate != &obj);
    }
}

fn live_indices(nodes: &[Option<NodeHarness>]) -> Vec<usize> {
    nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, node)| node.as_ref().map(|_| idx))
        .collect()
}

fn live_refs(nodes: &[Option<NodeHarness>]) -> Vec<&NodeHarness> {
    nodes.iter().filter_map(|node| node.as_ref()).collect()
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
        left.scoped_objs.len(),
        right.scoped_objs.len()
    );

    let mut only_left = Vec::new();
    let mut only_right = Vec::new();
    let mut differing = Vec::new();

    for (obj, snapshot) in &left.scoped_objs {
        match right.scoped_objs.get(obj) {
            None => only_left.push((obj, snapshot)),
            Some(other) if other != snapshot => differing.push((obj, snapshot, other)),
            Some(_) => {}
        }
    }
    for (obj, snapshot) in &right.scoped_objs {
        if !left.scoped_objs.contains_key(obj) {
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

async fn boot_cluster(world: Arc<TestWorld>) -> Res<Vec<Option<NodeHarness>>> {
    let mut nodes = Vec::with_capacity(STRESS_NODE_COUNT);
    for peer_seed in 1..=(STRESS_NODE_COUNT as u8) {
        nodes.push(Some(boot_node(Arc::clone(&world), peer_seed).await?));
    }
    Ok(nodes)
}

async fn connect_full_mesh(nodes: &[Option<NodeHarness>]) -> Res<()> {
    for left_idx in 0..nodes.len() {
        let Some(left) = nodes[left_idx].as_ref() else {
            continue;
        };
        for right in nodes.iter().skip(left_idx + 1) {
            let Some(right) = right.as_ref() else {
                continue;
            };
            tokio::try_join!(left.connect_to(right), right.connect_to(left))?;
        }
    }
    Ok(())
}

async fn wait_for_full_mesh(nodes: &[Option<NodeHarness>], timeout: Duration) -> Res<()> {
    let live = live_refs(nodes);
    let expected_peer_count = live.len().saturating_sub(1);
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let mut all_connected = true;
        for node in &live {
            let worker_snapshot = node.handle.snapshot().await?;
            if worker_snapshot.peer_parts.len() != expected_peer_count {
                all_connected = false;
                break;
            }
        }

        if all_connected {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(ferr!("timed out waiting for full mesh"));
        }

        connect_full_mesh(nodes).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn disconnect_all(nodes: &[Option<NodeHarness>]) -> Res<()> {
    for left_idx in 0..nodes.len() {
        let Some(left) = nodes[left_idx].as_ref() else {
            continue;
        };
        for right in nodes.iter().skip(left_idx + 1) {
            let Some(right) = right.as_ref() else {
                continue;
            };
            tokio::try_join!(
                left.host.remove_peer(right.peer_id),
                right.host.remove_peer(left.peer_id),
            )?;
        }
    }
    Ok(())
}

fn choose_active_topology(rng: &mut StdRng, nodes: &[Option<NodeHarness>]) -> Vec<usize> {
    let mut live = live_indices(nodes);
    if live.len() <= 2 {
        return live;
    }
    live.shuffle(rng);
    let active_len = rng.random_range(2..=live.len());
    live.truncate(active_len);
    live.sort_unstable();
    live
}

async fn connect_active_topology(
    rng: &mut StdRng,
    nodes: &[Option<NodeHarness>],
    active_idxs: &[usize],
) -> Res<()> {
    disconnect_all(nodes).await?;
    if active_idxs.len() < 2 {
        return Ok(());
    }

    let mut chain = active_idxs.to_vec();
    chain.shuffle(rng);
    for pair in chain.windows(2) {
        let left = nodes[pair[0]].as_ref().expect(ERROR_IMPOSSIBLE);
        let right = nodes[pair[1]].as_ref().expect(ERROR_IMPOSSIBLE);
        tokio::try_join!(left.connect_to(right), right.connect_to(left))?;
    }

    for i in 0..chain.len() {
        for j in (i + 2)..chain.len() {
            if rng.random_bool(0.35) {
                let left = nodes[chain[i]].as_ref().expect(ERROR_IMPOSSIBLE);
                let right = nodes[chain[j]].as_ref().expect(ERROR_IMPOSSIBLE);
                tokio::try_join!(left.connect_to(right), right.connect_to(left))?;
            }
        }
    }
    Ok(())
}

async fn apply_random_mutation(
    rng: &mut StdRng,
    state: &mut StressState,
    nodes: &[Option<NodeHarness>],
    phase: &str,
    step: usize,
    journal: &StressJournal,
) -> Res<()> {
    let live = live_indices(nodes);
    if live.is_empty() {
        return Ok(());
    }

    let node_idx = live[rng.random_range(0..live.len())];
    let node = nodes[node_idx].as_ref().expect(ERROR_IMPOSSIBLE);
    let fresh_obj = state.live_objs.is_empty() || rng.random_bool(0.30);
    let obj = if fresh_obj {
        let obj = state.publish_new_obj();
        journal.record(format!(
            "{phase}:step={step}:create node={node_idx} obj={obj:?}"
        ));
        obj
    } else {
        state.choose_live_obj(rng).expect(ERROR_IMPOSSIBLE)
    };

    if !fresh_obj && rng.random_bool(0.25) {
        journal.record(format!(
            "{phase}:step={step}:delete node={node_idx} obj={obj:?}"
        ));
        if obj == stress_obj(120) {
            tracing::debug!(
                phase,
                step,
                node_idx,
                obj = ?obj,
                "stress target delete"
            );
        }
        node.remove_obj(&obj).await?;
        state.retire_obj(obj);
    } else {
        let nonce = rng.random::<u64>();
        let value = stress_payload(phase, step, node_idx, &obj, nonce);
        journal.record(format!(
            "{phase}:step={step}:upsert node={node_idx} obj={obj:?} value={value:?}"
        ));
        if obj == stress_obj(120) {
            tracing::debug!(
                phase,
                step,
                node_idx,
                obj = ?obj,
                value = ?value,
                "stress target upsert"
            );
        }
        node.seed_obj(&obj, value).await?;
    }

    if rng.random_bool(0.25) {
        let sleep_ms = rng.random_range(1..15);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    Ok(())
}

async fn run_phase(
    rng: &mut StdRng,
    state: &mut StressState,
    nodes: &[Option<NodeHarness>],
    phase: &str,
    mutations: usize,
    journal: &StressJournal,
) -> Res<()> {
    for step in 0..mutations {
        apply_random_mutation(rng, state, nodes, phase, step, journal).await?;
    }
    Ok(())
}

async fn assert_cluster_alignment(nodes: &[&NodeHarness]) -> Res<()> {
    if nodes.is_empty() {
        return Ok(());
    }
    let expected_peer_count = nodes.len().saturating_sub(1);
    let mut worker_snaps = Vec::with_capacity(nodes.len());
    let mut store_snaps = Vec::with_capacity(nodes.len());

    for node in nodes {
        let worker_snapshot = node.handle.snapshot().await?;
        let part_id = node.host.resolve_part(&test_part()).await?;
        let expected_parts = [(part_id, TEST_BACKEND_ID)].into_iter().collect();
        assert_eq!(worker_snapshot.peer_parts.len(), expected_peer_count);
        for other in nodes {
            if node.peer_id == other.peer_id {
                continue;
            }
            assert_eq!(
                worker_snapshot.peer_parts.get(&other.peer_id),
                Some(&expected_parts)
            );
        }
        worker_snaps.push(worker_snapshot);
        let snapshot = node.snapshot().await?;
        for &(_, part_id) in snapshot.peer_part_cursors.keys() {
            assert_eq!(part_id, node.host.resolve_part(&test_part()).await?);
        }
        store_snaps.push((node.peer_id, snapshot));
    }

    for snapshot in store_snaps.iter().skip(1) {
        if store_snaps[0].1.scoped_objs != snapshot.1.scoped_objs {
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

async fn wait_for_cluster_quiescent(nodes: &[Option<NodeHarness>], timeout: Duration) -> Res<()> {
    let refs = live_refs(nodes);
    drain_cluster_zombies(nodes, timeout).await?;
    wait_for_cluster_settled(&refs, timeout).await?;
    drain_cluster_zombies(nodes, timeout).await?;
    wait_for_cluster_settled(&refs, timeout).await?;
    Ok(())
}

async fn wait_for_cluster_settled(nodes: &[&NodeHarness], timeout: Duration) -> Res<()> {
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
            if stable_rounds >= 100 {
                return Ok(());
            }
        } else {
            stable_rounds = 1;
        }

        last_snapshot = Some(current);
        if std::time::Instant::now() >= deadline {
            return Err(ferr!(
                "timed out waiting for stress cluster to settle: last_snapshot={last_snapshot:?}"
            ));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn drain_cluster_zombies(nodes: &[Option<NodeHarness>], timeout: Duration) -> Res<()> {
    for noded in live_refs(nodes) {
        node.handle.handle.drain_zombie_tasks(timeout).await
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sync_randomized_four_node_stress_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let seed = std::env::var("BIG_SYNC_STRESS_SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_STRESS_SEED);
    let mut rng = StdRng::seed_from_u64(seed);
    let journal = StressJournal::default();
    let mut state = StressState::default();
    journal.record(format!("seed={seed}"));

    let world = Arc::new(TestWorld::default());
    let nodes = boot_cluster(Arc::clone(&world)).await?;

    journal.record("phase1:start");
    let phase1_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase1:topology active={phase1_topology:?}"));
    connect_active_topology(&mut rng, &nodes, &phase1_topology).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase1",
        PHASE1_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_full_mesh(&nodes, Duration::from_secs(30)).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;

    journal.record("phase2:start");
    let phase2_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase2:topology active={phase2_topology:?}"));
    connect_active_topology(&mut rng, &nodes, &phase2_topology).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase2",
        PHASE2_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_full_mesh(&nodes, Duration::from_secs(30)).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;

    journal.record("phase3:start");
    let phase3_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase3:topology active={phase3_topology:?}"));
    connect_active_topology(&mut rng, &nodes, &phase3_topology).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase3",
        PHASE3_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_full_mesh(&nodes, Duration::from_secs(30)).await?;
    wait_for_cluster_quiescent(&nodes, Duration::from_secs(30)).await?;

    let refs = live_refs(&nodes);
    assert_cluster_alignment(&refs).await?;
    journal.record(format!(
        "final_journal_entries={}",
        journal.snapshot().len()
    ));

    for node in nodes.into_iter().flatten() {
        node.stop().await?;
    }

    Ok(())
}
