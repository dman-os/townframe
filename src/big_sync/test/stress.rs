use super::*;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::Mutex;

const STRESS_NODE_COUNT: usize = 4;
const PHASE1_MUTATIONS: usize = 48;
const PHASE2_MUTATIONS: usize = 24;
const PHASE3_MUTATIONS: usize = 32;
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

async fn shutdown_node(
    node: NodeHarness,
) -> Res<(PeerId, Arc<crate::part_store::MemoryPartStore>)> {
    let NodeHarness {
        world,
        peer_id,
        store,
        host: _host,
        handle,
        stop,
    } = node;
    world.set_online(peer_id, false);
    stop.stop().await?;
    drop(handle);
    Ok((peer_id, store))
}

async fn disconnect_from_node(nodes: &[Option<NodeHarness>], victim_idx: usize) -> Res<()> {
    let Some(victim) = nodes[victim_idx].as_ref() else {
        return Ok(());
    };
    let victim_peer_id = victim.peer_id;
    for (other_idx, other) in nodes.iter().enumerate() {
        if other_idx == victim_idx {
            continue;
        }
        let Some(other) = other.as_ref() else {
            continue;
        };
        tokio::try_join!(
            victim.host.remove_peer(other.peer_id),
            other.host.remove_peer(victim_peer_id),
        )?;
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
        node.remove_obj(&obj).await?;
        state.retire_obj(obj);
    } else {
        let nonce = rng.random::<u64>();
        let value = stress_payload(phase, step, node_idx, &obj, nonce);
        journal.record(format!(
            "{phase}:step={step}:upsert node={node_idx} obj={obj:?} value={value:?}"
        ));
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
        store_snaps.push(snapshot);
    }

    for snapshot in store_snaps.iter().skip(1) {
        assert_eq!(store_snaps[0].scoped_objs, snapshot.scoped_objs);
    }

    let _ = worker_snaps;
    Ok(())
}

async fn wait_for_cluster_convergence(nodes: &[Option<NodeHarness>], timeout: Duration) -> Res<()> {
    let refs = live_refs(nodes);
    wait_for_convergence(&refs, timeout).await?;
    assert_cluster_alignment(&refs).await
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
    let mut nodes = boot_cluster(Arc::clone(&world)).await?;

    connect_full_mesh(&nodes).await?;
    wait_for_cluster_convergence(&nodes, Duration::from_secs(30)).await?;

    journal.record("phase1:start");
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase1",
        PHASE1_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_cluster_convergence(&nodes, Duration::from_secs(30)).await?;

    let offline_idx = {
        let live = live_indices(&nodes);
        live[rng.random_range(0..live.len())]
    };
    journal.record(format!("phase2:offline idx={offline_idx}"));
    disconnect_from_node(&nodes, offline_idx).await?;
    let offline_node = nodes[offline_idx].take().expect(ERROR_IMPOSSIBLE);
    let (peer_id, store) = shutdown_node(offline_node).await?;

    let live_nodes = live_refs(&nodes);
    wait_for_convergence(&live_nodes, Duration::from_secs(30)).await?;

    journal.record("phase2:start");
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase2",
        PHASE2_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_cluster_convergence(&nodes, Duration::from_secs(30)).await?;

    let restarted = boot_node_with_store(Arc::clone(&world), peer_id, store).await?;
    nodes[offline_idx] = Some(restarted);
    connect_full_mesh(&nodes).await?;
    wait_for_cluster_convergence(&nodes, Duration::from_secs(30)).await?;

    journal.record("phase3:start");
    run_phase(
        &mut rng,
        &mut state,
        &nodes,
        "phase3",
        PHASE3_MUTATIONS,
        &journal,
    )
    .await?;
    wait_for_cluster_convergence(&nodes, Duration::from_secs(30)).await?;

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
