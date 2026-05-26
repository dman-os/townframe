use crate::interlude::*;

use big_sync_core::{Byte32Id, ObjId, PartId, PeerId};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

pub const STRESS_NODE_COUNT: usize = 3;
pub const PHASE1_MUTATIONS: usize = 48;
pub const PHASE2_MUTATIONS: usize = 24;
pub const PHASE3_MUTATIONS: usize = 32;
pub const DEFAULT_STRESS_SEED: u64 = 0xB1A0_5EED_5EED_0001;

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

pub fn payload(
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

pub fn test_part() -> PartId {
    PartId(Byte32Id::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

pub fn test_parts() -> Vec<PartId> {
    vec![test_part()]
}

pub fn gen_obj_id(rng: &mut impl Rng) -> ObjId {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    ObjId(Byte32Id::new(bytes))
}

#[async_trait]
pub trait StressFixture {
    type World: Send + Sync + 'static;
    type Node;
    type Observation: PartialEq + std::fmt::Debug;

    fn label(&self) -> &'static str;

    async fn boot_node(&self, world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node>;
    async fn stop_node(&self, node: Self::Node) -> Res<()>;
    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()>;
    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()>;
    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        nodes: &[Option<Self::Node>],
        obj: ObjId,
        payload: serde_json::Value,
    ) -> Res<()>;
    async fn seed_obj(&self, node: &Self::Node, obj: ObjId, payload: serde_json::Value) -> Res<()>;
    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation>;
    fn peer_id(&self, node: &Self::Node) -> PeerId;
    // Fixture-specific application content for a document mutation.
    fn make_doc_content(
        &self,
        phase: &str,
        step: usize,
        node_idx: usize,
        obj_id: &ObjId,
        nonce: u64,
        written_at: u64,
        writer_id: PeerId,
    ) -> serde_json::Value {
        stress_payload(phase, step, node_idx, obj_id, nonce, written_at, writer_id)
    }
    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()>;
}

#[derive(Default)]
pub struct StressJournal {
    entries: Mutex<Vec<String>>,
}

impl StressJournal {
    pub fn record(&self, entry: impl Into<String>) {
        self.entries.lock().expect(ERROR_MUTEX).push(entry.into());
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.entries.lock().expect(ERROR_MUTEX).clone()
    }
}

#[derive(Default)]
pub struct StressState {
    next_written_at: u64,
    pub live_objs: Vec<ObjId>,
}

impl StressState {
    pub fn fresh_obj(&mut self, rng: &mut impl Rng) -> ObjId {
        let obj = stress_obj(rng);
        obj
    }

    pub fn choose_live_obj(&self, rng: &mut StdRng) -> Option<ObjId> {
        if self.live_objs.is_empty() {
            return None;
        }
        Some(self.live_objs[rng.random_range(0..self.live_objs.len())].clone())
    }

    pub fn publish_new_obj(&mut self, rng: &mut impl Rng) -> ObjId {
        let obj = self.fresh_obj(rng);
        self.live_objs.push(obj.clone());
        obj
    }

    pub fn next_written_at(&mut self) -> u64 {
        let written_at = self.next_written_at;
        self.next_written_at = self.next_written_at.wrapping_add(1);
        written_at
    }
}

pub fn stress_obj(rng: &mut impl Rng) -> ObjId {
    gen_obj_id(rng)
}

pub fn stress_payload(
    phase: &str,
    step: usize,
    node_idx: usize,
    obj_id: &ObjId,
    nonce: u64,
    written_at: u64,
    writer_id: PeerId,
) -> serde_json::Value {
    payload(
        format!("{phase}:step={step}:node={node_idx}:obj={obj_id:?}:nonce={nonce}"),
        written_at,
        writer_id,
    )
}

pub fn live_indices<T>(nodes: &[Option<T>]) -> Vec<usize> {
    nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, node)| node.as_ref().map(|_| idx))
        .collect()
}

pub fn live_refs<T>(nodes: &[Option<T>]) -> Vec<&T> {
    nodes.iter().filter_map(|node| node.as_ref()).collect()
}

pub fn choose_active_topology<T>(rng: &mut StdRng, nodes: &[Option<T>]) -> Vec<usize> {
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

pub async fn boot_cluster_for_fixture<F: StressFixture>(
    fixture: &F,
    world: Arc<F::World>,
) -> Res<Vec<Option<F::Node>>> {
    let mut nodes = Vec::with_capacity(STRESS_NODE_COUNT);
    for peer_seed in 1..=(STRESS_NODE_COUNT as u8) {
        let node = fixture.boot_node(Arc::clone(&world), peer_seed).await?;
        nodes.push(Some(node));
    }
    Ok(nodes)
}

pub async fn disconnect_all<F: StressFixture>(fixture: &F, nodes: &[Option<F::Node>]) -> Res<()> {
    for left_idx in 0..nodes.len() {
        let Some(left) = nodes[left_idx].as_ref() else {
            continue;
        };
        for right in nodes.iter().skip(left_idx + 1) {
            let Some(right) = right.as_ref() else {
                continue;
            };
            fixture.disconnect_pair(left, right).await?;
        }
    }
    Ok(())
}

pub async fn connect_active_topology<F: StressFixture>(
    fixture: &F,
    rng: &mut StdRng,
    nodes: &[Option<F::Node>],
    active_idxs: &[usize],
) -> Res<()> {
    disconnect_all(fixture, nodes).await?;
    if active_idxs.len() < 2 {
        return Ok(());
    }

    let mut chain = active_idxs.to_vec();
    chain.shuffle(rng);
    for pair in chain.windows(2) {
        let left = nodes[pair[0]].as_ref().expect(ERROR_IMPOSSIBLE);
        let right = nodes[pair[1]].as_ref().expect(ERROR_IMPOSSIBLE);
        fixture.connect_pair(left, right).await?;
    }

    for i in 0..chain.len() {
        for j in (i + 2)..chain.len() {
            if rng.random_bool(0.35) {
                let left = nodes[chain[i]].as_ref().expect(ERROR_IMPOSSIBLE);
                let right = nodes[chain[j]].as_ref().expect(ERROR_IMPOSSIBLE);
                fixture.connect_pair(left, right).await?;
            }
        }
    }
    Ok(())
}

pub async fn connect_full_mesh<F: StressFixture>(
    fixture: &F,
    nodes: &[Option<F::Node>],
) -> Res<()> {
    let live = live_indices(nodes);
    for left_idx in 0..live.len() {
        let left = nodes[live[left_idx]].as_ref().expect(ERROR_IMPOSSIBLE);
        for right_idx in (left_idx + 1)..live.len() {
            let right = nodes[live[right_idx]].as_ref().expect(ERROR_IMPOSSIBLE);
            fixture.connect_pair(left, right).await?;
        }
    }
    Ok(())
}

pub async fn apply_random_mutation<F: StressFixture>(
    fixture: &F,
    rng: &mut StdRng,
    state: &mut StressState,
    nodes: &[Option<F::Node>],
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
        let obj = state.publish_new_obj(rng);
        journal.record(format!(
            "{phase}:step={step}:create node={node_idx} obj={obj:?}"
        ));
        obj
    } else {
        state.choose_live_obj(rng).expect(ERROR_IMPOSSIBLE)
    };

    let nonce = rng.random::<u64>();
    let written_at = state.next_written_at();
    let value = fixture.make_doc_content(
        phase,
        step,
        node_idx,
        &obj,
        nonce,
        written_at,
        fixture.peer_id(node),
    );
    journal.record(format!(
        "{phase}:step={step}:upsert node={node_idx} obj={obj:?} value={value:?}"
    ));
    if fresh_obj {
        fixture.seed_new_obj(node, nodes, obj, value).await?;
    } else {
        fixture.seed_obj(node, obj, value).await?;
    }

    if rng.random_bool(0.25) {
        let sleep_ms = rng.random_range(1..15);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    Ok(())
}

pub async fn run_phase<F: StressFixture>(
    fixture: &F,
    rng: &mut StdRng,
    state: &mut StressState,
    nodes: &[Option<F::Node>],
    phase: &str,
    mutations: usize,
    journal: &StressJournal,
) -> Res<()> {
    for step in 0..mutations {
        apply_random_mutation(fixture, rng, state, nodes, phase, step, journal).await?;
    }
    Ok(())
}

pub async fn wait_for_cluster_settled<F: StressFixture>(
    fixture: &F,
    nodes: &[&F::Node],
    timeout: Duration,
    label: &str,
) -> Res<()> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_snapshot = None;
    let mut stable_rounds = 0usize;

    loop {
        let mut current = Vec::with_capacity(nodes.len());
        for node in nodes {
            current.push(fixture.observed_state(node).await?);
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
                "timed out waiting for stress cluster to settle at {label}: last_snapshot={last_snapshot:?}"
            ));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn run_randomized_four_node_stress<F: StressFixture>(
    fixture: F,
    world: Arc<F::World>,
    phase1_mutations: usize,
    phase2_mutations: usize,
    phase3_mutations: usize,
) -> Res<()> {
    run_randomized_four_node_stress_with_settle_timeout(
        fixture,
        world,
        phase1_mutations,
        phase2_mutations,
        phase3_mutations,
        Duration::from_secs(60),
    )
    .await
}

pub async fn run_randomized_four_node_stress_with_settle_timeout<F: StressFixture>(
    fixture: F,
    world: Arc<F::World>,
    phase1_mutations: usize,
    phase2_mutations: usize,
    phase3_mutations: usize,
    settle_timeout: Duration,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let seed = std::env::var("BIG_SYNC_STRESS_SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_STRESS_SEED);
    let mut rng = StdRng::seed_from_u64(seed);
    let journal = StressJournal::default();
    let mut state = StressState::default();
    journal.record(format!("seed={seed}"));
    journal.record(format!("backend={}", fixture.label()));

    let nodes = boot_cluster_for_fixture(&fixture, Arc::clone(&world)).await?;

    journal.record("phase1:start");
    let phase1_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase1:topology active={phase1_topology:?}"));
    connect_active_topology(&fixture, &mut rng, &nodes, &phase1_topology).await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase1:post-connect",
    )
    .await?;
    run_phase(
        &fixture,
        &mut rng,
        &mut state,
        &nodes,
        "phase1",
        phase1_mutations,
        &journal,
    )
    .await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase1:post-mutations",
    )
    .await?;

    journal.record("phase2:start");
    let phase2_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase2:topology active={phase2_topology:?}"));
    connect_active_topology(&fixture, &mut rng, &nodes, &phase2_topology).await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase2:post-connect",
    )
    .await?;
    run_phase(
        &fixture,
        &mut rng,
        &mut state,
        &nodes,
        "phase2",
        phase2_mutations,
        &journal,
    )
    .await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase2:post-mutations",
    )
    .await?;

    journal.record("phase3:start");
    let phase3_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase3:topology active={phase3_topology:?}"));
    connect_active_topology(&fixture, &mut rng, &nodes, &phase3_topology).await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase3:post-connect",
    )
    .await?;
    run_phase(
        &fixture,
        &mut rng,
        &mut state,
        &nodes,
        "phase3",
        phase3_mutations,
        &journal,
    )
    .await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase3:post-mutations",
    )
    .await?;

    journal.record("final:reconnect_full_mesh");
    connect_full_mesh(&fixture, &nodes).await?;
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        Duration::from_secs(60),
        "final:post-connect",
    )
    .await?;

    let refs = live_refs(&nodes);
    fixture.assert_cluster_alignment(&refs).await?;
    journal.record(format!(
        "final_journal_entries={}",
        journal.snapshot().len()
    ));

    for node in nodes.into_iter().flatten() {
        fixture.stop_node(node).await?;
    }

    Ok(())
}
