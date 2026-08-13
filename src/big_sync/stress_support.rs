use crate::interlude::*;

use big_sync_core::{Byte32Id, ObjId, PartId, PeerId};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tracing::{info, warn};

pub const STRESS_NODE_COUNT: usize = 3;
pub const PHASE1_MUTATIONS: usize = 48;
pub const PHASE2_MUTATIONS: usize = 24;
pub const PHASE3_MUTATIONS: usize = 32;
pub const DEFAULT_STRESS_SEED: u64 = 0xB1A0_5EED_5EED_0001;
pub const SLOW_OP_LOG_THRESHOLD: Duration = Duration::from_millis(250);
const STRESS_SETTLE_STABLE_ROUNDS: usize = 20;

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

#[async_trait]
pub trait StressFixture: Sync {
    type World: Send + Sync + 'static;
    type Node: Send + 'static;
    type StressObj: Clone + Send + Sync + std::fmt::Debug + 'static;
    type Observation: PartialEq + std::fmt::Debug;

    fn label(&self) -> &'static str;

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj;
    async fn boot_node(&self, world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node>;
    async fn stop_node(&self, node: Self::Node) -> Res<()>;
    async fn restart_node(
        &self,
        world: Arc<Self::World>,
        peer_seed: u8,
        node: Self::Node,
    ) -> Res<Self::Node> {
        self.stop_node(node).await?;
        self.boot_node(world, peer_seed).await
    }
    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()>;
    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()>;
    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        nodes: &[Option<Self::Node>],
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()>;
    async fn seed_obj(
        &self,
        node: &Self::Node,
        nodes: &[Option<Self::Node>],
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()>;
    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation>;
    fn peer_id(&self, node: &Self::Node) -> PeerId;
    // Fixture-specific application content for a document mutation.
    #[expect(clippy::too_many_arguments)]
    fn make_doc_content(
        &self,
        phase: &str,
        step: usize,
        node_idx: usize,
        obj: &Self::StressObj,
        nonce: u64,
        written_at: u64,
        writer_id: PeerId,
    ) -> serde_json::Value {
        stress_payload(phase, step, node_idx, obj, nonce, written_at, writer_id)
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

pub struct StressState<Obj> {
    next_written_at: u64,
    pub live_objs: Vec<Obj>,
}

impl<Obj> Default for StressState<Obj> {
    fn default() -> Self {
        Self {
            next_written_at: 0,
            live_objs: Vec::new(),
        }
    }
}

impl<Obj: Clone> StressState<Obj> {
    pub fn choose_live_obj(&self, rng: &mut StdRng) -> Option<Obj> {
        if self.live_objs.is_empty() {
            return None;
        }
        Some(self.live_objs[rng.random_range(0..self.live_objs.len())].clone())
    }

    pub fn publish_new_obj(&mut self, obj: Obj) -> Obj {
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
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    ObjId(Byte32Id::new(bytes))
}

pub fn stress_payload(
    phase: &str,
    step: usize,
    node_idx: usize,
    obj: &impl std::fmt::Debug,
    nonce: u64,
    written_at: u64,
    writer_id: PeerId,
) -> serde_json::Value {
    payload(
        format!("{phase}:step={step}:node={node_idx}:obj={obj:?}:nonce={nonce}"),
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

fn log_if_slow(label: &str, started_at: std::time::Instant) {
    let elapsed = started_at.elapsed();
    if elapsed >= SLOW_OP_LOG_THRESHOLD {
        warn!(%label, ?elapsed, "stress operation took longer than expected");
    }
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
    let started_at = std::time::Instant::now();
    disconnect_all(fixture, nodes).await?;
    if active_idxs.len() < 2 {
        log_if_slow("connect_active_topology", started_at);
        return Ok(());
    }

    let mut chain = active_idxs.to_vec();
    chain.shuffle(rng);
    for pair in chain.windows(2) {
        let left = nodes[pair[0]].as_ref().expect(ERROR_IMPOSSIBLE);
        let right = nodes[pair[1]].as_ref().expect(ERROR_IMPOSSIBLE);
        fixture.connect_pair(left, right).await?;
    }

    for ii in 0..chain.len() {
        for jjj in (ii + 2)..chain.len() {
            if rng.random_bool(0.35) {
                let left = nodes[chain[ii]].as_ref().expect(ERROR_IMPOSSIBLE);
                let right = nodes[chain[jjj]].as_ref().expect(ERROR_IMPOSSIBLE);
                fixture.connect_pair(left, right).await?;
            }
        }
    }
    log_if_slow("connect_active_topology", started_at);
    Ok(())
}

pub async fn connect_full_mesh<F: StressFixture>(
    fixture: &F,
    nodes: &[Option<F::Node>],
) -> Res<()> {
    let started_at = std::time::Instant::now();
    let live = live_indices(nodes);
    for left_idx in 0..live.len() {
        let left = nodes[live[left_idx]].as_ref().expect(ERROR_IMPOSSIBLE);
        for right_idx in (left_idx + 1)..live.len() {
            let right = nodes[live[right_idx]].as_ref().expect(ERROR_IMPOSSIBLE);
            fixture.connect_pair(left, right).await?;
        }
    }
    log_if_slow("connect_full_mesh", started_at);
    Ok(())
}

pub async fn apply_random_mutation<F: StressFixture>(
    fixture: &F,
    rng: &mut StdRng,
    state: &mut StressState<F::StressObj>,
    nodes: &[Option<F::Node>],
    phase: &str,
    step: usize,
    journal: &StressJournal,
) -> Res<()> {
    let started_at = std::time::Instant::now();
    let live = live_indices(nodes);
    if live.is_empty() {
        return Ok(());
    }

    let node_idx = live[rng.random_range(0..live.len())];
    let node = nodes[node_idx].as_ref().expect(ERROR_IMPOSSIBLE);
    let fresh_obj = state.live_objs.is_empty() || rng.random_bool(0.30);
    let obj = if fresh_obj {
        let obj = fixture.make_stress_obj(rng);
        let obj = state.publish_new_obj(obj);
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
        fixture.seed_new_obj(node, nodes, &obj, value).await?;
    } else {
        fixture.seed_obj(node, nodes, &obj, value).await?;
    }

    if rng.random_bool(0.25) {
        let sleep_ms = rng.random_range(1..15);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    log_if_slow("apply_random_mutation", started_at);
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub async fn run_phase<F: StressFixture>(
    fixture: &F,
    world: Arc<F::World>,
    rng: &mut StdRng,
    state: &mut StressState<F::StressObj>,
    nodes: &mut [Option<F::Node>],
    phase: &str,
    mutations: usize,
    journal: &StressJournal,
) -> Res<()> {
    let started_at = std::time::Instant::now();
    for step in 0..mutations {
        apply_random_mutation(fixture, rng, state, nodes, phase, step, journal).await?;
        maybe_restart_node(
            fixture,
            Arc::clone(&world),
            rng,
            nodes,
            phase,
            step,
            journal,
        )
        .await?;
    }
    info!(phase, elapsed = ?started_at.elapsed(), "completed stress phase");
    Ok(())
}

pub async fn maybe_restart_node<F: StressFixture>(
    fixture: &F,
    world: Arc<F::World>,
    rng: &mut StdRng,
    nodes: &mut [Option<F::Node>],
    phase: &str,
    step: usize,
    journal: &StressJournal,
) -> Res<()> {
    if !rng.random_bool(0.08) {
        return Ok(());
    }
    let started_at = std::time::Instant::now();
    let live = live_indices(nodes);
    if live.len() < 2 {
        return Ok(());
    }

    let node_idx = live[rng.random_range(0..live.len())];
    let peer_seed = u8::try_from(node_idx + 1).expect("stress node index should fit in u8");
    journal.record(format!("{phase}:step={step}:restart node={node_idx}"));
    info!(phase, step, node_idx, "restarting stress node");

    disconnect_all(fixture, nodes).await?;
    let node = nodes[node_idx]
        .take()
        .expect("restart target should be live");
    let restarted = fixture.restart_node(world, peer_seed, node).await?;
    nodes[node_idx] = Some(restarted);

    let active = choose_active_topology(rng, nodes);
    journal.record(format!(
        "{phase}:step={step}:post-restart-topology active={active:?}"
    ));
    connect_active_topology(fixture, rng, nodes, &active).await?;
    info!(
        phase,
        step,
        node_idx,
        elapsed = ?started_at.elapsed(),
        "finished stress node restart"
    );
    Ok(())
}

pub async fn wait_for_cluster_settled<F: StressFixture>(
    fixture: &F,
    nodes: &[&F::Node],
    timeout: Duration,
    label: &str,
) -> Res<()> {
    let started_at = std::time::Instant::now();
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
            if stable_rounds >= STRESS_SETTLE_STABLE_ROUNDS {
                log_if_slow(label, started_at);
                return Ok(());
            }
        } else {
            stable_rounds = 1;
        }

        last_snapshot = Some(current);
        if std::time::Instant::now() >= deadline {
            log_if_slow(label, started_at);
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
    let mut state: StressState<F::StressObj> = StressState::default();
    journal.record(format!("seed={seed}"));
    journal.record(format!("backend={}", fixture.label()));

    let boot_started_at = std::time::Instant::now();
    let mut nodes = boot_cluster_for_fixture(&fixture, Arc::clone(&world)).await?;
    info!(elapsed = ?boot_started_at.elapsed(), "booted stress cluster");

    journal.record("phase1:start");
    info!("stress phase1 connect start");
    let phase1_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase1:topology active={phase1_topology:?}"));
    let phase1_connect_started_at = std::time::Instant::now();
    connect_active_topology(&fixture, &mut rng, &nodes, &phase1_topology).await?;
    info!(
        elapsed = ?phase1_connect_started_at.elapsed(),
        "stress phase1 connect complete"
    );
    let phase1_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase1:post-connect",
    )
    .await?;
    info!(
        elapsed = ?phase1_settle_started_at.elapsed(),
        "stress phase1 post-connect settled"
    );
    let phase1_mutate_started_at = std::time::Instant::now();
    run_phase(
        &fixture,
        Arc::clone(&world),
        &mut rng,
        &mut state,
        &mut nodes,
        "phase1",
        phase1_mutations,
        &journal,
    )
    .await?;
    info!(elapsed = ?phase1_mutate_started_at.elapsed(), "stress phase1 mutations complete");
    let phase1_post_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase1:post-mutations",
    )
    .await?;
    info!(
        elapsed = ?phase1_post_settle_started_at.elapsed(),
        "stress phase1 post-mutations settled"
    );

    journal.record("phase2:start");
    info!("stress phase2 connect start");
    let phase2_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase2:topology active={phase2_topology:?}"));
    let phase2_connect_started_at = std::time::Instant::now();
    connect_active_topology(&fixture, &mut rng, &nodes, &phase2_topology).await?;
    info!(
        elapsed = ?phase2_connect_started_at.elapsed(),
        "stress phase2 connect complete"
    );
    let phase2_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase2:post-connect",
    )
    .await?;
    info!(
        elapsed = ?phase2_settle_started_at.elapsed(),
        "stress phase2 post-connect settled"
    );
    let phase2_mutate_started_at = std::time::Instant::now();
    run_phase(
        &fixture,
        Arc::clone(&world),
        &mut rng,
        &mut state,
        &mut nodes,
        "phase2",
        phase2_mutations,
        &journal,
    )
    .await?;
    info!(elapsed = ?phase2_mutate_started_at.elapsed(), "stress phase2 mutations complete");
    let phase2_post_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase2:post-mutations",
    )
    .await?;
    info!(
        elapsed = ?phase2_post_settle_started_at.elapsed(),
        "stress phase2 post-mutations settled"
    );

    journal.record("phase3:start");
    info!("stress phase3 connect start");
    let phase3_topology = choose_active_topology(&mut rng, &nodes);
    journal.record(format!("phase3:topology active={phase3_topology:?}"));
    let phase3_connect_started_at = std::time::Instant::now();
    connect_active_topology(&fixture, &mut rng, &nodes, &phase3_topology).await?;
    info!(
        elapsed = ?phase3_connect_started_at.elapsed(),
        "stress phase3 connect complete"
    );
    let phase3_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase3:post-connect",
    )
    .await?;
    info!(
        elapsed = ?phase3_settle_started_at.elapsed(),
        "stress phase3 post-connect settled"
    );
    let phase3_mutate_started_at = std::time::Instant::now();
    run_phase(
        &fixture,
        Arc::clone(&world),
        &mut rng,
        &mut state,
        &mut nodes,
        "phase3",
        phase3_mutations,
        &journal,
    )
    .await?;
    info!(elapsed = ?phase3_mutate_started_at.elapsed(), "stress phase3 mutations complete");
    let phase3_post_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        settle_timeout,
        "phase3:post-mutations",
    )
    .await?;
    info!(
        elapsed = ?phase3_post_settle_started_at.elapsed(),
        "stress phase3 post-mutations settled"
    );

    journal.record("final:reconnect_full_mesh");
    let final_connect_started_at = std::time::Instant::now();
    connect_full_mesh(&fixture, &nodes).await?;
    info!(
        elapsed = ?final_connect_started_at.elapsed(),
        "stress final full-mesh reconnect complete"
    );
    let final_settle_started_at = std::time::Instant::now();
    wait_for_cluster_settled(
        &fixture,
        &live_refs(&nodes),
        Duration::from_secs(60),
        "final:post-connect",
    )
    .await?;
    info!(
        elapsed = ?final_settle_started_at.elapsed(),
        "stress final cluster settle complete"
    );

    let refs = live_refs(&nodes);
    let align_started_at = std::time::Instant::now();
    fixture.assert_cluster_alignment(&refs).await?;
    info!(elapsed = ?align_started_at.elapsed(), "stress cluster alignment complete");
    journal.record(format!(
        "final_journal_entries={}",
        journal.snapshot().len()
    ));

    for node in nodes.into_iter().flatten() {
        fixture.stop_node(node).await?;
    }

    Ok(())
}
