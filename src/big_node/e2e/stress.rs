//! Seeded randomized stress over the N-node arena (ADR 033 section 9): four nodes in a mesh take a
//! seeded interleaving of notes created and edited, access granted, nodes stopped and booted
//! again, and nodes partitioned (booted again but not relinked) and healed, with edits on both
//! sides of a partition. Then every node is brought back, linked and admitted, and every note
//! must read the same, with every edit, on every node.
//!
//! The seed is `TEST_SEED` (decimal, `0x` hex, or `rand`), defaulting to [`DEFAULT_SEED`]; it is
//! logged first and printed again if the run panics, so a failure is replayed by setting it.
//!
//! Edits never conflict on a value: node `i` writes only field `by{i}` of a note, each time a
//! fresh value, always on top of what it has. Its last value is then the field's value wherever
//! the note converges, which is what the end checks.

use super::harness::Cluster;
use super::harness::SharedDrawer;
use super::harness::Topology;
use big_cabinet_core::DocId;
use big_cabinet_core::drawer::DrawerAccess;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

const DEFAULT_SEED: u64 = 0xB16_0DE5_EED0_0001;
const NODES: [&str; 4] = ["alice", "bob", "carol", "dave"];
const STEPS: usize = 40;
/// The node that owns the shared drawer and admits the others.
const OWNER: usize = 0;

/// What the run expects each note to read once converged: field to value.
type Expected = BTreeMap<DocId, BTreeMap<String, String>>;

/// Prints the seed if the run panics, wherever the panic comes from.
struct SeedOnPanic(u64);

impl Drop for SeedOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("STRESS failed with TEST_SEED={:#x}", self.0);
        }
    }
}

struct Run {
    cluster: Cluster,
    drawer: SharedDrawer,
    rng: StdRng,
    members: BTreeSet<usize>,
    expected: Expected,
    writes: usize,
}

impl Run {
    fn pick<T: Copy>(&mut self, from: &[T]) -> Option<T> {
        from.choose(&mut self.rng).copied()
    }

    fn running_members(&self) -> Vec<usize> {
        self.cluster
            .running()
            .into_iter()
            .filter(|index| self.members.contains(index))
            .collect()
    }

    async fn create(&mut self) -> Option<String> {
        let node = self.pick(&self.running_members())?;
        self.writes += 1;
        let text = format!("created by {node} (write {})", self.writes);
        let Some(doc) = self
            .cluster
            .try_create_note(node, &self.drawer, &text)
            .await
        else {
            return Some(format!(
                "{} is refused a note: the drawer has not reached it yet",
                NODES[node]
            ));
        };
        self.expected
            .insert(doc.clone(), BTreeMap::from([("main".to_owned(), text)]));
        Some(format!("{} creates {doc}", NODES[node]))
    }

    /// A member that can read the note now edits its own field of it.
    async fn edit(&mut self) -> Option<String> {
        let docs: Vec<DocId> = self.expected.keys().cloned().collect();
        let doc = docs.choose(&mut self.rng)?.clone();
        let mut editors = Vec::new();
        for node in self.running_members() {
            if self.cluster.read_note(node, &doc).await.is_some() {
                editors.push(node);
            }
        }
        let node = self.pick(&editors)?;
        self.writes += 1;
        let field = format!("by{node}");
        let text = format!("write {}", self.writes);
        self.cluster.update_note(node, &doc, &field, &text).await;
        self.expected.get_mut(&doc).unwrap().insert(field, text);
        Some(format!("{} edits {doc}", NODES[node]))
    }

    async fn grant(&mut self) -> Option<String> {
        if !self.cluster.is_running(OWNER) {
            return None;
        }
        let outsiders: Vec<usize> = self
            .cluster
            .running()
            .into_iter()
            .filter(|index| !self.members.contains(index))
            .collect();
        let node = self.pick(&outsiders)?;
        self.cluster
            .admit(OWNER, node, &self.drawer.id, DrawerAccess::Write)
            .await;
        self.members.insert(node);
        Some(format!("{} admits {}", NODES[OWNER], NODES[node]))
    }

    /// Stops a node, keeping at least two running.
    async fn stop(&mut self) -> Option<String> {
        let running = self.cluster.running();
        if running.len() <= 2 {
            return None;
        }
        let node = self.pick(&running)?;
        self.cluster.stop(node).await;
        Some(format!("{} stops", NODES[node]))
    }

    async fn start(&mut self) -> Option<String> {
        let stopped: Vec<usize> = (0..NODES.len())
            .filter(|index| !self.cluster.is_running(*index))
            .collect();
        let node = self.pick(&stopped)?;
        self.cluster.restart(node).await;
        self.cluster.reconnect(node).await;
        Some(format!("{} starts", NODES[node]))
    }

    /// Boots a running node again without linking it: it keeps working, alone, until healed.
    async fn partition(&mut self) -> Option<String> {
        let running = self.cluster.running();
        let node = self.pick(&running)?;
        self.cluster.stop(node).await;
        self.cluster.restart(node).await;
        Some(format!("{} is partitioned", NODES[node]))
    }

    async fn heal(&mut self) -> Option<String> {
        let partitioned: Vec<usize> = self
            .cluster
            .running()
            .into_iter()
            .filter(|index| self.cluster.is_partitioned(*index))
            .collect();
        let node = self.pick(&partitioned)?;
        self.cluster.reconnect(node).await;
        Some(format!("{} heals", NODES[node]))
    }

    /// One seeded step. A step whose precondition does not hold (nothing to edit, no outsider
    /// to admit) does nothing; the step count is a budget, not a promise.
    async fn step(&mut self) -> Option<String> {
        match self.rng.random_range(0..100) {
            0..30 => self.create().await,
            30..60 => self.edit().await,
            60..68 => self.grant().await,
            68..77 => self.stop().await,
            77..86 => self.start().await,
            86..93 => self.partition().await,
            _ => self.heal().await,
        }
    }

    /// Every node running, linked along the mesh and admitted to the drawer.
    async fn quiesce(&mut self) {
        for node in 0..NODES.len() {
            if !self.cluster.is_running(node) {
                self.cluster.restart(node).await;
            }
        }
        for node in 0..NODES.len() {
            self.cluster.reconnect(node).await;
        }
        for node in 0..NODES.len() {
            if self.members.insert(node) {
                self.cluster
                    .admit(OWNER, node, &self.drawer.id, DrawerAccess::Write)
                    .await;
            }
        }
    }
}

async fn stress(seed: u64) {
    let _seed = SeedOnPanic(seed);
    let mut cluster = Cluster::boot(&NODES).await;
    tracing::info!(seed = format!("{seed:#x}"), "STRESS seed");
    eprintln!("STRESS TEST_SEED={seed:#x}");
    cluster.connect(Topology::Mesh).await;
    let drawer = cluster.share_drawer(OWNER, &[OWNER, 1]).await;
    let mut run = Run {
        cluster,
        drawer,
        rng: StdRng::seed_from_u64(seed),
        members: BTreeSet::from([OWNER, 1]),
        expected: Expected::new(),
        writes: 0,
    };
    for step in 0..STEPS {
        if let Some(action) = run.step().await {
            tracing::info!(step, action, "STRESS step");
        }
    }
    run.quiesce().await;
    tracing::info!(
        notes = run.expected.len(),
        "STRESS quiesced; awaiting convergence"
    );
    for (doc, fields) in &run.expected {
        for note in run.cluster.converged(doc).await {
            for (field, text) in fields {
                assert_eq!(
                    note.text(field).as_deref(),
                    Some(text.as_str()),
                    "{doc} field {field}"
                );
            }
        }
    }
    eprintln!(
        "STRESS TEST_SEED={seed:#x}: {} notes, {} writes converged on {} nodes",
        run.expected.len(),
        run.writes,
        NODES.len()
    );
    run.cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_seeded_partitions_restarts_and_grants_converge() {
    stress(utils_rs::testing::test_seed(DEFAULT_SEED)).await;
}
