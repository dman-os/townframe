//! Two nodes that each run a driver against the other, and the same state arriving through a
//! third peer: replication that is symmetric must settle, not echo.
//!
//! A node here is one part store that is both served (a [`ScriptedPeer`], the real responders)
//! and written (the backend of its drivers). What a driver commits to its node's store is a new
//! row in that node's log, which the other node's driver pulls in turn, so a pull that restamped
//! rows it already held would ping-pong forever. What is judged:
//!
//! - both nodes converge on the union of what they started with;
//! - nothing is stamped once they have: the logs' heads stay where they are, over more than
//!   one expiry of a held round and across reconnects of both drivers;
//! - one write costs one commit on each node (the echo comes back and changes nothing);
//! - the order the drivers connect in does not change the result;
//! - the same state arriving from a second peer commits nothing.
//!
//! A storm keeps the loops busy, so nothing here waits on the virtual clock alone: the waits count
//! looks, and the heads are compared after a number of scheduler turns.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::rig::*;
use crate::driver::DriverConfig;
use crate::driver::Facts;
use crate::driver::ObjJob;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::Settle;
use crate::driver::SyncBackend;
use crate::driver::memory::LwwPayload;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::part_store::PartStoreServe;
use crate::part_store::laws::grant;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::rev::Rev;

const PARTS: [u8; 2] = [1, 2];
const MAX_OBJ: u8 = 8;

/// What a node holds: payload by (part, object).
type Content = BTreeMap<(u8, u8), serde_json::Value>;

/// The backend calls one node's drivers may make in a whole test. A run that settles needs a few
/// dozen; an echo storm is thousands a second, and without a bound it starves the single thread
/// the test runs on, so the test would hang instead of failing. Past the budget every call fails
/// (the machine backs off, the loops go idle and the test gets its turn) and the node reports it.
const CALL_BUDGET: usize = 500;

/// A [`MemoryBackend`] that counts its calls against [`CALL_BUDGET`].
struct Budgeted {
    inner: MemoryBackend,
    calls: Arc<AtomicUsize>,
}

impl Budgeted {
    fn admit(&self) -> Res<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) >= CALL_BUDGET {
            eyre::bail!("over the call budget");
        }
        Ok(())
    }
}

impl SyncBackend for Budgeted {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        self.admit()?;
        self.inner.record(job).await
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        self.admit()?;
        self.inner.fetch(job).await
    }
}

struct Node {
    id: u8,
    peer: ScriptedPeer,
    facts: Facts,
    /// The calls its drivers made to their backends.
    calls: Arc<AtomicUsize>,
    /// One driver per node this one pulls from.
    pulls: Vec<(u8, PeerDriver<ScriptedPeer>)>,
}

impl Node {
    async fn new(id: u8) -> Self {
        let peer = ScriptedPeer::new();
        // every node serves both parts to the principal the scripted transport acts as
        peer.commit(vec![grant(&[(1, 0), (2, 0)])]).await;
        Self {
            id,
            peer,
            facts: Facts::new(),
            calls: default(),
            pulls: vec![],
        }
    }

    fn store(&self) -> &MemoryPartStore {
        self.peer.store()
    }

    /// The key `from` is known by to this node's drivers.
    fn key_of(from: &Node) -> PeerKey {
        principal(10 + from.id)
    }

    /// Start a driver pulling both parts from `from`; it connects when asked to.
    async fn pull_from(&mut self, from: &Node) {
        let config = DriverConfig::new(Self::key_of(from), "memory");
        let (driver, driver_loop) = PeerDriver::start(
            config,
            clock(),
            self.facts.clone(),
            Budgeted {
                inner: MemoryBackend::new(self.store().clone(), from.store().clone()),
                calls: Arc::clone(&self.calls),
            },
            MemoryPeerCursors::default(),
            self.store().clone(),
        );
        tokio::spawn(driver_loop);
        driver
            .update_routes(RouteUpdate {
                added_parts: PARTS.iter().map(|&at| part(at)).collect(),
                ..default()
            })
            .await;
        self.pulls.push((from.id, driver));
    }

    fn connect_to(&self, from: &Node) {
        let (_, driver) = self
            .pulls
            .iter()
            .find(|(id, _)| *id == from.id)
            .expect("the node pulls from it");
        driver.connect(from.peer.clone());
    }

    fn disconnect_all(&self) {
        for (_, driver) in &self.pulls {
            driver.disconnect();
        }
    }

    async fn synced_with(&self, from: &[&Node]) {
        let pairs = from
            .iter()
            .flat_map(|from| PARTS.map(|at| (Self::key_of(from), part(at))));
        within(self.facts.wait_for_full_sync(pairs))
            .await
            .expect("every pair stays wanted");
    }

    async fn content(&self) -> Content {
        let mut content = Content::new();
        for at in PARTS {
            for (obj, payload) in Rig::members(self.store(), at, MAX_OBJ).await {
                content.insert((at, obj), payload);
            }
        }
        content
    }

    /// The drivers stayed within their budget: no echo storm.
    fn assert_no_storm(&self) {
        let calls = self.calls.load(Ordering::SeqCst);
        assert!(
            calls <= CALL_BUDGET,
            "node {} made {calls} backend calls: an echo storm",
            self.id
        );
    }

    async fn head(&self) -> Rev {
        self.store().latest_rev().await.expect("memory store")
    }
}

fn expected(entries: &[(u8, u8, u8)]) -> Content {
    entries
        .iter()
        .map(|&(at, obj, value)| ((at, obj), LwwPayload::at(value)))
        .collect()
}

/// Wait until every node holds exactly `expected`. The clock is advanced by hand between looks,
/// and the wait is bounded in looks, not in time: a storm keeps the loop busy, so a timeout on
/// the virtual clock would never fire and the test would hang instead of failing.
async fn converged(nodes: &[&Node], expected: &Content) {
    for _ in 0..20_000 {
        let mut all = true;
        for node in nodes {
            all &= node.content().await == *expected;
        }
        if all {
            return;
        }
        for node in nodes {
            node.assert_no_storm();
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    for node in nodes {
        assert_eq!(
            node.content().await,
            *expected,
            "node {} did not converge",
            node.id
        );
    }
}

async fn heads(nodes: &[&Node]) -> Vec<Rev> {
    let mut heads = vec![];
    for node in nodes {
        heads.push(node.head().await);
    }
    heads
}

/// Nothing is stamped from now on: not over many scheduler turns, and not over several expiries
/// of a held round. The turns come first: they catch a storm without the clock moving.
async fn assert_quiet(nodes: &[&Node]) {
    let before = heads(nodes).await;
    for node in nodes {
        node.assert_no_storm();
    }
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(heads(nodes).await, before, "the nodes keep committing");
    // more than ten expiries of a held round, a second at a time so the loops run in between
    for _ in 0..300 {
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(heads(nodes).await, before, "a held round stamped something");
    for node in nodes {
        node.assert_no_storm();
    }
}

/// Both nodes start with part 1 and part 2 of their own.
async fn two_nodes() -> (Node, Node, Content) {
    let (mut a, mut b) = (Node::new(1).await, Node::new(2).await);
    a.peer.publish(1, &[1, 2], 10).await;
    a.peer.publish(2, &[5], 50).await;
    b.peer.publish(1, &[3], 30).await;
    b.peer.publish(2, &[6], 60).await;
    a.pull_from(&b).await;
    b.pull_from(&a).await;
    let union = expected(&[(1, 1, 10), (1, 2, 10), (1, 3, 30), (2, 5, 50), (2, 6, 60)]);
    (a, b, union)
}

#[tokio::test(start_paused = true)]
async fn two_drivers_pulling_each_other_converge_and_go_quiet() {
    let (a, b, union) = two_nodes().await;
    a.connect_to(&b);
    b.connect_to(&a);
    converged(&[&a, &b], &union).await;
    a.synced_with(&[&b]).await;
    b.synced_with(&[&a]).await;
    assert_quiet(&[&a, &b]).await;

    // idle is idempotent: both drivers reconnect, replay everything from their persisted
    // cursors and find nothing to change
    a.disconnect_all();
    b.disconnect_all();
    a.connect_to(&b);
    b.connect_to(&a);
    a.synced_with(&[&b]).await;
    b.synced_with(&[&a]).await;
    assert_quiet(&[&a, &b]).await;
}

/// A write at one node is one commit there and one at the other: the echo of it comes back to
/// the writer and changes nothing.
#[tokio::test(start_paused = true)]
async fn a_write_costs_one_commit_on_each_node() {
    let (a, b, union) = two_nodes().await;
    a.connect_to(&b);
    b.connect_to(&a);
    converged(&[&a, &b], &union).await;
    assert_quiet(&[&a, &b]).await;
    let before = heads(&[&a, &b]).await;

    a.peer.commit(vec![LwwPayload::set(&[(1, 11)])]).await;
    let mut written = union.clone();
    written.insert((1, 1), LwwPayload::at(11));
    converged(&[&a, &b], &written).await;
    assert_quiet(&[&a, &b]).await;
    let after = heads(&[&a, &b]).await;
    assert_eq!(
        [after[0] - before[0], after[1] - before[1]],
        [1, 1],
        "heads {before:?} -> {after:?}"
    );
}

/// The order the two drivers connect in is not in the result.
#[tokio::test(start_paused = true)]
async fn the_connect_order_does_not_change_the_result() {
    for order in ["a first", "b first", "together", "b after a is synced"] {
        let (a, b, union) = two_nodes().await;
        match order {
            "a first" => {
                a.connect_to(&b);
                b.connect_to(&a);
            }
            "b first" => {
                b.connect_to(&a);
                a.connect_to(&b);
            }
            "together" => {
                // before either has had a turn
                b.connect_to(&a);
                a.connect_to(&b);
                tokio::task::yield_now().await;
            }
            "b after a is synced" => {
                a.connect_to(&b);
                a.synced_with(&[&b]).await;
                converged(&[&a], &union).await;
                b.connect_to(&a);
            }
            _ => unreachable!(),
        }
        converged(&[&a, &b], &union).await;
        assert_quiet(&[&a, &b]).await;
    }
}

/// The same state from a second peer is not a change: a node that holds what `b` holds and then
/// pulls `c`, which holds the same, commits nothing for it.
#[tokio::test(start_paused = true)]
async fn the_same_state_via_a_third_peer_stays_quiet() {
    let mut a = Node::new(1).await;
    let (b, c) = (Node::new(2).await, Node::new(3).await);
    for node in [&b, &c] {
        node.peer.publish(1, &[1, 2, 3], 10).await;
        node.peer.publish(2, &[5], 50).await;
    }
    a.pull_from(&b).await;
    a.pull_from(&c).await;
    let state = expected(&[(1, 1, 10), (1, 2, 10), (1, 3, 10), (2, 5, 50)]);

    a.connect_to(&b);
    converged(&[&a], &state).await;
    a.synced_with(&[&b]).await;
    assert_quiet(&[&a]).await;
    let head = a.head().await;

    a.connect_to(&c);
    a.synced_with(&[&b, &c]).await;
    assert_quiet(&[&a]).await;
    assert_eq!(a.head().await, head, "the second peer's copy was a change");
    assert_eq!(a.content().await, state);
}
