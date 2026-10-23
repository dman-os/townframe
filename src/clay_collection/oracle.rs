//! Model-based property test: random operation sequences over a three-hop chain checked
//! against a BTreeMap oracle.
//!
//! The chain is a replicated host consumer over a scripted authority (a recording upstream the
//! test answers by hand) with two guests: one replicated, one local. Every hop's visible state
//! must equal the oracle's — base rows overlaid by the pending writes resident at that hop —
//! after every operation. Two further invariants ride along: the flattened `changes` a
//! subscriber sees replay into the same visible state the query API reports, and a live
//! consumer fed by each hop's relay reconstructs the same state (a downstream hop must be
//! reconstructible from its relay feed alone).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use proptest::prelude::*;
use serde_json::json;

use crate::BaseDelta;
use crate::Batch;
use crate::DemandId;
use crate::DemandToken;
use crate::Downstream;
use crate::OperationId;
use crate::OverlayPolicy;
use crate::Query;
use crate::ScopeId;
use crate::Subscription;
use crate::Upstream;
use crate::Version;
use crate::View;
use crate::WriteOutcome;

type K = u32;
type V = u8;

const HOST: usize = 0;
const GUEST_LOCAL: usize = 2;

fn token() -> DemandToken {
    DemandToken(json!({ "kind": "all" }))
}

#[derive(Default)]
struct Recording {
    releases: Vec<DemandId>,
}

/// The scripted authority: it never answers on its own, so every base row, settlement, and
/// coverage mark is an explicit operation the generator can interleave with guest writes.
#[derive(Clone)]
struct MockUpstream {
    recording: Rc<RefCell<Recording>>,
}

impl Upstream<K, V> for MockUpstream {
    fn attach(&self, _downstream: &Downstream<K, V>) {}
    fn demand(&self, _token: DemandToken, _id: DemandId) {}
    fn release(&self, id: DemandId) {
        self.recording.borrow_mut().releases.push(id);
    }
    fn write(&self, _id: OperationId, _key: K, _value: Option<V>, _writer: ScopeId) {}
}

#[derive(Clone, Debug)]
enum Op {
    Write {
        hop: usize,
        key: K,
        value: Option<V>,
    },
    Settle {
        pick: usize,
        durable: bool,
    },
    Reset {
        rows: Vec<(K, V)>,
    },
    Dispose {
        hop: usize,
        scope: u64,
    },
    Demand {
        hop: usize,
    },
    Cover {
        pick: usize,
    },
    Release {
        pick: usize,
    },
    StaleBatch,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let key = || 0..6_u32;
    let value = || prop::option::of(0..4_u8);
    let hop = || 0..3_usize;
    prop_oneof![
        6 => (hop(), key(), value()).prop_map(|(hop, key, value)| Op::Write { hop, key, value }),
        4 => (any::<usize>(), any::<bool>())
            .prop_map(|(pick, durable)| Op::Settle { pick, durable }),
        1 => prop::collection::vec((key(), 0..4_u8), 0..3).prop_map(|rows| Op::Reset { rows }),
        2 => (hop(), 0..3_u64).prop_map(|(hop, scope)| Op::Dispose { hop, scope }),
        2 => hop().prop_map(|hop| Op::Demand { hop }),
        2 => any::<usize>().prop_map(|pick| Op::Cover { pick }),
        2 => any::<usize>().prop_map(|pick| Op::Release { pick }),
        1 => Just(Op::StaleBatch),
    ]
}

/// One unsettled write. `recorded` is the residency set: which hops hold the overlay. The
/// origin always records its own prediction; any other hop records it only when its policy is
/// `Replicated` — which is also the set of hops where the write is visible.
struct Pending {
    id: OperationId,
    key: K,
    value: Option<V>,
    origin: usize,
    recorded: [bool; 3],
}

#[derive(Debug)]
struct Demand {
    id: DemandId,
    hop: usize,
    covered: bool,
    released: bool,
}

struct Model {
    host: View<K, V>,
    guests: [View<K, V>; 2],
    replays: [View<K, V>; 3],
    recording: Rc<RefCell<Recording>>,
    shadows: [Rc<RefCell<BTreeMap<K, V>>>; 3],
    subscriptions: [Subscription<K, V>; 3],
    incarnation: u64,
    seq: u64,
    base: BTreeMap<K, V>,
    pending: Vec<Pending>,
    demands: Vec<Demand>,
}

fn policy(hop: usize) -> OverlayPolicy {
    match hop {
        GUEST_LOCAL => OverlayPolicy::Local,
        _ => OverlayPolicy::Replicated,
    }
}

/// Hops a Clear applied at `hop` reaches: the hop itself plus its downstream relays. The
/// chain is a star — the host feeds both guests, the guests feed nobody.
fn clear_reach(hop: usize) -> Vec<usize> {
    match hop {
        HOST => vec![HOST, 1, 2],
        _ => vec![hop],
    }
}

impl Model {
    fn new() -> Self {
        let recording = Rc::new(RefCell::new(Recording::default()));
        let host = View::<K, V>::consumer(
            OverlayPolicy::Replicated,
            1,
            Box::new(MockUpstream {
                recording: recording.clone(),
            }),
        );
        let guests = [
            View::<K, V>::consumer(OverlayPolicy::Replicated, 1, Box::new(host.clone())),
            View::<K, V>::consumer(OverlayPolicy::Local, 1, Box::new(host.clone())),
        ];
        // One live consumer per hop attached to that hop as its upstream: the relay feed must
        // reconstruct the hop's state on its own. The replay is always Replicated — a hop's
        // forward stream carries its own origin writes, which a Local replay would skip
        // recording even though the live hop holds them.
        let views = [&host, &guests[0], &guests[1]];
        let replays = views.map(|view| {
            View::<K, V>::consumer(OverlayPolicy::Replicated, 1, Box::new(view.clone()))
        });
        let shadows = [(); 3].map(|()| Rc::new(RefCell::new(BTreeMap::new())));
        // One subscriber per hop maintaining the flattened-`changes` shadow. Listener delivery
        // is row-change gated (relays are forward-gated), so this shadows the query API, not
        // the relay feed — that is what the replay consumers above cover.
        let subscriptions = [0, 1, 2].map(|hop| {
            let shadow = shadows[hop].clone();
            views[hop].subscribe(move |event| {
                let mut shadow = shadow.borrow_mut();
                for change in &event.changes {
                    match &change.after {
                        Some(value) => shadow.insert(change.key, *value),
                        None => shadow.remove(&change.key),
                    };
                }
            })
        });
        Self {
            host,
            guests,
            replays,
            recording,
            shadows,
            subscriptions,
            incarnation: 1,
            seq: 0,
            base: BTreeMap::new(),
            pending: Vec::new(),
            demands: Vec::new(),
        }
    }

    fn view(&self, hop: usize) -> &View<K, V> {
        match hop {
            HOST => &self.host,
            hop => &self.guests[hop - 1],
        }
    }

    /// The oracle's visible map for one hop: base rows overlaid by the pending writes resident
    /// at that hop, newest write per key winning (operation ids mint in write order).
    fn visible(&self, hop: usize) -> BTreeMap<K, V> {
        let mut visible = self.base.clone();
        for write in &self.pending {
            if write.recorded[hop] {
                match write.value {
                    Some(value) => visible.insert(write.key, value),
                    None => visible.remove(&write.key),
                };
            }
        }
        visible
    }

    fn apply(&mut self, op: Op) {
        match op {
            Op::Write { hop, key, value } => {
                let id = self.view(hop).write(key, value, ScopeId(hop as u64));
                let mut recorded = [false; 3];
                for (other, held) in recorded.iter_mut().enumerate() {
                    *held = other == hop || policy(other) == OverlayPolicy::Replicated;
                }
                self.pending.push(Pending {
                    id,
                    key,
                    value,
                    origin: hop,
                    recorded,
                });
            }
            Op::Settle { pick, durable } => {
                if self.pending.is_empty() {
                    return;
                }
                let write = self.pending.remove(pick % self.pending.len());
                let mut batch = Batch::new(self.incarnation);
                if durable {
                    self.seq += 1;
                    let version = Version::new(self.incarnation, self.seq);
                    batch.base.push(match write.value {
                        Some(value) => BaseDelta::Upsert {
                            key: write.key,
                            version,
                            value,
                        },
                        None => BaseDelta::Remove {
                            key: write.key,
                            version,
                        },
                    });
                    batch
                        .settled
                        .push((write.id, WriteOutcome::Durable { version }));
                    match write.value {
                        Some(value) => self.base.insert(write.key, value),
                        None => self.base.remove(&write.key),
                    };
                } else {
                    batch.settled.push((write.id, WriteOutcome::Rejected));
                }
                // The host records every write (its policy is Replicated), so a settlement it
                // applies always moves its own ledger; the event is never empty.
                let event = self.host.apply(batch);
                assert!(
                    !event.is_empty(),
                    "a settlement the host holds must publish"
                );
            }
            Op::Reset { rows } => {
                self.incarnation += 1;
                self.seq = 0;
                self.base.clear();
                let mut batch = Batch::new(self.incarnation);
                for (key, value) in rows {
                    self.seq += 1;
                    batch.base.push(BaseDelta::Upsert {
                        key,
                        version: Version::new(self.incarnation, self.seq),
                        value,
                    });
                    self.base.insert(key, value);
                }
                // A later incarnation wipes base, overlays, and coverage; demand leases
                // themselves survive and must be re-covered.
                self.pending.clear();
                for demand in &mut self.demands {
                    demand.covered = false;
                }
                self.host.apply(batch);
            }
            Op::Dispose { hop, scope } => {
                self.view(hop).dispose_scope(ScopeId(scope));
                // Disposal withdraws the overlays the disposing hop itself holds; the Clears
                // then ride its relays, so residency shrinks to the clear's reach.
                self.pending.retain_mut(|write| {
                    if write.origin as u64 == scope && write.recorded[hop] {
                        for reach in clear_reach(hop) {
                            write.recorded[reach] = false;
                        }
                    }
                    write.recorded.iter().any(|held| *held)
                });
            }
            Op::Demand { hop } => {
                let id = self.view(hop).demand(token());
                self.demands.push(Demand {
                    id,
                    hop,
                    covered: false,
                    released: false,
                });
            }
            Op::Cover { pick } => {
                let mut uncovered: Vec<usize> = self
                    .demands
                    .iter()
                    .enumerate()
                    .filter(|(_, demand)| !demand.covered && !demand.released)
                    .map(|(index, _)| index)
                    .collect();
                if uncovered.is_empty() {
                    return;
                }
                let index = uncovered.remove(pick % uncovered.len());
                let id = self.demands[index].id;
                let mut batch = Batch::new(self.incarnation);
                batch.covered.push(id);
                // Coverage entered at the host resolves the minter's lease: demand ids
                // register at every hop they pass through, so the forward filter keeps the id.
                self.host.apply(batch);
                self.demands[index].covered = true;
            }
            Op::Release { pick } => {
                let mut live: Vec<usize> = self
                    .demands
                    .iter()
                    .enumerate()
                    .filter(|(_, demand)| !demand.released)
                    .map(|(index, _)| index)
                    .collect();
                if live.is_empty() {
                    return;
                }
                let index = live.remove(pick % live.len());
                let (hop, id) = {
                    let demand = &mut self.demands[index];
                    demand.released = true;
                    demand.covered = false;
                    (demand.hop, demand.id)
                };
                self.view(hop).release(id);
            }
            Op::StaleBatch => {
                if self.incarnation <= 1 {
                    return;
                }
                let mut batch = Batch::new(self.incarnation - 1);
                batch.base.push(BaseDelta::Upsert {
                    key: u32::MAX,
                    version: Version::new(self.incarnation - 1, 1),
                    value: u8::MAX,
                });
                let event = self.host.apply(batch);
                assert!(event.is_empty(), "a stale incarnation must be ignored");
            }
        }
    }

    /// The per-operation invariant battery: every hop's query, ledger sizes, event-replay
    /// shadow, and every outstanding lease's coverage flag against the oracle.
    fn check(&self) {
        for hop in 0..3 {
            let expected = self.visible(hop);
            let actual: BTreeMap<K, V> = self.view(hop).query(&Query::new()).into_iter().collect();
            assert_eq!(actual, expected, "hop {hop} query diverged from the oracle");
            assert_eq!(
                *self.shadows[hop].borrow(),
                expected,
                "hop {hop} event replay diverged from the oracle"
            );
            assert_eq!(
                self.view(hop).overlay_len(),
                self.pending
                    .iter()
                    .filter(|write| write.recorded[hop])
                    .count(),
                "hop {hop} overlay count diverged"
            );
            assert_eq!(
                self.view(hop).base_len(),
                self.base.len(),
                "hop {hop} base count diverged"
            );
            let replayed: BTreeMap<K, V> =
                self.replays[hop].query(&Query::new()).into_iter().collect();
            assert_eq!(replayed, expected, "hop {hop} relay feed diverged");
            assert_eq!(
                self.replays[hop].overlay_len(),
                self.view(hop).overlay_len(),
                "hop {hop} replay overlay residency diverged"
            );
            assert_eq!(
                self.replays[hop].base_len(),
                self.view(hop).base_len(),
                "hop {hop} replay base residency diverged"
            );
        }
        for demand in &self.demands {
            assert_eq!(
                self.view(demand.hop).is_covered(demand.id),
                demand.covered,
                "coverage diverged for lease {demand:?}"
            );
        }
    }

    /// Teardown invariants: dropping the chain releases every outstanding lease upstream
    /// exactly once. Relay-feed fidelity is checked per operation in `check` via the live
    /// replay consumers.
    fn finish(self) {
        let Self {
            host,
            guests,
            replays,
            recording,
            demands,
            subscriptions,
            ..
        } = self;
        drop(subscriptions);
        drop(replays);
        let [guest_replicated, guest_local] = guests;
        drop(guest_local);
        drop(guest_replicated);
        drop(host);

        let mut expected: Vec<DemandId> = demands.iter().map(|demand| demand.id).collect();
        expected.sort();
        let mut released = recording.borrow().releases.clone();
        released.sort();
        assert_eq!(
            released, expected,
            "every demand must be released upstream exactly once"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]
    #[test]
    fn random_op_sequences_match_a_btreemap_oracle(ops in prop::collection::vec(op_strategy(), 20..60)) {
        let mut model = Model::new();
        for op in ops {
            model.apply(op);
            model.check();
        }
        model.finish();
    }
}
