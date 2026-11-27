//! The ops a case is made of, and the proptest over them.

use proptest::prelude::*;

use super::*;

#[derive(Debug, Clone)]
pub(in crate::document) enum Op {
    /// `kind` picks the write: mostly normal, sometimes a no-op, a failing closure or a stale
    /// handle.
    Write {
        doc: u8,
        kind: u8,
    },
    Read {
        doc: u8,
    },
    /// An external author sends a change (or the last one again).
    Ingest {
        doc: u8,
        dup: bool,
    },
    Open {
        doc: u8,
    },
    DropHandle {
        doc: u8,
        pick: u8,
    },
    Interest {
        doc: u8,
        acquire: bool,
    },
    RemoteWrite {
        doc: u8,
        keyed: bool,
    },
    Keys {
        doc: u8,
    },
    Access {
        doc: u8,
        readable: bool,
    },
    PlanPrepare {
        mask: u8,
    },
    PlanFinish {
        pick: u8,
        commit: bool,
    },
    /// The driver finishes a call in flight.
    Complete {
        pick: u8,
        fail: bool,
    },
    /// The driver finishes a call, and a commit lands though it is reported failed.
    CompleteUndecided(u8),
    /// A commit that was reported failed runs after all.
    LandZombie(u8),
    Deliver(u8),
    Duplicate(u8),
    Advance(u8),
    Tick,
    Evict(u8),
    Crash {
        landed: u8,
    },
}

impl Sim {
    pub fn step(&mut self, op: Op) {
        if self.trace {
            eprintln!("{op:?}");
        }
        let doc = |doc: u8| usize::from(doc) % DOCS;
        match op {
            Op::Write { doc: d, kind } => {
                let kind = match kind % 8 {
                    0..=4 => WriteKind::Normal,
                    5 => WriteKind::Noop,
                    6 => WriteKind::Bad,
                    _ => WriteKind::Stale,
                };
                self.write(doc(d), kind);
            }
            Op::Ingest { doc: d, dup } => {
                self.ingest(doc(d), dup);
            }
            Op::Read { doc: d } => {
                self.read(doc(d));
            }
            Op::Open { doc: d } => {
                self.open(doc(d));
            }
            Op::DropHandle { doc: d, pick } => self.drop_handle(doc(d), usize::from(pick)),
            Op::Interest { doc: d, acquire } => self.interest(doc(d), acquire),
            Op::RemoteWrite { doc: d, keyed } => self.remote_write(doc(d), keyed),
            Op::Keys { doc: d } => self.grant_keys(doc(d)),
            Op::Access { doc: d, readable } => self.set_readable(doc(d), readable),
            Op::PlanPrepare { mask } => self.plan_prepare(usize::from(mask)),
            Op::PlanFinish { pick, commit } => self.plan_finish(usize::from(pick), commit),
            Op::Complete { pick, fail } => self.complete(usize::from(pick), fail),
            Op::CompleteUndecided(pick) => {
                let now = self.draw(2) == 0;
                self.complete_as(usize::from(pick), false, Some(now));
            }
            Op::LandZombie(pick) => self.land_zombie(usize::from(pick)),
            Op::Deliver(pick) => self.deliver(usize::from(pick)),
            Op::Duplicate(pick) => self.duplicate(usize::from(pick)),
            Op::Advance(by) => self.advance(Duration::from_millis(250 * u64::from(by))),
            Op::Tick => self.tick(),
            Op::Evict(d) => {
                let d = doc(d);
                if self.machines[d].as_ref().is_some_and(|m| m.doc.evictable()) {
                    self.evict(d);
                }
            }
            Op::Crash { landed } => self.crash(usize::from(landed)),
        }
        self.hub();
        self.check();
    }
}

pub(in crate::document) fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (any::<u8>(), any::<u8>()).prop_map(|(doc, kind)| Op::Write { doc, kind }),
        4 => any::<u8>().prop_map(|doc| Op::Read { doc }),
        5 => (any::<u8>(), any::<bool>()).prop_map(|(doc, dup)| Op::Ingest { doc, dup }),
        3 => any::<u8>().prop_map(|doc| Op::Open { doc }),
        3 => (any::<u8>(), any::<u8>()).prop_map(|(doc, pick)| Op::DropHandle { doc, pick }),
        2 => (any::<u8>(), any::<bool>()).prop_map(|(doc, acquire)| Op::Interest { doc, acquire }),
        4 => (any::<u8>(), any::<bool>()).prop_map(|(doc, keyed)| Op::RemoteWrite { doc, keyed }),
        2 => any::<u8>().prop_map(|doc| Op::Keys { doc }),
        1 => (any::<u8>(), any::<bool>()).prop_map(|(doc, readable)| Op::Access { doc, readable }),
        4 => any::<u8>().prop_map(|mask| Op::PlanPrepare { mask: mask & 3 }),
        4 => (any::<u8>(), any::<bool>()).prop_map(|(pick, commit)| Op::PlanFinish { pick, commit }),
        20 => (any::<u8>(), proptest::bool::weighted(0.1)).prop_map(|(pick, fail)| Op::Complete { pick, fail }),
        3 => any::<u8>().prop_map(Op::CompleteUndecided),
        2 => any::<u8>().prop_map(Op::LandZombie),
        8 => any::<u8>().prop_map(Op::Deliver),
        2 => any::<u8>().prop_map(Op::Duplicate),
        2 => any::<u8>().prop_map(Op::Advance),
        2 => Just(Op::Tick),
        2 => any::<u8>().prop_map(Op::Evict),
        1 => any::<u8>().prop_map(|landed| Op::Crash { landed }),
    ]
}

/// Run `ops`, drain, check, then crash, restart and drain again.
pub(in crate::document) fn run(ops: Vec<Op>, seed: u64, plant: Option<Plant>) {
    let mut sim = Sim::new(seed, plant);
    for op in ops {
        sim.step(op);
    }
    sim.quiesce();
    sim.crash(0);
    sim.quiesce();
}
