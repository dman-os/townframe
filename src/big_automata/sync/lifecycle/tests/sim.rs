//! The differential proptest: random ops against [`PartLifecycle`] and the [`Oracle`], with the
//! parent's part of the protocol played by the sim: it persists what the machine asks for, adds
//! a part with the persisted cursor, and upgrades only parts that are walking.

use super::*;

use crate::sync::lifecycle::oracle::Oracle;
use proptest::prelude::*;

const PARTS: u8 = 4;

#[derive(Debug, Clone)]
pub(in crate::sync::lifecycle::tests) enum Spec {
    /// The peer's generation (0 or 1) and its latest.
    Cursor(u8, Rev),
    Walk(u8, Rev),
    Unknown,
}

#[derive(Debug, Clone)]
pub(in crate::sync::lifecycle::tests) enum CallPick {
    Current,
    /// An older call id: `issued - k`, which for 0 is the newest call.
    Past(u8),
}

#[derive(Debug, Clone)]
pub(in crate::sync::lifecycle::tests) enum Reply {
    /// An answer: per part (by index) an entry or none.
    Parts(Vec<Option<Spec>>),
    TooMany(usize),
    Failed,
}

#[derive(Debug, Clone)]
pub(in crate::sync::lifecycle::tests) enum Op {
    Add {
        part: u8,
    },
    Remove {
        part: u8,
    },
    Answer {
        call: CallPick,
        reply: Reply,
    },
    Applied {
        part: u8,
        cursor: Rev,
    },
    /// The `pick`th walking part, if any: the parent only upgrades walking parts.
    Upgrade {
        pick: usize,
    },
    Flush {
        secs: u8,
    },
}

pub(in crate::sync::lifecycle::tests) fn op() -> impl Strategy<Value = Op> {
    let spec = prop_oneof![
        2 => (0u8..2, 0u64..8).prop_map(|(generation, latest)| Spec::Cursor(generation, latest)),
        2 => (0u8..2, 0u64..8).prop_map(|(generation, latest)| Spec::Walk(generation, latest)),
        1 => Just(Spec::Unknown),
    ];
    let reply = prop_oneof![
        6 => proptest::collection::vec(proptest::option::weighted(0.7, spec), PARTS as usize)
            .prop_map(Reply::Parts),
        1 => (0usize..4).prop_map(Reply::TooMany),
        2 => Just(Reply::Failed),
    ];
    let call = prop_oneof![
        5 => Just(CallPick::Current),
        2 => (0u8..4).prop_map(CallPick::Past),
    ];
    prop_oneof![
        5 => (0..PARTS).prop_map(|part| Op::Add { part }),
        3 => (0..PARTS).prop_map(|part| Op::Remove { part }),
        8 => (call, reply).prop_map(|(call, reply)| Op::Answer { call, reply }),
        4 => (0..PARTS, 0u64..9).prop_map(|(part, cursor)| Op::Applied { part, cursor }),
        3 => any::<usize>().prop_map(|pick| Op::Upgrade { pick }),
        6 => (0u8..4).prop_map(|secs| Op::Flush { secs }),
    ]
}

pub(in crate::sync::lifecycle::tests) struct Sim {
    lc: PartLifecycle,
    oracle: Oracle,
    now: Instant,
    config: Config,
    /// The persisted cursors, written from the commands the machine emitted.
    store: Map<PartKey, PeerCursor>,
    /// The call the driver has open, from the `Decide` it executed.
    open: Option<CallId>,
    /// Parts retired since their last add.
    retired: Set<PartKey>,
    /// Parts restarted since their last persist: their next persist may be below the stored
    /// cursor, even in the same generation (law 10).
    rewound: Set<PartKey>,
}

impl Sim {
    pub(in crate::sync::lifecycle::tests) fn new(max_parts: usize) -> Self {
        let config = Config {
            max_parts,
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(3),
        };
        // part 1 was never seen; the others hold a cursor of generation 0
        let store: Map<PartKey, PeerCursor> = [3, 0, 5, 2]
            .into_iter()
            .enumerate()
            .filter(|(p, _)| *p != 1)
            .map(|(p, rev)| {
                (
                    part(p as u8),
                    PeerCursor {
                        generation: generation(0),
                        rev,
                    },
                )
            })
            .collect();
        Self {
            lc: PartLifecycle::new(config.clone()),
            oracle: Oracle::new(config.clone(), store.clone()),
            now: Instant::now(),
            config,
            store,
            open: None,
            retired: default(),
            rewound: default(),
        }
    }

    fn phases(&self) -> Vec<Option<Phase>> {
        (0..PARTS).map(|p| self.lc.phase(&part(p))).collect()
    }

    fn cursors(&self) -> Vec<Option<Rev>> {
        (0..PARTS).map(|p| self.lc.cursor(&part(p))).collect()
    }

    pub(in crate::sync::lifecycle::tests) fn step(&mut self, op: Op) {
        let before = self.phases();
        let cursors_before = self.cursors();
        let store_before = self.store.clone();
        let mut out = vec![];
        let expected;
        let mut decidable = vec![];
        match &op {
            Op::Add { part: p } => {
                let cursor = self.store.get(&part(*p)).copied();
                if before[*p as usize].is_none() {
                    self.retired.remove(&part(*p));
                }
                self.lc.add(part(*p), cursor);
                self.oracle.add(part(*p), cursor);
                expected = vec![];
            }
            Op::Remove { part: p } => {
                self.lc.remove(&part(*p), &mut out);
                expected = self.oracle.remove(&part(*p));
            }
            Op::Answer { call, reply } => {
                let id = match call {
                    CallPick::Current => self
                        .oracle
                        .open_call()
                        .unwrap_or_else(|| self.oracle.issued()),
                    CallPick::Past(k) => self.oracle.issued().saturating_sub((*k).into()),
                };
                let outcome = match reply {
                    Reply::Parts(entries) => DecideOutcome::Answered(
                        entries
                            .iter()
                            .enumerate()
                            .filter_map(|(p, spec)| {
                                let strat = match spec.as_ref()? {
                                    Spec::Cursor(n, latest) => cursor_in((*n).into(), *latest),
                                    Spec::Walk(n, latest) => walk_in((*n).into(), *latest),
                                    Spec::Unknown => PeerPartStrat::Unknown,
                                };
                                Some((part(p as u8), strat))
                            })
                            .collect(),
                    ),
                    Reply::TooMany(cap) => DecideOutcome::TooManyParts { cap: *cap },
                    Reply::Failed => DecideOutcome::Failed,
                };
                decidable = self.oracle.decidable(id);
                if self.open == Some(id) {
                    self.open = None;
                }
                self.lc.decided(id, outcome.clone(), &mut out);
                expected = self.oracle.decided(id, outcome);
            }
            Op::Applied { part: p, cursor } => {
                self.lc.applied(&part(*p), *cursor, &mut out);
                expected = self.oracle.applied(&part(*p), *cursor);
            }
            Op::Upgrade { pick } => {
                let walking: Vec<u8> = (0..PARTS)
                    .filter(|p| before[*p as usize] == Some(Phase::Walking))
                    .collect();
                if walking.is_empty() {
                    expected = vec![];
                } else {
                    let p = walking[pick % walking.len()];
                    self.lc.upgrade(&part(p), &mut out);
                    expected = self.oracle.upgrade(&part(p));
                }
            }
            Op::Flush { secs } => {
                self.now += Duration::from_secs((*secs).into());
                self.lc.flush(self.now, &mut out);
                expected = self.oracle.flush(self.now);
            }
        }
        assert_eq!(out, expected, "{op:?}: the commands the oracle expects");
        self.check_commands(&op, &out, &before, &store_before, &decidable);
        self.check_state(&before, &cursors_before);
    }

    /// The properties of the commands themselves, which hold whatever the oracle says.
    fn check_commands(
        &mut self,
        op: &Op,
        out: &[LifecycleCommand],
        before: &[Option<Phase>],
        store_before: &Map<PartKey, PeerCursor>,
        decidable: &[PartKey],
    ) {
        let phase_of = |p: &PartKey| before[(p.as_bytes()[0] - 100) as usize];
        let decides = out
            .iter()
            .filter(|cmd| matches!(cmd, LifecycleCommand::Decide { .. }))
            .count();
        assert!(decides <= 1, "{op:?}: two calls in one flush");
        for cmd in out {
            match cmd {
                LifecycleCommand::Decide { call, parts } => {
                    assert!(self.open.is_none(), "{op:?}: a second call in flight");
                    self.open = Some(*call);
                    assert!(!parts.is_empty());
                    assert!(parts.len() <= self.config.max_parts);
                    assert!(
                        parts.len() <= self.oracle.max_parts(),
                        "{op:?}: over the cap"
                    );
                }
                LifecycleCommand::Restart { part: p, held, .. } => {
                    assert!(decidable.contains(p), "{op:?}: decided an undecidable part");
                    assert_eq!(phase_of(p), Some(Phase::Deciding));
                    assert_eq!(
                        store_before.get(p),
                        Some(held),
                        "{op:?}: restarted from a cursor not stored"
                    );
                    self.rewound.insert(p.clone());
                }
                LifecycleCommand::StartCursor { part: p, .. }
                | LifecycleCommand::StartWalk { part: p, .. } => {
                    assert!(decidable.contains(p), "{op:?}: decided an undecidable part");
                    assert_eq!(phase_of(p), Some(Phase::Deciding));
                }
                LifecycleCommand::PersistCursor { part: p, cursor } => {
                    assert_eq!(
                        self.lc.phase(p),
                        Some(Phase::Cursor),
                        "{op:?}: persisted a part that is not in cursor"
                    );
                    match op {
                        Op::Applied { .. } => {
                            assert_eq!(phase_of(p), Some(Phase::Cursor), "persisted while walking");
                        }
                        Op::Upgrade { .. } => assert_eq!(phase_of(p), Some(Phase::Walking)),
                        _ => panic!("{op:?}: a persist from nowhere"),
                    }
                    if let Some(held) = store_before.get(p)
                        && held.generation == cursor.generation
                        && !self.rewound.contains(p)
                    {
                        assert!(
                            cursor.rev >= held.rev,
                            "{op:?}: the persisted cursor went back"
                        );
                    }
                    self.rewound.remove(p);
                    self.store.insert(p.clone(), *cursor);
                }
                LifecycleCommand::Retire { part: p } => {
                    assert!(
                        matches!(phase_of(p), Some(Phase::Walking | Phase::Cursor)),
                        "{op:?}: retired a part that never started"
                    );
                    assert_eq!(self.lc.phase(p), None);
                    assert!(self.retired.insert(p.clone()), "{op:?}: retired twice");
                    self.rewound.remove(p);
                }
            }
        }
    }

    fn check_state(&self, before: &[Option<Phase>], cursors_before: &[Option<Rev>]) {
        for p in 0..PARTS {
            let key = part(p);
            assert_eq!(self.lc.phase(&key), self.oracle.phase(&key), "phase of {p}");
            assert_eq!(
                self.lc.cursor(&key),
                self.oracle.cursor(&key),
                "cursor of {p}"
            );
            assert_eq!(
                self.store.get(&key).copied(),
                self.oracle.persisted(&key),
                "persisted cursor of {p}"
            );
            let i = p as usize;
            let started = |phase| matches!(phase, Some(Phase::Walking | Phase::Cursor));
            // A restart is the one way a held cursor goes back (law 10): the part was
            // `Deciding` before it, so it is not judged here.
            if started(before[i]) && started(self.lc.phase(&key)) {
                assert!(
                    self.lc.cursor(&key) >= cursors_before[i],
                    "the held cursor of {p} went back"
                );
            }
        }
        assert_eq!(self.lc.max_parts(), self.oracle.max_parts());
        assert_eq!(
            self.lc.flight.as_ref().map(|f| f.call),
            self.oracle.open_call()
        );
        assert_eq!(self.lc.flight.as_ref().map(|f| f.call), self.open);
        assert_eq!(self.lc.next_due(), self.oracle.next_due());
    }
}
