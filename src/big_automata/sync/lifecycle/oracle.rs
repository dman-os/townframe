//! The dumbest model of `PartLifecycle`: one flat vector of parts scanned and sorted on every
//! call, no queue index, and a backoff kept as a failure count instead of a duration. It
//! answers the phase and held cursor of a part, the persisted cursor per part, which parts a
//! call's answer may still decide, and the commands each step must emit.

use super::*;

#[derive(Debug, Clone)]
struct OPart {
    part: PartKey,
    phase: Phase,
    /// The call a `Deciding` part is under; `None` while it waits for one.
    call: Option<CallId>,
    generation: Option<Generation>,
    cursor: Rev,
    /// Queue age: smaller is older.
    queued_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OGate {
    Open,
    Unarmed,
    Until(Instant),
}

#[derive(Debug)]
pub(super) struct Oracle {
    config: Config,
    max_parts: usize,
    parts: Vec<OPart>,
    clock: u64,
    issued: CallId,
    open: Option<CallId>,
    /// Consecutive backed-off outcomes; the backoff is derived from it.
    failures: u32,
    gate: OGate,
    last_flush: Option<Instant>,
    persisted: Map<PartKey, PeerCursor>,
}

impl Oracle {
    pub(super) fn new(config: Config, persisted: Map<PartKey, PeerCursor>) -> Self {
        Self {
            max_parts: config.max_parts,
            config,
            parts: vec![],
            clock: 0,
            issued: 0,
            open: None,
            failures: 0,
            gate: OGate::Open,
            last_flush: None,
            persisted,
        }
    }

    fn find(&self, part: &PartKey) -> Option<&OPart> {
        self.parts.iter().find(|p| p.part == *part)
    }

    fn find_mut(&mut self, part: &PartKey) -> Option<&mut OPart> {
        self.parts.iter_mut().find(|p| p.part == *part)
    }

    pub(super) fn phase(&self, part: &PartKey) -> Option<Phase> {
        self.find(part).map(|p| p.phase)
    }

    pub(super) fn cursor(&self, part: &PartKey) -> Option<Rev> {
        self.find(part).map(|p| p.cursor)
    }

    pub(super) fn persisted(&self, part: &PartKey) -> Option<PeerCursor> {
        self.persisted.get(part).copied()
    }

    pub(super) fn max_parts(&self) -> usize {
        self.max_parts
    }

    pub(super) fn open_call(&self) -> Option<CallId> {
        self.open
    }

    pub(super) fn issued(&self) -> CallId {
        self.issued
    }

    /// The parts an answer to `call` may decide: the ones still `Deciding` under it.
    pub(super) fn decidable(&self, call: CallId) -> Vec<PartKey> {
        self.sorted(|p| p.phase == Phase::Deciding && p.call == Some(call))
    }

    fn sorted(&self, keep: impl Fn(&OPart) -> bool) -> Vec<PartKey> {
        let mut found: Vec<&OPart> = self.parts.iter().filter(|p| keep(p)).collect();
        found.sort_by_key(|p| p.queued_at);
        found.into_iter().map(|p| p.part.clone()).collect()
    }

    pub(super) fn add(&mut self, part: PartKey, cursor: Option<PeerCursor>) {
        if self.find(&part).is_some() {
            return;
        }
        self.clock += 1;
        self.parts.push(OPart {
            part,
            phase: Phase::Deciding,
            call: None,
            generation: cursor.map(|cursor| cursor.generation),
            cursor: cursor.map_or(0, |cursor| cursor.rev),
            queued_at: self.clock,
        });
    }

    pub(super) fn remove(&mut self, part: &PartKey) -> Vec<LifecycleCommand> {
        let Some(at) = self.parts.iter().position(|p| p.part == *part) else {
            return vec![];
        };
        let gone = self.parts.remove(at);
        match gone.phase {
            Phase::Walking | Phase::Cursor => vec![LifecycleCommand::Retire { part: gone.part }],
            Phase::Deciding | Phase::Unanswered => vec![],
        }
    }

    fn backoff(&self) -> Duration {
        let floor = self.config.min_backoff.min(self.config.max_backoff);
        let mut backoff = Duration::ZERO;
        for _ in 0..self.failures {
            backoff = if backoff.is_zero() {
                floor
            } else {
                (backoff * 2).min(self.config.max_backoff)
            };
        }
        backoff
    }

    fn fail_lane(&mut self) {
        self.failures += 1;
        self.gate = OGate::Unarmed;
    }

    pub(super) fn decided(
        &mut self,
        call: CallId,
        outcome: DecideOutcome,
    ) -> Vec<LifecycleCommand> {
        if self.open != Some(call) {
            return vec![];
        }
        self.open = None;
        let members = self.decidable(call);
        let mut cmds = vec![];
        match outcome {
            DecideOutcome::Answered(answers) => {
                let mut decided = 0;
                let mut refused = 0;
                for part in members {
                    match answers.get(&part) {
                        None | Some(PeerPartStrat::Unknown) => {
                            self.requeue_later(&part);
                            refused += 1;
                        }
                        Some(strat) => {
                            cmds.extend(self.start(&part, *strat));
                            decided += 1;
                        }
                    }
                }
                if decided > 0 {
                    self.failures = 0;
                    self.gate = OGate::Open;
                } else if refused > 0 {
                    self.fail_lane();
                }
            }
            DecideOutcome::TooManyParts { cap } if cap.max(1) < self.max_parts => {
                self.max_parts = cap.max(1);
                for part in members {
                    self.find_mut(&part).expect("member").call = None;
                }
            }
            DecideOutcome::TooManyParts { .. } | DecideOutcome::Failed => {
                for part in &members {
                    self.requeue_later(part);
                }
                if !members.is_empty() {
                    self.fail_lane();
                }
            }
        }
        cmds
    }

    fn requeue_later(&mut self, part: &PartKey) {
        self.clock += 1;
        let clock = self.clock;
        let p = self.find_mut(part).expect("member");
        p.phase = Phase::Unanswered;
        p.call = None;
        p.queued_at = clock;
    }

    fn start(&mut self, part: &PartKey, strat: PeerPartStrat) -> Vec<LifecycleCommand> {
        let p = self.find_mut(part).expect("member");
        p.call = None;
        let (peer_generation, latest) = match strat {
            PeerPartStrat::Cursor { generation, latest }
            | PeerPartStrat::BucketWalk {
                generation, latest, ..
            } => (generation, latest),
            PeerPartStrat::Unknown => unreachable!("not an answer"),
        };
        let mut cmds = vec![];
        // The held cursor is a position in the generation it was stored under, and in a log
        // that reaches at least that far. A part never seen holds neither.
        let restart = match p.generation {
            Some(held) => held != peer_generation || latest < p.cursor,
            None => false,
        };
        if restart {
            cmds.push(LifecycleCommand::Restart {
                part: part.clone(),
                held: PeerCursor {
                    generation: p.generation.expect("restart has a generation"),
                    rev: p.cursor,
                },
                peer_generation,
                peer_latest: latest,
            });
            p.cursor = 0;
            let zero = PeerCursor {
                generation: peer_generation,
                rev: 0,
            };
            cmds.push(LifecycleCommand::PersistCursor {
                part: part.clone(),
                cursor: zero,
            });
        }
        p.generation = Some(peer_generation);
        match strat {
            PeerPartStrat::Cursor { .. } => {
                p.phase = Phase::Cursor;
                cmds.push(LifecycleCommand::StartCursor {
                    part: part.clone(),
                    from: p.cursor,
                });
            }
            PeerPartStrat::BucketWalk { .. } => {
                p.phase = Phase::Walking;
                let since = p.cursor;
                p.cursor = p.cursor.max(latest);
                cmds.push(LifecycleCommand::StartWalk {
                    part: part.clone(),
                    strat,
                    since,
                });
            }
            PeerPartStrat::Unknown => unreachable!("not an answer"),
        }
        for cmd in &cmds {
            if let LifecycleCommand::PersistCursor { part, cursor } = cmd {
                self.persisted.insert(part.clone(), *cursor);
            }
        }
        cmds
    }

    pub(super) fn applied(&mut self, part: &PartKey, cursor: Rev) -> Vec<LifecycleCommand> {
        let Some(p) = self.find_mut(part) else {
            return vec![];
        };
        match p.phase {
            Phase::Walking => {
                p.cursor = p.cursor.max(cursor);
                vec![]
            }
            Phase::Cursor if cursor > p.cursor => {
                p.cursor = cursor;
                let held = PeerCursor {
                    generation: p.generation.expect("a decided part has a generation"),
                    rev: cursor,
                };
                self.persisted.insert(part.clone(), held);
                vec![LifecycleCommand::PersistCursor {
                    part: part.clone(),
                    cursor: held,
                }]
            }
            _ => vec![],
        }
    }

    pub(super) fn upgrade(&mut self, part: &PartKey) -> Vec<LifecycleCommand> {
        let p = self.find_mut(part).expect("upgrade of a wanted part");
        assert_eq!(p.phase, Phase::Walking);
        p.phase = Phase::Cursor;
        let held = PeerCursor {
            generation: p.generation.expect("a decided part has a generation"),
            rev: p.cursor,
        };
        self.persisted.insert(part.clone(), held);
        vec![LifecycleCommand::PersistCursor {
            part: part.clone(),
            cursor: held,
        }]
    }

    pub(super) fn flush(&mut self, now: Instant) -> Vec<LifecycleCommand> {
        self.last_flush = Some(now);
        if self.gate == OGate::Unarmed {
            self.gate = OGate::Until(now + self.backoff());
        }
        if self.open.is_some() {
            return vec![];
        }
        let gate_open = match self.gate {
            OGate::Open => true,
            OGate::Unarmed => false,
            OGate::Until(at) => at <= now,
        };
        let mut due = self.sorted(|p| match p.phase {
            Phase::Deciding => p.call.is_none(),
            Phase::Unanswered => gate_open,
            Phase::Walking | Phase::Cursor => false,
        });
        due.truncate(self.max_parts);
        if due.is_empty() {
            return vec![];
        }
        self.issued += 1;
        let call = self.issued;
        self.open = Some(call);
        let mut parts = Map::new();
        for part in due {
            let p = self.find_mut(&part).expect("due");
            p.phase = Phase::Deciding;
            p.call = Some(call);
            parts.insert(part, p.cursor);
        }
        vec![LifecycleCommand::Decide { call, parts }]
    }

    pub(super) fn next_due(&self) -> Option<Instant> {
        let waiting = self.parts.iter().any(|p| p.phase == Phase::Unanswered);
        match (self.gate, self.last_flush) {
            (OGate::Until(at), Some(last)) if waiting && self.open.is_none() && at > last => {
                Some(at)
            }
            _ => None,
        }
    }
}
