//! The per-peer lifecycle of each part we sync from that peer: deciding how to catch up,
//! then catching up by cursor or by a bucket walk that hands over to the cursor.
//!
//! One machine per peer connection, sans-IO. It owns the phase of every wanted part and the
//! decision calls (the peer summary request) that move parts out of `Deciding`. It does not
//! own the walk, the object jobs or the cursor watermark: the parent routes those and tells
//! this machine the two facts it needs (a part's walk is drained, a part's applied cursor).
//!
//! # Phases
//!
//! ```text
//!   add ──► Deciding ──answer: Cursor──────────────► Cursor
//!              │   ▲  ──answer: BucketWalk──► Walking ──upgrade──► Cursor
//!              │   │
//!              ▼   │ (backoff)
//!          Unanswered
//! ```
//!
//! # Laws
//!
//! 1. **Every wanted part has exactly one phase; an unwanted part has none.** `add` on a
//!    wanted part and `remove` on an unwanted one are no-ops. `remove` emits `Retire` exactly
//!    once for a part that had left `Deciding`, so the parent tears down its walk, cursor
//!    slots and object wants; a part removed while `Deciding` or `Unanswered` had none.
//! 2. **One decision call per peer in flight, chunked.** A call names at most
//!    `Config::max_parts` parts, the oldest-queued first. Parts added while a call is in
//!    flight wait for the next call; nothing restarts the call in flight. (The old machine
//!    restarted every pending decision on each route change, and latched
//!    `PeerPartSetTooLarge` instead of chunking.)
//! 3. **Answers are matched per part.** An answer for call `c` decides part `p` only if `p`
//!    is still `Deciding` under `c`. A part removed (or removed and re-added) since `c` was
//!    sent ignores its entry; the rest of the answer still applies. (The old machine dropped
//!    the whole answer when any one part was stale.)
//! 4. **A part the answer does not decide is retried.** A part missing from the answer or
//!    answered `Unknown` becomes `Unanswered` and re-enters the queue after backoff. A failed
//!    call returns all its still-`Deciding` parts to `Unanswered`. A `TooManyParts { cap }`
//!    refusal lowers `max_parts` to `cap` (at least 1) for the session and re-queues the
//!    parts at once, without backoff: the refusal is about the request, not the parts. A
//!    refusal that cannot lower the cap counts as a failed call, so a peer repeating it
//!    backs off instead of looping.
//! 5. **Backoff is per peer decision lane**: from `min_backoff`, doubling, capped at
//!    `max_backoff`; reset by an answer that decides at least one part. One call carries
//!    every due `Unanswered` part, so a refused part never holds a decidable neighbour back
//!    beyond one call. Backoff gates only `Unanswered` parts: a newly added part goes out in
//!    the next call regardless. An outcome that sends a part back to `Unanswered` climbs the
//!    ladder; the deadline is set at the next `flush`. A part re-queued from `Unanswered`
//!    queues by its new age; one re-queued by `TooManyParts` keeps its age.
//! 6. **`Deciding → Cursor { from }`** emits `StartCursor { part, from }`, where `from` is
//!    the cursor we hold (the one advertised in the call, unless law 10 restarted the part).
//!    **`Deciding → Walking`** emits `StartWalk { part, strat, since }`, where `since` is that
//!    same cursor, and sets the part's handover cursor to the larger of the summary's `latest`
//!    and the cursor we hold: the walk reads state no older than the summary, and the cursor
//!    tail runs concurrently from there.
//! 7. **While `Walking`, the part's cursor is held.** The parent reports the tail's applied
//!    watermark with `applied(part, cursor)`; this machine raises the held handover cursor
//!    but emits no `PersistCursor` for a walking part. Persisting before the walk's work
//!    settled would let a crash skip what the walk found, for good: the next session resumes
//!    by cursor and never re-walks.
//! 8. **Upgrade happens once, and only when told.** `upgrade(part)` is valid only for a
//!    `Walking` part (anything else is a caller bug and panics); the parent sends it when the walk reports done **and** no object work
//!    for the part is outstanding (adds and removals alike). It emits `PersistCursor` with the
//!    held handover cursor and moves the part to `Cursor`. In `Cursor`, every `applied`
//!    emits `PersistCursor` when it raises the cursor; the cursor never goes back.
//! 9. **Disconnect clears everything.** The machine is per connection; nothing but the
//!    persisted cursors survives it.
//! 10. **A cursor belongs to the generation it was stored under** (the peer's
//!     [`Generation`](crate::part_store::Generation)). A part is added with `Option<PeerCursor>`:
//!     `None` is a part never seen, which holds cursor 0 and no generation. The decision
//!     compares the summary's generation with the held one, and the summary's `latest` with
//!     the held cursor. The part **resumes** from the held cursor when the generations match
//!     and `latest` is not below it, or when none is held (the part adopts the summary's
//!     generation, nothing was consumed). Otherwise the peer lost or replaced its data (a
//!     recreated store, a backup restored without a mint) and a rev we hold means nothing on
//!     its new log, where rows between its head and our old cursor would be missed for good.
//!     The part **restarts**: the held cursor becomes 0 (so the cursor tail replays the whole
//!     log, or the walk lists everything and reconciles against the local part), the part
//!     takes the summary's generation, and a `Restart` command (carrying the old cursor with
//!     its generation, and the summary's generation and `latest`) comes before
//!     `StartCursor`/`StartWalk` for the embedder to log. Every `PersistCursor` carries the
//!     part's current generation, so the first persist after a restart writes the new one; a
//!     crash before it leaves the old generation stored and the next connection restarts
//!     again, which is idempotent.
//!     Checked once per decision, never mid-connection: a generation change while connected
//!     means the peer restarted, which loses the replay session (its next update or round
//!     fails) and forces a reconnect, whose parts all decide again from fresh summaries. So
//!     the decision's summary is the only place a changed generation can first show.
//!
//! # Costs
//!
//! `add`, `remove`, `applied` and `upgrade` are O(log parts). An answer is O(parts it names).
//! `flush` is O(parts it puts in the call).
//!
//! # Open for the operator
//!
//! - Law 4: a failed call (transport error, not a refusal) is per-lane backoff, as drafted;
//!   the alternative is to fail the connection.
//! - Law 2: oldest-queued first is fair across retries; the alternative is priority by
//!   embedder hint, which no caller has today.

// # Implementation notes
//
// A part is `Queued` (Deciding, waiting for a call), `InCall(c)` (Deciding under call `c`),
// `Unanswered`, `Walking` or `Cursor`; the first two are the public `Deciding`. Every queued or
// unanswered part carries a `seq` from one machine-wide counter: the order of those two sets is
// "oldest-queued first". A part keeps its `seq` while in a call and when a `TooManyParts`
// refusal re-queues it (the refusal is about the request, not the part); a part that returns to
// `Unanswered` takes a fresh one, so retries do not starve newer parts.
//
// The lane's backoff has no clock in `decided`, so a failure marks the gate `Unarmed` and the
// next `flush` fixes the deadline, as in `object_jobs`. Only `Unanswered` parts wait on the gate;
// a freshly added part is never delayed by it.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::kit::scheduler::Instant;
use crate::part_store::Generation;
use crate::rev::Rev;
use crate::sync::PeerCursor;
use crate::sync::strategy::PeerPartStrat;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub(crate) type CallId = u64;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub max_parts: usize,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LifecycleCommand {
    /// Ask the peer to summarize `parts`, each with the cursor we hold for it.
    Decide {
        call: CallId,
        parts: Map<PartKey, Rev>,
    },
    /// Law 10: the part's decision found the peer on another generation, or behind the cursor
    /// we hold, and the part starts over from cursor 0. `held` is what we had; the peer is now
    /// on `peer_generation` and its log ends at `peer_latest`.
    Restart {
        part: PartKey,
        held: PeerCursor,
        peer_generation: Generation,
        peer_latest: Rev,
    },
    StartCursor {
        part: PartKey,
        from: Rev,
    },
    /// Walk the part's buckets. `since` is the cursor the walk lists changes from.
    StartWalk {
        part: PartKey,
        strat: PeerPartStrat,
        since: Rev,
    },
    PersistCursor {
        part: PartKey,
        cursor: PeerCursor,
    },
    Retire {
        part: PartKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecideOutcome {
    /// Per-part decisions; a part missing from the map was not answered.
    Answered(Map<PartKey, PeerPartStrat>),
    TooManyParts {
        cap: usize,
    },
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Deciding,
    Unanswered,
    Walking,
    Cursor,
}

/// The single decision lane's backoff gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    Open,
    /// A failure waits for the next flush to read the clock and fix the deadline.
    Unarmed,
    Until(Instant),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Queued,
    InCall(CallId),
    Unanswered,
    Walking,
    Cursor,
}

#[derive(Debug)]
struct Part {
    state: State,
    /// The generation `cursor` was stored under; `None` for a part never seen, whose cursor is
    /// 0. Set from the summary when the part starts (law 10).
    generation: Option<Generation>,
    /// Queued and unanswered: the cursor we advertise. `Walking`: the held handover cursor.
    /// `Cursor`: the persisted cursor.
    cursor: Rev,
    /// Position in the queue order; meaningful in `Queued`, `InCall` and `Unanswered`.
    seq: u64,
}

#[derive(Debug)]
struct Flight {
    call: CallId,
    /// In call order. An entry whose part is gone, or is no longer `InCall(call)` (removed
    /// and re-added), is stale and ignored.
    parts: Vec<PartKey>,
}

#[derive(Debug)]
pub(crate) struct PartLifecycle {
    config: Config,
    /// `config.max_parts`, lowered by `TooManyParts` refusals.
    max_parts: usize,
    parts: BTreeMap<PartKey, Part>,
    /// `Queued` parts by `(seq, part)`.
    queued: BTreeSet<(u64, PartKey)>,
    /// `Unanswered` parts by `(seq, part)`.
    unanswered: BTreeSet<(u64, PartKey)>,
    seq: u64,
    next_call: CallId,
    flight: Option<Flight>,
    backoff: Duration,
    gate: Gate,
    last_flush: Option<Instant>,
}

impl Part {
    /// The held cursor with its generation: only for a part that has been decided.
    fn held(&self) -> PeerCursor {
        PeerCursor {
            generation: self.generation.expect("a decided part has a generation"),
            rev: self.cursor,
        }
    }
}

impl PartLifecycle {
    pub(crate) fn new(config: Config) -> Self {
        assert!(config.max_parts > 0, "a call must be able to name a part");
        Self {
            max_parts: config.max_parts,
            config,
            parts: default(),
            queued: default(),
            unanswered: default(),
            seq: 0,
            next_call: 1,
            flight: None,
            backoff: Duration::ZERO,
            gate: Gate::Open,
            last_flush: None,
        }
    }

    /// Want `part`. `cursor` is the persisted cursor we hold for it, `None` for a part never
    /// seen. A part that is already wanted is left alone.
    pub(crate) fn add(&mut self, part: PartKey, cursor: Option<PeerCursor>) {
        if self.parts.contains_key(&part) {
            return;
        }
        self.seq += 1;
        self.queued.insert((self.seq, part.clone()));
        self.parts.insert(
            part,
            Part {
                state: State::Queued,
                generation: cursor.map(|cursor| cursor.generation),
                cursor: cursor.map_or(0, |cursor| cursor.rev),
                seq: self.seq,
            },
        );
    }

    /// Stop wanting `part`. A call in flight keeps flying; its entry for the part is ignored.
    pub(crate) fn remove(&mut self, part: &PartKey, out: &mut Vec<LifecycleCommand>) {
        let Some(removed) = self.parts.remove(part) else {
            return;
        };
        match removed.state {
            State::Queued => {
                self.queued.remove(&(removed.seq, part.clone()));
            }
            State::Unanswered => {
                self.unanswered.remove(&(removed.seq, part.clone()));
            }
            State::InCall(_) => {}
            State::Walking | State::Cursor => {
                out.push(LifecycleCommand::Retire { part: part.clone() });
            }
        }
    }

    /// The terminal event of decision call `call`. An event for any other call changes nothing.
    pub(crate) fn decided(
        &mut self,
        call: CallId,
        outcome: DecideOutcome,
        out: &mut Vec<LifecycleCommand>,
    ) {
        let Some(flight) = self.flight.take_if(|flight| flight.call == call) else {
            return;
        };
        let live: Vec<PartKey> = flight
            .parts
            .into_iter()
            .filter(|part| {
                self.parts
                    .get(part)
                    .is_some_and(|p| p.state == State::InCall(call))
            })
            .collect();
        match outcome {
            DecideOutcome::Answered(mut answers) => {
                let mut decided = 0usize;
                let mut refused = 0usize;
                for part in live {
                    match answers.remove(&part) {
                        Some(PeerPartStrat::Unknown) | None => {
                            self.unanswer(part);
                            refused += 1;
                        }
                        Some(strat) => {
                            self.start(part, strat, out);
                            decided += 1;
                        }
                    }
                }
                if decided > 0 {
                    self.backoff = Duration::ZERO;
                    self.gate = Gate::Open;
                } else if refused > 0 {
                    self.bump_backoff();
                }
            }
            DecideOutcome::TooManyParts { cap } if cap.max(1) < self.max_parts => {
                self.max_parts = cap.max(1);
                for part in live {
                    let p = self.parts.get_mut(&part).expect("live part");
                    p.state = State::Queued;
                    self.queued.insert((p.seq, part));
                }
            }
            // A refusal that does not lower the cap would re-send the same call at once, for
            // ever; it is a failure of the call.
            DecideOutcome::TooManyParts { .. } | DecideOutcome::Failed => {
                let any = !live.is_empty();
                for part in live {
                    self.unanswer(part);
                }
                if any {
                    self.bump_backoff();
                }
            }
        }
    }

    /// The cursor tail of `part` has applied everything up to `cursor`.
    pub(crate) fn applied(&mut self, part: &PartKey, cursor: Rev, out: &mut Vec<LifecycleCommand>) {
        let Some(p) = self.parts.get_mut(part) else {
            return;
        };
        match p.state {
            // The walk may still be finding work the cursor tail has passed: hold, never persist.
            State::Walking => p.cursor = p.cursor.max(cursor),
            State::Cursor if cursor > p.cursor => {
                p.cursor = cursor;
                out.push(LifecycleCommand::PersistCursor {
                    part: part.clone(),
                    cursor: p.held(),
                });
            }
            // A report from a previous incarnation of the part, or one that raises nothing.
            _ => {}
        }
    }

    /// The walk of `part` is done and no object work for it is outstanding. The parent sends
    /// this once, for a `Walking` part; anything else is a parent bug.
    pub(crate) fn upgrade(&mut self, part: &PartKey, out: &mut Vec<LifecycleCommand>) {
        let p = self
            .parts
            .get_mut(part)
            .unwrap_or_else(|| panic!("upgrade of unwanted part {part:?}"));
        assert_eq!(
            p.state,
            State::Walking,
            "upgrade is valid only for a walking part ({part:?})"
        );
        p.state = State::Cursor;
        out.push(LifecycleCommand::PersistCursor {
            part: part.clone(),
            cursor: p.held(),
        });
    }

    /// Issue the decision call if none is in flight and some part is due.
    pub(crate) fn flush(&mut self, now: Instant, out: &mut Vec<LifecycleCommand>) {
        self.last_flush = Some(now);
        if self.gate == Gate::Unarmed {
            self.gate = Gate::Until(now + self.backoff);
        }
        if self.flight.is_some() {
            return;
        }
        let gate_open = match self.gate {
            Gate::Open => true,
            Gate::Unarmed => false,
            Gate::Until(at) => at <= now,
        };
        let mut fresh = self.queued.iter().peekable();
        let mut retry = self.unanswered.iter().take_while(|_| gate_open).peekable();
        let mut picked: Vec<(u64, PartKey)> = vec![];
        while picked.len() < self.max_parts {
            let next = match (fresh.peek(), retry.peek()) {
                (Some(a), Some(b)) if a.0 < b.0 => fresh.next(),
                (Some(_), None) => fresh.next(),
                (_, Some(_)) => retry.next(),
                (None, None) => break,
            };
            picked.push(next.expect("peeked").clone());
        }
        if picked.is_empty() {
            return;
        }
        let call = self.next_call;
        self.next_call += 1;
        let mut parts = Map::with_capacity(picked.len());
        let mut order = Vec::with_capacity(picked.len());
        for (seq, part) in picked {
            self.queued.remove(&(seq, part.clone()));
            self.unanswered.remove(&(seq, part.clone()));
            let p = self.parts.get_mut(&part).expect("queued part");
            p.state = State::InCall(call);
            parts.insert(part.clone(), p.cursor);
            order.push(part);
        }
        self.flight = Some(Flight { call, parts: order });
        out.push(LifecycleCommand::Decide { call, parts });
    }

    /// When a flush would next put a backed-off part in a call, if only the clock is missing.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        if self.flight.is_some() || self.unanswered.is_empty() {
            return None;
        }
        match (self.gate, self.last_flush) {
            (Gate::Until(at), Some(last)) if at > last => Some(at),
            _ => None,
        }
    }

    pub(crate) fn phase(&self, part: &PartKey) -> Option<Phase> {
        self.parts.get(part).map(|p| match p.state {
            State::Queued | State::InCall(_) => Phase::Deciding,
            State::Unanswered => Phase::Unanswered,
            State::Walking => Phase::Walking,
            State::Cursor => Phase::Cursor,
        })
    }

    /// The cursor held for `part`: advertised while deciding, the handover cursor while
    /// walking, the persisted one in `Cursor`.
    pub(crate) fn cursor(&self, part: &PartKey) -> Option<Rev> {
        self.parts.get(part).map(|p| p.cursor)
    }

    pub(crate) fn max_parts(&self) -> usize {
        self.max_parts
    }

    fn unanswer(&mut self, part: PartKey) {
        self.seq += 1;
        let p = self.parts.get_mut(&part).expect("live part");
        p.state = State::Unanswered;
        p.seq = self.seq;
        self.unanswered.insert((self.seq, part));
    }

    fn start(&mut self, part: PartKey, strat: PeerPartStrat, out: &mut Vec<LifecycleCommand>) {
        let p = self.parts.get_mut(&part).expect("live part");
        let (peer_generation, latest) = match strat {
            PeerPartStrat::Cursor { generation, latest }
            | PeerPartStrat::BucketWalk {
                generation, latest, ..
            } => (generation, latest),
            PeerPartStrat::Unknown => unreachable!("an unknown strat decides nothing"),
        };
        // Law 10. A part never seen holds cursor 0 and has nothing to lose.
        if let Some(generation) = p.generation
            && (generation != peer_generation || latest < p.cursor)
        {
            out.push(LifecycleCommand::Restart {
                part: part.clone(),
                held: p.held(),
                peer_generation,
                peer_latest: latest,
            });
            p.cursor = 0;
        }
        p.generation = Some(peer_generation);
        match strat {
            PeerPartStrat::Cursor { .. } => {
                p.state = State::Cursor;
                out.push(LifecycleCommand::StartCursor {
                    part,
                    from: p.cursor,
                });
            }
            PeerPartStrat::BucketWalk { .. } => {
                let since = p.cursor;
                // The tail replays from `latest`. The cursor we hold is at most `latest` here:
                // one beyond it restarted the part above (law 10).
                p.state = State::Walking;
                p.cursor = p.cursor.max(latest);
                out.push(LifecycleCommand::StartWalk { part, strat, since });
            }
            PeerPartStrat::Unknown => unreachable!("an unknown strat decides nothing"),
        }
    }

    /// Same ladder as `Scheduler::retry`: the first retry waits the floor, later ones double,
    /// the cap wins.
    fn bump_backoff(&mut self) {
        self.backoff = if self.backoff.is_zero() {
            self.config.min_backoff.min(self.config.max_backoff)
        } else {
            self.backoff
                .saturating_mul(2)
                .max(self.config.min_backoff)
                .min(self.config.max_backoff)
        };
        self.gate = Gate::Unarmed;
    }
}
