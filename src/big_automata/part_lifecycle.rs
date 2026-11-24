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
//!    refusal lowers `max_parts` to `cap` for the session and re-queues the parts at once,
//!    without backoff: the refusal is about the request, not the parts.
//! 5. **Backoff is per peer decision lane**: from `min_backoff`, doubling, capped at
//!    `max_backoff`; reset by an answer that decides at least one part. One call carries
//!    every due `Unanswered` part, so a refused part never holds a decidable neighbour back
//!    beyond one call.
//! 6. **`Deciding → Cursor { from }`** emits `StartCursor { part, from }`, where `from` is
//!    the cursor we advertised in the call. **`Deciding → Walking`** emits
//!    `StartWalk { part, strat }` and sets the part's handover cursor to the summary's
//!    `latest`: the walk reads state no older than the summary, and the cursor tail runs
//!    concurrently from `latest`.
//! 7. **While `Walking`, the part's cursor is held.** The parent reports the tail's applied
//!    watermark with `applied(part, cursor)`; this machine raises the held handover cursor
//!    but emits no `PersistCursor` for a walking part. Persisting before the walk's work
//!    settled would let a crash skip what the walk found, for good: the next session resumes
//!    by cursor and never re-walks.
//! 8. **Upgrade happens once, and only when told.** `upgrade(part)` is valid only for a
//!    `Walking` part; the parent sends it when the walk reports done **and** no object work
//!    for the part is outstanding (adds and removals alike). It emits `PersistCursor` with the
//!    held handover cursor and moves the part to `Cursor`. In `Cursor`, every `applied`
//!    emits `PersistCursor` when it raises the cursor; the cursor never goes back.
//! 9. **Disconnect clears everything.** The machine is per connection; nothing but the
//!    persisted cursors survives it.
//!
//! # Costs
//!
//! `add`, `remove`, `applied` and `upgrade` are O(log parts). An answer is O(parts it names).
//! `flush` is O(parts it puts in the call).
//!
//! # Open for the operator
//!
//! - Law 4: is a failed call (transport error, not a refusal) also per-lane backoff, or
//!   should it fail the connection?
//! - Law 2: oldest-queued first is fair across retries; the alternative is priority by
//!   embedder hint, which no caller has today.

use crate::interlude::*;

use crate::rev::Rev;
use crate::strategy::PeerPartStrat;

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
    Decide { call: CallId, parts: Map<PartKey, Rev> },
    StartCursor { part: PartKey, from: Rev },
    StartWalk { part: PartKey, strat: PeerPartStrat },
    PersistCursor { part: PartKey, cursor: Rev },
    Retire { part: PartKey },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecideOutcome {
    /// Per-part decisions; a part missing from the map was not answered.
    Answered(Map<PartKey, PeerPartStrat>),
    TooManyParts { cap: usize },
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Deciding,
    Unanswered,
    Walking,
    Cursor,
}

// API sketch, implemented in the next commit:
//
// impl PartLifecycle {
//     fn new(config: Config) -> Self;
//     /// `cursor` is the persisted cursor we hold for the part (0 if none).
//     fn add(&mut self, part: PartKey, cursor: Rev);
//     fn remove(&mut self, part: &PartKey, out: &mut Vec<LifecycleCommand>);
//     fn decided(&mut self, call: CallId, outcome: DecideOutcome, out: &mut Vec<LifecycleCommand>);
//     fn applied(&mut self, part: &PartKey, cursor: Rev, out: &mut Vec<LifecycleCommand>);
//     fn upgrade(&mut self, part: &PartKey, out: &mut Vec<LifecycleCommand>);
//     fn flush(&mut self, now: Instant, out: &mut Vec<LifecycleCommand>);
//     fn next_due(&self) -> Option<Instant>;
//     fn phase(&self, part: &PartKey) -> Option<Phase>;
// }
