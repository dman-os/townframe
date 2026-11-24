//! Replay lanes: which acknowledged routes go in which round, and how long a round may wait.
//!
//! Latency and throughput pull against each other on one session. A part catching up on a
//! large backlog wants big pages back to back; a caught-up route wants its next change the
//! moment it is written. One round over both makes the caught-up route wait behind the
//! backlog's pages, and a held round (the server waits for rows) makes the backlog wait for
//! nothing. So rounds run in two lanes on the same session, concurrently: the protocol keeps
//! no cursor on the server, so two rounds over disjoint targets never interfere (replay laws
//! 6-9).
//!
//! Sans-IO. The parent tells it which routes are acknowledged and what each answer said; it
//! answers which routes the next round of each lane names and with which hold.
//!
//! # Laws
//!
//! 1. **Every acknowledged route is in exactly one lane.** An object route is always `Live`. A
//!    part route enters `Bulk`, moves to `Live` when an answer says it is drained, and back
//!    to `Bulk` when an answer advances it without draining it (it fell behind again).
//! 2. **A round names only its lane's routes**, and every one of them that the parent has a
//!    cursor for. A route never appears in both lanes' rounds in flight at once.
//! 3. **At most one round per lane in flight.** A lane's next round goes out only after its
//!    previous one is answered, failed or lost.
//! 4. **Holds.** A `Live` round may wait up to `Config::hold` for rows; a `Bulk` round never
//!    waits (`hold_ms = 0`).
//! 5. **Moving lanes takes effect at the next round.** A route that changes lane while a
//!    round naming it is in flight stays in that round; its answer still applies (the parent
//!    matches answers to the cursor the round was asked from, replay law 11).
//! 6. **Backpressure.** No round goes out while the parent's receive window is zero; a round's
//!    row limit is at most the window. The `Live` lane is served first when both are due.
//! 7. **A failed or lost round backs its lane off** from `min_backoff`, doubling, capped at
//!    `max_backoff`, reset by an answered round. The other lane is not delayed.
//! 8. **A reset session clears both lanes.** Routes re-enter by law 1 once acknowledged
//!    again.
//!
//! # What the simulation checks on top
//!
//! - Latency: while a part drains a backlog of any size, a change to a `Live` route is
//!   applied within a bounded number of `Live` rounds. Merging the lanes must fail this.
//! - Throughput: a backlog of `n` rows drains in about `n / limit` `Bulk` rounds.
