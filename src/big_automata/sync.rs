//! The per-peer sync machine: everything we pull from one connected peer, as one sans-IO
//! machine that owns the sub-machines and routes between them.
//!
//! One instance per peer connection. It replaces the old multi-peer `BigSyncMachine`
//! (`big_sync_core/lib.rs`): the only cross-peer state there was stats nobody consumed, and
//! waiting for full sync across peers is a layer above, a conjunction of this machine's
//! per-part facts.
//!
//! # Sub-machines it owns
//!
//! | Sub-machine | Owns |
//! | --- | --- |
//! | [`PartLifecycle`](crate::sync::lifecycle) | each part's phase and the summary calls; the persisted cursor |
//! | [`ReplayClient`](crate::replay::client) | the replay session, route ids, update requests |
//! | [`Lanes`](crate::replay::lanes) | which routes each round names, and holds |
//! | [`CursorSyncMachine`](crate::sync::cursor) | per-part applied cursors over replay events |
//! | [`BucketMachine`](crate::sync::bucket), one per walking part | the walk: list, leaf, deltas |
//! | [`ObjectJobs`](crate::sync::object_jobs) | every per-object backend call, its retries and settlements |
//!
//! No sub-machine calls another or holds a copy of another's state. This machine's own state
//! is only the routing glue: the call table, the route sets, object routes' session cursors
//! and the last emitted sync fact per part.
//!
//! # Laws
//!
//! **Inputs and calls**
//!
//! 1. **Incremental routes.** The embedder changes routes only by `update(added_parts,
//!    removed_parts, added_objects, removed_objects)`; nothing diffs sets. Adding a wanted
//!    route or removing an unwanted one is a no-op. A part is added with the cursor persisted
//!    for it and the peer generation that cursor is a position in (the driver reads both;
//!    `None` for a part never seen). A decision that finds another generation restarts the
//!    part from 0 (`PartLifecycle` law 10) and reports it as `PeerCommand::Restarted`.
//! 2. **One call-id space.** Every outward call (summary, session update, round, bucket list,
//!    bucket leaf, record, fetch) gets a fresh id from one counter, and its outcome comes back
//!    under that id. An outcome for an id not in flight (stale, duplicated, or from before a
//!    part was retired) changes nothing. Each outcome is handed to exactly the sub-machine
//!    that issued the call.
//! 3. **Peer input is checked at the edge; nothing the peer sends reaches an assert.** A page
//!    event that names no target, or a target the round did not ask for, makes the whole page
//!    a protocol violation: the round counts as failed (lanes law 7) and none of the page
//!    applies. So is an event outside its target's window: a `rev` at or below the cursor the
//!    round asked that target from or above the `resume` of its `Events` verdict in the same
//!    page (replay law 8), or an event for a target whose verdict is `Refused` or absent; a
//!    hostile peer could otherwise push the persisted cursor past rows it never delivered.
//!    So is a `Removed` event on an object target (replay law 12).
//!    All of this is judged against the round's request and page, not the current routes: an
//!    event for a target the round asked for whose route was dropped since is legitimate and
//!    is skipped for that route only. A summary entry for a part not asked is ignored (PartLifecycle law 3). A bucket or leaf page for a bucket
//!    not asked is ignored (bucket law 7). A leaf page whose `next_after` is not past the
//!    `after` it answers makes no progress and fails its call, so a peer cannot page one
//!    bucket for ever (the walk trusts `next_after` for where a bucket ends). Asserts inside sub-machines guard only the
//!    parent's own bugs.
//!
//! **Routing**
//!
//! 4. **A part's replay route is wanted exactly while the part is `Cursor` or `Walking`.**
//!    `StartCursor` and `StartWalk` want it; `Retire` unwants it. During a walk the cursor
//!    tail pages from the handover cursor, concurrently with the walk.
//! 5. **Part-route events go to the cursor machine; object-route events go to ObjectJobs.**
//!    An event naming both kinds is split. A part route's round cursor is
//!    `CursorSyncMachine::part_replay_cursor`; an object route's is its session cursor,
//!    advanced by each answered round's `resume` and never persisted.
//! 6. **Cursor-machine commands become object wants.** `SyncObj` wants the object present on
//!    each named part at its cursor; `RemoveObjFromParts` wants it absent. `SetPartCursor`
//!    goes to `PartLifecycle::applied`, which persists it or holds it during a walk.
//! 7. **Settlements flow back once.** ObjectJobs' `Settled(Cursor { job, cursor })` goes to
//!    the cursor machine; `Settled(Bucket)` is consumed by the upgrade check (law 9) and
//!    nothing else.
//! 8. **Retiring a part retires it everywhere in the same step**: lifecycle, cursor machine,
//!    ObjectJobs, its walk and its route. Removing an object route unwants it and drops its
//!    session cursor and its content route in ObjectJobs.
//!
//! **Walks**
//!
//! 9. **Upgrade only when the walk's work is settled.** A walking part is upgraded in the
//!    step in which its walk reports done **and** ObjectJobs has nothing outstanding for it,
//!    adds and removals alike. Its walk is dropped in the same step. Never earlier: a cursor
//!    persisted before a walk-found removal settled is lost for good after a crash.
//! 10. **Walk backpressure.** Each step pulls from walks only up to ObjectJobs' room, shared
//!     across walking parts in turn so one large walk cannot starve another.
//! 11. **Walk calls retry.** A failed list or leaf call is reissued unchanged after backoff;
//!     the walk is not told (it only sees answers).
//!
//! **Backpressure**
//!
//! 12. **Bounded work.** In-flight backend calls are bounded by ObjectJobs' window and fetch
//!     cap; rounds go out only while ObjectJobs has room, with row limits at most that room (at
//!     most half of it while any walk is pulling, so a backlog cannot hold the window against
//!     the walks; the walks take what the rounds in flight leave); walks hold at most their
//!     read-ahead. A round may carry the rows of one revision beyond its limit (a page never
//!     splits a revision), so the window bounds work up to one revision's rows. Nothing in
//!     this machine grows with the peer's
//!     data except per-route state.
//!
//! **Sync facts**
//!
//! 13. **`synced(part)` is derived, never told.** It is true exactly when the part is in
//!     `Cursor`, its route is acknowledged and its last answer was drained at the cursor the
//!     part is at, the cursor machine reports it settled, and ObjectJobs has nothing
//!     outstanding for it. The machine keeps only the last emitted value per part and emits
//!     an edge (`Synced` / `Unsynced`) when it changes. Test builds recompute every part after
//!     every step and assert the incremental facts agree.
//! 14. **Disconnect clears everything.** The machine is per connection: dropping it cancels
//!     every call in flight (ObjectJobs' `clear`), and nothing but persisted cursors survives.
//!
//! # Testing
//!
//! Law tests per law; a deterministic simulation with a peer whose part store is the ground
//! truth, a network that drops, duplicates, reorders and delays, hostile pages, a backend
//! whose fetches are slow and cancellable, and crashes that keep only persisted cursors.
//! Checked after every step: laws 2, 3, 9, 12, 13. Checked after a quiet drain: every wanted
//! part is `synced`, and the local store holds the peer's readable view of it (soundness of
//! `Synced`: whenever it is emitted, the local view matches the peer's view at that cursor).
//! Lane latency and throughput as in [`crate::replay::lanes`].

use crate::part_store::Generation;
use crate::rev::Rev;

/// How far a client has consumed a peer's part, and in which of the peer's [generations](Generation)
/// that position is. A rev means nothing under another generation, so the two are
/// persisted and compared together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCursor {
    pub generation: Generation,
    pub rev: Rev,
}

pub(crate) mod bucket;
pub(crate) mod cursor;
pub(crate) mod lifecycle;
pub(crate) mod object_jobs;
pub(crate) mod peer;
pub mod rpc;
pub(crate) mod strategy;
// FIXME(port): the old task bodies, disabled with `cfg(any())`; they become the driver's I/O.
#[cfg(any())]
mod tasks;
