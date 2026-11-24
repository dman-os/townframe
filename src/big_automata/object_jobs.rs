//! ObjectJobs: one peer's backend work per object. DRAFT LAWS for operator review; no code yet,
//! and the module is not wired into `lib.rs`.
//!
//! The cursor and bucket machines decide *what* an object's local membership should be on each
//! part. This machine decides *when* the backend is asked to make it so, merges requests that
//! arrive while the backend is busy, and reports every cursor back once the work that covers it
//! has landed.
//!
//! # Nouns
//!
//! - **Want**: per (object, part), the membership the peer's replay or bucket walk says the
//!   object should have: `Present` (with the payload hint the event carried) or `Absent`, and
//!   the cursor that said so (`None` for a bucket walk, which has no cursor). An object route
//!   adds a part-less content want.
//! - **Applied**: per (object, part), what the backend last confirmed, and at which cursor.
//! - **Task**: one backend call that reconciles an object: upsert it to some parts (fetching or
//!   using the payload) and remove it from others. Its **snapshot** is the wants it was issued
//!   for, with their cursors.
//! - **In flight**: the object's task from issue until its terminal event (completed, failed
//!   or stale). A cancelled task stays in flight until its terminal event lands: cancellation is
//!   cooperative, so the backend may still apply it.
//!
//! # Laws
//!
//! 1. **One task per object.** An object has at most one task in flight, counting a cancelled
//!    one. Work for an object whose task is in flight merges into its wants and is issued only
//!    after that task's terminal event. (This replaces the old zombie-removal protocol: a
//!    cancelled removal is simply the in-flight task, and nothing races it.)
//! 2. **Terminal events match by task id.** An event for a task that is not the object's task
//!    in flight is stale and changes nothing.
//! 3. **Wants are per part, newest cursor wins.** A want for (object, part) replaces an older
//!    one; a want at a cursor at or below the current want's is dropped. Within a bucket-walk
//!    part, the newest want wins by arrival.
//! 4. **Additions don't cancel; contradictions do.** A new want that only adds work (another
//!    part to upsert, a newer payload) waits for the next task. A change that makes the
//!    snapshot wrong cancels the task in flight: it would upsert to a part now `Absent` or
//!    retired, or remove from a part now `Present`. Either way the replacement is issued after
//!    the terminal event (law 1).
//! 5. **The next task is the difference.** After a terminal event, the machine issues one task
//!    for every want that differs from applied, or none. On success, applied takes the
//!    snapshot. On failure or stale, applied is unknown for the snapshot's parts, so they stay
//!    in the next task.
//! 6. **Each cursor is reported once, by the work that covers it.** A cursor `c` on (object,
//!    part) settles when a task succeeds that left the part applied at a cursor `>= c` (a
//!    removal at 1 superseded by an add at 2 settles both when the add lands). Settlements go
//!    to the cursor machine as `(JobKey, cursor)` for each cursor it still owes, and bucket-walk
//!    upserts to the bucket machine as `on_obj_sync_completed`. Retiring a part drops its
//!    unreported cursors; the cursor machine's `remove_part` releases them on its side.
//! 7. **Retry backs off per object.** Failed and stale tasks are reissued with a backoff that
//!    starts at 2s, doubles and is capped (a minute by default). Merging new wants keeps the
//!    backoff; only a success resets it.
//! 8. **Object routes keep content alive.** While the object is routed directly, a content want
//!    with no parts stays, so removing the object's last part does not cancel its fetch.
//! 9. **Removing the peer ends everything.** Every task in flight is cancelled and nothing is
//!    reported.
//!
//! # Old bugs these laws exclude
//!
//! From the old `big_sync_core/lib.rs`:
//! - A zombie was detected as "no remove worker" rather than by task id (`lib.rs:3391`), so a
//!   superseded task's completion could release the deferred work early (law 2).
//! - New work during a deferral spawned immediately, racing the zombie, and the resume then
//!   overwrote the live worker with a plain `insert` and leaked its task (`lib.rs:2969`, `2993`;
//!   law 1).
//! - `cancel_obj_removal_hint` removed the worker on the first matching hint, so every other
//!   re-added part stayed in the re-removal and could be evicted after its re-sync
//!   (`lib.rs:2853`; law 3).
//! - Part retirement restarted sync workers but never remove workers (`peer_routes.rs:72`), and
//!   a removal left a sync running with a snapshot naming the removed part (law 4).
//! - Every merge cancelled and respawned the task, resetting its backoff, and an object
//!   changing faster than one fetch could restart forever (laws 4, 7).
