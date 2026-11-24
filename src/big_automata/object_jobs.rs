//! ObjectJobs: one peer's backend work per object. DRAFT LAWS for operator review; no code yet,
//! and the module is not wired into `lib.rs`.
//!
//! The cursor and bucket machines decide *what* an object's local membership should be on each
//! part. This machine decides *when* the backend is asked to act on it, merges what arrives
//! while the backend is busy, and reports every cursor back once the work covering it has
//! settled. Its state lives only as long as the connection: the one thing that outlives a
//! session is the applied part cursor, which the cursor machine's watermark computes from the
//! settlements reported here.
//!
//! # The backend contract
//!
//! ```text
//! record(job) -> Settle   short and cancel-safe, never waits on bytes; default: Fetch
//! fetch(job)  -> ()       may be long (a terabyte blob); cancel-safe and idempotent
//! Settle = Done | Fetch
//! ```
//!
//! - A **job** is combined: one object, the parts to add it to, the parts to remove it from, and
//!   the payload hint. Both calls succeed or fail as a whole; there are no partial outcomes.
//! - `Done`: the cursor may pass the job now. Either `record` did the work, or the backend took
//!   it on durably (for example through big_automata's durable outbox, as big_blobs' blob
//!   observations do).
//! - `Fetch`: this machine runs `fetch` for the job, and the cursor passes it when `fetch`
//!   succeeds.
//! - A simple backend implements only `fetch`. A backend that learns an object's parts only from
//!   its bytes (big_repo) does the same: `record` never depends on bytes, by definition.
//! - Replays repeat after a reconnect, so both calls must accept a job they have seen before.
//!
//! # Nouns
//!
//! - **Want**: per (object, part), the membership the peer's replay or bucket walk says the
//!   object should have: `Present` with a payload hint, or `Absent`, and the cursor that said so
//!   (`None` for a bucket walk). An object route adds a content want with no part.
//! - **Snapshot**: the wants a call was issued for, with their cursors.
//! - **In flight**: a call from issue until its terminal event (succeeded, failed, stale or
//!   cancelled). Cancellation drops the call's future, so the cancelled event follows promptly;
//!   a backend write inside a transaction either committed or rolled back.
//!
//! # Laws
//!
//! 1. **One record per object, one fetch per object.** An object has at most one `record` and
//!    at most one `fetch` in flight. Wants arriving meanwhile merge, and go out with the next
//!    call after the terminal event.
//! 2. **Terminal events match by call id.** An event for a call that is not the object's call in
//!    flight is stale and changes nothing.
//! 3. **Wants are per part, newest cursor wins.** A want for (object, part) replaces an older
//!    one; a want at a cursor at or below the current one is dropped. In a bucket-walk part the
//!    newest want wins by arrival.
//! 4. **Additions run beside a fetch; contradictions cancel it.** A `record` for new wants may
//!    run while the object's `fetch` is in flight, so a follow-up add is immediate; if it answers
//!    `Fetch`, its wants join the next fetch. A want that makes the fetch's snapshot wrong (a
//!    part it adds to is now `Absent` or retired) cancels the fetch, and the follow-up `record`
//!    runs after the cancelled event.
//! 5. **The next call is the difference.** After a terminal event the machine issues one call
//!    for every want not yet settled, or none. A failed or stale call's snapshot stays in it.
//! 6. **Each cursor settles once, by the work that covers it.** A cursor `c` on (object, part)
//!    settles when a job leaving that part at a cursor `>= c` settles: `record` answered `Done`,
//!    or the `fetch` after a `Fetch` succeeded. A removal at 1 superseded by an add at 2 settles
//!    both when the add settles. Settlements go to the cursor machine as `(JobKey, cursor)` for
//!    each cursor it still owes, and bucket-walk upserts to the bucket machine as
//!    `on_obj_sync_completed`. Retiring a part drops its unsettled cursors; the cursor machine's
//!    `remove_part` releases them on its side.
//! 7. **Bounded work.** Unsettled jobs count against a receive window `W` that gates the next
//!    replay round, so a part with a million objects is recorded at most `W` ahead of what has
//!    settled. At most `C` fetches run at once. A long fetch holds one slot of each; the rest
//!    keep flowing. Fetches start in cursor order, which is what moves the watermark.
//! 8. **Retry backs off per call.** Failed and stale calls are reissued with a backoff that
//!    starts at 2s, doubles and is capped (a minute by default). Merging new wants keeps the
//!    backoff; only a success resets it.
//! 9. **Object routes keep content alive.** While the object is routed directly, a content want
//!    with no part stays, so removing the object's last part does not cancel its fetch.
//! 10. **The connection's end ends everything.** On disconnect or peer removal every call in
//!     flight is cancelled, nothing is reported, and nothing is kept. The next session replays
//!     from the applied cursor.
//!
//! # Costs a backend chooses between
//!
//! A `Fetch` holds the watermark until it lands: a terabyte download at cursor 5 lets later jobs
//! record and fetch, but after a reconnect up to `W` of them replay again. A backend whose fetches
//! are long and must survive a reconnect answers `Done` and keeps its own durable outbox, with
//! its own priorities.
//!
//! # Old bugs these laws exclude
//!
//! From the old `big_sync_core/lib.rs`:
//! - A zombie removal was detected as "no remove worker" rather than by task id
//!   (`lib.rs:3391`), so a superseded task's completion could release deferred work early
//!   (law 2).
//! - New work during a deferral spawned immediately and raced the zombie, and the resume then
//!   overwrote the live worker with a plain `insert`, leaking its task (`lib.rs:2969`, `2993`;
//!   law 1).
//! - `cancel_obj_removal_hint` removed the worker on the first matching hint, so every other
//!   re-added part stayed in the re-removal and could be evicted after its re-sync
//!   (`lib.rs:2853`; law 3).
//! - Part retirement restarted sync workers but never remove workers (`peer_routes.rs:72`), and
//!   a removal left a sync running with a snapshot naming the removed part (law 4).
//! - Every merge cancelled and respawned the task, resetting its backoff, so an object changing
//!   faster than one fetch could restart forever (laws 4, 8).
//! - Backends decided for themselves whether to download inline (`daybook_core/blobs/sync.rs`)
//!   or record and defer (`big_blobs/sync.rs`), with nothing in the contract saying which; the
//!   inline ones blocked removals for the length of the download (the contract above).
