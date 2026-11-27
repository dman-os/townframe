//! ObjectJobs: one peer's backend work per object. This module implements the laws below.
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
//!   (`None` for a bucket walk). An object route's event adds a **content want**: the object's
//!   content, with the event's payload hint and no part ([`ObjectJobs::want_content`]).
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
//!    each cursor it still owes, and bucket-origin wants (upserts and removals alike) as
//!    `Settlement::Bucket`, which only tells the parent that one more bucket-origin want of the
//!    part is met (the parent upgrades a walk through [`ObjectJobs::part_outstanding`], not
//!    through these). A content want owes no cursor and reports nothing: it is met when its
//!    call settles. Retiring a part drops its unsettled cursors; the cursor machine's
//!    `remove_part` releases them on its side.
//! 7. **Bounded work.** Unsettled objects count against a receive window `W` that gates the
//!    next replay round, so a part with a million objects is recorded at most `W` ahead of what
//!    has settled, with one exception: an object whose only outstanding work is waiting out a
//!    failure backoff (every lane that has work for it has no call in flight and a deadline not
//!    yet reached) does not count. It keeps retrying on its ladder (law 8) and counts again from
//!    the moment its next call is issued, so `W` persistently failing objects, for example ones
//!    a local policy rejects while membership has not arrived, do not stop the peer's replay
//!    rounds. It still owes its cursors, so its part's cursor does not advance past it: cursor
//!    liveness is then the backend's responsibility. A backend that cannot apply something yet
//!    should accept and park it, answering `Done`; one that fails forever holds that part's
//!    cursor forever. The window bounds work in flight; a separate, much larger cap `T`
//!    (`max_tracked`, at least `W`) bounds memory: it counts every object this machine holds
//!    state for, backing off or not. Once `T` objects are tracked `room` is 0, so no replay
//!    round or walk pull admits another object until tracked objects settle or their part is
//!    retired. Reaching `T` is a stall by design, at a size far above the window: a backend
//!    that fails everything stops a part with a million objects at `T`, not at a million.
//!    Both bounds can be overshot by the rows of one revision: a page never splits a
//!    revision, and one transaction may touch many objects. Object routes and their content
//!    wants are not admitted through `room`, so they can exceed `T` by the routed objects. At
//!    most `C` fetches run at once. A long fetch holds one slot of each; the rest keep flowing. Fetches start in
//!    cursor order, which is what moves the watermark.
//! 8. **Retry backs off per call.** Failed and stale calls are reissued with a backoff that
//!    starts at 2s, doubles and is capped (a minute by default). Merging new wants keeps the
//!    backoff; only a success resets it.
//! 9. **Object routes keep content alive.** While the object is routed directly, a content want
//!    with no part stays, so removing the object's last part does not cancel its fetch. An
//!    object route's event is [`ObjectJobs::want_content`]: it replaces the route's content want
//!    with a fresh one carrying the event's payload (newest arrival wins), so a change at the
//!    peer is fetched again even after the previous content want settled, and creates the
//!    route if the object has none. A content want merges like any other: while its object's
//!    fetch runs it waits for the terminal event and does not cancel the fetch (it contradicts
//!    nothing). The job it makes has no parts; its payload is the newest part want's hint, or
//!    the route's when no part want is in the job.
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

// # Implementation notes
//
// Each object owns its (part -> current want) pairs, an optional route, the cursors it still
// owes, and two call lanes (record, fetch). A want carries a `version` from one machine-wide
// counter. A call's snapshot names the version it covered per part, so "the work that covers a
// cursor" is the comparison `owed version <= snapshot version`, and a want that arrived after
// the call was issued is never settled by it.
//
// Inputs only mark state. Settlements decided by a terminal event wait in `pending` and leave
// with the next `flush`, which also arms backoff deadlines (the terminal events carry no clock),
// cancels contradicted fetches and issues every call that is due.

use crate::interlude::*;

use std::collections::BTreeMap;

use crate::kit::Instant;
use crate::part_store::ObjPayload;
use crate::rev::Rev;
use crate::sync::cursor::JobKey;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub(crate) type CallId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Want {
    Present { payload: ObjPayload },
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    Cursor(Rev),
    Bucket,
}

/// One combined backend job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjJob {
    pub obj: ObjKey,
    pub add: Vec<PartKey>,
    pub remove: Vec<PartKey>,
    pub payload: Option<ObjPayload>,
}

/// What `record` answers (the backend contract above).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    Done,
    Fetch,
}

/// A call's terminal event. A call the backend found stale reports `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome<T> {
    Succeeded(T),
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobsCommand {
    Record {
        call: CallId,
        job: ObjJob,
    },
    Fetch {
        call: CallId,
        job: ObjJob,
    },
    /// The driver aborts the call. It still reports `Cancelled`, or its real outcome if it
    /// raced, and the call counts as in flight until then.
    Cancel {
        call: CallId,
    },
    Settled(Settlement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// `Sync(obj)` for a settled `Present` want's cursor, `RemoveFromPart(obj)` for an
    /// `Absent` want's. Sent once per cursor, when every part that owed it has settled.
    Cursor { job: JobKey, cursor: Rev },
    /// A settled bucket-origin want of either kind, `Present` or `Absent`. Nothing tracks these
    /// per walk any more: the parent upgrades a walk through `part_outstanding`, and only
    /// consumes this to know that the part's debt moved.
    Bucket { part: PartKey, obj: ObjKey },
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// The receive window: unsettled objects admitted before the next replay round waits.
    pub window: usize,
    /// The cap on tracked objects, backing off or not (law 7); at least `window`.
    pub max_tracked: usize,
    /// Fetches in flight at once, across objects.
    pub fetch_cap: usize,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

/// Which `JobKey` a cursor settles under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum JobLane {
    Sync,
    Remove,
}

/// What an unsettled want still needs from the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    /// Needs a `record`.
    Fresh,
    /// `record` answered `Fetch`; needs a `fetch`.
    NeedsFetch,
    /// Routes only: the content want is met until the route is set again.
    Settled,
}

#[derive(Debug)]
struct Pair {
    want: Want,
    /// The cursor of the current want, `None` for a bucket walk's.
    cursor: Option<Rev>,
    /// The newest cursor-origin cursor seen here; a cursor at or below it is dropped (law 3).
    high: Option<Rev>,
    version: u64,
    status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum OwedKey {
    /// Ordered by cursor, so settlements come out in watermark order.
    Cursor(Rev, JobLane),
    Bucket(PartKey),
}

/// A settlement the cursor or bucket machine waits on. A cursor an event gave to several
/// parts settles once, when the last of them has settled or been retired.
#[derive(Debug, Default)]
struct Owed {
    /// The parts still owing it, with the version of the want that created the debt.
    parts: BTreeMap<PartKey, u64>,
    /// Some part settled it, so it is reported once `parts` is empty.
    settled: bool,
}

#[derive(Debug)]
struct Route {
    version: u64,
    status: Status,
    /// The newest content want's payload hint; `None` for a route set with no event yet.
    payload: Option<ObjPayload>,
}

/// What a call was issued for: per part the want version it covered and whether it adds.
#[derive(Debug, Default)]
struct Snapshot {
    parts: BTreeMap<PartKey, (u64, bool)>,
    route: Option<u64>,
}

#[derive(Debug)]
struct Flight {
    snapshot: Snapshot,
    /// A `Cancel` was sent; the call stays in flight until its terminal event.
    cancel_sent: bool,
    /// A want arrived that makes the snapshot wrong. Records wait for the terminal event.
    contradicted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    Open,
    /// A failure waits for the next flush to read the clock and fix the deadline.
    Unarmed,
    Until(Instant),
}

#[derive(Debug)]
struct CallLane {
    flight: Option<(CallId, Flight)>,
    backoff: Duration,
    gate: Gate,
}

impl Default for CallLane {
    fn default() -> Self {
        Self {
            flight: None,
            backoff: Duration::ZERO,
            gate: Gate::Open,
        }
    }
}

impl CallLane {
    fn gate_open(&self, now: Instant) -> bool {
        match self.gate {
            Gate::Open => true,
            Gate::Unarmed => false,
            Gate::Until(at) => at <= now,
        }
    }

    /// Same ladder as `Scheduler::retry`: the first retry waits the floor, later ones double,
    /// the cap wins.
    fn fail(&mut self, config: &Config) {
        self.backoff = if self.backoff.is_zero() {
            config.min_backoff.min(config.max_backoff)
        } else {
            self.backoff
                .saturating_mul(2)
                .max(config.min_backoff)
                .min(config.max_backoff)
        };
        self.gate = Gate::Unarmed;
    }

    fn succeed(&mut self) {
        self.backoff = Duration::ZERO;
        self.gate = Gate::Open;
    }
}

#[derive(Debug, Default)]
struct ObjState {
    pairs: BTreeMap<PartKey, Pair>,
    route: Option<Route>,
    owed: BTreeMap<OwedKey, Owed>,
    record: CallLane,
    fetch: CallLane,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallKind {
    Record,
    Fetch,
}

impl ObjState {
    fn is_idle(&self) -> bool {
        self.pairs.is_empty()
            && self.route.is_none()
            && self.record.flight.is_none()
            && self.fetch.flight.is_none()
    }

    fn is_unsettled(&self) -> bool {
        !self.pairs.is_empty()
            || self
                .route
                .as_ref()
                .is_some_and(|route| route.status != Status::Settled)
    }

    /// Law 7: the object's only outstanding work waits out a failure backoff. A lane counts
    /// only if it has work. A call in flight, or an open gate (no failure since the lane's
    /// last issue or success), makes the object live; `Unarmed` and `Until` are the failure's
    /// own states, and stay until the next call is issued, even once the deadline has passed.
    /// Merging wants keeps the gate, so a merge does not make the object count again.
    fn backing_off(&self) -> bool {
        let lanes = [
            (&self.record, self.has(Status::Fresh)),
            (&self.fetch, self.has(Status::NeedsFetch)),
        ];
        let mut waiting = false;
        for (lane, has_work) in lanes {
            if !has_work {
                continue;
            }
            if lane.flight.is_some() || lane.gate == Gate::Open {
                return false;
            }
            waiting = true;
        }
        waiting
    }

    fn has(&self, status: Status) -> bool {
        self.pairs.values().any(|pair| pair.status == status)
            || self
                .route
                .as_ref()
                .is_some_and(|route| route.status == status)
    }

    /// The job for every want in `status`, and the snapshot it covers.
    fn build(&self, obj: &ObjKey, status: Status) -> Option<(ObjJob, Snapshot)> {
        let mut job = ObjJob {
            obj: obj.clone(),
            add: vec![],
            remove: vec![],
            payload: None,
        };
        let mut snapshot = Snapshot::default();
        // newest cursor wins, a bucket walk's by arrival
        let mut payload_rank = None;
        for (part, pair) in self.pairs.iter().filter(|(_, pair)| pair.status == status) {
            let add = matches!(pair.want, Want::Present { .. });
            snapshot.parts.insert(part.clone(), (pair.version, add));
            match &pair.want {
                Want::Present { payload } => {
                    job.add.push(part.clone());
                    let rank = (pair.cursor, pair.version);
                    if payload_rank.is_none_or(|best| rank > best) {
                        payload_rank = Some(rank);
                        job.payload = Some(payload.clone());
                    }
                }
                Want::Absent => job.remove.push(part.clone()),
            }
        }
        if let Some(route) = self.route.as_ref().filter(|route| route.status == status) {
            snapshot.route = Some(route.version);
            if job.payload.is_none() {
                job.payload = route.payload.clone();
            }
        }
        (!snapshot.parts.is_empty() || snapshot.route.is_some()).then_some((job, snapshot))
    }

    /// The lowest cursor the fetch would settle; bucket origin has none and goes last.
    fn lowest_owed_for_fetch(&self) -> Option<Rev> {
        self.owed.iter().find_map(|(key, owed)| match key {
            OwedKey::Cursor(cursor, _)
                if owed.parts.keys().any(|part| {
                    self.pairs
                        .get(part)
                        .is_some_and(|pair| pair.status == Status::NeedsFetch)
                }) =>
            {
                Some(*cursor)
            }
            _ => None,
        })
    }

    /// The work `snapshot` covered is done: settle every debt it covers and drop the wants it
    /// met unchanged.
    fn settle(&mut self, obj: &ObjKey, snapshot: &Snapshot, pending: &mut Vec<Settlement>) {
        self.owed.retain(|key, owed| {
            owed.parts.retain(|part, version| {
                let covered = snapshot
                    .parts
                    .get(part)
                    .is_some_and(|(covered, _)| *version <= *covered);
                owed.settled |= covered;
                !covered
            });
            if !owed.parts.is_empty() {
                return true;
            }
            if owed.settled {
                pending.push(match key {
                    OwedKey::Cursor(cursor, JobLane::Sync) => Settlement::Cursor {
                        job: JobKey::Sync(obj.clone()),
                        cursor: *cursor,
                    },
                    OwedKey::Cursor(cursor, JobLane::Remove) => Settlement::Cursor {
                        job: JobKey::RemoveFromPart(obj.clone()),
                        cursor: *cursor,
                    },
                    OwedKey::Bucket(part) => Settlement::Bucket {
                        part: part.clone(),
                        obj: obj.clone(),
                    },
                });
            }
            false
        });
        for (part, (version, _)) in &snapshot.parts {
            if self
                .pairs
                .get(part)
                .is_some_and(|pair| pair.version == *version)
            {
                self.pairs.remove(part);
            }
        }
        if let (Some(route), Some(version)) = (self.route.as_mut(), snapshot.route)
            && route.version == version
        {
            route.status = Status::Settled;
        }
    }

    /// A `record` answered `Fetch`: the wants it covered now wait for a fetch.
    fn promote(&mut self, snapshot: &Snapshot) {
        for (part, (version, _)) in &snapshot.parts {
            if let Some(pair) = self.pairs.get_mut(part)
                && pair.version == *version
            {
                pair.status = Status::NeedsFetch;
            }
        }
        if let (Some(route), Some(version)) = (self.route.as_mut(), snapshot.route)
            && route.version == version
        {
            route.status = Status::NeedsFetch;
        }
    }

    /// Whether the fetch in flight must be cancelled: a want contradicted its snapshot, and no
    /// live object route keeps its content wanted anyway (law 9).
    fn fetch_must_cancel(&self) -> bool {
        let Some((_, flight)) = &self.fetch.flight else {
            return false;
        };
        // A newer content want replaces the route's version but keeps its content wanted.
        let route_keeps = flight.snapshot.route.is_some() && self.route.is_some();
        flight.contradicted && !flight.cancel_sent && !route_keeps
    }
}

#[derive(Debug)]
pub(crate) struct ObjectJobs {
    config: Config,
    /// Objects with an unsettled want, a route or a call in flight.
    objs: BTreeMap<ObjKey, ObjState>,
    calls: Map<CallId, (ObjKey, CallKind)>,
    next_call: CallId,
    next_version: u64,
    /// Settlements decided by terminal events, sent by the next flush.
    pending: Vec<Settlement>,
    last_flush: Option<Instant>,
}

impl ObjectJobs {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            config,
            objs: default(),
            calls: default(),
            next_call: 0,
            next_version: 0,
            pending: default(),
            last_flush: None,
        }
    }

    fn next_version(&mut self) -> u64 {
        self.next_version += 1;
        self.next_version
    }

    pub(crate) fn want(&mut self, obj: ObjKey, part: PartKey, want: Want, origin: Origin) {
        // law 3: a cursor at or below the current one is dropped
        if let Origin::Cursor(cursor) = origin
            && self
                .objs
                .get(&obj)
                .and_then(|state| state.pairs.get(&part))
                .and_then(|pair| pair.high)
                .is_some_and(|high| cursor <= high)
        {
            return;
        }
        let version = self.next_version();
        let state = self.objs.entry(obj).or_default();
        if let Some((_, flight)) = &mut state.fetch.flight
            && let Some((_, adds)) = flight.snapshot.parts.get(&part)
        {
            flight.contradicted |= *adds == matches!(want, Want::Absent);
        }
        let (cursor, key) = match origin {
            Origin::Cursor(cursor) => {
                let lane = match want {
                    Want::Present { .. } => JobLane::Sync,
                    Want::Absent => JobLane::Remove,
                };
                (Some(cursor), OwedKey::Cursor(cursor, lane))
            }
            Origin::Bucket => (None, OwedKey::Bucket(part.clone())),
        };
        let high = state
            .pairs
            .get(&part)
            .and_then(|pair| pair.high)
            .max(cursor);
        state.pairs.insert(
            part.clone(),
            Pair {
                want,
                cursor,
                high,
                version,
                status: Status::Fresh,
            },
        );
        // a newer bucket want supersedes the older one: only the newest is reported
        state
            .owed
            .entry(key)
            .or_default()
            .parts
            .insert(part, version);
    }

    /// An object route's event: `obj`'s content changed at the peer, with `payload` as the hint.
    /// It replaces the route's content want (law 9), creating the route if there is none.
    pub(crate) fn want_content(&mut self, obj: ObjKey, payload: ObjPayload) {
        let version = self.next_version();
        self.objs.entry(obj).or_default().route = Some(Route {
            version,
            status: Status::Fresh,
            payload: Some(payload),
        });
    }

    pub(crate) fn set_object_route(&mut self, obj: ObjKey, routed: bool) {
        if routed {
            let version = self.next_version();
            let state = self.objs.entry(obj).or_default();
            state.route.get_or_insert(Route {
                version,
                status: Status::Fresh,
                payload: None,
            });
            return;
        }
        let Some(state) = self.objs.get_mut(&obj) else {
            return;
        };
        // The fetch no longer has a route behind it: a route set again later is a new want.
        if state.route.take().is_some()
            && let Some((_, flight)) = &mut state.fetch.flight
            && flight.snapshot.route.take().is_some()
        {
            flight.contradicted = true;
        }
        if state.is_idle() {
            self.objs.remove(&obj);
        }
    }

    pub(crate) fn retire_part(&mut self, part: &PartKey) {
        self.pending.retain(
            |settlement| !matches!(settlement, Settlement::Bucket { part: p, .. } if p == part),
        );
        let pending = &mut self.pending;
        self.objs.retain(|obj, state| {
            if let Some((_, flight)) = &mut state.fetch.flight
                && flight
                    .snapshot
                    .parts
                    .get(part)
                    .is_some_and(|(_, adds)| *adds)
            {
                flight.contradicted = true;
            }
            state.pairs.remove(part);
            state.owed.retain(|key, owed| {
                owed.parts.remove(part);
                if !owed.parts.is_empty() {
                    return true;
                }
                // Another part settled it before this one went away: the cursor machine
                // still waits to hear that.
                if let (true, OwedKey::Cursor(cursor, lane)) = (owed.settled, key) {
                    pending.push(Settlement::Cursor {
                        job: match lane {
                            JobLane::Sync => JobKey::Sync(obj.clone()),
                            JobLane::Remove => JobKey::RemoveFromPart(obj.clone()),
                        },
                        cursor: *cursor,
                    });
                }
                false
            });
            !state.is_idle()
        });
    }

    fn ended(&mut self, call: CallId, kind: CallKind) -> Option<(ObjKey, Flight)> {
        // law 2: not the object's call in flight, so stale
        let (obj, expected) = self.calls.remove(&call)?;
        assert_eq!(expected, kind, "call {call} ended as the wrong kind");
        let state = self.objs.get_mut(&obj).expect(ERROR_IMPOSSIBLE);
        let lane = match kind {
            CallKind::Record => &mut state.record,
            CallKind::Fetch => &mut state.fetch,
        };
        let (id, flight) = lane.flight.take().expect(ERROR_IMPOSSIBLE);
        assert_eq!(id, call);
        Some((obj, flight))
    }

    pub(crate) fn record_ended(&mut self, call: CallId, outcome: Outcome<Settle>) {
        let Some((obj, flight)) = self.ended(call, CallKind::Record) else {
            return;
        };
        let state = self.objs.get_mut(&obj).expect(ERROR_IMPOSSIBLE);
        match outcome {
            Outcome::Succeeded(Settle::Done) => {
                state.record.succeed();
                state.settle(&obj, &flight.snapshot, &mut self.pending);
            }
            Outcome::Succeeded(Settle::Fetch) => {
                state.record.succeed();
                state.promote(&flight.snapshot);
            }
            Outcome::Failed => state.record.fail(&self.config),
            Outcome::Cancelled => {}
        }
        if state.is_idle() {
            self.objs.remove(&obj);
        }
    }

    pub(crate) fn fetch_ended(&mut self, call: CallId, outcome: Outcome<()>) {
        let Some((obj, flight)) = self.ended(call, CallKind::Fetch) else {
            return;
        };
        let state = self.objs.get_mut(&obj).expect(ERROR_IMPOSSIBLE);
        match outcome {
            Outcome::Succeeded(()) => {
                state.fetch.succeed();
                state.settle(&obj, &flight.snapshot, &mut self.pending);
            }
            Outcome::Failed => state.fetch.fail(&self.config),
            Outcome::Cancelled => {}
        }
        if state.is_idle() {
            self.objs.remove(&obj);
        }
    }

    pub(crate) fn flush(&mut self, now: Instant, out: &mut Vec<JobsCommand>) {
        self.last_flush = Some(now);
        out.extend(self.pending.drain(..).map(JobsCommand::Settled));
        for state in self.objs.values_mut() {
            for lane in [&mut state.record, &mut state.fetch] {
                if lane.gate == Gate::Unarmed {
                    lane.gate = Gate::Until(now + lane.backoff);
                }
            }
            if state.fetch_must_cancel() {
                let (call, flight) = state.fetch.flight.as_mut().expect(ERROR_IMPOSSIBLE);
                flight.cancel_sent = true;
                out.push(JobsCommand::Cancel { call: *call });
            }
        }
        for (obj, state) in &mut self.objs {
            // A contradicted fetch may still be applying the part the record is about to
            // change, so the record waits for its terminal event (law 4).
            let blocked = state
                .fetch
                .flight
                .as_ref()
                .is_some_and(|(_, flight)| flight.contradicted);
            if state.record.flight.is_some() || blocked || !state.record.gate_open(now) {
                continue;
            }
            let Some((job, snapshot)) = state.build(obj, Status::Fresh) else {
                continue;
            };
            let call = self.next_call;
            self.next_call += 1;
            self.calls.insert(call, (obj.clone(), CallKind::Record));
            state.record.gate = Gate::Open;
            state.record.flight = Some((
                call,
                Flight {
                    snapshot,
                    cancel_sent: false,
                    contradicted: false,
                },
            ));
            out.push(JobsCommand::Record { call, job });
        }
        // law 7: fetches start in order of their lowest owed cursor, bucket origin last
        let running = self
            .objs
            .values()
            .filter(|state| state.fetch.flight.is_some())
            .count();
        if running >= self.config.fetch_cap {
            return;
        }
        let mut ready: Vec<_> = self
            .objs
            .iter()
            .filter(|(_, state)| {
                state.fetch.flight.is_none()
                    && state.fetch.gate_open(now)
                    && state.has(Status::NeedsFetch)
            })
            .map(|(obj, state)| {
                let lowest = state.lowest_owed_for_fetch();
                (lowest.is_none(), lowest, obj.clone())
            })
            .collect();
        ready.sort();
        let slots = self.config.fetch_cap - running;
        for (_, _, obj) in ready.into_iter().take(slots) {
            let state = self.objs.get_mut(&obj).expect(ERROR_IMPOSSIBLE);
            let (job, snapshot) = state
                .build(&obj, Status::NeedsFetch)
                .expect(ERROR_IMPOSSIBLE);
            let call = self.next_call;
            self.next_call += 1;
            self.calls.insert(call, (obj, CallKind::Fetch));
            state.fetch.gate = Gate::Open;
            state.fetch.flight = Some((
                call,
                Flight {
                    snapshot,
                    cancel_sent: false,
                    contradicted: false,
                },
            ));
            out.push(JobsCommand::Fetch { call, job });
        }
    }

    /// The earliest backoff deadline after the last flush of a lane that has work waiting.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        let last = self.last_flush?;
        self.objs
            .values()
            .flat_map(|state| {
                [
                    (&state.record, state.has(Status::Fresh)),
                    (&state.fetch, state.has(Status::NeedsFetch)),
                ]
            })
            .filter(|(lane, has_work)| *has_work && lane.flight.is_none())
            .filter_map(|(lane, _)| match lane.gate {
                Gate::Until(at) if at > last => Some(at),
                _ => None,
            })
            .min()
    }

    /// Objects still admissible: the receive window less the unsettled objects (law 7: those
    /// waiting out a failure backoff do not count), and never beyond the cap on tracked objects.
    pub(crate) fn room(&self) -> usize {
        let counted = self
            .objs
            .values()
            .filter(|state| state.is_unsettled() && !state.backing_off())
            .count();
        self.config
            .window
            .saturating_sub(counted)
            .min(self.config.max_tracked.saturating_sub(self.objs.len()))
    }

    /// Objects this machine holds state for: unsettled, routed or with a call in flight.
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.objs.len()
    }

    /// Objects with unsettled wants, backing off or not: what a quiet drain must see reach 0
    /// ([`room`](Self::room) hides the ones waiting out a backoff).
    #[cfg(test)]
    pub(crate) fn unsettled(&self) -> usize {
        self.objs
            .values()
            .filter(|state| state.is_unsettled())
            .count()
    }

    pub(crate) fn part_outstanding(&self, part: &PartKey) -> bool {
        self.objs
            .values()
            .any(|state| state.pairs.contains_key(part))
    }

    /// Law 10: cancel every call in flight, report nothing, forget everything. Late terminal
    /// events of the cancelled calls are stale: their ids are forgotten and never reused.
    pub(crate) fn clear(&mut self, out: &mut Vec<JobsCommand>) {
        for state in self.objs.values() {
            for lane in [&state.record, &state.fetch] {
                if let Some((call, flight)) = &lane.flight
                    && !flight.cancel_sent
                {
                    out.push(JobsCommand::Cancel { call: *call });
                }
            }
        }
        self.objs.clear();
        self.calls.clear();
        self.pending.clear();
        self.last_flush = None;
    }
}
