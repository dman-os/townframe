//! Watermark bookkeeping for event-fed stream machines.
//!
//! This is the generic core of [`crate::cursor::CursorSyncMachine`]: the
//! per-stream slot tracker (`mark_pending_cursor` /
//! `drain_ready_cursor_advances`), the per-job waiter aggregation
//! (`active_obj_jobs`) and the supersede path used by removal dispositions.
//!
//! The machine is a pure reducer component: it never pulls, fetches, spawns
//! or sleeps. A use site feeds it admissions/completions inside its own event
//! handler and turns the returned watermarks / freed cursors into whatever
//! commands its driver understands. Replay/fetching stays entirely outside.
//!
//! Semantics preserved from the existing code:
//! - **At-least-once admission guard** ([`WatermarkBook::begin`]): a cursor
//!   at or below the last emitted watermark, or already tracked as pending,
//!   is a duplicate and is ignored.
//! - **Explicit advancement**: a stream's watermark only moves to the
//!   contiguous prefix of finished slots ([`WatermarkBook::drain`]). Work is
//!   concurrent across jobs; the watermark is exactly "highest cursor whose
//!   entire prefix is terminal".
//! - **Job aggregation**: one job key can have several in-flight cursors and
//!   each cursor can wait on multiple lanes (e.g. membership + sync); a slot
//!   only finishes when every lane settled.

use std::collections::{BTreeMap, HashMap};

/// Per-stream slot bookkeeping: which cursors are pending work and where the
/// emitted watermark stands. Direct extraction of `CursorStreamState`.
#[derive(Debug, Default, Clone)]
pub struct WatermarkBook<Cursor> {
    last_emitted: Option<Cursor>,
    slots: BTreeMap<Cursor, Slot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    Pending,
    Ready,
}

impl<Cursor> WatermarkBook<Cursor>
where
    Cursor: Ord + Copy + Default,
{
    /// Admit a cursor for processing. Returns `false` (and tracks nothing)
    /// when the cursor is a replay of an already-emitted watermark or an
    /// already-pending duplicate — the at-least-once handoff guard from
    /// `CursorSyncMachine::mark_pending_cursor`.
    pub fn begin(&mut self, cursor: Cursor) -> bool {
        if cursor <= self.last_emitted.unwrap_or_default() {
            // Replay/live handoff is at-least-once. A replacement immutable
            // subscription can repeat an event whose cursor was already
            // durably advanced by the previous generation.
            return false;
        }
        if self.slots.contains_key(&cursor) {
            return false;
        }
        self.slots.insert(cursor, Slot::Pending);
        true
    }

    /// Mark an admitted cursor as finished. Invariant violation if the
    /// cursor was never admitted.
    pub fn finish(&mut self, cursor: Cursor) {
        let slot = self
            .slots
            .get_mut(&cursor)
            .expect("finished a cursor that was never admitted");
        *slot = Slot::Ready;
    }

    /// Mark an admitted cursor as finished without requiring prior admission
    /// (used by supersede paths that free cursors whose work was cancelled).
    pub fn force_finish(&mut self, cursor: Cursor) {
        if let Some(slot) = self.slots.get_mut(&cursor) {
            *slot = Slot::Ready;
        }
    }

    /// Advance to the contiguous prefix of ready slots and report the new
    /// watermark, if any. Covered slots are dropped. Mirrors
    /// `CursorSyncMachine::drain_ready_cursor_advances`.
    pub fn drain(&mut self) -> Option<Cursor> {
        let mut latest_ready = None;
        for (cursor, slot) in &self.slots {
            match slot {
                Slot::Ready => latest_ready = Some(*cursor),
                Slot::Pending => break,
            }
        }
        let watermark = latest_ready?;
        while self
            .slots
            .first_key_value()
            .is_some_and(|(slot_cursor, _)| *slot_cursor <= watermark)
        {
            self.slots.pop_first();
        }
        self.last_emitted = Some(watermark);
        Some(watermark)
    }

    /// The currently emitted watermark, if any.
    pub fn watermark(&self) -> Option<Cursor> {
        self.last_emitted
    }

    /// No un-advanced cursors remain for this stream.
    pub fn is_settled(&self) -> bool {
        self.slots.is_empty()
    }
}

/// A single in-flight cursor within a job: which streams reference it, which
/// lanes are still pending, and the caller's payload. Extraction of
/// `CursorWaiter`.
#[derive(Debug, Clone)]
pub struct Waiter<StreamId, Lane, Payload> {
    /// Streams whose watermark slots this cursor gates.
    pub streams: Vec<StreamId>,
    /// Lanes of work that must settle before this cursor finishes.
    pub lanes: Vec<Lane>,
    pub payload: Payload,
}

// Manual impl: a derived one would demand `Default` on `StreamId`/`Lane`
// even though only `Payload` ends up inside a fresh waiter (the Vec fields
// are always empty). Mirrors how `CursorWaiter::default()` only needs its
// parts/pending flags to be zero-valued.
impl<StreamId, Lane, Payload: Default> Default for Waiter<StreamId, Lane, Payload> {
    fn default() -> Self {
        Self {
            streams: Default::default(),
            lanes: Default::default(),
            payload: Default::default(),
        }
    }
}

/// Job aggregation across streams: many jobs in flight concurrently, each
/// possibly gating several cursors on several streams. Extraction of
/// `active_obj_jobs` / `ObjectJobState` (whose waiters are keyed by
/// `CursorIndex`). Generic over the cursor type so use sites with richer
/// cursors than `u64` can reuse it unchanged.
#[derive(Debug, Clone)]
pub struct JobBoard<JobKey, StreamId, Lane, Cursor, Payload> {
    jobs: BTreeMap<JobKey, BTreeMap<Cursor, Waiter<StreamId, Lane, Payload>>>,
}

// Manual impl: a derived one would demand `Default` on every type parameter
// even though an empty board holds nothing.
impl<JobKey, StreamId, Lane, Cursor, Payload> Default
    for JobBoard<JobKey, StreamId, Lane, Cursor, Payload>
{
    fn default() -> Self {
        Self {
            jobs: Default::default(),
        }
    }
}

impl<JobKey, StreamId, Lane, Cursor, Payload> JobBoard<JobKey, StreamId, Lane, Cursor, Payload>
where
    JobKey: Ord + Copy,
    Lane: PartialEq + Copy,
    StreamId: PartialEq + Copy,
    Cursor: Ord + Copy,
    Payload: Default,
{
    /// Track in-flight work for `(job, cursor)`, adding `streams` and
    /// `lanes` to any existing waiter. Mirrors the waiter creation in
    /// `on_subscription_evt`.
    pub fn track(
        &mut self,
        job: JobKey,
        cursor: Cursor,
        streams: impl IntoIterator<Item = StreamId>,
        lanes: impl IntoIterator<Item = Lane>,
        payload: Payload,
    ) {
        let waiter = self.jobs.entry(job).or_default().entry(cursor).or_default();
        waiter.streams.extend(streams);
        // Lanes are set-semantics like the original's boolean flags
        // (`pending_sync`/`pending_membership`): re-tracking the same job
        // cursor from another stream must not duplicate them.
        for lane in lanes {
            if !waiter.lanes.contains(&lane) {
                waiter.lanes.push(lane);
            }
        }
        waiter.payload = payload;
    }

    /// Settle one lane of `(job, cursor)` (mirrors `on_obj_sync_job_evt`).
    /// Returns the streams referenced by the waiter when it fully settled —
    /// those streams may now be able to advance. Returns an empty vec while
    /// other lanes remain pending.
    ///
    /// Panics when settling a lane that was never pending: same invariant as
    /// the original's "cursor membership completion without pending
    /// membership".
    pub fn settle(&mut self, job: JobKey, cursor: Cursor, lane: Lane) -> Vec<StreamId> {
        let Some(job_entry) = self.jobs.get_mut(&job) else {
            return Vec::new();
        };
        let Some(waiter) = job_entry.get_mut(&cursor) else {
            return Vec::new();
        };
        let idx = waiter
            .lanes
            .iter()
            .position(|candidate| *candidate == lane)
            .unwrap_or_else(|| panic!("lane completion without a pending lane"));
        waiter.lanes.swap_remove(idx);
        if !waiter.lanes.is_empty() {
            return Vec::new();
        }
        let waiter = job_entry.remove(&cursor).expect("just checked");
        if job_entry.is_empty() {
            self.jobs.remove(&job);
        }
        waiter.streams
    }

    /// Supersede in-flight work for `job` on `stream` below `bound`: for
    /// every waiter with `cursor < bound` referencing `stream`, drop the
    /// lanes for which `keep` returns `false`. Waiters left with neither
    /// lanes nor other streams are removed and their cursors reported as
    /// freed (the caller marks them finished on `stream` immediately).
    /// Mirrors `supersede_obj_part`, with the original's "pending membership
    /// must still finish before its cursor advances" special case expressed
    /// as a lane predicate.
    pub fn supersede(
        &mut self,
        job: JobKey,
        stream: StreamId,
        bound: Cursor,
        keep: impl Fn(Lane) -> bool,
    ) -> Vec<Cursor> {
        let Some(job_entry) = self.jobs.get_mut(&job) else {
            return Vec::new();
        };
        let mut freed = Vec::new();
        let mut emptied = Vec::new();
        for (&cursor, waiter) in job_entry.range_mut(..bound) {
            if !waiter.streams.iter().any(|candidate| *candidate == stream) {
                continue;
            }
            waiter.lanes.retain(|lane| keep(*lane));
            if waiter.lanes.is_empty() {
                waiter.streams.retain(|candidate| *candidate != stream);
                if waiter.streams.is_empty() {
                    emptied.push(cursor);
                    freed.push(cursor);
                }
            }
        }
        for cursor in emptied {
            job_entry.remove(&cursor);
        }
        if job_entry.is_empty() {
            self.jobs.remove(&job);
        }
        freed
    }

    /// Drop every waiter referencing `stream` across all jobs. Mirrors
    /// `remove_part`: waiters left with no streams are removed entirely.
    /// Returns freed `(job, cursor)` pairs.
    pub fn retire_stream(&mut self, stream: StreamId) -> Vec<(JobKey, Cursor)> {
        let mut retired = Vec::new();
        let mut empty_jobs = Vec::new();
        for (&job, job_entry) in self.jobs.range_mut(..) {
            let mut emptied = Vec::new();
            for (&cursor, waiter) in job_entry.iter_mut() {
                waiter.streams.retain(|candidate| *candidate != stream);
                if waiter.streams.is_empty() {
                    emptied.push(cursor);
                    retired.push((job, cursor));
                }
            }
            for cursor in emptied {
                job_entry.remove(&cursor);
            }
            if job_entry.is_empty() {
                empty_jobs.push(job);
            }
        }
        for job in empty_jobs {
            self.jobs.remove(&job);
        }
        retired
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

/// The full multi-stream machine: one instance holds MANY named streams
/// (mirroring `cursor_state: HashMap<PartId, CursorStreamState>`) plus the
/// job board aggregating concurrent work across them. Composition granularity
/// follows the existing code: one machine per entity (peer), not per stream.
///
/// Pure state only — events/effects stay in the use-site reducer, which calls
/// these operations inside its own event match and emits its own commands.
#[derive(Debug)]
pub struct WatermarkMachine<StreamId, JobKey, Lane, Payload, Cursor> {
    streams: HashMap<StreamId, WatermarkBook<Cursor>>,
    jobs: JobBoard<JobKey, StreamId, Lane, Cursor, Payload>,
}

// Manual impl for the same reason as `JobBoard`: an empty machine exists for
// any parameters (e.g. `&str` stream ids) without requiring `Default`.
impl<StreamId, JobKey, Lane, Payload, Cursor> Default
    for WatermarkMachine<StreamId, JobKey, Lane, Payload, Cursor>
{
    fn default() -> Self {
        Self {
            streams: Default::default(),
            jobs: Default::default(),
        }
    }
}

impl<StreamId, JobKey, Lane, Payload, Cursor>
    WatermarkMachine<StreamId, JobKey, Lane, Payload, Cursor>
where
    StreamId: Eq + std::hash::Hash + PartialEq + Copy,
    JobKey: Ord + Copy,
    Lane: PartialEq + Copy,
    Cursor: Ord + Copy + Default,
    Payload: Default,
{
    /// Admit work arriving on `stream` at `cursor`. Returns `false` for
    /// replays/duplicates (see [`WatermarkBook::begin`]); on success the
    /// caller should emit its domain work effects and call [`Self::track`]
    /// with the same identity.
    pub fn admit(&mut self, stream: StreamId, cursor: Cursor) -> bool {
        self.stream_book_mut(stream).begin(cursor)
    }

    /// Register the in-flight work admitted above: `(job, cursor)` waits on
    /// `lanes` and gates `stream`'s watermark.
    pub fn track(
        &mut self,
        stream: StreamId,
        job: JobKey,
        cursor: Cursor,
        lanes: impl IntoIterator<Item = Lane>,
        payload: Payload,
    ) {
        self.jobs.track(job, cursor, [stream], lanes, payload);
    }

    /// A lane of `(job, cursor)` finished. When the waiter fully settles,
    /// every stream it gated gets its slot marked ready and drained;
    /// newly-reachable watermarks are returned so the caller can persist
    /// them via its own commands.
    pub fn settle(
        &mut self,
        job: JobKey,
        cursor: Cursor,
        lane: Lane,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let streams = self.jobs.settle(job, cursor, lane);
        streams
            .into_iter()
            .map(|stream| {
                let book = self.stream_book_mut(stream);
                book.force_finish(cursor);
                (stream, book.drain())
            })
            .collect()
    }

    /// Supersede pending work for `job` on `stream` below `bound_cursor`.
    /// Freed cursors are marked finished and may immediately advance the
    /// stream. Returns the new watermarks reached, mirroring how
    /// `supersede_obj_part` re-drains each affected part.
    pub fn supersede(
        &mut self,
        stream: StreamId,
        job: JobKey,
        bound_cursor: Cursor,
        keep: impl Fn(Lane) -> bool,
    ) -> Vec<Option<Cursor>> {
        let freed = self.jobs.supersede(job, stream, bound_cursor, keep);
        let book = self.stream_book_mut(stream);
        freed
            .into_iter()
            .map(|cursor| {
                book.force_finish(cursor);
                book.drain()
            })
            .collect()
    }

    /// Retire a stream entirely (part removed / subscription dropped).
    /// Jobs losing their last stream are dropped; surviving jobs simply lose
    /// one stream reference. Mirrors `remove_part`.
    pub fn retire_stream(&mut self, stream: StreamId) {
        self.streams.remove(&stream);
        self.jobs.retire_stream(stream);
    }

    /// Current emitted watermark for a stream.
    pub fn watermark(&self, stream: &StreamId) -> Option<Cursor> {
        self.streams.get(stream).and_then(WatermarkBook::watermark)
    }

    /// True when the stream has no outstanding cursors (idle). Callers use
    /// this to emit idle notifications like `PartIdle`.
    pub fn is_settled(&self, stream: &StreamId) -> bool {
        self.streams
            .get(stream)
            .is_some_and(WatermarkBook::is_settled)
    }

    fn stream_book_mut(&mut self, stream: StreamId) -> &mut WatermarkBook<Cursor> {
        self.streams.entry(stream).or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Machine = WatermarkMachine<&'static str, u64, Lane, (), u64>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Lane {
        Membership,
        Sync,
    }

    #[test]
    fn replays_and_duplicates_are_ignored() {
        let mut m = Machine::default();
        assert!(m.admit("p", 5));
        m.track("p", 0, 5, [Lane::Sync], ());
        // same cursor admitted twice
        assert!(!m.admit("p", 5));
        m.settle(0, 5, Lane::Sync);
        // at-or-below the emitted watermark (replayed event)
        assert!(!m.admit("p", 5));
        assert!(!m.admit("p", 3));
        assert!(m.admit("p", 6));
    }

    #[test]
    fn watermark_advances_only_over_contiguous_prefix() {
        let mut m = Machine::default();
        m.admit("p", 10);
        m.track("p", 100, 10, [Lane::Sync], ());
        m.admit("p", 11);
        m.track("p", 100, 11, [Lane::Sync], ());

        // later cursor settles first: no advancement (head-of-line)
        let advanced = m.settle(100, 11, Lane::Sync);
        assert_eq!(advanced, vec![("p", None)]);
        assert_eq!(m.watermark(&"p"), None);

        // settling the blocking cursor advances past both
        let advanced = m.settle(100, 10, Lane::Sync);
        assert_eq!(advanced, vec![("p", Some(11))]);
        assert_eq!(m.watermark(&"p"), Some(11));
        assert!(m.is_settled(&"p"));
    }

    #[test]
    fn multi_lane_job_waits_for_every_lane() {
        let mut m = Machine::default();
        m.admit("p", 7);
        m.track("p", 42, 7, [Lane::Membership, Lane::Sync], ());
        assert!(m.settle(42, 7, Lane::Sync).is_empty());
        let advanced = m.settle(42, 7, Lane::Membership);
        assert_eq!(advanced, vec![("p", Some(7))]);
    }

    #[test]
    fn streams_track_watermarks_independently() {
        let mut m = Machine::default();
        m.admit("a", 1);
        m.track("a", 1, 1, [Lane::Sync], ());
        m.admit("b", 1);
        m.track("b", 2, 1, [Lane::Sync], ());
        assert_eq!(m.settle(1, 1, Lane::Sync), vec![("a", Some(1))]);
        assert_eq!(m.watermark(&"a"), Some(1));
        assert_eq!(m.watermark(&"b"), None);
        assert_eq!(m.settle(2, 1, Lane::Sync), vec![("b", Some(1))]);
    }

    #[test]
    fn supersede_cancels_sync_lane_but_membership_still_gates() {
        // mirrors removal with pending membership: sync work cancelled, but
        // the queued membership must still finish before its cursor can
        // advance (`supersede_obj_part`'s pending-membership special case).
        let mut m = Machine::default();
        m.admit("p", 3);
        m.track("p", 9, 3, [Lane::Membership, Lane::Sync], ());
        m.admit("p", 4);
        m.track("p", 9, 4, [Lane::Membership, Lane::Sync], ());

        // bound=4 covers cursor 3 only; its membership lane is kept, so
        // nothing is freed and nothing advances yet.
        assert!(m
            .supersede("p", 9, 4, |lane| lane == Lane::Membership)
            .is_empty());
        assert_eq!(m.watermark(&"p"), None);

        // Membership completion for cursor 3 unblocks it up to 3; cursor 4
        // (not below the bound) still owes BOTH its lanes.
        assert_eq!(m.settle(9, 3, Lane::Membership), vec![("p", Some(3))]);
        assert_eq!(
            m.settle(9, 4, Lane::Membership),
            Vec::<(&str, Option<u64>)>::new()
        );

        // Finish cursor 4's remaining sync lane: watermark jumps to 4.
        assert_eq!(m.settle(9, 4, Lane::Sync), vec![("p", Some(4))]);
        assert!(m.is_settled(&"p"));
    }

    #[test]
    fn supersede_frees_sync_only_waiters_immediately() {
        // The other branch of `supersede_obj_part`: a waiter whose only
        // remaining lane was cancelled is freed outright and its stream
        // re-drains immediately.
        let mut m = Machine::default();
        m.admit("p", 2);
        m.track("p", 7, 2, [Lane::Sync], ());

        let reached = m.supersede("p", 7, 3, |_| false);
        assert_eq!(reached, vec![Some(2)]);
        assert!(m.is_settled(&"p"));
    }

    #[test]
    fn retire_stream_drops_only_that_stream() {
        let mut m = Machine::default();
        m.admit("gone", 1);
        m.track("gone", 1, 1, [Lane::Sync], ());
        m.admit("kept", 2);
        m.track("kept", 2, 2, [Lane::Sync], ());
        m.retire_stream("gone");
        assert_eq!(m.watermark(&"gone"), None);
        assert_eq!(m.settle(2, 2, Lane::Sync), vec![("kept", Some(2))]);
    }

    #[test]
    fn multi_stream_scenario_mirrors_peer_part_structure() {
        // Mirrors `CursorSyncMachine`'s real shape: ONE machine instance holds
        // many part-streams (`cursor_state: HashMap<PartId, _>`) while a
        // single obj job aggregates work across them — a Changed event lists
        // several part_ids and one job gates every listed part's watermark
        // (`waiter.parts` in the original).
        let mut m = Machine::default();

        // Changed{obj 100, cursor 30, parts [alpha, beta]}: one admission per
        // listed part, one shared job, both streams gated.
        assert!(m.admit("alpha", 30));
        assert!(m.admit("beta", 30));
        m.track("alpha", 100, 30, [Lane::Sync], ());
        m.track("beta", 100, 30, [Lane::Sync], ());

        // The obj's sync job finishes once: both gated streams re-evaluate.
        let advanced = m.settle(100, 30, Lane::Sync);
        assert_eq!(advanced, vec![("alpha", Some(30)), ("beta", Some(30))]);

        // Parts advance independently afterwards (per-part cursors in the
        // original): alpha sees cursor 31, beta jumps to 44.
        assert!(m.admit("alpha", 31));
        m.track("alpha", 101, 31, [Lane::Sync], ());
        assert!(m.admit("beta", 44));
        m.track("beta", 101, 44, [Lane::Membership, Lane::Sync], ());

        assert_eq!(m.settle(101, 31, Lane::Sync), vec![("alpha", Some(31))]);
        assert_eq!(
            m.settle(101, 44, Lane::Sync),
            Vec::<(&str, Option<u64>)>::new()
        );
        assert_eq!(
            m.settle(101, 44, Lane::Membership),
            vec![("beta", Some(44))]
        );
        assert!(m.is_settled(&"alpha"));
        assert!(m.is_settled(&"beta"));
    }
}
