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
//! - **At-least-once admission guard** (`WatermarkBook::begin`): a cursor
//!   at or below the last emitted watermark, or already tracked as pending,
//!   is a duplicate and is ignored.
//! - **Explicit advancement**: a stream's watermark only moves to the
//!   contiguous prefix of finished slots ([`WatermarkBook::drain`]). Work is
//!   concurrent across jobs; the watermark is exactly "highest cursor whose
//!   entire prefix is terminal".
//! - **Job aggregation**: one job key can have several in-flight cursors and
//!   each cursor can wait on multiple lanes (e.g. membership + sync); a slot
//!   only finishes when every lane settled.
//! - **Shared slots finish only when every waiter settles**: several jobs may
//!   gate the same `(stream, cursor)` slot (one source read batch tracks all
//!   of its entries at the batch revision). The slot is force-finished only
//!   when the last tracked waiter is released, so settling one job never
//!   durably advances the stream past its unsettled siblings.

use std::collections::{BTreeMap, HashMap};

/// Per-stream slot bookkeeping: which cursors are pending work and where the
/// emitted watermark stands. Direct extraction of `CursorStreamState`.
#[derive(Debug, Default, Clone)]
struct WatermarkBook<Cursor> {
    last_emitted: Option<Cursor>,
    slots: BTreeMap<Cursor, Slot>,
    /// Tracked-waiter count per slot cursor. A slot is only finished when
    /// its count reaches zero (every waiter settled or was superseded).
    tracked: BTreeMap<Cursor, usize>,
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

    /// Mark an admitted cursor as finished without requiring prior admission
    /// (used for empty source revisions, which have no tracked waiters).
    pub fn force_finish(&mut self, cursor: Cursor) {
        if let Some(slot) = self.slots.get_mut(&cursor) {
            *slot = Slot::Ready;
        }
    }

    /// Register a waiter gating the slot at `cursor`. The slot must have
    /// been admitted and not yet emitted; tracking an emitted cursor is a
    /// no-op (its gating is moot).
    pub fn track_ref(&mut self, cursor: Cursor) {
        if self.slots.contains_key(&cursor) {
            *self.tracked.entry(cursor).or_default() += 1;
        }
    }

    /// Release one waiter gating the slot at `cursor`. When the last waiter
    /// is released the slot is force-finished and drained. Returns the newly
    /// reachable watermark, if any. Releasing an untracked (or already
    /// emitted) cursor is a no-op returning `None`.
    pub fn release(&mut self, cursor: Cursor) -> Option<Cursor> {
        let count = self.tracked.entry(cursor).or_default();
        if *count > 1 {
            *count -= 1;
            return None;
        }
        self.tracked.remove(&cursor);
        if !self.slots.contains_key(&cursor) {
            return None;
        }
        self.force_finish(cursor);
        self.drain()
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
            self.tracked.pop_first();
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

/// What [`JobBoard::drop_stream`] should do with a waiter when one stream is
/// going away for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDrop<Lane> {
    /// Keep gating the stream, but only with the lanes in `keep`. This is
    /// `supersede_obj_part`'s queued-membership case: the removal makes the
    /// sync work unnecessary while the already-queued membership mutation
    /// still has to finish before the cursor may advance.
    Retain { keep: Vec<Lane> },
    /// Stop gating this stream for this waiter. Its lanes are untouched and
    /// its cursor is freed on this stream alone; a waiter that still gates
    /// other streams stays alive for them.
    Release,
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
    JobKey: Ord + Clone,
    Lane: PartialEq + Clone,
    StreamId: PartialEq + Clone,
    Cursor: Ord + Copy,
    Payload: Default,
{
    /// Track in-flight work for `(job, cursor)`, adding `streams` and
    /// `lanes` to any existing waiter. Mirrors the waiter creation in
    /// `on_subscription_evt`. Returns the streams that were newly
    /// associated with the waiter — a duplicate association (the same job
    /// tracked twice for one cursor, e.g. two source rows of one batch
    /// carrying the same document key) must not double-count the stream
    /// slot's waiter, or the slot can never finish.
    pub fn track(
        &mut self,
        job: JobKey,
        cursor: Cursor,
        streams: impl IntoIterator<Item = StreamId>,
        lanes: impl IntoIterator<Item = Lane>,
        payload: Payload,
    ) -> Vec<StreamId> {
        let waiter = self.jobs.entry(job).or_default().entry(cursor).or_default();
        let mut new_streams = Vec::new();
        for stream in streams {
            if !waiter.streams.contains(&stream) {
                waiter.streams.push(stream.clone());
                new_streams.push(stream);
            }
        }
        // Lanes are set-semantics like the original's boolean flags
        // (`pending_sync`/`pending_membership`): re-tracking the same job
        // cursor from another stream must not duplicate them.
        for lane in lanes {
            if !waiter.lanes.contains(&lane) {
                waiter.lanes.push(lane);
            }
        }
        waiter.payload = payload;
        new_streams
    }

    /// Whether `(job, cursor)` still owes `lane`.
    ///
    /// The waiter is the authority on what is owed: [`Self::supersede`] and
    /// [`Self::drop_stream`] can drop a lane after the work that would settle it
    /// was already scheduled, and a freed waiter owes nothing at all. A
    /// completion must ask this before [`Self::settle`], which panics by design
    /// when the lane it settles was never owed.
    pub fn owes_lane(&self, job: &JobKey, cursor: Cursor, lane: &Lane) -> bool {
        self.jobs
            .get(job)
            .and_then(|entry| entry.get(&cursor))
            .is_some_and(|waiter| waiter.lanes.iter().any(|candidate| candidate == lane))
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
    /// Settle EVERY waiter keyed under `job`, regardless of cursor. This
    /// codifies the batch-gate pattern where one scheduler job (e.g. a
    /// reconciliation sweep gate) covers many cursors and finishes them all
    /// at once when the job completes. Returns each freed cursor with the
    /// streams it gated, so the caller can mark those streams finished.
    pub fn settle_job(&mut self, job: JobKey) -> Vec<(Cursor, Vec<StreamId>)> {
        let Some(entry) = self.jobs.remove(&job) else {
            return Vec::new();
        };
        entry
            .into_iter()
            .map(|(cursor, waiter)| (cursor, waiter.streams))
            .collect()
    }

    /// Supersede in-flight work for `job` on `stream` below `bound`: for
    /// every waiter with `cursor < bound` referencing `stream`, drop the
    /// lanes for which `keep` returns `false`.
    ///
    /// A waiter whose lanes are all dropped owes no work on ANY stream it
    /// was registered on (a lane-less waiter cannot gate anything — keeping
    /// it alive would panic the next `settle` on a surviving stream), so it
    /// is removed outright and its cursor is reported freed **once per
    /// stream it gated**. The caller must release the cursor on every one of
    /// them; releasing only the superseding stream leaves the others holding
    /// a pending slot that no waiter can ever settle, which stalls their
    /// watermark for the life of the machine.
    ///
    /// Mirrors `supersede_obj_part`'s "pending membership must still finish
    /// before its cursor advances" special case as a lane predicate. Note the
    /// original could not reach the multi-stream empty case: there a waiter
    /// lost all its streams only with its last one, so releasing that stream
    /// covered everything. Expressing the same cancel as a lane predicate
    /// makes the state reachable, so the reported set has to name every
    /// stream rather than assume the superseding one.
    pub fn supersede(
        &mut self,
        job: JobKey,
        stream: StreamId,
        bound: Cursor,
        keep: impl Fn(Lane) -> bool,
    ) -> Vec<(Cursor, Vec<StreamId>)> {
        let Some(job_entry) = self.jobs.get_mut(&job) else {
            return Vec::new();
        };
        let mut emptied = Vec::new();
        for (&cursor, waiter) in job_entry.range_mut(..bound) {
            if !waiter.streams.contains(&stream) {
                continue;
            }
            waiter.lanes.retain(|lane| keep(lane.clone()));
            if waiter.lanes.is_empty() {
                emptied.push(cursor);
            }
        }
        // Take the waiter's stream set as it is removed, so every stream that
        // gated the cursor is named in the result.
        let mut freed = Vec::new();
        for cursor in emptied {
            if let Some(waiter) = job_entry.remove(&cursor) {
                freed.push((cursor, waiter.streams));
            }
        }
        if job_entry.is_empty() {
            self.jobs.remove(&job);
        }
        freed
    }

    /// Drop `stream` for `job`'s waiters below `bound` — the other half of
    /// `supersede_obj_part`, which the extraction left out. For every waiter
    /// referencing `stream`, `decide` says whether the removal lets it stop
    /// gating the stream ([`StreamDrop::Release`], freeing the cursor here
    /// while the waiter stays alive on its other streams) or merely sheds
    /// some lanes ([`StreamDrop::Retain`]).
    ///
    /// Returns, per cursor, every stream whose book must release it: the
    /// dropped stream for a released waiter, and every stream a retained
    /// waiter still gated when its remaining lanes emptied — such a waiter
    /// owes nothing anywhere, so it is removed outright by the same rule
    /// [`Self::supersede`] applies.
    pub fn drop_stream(
        &mut self,
        job: JobKey,
        stream: StreamId,
        bound: Cursor,
        decide: impl Fn(&Waiter<StreamId, Lane, Payload>) -> StreamDrop<Lane>,
    ) -> Vec<(Cursor, Vec<StreamId>)> {
        let Some(job_entry) = self.jobs.get_mut(&job) else {
            return Vec::new();
        };
        let mut freed = Vec::new();
        let mut emptied = Vec::new();
        for (&cursor, waiter) in job_entry.range_mut(..bound) {
            if !waiter.streams.contains(&stream) {
                continue;
            }
            match decide(waiter) {
                StreamDrop::Retain { keep } => {
                    waiter.lanes.retain(|lane| keep.contains(lane));
                    if waiter.lanes.is_empty() {
                        freed.push((cursor, waiter.streams.clone()));
                        emptied.push(cursor);
                    }
                }
                StreamDrop::Release => {
                    waiter.streams.retain(|candidate| *candidate != stream);
                    freed.push((cursor, vec![stream.clone()]));
                    if waiter.streams.is_empty() {
                        emptied.push(cursor);
                    }
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
        for (job, job_entry) in self.jobs.range_mut(..) {
            let mut emptied = Vec::new();
            for (&cursor, waiter) in job_entry.iter_mut() {
                waiter.streams.retain(|candidate| *candidate != stream);
                if waiter.streams.is_empty() {
                    emptied.push(cursor);
                    retired.push((job.clone(), cursor));
                }
            }
            for cursor in emptied {
                job_entry.remove(&cursor);
            }
            if job_entry.is_empty() {
                empty_jobs.push(job.clone());
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
/// (mirroring `cursor_state: HashMap<PartKey, CursorStreamState>`) plus the
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
    StreamId: Eq + std::hash::Hash + Clone,
    JobKey: Ord + Clone,
    Lane: PartialEq + Clone,
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
    /// Finish an admitted cursor with no keyed job attached.  This is used for
    /// empty source revisions, which still need to advance the stream book.
    pub fn finish(&mut self, stream: StreamId, cursor: Cursor) -> Option<Cursor> {
        let book = self.stream_book_mut(stream);
        book.force_finish(cursor);
        book.drain()
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
        let new_streams = self.jobs.track(job, cursor, [stream], lanes, payload);
        // Only a genuinely new association gates the slot: a duplicate
        // track of the same (job, cursor) is one waiter, not two, and must
        // release the slot exactly once on settle.
        for stream in new_streams {
            self.stream_book_mut(stream).track_ref(cursor);
        }
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
                let reached = self.stream_book_mut(stream.clone()).release(cursor);
                (stream, reached)
            })
            .collect()
    }

    /// Settle every cursor gated by `job` (see [`JobBoard::settle_job`]).
    /// Each referenced stream is force-finished per freed cursor and
    /// re-drained; the aggregated new watermarks are returned so the caller
    /// can persist them via its own commands.
    pub fn settle_job(&mut self, job: JobKey) -> Vec<(StreamId, Option<Cursor>)> {
        let freed = self.jobs.settle_job(job);
        self.release_freed(freed)
    }

    /// Drop `stream` for `job`'s waiters below `bound_cursor` (see
    /// [`JobBoard::drop_stream`]). Every stream whose book releases a cursor
    /// may now advance; the reached watermarks are returned.
    pub fn drop_stream(
        &mut self,
        stream: StreamId,
        job: JobKey,
        bound_cursor: Cursor,
        decide: impl Fn(&Waiter<StreamId, Lane, Payload>) -> StreamDrop<Lane>,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let freed = self.jobs.drop_stream(job, stream, bound_cursor, decide);
        self.release_freed(freed)
    }

    /// Release each freed cursor on every stream the waiter gated, and
    /// aggregate the newly reachable watermark per stream. This is the shared
    /// tail of every operation that frees waiters: a waiter's cursor gates all
    /// of its streams, so every one of them has to be told.
    fn release_freed(
        &mut self,
        freed: Vec<(Cursor, Vec<StreamId>)>,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let mut out: Vec<(StreamId, Option<Cursor>)> = Vec::new();
        for (cursor, streams) in freed {
            for stream in streams {
                let reached = self.stream_book_mut(stream.clone()).release(cursor);
                match out.iter_mut().find(|(candidate, _)| candidate == &stream) {
                    Some((_, slot)) => {
                        if reached.is_some() {
                            *slot = reached;
                        }
                    }
                    None => out.push((stream, reached)),
                }
            }
        }
        out
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
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let freed = self.jobs.supersede(job, stream, bound_cursor, keep);
        self.release_freed(freed)
    }

    /// Retire a stream entirely (part removed / subscription dropped).
    /// Jobs losing their last stream are dropped; surviving jobs simply lose
    /// one stream reference. Mirrors `remove_part`.
    pub fn retire_stream(&mut self, stream: StreamId) {
        self.streams.remove(&stream);
        self.jobs.retire_stream(stream);
        // The stream book is dropped wholesale, so its tracked counts go
        // with it; surviving jobs keep their other stream references.
    }

    /// Diagnostics: every pending `(job, cursor)` waiter across streams.
    pub fn pending_jobs(&self) -> Vec<(JobKey, Cursor)> {
        let mut out = Vec::new();
        for (job, cursors) in &self.jobs.jobs {
            for cursor in cursors.keys() {
                out.push((job.clone(), *cursor));
            }
        }
        out
    }

    /// Whether the `(job, cursor)` waiter still owes `lane` on any stream.
    ///
    /// See [`JobBoard::owes_lane`]: a lane can be dropped by a supersede after
    /// the work that settles it is in flight, so a completion has to check what
    /// is still owed before settling it.
    pub fn owes_lane(&self, job: &JobKey, cursor: Cursor, lane: &Lane) -> bool {
        self.jobs.owes_lane(job, cursor, lane)
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
            .is_none_or(WatermarkBook::is_settled)
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
    fn shared_batch_slot_finishes_only_when_every_waiter_settles() {
        let mut m = Machine::default();
        // One batch read at revision 10 carrying two jobs.
        assert!(m.admit("p", 10));
        m.track("p", 1, 10, [Lane::Sync], ());
        m.track("p", 2, 10, [Lane::Sync], ());

        // Settling the first job must NOT advance the durable watermark
        // past the batch: the sibling is still in flight.
        let advanced = m.settle(1, 10, Lane::Sync);
        assert_eq!(advanced, vec![("p", None)]);
        assert_eq!(m.watermark(&"p"), None);
        assert!(!m.is_settled(&"p"));

        // The last waiter settles: the batch slot finishes and advances.
        let advanced = m.settle(2, 10, Lane::Sync);
        assert_eq!(advanced, vec![("p", Some(10))]);
        assert_eq!(m.watermark(&"p"), Some(10));
        assert!(m.is_settled(&"p"));
    }

    #[test]
    fn shared_batch_slot_across_contiguous_batches_waits_for_both() {
        let mut m = Machine::default();
        assert!(m.admit("p", 10));
        m.track("p", 1, 10, [Lane::Sync], ());
        m.track("p", 2, 10, [Lane::Sync], ());
        assert!(m.admit("p", 11));
        m.track("p", 1, 11, [Lane::Sync], ());

        // Job 1's newer cursor supersedes its older one; the batch-10 slot
        // still owes job 2, so nothing advances past 10.
        let advanced = m.settle(1, 11, Lane::Sync);
        assert_eq!(advanced, vec![("p", None)]);
        let freed = m.supersede("p", 1, 11, |_| false);
        // Superseding job 1's cursor-10 waiter does not finish the slot:
        // job 2 still gates it.
        assert_eq!(freed, vec![("p", None)]);
        assert_eq!(m.watermark(&"p"), None);

        // Job 2 settles: both slots are now ready (job 1 finished cursor 11
        // earlier), so the watermark drains past both batches.
        let advanced = m.settle(2, 10, Lane::Sync);
        assert_eq!(advanced, vec![("p", Some(11))]);
        assert_eq!(m.watermark(&"p"), Some(11));
    }

    #[test]
    fn laneless_waiter_is_freed_across_all_streams() {
        let mut m = Machine::default();
        m.admit("a", 10);
        m.admit("b", 10);
        m.track("a", 9, 10, [Lane::Sync], ());
        m.track("b", 9, 10, [Lane::Sync], ());

        // Superseding stream "a" drops the only lane: the waiter owes no
        // work on ANY stream it gated, so it is freed on both. The machine
        // must release the cursor on every reported stream; releasing only
        // the superseding one leaves "b" holding a pending slot that no
        // waiter can ever settle.
        let freed = m.supersede("a", 9, 11, |_| false);
        assert_eq!(freed, vec![("a", Some(10)), ("b", Some(10))]);
        assert_eq!(m.watermark(&"a"), Some(10));
        assert_eq!(m.watermark(&"b"), Some(10));
        assert!(m.is_settled(&"a"));
        assert!(m.is_settled(&"b"));
        // The waiter is gone: settling on the surviving stream is stale, not
        // a "lane completion without a pending lane" panic.
        assert!(m.settle(9, 10, Lane::Sync).is_empty());
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

    /// A completion must be able to ask what is still owed: the lane it would
    /// settle can be dropped (here by a supersede) while the work already
    /// scheduled for that lane lives on, and settling it then panics.
    #[test]
    fn owes_lane_reports_only_lanes_the_waiter_still_holds() {
        let mut m = Machine::default();
        m.admit("p", 7);
        m.track("p", 42, 7, [Lane::Membership, Lane::Sync], ());
        assert!(m.owes_lane(&42, 7, &Lane::Membership));
        assert!(m.owes_lane(&42, 7, &Lane::Sync));

        // The supersede sheds the sync lane for a cursor below the bound.
        m.supersede("p", 42, 8, |lane| lane == Lane::Membership);
        assert!(m.owes_lane(&42, 7, &Lane::Membership));
        assert!(!m.owes_lane(&42, 7, &Lane::Sync));

        m.settle(42, 7, Lane::Membership);
        // Fully settled: the waiter is gone, so nothing is owed at all.
        assert!(!m.owes_lane(&42, 7, &Lane::Membership));
        assert!(!m.owes_lane(&42, 7, &Lane::Sync));
        // An untracked cursor owes nothing either.
        assert!(!m.owes_lane(&42, 6, &Lane::Sync));
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
        assert!(
            m.supersede("p", 9, 4, |lane| lane == Lane::Membership)
                .is_empty()
        );
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
        assert_eq!(reached, vec![("p", Some(2))]);
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
        // many part-streams (`cursor_state: HashMap<PartKey, _>`) while a
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
    #[test]
    fn settle_job_frees_every_cursor_gated_by_the_job() {
        // The batch-gate pattern: one scheduler job covers many cursors;
        // settle_job finishes them all at once and re-drains each stream.
        let mut m = Machine::default();
        m.admit("p", 1);
        m.track("p", 9, 1, [Lane::Sync], ());
        m.admit("p", 2);
        m.track("p", 9, 2, [Lane::Sync], ());
        m.admit("q", 5);
        m.track("q", 9, 5, [Lane::Sync], ());

        let reached = m.settle_job(9);
        // p advances to 2 (both its cursors freed), q to 5, in one call.
        assert_eq!(reached, vec![("p", Some(2)), ("q", Some(5))]);
        assert!(m.is_settled(&"p"));
        assert!(m.is_settled(&"q"));

        // Unknown job: no-op.
        assert_eq!(m.settle_job(404), Vec::<(&str, Option<u64>)>::new());
    }

    #[test]
    fn settle_job_leaves_other_jobs_waiting() {
        let mut m = Machine::default();
        m.admit("p", 1);
        m.track("p", 9, 1, [Lane::Sync], ());
        m.admit("p", 2);
        m.track("p", 10, 2, [Lane::Sync], ());

        let reached = m.settle_job(9);
        // cursor 1 is terminal and contiguous, so the watermark reaches it;
        // cursor 2 (job 10) is still pending and gates nothing yet.
        assert_eq!(reached, vec![("p", Some(1))]);
        assert_eq!(m.watermark(&"p"), Some(1));
        assert!(!m.is_settled(&"p"));
        // Job 10 still pending: settling it advances past both.
        assert_eq!(m.settle_job(10), vec![("p", Some(2))]);
        assert!(m.is_settled(&"p"));
    }

    #[test]
    fn drop_stream_release_sheds_only_that_stream_and_keeps_the_waiter() {
        // supersede_obj_part's general branch: the removal makes the stream
        // irrelevant, so the waiter stops gating it and its cursor is freed
        // there — while the SAME waiter keeps owing work on the other stream.
        let mut m = Machine::default();
        assert!(m.admit("alpha", 30));
        assert!(m.admit("beta", 30));
        m.track("alpha", 100, 30, [Lane::Sync], ());
        m.track("beta", 100, 30, [Lane::Sync], ());

        let freed = m.drop_stream("alpha", 100, 31, |_| StreamDrop::Release);
        assert_eq!(freed, vec![("alpha", Some(30))]);
        assert!(m.is_settled(&"alpha"));
        assert!(
            !m.is_settled(&"beta"),
            "beta still owes the sync lane and must not be released"
        );
        assert_eq!(m.watermark(&"beta"), None);

        // The surviving stream settles through the normal path.
        assert_eq!(m.settle(100, 30, Lane::Sync), vec![("beta", Some(30))]);
        assert!(m.is_settled(&"beta"));
    }

    #[test]
    fn drop_stream_retain_keeps_gating_the_stream_with_fewer_lanes() {
        // supersede_obj_part's special case: a queued membership mutation
        // still has to finish before the cursor can advance, so the stream
        // stays gated and only the sync lane is shed.
        let mut m = Machine::default();
        assert!(m.admit("alpha", 30));
        m.track("alpha", 100, 30, [Lane::Membership, Lane::Sync], ());

        let freed = m.drop_stream("alpha", 100, 31, |_| StreamDrop::Retain {
            keep: vec![Lane::Membership],
        });
        assert!(freed.is_empty(), "nothing is freed while membership gates");
        assert_eq!(m.watermark(&"alpha"), None);

        assert_eq!(
            m.settle(100, 30, Lane::Membership),
            vec![("alpha", Some(30))]
        );
        assert!(m.is_settled(&"alpha"));
    }

    #[test]
    fn drop_stream_retain_that_empties_lanes_frees_every_stream() {
        // A retained waiter left with no lanes owes nothing on any stream it
        // gated, so it is removed outright and every one of them is released.
        let mut m = Machine::default();
        assert!(m.admit("alpha", 30));
        assert!(m.admit("beta", 30));
        m.track("alpha", 100, 30, [Lane::Sync], ());
        m.track("beta", 100, 30, [Lane::Sync], ());

        let freed = m.drop_stream("alpha", 100, 31, |_| StreamDrop::Retain { keep: vec![] });
        assert_eq!(freed, vec![("alpha", Some(30)), ("beta", Some(30))]);
        assert!(m.is_settled(&"alpha"));
        assert!(m.is_settled(&"beta"));
    }
}
