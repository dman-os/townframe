use std::collections::BTreeSet;
use std::hash::Hash;

use crate::interlude::*;

strike! {
    #[structstruck::each[derive(Debug)]]
    /// Jobs are identifed by keys
    /// Streams are the source of the jobs
    /// Multiple streams can request jobs on the same keys
    /// The cursor monotincally orders versions of jobs
    /// What kind of work a job is belongs in its key: two kinds of work on the same
    /// object are two keys, so settling or releasing one never touches the other.
    pub struct WatermarkMachine<Key, StreamId, Cursor> {
        streams: Map<StreamId, struct StreamState<Cursor> {
            last_emitted: Option<Cursor>,
            /// One waiter per slot: `add_job` refuses a cursor a stream has already seen.
            slots: BTreeMap<Cursor, struct CursorSlotState {
                ready: bool,
            }>
        }>,
        jobs: BTreeMap<Key, struct JobState<StreamId, Cursor> {
            waiters: BTreeMap<
                Cursor,
                struct Waiter<StreamId> {
                    // ordered so that the advances a settle reports come out in a
                    // deterministic order
                    streams: BTreeSet<StreamId>,
                }
            >
        }>,

        stream_buf: Set<StreamId>,
        cursor_buf: Vec<Cursor>,
    }
}

impl<Key, StreamId, Cursor> Default for WatermarkMachine<Key, StreamId, Cursor> {
    fn default() -> Self {
        Self {
            streams: default(),
            jobs: default(),
            stream_buf: default(),
            cursor_buf: default(),
        }
    }
}
impl<Cursor> Default for StreamState<Cursor> {
    fn default() -> Self {
        Self {
            last_emitted: default(),
            slots: default(),
        }
    }
}
impl<StreamId, Cursor> Default for JobState<StreamId, Cursor> {
    fn default() -> Self {
        Self { waiters: default() }
    }
}
impl<StreamId> Default for Waiter<StreamId> {
    fn default() -> Self {
        Self { streams: default() }
    }
}

impl<Key, StreamId, Cursor> WatermarkMachine<Key, StreamId, Cursor>
where
    StreamId: Eq + Hash + Ord + Clone,
    Cursor: Ord + Copy,
    Key: Ord + Clone,
{
    /// Forget the stream: its slots, and its place in every waiter. Waiters and jobs left
    /// with no streams are dropped. Nothing is reported, a retired stream has no watermark
    /// left to advance.
    pub fn retire_stream(&mut self, sid: &StreamId) {
        self.streams.remove(sid);
        self.jobs.retain(|_, job| {
            job.waiters.retain(|_, waiter| {
                waiter.streams.remove(sid);
                !waiter.streams.is_empty()
            });
            !job.waiters.is_empty()
        });
    }

    pub fn pending_jobs_len(&self) -> usize {
        self.jobs.values().map(|job| job.waiters.len()).sum()
    }

    pub fn watermark(&self, sid: &StreamId) -> Option<Cursor> {
        let stream_state = self.streams.get(sid)?;
        stream_state.last_emitted
    }

    /// Whether the job still has a waiter on `cursor`.
    pub fn owes(&self, key: &Key, cursor: &Cursor) -> bool {
        self.jobs
            .get(key)
            .is_some_and(|job| job.waiters.contains_key(cursor))
    }

    /// Open a slot at `cursor` on each stream and make the job wait on it. Streams that
    /// already emitted past the cursor, or already hold a slot for it (a re-delivery), are
    /// skipped. Returns the streams the job now waits on.
    pub fn add_job(
        &mut self,
        key: Key,
        cursor: Cursor,
        sids: impl IntoIterator<Item = StreamId>,
    ) -> Set<StreamId> {
        self.stream_buf.clear();
        // open slots on new cursor for all streams
        for sid in sids {
            let stream_state = self.streams.entry(sid.clone()).or_default();
            // compared against the Option: on a fresh stream every cursor is new,
            // including `Cursor::default()`
            if stream_state
                .last_emitted
                .is_some_and(|last_emitted| cursor <= last_emitted)
            {
                continue;
            }
            if stream_state.slots.contains_key(&cursor) {
                continue;
            }
            // the slot is not ready until the job settles it
            stream_state
                .slots
                .insert(cursor, CursorSlotState { ready: false });
            self.stream_buf.insert(sid);
        }
        // if any new slots opened, add waiters
        if !self.stream_buf.is_empty() {
            let job = self.jobs.entry(key).or_default();
            let waiter = job.waiters.entry(cursor).or_default();
            waiter.streams.extend(self.stream_buf.iter().cloned());
        }
        std::mem::replace(&mut self.stream_buf, default())
    }

    /// Whether the stream holds no slot that has not passed. A stream never seen, or retired,
    /// owes nothing.
    pub fn is_settled(&self, sid: &StreamId) -> bool {
        self.streams.get(sid).is_none_or(|ss| ss.slots.is_empty())
    }

    /// The job's work at `cursor` is done on every stream it waits on there. A cursor the job
    /// has no waiter on (stale, or released) settles nothing.
    pub fn settle_on_cursor(
        &mut self,
        key: &Key,
        cursor: &Cursor,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let Some(job) = self.jobs.get_mut(key) else {
            return default();
        };
        let Some(waiter) = job.waiters.remove(cursor) else {
            return default();
        };
        if job.waiters.is_empty() {
            self.jobs.remove(key);
        }
        waiter
            .streams
            .into_iter()
            .map(|sid| {
                let stream_state = self.streams.get_mut(&sid).expect(ERROR_IMPOSSIBLE);
                let slot_state = stream_state.slots.get_mut(cursor).expect(ERROR_IMPOSSIBLE);
                slot_state.ready = true;
                let reached = stream_state.drain();
                (sid, reached)
            })
            .collect()
    }

    /// Stop the job waiting on `sid` at every cursor before `before_cursor`, leaving its
    /// waiters on other streams alone. Those slots no longer owe this job's work, so they are
    /// ready. This is the supersede path: a later event made the job's work on that one stream
    /// moot.
    pub fn release_stream(
        &mut self,
        key: &Key,
        sid: &StreamId,
        before_cursor: &Cursor,
    ) -> Option<Cursor> {
        let job = self.jobs.get_mut(key)?;
        // a stream that never held a slot has nothing for the job to release
        let stream_state = self.streams.get_mut(sid)?;
        self.cursor_buf.clear();
        let mut released = false;
        for (cursor, waiter) in job.waiters.range_mut(..*before_cursor) {
            if !waiter.streams.remove(sid) {
                continue;
            }
            stream_state
                .slots
                .get_mut(cursor)
                .expect(ERROR_IMPOSSIBLE)
                .ready = true;
            released = true;
            if waiter.streams.is_empty() {
                self.cursor_buf.push(*cursor);
            }
        }
        for cursor in self.cursor_buf.drain(..) {
            job.waiters.remove(&cursor);
        }
        if job.waiters.is_empty() {
            self.jobs.remove(key);
        }
        if !released {
            return None;
        }
        stream_state.drain()
    }
}

impl<Cursor> StreamState<Cursor>
where
    Cursor: Ord + Copy,
{
    fn drain(&mut self) -> Option<Cursor> {
        // calculate Ready highmark
        let latest_ready = {
            let mut latest_ready = None;
            for (cursor, slot) in &self.slots {
                if slot.ready {
                    latest_ready = Some(*cursor);
                } else {
                    break;
                }
            }
            latest_ready
        };
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
}

#[cfg(test)]
mod tests;
