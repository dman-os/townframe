use std::hash::Hash;

use crate::interlude::*;

strike! {
    /// Jobs are identifed by keys
    /// Streams are the source of the jobs
    /// Multiple streams can request jobs on the same keys
    /// The cursor monotincally orders versions of jobs
    /// Lanes indicate varieties of jobs for the keys
    pub struct WatermarkMachine<Key, StreamId, Cursor, Lane> {
        streams: Map<StreamId, struct StreamState<Cursor, Lane> {
            last_emitted: Option<Cursor>,
            slots: BTreeMap<Cursor, struct CursorSlotState<Lane> {
                ready: bool,
                lane: Lane,
            }>
        }>,
        jobs: BTreeMap<Key, struct JobState<StreamId, Cursor> {
            waiters: BTreeMap<
                Cursor,
                struct Waiter<StreamId> {
                    streams: Set<StreamId>,
                }
            >
        }>,

        stream_buf: Set<StreamId>,
        cursor_buf: Vec<Cursor>,
    }
}

impl<Cursor, Lane> Default for StreamState<Cursor, Lane> {
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

impl<Key, StreamId, Cursor, Lane> WatermarkMachine<Key, StreamId, Cursor, Lane>
where
    StreamId: Eq + Hash + Clone,
    Cursor: Ord + Copy + Default,
    Key: Ord,
    Lane: PartialEq,
{
    pub fn retire_stream(&mut self, sid: &StreamId) {
        todo!()
    }

    pub fn pending_jobs_len(&self) -> usize {
        self.jobs.values().map(|job| job.waiters.len()).sum()
    }

    pub fn watermark(&self, sid: &StreamId) -> Option<Cursor> {
        let stream_state = self.streams.get(sid)?;
        stream_state.last_emitted
    }

    pub fn add_job(
        &mut self,
        key: Key,
        cursor: Cursor,
        sids: impl IntoIterator<Item = (StreamId, Lane)>,
    ) -> Set<StreamId> {
        self.stream_buf.clear();
        // open slots on new cursor for all streams
        for (sid, lane) in sids {
            let stream_state = self.streams.entry(sid.clone()).or_default();
            if cursor <= stream_state.last_emitted.unwrap_or_default() {
                continue;
            }
            if let Some(existing_lane) = stream_state.slots.get(&cursor) {
                if existing_lane.lane != lane {
                    panic!("separate lane seen on same cursor for stream");
                }
                continue;
            }
            stream_state
                .slots
                .insert(cursor, CursorSlotState { ready: true, lane });
            self.stream_buf.insert(sid);
        }
        // if any new slots opened, add waiters
        if !self.stream_buf.is_empty() {
            let job = self.jobs.entry(key).or_default();
            let waiter = job.waiters.entry(cursor).or_default();
            for sid in &self.stream_buf {
                if !waiter.streams.contains(&sid) {
                    waiter.streams.insert(sid.clone());
                }
            }
        }
        std::mem::replace(&mut self.stream_buf, default())
    }

    pub fn is_settled(&self, sid: &StreamId) -> bool {
        self.streams
            .get(sid)
            .map(|ss| ss.slots.is_empty())
            .unwrap_or_default()
    }

    pub fn settle_on_cursor(
        &mut self,
        key: &Key,
        cursor: &Cursor,
        lane: &Lane,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let Some(job) = self.jobs.get_mut(key) else {
            return default();
        };
        let Some(waiter) = job.waiters.remove(cursor) else {
            return default();
        };
        if job.waiters.is_empty() {
            self.jobs.remove(&key);
        }
        waiter
            .streams
            .into_iter()
            .map(|sid| {
                let stream_state = self.streams.get_mut(&sid).expect(ERROR_IMPOSSIBLE);
                let slot_state = stream_state.slots.get_mut(cursor).expect(ERROR_IMPOSSIBLE);
                slot_state.ready = true;
                if &slot_state.lane != lane {
                    panic!("lane mismatch in settle_on_cursor");
                }
                (sid, stream_state.drain())
            })
            .collect()
    }

    pub fn settle_all_on_lane(
        &mut self,
        key: &Key,
        lane: &Lane,
    ) -> Vec<(StreamId, Option<Cursor>)> {
        let Some(job) = self.jobs.get_mut(key) else {
            return default();
        };
        self.cursor_buf.clear();
        self.stream_buf.clear();
        let mut out = Set::new();
        for (cursor, waiter) in &mut job.waiters {
            self.stream_buf.clear();
            for sid in &waiter.streams {
                let stream_state = self.streams.get_mut(&sid).expect(ERROR_IMPOSSIBLE);
                let slot_state = stream_state.slots.get_mut(cursor).expect(ERROR_IMPOSSIBLE);
                if lane == &slot_state.lane {
                    slot_state.ready = true;
                    out.insert(sid.clone());
                    self.stream_buf.insert(sid.clone());
                }
            }
            for sid in self.stream_buf.drain() {
                waiter.streams.remove(&sid);
            }

            if waiter.streams.is_empty() {
                self.cursor_buf.push(cursor.clone());
            }
        }
        for cursor in self.cursor_buf.drain(..) {
            job.waiters.remove(&cursor);
        }
        if job.waiters.is_empty() {
            self.jobs.remove(key);
        }
        out.into_iter()
            .map(|sid| {
                let stream_state = self.streams.get_mut(&sid).expect(ERROR_IMPOSSIBLE);
                (sid, stream_state.drain())
            })
            .collect()
    }
}

impl<Cursor, Lane> StreamState<Cursor, Lane>
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
mod tests {
    fn readding_with_more_parts() {}
    fn zero_lanes() {}
}
