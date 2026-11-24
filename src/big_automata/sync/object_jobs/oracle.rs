//! The dumbest model of `ObjectJobs`: flat vectors of wants and owed cursors, scanned
//! linearly, with no index and no state beyond them. It answers what a job must contain, who
//! has settled, `room` and `part_outstanding`; it knows nothing of calls, backoff or windows
//! of time.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Fresh,
    NeedsFetch,
    Settled,
}

#[derive(Debug, Clone)]
struct OPair {
    obj: ObjKey,
    part: PartKey,
    want: Want,
    cursor: Option<Rev>,
    high: Option<Rev>,
    seq: u64,
    stage: Stage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RowKey {
    Cursor(Rev, JobKey),
    Bucket,
}

/// One part's debt toward a cursor (or a bucket report).
#[derive(Debug, Clone)]
struct Row {
    obj: ObjKey,
    part: PartKey,
    key: RowKey,
    seq: u64,
    settled: bool,
}

/// What a call covered, as the oracle sees it.
#[derive(Debug, Clone, Default)]
pub(super) struct OSnap {
    pub obj: Option<ObjKey>,
    /// Part, want seq, whether the call adds to the part.
    pub parts: Vec<(PartKey, u64, bool)>,
    pub route: Option<u64>,
}

#[derive(Debug, Default)]
pub(super) struct Oracle {
    window: usize,
    seq: u64,
    pairs: Vec<OPair>,
    rows: Vec<Row>,
    /// Object, seq, stage and the newest content want's payload.
    routes: Vec<(ObjKey, u64, Stage, Option<ObjPayload>)>,
    expected: Vec<Settlement>,
}

impl Oracle {
    pub(super) fn new(window: usize) -> Self {
        Self {
            window,
            ..default()
        }
    }

    /// Whether the want was accepted (law 3).
    pub(super) fn want(
        &mut self,
        obj: &ObjKey,
        part: &PartKey,
        want: Want,
        origin: Origin,
    ) -> bool {
        let old = self
            .pairs
            .iter()
            .find(|pair| pair.obj == *obj && pair.part == *part);
        let cursor = match origin {
            Origin::Cursor(cursor) => Some(cursor),
            Origin::Bucket => None,
        };
        if cursor.is_some_and(|cursor| old.is_some_and(|old| old.high.is_some_and(|h| cursor <= h)))
        {
            return false;
        }
        let high = old.and_then(|old| old.high).max(cursor);
        self.pairs
            .retain(|pair| !(pair.obj == *obj && pair.part == *part));
        self.seq += 1;
        let key = match (cursor, &want) {
            (Some(cursor), Want::Present { .. }) => {
                RowKey::Cursor(cursor, JobKey::Sync(obj.clone()))
            }
            (Some(cursor), Want::Absent) => {
                RowKey::Cursor(cursor, JobKey::RemoveFromPart(obj.clone()))
            }
            (None, _) => RowKey::Bucket,
        };
        if key == RowKey::Bucket {
            self.rows
                .retain(|row| !(row.obj == *obj && row.part == *part && row.key == RowKey::Bucket));
        }
        self.rows.push(Row {
            obj: obj.clone(),
            part: part.clone(),
            key,
            seq: self.seq,
            settled: false,
        });
        self.pairs.push(OPair {
            obj: obj.clone(),
            part: part.clone(),
            want,
            cursor,
            high,
            seq: self.seq,
            stage: Stage::Fresh,
        });
        true
    }

    /// The route's seq if it was just turned off.
    pub(super) fn set_route(&mut self, obj: &ObjKey, on: bool) -> Option<u64> {
        let live = self.routes.iter().position(|(o, ..)| o == obj);
        match (on, live) {
            (true, None) => {
                self.seq += 1;
                self.routes
                    .push((obj.clone(), self.seq, Stage::Fresh, None));
                None
            }
            (false, Some(at)) => Some(self.routes.remove(at).1),
            _ => None,
        }
    }

    /// A content want: the route is replaced by a fresh one carrying `payload`.
    pub(super) fn content(&mut self, obj: &ObjKey, payload: ObjPayload) {
        self.routes.retain(|(o, ..)| o != obj);
        self.seq += 1;
        self.routes
            .push((obj.clone(), self.seq, Stage::Fresh, Some(payload)));
    }

    pub(super) fn route_seq(&self, obj: &ObjKey) -> Option<u64> {
        self.routes
            .iter()
            .find(|(o, ..)| o == obj)
            .map(|(_, seq, ..)| *seq)
    }

    pub(super) fn retire(&mut self, part: &PartKey) {
        self.pairs.retain(|pair| pair.part != *part);
        // a part that already settled a cursor keeps that fact until the cursor completes
        self.rows.retain(|row| row.part != *part || row.settled);
        self.expected
            .retain(|s| !matches!(s, Settlement::Bucket { part: p, .. } if p == part));
        self.emit_complete();
    }

    pub(super) fn settle(&mut self, snap: &OSnap) {
        let obj = snap.obj.as_ref().expect("snapshot has an object");
        for row in &mut self.rows {
            if row.obj == *obj
                && snap
                    .parts
                    .iter()
                    .any(|(part, seq, _)| *part == row.part && row.seq <= *seq)
            {
                row.settled = true;
            }
        }
        self.pairs.retain(|pair| {
            !(pair.obj == *obj
                && snap
                    .parts
                    .iter()
                    .any(|(p, s, _)| *p == pair.part && *s == pair.seq))
        });
        for route in &mut self.routes {
            if route.0 == *obj && snap.route == Some(route.1) {
                route.2 = Stage::Settled;
            }
        }
        self.emit_complete();
    }

    pub(super) fn promote(&mut self, snap: &OSnap) {
        let obj = snap.obj.as_ref().expect("snapshot has an object");
        for pair in &mut self.pairs {
            if pair.obj == *obj
                && snap
                    .parts
                    .iter()
                    .any(|(p, s, _)| *p == pair.part && *s == pair.seq)
            {
                pair.stage = Stage::NeedsFetch;
            }
        }
        for route in &mut self.routes {
            if route.0 == *obj && snap.route == Some(route.1) {
                route.2 = Stage::NeedsFetch;
            }
        }
    }

    /// Report every (object, cursor) all of whose parts have settled or gone.
    fn emit_complete(&mut self) {
        let rows = self.rows.clone();
        let complete = |row: &Row| {
            rows.iter()
                .filter(|other| {
                    other.obj == row.obj
                        && other.key == row.key
                        && (row.key != RowKey::Bucket || other.part == row.part)
                })
                .all(|other| other.settled)
        };
        let mut done = vec![];
        for row in rows.iter().filter(|row| row.settled && complete(row)) {
            let settlement = match &row.key {
                RowKey::Cursor(cursor, job) => Settlement::Cursor {
                    job: job.clone(),
                    cursor: *cursor,
                },
                RowKey::Bucket => Settlement::Bucket {
                    part: row.part.clone(),
                    obj: row.obj.clone(),
                },
            };
            // a cursor shared by several parts is one settlement
            if !done.contains(&settlement) {
                done.push(settlement);
            }
        }
        self.rows.retain(|row| !(row.settled && complete(row)));
        self.expected.extend(done);
    }

    pub(super) fn is_expected(&self, settlement: &Settlement) -> bool {
        self.expected.contains(settlement)
    }

    pub(super) fn take_expected(&mut self) -> Vec<Settlement> {
        std::mem::take(&mut self.expected)
    }

    /// The job every want at `stage` of the object adds up to.
    pub(super) fn job(&self, obj: &ObjKey, stage: Stage) -> Option<(ObjJob, OSnap)> {
        let mine: Vec<_> = self
            .pairs
            .iter()
            .filter(|pair| pair.obj == *obj && pair.stage == stage)
            .collect();
        let route = self
            .routes
            .iter()
            .find(|(o, _, st, _)| o == obj && *st == stage)
            .map(|(_, seq, ..)| *seq);
        if mine.is_empty() && route.is_none() {
            return None;
        }
        let mut add: Vec<_> = mine
            .iter()
            .filter(|p| matches!(p.want, Want::Present { .. }))
            .collect();
        add.sort_by(|a, b| a.part.cmp(&b.part));
        let mut remove: Vec<_> = mine
            .iter()
            .filter(|p| matches!(p.want, Want::Absent))
            .collect();
        remove.sort_by(|a, b| a.part.cmp(&b.part));
        let payload = add
            .iter()
            .max_by_key(|p| (p.cursor, p.seq))
            .map(|p| match &p.want {
                Want::Present { payload } => payload.clone(),
                Want::Absent => unreachable!(),
            })
            .or_else(|| {
                self.routes
                    .iter()
                    .find(|(o, _, st, _)| o == obj && *st == stage)
                    .and_then(|(.., payload)| payload.clone())
            });
        let job = ObjJob {
            obj: obj.clone(),
            add: add.iter().map(|p| p.part.clone()).collect(),
            remove: remove.iter().map(|p| p.part.clone()).collect(),
            payload,
        };
        let snap = OSnap {
            obj: Some(obj.clone()),
            parts: mine
                .iter()
                .map(|p| {
                    (
                        p.part.clone(),
                        p.seq,
                        matches!(p.want, Want::Present { .. }),
                    )
                })
                .collect(),
            route,
        };
        Some((job, snap))
    }

    pub(super) fn room(&self) -> usize {
        let mut unsettled: Vec<&ObjKey> = self.pairs.iter().map(|p| &p.obj).collect();
        unsettled.extend(
            self.routes
                .iter()
                .filter(|(_, _, st, _)| *st != Stage::Settled)
                .map(|(o, ..)| o),
        );
        unsettled.sort();
        unsettled.dedup();
        self.window.saturating_sub(unsettled.len())
    }

    pub(super) fn part_outstanding(&self, part: &PartKey) -> bool {
        self.pairs.iter().any(|pair| pair.part == *part)
    }

    pub(super) fn pair_exists(&self, obj: &ObjKey, part: &PartKey) -> bool {
        self.pairs
            .iter()
            .any(|pair| pair.obj == *obj && pair.part == *part)
    }

    pub(super) fn quiet(&self) -> bool {
        self.pairs.is_empty()
            && self.rows.is_empty()
            && self
                .routes
                .iter()
                .all(|(_, _, st, _)| *st == Stage::Settled)
    }
}
