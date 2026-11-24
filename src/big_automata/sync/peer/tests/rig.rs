//! The scripted rig: a [`PeerSync`] driven by hand-built answers, one call at a time. Every
//! flush runs the machine's own law 13 recomputation (test builds assert it at the end of
//! `flush`), so every script checks the incremental facts after every step.

use super::*;

pub(super) use crate::part_store::laws::obj;
pub(super) use crate::part_store::laws::part;

use crate::part_store::PartDirtyCount;
use crate::part_store::bucket_summary::BucketFingerprint;
use crate::sync::bucket::BucketObjEntry;
use crate::sync::bucket::PartObjDelta;

pub(super) fn config() -> Config {
    Config {
        lifecycle: lifecycle::Config {
            max_parts: 4,
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
        },
        jobs: object_jobs::Config {
            window: 4,
            fetch_cap: 2,
            min_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(5),
        },
        lanes: lanes::Config {
            hold_ms: 300,
            live_limit: 2,
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
        },
        strat: AllowedStrat::CursorAndBucket,
        min_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(4),
    }
}

pub(super) fn part_route(n: u8) -> Route {
    Route::Part { part_key: part(n) }
}

pub(super) fn obj_route(n: u8) -> Route {
    Route::Object { obj_key: obj(n) }
}

/// A summary that makes the peer's part a cursor catch-up.
pub(super) fn cursor_summary(latest: Rev) -> PartSummary {
    PartSummary {
        latest_cursor: latest,
        member_count: 3,
        deepest_bucket_level: 1,
        dirty: PartDirtyCount {
            member_changes: 1,
            access_changes: 0,
        },
    }
}

/// A summary that makes it a bucket walk: far behind, and a walk is on offer. The part is
/// small, so the walk is one root bucket at level 0.
pub(super) fn walk_summary(latest: Rev) -> PartSummary {
    PartSummary {
        latest_cursor: latest,
        member_count: 3,
        deepest_bucket_level: 1,
        dirty: PartDirtyCount {
            member_changes: 1000,
            access_changes: 0,
        },
    }
}

pub(super) fn root_bucket(changed_at: Rev, len: u32) -> BucketSummary {
    BucketSummary {
        id: BuckId::ROOT,
        len,
        live_count: len,
        fp: (BucketFingerprint::default(), BucketFingerprint::default()),
        changed_at,
    }
}

/// A leaf page of the root bucket answering a request from `after`, with entries for the
/// objects of `delta`s.
pub(super) fn leaf_page(
    after: Option<ObjKey>,
    entries: &[(u8, PartObjDelta)],
    next_after: Option<ObjKey>,
) -> Map<BuckId, BucketObjLeafPage> {
    Map::from([(
        BuckId::ROOT,
        BucketObjLeafPage {
            after,
            entries: entries
                .iter()
                .map(|(n, delta)| BucketObjEntry {
                    obj_key: obj(*n),
                    delta: *delta,
                })
                .collect(),
            next_after,
        },
    )])
}

pub(super) fn updated(update: &UpdateSession) -> Reply<Result<SessionUpdated, SessionError>> {
    Reply::Answer(Ok(SessionUpdated {
        generation: update.generation,
        refused: vec![],
    }))
}

/// What a flush emitted, with finders for each kind of command.
#[derive(Debug, Default)]
pub(super) struct Cmds(pub Vec<PeerCommand>);

impl Cmds {
    fn pick<T>(&self, pick: impl Fn(&PeerCommand) -> Option<T>) -> Vec<T> {
        self.0.iter().filter_map(pick).collect()
    }

    pub(super) fn summaries(&self) -> Vec<(CallId, Map<PartKey, Rev>)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Summary { call, parts } => Some((*call, parts.clone())),
            _ => None,
        })
    }

    pub(super) fn sessions(&self) -> Vec<(CallId, UpdateSession)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Session { call, update } => Some((*call, update.clone())),
            _ => None,
        })
    }

    pub(super) fn rounds(&self) -> Vec<(CallId, Lane, PageSession)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Round { call, lane, page } => Some((*call, *lane, page.clone())),
            _ => None,
        })
    }

    pub(super) fn lists(&self) -> Vec<(CallId, PartKey, BuckId, Rev)> {
        self.pick(|cmd| match cmd {
            PeerCommand::ListBuckets {
                call,
                part_key,
                offset,
                since,
                ..
            } => Some((*call, part_key.clone(), *offset, *since)),
            _ => None,
        })
    }

    pub(super) fn leaves(&self) -> Vec<(CallId, PartKey, Vec<LeafBucketRequest>)> {
        self.pick(|cmd| match cmd {
            PeerCommand::LeafBuckets {
                call,
                part_key,
                buckets,
                ..
            } => Some((*call, part_key.clone(), buckets.clone())),
            _ => None,
        })
    }

    pub(super) fn records(&self) -> Vec<(CallId, ObjJob)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Record { call, job } => Some((*call, job.clone())),
            _ => None,
        })
    }

    pub(super) fn fetches(&self) -> Vec<(CallId, ObjJob)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Fetch { call, job } => Some((*call, job.clone())),
            _ => None,
        })
    }

    pub(super) fn cancels(&self) -> Vec<CallId> {
        self.pick(|cmd| match cmd {
            PeerCommand::Cancel { call } => Some(*call),
            _ => None,
        })
    }

    pub(super) fn persists(&self) -> Vec<(PartKey, Rev)> {
        self.pick(|cmd| match cmd {
            PeerCommand::PersistCursor { part, cursor } => Some((part.clone(), *cursor)),
            _ => None,
        })
    }

    /// The sync edges, `true` for `Synced`.
    pub(super) fn edges(&self) -> Vec<(PartKey, bool)> {
        self.pick(|cmd| match cmd {
            PeerCommand::Synced { part } => Some((part.clone(), true)),
            PeerCommand::Unsynced { part } => Some((part.clone(), false)),
            _ => None,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The only command of the flush, whatever it is.
    pub(super) fn only_round(&self) -> (CallId, Lane, PageSession) {
        let mut rounds = self.rounds();
        assert_eq!(rounds.len(), 1, "one round expected in {self:?}");
        rounds.remove(0)
    }

    pub(super) fn only_record(&self) -> (CallId, ObjJob) {
        let mut records = self.records();
        assert_eq!(records.len(), 1, "one record expected in {self:?}");
        records.remove(0)
    }
}

pub(super) struct Rig {
    pub sync: PeerSync,
    pub now: Instant,
}

impl Rig {
    pub(super) fn new() -> Self {
        Self::with(config())
    }

    pub(super) fn with(config: Config) -> Self {
        Self {
            sync: PeerSync::new(config, Uuid::from_u128(7)),
            now: Instant::now(),
        }
    }

    pub(super) fn flush(&mut self) -> Cmds {
        let mut out = vec![];
        self.sync.flush(self.now, &mut out);
        Cmds(out)
    }

    pub(super) fn wait(&mut self, secs: u64) {
        self.now += Duration::from_secs(secs);
    }

    /// Want `n` at `cursor`, and flush.
    pub(super) fn add_part(&mut self, n: u8, cursor: Rev) -> Cmds {
        self.sync.update(&[(part(n), cursor)], &[], &[], &[]);
        self.flush()
    }

    pub(super) fn add_object(&mut self, n: u8) -> Cmds {
        self.sync.update(&[], &[], &[obj(n)], &[]);
        self.flush()
    }

    pub(super) fn remove_part(&mut self, n: u8) -> Cmds {
        self.sync.update(&[], &[part(n)], &[], &[]);
        self.flush()
    }

    /// Answer the summary call in `cmds` with `summaries` and flush.
    pub(super) fn decide(&mut self, cmds: &Cmds, summaries: &[(u8, PartSummary)]) -> Cmds {
        let (call, _) = cmds.summaries().remove(0);
        self.sync.summary_replied(
            call,
            SummaryReply::Summaries(summaries.iter().map(|(n, s)| (part(*n), *s)).collect()),
        );
        self.flush()
    }

    /// Acknowledge the session update in `cmds` in full and flush.
    pub(super) fn ack(&mut self, cmds: &Cmds) -> Cmds {
        let (call, update) = cmds.sessions().remove(0);
        self.sync.update_replied(call, updated(&update));
        self.flush()
    }

    /// Bring part `n` to a cursor catch-up whose route is acknowledged; the flush after the
    /// acknowledgement carries the first round, which is returned.
    pub(super) fn follow(&mut self, n: u8, cursor: Rev) -> Cmds {
        let asked = self.add_part(n, cursor);
        let decided = self.decide(&asked, &[(n, cursor_summary(cursor))]);
        self.ack(&decided)
    }

    /// The target id the client gave `route`.
    pub(super) fn target(&self, route: &Route) -> TargetId {
        self.sync
            .client
            .acked()
            .find(|(_, acked)| *acked == route)
            .expect("the route is acknowledged")
            .0
    }

    pub(super) fn changed(&self, rev: Rev, o: u8, routes: &[Route]) -> PageEvent {
        PageEvent::Changed {
            rev,
            obj_key: obj(o),
            targets: routes.iter().map(|route| self.target(route)).collect(),
            payload: serde_json::json!({ "o": o, "rev": rev }),
        }
    }

    pub(super) fn removed(&self, rev: Rev, o: u8, route: &Route) -> PageEvent {
        PageEvent::Removed {
            rev,
            obj_key: obj(o),
            target: self.target(route),
        }
    }

    /// Answer round `call` (asked as `page`): every target it names resumes at `resume`.
    pub(super) fn answer_round(
        &mut self,
        call: CallId,
        page: &PageSession,
        events: Vec<PageEvent>,
        resume: Rev,
        drained: bool,
    ) -> Cmds {
        let verdicts = page
            .targets
            .iter()
            .map(|(id, _)| (*id, Verdict::Events { resume, drained }))
            .collect();
        self.sync
            .round_replied(call, Reply::Answer(Ok(SessionPage { events, verdicts })));
        self.flush()
    }

    pub(super) fn record_done(&mut self, call: CallId) -> Cmds {
        self.sync
            .record_ended(call, Outcome::Succeeded(Settle::Done));
        self.flush()
    }

    pub(super) fn fetch_done(&mut self, call: CallId) -> Cmds {
        self.sync.fetch_ended(call, Outcome::Succeeded(()));
        self.flush()
    }

    /// Drive part `n` to a cursor catch-up whose first round is answered drained at `resume`
    /// with no events: the part is `Synced`.
    pub(super) fn synced_at(&mut self, n: u8, resume: Rev) -> (CallId, Lane, PageSession) {
        let round = self.follow(n, 0).only_round();
        let cmds = self.answer_round(round.0, &round.2, vec![], resume, true);
        assert_eq!(cmds.edges(), [(part(n), true)]);
        // the next round of the same lane is out
        cmds.only_round()
    }

    /// Start a bucket walk of part `n` (strategy decided at `latest`, cursor held `since`) and
    /// return the list call.
    pub(super) fn walk(&mut self, n: u8, since: Rev, latest: Rev) -> (CallId, BuckId, Rev) {
        let asked = self.add_part(n, since);
        let decided = self.decide(&asked, &[(n, walk_summary(latest))]);
        let mut lists = decided.lists();
        assert_eq!(lists.len(), 1, "a walk lists first: {decided:?}");
        let (call, listed, offset, listed_since) = lists.remove(0);
        assert_eq!(
            (listed, offset, listed_since),
            (part(n), BuckId::ROOT, since)
        );
        (call, offset, since)
    }

    /// Answer a list call with the root bucket and return the leaf call that follows.
    pub(super) fn list_root(
        &mut self,
        call: CallId,
        changed_at: Rev,
        len: u32,
    ) -> (CallId, Vec<LeafBucketRequest>) {
        self.sync
            .list_replied(call, Reply::Answer(vec![root_bucket(changed_at, len)]));
        let mut leaves = self.flush().leaves();
        assert_eq!(leaves.len(), 1);
        let (call, _, buckets) = leaves.remove(0);
        (call, buckets)
    }
}
