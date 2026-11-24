//! [`PeerSync`]: the per-peer sync machine. The laws are in the [parent module](super); this
//! module is the routing glue that makes them hold.
//!
//! # The driver's contract
//!
//! Sans-IO. Every input is a method; every output is a [`PeerCommand`] that leaves with the
//! next [`PeerSync::flush`], which is also the only place the clock is read. The driver calls
//! `flush` after every input and when [`PeerSync::next_due`] passes.
//!
//! - Every outward call carries a fresh [`CallId`]. Its outcome comes back through the input
//!   that names its kind, under that id; a call that cannot be answered comes back as
//!   [`Reply::Lost`]. An outcome for an id that is not in flight changes nothing (law 2).
//!   `Cancel` asks the driver to abort a call; it still reports the call's outcome (a
//!   cancelled backend call reports [`Outcome::Cancelled`]).
//! - A list-buckets call is the filtered listing: the driver lists, filters against the local
//!   part (`bucket::filter_buckets`), relists past a page that filtered to nothing, and
//!   answers with the buckets at the working level. An empty answer ends the listing. A
//!   leaf-buckets call is answered with the filtered pages (`bucket::filter_objects`).
//!   Anything else the peer or the transport did wrong is [`Reply::Lost`]: the call is
//!   reissued unchanged after backoff (law 11).
//! - `PersistCursor` commands are written in order per part. `Synced` and `Unsynced` are
//!   edges of one derived fact (law 13); a part's first edge is `Synced`.
//! - [`PeerSync::disconnect`] ends the machine (law 14).
//!
//! # State
//!
//! Besides its sub-machines the machine keeps only routing glue: the call table, the wanted
//! parts with the last sync fact emitted for each, the wanted objects with their session
//! cursors, the last drained cursor of each part route, the walks, and three backoff gates
//! (session updates, walk calls, re-adding refused routes). Calls of a retired part leave the
//! table with it, so its late outcomes are stale.
//!
//! # Sync facts
//!
//! Every place that changes an input of `synced(part)` marks the part `touched`; `flush`
//! recomputes the touched parts last and emits the edges. A missed mark delays an edge and
//! never corrupts state; test builds recompute every part at the end of every flush and
//! assert that the incremental fact agrees.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::AllowedStrat;
use crate::kit::scheduler::Instant;
use crate::part_store::PartSummary;
use crate::part_store::bucket_summary::BucketSummary;
use crate::replay::client::ReplayClient;
use crate::replay::client::RoundOutcome;
use crate::replay::client::TargetOutcome;
use crate::replay::client::UpdateOutcome;
use crate::replay::lanes;
use crate::replay::lanes::Lane;
use crate::replay::lanes::Lanes;
use crate::replay::lanes::LanesCommand;
use crate::replay::wire::PageEvent;
use crate::replay::wire::PageSession;
use crate::replay::wire::Route;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionId;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::TargetId;
use crate::replay::wire::UpdateSession;
use crate::replay::wire::Verdict;
use crate::rev::Rev;
use crate::sync::bucket::BucketMachine;
use crate::sync::bucket::BucketMachineCommand;
use crate::sync::bucket::BucketObjLeafPage;
use crate::sync::cursor::CursorEvent;
use crate::sync::cursor::CursorMachineCommand;
use crate::sync::cursor::CursorSyncMachine;
use crate::sync::cursor::ObjChanged;
use crate::sync::cursor::ObjRemovedFromPart;
use crate::sync::lifecycle;
use crate::sync::lifecycle::DecideOutcome;
use crate::sync::lifecycle::LifecycleCommand;
use crate::sync::lifecycle::PartLifecycle;
use crate::sync::lifecycle::Phase;
use crate::sync::object_jobs;
use crate::sync::object_jobs::JobsCommand;
use crate::sync::object_jobs::ObjJob;
use crate::sync::object_jobs::ObjectJobs;
use crate::sync::object_jobs::Origin;
use crate::sync::object_jobs::Outcome;
use crate::sync::object_jobs::Settle;
use crate::sync::object_jobs::Settlement;
use crate::sync::object_jobs::Want;
use crate::sync::rpc::bucket::LeafBucketRequest;
use crate::sync::strategy::PeerPartStrat;
use crate::sync::strategy::decide_peer_strat;

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

/// The one id space of every outward call (law 2).
pub(crate) type CallId = u64;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub lifecycle: lifecycle::Config,
    pub jobs: object_jobs::Config,
    pub lanes: lanes::Config,
    /// How parts catch up unless the peer's summary rules a walk out.
    pub strat: AllowedStrat,
    /// The ladder of the machine's own call lanes: session updates, walk calls and re-adding
    /// refused routes. The sub-machines pace their own calls.
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

/// What the driver reports for a call that it could or could not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reply<T> {
    Answer(T),
    /// The call failed, timed out, was lost or cannot be answered (a peer that stopped serving
    /// a part answers its walk calls so). It is retried on its lane's backoff, a walk call
    /// unchanged and for ever: a walk on a part the peer stopped serving keeps retrying with
    /// backoff until the embedder removes the part.
    Lost,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SummaryReply {
    /// The peer's summary of each part it answered; a part it left out is retried
    /// (`PartLifecycle` law 4), and one nobody asked for is ignored (law 3).
    Summaries(Map<PartKey, PartSummary>),
    TooManyParts {
        cap: usize,
    },
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerCommand {
    /// Ask the peer to summarize `parts`, each with the cursor we hold for it.
    Summary {
        call: CallId,
        parts: Map<PartKey, Rev>,
    },
    /// Send one session update.
    Session {
        call: CallId,
        update: UpdateSession,
    },
    /// Send one round of `lane`.
    Round {
        call: CallId,
        lane: Lane,
        page: PageSession,
    },
    /// List the part's changed buckets from `offset`, and filter them at `working_level`.
    ListBuckets {
        call: CallId,
        part_key: PartKey,
        offset: BuckId,
        since: Rev,
        working_level: BuckLevel,
    },
    LeafBuckets {
        call: CallId,
        part_key: PartKey,
        since: Rev,
        buckets: Vec<LeafBucketRequest>,
    },
    Record {
        call: CallId,
        job: ObjJob,
    },
    Fetch {
        call: CallId,
        job: ObjJob,
    },
    /// Abort a call in flight. Its outcome is still reported.
    Cancel {
        call: CallId,
    },
    /// Write `cursor` as the cursor consumed from this peer for `part`.
    PersistCursor {
        part: PartKey,
        cursor: Rev,
    },
    Synced {
        part: PartKey,
    },
    Unsynced {
        part: PartKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WalkRequest {
    List {
        offset: BuckId,
        since: Rev,
        working_level: BuckLevel,
    },
    Leaf {
        since: Rev,
        buckets: Vec<LeafBucketRequest>,
    },
}

/// One call in flight, with what its outcome needs.
#[derive(Debug)]
enum Call {
    Summary {
        sub: lifecycle::CallId,
        /// The parts asked, with the cursors advertised.
        parts: Map<PartKey, Rev>,
    },
    Update {
        request: UpdateSession,
    },
    Round {
        sub: lanes::CallId,
        request: PageSession,
    },
    Walk {
        part: PartKey,
        request: WalkRequest,
    },
    Record {
        sub: object_jobs::CallId,
        /// The parts the job names: their sync facts may change when it ends.
        parts: Vec<PartKey>,
    },
    Fetch {
        sub: object_jobs::CallId,
        parts: Vec<PartKey>,
    },
}

#[derive(Debug)]
struct Walk {
    machine: BucketMachine,
    /// The walk's calls in flight, so retiring the part drops them.
    calls: BTreeSet<CallId>,
}

#[derive(Debug, Default)]
enum Gate {
    #[default]
    Open,
    /// A failure waits for the next flush to read the clock and fix the deadline.
    Unarmed,
    Until(Instant),
}

/// One of the machine's own call lanes: a backoff ladder and its gate.
#[derive(Debug, Default)]
struct Pacer {
    backoff: Duration,
    gate: Gate,
}

impl Pacer {
    /// Same ladder as the sub-machines': the first failure waits the floor, later ones
    /// double, the cap wins.
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

    fn open(&mut self, now: Instant) -> bool {
        if matches!(self.gate, Gate::Unarmed) {
            self.gate = Gate::Until(now + self.backoff);
        }
        match self.gate {
            Gate::Open => true,
            Gate::Unarmed => unreachable!("armed above"),
            Gate::Until(at) => at <= now,
        }
    }

    fn due(&self, last_flush: Instant) -> Option<Instant> {
        match self.gate {
            Gate::Until(at) if at > last_flush => Some(at),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeerSync {
    config: Config,
    lifecycle: PartLifecycle,
    client: ReplayClient,
    lanes: Lanes,
    cursor: CursorSyncMachine,
    jobs: ObjectJobs,
    walks: BTreeMap<PartKey, Walk>,
    /// Where the next flush starts serving walks, so no walk is always last (law 10).
    walk_turn: usize,

    next_call: CallId,
    calls: BTreeMap<CallId, Call>,
    /// `ObjectJobs`' own ids of the calls it has in flight, for its `Cancel`s.
    job_calls: Map<object_jobs::CallId, CallId>,
    /// Every wanted part, with the last sync fact emitted for it (law 13).
    parts: BTreeMap<PartKey, bool>,
    /// Every wanted object route, with its session cursor (law 5).
    objects: BTreeMap<ObjKey, Rev>,
    /// The `resume` of the last answer for a part route if it drained, until a later answer
    /// or a reset says otherwise.
    drained: BTreeMap<PartKey, Rev>,
    /// Parts whose sync fact may have changed since the last flush.
    touched: BTreeSet<PartKey>,
    outbox: Vec<PeerCommand>,
    /// Walk calls that failed and wait for backoff, to be reissued unchanged (law 11).
    walk_retry: Vec<(PartKey, WalkRequest)>,
    walk_pacer: Pacer,
    update_pacer: Pacer,
    blocked_pacer: Pacer,
    last_flush: Option<Instant>,
    /// Scratch for the client's per-target outcomes and acknowledged routes.
    outcomes: Vec<(TargetId, Route, TargetOutcome)>,
    acked: Vec<Route>,
}

/// The cursor a round for `route` is asked from (law 5): the part's replay cursor while the
/// part is catching up, an object route's session cursor.
fn route_cursor(
    route: &Route,
    lifecycle: &PartLifecycle,
    cursor: &CursorSyncMachine,
    objects: &BTreeMap<ObjKey, Rev>,
) -> Option<Rev> {
    match route {
        Route::Part { part_key } => match lifecycle.phase(part_key)? {
            Phase::Cursor | Phase::Walking => {
                Some(cursor.part_replay_cursor(part_key, lifecycle.cursor(part_key)?))
            }
            Phase::Deciding | Phase::Unanswered => None,
        },
        Route::Object { obj_key } => objects.get(obj_key).copied(),
    }
}

/// The cursor `request` asked target `id` from. A request lists its targets by id.
fn asked_from(request: &PageSession, id: TargetId) -> Option<Rev> {
    debug_assert!(request.targets.is_sorted_by_key(|(id, _)| *id));
    request
        .targets
        .binary_search_by_key(&id, |(id, _)| *id)
        .ok()
        .map(|at| request.targets[at].1)
}

/// A bucket listing the walk's machine can take without asserting: in order from `offset`,
/// at the working level, and unchanged since the cursor it asked past.
fn listing_is_sound(
    listing: &[BucketSummary],
    offset: BuckId,
    since: Rev,
    working_level: BuckLevel,
) -> bool {
    let mut next = offset;
    listing.iter().all(|buck| {
        let sound = buck.id >= next && buck.id.level() == working_level && buck.changed_at >= since;
        next = buck.id.increment();
        sound
    })
}

impl PeerSync {
    pub(crate) fn new(config: Config, session_id: SessionId) -> Self {
        // While a walk runs, rounds get half the window (law 12) and a held `Live` round takes
        // up to `live_limit` of that first; a window not above twice `live_limit` would leave
        // `Bulk` nothing for as long as any walk runs, and a walk of a part the peer stopped
        // serving retries for good.
        assert!(
            config.jobs.window > 2 * config.lanes.live_limit,
            "the window ({}) must exceed twice the Live share ({})",
            config.jobs.window,
            config.lanes.live_limit,
        );
        Self {
            lifecycle: PartLifecycle::new(config.lifecycle.clone()),
            client: ReplayClient::new(session_id),
            lanes: Lanes::new(config.lanes.clone()),
            cursor: default(),
            jobs: ObjectJobs::new(config.jobs.clone()),
            config,
            walks: default(),
            walk_turn: 0,
            next_call: 0,
            calls: default(),
            job_calls: default(),
            parts: default(),
            objects: default(),
            drained: default(),
            touched: default(),
            outbox: default(),
            walk_retry: default(),
            walk_pacer: default(),
            update_pacer: default(),
            blocked_pacer: default(),
            last_flush: None,
            outcomes: default(),
            acked: default(),
        }
    }

    fn new_call(&mut self, call: Call) -> CallId {
        let id = self.next_call;
        self.next_call = id.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.calls.insert(id, call);
        id
    }

    // # Routes (laws 1, 8)

    /// Change the routes wanted from the peer (law 1). Removals apply before additions, so
    /// one call can retire a part and start it over. A part is added with the cursor
    /// persisted for it.
    pub(crate) fn update(
        &mut self,
        added_parts: &[(PartKey, Rev)],
        removed_parts: &[PartKey],
        added_objects: &[ObjKey],
        removed_objects: &[ObjKey],
    ) {
        for part in removed_parts {
            let mut cmds = vec![];
            self.lifecycle.remove(part, &mut cmds);
            self.lifecycle_commands(cmds, None);
            if self.parts.remove(part) == Some(true) {
                self.outbox
                    .push(PeerCommand::Unsynced { part: part.clone() });
            }
        }
        for obj in removed_objects {
            if self.objects.remove(obj).is_some() {
                let route = Route::Object {
                    obj_key: obj.clone(),
                };
                self.client.unwant(&route);
                self.lanes.dropped(&route);
                self.jobs.set_object_route(obj.clone(), false);
            }
        }
        for (part, cursor) in added_parts {
            if !self.parts.contains_key(part) {
                self.lifecycle.add(part.clone(), *cursor);
                self.parts.insert(part.clone(), false);
            }
        }
        for obj in added_objects {
            if !self.objects.contains_key(obj) {
                self.objects.insert(obj.clone(), 0);
                self.client.want(Route::Object {
                    obj_key: obj.clone(),
                });
            }
        }
    }

    /// Law 8: a retired part leaves every sub-machine in this step.
    fn retire(&mut self, part: PartKey) {
        self.cursor.remove_part(part.clone());
        self.jobs.retire_part(&part);
        if let Some(walk) = self.walks.remove(&part) {
            for call in walk.calls {
                self.calls.remove(&call);
            }
        }
        self.walk_retry.retain(|(walk_part, _)| *walk_part != part);
        let route = Route::Part {
            part_key: part.clone(),
        };
        self.client.unwant(&route);
        self.lanes.dropped(&route);
        self.drained.remove(&part);
        self.touched.insert(part);
    }

    /// Route what the lifecycle decided. `advertised` is the call a decision answers: a walk
    /// lists changes since the cursor that call advertised.
    fn lifecycle_commands(
        &mut self,
        cmds: Vec<LifecycleCommand>,
        advertised: Option<&Map<PartKey, Rev>>,
    ) {
        for cmd in cmds {
            match cmd {
                LifecycleCommand::Decide { call, parts } => {
                    let id = self.new_call(Call::Summary {
                        sub: call,
                        parts: parts.clone(),
                    });
                    self.outbox.push(PeerCommand::Summary { call: id, parts });
                }
                LifecycleCommand::StartCursor { part, .. } => {
                    self.client.want(Route::Part {
                        part_key: part.clone(),
                    });
                    self.touched.insert(part);
                }
                LifecycleCommand::StartWalk { part, strat } => {
                    let PeerPartStrat::BucketWalk {
                        remote_depth,
                        remote_len,
                        ..
                    } = strat
                    else {
                        unreachable!("a walk starts on a bucket decision");
                    };
                    let since = advertised.expect("only an answer starts a walk")[&part];
                    self.client.want(Route::Part {
                        part_key: part.clone(),
                    });
                    let mut machine =
                        BucketMachine::new(part.clone(), remote_depth, remote_len, since);
                    let mut walk_cmds = vec![];
                    machine.pull(0, &mut walk_cmds);
                    self.walks.insert(
                        part.clone(),
                        Walk {
                            machine,
                            calls: default(),
                        },
                    );
                    self.walk_commands(&part, walk_cmds);
                    self.touched.insert(part);
                }
                LifecycleCommand::PersistCursor { part, cursor } => {
                    self.outbox
                        .push(PeerCommand::PersistCursor { part, cursor });
                }
                LifecycleCommand::Retire { part } => self.retire(part),
            }
        }
    }

    // # Calls in: the decision, the session, rounds

    pub(crate) fn summary_replied(&mut self, call: CallId, reply: SummaryReply) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call::Summary { sub, parts } = entry else {
            panic!("call {call} is not a summary call: {entry:?}");
        };
        let outcome = match reply {
            SummaryReply::Summaries(summaries) => {
                let asked: Set<PartKey> = parts.keys().cloned().collect();
                DecideOutcome::Answered(decide_peer_strat(
                    &asked,
                    &summaries,
                    &parts,
                    &default(),
                    self.config.strat,
                ))
            }
            SummaryReply::TooManyParts { cap } => DecideOutcome::TooManyParts { cap },
            SummaryReply::Lost => DecideOutcome::Failed,
        };
        let mut cmds = vec![];
        self.lifecycle.decided(sub, outcome, &mut cmds);
        self.lifecycle_commands(cmds, Some(&parts));
    }

    pub(crate) fn update_replied(
        &mut self,
        call: CallId,
        reply: Reply<Result<SessionUpdated, SessionError>>,
    ) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call::Update { request } = entry else {
            panic!("call {call} is not a session update: {entry:?}");
        };
        let answer = match reply {
            Reply::Lost => {
                self.client.update_lost(&request);
                self.update_pacer.fail(&self.config);
                return;
            }
            Reply::Answer(answer) => answer,
        };
        let mut acked = std::mem::take(&mut self.acked);
        match self.client.update_answered(&request, &answer, &mut acked) {
            // An answer no correct peer gives is a failed update: resent after backoff.
            Err(err) => {
                tracing::warn!(?err, "the peer answered a session update wrongly");
                self.client.update_lost(&request);
                self.update_pacer.fail(&self.config);
            }
            Ok(UpdateOutcome::Applied { blocked }) => {
                self.update_pacer.succeed();
                let acked_any = !acked.is_empty();
                for route in acked.drain(..) {
                    if let Route::Part { part_key } = &route {
                        self.touched.insert(part_key.clone());
                    }
                    self.lanes.acked(route);
                }
                // The ladder of re-adding refused routes resets once an update carrying
                // routes leaves none blocked; a removal-only update says nothing about it.
                if blocked > 0 {
                    self.blocked_pacer.fail(&self.config);
                } else if acked_any && self.client.blocked().next().is_none() {
                    self.blocked_pacer.succeed();
                }
            }
            Ok(UpdateOutcome::Stale) => {}
            Ok(UpdateOutcome::Reset) => {
                self.update_pacer.succeed();
                self.session_reset();
            }
            Ok(UpdateOutcome::Limit) => self.update_pacer.fail(&self.config),
        }
        self.acked = acked;
    }

    pub(crate) fn round_replied(
        &mut self,
        call: CallId,
        reply: Reply<Result<SessionPage, SessionError>>,
    ) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call::Round { sub, request } = entry else {
            panic!("call {call} is not a round: {entry:?}");
        };
        let Reply::Answer(answer) = reply else {
            self.lanes.failed(sub);
            return;
        };
        // Law 3: nothing of a violating page applies, not even its verdicts.
        if answer
            .as_ref()
            .is_ok_and(|page| self.page_violates(&request, page))
        {
            tracing::warn!(
                call,
                "the peer sent a page naming targets the round did not ask for"
            );
            self.lanes.failed(sub);
            return;
        }
        let mut outcomes = std::mem::take(&mut self.outcomes);
        match self.client.round_answered(&request, &answer, &mut outcomes) {
            Err(err) => {
                tracing::warn!(?err, "the peer answered a round wrongly");
                self.lanes.failed(sub);
            }
            Ok(RoundOutcome::Stale) => self.lanes.stale(sub),
            Ok(RoundOutcome::Reset) => self.session_reset(),
            Ok(RoundOutcome::Answered) => {
                let Ok(page) = answer else {
                    unreachable!("only a page is answered");
                };
                self.apply_round(&request, page, &outcomes);
                self.lanes.answered(sub, &outcomes);
            }
        }
        self.outcomes = outcomes;
    }

    /// Law 3: a page is a protocol violation when an event names no target, or a target whose
    /// window in this page it falls outside of. A target's window is `(asked cursor, resume]`
    /// of its `Events` verdict (replay law 8); a target the round did not ask for, one with a
    /// `Refused` verdict and one with no verdict have none. An event on a target the client
    /// still holds as an object route may not be a `Removed` (replay law 12). Judged against
    /// the request and the page, never the current routes: a target whose route was dropped
    /// since is legitimate (its events are skipped when applied).
    fn page_violates(&self, request: &PageSession, page: &SessionPage) -> bool {
        let windows: Map<TargetId, (Rev, Rev)> = page
            .verdicts
            .iter()
            .filter_map(|(id, verdict)| match verdict {
                Verdict::Events { resume, .. } => Some((*id, (asked_from(request, *id)?, *resume))),
                Verdict::Refused => None,
            })
            .collect();
        let inside = |id: &TargetId, rev: Rev| {
            windows
                .get(id)
                .is_some_and(|(asked, resume)| *asked < rev && rev <= *resume)
        };
        page.events.iter().any(|event| match event {
            PageEvent::Changed { rev, targets, .. } => {
                targets.is_empty() || targets.iter().any(|id| !inside(id, *rev))
            }
            PageEvent::Removed { rev, target, .. } => {
                !inside(target, *rev)
                    || matches!(self.client.route_of(*target), Some(Route::Object { .. }))
            }
        })
    }

    /// The server lost the session: the client starts a new one and its lanes are empty until
    /// the routes are acknowledged again. Nothing is drained until a round says so.
    fn session_reset(&mut self) {
        self.lanes.reset();
        self.drained.clear();
        self.touched.extend(self.parts.keys().cloned());
    }

    /// Apply an answered round (laws 5, 6). Every target is judged first, against the cursor
    /// it was asked from (replay law 11), because applying the events can move the cursors.
    fn apply_round(
        &mut self,
        request: &PageSession,
        page: SessionPage,
        outcomes: &[(TargetId, Route, TargetOutcome)],
    ) {
        let fresh: Set<TargetId> = outcomes
            .iter()
            .filter(|(id, route, outcome)| {
                matches!(outcome, TargetOutcome::Advance { .. })
                    && asked_from(request, *id)
                        == route_cursor(route, &self.lifecycle, &self.cursor, &self.objects)
            })
            .map(|(id, ..)| *id)
            .collect();
        let mut cursor_cmds = vec![];
        for event in page.events {
            match event {
                PageEvent::Changed {
                    rev,
                    obj_key,
                    targets,
                    payload,
                } => {
                    let mut parts: Vec<PartKey> = vec![];
                    let mut content = false;
                    for id in targets.iter().filter(|id| fresh.contains(id)) {
                        match self.client.route_of(*id) {
                            Some(Route::Part { part_key }) if !parts.contains(part_key) => {
                                parts.push(part_key.clone());
                            }
                            Some(Route::Object { .. }) => content = true,
                            _ => {}
                        }
                    }
                    if content {
                        self.jobs.want_content(obj_key.clone(), payload.clone());
                    }
                    if !parts.is_empty() {
                        self.touched.extend(parts.iter().cloned());
                        self.cursor.on_subscription_evt(
                            CursorEvent::ObjChanged(ObjChanged {
                                cursor: rev,
                                parts,
                                obj_key,
                                payload,
                            }),
                            &mut cursor_cmds,
                        );
                    }
                }
                // An object route carries no removals (replay law 12): only a part route's
                // target has one to apply.
                PageEvent::Removed {
                    rev,
                    obj_key,
                    target,
                } => {
                    if fresh.contains(&target)
                        && let Some(Route::Part { part_key }) = self.client.route_of(target)
                    {
                        let part_key = part_key.clone();
                        self.touched.insert(part_key.clone());
                        self.cursor.on_subscription_evt(
                            CursorEvent::ObjRemovedFromPart(ObjRemovedFromPart {
                                cursor: rev,
                                part_key,
                                obj_key,
                            }),
                            &mut cursor_cmds,
                        );
                    }
                }
            }
        }
        self.cursor_commands(cursor_cmds);
        let mut refused = false;
        for (id, route, outcome) in outcomes {
            match (route, outcome) {
                (Route::Part { part_key }, TargetOutcome::Advance { resume, drained })
                    if fresh.contains(id) =>
                {
                    self.cursor
                        .advance_part_replay_cursor(part_key.clone(), *resume);
                    if *drained {
                        self.drained.insert(part_key.clone(), *resume);
                    } else {
                        self.drained.remove(part_key);
                    }
                    self.touched.insert(part_key.clone());
                }
                (Route::Object { obj_key }, TargetOutcome::Advance { resume, .. })
                    if fresh.contains(id) =>
                {
                    if let Some(cursor) = self.objects.get_mut(obj_key) {
                        *cursor = (*cursor).max(*resume);
                    }
                }
                (route, TargetOutcome::Refused) => {
                    if let Route::Part { part_key } = route {
                        self.drained.remove(part_key);
                        self.touched.insert(part_key.clone());
                    }
                    refused = true;
                }
                // an answer to a round the target has moved on from (replay law 11)
                (_, TargetOutcome::Advance { .. }) => {}
            }
        }
        // the refused routes are blocked: they come back on the ladder, once per round
        if refused {
            self.blocked_pacer.fail(&self.config);
        }
    }

    /// Law 6: the cursor machine's commands become wants, and its cursors go to the lifecycle.
    fn cursor_commands(&mut self, cmds: Vec<CursorMachineCommand>) {
        for cmd in cmds {
            match cmd {
                CursorMachineCommand::SyncObj {
                    obj_key,
                    remote_payload,
                    cursor,
                    parts,
                } => {
                    for part in parts {
                        self.touched.insert(part.clone());
                        self.jobs.want(
                            obj_key.clone(),
                            part,
                            Want::Present {
                                payload: remote_payload.clone(),
                            },
                            Origin::Cursor(cursor),
                        );
                    }
                }
                CursorMachineCommand::RemoveObjFromParts {
                    obj_key,
                    part_key,
                    cursor,
                } => {
                    self.touched.insert(part_key.clone());
                    self.jobs
                        .want(obj_key, part_key, Want::Absent, Origin::Cursor(cursor));
                }
                CursorMachineCommand::SetPartCursor { part_key, cursor } => {
                    self.touched.insert(part_key.clone());
                    let mut cmds = vec![];
                    self.lifecycle.applied(&part_key, cursor, &mut cmds);
                    self.lifecycle_commands(cmds, None);
                }
            }
        }
    }

    // # Calls in: backend work

    pub(crate) fn record_ended(&mut self, call: CallId, outcome: Outcome<Settle>) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call::Record { sub, parts } = entry else {
            panic!("call {call} is not a record: {entry:?}");
        };
        self.job_calls.remove(&sub);
        self.touched.extend(parts);
        self.jobs.record_ended(sub, outcome);
    }

    pub(crate) fn fetch_ended(&mut self, call: CallId, outcome: Outcome<()>) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call::Fetch { sub, parts } = entry else {
            panic!("call {call} is not a fetch: {entry:?}");
        };
        self.job_calls.remove(&sub);
        self.touched.extend(parts);
        self.jobs.fetch_ended(sub, outcome);
    }

    // # Walks (laws 9, 10, 11)

    pub(crate) fn list_replied(&mut self, call: CallId, reply: Reply<Vec<BucketSummary>>) {
        let Some((part, request)) = self.take_walk_call(call) else {
            return;
        };
        let WalkRequest::List {
            offset,
            since,
            working_level,
        } = request
        else {
            panic!("call {call} is not a list call");
        };
        match reply {
            Reply::Answer(listing) if listing_is_sound(&listing, offset, since, working_level) => {
                self.walk_pacer.succeed();
                let walk = self.walks.get_mut(&part).expect(ERROR_IMPOSSIBLE);
                let mut cmds = vec![];
                walk.machine.on_bucket_page(listing, &mut cmds);
                self.walk_commands(&part, cmds);
            }
            // a listing the machine cannot take is a failed call, like a lost one
            Reply::Answer(_) | Reply::Lost => self.walk_failed(
                part,
                WalkRequest::List {
                    offset,
                    since,
                    working_level,
                },
            ),
        }
    }

    pub(crate) fn leaf_replied(
        &mut self,
        call: CallId,
        reply: Reply<Map<BuckId, BucketObjLeafPage>>,
    ) {
        let Some((part, request)) = self.take_walk_call(call) else {
            return;
        };
        let WalkRequest::Leaf { since, buckets } = request else {
            panic!("call {call} is not a leaf call");
        };
        match reply {
            Reply::Answer(mut pages) => {
                self.walk_pacer.succeed();
                // A page that does not move its bucket forward makes no progress: the peer
                // could page one bucket for ever. The call failed (law 3).
                let stuck = pages.values().any(|page| {
                    matches!((&page.after, &page.next_after), (Some(after), Some(next)) if next <= after)
                });
                if stuck {
                    return self.walk_failed(part, WalkRequest::Leaf { since, buckets });
                }
                // a page for a bucket this call did not ask for is ignored (law 3)
                pages.retain(|id, _| buckets.iter().any(|asked| asked.buck_id == *id));
                let walk = self.walks.get_mut(&part).expect(ERROR_IMPOSSIBLE);
                let mut cmds = vec![];
                walk.machine.on_obj_page(pages, &mut cmds);
                self.walk_commands(&part, cmds);
            }
            Reply::Lost => self.walk_failed(part, WalkRequest::Leaf { since, buckets }),
        }
    }

    /// The walk call `call` is over: its part and request. `None` when it is not in flight.
    fn take_walk_call(&mut self, call: CallId) -> Option<(PartKey, WalkRequest)> {
        let entry = self.calls.remove(&call)?;
        let Call::Walk { part, request } = entry else {
            panic!("call {call} is not a walk call: {entry:?}");
        };
        let walk = self.walks.get_mut(&part).expect(ERROR_IMPOSSIBLE);
        walk.calls.remove(&call);
        Some((part, request))
    }

    /// Law 11: the call is reissued unchanged after backoff; the walk is not told.
    fn walk_failed(&mut self, part: PartKey, request: WalkRequest) {
        self.walk_retry.push((part, request));
        self.walk_pacer.fail(&self.config);
    }

    fn issue_walk_call(&mut self, part: PartKey, request: WalkRequest) {
        let call = self.new_call(Call::Walk {
            part: part.clone(),
            request: request.clone(),
        });
        self.walks
            .get_mut(&part)
            .expect(ERROR_IMPOSSIBLE)
            .calls
            .insert(call);
        self.outbox.push(match request {
            WalkRequest::List {
                offset,
                since,
                working_level,
            } => PeerCommand::ListBuckets {
                call,
                part_key: part,
                offset,
                since,
                working_level,
            },
            WalkRequest::Leaf { since, buckets } => PeerCommand::LeafBuckets {
                call,
                part_key: part,
                since,
                buckets,
            },
        });
    }

    /// Route what a walk asks for: deltas become bucket-origin wants, requests become calls.
    fn walk_commands(&mut self, part: &PartKey, cmds: Vec<BucketMachineCommand>) {
        self.touched.insert(part.clone());
        for cmd in cmds {
            match cmd {
                BucketMachineCommand::SyncObj {
                    obj_key,
                    remote_payload,
                    part_key,
                } => self.jobs.want(
                    obj_key,
                    part_key,
                    Want::Present {
                        payload: remote_payload.unwrap_or_default(),
                    },
                    Origin::Bucket,
                ),
                BucketMachineCommand::RemoveObjFromParts { obj_key, part_key } => {
                    self.jobs
                        .want(obj_key, part_key, Want::Absent, Origin::Bucket);
                }
                BucketMachineCommand::ListBuckets {
                    part_key,
                    offset,
                    since,
                    working_level,
                } => self.issue_walk_call(
                    part_key,
                    WalkRequest::List {
                        offset,
                        since,
                        working_level,
                    },
                ),
                BucketMachineCommand::LeafBuckets {
                    part_key,
                    since,
                    buckets,
                } => self.issue_walk_call(part_key, WalkRequest::Leaf { since, buckets }),
            }
        }
    }

    /// Law 10: pull deltas from the walks up to what the window has left after the rounds in
    /// flight, each walk taking a fair share of what remains, starting one walk later each
    /// flush.
    fn pull_walks(&mut self) {
        let count = self.walks.len();
        if count == 0 {
            return;
        }
        let mut budget = self
            .jobs
            .room()
            .saturating_sub(self.lanes.in_flight_limit());
        let mut order: Vec<PartKey> = self.walks.keys().cloned().collect();
        order.rotate_left(self.walk_turn % count);
        self.walk_turn = self.walk_turn.wrapping_add(1);
        for (served, part) in order.into_iter().enumerate() {
            let mut cmds = vec![];
            let walk = self.walks.get_mut(&part).expect(ERROR_IMPOSSIBLE);
            walk.machine
                .pull(budget.div_ceil(count - served), &mut cmds);
            budget -= cmds
                .iter()
                .filter(|cmd| {
                    matches!(
                        cmd,
                        BucketMachineCommand::SyncObj { .. }
                            | BucketMachineCommand::RemoveObjFromParts { .. }
                    )
                })
                .count();
            self.walk_commands(&part, cmds);
        }
    }

    /// Law 9: upgrade the walks that are done with nothing outstanding for their part.
    fn upgrade_walks(&mut self) {
        let done: Vec<PartKey> = self
            .walks
            .iter()
            .filter(|(part, walk)| walk.machine.walk_done() && !self.jobs.part_outstanding(part))
            .map(|(part, _)| part.clone())
            .collect();
        for part in done {
            let walk = self.walks.remove(&part).expect(ERROR_IMPOSSIBLE);
            for call in walk.calls {
                self.calls.remove(&call);
            }
            self.walk_retry.retain(|(walk_part, _)| *walk_part != part);
            let mut cmds = vec![];
            self.lifecycle.upgrade(&part, &mut cmds);
            self.lifecycle_commands(cmds, None);
            self.touched.insert(part);
        }
    }

    // # Flush

    /// Issue everything that is due, and emit the sync edges the step caused. The commands of
    /// the inputs since the last flush come out first.
    pub(crate) fn flush(&mut self, now: Instant, out: &mut Vec<PeerCommand>) {
        self.last_flush = Some(now);

        let mut cmds = vec![];
        self.lifecycle.flush(now, &mut cmds);
        self.lifecycle_commands(cmds, None);

        if self.blocked_pacer.open(now) && self.client.blocked().next().is_some() {
            self.client.retry_blocked();
        }
        if self.update_pacer.open(now)
            && let Some(update) = self.client.next_update().cloned()
        {
            let call = self.new_call(Call::Update {
                request: update.clone(),
            });
            self.outbox.push(PeerCommand::Session { call, update });
        }

        self.flush_rounds(now);
        self.pull_walks();
        if !self.walk_retry.is_empty() && self.walk_pacer.open(now) {
            for (part, request) in std::mem::take(&mut self.walk_retry) {
                self.issue_walk_call(part, request);
            }
        }
        self.flush_jobs(now);
        self.upgrade_walks();
        self.emit_edges();

        out.append(&mut self.outbox);
        #[cfg(test)]
        self.assert_facts();
    }

    /// Law 12: a round goes out only into the room `ObjectJobs` has left. While walks are
    /// pulling, rounds take at most half of it, so a long tail or a big backlog elsewhere
    /// cannot hold the whole window against them (the walks get the rest, see `pull_walks`).
    fn flush_rounds(&mut self, now: Instant) {
        let mut cmds = vec![];
        let room = match self.walks.is_empty() {
            true => self.jobs.room(),
            false => self.jobs.room().div_ceil(2),
        };
        self.lanes.flush(
            now,
            &self.client,
            room,
            |route| route_cursor(route, &self.lifecycle, &self.cursor, &self.objects),
            &mut cmds,
        );
        for cmd in cmds {
            let LanesCommand::Round { call, lane, page } = cmd;
            let id = self.new_call(Call::Round {
                sub: call,
                request: page.clone(),
            });
            self.outbox.push(PeerCommand::Round {
                call: id,
                lane,
                page,
            });
        }
    }

    fn flush_jobs(&mut self, now: Instant) {
        let mut cmds = vec![];
        self.jobs.flush(now, &mut cmds);
        for cmd in cmds {
            match cmd {
                JobsCommand::Record { call, job } => {
                    let parts = job.add.iter().chain(&job.remove).cloned().collect();
                    let id = self.new_call(Call::Record { sub: call, parts });
                    self.job_calls.insert(call, id);
                    self.outbox.push(PeerCommand::Record { call: id, job });
                }
                JobsCommand::Fetch { call, job } => {
                    let parts = job.add.iter().chain(&job.remove).cloned().collect();
                    let id = self.new_call(Call::Fetch { sub: call, parts });
                    self.job_calls.insert(call, id);
                    self.outbox.push(PeerCommand::Fetch { call: id, job });
                }
                JobsCommand::Cancel { call } => {
                    self.outbox.push(PeerCommand::Cancel {
                        call: self.job_calls[&call],
                    });
                }
                // Law 7: a cursor settlement goes to the cursor machine only.
                JobsCommand::Settled(Settlement::Cursor { job, cursor }) => {
                    let mut cursor_cmds = vec![];
                    self.cursor
                        .on_obj_sync_job_evt(job, cursor, &mut cursor_cmds);
                    self.cursor_commands(cursor_cmds);
                }
                // Consumed by the upgrade check (law 9), which reads `part_outstanding`.
                JobsCommand::Settled(Settlement::Bucket { part, .. }) => {
                    self.touched.insert(part);
                }
            }
        }
    }

    // # Sync facts (law 13)

    /// `synced(part)` from the sub-machines' answers; nothing here is remembered.
    fn derive_synced(&self, part: &PartKey) -> bool {
        self.lifecycle.phase(part) == Some(Phase::Cursor)
            && self.client.is_acked(&Route::Part {
                part_key: part.clone(),
            })
            && self.drained.get(part).copied()
                == self
                    .lifecycle
                    .cursor(part)
                    .map(|applied| self.cursor.part_replay_cursor(part, applied))
            && self.cursor.is_settled(part)
            && !self.jobs.part_outstanding(part)
    }

    fn emit_edges(&mut self) {
        for part in std::mem::take(&mut self.touched) {
            let Some(last) = self.parts.get(&part).copied() else {
                continue;
            };
            let synced = self.derive_synced(&part);
            if synced != last {
                self.parts.insert(part.clone(), synced);
                self.outbox.push(if synced {
                    PeerCommand::Synced { part }
                } else {
                    PeerCommand::Unsynced { part }
                });
            }
        }
    }

    #[cfg(test)]
    fn assert_facts(&self) {
        for (part, last) in &self.parts {
            assert_eq!(
                self.derive_synced(part),
                *last,
                "law 13: the incremental fact for {part:?} disagrees with the sub-machines"
            );
        }
    }

    /// The last fact emitted for `part`; `None` for a part that is not wanted.
    #[cfg(test)]
    pub(crate) fn synced(&self, part: &PartKey) -> Option<bool> {
        self.parts.get(part).copied()
    }

    #[cfg(test)]
    pub(crate) fn phase(&self, part: &PartKey) -> Option<Phase> {
        self.lifecycle.phase(part)
    }

    #[cfg(test)]
    pub(crate) fn walking(&self, part: &PartKey) -> bool {
        self.walks.contains_key(part)
    }

    /// Calls the driver has not reported yet.
    #[cfg(test)]
    pub(crate) fn calls_in_flight(&self) -> usize {
        self.calls.len()
    }

    /// The earliest deadline after the last flush at which a flush would issue something.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        let last = self.last_flush?;
        [
            self.lifecycle.next_due(),
            self.jobs.next_due(),
            self.lanes.next_due(),
            (!self.client.is_settled())
                .then(|| self.update_pacer.due(last))
                .flatten(),
            (!self.walk_retry.is_empty())
                .then(|| self.walk_pacer.due(last))
                .flatten(),
            self.client
                .blocked()
                .next()
                .and_then(|_| self.blocked_pacer.due(last)),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Law 14: the connection is over. Every call in flight is cancelled, nothing else is
    /// reported, and nothing but the cursors already handed out for persisting survives.
    pub(crate) fn disconnect(mut self, out: &mut Vec<PeerCommand>) {
        let mut cmds = vec![];
        self.jobs.clear(&mut cmds);
        for cmd in cmds {
            if let JobsCommand::Cancel { call } = cmd {
                out.push(PeerCommand::Cancel {
                    call: self.job_calls[&call],
                });
            }
        }
        for (id, call) in &self.calls {
            if !matches!(call, Call::Record { .. } | Call::Fetch { .. }) {
                out.push(PeerCommand::Cancel { call: *id });
            }
        }
    }
}
