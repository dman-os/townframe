//! The server half of the replay session protocol: a registry of the sessions each client
//! principal holds, and the resolution of one round into one authorized read of the part store.
//!
//! The machine is sans-IO and store-free. Admission (law 4) is a decision the caller supplies
//! per addition; a round resolves into a [`Round`] whose routes the responder hands to
//! [`PartStoreServe::read_authorized`](crate::part_store::PartStoreServe::read_authorized), and
//! [`Round::answer`] maps that read back to the wire. Time is the caller's `now`.

use std::cmp::Ordering;
use std::num::NonZeroUsize;

use crate::interlude::*;

use crate::kit::scheduler::Instant;
use crate::part_store::AuthorizedPage;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogRoutes;
use crate::part_store::ObjPayload;
use crate::part_store::ReadVerdict;
use crate::replay::wire::CloseSession;
use crate::replay::wire::Generation;
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
use crate::rev::Limits;
use crate::rev::Rev;

#[cfg(test)]
mod tests;

/// The most targets one session may hold (law 14). An update that would leave the session
/// holding more is refused whole with [`SessionError::Limit`]. The count is taken before
/// admission, so whether an update hits the cap never depends on what the client may read.
pub const MAX_TARGETS_PER_SESSION: usize = 16_384;

/// The most sessions one client principal may hold at once (law 14). A client keeps one per
/// peer; the slack covers sessions it abandoned that the TTL has not swept yet. An opening update
/// past the cap is refused with [`SessionError::Limit`].
pub const MAX_SESSIONS_PER_CLIENT: usize = 8;

/// The largest page `limit` a round may ask for (law 14), in log rows. A larger one is capped
/// silently, and `0` is raised to `1`: a read always makes progress by at least one revision.
pub const MAX_PAGE_LIMIT: u32 = 1024;

/// The longest a round may wait for rows (law 13, 14). A longer `hold_ms` is capped silently.
pub const MAX_HOLD: Duration = Duration::from_secs(15);

/// How long a session may go without a request naming it before [`ReplayServer::sweep`] forgets
/// it (law 5). Every update and round naming the session touches it, stale ones included.
pub const SESSION_TTL: Duration = Duration::from_secs(10 * 60);

strike! {
    #[structstruck::each[derive(Debug)]]
    /// The sessions of every client principal. Sessions are keyed by principal first, so one
    /// principal can never name, touch or count against another's sessions.
    pub struct ReplayServer {
        clients: Map<PeerKey, Map<SessionId, pub(crate) struct Session {
            /// The generation of the last update applied.
            pub(crate) generation: Generation,
            pub(crate) targets: Map<TargetId, Route>,
            /// The additions `generation` did not take. A repeat of that generation (a retry
            /// whose answer was lost) reads them again, as law 2 requires.
            pub(crate) refused: Vec<TargetId>,
            last_touched: Instant,
        }>>,
        /// Scratch for spotting an id named twice in one request.
        id_buf: Set<TargetId>,
    }
}

impl Default for ReplayServer {
    fn default() -> Self {
        Self {
            clients: default(),
            id_buf: default(),
        }
    }
}

impl Session {
    fn answer(&self) -> SessionUpdated {
        SessionUpdated {
            generation: self.generation,
            refused: self.refused.clone(),
        }
    }
}

impl ReplayServer {
    /// Apply one [`UpdateSession`] from `principal` (laws 1–5, 14). `admits` answers, for one
    /// addition, whether its route is known to the server and readable by `principal` now; it is
    /// asked only for the additions of an update that applies.
    ///
    /// A request that fails ([`SessionError`]) changes nothing but the session's TTL.
    pub fn update(
        &mut self,
        principal: &PeerKey,
        update: &UpdateSession,
        now: Instant,
        mut admits: impl FnMut(&Route) -> bool,
    ) -> Result<SessionUpdated, SessionError> {
        // The ids are the client's own, so naming one twice in a request is its bug, not a
        // target the server has to adjudicate. This is checked before idempotence: a request
        // that is malformed is malformed whether or not it is a repeat.
        if has_duplicate(&mut self.id_buf, update.additions.iter().map(|(id, _)| *id)) {
            return Err(SessionError::Invalid("an update adds one id twice".into()));
        }
        if has_duplicate(&mut self.id_buf, update.removals.iter().copied()) {
            return Err(SessionError::Invalid(
                "an update removes one id twice".into(),
            ));
        }
        let Some(session) = self
            .clients
            .get_mut(principal)
            .and_then(|sessions| sessions.get_mut(&update.session_id))
        else {
            return self.open(principal, update, now, admits);
        };
        session.last_touched = now;
        // Law 2: a generation the session already holds (or passed) is a repeat or a stale
        // copy. It applies nothing and reads back the held generation's outcome.
        if update.generation <= session.generation {
            return Ok(session.answer());
        }
        // Law 3: removals apply before additions, and removing an unheld id is a no-op. Both
        // checks below run against the set as it will be after the removals, before anything
        // is touched, so a refused request leaves the session as it was.
        let removed = update
            .removals
            .iter()
            .filter(|id| session.targets.contains_key(id))
            .count();
        // `id_buf` holds exactly the removals here: the duplicate check above inserted every one
        // of them and found none twice. Probing it keeps this linear in the update's size.
        let removals = &self.id_buf;
        let reused = update
            .additions
            .iter()
            .any(|(id, _)| session.targets.contains_key(id) && !removals.contains(id));
        if reused {
            return Err(SessionError::Invalid(
                "an update adds an id the session holds".into(),
            ));
        }
        if session.targets.len() - removed + update.additions.len() > MAX_TARGETS_PER_SESSION {
            return Err(SessionError::Limit);
        }
        for id in &update.removals {
            session.targets.remove(id);
        }
        session.generation = update.generation;
        session.refused.clear();
        admit_into(session, &update.additions, &mut admits);
        Ok(session.answer())
    }

    /// Law 1: only generation 0 opens a session, with its additions as the whole target set
    /// (its removals have nothing to remove).
    fn open(
        &mut self,
        principal: &PeerKey,
        update: &UpdateSession,
        now: Instant,
        mut admits: impl FnMut(&Route) -> bool,
    ) -> Result<SessionUpdated, SessionError> {
        if update.generation != 0 {
            return Err(SessionError::UnknownSession);
        }
        let held = self.clients.get(principal).map_or(0, Map::len);
        if held >= MAX_SESSIONS_PER_CLIENT || update.additions.len() > MAX_TARGETS_PER_SESSION {
            return Err(SessionError::Limit);
        }
        let mut session = Session {
            generation: 0,
            targets: default(),
            refused: default(),
            last_touched: now,
        };
        admit_into(&mut session, &update.additions, &mut admits);
        let answer = session.answer();
        self.clients
            .entry(principal.clone())
            .or_default()
            .insert(update.session_id, session);
        Ok(answer)
    }

    /// Forget a session (law 5). Closing a session the server does not hold is a no-op: the
    /// outcome the client asked for already holds.
    pub fn close(&mut self, principal: &PeerKey, close: &CloseSession) {
        let Some(sessions) = self.clients.get_mut(principal) else {
            return;
        };
        sessions.remove(&close.session_id);
        if sessions.is_empty() {
            self.clients.remove(principal);
        }
    }

    /// Forget every session no request has named for [`SESSION_TTL`] (law 5).
    pub fn sweep(&mut self, now: Instant) {
        self.clients.retain(|_, sessions| {
            sessions.retain(|_, session| now.duration_since(session.last_touched) < SESSION_TTL);
            !sessions.is_empty()
        });
    }

    /// Resolve one round of `principal`'s into `round` (law 6): every named id to its route,
    /// with its cursor. The responder then reads [`Round::routes`] and answers with
    /// [`Round::answer`].
    ///
    /// Naming a session the server does not hold is [`SessionError::UnknownSession`]; naming an
    /// id the session does not hold is [`SessionError::UnknownTarget`]. A round that names no
    /// target, one id twice, or one route under two ids is [`SessionError::Invalid`]: a single
    /// read carries one bound per route, and two cursors on one route cannot both be honoured
    /// by it (see [`Round::routes`]).
    pub fn resolve_round(
        &mut self,
        principal: &PeerKey,
        request: &PageSession,
        now: Instant,
        round: &mut Round,
    ) -> Result<(), SessionError> {
        let session = self
            .clients
            .get_mut(principal)
            .and_then(|sessions| sessions.get_mut(&request.session_id))
            .ok_or(SessionError::UnknownSession)?;
        session.last_touched = now;
        if request.targets.is_empty() {
            return Err(SessionError::Invalid("a round names no target".into()));
        }
        if has_duplicate(&mut self.id_buf, request.targets.iter().map(|(id, _)| *id)) {
            return Err(SessionError::Invalid("a round names one id twice".into()));
        }
        round.targets.clear();
        for &(id, cursor) in &request.targets {
            let route = session
                .targets
                .get(&id)
                .ok_or(SessionError::UnknownTarget { target: id })?;
            round.targets.push(RoundTarget {
                id,
                route: route.clone(),
                cursor,
                slot: 0,
            });
        }
        round.index()?;
        round.limit = request.limit.clamp(1, MAX_PAGE_LIMIT);
        round.hold = Duration::from_millis(request.hold_ms.into()).min(MAX_HOLD);
        round.linger = Duration::from_millis(request.linger_ms.unwrap_or(0).into());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn session(&self, principal: &PeerKey, session_id: &SessionId) -> Option<&Session> {
        self.clients.get(principal)?.get(session_id)
    }
}

/// Law 4: an addition is held only when `admits` says its route is known and readable now;
/// every other one is refused, and a refusal says nothing about which of the two failed.
fn admit_into(
    session: &mut Session,
    additions: &[(TargetId, Route)],
    admits: &mut impl FnMut(&Route) -> bool,
) {
    for (id, route) in additions {
        if admits(route) {
            session.targets.insert(*id, route.clone());
        } else {
            session.refused.push(*id);
        }
    }
}

fn has_duplicate(buf: &mut Set<TargetId>, ids: impl Iterator<Item = TargetId>) -> bool {
    buf.clear();
    let mut ids = ids;
    ids.any(|id| !buf.insert(id))
}

strike! {
    #[structstruck::each[derive(Debug)]]
    /// One resolved round: what the responder reads and how the read maps back to the wire.
    /// The responder owns it and reuses it across rounds; resolving refills every field.
    ///
    /// The responder's sequence ([`crate::replay::responder`]), all for the round's
    /// `principal`:
    ///
    /// 1. pick `until`, a committed rev of the store, and read from [`Round::after`] capped at
    ///    `until` (a window that may be empty);
    /// 2. [`read_authorized`](crate::part_store::PartStoreServe::read_authorized) of
    ///    [`Round::routes`] in `(after, until]` under [`Round::limits`], which carries the
    ///    rows' payloads from the same snapshot;
    /// 3. [`Round::answer`]. When the read carries no row and [`Round::hold`] is not zero, the
    ///    responder may wait for a new rev and go again from step 1 (law 13).
    pub struct Round {
        /// In the order the request named them.
        targets: Vec<struct RoundTarget {
            id: TargetId,
            route: Route,
            cursor: Rev,
            /// The route's index in `routes.parts` or `routes.objects`, which is also its
            /// index in the read's verdicts.
            slot: usize,
        }>,
        /// Indices into `targets`, ordered by route: parts first, then objects, each in key
        /// order, which is [`LogRoutes`]' own order.
        by_route: Vec<usize>,
        routes: LogRoutes,
        after: Rev,
        limit: u32,
        hold: Duration,
        /// The client's linger hint, as asked (law 13); the responder caps it.
        linger: Duration,
        /// Scratch for [`Round::answer`]: per payload of the read, the event its payload
        /// moved into, so the events before it clone from there.
        moved_into: Vec<Option<usize>>,
    }
}

impl Default for Round {
    fn default() -> Self {
        Self {
            targets: default(),
            by_route: default(),
            routes: LogRoutes {
                parts: default(),
                objects: default(),
            },
            after: 0,
            limit: 1,
            hold: Duration::ZERO,
            linger: Duration::ZERO,
            moved_into: default(),
        }
    }
}

/// A route without its owned key, for searching the round's targets by a log row's key.
#[derive(Clone, Copy)]
enum RouteProbe<'a> {
    Part(&'a PartKey),
    Object(&'a ObjKey),
}

impl RouteProbe<'_> {
    /// `route`'s place relative to the probe, in [`Route`]'s derived order (parts first).
    fn order_of(self, route: &Route) -> Ordering {
        match (route, self) {
            (Route::Part { part_key }, RouteProbe::Part(probe)) => part_key.cmp(probe),
            (Route::Object { obj_key }, RouteProbe::Object(probe)) => obj_key.cmp(probe),
            (Route::Part { .. }, RouteProbe::Object(_)) => Ordering::Less,
            (Route::Object { .. }, RouteProbe::Part(_)) => Ordering::Greater,
        }
    }
}

impl Round {
    /// The routes to read, each bounded by its target's cursor. Rows of a route at or below its
    /// bound are not selected; removals above it are always delivered (law 11). A round may not
    /// name one route twice: the store keeps one bound per route.
    pub fn routes(&self) -> &LogRoutes {
        &self.routes
    }

    /// The read's lower end: the lowest cursor of the round.
    pub fn after(&self) -> Rev {
        self.after
    }

    /// The page `limit`, capped (law 14). The page's bytes are not bounded here: a page is
    /// bounded in rows, and payloads ride beside the rows.
    pub fn limits(&self) -> Limits {
        Limits {
            max_entries: NonZeroUsize::new(self.limit as usize).expect(ERROR_IMPOSSIBLE),
            max_bytes: None,
        }
    }

    /// How long the round may wait for rows, capped (laws 13, 14).
    pub fn hold(&self) -> Duration {
        self.hold
    }

    /// How long the client asks the round to linger after its first row, as asked: the
    /// responder caps it (law 13). Zero when the request carried no hint.
    pub fn linger(&self) -> Duration {
        self.linger
    }

    fn index(&mut self) -> Result<(), SessionError> {
        let Self {
            targets,
            by_route,
            routes,
            after,
            ..
        } = self;
        by_route.clear();
        by_route.extend(0..targets.len());
        by_route.sort_unstable_by(|left, right| targets[*left].route.cmp(&targets[*right].route));
        if by_route
            .windows(2)
            .any(|pair| targets[pair[0]].route == targets[pair[1]].route)
        {
            return Err(SessionError::Invalid(
                "a round names one route under two ids".into(),
            ));
        }
        routes.parts.clear();
        routes.objects.clear();
        *after = Rev::MAX;
        for &ix in by_route.iter() {
            let target = &mut targets[ix];
            *after = (*after).min(target.cursor);
            match &target.route {
                Route::Part { part_key } => {
                    target.slot = routes.parts.len();
                    routes.parts.insert(part_key.clone(), target.cursor);
                }
                Route::Object { obj_key } => {
                    target.slot = routes.objects.len();
                    routes.objects.insert(obj_key.clone(), target.cursor);
                }
            }
        }
        Ok(())
    }

    fn targets_on(&self, probe: RouteProbe<'_>) -> impl Iterator<Item = &RoundTarget> {
        let start = self
            .by_route
            .partition_point(|ix| probe.order_of(&self.targets[*ix].route) == Ordering::Less);
        self.by_route[start..]
            .iter()
            .map(|ix| &self.targets[*ix])
            .take_while(move |target| probe.order_of(&target.route) == Ordering::Equal)
    }

    fn verdict(&self, read: &AuthorizedPage, target: &RoundTarget) -> ReadVerdict {
        match target.route {
            Route::Part { .. } => read.parts[target.slot],
            Route::Object { .. } => read.objects[target.slot],
        }
    }

    /// Map the authorized read of this round to its page, into `out` (cleared first).
    ///
    /// `until` is the read's upper end. The read is taken apart: its payloads move into the
    /// page's events.
    ///
    /// - Verdicts (law 7) are the read's: a permitted route is `Events`, any other `Refused`.
    /// - A live row goes to the permitted part target of its part and the permitted object
    ///   target of its object whose cursor is below the row; one object's live rows at one rev
    ///   are one [`PageEvent::Changed`]. A removal goes to part targets only (law 12).
    /// - `resume` is the read's progress, never below the target's cursor (law 8), and
    ///   `drained` says it reached `until` (law 10).
    ///
    /// The content is the read's own ([`AuthorizedPage::payloads`]), from the snapshot that
    /// decided the rows and verdicts, so every live row has it and none can leak past access.
    pub fn answer(&mut self, read: &mut AuthorizedPage, until: Rev, out: &mut SessionPage) {
        assert_eq!(read.parts.len(), self.routes.parts.len());
        assert_eq!(read.objects.len(), self.routes.objects.len());
        out.events.clear();
        out.verdicts.clear();
        for revision in read.rows.revisions() {
            self.map_revision(read, revision, out);
        }
        self.fill_payloads(read, out);
        let through = read.rows.through;
        out.verdicts.extend(self.targets.iter().map(|target| {
            let verdict = match self.verdict(read, target) {
                ReadVerdict::Permitted => {
                    let resume = target.cursor.max(through);
                    Verdict::Events {
                        resume,
                        drained: resume >= until,
                    }
                }
                ReadVerdict::Denied | ReadVerdict::UnknownPart => Verdict::Refused,
            };
            (target.id, verdict)
        }));
    }

    /// Push one revision's events, each [`PageEvent::Changed`] with a placeholder payload that
    /// [`Round::fill_payloads`] replaces.
    fn map_revision(
        &self,
        read: &AuthorizedPage,
        revision: &[crate::rev::Entry<LogKey, LogEntry>],
        out: &mut SessionPage,
    ) {
        let rev = revision[0].rev;
        let carries = |target: &&RoundTarget| {
            target.cursor < rev && self.verdict(read, target) == ReadVerdict::Permitted
        };
        // Member keys sort object first, so one object's rows of a revision are contiguous.
        for rows in revision.chunk_by(|left, right| member_obj(&left.key) == member_obj(&right.key))
        {
            let obj = member_obj(&rows[0].key);
            let mut changed = Vec::new();
            for row in rows {
                let LogKey::Member { part, .. } = &row.key else {
                    unreachable!("an authorized read carries no access row")
                };
                let on_part = self.targets_on(RouteProbe::Part(part)).filter(carries);
                match row.value {
                    Some(_) => changed.extend(on_part.map(|target| target.id)),
                    None => out.events.extend(on_part.map(|target| PageEvent::Removed {
                        rev,
                        obj_key: obj.clone(),
                        target: target.id,
                    })),
                }
            }
            if rows.iter().any(|row| row.value.is_some()) {
                changed.extend(
                    self.targets_on(RouteProbe::Object(obj))
                        .filter(carries)
                        .map(|target| target.id),
                );
            }
            if !changed.is_empty() {
                out.events.push(PageEvent::Changed {
                    rev,
                    obj_key: obj.clone(),
                    targets: changed,
                    payload: ObjPayload::Null,
                });
            }
        }
    }

    /// Give every [`PageEvent::Changed`] its object's payload. The walk goes from the last event
    /// back: an object's last event takes the payload out of the read, and its earlier events
    /// (the object's rows at other revs of the page) clone from that event. So a payload is
    /// moved, not copied, unless its object has events at more than one rev.
    fn fill_payloads(&mut self, read: &mut AuthorizedPage, out: &mut SessionPage) {
        self.moved_into.clear();
        self.moved_into.resize(read.payloads.len(), None);
        for ix in (0..out.events.len()).rev() {
            let PageEvent::Changed { obj_key, .. } = &out.events[ix] else {
                continue;
            };
            let slot = read.payload_slot(obj_key);
            let filled = match self.moved_into[slot] {
                None => {
                    self.moved_into[slot] = Some(ix);
                    std::mem::take(&mut read.payloads[slot].1)
                }
                Some(later) => match &out.events[later] {
                    PageEvent::Changed { payload, .. } => payload.clone(),
                    PageEvent::Removed { .. } => unreachable!("a payload moved into a removal"),
                },
            };
            let PageEvent::Changed { payload, .. } = &mut out.events[ix] else {
                unreachable!("matched as a change above")
            };
            *payload = filled;
        }
    }
}

fn member_obj(key: &LogKey) -> &ObjKey {
    match key {
        LogKey::Member { obj, .. } => obj,
        LogKey::Access { .. } => unreachable!("an authorized read carries no access row"),
    }
}
