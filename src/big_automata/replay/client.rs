//! The client half of the replay session protocol: one session with one peer, kept in step with
//! the routes the embedder wants from that peer.
//!
//! The machine is sans-IO. The embedder wants and unwants routes one at a time; the driver takes the next
//! [`UpdateSession`] to send and reports its answer (or its loss), builds rounds from the
//! acknowledged targets with the cursors the cursor machine owns, and reports their answers.
//! Each answer comes back paired with the request it answers, because answers carry no session
//! id: the pairing is what tells a stale answer from a current one.
//!
//! One update is in flight at a time. A generation therefore names exactly one update's content
//! for the life of a session, which is what lets the client trust law 2: an answer for the
//! generation in flight describes that update, whether it is the first answer or a retry's.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::interlude::*;

use crate::replay::wire::Generation;
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

#[cfg(test)]
mod differential;
#[cfg(test)]
mod tests;

strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct ReplayClient {
        session_id: SessionId,
        /// The generation the next new update carries.
        generation: Generation,
        next_target: TargetId,
        /// Every wanted route and where it stands with the server. The other maps index it.
        routes: BTreeMap<Route, enum Standing {
            #![derive(Clone, Copy, PartialEq, Eq)]
            /// Waiting for the next update, under this id.
            Pending(TargetId),
            /// Carried by the update in flight under this id.
            InFlight(TargetId),
            /// Held by the server under this id; rounds may name it.
            Acked(TargetId),
            /// Refused, by an update or a round; re-added by [`ReplayClient::retry_blocked`].
            Blocked,
        }>,
        /// The pending additions, by id: the next update's additions in id order.
        pending: BTreeMap<TargetId, Route>,
        /// The acknowledged targets, by id: what rounds name and answers resolve.
        acked: BTreeMap<TargetId, Route>,
        blocked: BTreeSet<Route>,
        /// Ids the server may hold that the client no longer wants: the next update's removals.
        removals: BTreeSet<TargetId>,
        in_flight: Option<struct InFlight {
            update: UpdateSession,
            /// Whether the driver has it. A reported loss clears it, so the same update is
            /// handed out again.
            sent: bool,
        }>,
        /// Scratch for checking a page's verdicts against its request.
        round_buf: Map<TargetId, (Rev, bool)>,
        refused_buf: Set<TargetId>,
    }
}

/// What an answer to an update did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// The update applied; `blocked` of its additions were refused and are now blocked. The
    /// driver arms its backoff for [`ReplayClient::retry_blocked`] when it is not zero.
    Applied { blocked: usize },
    /// The answer is for a request that is no longer in flight; nothing changed.
    Stale,
    /// The server lost the session (or holds one this client cannot have made): the client
    /// moved to a fresh session that re-states the whole wanted set.
    Reset,
    /// The server refused the update whole for a limit. Its additions may still land from a
    /// late copy, so they are queued for removal and re-queued under fresh ids; the driver
    /// backs off before sending again.
    Limit,
}

/// What a round's answer did, as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundOutcome {
    /// The page was read; per-target outcomes are in the caller's buffer.
    Answered,
    /// The answer is for an earlier session, or names a target the client has dropped since;
    /// the caller repeats the round from the same cursors (law 9).
    Stale,
    /// The server lost the session, or a target the client holds: the client moved to a fresh
    /// session, and rounds resume once its update is acknowledged.
    Reset,
}

/// What a round answered for one target, for the caller (the cursor machine and the pacer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetOutcome {
    /// Resume the target from `resume` (law 8). `drained` is a pacing hint (law 10).
    Advance { resume: Rev, drained: bool },
    /// The client may not read the route now. The route is blocked and stays wanted; its
    /// cursor stays where it was (law 7).
    Refused,
}

impl ReplayClient {
    /// A client with no wanted routes, on session `session_id`. The id has to be one this
    /// client has never used with the peer: a fresh random one.
    pub fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            generation: 0,
            next_target: 0,
            routes: default(),
            pending: default(),
            acked: default(),
            blocked: default(),
            removals: default(),
            in_flight: None,
            round_buf: default(),
            refused_buf: default(),
        }
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// The acknowledged targets: the ids a round may name, and the routes they follow.
    pub fn acked(&self) -> impl Iterator<Item = (TargetId, &Route)> {
        self.acked.iter().map(|(id, route)| (*id, route))
    }

    /// The route an acknowledged target follows; `None` once the client dropped the id, so a
    /// page's events for it are dropped too.
    pub fn route_of(&self, id: TargetId) -> Option<&Route> {
        self.acked.get(&id)
    }

    /// Whether the server holds `route` (acknowledged, not merely wanted).
    pub fn is_acked(&self, route: &Route) -> bool {
        matches!(self.routes.get(route), Some(Standing::Acked(_)))
    }

    /// The refused routes waiting for [`ReplayClient::retry_blocked`].
    pub fn blocked(&self) -> impl Iterator<Item = &Route> {
        self.blocked.iter()
    }

    /// Whether the server holds exactly what the client wants, as far as the client can tell:
    /// nothing pending, nothing to remove, no update in flight. Blocked routes do not count.
    pub fn is_settled(&self) -> bool {
        self.pending.is_empty() && self.removals.is_empty() && self.in_flight.is_none()
    }

    fn fresh_id(next_target: &mut TargetId) -> TargetId {
        let id = *next_target;
        *next_target = next_target.checked_add(1).expect(ERROR_IMPOSSIBLE);
        id
    }

    /// Start following `route`: it is queued for the next update under a fresh id. Wanting a
    /// route that is already wanted, in whatever standing, changes nothing: it keeps its id and
    /// its standing, and no second id is made for it.
    pub fn want(&mut self, route: Route) {
        if self.routes.contains_key(&route) {
            return;
        }
        let id = Self::fresh_id(&mut self.next_target);
        self.routes.insert(route.clone(), Standing::Pending(id));
        self.pending.insert(id, route);
    }

    /// Stop following `route`: it leaves every index, and an id the server may hold for it
    /// (acknowledged, or carried by the update in flight) is queued for removal. Unwanting a
    /// route that is not wanted changes nothing. A route wanted again later gets a fresh id,
    /// never the dropped one. Only this drops a route: the server's refusals block one, which stays
    /// wanted (law 4).
    pub fn unwant(&mut self, route: &Route) {
        let Some(standing) = self.routes.remove(route) else {
            return;
        };
        match standing {
            Standing::Pending(id) => {
                self.pending.remove(&id);
            }
            // The update in flight may land: removing the id next is what keeps the server
            // from serving a route nobody wants. The next update goes out only after this one
            // is answered, so the removal applies after the addition.
            Standing::InFlight(id) => {
                self.removals.insert(id);
            }
            Standing::Acked(id) => {
                self.acked.remove(&id);
                self.removals.insert(id);
            }
            Standing::Blocked => {
                self.blocked.remove(route);
            }
        }
    }

    /// Re-add every blocked route under a fresh id (law 4). The driver calls this when the
    /// backoff it armed for a refusal runs out.
    pub fn retry_blocked(&mut self) {
        for route in std::mem::take(&mut self.blocked) {
            let id = Self::fresh_id(&mut self.next_target);
            *self.routes.get_mut(&route).expect(ERROR_IMPOSSIBLE) = Standing::Pending(id);
            self.pending.insert(id, route);
        }
    }

    /// The update to send now, if any: the one in flight again after a reported loss, or a new
    /// one carrying everything pending once nothing is in flight.
    pub fn next_update(&mut self) -> Option<&UpdateSession> {
        let resend = match &self.in_flight {
            Some(in_flight) if in_flight.sent => return None,
            Some(_) => true,
            None => false,
        };
        if !resend {
            if self.pending.is_empty() && self.removals.is_empty() {
                return None;
            }
            let update = UpdateSession {
                session_id: self.session_id,
                generation: self.generation,
                additions: std::mem::take(&mut self.pending).into_iter().collect(),
                removals: std::mem::take(&mut self.removals).into_iter().collect(),
            };
            for (id, route) in &update.additions {
                *self.routes.get_mut(route).expect(ERROR_IMPOSSIBLE) = Standing::InFlight(*id);
            }
            self.generation = self.generation.checked_add(1).expect(ERROR_IMPOSSIBLE);
            self.in_flight = Some(InFlight {
                update,
                sent: false,
            });
        }
        let in_flight = self.in_flight.as_mut().expect(ERROR_IMPOSSIBLE);
        in_flight.sent = true;
        Some(&in_flight.update)
    }

    /// Whether `request` is the update in flight. A session's generation names one update, so
    /// the pair is the whole identity.
    fn is_in_flight(&self, request: &UpdateSession) -> bool {
        self.in_flight.as_ref().is_some_and(|in_flight| {
            in_flight.update.session_id == request.session_id
                && in_flight.update.generation == request.generation
        })
    }

    /// The transport lost `request` or its answer (law 9 holds for updates too, through law 2):
    /// the same update is handed out again by [`ReplayClient::next_update`].
    pub fn update_lost(&mut self, request: &UpdateSession) {
        if self.is_in_flight(request) {
            self.in_flight.as_mut().expect(ERROR_IMPOSSIBLE).sent = false;
        }
    }

    /// Take the server's answer to `request`.
    ///
    /// `acked` is cleared and then holds the routes this answer acknowledged, the only routes
    /// an update makes acknowledged: the caller tells its lanes about each (an answer that
    /// resets, limits or errs acknowledges none). The routes it blocked are counted in
    /// [`UpdateOutcome::Applied`]; they were never acknowledged, so nothing leaves a lane.
    ///
    /// Errors are answers no correct server gives this client's requests; the driver treats
    /// the peer as faulty.
    pub fn update_answered(
        &mut self,
        request: &UpdateSession,
        answer: &Result<SessionUpdated, SessionError>,
        acked: &mut Vec<Route>,
    ) -> Res<UpdateOutcome> {
        acked.clear();
        if !self.is_in_flight(request) {
            return Ok(UpdateOutcome::Stale);
        }
        match answer {
            Ok(updated) if updated.generation == request.generation => self
                .acknowledge(&updated.refused, acked)
                .map(|blocked| UpdateOutcome::Applied { blocked }),
            // Law 2 answers a repeat with the generation held, which is never below the
            // request's.
            Ok(updated) if updated.generation < request.generation => Err(ferr!(
                "the server answered generation {} with an older one, {}",
                request.generation,
                updated.generation
            )),
            // The server holds a generation this client never sent on this session: a session
            // it cannot share with the server. Starting over is the one way back.
            Ok(_) => {
                self.reset();
                Ok(UpdateOutcome::Reset)
            }
            Err(SessionError::UnknownSession) => {
                self.reset();
                Ok(UpdateOutcome::Reset)
            }
            Err(SessionError::Limit) => {
                self.unwind_in_flight();
                Ok(UpdateOutcome::Limit)
            }
            // A deadline or cancellation is the transport's, not the server's verdict: the
            // update may or may not have applied, so it is resent unchanged, as a lost one is
            // (law 2 makes the repeat safe).
            Err(SessionError::Context(_)) => {
                self.update_lost(request);
                Ok(UpdateOutcome::Stale)
            }
            Err(err @ (SessionError::UnknownTarget { .. } | SessionError::Invalid(_))) => Err(
                ferr!("the server rejected update {}: {err}", request.generation),
            ),
        }
    }

    /// The update in flight applied: its additions are acknowledged, its refusals blocked.
    fn acknowledge(&mut self, refused: &[TargetId], acked: &mut Vec<Route>) -> Res<usize> {
        let InFlight { update, .. } = self.in_flight.take().expect(ERROR_IMPOSSIBLE);
        self.refused_buf.clear();
        self.refused_buf.extend(refused.iter().copied());
        let named = update
            .additions
            .iter()
            .filter(|(id, _)| self.refused_buf.contains(id))
            .count();
        if self.refused_buf.len() != refused.len() || named != refused.len() {
            // Put it back: a faulty answer acknowledges nothing.
            self.in_flight = Some(InFlight { update, sent: true });
            return Err(ferr!("the server refused ids the update did not add"));
        }
        let mut blocked = 0;
        for (id, route) in update.additions {
            let refused = self.refused_buf.contains(&id);
            match self.routes.get_mut(&route) {
                Some(standing) if *standing == Standing::InFlight(id) => {
                    if refused {
                        *standing = Standing::Blocked;
                        self.blocked.insert(route);
                        blocked += 1;
                    } else {
                        *standing = Standing::Acked(id);
                        acked.push(route.clone());
                        self.acked.insert(id, route);
                    }
                }
                // The route was dropped while the update flew, and its removal is queued. A
                // refused addition was never held, so that removal would be a no-op.
                _ => {
                    if refused {
                        self.removals.remove(&id);
                    }
                }
            }
        }
        Ok(blocked)
    }

    /// The update in flight was refused whole, but a late copy of it may still apply: its ids
    /// are queued for removal, its routes re-queued under fresh ids and its removals re-queued.
    /// The next update then removes before it adds (law 3), whichever way the copy went.
    fn unwind_in_flight(&mut self) {
        let InFlight { update, .. } = self.in_flight.take().expect(ERROR_IMPOSSIBLE);
        self.removals.extend(update.removals);
        for (id, route) in update.additions {
            self.removals.insert(id);
            if let Some(standing) = self.routes.get_mut(&route)
                && *standing == Standing::InFlight(id)
            {
                let fresh = Self::fresh_id(&mut self.next_target);
                *standing = Standing::Pending(fresh);
                self.pending.insert(fresh, route);
            }
        }
    }

    /// Open a fresh session that re-states the whole wanted set (law 5). The old session id is
    /// never used again, so nothing the server still holds or receives for it can be taken for
    /// the new one.
    fn reset(&mut self) {
        self.session_id = Uuid::new_v4();
        self.generation = 0;
        self.in_flight = None;
        self.pending.clear();
        self.acked.clear();
        self.blocked.clear();
        self.removals.clear();
        for (route, standing) in &mut self.routes {
            let id = Self::fresh_id(&mut self.next_target);
            *standing = Standing::Pending(id);
            self.pending.insert(id, route.clone());
        }
    }

    /// A round over the acknowledged targets `cursor_of` gives a cursor for (law 6). `None`
    /// when it gives none. The cursors are the caller's: this machine keeps none (law 9).
    pub fn round(
        &self,
        mut cursor_of: impl FnMut(&Route) -> Option<Rev>,
        limit: u32,
        hold_ms: u32,
    ) -> Option<PageSession> {
        let targets: Vec<_> = self
            .acked
            .iter()
            .filter_map(|(id, route)| cursor_of(route).map(|cursor| (*id, cursor)))
            .collect();
        (!targets.is_empty()).then_some(PageSession {
            session_id: self.session_id,
            targets,
            limit,
            hold_ms,
        })
    }

    /// Take the server's answer to round `request`, pushing one outcome per target the client
    /// still holds into `outcomes` (cleared first), with the route it follows. A refused
    /// target's route is blocked. The page's events are the caller's to apply, resolving their
    /// ids with [`ReplayClient::route_of`] before the next change to the target set.
    ///
    /// A round lost in transport needs no call: the caller repeats it from the same cursors.
    /// Errors are answers no correct server gives; the driver treats the peer as faulty.
    pub fn round_answered(
        &mut self,
        request: &PageSession,
        answer: &Result<SessionPage, SessionError>,
        outcomes: &mut Vec<(TargetId, Route, TargetOutcome)>,
    ) -> Res<RoundOutcome> {
        outcomes.clear();
        if request.session_id != self.session_id {
            return Ok(RoundOutcome::Stale);
        }
        let page = match answer {
            Ok(page) => page,
            Err(SessionError::UnknownSession) => {
                self.reset();
                return Ok(RoundOutcome::Reset);
            }
            // The round raced a removal of the client's own (law 3 after law 6): stale.
            Err(SessionError::UnknownTarget { target }) if !self.acked.contains_key(target) => {
                return Ok(RoundOutcome::Stale);
            }
            // The server lost a target it acknowledged and the client still holds: the
            // session no longer matches the client's, and only a fresh one can be trusted.
            Err(SessionError::UnknownTarget { .. }) => {
                self.reset();
                return Ok(RoundOutcome::Reset);
            }
            // A round cut short by a deadline or cancellation changed nothing on either side
            // (law 9): the caller repeats it from the same cursors.
            Err(SessionError::Context(_)) => return Ok(RoundOutcome::Stale),
            Err(err @ (SessionError::Limit | SessionError::Invalid(_))) => {
                return Err(ferr!("the server rejected a round: {err}"));
            }
        };
        self.check_page(request, page)?;
        for &(id, verdict) in &page.verdicts {
            let Some(route) = self.acked.get(&id) else {
                continue;
            };
            let outcome = match verdict {
                Verdict::Events { resume, drained } => TargetOutcome::Advance { resume, drained },
                Verdict::Refused => TargetOutcome::Refused,
            };
            outcomes.push((id, route.clone(), outcome));
            if outcome == TargetOutcome::Refused {
                // The server keeps holding the id; dropping it and re-adding the route under
                // a fresh id lets admission (law 4) decide when the route is readable again.
                let route = self.acked.remove(&id).expect(ERROR_IMPOSSIBLE);
                self.removals.insert(id);
                *self.routes.get_mut(&route).expect(ERROR_IMPOSSIBLE) = Standing::Blocked;
                self.blocked.insert(route);
            }
        }
        Ok(RoundOutcome::Answered)
    }

    /// Law 6 and law 8 from the client's side: one verdict per requested target, and no
    /// `resume` below its cursor.
    fn check_page(&mut self, request: &PageSession, page: &SessionPage) -> Res<()> {
        self.round_buf.clear();
        self.round_buf.extend(
            request
                .targets
                .iter()
                .map(|(id, cursor)| (*id, (*cursor, false))),
        );
        for (id, verdict) in &page.verdicts {
            let Some((cursor, answered)) = self.round_buf.get_mut(id) else {
                return Err(ferr!(
                    "the page answers target {id}, which the round did not name"
                ));
            };
            if std::mem::replace(answered, true) {
                return Err(ferr!("the page answers target {id} twice"));
            }
            if let Verdict::Events { resume, .. } = verdict
                && resume < cursor
            {
                return Err(ferr!(
                    "the page resumes target {id} at {resume}, below {cursor}"
                ));
            }
        }
        if page.verdicts.len() != self.round_buf.len() {
            return Err(ferr!("the page leaves targets of the round unanswered"));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn wanted(&self) -> impl Iterator<Item = &Route> {
        self.routes.keys()
    }
}
