//! A deterministic simulation of one client and one server over a network that drops,
//! duplicates, reorders and delays messages, while the wanted set churns, readability flips and
//! the server forgets. The properties are checked after every step, and once more after a
//! quiet phase that drains the network.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::*;

use crate::interlude::*;

use super::client::ReplayClient;
use super::client::TargetOutcome;
use super::server::ReplayServer;
use super::server::Round;
use super::server::SESSION_TTL;
use super::wire::PageSession;
use super::wire::Route;
use super::wire::SessionError;
use super::wire::SessionId;
use super::wire::SessionPage;
use super::wire::SessionUpdated;
use super::wire::TargetId;
use super::wire::UpdateSession;
use crate::part_store::AuthorizedPage;
use crate::part_store::ReadVerdict;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::rev::Rev;
use crate::scheduler::Instant;

/// Routes 0..4 are parts, 4..6 objects.
const ROUTES: u8 = 6;

fn route(index: u8) -> Route {
    if index < 4 {
        Route::Part {
            part_key: part(index),
        }
    } else {
        Route::Object {
            obj_key: obj(index),
        }
    }
}

#[derive(Debug, Clone)]
enum Step {
    /// Toggle whether the embedder wants a route.
    Want(u8),
    /// Toggle whether the client may read a route; admission and verdicts both follow it.
    Readable(u8),
    SendUpdate,
    SendRound,
    /// Deliver the message at this index (mod the number in flight): reordering and delay.
    Deliver(u8),
    /// Lose a message; the driver hears of a lost update or update answer.
    Drop(u8),
    Duplicate(u8),
    RetryBlocked,
    /// The TTL elapses with no request: the server sweeps the session. The network's delay is
    /// bounded well below the TTL, so every message in flight is delivered or lost by then;
    /// the sim loses them. A message outliving the TTL would break law 5, see
    /// `a_late_opening_update_revives_a_forgotten_session`.
    Sweep,
    /// The server restarts: it forgets everything, and the connection's messages are lost.
    Restart,
    /// The store commits a revision.
    Tick,
}

#[derive(Debug, Clone)]
enum Msg {
    Update(UpdateSession),
    Updated(UpdateSession, Result<SessionUpdated, SessionError>),
    Round(PageSession),
    Paged(PageSession, Result<SessionPage, SessionError>),
}

/// What the server holds for one session, as the properties compare it.
type Held = (u64, BTreeMap<TargetId, Route>, Vec<TargetId>);

struct Sim {
    client: ReplayClient,
    server: ReplayServer,
    round: Round,
    read: AuthorizedPage,
    net: Vec<Msg>,
    wanted: BTreeSet<Route>,
    readable: BTreeSet<Route>,
    /// The cursor machine's cursors.
    cursors: BTreeMap<Route, Rev>,
    rev: Rev,
    now: Instant,
    /// Every id each live session ever held, to catch a reuse. A forgotten session's history
    /// goes with it: a late opening update may open the same id again, as a new session.
    ever_held: Map<SessionId, Set<TargetId>>,
    outcomes: Vec<(TargetId, Route, TargetOutcome)>,
    /// Whether the last round's answer was a full page with no refusal.
    last_round_clean: bool,
}

impl Sim {
    fn new(readable: BTreeSet<Route>) -> Self {
        Self {
            client: ReplayClient::new(Uuid::from_u128(1)),
            server: ReplayServer::default(),
            round: Round::default(),
            read: AuthorizedPage::default(),
            net: Vec::new(),
            wanted: BTreeSet::new(),
            readable,
            cursors: BTreeMap::new(),
            rev: 1,
            now: Instant::now(),
            ever_held: default(),
            outcomes: Vec::new(),
            last_round_clean: false,
        }
    }

    fn held(&self, session_id: SessionId) -> Option<Held> {
        let session = self.server.session(&principal(0), &session_id)?;
        Some((
            session.generation,
            session
                .targets
                .iter()
                .map(|(id, route)| (*id, route.clone()))
                .collect(),
            session.refused.clone(),
        ))
    }

    fn step(&mut self, step: Step) {
        match step {
            Step::Want(index) => {
                let route = route(index % ROUTES);
                if !self.wanted.remove(&route) {
                    self.wanted.insert(route);
                }
                self.client.set_wanted(&self.wanted);
            }
            Step::Readable(index) => {
                let route = route(index % ROUTES);
                if !self.readable.remove(&route) {
                    self.readable.insert(route);
                }
            }
            Step::SendUpdate => {
                if let Some(update) = self.client.next_update() {
                    self.net.push(Msg::Update(update.clone()));
                }
            }
            Step::SendRound => self.send_round(),
            Step::Deliver(index) if !self.net.is_empty() => {
                let msg = self.net.remove(index as usize % self.net.len());
                self.deliver(msg);
            }
            Step::Drop(index) if !self.net.is_empty() => {
                let msg = self.net.remove(index as usize % self.net.len());
                self.lost(&msg);
            }
            Step::Duplicate(index) if !self.net.is_empty() => {
                let msg = self.net[index as usize % self.net.len()].clone();
                self.net.push(msg);
            }
            Step::Deliver(_) | Step::Drop(_) | Step::Duplicate(_) => {}
            Step::RetryBlocked => self.client.retry_blocked(),
            Step::Sweep => {
                for msg in std::mem::take(&mut self.net) {
                    self.lost(&msg);
                }
                self.now += SESSION_TTL;
                self.server.sweep(self.now);
                let server = &self.server;
                self.ever_held
                    .retain(|session_id, _| server.session(&principal(0), session_id).is_some());
            }
            Step::Restart => {
                self.server = ReplayServer::default();
                self.ever_held.clear();
                for msg in std::mem::take(&mut self.net) {
                    self.lost(&msg);
                }
            }
            Step::Tick => self.rev += 1,
        }
        self.check();
    }

    fn lost(&mut self, msg: &Msg) {
        if let Msg::Update(request) | Msg::Updated(request, _) = msg {
            self.client.update_lost(request);
        }
    }

    fn send_round(&mut self) {
        let cursors = &self.cursors;
        let request = self.client.round(
            |route| Some(cursors.get(route).copied().unwrap_or(0)),
            100,
            0,
        );
        if let Some(request) = request {
            for (id, _) in &request.targets {
                assert!(
                    self.client.route_of(*id).is_some(),
                    "a round names only acknowledged ids"
                );
            }
            self.net.push(Msg::Round(request));
        }
    }

    fn deliver(&mut self, msg: Msg) {
        match msg {
            Msg::Update(request) => {
                let before = self.held(request.session_id);
                let readable = &self.readable;
                let answer = self
                    .server
                    .update(&principal(0), &request, self.now, |route| {
                        readable.contains(route)
                    });
                let after = self.held(request.session_id);
                if let Some(before) = &before
                    && request.generation <= before.0
                {
                    assert_eq!(
                        after.as_ref(),
                        Some(before),
                        "a stale update changed the server"
                    );
                }
                if let Some((_, targets, _)) = &after {
                    let ever = self.ever_held.entry(request.session_id).or_default();
                    let fresh = targets.keys().filter(|id| {
                        before
                            .as_ref()
                            .is_none_or(|(_, held, _)| !held.contains_key(*id))
                    });
                    for id in fresh {
                        assert!(ever.insert(*id), "the server took id {id} a second time");
                    }
                    let routes: BTreeSet<_> = targets.values().collect();
                    assert_eq!(routes.len(), targets.len(), "a session holds a route twice");
                }
                self.net.push(Msg::Updated(request, answer));
            }
            Msg::Updated(request, answer) => {
                self.client
                    .update_answered(&request, &answer)
                    .expect("a correct server's answer");
            }
            Msg::Round(request) => {
                let answer = self.serve(&request);
                if let Err(SessionError::UnknownTarget { target }) = &answer
                    && request.session_id == self.client.session_id()
                {
                    assert!(
                        self.client.route_of(*target).is_none(),
                        "the server lost target {target}, which the client holds"
                    );
                }
                self.net.push(Msg::Paged(request, answer));
            }
            Msg::Paged(request, answer) => {
                let outcome = self
                    .client
                    .round_answered(&request, &answer, &mut self.outcomes)
                    .expect("a correct server's page");
                self.last_round_clean = outcome == super::client::RoundOutcome::Answered
                    && self.outcomes.len() == request.targets.len()
                    && self
                        .outcomes
                        .iter()
                        .all(|(_, _, outcome)| *outcome != TargetOutcome::Refused);
                for (_, route, outcome) in &self.outcomes {
                    if let TargetOutcome::Advance { resume, .. } = outcome {
                        let cursor = self.cursors.entry(route.clone()).or_default();
                        *cursor = (*cursor).max(*resume);
                    }
                }
            }
        }
    }

    /// The responder, with readability standing in for the store: verdicts from it, no rows.
    fn serve(&mut self, request: &PageSession) -> Result<SessionPage, SessionError> {
        self.server
            .resolve_round(&principal(0), request, self.now, &mut self.round)?;
        let verdict = |route: Route| {
            if self.readable.contains(&route) {
                ReadVerdict::Permitted
            } else {
                ReadVerdict::Denied
            }
        };
        self.read.parts.clear();
        self.read
            .parts
            .extend(self.round.routes().parts.keys().map(|part_key| {
                verdict(Route::Part {
                    part_key: part_key.clone(),
                })
            }));
        self.read.objects.clear();
        self.read
            .objects
            .extend(self.round.routes().objects.keys().map(|obj_key| {
                verdict(Route::Object {
                    obj_key: obj_key.clone(),
                })
            }));
        self.read.rows.entries.clear();
        self.read.rows.through = self.rev;
        assert!(self.round.content_keys(&self.read).is_empty());
        let mut page = SessionPage {
            events: default(),
            verdicts: default(),
        };
        self.round.answer(&self.read, self.rev, &[], &mut page);
        Ok(page)
    }

    fn check(&self) {
        let wanted: BTreeSet<_> = self.client.wanted().cloned().collect();
        assert_eq!(wanted, self.wanted);
        for route in self.client.blocked() {
            assert!(
                self.wanted.contains(route),
                "blocked route {route:?} is not wanted"
            );
        }
        for (_, route) in self.client.acked() {
            assert!(
                self.wanted.contains(route),
                "acked route {route:?} is not wanted"
            );
        }
    }

    /// Drain: no churn, no flips, no loss. The driver retries what is blocked and readable and
    /// keeps rounds going, which is how it hears that the server forgot.
    fn quiesce(&mut self) {
        for _ in 0..32 {
            let readable_blocked = self
                .client
                .blocked()
                .any(|route| self.readable.contains(route));
            if self.client.is_settled() && readable_blocked {
                self.client.retry_blocked();
            }
            self.last_round_clean = false;
            if let Some(update) = self.client.next_update() {
                self.net.push(Msg::Update(update.clone()));
            } else {
                self.send_round();
            }
            while !self.net.is_empty() {
                let msg = self.net.remove(0);
                self.deliver(msg);
            }
            self.check();
            let readable_blocked = self
                .client
                .blocked()
                .any(|route| self.readable.contains(route));
            // With nothing acknowledged there is no round to send, and nothing for the server
            // to hold for this session either.
            let confirmed = self.last_round_clean || self.client.acked().next().is_none();
            if self.client.is_settled() && !readable_blocked && confirmed {
                return self.check_quiet();
            }
        }
        panic!("the simulation did not quiesce");
    }

    fn check_quiet(&self) {
        let acked: BTreeMap<_, _> = self
            .client
            .acked()
            .map(|(id, route)| (id, route.clone()))
            .collect();
        let held = self
            .held(self.client.session_id())
            .map(|(_, targets, _)| targets)
            .unwrap_or_default();
        assert_eq!(held, acked, "the server holds what the client acknowledged");
        let acked_routes: BTreeSet<_> = acked.values().collect();
        for route in self.wanted.intersection(&self.readable) {
            assert!(
                acked_routes.contains(route),
                "wanted readable {route:?} is not acked"
            );
        }
    }
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => any::<u8>().prop_map(Step::Want),
        2 => any::<u8>().prop_map(Step::Readable),
        3 => Just(Step::SendUpdate),
        3 => Just(Step::SendRound),
        8 => any::<u8>().prop_map(Step::Deliver),
        2 => any::<u8>().prop_map(Step::Drop),
        2 => any::<u8>().prop_map(Step::Duplicate),
        1 => Just(Step::RetryBlocked),
        1 => Just(Step::Sweep),
        1 => Just(Step::Restart),
        1 => Just(Step::Tick),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn client_and_server_agree_over_a_faulty_network(
        readable in proptest::collection::btree_set(0..ROUTES, 0..=ROUTES as usize),
        steps in proptest::collection::vec(step(), 0..200),
    ) {
        let mut sim = Sim::new(readable.into_iter().map(route).collect());
        for step in steps {
            sim.step(step);
        }
        sim.quiesce();
    }
}

/// Pins a gap in law 5: the server cannot tell a late copy of a session's opening update from a
/// fresh opening. When a copy outlives the session's TTL, it re-opens the forgotten session with
/// the opening's targets, and the client, which never heard of the forget, carries on sending
/// deltas against a target set the server no longer has. Law 5's "nothing about a forgotten
/// session can corrupt the new one" holds only when the transport never delivers a request
/// later than the TTL after it was sent, which the laws do not state.
///
/// Here the revived set holds a target the client already removed, which nothing removes
/// again. Had the client added targets between the opening and the forget, the revived set would
/// miss them, and the client's next round naming one gets `UnknownTarget`, which resets it.
#[test]
fn a_late_opening_update_revives_a_forgotten_session() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    let mut server = ReplayServer::default();
    let start = Instant::now();
    let exchange = |client: &mut ReplayClient, server: &mut ReplayServer, now| {
        let request = client.next_update().unwrap().clone();
        let answer = server.update(&principal(0), &request, now, |_| true);
        client.update_answered(&request, &answer).unwrap();
        request
    };
    client.set_wanted(&[route(0)].into());
    let open = exchange(&mut client, &mut server, start);
    client.set_wanted(&BTreeSet::new());
    exchange(&mut client, &mut server, start);
    // The session goes quiet for a TTL and is swept; nothing tells the client.
    server.sweep(start + SESSION_TTL);
    // A copy of the opening, delayed past the TTL, arrives and opens the session again.
    let late = start + SESSION_TTL;
    server.update(&principal(0), &open, late, |_| true).unwrap();
    client.set_wanted(&[route(1)].into());
    exchange(&mut client, &mut server, late);

    let held: BTreeMap<_, _> = server
        .session(&principal(0), &client.session_id())
        .unwrap()
        .targets
        .iter()
        .map(|(id, route)| (*id, route.clone()))
        .collect();
    let acked: BTreeMap<_, _> = client
        .acked()
        .map(|(id, route)| (id, route.clone()))
        .collect();
    assert_eq!(acked, [(1, route(1))].into());
    assert_eq!(
        held,
        [(0, route(0)), (1, route(1))].into(),
        "the revived target is orphaned"
    );
}
