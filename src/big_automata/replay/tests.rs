//! A deterministic simulation of the whole stack: one client, one server, the responder and a
//! memory part store, over a network that drops, duplicates, reorders and delays messages,
//! while the wanted set churns, the store is written, access is granted and revoked, and the
//! server forgets. The properties are checked after every step, and once more after a quiet
//! phase that drains the network and pages every wanted, readable route to the store's head.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::executor::block_on;
use proptest::prelude::*;

use crate::interlude::*;

use super::client::ReplayClient;
use super::client::RoundOutcome;
use super::client::TargetOutcome;
use super::responder::Responder;
use super::responder::Timer;
use super::server::ReplayServer;
use super::server::SESSION_TTL;
use super::wire::PageEvent;
use super::wire::PageSession;
use super::wire::Route;
use super::wire::SessionError;
use super::wire::SessionId;
use super::wire::SessionPage;
use super::wire::SessionUpdated;
use super::wire::TargetId;
use super::wire::UpdateSession;
use super::wire::Verdict;
use crate::part_store::LogKey;
use crate::part_store::LogRoutes;
use crate::part_store::ObjPayload;
use crate::part_store::laws;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartOracle;
use crate::part_store::oracle::PartWrite;
use crate::rev::Limits;
use crate::rev::Rev;
use crate::scheduler::Instant;

/// Routes 0..4 are parts 0..4, 4..6 objects 4 and 5.
const ROUTES: u8 = 6;
const PARTS: u8 = 4;
/// The objects writes touch: 4 and 5 have object routes, 6 and 7 only ride part routes.
const OBJS: std::ops::Range<u8> = 4..8;
/// Small, so a route's backlog takes several rounds.
const PAGE_LIMIT: u32 = 3;
/// Every round holds; the sim's timer ends a hold at once, so a hold with no rows answers
/// after one wait on the watch.
const HOLD_MS: u32 = 50;

fn route(index: u8) -> Route {
    if index < PARTS {
        Route::Part {
            part_key: part(index),
        }
    } else {
        Route::Object {
            obj_key: obj(index),
        }
    }
}

/// A timer whose sleeps are over as soon as they start.
#[derive(Clone, Copy)]
struct Expired;

impl Timer for Expired {
    fn sleep(&self, _: Duration) -> impl Future<Output = ()> + Send + use<> {
        std::future::ready(())
    }
}

#[derive(Debug, Clone)]
enum Step {
    /// The embedder wants a route (index mod the routes); redundant when it already does.
    Want(u8),
    /// The embedder stops wanting a route; redundant when it does not want it.
    Unwant(u8),
    /// Toggle the client's access to a part.
    Access(u8),
    /// One store write on `(object, part or value)`: set a payload, add or remove a member.
    Write(u8, u8, u8),
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
    /// the sim loses them. A message outliving the TTL is the revival law 5 allows, pinned
    /// separately by `a_late_opening_update_revives_a_forgotten_session`.
    Sweep,
    /// The server restarts: it forgets everything, and the connection's messages are lost.
    /// The store is durable and keeps its revs.
    Restart,
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
    store: MemoryPartStore,
    /// The store's twin, written alongside it: what the client must end up with.
    oracle: PartOracle,
    responder: Responder<MemoryPartStore, Expired>,
    net: Vec<Msg>,
    wanted: BTreeSet<Route>,
    /// The cursor machine's cursors.
    cursors: BTreeMap<Route, Rev>,
    /// Every row the client was handed, by route: `(route, object, rev)` to the payload of a
    /// change, `None` for a removal.
    received: BTreeMap<(Route, ObjKey, Rev), Option<ObjPayload>>,
    now: Instant,
    /// Every id each live session ever held, to catch a reuse. A forgotten session's history
    /// goes with it: a late opening update may open the same id again, as a new session.
    ever_held: Map<SessionId, Set<TargetId>>,
    outcomes: Vec<(TargetId, Route, TargetOutcome)>,
    /// Whether the last round's answer was a page with no refusal that drained every target.
    last_round_drained: bool,
    /// The ids the last drained round named.
    drained_ids: BTreeSet<TargetId>,
    /// The targets of the round being applied whose cursor is still the round's.
    fresh: BTreeSet<TargetId>,
}

impl Sim {
    fn new() -> Self {
        let store = MemoryPartStore::default();
        Self {
            client: ReplayClient::new(Uuid::from_u128(1)),
            server: ReplayServer::default(),
            responder: Responder::new(store.clone(), Expired),
            store,
            oracle: default(),
            net: Vec::new(),
            wanted: BTreeSet::new(),
            cursors: BTreeMap::new(),
            received: BTreeMap::new(),
            now: Instant::now(),
            ever_held: default(),
            outcomes: Vec::new(),
            last_round_drained: false,
            drained_ids: BTreeSet::new(),
            fresh: BTreeSet::new(),
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

    /// Whether the client may read `route` now, by the oracle: a part it holds a grant on, or
    /// an object live in such a part.
    fn readable(&self, route: &Route) -> bool {
        match route {
            Route::Part { part_key } => self
                .oracle
                .grants()
                .contains_key(&(part_key.clone(), principal(0))),
            Route::Object { obj_key } => self
                .oracle
                .readable_obj_state(&principal(0), obj_key)
                .is_some(),
        }
    }

    /// Commit one transaction to the store and the oracle alike.
    fn commit(&mut self, write: PartWrite) {
        let mut tx = self.store.begin();
        block_on(laws::stage(&self.store, &mut tx, write.clone())).unwrap();
        assert_eq!(tx.commit(), self.oracle.commit(vec![write]));
    }

    fn write(&mut self, kind: u8, obj_index: u8, other: u8) {
        let obj_index = OBJS.start + obj_index % OBJS.len() as u8;
        let part_index = other % PARTS;
        let write = match kind % 3 {
            0 => laws::set(&[(obj_index, other % 4)]),
            1 => laws::add(&[(obj_index, part_index)]),
            _ => laws::remove(&[(obj_index, part_index)]),
        };
        self.commit(write);
    }

    fn step(&mut self, step: Step) {
        match step {
            Step::Want(index) => {
                let route = route(index % ROUTES);
                self.wanted.insert(route.clone());
                self.client.want(route);
            }
            Step::Unwant(index) => {
                let route = route(index % ROUTES);
                self.wanted.remove(&route);
                self.client.unwant(&route);
            }
            Step::Access(index) => {
                let index = index % PARTS;
                let write = if self.readable(&route(index)) {
                    laws::revoke(&[(index, 0)])
                } else {
                    laws::grant(&[(index, 0)])
                };
                self.commit(write);
            }
            Step::Write(kind, obj_index, other) => self.write(kind, obj_index, other),
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
            PAGE_LIMIT,
            HOLD_MS,
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
            Msg::Update(request) => self.serve_update(request),
            Msg::Updated(request, answer) => {
                self.client
                    .update_answered(&request, &answer)
                    .expect("a correct server's answer");
            }
            Msg::Round(request) => {
                let answer = self.serve_round(&request);
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
            Msg::Paged(request, answer) => self.paged(&request, &answer),
        }
    }

    /// The server's side of an update: admission read from the store by the responder, then
    /// the session machine.
    fn serve_update(&mut self, request: UpdateSession) {
        let before = self.held(request.session_id);
        let admits = block_on(self.responder.admission(&principal(0), &request)).unwrap();
        let answer = self
            .server
            .update(&principal(0), &request, self.now, admits);
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

    fn serve_round(&mut self, request: &PageSession) -> Result<SessionPage, SessionError> {
        let Self {
            responder,
            server,
            now,
            ..
        } = self;
        block_on(responder.page(&principal(0), |round| {
            server.resolve_round(&principal(0), request, *now, round)
        }))
        .expect("the memory store does not fail")
        .cloned()
    }

    /// The client takes a page: the client machine first, then the page's rows, each resolved
    /// to the route its target follows, and the cursors.
    fn paged(&mut self, request: &PageSession, answer: &Result<SessionPage, SessionError>) {
        let outcome = self
            .client
            .round_answered(request, answer, &mut self.outcomes)
            .expect("a correct server's page");
        let head = self.oracle.latest_rev();
        // Law 11: a round's omissions are judged against the cursor it was asked from, so its
        // answer for a target is applied only while the client's cursor for that target is
        // still that cursor. An answer to an older round, overtaken in the network, is dropped:
        // applying its rows next to a newer round's could keep a member the newer one removed.
        self.fresh.clear();
        for (id, cursor) in &request.targets {
            let current = self
                .client
                .route_of(*id)
                .map(|route| self.cursors.get(route).copied().unwrap_or(0));
            if current == Some(*cursor) {
                self.fresh.insert(*id);
            }
        }
        // Quiet is confirmed by a round that drained every target it named; whether it named
        // every target the client holds is judged when quiet is decided (`drained_ids`), since
        // an update acknowledged later in the same drain can add one.
        self.last_round_drained = outcome == RoundOutcome::Answered
            && self.fresh.len() == request.targets.len()
            && self.outcomes.len() == request.targets.len()
            && self.outcomes.iter().all(|(_, _, outcome)| {
                // `drained` is as of the round's snapshot; a page that waited in the network
                // past a commit is behind the head, so quiet asks for the head itself.
                matches!(outcome, TargetOutcome::Advance { resume, .. } if *resume >= head)
            });
        if self.last_round_drained {
            self.drained_ids = request.targets.iter().map(|(id, _)| *id).collect();
        }
        if outcome != RoundOutcome::Answered {
            return;
        }
        let page = answer.as_ref().expect("an answered round has a page");
        for event in &page.events {
            self.receive(request, page, event);
        }
        for (id, route, outcome) in &self.outcomes {
            if !self.fresh.contains(id) {
                continue;
            }
            if let TargetOutcome::Advance { resume, .. } = outcome {
                let cursor = self.cursors.entry(route.clone()).or_default();
                *cursor = (*cursor).max(*resume);
            }
        }
    }

    /// One row of a page, checked against law 8 (it lies in its target's `(cursor, resume]`)
    /// and law 12 (removals ride part targets only), and recorded under each route it reached.
    /// A row met again must say the same thing: one rev of one key is one row.
    fn receive(&mut self, request: &PageSession, page: &SessionPage, event: &PageEvent) {
        let (rev, obj_key, targets, payload) = match event {
            PageEvent::Changed {
                rev,
                obj_key,
                targets,
                payload,
            } => (*rev, obj_key, targets.as_slice(), Some(payload)),
            PageEvent::Removed {
                rev,
                obj_key,
                target,
            } => (*rev, obj_key, std::slice::from_ref(target), None),
        };
        for target in targets {
            if !self.fresh.contains(target) {
                continue;
            }
            let cursor = request
                .targets
                .iter()
                .find(|(id, _)| id == target)
                .map(|(_, cursor)| *cursor)
                .expect("an event names a target of its round");
            let resume = page
                .verdicts
                .iter()
                .find_map(|(id, verdict)| match verdict {
                    Verdict::Events { resume, .. } if id == target => Some(*resume),
                    _ => None,
                })
                .expect("an event names a target answered with events");
            assert!(
                cursor < rev && rev <= resume,
                "{event:?} outside ({cursor}, {resume}]"
            );
            // A target the client dropped since the round went out: its rows are dropped too.
            let Some(route) = self.client.route_of(*target) else {
                continue;
            };
            assert!(
                payload.is_some() || matches!(route, Route::Part { .. }),
                "a removal on object target {target}"
            );
            let row = (route.clone(), obj_key.clone(), rev);
            let payload = payload.cloned();
            if let Some(seen) = self.received.get(&row) {
                assert_eq!(seen, &payload, "row {row:?} said two things");
            }
            self.received.insert(row, payload);
        }
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

    /// Drain: no churn, no writes, no loss. The driver retries what is blocked and readable
    /// and keeps rounds going until one drains every target, which is also how it hears that
    /// the server forgot.
    fn quiesce(&mut self) {
        for _ in 0..64 {
            let readable_blocked = self.client.blocked().any(|route| self.readable(route));
            if self.client.is_settled() && readable_blocked {
                self.client.retry_blocked();
            }
            self.last_round_drained = false;
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
            let readable_blocked = self.client.blocked().any(|route| self.readable(route));
            // With nothing acknowledged there is no round to send, and nothing for the server
            // to hold for this session either.
            let acked_ids: BTreeSet<_> = self.client.acked().map(|(id, _)| id).collect();
            let confirmed =
                (self.last_round_drained && self.drained_ids == acked_ids) || acked_ids.is_empty();
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
        for route in self.wanted.iter().filter(|route| self.readable(route)) {
            assert!(
                acked_routes.contains(route),
                "wanted readable {route:?} is not acked"
            );
            self.check_replicated(route);
        }
    }

    /// A wanted, readable route is replicated: its cursor is at the store's head, the client
    /// was handed every current live row of the route (the oracle's read from nothing) with
    /// the payload it carries now, and, for a part, folding what the client was handed gives
    /// the part's live members (so every removal it needed reached it too).
    fn check_replicated(&self, route: &Route) {
        let head = self.oracle.latest_rev();
        assert_eq!(
            self.cursors.get(route).copied().unwrap_or(0),
            head,
            "{route:?} is not at the head"
        );
        let routes = match route {
            Route::Part { part_key } => LogRoutes {
                parts: [(part_key.clone(), 0)].into(),
                objects: default(),
            },
            Route::Object { obj_key } => LogRoutes {
                parts: default(),
                objects: [(obj_key.clone(), 0)].into(),
            },
        };
        let unlimited = Limits {
            max_entries: std::num::NonZeroUsize::MAX,
            max_bytes: None,
        };
        let expected = self
            .oracle
            .read_authorized(&principal(0), &routes, 0, head, unlimited);
        // Law 12: an object target carries content, not membership. A row of the object in a
        // part the client could not read when it paged is withheld, and the target moves past
        // it; what the client must end with is the object's current content.
        if let Route::Object { obj_key } = route {
            let last = self
                .received
                .range((route.clone(), obj_key.clone(), 0)..)
                .take_while(|((received_route, obj, _), _)| {
                    received_route == route && obj == obj_key
                })
                .last()
                .map(|(_, payload)| payload);
            let current = &expected.payloads[expected.payload_slot(obj_key)].1;
            assert_eq!(
                last,
                Some(&Some(current.clone())),
                "{route:?} lacks its current content"
            );
            return;
        }
        for entry in expected.rows.entries.iter().filter(|e| e.value.is_some()) {
            let LogKey::Member { obj, .. } = &entry.key else {
                unreachable!("an authorized read carries no access row")
            };
            let payload = &expected.payloads[expected.payload_slot(obj)].1;
            assert_eq!(
                self.received.get(&(route.clone(), obj.clone(), entry.rev)),
                Some(&Some(payload.clone())),
                "{route:?} never got {entry:?}"
            );
        }
        if let Route::Part { part_key } = route {
            let mut folded = BTreeMap::new();
            for ((_, obj, _), payload) in self
                .received
                .range((route.clone(), ObjKey::new(&[]), 0)..)
                .take_while(|((received_route, ..), _)| received_route == route)
            {
                // Keys sort by rev within an object, so the last one wins.
                folded.insert(obj.clone(), payload.is_some());
            }
            let live: BTreeSet<_> = folded
                .into_iter()
                .filter(|(_, live)| *live)
                .map(|(obj, _)| obj)
                .collect();
            let members: BTreeSet<_> = self
                .oracle
                .live_members()
                .into_iter()
                .filter(|(_, member_part)| member_part == part_key)
                .map(|(obj, _)| obj)
                .collect();
            assert_eq!(live, members, "the client's view of {part_key}");
        }
    }
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        2 => any::<u8>().prop_map(Step::Want),
        2 => any::<u8>().prop_map(Step::Unwant),
        2 => any::<u8>().prop_map(Step::Access),
        3 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(kind, obj, other)| Step::Write(kind, obj, other)),
        3 => Just(Step::SendUpdate),
        3 => Just(Step::SendRound),
        8 => any::<u8>().prop_map(Step::Deliver),
        2 => any::<u8>().prop_map(Step::Drop),
        2 => any::<u8>().prop_map(Step::Duplicate),
        1 => Just(Step::RetryBlocked),
        1 => Just(Step::Sweep),
        1 => Just(Step::Restart),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn client_and_server_agree_over_a_faulty_network(
        steps in proptest::collection::vec(step(), 0..200),
    ) {
        let mut sim = Sim::new();
        for step in steps {
            sim.step(step);
        }
        sim.quiesce();
    }
}

/// Shrunk by the stack simulation: the client's view of a part kept a member the store removed.
#[test]
fn a_removed_member_leaves_the_clients_view() {
    use Step::*;
    let mut sim = Sim::new();
    for step in [
        Write(15, 61, 0),
        Want(165),
        Want(52),
        Unwant(15),
        Unwant(34),
        Want(0),
        Write(91, 181, 143),
        Access(43),
        SendUpdate,
        Want(4),
        Unwant(64),
        Access(11),
        Want(201),
        Want(4),
        RetryBlocked,
        Write(0, 0, 0),
        Deliver(0),
        Deliver(0),
        Access(191),
        SendUpdate,
        Deliver(0),
        Duplicate(0),
        Unwant(0),
        Deliver(0),
        Write(0, 0, 0),
        SendRound,
        Write(0, 0, 0),
        Deliver(215),
        Write(65, 1, 123),
    ] {
        sim.step(step);
    }
    sim.quiesce();
}

/// Shrunk by the stack simulation: a readable part wanted while an update was in flight.
#[test]
fn a_part_wanted_during_an_update_catches_up() {
    use Step::*;
    let mut sim = Sim::new();
    for step in [
        Unwant(70),
        Unwant(9),
        Access(214),
        Access(43),
        Want(92),
        Unwant(182),
        SendUpdate,
        Want(20),
        Deliver(0),
        Duplicate(0),
        Deliver(0),
        SendRound,
    ] {
        sim.step(step);
    }
    sim.quiesce();
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
    client.want(route(0));
    let open = exchange(&mut client, &mut server, start);
    client.unwant(&route(0));
    exchange(&mut client, &mut server, start);
    // The session goes quiet for a TTL and is swept; nothing tells the client.
    server.sweep(start + SESSION_TTL);
    // A copy of the opening, delayed past the TTL, arrives and opens the session again.
    let late = start + SESSION_TTL;
    server.update(&principal(0), &open, late, |_| true).unwrap();
    client.want(route(1));
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
