//! The simulation of both machines together: random leases, grants and revokes of standing on
//! both sides, peers connecting and disconnecting, dials that fail and sessions that are lost,
//! and clock jumps. A fake Keyline says who stands where, a fake network answers dials, and the
//! checks compare the machines to that world after every step, then after a quiet drain.
//!
//! The world: [`OBJECTS`] objects, [`PEERS`] peers and [`PRINCIPALS`] local principals, of which
//! principal 0 is our own (the default tier) and the others are shared (on demand). A session
//! `(peer, principal)` may serve an object when the peer's standing on it and the principal's
//! both hold. Remotes refuse objects on sessions, and a refusal lasts until the object's standing
//! changes or the session closes.
//!
//! The checks, after every step: a leased object is routed on every open default session that may
//! serve it, and on one on-demand session (an open one if there is one) only when every default
//! session has refused it or none may serve it, never on one that refused (the laws on standing,
//! default before on-demand, and prompt routing); an idle
//! object is gone exactly at its deadline and only ever loses routes; no shared session exists
//! unless some object needs it; the network's open sessions are the machine's open and idle ones;
//! caps hold; a connected peer's default session exists and is never idle; no idle session is
//! past its grace. After a quiet drain (every dial answered, time passed, every lease dropped):
//! nothing is dialing, and only the defaults of connected peers remain.
//!
//! `SIM_TRACE=1` prints each op.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::Grace;
use super::Plant;
use super::interest::Command as IC;
use super::interest::Interest;
use super::interest::Tier;
use super::session::CallId;
use super::session::Command as SC;
use super::session::Config;
use super::session::Phase;
use super::session::Sessions;
use crate::interlude::*;
use crate::kit::Instant;
use proptest::prelude::*;

const OBJECTS: u8 = 3;
const PEERS: u8 = 3;
const PRINCIPALS: u8 = 3;
const INTEREST_GRACE: Duration = Duration::from_secs(30);
const CFG: Config = Config {
    grace: Duration::from_secs(20),
    per_peer: 2,
    total: 4,
    backoff: Duration::from_secs(1),
    max_backoff: Duration::from_secs(8),
};

type Session = (u8, u8);

#[derive(Debug, Clone)]
pub(super) enum Op {
    Acquire(u8),
    Drop(u8),
    PeerStanding(u8, u8, bool),
    LocalStanding(u8, u8, bool),
    Connect(u8),
    Disconnect(u8),
    Dial(usize, bool),
    Lose(usize),
    Refuse(u8, usize),
    Advance(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..OBJECTS).prop_map(Op::Acquire),
        3 => (0..OBJECTS).prop_map(Op::Drop),
        4 => (0..PEERS, 0..OBJECTS, any::<bool>()).prop_map(|(p, o, on)| Op::PeerStanding(p, o, on)),
        4 => (0..PRINCIPALS, 0..OBJECTS, any::<bool>()).prop_map(|(l, o, on)| Op::LocalStanding(l, o, on)),
        2 => (0..PEERS).prop_map(Op::Connect),
        2 => (0..PEERS).prop_map(Op::Disconnect),
        6 => (any::<usize>(), prop::bool::weighted(0.75)).prop_map(|(i, ok)| Op::Dial(i, ok)),
        2 => any::<usize>().prop_map(Op::Lose),
        3 => (0..OBJECTS, any::<usize>()).prop_map(|(o, i)| Op::Refuse(o, i)),
        4 => (0..5usize).prop_map(Op::Advance),
    ]
}

struct Sim {
    now: Instant,
    interest: Interest<u8, Session>,
    sessions: Sessions<u8, u8>,
    peer_standing: BTreeSet<(u8, u8)>,
    local_standing: BTreeSet<(u8, u8)>,
    connected: BTreeSet<u8>,
    leases: BTreeMap<u8, usize>,
    dropped_at: BTreeMap<u8, Instant>,
    flights: BTreeMap<CallId, Session>,
    net_open: BTreeSet<Session>,
    last_routes: BTreeMap<u8, BTreeSet<Session>>,
    /// What each object was last told its standing was.
    told: BTreeMap<u8, BTreeMap<Session, Tier>>,
    /// The refusals the fake remotes gave, until the standing changes or the session closes.
    refused: BTreeSet<(u8, Session)>,
    trace: bool,
}

impl Sim {
    fn new(plant: Option<Plant>) -> Self {
        let mut interest = Interest::new(INTEREST_GRACE);
        let mut sessions = Sessions::new(CFG);
        if let Some(plant) = plant {
            interest.plant(plant);
            sessions.plant(plant);
        }
        Self {
            now: Instant::now(),
            interest,
            sessions,
            peer_standing: default(),
            local_standing: default(),
            connected: default(),
            leases: default(),
            dropped_at: default(),
            flights: default(),
            net_open: default(),
            last_routes: default(),
            told: default(),
            refused: default(),
            trace: std::env::var_os("SIM_TRACE").is_some(),
        }
    }

    /// The fake Keyline: the sessions that may serve `object`.
    fn eligible(&self, object: u8) -> BTreeMap<Session, Tier> {
        let mut out = BTreeMap::new();
        for peer in (0..PEERS).filter(|p| self.peer_standing.contains(&(*p, object))) {
            for local in (0..PRINCIPALS).filter(|l| self.local_standing.contains(&(*l, object))) {
                out.insert(
                    (peer, local),
                    if local == 0 {
                        Tier::Default
                    } else {
                        Tier::OnDemand
                    },
                );
            }
        }
        out
    }

    /// The naive oracle of the policy: the eligible default sessions, which are all desired, and
    /// the on-demand candidates (eligible, not refused), of which one is desired, but only while
    /// every default session has refused (so, when there are none).
    fn policy(&self, object: u8) -> (BTreeSet<Session>, bool, BTreeSet<Session>) {
        let eligible = self.eligible(object);
        let of = |tier: Tier| -> BTreeSet<Session> {
            eligible
                .iter()
                .filter(|(_, t)| **t == tier)
                .map(|(s, _)| *s)
                .collect()
        };
        let defaults = of(Tier::Default);
        let fallback = defaults
            .iter()
            .all(|s| self.refused.contains(&(object, *s)));
        let candidates = of(Tier::OnDemand)
            .into_iter()
            .filter(|s| !self.refused.contains(&(object, *s)))
            .collect();
        (defaults, fallback, candidates)
    }

    fn restand(&mut self) {
        for object in 0..OBJECTS {
            if self.interest.phase(&object).is_some() {
                self.stand(object);
            }
        }
    }

    fn stand(&mut self, object: u8) {
        let eligible = self.eligible(object);
        if self.told.get(&object) != Some(&eligible) {
            self.refused.retain(|(o, _)| *o != object);
            self.told.insert(object, eligible.clone());
        }
        self.interest.standing(&object, eligible);
    }

    fn session_closed(&mut self, session: Session) {
        self.refused.retain(|(_, s)| *s != session);
        self.interest.session_closed(&session);
    }

    fn step(&mut self, op: &Op) {
        if self.trace {
            eprintln!("{op:?}");
        }
        match *op {
            Op::Acquire(o) => {
                *self.leases.entry(o).or_default() += 1;
                self.dropped_at.remove(&o);
                self.interest.lease_acquired(o);
                self.stand(o);
            }
            Op::Drop(o) => {
                if let Some(n) = self.leases.get_mut(&o) {
                    *n -= 1;
                    if *n == 0 {
                        self.leases.remove(&o);
                        self.dropped_at.insert(o, self.now);
                    }
                    self.interest.lease_dropped(&o);
                }
            }
            Op::PeerStanding(p, o, on) => {
                if on {
                    self.peer_standing.insert((p, o))
                } else {
                    self.peer_standing.remove(&(p, o))
                };
                self.restand();
            }
            Op::LocalStanding(l, o, on) => {
                if on {
                    self.local_standing.insert((l, o))
                } else {
                    self.local_standing.remove(&(l, o))
                };
                self.restand();
            }
            Op::Connect(p) => {
                if self.connected.insert(p) {
                    self.sessions.peer_connected(p, 0);
                }
            }
            Op::Disconnect(p) => {
                if self.connected.remove(&p) {
                    self.sessions.peer_disconnected(&p);
                }
            }
            Op::Dial(pick, ok) => self.dial(pick, ok),
            Op::Lose(pick) => {
                if let Some(&session) = self.net_open.iter().nth(pick % self.net_open.len().max(1))
                {
                    self.net_open.remove(&session);
                    self.sessions.closed(self.now, session.0, session.1);
                    self.session_closed(session);
                }
            }
            Op::Refuse(o, pick) => {
                if let Some(routes) = self.interest.routes(&o)
                    && let Some(&session) = routes.iter().nth(pick % routes.len().max(1))
                {
                    self.refused.insert((o, session));
                    self.interest.refused(&o, &session);
                }
            }
            Op::Advance(i) => {
                self.now += [100, 5_000, 25_000, 60_000, 600_000].map(Duration::from_millis)[i];
            }
        }
        self.settle();
        self.check();
    }

    fn dial(&mut self, pick: usize, ok: bool) {
        let Some((&call, &session)) = self.flights.iter().nth(pick % self.flights.len().max(1))
        else {
            return;
        };
        self.flights.remove(&call);
        if ok {
            self.net_open.insert(session);
            self.sessions.dialed(self.now, call, true);
            self.interest.session_opened(session);
        } else {
            self.sessions.dialed(self.now, call, false);
        }
    }

    /// Flush both machines and carry their commands until neither says anything more.
    fn settle(&mut self) {
        loop {
            let mut quiet = true;
            let mut commands = vec![];
            self.interest.flush(self.now, &mut commands);
            for command in commands {
                quiet = false;
                match command {
                    IC::NeedSession { session } => self.sessions.need(session.0, session.1),
                    IC::ReleaseSession { session } => self.sessions.release(session.0, session.1),
                    IC::AddRoute { object, session } => {
                        assert!(
                            self.net_open.contains(&session),
                            "route on a session that is not open"
                        );
                        assert!(
                            self.eligible(object).contains_key(&session),
                            "route without standing"
                        );
                    }
                    IC::RemoveRoute { .. } => {}
                }
            }
            let mut commands = vec![];
            self.sessions.flush(self.now, &mut commands);
            for command in commands {
                quiet = false;
                match command {
                    SC::Dial {
                        call,
                        peer,
                        principal,
                    } => {
                        assert!(
                            self.connected.contains(&peer),
                            "dial to a peer that is not connected"
                        );
                        assert!(
                            !self.flights.values().any(|s| *s == (peer, principal)),
                            "two dials for one session"
                        );
                        self.flights.insert(call, (peer, principal));
                    }
                    SC::Close { peer, principal } => {
                        assert!(
                            self.net_open.remove(&(peer, principal)),
                            "close of a session that was not open"
                        );
                        self.session_closed((peer, principal));
                    }
                }
            }
            if quiet {
                break;
            }
        }
    }

    fn check(&mut self) {
        let mut routes_now = BTreeMap::new();
        for object in 0..OBJECTS {
            let routes = self.interest.routes(&object).cloned();
            let (defaults, fallback, candidates) = self.policy(object);
            if self.leases.contains_key(&object) {
                let routes = routes.as_ref().expect("a leased object is known");
                let open_defaults: BTreeSet<_> = defaults
                    .iter()
                    .copied()
                    .filter(|s| self.net_open.contains(s))
                    .collect();
                let on_demand: BTreeSet<_> = routes
                    .iter()
                    .copied()
                    .filter(|s| !defaults.contains(s))
                    .collect();
                assert_eq!(
                    routes
                        .intersection(&defaults)
                        .copied()
                        .collect::<BTreeSet<_>>(),
                    open_defaults,
                    "object {object}: default routes are not the open default sessions"
                );
                if fallback {
                    assert!(
                        on_demand.len() <= 1 && on_demand.is_subset(&candidates),
                        "object {object}: on-demand routes {on_demand:?} are not one unrefused candidate"
                    );
                    assert_eq!(
                        on_demand.is_empty(),
                        !candidates.iter().any(|s| self.net_open.contains(s)),
                        "object {object}: an open on-demand candidate is not routed"
                    );
                } else {
                    assert!(
                        on_demand.is_empty(),
                        "object {object}: routed on-demand though a default session has not refused"
                    );
                }
                let needs = self.interest.needs(&object).expect("known");
                let shared = needs.iter().filter(|s| !defaults.contains(s)).count();
                assert!(
                    shared <= usize::from(fallback),
                    "object {object}: needs {needs:?}"
                );
                assert!(
                    defaults.iter().all(|s| needs.contains(s)),
                    "object {object}: a default session is not needed"
                );
            } else if let Some(at) = self.dropped_at.get(&object) {
                let held = *at + INTEREST_GRACE > self.now;
                assert_eq!(
                    routes.is_some(),
                    held,
                    "object {object}: released other than at its deadline"
                );
                if let Some(routes) = &routes {
                    let mut allowed = defaults.clone();
                    if fallback {
                        allowed.extend(candidates.iter().copied());
                    }
                    assert!(
                        routes.is_subset(&allowed),
                        "idle object {object} keeps a route it may not have"
                    );
                    assert!(
                        routes.is_subset(
                            self.last_routes
                                .get(&object)
                                .expect("an idle object had routes")
                        ),
                        "idle object {object} gained routes"
                    );
                }
            } else {
                assert!(routes.is_none(), "an object nobody ever held is known");
            }
            if let Some(routes) = routes {
                routes_now.insert(object, routes);
            }
        }
        self.last_routes = routes_now;
        self.check_sessions();
    }

    fn check_sessions(&self) {
        let (mut total, mut per_peer) = (0, BTreeMap::<u8, usize>::new());
        let mut calls = BTreeSet::new();
        let mut open = BTreeSet::new();
        for (&peer, &principal, phase) in self.sessions.sessions() {
            if phase != (Phase::Wanted { admitted: false }) {
                total += 1;
                *per_peer.entry(peer).or_default() += 1;
            }
            match phase {
                Phase::Dialing { call } => {
                    calls.insert(call);
                }
                Phase::Open => {
                    open.insert((peer, principal));
                }
                Phase::Idle(grace) => {
                    assert!(
                        matches!(grace, Grace::Until(until) if until > self.now),
                        "an idle session is not within its grace: {grace:?}"
                    );
                    open.insert((peer, principal));
                }
                Phase::Wanted { admitted: true } => assert!(self.connected.contains(&peer)),
                Phase::Wanted { admitted: false } => {}
            }
            if principal != 0 && matches!(phase, Phase::Open | Phase::Wanted { .. }) {
                assert!(
                    self.needed((peer, principal)),
                    "shared session {peer}/{principal} exists though no object needs it"
                );
            }
        }
        assert!(
            total <= CFG.total && per_peer.values().all(|n| *n <= CFG.per_peer),
            "caps exceeded"
        );
        assert_eq!(
            open, self.net_open,
            "the network's open sessions are not the machine's"
        );
        assert_eq!(calls, self.flights.keys().copied().collect::<BTreeSet<_>>());
        for &peer in &self.connected {
            let phase = self.sessions.phase(&peer, &0);
            assert!(
                phase.is_some() && !matches!(phase, Some(Phase::Idle(_))),
                "no default session toward {peer}"
            );
        }
    }

    /// Whether the policy may want the session: a leased object may ask for it as its default or
    /// on-demand candidate, or an idle object still holds a route on it.
    fn needed(&self, session: Session) -> bool {
        (0..OBJECTS).any(|object| {
            if self.leases.contains_key(&object) {
                let (defaults, fallback, candidates) = self.policy(object);
                defaults.contains(&session) || (fallback && candidates.contains(&session))
            } else {
                self.interest
                    .routes(&object)
                    .is_some_and(|r| r.contains(&session))
            }
        })
    }

    /// Answer every dial, let time pass, and check what stands; then drop every lease and check
    /// that only the defaults of connected peers remain.
    fn quiesce(&mut self) {
        for _ in 0..16 {
            while !self.flights.is_empty() {
                self.dial(0, true);
                self.settle();
                self.check();
            }
            self.now += Duration::from_secs(10);
            self.settle();
            self.check();
        }
        assert!(self.flights.is_empty());
        let (total, mut per_peer) = (self.slots(), BTreeMap::<u8, usize>::new());
        for (&peer, _, phase) in self.sessions.sessions() {
            if phase != (Phase::Wanted { admitted: false }) {
                *per_peer.entry(peer).or_default() += 1;
            }
        }
        for (&peer, &principal, phase) in self.sessions.sessions() {
            if phase == (Phase::Wanted { admitted: false }) && self.connected.contains(&peer) {
                assert!(
                    total >= CFG.total || per_peer.get(&peer).copied().unwrap_or(0) >= CFG.per_peer,
                    "wanted session {peer}/{principal} not admitted though the caps have room"
                );
            } else if matches!(
                phase,
                Phase::Dialing { .. } | Phase::Wanted { admitted: true }
            ) {
                panic!("session {peer}/{principal} is {phase:?} after the drain");
            }
        }
        for object in self.leases.keys().copied().collect::<Vec<_>>() {
            while self.leases.contains_key(&object) {
                self.step(&Op::Drop(object));
            }
        }
        // the objects are released after their grace, which releases the sessions, which then
        // close after theirs
        for _ in 0..2 {
            self.now += Duration::from_secs(3_600);
            self.settle();
            self.check();
        }
        assert!(
            self.interest.is_empty(),
            "interest holds something with no lease"
        );
        for (&peer, &principal, phase) in self.sessions.sessions() {
            assert!(
                principal == 0
                    && self.connected.contains(&peer)
                    && !matches!(phase, Phase::Idle(_)),
                "session {peer}/{principal} outlived its grace: {phase:?}"
            );
        }
    }

    fn slots(&self) -> usize {
        self.sessions
            .sessions()
            .filter(|(_, _, p)| *p != (Phase::Wanted { admitted: false }))
            .count()
    }
}

pub(super) fn run(ops: &[Op], plant: Option<Plant>) {
    let mut sim = Sim::new(plant);
    for op in ops {
        sim.step(op);
    }
    sim.quiesce();
}

#[cfg(test)]
mod cases;
