//! Replay lanes: which acknowledged routes go in which round, and how long a round may wait.
//!
//! Latency and throughput pull against each other on one session. A part catching up on a
//! large backlog wants big pages back to back; a caught-up route wants its next change the
//! moment it is written. One round over both makes the caught-up route wait behind the
//! backlog's pages, and a held round (the server waits for rows) makes the backlog wait for
//! nothing. So rounds run in two lanes on the same session, concurrently: the protocol keeps
//! no cursor on the server, so two rounds over disjoint targets never interfere (replay laws
//! 6-9).
//!
//! Sans-IO. The parent tells it which routes are acknowledged and what each answer said; it
//! answers which routes the next round of each lane names and with which hold.
//!
//! # Laws
//!
//! 1. **Every acknowledged route is in exactly one lane.** An object route is always `Live`. A
//!    part route enters `Bulk`, moves to `Live` when an answer says it is drained, and back
//!    to `Bulk` when an answer advances it without draining it (it fell behind again).
//! 2. **A round names only its lane's routes**, and every one of them that the parent has a
//!    cursor for. A route never appears in both lanes' rounds in flight at once.
//! 3. **At most one round per lane in flight.** A lane's next round goes out only after its
//!    previous one is answered, failed or lost.
//! 4. **Holds.** A `Live` round may wait up to `Config::hold` for rows; a `Bulk` round never
//!    waits (`hold_ms = 0`). A `Live` round also asks the server to linger after its first row
//!    (`Config::live_linger_ms`); a `Bulk` round asks for none unless `Config::bulk_linger_ms`
//!    says so.
//! 5. **Moving lanes takes effect at the next round.** A route that changes lane while a
//!    round naming it is in flight stays in that round; its answer still applies (the parent
//!    matches answers to the cursor the round was asked from, replay law 11).
//! 6. **Backpressure.** The row limits of the rounds in flight never add up to more than the
//!    parent's receive window (an answer may still overshoot its limit by the rows of one
//!    revision, which a page never splits): a round goes out only into what the window has left after the
//!    rounds already in flight, and not at all when nothing is left. `Live` is served first
//!    and takes at most `Config::live_limit` rows; `Bulk` gets the rest, so a held `Live`
//!    round never starves the backlog of the window.
//! 9. **`Bulk` batches, with a linger.** A `Bulk` round goes out once at least
//!    `Config::bulk_batch` rows of the window are left for it, or once it has waited
//!    `Config::bulk_linger` since it first could have gone out with fewer. The parent frees the
//!    window one settled object at a time; without this the lane would ask for the one free
//!    row, and the rows freed while that round is in flight would wait for it, so rounds would
//!    alternate between one row and nearly the whole window. The linger keeps the lane moving
//!    when long fetches hold most of the window for hours.
//! 7. **A failed or lost round backs its lane off** from `min_backoff`, doubling, capped at
//!    `max_backoff`, reset by an answered round. The other lane is not delayed. A round the
//!    client calls stale (`RoundOutcome::Stale`: answered for an older session, or racing a
//!    route the client dropped) frees its lane at once, with its backoff and gate unchanged.
//! 8. **A reset session clears both lanes.** Routes re-enter by law 1 once acknowledged
//!    again.
//!
//! # What the simulation checks on top
//!
//! - Latency: while a part drains a backlog of any size, a change to a `Live` route is
//!   applied within a bounded number of `Live` rounds. Merging the lanes must fail this.
//! - Throughput: a backlog of `n` rows drains in about `n / limit` `Bulk` rounds.
//!
//! # Costs
//!
//! `acked`, `dropped` and the answer to a route are O(log routes). `flush` is O(routes) per
//! lane that is due, and nothing for a lane in flight or backing off.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::kit::Instant;
use crate::replay::client::ReplayClient;
use crate::replay::client::TargetOutcome;
use crate::replay::wire::PageSession;
use crate::replay::wire::Route;
use crate::replay::wire::TargetId;
use crate::rev::Rev;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub(crate) type CallId = u64;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// How long a `Live` round may wait for rows.
    pub hold_ms: u32,
    /// The linger a `Live` round asks of the server after its first row (replay law 13), or
    /// `None` to ask for none. The server caps it.
    pub live_linger_ms: Option<u32>,
    /// The linger a `Bulk` round asks for. `Bulk` has no latency to protect, so the default is
    /// `None`: its rounds answer as soon as they have a row.
    pub bulk_linger_ms: Option<u32>,
    /// The most rows a `Live` round asks for; `Bulk` gets the rest of the window (law 6).
    pub live_limit: usize,
    /// The rows a `Bulk` round waits for, up to `bulk_linger` (law 9).
    pub bulk_batch: usize,
    pub bulk_linger: Duration,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Lane {
    /// Caught up (or an object route): rounds may wait for rows.
    Live,
    /// Behind: back-to-back pages that never wait.
    Bulk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LanesCommand {
    /// Send `page` as one round of `lane`, and report its answer or loss under `call`.
    Round {
        call: CallId,
        lane: Lane,
        page: PageSession,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    Open,
    /// A failure waits for the next flush to read the clock and fix the deadline.
    Unarmed,
    Until(Instant),
}

#[derive(Debug)]
struct Flight {
    call: CallId,
    /// The routes the round named. Moving or re-acknowledging a route does not touch this: the
    /// round carries what it was built with (law 5).
    routes: BTreeSet<Route>,
    /// The rows the round asked for, held against the window until it ends (law 6).
    limit: usize,
}

#[derive(Debug)]
struct LaneState {
    flight: Option<Flight>,
    backoff: Duration,
    gate: Gate,
}

impl Default for LaneState {
    fn default() -> Self {
        Self {
            flight: None,
            backoff: Duration::ZERO,
            gate: Gate::Open,
        }
    }
}

impl LaneState {
    fn gate_open(&self, now: Instant) -> bool {
        match self.gate {
            Gate::Open => true,
            Gate::Unarmed => false,
            Gate::Until(at) => at <= now,
        }
    }

    /// The first failure waits the floor, later ones double, the cap wins.
    fn fail(&mut self, config: &Config) {
        self.flight = None;
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
        self.flight = None;
        self.backoff = Duration::ZERO;
        self.gate = Gate::Open;
    }
}

#[derive(Debug)]
pub(crate) struct Lanes {
    config: Config,
    /// Every acknowledged route and its lane (law 1). Cursors and target ids are the parent's
    /// and the client's: nothing here copies them.
    routes: BTreeMap<Route, Lane>,
    live: LaneState,
    bulk: LaneState,
    next_call: CallId,
    last_flush: Option<Instant>,
    /// Since when `Bulk` has had a round to send with fewer than `bulk_batch` rows (law 9).
    bulk_waiting: Option<Instant>,
}

impl Lanes {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            routes: default(),
            live: default(),
            bulk: default(),
            next_call: 0,
            last_flush: None,
            bulk_waiting: None,
        }
    }

    fn state(&self, lane: Lane) -> &LaneState {
        match lane {
            Lane::Live => &self.live,
            Lane::Bulk => &self.bulk,
        }
    }

    fn state_mut(&mut self, lane: Lane) -> &mut LaneState {
        match lane {
            Lane::Live => &mut self.live,
            Lane::Bulk => &mut self.bulk,
        }
    }

    /// The server acknowledged `route`: an object route joins `Live`, a part route `Bulk`.
    /// A route already acknowledged keeps its lane.
    pub fn acked(&mut self, route: Route) {
        let lane = match route {
            Route::Object { .. } => Lane::Live,
            Route::Part { .. } => Lane::Bulk,
        };
        self.routes.entry(route).or_insert(lane);
    }

    /// `route` is no longer acknowledged (unwanted, or refused). A round already naming it
    /// keeps naming it until it is answered or fails.
    pub fn dropped(&mut self, route: &Route) {
        self.routes.remove(route);
    }

    /// The session was reset: both lanes are empty, nothing is in flight (the old session's
    /// answers are stale to the client) and no backoff carries over, since a lost session is
    /// not a failing lane. Routes re-enter through [`Lanes::acked`].
    pub fn reset(&mut self) {
        self.routes.clear();
        self.live = default();
        self.bulk = default();
        self.bulk_waiting = None;
    }

    /// Take the answer to round `call`. An answer for a call that is not in flight (failed,
    /// reset away, or never issued) changes nothing. Otherwise the lane's backoff is reset, and
    /// each acknowledged part route moves by its outcome (law 1): drained to `Live`, advanced
    /// without draining to `Bulk`. A refused route is no longer acknowledged and leaves.
    pub fn answered(&mut self, call: CallId, outcomes: &[(TargetId, Route, TargetOutcome)]) {
        let Some(lane) = self.lane_of_call(call) else {
            return;
        };
        self.state_mut(lane).succeed();
        for (_, route, outcome) in outcomes {
            match (route, outcome) {
                (_, TargetOutcome::Refused) => {
                    self.routes.remove(route);
                }
                (Route::Object { .. }, TargetOutcome::Advance { .. }) => {}
                (Route::Part { .. }, TargetOutcome::Advance { drained, .. }) => {
                    if let Some(current) = self.routes.get_mut(route) {
                        *current = if *drained { Lane::Live } else { Lane::Bulk };
                    }
                }
            }
        }
    }

    /// Round `call` failed or was lost: its lane backs off (law 7). Stale calls change nothing.
    pub fn failed(&mut self, call: CallId) {
        let Some(lane) = self.lane_of_call(call) else {
            return;
        };
        let config = self.config.clone();
        self.state_mut(lane).fail(&config);
    }

    /// Round `call` was answered stale: its lane is free for the repeat at once, with no
    /// change to its backoff (law 7). Stale calls change nothing.
    pub fn stale(&mut self, call: CallId) {
        let Some(lane) = self.lane_of_call(call) else {
            return;
        };
        self.state_mut(lane).flight = None;
    }

    fn lane_of_call(&self, call: CallId) -> Option<Lane> {
        [Lane::Live, Lane::Bulk].into_iter().find(|lane| {
            self.state(*lane)
                .flight
                .as_ref()
                .is_some_and(|flight| flight.call == call)
        })
    }

    /// Emit at most one round per due lane, `Live` first. `window` is the parent's receive
    /// window; rounds in flight hold their limits against it, and a new round asks for at most
    /// what is left (law 6).
    /// `cursor_of` is the parent's cursor for a route; a route without one stays out of the
    /// round. Rounds are built by `client`, which names the targets it holds acknowledged.
    pub fn flush(
        &mut self,
        now: Instant,
        client: &ReplayClient,
        window: usize,
        mut cursor_of: impl FnMut(&Route) -> Option<Rev>,
        out: &mut Vec<LanesCommand>,
    ) {
        self.last_flush = Some(now);
        for lane in [&mut self.live, &mut self.bulk] {
            if lane.gate == Gate::Unarmed {
                lane.gate = Gate::Until(now + lane.backoff);
            }
        }
        let in_flight: usize = [&self.live, &self.bulk]
            .into_iter()
            .filter_map(|lane| lane.flight.as_ref())
            .map(|flight| flight.limit)
            .sum();
        let mut available = window.saturating_sub(in_flight);
        for lane in [Lane::Live, Lane::Bulk] {
            let state = self.state(lane);
            if available == 0 || state.flight.is_some() || !state.gate_open(now) {
                continue;
            }
            let limit = match lane {
                Lane::Live => available.min(self.config.live_limit),
                Lane::Bulk => available,
            };
            let other = match lane {
                Lane::Live => &self.bulk,
                Lane::Bulk => &self.live,
            };
            let hold_ms = match lane {
                Lane::Live => self.config.hold_ms,
                Lane::Bulk => 0,
            };
            let linger_ms = match lane {
                Lane::Live => self.config.live_linger_ms,
                Lane::Bulk => self.config.bulk_linger_ms,
            };
            // A route in the other lane's round in flight waits for it (law 2), whichever
            // lane it is in now.
            let page = client.round(
                |route| {
                    let named_elsewhere = other
                        .flight
                        .as_ref()
                        .is_some_and(|flight| flight.routes.contains(route));
                    if self.routes.get(route) != Some(&lane) || named_elsewhere {
                        return None;
                    }
                    cursor_of(route)
                },
                u32::try_from(limit).unwrap_or(u32::MAX),
                hold_ms,
                linger_ms,
            );
            let Some(page) = page else {
                if lane == Lane::Bulk {
                    self.bulk_waiting = None;
                }
                continue;
            };
            if lane == Lane::Bulk {
                if limit < self.config.bulk_batch {
                    let since = *self.bulk_waiting.get_or_insert(now);
                    if now < since + self.config.bulk_linger {
                        continue;
                    }
                }
                self.bulk_waiting = None;
            }
            let call = self.next_call;
            self.next_call = call.checked_add(1).expect(ERROR_IMPOSSIBLE);
            let routes = page
                .targets
                .iter()
                .map(|(id, _)| client.route_of(*id).expect(ERROR_IMPOSSIBLE).clone())
                .collect();
            available -= limit;
            self.state_mut(lane).flight = Some(Flight {
                call,
                routes,
                limit,
            });
            out.push(LanesCommand::Round { call, lane, page });
        }
    }

    /// The earliest backoff deadline after the last flush of an idle lane that has routes, or
    /// the end of `Bulk`'s linger (law 9).
    pub fn next_due(&self) -> Option<Instant> {
        let last = self.last_flush?;
        let linger = self
            .bulk_waiting
            .filter(|_| self.bulk.flight.is_none())
            .map(|since| since + self.config.bulk_linger)
            .filter(|at| *at > last);
        [Lane::Live, Lane::Bulk]
            .into_iter()
            .filter(|lane| self.routes.values().any(|have| have == lane))
            .map(|lane| self.state(lane))
            .filter(|state| state.flight.is_none())
            .filter_map(|state| match state.gate {
                Gate::Until(at) if at > last => Some(at),
                _ => None,
            })
            .chain(linger)
            .min()
    }

    /// The rows the rounds in flight hold against the window (law 6).
    pub(crate) fn in_flight_limit(&self) -> usize {
        [&self.live, &self.bulk]
            .into_iter()
            .filter_map(|lane| lane.flight.as_ref())
            .map(|flight| flight.limit)
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn lane_of(&self, route: &Route) -> Option<Lane> {
        self.routes.get(route).copied()
    }

    #[cfg(test)]
    pub(crate) fn flight(&self, lane: Lane) -> Option<(CallId, &BTreeSet<Route>)> {
        self.state(lane)
            .flight
            .as_ref()
            .map(|flight| (flight.call, &flight.routes))
    }
}
