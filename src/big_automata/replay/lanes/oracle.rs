//! The dumbest model of `Lanes`: flat vectors scanned linearly, and backoff recomputed from a
//! count of consecutive failures instead of a ladder carried between them. It answers what
//! each lane holds, which rounds a flush emits and when the next deadline is.

use super::*;

/// One round, as the oracle names it. The targets are routes with their cursors: the oracle
/// knows nothing of target ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ORound {
    pub call: CallId,
    pub lane: Lane,
    pub targets: BTreeSet<(Route, Rev)>,
    pub limit: u32,
    pub hold_ms: u32,
}

#[derive(Debug, Default, Clone)]
struct OLane {
    /// The call in flight, the routes it names and its row limit.
    flight: Option<(CallId, Vec<Route>, usize)>,
    /// Consecutive failures since the last answer.
    fails: u32,
    /// A failure waits for the next flush to read the clock.
    unarmed: bool,
    open_at: Option<Instant>,
}

#[derive(Debug)]
pub(super) struct Oracle {
    config: Config,
    members: Vec<(Route, Lane)>,
    live: OLane,
    bulk: OLane,
    next_call: CallId,
    last_flush: Option<Instant>,
    /// When `Bulk` first had a round with fewer than `bulk_batch` rows to send.
    bulk_waiting: Option<Instant>,
}

impl Oracle {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            members: vec![],
            live: default(),
            bulk: default(),
            next_call: 0,
            last_flush: None,
            bulk_waiting: None,
        }
    }

    fn lane(&self, lane: Lane) -> &OLane {
        match lane {
            Lane::Live => &self.live,
            Lane::Bulk => &self.bulk,
        }
    }

    fn lane_mut(&mut self, lane: Lane) -> &mut OLane {
        match lane {
            Lane::Live => &mut self.live,
            Lane::Bulk => &mut self.bulk,
        }
    }

    pub fn lane_of(&self, route: &Route) -> Option<Lane> {
        self.members
            .iter()
            .find(|(member, _)| member == route)
            .map(|(_, lane)| *lane)
    }

    pub fn acked(&mut self, route: Route) {
        if self.lane_of(&route).is_some() {
            return;
        }
        let lane = if matches!(route, Route::Object { .. }) {
            Lane::Live
        } else {
            Lane::Bulk
        };
        self.members.push((route, lane));
    }

    pub fn dropped(&mut self, route: &Route) {
        self.members.retain(|(member, _)| member != route);
    }

    pub fn reset(&mut self) {
        self.members.clear();
        self.live = default();
        self.bulk = default();
        self.bulk_waiting = None;
    }

    fn call_lane(&self, call: CallId) -> Option<Lane> {
        [Lane::Live, Lane::Bulk]
            .into_iter()
            .find(|lane| matches!(&self.lane(*lane).flight, Some((id, _, _)) if *id == call))
    }

    pub fn answered(&mut self, call: CallId, outcomes: &[(TargetId, Route, TargetOutcome)]) {
        let Some(lane) = self.call_lane(call) else {
            return;
        };
        let state = self.lane_mut(lane);
        state.flight = None;
        state.fails = 0;
        state.unarmed = false;
        state.open_at = None;
        for (_, route, outcome) in outcomes {
            if self.lane_of(route).is_none() {
                continue;
            }
            match outcome {
                TargetOutcome::Refused => self.dropped(route),
                TargetOutcome::Advance { drained, .. } => {
                    if matches!(route, Route::Part { .. }) {
                        let to = if *drained { Lane::Live } else { Lane::Bulk };
                        for (member, lane) in &mut self.members {
                            if member == route {
                                *lane = to;
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn failed(&mut self, call: CallId) {
        let Some(lane) = self.call_lane(call) else {
            return;
        };
        let state = self.lane_mut(lane);
        state.flight = None;
        state.fails += 1;
        state.unarmed = true;
        state.open_at = None;
    }

    pub fn stale(&mut self, call: CallId) {
        if let Some(lane) = self.call_lane(call) {
            self.lane_mut(lane).flight = None;
        }
    }

    /// `min * 2^(fails - 1)`, capped at `max`.
    fn delay(&self, fails: u32) -> Duration {
        let mut delay = self.config.min_backoff.min(self.config.max_backoff);
        for _ in 1..fails {
            delay = (delay * 2).min(self.config.max_backoff);
        }
        delay
    }

    fn is_open(&self, lane: Lane, now: Instant) -> bool {
        let state = self.lane(lane);
        !state.unarmed && state.open_at.is_none_or(|at| at <= now)
    }

    /// `acked` is what the client holds acknowledged; `cursor_of` the parent's cursors.
    pub fn flush(
        &mut self,
        now: Instant,
        acked: &[Route],
        window: usize,
        cursor_of: &dyn Fn(&Route) -> Option<Rev>,
    ) -> Vec<ORound> {
        self.last_flush = Some(now);
        for lane in [Lane::Live, Lane::Bulk] {
            if self.lane(lane).unarmed {
                let delay = self.delay(self.lane(lane).fails);
                let state = self.lane_mut(lane);
                state.unarmed = false;
                state.open_at = Some(now + delay);
            }
        }
        let mut rounds = vec![];
        let mut used = 0;
        for lane in [Lane::Live, Lane::Bulk] {
            if let Some((_, _, limit)) = &self.lane(lane).flight {
                used += limit;
            }
        }
        for lane in [Lane::Live, Lane::Bulk] {
            let left = window.saturating_sub(used);
            if left == 0 || self.lane(lane).flight.is_some() || !self.is_open(lane, now) {
                continue;
            }
            let limit = if lane == Lane::Live {
                left.min(self.config.live_limit)
            } else {
                left
            };
            let other = match lane {
                Lane::Live => Lane::Bulk,
                Lane::Bulk => Lane::Live,
            };
            let named_elsewhere = self
                .lane(other)
                .flight
                .as_ref()
                .map(|(_, routes, _)| routes.clone())
                .unwrap_or_default();
            let targets: BTreeSet<(Route, Rev)> = self
                .members
                .iter()
                .filter(|(route, have)| {
                    *have == lane && acked.contains(route) && !named_elsewhere.contains(route)
                })
                .filter_map(|(route, _)| cursor_of(route).map(|cursor| (route.clone(), cursor)))
                .collect();
            if targets.is_empty() {
                if lane == Lane::Bulk {
                    self.bulk_waiting = None;
                }
                continue;
            }
            if lane == Lane::Bulk {
                if limit < self.config.bulk_batch {
                    if self.bulk_waiting.is_none() {
                        self.bulk_waiting = Some(now);
                    }
                    if now < self.bulk_waiting.unwrap() + self.config.bulk_linger {
                        continue;
                    }
                }
                self.bulk_waiting = None;
            }
            let call = self.next_call;
            self.next_call += 1;
            let routes = targets.iter().map(|(route, _)| route.clone()).collect();
            used += limit;
            self.lane_mut(lane).flight = Some((call, routes, limit));
            rounds.push(ORound {
                call,
                lane,
                targets,
                limit: u32::try_from(limit).unwrap(),
                hold_ms: if lane == Lane::Live {
                    self.config.hold_ms
                } else {
                    0
                },
            });
        }
        rounds
    }

    pub fn next_due(&self) -> Option<Instant> {
        let last = self.last_flush?;
        [Lane::Live, Lane::Bulk]
            .into_iter()
            .filter(|lane| self.members.iter().any(|(_, have)| have == lane))
            .filter(|lane| self.lane(*lane).flight.is_none())
            .filter_map(|lane| self.lane(lane).open_at)
            .chain(
                self.bulk_waiting
                    .filter(|_| self.bulk.flight.is_none())
                    .map(|since| since + self.config.bulk_linger),
            )
            .filter(|at| *at > last)
            .min()
    }

    /// Calls in flight, by lane.
    pub fn flights(&self) -> [Option<(CallId, Vec<Route>, usize)>; 2] {
        [self.live.flight.clone(), self.bulk.flight.clone()]
    }
}
