//! The differential proptest: random acknowledgements, drops, answers (for current and stale
//! calls), failures, resets, time and flushes against [`Lanes`] and the [`Oracle`], with the
//! per-step properties checked on the way.

use super::*;

use crate::replay::lanes::oracle::ORound;
use crate::replay::lanes::oracle::Oracle;
use proptest::prelude::*;

const ROUTES: usize = 5;

/// A cap one doubling above the floor, so the cap binds on the second failure and waits of 0..4
/// seconds straddle both deadlines.
fn sim_config() -> Config {
    Config {
        max_backoff: Duration::from_secs(3),
        bulk_batch: 3,
        ..config()
    }
}

/// Two object routes, then three part routes.
fn route(index: usize) -> Route {
    if index < 2 {
        obj_route(index as u8)
    } else {
        part_route(index as u8)
    }
}

#[derive(Debug, Clone)]
enum Op {
    Ack(usize),
    Drop(usize),
    /// Drop and acknowledge again at once: the route changes lane while a round may carry it.
    Churn(usize),
    /// Answer an issued call, current or stale. Per route the call named: 0 absent from the
    /// answer, 1 drained, 2 advanced without draining, 3 refused.
    Answer {
        pick: usize,
        outcomes: [u8; ROUTES],
    },
    Fail {
        pick: usize,
    },
    Stale {
        pick: usize,
    },
    Reset,
    Wait(u8),
    Flush {
        window: usize,
        cursors: [Option<u8>; ROUTES],
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..ROUTES).prop_map(Op::Ack),
        2 => (0..ROUTES).prop_map(Op::Drop),
        3 => (0..ROUTES).prop_map(Op::Churn),
        7 => (any::<usize>(), proptest::array::uniform5(0u8..4))
            .prop_map(|(pick, outcomes)| Op::Answer { pick, outcomes }),
        3 => any::<usize>().prop_map(|pick| Op::Fail { pick }),
        2 => any::<usize>().prop_map(|pick| Op::Stale { pick }),
        1 => Just(Op::Reset),
        3 => (0u8..4).prop_map(Op::Wait),
        8 => (0usize..6, proptest::array::uniform5(proptest::option::weighted(0.85, 0u8..4)))
            .prop_map(|(window, cursors)| Op::Flush { window, cursors }),
    ]
}

struct Harness {
    lanes: Lanes,
    oracle: Oracle,
    client: ReplayClient,
    now: Instant,
    /// Every call issued, with the routes it named: what answers are made from.
    issued: Vec<(CallId, Vec<Route>)>,
    /// The call in flight per lane as this harness saw it issued and finished, independent of
    /// what the machines say. `[Live, Bulk]`.
    outstanding: [Option<(CallId, BTreeSet<Route>)>; 2],
}

impl Harness {
    fn new() -> Self {
        Self {
            lanes: Lanes::new(sim_config()),
            oracle: Oracle::new(sim_config()),
            client: ReplayClient::new(Uuid::from_u128(1)),
            now: Instant::now(),
            issued: vec![],
            outstanding: [None, None],
        }
    }

    fn slot(lane: Lane) -> usize {
        match lane {
            Lane::Live => 0,
            Lane::Bulk => 1,
        }
    }

    fn acked_routes(&self) -> Vec<Route> {
        self.client
            .acked()
            .map(|(_, route)| route.clone())
            .collect()
    }

    fn finish(&mut self, call: CallId) {
        for slot in &mut self.outstanding {
            if slot.as_ref().is_some_and(|(have, _)| *have == call) {
                *slot = None;
            }
        }
    }

    fn step(&mut self, op: Op) {
        match op {
            Op::Ack(index) => {
                let route = route(index);
                if !self.acked_routes().contains(&route) {
                    self.client.want(route.clone());
                    let update = self.client.next_update().unwrap().clone();
                    let answer = Ok(SessionUpdated {
                        generation: update.generation,
                        refused: vec![],
                    });
                    self.client
                        .update_answered(&update, &answer, &mut vec![])
                        .unwrap();
                }
                self.lanes.acked(route.clone());
                self.oracle.acked(route);
            }
            Op::Drop(index) => {
                let route = route(index);
                self.client.unwant(&route);
                self.lanes.dropped(&route);
                self.oracle.dropped(&route);
            }
            Op::Churn(index) => {
                self.step(Op::Drop(index));
                self.step(Op::Ack(index));
            }
            Op::Answer { pick, outcomes } => {
                if self.issued.is_empty() {
                    return;
                }
                let (call, named) = self.issued[pick % self.issued.len()].clone();
                let answer: Vec<_> = named
                    .iter()
                    .filter_map(|named| {
                        let index = (0..ROUTES).find(|i| route(*i) == *named).unwrap();
                        match outcomes[index] {
                            1 => Some(advance(named.clone(), true)),
                            2 => Some(advance(named.clone(), false)),
                            3 => Some(refused(named.clone())),
                            _ => None,
                        }
                    })
                    .collect();
                self.lanes.answered(call, &answer);
                self.oracle.answered(call, &answer);
                self.finish(call);
            }
            Op::Fail { pick } => {
                if self.issued.is_empty() {
                    return;
                }
                let call = self.issued[pick % self.issued.len()].0;
                self.lanes.failed(call);
                self.oracle.failed(call);
                self.finish(call);
            }
            Op::Stale { pick } => {
                if self.issued.is_empty() {
                    return;
                }
                let call = self.issued[pick % self.issued.len()].0;
                self.lanes.stale(call);
                self.oracle.stale(call);
                self.finish(call);
            }
            Op::Reset => {
                for index in 0..ROUTES {
                    self.client.unwant(&route(index));
                }
                self.lanes.reset();
                self.oracle.reset();
                self.outstanding = [None, None];
            }
            Op::Wait(secs) => self.now += Duration::from_secs(secs.into()),
            Op::Flush { window, cursors } => self.flush(window, cursors),
        }
        self.check();
    }

    fn flush(&mut self, window: usize, cursors: [Option<u8>; ROUTES]) {
        let cursor_of = |route: &Route| {
            (0..ROUTES)
                .find(|index| self::route(*index) == *route)
                .and_then(|index| cursors[index])
                .map(Rev::from)
        };
        let mut out = vec![];
        self.lanes
            .flush(self.now, &self.client, window, cursor_of, &mut out);
        let expected = self
            .oracle
            .flush(self.now, &self.acked_routes(), window, &cursor_of);
        let rounds: Vec<ORound> = out
            .iter()
            .map(|cmd| {
                let LanesCommand::Round { call, lane, page } = cmd;
                let targets = page
                    .targets
                    .iter()
                    .map(|(id, cursor)| (self.client.route_of(*id).unwrap().clone(), *cursor))
                    .collect();
                ORound {
                    call: *call,
                    lane: *lane,
                    targets,
                    limit: page.limit,
                    hold_ms: page.hold_ms,
                }
            })
            .collect();
        assert_eq!(rounds, expected, "rounds differ from the oracle's");

        // Properties, independent of the oracle.
        assert!(window > 0 || rounds.is_empty(), "a round at window 0");
        if !rounds.is_empty() {
            assert!(
                self.lanes.in_flight_limit() <= window,
                "rounds in flight ask for more than the window"
            );
        }
        for lane in [Lane::Live, Lane::Bulk] {
            let mine: Vec<_> = rounds.iter().filter(|round| round.lane == lane).collect();
            assert!(mine.len() <= 1, "two rounds of one lane in one flush");
            if let Some(round) = mine.first() {
                assert!(
                    self.outstanding[Self::slot(lane)].is_none(),
                    "a second round in flight in {lane:?}"
                );
                assert!(!round.targets.is_empty());
                assert!(u64::from(round.limit) <= window as u64);
                for (route, cursor) in &round.targets {
                    assert_eq!(cursor_of(route), Some(*cursor));
                    assert_eq!(
                        self.lanes.lane_of(route),
                        Some(lane),
                        "a round names a route of the other lane"
                    );
                }
                if lane == Lane::Bulk {
                    assert_eq!(round.hold_ms, 0, "a Bulk round holds");
                    assert!(
                        round
                            .targets
                            .iter()
                            .all(|(route, _)| matches!(route, Route::Part { .. })),
                        "an object route in Bulk"
                    );
                } else {
                    assert_eq!(round.hold_ms, sim_config().hold_ms);
                }
                self.issued.push((
                    round.call,
                    round.targets.iter().map(|(r, _)| r.clone()).collect(),
                ));
                self.outstanding[Self::slot(lane)] = Some((
                    round.call,
                    round.targets.iter().map(|(r, _)| r.clone()).collect(),
                ));
            }
        }
        if rounds.len() == 2 {
            assert_eq!(rounds[0].lane, Lane::Live, "Live is served first");
        }
    }

    fn check(&self) {
        for index in 0..ROUTES {
            let route = route(index);
            let lane = self.lanes.lane_of(&route);
            assert_eq!(lane, self.oracle.lane_of(&route), "lane of {route:?}");
            if matches!(route, Route::Object { .. }) {
                assert_ne!(lane, Some(Lane::Bulk), "an object route in Bulk");
            }
        }
        let oracle_flights = self.oracle.flights();
        for lane in [Lane::Live, Lane::Bulk] {
            let have = self
                .lanes
                .flight(lane)
                .map(|(call, routes)| (call, routes.clone()));
            let want = oracle_flights[Self::slot(lane)]
                .clone()
                .map(|(call, routes, _)| (call, routes.into_iter().collect::<BTreeSet<_>>()));
            assert_eq!(have, want, "flight of {lane:?}");
            assert_eq!(
                have,
                self.outstanding[Self::slot(lane)].clone(),
                "flight of {lane:?} against the harness's own record"
            );
        }
        if let [Some((_, live)), Some((_, bulk))] = &self.outstanding {
            assert!(live.is_disjoint(bulk), "a route in both rounds in flight");
        }
        assert_eq!(self.lanes.next_due(), self.oracle.next_due(), "next_due");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn lanes_match_the_oracle(ops in proptest::collection::vec(op(), 0..120)) {
        let mut harness = Harness::new();
        for op in ops {
            harness.step(op);
        }
    }
}
