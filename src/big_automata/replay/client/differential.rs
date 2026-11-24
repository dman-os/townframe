//! The client's route bookkeeping against the dumbest model: the wanted set. The routes are
//! churned one at a time (redundant calls included) between updates sent, lost and answered by a
//! naive server, and after every operation the client must follow exactly the model's routes
//! with ids that are unique, already issued and never both live and queued for removal. Once the
//! traffic drains, the server must hold exactly the acknowledged targets, and those and the
//! blocked routes must be the model.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::*;

use super::*;
use crate::part_store::laws::part;

const ROUTES: u8 = 4;

fn route(index: u8) -> Route {
    Route::Part {
        part_key: part(index % ROUTES),
    }
}

#[derive(Debug, Clone)]
enum Op {
    Want(u8),
    Unwant(u8),
    /// Toggle whether the server refuses additions of a route.
    Refuse(u8),
    /// Hand the update due to the server, which answers it as law 2 says; the answer waits.
    Send,
    /// The oldest waiting answer reaches the client.
    Answer,
    /// The client hears the update it last sent was lost.
    Lose,
    RetryBlocked,
}

/// The server as a map: what an update leaves held, with law 2's repeat answer.
#[derive(Default)]
struct NaiveServer {
    held: BTreeMap<TargetId, Route>,
    refusing: BTreeSet<Route>,
    applied: Option<(Generation, Vec<TargetId>)>,
}

impl NaiveServer {
    fn apply(&mut self, update: &UpdateSession) -> Vec<TargetId> {
        if let Some((generation, refused)) = &self.applied
            && *generation >= update.generation
        {
            return refused.clone();
        }
        for id in &update.removals {
            self.held.remove(id);
        }
        let mut refused = Vec::new();
        for (id, route) in &update.additions {
            if self.refusing.contains(route) {
                refused.push(*id);
            } else {
                assert!(self.held.insert(*id, route.clone()).is_none(), "id reused");
            }
        }
        self.applied = Some((update.generation, refused.clone()));
        refused
    }
}

struct Harness {
    client: ReplayClient,
    server: NaiveServer,
    /// The oracle: the routes the embedder wants.
    wanted: BTreeSet<Route>,
    last_sent: Option<UpdateSession>,
    answers: Vec<(UpdateSession, Result<SessionUpdated, SessionError>)>,
}

impl Harness {
    fn new() -> Self {
        Self {
            client: ReplayClient::new(Uuid::from_u128(1)),
            server: default(),
            wanted: default(),
            last_sent: None,
            answers: default(),
        }
    }

    fn step(&mut self, op: Op) {
        match op {
            Op::Want(index) => {
                self.wanted.insert(route(index));
                self.client.want(route(index));
            }
            Op::Unwant(index) => {
                self.wanted.remove(&route(index));
                self.client.unwant(&route(index));
            }
            Op::Refuse(index) => {
                let route = route(index);
                if !self.server.refusing.remove(&route) {
                    self.server.refusing.insert(route);
                }
            }
            Op::Send => {
                if let Some(update) = self.client.next_update().cloned() {
                    let refused = self.server.apply(&update);
                    self.answers.push((
                        update.clone(),
                        Ok(SessionUpdated {
                            generation: update.generation,
                            refused,
                        }),
                    ));
                    self.last_sent = Some(update);
                }
            }
            Op::Answer => {
                if !self.answers.is_empty() {
                    let (update, answer) = self.answers.remove(0);
                    let before: BTreeSet<Route> = self
                        .client
                        .acked()
                        .map(|(_, route)| route.clone())
                        .collect();
                    let mut named = vec![];
                    self.client
                        .update_answered(&update, &answer, &mut named)
                        .unwrap();
                    // an answer only ever adds acknowledged routes, and names exactly those
                    let after: BTreeSet<Route> = self
                        .client
                        .acked()
                        .map(|(_, route)| route.clone())
                        .collect();
                    assert_eq!(named.len(), named.iter().collect::<BTreeSet<_>>().len());
                    assert_eq!(
                        before
                            .iter()
                            .chain(&named)
                            .cloned()
                            .collect::<BTreeSet<_>>(),
                        after
                    );
                    assert!(named.iter().all(|route| !before.contains(route)));
                    assert!(named.iter().all(|route| self.client.is_acked(route)));
                }
            }
            Op::Lose => {
                if let Some(update) = &self.last_sent {
                    self.client.update_lost(update);
                }
            }
            Op::RetryBlocked => self.client.retry_blocked(),
        }
        self.check();
    }

    /// The client's indexes agree with each other and with the model.
    fn check(&self) {
        let client = &self.client;
        assert_eq!(
            client.wanted().cloned().collect::<BTreeSet<_>>(),
            self.wanted
        );
        let routes_in = |is_standing: fn(&Standing) -> bool| -> BTreeSet<Route> {
            client
                .routes
                .iter()
                .filter(|(_, standing)| is_standing(standing))
                .map(|(route, _)| route.clone())
                .collect()
        };
        let values = |map: &BTreeMap<TargetId, Route>| -> BTreeSet<Route> {
            map.values().cloned().collect()
        };
        assert_eq!(
            values(&client.pending),
            routes_in(|s| matches!(s, Standing::Pending(_)))
        );
        assert_eq!(
            values(&client.acked),
            routes_in(|s| matches!(s, Standing::Acked(_)))
        );
        assert_eq!(
            client.blocked,
            routes_in(|s| matches!(s, Standing::Blocked))
        );
        // Every id the client holds for a route is unique and already issued, and none of
        // them is also queued for removal.
        let mut live: Vec<TargetId> = client
            .pending
            .keys()
            .chain(client.acked.keys())
            .copied()
            .collect();
        live.extend(
            client
                .routes
                .values()
                .filter_map(|standing| match standing {
                    Standing::InFlight(id) => Some(*id),
                    _ => None,
                }),
        );
        let unique: BTreeSet<_> = live.iter().copied().collect();
        assert_eq!(unique.len(), live.len(), "an id names two routes");
        assert!(live.iter().all(|id| *id < client.next_target));
        assert!(
            unique.is_disjoint(&client.removals),
            "a live id is queued for removal"
        );
        // Each addition of the update in flight is still the route's InFlight id, or the
        // route was dropped since and the id is queued for removal: the server may hold it.
        if let Some(in_flight) = &client.in_flight {
            for (id, route) in &in_flight.update.additions {
                assert!(
                    client.routes.get(route) == Some(&Standing::InFlight(*id))
                        || client.removals.contains(id),
                    "a dropped in-flight addition is not queued for removal"
                );
            }
        }
    }

    /// Run the traffic out with the server's refusals as they are, and compare.
    fn drain(&mut self) {
        for _ in 0..16 {
            while !self.answers.is_empty() {
                self.step(Op::Answer);
            }
            if self.client.is_settled() {
                break;
            }
            self.step(Op::Send);
        }
        assert!(self.client.is_settled(), "the traffic did not drain");
        let acked: BTreeMap<_, _> = self
            .client
            .acked()
            .map(|(id, route)| (id, route.clone()))
            .collect();
        assert_eq!(
            self.server.held, acked,
            "the server holds what the client acknowledged"
        );
        let held: BTreeSet<_> = acked.values().cloned().collect();
        assert_eq!(held.len(), acked.len(), "a route is held under two ids");
        let blocked: BTreeSet<_> = self.client.blocked().cloned().collect();
        assert!(held.is_disjoint(&blocked));
        assert_eq!(
            held.union(&blocked).cloned().collect::<BTreeSet<_>>(),
            self.wanted
        );
    }
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => any::<u8>().prop_map(Op::Want),
        4 => any::<u8>().prop_map(Op::Unwant),
        1 => any::<u8>().prop_map(Op::Refuse),
        4 => Just(Op::Send),
        4 => Just(Op::Answer),
        1 => Just(Op::Lose),
        1 => Just(Op::RetryBlocked),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn route_churn_matches_the_wanted_set(ops in proptest::collection::vec(op(), 0..120)) {
        let mut harness = Harness::new();
        for op in ops {
            harness.step(op);
        }
        harness.drain();
        // Once the server stops refusing, every blocked route comes back and the client
        // follows the whole model.
        harness.server.refusing.clear();
        harness.step(Op::RetryBlocked);
        harness.drain();
        let acked: BTreeSet<_> = harness.client.acked().map(|(_, route)| route.clone()).collect();
        prop_assert_eq!(acked, harness.wanted.clone());
    }
}
