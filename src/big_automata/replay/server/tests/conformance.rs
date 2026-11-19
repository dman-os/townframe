//! Full-graph conformance of [`ReplayServer`] with the Quint model `replay/spec/server.qnt`.
//!
//! TLC enumerates the model's whole reachable graph and `replay/spec/server.py` exports it as
//! `server.graph.json` (how to regenerate it: `replay/spec/server.md`). Every node is a server
//! state, every event of the alphabet is an edge from every node, and every edge carries the
//! answer the model gives. The tests:
//!
//! * `every_edge_matches_the_model`: from each node, applying each event answers and lands
//!   exactly as the edge says, and calls `admits` only for an update that applies;
//! * `reaches_exactly_the_graph`: a breadth-first search over the Rust machine under the same
//!   events reaches no state outside the graph and every node of it;
//! * `sessions_are_isolated`: the graph has one session id, because nothing couples two
//!   sessions and each id would multiply the node count by the per-session count. This runs
//!   every edge with a second, busy session beside it, on either id, and checks the other
//!   session is untouched (a sweep forgets both).

use std::collections::HashMap;
use std::collections::VecDeque;

use serde_json::Value;

use super::*;

const GRAPH: &str = include_str!("../../spec/server.graph.json");

/// One session's state: `None` when the server does not hold it, else generation, held
/// `(target, route index)` pairs in id order, and refused ids.
type SessionState = Option<(Generation, Vec<(TargetId, u8)>, Vec<TargetId>)>;

/// The server's states for sessions `0..n`.
type State = Vec<SessionState>;

/// The model's answer: `None` for `close` and `sweep`.
type Answer = Option<Result<(Generation, Vec<TargetId>), &'static str>>;

#[derive(Debug)]
enum Event {
    Update {
        generation: Generation,
        additions: Vec<(TargetId, u8)>,
        removals: Vec<TargetId>,
        admitted: Vec<u8>,
    },
    Close,
    Sweep,
}

struct Graph {
    nodes: Vec<State>,
    events: Vec<Event>,
    answers: Vec<Answer>,
    /// `next[node][2 * event]` is the answer index, `next[node][2 * event + 1]` the next node.
    next: Vec<Vec<usize>>,
    /// The events from node 0 to each node, along a shortest path.
    paths: Vec<Vec<usize>>,
}

fn session_id(index: usize) -> SessionId {
    session(index as u128 + 1)
}

fn route_index(route: &Route) -> u8 {
    (0..2)
        .find(|index| part_route(*index) == *route)
        .expect("the routes of the model are parts 0 and 1")
}

fn ints<T: TryFrom<u64>>(value: &Value) -> Vec<T> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| T::try_from(item.as_u64().unwrap()).ok().unwrap())
        .collect()
}

fn pairs(value: &Value) -> Vec<(TargetId, u8)> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            let pair: Vec<u64> = ints(pair);
            (pair[0] as TargetId, pair[1] as u8)
        })
        .collect()
}

impl Graph {
    fn load() -> Self {
        let json: Value = serde_json::from_str(GRAPH).unwrap();
        let nodes = json["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|sessions| {
                sessions
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|held| {
                        (!held.is_null()).then(|| {
                            (
                                held["generation"].as_u64().unwrap(),
                                pairs(&held["targets"]),
                                ints(&held["refused"]),
                            )
                        })
                    })
                    .collect()
            })
            .collect::<Vec<State>>();
        let events = json["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| match event["kind"].as_str().unwrap() {
                "update" => {
                    assert_eq!(event["session"], 0, "the graph has one session");
                    Event::Update {
                        generation: event["generation"].as_u64().unwrap(),
                        additions: pairs(&event["additions"]),
                        removals: ints(&event["removals"]),
                        admitted: ints(&event["admitted"]),
                    }
                }
                "close" => Event::Close,
                "sweep" => Event::Sweep,
                kind => panic!("unknown event kind {kind}"),
            })
            .collect::<Vec<_>>();
        let answers = json["answers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|answer| {
                (!answer.is_null()).then(|| match answer["kind"].as_str().unwrap() {
                    "updated" => Ok((
                        answer["generation"].as_u64().unwrap(),
                        ints(&answer["refused"]),
                    )),
                    "unknown_session" => Err("unknown_session"),
                    "invalid" => Err("invalid"),
                    kind => panic!("unknown answer kind {kind}"),
                })
            })
            .collect();
        let next = json["next"]
            .as_array()
            .unwrap()
            .iter()
            .map(ints)
            .collect::<Vec<Vec<usize>>>();
        let mut graph = Self {
            nodes,
            events,
            answers,
            next,
            paths: default(),
        };
        graph.paths = graph.shortest_paths();
        graph
    }

    fn shortest_paths(&self) -> Vec<Vec<usize>> {
        let mut paths: Vec<Option<Vec<usize>>> = vec![None; self.nodes.len()];
        paths[0] = Some(vec![]);
        let mut queue = VecDeque::from([0]);
        while let Some(node) = queue.pop_front() {
            for event in 0..self.events.len() {
                let to = self.next[node][2 * event + 1];
                if paths[to].is_none() {
                    let mut path = paths[node].clone().unwrap();
                    path.push(event);
                    paths[to] = Some(path);
                    queue.push_back(to);
                }
            }
        }
        paths
            .into_iter()
            .map(|path| path.expect("every node is reachable from the initial one"))
            .collect()
    }

    /// Bring `session_index` of `server` to `node` through the public API, and check it got
    /// there (a mismatch here is a conformance failure along the path).
    fn drive(&self, server: &mut ReplayServer, session_index: usize, node: usize) {
        for &event in &self.paths[node] {
            apply(server, session_index, &self.events[event]);
        }
        assert_eq!(
            state(server, session_index + 1)[session_index],
            self.nodes[node][0],
            "path {:?} did not reach node {node}",
            self.paths[node]
        );
    }

    fn node_of(&self) -> HashMap<&State, usize> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(node, state)| (state, node))
            .collect()
    }
}

/// Apply `event` to session `session_index`, with `admits` answering from the event's admitted
/// routes. Returns the answer, and how many times admission was asked.
fn apply_counting(
    server: &mut ReplayServer,
    session_index: usize,
    event: &Event,
) -> (Answer, usize) {
    match event {
        Event::Update {
            generation,
            additions,
            removals,
            admitted,
        } => {
            let update = UpdateSession {
                session_id: session_id(session_index),
                generation: *generation,
                additions: additions
                    .iter()
                    .map(|(id, route)| (*id, part_route(*route)))
                    .collect(),
                removals: removals.clone(),
            };
            let mut asked = 0;
            let answer = server.update(&principal(0), &update, at(0), |route| {
                asked += 1;
                admitted.contains(&route_index(route))
            });
            let answer = Some(match answer {
                Ok(updated) => Ok((updated.generation, updated.refused)),
                Err(SessionError::UnknownSession) => Err("unknown_session"),
                Err(SessionError::Invalid(_)) => Err("invalid"),
                Err(other) => panic!("the model has no answer for {other:?}"),
            });
            (answer, asked)
        }
        Event::Close => {
            server.close(
                &principal(0),
                &CloseSession {
                    session_id: session_id(session_index),
                },
            );
            (None, 0)
        }
        // Every update above is at `at(0)`, so a sweep a whole TTL later forgets everything.
        Event::Sweep => {
            server.sweep(at(SESSION_TTL.as_secs()));
            (None, 0)
        }
    }
}

fn apply(server: &mut ReplayServer, session_index: usize, event: &Event) -> Answer {
    apply_counting(server, session_index, event).0
}

fn state(server: &ReplayServer, sessions: usize) -> State {
    (0..sessions)
        .map(|index| {
            server
                .session(&principal(0), &session_id(index))
                .map(|held| {
                    let mut targets: Vec<_> = held
                        .targets
                        .iter()
                        .map(|(id, route)| (*id, route_index(route)))
                        .collect();
                    targets.sort_unstable();
                    (held.generation, targets, held.refused.clone())
                })
        })
        .collect()
}

#[test]
fn every_edge_matches_the_model() {
    let graph = Graph::load();
    for node in 0..graph.nodes.len() {
        for (index, event) in graph.events.iter().enumerate() {
            let mut server = ReplayServer::default();
            graph.drive(&mut server, 0, node);
            let (answer, asked) = apply_counting(&mut server, 0, event);
            let context = format!("node {node} {:?} --{event:?}-->", graph.nodes[node]);
            assert_eq!(
                answer,
                graph.answers[graph.next[node][2 * index]],
                "answer of {context}"
            );
            let to = graph.next[node][2 * index + 1];
            assert_eq!(
                state(&server, 1),
                graph.nodes[to],
                "next state of {context}"
            );
            // Law 2: admission is asked only for the additions of an update that applies, and
            // an update applies exactly when it changes the session (a new generation, or an
            // opening).
            if let Event::Update { additions, .. } = event {
                let applied = graph.nodes[node] != graph.nodes[to];
                assert_eq!(
                    asked,
                    if applied { additions.len() } else { 0 },
                    "admission asks of {context}"
                );
            }
        }
    }
}

#[test]
fn reaches_exactly_the_graph() {
    let graph = Graph::load();
    let node_of = graph.node_of();
    // The Rust machine's own search: every state is rebuilt from the events that reached it.
    let mut seen: HashMap<State, Vec<usize>> = HashMap::from([(vec![None], vec![])]);
    let mut queue = VecDeque::from([vec![None]]);
    while let Some(from) = queue.pop_front() {
        let path = seen[&from].clone();
        for (index, event) in graph.events.iter().enumerate() {
            let mut server = ReplayServer::default();
            for &step in &path {
                apply(&mut server, 0, &graph.events[step]);
            }
            apply(&mut server, 0, event);
            let to = state(&server, 1);
            assert!(
                node_of.contains_key(&to),
                "{from:?} --{event:?}--> {to:?} is not a node of the model's graph"
            );
            if !seen.contains_key(&to) {
                let mut longer = path.clone();
                longer.push(index);
                seen.insert(to.clone(), longer);
                queue.push_back(to);
            }
        }
    }
    assert_eq!(
        seen.len(),
        graph.nodes.len(),
        "the model reaches states the Rust machine never does"
    );
}

#[test]
fn sessions_are_isolated() {
    let graph = Graph::load();
    // The busiest node: most held targets and refusals.
    let busy = (0..graph.nodes.len())
        .max_by_key(|node| {
            graph.nodes[*node][0]
                .as_ref()
                .map_or(0, |(_, held, refused)| held.len() + refused.len())
        })
        .unwrap();
    for node in 0..graph.nodes.len() {
        for (index, event) in graph.events.iter().enumerate() {
            for (target, other) in [(0, 1), (1, 0)] {
                let mut server = ReplayServer::default();
                graph.drive(&mut server, target, node);
                graph.drive(&mut server, other, busy);
                let before = state(&server, 2);
                apply(&mut server, target, event);
                let mut after = state(&server, 2);
                let to = graph.next[node][2 * index + 1];
                assert_eq!(
                    after[target], graph.nodes[to][0],
                    "node {node} --{event:?}--> on session {target} beside a busy one"
                );
                // Only a sweep reaches past the named session.
                let expected_other = if matches!(event, Event::Sweep) {
                    None
                } else {
                    before[other].clone()
                };
                assert_eq!(
                    after.swap_remove(other),
                    expected_other,
                    "node {node} --{event:?}--> on session {target} touched session {other}"
                );
            }
        }
    }
}
