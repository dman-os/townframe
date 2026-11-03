//! The host's waits-for graph of in-flight cross-incarnation calls.
//!
//! An engine runs at most one async guest task per incarnation at a time, so a call into a
//! busy incarnation queues behind the one in flight. That makes a call cycle (A calls B, B calls
//! A while A still waits on B) a deadlock instead of a re-entry. Every call registers an edge
//! `caller -> target` from the moment it is issued, whether it runs or queues, until its future
//! completes or is dropped. Registering an edge that would close a cycle is refused up front,
//! which is the only way a cycle is observed: nothing is ever left waiting on it.
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use crate::Instance;

/// Refusal to add a call edge that would close a cycle of waiting incarnations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the call would wait on itself through a cycle of incarnations")]
pub struct CallCycle;

/// The edges of in-flight calls, keyed by incarnation id. A node may have several outgoing
/// edges (a guest task can await several calls at once) and parallel edges between one pair
/// are counted.
#[derive(Default)]
pub(crate) struct CallGraph {
    edges: Mutex<BTreeMap<u64, BTreeMap<u64, usize>>>,
}

/// One registered call edge; dropping it (the call completed or was cancelled) removes it.
#[must_use = "the edge lives exactly as long as the call it guards"]
pub struct CallEdge {
    graph: Arc<CallGraph>,
    from: u64,
    to: u64,
}

impl CallGraph {
    /// Register `from -> to`, unless `from` is already reachable from `to`, which means `to`
    /// transitively waits on `from` and the call could never be answered.
    pub(crate) fn enter(self: &Arc<Self>, from: u64, to: u64) -> Result<CallEdge, CallCycle> {
        let mut edges = self.edges.lock().unwrap();
        let mut seen = BTreeSet::from([to]);
        let mut pending = vec![to];
        while let Some(node) = pending.pop() {
            if node == from {
                return Err(CallCycle);
            }
            for &next in edges.get(&node).into_iter().flat_map(BTreeMap::keys) {
                if seen.insert(next) {
                    pending.push(next);
                }
            }
        }
        *edges.entry(from).or_default().entry(to).or_default() += 1;
        Ok(CallEdge {
            graph: Arc::clone(self),
            from,
            to,
        })
    }

    fn waits_on(&self, from: u64, to: u64) -> bool {
        self.edges
            .lock()
            .unwrap()
            .get(&from)
            .is_some_and(|targets| targets.contains_key(&to))
    }
}

impl Drop for CallEdge {
    fn drop(&mut self) {
        let mut edges = self.graph.edges.lock().unwrap();
        let targets = edges
            .get_mut(&self.from)
            .expect("a live edge has its source");
        let count = targets
            .get_mut(&self.to)
            .expect("a live edge has its target");
        *count -= 1;
        if *count == 0 {
            targets.remove(&self.to);
            if targets.is_empty() {
                edges.remove(&self.from);
            }
        }
    }
}

impl crate::Host {
    /// Register that `from` now waits on a call into `to`, refusing a call that would close a
    /// cycle. Hold the returned edge for as long as the call is outstanding.
    pub fn enter_call(&self, from: &Instance, to: &Instance) -> Result<CallEdge, CallCycle> {
        self.calls.enter(from.id, to.id)
    }

    /// Whether `from` has a call into `to` outstanding, running or queued.
    pub fn waits_on(&self, from: &Instance, to: &Instance) -> bool {
        self.calls.waits_on(from.id, to.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycles_are_refused_through_multi_edge_nodes_and_edges_end_with_their_calls() {
        let graph = Arc::new(CallGraph::default());
        // 1 waits on both 2 and 3; 3 waits on 4.
        let one_two = graph.enter(1, 2).unwrap();
        let one_three = graph.enter(1, 3).unwrap();
        let three_four = graph.enter(3, 4).unwrap();
        // Every node on a path back to the caller closes a cycle, on either branch.
        assert_eq!(graph.enter(2, 1).err(), Some(CallCycle));
        assert_eq!(graph.enter(4, 1).err(), Some(CallCycle));
        assert_eq!(graph.enter(4, 3).err(), Some(CallCycle));
        assert_eq!(graph.enter(1, 1).err(), Some(CallCycle));
        // Unrelated direction and sibling branches are not cycles.
        let two_four = graph.enter(2, 4).unwrap();
        let four_five = graph.enter(4, 5).unwrap();
        assert_eq!(graph.enter(5, 2).err(), Some(CallCycle));
        // A refused edge leaves nothing behind.
        drop(four_five);
        let five_two = graph.enter(5, 2);
        assert!(five_two.is_ok());
        drop(five_two);
        // Ending a call removes its edge, and only that edge: 3 -> 4 still holds the path
        // from 1 to 4 after 1 -> 2 -> 4 is gone.
        drop((one_two, two_four));
        assert_eq!(graph.enter(4, 1).err(), Some(CallCycle));
        assert!(graph.enter(2, 1).is_ok());
        drop(three_four);
        assert!(graph.enter(4, 1).is_ok());
        // Parallel calls over one pair are counted; the edge stays until the last ends.
        let first = graph.enter(7, 8).unwrap();
        let second = graph.enter(7, 8).unwrap();
        drop(first);
        assert_eq!(graph.enter(8, 7).err(), Some(CallCycle));
        drop(second);
        assert!(graph.enter(8, 7).is_ok());
        drop(one_three);
        assert!(graph.edges.lock().unwrap().is_empty());
    }
}
