//! The machine's own consistency, asserted at the end of every flush in test builds.

use super::*;

impl<O> Document<O> {
    pub(in crate::document) fn check(&self) {
        assert!(
            self.frontier_rev <= self.known_rev,
            "frontier ahead of the tree"
        );
        if matches!(self.frontier, Frontier::Current) {
            // current means nothing to publish now: it reflects what is applied, or, with
            // nothing applied, the tree
            let reflected = match &self.phase {
                Phase::Live(live) => live.applied_rev,
                _ => self.known_rev,
            };
            assert_eq!(
                self.frontier_rev, reflected,
                "a current frontier reflects the document"
            );
        }
        let mut calls = vec![];
        match &self.phase {
            Phase::Hydrating { call } => calls.push(*call),
            Phase::Live(live) => {
                assert!(
                    self.frontier_rev <= live.applied_rev && live.applied_rev <= self.known_rev,
                    "frontier_rev <= applied_rev <= known_rev"
                );
                match &live.remote {
                    Remote::UpToDate => assert_eq!(live.applied_rev, self.known_rev),
                    Remote::Behind => {}
                    Remote::Applying { call } => calls.push(*call),
                    Remote::Blocked { refs } => assert!(!refs.is_empty()),
                }
            }
            Phase::Parked { missing } => assert!(!missing.is_empty()),
            Phase::Cold => {}
        }
        if let Frontier::Publishing { call, rev, .. } = &self.frontier {
            // (a failed write may have thrown the live document away since)
            calls.push(*call);
            assert!(self.plant.is_some() || *rev <= self.known_rev);
        }
        match &self.writes {
            Writes::Idle => {}
            Writes::Encrypting { call, .. } | Writes::Committing { call, .. } => {
                calls.push(*call);
                assert!(matches!(self.phase, Phase::Live(_)));
            }
            Writes::HeldForPlan { .. } => assert!(matches!(self.phase, Phase::Live(_))),
        }
        let distinct: BTreeSet<_> = calls.iter().collect();
        assert_eq!(distinct.len(), calls.len(), "one call per slot");
        assert!(
            calls.iter().all(|call| !self.stopping.contains(call)),
            "a cancelled call holds no slot"
        );
        // after a flush nothing that could start is left waiting
        if self.plant.is_none() {
            if matches!(self.phase, Phase::Live(_)) {
                assert!(
                    self.reads.is_empty() || !matches!(self.writes, Writes::Idle),
                    "a live document with no write in flight serves its reads"
                );
                if matches!(self.writes, Writes::Idle)
                    && self.readable
                    && !matches!(self.frontier, Frontier::Publishing { .. })
                {
                    assert!(self.queue.is_empty(), "no write in flight starts the queue");
                }
            }
            if matches!(self.phase, Phase::Cold) {
                assert!(
                    !self.wanted() || self.backing_off(),
                    "a wanted cold document is hydrating"
                );
            }
        }
    }
}

impl<O> Document<O> {
    /// The states of the four machines and the revisions, as text for traces.
    pub(in crate::document) fn describe(&self) -> String {
        let phase = match &self.phase {
            Phase::Cold => "cold".to_string(),
            Phase::Hydrating { .. } => "hydrating".to_string(),
            Phase::Parked { .. } => "parked".to_string(),
            Phase::Live(live) => format!(
                "live@{}/{}",
                live.applied_rev,
                match live.remote {
                    Remote::UpToDate => "up",
                    Remote::Behind => "behind",
                    Remote::Applying { .. } => "applying",
                    Remote::Blocked { .. } => "blocked",
                }
            ),
        };
        let writes = match self.writes {
            Writes::Idle => "idle",
            Writes::Encrypting { .. } => "encrypting",
            Writes::Committing { .. } => "committing",
            Writes::HeldForPlan { .. } => "held",
        };
        let frontier = match self.frontier {
            Frontier::Current => "current",
            Frontier::Stale => "stale",
            Frontier::Publishing { .. } => "publishing",
        };
        format!(
            "{phase} {writes} {frontier} k{} f{} q{} h{} i{}",
            self.known_rev,
            self.frontier_rev,
            self.queue.len(),
            self.handles.len(),
            self.interest
        )
    }
}
