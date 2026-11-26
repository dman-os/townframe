//! What the simulation asserts of the machines and the world. The law numbers are the
//! [parent module's](crate::document).

use super::*;

impl Sim {
    /// Law 8: nothing reads a document the machine was told is unreadable.
    pub(super) fn check_readable(&self, doc: usize) {
        let machine = self.machines[doc].as_ref().expect("a machine reads");
        assert!(
            machine.told_readable,
            "doc {doc}: a read was issued for a document told to be unreadable (law 8)"
        );
    }

    /// Law 4: a document prepared for a plan starts nothing until the plan is resolved.
    pub(super) fn check_not_held(&self, doc: usize) {
        if let Some(plan) = self.held.get(&doc) {
            panic!("doc {doc}: work started while prepared for plan {plan} (law 4)");
        }
    }

    /// Law 5: a frontier is never ahead of what the machine applied.
    pub(super) fn check_publish(&self, doc: usize, rev: Rev) {
        let machine = self.machines[doc].as_ref().expect("a machine publishes");
        let Phase::Live(live) = &machine.doc.phase else {
            panic!("doc {doc}: published without a live document");
        };
        assert!(
            rev <= live.applied_rev,
            "doc {doc}: frontier published at {rev}, ahead of the applied {} (law 5)",
            live.applied_rev
        );
    }

    /// A reply: a write's change is applied and committed before the caller hears of it (laws
    /// 1 and 3), a failed write left nothing, and a read sees what was acknowledged before it
    /// was asked and nothing uncommitted.
    pub(super) fn replied(&mut self, ticket: Ticket, result: Result<Out, Rejected>) {
        let waiting = self
            .waiting
            .remove(&ticket)
            .expect("a reply names a request");
        match (waiting, result) {
            (Waiting::Write { doc, tag }, Ok(out)) => {
                let Some(tag) = tag else {
                    assert!(
                        out.is_empty(),
                        "a write that changes nothing returns nothing"
                    );
                    return;
                };
                let key = format!("l{tag}");
                assert_eq!(out, vec![key.clone()]);
                // (an unreadable document's live copy is dropped in the flush that replies)
                if let Some(live) = self.live(doc) {
                    assert!(
                        live.contains(&key),
                        "doc {doc}: {key} replied before applied (law 3)"
                    );
                }
                assert!(
                    self.world.visible(doc).contains(&key),
                    "doc {doc}: {key} applied but not committed (law 1)"
                );
                self.acked[doc].insert(key);
            }
            (Waiting::Write { doc, tag }, Err(rejected)) => {
                if let Some(tag) = tag {
                    let key = format!("l{tag}");
                    assert!(
                        !self.live(doc).is_some_and(|live| live.contains(&key)),
                        "doc {doc}: a refused write ({rejected:?}) left {key} in the document (law 1)"
                    );
                    assert!(!matches!(rejected, Rejected::Op(_)));
                } else {
                    assert!(matches!(
                        rejected,
                        Rejected::Op(_) | Rejected::StaleHandle | Rejected::Unreadable
                    ));
                }
            }
            (Waiting::Read { doc, must_see }, Ok(keys)) => {
                let keys: BTreeSet<String> = keys.into_iter().collect();
                assert!(
                    must_see.is_subset(&keys),
                    "doc {doc}: a read missed acknowledged writes {:?} (law 3)",
                    must_see.difference(&keys).collect::<Vec<_>>()
                );
                assert!(
                    keys.is_subset(&self.world.visible(doc)),
                    "doc {doc}: a read saw uncommitted state {:?} (law 1)",
                    keys.difference(&self.world.visible(doc))
                        .collect::<Vec<_>>()
                );
            }
            (Waiting::Read { .. }, Err(rejected)) => {
                assert!(matches!(
                    rejected,
                    Rejected::StaleHandle | Rejected::Unreadable
                ));
            }
        }
    }

    /// After every step.
    pub(super) fn check(&self) {
        for doc in 0..DOCS {
            let revs = (self.world.frontier_rev(doc), self.world.tree_rev(doc));
            assert!(
                revs.0 <= revs.1,
                "doc {doc}: frontier {} ahead of the tree {}",
                revs.0,
                revs.1
            );
            let Some(machine) = self.machines[doc].as_ref() else {
                continue;
            };
            if let Phase::Live(live) = &machine.doc.phase {
                // law 1: the live document holds only committed changes
                let committed = self.world.committed_hashes(doc);
                for change in live.doc.get_changes(&[]) {
                    assert!(
                        committed.contains(&change.hash()),
                        "doc {doc}: the live document holds an uncommitted change (law 1)"
                    );
                }
                assert!(machine.doc.known_rev <= revs.1);
            }
            if let Phase::Parked { .. } = &machine.doc.phase {
                // law 6: there is a reason, or the epoch move that ends it is on its way
                let on_its_way = self
                    .mail
                    .iter()
                    .any(|mail| matches!(mail, Mail::Epoch(d, _) if *d == doc));
                assert!(
                    !self.world.docs[doc].keyless().is_empty()
                        || on_its_way
                        || !self.world.docs[doc].readable,
                    "doc {doc}: parked with every key present (law 6)"
                );
            }
            if machine.doc.evictable() {
                // law 7: nothing is left in any of the places a machine can hold work
                let calls = self.flights.iter().any(|flight| flight.doc == doc);
                let asked = self.waiting.values().any(|waiting| match waiting {
                    Waiting::Write { doc: d, .. } | Waiting::Read { doc: d, .. } => *d == doc,
                });
                assert!(
                    machine.handles.is_empty()
                        && machine.interest == 0
                        && !calls
                        && !asked
                        && !self.held.contains_key(&doc),
                    "doc {doc}: evictable with work left (law 7)"
                );
            }
        }
    }

    /// After a quiet drain.
    pub(super) fn check_quiet(&self) {
        assert!(self.flights.is_empty() && self.mail.is_empty());
        for doc in 0..DOCS {
            let model = &self.world.docs[doc];
            let machine = self.machines[doc].as_ref();
            let parked = machine.is_some_and(|m| matches!(m.doc.phase, Phase::Parked { .. }));
            if let Some(machine) = machine {
                let machine = &machine.doc;
                assert!(machine.stopping.is_empty() && machine.pending.is_empty());
                assert!(matches!(machine.writes, Writes::Idle) || self.held.contains_key(&doc));
                assert!(!matches!(machine.phase, Phase::Hydrating { .. }));
                if !parked {
                    assert!(machine.queue.is_empty(), "doc {doc}: writes left waiting");
                }
                if let Phase::Live(live) = &machine.phase
                    && model.readable
                {
                    assert_eq!(
                        live.doc.keys(ROOT).collect::<BTreeSet<_>>(),
                        self.world.visible(doc),
                        "doc {doc}: a quiet live document is exactly what a reader may see"
                    );
                    assert!(
                        !matches!(live.remote, Remote::Behind | Remote::Applying { .. }),
                        "doc {doc}: still behind"
                    );
                }
                if parked && model.readable {
                    assert!(
                        !model.keyless().is_empty(),
                        "doc {doc}: parked with every key present (law 6)"
                    );
                }
            }
            if model.readable && model.keyless().is_empty() {
                assert_eq!(
                    self.world.frontier_rev(doc),
                    self.world.tree_rev(doc),
                    "doc {doc}: a readable document is left with a stale frontier (law 5)"
                );
            }
        }
        for waiting in self.waiting.values() {
            let (Waiting::Write { doc, .. } | Waiting::Read { doc, .. }) = waiting;
            let parked = self.machines[*doc]
                .as_ref()
                .is_some_and(|m| matches!(m.doc.phase, Phase::Parked { .. }));
            assert!(parked, "doc {doc}: a request was left unanswered");
        }
    }
}
