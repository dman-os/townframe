//! Scripted control of the rig, for the law tests.

use super::*;

impl Sim {
    /// The kinds of the calls in flight for `doc`, oldest first.
    pub(in crate::document) fn in_flight(&self, doc: usize) -> Vec<&'static str> {
        self.flights
            .iter()
            .filter(|flight| flight.doc == doc)
            .map(|flight| flight.ask.kind())
            .collect()
    }

    /// Finish the oldest call of `kind` for `doc`, failing it if `fail`. Answers whether there
    /// was one.
    pub(in crate::document) fn complete_kind(
        &mut self,
        doc: usize,
        kind: &str,
        fail: bool,
    ) -> bool {
        let Some(at) = self
            .flights
            .iter()
            .position(|flight| flight.doc == doc && flight.ask.kind() == kind)
        else {
            return false;
        };
        self.complete(at, fail);
        self.check();
        true
    }

    /// The keys of the live document, `None` if there is none.
    pub(in crate::document) fn live(&self, doc: usize) -> Option<BTreeSet<String>> {
        match &self.machines[doc].as_ref()?.doc.phase {
            Phase::Live(live) => Some(live.doc.keys(ROOT).collect()),
            _ => None,
        }
    }

    pub(in crate::document) fn describe(&self, doc: usize) -> String {
        self.machines[doc]
            .as_ref()
            .map_or("-".to_string(), |machine| machine.doc.describe())
    }

    pub(in crate::document) fn evictable(&self, doc: usize) -> bool {
        self.machines[doc]
            .as_ref()
            .is_some_and(|machine| machine.doc.evictable())
    }

    /// How many requests have not been answered.
    pub(in crate::document) fn unanswered(&self) -> usize {
        self.waiting.len()
    }

    /// Prepare a plan of plain writes on the documents of `mask`.
    pub(in crate::document) fn plan_prepare_writes(&mut self, mask: usize) {
        self.plan_prepare_kind(mask, Some(false));
    }

    /// As `complete_kind`, for a commit that lands and is reported failed.
    pub(in crate::document) fn complete_undecided(&mut self, doc: usize, now: bool) -> bool {
        let Some(at) = self
            .flights
            .iter()
            .position(|f| f.doc == doc && f.ask.kind() == "commit")
        else {
            return false;
        };
        self.complete_as(at, false, Some(now));
        self.check();
        true
    }

    pub(in crate::document) fn land_zombies(&mut self) {
        while !self.zombies.is_empty() {
            self.land_zombie(0);
        }
    }

    /// A membership change moves the document's key epoch.
    pub(in crate::document) fn move_epoch(&mut self, doc: usize) {
        self.world.docs[doc].epoch += 1;
    }

    pub(in crate::document) fn deliver_all(&mut self) {
        while !self.mail.is_empty() {
            self.deliver(0);
        }
    }
}
