//! What the simulation checks after every step, and after a quiet drain. The laws are numbered in
//! the [Hub's module docs](super::super).

use super::*;

impl Sim {
    pub(super) fn check(&mut self) {
        let window = config().window as u64;
        for doc in 0..DOCS {
            let mail = self.mailbox.get(&doc).map_or(0, |m| {
                m.iter()
                    .filter(|item| matches!(item, Mail::Msg(..)))
                    .count()
            }) as u64;
            assert!(
                mail <= window + 1,
                "doc {doc}: {mail} messages in the mailbox (law 8)"
            );
            assert!(
                self.hub.doc(doc as u64).in_flight <= window + 1,
                "doc {doc}: {} messages in flight (law 8)",
                self.hub.doc(doc as u64).in_flight
            );
            if let Some(job) = self.hub.doc(doc as u64).unsettled {
                assert!(
                    job > self.saved_cursor || job <= self.restart_cursor,
                    "doc {doc}: the cursor {} passed the unsettled row at {job} (law 7)",
                    self.saved_cursor
                );
            }
        }
        assert!(
            self.hub.catching_up() <= config().catch_up,
            "{} documents caught up at once (law 6)",
            self.hub.catching_up()
        );
        self.check_stale();
        self.check_routes();
        self.check_drain();
    }

    /// Law 3: a document the Hub was told is readable and stale is running, queued or parked.
    fn check_stale(&self) {
        if self.draining {
            return;
        }
        for doc in 0..DOCS {
            let told_tree = self.delivered_tree.get(&doc).copied().unwrap_or(0);
            let readable = self.told_readable.get(&doc).copied().unwrap_or(false);
            if !readable || self.world.frontier_rev(doc) >= told_tree {
                continue;
            }
            let catch = self.hub.doc(doc as u64).catch;
            assert!(
                catch.is_some(),
                "doc {doc}: stale and readable (tree {told_tree}, frontier {}), and neither running, queued nor parked (law 3)",
                self.world.frontier_rev(doc)
            );
            match catch {
                Some(Catch::Running) => assert!(
                    self.machines.contains_key(&doc) || self.stopping.contains(&doc),
                    "doc {doc}: catching up with no machine"
                ),
                Some(Catch::Parked) => assert!(
                    !self.world.docs[doc].keyless().is_empty() || self.epoch_mail.contains(&doc),
                    "doc {doc}: parked though its keys are all here and no epoch move is due (law 4)"
                ),
                _ => {}
            }
        }
    }

    /// Law 11: every route is on an open session whose peer and principal stand.
    fn check_routes(&self) {
        for (doc, session) in &self.routes {
            assert!(
                self.net_open.contains(session),
                "a route on a closed session"
            );
            assert!(
                self.eligible(*doc).contains_key(session),
                "doc {doc}: routed on {session:?} without standing (law 11)"
            );
        }
    }

    /// Law 10: while draining, the Hub is drained exactly when nothing is in flight.
    fn check_drain(&self) {
        if !self.draining {
            return;
        }
        let quiet = self.machines.is_empty()
            && self.stopping.is_empty()
            && self.round_flights.is_empty()
            && self.dial_flights.is_empty()
            && (0..DOCS).all(|doc| self.hub.doc(doc as u64).catch != Some(Catch::Running));
        assert_eq!(
            self.hub.drained(),
            quiet,
            "the Hub is drained={} while quiet={quiet} (law 10): machines {:?}, stopping {:?}, rounds {:?}, dials {:?}",
            self.hub.drained(),
            self.machines.keys().collect::<Vec<_>>(),
            self.stopping,
            self.round_flights,
            self.dial_flights
        );
    }

    /// After the callers let go and everything was answered.
    pub(super) fn check_quiet(&self) {
        assert!(
            self.machines.is_empty(),
            "machines still run: {:?}",
            self.machines.keys()
        );
        assert!(self.flights.is_empty() && self.reports.is_empty());
        assert_eq!(self.hub.catching_up(), 0);
        for doc in 0..DOCS {
            let catch = self.hub.doc(doc as u64).catch;
            assert!(
                !matches!(catch, Some(Catch::Queued | Catch::Running)),
                "doc {doc}: catch-up {catch:?} after the drain"
            );
            let model = &self.world.docs[doc];
            if !self.draining && model.readable && model.keyless().is_empty() {
                assert_eq!(
                    self.world.frontier_rev(doc),
                    model.tree_rev,
                    "doc {doc}: a readable keyed document whose frontier is not at the tree"
                );
            }
        }
        assert!(
            self.waiters.keys().all(|w| self.resolved.contains(w)),
            "a waiter was never resolved (law 9)"
        );
        if self.draining {
            assert!(
                self.hub.drained(),
                "the Hub did not finish draining (law 10)"
            );
        }
    }
}
