//! The simulation's ops and how each one acts.

use proptest::prelude::*;

use super::*;

#[derive(Debug, Clone)]
pub(in super::super) enum Op {
    Open(usize),
    CloseHandle(usize, usize),
    InterestOn(usize),
    InterestOff(usize),
    Plan(usize),
    RemoteWrite(usize, bool),
    GrantKeys(usize),
    SetReadable(usize, bool),
    Feed(usize),
    Epoch(usize),
    DocMail(usize),
    Reports(usize),
    Complete(usize, bool),
    PeerStanding(u8, usize, bool),
    LocalStanding(u8, usize, bool),
    Connect(u8),
    Disconnect(u8),
    Dial(usize, bool),
    Lose(usize),
    Refuse(usize, usize),
    Want(u8, bool),
    RoundDone(usize, bool),
    Admit(u8),
    Advance(usize),
    Restart,
    Drain,
}

pub(in super::super) fn op() -> impl Strategy<Value = Op> {
    let doc = 0..DOCS;
    prop_oneof![
        3 => doc.clone().prop_map(Op::Open),
        3 => (doc.clone(), any::<usize>()).prop_map(|(d, i)| Op::CloseHandle(d, i)),
        2 => doc.clone().prop_map(Op::InterestOn),
        2 => doc.clone().prop_map(Op::InterestOff),
        3 => (1..(1usize << DOCS)).prop_map(Op::Plan),
        4 => (doc.clone(), prop::bool::weighted(0.5)).prop_map(|(d, k)| Op::RemoteWrite(d, k)),
        3 => doc.clone().prop_map(Op::GrantKeys),
        2 => (doc.clone(), prop::bool::weighted(0.7)).prop_map(|(d, r)| Op::SetReadable(d, r)),
        8 => (0..4usize).prop_map(Op::Feed),
        5 => any::<usize>().prop_map(Op::Epoch),
        10 => any::<usize>().prop_map(Op::DocMail),
        10 => any::<usize>().prop_map(Op::Reports),
        12 => (any::<usize>(), prop::bool::weighted(0.1)).prop_map(|(i, f)| Op::Complete(i, f)),
        3 => (0..PEERS, doc.clone(), any::<bool>()).prop_map(|(p, d, on)| Op::PeerStanding(p, d, on)),
        3 => (0..PRINCIPALS, doc.clone(), any::<bool>()).prop_map(|(l, d, on)| Op::LocalStanding(l, d, on)),
        2 => (0..PEERS).prop_map(Op::Connect),
        1 => (0..PEERS).prop_map(Op::Disconnect),
        4 => (any::<usize>(), prop::bool::weighted(0.75)).prop_map(|(i, ok)| Op::Dial(i, ok)),
        1 => any::<usize>().prop_map(Op::Lose),
        2 => (doc, any::<usize>()).prop_map(|(d, i)| Op::Refuse(d, i)),
        3 => (0..PEERS, any::<bool>()).prop_map(|(p, w)| Op::Want(p, w)),
        3 => (any::<usize>(), prop::bool::weighted(0.8)).prop_map(|(i, ok)| Op::RoundDone(i, ok)),
        2 => (1..4u8).prop_map(Op::Admit),
        4 => (0..5usize).prop_map(Op::Advance),
        1 => Just(Op::Restart),
        1 => Just(Op::Drain),
    ]
}

pub(in super::super) fn run(ops: &[Op], plant: Option<Plant>) {
    let mut sim = Sim::new(plant);
    for op in ops {
        sim.step(op);
    }
    sim.quiesce();
    sim.restart();
    sim.quiesce();
}

impl Sim {
    pub(super) fn step(&mut self, op: &Op) {
        if self.trace {
            eprintln!("{op:?}");
        }
        match *op {
            Op::Open(d) => self.open(d),
            Op::CloseHandle(d, pick) => self.close_handle(d, pick),
            Op::InterestOn(d) => {
                if self.hub.interest_acquired(d as u64).is_ok() {
                    *self.leases.entry(d).or_default() += 1;
                    self.stand(d);
                }
            }
            Op::InterestOff(d) => {
                if self.leases.get(&d).is_some_and(|n| *n > 0) {
                    *self.leases.get_mut(&d).expect("held") -= 1;
                    self.hub.interest_released(d as u64);
                    self.pump();
                }
            }
            Op::Plan(mask) => self.plan(mask),
            Op::RemoteWrite(d, keyed) => {
                self.world.remote_write(d, keyed);
                self.bump_feed(d);
            }
            Op::GrantKeys(d) => {
                self.world.grant_keys(d);
                self.epoch_mail.push(d);
            }
            Op::SetReadable(d, readable) => {
                self.world.docs[d].readable = readable;
                self.epoch_mail.push(d);
            }
            Op::Feed(n) => self.feed(n + 1),
            Op::Epoch(pick) => self.epoch(pick),
            Op::DocMail(pick) => self.doc_mail(pick),
            Op::Reports(pick) => self.deliver_report(pick),
            Op::Complete(pick, fail) => self.complete(pick, fail),
            Op::PeerStanding(p, d, on) => {
                if on {
                    self.peer_standing.insert((p, d));
                } else {
                    self.peer_standing.remove(&(p, d));
                }
                self.restand();
            }
            Op::LocalStanding(l, d, on) => {
                if on {
                    self.local_standing.insert((l, d));
                } else {
                    self.local_standing.remove(&(l, d));
                }
                self.restand();
            }
            Op::Connect(p) => {
                if self.connected.insert(p) {
                    self.hub.peer_connected(p, 0);
                }
            }
            Op::Disconnect(p) => {
                if self.connected.remove(&p) {
                    self.hub.peer_disconnected(&p);
                }
            }
            Op::Dial(pick, ok) => self.dial(pick, ok),
            Op::Lose(pick) => {
                if let Some(&session) = self.net_open.iter().nth(pick % self.net_open.len().max(1))
                {
                    self.net_open.remove(&session);
                    self.refused.retain(|(_, s)| *s != session);
                    self.routes.retain(|(_, s)| *s != session);
                    self.hub.session_lost(self.now, session.0, session.1);
                }
            }
            Op::Refuse(d, pick) => {
                let routes: Vec<_> = self.routes.iter().filter(|(doc, _)| *doc == d).collect();
                if let Some(&&(_, session)) = routes.get(pick % routes.len().max(1)) {
                    self.refused.insert((d, session));
                    self.hub.refused(d as u64, session);
                }
            }
            Op::Want(p, waiter) => {
                let waiter = waiter.then(|| {
                    self.next_waiter += 1;
                    self.waiters.insert(self.next_waiter, p);
                    self.next_waiter
                });
                self.hub.keyhive_wanted(&p, waiter);
            }
            Op::RoundDone(pick, ok) => self.round_done(pick, ok),
            Op::Admit(n) => {
                self.admission_head += u64::from(n);
                self.hub.admission_advanced(self.admission_head);
            }
            Op::Advance(i) => {
                self.now += [100, 5_000, 25_000, 60_000, 600_000].map(Duration::from_millis)[i];
                let docs: Vec<_> = self.machines.keys().copied().collect();
                for doc in docs {
                    self.flush_doc(doc);
                }
            }
            Op::Restart => self.restart(),
            Op::Drain => {
                self.draining = true;
                self.hub.drain();
            }
        }
        self.pump();
        self.submit_ready_plans();
        self.check();
    }

    fn open(&mut self, d: usize) {
        if self.hub.demand(d as u64).is_err() {
            return;
        }
        *self.opening.entry(d).or_default() += 1;
        self.pump();
        self.serve_opening(d);
    }

    fn close_handle(&mut self, d: usize, pick: usize) {
        let Some(machine) = self.machines.get_mut(&d) else {
            return;
        };
        if machine.handles.is_empty() {
            return;
        }
        let at = pick % machine.handles.len();
        let handle = machine.handles.remove(at);
        machine.doc.handle_dropped(handle);
        self.flush_doc(d);
        self.hub.undemand(d as u64);
    }

    fn plan(&mut self, mask: usize) {
        let docs: Vec<usize> = (0..DOCS).filter(|d| mask >> d & 1 == 1).collect();
        for &d in &docs {
            if self.hub.demand(d as u64).is_err() {
                for &held in docs.iter().take_while(|h| **h != d) {
                    self.hub.undemand(held as u64);
                }
                return;
            }
        }
        self.next_ticket += 1;
        let ticket = self.next_ticket;
        self.plans.insert(ticket, docs);
        self.deferred.push(ticket);
        self.pump();
    }

    /// Submit the plans whose documents all have a machine that is not stopping.
    fn submit_ready_plans(&mut self) {
        let ready: Vec<_> = self
            .deferred
            .iter()
            .copied()
            .filter(|ticket| {
                self.plans[ticket].iter().all(|d| {
                    self.hub.doc(*d as u64).serving.is_some() && self.machines.contains_key(d)
                })
            })
            .collect();
        for ticket in ready {
            self.deferred.retain(|t| *t != ticket);
            let docs = self.plans[&ticket].clone();
            let writes = docs.iter().map(|&d| (d as u64, self.plan_work())).collect();
            self.coordinator.submit(ticket, writes);
            self.pump_coordinator();
        }
    }

    fn stand(&mut self, d: usize) {
        let eligible = self.eligible(d);
        if self.told_standing.get(&d) != Some(&eligible) {
            self.refused.retain(|(doc, _)| *doc != d);
            self.told_standing.insert(d, eligible.clone());
        }
        self.hub.standing(d as u64, eligible);
    }

    fn restand(&mut self) {
        for d in 0..DOCS {
            if self.hub.interest().phase(&(d as u64)).is_some() {
                self.stand(d);
            }
        }
    }

    fn feed(&mut self, count: usize) {
        for _ in 0..count {
            let Some((rev, doc)) = self.feed_log.pop_front() else {
                return;
            };
            // the feed holds a tree at its latest revision only
            if rev != self.world.tree_rev(doc) {
                self.complete_feed(rev);
                continue;
            }
            let readable = self.world.docs[doc].readable;
            self.hub.feed(FeedRow {
                doc: doc as u64,
                rev,
                frontier_rev: self.world.frontier_rev(doc),
                readable,
            });
            let seen = self.delivered_tree.entry(doc).or_default();
            *seen = (*seen).max(rev);
            self.told_readable.insert(doc, readable);
            self.complete_feed(rev);
        }
    }

    /// Tell the Hub that every row at or below `rev` was delivered, once the log has no more.
    fn complete_feed(&mut self, rev: Rev) {
        if self.feed_log.front().is_none_or(|(next, _)| *next > rev) {
            self.hub.feed_complete(rev);
        }
    }

    fn epoch(&mut self, pick: usize) {
        if self.epoch_mail.is_empty() {
            return;
        }
        let doc = self.epoch_mail.remove(pick % self.epoch_mail.len());
        let readable = self.world.docs[doc].readable;
        let tree_rev = self.world.tree_rev(doc);
        let frontier_rev = self.world.frontier_rev(doc);
        let before = self.hub.doc(doc as u64).catch;
        self.hub.epoch_moved(EpochMove {
            doc: doc as u64,
            readable,
            tree_rev,
            frontier_rev,
        });
        let seen = self.delivered_tree.entry(doc).or_default();
        *seen = (*seen).max(tree_rev);
        self.told_readable.insert(doc, readable);
        // law 4: the epoch move of a parked document that is now readable and stale wakes it
        if before == Some(Catch::Parked) && readable && frontier_rev < tree_rev && !self.draining {
            assert_ne!(
                self.hub.doc(doc as u64).catch,
                Some(Catch::Parked),
                "doc {doc}: a parked document did not wake on its epoch move (law 4)"
            );
        }
    }

    fn doc_mail(&mut self, pick: usize) {
        let docs: Vec<usize> = self
            .mailbox
            .iter()
            .filter(|(_, mail)| !mail.is_empty())
            .map(|(doc, _)| *doc)
            .collect();
        if docs.is_empty() {
            return;
        }
        let doc = docs[pick % docs.len()];
        let item = self
            .mailbox
            .get_mut(&doc)
            .and_then(VecDeque::pop_front)
            .expect("a document with mail");
        let (generation, input) = match item {
            Mail::Stop(generation) => return self.stop_requested(doc, generation),
            Mail::Msg(generation, input) => (generation, input),
        };
        let machine = self
            .machines
            .get_mut(&doc)
            .expect("mail for a running machine");
        assert_eq!(machine.generation, generation, "mail for another machine");
        machine.consumed += 1;
        match input {
            ToDoc::Notice(notice) => {
                if let Some(readable) = notice.readable {
                    machine.doc.epoch_moved(readable);
                }
                if let Some(rev) = notice.tree {
                    machine.doc.tree_changed(rev);
                }
                for _ in 0..notice.acquired {
                    machine.interest += 1;
                    machine.doc.interest_acquired();
                }
                for _ in 0..notice.released {
                    machine.interest -= 1;
                    machine.doc.interest_released();
                }
            }
            ToDoc::ActorAssigned { call, actor } => {
                assert!(
                    self.actors.insert(actor.to_bytes().to_vec()),
                    "an actor was reused"
                );
                machine.doc.actor_assigned(call, actor);
            }
        }
        self.flush_doc(doc);
    }

    fn deliver_report(&mut self, pick: usize) {
        if self.reports.is_empty() {
            return;
        }
        let at = pick % self.reports.len();
        // reports of one document arrive in order: take the first of the document's
        let doc = self.reports[at].0;
        let at = self
            .reports
            .iter()
            .position(|(d, ..)| *d == doc)
            .expect("the picked one is there");
        let (doc, generation, report) = self.reports.remove(at).expect("in range");
        let answered = matches!(report, Report::Stopped | Report::StopRefused);
        if answered {
            self.stopping.remove(&doc);
        }
        self.hub.reported(doc as u64, generation, report);
        if report == Report::StopRefused {
            self.serve_opening(doc);
        }
    }

    fn complete(&mut self, pick: usize, fail: bool) {
        if self.flights.is_empty() {
            return;
        }
        let at = pick % self.flights.len();
        let flight = self.flights.remove(at);
        self.answer(flight, fail);
    }

    fn dial(&mut self, pick: usize, ok: bool) {
        let Some((&call, &session)) = self
            .dial_flights
            .iter()
            .nth(pick % self.dial_flights.len().max(1))
        else {
            return;
        };
        self.dial_flights.remove(&call);
        if ok {
            self.net_open.insert(session);
        }
        self.hub.dialed(self.now, call, ok);
    }

    fn round_done(&mut self, pick: usize, ok: bool) {
        let Some((&call, &(peer, live))) = self
            .round_flights
            .iter()
            .nth(pick % self.round_flights.len().max(1))
        else {
            return;
        };
        self.round_flights.remove(&call);
        if !live {
            self.hub.round_done(call, Outcome::Cancelled);
        } else if ok {
            self.next_seq += 1;
            let seq = self.admission_head + self.next_seq % 3;
            self.completed_seq.insert(peer, seq);
            self.hub.round_done(call, Outcome::Done(seq));
        } else {
            self.hub.round_done(call, Outcome::Failed);
        }
    }

    // ── the quiet drain ─────────────────────────────────────────────────────────────────────

    /// Answer everything reliably, deliver every message, let time pass; then check what stands.
    pub(super) fn quiesce(&mut self) {
        for _ in 0..40 {
            let before = self.activity();
            while !self.feed_log.is_empty() {
                self.step(&Op::Feed(8));
            }
            while !self.epoch_mail.is_empty() {
                self.step(&Op::Epoch(0));
            }
            while !self.mailbox.values().all(VecDeque::is_empty) {
                self.step(&Op::DocMail(0));
            }
            while !self.reports.is_empty() {
                self.step(&Op::Reports(0));
            }
            while !self.flights.is_empty() {
                self.step(&Op::Complete(0, false));
            }
            while !self.dial_flights.is_empty() {
                self.step(&Op::Dial(0, true));
            }
            while !self.round_flights.is_empty() {
                self.step(&Op::RoundDone(0, true));
            }
            self.step(&Op::Admit(3));
            self.step(&Op::Advance(1));
            if self.activity() == before && self.quiet() {
                break;
            }
        }
        // the callers let go of everything, and time passes
        for d in 0..DOCS {
            while self.machines.get(&d).is_some_and(|m| !m.handles.is_empty()) {
                self.step(&Op::CloseHandle(d, 0));
            }
            while self.leases.get(&d).is_some_and(|n| *n > 0) {
                self.step(&Op::InterestOff(d));
            }
        }
        for _ in 0..12 {
            while !self.mailbox.values().all(VecDeque::is_empty) {
                self.step(&Op::DocMail(0));
            }
            while !self.reports.is_empty() {
                self.step(&Op::Reports(0));
            }
            while !self.flights.is_empty() {
                self.step(&Op::Complete(0, false));
            }
            self.step(&Op::Advance(3));
        }
        self.check_quiet();
    }

    fn activity(&self) -> (usize, usize, usize, usize) {
        (
            self.machines.len(),
            self.flights.len(),
            self.reports.len(),
            self.plans.len(),
        )
    }

    fn quiet(&self) -> bool {
        self.flights.is_empty()
            && self.reports.is_empty()
            && self.mailbox.values().all(VecDeque::is_empty)
            && self.plans.is_empty()
            && self.round_flights.is_empty()
            && self.dial_flights.is_empty()
            && self.waiters.keys().all(|w| self.resolved.contains(w))
    }
}
