//! What the simulation asserts: after every step, at every `Synced`, and after a quiet drain.

use super::*;

impl Sim {
    /// Laws 2, 3, 9, 12 and 13 after every step, from outside the machine.
    pub(super) fn check_step(&mut self) {
        // Law 13: the machine recomputes every part's fact against its incremental one in test
        // builds; asserting it again here covers inputs that did not flush.
        self.sync.assert_facts();
        for (part, flag) in &self.sync.parts {
            assert_eq!(
                self.mirror.get(part).copied().unwrap_or(false),
                *flag,
                "the edges the driver saw disagree with the machine's fact for {part}"
            );
        }
        for (part, flag) in &self.mirror {
            assert!(
                !flag || self.sync.parts.get(part) == Some(&true),
                "{part} was reported Synced and is not wanted or not synced"
            );
        }
        for part in self.sync.walks.keys() {
            self.walked.insert(part.clone());
        }
        self.check_bounded_work();
        self.check_persisted();
    }

    /// Law 12: in-flight backend calls stay within the window and the fetch cap, and the
    /// rounds in flight within their lanes and the window.
    fn check_bounded_work(&self) {
        let jobs = &self.shape.config.jobs;
        let flights = || self.local.calls.values();
        let fetches = flights()
            .filter(|flight| matches!(flight, Flight::Fetch { .. }))
            .count();
        assert!(fetches <= jobs.fetch_cap, "{fetches} fetches in flight");
        let mut per_obj: BTreeMap<&ObjKey, (usize, usize)> = default();
        for flight in flights() {
            let counts = per_obj.entry(&flight.job().obj).or_default();
            match flight {
                Flight::Record { .. } => counts.0 += 1,
                Flight::Fetch { .. } => counts.1 += 1,
            }
        }
        for (obj, (records, fetches)) in &per_obj {
            assert!(
                *records <= 1 && *fetches <= 1,
                "law 1 of ObjectJobs: {records} records and {fetches} fetches of {obj} in flight"
            );
        }
        // a page is never split inside a revision, so it can overshoot the window by the
        // objects of one revision less one: a batch commits at most `MAX_BATCH` objects. An
        // object that failed may have waited its backoff out of the window and be retried
        // beside a full one (law 7 of ObjectJobs), so only the others are bounded.
        let bound = jobs.window + MAX_BATCH - 1;
        let counted = per_obj
            .keys()
            .filter(|obj| !self.local.failed.contains(**obj))
            .count();
        assert!(
            counted <= bound,
            "{counted} objects that never failed have calls in flight, the window is {} and a \
             revision brings up to {MAX_BATCH}",
            jobs.window
        );
        // a reset session leaves the old session's rounds in flight; they are answered stale
        let session = self.sync.client.session_id();
        let rounds = self
            .sync
            .calls
            .values()
            .filter(
                |call| matches!(call, Call::Round { request, .. } if request.session_id == session),
            )
            .count();
        assert!(
            rounds <= 2,
            "{rounds} rounds of the session in flight, at most one per lane"
        );
        assert!(
            self.sync.lanes.in_flight_limit() <= jobs.window,
            "the rounds in flight ask for {} rows, the window is {}",
            self.sync.lanes.in_flight_limit(),
            jobs.window
        );
    }

    /// A persisted cursor is crash-safe: everything the peer has at or below it, the local
    /// store holds. For each object whose row the peer last stamped at or below the cursor,
    /// the local membership is the peer's membership at the cursor; a row stamped after the
    /// cursor is the replay's to deliver, so it is not judged.
    fn check_persisted(&self) {
        for (part, persisted) in &self.local.persisted {
            // A cursor of another generation is a position in a log the peer no longer has:
            // the part restarts when it is next decided, and the cursor says nothing now.
            if persisted.generation != self.remote.generation() {
                continue;
            }
            let cursor = &persisted.rev;
            assert!(
                *cursor <= self.remote.head() || self.remote.unminted,
                "{part} persisted at {cursor}, past the peer's head"
            );
            let local = self.local.members(part);
            let remote = self.remote.view_at(*cursor).members[part].clone();
            for obj in (0..self.shape.objs).map(obj) {
                if self.remote.row_rev(&obj, part) <= *cursor {
                    assert!(
                        local.contains(&obj) == remote.contains(&obj)
                            || (local.contains(&obj) && self.remote.is_ghost(part, &obj)),
                        "{part} persisted at {cursor} (head {}) but the local store disagrees about \
                         {obj}: a crash now loses it for good. local {local:?}, the peer's at \
                         the cursor {remote:?}, {obj}'s row was last stamped at {}, walked {}",
                        self.remote.head(),
                        self.remote.row_rev(&obj, part),
                        self.walked.contains(part)
                    );
                }
            }
        }
    }

    /// `Synced(part)` was just emitted: the local members equal the peer's members readable by
    /// us as of the cursor the part is at. A part whose walk read state newer than that cursor
    /// may be ahead of it, so it is compared with every view from the cursor on, until it has
    /// matched the cursor's own once.
    pub(super) fn check_synced(&mut self, part: &PartKey) {
        let at = self.sync.drained[part];
        let local = self.local.members(part);
        let view = self.remote.view_at(at);
        assert!(
            view.readable.contains(part),
            "Synced({part}) at {at}, where the peer did not let us read it"
        );
        if self.remote.holds(part, &local, &view.members[part]) {
            self.walked.remove(part);
            return;
        }
        assert!(
            self.walked.contains(part)
                && self
                    .remote
                    .views_from(at)
                    .any(|later| later.readable.contains(part)
                        && self.remote.holds(part, &local, &later.members[part])),
            "Synced({part}) at {at} with local members {local:?}, the peer's readable members \
             then were {:?}",
            view.members[part]
        );
    }

    // # The quiet drain

    /// No churn, no writes, no loss: deliver what is in the network, complete the backend, let
    /// time pass to the next deadline, and repeat until the machine is quiet against the
    /// peer's state now.
    pub(super) fn quiesce(&mut self) {
        for cycle in 0..400 {
            self.cycle();
            if self.is_quiet() {
                self.check_converged();
                return;
            }
            // a server that still holds stale sessions refuses ours until they are swept
            if cycle % 40 == 39 {
                self.step(Op::Sweep);
            }
        }
        panic!("the simulation did not quiesce:\n{}", self.dump());
    }

    /// One round trip's half: every message in the network now is delivered (what they cause
    /// waits for the next cycle), every backend call completes, and the clock moves to the
    /// machine's next deadline.
    pub(super) fn cycle(&mut self) {
        self.deliver_all();
        self.check_contradictions();
        for call in self.local.calls.keys().copied().collect::<Vec<_>>() {
            self.progress_call(call, true);
        }
        self.pass_time();
    }

    /// Every backend call completes, and the machine is flushed once for all of them.
    pub(super) fn complete_batch(&mut self) {
        for call in self.local.calls.keys().copied().collect::<Vec<_>>() {
            let payload = self.remote.payload(&self.local.calls[&call].job().obj);
            match self.local.step(call, payload, true) {
                Step::Running => unreachable!("a fetch is run to its end"),
                Step::Record(outcome) => self.sync.record_ended(call, outcome),
                Step::Fetch(outcome) => self.sync.fetch_ended(call, outcome),
            }
        }
        self.flush();
    }

    /// Deliver every message in the network now, in order; what they cause waits.
    pub(super) fn deliver_all(&mut self) {
        for _ in 0..self.net.len() {
            let msg = self.net.remove(0);
            self.deliver_msg(msg);
        }
    }

    /// The clock moves to the machine's next deadline, or a second on.
    pub(super) fn pass_time(&mut self) {
        match self.sync.next_due() {
            Some(due) if due > self.now => self.now = due,
            Some(_) => {}
            None => self.now += Duration::from_secs(1),
        }
        self.remote.sweep(self.now);
        self.flush();
    }

    /// Law 4 of ObjectJobs: a fetch whose part the peer no longer lets the object into is
    /// cancelled once the machine has applied the removal. A part drained at the head has
    /// applied everything; a walking part may lag.
    fn check_contradictions(&self) {
        let head = self.remote.head();
        for (call, flight) in &self.local.calls {
            let Flight::Fetch {
                job,
                cancelled: false,
                ..
            } = flight
            else {
                continue;
            };
            for part in &job.add {
                let view = self.remote.view_at(head);
                assert!(
                    !self.wanted_parts.contains(part)
                        || self.sync.walking(part)
                        || self.sync.drained.get(part) != Some(&head)
                        || !view.readable.contains(part)
                        || view.members[part].contains(&job.obj),
                    "fetch {call} adds {} to {part}, which the peer removed it from: \
                     it was not cancelled",
                    job.obj
                );
            }
        }
    }

    /// Quiet, decided against the current state: nothing in flight for the backend or a
    /// walk, every wanted route we may read acknowledged (a route we may not read keeps being
    /// retried on its ladder, so the session is never idle), and every wanted part we may read
    /// has drained at the peer's head as of now, never a flag an earlier message set.
    fn is_quiet(&self) -> bool {
        let head = self.remote.head();
        let part_route = |part: &PartKey| Route::Part {
            part_key: part.clone(),
        };
        let parts_ok = self.wanted_parts.iter().all(|part| {
            if self.remote.readable(&part_route(part)) {
                self.sync.parts.get(part) == Some(&true)
                    && self.sync.drained.get(part) == Some(&head)
            } else {
                self.sync.parts.get(part) == Some(&false)
            }
        });
        let objects_ok = self.wanted_objs.iter().all(|obj| {
            let route = Route::Object {
                obj_key: obj.clone(),
            };
            !self.remote.readable(&route)
                || (self.sync.client.is_acked(&route) && self.sync.objects.get(obj) == Some(&head))
        });
        let readable_blocked = self
            .sync
            .client
            .blocked()
            .any(|route| self.remote.readable(route));
        // a walk of a part we may not read retries for ever, until the part is unwanted
        let walks_ok = self
            .sync
            .walks
            .keys()
            .all(|part| !self.remote.readable(&part_route(part)));
        self.local.calls.is_empty()
            && walks_ok
            && self.sync.job_calls.is_empty()
            && !readable_blocked
            && self.sync.jobs.unsettled() == 0
            && parts_ok
            && objects_ok
    }

    /// After a quiet drain: the local view of every wanted readable part is the peer's, and
    /// the local content of every wanted readable object route is the peer's.
    fn check_converged(&self) {
        let head = self.remote.head();
        let view = self.remote.view_at(head);
        for part in &self.wanted_parts {
            if view.readable.contains(part) {
                assert!(
                    self.remote
                        .holds(part, &self.local.members(part), &view.members[part]),
                    "{part} is Synced and its local members {:?} are not the peer's {:?}",
                    self.local.members(part),
                    view.members[part]
                );
            }
        }
        for obj in &self.wanted_objs {
            let route = Route::Object {
                obj_key: obj.clone(),
            };
            if self.remote.readable(&route) {
                assert_eq!(
                    self.local.payload(obj),
                    self.remote.payload(obj),
                    "the content of routed {obj} is not the peer's"
                );
            }
        }
    }

    /// Law 14: dropping the machine cancels every call in flight, and reports nothing else.
    pub(super) fn check_disconnect(&mut self) {
        let in_flight: BTreeSet<CallId> = self.sync.calls.keys().copied().collect();
        let sync = std::mem::replace(
            &mut self.sync,
            PeerSync::new(self.shape.config.clone(), Uuid::from_u128(u128::MAX)),
        );
        let mut out = vec![];
        sync.disconnect(&mut out);
        let cancelled: BTreeSet<CallId> = out
            .iter()
            .map(|cmd| match cmd {
                PeerCommand::Cancel { call } => *call,
                other => panic!("disconnect reported {other:?}"),
            })
            .collect();
        assert_eq!(
            cancelled, in_flight,
            "law 14: every call in flight is cancelled"
        );
    }

    pub(super) fn dump(&self) -> String {
        format!(
            "head {}\nwanted parts {:?}\nwanted objs {:?}\nparts {:?}\ndrained {:?}\nobjects {:?}\n\
             net {}\nbackend {:?}\nwalks {:?}\ncalls {:?}\nclient settled {} blocked {:?}\nroom {}",
            self.remote.head(),
            self.wanted_parts,
            self.wanted_objs,
            self.sync.parts,
            self.sync.drained,
            self.sync.objects,
            self.net.len(),
            self.local.calls,
            self.sync.walks.keys().collect::<Vec<_>>(),
            self.sync.calls,
            self.sync.client.is_settled(),
            self.sync.client.blocked().collect::<Vec<_>>(),
            self.sync.jobs.room(),
        )
    }
}
