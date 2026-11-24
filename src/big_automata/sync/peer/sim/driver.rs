//! The driver: the network's side of the simulation. It serves the peer's answers, hands them to
//! the machine (filtering listings and leaves against the local store, as a real driver does),
//! loses messages, and tampers with pages.

use super::*;

impl Sim {
    /// The call is lost: a drop, a sweep, a restart. The machine hears `Lost`.
    pub(super) fn lose(&mut self, msg: &Msg) {
        let call = msg.call;
        match msg.kind() {
            Kind::Summary => self.input(|s| s.summary_replied(call, SummaryReply::Lost)),
            Kind::Session => self.input(|s| s.update_replied(call, Reply::Lost)),
            Kind::Round => self.input(|s| s.round_replied(call, Reply::Lost)),
            Kind::List => {
                self.relist.remove(&call);
                self.input(|s| s.list_replied(call, Reply::Lost));
            }
            Kind::Leaf => self.input(|s| s.leaf_replied(call, Reply::Lost)),
        }
    }

    pub(super) fn lose_all(&mut self) {
        for msg in std::mem::take(&mut self.net) {
            self.lose(&msg);
        }
    }

    pub(super) fn deliver_msg(&mut self, msg: Msg) {
        match msg.body {
            Body::Req(req) => self.serve(msg.call, req),
            Body::Resp(resp) => self.receive(msg.call, resp, msg.hostile),
        }
    }

    /// The peer answers a request.
    fn serve(&mut self, call: CallId, req: Req) {
        let resp = match req {
            Req::Summary(parts) => Resp::Summary(self.remote.summary(&parts)),
            Req::Session(update) => Resp::Session(self.remote.update(&update, self.now)),
            Req::Round(request) => {
                let answer = self.remote.round(&request, self.now);
                Resp::Round(request, answer)
            }
            Req::List {
                part,
                offset,
                since,
                level,
            } => Resp::List {
                raw: self.remote.list(&part, offset, level, since),
                part,
                offset,
                since,
                level,
            },
            Req::Leaf {
                part,
                since,
                buckets,
                seed,
            } => Resp::Leaf {
                // two entries a page, so a bucket takes several
                raw: self.remote.leaves(&part, &buckets, since, &seed, 2),
                part,
                buckets,
                seed,
            },
        };
        self.net.push(Msg {
            call,
            body: Body::Resp(resp),
            hostile: None,
        });
    }

    /// The machine's side of an answer, with the driver's filtering against the local store.
    fn receive(&mut self, call: CallId, resp: Resp, hostile: Option<Hostile>) {
        if std::env::var("SIM_TRACE").is_ok() {
            eprintln!("TRACE answer {call}: {resp:?}");
        }
        match resp {
            Resp::Summary(reply) => self.input(|s| s.summary_replied(call, reply)),
            Resp::Session(answer) => self.input(|s| s.update_replied(call, Reply::Answer(answer))),
            Resp::Round(_, answer) => {
                let before = self.signature();
                self.sync.round_replied(call, Reply::Answer(answer));
                let rejected = match hostile {
                    None => false,
                    Some(Hostile::Always) => true,
                    Some(Hostile::IfObject(id)) => {
                        matches!(self.sync.client.route_of(id), Some(Route::Object { .. }))
                    }
                };
                if rejected {
                    assert_eq!(before, self.signature(), "law 3: a tampered page applied");
                }
                self.flush();
            }
            Resp::List {
                part,
                offset,
                since,
                level,
                raw,
            } => {
                // an answer to an earlier stage of the listing, or to a call that is over
                if self.relist.get(&call) != Some(&offset) || !self.sync.calls.contains_key(&call) {
                    return;
                }
                let Some(raw) = raw else {
                    self.relist.remove(&call);
                    return self.input(|s| s.list_replied(call, Reply::Lost));
                };
                let filtered =
                    block_on(filter_buckets(part.clone(), level, raw, &self.local.store)).unwrap();
                match filtered {
                    FilteredBuckets::Done => {
                        self.relist.remove(&call);
                        self.input(|s| s.list_replied(call, Reply::Answer(vec![])));
                    }
                    FilteredBuckets::Handoff(buckets) => {
                        self.relist.remove(&call);
                        self.input(|s| s.list_replied(call, Reply::Answer(buckets)));
                    }
                    FilteredBuckets::Relist(next) => {
                        self.relist.insert(call, next);
                        let req = Req::List {
                            part,
                            offset: next,
                            since,
                            level,
                        };
                        self.send(call, req);
                    }
                }
            }
            Resp::Leaf {
                part,
                buckets,
                seed,
                raw,
            } => {
                if !self.sync.calls.contains_key(&call) {
                    return;
                }
                let Some(raw) = raw else {
                    return self.input(|s| s.leaf_replied(call, Reply::Lost));
                };
                let filtered =
                    block_on(filter_objects(part, buckets, raw, seed, &self.local.store)).unwrap();
                self.input(|s| s.leaf_replied(call, Reply::Answer(filtered)));
            }
        }
    }

    /// What a rejected page must leave alone: the cursors, the drained marks and the window.
    fn signature(
        &self,
    ) -> (
        BTreeMap<PartKey, Rev>,
        BTreeMap<ObjKey, Rev>,
        Vec<Rev>,
        usize,
    ) {
        (
            self.sync.drained.clone(),
            self.sync.objects.clone(),
            self.wanted_parts
                .iter()
                .filter_map(|part| {
                    let applied = self.sync.lifecycle.cursor(part)?;
                    Some(self.sync.cursor.part_replay_cursor(part, applied))
                })
                .collect(),
            self.sync.jobs.room(),
        )
    }

    /// Tamper with one answered round in the network.
    pub(super) fn tamper(&mut self, kind: u8, pick: u8) {
        let rounds: Vec<usize> = self
            .net
            .iter()
            .enumerate()
            .filter(|(_, msg)| matches!(msg.body, Body::Resp(Resp::Round(_, Ok(_)))))
            .map(|(at, _)| at)
            .collect();
        if rounds.is_empty() {
            return;
        }
        let at = rounds[pick as usize % rounds.len()];
        let Body::Resp(Resp::Round(request, Ok(page))) = &mut self.net[at].body else {
            unreachable!("picked above");
        };
        let object_target = request
            .targets
            .iter()
            .map(|(id, _)| *id)
            .find(|id| matches!(self.sync.client.route_of(*id), Some(Route::Object { .. })));
        let hostile = tamper(request, page, kind, object_target, obj(0));
        self.net[at].hostile = hostile;
    }
}
