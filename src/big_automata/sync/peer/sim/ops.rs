//! The ops a case is made of, and the proptest over them.

use super::*;

#[derive(Debug, Clone)]
pub(super) enum Op {
    /// The peer commits: a payload (`kind % 3 == 0`), an add or a remove of `obj` in `part`.
    Write {
        kind: u8,
        obj: u8,
        part: u8,
        val: u8,
    },
    /// The peer grants us the part, or revokes it.
    Access(u8),
    /// The embedder changes its routes, as bit masks over the universe.
    Routes {
        add_parts: u8,
        rem_parts: u8,
        add_objs: u8,
        rem_objs: u8,
    },
    Deliver(u8),
    Drop(u8),
    Duplicate(u8),
    /// Tamper with an answered round in the network.
    Hostile {
        kind: u8,
        pick: u8,
    },
    /// The backend makes progress on one call.
    Backend(u8),
    Advance(u8),
    /// Time moves to the machine's next deadline.
    Tick,
    /// The TTL passes with no request: the server forgets every session.
    Sweep,
    /// The server restarts and forgets every session.
    Restart,
    /// The process dies and restarts. Bit `i` of `committed` says the `i`th call in flight
    /// had committed.
    Crash {
        committed: u8,
        close: bool,
    },
    /// The peer's summaries claim every part is far behind, so the machine walks.
    Bias(bool),
}

impl Sim {
    /// A replay of `parts` is about to restart from their persisted cursors (a crash, or a part
    /// unwanted and wanted again): note the members the local store holds or may soon hold above
    /// them, the ones the peer's removal never reaches (see [`Sim::ghosts`]).
    fn note_ghosts(&mut self, parts: &[PartKey]) {
        for part in parts {
            let cursor = self.local.persisted.get(part).copied().unwrap_or(0);
            let covered = &self.remote.view_at(cursor).members[part];
            let in_flight = self
                .local
                .calls
                .values()
                .map(|flight| flight.job())
                .filter(|job| job.add.contains(part))
                .map(|job| job.obj.clone());
            let ahead = self
                .local
                .holds(part)
                .into_iter()
                .chain(in_flight)
                .filter(|obj| !covered.contains(obj))
                .collect::<Vec<_>>();
            self.ghosts.entry(part.clone()).or_default().extend(ahead);
        }
    }

    fn crash(&mut self, committed: u8, close: bool) {
        self.note_ghosts(&(0..self.shape.parts).map(part).collect::<Vec<_>>());
        for (at, call) in self
            .local
            .calls
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .into_iter()
            .enumerate()
        {
            let payload = self.remote.payload(&self.local.calls[&call].job().obj);
            self.local
                .die_with(call, (committed >> (at % 8)) & 1 == 1, payload);
        }
        self.net.clear();
        self.relist.clear();
        self.mirror.clear();
        self.walked.clear();
        if close {
            self.remote.close(self.sync.client.session_id());
        }
        self.session += 1;
        self.sync = PeerSync::new(self.shape.config.clone(), Uuid::from_u128(self.session));
        let parts: Vec<(PartKey, Rev)> = self
            .wanted_parts
            .iter()
            .map(|part| {
                (
                    part.clone(),
                    self.local.persisted.get(part).copied().unwrap_or(0),
                )
            })
            .collect();
        let objs: Vec<ObjKey> = self.wanted_objs.iter().cloned().collect();
        self.input(|s| s.update(&parts, &[], &objs, &[]));
    }

    pub(super) fn step(&mut self, op: Op) {
        let Shape {
            parts,
            objs,
            obj_routes,
            ..
        } = self.shape.clone();
        match op {
            Op::Write {
                kind,
                obj,
                part,
                val,
            } => {
                let (obj, part) = (obj % objs, part % parts);
                let write = match kind % 3 {
                    0 => laws::set(&[(obj, val % 3)]),
                    1 => laws::add(&[(obj, part)]),
                    _ => laws::remove(&[(obj, part)]),
                };
                self.remote.commit(write);
            }
            Op::Access(index) => {
                let index = index % parts;
                let readable = self.remote.readable(&Route::Part {
                    part_key: laws::part(index),
                });
                let write = if readable {
                    laws::revoke(&[(index, 0)])
                } else {
                    laws::grant(&[(index, 0)])
                };
                self.remote.commit(write);
            }
            Op::Routes {
                add_parts,
                rem_parts,
                add_objs,
                rem_objs,
            } => {
                let picked =
                    |mask: u8, count: u8| (0..count).filter(move |at| (mask >> at) & 1 == 1);
                let removed_parts: Vec<PartKey> =
                    picked(rem_parts, parts).map(laws::part).collect();
                let added_parts: Vec<(PartKey, Rev)> = picked(add_parts, parts)
                    .map(|at| {
                        let part = laws::part(at);
                        let cursor = self.local.persisted.get(&part).copied().unwrap_or(0);
                        (part, cursor)
                    })
                    .collect();
                let removed_objs: Vec<ObjKey> = picked(rem_objs, obj_routes).map(obj).collect();
                let added_objs: Vec<ObjKey> = picked(add_objs, obj_routes).map(obj).collect();
                self.note_ghosts(&removed_parts);
                for part in &removed_parts {
                    self.wanted_parts.remove(part);
                    self.walked.remove(part);
                }
                self.wanted_parts
                    .extend(added_parts.iter().map(|(part, _)| part.clone()));
                for obj in &removed_objs {
                    self.wanted_objs.remove(obj);
                }
                self.wanted_objs.extend(added_objs.iter().cloned());
                self.input(|s| s.update(&added_parts, &removed_parts, &added_objs, &removed_objs));
            }
            Op::Deliver(at) if !self.net.is_empty() => {
                let msg = self.net.remove(at as usize % self.net.len());
                self.deliver_msg(msg);
            }
            Op::Drop(at) if !self.net.is_empty() => {
                let msg = self.net.remove(at as usize % self.net.len());
                self.lose(&msg);
            }
            Op::Duplicate(at) if !self.net.is_empty() => {
                let msg = self.net[at as usize % self.net.len()].clone();
                self.net.push(msg);
            }
            Op::Deliver(_) | Op::Drop(_) | Op::Duplicate(_) => {}
            Op::Hostile { kind, pick } => self.tamper(kind, pick),
            Op::Backend(pick) => {
                let calls: Vec<CallId> = self.local.calls.keys().copied().collect();
                if !calls.is_empty() {
                    self.progress_call(calls[pick as usize % calls.len()], false);
                }
            }
            Op::Advance(at) => {
                const STEPS: [u64; 6] = [1, 2, 3, 5, 10, 30];
                self.now += Duration::from_secs(STEPS[at as usize % STEPS.len()]);
                self.remote.sweep(self.now);
                self.flush();
            }
            Op::Tick => {
                if let Some(due) = self.sync.next_due()
                    && due > self.now
                {
                    self.now = due;
                }
                self.remote.sweep(self.now);
                self.flush();
            }
            Op::Sweep => {
                self.lose_all();
                self.now += SESSION_TTL;
                self.remote.sweep(self.now);
                self.flush();
            }
            Op::Restart => {
                self.lose_all();
                self.remote.forget_sessions();
            }
            Op::Crash { committed, close } => self.crash(committed, close),
            Op::Bias(on) => self.remote.walk_bias = on,
        }
    }
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(kind, obj, part, val)| Op::Write { kind, obj, part, val }),
        2 => any::<u8>().prop_map(Op::Access),
        3 => (any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>()).prop_map(
            |(add_parts, rem_parts, add_objs, rem_objs)| Op::Routes {
                add_parts: add_parts & 0b111,
                // removals are rarer than additions, so parts mostly stay wanted
                rem_parts: rem_parts & (rem_parts >> 3) & 0b111,
                add_objs: add_objs & 0b11,
                rem_objs: rem_objs & (rem_objs >> 2) & 0b11,
            }
        ),
        14 => any::<u8>().prop_map(Op::Deliver),
        2 => any::<u8>().prop_map(Op::Drop),
        2 => any::<u8>().prop_map(Op::Duplicate),
        2 => (any::<u8>(), any::<u8>()).prop_map(|(kind, pick)| Op::Hostile { kind, pick }),
        6 => any::<u8>().prop_map(Op::Backend),
        2 => any::<u8>().prop_map(Op::Advance),
        2 => Just(Op::Tick),
        1 => Just(Op::Sweep),
        1 => Just(Op::Restart),
        1 => (any::<u8>(), any::<bool>()).prop_map(|(committed, close)| Op::Crash { committed, close }),
        1 => any::<bool>().prop_map(Op::Bias),
    ]
}

/// Run `ops`, drain, check the disconnect, then crash, restart and drain again.
pub(super) fn run(ops: Vec<Op>, seed: u64) {
    let mut sim = Sim::new(Shape::small(), seed);
    for op in ops {
        sim.step(op);
    }
    sim.quiesce();
    sim.step(Op::Crash {
        committed: 0,
        close: true,
    });
    sim.quiesce();
    sim.check_disconnect();
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_machine_agrees_with_the_peer_over_a_faulty_network(
        ops in proptest::collection::vec(op(), 0..200),
        seed in any::<u64>(),
    ) {
        run(ops, seed);
    }
}
