//! The differential proptest: random ops against [`ObjectJobs`] and the [`Oracle`], with a
//! simulated driver that answers calls with random outcomes.

use super::*;

use crate::sync::object_jobs::oracle::End;
use crate::sync::object_jobs::oracle::OSnap;
use crate::sync::object_jobs::oracle::Oracle;
use crate::sync::object_jobs::oracle::Stage;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub(in crate::sync::object_jobs::tests) enum How {
    Done,
    Fetch,
    /// A fetch's success
    Ok,
    Fail,
    Cancelled,
}

#[derive(Debug, Clone)]
pub(in crate::sync::object_jobs::tests) enum Op {
    Want {
        obj: u8,
        part: u8,
        /// `Some(payload)` is a `Present` want, `None` an `Absent` one
        present: Option<u8>,
        /// `None` is a bucket walk's want
        cursor: Option<u8>,
    },
    Route {
        obj: u8,
        on: bool,
    },
    /// An object route's event: a content want with this payload hint.
    Content {
        obj: u8,
        payload: u8,
    },
    Retire {
        part: u8,
    },
    /// The backend starts or stops failing every call for the object, for as long as it is on.
    Poison {
        obj: u8,
        on: bool,
    },
    Flush {
        secs: u8,
    },
    Finish {
        pick: usize,
        how: How,
    },
}

pub(in crate::sync::object_jobs::tests) fn op() -> impl Strategy<Value = Op> {
    let how = prop_oneof![
        Just(How::Done),
        Just(How::Fetch),
        Just(How::Ok),
        Just(How::Fail),
        Just(How::Cancelled),
    ];
    prop_oneof![
        6 => (0u8..3, 0u8..3, proptest::option::of(0u8..3), proptest::option::weighted(0.7, 1u8..=8))
            .prop_map(|(obj, part, present, cursor)| Op::Want { obj, part, present, cursor }),
        1 => (0u8..3, any::<bool>()).prop_map(|(obj, on)| Op::Route { obj, on }),
        1 => (0u8..3, 0u8..3).prop_map(|(obj, payload)| Op::Content { obj, payload }),
        1 => (0u8..3).prop_map(|part| Op::Retire { part }),
        2 => (0u8..3, any::<bool>()).prop_map(|(obj, on)| Op::Poison { obj, on }),
        3 => (0u8..4).prop_map(|secs| Op::Flush { secs }),
        6 => (any::<usize>(), how).prop_map(|(pick, how)| Op::Finish { pick, how }),
    ]
}

struct SimCall {
    kind: CallKind,
    obj: ObjKey,
    snap: OSnap,
    cancel_sent: bool,
    contradicted: bool,
}

pub(in crate::sync::object_jobs::tests) struct Sim {
    jobs: ObjectJobs,
    oracle: Oracle,
    now: Instant,
    calls: BTreeMap<CallId, SimCall>,
    /// The newest cursor wanted per (object, part): the cursor machine never redelivers one.
    ever: Map<(u8, u8), Rev>,
    reported: Vec<Settlement>,
    /// Objects whose every call fails: persistent failure, which lands them in backoff.
    poisoned: Vec<ObjKey>,
}

impl Sim {
    /// A window of 2 and a cap of 3 over the 3 objects the ops touch: persistently failing
    /// objects fill the cap while backing off outside the window, so both bind.
    pub(in crate::sync::object_jobs::tests) fn config() -> Config {
        Config {
            window: 2,
            max_tracked: 3,
            ..config()
        }
    }

    pub(in crate::sync::object_jobs::tests) fn new() -> Self {
        let config = Self::config();
        Self {
            oracle: Oracle::new(config.window, config.max_tracked),
            jobs: ObjectJobs::new(config),
            now: Instant::now(),
            calls: default(),
            ever: default(),
            reported: vec![],
            poisoned: vec![],
        }
    }

    pub(in crate::sync::object_jobs::tests) fn step(&mut self, op: Op) {
        match op {
            Op::Want {
                obj: o,
                part: p,
                present: payload,
                cursor,
            } => {
                self.want(o, p, payload, cursor);
            }
            Op::Route { obj: o, on } => {
                let old = self.oracle.set_route(&obj(o), on);
                self.jobs.set_object_route(obj(o), on);
                // turning the route off takes the content want away from every fetch it justified
                if old.is_some() {
                    for call in self.calls.values_mut() {
                        if call.kind == CallKind::Fetch
                            && call.obj == obj(o)
                            && call.snap.route.take().is_some()
                        {
                            call.contradicted = true;
                        }
                    }
                }
            }
            Op::Content { obj: o, payload } => {
                self.oracle.content(&obj(o), serde_json::json!(payload));
                self.jobs.want_content(obj(o), serde_json::json!(payload));
            }
            Op::Retire { part: p } => {
                for call in self.calls.values_mut() {
                    call.contradicted |= call.kind == CallKind::Fetch
                        && call
                            .snap
                            .parts
                            .iter()
                            .any(|(q, _, adds)| *q == part(p) && *adds);
                }
                self.ever.retain(|(_, q), _| *q != p);
                self.oracle.retire(&part(p));
                self.jobs.retire_part(&part(p));
            }
            Op::Poison { obj: o, on } => {
                self.poisoned.retain(|p| *p != obj(o));
                if on {
                    self.poisoned.push(obj(o));
                }
            }
            Op::Flush { secs } => {
                self.now += Duration::from_secs(secs.into());
                self.flush();
            }
            Op::Finish { pick, how } => {
                if !self.calls.is_empty() {
                    let call = *self.calls.keys().nth(pick % self.calls.len()).unwrap();
                    self.finish(call, how);
                }
            }
        }
        self.check();
    }

    fn want(&mut self, o: u8, p: u8, payload: Option<u8>, cursor: Option<u8>) {
        let want = match payload {
            Some(n) => present(n.into()),
            None => Want::Absent,
        };
        let origin = match cursor {
            Some(c) => {
                let c = Rev::from(c);
                let lane = match want {
                    Want::Present { .. } => Sync(obj(o)),
                    Want::Absent => RemoveFromPart(obj(o)),
                };
                let redelivery = self.ever.get(&(o, p)).is_some_and(|ever| c <= *ever)
                    && !self.oracle.pair_exists(&obj(o), &part(p));
                let settlement = Settlement::Cursor {
                    job: lane,
                    cursor: c,
                };
                let reported =
                    self.reported.contains(&settlement) || self.oracle.is_expected(&settlement);
                if redelivery || reported {
                    return;
                }
                Origin::Cursor(c)
            }
            None => Origin::Bucket,
        };
        let accepted = self.oracle.want(&obj(o), &part(p), want.clone(), origin);
        self.jobs.want(obj(o), part(p), want.clone(), origin);
        if let Origin::Cursor(c) = origin {
            let ever = self.ever.entry((o, p)).or_default();
            *ever = (*ever).max(c);
        }
        if accepted {
            for call in self.calls.values_mut() {
                call.contradicted |=
                    call.kind == CallKind::Fetch
                        && call.obj == obj(o)
                        && call.snap.parts.iter().any(|(q, _, adds)| {
                            *q == part(p) && *adds == matches!(want, Want::Absent)
                        });
            }
        }
    }

    fn finish(&mut self, call: CallId, how: How) {
        let sim = self.calls.remove(&call).unwrap();
        // a poisoned object's call fails, unless it was cancelled, which only needs the driver
        let how = match how {
            How::Cancelled if sim.cancel_sent => how,
            _ if self.poisoned.contains(&sim.obj) => How::Fail,
            _ => how,
        };
        let stage = match sim.kind {
            CallKind::Record => Stage::Fresh,
            CallKind::Fetch => Stage::NeedsFetch,
        };
        let end = match (sim.kind, how) {
            (CallKind::Record, How::Fail | How::Cancelled) | (CallKind::Fetch, How::Fail) => {
                End::Failed
            }
            (CallKind::Fetch, How::Cancelled) if sim.cancel_sent => End::Cancelled,
            (CallKind::Fetch, How::Cancelled) => End::Failed,
            _ => End::Ok,
        };
        self.oracle.ended(&sim.obj, stage, end);
        match sim.kind {
            CallKind::Record => {
                let outcome = match how {
                    How::Done | How::Ok => DONE,
                    How::Fetch => FETCH,
                    How::Fail | How::Cancelled => Outcome::Failed,
                };
                self.jobs.record_ended(call, outcome);
                match outcome {
                    DONE => self.oracle.settle(&sim.snap),
                    FETCH => self.oracle.promote(&sim.snap),
                    _ => {}
                }
            }
            CallKind::Fetch => {
                let outcome = match how {
                    How::Done | How::Fetch | How::Ok => Outcome::Succeeded(()),
                    How::Fail => Outcome::Failed,
                    // a driver only reports Cancelled for a call it was told to cancel
                    How::Cancelled if sim.cancel_sent => Outcome::Cancelled,
                    How::Cancelled => Outcome::Failed,
                };
                self.jobs.fetch_ended(call, outcome);
                if outcome == Outcome::Succeeded(()) {
                    self.oracle.settle(&sim.snap);
                }
            }
        }
    }

    fn in_flight(&self, o: &ObjKey, kind: CallKind) -> Option<&SimCall> {
        self.calls
            .values()
            .find(|call| call.kind == kind && call.obj == *o)
    }

    /// The fetch the oracle says is only kept alive by a live object route.
    fn route_keeps(&self, call: &SimCall) -> bool {
        call.snap.route.is_some() && self.oracle.route_seq(&call.obj).is_some()
    }

    fn flush(&mut self) -> usize {
        let mut out = vec![];
        self.jobs.flush(self.now, &mut out);
        let issued = out.len();
        let mut got = vec![];
        for cmd in out {
            match cmd {
                JobsCommand::Settled(settlement) => got.push(settlement),
                JobsCommand::Record { call, job } => {
                    let o = job.obj.clone();
                    assert!(
                        self.in_flight(&o, CallKind::Record).is_none(),
                        "law 1: two records"
                    );
                    assert!(
                        !self
                            .in_flight(&o, CallKind::Fetch)
                            .is_some_and(|fetch| fetch.contradicted),
                        "a record ran beside a contradicted fetch"
                    );
                    let (expected, snap) = self
                        .oracle
                        .job(&o, Stage::Fresh)
                        .expect("a record with no fresh want");
                    assert_eq!(job, expected, "law 5: the record is the difference");
                    self.oracle.issued(&o, Stage::Fresh);
                    self.register(call, CallKind::Record, o, snap);
                }
                JobsCommand::Fetch { call, job } => {
                    let o = job.obj.clone();
                    assert!(
                        self.in_flight(&o, CallKind::Fetch).is_none(),
                        "law 1: two fetches"
                    );
                    let (expected, snap) = self
                        .oracle
                        .job(&o, Stage::NeedsFetch)
                        .expect("a fetch with nothing to fetch");
                    assert_eq!(job, expected);
                    self.oracle.issued(&o, Stage::NeedsFetch);
                    self.register(call, CallKind::Fetch, o, snap);
                    let fetching = self
                        .calls
                        .values()
                        .filter(|c| c.kind == CallKind::Fetch)
                        .count();
                    assert!(
                        fetching <= Self::config().fetch_cap,
                        "law 7: {fetching} fetches"
                    );
                }
                JobsCommand::Cancel { call } => {
                    let sim = self.calls.get(&call).expect("a cancel for an unknown call");
                    assert_eq!(sim.kind, CallKind::Fetch);
                    assert!(!sim.cancel_sent, "cancelled twice");
                    assert!(
                        sim.contradicted && !self.route_keeps(sim),
                        "a cancel nothing asked for"
                    );
                    self.calls.get_mut(&call).unwrap().cancel_sent = true;
                }
            }
        }
        for settlement in &got {
            assert!(
                !self.reported.contains(settlement),
                "{settlement:?} settled twice"
            );
            if matches!(settlement, Settlement::Cursor { .. }) {
                self.reported.push(settlement.clone());
            }
        }
        let mut expected = self.oracle.take_expected();
        got.sort_by_key(|s| format!("{s:?}"));
        expected.sort_by_key(|s| format!("{s:?}"));
        assert_eq!(got, expected, "settlements differ from the oracle's");
        for call in self.calls.values() {
            assert!(
                !(call.kind == CallKind::Fetch
                    && call.contradicted
                    && !self.route_keeps(call)
                    && !call.cancel_sent),
                "a contradicted fetch was left running"
            );
        }
        issued
    }

    fn register(&mut self, call: CallId, kind: CallKind, o: ObjKey, snap: OSnap) {
        let old = self.calls.insert(
            call,
            SimCall {
                kind,
                obj: o,
                snap,
                cancel_sent: false,
                contradicted: false,
            },
        );
        assert!(old.is_none(), "call id {call} reused");
    }

    fn check(&mut self) {
        self.oracle.prune();
        assert_eq!(self.jobs.room(), self.oracle.room());
        assert_eq!(self.jobs.tracked(), self.oracle.tracked());
        for p in 0..3 {
            assert_eq!(
                self.jobs.part_outstanding(&part(p)),
                self.oracle.part_outstanding(&part(p)),
                "part {p}"
            );
        }
        assert_eq!(self.jobs.calls.len(), self.calls.len(), "calls in flight");
    }

    /// Answer everything with success until nothing moves, then check that all is settled.
    pub(in crate::sync::object_jobs::tests) fn drain(&mut self) {
        self.poisoned.clear();
        for _ in 0..200 {
            self.now += Duration::from_secs(10);
            let issued = self.flush();
            let pending: Vec<_> = self.calls.iter().map(|(call, c)| (*call, c.kind)).collect();
            if pending.is_empty() && issued == 0 && self.oracle.quiet() {
                break;
            }
            for (call, kind) in pending {
                self.finish(
                    call,
                    if kind == CallKind::Record {
                        How::Done
                    } else {
                        How::Ok
                    },
                );
            }
        }
        assert!(
            self.calls.is_empty(),
            "calls still in flight after the drain"
        );
        assert!(
            self.oracle.quiet(),
            "a want or an owed cursor was left behind"
        );
        // met routes stay tracked (and count against the cap) until they are switched off
        let config = Self::config();
        assert_eq!(
            self.jobs.room(),
            config
                .window
                .min(config.max_tracked - self.oracle.tracked())
        );
        assert!(
            self.jobs
                .objs
                .values()
                .all(|state| state.is_idle() || state.route.is_some())
        );
    }
}
