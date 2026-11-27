//! The naive outbox the machine is checked against: no indexes, no scheduler, every decision
//! recomputed by scanning every want. It is written from the laws, not from the machine.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::interlude::*;
use crate::kit::Instant;

use super::CallId;
use super::Command;
use super::Config;
use super::Ended;
use super::Fetched;
use super::Job;
use super::Phase;
use super::Version;
use super::Want;

#[derive(Debug)]
enum St {
    Idle,
    Fetching(CallId),
    Landing(Version),
    Stale,
}

#[derive(Debug)]
struct OWant {
    want: Want,
    st: St,
    tried: BTreeSet<PeerKey>,
    rounds: u32,
    until: Option<Instant>,
}

/// A call the oracle has issued and not seen end.
#[derive(Debug)]
struct OCall {
    job: Job,
    /// Cancelled: it holds its slot until it reports.
    stopping: bool,
}

#[derive(Debug)]
pub(super) struct Oracle {
    config: Config,
    wants: BTreeMap<ObjKey, OWant>,
    connected: BTreeSet<PeerKey>,
    calls: BTreeMap<CallId, OCall>,
    next_call: CallId,
    landing: BTreeSet<(ObjKey, Version)>,
    lands: Vec<Command>,
    cancels: Vec<CallId>,
}

impl Oracle {
    pub(super) fn new(config: Config) -> Self {
        Self {
            config,
            wants: default(),
            connected: default(),
            calls: default(),
            next_call: 0,
            landing: default(),
            lands: default(),
            cancels: default(),
        }
    }

    pub(super) fn rows(&mut self, rows: Vec<(ObjKey, Option<Want>)>) {
        for (obj, row) in rows {
            match row {
                None => {
                    if self.wants.contains_key(&obj) {
                        self.cancel(&obj);
                        self.wants.remove(&obj);
                    }
                }
                Some(want) => match self.wants.get(&obj).map(|held| held.want.version) {
                    None => {
                        self.wants.insert(
                            obj,
                            OWant {
                                want,
                                st: St::Idle,
                                tried: default(),
                                rounds: 0,
                                until: None,
                            },
                        );
                    }
                    Some(version) => {
                        let superseded = version != want.version;
                        self.wants.get_mut(&obj).unwrap().want = want;
                        if superseded {
                            self.supersede(&obj);
                        }
                    }
                },
            }
        }
    }

    pub(super) fn peer_connected(&mut self, peer: PeerKey) {
        self.connected.insert(peer);
    }

    pub(super) fn peer_disconnected(&mut self, peer: &PeerKey) {
        self.connected.remove(peer);
        let against: Vec<ObjKey> = self
            .wants
            .iter()
            .filter_map(|(obj, held)| match held.st {
                St::Fetching(call) if self.calls[&call].job.provider == *peer => Some(obj.clone()),
                _ => None,
            })
            .collect();
        for obj in against {
            self.cancel(&obj);
        }
    }

    pub(super) fn fetch_ended(&mut self, now: Instant, call: CallId, ended: Ended) {
        let ocall = self.calls.remove(&call).expect("a call in flight");
        if ocall.stopping {
            return;
        }
        let obj = ocall.job.obj.clone();
        self.wants.get_mut(&obj).unwrap().st = St::Idle;
        match ended {
            Ended::Fetched(fetched) => {
                let version = ocall.job.version;
                self.wants.get_mut(&obj).unwrap().st = St::Landing(version);
                self.landing.insert((obj.clone(), version));
                match fetched {
                    Fetched::Durable => self.lands.push(Command::Land { obj, version }),
                    Fetched::Landed { deleted } => self.landed(&obj, version, deleted),
                }
            }
            Ended::Failed => self.failed(now, &obj, ocall.job.provider),
            Ended::Cancelled => unreachable!("a call that was not cancelled"),
        }
    }

    pub(super) fn landed(&mut self, obj: &ObjKey, version: Version, deleted: bool) {
        assert!(self.landing.remove(&(obj.clone(), version)));
        let Some(held) = self.wants.get_mut(obj) else {
            return;
        };
        if !matches!(held.st, St::Landing(landing) if landing == version) {
            return;
        }
        if held.want.version != version {
            held.st = St::Idle;
        } else if deleted {
            self.wants.remove(obj);
        } else {
            held.st = St::Stale;
        }
    }

    pub(super) fn flush(&mut self, now: Instant, out: &mut Vec<Command>) {
        for held in self.wants.values_mut() {
            if held.until.is_some_and(|until| until <= now) {
                held.until = None;
            }
        }
        let mut fetches = vec![];
        while self.calls.len() < self.config.max_fetches
            && self.calls.len() + self.landing.len() < self.config.max_in_flight
        {
            let Some(obj) = self.best() else {
                break;
            };
            let call = self.next_call;
            self.next_call += 1;
            let held = self.wants.get_mut(&obj).unwrap();
            let provider = Self::pick(&self.connected, held).unwrap().clone();
            let job = Job {
                obj: obj.clone(),
                version: held.want.version,
                identity: held.want.identity.clone(),
                provider,
            };
            held.st = St::Fetching(call);
            self.calls.insert(
                call,
                OCall {
                    job: job.clone(),
                    stopping: false,
                },
            );
            fetches.push(Command::Fetch { call, job });
        }
        out.append(&mut self.lands);
        out.extend(
            std::mem::take(&mut self.cancels)
                .into_iter()
                .map(|call| Command::Cancel { call }),
        );
        out.append(&mut fetches);
    }

    pub(super) fn next_due(&self) -> Option<Instant> {
        self.wants.values().filter_map(|held| held.until).min()
    }

    pub(super) fn phases(&self) -> BTreeMap<ObjKey, Phase> {
        self.wants
            .iter()
            .map(|(obj, held)| {
                let busy = self.calls.values().any(|call| call.job.obj == *obj);
                let phase = match held.st {
                    St::Fetching(call) => Phase::Fetching(call),
                    St::Landing(_) => Phase::Landing,
                    St::Stale => Phase::Stale,
                    St::Idle if busy => Phase::Draining,
                    St::Idle if held.until.is_some() => Phase::Backoff,
                    St::Idle if Self::pick(&self.connected, held).is_none() => Phase::Parked,
                    St::Idle => Phase::Ready,
                };
                (obj.clone(), phase)
            })
            .collect()
    }

    /// The want to start next: the smallest order among the wants that may start.
    fn best(&self) -> Option<ObjKey> {
        let aging = i128::try_from(self.config.aging.as_millis()).unwrap();
        self.wants
            .iter()
            .filter(|(obj, held)| {
                matches!(held.st, St::Idle)
                    && held.until.is_none()
                    && !self.calls.values().any(|call| call.job.obj == **obj)
                    && Self::pick(&self.connected, held).is_some()
            })
            .min_by_key(|(obj, held)| {
                let age = i128::from(held.want.queued_at);
                (age - i128::from(held.want.priority) * aging, (*obj).clone())
            })
            .map(|(obj, _)| obj.clone())
    }

    fn pick<'w>(connected: &BTreeSet<PeerKey>, held: &'w OWant) -> Option<&'w PeerKey> {
        held.want
            .providers
            .iter()
            .find(|peer| connected.contains(*peer) && !held.tried.contains(*peer))
    }

    /// Cancel the want's fetch and leave it idle; its call keeps its slot until it reports.
    fn cancel(&mut self, obj: &ObjKey) {
        let held = self.wants.get_mut(obj).unwrap();
        if let St::Fetching(call) = held.st {
            held.st = St::Idle;
            self.calls.get_mut(&call).unwrap().stopping = true;
            self.cancels.push(call);
        }
    }

    fn supersede(&mut self, obj: &ObjKey) {
        let held = self.wants.get_mut(obj).unwrap();
        held.tried.clear();
        held.rounds = 0;
        held.until = None;
        match held.st {
            St::Fetching(_) => self.cancel(obj),
            St::Stale => held.st = St::Idle,
            St::Idle | St::Landing(_) => {}
        }
    }

    fn failed(&mut self, now: Instant, obj: &ObjKey, provider: PeerKey) {
        let held = self.wants.get_mut(obj).unwrap();
        held.tried.insert(provider);
        if Self::pick(&self.connected, held).is_some() {
            return;
        }
        held.rounds += 1;
        held.tried.clear();
        let backoff = self
            .config
            .min_backoff
            .saturating_mul(2u32.saturating_pow(held.rounds - 1))
            .min(self.config.max_backoff);
        held.until = Some(now + backoff);
    }
}
