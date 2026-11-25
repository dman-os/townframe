//! The machine against its naive oracle: the same inputs, every command and every want's phase
//! compared after every step. The inputs come from a model table that behaves as the store
//! does (rows written by [`Want::record`], a landing deleting only the version it fetched,
//! the stream delivering the latest row of the keys that changed), so every row the machine
//! sees is one the real table could hold.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::*;

use super::*;
use crate::durable_outbox::oracle::Oracle;

#[derive(Debug, Clone)]
enum Op {
    Record {
        obj: u8,
        provider: u8,
        priority: u8,
        identity: u8,
    },
    Withdraw(u8),
    /// Deliver the first `n` changed keys (all of them past the end).
    Deliver(u8),
    Connect(u8),
    Disconnect(u8),
    /// A call in flight ends: `kind` picks the outcome the call's state allows.
    End {
        pick: usize,
        kind: u8,
    },
    /// A landing in flight answers.
    Land {
        pick: usize,
    },
    Advance(u8),
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0u8..3, 1u8..4, 0u8..3, 0u8..2).prop_map(|(obj, provider, priority, identity)| {
            Op::Record { obj, provider, priority, identity }
        }),
        1 => (0u8..3).prop_map(Op::Withdraw),
        4 => (0u8..4).prop_map(Op::Deliver),
        2 => (1u8..4).prop_map(Op::Connect),
        1 => (1u8..4).prop_map(Op::Disconnect),
        5 => (any::<usize>(), any::<u8>()).prop_map(|(pick, kind)| Op::End { pick, kind }),
        3 => any::<usize>().prop_map(|pick| Op::Land { pick }),
        2 => (0u8..20).prop_map(Op::Advance),
    ]
}

fn any_config() -> impl Strategy<Value = Config> {
    (
        1usize..4,
        0usize..3,
        prop_oneof![Just(60u64), Just(1_000_000)],
    )
        .prop_map(|(fetches, extra, aging)| Config {
            max_fetches: fetches,
            max_in_flight: fetches + extra,
            min_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(16),
            aging: Duration::from_secs(aging),
        })
}

struct Harness {
    machine: Outbox,
    oracle: Oracle,
    now: Instant,
    /// The model table: the live rows and the keys changed since they were last delivered.
    table: BTreeMap<u8, Want>,
    changed: BTreeSet<u8>,
    ms: u64,
    next_version: u64,
    /// Calls in flight: object, version and whether they were cancelled.
    calls: BTreeMap<CallId, (u8, Version, bool)>,
    lands: Vec<(u8, Version)>,
}

impl Harness {
    fn new(config: Config) -> Self {
        Self {
            machine: Outbox::new(config.clone()),
            oracle: Oracle::new(config),
            now: t(0),
            table: default(),
            changed: default(),
            ms: 0,
            next_version: 1,
            calls: default(),
            lands: default(),
        }
    }

    /// The store's landing: delete the row if it holds `version`.
    fn land_in_table(&mut self, n: u8, version: Version) -> bool {
        let deleted = self.table.get(&n).is_some_and(|row| row.version == version);
        if deleted {
            self.table.remove(&n);
            self.changed.insert(n);
        }
        deleted
    }

    fn apply(&mut self, op: Op) {
        match op {
            Op::Record {
                obj: n,
                provider,
                priority,
                identity: id,
            } => {
                self.ms += 13_000;
                let advert = Advert {
                    provider: peer(provider),
                    priority,
                    identity: identity(id),
                    version: self.next_version,
                };
                self.next_version += 1;
                if let Some((row, _)) = Want::record(self.table.get(&n), advert, self.ms) {
                    self.table.insert(n, row);
                    self.changed.insert(n);
                }
            }
            Op::Withdraw(n) => {
                if self.table.remove(&n).is_some() {
                    self.changed.insert(n);
                }
            }
            Op::Deliver(count) => {
                let keys: Vec<u8> = self.changed.iter().copied().take(count.into()).collect();
                let rows: Vec<_> = keys
                    .iter()
                    .map(|n| (*n, self.table.get(n).cloned()))
                    .collect();
                for key in keys {
                    self.changed.remove(&key);
                }
                self.machine
                    .rows(rows.iter().map(|(n, row)| (obj(*n), row.clone())));
                self.oracle
                    .rows(rows.into_iter().map(|(n, row)| (obj(n), row)).collect());
            }
            Op::Connect(p) => {
                self.machine.peer_connected(peer(p));
                self.oracle.peer_connected(peer(p));
            }
            Op::Disconnect(p) => {
                self.machine.peer_disconnected(&peer(p));
                self.oracle.peer_disconnected(&peer(p));
            }
            Op::End { pick, kind } => self.end(pick, kind),
            Op::Land { pick } => {
                if self.lands.is_empty() {
                    return;
                }
                let (n, version) = self.lands.remove(pick % self.lands.len());
                let deleted = self.land_in_table(n, version);
                self.machine.landed(&obj(n), version, deleted);
                self.oracle.landed(&obj(n), version, deleted);
            }
            Op::Advance(secs) => self.now += Duration::from_secs(secs.into()),
        }
    }

    fn end(&mut self, pick: usize, kind: u8) {
        if self.calls.is_empty() {
            return;
        }
        let call = *self.calls.keys().nth(pick % self.calls.len()).unwrap();
        let (n, version, stopping) = self.calls.remove(&call).unwrap();
        // a cancelled call reports `Cancelled` or the result it raced to; a live one any result
        let ended = match (stopping, kind % 3) {
            (true, 0) => Ended::Cancelled,
            (true, 1) | (false, 2) => Ended::Failed,
            (true, _) | (false, 0) => DURABLE,
            (false, _) => Ended::Fetched(Fetched::Landed {
                deleted: self.land_in_table(n, version),
            }),
        };
        self.machine.fetch_ended(self.now, call, ended);
        self.oracle.fetch_ended(self.now, call, ended);
    }

    /// Flush both and compare everything they say.
    fn check(&mut self) -> Result<(), TestCaseError> {
        let (mut ours, mut theirs) = (vec![], vec![]);
        self.machine.flush(self.now, &mut ours);
        self.oracle.flush(self.now, &mut theirs);
        prop_assert_eq!(&ours, &theirs, "commands");
        for cmd in ours {
            match cmd {
                Command::Fetch { call, job } => {
                    let n = job.obj.as_bytes()[0];
                    prop_assert!(
                        !self.calls.values().any(|(held, ..)| *held == n),
                        "a second fetch of object {n} beside one that has not reported"
                    );
                    self.calls.insert(call, (n, job.version, false));
                }
                Command::Cancel { call } => self.calls.get_mut(&call).unwrap().2 = true,
                Command::Land { obj, version } => self.lands.push((obj.as_bytes()[0], version)),
            }
        }
        let phases = self.oracle.phases();
        prop_assert_eq!(self.machine.len(), phases.len());
        for (obj, phase) in phases {
            prop_assert_eq!(
                self.machine.phase_of(&obj),
                Some(phase),
                "phase of {:?}",
                obj
            );
        }
        prop_assert_eq!(self.machine.next_due(), self.oracle.next_due());
        prop_assert_eq!(self.machine.fetching(), self.calls.len());
        Ok(())
    }
}

proptest! {
    #[test]
    fn matches_the_oracle(
        config in any_config(),
        ops in prop::collection::vec(any_op(), 1..80),
    ) {
        let mut harness = Harness::new(config);
        for op in ops {
            harness.apply(op);
            harness.check()?;
        }
    }
}
