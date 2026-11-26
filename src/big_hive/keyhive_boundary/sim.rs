//! The simulation of the [Keyhive boundary](super): the machine is the code under test, and the
//! driver (keyhive, the transaction, Keyline) is a fake that answers each call in flight in any
//! order the seed picks. The laws are in the [parent module](super); the checks that state them
//! are the naive [`oracle::Oracle`], which watches every command and every answer.
//!
//! # What is modelled
//!
//! - **Decryptions**, which hold the shared lock while out, and refills that are refused and
//!   retried on a backoff deadline.
//! - **Encryptions** from three documents and the coordinator, which finish in any order, fail
//!   (refused by keyhive, or the store failing), and are then settled: the commit landed and the
//!   preparation is applied, the commit failed and it is dropped, or the commit landed and the
//!   answer was lost, so it is dropped though the rows are there.
//! - **Locked operations**: membership changes and ingests from a session, each step of which can
//!   be refused or hit a store failure. Each hydrates a few documents and reports events with
//!   their source session and a prekey level.
//! - **Peers** connecting and disconnecting, with a Keyline whose answer depends on a standing
//!   per session and class of event that ops flip, so that eligibility changes between a commit
//!   and the answer.
//! - **Eviction** requests, prekeys being consumed, and clock advances and jumps.
//!
//! # Checks
//!
//! The oracle checks every command as the machine issues it (every law, see its docs), and
//! every answer as the driver gives it. After a quiet drain (reliable calls, preparations
//! settled, time advanced past every debounce): the machine is idle, every committed event that
//! owed a session a notification has either been sent or was filtered by Keyline, the prekeys
//! are at the floor, and the machine's loaded documents are the oracle's. `SIM_TRACE=1` prints
//! each op and the commands it caused.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use super::*;

mod cases;
pub(super) mod oracle;

use oracle::Oracle;

pub(super) const DOCS: u64 = 3;
pub(super) const SLOTS: usize = 3;
/// Classes of event, which Keyline's standing is per.
pub(super) const CLASSES: u64 = 3;

enum Ask {
    Hydrate(DocId),
    Encrypt,
    Decrypt,
    Apply(u64),
    Lock,
    Begin,
    Run {
        op: Option<LockedOp>,
        refill: bool,
    },
    Commit,
    Rollback,
    Reopen,
    Eligible {
        session: SessionId,
        events: BTreeSet<EventId>,
    },
}

struct Flight {
    call: CallId,
    ask: Ask,
    cancelled: bool,
}

/// An encryption the machine answered, waiting for the sim's commit to be settled.
struct Prep {
    token: u64,
}

pub(super) struct Sim {
    pub m: KeyhiveBoundary,
    o: Oracle,
    flights: Vec<Flight>,
    prepared: Vec<Prep>,
    /// The session of each slot, when connected, and Keyline's standing per slot and class.
    slots: [Option<SessionId>; SLOTS],
    standing: [[bool; CLASSES as usize]; SLOTS],
    replies: BTreeMap<Ticket, Result<(), Refused>>,
    notified: BTreeMap<SessionId, Vec<EventId>>,
    next_session: SessionId,
    /// Events the next run reports instead of drawn ones, for scripted tests.
    /// Whether runs consume prekeys, as peers do. Scripted tests keep the level steady.
    pub consume: bool,
    forced: Option<Vec<(EventId, Option<SessionId>)>>,
    /// Every command the machine issued, as text.
    pub log: Vec<String>,
    next_ask: CallId,
    next_ticket: Ticket,
    next_event: EventId,
    now: Instant,
    start: Instant,
    rng: u64,
    trace: bool,
}

impl Sim {
    pub fn new(seed: u64, plant: Option<Plant>) -> Self {
        let now = Instant::now();
        let mut m = KeyhiveBoundary::new(Config::default());
        m.plant = plant;
        Self {
            m,
            o: Oracle::new(Config::default().prekey_floor),
            flights: vec![],
            prepared: vec![],
            slots: [None; SLOTS],
            standing: [[true; CLASSES as usize]; SLOTS],
            replies: BTreeMap::new(),
            notified: BTreeMap::new(),
            consume: false,
            forced: None,
            log: vec![],
            next_session: 0,
            next_ask: 0,
            next_ticket: 0,
            next_event: 0,
            now,
            start: now,
            rng: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            trace: std::env::var_os("SIM_TRACE").is_some(),
        }
    }

    pub fn draw(&mut self, below: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as usize % below.max(1)
    }

    pub fn fresh_event(&mut self) -> EventId {
        self.next_event += 1;
        self.next_event
    }

    fn slot_of(&self, session: SessionId) -> Option<usize> {
        self.slots.iter().position(|s| *s == Some(session))
    }

    // ── driving the machine ─────────────────────────────────────────────────────────────────

    /// Flush the machine and carry its commands out, checking each against the laws.
    fn pump(&mut self) {
        let mut out = vec![];
        self.m.flush(self.now, &mut out);
        for command in out {
            if self.trace {
                eprintln!("  {command:?}");
            }
            self.log.push(format!("{command:?}"));
            self.o.on_command(&command);
            self.act(command);
        }
    }

    fn fly(&mut self, call: CallId, ask: Ask) {
        self.flights.push(Flight {
            call,
            ask,
            cancelled: false,
        });
    }

    fn act(&mut self, command: Command) {
        match command {
            Command::Hydrate { call, doc } => self.fly(call, Ask::Hydrate(doc)),
            Command::Encrypt { call, .. } => self.fly(call, Ask::Encrypt),
            Command::Decrypt { call, .. } => self.fly(call, Ask::Decrypt),
            Command::ApplyEncrypt { call, token } => self.fly(call, Ask::Apply(token)),
            Command::AcquireLock { call } => self.fly(call, Ask::Lock),
            Command::Begin { call } => self.fly(call, Ask::Begin),
            Command::Run { call, op, refill } => self.fly(call, Ask::Run { op, refill }),
            Command::Commit { call } => self.fly(call, Ask::Commit),
            Command::Rollback { call } => self.fly(call, Ask::Rollback),
            Command::Reopen { call } => self.fly(call, Ask::Reopen),
            Command::Eligible {
                call,
                session,
                events,
            } => self.fly(call, Ask::Eligible { session, events }),
            Command::Cancel { call } => {
                self.flights
                    .iter_mut()
                    .find(|f| f.call == call)
                    .expect("a cancelled call is in flight")
                    .cancelled = true;
            }
            Command::Encrypted { outcome, .. } => {
                if let Outcome::Done(enc) = outcome {
                    self.prepared.push(Prep { token: enc.token });
                }
            }
            Command::Reply { ticket, result } => {
                assert!(
                    self.replies.insert(ticket, result).is_none(),
                    "a locked operation is answered once"
                );
            }
            Command::Notify { session, events } => {
                self.notified.entry(session).or_default().extend(events);
            }
            Command::DropEncrypt { .. }
            | Command::Decrypted { .. }
            | Command::Evict { .. }
            | Command::ReleaseLock
            | Command::Trap { .. } => {}
        }
    }

    // ── ops ─────────────────────────────────────────────────────────────────────────────────

    /// An encryption asked by document `doc` (the coordinator when `doc >= DOCS`).
    pub fn encrypt(&mut self, doc: u64) -> CallId {
        self.next_ask += 1;
        let asker = if doc < DOCS {
            Asker::Document(doc)
        } else {
            Asker::Coordinator
        };
        self.m
            .prepare_encrypt(asker, self.next_ask, doc % DOCS, vec![1, 2, 3]);
        self.pump();
        self.next_ask
    }

    /// A decryption asked by document `doc` (the coordinator when `doc >= DOCS`).
    pub fn decrypt(&mut self, doc: u64) -> CallId {
        self.next_ask += 1;
        let asker = if doc < DOCS {
            Asker::Document(doc)
        } else {
            Asker::Coordinator
        };
        self.m
            .decrypt(asker, self.next_ask, doc % DOCS, vec![4, 5, 6]);
        self.pump();
        self.next_ask
    }

    /// A locked operation, a membership change or an ingest from `slot`'s session.
    pub fn locked(&mut self, slot: Option<usize>) -> Ticket {
        self.next_ticket += 1;
        let payload = self.next_ticket.to_be_bytes().to_vec();
        let op = match slot.and_then(|slot| self.slots[slot]) {
            Some(from) => LockedOp::Ingest {
                from,
                events: payload,
            },
            None => LockedOp::Membership(payload),
        };
        self.m.locked(self.next_ticket, op);
        self.pump();
        self.next_ticket
    }

    pub fn connect(&mut self, slot: usize) -> Option<SessionId> {
        if self.slots[slot].is_some() {
            return None;
        }
        self.next_session += 1;
        let session = self.next_session;
        self.slots[slot] = Some(session);
        self.o.session_opened(session);
        self.m.session_opened(session);
        self.pump();
        Some(session)
    }

    pub fn disconnect(&mut self, slot: usize) {
        let Some(session) = self.slots[slot].take() else {
            return;
        };
        self.o.session_closed(session);
        self.m.session_closed(session);
        self.pump();
    }

    pub fn set_standing(&mut self, slot: usize, class: u64, on: bool) {
        self.standing[slot][(class % CLASSES) as usize] = on;
    }

    pub fn prekeys(&mut self, left: usize) {
        self.o.set_level(left);
        self.m.prekeys_remaining(left);
        self.pump();
    }

    pub fn evict(&mut self, doc: u64) {
        self.m.evict(doc % DOCS);
        self.pump();
    }

    pub fn advance(&mut self, by: Duration) {
        self.now += by;
        self.pump();
    }

    /// Let the next debounce expire.
    pub fn tick(&mut self) {
        if let Some(due) = self.m.next_due() {
            self.now = self.now.max(due);
            self.pump();
        }
    }

    /// Settle the prepared encryption at `pick`: `mode` 0 the commit landed and it is applied,
    /// 1 the commit failed, 2 the commit landed and the answer was lost.
    pub fn settle(&mut self, pick: usize, mode: usize) {
        if self.prepared.is_empty() {
            return;
        }
        let prep = self.prepared.remove(pick % self.prepared.len());
        if mode % 3 != 1 {
            self.o.landed(prep.token);
        }
        self.m.settle_encrypt(prep.token, mode.is_multiple_of(3));
        self.pump();
    }

    /// Finish the call at `pick`: `mode` 0 ok, 1 refused by keyhive (where a call can be), 2 the
    /// store failed. A call that was cancelled may answer `Cancelled` instead.
    pub fn complete(&mut self, pick: usize, mode: usize) {
        if self.flights.is_empty() {
            return;
        }
        let flight = self.flights.remove(pick % self.flights.len());
        let cancelled = flight.cancelled && self.draw(2) == 0;
        let call = flight.call;
        let fail = mode == 2;
        let refuse = mode == 1;
        macro_rules! ended {
            ($done:expr) => {
                if cancelled {
                    Ended::Cancelled
                } else if fail {
                    Ended::StoreFailed
                } else {
                    $done
                }
            };
        }
        match flight.ask {
            Ask::Hydrate(doc) => {
                let ended = ended!(Ended::Done(()));
                self.o.hydrated(doc, &ended);
                self.m.hydrated(call, ended);
            }
            Ask::Encrypt => {
                let events = if self.draw(4) == 0 {
                    vec![self.fresh_event()]
                } else {
                    vec![]
                };
                let ended = if refuse && !cancelled {
                    Ended::Refused
                } else {
                    ended!(Ended::Done(Prepared {
                        ciphertext: vec![9],
                        events: events.clone(),
                    }))
                };
                self.o.encrypted(call, &ended);
                self.m.encrypted(call, ended);
            }
            Ask::Decrypt => {
                let ended = if refuse && !cancelled {
                    Ended::Refused
                } else {
                    ended!(Ended::Done(vec![7]))
                };
                self.o.decrypted(call, &ended);
                self.m.decrypted(call, ended);
            }
            Ask::Apply(token) => {
                let ended = ended!(Ended::Done(()));
                self.o.applied(token, &ended);
                self.m.applied(call, ended);
            }
            Ask::Lock => self.m.lock_acquired(call),
            Ask::Begin => {
                let ended = ended!(Ended::Done(()));
                self.o.began(&ended);
                self.m.began(call, ended);
            }
            Ask::Run { op, refill } => {
                let ticket = match &op {
                    Some(LockedOp::Membership(bytes) | LockedOp::Ingest { events: bytes, .. }) => {
                        Some(u64::from_be_bytes(
                            bytes.clone().try_into().expect("ticket"),
                        ))
                    }
                    None => None,
                };
                let ended = if refuse && !cancelled {
                    Ended::Refused
                } else {
                    let report = self.report(&op, refill);
                    ended!(Ended::Done(report))
                };
                self.o.ran(ticket, refill, &ended);
                self.m.ran(call, ended);
            }
            Ask::Commit => {
                let ended = if refuse && !cancelled {
                    Ended::Refused
                } else {
                    ended!(Ended::Done(()))
                };
                self.o.committed(&ended);
                self.m.committed(call, ended);
            }
            Ask::Rollback => {
                let ended = ended!(Ended::Done(()));
                self.o.rolled_back(&ended);
                self.m.rolled_back(call, ended);
            }
            Ask::Reopen => {
                let ended = ended!(Ended::Done(()));
                self.o.reopened(&ended);
                self.m.reopened(call, ended);
            }
            Ask::Eligible { session, events } => {
                let granted: BTreeSet<EventId> = match self.slot_of(session) {
                    Some(slot) => events
                        .iter()
                        .copied()
                        .filter(|e| self.standing[slot][(e % CLASSES) as usize])
                        .collect(),
                    None => BTreeSet::new(),
                };
                let ended = ended!(Ended::Done(granted));
                self.o.eligible(session, &events, &ended);
                self.m.eligible(call, ended);
            }
        }
        self.pump();
    }

    /// What keyhive reports for an operation: events of random classes, from the session the
    /// ingest came from (and sometimes from nobody or a pending one from elsewhere).
    fn report(&mut self, op: &Option<LockedOp>, refill: bool) -> Report {
        let from = match op {
            Some(LockedOp::Ingest { from, .. }) => Some(*from),
            _ => None,
        };
        let count = if op.is_some() && self.forced.is_none() {
            self.draw(4)
        } else {
            0
        };
        let mut events = self.forced.take().unwrap_or_default();
        for _ in 0..count {
            let source = match self.draw(4) {
                0 => None,
                1 => self.slots.iter().flatten().copied().next(),
                _ => from,
            };
            events.push((self.fresh_event(), source));
        }
        let hydrated = (0..self.draw(3) as u64)
            .map(|d| (d + self.draw(2) as u64) % DOCS)
            .collect();
        let prekeys = if refill {
            self.m.config.prekey_floor + self.draw(3)
        } else {
            if self.consume {
                self.o.level().saturating_sub(self.draw(3))
            } else {
                self.o.level()
            }
        };
        Report {
            hydrated,
            events,
            prekeys,
        }
    }

    /// Drain: reliable calls, every preparation applied, time advanced past every debounce, until
    /// nothing is left; then check the quiet state.
    pub fn quiesce(&mut self) {
        for _ in 0..10_000 {
            if !self.flights.is_empty() {
                self.complete(0, 0);
            } else if !self.prepared.is_empty() {
                self.settle(0, 0);
            } else if self.m.next_due().is_some() {
                self.tick();
            } else {
                self.check_quiet();
                return;
            }
        }
        panic!("the drain does not settle");
    }

    pub fn snapshot(&self) -> String {
        format!(
            "{} flights={} prepared={} now={:?}",
            self.m.describe(),
            self.flights.len(),
            self.prepared.len(),
            self.now - self.start
        )
    }

    fn check_quiet(&self) {
        if self.m.is_trapped() {
            return;
        }
        assert!(
            self.m.is_idle(),
            "the machine is idle after a drain: {}",
            self.m.describe()
        );
        let loaded: BTreeSet<DocId> = self
            .m
            .docs
            .iter()
            .filter(|(_, load)| **load == Load::Loaded)
            .map(|(doc, _)| *doc)
            .collect();
        self.o.check_quiet(&loaded, self.next_ticket, &self.replies);
    }
}

/// Scripted control, for the law tests.
impl Sim {
    /// The kinds of the calls in flight, oldest first.
    pub fn kinds(&self) -> Vec<&'static str> {
        self.flights
            .iter()
            .map(|flight| match flight.ask {
                Ask::Hydrate(_) => "hydrate",
                Ask::Encrypt => "encrypt",
                Ask::Decrypt => "decrypt",
                Ask::Apply(_) => "apply",
                Ask::Lock => "lock",
                Ask::Begin => "begin",
                Ask::Run { .. } => "run",
                Ask::Commit => "commit",
                Ask::Rollback => "rollback",
                Ask::Reopen => "reopen",
                Ask::Eligible { .. } => "eligible",
            })
            .collect()
    }

    /// Finish the oldest call of `kind` in `mode`. Answers whether there was one.
    pub fn complete_kind(&mut self, kind: &str, mode: usize) -> bool {
        let Some(at) = self.kinds().iter().position(|k| *k == kind) else {
            return false;
        };
        self.complete(at, mode);
        true
    }

    /// Finish every call of `kind` until none is in flight.
    pub fn complete_all(&mut self, kind: &str) {
        while self.complete_kind(kind, 0) {}
    }

    pub fn reply(&self, ticket: Ticket) -> Option<&Result<(), Refused>> {
        self.replies.get(&ticket)
    }

    pub fn notified(&self, session: SessionId) -> Vec<EventId> {
        self.notified.get(&session).cloned().unwrap_or_default()
    }

    pub fn now(&self) -> Instant {
        self.now
    }

    pub fn prepared_count(&self) -> usize {
        self.prepared.len()
    }
}

impl Sim {
    /// The next run reports exactly these events.
    pub fn report_next(&mut self, events: Vec<(EventId, Option<SessionId>)>) {
        self.forced = Some(events);
    }
}
