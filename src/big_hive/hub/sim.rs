//! The simulation of the Hub together with the real Document machines, the real Write
//! coordinator, and the `Interest` and `Sessions` machines the Hub holds, in one deterministic
//! test. A fake world (the oracle of the Document simulation: committed changes with their
//! revisions, key arrivals, readability and frontier rows), a fake driver and a fake keyhive
//! boundary answer everything else.
//!
//! # What is modelled
//!
//! - **The driver.** Hub commands are carried out at once: a start builds the Document from the
//!   world as it is then; a notice goes to the document's mailbox, from which an op delivers it
//!   (FIFO per document, any order between documents). Every command of a Document that has a
//!   call becomes a flight that an op completes, in any order, or fails. A Document's reports to
//!   the Hub are mail too: an op delivers them, so the Hub acts on statuses that are out of date,
//!   which is what `Status::consumed` is for.
//! - **The world.** Plans through the coordinator and remote writes commit changes at revisions
//!   of one sequence; keys arrive later or never; documents turn unreadable and readable. The
//!   change feed is the log of tree bumps, and an op delivers its next row (as its latest
//!   revision, with the frontier and readability read at delivery). Keyhive epoch moves are mail
//!   delivered in any order.
//! - **Sessions.** A fake Keyline says who stands where (peers and our principals, as in the
//!   `big_automata::sessions` simulation); dials succeed or fail; sessions are lost; remotes
//!   refuse documents.
//! - **Keyhive rounds.** Wants (with and without a waiter), rounds that complete with an
//!   admission sequence number, and admission heads that advance.
//! - **Crashes.** `Restart` drops the Hub, every machine, every flight and all mail, keeps the
//!   world and the Hub's saved cursor, and rebuilds the Hub as its driver would: the feed rows
//!   after the cursor and, by a query, every document that is stale and readable.
//! - **Time** is a virtual clock; the Hub and every machine are flushed on it.
//!
//! # Checks
//!
//! After every step: one machine per document, a stop only of an evictable machine nobody wants
//! (laws 1 and 2); a start only for demand or catch-up (law 5); every stale readable document
//! the Hub was told about is running, queued or parked, and parked means keys are missing (law
//! 3); an epoch move wakes a parked document (law 4); catch-up is within its limit (law 6); the
//! cursor is monotone and has not passed a document's unsettled row (law 7); mailboxes are within
//! the window (law 8); a waiter is resolved once, and never ahead of the admitted head (law 9);
//! the Hub is drained exactly when nothing is in flight (law 10); every route is on an open
//! session whose peer and principal stand (law 11); actors are fresh (law 12).
//!
//! After a quiet drain: every readable document whose content is keyed has its frontier at the
//! tree, nothing is queued or running, no machine runs, every waiter is resolved. Every case
//! then restarts and drains again.
//!
//! `SIM_TRACE=1` prints each op.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

use automerge::ActorId;
use automerge::Change;
use automerge::ROOT;
use automerge::transaction::Transactable;
use big_automata::kit::Instant;
use big_automata::rev::Rev;
use big_automata::sessions::interest::Tier;
use big_automata::sessions::session;

use super::*;
use crate::coordinator;
use crate::coordinator::Coordinator;
use crate::document;
use crate::document::Document;
use crate::document::EncryptedChange;
use crate::document::Landed;
use crate::document::Outcome;
use crate::document::Work;
use crate::document::tests::oracle::World;

mod cases;
mod checks;
mod ops;

pub(super) use ops::Op;

type Out = u64;
type Session = (u8, u8);

pub(super) const DOCS: usize = 3;
pub(super) const PEERS: u8 = 2;
pub(super) const PRINCIPALS: u8 = 3;

pub(super) fn config() -> Config {
    Config {
        catch_up: 2,
        window: 2,
        interest_grace: Duration::from_secs(30),
        sessions: session::Config {
            grace: Duration::from_secs(20),
            per_peer: 2,
            total: 3,
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(8),
        },
    }
}

struct Machine {
    doc: Document<Out>,
    generation: u64,
    consumed: u64,
    handles: Vec<document::Handle>,
    interest: u32,
}

/// What a document's mailbox holds, in the order the Hub sent it.
enum Mail {
    Msg(u64, ToDoc),
    Stop(u64),
}

enum Ask {
    Hydrate,
    Decrypt,
    Publish {
        heads: Vec<automerge::ChangeHash>,
        rev: Rev,
    },
    Encrypt {
        change: Vec<u8>,
    },
    Commit {
        rows: Vec<coordinator::DocRows>,
    },
}

struct Flight {
    doc: usize,
    call: document::CallId,
    ask: Ask,
    cancelled: bool,
}

pub(super) struct Sim {
    pub(super) world: World,
    pub(super) hub: Hub<u8, u8>,
    coordinator: Coordinator<Out>,
    machines: BTreeMap<usize, Machine>,
    /// Flights of documents' calls, and of the coordinator's (named by call).
    flights: Vec<Flight>,
    /// The coordinator's calls, which have no machine of their own.
    mailbox: BTreeMap<usize, VecDeque<Mail>>,
    /// Documents with a stop request out, whose answer the Hub has not heard.
    stopping: BTreeSet<usize>,
    /// The coordinator's commands are being carried out.
    pumping: bool,
    /// Plans waiting for every one of their documents to have a machine that is not stopping.
    deferred: Vec<document::Ticket>,
    reports: VecDeque<(usize, u64, Report)>,
    feed_log: VecDeque<(Rev, usize)>,
    epoch_mail: Vec<usize>,
    /// Demand and interest the caller holds, which the Hub must honour.
    plans: BTreeMap<document::Ticket, Vec<usize>>,
    /// Documents a handle was demanded for and has not arrived.
    opening: BTreeMap<usize, usize>,
    next_ticket: document::Ticket,
    next_key: u64,
    next_token: u64,
    /// What the Hub has been told, for the oracle.
    delivered_tree: BTreeMap<usize, Rev>,
    told_readable: BTreeMap<usize, bool>,
    saved_cursor: Rev,
    /// The cursor the Hub restarted from: rows at or below it that are stale are replayed by the
    /// startup query, and stay unsettled without passing the cursor.
    restart_cursor: Rev,
    last_generation: BTreeMap<usize, u64>,
    actors: BTreeSet<Vec<u8>>,
    // the network and Keyline
    peer_standing: BTreeSet<(u8, usize)>,
    local_standing: BTreeSet<(u8, usize)>,
    connected: BTreeSet<u8>,
    leases: BTreeMap<usize, usize>,
    dial_flights: BTreeMap<CallId, Session>,
    net_open: BTreeSet<Session>,
    routes: BTreeSet<(usize, Session)>,
    refused: BTreeSet<(usize, Session)>,
    told_standing: BTreeMap<usize, BTreeMap<Session, Tier>>,
    // keyhive rounds
    round_flights: BTreeMap<CallId, (u8, bool)>,
    next_waiter: Waiter,
    waiters: BTreeMap<Waiter, u8>,
    resolved: BTreeSet<Waiter>,
    completed_seq: BTreeMap<u8, u64>,
    admission_head: u64,
    next_seq: u64,
    draining: bool,
    drained_seen: bool,
    now: Instant,
    boots: u8,
    plant: Option<Plant>,
    trace: bool,
}

impl Sim {
    pub(super) fn new(plant: Option<Plant>) -> Self {
        let now = Instant::now();
        let mut sim = Self {
            world: World::new(DOCS),
            hub: Hub::new(config(), boot(0, 0)),
            coordinator: Coordinator::new(coordinator::Config::default()),
            machines: BTreeMap::new(),
            flights: vec![],
            mailbox: BTreeMap::new(),
            stopping: BTreeSet::new(),
            pumping: false,
            deferred: vec![],
            reports: VecDeque::new(),
            feed_log: VecDeque::new(),
            epoch_mail: vec![],
            plans: BTreeMap::new(),
            opening: BTreeMap::new(),
            next_ticket: 0,
            next_key: 0,
            next_token: 0,
            delivered_tree: BTreeMap::new(),
            told_readable: BTreeMap::new(),
            saved_cursor: 0,
            restart_cursor: 0,
            last_generation: BTreeMap::new(),
            actors: BTreeSet::new(),
            peer_standing: BTreeSet::new(),
            local_standing: BTreeSet::new(),
            connected: BTreeSet::new(),
            leases: BTreeMap::new(),
            dial_flights: BTreeMap::new(),
            net_open: BTreeSet::new(),
            routes: BTreeSet::new(),
            refused: BTreeSet::new(),
            told_standing: BTreeMap::new(),
            round_flights: BTreeMap::new(),
            next_waiter: 0,
            waiters: BTreeMap::new(),
            resolved: BTreeSet::new(),
            completed_seq: BTreeMap::new(),
            admission_head: 0,
            next_seq: 0,
            draining: false,
            drained_seen: false,
            now,
            boots: 0,
            plant,
            trace: std::env::var_os("SIM_TRACE").is_some(),
        };
        if let Some(plant) = plant {
            sim.hub.plant(plant);
        }
        sim
    }

    // ── the driver: Hub commands ────────────────────────────────────────────────────────────

    /// Flush the Hub and carry out what it asks, until it asks for nothing more.
    pub(super) fn pump(&mut self) {
        loop {
            let mut commands = vec![];
            self.hub.flush(self.now, &mut commands);
            if commands.is_empty() {
                return;
            }
            for command in commands {
                if self.trace {
                    eprintln!("  hub: {command:?}");
                }
                self.execute(command);
            }
        }
    }

    fn execute(&mut self, command: Command<u8, u8>) {
        match command {
            Command::StartDocument {
                doc,
                generation,
                actor,
            } => self.start(doc as usize, generation, actor),
            Command::StopDocument { doc, generation } => self.stop(doc as usize, generation),
            Command::ToDocument {
                doc,
                generation,
                input,
            } => {
                assert!(
                    self.machines.contains_key(&(doc as usize)),
                    "a message for no machine"
                );
                self.mailbox
                    .entry(doc as usize)
                    .or_default()
                    .push_back(Mail::Msg(generation, input));
            }
            Command::SaveCursor { rev } => {
                assert!(rev > self.saved_cursor, "the cursor only moves forward");
                self.saved_cursor = rev;
            }
            Command::Dial {
                call,
                peer,
                principal,
            } => {
                assert!(
                    self.connected.contains(&peer),
                    "dial to a peer that is not connected"
                );
                assert!(
                    !self.dial_flights.values().any(|s| *s == (peer, principal)),
                    "two dials for one session"
                );
                self.dial_flights.insert(call, (peer, principal));
            }
            Command::Close { peer, principal } => {
                assert!(
                    self.net_open.remove(&(peer, principal)),
                    "close of a session not open"
                );
                self.routes.retain(|(_, s)| *s != (peer, principal));
                self.refused.retain(|(_, s)| *s != (peer, principal));
            }
            Command::AddRoute {
                doc,
                peer,
                principal,
            } => {
                let session = (peer, principal);
                assert!(
                    self.net_open.contains(&session),
                    "a route on a session that is not open"
                );
                assert!(
                    self.eligible(doc as usize).contains_key(&session),
                    "a route without standing"
                );
                self.routes.insert((doc as usize, session));
            }
            Command::RemoveRoute {
                doc,
                peer,
                principal,
            } => {
                self.routes.remove(&(doc as usize, (peer, principal)));
            }
            Command::StartRound { call, peer } => {
                assert!(
                    !self
                        .round_flights
                        .values()
                        .any(|(p, live)| *p == peer && *live),
                    "two rounds for one peer"
                );
                self.round_flights.insert(call, (peer, true));
            }
            Command::CancelRound { call } => {
                let flight = self
                    .round_flights
                    .get_mut(&call)
                    .expect("a round in flight");
                flight.1 = false;
            }
            Command::RoundResolved { waiter, ok } => {
                assert!(self.waiters.contains_key(&waiter), "an unknown waiter");
                assert!(
                    self.resolved.insert(waiter),
                    "waiter {waiter} resolved twice"
                );
                if ok {
                    let peer = self.waiters[&waiter];
                    let seq = self.completed_seq.get(&peer).copied().unwrap_or(0);
                    assert!(
                        seq <= self.admission_head,
                        "waiter {waiter} resolved before the admitted head reached {seq}"
                    );
                }
            }
            Command::Drained => {
                assert!(!self.drained_seen, "drained twice");
                self.drained_seen = true;
            }
        }
    }

    fn start(&mut self, doc: usize, generation: u64, actor: ActorId) {
        assert!(!self.machines.contains_key(&doc), "doc {doc}: two machines");
        let last = self.last_generation.insert(doc, generation).unwrap_or(0);
        assert!(generation > last, "doc {doc}: a generation was reused");
        assert!(
            self.actors.insert(actor.to_bytes().to_vec()),
            "an actor was reused"
        );
        assert!(
            self.wants(doc) || self.hub.doc(doc as u64).catch == Some(Catch::Running),
            "doc {doc}: started for nothing (law 5)"
        );
        let model = &self.world.docs[doc];
        let machine = Document::new(
            document::Config::default(),
            document::Start {
                generation,
                actor,
                readable: model.readable,
                known_rev: model.tree_rev,
                frontier_rev: self.world.frontier_rev(doc),
            },
        );
        self.machines.insert(
            doc,
            Machine {
                doc: machine,
                generation,
                consumed: 0,
                handles: vec![],
                interest: 0,
            },
        );
        self.flush_doc(doc);
        self.serve_opening(doc);
    }

    /// Hand out the handles waiting for the document, if a machine runs that is not stopping.
    pub(super) fn serve_opening(&mut self, doc: usize) {
        if self.hub.doc(doc as u64).serving.is_none() {
            return;
        }
        let Some(machine) = self.machines.get_mut(&doc) else {
            return;
        };
        for _ in 0..self.opening.remove(&doc).unwrap_or(0) {
            let handle = machine.doc.opened();
            machine.handles.push(handle);
        }
        self.flush_doc(doc);
    }

    /// The Hub asks for a stop: the request goes to the machine's mailbox, behind everything the
    /// Hub sent before it. Nothing wants the document (law 2).
    fn stop(&mut self, doc: usize, generation: u64) {
        let machine = self.machines.get(&doc).expect("doc: a stop for no machine");
        assert_eq!(machine.generation, generation);
        assert!(
            !self.wants(doc),
            "doc {doc}: a stop asked for though the document is wanted (law 2)"
        );
        assert!(self.stopping.insert(doc), "doc {doc}: two stop requests");
        self.mailbox
            .entry(doc)
            .or_default()
            .push_back(Mail::Stop(generation));
    }

    /// The driver's loop reaches the stop request: it grants it only to a machine that is
    /// evictable now and has nothing in flight.
    pub(super) fn stop_requested(&mut self, doc: usize, generation: u64) {
        let machine = self
            .machines
            .get(&doc)
            .expect("a stop for a machine that runs");
        let calls = self.flights.iter().any(|f| f.doc == doc);
        let grant = machine.doc.evictable() && !calls;
        // law 13: a plan that was submitted owns its documents until its reply
        assert!(
            !grant || !self.owned_by_plan(doc),
            "doc {doc}: a stop granted to a document a coordinator plan holds or is owed (law 13)"
        );
        if grant {
            self.machines.remove(&doc);
            self.reports.push_back((doc, generation, Report::Stopped));
        } else {
            self.reports
                .push_back((doc, generation, Report::StopRefused));
            self.report_status(doc);
        }
    }

    /// Whether a plan that was submitted to the coordinator, and has not replied, names the document.
    pub(super) fn owned_by_plan(&self, doc: usize) -> bool {
        self.plans
            .iter()
            .any(|(ticket, docs)| docs.contains(&doc) && !self.deferred.contains(ticket))
    }

    /// What keeps a document's machine: demand the caller holds.
    pub(super) fn wants(&self, doc: usize) -> bool {
        self.machines
            .get(&doc)
            .is_some_and(|m| !m.handles.is_empty() || m.interest > 0)
            || self.plans.values().any(|docs| docs.contains(&doc))
            || self.leases.get(&doc).is_some_and(|n| *n > 0)
            || self.opening.get(&doc).is_some_and(|n| *n > 0)
    }

    // ── the driver: Document machines ───────────────────────────────────────────────────────

    pub(super) fn flush_doc(&mut self, doc: usize) {
        let Some(machine) = self.machines.get_mut(&doc) else {
            return;
        };
        let mut out = vec![];
        machine.doc.flush(self.now, &mut out);
        let generation = machine.generation;
        for command in out {
            if self.trace {
                eprintln!("  doc {doc}: {command:?}");
            }
            self.dispatch(doc, generation, command);
        }
        self.report_status(doc);
    }

    fn report_status(&mut self, doc: usize) {
        let Some(machine) = self.machines.get(&doc) else {
            return;
        };
        let status = Status {
            consumed: machine.consumed,
            catching_up: machine.doc.catching_up(),
            parked: !machine.doc.missing().is_empty(),
            evictable: machine.doc.evictable(),
        };
        self.reports
            .push_back((doc, machine.generation, Report::Status(status)));
    }

    fn dispatch(&mut self, doc: usize, generation: u64, command: document::Command<Out>) {
        use document::Command as C;
        match command {
            C::Hydrate { call } => self.fly(doc, call, Ask::Hydrate),
            C::Decrypt { call, .. } => self.fly(doc, call, Ask::Decrypt),
            C::PublishFrontier { call, heads, rev } => {
                self.fly(doc, call, Ask::Publish { heads, rev });
            }
            C::NewActor { call } => {
                self.reports
                    .push_back((doc, generation, Report::NeedsActor { call }));
            }
            C::Evictable => self.reports.push_back((doc, generation, Report::Evictable)),
            C::Prepared { plan, result } => {
                self.coordinator.prepared(doc as u64, plan, result);
                self.pump_coordinator();
            }
            C::Cancel { call } => {
                self.flights
                    .iter_mut()
                    .find(|f| f.doc == doc && f.call == call)
                    .expect("a cancelled call is in flight")
                    .cancelled = true;
            }
            C::Notify { .. } | C::SettleEncrypt { .. } => {}
            C::PrepareEncrypt { .. } | C::Commit { .. } | C::Reply { .. } | C::Ingested { .. } => {
                unreachable!("the simulation writes only through plans")
            }
        }
    }

    fn fly(&mut self, doc: usize, call: document::CallId, ask: Ask) {
        self.flights.push(Flight {
            doc,
            call,
            ask,
            cancelled: false,
        });
    }

    // ── the driver: the coordinator ─────────────────────────────────────────────────────────

    /// Carry out the coordinator's commands. A Document's answer to one of them can make the
    /// coordinator say more; that waits until the batch in hand is done, because the commands to
    /// one Document reach it in the order the coordinator issued them (an abort of a plan must
    /// not overtake that plan's later prepare).
    pub(super) fn pump_coordinator(&mut self) {
        if std::mem::replace(&mut self.pumping, true) {
            return;
        }
        loop {
            let mut commands = vec![];
            self.coordinator.flush(&mut commands);
            if commands.is_empty() {
                break;
            }
            for command in commands {
                self.coordinate(command);
            }
        }
        self.pumping = false;
    }

    fn coordinate(&mut self, command: coordinator::Command<Out>) {
        use coordinator::Command as C;
        match command {
            C::Prepare { doc, plan, work } => {
                let doc = doc as usize;
                assert!(
                    self.machines.contains_key(&doc) && !self.stopping.contains(&doc),
                    "doc {doc}: a prepare sent to a machine that is gone or being stopped (law 13)"
                );
                self.machines
                    .get_mut(&doc)
                    .expect("a plan's document is running")
                    .doc
                    .prepare_work(plan, work);
                self.flush_doc(doc);
            }
            C::PrepareEncrypt {
                call, doc, change, ..
            } => self.fly(doc as usize, call, Ask::Encrypt { change }),
            C::Commit { call, rows, .. } => {
                let doc = rows.first().map_or(0, |row| row.doc as usize);
                self.fly(doc, call, Ask::Commit { rows });
            }
            C::SettleEncrypt { .. } => {}
            C::Cancel { call } => {
                if let Some(flight) = self.flights.iter_mut().find(|f| f.call == call) {
                    flight.cancelled = true;
                }
            }
            C::Apply { doc, plan, landed } => {
                let doc = doc as usize;
                self.machines
                    .get_mut(&doc)
                    .expect("a plan's document is running")
                    .doc
                    .apply(plan, landed);
                self.flush_doc(doc);
            }
            C::Abort { doc, plan } => {
                let doc = doc as usize;
                self.machines
                    .get_mut(&doc)
                    .expect("a plan's document is running")
                    .doc
                    .abort(plan);
                self.flush_doc(doc);
            }
            C::Reply { ticket, .. } => {
                for doc in self.plans.remove(&ticket).expect("a plan's ticket") {
                    self.hub.undemand(doc as u64);
                }
            }
        }
    }

    // ── what the world answers ──────────────────────────────────────────────────────────────

    /// The sessions that may serve `doc`: the fake Keyline.
    pub(super) fn eligible(&self, doc: usize) -> BTreeMap<Session, Tier> {
        let mut out = BTreeMap::new();
        for peer in (0..PEERS).filter(|p| self.peer_standing.contains(&(*p, doc))) {
            for local in (0..PRINCIPALS).filter(|l| self.local_standing.contains(&(*l, doc))) {
                let tier = if local == 0 {
                    Tier::Default
                } else {
                    Tier::OnDemand
                };
                out.insert((peer, local), tier);
            }
        }
        out
    }

    pub(super) fn bump_feed(&mut self, doc: usize) {
        self.feed_log.push_back((self.world.tree_rev(doc), doc));
    }

    pub(super) fn plan_work(&mut self) -> Work<Out> {
        self.next_key += 1;
        let key = format!("l{}", self.next_key);
        Work::Op(Box::new(move |tx| {
            tx.put(ROOT, key, 0_i64)?;
            Ok(0)
        }))
    }

    fn answer(&mut self, flight: Flight, fail: bool) {
        let Flight {
            doc,
            call,
            ask,
            cancelled,
        } = flight;
        let refused = fail || cancelled;
        let readable = self.world.docs[doc].readable;
        match ask {
            Ask::Hydrate => {
                let outcome = if refused || !readable {
                    refusal(cancelled)
                } else {
                    Outcome::Done(self.world.materialize(doc))
                };
                if let Some(machine) = self.machines.get_mut(&doc) {
                    machine.doc.hydrated(call, outcome);
                }
            }
            Ask::Decrypt => {
                let outcome = if refused || !readable {
                    refusal(cancelled)
                } else {
                    Outcome::Done(self.world.materialize(doc))
                };
                if let Some(machine) = self.machines.get_mut(&doc) {
                    machine.doc.decrypted(call, outcome);
                }
            }
            Ask::Publish { heads, rev } => {
                let outcome = if refused {
                    refusal(cancelled)
                } else {
                    self.world.publish(doc, heads, rev);
                    Outcome::Done(())
                };
                if let Some(machine) = self.machines.get_mut(&doc) {
                    machine.doc.published(call, outcome);
                }
            }
            Ask::Encrypt { change, .. } => {
                self.next_token += 1;
                let outcome = if refused {
                    refusal(cancelled)
                } else {
                    Outcome::Done(EncryptedChange {
                        token: self.next_token,
                        ciphertext: change,
                        epoch: self.world.docs[doc].epoch,
                    })
                };
                self.coordinator.encrypted(call, outcome);
                self.pump_coordinator();
            }
            Ask::Commit { rows } => {
                // the commit's transaction checks the epochs of the plan's own documents
                let stale = rows.iter().find(|row| {
                    row.if_epoch
                        .is_some_and(|epoch| epoch != self.world.docs[row.doc as usize].epoch)
                });
                let outcome = if refused {
                    refusal(cancelled)
                } else if let Some(row) = stale {
                    Outcome::Stale { doc: row.doc }
                } else {
                    let batch = rows
                        .iter()
                        .map(|row| {
                            let change = Change::from_bytes(row.change.ciphertext.clone())
                                .expect("a change");
                            (row.doc as usize, change, row.frontier.clone())
                        })
                        .collect();
                    let landed: Vec<Landed> = self.world.commit(batch);
                    let mut out = vec![];
                    for (row, landed) in rows.iter().zip(landed) {
                        self.bump_feed(row.doc as usize);
                        out.push((row.doc, landed));
                    }
                    Outcome::Done(out)
                };
                self.coordinator.committed(call, outcome);
                self.pump_coordinator();
            }
        }
        if let Some(machine_doc) = self.machines.contains_key(&doc).then_some(doc) {
            self.flush_doc(machine_doc);
        }
    }

    // ── restart ─────────────────────────────────────────────────────────────────────────────

    /// The process dies: every machine, flight and message goes, the world and the saved cursor
    /// stay; the Hub is rebuilt as its driver would.
    pub(super) fn restart(&mut self) {
        self.boots += 1;
        self.restart_cursor = self.saved_cursor;
        self.hub = Hub::new(config(), boot(self.saved_cursor, self.boots));
        if let Some(plant) = self.plant {
            self.hub.plant(plant);
        }
        self.coordinator = Coordinator::new(coordinator::Config::default());
        self.machines.clear();
        self.last_generation.clear();
        self.flights.clear();
        self.mailbox.clear();
        self.stopping.clear();
        self.deferred.clear();
        self.reports.clear();
        self.epoch_mail.clear();
        self.plans.clear();
        self.opening.clear();
        self.leases.clear();
        self.dial_flights.clear();
        self.net_open.clear();
        self.routes.clear();
        self.refused.clear();
        self.told_standing.clear();
        self.round_flights.clear();
        self.waiters
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|w| {
                self.resolved.insert(w);
            });
        self.delivered_tree.clear();
        self.told_readable.clear();
        self.draining = false;
        self.drained_seen = false;
        // the feed after the cursor, then the documents that are stale now, by a query
        let mut rows: BTreeMap<usize, Rev> = BTreeMap::new();
        for doc in 0..DOCS {
            let tree = self.world.tree_rev(doc);
            let stale = self.world.docs[doc].readable && self.world.frontier_rev(doc) < tree;
            if tree > self.saved_cursor || stale {
                rows.insert(doc, tree);
            }
        }
        let mut rows: Vec<_> = rows.into_iter().map(|(doc, rev)| (rev, doc)).collect();
        rows.sort();
        self.feed_log = rows.into();
        for peer in self.connected.clone() {
            self.hub.peer_connected(peer, 0);
        }
        self.pump();
    }
}

/// A call the driver refuses or that was cancelled.
fn refusal<T>(cancelled: bool) -> Outcome<T> {
    if cancelled {
        Outcome::Cancelled
    } else {
        Outcome::Failed
    }
}

fn boot(cursor: Rev, boots: u8) -> Boot {
    let mut nonce = [0u8; 16];
    nonce[0] = boots;
    nonce[1] = 0x5a;
    Boot { cursor, nonce }
}
