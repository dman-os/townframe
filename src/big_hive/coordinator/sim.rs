//! The simulation of the Write coordinator composed with real [Document](crate::document)
//! machines: the coordinator and three Document machines are the code under test, and the rest
//! (the Keyhive boundary's encryption, the storage commit, the keyhive epoch, the change feed,
//! the Hub) is faked by a driver over the oracle [`World`]. The laws are in the [parent
//! module](super). One deterministic test, as the Document machine's own simulation.
//!
//! # What is modelled
//!
//! - **Plans.** Caller plans over any subset of the documents, each part a write, an ingest of
//!   an external author's change, a closure that changes nothing, or a closure that fails
//!   (which its Document refuses). Documents may be unreadable or parked on keys that have not
//!   arrived, so plans are refused too.
//! - **The driver.** Every call becomes a call in flight that an op finishes in any order or
//!   fails: encryptions, commits, and the Documents' own hydrations, decryptions, publications
//!   and actor requests. A commit lands all of its rows in the world at once or none; it may
//!   also be reported failed though it ran, now or later.
//! - **The world.** Other nodes commit changes with their keys arriving later, and documents
//!   become unreadable and readable again. The feed and epoch moves are mail, per document in
//!   order and across documents in any order.
//! - **Crashes** drop the coordinator, every Document and every call, and keep the world: a
//!   commit in flight either ran or did not.
//!
//! # Checks
//!
//! After every step: documents in flight are disjoint (law 1, from the commands the machine
//! issues); plans that share a document start in submission order (law 4); a plan's changes are in the world all or
//! none (law 2); a Document that holds a plan is a plan in flight (law 3); the number of plans
//! in flight is within the window. At each answer: every Document that did not refuse was
//! released exactly once (law 3), a refusal left nothing committed and named its document (law
//! 5), a commit's answer has every change committed, and each ticket is answered once (law 6).
//! Every encryption is settled once (law 6). After a quiet drain: nothing is queued or in
//! flight, no Document is held, every ticket was answered, and every live readable Document
//! holds exactly what a reader may see.
//!
//! `SIM_TRACE=1` prints each op and the commands it caused.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Duration;

use automerge::ActorId;
use automerge::Change;
use automerge::ChangeHash;
use automerge::ROOT;
use automerge::transaction::Transactable;
use proptest::prelude::*;
use web_time::Instant;

use super::*;
use crate::document::Command as DocCommand;
use crate::document::Config as DocConfig;
use crate::document::Document;
use crate::document::Start;
use crate::document::tests::oracle::World;
use big_automata::rev::Rev;

mod cases;

type Out = Vec<String>;

pub(super) const DOCS: usize = 3;

enum Ask {
    Hydrate,
    Decrypt,
    Publish { heads: Vec<ChangeHash>, rev: Rev },
    Actor,
    Encrypt { change: Vec<u8> },
    Commit { rows: Vec<DocRows> },
}

enum Owner {
    Doc(usize),
    Coordinator,
}

struct Flight {
    owner: Owner,
    call: CallId,
    ask: Ask,
    cancelled: bool,
}

#[derive(Debug, Clone)]
enum Mail {
    Tree(usize, Rev),
    Epoch(usize, bool),
}

impl Mail {
    fn doc(&self) -> usize {
        match self {
            Mail::Tree(doc, _) | Mail::Epoch(doc, _) => *doc,
        }
    }
}

/// What a submitted plan is expected to do, and what has been seen of it.
struct Seen {
    ticket: Ticket,
    docs: Vec<usize>,
    /// The documents that refused to prepare.
    refused: BTreeSet<usize>,
    /// The hashes of the changes its Documents made.
    changes: BTreeSet<ChangeHash>,
    releases: BTreeMap<usize, u32>,
    started: bool,
}

pub(super) enum Part {
    Write,
    Ingest,
    Noop,
    Bad,
}

pub(super) struct Sim {
    pub world: World,
    docs: Vec<Document<Out>>,
    coordinator: Coordinator<Out>,
    flights: Vec<Flight>,
    /// The coordinator's commands not yet carried out.
    outbox: VecDeque<Command<Out>>,
    draining: bool,
    mail: Vec<Mail>,
    zombies: Vec<Vec<(usize, Change)>>,
    plans: BTreeMap<PlanId, Seen>,
    /// The plans in the order they were submitted.
    order: Vec<PlanId>,
    /// The documents of the plans that have started and not been answered.
    in_flight: BTreeMap<PlanId, Vec<usize>>,
    answered: BTreeSet<Ticket>,
    replies: BTreeMap<Ticket, Answer<Out>>,
    issued: BTreeSet<u64>,
    settled: BTreeSet<u64>,
    told_readable: Vec<bool>,
    next_ticket: Ticket,
    next_token: u64,
    next_tag: Vec<u64>,
    next_author: Vec<u64>,
    next_actor: u64,
    generation: u64,
    now: Instant,
    start: Instant,
    rng: u64,
    plant: Option<Plant>,
    config: Config,
    trace: bool,
}

impl Sim {
    pub fn new(seed: u64, plant: Option<Plant>) -> Self {
        Self::with(seed, plant, Config::default())
    }

    pub fn with(seed: u64, plant: Option<Plant>, config: Config) -> Self {
        let now = Instant::now();
        let mut sim = Self {
            world: World::new(DOCS),
            docs: vec![],
            coordinator: Coordinator::new(config.clone()),
            flights: vec![],
            outbox: VecDeque::new(),
            draining: false,
            mail: vec![],
            zombies: vec![],
            plans: BTreeMap::new(),
            order: vec![],
            in_flight: BTreeMap::new(),
            answered: BTreeSet::new(),
            replies: BTreeMap::new(),
            issued: BTreeSet::new(),
            settled: BTreeSet::new(),
            told_readable: vec![true; DOCS],
            next_ticket: 0,
            next_token: 0,
            next_tag: vec![0; DOCS],
            next_author: vec![0; DOCS],
            next_actor: 0,
            generation: 0,
            now,
            start: now,
            rng: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            plant,
            config,
            trace: std::env::var_os("SIM_TRACE").is_some(),
        };
        sim.boot();
        sim
    }

    pub fn draw(&mut self, below: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as usize % below.max(1)
    }

    /// A fresh coordinator and a Document machine for each document, interested so that they
    /// hydrate, as the Hub would start them.
    fn boot(&mut self) {
        self.coordinator = Coordinator::new(self.config.clone());
        #[cfg(test)]
        {
            self.coordinator.plant = self.plant;
        }
        self.docs.clear();
        for doc in 0..DOCS {
            self.generation += 1;
            let model = &self.world.docs[doc];
            let mut machine = Document::new(
                DocConfig::default(),
                Start {
                    generation: self.generation,
                    actor: ActorId::from(vec![0xa0, doc as u8, self.generation as u8]),
                    readable: model.readable,
                    known_rev: model.tree_rev,
                    frontier_rev: self.world.frontier_rev(doc),
                },
            );
            machine.interest_acquired();
            self.told_readable[doc] = model.readable;
            self.docs.push(machine);
        }
        for doc in 0..DOCS {
            self.flush_doc(doc);
        }
    }

    // ── driving the machines ────────────────────────────────────────────────────────────────

    fn flush_doc(&mut self, doc: usize) {
        let mut out = vec![];
        self.docs[doc].flush(self.now, &mut out);
        for command in out {
            if self.trace {
                eprintln!("  doc {doc}: {command:?}");
            }
            match command {
                DocCommand::Hydrate { call } => self.fly(Owner::Doc(doc), call, Ask::Hydrate),
                DocCommand::Decrypt { call, .. } => self.fly(Owner::Doc(doc), call, Ask::Decrypt),
                DocCommand::PublishFrontier { call, heads, rev } => {
                    self.fly(Owner::Doc(doc), call, Ask::Publish { heads, rev })
                }
                DocCommand::NewActor { call } => self.fly(Owner::Doc(doc), call, Ask::Actor),
                DocCommand::Cancel { call } => self.cancel(&Owner::Doc(doc), call),
                DocCommand::Prepared { plan, result } => self.on_prepared(doc, plan, result),
                // single-document writes and listeners are not part of these plans
                DocCommand::PrepareEncrypt { .. }
                | DocCommand::Commit { .. }
                | DocCommand::SettleEncrypt { .. }
                | DocCommand::Reply { .. }
                | DocCommand::Ingested { .. }
                | DocCommand::Notify { .. }
                | DocCommand::Evictable => {}
            }
        }
    }

    /// Flush the coordinator and carry its commands out in the order it issued them. A command
    /// is never carried out inside another's: the Documents see what one machine sent them in
    /// order, as through a mailbox, so a re-entrant flush only queues.
    pub fn flush_coordinator(&mut self) {
        let mut out = vec![];
        self.coordinator.flush(&mut out);
        self.outbox.extend(out);
        if std::mem::replace(&mut self.draining, true) {
            return;
        }
        while let Some(command) = self.outbox.pop_front() {
            if self.trace {
                eprintln!("  coordinator: {command:?}");
            }
            self.on_command(command);
        }
        self.draining = false;
        self.check();
    }

    fn fly(&mut self, owner: Owner, call: CallId, ask: Ask) {
        self.flights.push(Flight {
            owner,
            call,
            ask,
            cancelled: false,
        });
    }

    fn cancel(&mut self, owner: &Owner, call: CallId) {
        let flight = self
            .flights
            .iter_mut()
            .find(|f| f.call == call && same(&f.owner, owner))
            .expect("a cancelled call is in flight");
        flight.cancelled = true;
    }

    /// A coordinator command, checked against the laws and carried out.
    fn on_command(&mut self, command: Command<Out>) {
        match command {
            Command::Prepare { doc, plan, work } => {
                let doc = doc as usize;
                let seen = self.plans.get_mut(&plan).expect("a submitted plan");
                if !seen.started {
                    seen.started = true;
                    // law 4: every plan submitted before it that shares a document has started
                    let mine = self.plans[&plan].docs.clone();
                    let earlier = self.order.iter().take_while(|p| **p != plan);
                    for before in earlier {
                        let shares = self.plans[before].docs.iter().any(|d| mine.contains(d));
                        assert!(
                            !shares || self.plans[before].started,
                            "plan {plan} started before plan {before}, which shares a document and was submitted first (law 4)"
                        );
                    }
                    // law 1: its documents are in no plan in flight
                    let docs = self.plans[&plan].docs.clone();
                    for (other, others) in &self.in_flight {
                        assert!(
                            docs.iter().all(|d| !others.contains(d)),
                            "plans {plan} and {other} are in flight on one document (law 1)"
                        );
                    }
                    self.in_flight.insert(plan, docs);
                }
                self.docs[doc].prepare_work(plan, work);
                self.flush_doc(doc);
            }
            Command::PrepareEncrypt { call, change, .. } => {
                self.fly(Owner::Coordinator, call, Ask::Encrypt { change });
            }
            Command::Commit { call, plan, rows } => {
                let docs: BTreeSet<_> =
                    self.plans[&plan].docs.iter().map(|d| *d as DocId).collect();
                assert!(
                    rows.iter().all(|row| docs.contains(&row.doc)),
                    "a commit carries only its plan's documents"
                );
                self.fly(Owner::Coordinator, call, Ask::Commit { rows });
            }
            Command::SettleEncrypt { token, .. } => {
                assert!(
                    self.issued.contains(&token),
                    "settling a preparation that was made"
                );
                assert!(
                    self.settled.insert(token),
                    "a preparation is settled once (law 6)"
                );
            }
            Command::Apply { doc, plan, landed } => {
                self.release(plan, doc as usize);
                self.docs[doc as usize].apply(plan, landed);
                self.flush_doc(doc as usize);
            }
            Command::Abort { doc, plan } => {
                self.release(plan, doc as usize);
                self.docs[doc as usize].abort(plan);
                self.flush_doc(doc as usize);
            }
            Command::Cancel { call } => self.cancel(&Owner::Coordinator, call),
            Command::Reply { ticket, result } => self.on_reply(ticket, result),
        }
    }

    fn release(&mut self, plan: PlanId, doc: usize) {
        *self
            .plans
            .get_mut(&plan)
            .expect("a submitted plan")
            .releases
            .entry(doc)
            .or_default() += 1;
    }

    fn on_prepared(
        &mut self,
        doc: usize,
        plan: PlanId,
        result: Result<PlanPrepared<Out>, Rejected>,
    ) {
        // An answer that comes after the plan is over (it was aborted while the Document was
        // still preparing) is ignored by the coordinator, and was released with the rest.
        let running = self.coordinator.running.contains_key(&plan);
        if let Some(seen) = self.plans.get_mut(&plan).filter(|_| running) {
            match &result {
                Ok(PlanPrepared {
                    change: Some(change),
                    ..
                }) => {
                    let hash = Change::from_bytes(change.clone()).expect("a change").hash();
                    seen.changes.insert(hash);
                }
                Ok(_) => {}
                Err(_) => {
                    seen.refused.insert(doc);
                }
            }
        }
        self.coordinator.prepared(doc as DocId, plan, result);
        self.flush_coordinator();
    }

    fn on_reply(&mut self, ticket: Ticket, result: Answer<Out>) {
        assert!(
            self.answered.insert(ticket),
            "a plan is answered once (law 6)"
        );
        let (plan, seen) = self
            .plans
            .iter()
            .find(|(_, seen)| seen.ticket == ticket)
            .map(|(plan, seen)| (*plan, seen))
            .expect("a ticket of a plan");
        // law 3: every document that did not refuse was released exactly once
        for doc in &seen.docs {
            let expected = u32::from(!seen.refused.contains(doc));
            let got = seen.releases.get(doc).copied().unwrap_or(0);
            assert_eq!(
                got, expected,
                "plan {plan}: document {doc} was released {got} time(s), not {expected} (law 3)"
            );
        }
        let committed = self.committed_changes(plan);
        let all = seen.changes.len();
        match &result {
            Ok(outs) => {
                assert_eq!(
                    committed, all,
                    "plan {plan} answered but not all committed (law 2)"
                );
                assert_eq!(outs.len(), seen.docs.len());
            }
            Err(PlanError::Refused { doc, .. }) => {
                assert!(
                    seen.refused.contains(&(*doc as usize)),
                    "plan {plan} names a document that did not refuse (law 5)"
                );
                assert_eq!(
                    committed, 0,
                    "plan {plan} was refused and committed (law 5)"
                );
            }
            Err(PlanError::Failed) => {}
        }
        self.in_flight.remove(&plan);
        self.replies.insert(ticket, result);
    }

    /// How many of the plan's changes are in the world.
    fn committed_changes(&self, plan: PlanId) -> usize {
        let seen = &self.plans[&plan];
        let mut hashes = BTreeSet::new();
        for doc in 0..DOCS {
            hashes.extend(self.world.committed_hashes(doc));
        }
        seen.changes
            .iter()
            .filter(|hash| hashes.contains(hash))
            .count()
    }

    // ── ops ─────────────────────────────────────────────────────────────────────────────────

    /// Submit a plan over the documents of `mask`, each part as `parts` says.
    pub fn submit(&mut self, mask: usize, parts: &[Part]) -> Ticket {
        self.next_ticket += 1;
        let ticket = self.next_ticket;
        let mut docs = vec![];
        let mut writes = vec![];
        for (at, part) in parts.iter().enumerate() {
            let doc = (0..DOCS)
                .filter(|d| mask >> d & 1 == 1)
                .nth(at)
                .expect("a part per document");
            docs.push(doc);
            writes.push((doc as DocId, self.work(doc, part)));
        }
        let plan = self.coordinator.submit(ticket, writes);
        self.order.push(plan);
        self.plans.insert(
            plan,
            Seen {
                ticket,
                docs,
                refused: BTreeSet::new(),
                changes: BTreeSet::new(),
                releases: BTreeMap::new(),
                started: false,
            },
        );
        self.flush_coordinator();
        ticket
    }

    fn work(&mut self, doc: usize, part: &Part) -> Work<Out> {
        match part {
            Part::Write => {
                self.next_tag[doc] += 1;
                let tag = self.next_tag[doc];
                Work::Op(Box::new(move |tx| {
                    tx.put(ROOT, format!("l{tag}"), tag as i64)?;
                    Ok(vec![format!("l{tag}")])
                }))
            }
            Part::Ingest => {
                self.next_author[doc] += 1;
                Work::Changes(vec![self.world.author_change(doc, self.next_author[doc])])
            }
            Part::Noop => Work::Op(Box::new(|_| Ok(vec![]))),
            Part::Bad => Work::Op(Box::new(|tx| tx.insert(ROOT, 0, 1_i64).map(|()| vec![]))),
        }
    }

    pub fn remote_write(&mut self, doc: usize, keyed: bool) {
        let rev = self.world.remote_write(doc, keyed);
        self.mail.push(Mail::Tree(doc, rev));
    }

    pub fn grant_keys(&mut self, doc: usize) {
        self.world.grant_keys(doc);
        self.mail
            .push(Mail::Epoch(doc, self.world.docs[doc].readable));
    }

    pub fn set_readable(&mut self, doc: usize, readable: bool) {
        self.world.docs[doc].readable = readable;
        self.mail.push(Mail::Epoch(doc, readable));
    }

    /// Mail to the document of the `pick`th piece, its oldest first.
    pub fn deliver(&mut self, pick: usize) {
        if self.mail.is_empty() {
            return;
        }
        let doc = self.mail[pick % self.mail.len()].doc();
        let at = self
            .mail
            .iter()
            .position(|m| m.doc() == doc)
            .expect("picked");
        match self.mail.remove(at) {
            Mail::Tree(doc, rev) => self.docs[doc].tree_changed(rev),
            Mail::Epoch(doc, readable) => {
                self.told_readable[doc] = readable;
                self.docs[doc].epoch_moved(readable);
            }
        }
        self.flush_doc(doc);
        self.flush_coordinator();
    }

    /// Finish the call at `pick`: `mode` 0 ok, 1 failed, 2 ran but reported failed, 3 reported
    /// failed and runs later (the last two only for a commit).
    pub fn complete(&mut self, pick: usize, mode: usize) {
        if self.flights.is_empty() {
            return;
        }
        let flight = self.flights.remove(pick % self.flights.len());
        let cancelled = flight.cancelled && self.draw(2) == 0;
        let refused = mode == 1 || cancelled;
        match (flight.owner, flight.ask) {
            (Owner::Doc(doc), Ask::Hydrate) => {
                let readable = self.world.docs[doc].readable;
                let out = if refused || !readable {
                    failed(cancelled)
                } else {
                    Outcome::Done(self.world.materialize(doc))
                };
                self.docs[doc].hydrated(flight.call, out);
                self.flush_doc(doc);
            }
            (Owner::Doc(doc), Ask::Decrypt) => {
                let readable = self.world.docs[doc].readable;
                let out = if refused || !readable {
                    failed(cancelled)
                } else {
                    Outcome::Done(self.world.materialize(doc))
                };
                self.docs[doc].decrypted(flight.call, out);
                self.flush_doc(doc);
            }
            (Owner::Doc(doc), Ask::Publish { heads, rev }) => {
                let out = if refused {
                    failed(cancelled)
                } else {
                    self.world.publish(doc, heads, rev);
                    Outcome::Done(())
                };
                self.docs[doc].published(flight.call, out);
                self.flush_doc(doc);
            }
            (Owner::Doc(doc), Ask::Actor) => {
                self.next_actor += 1;
                let mut actor = vec![0xa1, doc as u8];
                actor.extend(self.next_actor.to_be_bytes());
                self.docs[doc].actor_assigned(flight.call, ActorId::from(actor));
                self.flush_doc(doc);
            }
            (Owner::Coordinator, Ask::Encrypt { change }) => {
                let out = if refused {
                    failed(cancelled)
                } else {
                    self.next_token += 1;
                    self.issued.insert(self.next_token);
                    Outcome::Done(EncryptedChange {
                        token: self.next_token,
                        ciphertext: change,
                    })
                };
                self.coordinator.encrypted(flight.call, out);
                self.flush_coordinator();
            }
            (Owner::Coordinator, Ask::Commit { rows }) => {
                let batch: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        let change =
                            Change::from_bytes(row.change.ciphertext.clone()).expect("a change");
                        (row.doc as usize, change, row.frontier.clone())
                    })
                    .collect();
                let docs: Vec<DocId> = rows.iter().map(|row| row.doc).collect();
                let out = if cancelled {
                    failed(true)
                } else {
                    match mode {
                        1 => failed(false),
                        2 => {
                            // it ran, and the driver lost the answer
                            self.land(batch);
                            failed(false)
                        }
                        3 => {
                            self.zombies.push(
                                batch
                                    .into_iter()
                                    .map(|(doc, change, _)| (doc, change))
                                    .collect(),
                            );
                            failed(false)
                        }
                        _ => {
                            let landed = self.land(batch);
                            Outcome::Done(docs.into_iter().zip(landed).collect())
                        }
                    }
                };
                self.coordinator.committed(flight.call, out);
                self.flush_coordinator();
            }
            _ => unreachable!("a call is answered by its asker"),
        }
    }

    /// Commit rows of several documents in one transaction of the world.
    fn land(&mut self, batch: Vec<(usize, Change, Option<FrontierRow>)>) -> Vec<Landed> {
        let docs: Vec<usize> = batch.iter().map(|(doc, ..)| *doc).collect();
        let landed = self.world.commit(batch);
        for (doc, landed) in docs.into_iter().zip(&landed) {
            self.mail.push(Mail::Tree(doc, landed.rev));
        }
        landed
    }

    /// A commit that was reported failed runs now (with no frontier rows).
    pub fn land_zombie(&mut self, pick: usize) {
        if self.zombies.is_empty() {
            return;
        }
        let batch = self.zombies.remove(pick % self.zombies.len());
        self.land(
            batch
                .into_iter()
                .map(|(doc, change)| (doc, change, None))
                .collect(),
        );
    }

    /// The process dies: every machine and call is gone. Bit `i` of `landed` says whether the
    /// `i`th commit in flight had run.
    pub fn crash(&mut self, landed: usize) {
        let commits: Vec<Vec<DocRows>> = self
            .flights
            .drain(..)
            .filter_map(|flight| match flight.ask {
                Ask::Commit { rows } => Some(rows),
                _ => None,
            })
            .collect();
        for (bit, rows) in commits.into_iter().enumerate() {
            if landed >> (bit % 8) & 1 == 1 {
                let batch = rows
                    .into_iter()
                    .map(|row| {
                        let change = Change::from_bytes(row.change.ciphertext).expect("a change");
                        (row.doc as usize, change, row.frontier)
                    })
                    .collect();
                self.land(batch);
            }
        }
        self.zombies.clear();
        self.outbox.clear();
        self.mail.clear();
        self.in_flight.clear();
        // the Keyhive boundary restarts too: preparations made before are gone
        self.issued.clear();
        self.settled.clear();
        // plans in flight are lost with the process; their tickets are never answered
        let lost: Vec<Ticket> = self
            .plans
            .values()
            .map(|seen| seen.ticket)
            .filter(|ticket| !self.answered.contains(ticket))
            .collect();
        self.answered.extend(lost);
        for seen in self.plans.values_mut() {
            seen.started = true;
        }
        self.boot();
    }

    pub fn advance(&mut self, by: Duration) {
        self.now += by;
        for doc in 0..DOCS {
            self.flush_doc(doc);
        }
        self.flush_coordinator();
    }

    pub fn tick(&mut self) {
        let due = self.docs.iter().filter_map(|doc| doc.next_due()).min();
        if let Some(due) = due {
            self.now = self.now.max(due);
            for doc in 0..DOCS {
                self.flush_doc(doc);
            }
            self.flush_coordinator();
        }
    }

    /// Drain: reliable calls, all mail delivered, backoffs waited out, until nothing is left;
    /// then check the world.
    pub fn quiesce(&mut self) {
        for _ in 0..10_000 {
            if !self.mail.is_empty() {
                self.deliver(0);
            } else if !self.zombies.is_empty() {
                self.land_zombie(0);
            } else if !self.flights.is_empty() {
                self.complete(0, 0);
            } else if let Some(due) = self.docs.iter().filter_map(|d| d.next_retry()).min() {
                self.now = self.now.max(due);
                for doc in 0..DOCS {
                    self.flush_doc(doc);
                }
                self.flush_coordinator();
            } else {
                self.check_quiet();
                return;
            }
        }
        panic!("the drain does not settle");
    }

    pub fn snapshot(&self) -> String {
        format!(
            "running={:?} queued={} flights={} mail={} docs={:?} trees={:?} frontiers={:?} now={:?}",
            self.coordinator.running.keys().collect::<Vec<_>>(),
            self.coordinator.queue.len(),
            self.flights.len(),
            self.mail.len(),
            self.docs.iter().map(Document::describe).collect::<Vec<_>>(),
            (0..DOCS)
                .map(|d| self.world.tree_rev(d))
                .collect::<Vec<_>>(),
            (0..DOCS)
                .map(|d| self.world.frontier_rev(d))
                .collect::<Vec<_>>(),
            self.now - self.start,
        )
    }

    // ── checks ──────────────────────────────────────────────────────────────────────────────

    /// After every step.
    pub fn check(&self) {
        assert!(
            self.coordinator.running.len() <= self.config.max_plans,
            "more plans in flight than the window"
        );
        // law 1, from the machine's own record: the plans in flight name disjoint documents
        let mut seen = BTreeSet::new();
        for running in self.coordinator.running.values() {
            for doc in &running.docs {
                assert!(seen.insert(*doc), "document {doc} is in two plans (law 1)");
            }
        }
        // law 2: each plan's changes are in the world all or none
        for (plan, seen) in &self.plans {
            let committed = self.committed_changes(*plan);
            assert!(
                committed == 0 || committed == seen.changes.len(),
                "plan {plan} is partly committed: {committed} of {} (law 2)",
                seen.changes.len()
            );
        }
        // law 3 (and 5): a document held for a plan is held for one that is in flight
        for (doc, machine) in self.docs.iter().enumerate() {
            if let Some(plan) = machine.held_plan() {
                assert!(
                    self.coordinator.running.contains_key(&plan),
                    "document {doc} is held for plan {plan}, which is over (laws 3 and 5)"
                );
            }
        }
    }

    /// After a quiet drain.
    pub fn check_quiet(&self) {
        assert!(self.flights.is_empty() && self.mail.is_empty());
        assert!(
            self.coordinator.is_idle(),
            "the coordinator is idle after a drain"
        );
        let unanswered = self
            .plans
            .values()
            .filter(|s| !self.answered.contains(&s.ticket))
            .count();
        assert_eq!(unanswered, 0, "every plan was answered (law 6)");
        assert_eq!(
            self.issued, self.settled,
            "every prepared encryption was settled (law 6)"
        );
        for (doc, machine) in self.docs.iter().enumerate() {
            assert!(
                machine.held_plan().is_none(),
                "document {doc} is still held after a drain (laws 3 and 5)"
            );
            if let Some(live) = machine.live_keys()
                && self.world.docs[doc].readable
            {
                assert_eq!(
                    live,
                    self.world.visible(doc),
                    "doc {doc}: a quiet live document is what a reader may see ({})",
                    machine.describe()
                );
            }
        }
    }
}

fn failed<T>(cancelled: bool) -> Outcome<T> {
    if cancelled {
        Outcome::Cancelled
    } else {
        Outcome::Failed
    }
}

fn same(a: &Owner, b: &Owner) -> bool {
    match (a, b) {
        (Owner::Doc(a), Owner::Doc(b)) => a == b,
        (Owner::Coordinator, Owner::Coordinator) => true,
        _ => false,
    }
}

/// Scripted control, for the law tests.
impl Sim {
    /// The kinds of the calls in flight, oldest first: the coordinator's (`encrypt` and
    /// `commit`), and the Documents' (`hydrate`, `decrypt`, `publish`, `actor`) if `documents`.
    fn kinds_of(&self, documents: bool) -> Vec<&'static str> {
        self.flights
            .iter()
            .filter(|flight| documents || matches!(flight.owner, Owner::Coordinator))
            .map(|flight| match flight.ask {
                Ask::Hydrate => "hydrate",
                Ask::Decrypt => "decrypt",
                Ask::Publish { .. } => "publish",
                Ask::Actor => "actor",
                Ask::Encrypt { .. } => "encrypt",
                Ask::Commit { .. } => "commit",
            })
            .collect()
    }

    /// The coordinator's calls in flight, oldest first.
    pub fn kinds(&self) -> Vec<&'static str> {
        self.kinds_of(false)
    }

    /// Finish the oldest call of `kind` in `mode` (see [`Sim::complete`]). Answers whether
    /// there was one.
    pub fn complete_kind(&mut self, kind: &str, mode: usize) -> bool {
        let Some(at) = self.kinds_of(true).iter().position(|k| *k == kind) else {
            return false;
        };
        self.complete(at, mode);
        true
    }

    /// Finish every call of the Documents until only the coordinator's are left.
    pub fn settle_documents(&mut self) {
        while let Some(at) = self
            .flights
            .iter()
            .position(|f| matches!(f.owner, Owner::Doc(_)))
        {
            self.complete(at, 0);
        }
        self.deliver_all();
        while let Some(at) = self
            .flights
            .iter()
            .position(|f| matches!(f.owner, Owner::Doc(_)))
        {
            self.complete(at, 0);
        }
    }

    pub fn deliver_all(&mut self) {
        while !self.mail.is_empty() {
            self.deliver(0);
        }
    }

    /// The answer to a plan, once it has been given.
    pub fn reply(&self, ticket: Ticket) -> &Answer<Out> {
        self.replies.get(&ticket).expect("the plan was answered")
    }

    pub fn running(&self) -> usize {
        self.coordinator.running.len()
    }

    pub fn queued(&self) -> usize {
        self.coordinator.queue.len()
    }

    /// Whether the ticket has been answered.
    pub fn is_answered(&self, ticket: Ticket) -> bool {
        self.answered.contains(&ticket)
    }
}
