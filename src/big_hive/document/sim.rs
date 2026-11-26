//! The whole-machine simulation of [`Document`]: a few documents, each with a machine, a driver
//! that answers every command from the oracle [`World`](oracle::World), a Hub that starts and
//! stops machines, and a Write coordinator, in one deterministic test. The laws are in the
//! [parent module](super); the law tests in `tests/` drive this same rig by hand.
//!
//! # What is modelled
//!
//! - **The driver.** Every command with a call becomes a call in flight. An op completes any
//!   call in flight, in any order, or fails it. A hydration or decryption answers from the
//!   world as it is when the call completes; a commit lands in the world when it completes
//!   (and may fail, in which case nothing landed); an encryption answers with the change.
//!   A call the machine cancelled answers `Cancelled` or, as a real driver may, its result.
//! - **The world.** Local changes land at revisions of one sequence. Other nodes commit changes
//!   too, with their keys arriving later or never, and a document can become unreadable and
//!   readable again.
//! - **The Hub.** Feed rows (`tree_changed`) and epoch moves are mail that ops deliver in any
//!   order, and duplicate. It starts a machine for a document whose frontier is stale and which
//!   is readable and not parked, and stops a machine when it reports itself evictable.
//! - **The Write coordinator.** Plans over one or two documents: prepared on each, committed
//!   together or aborted, in any order the ops pick.
//! - **Handles.** Opening and dropping handles, interest, writes (which succeed, change
//!   nothing, fail in their closure, or use a handle of a machine that is gone) and reads.
//! - **Crashes** drop every machine, every call in flight and all mail, and keep the world. A
//!   commit in flight at the crash either landed or did not, as the op says. Every machine is
//!   then rebuilt from the world, as the Hub would.
//! - **Time** is a virtual clock; backoffs and idle deadlines run on it.
//!
//! # What is not modelled
//!
//! Real encryption, SQL, the keyhive instance, author filtering, and a machine that
//! disagrees with its driver about the protocol (the driver is correct by construction).
//!
//! # Checks
//!
//! After every step ([`checks`]): the machine's own invariants (asserted at the end of every
//! flush in test builds); the live document holds only committed changes (law 1); per-document
//! landing order is submission order and each change is built on the previous one (law 2); a
//! reply's write is applied and committed (law 3) and a read sees every write acknowledged before
//! it was asked and nothing uncommitted (laws 1 and 3); a document prepared for a plan starts
//! nothing else (law 4); a published frontier holds the heads of the tree at its revision and
//! is never ahead of the tree or the machine (law 5); a parked document has a reason (law 6); an
//! evictable machine has nothing left (law 7); no command reads an unreadable document (law 8).
//!
//! After a quiet drain (reliable calls, all mail delivered, plans finished): no machine has
//! anything in flight, every live readable document holds exactly what a reader may see, and
//! every readable document whose content is all keyed has its frontier at the tree. Every case
//! then crashes, restarts and drains again.
//!
//! `SIM_TRACE=1` prints each op and the commands it caused.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use automerge::ROOT;
use automerge::ReadDoc;
use automerge::transaction::Transactable;

use super::tests::oracle::World;
use super::*;

mod checks;
mod ops;
mod script;

mod cases;

pub(super) use ops::Op;

type Out = Vec<String>;

/// How many documents the world has.
pub(super) const DOCS: usize = 2;

struct Machine {
    doc: Document<Out>,
    handles: Vec<Handle>,
    interest: u32,
    /// The readability the machine was last told, which law 8 is checked against.
    told_readable: bool,
}

/// What a call in flight asks of the driver.
#[derive(Debug, Clone)]
enum Ask {
    Hydrate,
    Decrypt,
    Encrypt(Vec<u8>),
    Commit(Plan),
    Publish { heads: Vec<ChangeHash>, rev: Rev },
}

struct Flight {
    doc: usize,
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

struct PlanState {
    docs: Vec<usize>,
    prepared: BTreeMap<usize, PlanPrepared<Out>>,
    failed: bool,
}

pub(super) enum WriteKind {
    Normal,
    /// The closure changes nothing.
    Noop,
    /// The closure fails.
    Bad,
    /// The handle belongs to a machine that is gone.
    Stale,
}

/// A request waiting for its reply, with what the reply is checked against.
enum Waiting {
    Write {
        doc: usize,
        tag: Option<u64>,
    },
    /// An external author's changes, adding `key`.
    Ingest {
        doc: usize,
        key: String,
    },
    Read {
        doc: usize,
        must_see: BTreeSet<String>,
    },
}

pub(super) struct Sim {
    pub world: World,
    machines: Vec<Option<Machine>>,
    flights: Vec<Flight>,
    mail: Vec<Mail>,
    plans: BTreeMap<PlanId, PlanState>,
    next_plan: PlanId,
    /// The plan that holds each document, from its `Prepared` to its `apply` or `abort`.
    held: BTreeMap<usize, PlanId>,
    /// Documents the Hub gave up starting until their keys arrive.
    parked: BTreeSet<usize>,
    waiting: BTreeMap<Ticket, Waiting>,
    next_ticket: Ticket,
    next_tag: Vec<u64>,
    last_landed: Vec<Option<(u64, ChangeHash)>>,
    acked: Vec<BTreeSet<String>>,
    stale_handles: Vec<(usize, Handle)>,
    /// The last change an external author sent for each document, and the key it adds.
    last_ingest: Vec<Option<(Change, String)>>,
    next_author: Vec<u64>,
    settled: BTreeSet<u64>,
    next_token: u64,
    generation: u64,
    now: Instant,
    start: Instant,
    rng: u64,
    plant: Option<Plant>,
    trace: bool,
}

impl Ask {
    fn writes_storage(&self) -> bool {
        matches!(self, Ask::Commit(_) | Ask::Publish { .. })
    }

    fn kind(&self) -> &'static str {
        match self {
            Ask::Hydrate => "hydrate",
            Ask::Decrypt => "decrypt",
            Ask::Encrypt(_) => "encrypt",
            Ask::Commit(_) => "commit",
            Ask::Publish { .. } => "publish",
        }
    }
}

impl Sim {
    pub fn new(seed: u64, plant: Option<Plant>) -> Self {
        let now = Instant::now();
        Self {
            world: World::new(DOCS),
            machines: (0..DOCS).map(|_| None).collect(),
            flights: vec![],
            mail: vec![],
            plans: BTreeMap::new(),
            next_plan: 0,
            held: BTreeMap::new(),
            parked: BTreeSet::new(),
            waiting: BTreeMap::new(),
            next_ticket: 0,
            next_tag: vec![0; DOCS],
            last_landed: vec![None; DOCS],
            acked: vec![BTreeSet::new(); DOCS],
            stale_handles: vec![],
            last_ingest: vec![None; DOCS],
            next_author: vec![0; DOCS],
            settled: BTreeSet::new(),
            next_token: 0,
            generation: 0,
            now,
            start: now,
            rng: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            plant,
            trace: std::env::var_os("SIM_TRACE").is_some(),
        }
    }

    /// The seed's next draw.
    pub fn draw(&mut self, below: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as usize % below.max(1)
    }

    // ── machines ────────────────────────────────────────────────────────────────────────────

    /// The Hub starts the document's machine from what the world says now.
    pub fn start(&mut self, doc: usize) {
        assert!(self.machines[doc].is_none(), "one machine per document");
        self.generation += 1;
        let model = &self.world.docs[doc];
        let mut machine = Document::new(
            Config::default(),
            Start {
                generation: self.generation,
                actor: ActorId::from(vec![0xa0, doc as u8, self.generation as u8]),
                readable: model.readable,
                known_rev: model.tree_rev,
                frontier_rev: self.world.frontier_rev(doc),
            },
        );
        #[cfg(test)]
        {
            machine.plant = self.plant;
        }
        self.machines[doc] = Some(Machine {
            doc: machine,
            handles: vec![],
            interest: 0,
            told_readable: model.readable,
        });
        self.flush(doc);
    }

    fn machine(&mut self, doc: usize) -> &mut Machine {
        if self.machines[doc].is_none() {
            self.start(doc);
        }
        self.machines[doc].as_mut().expect("just started")
    }

    /// Flush the document's machine and carry out what it asks.
    pub fn flush(&mut self, doc: usize) {
        let Some(machine) = self.machines[doc].as_mut() else {
            return;
        };
        let mut out = vec![];
        machine.doc.flush(self.now, &mut out);
        for command in out {
            if self.trace {
                eprintln!("  doc {doc}: {command:?}");
            }
            self.dispatch(doc, command);
        }
    }

    pub fn flush_all(&mut self) {
        for doc in 0..DOCS {
            self.flush(doc);
        }
    }

    fn dispatch(&mut self, doc: usize, command: Command<Out>) {
        match command {
            Command::Hydrate { call } => {
                self.check_readable(doc);
                self.fly(doc, call, Ask::Hydrate);
            }
            Command::Decrypt { call, heads, .. } => {
                self.check_readable(doc);
                // the heads may name the change of the write in flight
                let known = self.world.committed_hashes(doc);
                let in_flight = self.in_flight_changes(doc);
                assert!(
                    heads
                        .iter()
                        .all(|head| known.contains(head) || in_flight.contains(head)),
                    "heads of the world"
                );
                self.fly(doc, call, Ask::Decrypt);
            }
            Command::PrepareEncrypt { call, change } => {
                self.check_not_held(doc);
                self.fly(doc, call, Ask::Encrypt(change));
            }
            Command::Commit { call, plan } => {
                self.check_not_held(doc);
                self.fly(doc, call, Ask::Commit(plan));
            }
            Command::SettleEncrypt { token, .. } => {
                assert!(self.settled.insert(token), "a preparation is settled once");
            }
            Command::Prepared { plan, result } => self.prepared(doc, plan, result),
            Command::Reply { ticket, result } => self.replied(ticket, result),
            Command::Ingested { ticket, result } => self.ingested(ticket, result),
            Command::Notify { heads, .. } => {
                // listeners are told of committed heads only (law 1)
                let known = self.world.committed_hashes(doc);
                assert!(
                    heads.iter().all(|head| known.contains(head)),
                    "doc {doc}: a notification names an uncommitted change (law 1)"
                );
            }
            Command::PublishFrontier { call, heads, rev } => {
                self.check_publish(doc, rev);
                self.fly(doc, call, Ask::Publish { heads, rev });
            }
            Command::Cancel { call } => {
                let flight = self
                    .flights
                    .iter_mut()
                    .find(|flight| flight.doc == doc && flight.call == call)
                    .expect("a cancelled call is in flight");
                flight.cancelled = true;
            }
            Command::Evictable => {
                let machine = self.machines[doc].as_ref().expect("a machine reports");
                if machine.doc.evictable() {
                    self.evict(doc);
                }
            }
        }
    }

    fn fly(&mut self, doc: usize, call: CallId, ask: Ask) {
        self.flights.push(Flight {
            doc,
            call,
            ask,
            cancelled: false,
        });
    }

    /// The Hub stops the machine. Nothing is lost: it was evictable.
    pub fn evict(&mut self, doc: usize) {
        let machine = self.machines[doc].take().expect("a machine to stop");
        assert!(machine.doc.evictable(), "only an evictable machine stops");
        // The Hub parks the document until its keys arrive. Keys that arrived while the machine
        // was parked (their epoch move still in the mail) leave nothing to wait for.
        if matches!(machine.doc.phase, Phase::Parked { .. })
            && !self.world.docs[doc].keyless().is_empty()
        {
            self.parked.insert(doc);
        }
        self.flights.retain(|flight| flight.doc != doc);
        self.mail.retain(|mail| mail.doc() != doc);
        self.stale_handles
            .extend(machine.handles.iter().map(|handle| (doc, *handle)));
    }

    /// The Hub starts a machine for every readable document whose frontier is stale and that
    /// it has not parked. Answers whether it started any.
    pub fn hub(&mut self) -> bool {
        let mut started = false;
        for doc in 0..DOCS {
            let model = &self.world.docs[doc];
            let stale = self.world.frontier_rev(doc) < model.tree_rev;
            if self.machines[doc].is_none()
                && model.readable
                && stale
                && !self.parked.contains(&doc)
            {
                self.start(doc);
                started = true;
            }
        }
        started
    }

    // ── handles and requests ────────────────────────────────────────────────────────────────

    pub fn open(&mut self, doc: usize) -> Handle {
        let machine = self.machine(doc);
        let handle = machine.doc.opened();
        machine.handles.push(handle);
        self.flush(doc);
        handle
    }

    pub fn drop_handle(&mut self, doc: usize, pick: usize) {
        let Some(machine) = self.machines[doc].as_mut() else {
            return;
        };
        if machine.handles.is_empty() {
            return;
        }
        let at = pick % machine.handles.len();
        let handle = machine.handles.swap_remove(at);
        machine.doc.handle_dropped(handle);
        self.flush(doc);
    }

    pub fn interest(&mut self, doc: usize, acquire: bool) {
        let machine = self.machine(doc);
        if acquire {
            machine.interest += 1;
            machine.doc.interest_acquired();
        } else if machine.interest > 0 {
            machine.interest -= 1;
            machine.doc.interest_released();
        }
        self.flush(doc);
    }

    fn ticket(&mut self, waiting: Waiting) -> Ticket {
        self.next_ticket += 1;
        self.waiting.insert(self.next_ticket, waiting);
        self.next_ticket
    }

    /// The tag of the next write of `doc`, and the closure that writes it.
    fn write_op(&mut self, doc: usize, kind: &WriteKind) -> (Option<u64>, WriteOp<Out>) {
        self.next_tag[doc] += 1;
        let tag = self.next_tag[doc];
        match kind {
            WriteKind::Normal | WriteKind::Stale => (
                Some(tag),
                Box::new(move |tx| {
                    tx.put(ROOT, format!("l{tag}"), tag as i64)?;
                    Ok(vec![format!("l{tag}")])
                }),
            ),
            WriteKind::Noop => (None, Box::new(|_| Ok(vec![]))),
            WriteKind::Bad => (
                None,
                // an insert into a map is the caller's mistake
                Box::new(|tx| tx.insert(ROOT, 0, 1_i64).map(|()| vec![])),
            ),
        }
    }

    pub fn write(&mut self, doc: usize, kind: WriteKind) -> Ticket {
        let handle = match kind {
            WriteKind::Stale => {
                let stale = self.stale_handles.iter().rev().find(|(d, _)| *d == doc);
                match stale.map(|(_, handle)| *handle) {
                    Some(handle) => {
                        self.machine(doc);
                        handle
                    }
                    None => self.open(doc),
                }
            }
            _ => {
                let machine = self.machine(doc);
                match machine.handles.first() {
                    Some(handle) => *handle,
                    None => self.open(doc),
                }
            }
        };
        let (tag, op) = self.write_op(doc, &kind);
        let ticket = self.ticket(Waiting::Write { doc, tag });
        self.machine(doc).doc.write(ticket, handle, op);
        self.flush(doc);
        ticket
    }

    /// An external author sends a change, or sends the last one again if `dup`.
    pub fn ingest(&mut self, doc: usize, dup: bool) -> Ticket {
        let (change, key) = match (dup, self.last_ingest[doc].clone()) {
            (true, Some(sent)) => sent,
            _ => {
                self.next_author[doc] += 1;
                let n = self.next_author[doc];
                let sent = (self.world.author_change(doc, n), format!("e{n}"));
                self.last_ingest[doc] = Some(sent.clone());
                sent
            }
        };
        let ticket = self.ticket(Waiting::Ingest { doc, key });
        self.machine(doc).doc.ingest(ticket, vec![change]);
        self.flush(doc);
        ticket
    }

    pub fn read(&mut self, doc: usize) -> Ticket {
        let handle = match self.machine(doc).handles.first() {
            Some(handle) => *handle,
            None => self.open(doc),
        };
        let must_see = self.acked[doc].clone();
        let ticket = self.ticket(Waiting::Read { doc, must_see });
        self.machine(doc).doc.read(
            ticket,
            handle,
            Box::new(|doc| doc.keys(ROOT).collect::<Vec<_>>()),
        );
        self.flush(doc);
        ticket
    }

    // ── the world ───────────────────────────────────────────────────────────────────────────

    pub fn remote_write(&mut self, doc: usize, keyed: bool) {
        let rev = self.world.remote_write(doc, keyed);
        self.mail.push(Mail::Tree(doc, rev));
    }

    pub fn grant_keys(&mut self, doc: usize) {
        self.world.grant_keys(doc);
        self.parked.remove(&doc);
        self.mail
            .push(Mail::Epoch(doc, self.world.docs[doc].readable));
    }

    pub fn set_readable(&mut self, doc: usize, readable: bool) {
        self.world.docs[doc].readable = readable;
        self.mail.push(Mail::Epoch(doc, readable));
    }

    // ── mail ────────────────────────────────────────────────────────────────────────────────

    /// Deliver mail to the document of the `pick`th piece, its oldest first: the Hub's mailbox
    /// to a Document is FIFO, and only different documents' mail interleaves freely.
    pub fn deliver(&mut self, pick: usize) {
        if self.mail.is_empty() {
            return;
        }
        let doc = self.mail[pick % self.mail.len()].doc();
        let at = self
            .mail
            .iter()
            .position(|mail| mail.doc() == doc)
            .expect("the picked mail is there");
        match self.mail.remove(at) {
            Mail::Tree(doc, rev) => {
                if let Some(machine) = self.machines[doc].as_mut() {
                    machine.doc.tree_changed(rev);
                    self.flush(doc);
                }
            }
            Mail::Epoch(doc, readable) => {
                if let Some(machine) = self.machines[doc].as_mut() {
                    machine.told_readable = readable;
                    machine.doc.epoch_moved(readable);
                    self.flush(doc);
                }
            }
        }
    }

    /// Mail arrives twice, one copy straight after the other.
    pub fn duplicate(&mut self, pick: usize) {
        if !self.mail.is_empty() {
            let at = pick % self.mail.len();
            let mail = self.mail[at].clone();
            self.mail.insert(at + 1, mail);
        }
    }

    // ── calls ───────────────────────────────────────────────────────────────────────────────

    /// The driver finishes the call at `pick` among those in flight, failing it if `fail`.
    pub fn complete(&mut self, pick: usize, fail: bool) {
        if self.flights.is_empty() {
            return;
        }
        let at = pick % self.flights.len();
        let flight = self.flights.remove(at);
        let doc = flight.doc;
        let cancelled = flight.cancelled && self.draw(2) == 0;
        let readable = self.world.docs[doc].readable;
        let call = flight.call;
        // A cancelled call that raced the cancel reports its real result; otherwise it reports
        // the cancel. The driver also refuses to read what the node may not.
        let refused = fail || cancelled;
        let machine = self.machines[doc]
            .as_mut()
            .expect("a call's machine is there");
        match flight.ask {
            Ask::Hydrate if refused || !readable => machine.doc.hydrated(
                call,
                if cancelled {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                },
            ),
            Ask::Hydrate => {
                let found = self.world.materialize(doc);
                machine.doc.hydrated(call, Outcome::Done(found));
            }
            Ask::Decrypt if refused || !readable => machine.doc.decrypted(
                call,
                if cancelled {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                },
            ),
            Ask::Decrypt => {
                let found = self.world.materialize(doc);
                machine.doc.decrypted(call, Outcome::Done(found));
            }
            Ask::Encrypt(_) if refused => machine.doc.encrypted(
                call,
                if cancelled {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                },
            ),
            Ask::Encrypt(change) => {
                self.next_token += 1;
                let token = self.next_token;
                machine.doc.encrypted(
                    call,
                    Outcome::Done(EncryptedChange {
                        token,
                        ciphertext: change,
                    }),
                );
            }
            Ask::Commit(_) if refused => machine.doc.committed(
                call,
                if cancelled {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                },
            ),
            Ask::Commit(plan) => {
                let change = Change::from_bytes(plan.change.ciphertext).expect("a change");
                let landed = self.land(vec![(doc, change, plan.frontier)]);
                self.machines[doc]
                    .as_mut()
                    .expect("a call's machine is there")
                    .doc
                    .committed(call, Outcome::Done(landed[0]));
            }
            Ask::Publish { .. } if refused => machine.doc.published(
                call,
                if cancelled {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                },
            ),
            Ask::Publish { heads, rev } => {
                self.world.publish(doc, heads, rev);
                machine.doc.published(call, Outcome::Done(()));
            }
        }
        self.flush(doc);
    }

    /// Commit changes of several documents in one transaction of the world, checking the order
    /// of the writes (law 2) as they land.
    fn land(&mut self, batch: Vec<(usize, Change, Option<FrontierRow>)>) -> Vec<Landed> {
        for (doc, change, _) in &batch {
            // an external author's change is built on nothing of ours, and keeps no order
            let Some(tag) = self.world.new_tag(*doc, change) else {
                continue;
            };
            if let Some((last, last_hash)) = self.last_landed[*doc] {
                assert!(
                    tag > last,
                    "doc {doc}: write {tag} landed after write {last}: submission order broken"
                );
                assert!(
                    change.deps().contains(&last_hash),
                    "doc {doc}: write {tag} was not built on write {last}"
                );
            }
            self.last_landed[*doc] = Some((tag, change.hash()));
        }
        let docs: Vec<usize> = batch.iter().map(|(doc, ..)| *doc).collect();
        let landed = self.world.commit(batch);
        for (doc, landed) in docs.into_iter().zip(&landed) {
            self.mail.push(Mail::Tree(doc, landed.rev));
        }
        landed
    }

    // ── plans ───────────────────────────────────────────────────────────────────────────────

    pub fn plan_prepare(&mut self, mask: usize) {
        self.plan_prepare_kind(mask, None);
    }

    /// Prepare a plan on the documents of `mask`: each by an ingest or a write as drawn, or all
    /// of one kind if `ingest` says.
    pub fn plan_prepare_kind(&mut self, mask: usize, ingest: Option<bool>) {
        let docs: Vec<usize> = (0..DOCS).filter(|doc| mask >> doc & 1 == 1).collect();
        if docs.is_empty() {
            return;
        }
        self.next_plan += 1;
        let plan = self.next_plan;
        self.plans.insert(
            plan,
            PlanState {
                docs: docs.clone(),
                prepared: BTreeMap::new(),
                failed: false,
            },
        );
        for doc in docs {
            if !self.plans.contains_key(&plan) {
                // a refusal already ended the plan
                break;
            }
            if ingest.unwrap_or_else(|| self.draw(3) == 0) {
                self.next_author[doc] += 1;
                let change = self.world.author_change(doc, self.next_author[doc]);
                self.machine(doc).doc.prepare_ingest(plan, vec![change]);
            } else {
                let (_, op) = self.write_op(doc, &WriteKind::Normal);
                self.machine(doc).doc.prepare(plan, op);
            }
            self.flush(doc);
        }
    }

    fn prepared(&mut self, doc: usize, plan: PlanId, result: Result<PlanPrepared<Out>, Rejected>) {
        if result.is_ok() {
            // a refusal starts nothing
            self.check_not_held(doc);
        }
        let state = self
            .plans
            .get_mut(&plan)
            .expect("a plan the coordinator forgot was prepared");
        match result {
            Ok(prepared) => {
                state.prepared.insert(doc, prepared);
                self.held.insert(doc, plan);
            }
            Err(_) => state.failed = true,
        }
        if state.failed {
            self.plan_end(plan, None);
        }
    }

    /// The plans every document of which has been prepared.
    pub fn finishable(&self) -> Vec<PlanId> {
        self.plans
            .iter()
            .filter(|(_, state)| !state.failed && state.prepared.len() == state.docs.len())
            .map(|(plan, _)| *plan)
            .collect()
    }

    pub fn plan_finish(&mut self, pick: usize, commit: bool) {
        let ready = self.finishable();
        if ready.is_empty() {
            return;
        }
        let plan = ready[pick % ready.len()];
        let batch = commit.then(|| {
            let state = &self.plans[&plan];
            state
                .docs
                .iter()
                .map(|doc| {
                    let prepared = &state.prepared[doc];
                    let bytes = prepared.change.clone().expect("a plan writes");
                    (
                        *doc,
                        Change::from_bytes(bytes).expect("a change"),
                        prepared.frontier.clone(),
                    )
                })
                .collect()
        });
        let landed = batch.map(|batch| self.land(batch));
        self.plan_end(plan, landed);
    }

    /// The coordinator resolves `plan`: apply to every document it holds if it committed (with
    /// what each commit reported), abort otherwise.
    fn plan_end(&mut self, plan: PlanId, landed: Option<Vec<Landed>>) {
        let state = self.plans.remove(&plan).expect("a plan in flight");
        for (at, doc) in state.docs.iter().copied().enumerate() {
            if self.held.get(&doc) == Some(&plan) {
                self.held.remove(&doc);
            }
            let Some(machine) = self.machines[doc].as_mut() else {
                continue;
            };
            match &landed {
                Some(landed) => machine.doc.apply(plan, landed[at]),
                None => machine.doc.abort(plan),
            }
            self.flush(doc);
        }
    }

    // ── faults and time ─────────────────────────────────────────────────────────────────────

    /// The process dies. Bit `i` of `landed` says whether the `i`th commit in flight had
    /// landed. Every machine is rebuilt from the world, as the Hub would.
    pub fn crash(&mut self, landed: usize) {
        // A document's storage writes ran in order, so those that had landed are a prefix.
        let mut batch = vec![];
        let mut stopped = BTreeSet::new();
        let mut publishes = vec![];
        for (bit, flight) in self
            .flights
            .iter()
            .filter(|flight| flight.ask.writes_storage())
            .enumerate()
        {
            if stopped.contains(&flight.doc) {
                continue;
            }
            if landed >> (bit % 8) & 1 == 0 {
                stopped.insert(flight.doc);
                continue;
            }
            match &flight.ask {
                Ask::Commit(plan) => {
                    let change =
                        Change::from_bytes(plan.change.ciphertext.clone()).expect("a change");
                    batch.push((flight.doc, change, plan.frontier.clone()));
                }
                Ask::Publish { heads, rev } => publishes.push((flight.doc, heads.clone(), *rev)),
                _ => unreachable!("filtered"),
            }
        }
        for (doc, heads, rev) in publishes {
            self.world.publish(doc, heads, rev);
        }
        for item in batch {
            self.land(vec![item]);
        }
        for (doc, machine) in self.machines.iter_mut().enumerate() {
            if let Some(machine) = machine.take() {
                self.stale_handles
                    .extend(machine.handles.iter().map(|handle| (doc, *handle)));
            }
        }
        self.flights.clear();
        self.mail.clear();
        self.plans.clear();
        self.held.clear();
        self.parked.clear();
        self.waiting.clear();
        self.hub();
    }

    pub fn advance(&mut self, by: Duration) {
        self.now += by;
        self.flush_all();
    }

    /// Time moves to the next deadline of any machine.
    pub fn tick(&mut self) {
        let due = self
            .machines
            .iter()
            .flatten()
            .filter_map(|machine| machine.doc.next_due())
            .min();
        if let Some(due) = due {
            self.now = self.now.max(due);
            self.flush_all();
        }
    }

    /// The next deadline that is a backoff, which a drain must wait out.
    fn next_retry(&self) -> Option<Instant> {
        self.machines
            .iter()
            .flatten()
            .filter_map(|machine| machine.doc.retry.until)
            .min()
    }

    /// Drain: reliable calls, all mail delivered, plans finished, backoffs waited out, until
    /// nothing is left to do; then check the world.
    pub fn quiesce(&mut self) {
        for round in 0..10_000 {
            if self.trace && round > 9_900 {
                eprintln!("drain {round}: {}", self.snapshot());
            }
            if !self.mail.is_empty() {
                self.deliver(0);
            } else if !self.finishable().is_empty() {
                self.plan_finish(0, true);
            } else if !self.flights.is_empty() {
                self.complete(0, false);
            } else if self.hub() {
            } else if let Some(due) = self.next_retry() {
                self.now = self.now.max(due);
                self.flush_all();
            } else {
                self.check_quiet();
                return;
            }
            self.check();
        }
        panic!("the drain does not settle");
    }

    /// What the driver observes of the machines, as text: a trace line for the determinism test.
    pub fn snapshot(&self) -> String {
        let machines: Vec<String> = self
            .machines
            .iter()
            .map(|machine| match machine {
                None => "-".to_string(),
                Some(machine) => machine.doc.describe(),
            })
            .collect();
        format!(
            "machines={machines:?} flights={} mail={} plans={} now={:?} frontiers={:?} trees={:?}",
            self.flights.len(),
            self.mail.len(),
            self.plans.len(),
            self.now - self.start,
            (0..DOCS)
                .map(|doc| self.world.frontier_rev(doc))
                .collect::<Vec<_>>(),
            (0..DOCS)
                .map(|doc| self.world.tree_rev(doc))
                .collect::<Vec<_>>(),
        )
    }
}
