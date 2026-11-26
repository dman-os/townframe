//! The Write coordinator: the one place a write that spans several documents is run, as a pure
//! state machine. A caller hands it changes for documents D1..Dn (each one a closure to run, or
//! changes an external author already built) and gets back either every document's change,
//! committed, or none of them. The [Document machine](crate::document) is what holds one
//! document; this machine is what makes N of them move together.
//!
//! # What it is, and what it is not
//!
//! Like the Document machine it is sans-IO: no awaits, no clock, no IO, no channels, no
//! locks. Every input is a method; everything it wants done leaves as a [`Command`] from the next
//! [`Coordinator::flush`]. (It reads no clock, because nothing in it waits for time: a plan waits
//! for other plans and for answers, never for a deadline. So `flush` takes no `now`, and there
//! is no `next_due`.) A driver owns it, forwards the commands to the Document machines, the
//! Keyhive boundary and storage, and reports each answer back through the input that names it.
//! The Keyhive boundary, the storage and the Hub are other pieces; their interface here is the
//! commands and inputs below, kept abstract.
//!
//! A single-document write does not come through here. The Document machine runs that itself,
//! as the same protocol with n = 1. This machine is for n >= 1 when the caller wants one commit
//! across documents, and it is the back end for every multi-document shape: a caller that locks
//! several documents on its own side (a `BigDocHandle`-level lock, not in any machine) and mutates
//! their live docs submits a plan of closures; a client that holds its own automerge document
//! and sends change bytes for several documents submits a plan of ingests. Both go down the same
//! path with the same laws.
//!
//! # The protocol, in order
//!
//! 1. **Admission.** [`Coordinator::submit`] queues a plan. A plan starts only when its
//!    documents are disjoint from those of every plan in flight, and plans start in the order they
//!    were submitted: the front of the queue waits, and nothing behind it overtakes it. Because
//!    this is the only place plans start, two plans that overlap can never wait on each other in a
//!    cycle. (A [`Config::max_plans`] window bounds how many are in flight, so the mailboxes
//!    around it have a known size.)
//! 2. **Prepare.** [`Command::Prepare`] goes to each Document, with the work. The Document runs
//!    it on its live document, answers with the change's bytes and the frontier row the commit
//!    must carry, and holds itself until it is told the plan's fate. A Document that cannot
//!    prepare (it is parked on keys that have not arrived, or the node may not read it) refuses.
//! 3. **Encrypt.** [`Command::PrepareEncrypt`] goes to the Keyhive boundary for each document that
//!    changed. Encrypting changes no memory, and a prepared encryption that is not committed must
//!    be dropped: [`Command::SettleEncrypt`] says which.
//! 4. **Commit.** One [`Command::Commit`] carries the whole plan: for each document its encrypted
//!    change and its frontier row (with `if_prior_rev`). The driver runs it as one transaction,
//!    so the plan's rows land together or not at all, and answers with what each document's tree
//!    reported ([`Landed`]).
//! 5. **Release.** On a commit, each changed document gets [`Command::Apply`] with its `Landed`;
//!    on any failure each gets [`Command::Abort`]. A document that changed nothing is always
//!    aborted, which costs it nothing. [`Command::Reply`] answers the caller.
//!
//! # The states of a plan
//!
//! `Queued`, then `Preparing` (waiting for every Document's answer), `Encrypting` (waiting for
//! every encryption), `Committing` (waiting for the one commit), then it is gone: committed and
//! applied, or aborted. There is no state that waits for the Documents after the release: the
//! commands to one Document arrive in the order they were issued, so an abort is always seen
//! before the next plan's prepare for that Document, and the machine never needs an answer.
//!
//! # Failure
//!
//! A refusal to prepare, a failed encryption or a failed commit aborts the whole plan and the
//! caller gets the reason ([`PlanError`]): the first refusal names its document. An abort is
//! clean: every Document that was sent a prepare and did not refuse gets exactly one
//! release, every prepared encryption is settled as not applied (including one that is still
//! in flight when the plan aborts; its answer, when it comes, is settled the same way), and
//! calls still out are cancelled. A commit that failed may have run all the same: the Documents
//! that are aborted after a commit was sent take a fresh actor (the Document machine does this
//! on every abort of a change), so they cannot collide with it.
//!
//! # Laws
//!
//! 1. **Documents in flight are disjoint.** No document is in two plans at once.
//! 2. **All or nothing.** A plan's changes are committed together by one commit, or none of
//!    them is: no commit carries a part of a plan, and a plan that fails is never partly
//!    committed.
//! 3. **Every prepared Document is released exactly once.** A Document that was sent a prepare
//!    and did not refuse gets exactly one of apply or abort, and never both. A Document that
//!    refused gets none, because it holds nothing.
//! 4. **Plans start in order.** A queued plan starts only after every plan submitted before it
//!    has started.
//! 5. **A refusal aborts cleanly.** A refused prepare aborts the whole plan: nothing is
//!    committed, every other Document is released, and the caller is told which document
//!    refused.
//! 6. **One answer, one settlement.** Every plan is answered exactly once, and every prepared
//!    encryption is settled exactly once.
//!
//! # Where its neighbours come in
//!
//! The Document machine's inputs [`prepare_work`](crate::document::Document::prepare_work),
//! `apply` and `abort` are the ends of this machine's commands, and its `Prepared` command is
//! this machine's [`Coordinator::prepared`]. The Keyhive boundary answers
//! [`Coordinator::encrypted`]; storage answers [`Coordinator::committed`]. The Hub does not
//! come into it: a plan names the documents it needs, and the Hub's job of starting their
//! Document machines is done before the plan is submitted.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

use crate::document::CallId;
use crate::document::EncryptedChange;
use crate::document::FrontierRow;
use crate::document::Landed;
use crate::document::Outcome;
use crate::document::PlanId;
use crate::document::PlanPrepared;
use crate::document::Rejected;
use crate::document::Ticket;
use crate::document::Work;

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

/// A document, as the Hub names it.
pub type DocId = u64;

#[derive(Debug, Clone)]
pub struct Config {
    /// The most plans in flight at once.
    pub max_plans: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { max_plans: 8 }
    }
}

/// Why a plan did not commit. Nothing was changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// A document could not prepare its part.
    Refused { doc: DocId, why: Rejected },
    /// Encrypting or committing failed.
    Failed,
}

/// A plan's answer: each document's output, in the order the plan named its documents, or why
/// nothing was committed.
pub type Answer<O> = Result<Vec<(DocId, Option<O>)>, PlanError>;

/// One document's rows of a plan's commit, by kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocRows {
    pub doc: DocId,
    pub change: EncryptedChange,
    pub frontier: Option<FrontierRow>,
}

#[derive(Debug)]
pub enum Command<O> {
    /// Tell a Document machine to prepare `work` for `plan`.
    Prepare {
        doc: DocId,
        plan: PlanId,
        work: Work<O>,
    },
    /// Ask the Keyhive boundary to encrypt a document's change.
    PrepareEncrypt {
        call: CallId,
        plan: PlanId,
        doc: DocId,
        change: Vec<u8>,
    },
    /// Commit every row of the plan in one transaction.
    Commit {
        call: CallId,
        plan: PlanId,
        rows: Vec<DocRows>,
    },
    /// The preparation `token` was committed (`applied`) or will not be.
    SettleEncrypt { token: u64, applied: bool },
    /// The plan committed: the Document applies it, with what its tree reported.
    Apply {
        doc: DocId,
        plan: PlanId,
        landed: Landed,
    },
    /// The plan did not commit (or this document changed nothing): release the Document.
    Abort { doc: DocId, plan: PlanId },
    /// Abort a call in flight. Its outcome is still reported.
    Cancel { call: CallId },
    /// The plan's answer: each document's output (`None` for an ingest and for a document that
    /// changed nothing), in the order the plan named its documents.
    Reply { ticket: Ticket, result: Answer<O> },
}

/// A plan the caller submitted, waiting to start.
struct Queued<O> {
    ticket: Ticket,
    plan: PlanId,
    writes: Vec<(DocId, Work<O>)>,
}

/// What a Document answered to its prepare.
struct Prepared<O> {
    change: Option<Vec<u8>>,
    frontier: Option<FrontierRow>,
    out: Option<O>,
}

enum Stage<O> {
    /// Waiting for the Documents named in `waiting`.
    Preparing {
        waiting: BTreeSet<DocId>,
        prepared: BTreeMap<DocId, Prepared<O>>,
    },
    /// Waiting for the encryptions in `calls`.
    Encrypting {
        calls: BTreeMap<CallId, DocId>,
        prepared: BTreeMap<DocId, Prepared<O>>,
        encrypted: BTreeMap<DocId, EncryptedChange>,
    },
    /// Waiting for the commit.
    Committing {
        call: CallId,
        prepared: BTreeMap<DocId, Prepared<O>>,
        encrypted: BTreeMap<DocId, EncryptedChange>,
    },
}

struct Running<O> {
    ticket: Ticket,
    /// The plan's documents in the order it named them.
    docs: Vec<DocId>,
    stage: Stage<O>,
}

/// Bugs planted on purpose, to show the simulation catches them (see `tests`).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    /// A plan starts although a document of it is in another plan (law 1).
    AdmitOverlapping,
    /// A plan behind one that cannot start is started (law 4).
    Overtake,
    /// A failed encryption still commits the plan's other rows (law 2).
    CommitDespiteFailure,
    /// An aborted plan leaves its prepared Documents held (laws 3 and 5).
    ForgetToAbort,
    /// An aborted plan forgets to settle its encryptions (law 6).
    ForgetToSettle,
}

/// The machine.
pub struct Coordinator<O> {
    config: Config,
    next_plan: PlanId,
    next_call: CallId,
    queue: VecDeque<Queued<O>>,
    running: BTreeMap<PlanId, Running<O>>,
    /// The documents of the plans in flight.
    busy: BTreeSet<DocId>,
    /// Calls of plans that aborted: when one answers with an encryption, it is settled as not
    /// applied.
    orphans: BTreeSet<CallId>,
    pending: Vec<Command<O>>,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl<O> Coordinator<O> {
    pub fn new(config: Config) -> Self {
        assert!(config.max_plans >= 1, "at least one plan in flight");
        Self {
            config,
            next_plan: 0,
            next_call: 0,
            queue: VecDeque::new(),
            running: BTreeMap::new(),
            busy: BTreeSet::new(),
            orphans: BTreeSet::new(),
            pending: Vec::new(),
            #[cfg(test)]
            plant: None,
        }
    }

    /// Queue a plan: `writes` names each document once, with its work. Answers the plan's id,
    /// which is how the Documents know it. The answer to the caller is [`Command::Reply`].
    pub fn submit(&mut self, ticket: Ticket, writes: Vec<(DocId, Work<O>)>) -> PlanId {
        assert!(!writes.is_empty(), "a plan writes something");
        let docs: BTreeSet<_> = writes.iter().map(|(doc, _)| doc).collect();
        assert_eq!(docs.len(), writes.len(), "a plan names a document once");
        self.next_plan += 1;
        let plan = self.next_plan;
        self.queue.push_back(Queued {
            ticket,
            plan,
            writes,
        });
        plan
    }

    /// A Document's answer to its prepare ([`Command::Prepared`] of the Document machine). An
    /// answer for a plan that is no longer waiting for it changes nothing.
    pub fn prepared(
        &mut self,
        doc: DocId,
        plan: PlanId,
        result: Result<PlanPrepared<O>, Rejected>,
    ) {
        let Some(running) = self.running.get_mut(&plan) else {
            return;
        };
        let Stage::Preparing { waiting, prepared } = &mut running.stage else {
            return;
        };
        if !waiting.remove(&doc) {
            return;
        }
        match result {
            Ok(answer) => {
                prepared.insert(
                    doc,
                    Prepared {
                        change: answer.change,
                        frontier: answer.frontier,
                        out: answer.out,
                    },
                );
                if waiting.is_empty() {
                    self.encrypt(plan);
                }
            }
            Err(why) => self.abort(plan, PlanError::Refused { doc, why }, Some(doc)),
        }
    }

    /// The Keyhive boundary's answer to a [`Command::PrepareEncrypt`].
    pub fn encrypted(&mut self, call: CallId, outcome: Outcome<EncryptedChange>) {
        if self.orphans.remove(&call) {
            // the plan is gone: a preparation made for it is dropped
            if let Outcome::Done(enc) = outcome {
                self.settle(enc.token, false);
            }
            return;
        }
        let Some((plan, doc)) =
            self.running
                .iter()
                .find_map(|(plan, running)| match &running.stage {
                    Stage::Encrypting { calls, .. } => calls.get(&call).map(|doc| (*plan, *doc)),
                    _ => None,
                })
        else {
            return;
        };
        let running = self.running.get_mut(&plan).expect("found above");
        let Stage::Encrypting {
            calls, encrypted, ..
        } = &mut running.stage
        else {
            unreachable!("found above");
        };
        calls.remove(&call);
        let all_in = calls.is_empty();
        match outcome {
            Outcome::Done(enc) => {
                encrypted.insert(doc, enc);
                if all_in {
                    self.commit(plan);
                }
            }
            Outcome::Failed | Outcome::Cancelled => {
                #[cfg(test)]
                if self.plant == Some(Plant::CommitDespiteFailure) {
                    if all_in && !encrypted.is_empty() {
                        self.commit(plan);
                    }
                    return;
                }
                self.abort(plan, PlanError::Failed, None);
            }
        }
    }

    /// The storage's answer to a [`Command::Commit`]: what each document's tree reported.
    pub fn committed(&mut self, call: CallId, outcome: Outcome<Vec<(DocId, Landed)>>) {
        let Some(plan) = self
            .running
            .iter()
            .find_map(|(plan, running)| match &running.stage {
                Stage::Committing { call: c, .. } if *c == call => Some(*plan),
                _ => None,
            })
        else {
            return;
        };
        match outcome {
            Outcome::Done(landed) => self.release(plan, landed),
            Outcome::Failed | Outcome::Cancelled => self.abort(plan, PlanError::Failed, None),
        }
    }

    /// Start the plans that can start, and take the commands.
    pub fn flush(&mut self, out: &mut Vec<Command<O>>) {
        self.admit();
        out.append(&mut self.pending);
    }

    /// Whether nothing is queued or in flight, for a driver that is draining.
    pub fn is_idle(&self) -> bool {
        self.queue.is_empty() && self.running.is_empty() && self.orphans.is_empty()
    }

    fn fresh_call(&mut self) -> CallId {
        self.next_call += 1;
        self.next_call
    }

    fn settle(&mut self, token: u64, applied: bool) {
        self.pending.push(Command::SettleEncrypt { token, applied });
    }

    // ── the stages ──────────────────────────────────────────────────────────────────────────

    /// Start plans from the front of the queue while their documents are free: in order, and
    /// within the window.
    fn admit(&mut self) {
        loop {
            #[cfg(test)]
            let overlap_ok = self.plant == Some(Plant::AdmitOverlapping);
            #[cfg(not(test))]
            let overlap_ok = false;
            #[cfg(test)]
            let overtake = self.plant == Some(Plant::Overtake);
            #[cfg(not(test))]
            let overtake = false;
            if self.running.len() >= self.config.max_plans {
                return;
            }
            let free = |queued: &Queued<O>, busy: &BTreeSet<DocId>| {
                overlap_ok || queued.writes.iter().all(|(doc, _)| !busy.contains(doc))
            };
            let at = if self
                .queue
                .front()
                .is_some_and(|front| free(front, &self.busy))
            {
                Some(0)
            } else if overtake {
                self.queue
                    .iter()
                    .position(|queued| free(queued, &self.busy))
            } else {
                None
            };
            let Some(at) = at else {
                return;
            };
            let queued = self.queue.remove(at).expect("found above");
            self.start(queued);
        }
    }

    fn start(&mut self, queued: Queued<O>) {
        let Queued {
            ticket,
            plan,
            writes,
        } = queued;
        let docs: Vec<DocId> = writes.iter().map(|(doc, _)| *doc).collect();
        self.busy.extend(docs.iter().copied());
        for (doc, work) in writes {
            self.pending.push(Command::Prepare { doc, plan, work });
        }
        self.running.insert(
            plan,
            Running {
                ticket,
                stage: Stage::Preparing {
                    waiting: docs.iter().copied().collect(),
                    prepared: BTreeMap::new(),
                },
                docs,
            },
        );
    }

    /// Every Document has prepared: encrypt the changes, or finish at once if none changed.
    fn encrypt(&mut self, plan: PlanId) {
        let running = self.running.get_mut(&plan).expect("a plan in flight");
        let Stage::Preparing { prepared, .. } = &mut running.stage else {
            unreachable!("encrypting follows preparing");
        };
        let prepared = std::mem::take(prepared);
        let changed: Vec<(DocId, Vec<u8>)> = running
            .docs
            .iter()
            .filter_map(|doc| Some((*doc, prepared[doc].change.clone()?)))
            .collect();
        if changed.is_empty() {
            running.stage = Stage::Encrypting {
                calls: BTreeMap::new(),
                prepared,
                encrypted: BTreeMap::new(),
            };
            self.release(plan, vec![]);
            return;
        }
        let mut calls = BTreeMap::new();
        for (doc, change) in changed {
            let call = self.fresh_call();
            calls.insert(call, doc);
            self.pending.push(Command::PrepareEncrypt {
                call,
                plan,
                doc,
                change,
            });
        }
        let running = self.running.get_mut(&plan).expect("a plan in flight");
        running.stage = Stage::Encrypting {
            calls,
            prepared,
            encrypted: BTreeMap::new(),
        };
    }

    /// Every change is encrypted: one commit for the whole plan.
    fn commit(&mut self, plan: PlanId) {
        let call = self.fresh_call();
        let running = self.running.get_mut(&plan).expect("a plan in flight");
        let Stage::Encrypting {
            prepared,
            encrypted,
            ..
        } = &mut running.stage
        else {
            unreachable!("committing follows encrypting");
        };
        let (prepared, encrypted) = (std::mem::take(prepared), std::mem::take(encrypted));
        let rows = running
            .docs
            .iter()
            .filter_map(|doc| {
                Some(DocRows {
                    doc: *doc,
                    change: encrypted.get(doc)?.clone(),
                    frontier: prepared[doc].frontier.clone(),
                })
            })
            .collect();
        running.stage = Stage::Committing {
            call,
            prepared,
            encrypted,
        };
        self.pending.push(Command::Commit { call, plan, rows });
    }

    /// The plan committed (or changed nothing, with `landed` empty): apply to the documents
    /// that changed, release the ones that did not, settle the encryptions, answer the caller.
    fn release(&mut self, plan: PlanId, landed: Vec<(DocId, Landed)>) {
        let running = self.running.remove(&plan).expect("a plan in flight");
        let (prepared, encrypted) = match running.stage {
            Stage::Committing {
                prepared,
                encrypted,
                ..
            } => (prepared, encrypted),
            Stage::Encrypting {
                prepared,
                encrypted,
                ..
            } => (prepared, encrypted),
            Stage::Preparing { .. } => unreachable!("a plan is released after it has prepared"),
        };
        assert_eq!(
            landed.len(),
            encrypted.len(),
            "a commit reports each of its rows"
        );
        let mut landed: BTreeMap<DocId, Landed> = landed.into_iter().collect();
        for doc in &running.docs {
            self.busy.remove(doc);
            match landed.remove(doc) {
                Some(landed) => self.pending.push(Command::Apply {
                    doc: *doc,
                    plan,
                    landed,
                }),
                None => self.pending.push(Command::Abort { doc: *doc, plan }),
            }
        }
        for enc in encrypted.values() {
            self.settle(enc.token, true);
        }
        let mut prepared = prepared;
        let outs = running
            .docs
            .iter()
            .map(|doc| (*doc, prepared.get_mut(doc).and_then(|p| p.out.take())))
            .collect();
        self.pending.push(Command::Reply {
            ticket: running.ticket,
            result: Ok(outs),
        });
    }

    /// The plan did not commit: release every Document that holds something, settle every
    /// encryption, cancel the calls still out and answer the caller. `refused` is the document
    /// that refused, which holds nothing.
    fn abort(&mut self, plan: PlanId, error: PlanError, refused: Option<DocId>) {
        let running = self.running.remove(&plan).expect("a plan in flight");
        // (a commit that failed has reported, and an encryption that failed was removed)
        let (calls, encrypted) = match running.stage {
            Stage::Preparing { .. } => (BTreeMap::new(), BTreeMap::new()),
            Stage::Encrypting {
                calls, encrypted, ..
            } => (calls, encrypted),
            Stage::Committing { encrypted, .. } => (BTreeMap::new(), encrypted),
        };
        #[cfg(test)]
        let leave_held = self.plant == Some(Plant::ForgetToAbort);
        #[cfg(not(test))]
        let leave_held = false;
        #[cfg(test)]
        let skip_settle = self.plant == Some(Plant::ForgetToSettle);
        #[cfg(not(test))]
        let skip_settle = false;
        for doc in &running.docs {
            self.busy.remove(doc);
            if Some(*doc) != refused && !leave_held {
                self.pending.push(Command::Abort { doc: *doc, plan });
            }
        }
        if !skip_settle {
            for enc in encrypted.values() {
                self.settle(enc.token, false);
            }
        }
        for call in calls.into_keys() {
            self.orphans.insert(call);
            self.pending.push(Command::Cancel { call });
        }
        self.pending.push(Command::Reply {
            ticket: running.ticket,
            result: Err(error),
        });
    }
}
