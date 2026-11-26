//! The inputs of [`Document`]: each records what happened and decides what it can. The commands
//! that need the clock, and the starting of work, are [`Document::flush`]'s.

use super::*;

impl<O> Document<O> {
    // ── handles and lifetime ────────────────────────────────────────────────────────────────

    pub fn opened(&mut self) -> Handle {
        let id = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(id);
        Handle {
            generation: self.generation,
            id,
        }
    }

    /// A handle of another generation is a handle of a machine that is gone: nothing to do.
    pub fn handle_dropped(&mut self, handle: Handle) {
        if handle.generation != self.generation {
            return;
        }
        assert!(self.handles.remove(&handle.id), "a handle is dropped once");
    }

    pub fn interest_acquired(&mut self) {
        self.interest += 1;
    }

    pub fn interest_released(&mut self) {
        self.interest = self
            .interest
            .checked_sub(1)
            .expect("an interest is released once");
    }

    fn is_open(&self, handle: Handle) -> bool {
        handle.generation == self.generation && self.handles.contains(&handle.id)
    }

    // ── requests of handles ─────────────────────────────────────────────────────────────────

    /// Queue a write. Its closure runs when the write reaches the front of the queue and the
    /// document is live and idle; the reply comes after the change is committed and applied.
    pub fn write(&mut self, ticket: Ticket, handle: Handle, op: WriteOp<O>) {
        if !self.is_open(handle) {
            self.reply(ticket, Err(Rejected::StaleHandle));
        } else {
            self.queue.push_back(Queued::Write { ticket, op });
        }
    }

    /// Queue a read. It is served as soon as the document is live, from committed state.
    pub fn read(&mut self, ticket: Ticket, handle: Handle, op: ReadOp<O>) {
        if !self.is_open(handle) {
            self.reply(ticket, Err(Rejected::StaleHandle));
        } else {
            self.reads.push_back((ticket, op));
        }
    }

    pub(super) fn reply(&mut self, ticket: Ticket, result: Result<O, Rejected>) {
        self.pending.push(Command::Reply { ticket, result });
    }

    // ── the Hub ─────────────────────────────────────────────────────────────────────────────

    /// The change feed reports the tree at `rev`. Revisions only move forward: an older or
    /// repeated one changes nothing.
    pub fn tree_changed(&mut self, rev: Rev) {
        if rev <= self.known_rev {
            return;
        }
        self.known_rev = rev;
        if let Frontier::Current = self.frontier {
            self.frontier = Frontier::Stale;
        }
        if let Phase::Live(live) = &mut self.phase {
            match live.remote {
                Remote::UpToDate => live.remote = Remote::Behind,
                // new content may unblock what was blocked, or be blocked itself: ask again
                Remote::Blocked { .. } => live.remote = Remote::Behind,
                Remote::Behind | Remote::Applying { .. } => {}
            }
        }
    }

    /// The keyhive epoch for this document moved, and the document is `readable` now.
    /// Content that was missing keys is tried again; losing access cancels what is being read
    /// and drops the live document once nothing is in flight.
    pub fn epoch_moved(&mut self, readable: bool) {
        self.readable = readable;
        if !readable {
            self.cancel_reading();
            return;
        }
        self.retry = Retry::default();
        match &mut self.phase {
            Phase::Parked { .. } => self.phase = Phase::Cold,
            Phase::Live(live) => {
                if let Remote::Blocked { .. } = live.remote {
                    live.remote = Remote::Behind;
                }
            }
            Phase::Cold | Phase::Hydrating { .. } => {}
        }
    }

    fn cancel_reading(&mut self) {
        let call = match &mut self.phase {
            Phase::Hydrating { call } => {
                let call = *call;
                self.phase = Phase::Cold;
                Some(call)
            }
            Phase::Live(live) => match live.remote {
                Remote::Applying { call } => {
                    live.remote = Remote::Behind;
                    Some(call)
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(call) = call {
            self.stopping.insert(call);
            self.pending.push(Command::Cancel { call });
        }
    }

    // ── the Write coordinator ───────────────────────────────────────────────────────────────

    /// Queue the preparation of a change for `plan`. It is answered with [`Command::Prepared`]
    /// when it reaches the front of the queue and the document is idle, and then the document is
    /// held for the plan.
    pub fn prepare(&mut self, plan: PlanId, op: WriteOp<O>) {
        self.queue.push_back(Queued::Plan { plan, op });
    }

    /// The plan committed: apply its change and release the document.
    pub fn apply(&mut self, plan: PlanId, landed: Landed) {
        let Writes::HeldForPlan {
            plan: held,
            change,
            frontier,
        } = std::mem::replace(&mut self.writes, Writes::Idle)
        else {
            panic!("apply for plan {plan}, which holds no document");
        };
        assert_eq!(held, plan, "apply names the plan that holds the document");
        self.land(change, frontier.is_some(), landed);
    }

    /// The plan did not commit: release the document, applying nothing. A plan still waiting
    /// in the queue is forgotten.
    pub fn abort(&mut self, plan: PlanId) {
        match &self.writes {
            Writes::HeldForPlan { plan: held, .. } if *held == plan => {
                self.writes = Writes::Idle;
            }
            _ => self
                .queue
                .retain(|queued| !matches!(queued, Queued::Plan { plan: p, .. } if *p == plan)),
        }
    }

    // ── outcomes ────────────────────────────────────────────────────────────────────────────

    pub fn hydrated(&mut self, call: CallId, outcome: Outcome<Materialized>) {
        if self.stopping.remove(&call) {
            return;
        }
        if !matches!(self.phase, Phase::Hydrating { call: c } if c == call) {
            return;
        }
        match outcome {
            Outcome::Done(found) if !found.blocked.is_empty() => {
                self.retry.failures = 0;
                self.phase = Phase::Parked {
                    missing: found.blocked,
                };
            }
            Outcome::Done(found) => {
                self.retry.failures = 0;
                let mut doc = Automerge::new().with_actor(self.actor.clone());
                doc.apply_changes(found.changes)
                    .expect("committed changes apply");
                self.known_rev = self.known_rev.max(found.rev);
                self.frontier = if self.frontier_rev >= found.rev {
                    Frontier::Current
                } else {
                    Frontier::Stale
                };
                let mut live = Live {
                    doc,
                    applied_rev: found.rev,
                    remote: Remote::UpToDate,
                };
                settle(&mut live, self.known_rev);
                self.phase = Phase::Live(Box::new(live));
            }
            Outcome::Failed => {
                self.phase = Phase::Cold;
                self.failed();
            }
            Outcome::Cancelled => self.phase = Phase::Cold,
        }
    }

    pub fn decrypted(&mut self, call: CallId, outcome: Outcome<Materialized>) {
        if self.stopping.remove(&call) {
            return;
        }
        let Phase::Live(live) = &mut self.phase else {
            return;
        };
        if !matches!(live.remote, Remote::Applying { call: c } if c == call) {
            return;
        }
        match outcome {
            Outcome::Done(found) => {
                assert!(
                    found.rev >= live.applied_rev,
                    "reads of the tree never go back"
                );
                self.retry.failures = 0;
                let before = live.doc.get_heads();
                live.doc
                    .apply_changes(found.changes)
                    .expect("committed changes apply");
                live.applied_rev = found.rev;
                self.known_rev = self.known_rev.max(found.rev);
                live.remote = if !found.blocked.is_empty() {
                    Remote::Blocked {
                        refs: found.blocked,
                    }
                } else if self.known_rev > live.applied_rev {
                    Remote::Behind
                } else {
                    Remote::UpToDate
                };
                let heads = live.doc.get_heads();
                if heads != before || live.applied_rev > self.frontier_rev {
                    self.frontier = Frontier::Stale;
                }
                if heads != before {
                    self.pending.push(Command::Notify {
                        origin: Origin::Remote,
                        heads,
                    });
                }
            }
            Outcome::Failed => {
                live.remote = Remote::Behind;
                self.failed();
            }
            Outcome::Cancelled => live.remote = Remote::Behind,
        }
    }

    pub fn encrypted(&mut self, call: CallId, outcome: Outcome<EncryptedChange>) {
        if !matches!(self.writes, Writes::Encrypting { call: c, .. } if c == call) {
            return;
        }
        let Writes::Encrypting { ticket, built, .. } =
            std::mem::replace(&mut self.writes, Writes::Idle)
        else {
            unreachable!("matched above");
        };
        let Outcome::Done(enc) = outcome else {
            self.reply(ticket, Err(Rejected::Failed));
            return;
        };
        #[cfg(test)]
        if self.plant == Some(Plant::ApplyBeforeCommit) {
            let Phase::Live(live) = &mut self.phase else {
                unreachable!("a write is in flight on a live document");
            };
            live.doc
                .apply_changes([built.change.clone()])
                .expect("a change built on this document applies");
        }
        let call = self.fresh_call();
        self.pending.push(Command::Commit {
            call,
            plan: Plan {
                change: enc.clone(),
                frontier: built.frontier.clone(),
            },
        });
        self.writes = Writes::Committing {
            call,
            ticket,
            token: enc.token,
            built,
        };
    }

    pub fn committed(&mut self, call: CallId, outcome: Outcome<Landed>) {
        if !matches!(self.writes, Writes::Committing { call: c, .. } if c == call) {
            return;
        }
        let Writes::Committing {
            ticket,
            token,
            built,
            ..
        } = std::mem::replace(&mut self.writes, Writes::Idle)
        else {
            unreachable!("matched above");
        };
        match outcome {
            Outcome::Done(landed) => {
                self.pending.push(Command::SettleEncrypt {
                    token,
                    applied: true,
                });
                self.land(Some(built.change), built.frontier.is_some(), landed);
                // the change is applied before the caller hears of it (law 3)
                self.reply(ticket, Ok(built.out));
            }
            Outcome::Failed | Outcome::Cancelled => {
                self.pending.push(Command::SettleEncrypt {
                    token,
                    applied: false,
                });
                self.reply(ticket, Err(Rejected::Failed));
            }
        }
    }

    pub fn published(&mut self, call: CallId, outcome: Outcome<()>) {
        let Frontier::Publishing { call: c, rev } = self.frontier else {
            return;
        };
        if c != call {
            return;
        }
        match outcome {
            Outcome::Done(()) => {
                self.retry.failures = 0;
                self.frontier_rev = rev;
                self.frontier = Frontier::Current;
            }
            Outcome::Failed => {
                self.frontier = Frontier::Stale;
                self.failed();
            }
            Outcome::Cancelled => self.frontier = Frontier::Stale,
        }
    }

    // ── shared transitions ──────────────────────────────────────────────────────────────────

    /// A committed change takes effect: it is applied to the live document, and the tree
    /// revision and the frontier follow as far as the commit proves them.
    fn land(&mut self, change: Option<Change>, with_frontier: bool, landed: Landed) {
        let Phase::Live(live) = &mut self.phase else {
            unreachable!("a write holds a live document");
        };
        if let Some(change) = change {
            live.doc
                .apply_changes([change])
                .expect("a change built on this document applies");
        }
        // Nothing else touched the document since the change was built, so the tree held
        // nothing unapplied exactly when it was at the revision applied.
        #[cfg(test)]
        let trust = self.plant == Some(Plant::TrustLandedRev);
        #[cfg(not(test))]
        let trust = false;
        let gap_free = trust || landed.prior_rev == live.applied_rev;
        self.known_rev = self.known_rev.max(landed.rev);
        if gap_free {
            live.applied_rev = landed.rev;
        }
        settle(live, self.known_rev);
        if gap_free && with_frontier {
            self.frontier_rev = landed.rev;
            self.frontier = Frontier::Current;
        } else {
            self.frontier = Frontier::Stale;
        }
        self.pending.push(Command::Notify {
            origin: Origin::Local,
            heads: live.doc.get_heads(),
        });
    }

    fn failed(&mut self) {
        self.retry.failures += 1;
        self.retry.arm = true;
    }
}

/// A document that is up to date and finds the tree ahead is behind. Nothing else moves here:
/// `Behind` also means "ask again" (keys arrived for blocked content), which only a decryption
/// ends.
fn settle(live: &mut Live, known_rev: Rev) {
    if matches!(live.remote, Remote::UpToDate) && known_rev > live.applied_rev {
        live.remote = Remote::Behind;
    }
}
