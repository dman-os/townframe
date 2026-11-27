//! [`Document::flush`]: where queued work starts, the clock is read, and the machine's pending
//! commands leave.

use super::*;

impl<O> Document<O> {
    /// Start what the inputs since the last flush made possible, and take the commands.
    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command<O>>) {
        self.read_clock(now);
        self.pump();
        self.tick_idle(now);
        out.append(&mut self.pending);
        #[cfg(test)]
        self.check();
    }

    /// When the driver should flush next: a backoff ending, or the idle deadline.
    pub fn next_due(&self) -> Option<Instant> {
        let idle = match self.idle {
            IdleClock::Counting(due) => Some(due),
            IdleClock::Busy | IdleClock::Reported => None,
        };
        [self.retry.until, idle].into_iter().flatten().min()
    }

    /// Whether the machine may be stopped without losing anything (law 7): no handle, no
    /// interest, nothing in flight, nothing held, and no catch-up left to do.
    pub fn evictable(&self) -> bool {
        self.handles.is_empty()
            && self.interest == 0
            && matches!(self.writes, Writes::Idle)
            && self.queue.is_empty()
            && self.reads.is_empty()
            && self.stopping.is_empty()
            && self.awaiting_actor.is_none()
            && self.pending.is_empty()
            && !matches!(self.frontier, Frontier::Publishing { .. })
            && !matches!(self.phase, Phase::Hydrating { .. })
            && !self.has_catch_up()
    }

    /// Whether a readable document still has remote content or a frontier to bring up to date.
    /// A parked or blocked document does not: it waits for keys, which the Hub wakes it for.
    /// The content a parked or blocked document waits on, for the Hub to park it by.
    pub fn missing(&self) -> &[ContentRef] {
        match &self.phase {
            Phase::Parked { missing } => missing,
            Phase::Live(live) => match &live.remote {
                Remote::Blocked { refs } => refs,
                _ => &[],
            },
            Phase::Cold | Phase::Hydrating { .. } => &[],
        }
    }

    fn has_catch_up(&self) -> bool {
        if !self.readable {
            return false;
        }
        match &self.phase {
            Phase::Cold => self.known_rev > self.frontier_rev,
            Phase::Live(live) => match live.remote {
                Remote::Behind | Remote::Applying { .. } => true,
                Remote::UpToDate => matches!(self.frontier, Frontier::Stale),
                Remote::Blocked { .. } => false,
            },
            Phase::Hydrating { .. } => true,
            Phase::Parked { .. } => false,
        }
    }

    fn read_clock(&mut self, now: Instant) {
        if self.retry.arm {
            self.retry.arm = false;
            let steps = self.retry.failures.saturating_sub(1).min(16);
            let wait = self
                .config
                .min_backoff
                .saturating_mul(1 << steps)
                .min(self.config.max_backoff);
            self.retry.until = Some(now + wait);
        }
        if self.retry.until.is_some_and(|until| until <= now) {
            self.retry.until = None;
        }
    }

    fn tick_idle(&mut self, now: Instant) {
        if !self.evictable() {
            self.idle = IdleClock::Busy;
            return;
        }
        match self.idle {
            IdleClock::Busy => self.idle = IdleClock::Counting(now + self.config.idle_ttl),
            IdleClock::Counting(due) if due <= now => {
                self.idle = IdleClock::Reported;
                self.pending.push(Command::Evictable);
            }
            IdleClock::Counting(_) | IdleClock::Reported => {}
        }
    }

    pub(super) fn backing_off(&self) -> bool {
        self.retry.until.is_some()
    }

    /// Whether somebody needs the document in memory.
    pub(super) fn wanted(&self) -> bool {
        #[cfg(test)]
        if self.plant == Some(Plant::HydrateUnreadable) {
            return true;
        }
        self.readable
            && (!self.handles.is_empty()
                || self.interest > 0
                || !self.queue.is_empty()
                || !self.reads.is_empty()
                || self.known_rev > self.frontier_rev)
    }

    fn pump(&mut self) {
        if !self.readable {
            self.refuse_waiting();
            self.drop_live_when_idle();
        }
        loop {
            match &self.phase {
                Phase::Cold => {
                    if self.wanted() && !self.backing_off() && self.awaiting_actor.is_none() {
                        let call = self.fresh_call();
                        self.phase = Phase::Hydrating { call };
                        self.pending.push(Command::Hydrate { call });
                    }
                    return;
                }
                Phase::Hydrating { .. } => return,
                Phase::Parked { .. } => {
                    self.refuse_plans();
                    return;
                }
                Phase::Live(_) => {}
            }
            self.serve_reads();
            if !self.step_pipeline() {
                return;
            }
        }
    }

    /// Unreadable: nothing waiting can ever be served, so it is refused.
    fn refuse_waiting(&mut self) {
        for (ticket, _) in std::mem::take(&mut self.reads) {
            self.reply(ticket, Err(Rejected::Unreadable));
        }
        for queued in std::mem::take(&mut self.queue) {
            match queued {
                Queued::Write { ticket, work } => self.answer(
                    ticket,
                    matches!(work, Work::Changes(_)),
                    Err(Rejected::Unreadable),
                ),
                Queued::Plan { plan, .. } => self.pending.push(Command::Prepared {
                    plan,
                    result: Err(Rejected::Unreadable),
                }),
            }
        }
    }

    /// A plan holds every document it has prepared until all are, so it cannot wait on keys
    /// that may never come: a parked document refuses to prepare. (Single writes wait.)
    fn refuse_plans(&mut self) {
        let (plans, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|queued| matches!(queued, Queued::Plan { .. }));
        self.queue = rest.into();
        for queued in plans {
            let Queued::Plan { plan, .. } = queued else {
                unreachable!("partitioned");
            };
            self.pending.push(Command::Prepared {
                plan,
                result: Err(Rejected::Parked),
            });
        }
    }

    fn drop_live_when_idle(&mut self) {
        let Phase::Live(live) = &self.phase else {
            return;
        };
        let idle = matches!(self.writes, Writes::Idle)
            && !matches!(live.remote, Remote::Applying { .. })
            && !matches!(self.frontier, Frontier::Publishing { .. });
        if idle {
            self.go_cold();
        }
    }

    /// A write ended without a commit: forget its change by throwing the live document away, and
    /// tell the listeners again once the new one is there.
    pub(super) fn forget_write(&mut self, undecided: bool) {
        self.renotify = true;
        self.go_cold();
        #[cfg(test)]
        let ask = undecided && self.plant != Some(Plant::ReuseActor);
        #[cfg(not(test))]
        let ask = undecided;
        if ask && self.awaiting_actor.is_none() {
            let call = self.fresh_call();
            self.awaiting_actor = Some(call);
            self.pending.push(Command::NewActor { call });
        }
    }

    /// Throw the live document away. It is hydrated again when somebody wants it, from
    /// committed state; this is how a failed write forgets its change.
    pub(super) fn go_cold(&mut self) {
        // a remote read in flight answers a document that is gone
        if let Phase::Live(live) = &mut self.phase
            && let Remote::Applying { call } = live.remote
        {
            self.stopping.insert(call);
            self.pending.push(Command::Cancel { call });
        }
        self.phase = Phase::Cold;
        // A frontier that was current for what was applied is stale for the tree. (One being
        // published stays: its call is still out, and its outcome settles it.)
        if self.known_rev > self.frontier_rev && matches!(self.frontier, Frontier::Current) {
            self.frontier = Frontier::Stale;
        }
    }

    /// Serve the waiting reads, unless a write is in flight: the live document then holds a
    /// change that is not committed, and no reader may see it (law 1).
    fn serve_reads(&mut self) {
        #[cfg(test)]
        let held = self.plant != Some(Plant::ServeReadsDuringWrite);
        #[cfg(not(test))]
        let held = true;
        if held && !matches!(self.writes, Writes::Idle) {
            return;
        }
        let Phase::Live(live) = &self.phase else {
            unreachable!("reads are served from a live document");
        };
        for (ticket, op) in std::mem::take(&mut self.reads) {
            self.pending.push(Command::Reply {
                ticket,
                result: Ok(op(&live.doc)),
            });
        }
    }

    /// Whether a prepared plan keeps the document from starting anything else (law 4).
    fn plan_holds(&self) -> bool {
        #[cfg(test)]
        return self.plant != Some(Plant::PlanDoesNotHold);
        #[cfg(not(test))]
        true
    }

    /// Start what can start on a live document. Remote materialization runs alongside a write.
    /// A write starts when none is in flight and no publication is out; the frontier is
    /// published only when no write is in flight and none is queued, since its heads would
    /// otherwise include an uncommitted change. The two never overlap, so the rows of the
    /// frontier are ordered by the machine itself and not by any driver: a plan's commit is
    /// run by the Write coordinator, not by this document's driver.
    /// Answers whether a write finished at once, so the next may start in the same flush.
    fn step_pipeline(&mut self) -> bool {
        let Phase::Live(live) = &self.phase else {
            unreachable!("the pipeline runs on a live document");
        };
        let writing = match self.writes {
            Writes::Idle => false,
            Writes::HeldForPlan { .. } => self.plan_holds(),
            Writes::Encrypting { .. } | Writes::Committing { .. } => true,
        };
        let publishing = matches!(self.frontier, Frontier::Publishing { .. });
        let applying = matches!(live.remote, Remote::Applying { .. });
        let behind = matches!(live.remote, Remote::Behind);
        let up_to_date = matches!(live.remote, Remote::UpToDate);
        let stale = matches!(self.frontier, Frontier::Stale);
        #[cfg(test)]
        let publish_while_writing = self.plant == Some(Plant::PublishDuringWrite);
        #[cfg(not(test))]
        let publish_while_writing = false;
        #[cfg(test)]
        if self.plant == Some(Plant::FrontierAhead) && behind && stale && !publishing && !writing {
            self.start_publish();
            return false;
        }
        if behind && !applying && self.readable && !self.backing_off() {
            self.start_decrypt();
        }
        #[cfg(test)]
        let write_while_publishing = self.plant == Some(Plant::WriteWhilePublishing);
        #[cfg(not(test))]
        let write_while_publishing = false;
        if !writing && (!publishing || write_while_publishing) && !self.queue.is_empty() {
            return self.start_write();
        }
        let idle = (!writing && self.queue.is_empty()) || publish_while_writing;
        if idle && up_to_date && stale && !publishing && !self.backing_off() {
            self.start_publish();
        }
        false
    }

    fn start_decrypt(&mut self) {
        let call = self.fresh_call();
        let known_rev = self.known_rev;
        let Phase::Live(live) = &mut self.phase else {
            unreachable!("the pipeline runs on a live document");
        };
        live.remote = Remote::Applying { call };
        self.pending.push(Command::Decrypt {
            call,
            heads: live.doc.get_heads(),
            rev: known_rev,
        });
    }

    fn start_publish(&mut self) {
        let call = self.fresh_call();
        let Phase::Live(live) = &self.phase else {
            unreachable!("the pipeline runs on a live document");
        };
        let rev = live.applied_rev;
        #[cfg(test)]
        let rev = if self.plant == Some(Plant::FrontierAhead) {
            self.known_rev
        } else {
            rev
        };
        let heads = live.doc.get_heads();
        self.pending.push(Command::PublishFrontier {
            call,
            heads: heads.clone(),
            rev,
        });
        self.frontier = Frontier::Publishing { call, rev, heads };
    }

    /// Make the front of the queue on the live document and start its commit (or answer the
    /// plan). Answers `true` if the pipeline is free again: the write failed or changed nothing.
    fn start_write(&mut self) -> bool {
        #[cfg(test)]
        let next = if self.plant == Some(Plant::LifoQueue) {
            self.queue.pop_back()
        } else {
            self.queue.pop_front()
        };
        #[cfg(not(test))]
        let next = self.queue.pop_front();
        match next.expect("a write is due") {
            Queued::Write { ticket, work } => {
                let ingest = matches!(work, Work::Changes(_));
                match self.build(work) {
                    Err(error) => {
                        self.answer(ticket, ingest, Err(error));
                        true
                    }
                    Ok(Drafted {
                        bytes: None, out, ..
                    }) => {
                        self.answer(ticket, ingest, Ok(out));
                        true
                    }
                    Ok(Drafted {
                        bytes: Some(change),
                        frontier,
                        out,
                    }) => {
                        let call = self.fresh_call();
                        self.pending.push(Command::PrepareEncrypt {
                            call,
                            change: change.clone(),
                        });
                        self.writes = Writes::Encrypting {
                            call,
                            ticket,
                            built: Built {
                                bytes: change,
                                frontier,
                                out,
                            },
                            attempts: 0,
                        };
                        false
                    }
                }
            }
            Queued::Plan { plan, work } => match self.build(work) {
                Err(error) => {
                    self.pending.push(Command::Prepared {
                        plan,
                        result: Err(error),
                    });
                    true
                }
                Ok(Drafted {
                    bytes,
                    frontier,
                    out,
                }) => {
                    let changed = bytes.is_some();
                    self.pending.push(Command::Prepared {
                        plan,
                        result: Ok(PlanPrepared {
                            change: bytes,
                            frontier: frontier.clone(),
                            out,
                        }),
                    });
                    self.writes = Writes::HeldForPlan {
                        plan,
                        frontier,
                        changed,
                    };
                    false
                }
            },
        }
    }

    /// Make the work on the live document: run the closure in a transaction and commit it, or
    /// apply the author's changes, and take the bytes of what changed. The change is in the live
    /// document from here until the write ends, which is why nothing may look at it.
    fn build(&mut self, work: Work<O>) -> Result<Drafted<O>, Rejected> {
        let Phase::Live(live) = &mut self.phase else {
            unreachable!("a write is made on a live document");
        };
        let (bytes, out) = match work {
            Work::Op(op) => {
                let mut tx = live.doc.transaction();
                // a failed closure leaves `tx` to roll itself back
                let out = op(&mut tx).map_err(|error| Rejected::Op(error.to_string()))?;
                let (hash, _) = tx.commit();
                if hash.is_none() {
                    return Ok(Drafted {
                        bytes: None,
                        frontier: None,
                        out: Some(out),
                    });
                }
                let change = live
                    .doc
                    .get_last_local_change()
                    .expect("the change just made");
                (change.raw_bytes().to_vec(), Some(out))
            }
            Work::Changes(changes) => {
                let new: Vec<Change> = changes
                    .into_iter()
                    .filter(|change| live.doc.get_change_meta_by_hash(&change.hash()).is_none())
                    .collect();
                if new.is_empty() {
                    return Ok(Drafted {
                        bytes: None,
                        frontier: None,
                        out: None,
                    });
                }
                let bytes = new
                    .iter()
                    .flat_map(|change| change.raw_bytes())
                    .copied()
                    .collect();
                live.doc
                    .apply_changes(new)
                    .expect("an author's changes apply");
                (bytes, None)
            }
        };
        // A document that is behind or blocked has heads that are not the heads at a revision.
        let frontier = matches!(live.remote, Remote::UpToDate).then(|| FrontierRow {
            heads: live.doc.get_heads(),
            if_prior_rev: live.applied_rev,
        });
        Ok(Drafted {
            bytes: Some(bytes),
            frontier,
            out,
        })
    }
}
