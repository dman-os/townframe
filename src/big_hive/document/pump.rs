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
                    if self.wanted() && !self.backing_off() {
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
                Queued::Write { ticket, .. } => self.reply(ticket, Err(Rejected::Unreadable)),
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
            self.phase = Phase::Cold;
            // A frontier that was current for what was applied is stale for the tree.
            if self.known_rev > self.frontier_rev {
                self.frontier = Frontier::Stale;
            }
        }
    }

    fn serve_reads(&mut self) {
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

    /// Start the next piece of work on a live document, if the pipeline is free. Answers
    /// whether it finished at once, so the next piece may start in the same flush.
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
        if writing || publishing || applying {
            return false;
        }
        #[cfg(test)]
        if self.plant == Some(Plant::FrontierAhead)
            && behind
            && matches!(self.frontier, Frontier::Stale)
        {
            self.start_publish();
            return false;
        }
        let remote_due = behind && !self.backing_off();
        let write_due = !self.queue.is_empty();
        if remote_due && (!write_due || !self.last_ran_remote) {
            self.last_ran_remote = true;
            self.start_decrypt();
            false
        } else if write_due {
            self.last_ran_remote = false;
            self.start_write()
        } else {
            let publishable =
                up_to_date && matches!(self.frontier, Frontier::Stale) && !self.backing_off();
            if publishable {
                self.start_publish();
            }
            false
        }
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
        self.pending.push(Command::PublishFrontier {
            call,
            heads: live.doc.get_heads(),
            rev,
        });
        self.frontier = Frontier::Publishing { call, rev };
    }

    /// Build the front of the queue against the live document and start its commit (or answer
    /// the plan). Answers `true` if the pipeline is free again: the closure failed or changed
    /// nothing.
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
            Queued::Write { ticket, op } => match self.build(op) {
                Err(error) => {
                    self.reply(ticket, Err(error));
                    true
                }
                Ok(Drafted {
                    change: None, out, ..
                }) => {
                    self.reply(ticket, Ok(out));
                    true
                }
                Ok(Drafted {
                    change: Some(change),
                    frontier,
                    out,
                }) => {
                    let call = self.fresh_call();
                    self.pending.push(Command::PrepareEncrypt {
                        call,
                        change: change.raw_bytes().to_vec(),
                    });
                    self.writes = Writes::Encrypting {
                        call,
                        ticket,
                        built: Built {
                            change,
                            frontier,
                            out,
                        },
                    };
                    false
                }
            },
            Queued::Plan { plan, op } => match self.build(op) {
                Err(error) => {
                    self.pending.push(Command::Prepared {
                        plan,
                        result: Err(error),
                    });
                    true
                }
                Ok(Drafted {
                    change,
                    frontier,
                    out,
                }) => {
                    self.pending.push(Command::Prepared {
                        plan,
                        result: Ok(PlanPrepared {
                            change: change.as_ref().map(|change| change.raw_bytes().to_vec()),
                            frontier: frontier.clone(),
                            out,
                        }),
                    });
                    self.writes = Writes::HeldForPlan {
                        plan,
                        change,
                        frontier,
                    };
                    false
                }
            },
        }
    }

    /// Run the caller's closure on a scratch copy of the live document and take the change it
    /// made. The live document is not touched (persist, then apply).
    fn build(&self, op: WriteOp<O>) -> Result<Drafted<O>, Rejected> {
        let Phase::Live(live) = &self.phase else {
            unreachable!("a write is built on a live document");
        };
        let mut scratch = live.doc.clone();
        let mut tx = scratch.transaction();
        // a failed closure leaves `tx` to roll itself back, and the scratch is dropped
        let out = op(&mut tx).map_err(|error| Rejected::Op(error.to_string()))?;
        let (hash, _) = tx.commit();
        if hash.is_none() {
            return Ok(Drafted {
                change: None,
                frontier: None,
                out,
            });
        }
        let change = scratch
            .get_last_local_change()
            .expect("the change just made");
        // A document that is behind or blocked has heads that are not the heads at a revision.
        let frontier = matches!(live.remote, Remote::UpToDate).then(|| FrontierRow {
            heads: scratch.get_heads(),
            if_prior_rev: live.applied_rev,
        });
        Ok(Drafted {
            change: Some(change),
            frontier,
            out,
        })
    }
}
