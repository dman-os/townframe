//! What the driver reports back: one input per kind of command that has an answer. See the
//! [parent module](super) for the protocol and the laws.

use super::*;

impl KeyhiveBoundary {
    /// A [`Command::Hydrate`] ended. It reports `Done` or `StoreFailed`.
    pub fn hydrated(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let doc = self
            .docs
            .iter()
            .find_map(|(doc, load)| (*load == Load::Loading(call)).then_some(*doc))
            .expect("an answer to a hydration in flight");
        match ended {
            Ended::Done(()) => {
                if self.evict_wanted.remove(&doc) {
                    // it read rows that another process has moved since: start over
                    self.docs.remove(&doc);
                    self.pending.push(Command::Evict { doc });
                } else {
                    self.docs.insert(doc, Load::Loaded);
                }
            }
            Ended::StoreFailed => self.trap(Step::Hydrate),
            Ended::Refused | Ended::Cancelled => panic!("a hydration is not refused or cancelled"),
        }
    }

    /// A [`Command::Encrypt`] ended. It reports `Done`, `Refused` or `StoreFailed`.
    pub fn encrypted(&mut self, call: CallId, ended: Ended<Prepared>) {
        if self.trapped {
            return;
        }
        let token = self
            .tokens
            .get_mut(&call)
            .expect("an answer to an encryption in flight");
        let TokenState::Encrypting { asker, call: ask } = token.state else {
            panic!("an encryption is answered once");
        };
        match ended {
            Ended::Done(prepared) => {
                token.state = TokenState::Prepared {
                    events: prepared.events,
                };
                self.pending.push(Command::Encrypted {
                    to: asker,
                    call: ask,
                    outcome: Outcome::Done(EncryptedChange {
                        token: call,
                        ciphertext: prepared.ciphertext,
                    }),
                });
            }
            Ended::Refused => {
                self.tokens.remove(&call);
                self.pending.push(Command::Encrypted {
                    to: asker,
                    call: ask,
                    outcome: Outcome::Failed,
                });
            }
            Ended::StoreFailed => self.trap(Step::Encrypt),
            Ended::Cancelled => panic!("an encryption is not cancelled"),
        }
    }

    /// A [`Command::ApplyEncrypt`] ended. It reports `Done` or `StoreFailed`.
    pub fn applied(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let token = self
            .tokens
            .iter()
            .find_map(|(token, state)| {
                matches!(state.state, TokenState::Applying { call: c, .. } if c == call)
                    .then_some(*token)
            })
            .expect("an answer to an apply in flight");
        match ended {
            Ended::Done(()) => {
                let Some(Token {
                    state: TokenState::Applying { events, .. },
                    ..
                }) = self.tokens.remove(&token)
                else {
                    unreachable!("found above");
                };
                self.publish(events.into_iter().map(|event| (event, None)));
            }
            Ended::StoreFailed => self.trap(Step::Apply),
            Ended::Refused | Ended::Cancelled => panic!("an apply is not refused or cancelled"),
        }
    }

    /// The exclusive lock is ours.
    pub fn lock_acquired(&mut self, call: CallId) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::Locking(c) if c == call));
        let call = {
            self.next_call += 1;
            self.next_call
        };
        self.active.as_mut().expect("active").phase = Phase::Beginning(call);
        self.pending.push(Command::Begin { call });
    }

    /// A [`Command::Begin`] ended. It reports `Done` or `StoreFailed`.
    pub fn began(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::Beginning(c) if c == call));
        match ended {
            Ended::Done(()) => {
                let op = active.op.take();
                let refill = self.prekeys < self.config.prekey_floor && !self.planted_skip_refill();
                if self.prekeys < self.config.prekey_floor {
                    self.refill_wanted = false;
                }
                let call = self.fresh_call();
                self.active.as_mut().expect("active").phase = Phase::Running(call);
                self.pending.push(Command::Run { call, op, refill });
            }
            Ended::StoreFailed => self.trap(Step::Begin),
            Ended::Refused | Ended::Cancelled => panic!("a begin is not refused or cancelled"),
        }
    }

    /// A [`Command::Run`] ended. It reports `Done`, `Refused` or `StoreFailed`.
    pub fn ran(&mut self, call: CallId, ended: Ended<Report>) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::Running(c) if c == call));
        match ended {
            Ended::Done(report) => {
                for doc in &report.hydrated {
                    self.docs.insert(*doc, Load::Loaded);
                }
                let call = self.fresh_call();
                self.active.as_mut().expect("active").phase = Phase::Committing(call, report);
                self.pending.push(Command::Commit { call });
            }
            Ended::Refused => self.roll_back(),
            Ended::StoreFailed => self.trap(Step::Run),
            Ended::Cancelled => panic!("a run is not cancelled"),
        }
    }

    /// A [`Command::Commit`] ended. It reports `Done`, `Refused` (the transaction was rejected
    /// cleanly) or `StoreFailed`.
    pub fn committed(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::Committing(c, _) if c == call));
        match ended {
            Ended::Done(()) => {
                let active = self.active.take().expect("active");
                let Phase::Committing(_, report) = active.phase else {
                    unreachable!("checked above");
                };
                self.prekeys = report.prekeys;
                if report.prekeys < self.config.prekey_floor {
                    self.refill_wanted = true;
                }
                self.pending.push(Command::ReleaseLock);
                if let Some(ticket) = active.ticket {
                    self.pending.push(Command::Reply {
                        ticket,
                        result: Ok(()),
                    });
                }
                self.publish(report.events);
            }
            Ended::Refused => self.roll_back(),
            Ended::StoreFailed => self.trap(Step::Commit),
            Ended::Cancelled => panic!("a commit is not cancelled"),
        }
    }

    /// A [`Command::Rollback`] ended. It reports `Done` or `StoreFailed`.
    pub fn rolled_back(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::RollingBack(c) if c == call));
        match ended {
            Ended::Done(()) => {
                #[cfg(test)]
                if self.plant == Some(Plant::SkipReopen) {
                    self.finish_refused();
                    return;
                }
                let call = self.fresh_call();
                self.active.as_mut().expect("active").phase = Phase::Reopening(call);
                self.pending.push(Command::Reopen { call });
            }
            Ended::StoreFailed => self.trap(Step::Rollback),
            Ended::Refused | Ended::Cancelled => panic!("a rollback is not refused or cancelled"),
        }
    }

    /// A [`Command::Reopen`] ended. It reports `Done` or `StoreFailed`.
    pub fn reopened(&mut self, call: CallId, ended: Ended<()>) {
        if self.trapped {
            return;
        }
        let active = self.active.as_mut().expect("a locked operation");
        assert!(matches!(active.phase, Phase::Reopening(c) if c == call));
        match ended {
            Ended::Done(()) => {
                // a new instance holds nothing, and no preparation survives: the lock waited
                // for them all
                assert!(
                    self.tokens.is_empty(),
                    "nothing prepared across a locked operation"
                );
                self.docs.clear();
                self.evict_wanted.clear();
                self.finish_refused();
            }
            Ended::StoreFailed => self.trap(Step::Reopen),
            Ended::Refused | Ended::Cancelled => panic!("a reopen is not refused or cancelled"),
        }
    }

    /// A [`Command::Eligible`] ended. It reports `Done` with the granted subset, or
    /// `StoreFailed`; a query cancelled by a closed session may report `Cancelled`.
    pub fn eligible(&mut self, call: CallId, ended: Ended<BTreeSet<EventId>>) {
        if self.trapped {
            return;
        }
        if self.cancelled.remove(&call) {
            if ended == Ended::StoreFailed {
                self.trap(Step::Eligible);
            }
            return;
        }
        let (session, peer) = self
            .peers
            .iter_mut()
            .find(|(_, peer)| peer.query.as_ref().is_some_and(|(c, _)| *c == call))
            .expect("an answer to a query in flight");
        let session = *session;
        match ended {
            Ended::Done(granted) => {
                let (_, asked) = peer.query.take().expect("found above");
                #[cfg(test)]
                let granted = if self.plant == Some(Plant::NotifyIneligible) {
                    asked.clone()
                } else {
                    granted
                };
                let events: Vec<EventId> = asked
                    .iter()
                    .copied()
                    .filter(|event| granted.contains(event))
                    .collect();
                #[cfg(test)]
                if self.plant == Some(Plant::NotifyTwice) {
                    peer.pending.extend(events.iter().copied());
                }
                if !peer.pending.is_empty() {
                    peer.dirty = true;
                }
                if !events.is_empty() {
                    self.pending.push(Command::Notify { session, events });
                }
            }
            Ended::StoreFailed => self.trap(Step::Eligible),
            Ended::Refused | Ended::Cancelled => panic!("a query is not refused or cancelled"),
        }
    }
}
