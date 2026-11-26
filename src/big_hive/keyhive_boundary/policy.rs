//! The policy: what the machine decides on each flush, from its state alone. See the
//! [parent module](super) for the protocol and the laws.

use super::*;

impl KeyhiveBoundary {
    pub(super) fn advance(&mut self, now: Instant) {
        self.stamp(now);
        if self.refill_due.is_some_and(|due| due <= now) {
            self.refill_due = None;
            self.refill_wanted = self.prekeys < self.config.prekey_floor;
        }
        if self.refill_wanted && self.active.is_none() && self.queue.is_empty() {
            self.refill_wanted = false;
            self.queue.push_back(Queued {
                ticket: None,
                op: None,
            });
        }
        if self.active.is_none() {
            self.evict_idle();
        }
        let exclusive_pending = self.active.is_some() || !self.queue.is_empty();
        if !exclusive_pending || self.planted_encrypt_during_locked() {
            self.issue_shared();
        }
        if self.active.is_none()
            && !self.queue.is_empty()
            && (self.shared() == 0 || self.planted_lock_with_encryption())
        {
            self.start_locked();
        }
        self.ask_due(now);
    }

    /// How many calls hold the shared lock: the keyhive calls out. A preparation waiting for
    /// its commit is not one.
    pub(super) fn shared(&self) -> usize {
        self.tokens
            .values()
            .filter(|token| {
                matches!(
                    token.state,
                    TokenState::Encrypting { .. } | TokenState::Applying { .. }
                )
            })
            .count()
            + self.decrypting.len()
            + self
                .docs
                .values()
                .filter(|load| matches!(load, Load::Loading(_)))
                .count()
    }

    pub(super) fn fresh_call(&mut self) -> CallId {
        self.next_call += 1;
        self.next_call
    }

    pub(super) fn trap(&mut self, during: Step) {
        self.pending.clear();
        self.pending.push(Command::Trap { during });
        self.trapped = !self.planted_continue();
    }

    /// Hydrate what the held requests need and issue the encryptions that can start, keeping
    /// the order of one document's requests.
    pub(super) fn issue_shared(&mut self) {
        let wanted: Vec<u64> = self
            .tokens
            .iter()
            .filter(|(_, token)| matches!(token.state, TokenState::ApplyWanted { .. }))
            .map(|(token, _)| *token)
            .collect();
        for token in wanted {
            let call = self.fresh_call();
            let entry = self.tokens.get_mut(&token).expect("listed above");
            let TokenState::ApplyWanted { events } = &mut entry.state else {
                unreachable!("listed above");
            };
            let events = std::mem::take(events);
            entry.state = TokenState::Applying { call, events };
            self.pending.push(Command::ApplyEncrypt { call, token });
        }
        let held = std::mem::take(&mut self.held);
        for request in held {
            let load = self.docs.get(&request.doc).copied();
            let ready = match load {
                Some(Load::Loaded) => !self.evict_wanted.contains(&request.doc),
                None => match request.work {
                    Work::Encrypt(_) => self.planted_skip_hydrate(),
                    Work::Decrypt(_) => self.planted_decrypt_without_hydrate(),
                },
                Some(Load::Loading(_)) => false,
            };
            if ready {
                let call = self.fresh_call();
                match request.work {
                    Work::Encrypt(change) => {
                        self.tokens.insert(
                            call,
                            Token {
                                doc: request.doc,
                                lost: false,
                                state: TokenState::Encrypting {
                                    asker: request.asker,
                                    call: request.call,
                                },
                            },
                        );
                        self.pending.push(Command::Encrypt {
                            call,
                            doc: request.doc,
                            change,
                        });
                    }
                    Work::Decrypt(payload) => {
                        self.decrypting
                            .insert(call, (request.asker, request.call, request.doc));
                        self.pending.push(Command::Decrypt {
                            call,
                            doc: request.doc,
                            payload,
                        });
                    }
                }
                continue;
            }
            if load.is_none() {
                let call = self.fresh_call();
                self.docs.insert(request.doc, Load::Loading(call));
                self.pending.push(Command::Hydrate {
                    call,
                    doc: request.doc,
                });
            }
            self.held.push_back(request);
        }
    }

    /// Evict the documents the hub said moved, when nothing prepared refers to them. A document
    /// that is loading is evicted when its hydration ends.
    pub(super) fn evict_idle(&mut self) {
        let wanted = std::mem::take(&mut self.evict_wanted);
        for doc in wanted {
            match self.docs.get(&doc) {
                None => {}
                Some(Load::Loading(_)) => {
                    self.evict_wanted.insert(doc);
                }
                Some(Load::Loaded)
                    if self
                        .tokens
                        .values()
                        .any(|token| token.doc == doc && !token.lost)
                        || self.decrypting.values().any(|(_, _, d)| *d == doc) =>
                {
                    self.evict_wanted.insert(doc);
                }
                Some(Load::Loaded) => {
                    self.docs.remove(&doc);
                    self.pending.push(Command::Evict { doc });
                }
            }
        }
    }

    pub(super) fn start_locked(&mut self) {
        let Queued { ticket, op } = self.queue.pop_front().expect("queued");
        let call = self.fresh_call();
        self.active = Some(Active {
            ticket,
            op,
            refill: false,
            phase: Phase::Locking(call),
        });
        self.pending.push(Command::AcquireLock { call });
    }

    /// The operation failed after keyhive may have changed its memory: roll back, then reopen.
    pub(super) fn roll_back(&mut self) {
        let call = self.fresh_call();
        let active = self.active.as_mut().expect("active");
        active.phase = Phase::RollingBack(call);
        if active.refill && !self.planted_no_refill_retry() {
            // the refill was refused with the operation: try again after a growing wait
            self.refill_failures += 1;
            self.refill_stamp = true;
        }
        self.pending.push(Command::Rollback { call });
    }

    pub(super) fn finish_refused(&mut self) {
        let active = self.active.take().expect("active");
        self.pending.push(Command::ReleaseLock);
        if let Some(ticket) = active.ticket {
            self.pending.push(Command::Reply {
                ticket,
                result: Err(Refused),
            });
        }
    }

    // ── telling peers ───────────────────────────────────────────────────────────────────────

    /// Events that committed: every connected session but their source may be told.
    pub(super) fn publish(
        &mut self,
        events: impl IntoIterator<Item = (EventId, Option<SessionId>)>,
    ) {
        let echo = self.planted_echo();
        for (event, source) in events {
            for (session, peer) in &mut self.peers {
                if Some(*session) == source && !echo {
                    continue;
                }
                peer.pending.insert(event);
                peer.dirty = true;
            }
        }
    }

    /// The floor is back: forget the refusals.
    pub(super) fn refill_restored(&mut self) {
        self.refill_failures = 0;
        self.refill_stamp = false;
        self.refill_due = None;
    }

    pub(super) fn stamp(&mut self, now: Instant) {
        if std::mem::take(&mut self.refill_stamp) {
            let doublings = (self.refill_failures - 1).min(16);
            let wait = (self.config.refill_backoff_min * 2u32.pow(doublings))
                .min(self.config.refill_backoff_max);
            self.refill_due = Some(now + wait);
        }
        for peer in self.peers.values_mut().filter(|peer| peer.dirty) {
            peer.dirty = false;
            peer.first.get_or_insert(now);
            peer.last = Some(now);
        }
    }

    pub(super) fn due(&self, peer: &Peer) -> Option<Instant> {
        if peer.query.is_some() || peer.pending.is_empty() {
            return None;
        }
        let (first, last) = (peer.first?, peer.last?);
        Some((last + self.config.debounce).min(first + self.config.max_delay))
    }

    /// Ask Keyline about each session whose debounce ran out.
    pub(super) fn ask_due(&mut self, now: Instant) {
        let due: Vec<SessionId> = self
            .peers
            .iter()
            .filter(|(_, peer)| self.due(peer).is_some_and(|due| due <= now))
            .map(|(session, _)| *session)
            .collect();
        for session in due {
            let call = self.fresh_call();
            let peer = self.peers.get_mut(&session).expect("listed above");
            let events = std::mem::take(&mut peer.pending);
            peer.first = None;
            peer.last = None;
            peer.query = Some((call, events.clone()));
            self.pending.push(Command::Eligible {
                call,
                session,
                events,
            });
        }
    }

    /// A line for traces.
    #[cfg(test)]
    pub(super) fn describe(&self) -> String {
        format!(
            "docs={:?} tokens={} held={} queue={} active={} prekeys={} trapped={}",
            self.docs,
            self.tokens.len(),
            self.held.len(),
            self.queue.len(),
            self.active.is_some(),
            self.prekeys,
            self.trapped
        )
    }

    // The planted bugs are test-only; the production build reads each as `false`.
    #[cfg(test)]
    pub(super) fn planted_continue(&self) -> bool {
        self.plant == Some(Plant::ContinueAfterTrap)
    }
    #[cfg(not(test))]
    pub(super) fn planted_continue(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_encrypt_during_locked(&self) -> bool {
        self.plant == Some(Plant::EncryptDuringLocked)
    }
    #[cfg(not(test))]
    pub(super) fn planted_encrypt_during_locked(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_lock_with_encryption(&self) -> bool {
        self.plant == Some(Plant::LockWithEncryptionInFlight)
    }
    #[cfg(not(test))]
    pub(super) fn planted_lock_with_encryption(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_echo(&self) -> bool {
        self.plant == Some(Plant::EchoToSource)
    }
    #[cfg(not(test))]
    pub(super) fn planted_echo(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_skip_hydrate(&self) -> bool {
        self.plant == Some(Plant::SkipHydrate)
    }
    #[cfg(not(test))]
    pub(super) fn planted_skip_hydrate(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_decrypt_without_hydrate(&self) -> bool {
        self.plant == Some(Plant::DecryptWithoutHydrate)
    }
    #[cfg(not(test))]
    pub(super) fn planted_decrypt_without_hydrate(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_no_refill_retry(&self) -> bool {
        self.plant == Some(Plant::NoRefillRetry)
    }
    #[cfg(not(test))]
    pub(super) fn planted_no_refill_retry(&self) -> bool {
        false
    }
    #[cfg(test)]
    pub(super) fn planted_skip_refill(&self) -> bool {
        self.plant == Some(Plant::SkipRefill)
    }
    #[cfg(not(test))]
    pub(super) fn planted_skip_refill(&self) -> bool {
        false
    }
}
