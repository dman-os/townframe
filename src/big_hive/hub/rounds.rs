//! The keyhive rounds of the Hub: per peer, `Idle`, `Active` and `Deferred`, from runtime2's
//! hub (`hub.rs:2808`). The round itself runs in the Keyhive boundary; this is the policy around
//! it, and the module docs of the [parent](super) state its laws (9).
//!
//! A round has *waiters*: callers who asked to hear when the peer's keyhive state has been
//! reconciled. A waiter that was queued when the round started is *admitted* to it and is
//! resolved when it finishes; one that arrived later waits for the next round, which is
//! started once, whatever the number of wants that arrived meanwhile (a notification with no
//! waiter latches the same way). A round whose completion names an admission watermark ahead of
//! the Hub's `admitted_head` is `Deferred` and finishes when the head catches up, so a waiter is
//! never told "reconciled" before the admissions it covers are projected.

use std::collections::BTreeSet;
use std::fmt::Debug;

use super::Command;
use super::Hub;
use super::Plant;
use super::Waiter;
use crate::document::CallId;
use crate::document::Outcome;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundState {
    Idle,
    Active {
        call: CallId,
    },
    /// The round completed, and waits for the admitted head to reach `required`.
    Deferred {
        call: CallId,
        required: u64,
    },
}

#[derive(Debug)]
pub struct PeerRound {
    state: RoundState,
    /// The waiters the round in flight resolves.
    admitted: BTreeSet<Waiter>,
    /// Waiters that arrived during the round: the next round's.
    waiting: BTreeSet<Waiter>,
    /// A want with no waiter arrived during the round.
    notify: bool,
}

impl PeerRound {
    pub fn new() -> Self {
        Self {
            state: RoundState::Idle,
            admitted: BTreeSet::new(),
            waiting: BTreeSet::new(),
            notify: false,
        }
    }
}

impl<P, L> Hub<P, L>
where
    P: Ord + Clone + Debug,
    L: Ord + Clone + Debug,
{
    /// Somebody wants the peer's keyhive state reconciled: a caller that waits (`waiter`), or a
    /// notification that the peer has something. Refused at once (`ok: false`) for a peer that
    /// is not connected, and while draining.
    pub fn keyhive_wanted(&mut self, peer: &P, waiter: Option<Waiter>) {
        let usable = self.life == super::Life::Open && self.peers.contains_key(peer);
        if !usable {
            self.resolve(waiter.into_iter());
            return;
        }
        let round = self.peers.get_mut(peer).expect("checked above");
        match round.state {
            RoundState::Idle => {
                let admitted = waiter.into_iter().collect();
                self.start_round(peer, admitted);
            }
            RoundState::Active { .. } | RoundState::Deferred { .. } => match waiter {
                Some(waiter) => {
                    round.waiting.insert(waiter);
                }
                None => round.notify = true,
            },
        }
    }

    /// A round ended: with the admission sequence number it covered, or not.
    pub fn round_done(&mut self, call: CallId, outcome: Outcome<u64>) {
        if self.cancelled.remove(&call) {
            return;
        }
        let Some(peer) = self
            .peers
            .iter()
            .find_map(|(peer, round)| match round.state {
                RoundState::Active { call: c } | RoundState::Deferred { call: c, .. }
                    if c == call =>
                {
                    Some(peer.clone())
                }
                _ => None,
            })
        else {
            panic!("an outcome for a round that is not in flight");
        };
        match outcome {
            Outcome::Done(seq) => {
                let round = self.peers.get_mut(&peer).expect("found above");
                // the deferral is sticky: a later report cannot lower what is required
                let required = match round.state {
                    RoundState::Deferred { required, .. } => required.max(seq),
                    _ => seq,
                };
                if required > self.admitted_head {
                    round.state = RoundState::Deferred { call, required };
                } else {
                    self.finish_round(&peer, true);
                }
            }
            Outcome::Failed | Outcome::Cancelled => self.finish_round(&peer, false),
            Outcome::Stale { .. } => unreachable!("only a commit is answered stale, not a round"),
        }
    }

    /// The Hub's admission state advanced to `head`.
    pub fn admission_advanced(&mut self, head: u64) {
        self.admitted_head = self.admitted_head.max(head);
        let ready: Vec<P> = self
            .peers
            .iter()
            .filter_map(|(peer, round)| match round.state {
                RoundState::Deferred { required, .. } if required <= self.admitted_head => {
                    Some(peer.clone())
                }
                _ => None,
            })
            .collect();
        for peer in ready {
            self.finish_round(&peer, true);
        }
    }

    pub fn round_of(&self, peer: &P) -> Option<RoundState> {
        self.peers.get(peer).map(|round| round.state)
    }

    pub(super) fn rounds_quiet(&self) -> bool {
        self.cancelled.is_empty()
            && self
                .peers
                .values()
                .all(|round| round.state == RoundState::Idle)
    }

    /// A peer's connection is gone: its round is cancelled and nobody waits on it any more.
    pub(super) fn drop_rounds(&mut self, peer: &P) {
        let Some(round) = self.peers.remove(peer) else {
            return;
        };
        self.cancel(&round);
        self.resolve(round.admitted.into_iter().chain(round.waiting));
    }

    /// Draining: cancel every round in flight, and resolve what waited on it.
    pub(super) fn cancel_rounds(&mut self) {
        let peers: Vec<P> = self.peers.keys().cloned().collect();
        for peer in peers {
            let round = self.peers.get_mut(&peer).expect("listed above");
            let state = std::mem::replace(&mut round.state, RoundState::Idle);
            let waiters: Vec<Waiter> = std::mem::take(&mut round.admitted)
                .into_iter()
                .chain(std::mem::take(&mut round.waiting))
                .collect();
            round.notify = false;
            self.cancel_state(state);
            self.resolve(waiters.into_iter());
        }
    }

    fn start_round(&mut self, peer: &P, admitted: BTreeSet<Waiter>) {
        self.next_call += 1;
        let call = self.next_call;
        let round = self.peers.get_mut(peer).expect("a connected peer");
        round.state = RoundState::Active { call };
        round.admitted = admitted;
        self.pending.push(Command::StartRound {
            call,
            peer: peer.clone(),
        });
    }

    /// The round ended: resolve its waiters, then start the one follow-up the demand that
    /// arrived meanwhile asks for.
    fn finish_round(&mut self, peer: &P, ok: bool) {
        let round = self.peers.get_mut(peer).expect("a connected peer");
        round.state = RoundState::Idle;
        let admitted = std::mem::take(&mut round.admitted);
        let follow_up = if !round.waiting.is_empty() {
            round.notify = false;
            Some(std::mem::take(&mut round.waiting))
        } else if std::mem::take(&mut round.notify) {
            Some(BTreeSet::new())
        } else {
            None
        };
        self.resolve_with(admitted.into_iter(), ok);
        // a failed round does not start again on the notification alone: only a waiter asks for
        // another
        match follow_up {
            Some(next) if self.life == super::Life::Open && (ok || !next.is_empty()) => {
                self.start_round(peer, next);
            }
            Some(next) => self.resolve_with(next.into_iter(), false),
            None => {}
        }
    }

    fn cancel(&mut self, round: &PeerRound) {
        self.cancel_state(round.state);
    }

    fn cancel_state(&mut self, state: RoundState) {
        // a deferred round has completed from the driver's side: there is nothing to cancel
        if let RoundState::Active { call } = state {
            self.cancelled.insert(call);
            self.pending.push(Command::CancelRound { call });
        }
    }

    fn resolve(&mut self, waiters: impl Iterator<Item = Waiter>) {
        self.resolve_with(waiters, false);
    }

    fn resolve_with(&mut self, waiters: impl Iterator<Item = Waiter>, ok: bool) {
        if !ok && self.planted(Plant::LoseWaiters) {
            // planted: a waiter whose round did not complete is never answered
            return;
        }
        for waiter in waiters {
            self.pending.push(Command::RoundResolved { waiter, ok });
        }
    }
}
