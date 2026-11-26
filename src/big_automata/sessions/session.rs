//! The Session machine: which sessions to the peers exist, and when.
//!
//! A *session* is a connection a peer is reached over, as one of our principals: it is named by
//! `(peer, principal)`, and the machine is generic over both types. It decides when to dial, how
//! to back off, how many may exist, and when to close one. Dialing and closing themselves are
//! commands: a driver does them and reports back. It is sans-IO like every machine in this
//! crate: no clock reads (the clock is a parameter), no awaits, and every outcome is an input.
//!
//! # What makes a session wanted
//!
//! - It is *needed*: the Interest machine asked for it ([`Sessions::need`]) and has not released
//!   it ([`Sessions::release`]). Needs are an edge, not a count: the Interest machine reports
//!   the first object that needs a session and the last that stops.
//! - It is the peer's *default* session: [`Sessions::peer_connected`] names the principal of the
//!   session that exists toward a connected peer whatever anyone asks for (our own node
//!   principal). It stays for as long as the peer is connected, and is never closed for
//!   idleness. Every other session is closed once nobody needs it.
//!
//! # The states of a session
//!
//! - `Wanted`: it should exist and does not. It is either not yet admitted (a cap is full, or the
//!   peer is not connected), or admitted: queued, or backing off after a failed dial.
//! - `Dialing { call }`: a [`Command::Dial`] is out.
//! - `Open`: the driver reports it up. The Interest machine routes objects on open sessions only.
//! - `Idle(grace)`: an open session nobody wants. Closed at the end of the grace, unless wanted
//!   again. As in the Interest machine the deadline is stamped by the next flush.
//! - `Closed`: no entry at all.
//!
//! The dial attempts of one session are one task of an embedded
//! [`KeyedScheduler`](crate::kit::scheduler::keyed::KeyedScheduler), so a failed dial retries
//! after an exponentially growing backoff, and at most one dial per session is out. A dial that
//! is out is never cancelled, because a cancelled dial may still finish at the remote and leave
//! a connection nobody knows of: it ends in `dialed`, and a session found unwanted by then
//! simply goes idle.
//!
//! # Inputs and commands
//!
//! Inputs: [`Sessions::need`], [`Sessions::release`], [`Sessions::peer_connected`],
//! [`Sessions::peer_disconnected`], [`Sessions::dialed`] and [`Sessions::closed`] (an open
//! session was lost; it is dialed again, if still wanted, after a backoff that doubles with each
//! loss that follows an open shorter than the maximum backoff, so a connection that flaps is not
//! dialed in a loop). The outcome inputs take `now`, as `Outbox::fetch_ended` does, because they
//! start a backoff.
//!
//! Commands: [`Command::Dial`] (a [`CallId`] names the attempt) and [`Command::Close`].
//!
//! # Laws
//!
//! After every flush:
//!
//! 1. **Caps hold.** At most `per_peer` sessions to one peer and `total` overall are past
//!    admission (`Dialing`, `Open`, `Idle`, or `Wanted` and admitted).
//! 2. **Only what is wanted.** A session exists only if it is needed, is a connected peer's
//!    default, is idle within its grace, or has a dial out.
//! 3. **Grace.** An idle session is closed at the first flush at or after its deadline, and
//!    not before; a need before then makes it open again.
//! 4. **Defaults stay.** A connected peer's default session is never idle and never closed
//!    for idleness.
//! 5. **Re-dial only while wanted, and not at once.** A lost or failed session is dialed again
//!    only if it is still wanted and its peer connected, and a lost one not before its backoff.
//! 6. **One dial per session**, and none to a peer that is not connected.
//! 7. **Admitted promptly.** A wanted session whose peer is connected and which fits under
//!    the caps is admitted at the next flush, defaults first.
//! 8. **No close without an open.** [`Command::Close`] is for sessions that were open or idle.

use std::collections::BTreeMap;
use std::fmt::Debug;

use super::Grace;
#[cfg(test)]
use super::Plant;
use crate::interlude::*;
use crate::kit::Instant;
use crate::kit::scheduler::keyed::KeyedScheduler;

pub type CallId = u64;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// How long an unwanted session stays open.
    pub grace: Duration,
    /// The most sessions to one peer.
    pub per_peer: usize,
    /// The most sessions in all.
    pub total: usize,
    /// The first retry delay of a failed dial; later ones double.
    pub backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<P, L> {
    Dial { call: CallId, peer: P, principal: L },
    Close { peer: P, principal: L },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Wanted { admitted: bool },
    Dialing { call: CallId },
    Open,
    Idle(Grace),
}

#[derive(Debug)]
struct Entry {
    phase: Phase,
    needed: bool,
    /// The default session of a connected peer.
    default: bool,
    /// Losses in a row, for the re-dial backoff.
    lost: u32,
    /// Not admitted before this: the backoff of a lost session.
    retry_at: Option<Instant>,
    opened_at: Option<Instant>,
}

impl Entry {
    fn wanted(&self) -> bool {
        self.needed || self.default
    }

    /// Past admission: it holds a slot under the caps.
    fn holds_slot(&self) -> bool {
        !matches!(self.phase, Phase::Wanted { admitted: false })
    }
}

#[derive(Debug)]
pub struct Sessions<P, L> {
    cfg: Config,
    /// Connected peers and the principal of their default session.
    peers: BTreeMap<P, L>,
    entries: BTreeMap<(P, L), Entry>,
    /// One task per admitted session that is queued, backing off or dialing.
    dials: KeyedScheduler<(P, L), ()>,
    to_close: Vec<(P, L)>,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl<P, L> Sessions<P, L>
where
    P: Ord + Clone + Debug,
    L: Ord + Clone + Debug,
{
    pub fn new(cfg: Config) -> Self {
        assert!(cfg.per_peer >= 1 && cfg.total >= cfg.per_peer);
        let mut dials = KeyedScheduler::default();
        dials.set_max_backoff(cfg.max_backoff);
        Self {
            cfg,
            peers: default(),
            entries: default(),
            dials,
            to_close: default(),
            #[cfg(test)]
            plant: None,
        }
    }

    #[cfg(test)]
    pub(super) fn plant(&mut self, plant: Plant) {
        self.plant = Some(plant);
    }

    /// The Interest machine needs the session `(peer, principal)`.
    pub fn need(&mut self, peer: P, principal: L) {
        let entry = self.entry(peer, principal);
        entry.needed = true;
        if matches!(entry.phase, Phase::Idle(_)) {
            entry.phase = Phase::Open;
        }
    }

    /// Nothing needs the session any more.
    pub fn release(&mut self, peer: P, principal: L) {
        let key = (peer, principal);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.needed = false;
            self.settle(&key);
        }
    }

    /// A peer is connected, and `default` is the principal of the session that exists toward it
    /// whatever anyone needs.
    pub fn peer_connected(&mut self, peer: P, default: L) {
        let previous = self.peers.insert(peer.clone(), default.clone());
        assert!(previous.is_none(), "a peer was connected twice");
        let entry = self.entry(peer, default);
        entry.default = true;
        if matches!(entry.phase, Phase::Idle(_)) {
            entry.phase = Phase::Open;
        }
    }

    /// The peer's connection is gone, and its sessions with it. What is still needed waits for
    /// the peer to connect again.
    pub fn peer_disconnected(&mut self, peer: &P) {
        self.peers
            .remove(peer)
            .expect("a peer that was not connected");
        let keys: Vec<_> = self
            .entries
            .keys()
            .filter(|(p, _)| p == peer)
            .cloned()
            .collect();
        for key in keys {
            let entry = self.entries.get_mut(&key).expect(ERROR_IMPOSSIBLE);
            entry.default = false;
            match entry.phase {
                Phase::Open | Phase::Idle(_) => {
                    self.to_close.push(key.clone());
                    self.lose(&key, None);
                }
                Phase::Wanted { admitted } => {
                    if admitted {
                        self.dials.cancel(&key);
                    }
                    self.lose(&key, None);
                }
                // The dial ends in `dialed`, which finds the peer gone.
                Phase::Dialing { .. } => {}
            }
        }
    }

    /// The outcome of a dial.
    pub fn dialed(&mut self, now: Instant, call: CallId, ok: bool) {
        let Some(key) = self.dials.key_of(call).cloned() else {
            panic!("a dial outcome for a call that is not in flight");
        };
        let entry = self.entries.get_mut(&key).expect(ERROR_IMPOSSIBLE);
        assert_eq!(entry.phase, Phase::Dialing { call });
        let connected = self.peers.contains_key(&key.0);
        if ok {
            self.dials.complete(call).expect(ERROR_IMPOSSIBLE);
            if connected {
                entry.phase = Phase::Open;
                entry.opened_at = Some(now);
                self.settle(&key);
            } else {
                // The peer left while the dial was out: the session is not wanted any more.
                self.to_close.push(key.clone());
                self.lose(&key, None);
            }
        } else if connected && entry.wanted() {
            self.dials
                .fail(call, self.cfg.backoff, now)
                .expect(ERROR_IMPOSSIBLE);
            entry.phase = Phase::Wanted { admitted: true };
        } else {
            self.dials.complete(call).expect(ERROR_IMPOSSIBLE);
            self.lose(&key, None);
        }
    }

    /// An open or idle session was lost without being closed by us.
    pub fn closed(&mut self, now: Instant, peer: P, principal: L) {
        let key = (peer, principal);
        let Some(entry) = self.entries.get_mut(&key) else {
            return;
        };
        if matches!(entry.phase, Phase::Open | Phase::Idle(_)) {
            if entry
                .opened_at
                .is_some_and(|at| now - at >= self.cfg.max_backoff)
            {
                entry.lost = 0;
            }
            let delay = self
                .cfg
                .backoff
                .saturating_mul(1u32 << entry.lost.min(16))
                .min(self.cfg.max_backoff);
            entry.lost += 1;
            self.lose(&key, Some(now + delay));
        }
    }

    pub fn next_due(&self) -> Option<Instant> {
        let deadlines = self.entries.values().filter_map(|entry| match entry.phase {
            Phase::Idle(Grace::Until(until)) => Some(until),
            Phase::Wanted { admitted: false } => entry.retry_at,
            _ => None,
        });
        deadlines.chain(self.dials.next_due()).min()
    }

    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command<P, L>>) {
        for (peer, principal) in std::mem::take(&mut self.to_close) {
            out.push(Command::Close { peer, principal });
        }
        for entry in self.entries.values_mut() {
            if entry.phase == Phase::Idle(Grace::Starting) {
                entry.phase = Phase::Idle(Grace::Until(now + self.cfg.grace));
            }
        }
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| matches!(e.phase, Phase::Idle(Grace::Until(until)) if until <= now))
            .map(|(key, _)| key.clone())
            .collect();
        #[cfg(test)]
        let expired = if self.plant == Some(Plant::NeverClose) {
            vec![]
        } else {
            expired
        };
        for key in expired {
            self.entries.remove(&key);
            out.push(Command::Close {
                peer: key.0,
                principal: key.1,
            });
        }
        self.admit(now);
        self.dials.tick(now);
        assert_eq!(
            self.dials.drain_stop_queue().count(),
            0,
            "a dial was stopped"
        );
        let spawned: Vec<_> = self.dials.drain_spawn_queue().collect();
        for task in spawned {
            let key = self.dials.key_of(task.id).expect(ERROR_IMPOSSIBLE);
            let entry = self.entries.get_mut(key).expect(ERROR_IMPOSSIBLE);
            assert_eq!(entry.phase, Phase::Wanted { admitted: true });
            entry.phase = Phase::Dialing { call: task.id };
            out.push(Command::Dial {
                call: task.id,
                peer: key.0.clone(),
                principal: key.1.clone(),
            });
        }
        #[cfg(test)]
        if self.plant.is_none() {
            self.check();
        }
    }

    pub fn phase(&self, peer: &P, principal: &L) -> Option<Phase> {
        self.entries
            .get(&(peer.clone(), principal.clone()))
            .map(|entry| entry.phase)
    }

    /// Every session that exists, in order.
    pub fn sessions(&self) -> impl Iterator<Item = (&P, &L, Phase)> {
        self.entries
            .iter()
            .map(|((peer, principal), entry)| (peer, principal, entry.phase))
    }

    fn entry(&mut self, peer: P, principal: L) -> &mut Entry {
        self.entries.entry((peer, principal)).or_insert(Entry {
            phase: Phase::Wanted { admitted: false },
            needed: false,
            default: false,
            lost: 0,
            retry_at: None,
            opened_at: None,
        })
    }

    /// An unwanted session winds down: a queued or backing-off one is dropped, an open one goes
    /// idle, and one that is dialing is left to finish.
    fn settle(&mut self, key: &(P, L)) {
        let entry = self.entries.get_mut(key).expect(ERROR_IMPOSSIBLE);
        if entry.wanted() {
            return;
        }
        match entry.phase {
            Phase::Wanted { admitted } => {
                if admitted {
                    self.dials.cancel(key);
                }
                self.entries.remove(key);
            }
            Phase::Open => {
                entry.phase = Phase::Idle(Grace::Starting);
            }
            Phase::Dialing { .. } | Phase::Idle(_) => {}
        }
    }

    /// The session is gone (lost, or closed by us): it is wanted again, to be dialed afresh
    /// when admitted, or it ceases to exist.
    fn lose(&mut self, key: &(P, L), retry_at: Option<Instant>) {
        let entry = self.entries.get_mut(key).expect(ERROR_IMPOSSIBLE);
        if entry.wanted() {
            entry.phase = Phase::Wanted { admitted: false };
            entry.retry_at = retry_at;
        } else {
            self.entries.remove(key);
        }
    }

    /// Admit wanted sessions to the dialing queue while the caps allow, defaults first.
    fn admit(&mut self, now: Instant) {
        let mut total = self.entries.values().filter(|e| e.holds_slot()).count();
        let mut per_peer: BTreeMap<P, usize> = BTreeMap::new();
        for ((peer, _), entry) in &self.entries {
            if entry.holds_slot() {
                *per_peer.entry(peer.clone()).or_default() += 1;
            }
        }
        let mut candidates: Vec<_> = self
            .entries
            .iter()
            .filter(|((peer, _), e)| {
                e.phase == (Phase::Wanted { admitted: false })
                    && self.peers.contains_key(peer)
                    && e.retry_at.is_none_or(|at| at <= now)
            })
            .map(|(key, e)| (!e.default, key.clone()))
            .collect();
        candidates.sort();
        let mut admitted = vec![];
        for (_, key) in candidates {
            let held = per_peer.entry(key.0.clone()).or_default();
            let full = total >= self.cfg.total || *held >= self.cfg.per_peer;
            // planted: the caps are not enforced
            #[cfg(test)]
            let full = full && self.plant != Some(Plant::NoCaps);
            if full {
                continue;
            }
            total += 1;
            *held += 1;
            admitted.push(key);
        }
        for key in admitted {
            let task = self.dials.replace(now, key.clone(), ());
            debug_assert!(task.is_some());
            let entry = self.entries.get_mut(&key).expect(ERROR_IMPOSSIBLE);
            entry.phase = Phase::Wanted { admitted: true };
            entry.retry_at = None;
        }
    }

    /// The machine's own invariants, asserted at the end of every flush in test builds.
    #[cfg(test)]
    fn check(&self) {
        let mut per_peer: BTreeMap<&P, usize> = BTreeMap::new();
        for ((peer, _), entry) in &self.entries {
            if entry.holds_slot() {
                *per_peer.entry(peer).or_default() += 1;
            }
            assert!(entry.wanted() || !matches!(entry.phase, Phase::Wanted { .. }));
            if entry.default {
                assert!(self.peers.contains_key(peer));
                assert!(!matches!(entry.phase, Phase::Idle(_)));
            }
        }
        assert!(per_peer.values().all(|n| *n <= self.cfg.per_peer));
        assert!(self.entries.values().filter(|e| e.holds_slot()).count() <= self.cfg.total);
    }
}
