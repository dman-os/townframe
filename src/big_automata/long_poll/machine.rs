//! The long-poll machine: sans-IO, one value that holds every parked poll. See the
//! [module](super) docs for the laws.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::interlude::*;
use crate::kit::Instant;
use crate::rev::Rev;

/// Names one parked poll. The caller picks it and never reuses a live one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WaiterId(pub u64);

/// How long a poll waits, after its first relevant commit, for more to arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Linger {
    /// The window, fixed from the first relevant commit: later commits do not extend it.
    pub window: Duration,
    /// The poll is ready at once when the amounts of its relevant commits sum to this much
    /// (a full page). `0` makes the first relevant commit enough, so the window never runs.
    pub min_amount: u64,
}

impl Linger {
    /// Ready on the first relevant commit.
    pub const NONE: Self = Self {
        window: Duration::ZERO,
        min_amount: 0,
    };
}

/// What [`LongPoll::park`] takes: one poll's request.
#[derive(Debug, Clone, Copy)]
pub struct Park<'a, K> {
    /// What the poll watches. Order and repeats do not matter; none means only the deadline
    /// can end it.
    pub interests: &'a [K],
    /// The poll has read everything up to this rev: commits at or below it are not news.
    pub after_rev: Rev,
    pub deadline: Instant,
    pub linger: Linger,
}

/// What the machine tells the driver. `Ready` and `TimedOut` are terminal: the waiter is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Output {
    /// The waiter's first relevant commit arrived and its window runs until `until`: its next
    /// wake is then, not at its deadline. Not terminal.
    Lingering {
        waiter: WaiterId,
        until: Instant,
    },
    /// The poll should read its page now, up to `through_rev`: the highest rev the machine has
    /// been told of, which is past the waiter's `after_rev`.
    Ready {
        waiter: WaiterId,
        through_rev: Rev,
    },
    TimedOut {
        waiter: WaiterId,
    },
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Parked,
    /// `until` is `min(first relevant commit + window, deadline)`; `amount` is what the
    /// relevant commits since have summed to.
    Lingering {
        until: Instant,
        amount: u64,
    },
}

#[derive(Debug)]
struct Waiter<K> {
    interests: Box<[K]>,
    after_rev: Rev,
    deadline: Instant,
    linger: Linger,
    phase: Phase,
}

impl<K> Waiter<K> {
    /// When the machine next owes this waiter an output.
    fn due(&self) -> Instant {
        match self.phase {
            Phase::Parked => self.deadline,
            Phase::Lingering { until, .. } => until,
        }
    }
}

/// Every parked poll of one source, indexed by what it watches.
///
/// `K` is the interest: a route, a namespace, a drawer. A commit names the interests it changed
/// with an amount each, and costs `O(changed interests + the waiters watching them)`, however
/// many other waiters are parked.
#[derive(Debug)]
pub struct LongPoll<K> {
    /// The latest `now` any input carried. Time never goes back.
    now: Option<Instant>,
    /// The highest rev any commit named.
    head: Rev,
    waiters: BTreeMap<WaiterId, Waiter<K>>,
    /// Interest to the waiters watching it: exactly the parked waiters' interests (law 8).
    index: BTreeMap<K, BTreeSet<WaiterId>>,
    /// Each waiter's next due time (law 8).
    due: BTreeSet<(Instant, WaiterId)>,
    /// A commit's relevant waiters and what it adds to each, reused across commits.
    hit: BTreeMap<WaiterId, u64>,
    /// Waiters looked at by commits and expiries: the work counter law 9 is checked against.
    touched: u64,
}

impl<K> Default for LongPoll<K> {
    fn default() -> Self {
        Self {
            now: None,
            head: 0,
            waiters: default(),
            index: default(),
            due: default(),
            hit: default(),
            touched: 0,
        }
    }
}

impl<K: Ord + Clone> LongPoll<K> {
    pub fn new() -> Self {
        default()
    }

    /// The highest rev any commit named.
    pub fn head(&self) -> Rev {
        self.head
    }

    pub fn parked(&self) -> usize {
        self.waiters.len()
    }

    pub fn is_parked(&self, waiter: WaiterId) -> bool {
        self.waiters.contains_key(&waiter)
    }

    /// How many waiters commits and expiries have looked at, for cost checks.
    pub fn touched(&self) -> u64 {
        self.touched
    }

    /// When the next output is owed, if any waiter is parked.
    pub fn next_due(&self) -> Option<Instant> {
        self.due.first().map(|(at, _)| *at)
    }

    /// When `waiter`'s next output is owed.
    pub fn due_of(&self, waiter: WaiterId) -> Option<Instant> {
        self.waiters.get(&waiter).map(Waiter::due)
    }

    /// Time only moves forward: a late input's `now` is read as the latest one seen.
    fn advance(&mut self, now: Instant) -> Instant {
        let now = self.now.map_or(now, |seen| seen.max(now));
        self.now = Some(now);
        now
    }

    /// Every input first ends the waiters due by `now`, so a commit that arrives after a
    /// deadline never revives its waiter, whether or not anyone ticked.
    fn expire(&mut self, now: Instant, out: &mut Vec<Output>) {
        while let Some(&(at, waiter)) = self.due.first() {
            if at > now {
                break;
            }
            self.touched += 1;
            let ended = self.remove(waiter).expect(ERROR_IMPOSSIBLE);
            out.push(match ended.phase {
                Phase::Parked => Output::TimedOut { waiter },
                Phase::Lingering { .. } => Output::Ready {
                    waiter,
                    through_rev: self.head,
                },
            });
        }
    }

    fn remove(&mut self, waiter: WaiterId) -> Option<Waiter<K>> {
        let ended = self.waiters.remove(&waiter)?;
        self.due.remove(&(ended.due(), waiter));
        for interest in ended.interests.iter() {
            let watchers = self.index.get_mut(interest).expect(ERROR_IMPOSSIBLE);
            watchers.remove(&waiter);
            if watchers.is_empty() {
                self.index.remove(interest);
            }
        }
        Some(ended)
    }

    /// Park a poll. It is ready at once, or after its linger, if `after_rev` is behind a rev
    /// the machine has already been told of (law 6): the machine keeps no per-interest history,
    /// so it cannot tell whether that commit was relevant, and says yes.
    ///
    /// Panics if `waiter` is parked: ids are the caller's to keep unique.
    pub fn park(
        &mut self,
        now: Instant,
        waiter: WaiterId,
        request: Park<'_, K>,
        out: &mut Vec<Output>,
    ) {
        let now = self.advance(now);
        self.expire(now, out);
        assert!(
            !self.waiters.contains_key(&waiter),
            "waiter {waiter:?} is already parked"
        );
        let mut interests = request.interests.to_vec();
        interests.sort_unstable();
        interests.dedup();
        for interest in &interests {
            self.index
                .entry(interest.clone())
                .or_default()
                .insert(waiter);
        }
        self.due.insert((request.deadline, waiter));
        self.waiters.insert(
            waiter,
            Waiter {
                interests: interests.into_boxed_slice(),
                after_rev: request.after_rev,
                deadline: request.deadline,
                linger: request.linger,
                phase: Phase::Parked,
            },
        );
        if request.after_rev < self.head {
            self.accrue(now, waiter, 0, out);
        }
        // A deadline already behind `now`.
        self.expire(now, out);
    }

    /// A commit at `rev` changed `changes`: each interest with the amount it gained (rows, say).
    /// Interests should be distinct within one commit; a repeat adds to the first.
    ///
    /// Commits may be told out of order, since a store's committers race to notify: a commit
    /// at or below a waiter's `after_rev` is not news to it, whenever it arrives.
    pub fn committed(
        &mut self,
        now: Instant,
        rev: Rev,
        changes: impl IntoIterator<Item = (K, u64)>,
        out: &mut Vec<Output>,
    ) {
        let now = self.advance(now);
        self.expire(now, out);
        self.head = self.head.max(rev);
        let mut hit = std::mem::take(&mut self.hit);
        for (interest, amount) in changes {
            let Some(watchers) = self.index.get(&interest) else {
                continue;
            };
            for waiter in watchers {
                self.touched += 1;
                if rev > self.waiters[waiter].after_rev {
                    *hit.entry(*waiter).or_default() += amount;
                }
            }
        }
        for (waiter, amount) in &hit {
            self.accrue(now, *waiter, *amount, out);
        }
        hit.clear();
        self.hit = hit;
    }

    /// Advance time: end every waiter due by `now`.
    pub fn tick(&mut self, now: Instant, out: &mut Vec<Output>) {
        let now = self.advance(now);
        self.expire(now, out);
    }

    /// The waiter's poll was dropped. It leaves without an output; a waiter that already
    /// ended or never was is a no-op, so a cancel racing a terminal output is harmless.
    pub fn cancelled(&mut self, waiter: WaiterId) {
        self.remove(waiter);
    }

    /// A relevant commit worth `amount` reached `waiter`: start its window if it has none,
    /// and end it if the window or the amount is done.
    fn accrue(&mut self, now: Instant, waiter: WaiterId, amount: u64, out: &mut Vec<Output>) {
        let entry = self.waiters.get_mut(&waiter).expect(ERROR_IMPOSSIBLE);
        let (until, total, began) = match entry.phase {
            Phase::Parked => {
                let until = now
                    .checked_add(entry.linger.window)
                    .map_or(entry.deadline, |end| end.min(entry.deadline));
                self.due.remove(&(entry.deadline, waiter));
                self.due.insert((until, waiter));
                (until, amount, true)
            }
            Phase::Lingering {
                until,
                amount: seen,
            } => (until, seen.saturating_add(amount), false),
        };
        entry.phase = Phase::Lingering {
            until,
            amount: total,
        };
        if total >= entry.linger.min_amount || until <= now {
            self.remove(waiter);
            out.push(Output::Ready {
                waiter,
                through_rev: self.head,
            });
        } else if began {
            out.push(Output::Lingering { waiter, until });
        }
    }
}

#[cfg(test)]
impl<K: Ord + Clone + std::fmt::Debug> LongPoll<K> {
    /// Law 8, for tests: the index and the due queue hold exactly the parked waiters.
    pub(super) fn audit(&self) {
        let indexed = self
            .index
            .iter()
            .flat_map(|(interest, ws)| ws.iter().map(move |w| (interest.clone(), *w)))
            .collect::<BTreeSet<_>>();
        let expected = self
            .waiters
            .iter()
            .flat_map(|(w, waiter)| waiter.interests.iter().map(move |i| (i.clone(), *w)))
            .collect::<BTreeSet<_>>();
        assert_eq!(indexed, expected, "the index holds exactly the parked");
        assert!(
            self.index.values().all(|ws| !ws.is_empty()),
            "no empty index entries"
        );
        assert_eq!(
            self.due,
            self.waiters
                .iter()
                .map(|(w, waiter)| (waiter.due(), *w))
                .collect::<BTreeSet<_>>(),
            "the due queue holds each parked waiter once, at its due time"
        );
        assert!(self.hit.is_empty());
    }
}
