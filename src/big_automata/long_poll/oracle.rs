//! The naive long-poll machine: no index, no deadline queue, every input scans every waiter and
//! recomputes each one's due time from what it was told. The differential tests hold
//! [`LongPoll`](super::LongPoll) to it.

use crate::kit::Instant;
use crate::rev::Rev;

use super::Linger;
use super::Output;
use super::Park;
use super::WaiterId;

struct Waiter<K> {
    id: WaiterId,
    interests: Vec<K>,
    after_rev: Rev,
    deadline: Instant,
    linger: Linger,
    /// When the first relevant commit arrived.
    first: Option<Instant>,
    amount: u64,
}

impl<K> Waiter<K> {
    fn due(&self) -> Instant {
        match self.first {
            None => self.deadline,
            Some(first) => first
                .checked_add(self.linger.window)
                .map_or(self.deadline, |end| end.min(self.deadline)),
        }
    }
}

pub(super) struct Oracle<K> {
    now: Option<Instant>,
    head: Rev,
    waiters: Vec<Waiter<K>>,
}

impl<K: PartialEq + Clone> Oracle<K> {
    pub(super) fn new() -> Self {
        Self {
            now: None,
            head: 0,
            waiters: vec![],
        }
    }

    pub(super) fn parked(&self) -> Vec<WaiterId> {
        let mut ids = self.waiters.iter().map(|w| w.id).collect::<Vec<_>>();
        ids.sort();
        ids
    }

    fn advance(&mut self, now: Instant) -> Instant {
        let now = self.now.map_or(now, |seen| seen.max(now));
        self.now = Some(now);
        now
    }

    fn expire(&mut self, now: Instant, out: &mut Vec<Output>) {
        let head = self.head;
        self.waiters.retain(|w| {
            if w.due() > now {
                return true;
            }
            out.push(match w.first {
                None => Output::TimedOut { waiter: w.id },
                Some(_) => Output::Ready {
                    waiter: w.id,
                    through_rev: head,
                },
            });
            false
        });
    }

    fn accrue(&mut self, now: Instant, id: WaiterId, amount: u64, out: &mut Vec<Output>) {
        let head = self.head;
        let ix = self.waiters.iter().position(|w| w.id == id).unwrap();
        let waiter = &mut self.waiters[ix];
        let began = waiter.first.is_none();
        if began {
            waiter.first = Some(now);
        }
        waiter.amount = waiter.amount.saturating_add(amount);
        if waiter.amount >= waiter.linger.min_amount || waiter.due() <= now {
            self.waiters.remove(ix);
            out.push(Output::Ready {
                waiter: id,
                through_rev: head,
            });
        } else if began {
            out.push(Output::Lingering {
                waiter: id,
                until: waiter.due(),
            });
        }
    }

    pub(super) fn park(
        &mut self,
        now: Instant,
        id: WaiterId,
        request: Park<'_, K>,
        out: &mut Vec<Output>,
    ) {
        let now = self.advance(now);
        self.expire(now, out);
        self.waiters.push(Waiter {
            id,
            interests: request.interests.to_vec(),
            after_rev: request.after_rev,
            deadline: request.deadline,
            linger: request.linger,
            first: None,
            amount: 0,
        });
        if request.after_rev < self.head {
            self.accrue(now, id, 0, out);
        }
        self.expire(now, out);
    }

    pub(super) fn committed(
        &mut self,
        now: Instant,
        rev: Rev,
        changes: &[(K, u64)],
        out: &mut Vec<Output>,
    ) {
        let now = self.advance(now);
        self.expire(now, out);
        self.head = self.head.max(rev);
        let hits = self
            .waiters
            .iter()
            .filter(|w| rev > w.after_rev)
            .filter_map(|w| {
                let matching = changes
                    .iter()
                    .filter(|(interest, _)| w.interests.contains(interest))
                    .collect::<Vec<_>>();
                (!matching.is_empty()).then(|| (w.id, matching.iter().map(|(_, a)| *a).sum()))
            })
            .collect::<Vec<(WaiterId, u64)>>();
        for (id, amount) in hits {
            self.accrue(now, id, amount, out);
        }
    }

    pub(super) fn tick(&mut self, now: Instant, out: &mut Vec<Output>) {
        let now = self.advance(now);
        self.expire(now, out);
    }

    pub(super) fn cancelled(&mut self, id: WaiterId) {
        self.waiters.retain(|w| w.id != id);
    }
}
