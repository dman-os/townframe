//! The long-poll driver: a shared [`LongPoll`] behind one lock, and the future a parked poll
//! awaits.
//!
//! It spawns nothing and reads no runtime's clock or timer. Time comes from the injected
//! [`Clock`] and from the [`Timer`] a poll waits with, so it builds for
//! `wasm32-unknown-unknown` and a simulation can end every sleep at once.
//!
//! - The source's commit path calls [`LongPollHub::committed`], synchronously, after the commit
//!   is visible to reads. The machine decides which waiters it concerns, and each of those
//!   (and only those) is woken through its own channel.
//! - A poll calls [`LongPollHub::park`], which parks at once (no `.await` between deciding to
//!   wait and being indexed), and awaits [`Waiting::wait`]. That future sleeps on its own
//!   timer, for its deadline or its window, and ticks the machine when the timer fires, so no
//!   task has to run the machine's clock.
//! - Dropping a [`Waiting`] cancels the waiter.

use tokio::sync::watch;

use crate::Clock;
use crate::interlude::*;
use crate::kit::scheduler::Instant;
use crate::rev::Rev;

use super::Linger;
use super::LongPoll;
use super::Output;
use super::Park;
use super::WaiterId;

/// A runtime's sleep. Dropping the future cancels it. The future is `Send` and owns what it
/// needs, so it does not borrow the timer.
pub trait Timer {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<Self>;
}

/// The runtime's own sleep: tokio's natively, the browser's `setTimeout` on wasm.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeTimer;

impl Timer for RuntimeTimer {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        n0_future::time::sleep(duration)
    }
}

/// How a poll ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Read the page now, up to `through_rev`.
    Ready {
        through_rev: Rev,
    },
    TimedOut,
}

/// What a parked poll's slot says.
#[derive(Debug, Clone, Copy)]
enum Progress {
    /// Parked, next due at this instant: its deadline, or the end of its window.
    Waiting(Instant),
    Done(Outcome),
}

struct Inner<K> {
    machine: LongPoll<K>,
    slots: BTreeMap<WaiterId, watch::Sender<Progress>>,
    next_id: u64,
    /// The machine's outputs of the call in progress, reused.
    out: Vec<Output>,
}

impl<K: Ord + Clone> Inner<K> {
    /// Hand the call's outputs to their waiters. A terminal output removes the slot.
    fn dispatch(&mut self) {
        let Self { slots, out, .. } = self;
        for output in out.drain(..) {
            match output {
                Output::Lingering { waiter, until } => {
                    slots[&waiter].send_replace(Progress::Waiting(until));
                }
                Output::Ready {
                    waiter,
                    through_rev,
                } => {
                    let slot = slots.remove(&waiter).expect(ERROR_IMPOSSIBLE);
                    slot.send_replace(Progress::Done(Outcome::Ready { through_rev }));
                }
                Output::TimedOut { waiter } => {
                    let slot = slots.remove(&waiter).expect(ERROR_IMPOSSIBLE);
                    slot.send_replace(Progress::Done(Outcome::TimedOut));
                }
            }
        }
    }
}

/// The parked polls of one source. Clones share them.
pub struct LongPollHub<K> {
    inner: Arc<surelock::mutex::Mutex<Inner<K>>>,
    clock: Clock,
}

impl<K> Clone for LongPollHub<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            clock: Arc::clone(&self.clock),
        }
    }
}

impl<K: Ord + Clone> LongPollHub<K> {
    pub fn new(clock: Clock) -> Self {
        Self {
            inner: Arc::new(surelock::mutex::Mutex::new(Inner {
                machine: LongPoll::new(),
                slots: default(),
                next_id: 0,
                out: default(),
            })),
            clock,
        }
    }

    /// The clock the hub reads, for a second hub that must tell the same time.
    pub fn clock(&self) -> Clock {
        Arc::clone(&self.clock)
    }

    /// The hub's clock reading: what a poll's deadline is measured from.
    pub fn now(&self) -> Instant {
        (self.clock)()
    }

    fn with<R>(&self, step: impl FnOnce(&mut Inner<K>) -> R) -> R {
        surelock::key::lock_scope(|key| {
            let (mut inner, _key) = key.lock(&self.inner);
            let stepped = step(&mut inner);
            inner.dispatch();
            stepped
        })
    }

    /// The source committed `rev`, changing `changes` (each interest with the amount it
    /// gained). Call it after the commit is visible to reads, from the commit path. It wakes
    /// the waiters those interests concern and no others.
    pub fn committed(&self, rev: Rev, changes: impl IntoIterator<Item = (K, u64)>) {
        let now = self.now();
        self.with(|inner| {
            let Inner { machine, out, .. } = inner;
            machine.committed(now, rev, changes, out);
        });
    }

    /// Park a poll that has read through `after_rev`, to be ended by a relevant commit or by
    /// `deadline`. It is parked when this returns: a commit after this call reaches it.
    pub fn park(
        &self,
        interests: &[K],
        after_rev: Rev,
        deadline: Instant,
        linger: Linger,
    ) -> Waiting<K> {
        let now = self.now();
        let (id, slot) = self.with(|inner| {
            let id = WaiterId(inner.next_id);
            inner.next_id += 1;
            let (slot, watching) = watch::channel(Progress::Waiting(deadline));
            inner.slots.insert(id, slot);
            let Inner { machine, out, .. } = inner;
            machine.park(
                now,
                id,
                Park {
                    interests,
                    after_rev,
                    deadline,
                    linger,
                },
                out,
            );
            (id, watching)
        });
        Waiting {
            hub: self.clone(),
            id,
            slot,
            ended: false,
        }
    }

    /// A timer fired at `at`: end every waiter due by then.
    fn tick(&self, at: Instant) {
        self.with(|inner| {
            let Inner { machine, out, .. } = inner;
            machine.tick(at, out);
        });
    }

    fn cancel(&self, id: WaiterId) {
        self.with(|inner| {
            inner.machine.cancelled(id);
            inner.slots.remove(&id);
        });
    }

    /// How many polls are parked.
    pub fn parked(&self) -> usize {
        self.with(|inner| inner.machine.parked())
    }

    /// How many waiters commits and timers have looked at since the hub began.
    pub fn touched(&self) -> u64 {
        self.with(|inner| inner.machine.touched())
    }
}

/// One parked poll. Awaiting [`Waiting::wait`] ends it with an [`Outcome`]; dropping it first
/// cancels it, leaving nothing parked.
pub struct Waiting<K: Ord + Clone> {
    hub: LongPollHub<K>,
    id: WaiterId,
    slot: watch::Receiver<Progress>,
    ended: bool,
}

impl<K: Ord + Clone> Waiting<K> {
    /// How the poll ended, if it has.
    pub fn outcome(&self) -> Option<Outcome> {
        match *self.slot.borrow() {
            Progress::Done(outcome) => Some(outcome),
            Progress::Waiting(_) => None,
        }
    }

    /// Wait for a relevant commit (and the end of its window) or the deadline.
    ///
    /// Cancel safe: dropping the future drops the poll, and with it the waiter and the sleep.
    pub async fn wait(mut self, timer: &impl Timer) -> Outcome {
        loop {
            let due = match *self.slot.borrow_and_update() {
                Progress::Done(outcome) => {
                    self.ended = true;
                    return outcome;
                }
                Progress::Waiting(due) => due,
            };
            let sleep = timer.sleep(due.saturating_duration_since(self.hub.now()));
            let timer_fired = {
                let mut changed = std::pin::pin!(self.slot.changed());
                let mut sleep = std::pin::pin!(sleep);
                match futures::future::select(changed.as_mut(), sleep.as_mut()).await {
                    futures::future::Either::Left((changed, _)) => {
                        changed.expect("a parked poll's slot outlives it");
                        false
                    }
                    futures::future::Either::Right(((), _)) => true,
                }
            };
            if timer_fired {
                // The timer ran out the time to `due`, whatever the clock reads, so the
                // machine is told that time and the clock only sizes the sleep.
                self.hub.tick(due);
            }
        }
    }
}

impl<K: Ord + Clone> Drop for Waiting<K> {
    fn drop(&mut self) {
        if !self.ended {
            self.hub.cancel(self.id);
        }
    }
}
