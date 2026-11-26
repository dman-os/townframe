//! The gate that decides who may hold a connection of a database.
//!
//! A database has one write connection and a few read connections. Each kind sits behind a
//! `Gate`: a counting semaphore whose waiters are woken through their own wakers, so it
//! belongs to no async runtime and needs no thread other than the caller's. Both executors use
//! it. `Inline` has nothing else to lean on; for `Pool` it means the lease path behaves the
//! same on either executor.
//!
//! Waiters are served in arrival order. A caller that arrives while others are queued waits
//! behind them even when a permit is free at that instant, and `try_acquire` does the same, so a
//! stream of short readers cannot starve the one writer. A cancelled wait leaves the queue and
//! passes its turn on.
//!
//! The gate never blocks a thread. That is what makes `Inline` safe on one thread: a task that
//! has to wait for the writer returns `Pending` and the thread goes on to run whichever task
//! holds it.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use utils_rs::expect_tags::ERROR_MUTEX;

struct Waiter {
    ticket: u64,
    waker: Waker,
}

struct State {
    permits: usize,
    next_ticket: u64,
    waiting: VecDeque<Waiter>,
}

pub(crate) struct Gate {
    state: Mutex<State>,
}

/// One held permit. Dropping it hands the permit to the longest-waiting caller.
pub(crate) struct Permit {
    gate: Arc<Gate>,
    count: usize,
}

/// The future returned by [`Gate::acquire`].
pub(crate) struct Acquire {
    gate: Arc<Gate>,
    ticket: Option<u64>,
}

impl Gate {
    pub(crate) fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                permits,
                next_ticket: 0,
                waiting: VecDeque::new(),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.state.lock().expect(ERROR_MUTEX).permits
    }

    /// Wait for one permit.
    pub(crate) fn acquire(self: &Arc<Self>) -> Acquire {
        Acquire {
            gate: Arc::clone(self),
            ticket: None,
        }
    }

    /// Take `count` permits now, or none: it fails when too few are free and also when
    /// anyone is queued, because those callers are ahead of this one.
    pub(crate) fn try_acquire(self: &Arc<Self>, count: usize) -> Option<Permit> {
        let mut state = self.state.lock().expect(ERROR_MUTEX);
        if !state.waiting.is_empty() || state.permits < count {
            return None;
        }
        state.permits -= count;
        Some(Permit {
            gate: Arc::clone(self),
            count,
        })
    }

    /// The waker of the caller next in line, cloned so that it is woken after the lock is gone.
    fn front_waker(state: &State) -> Option<Waker> {
        state.waiting.front().map(|waiter| waiter.waker.clone())
    }
}

impl Future for Acquire {
    type Output = Permit;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Permit> {
        let gate = Arc::clone(&self.gate);
        let mut state = gate.state.lock().expect(ERROR_MUTEX);
        let ticket = match self.ticket {
            Some(ticket) => ticket,
            None => {
                let ticket = state.next_ticket;
                state.next_ticket += 1;
                self.ticket = Some(ticket);
                ticket
            }
        };
        let my_turn = state
            .waiting
            .front()
            .is_none_or(|waiter| waiter.ticket == ticket);
        if my_turn && state.permits >= 1 {
            state.permits -= 1;
            if state
                .waiting
                .front()
                .is_some_and(|waiter| waiter.ticket == ticket)
            {
                state.waiting.pop_front();
            }
            // Permits may remain (several were released before anyone polled): the next in
            // line has to hear about them from us, as no release will.
            let next = if state.permits >= 1 {
                Gate::front_waker(&state)
            } else {
                None
            };
            drop(state);
            self.ticket = None;
            if let Some(next) = next {
                next.wake();
            }
            return Poll::Ready(Permit {
                gate: Arc::clone(&self.gate),
                count: 1,
            });
        }
        match state
            .waiting
            .iter_mut()
            .find(|waiter| waiter.ticket == ticket)
        {
            Some(waiter) => waiter.waker.clone_from(context.waker()),
            None => state.waiting.push_back(Waiter {
                ticket,
                waker: context.waker().clone(),
            }),
        }
        Poll::Pending
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        let mut state = self.gate.state.lock().expect(ERROR_MUTEX);
        let Some(position) = state
            .waiting
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        else {
            return;
        };
        state.waiting.remove(position);
        // A waiter that gave up at the front leaves its turn, and free permits, to the next.
        let next = if position == 0 && state.permits >= 1 {
            Gate::front_waker(&state)
        } else {
            None
        };
        drop(state);
        if let Some(next) = next {
            next.wake();
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().expect(ERROR_MUTEX);
        state.permits += self.count;
        let next = Gate::front_waker(&state);
        drop(state);
        if let Some(next) = next {
            next.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Poll `future` once with a waker that does nothing.
    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn a_free_permit_is_taken_at_once_and_returned_on_drop() {
        let gate = Gate::new(1);
        let mut acquire = Box::pin(gate.acquire());
        let Poll::Ready(permit) = poll_once(acquire.as_mut()) else {
            panic!("a free gate admits at once");
        };
        assert_eq!(gate.available_permits(), 0);
        drop(permit);
        assert_eq!(gate.available_permits(), 1);
    }

    #[test]
    fn waiters_are_served_in_arrival_order() {
        let gate = Gate::new(1);
        let Poll::Ready(held) = poll_once(Box::pin(gate.acquire()).as_mut()) else {
            panic!("a free gate admits at once");
        };
        let mut first = Box::pin(gate.acquire());
        let mut second = Box::pin(gate.acquire());
        assert!(poll_once(first.as_mut()).is_pending());
        assert!(poll_once(second.as_mut()).is_pending());
        drop(held);
        // The later arrival polls first and still has to wait for its turn.
        assert!(poll_once(second.as_mut()).is_pending());
        let Poll::Ready(first_permit) = poll_once(first.as_mut()) else {
            panic!("the first waiter is next");
        };
        assert!(poll_once(second.as_mut()).is_pending());
        drop(first_permit);
        assert!(poll_once(second.as_mut()).is_ready());
    }

    #[test]
    fn a_cancelled_waiter_passes_its_turn_on() {
        let gate = Gate::new(1);
        let Poll::Ready(held) = poll_once(Box::pin(gate.acquire()).as_mut()) else {
            panic!("a free gate admits at once");
        };
        let mut first = Box::pin(gate.acquire());
        let mut second = Box::pin(gate.acquire());
        assert!(poll_once(first.as_mut()).is_pending());
        assert!(poll_once(second.as_mut()).is_pending());
        drop(first);
        drop(held);
        assert!(poll_once(second.as_mut()).is_ready());
    }

    #[test]
    fn try_acquire_does_not_overtake_a_queued_waiter() {
        let gate = Gate::new(2);
        let held = gate.try_acquire(2).expect("both permits are free");
        let mut waiter = Box::pin(gate.acquire());
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(held);
        assert!(gate.try_acquire(1).is_none(), "the waiter is ahead");
        assert!(poll_once(waiter.as_mut()).is_ready());
    }

    #[test]
    fn a_release_wakes_the_waiter() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        struct Counter(AtomicUsize);
        impl std::task::Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let gate = Gate::new(1);
        let held = gate.try_acquire(1).expect("free");
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut waiter = Box::pin(gate.acquire());
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        drop(held);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    }
}
