//! The long-poll tests: a `law_N_...` per law of the [parent module](super), the machine
//! against its naive oracle, a cost check, and the driver on a paused clock.

use std::sync::LazyLock;

use super::*;
use crate::interlude::*;
use crate::kit::Instant;
use crate::rev::Rev;

mod differential;
mod hub;
mod laws;

static T0: LazyLock<Instant> = LazyLock::new(Instant::now);

/// `ms` milliseconds after the tests' origin.
pub(super) fn at(ms: u64) -> Instant {
    *T0 + Duration::from_millis(ms)
}

pub(super) fn linger(window_ms: u64, min_amount: u64) -> Linger {
    Linger {
        window: Duration::from_millis(window_ms),
        min_amount,
    }
}

pub(super) fn w(id: u64) -> WaiterId {
    WaiterId(id)
}

pub(super) fn ready(id: u64, through_rev: Rev) -> Output {
    Output::Ready {
        waiter: w(id),
        through_rev,
    }
}

pub(super) fn timed_out(id: u64) -> Output {
    Output::TimedOut { waiter: w(id) }
}

pub(super) fn lingering(id: u64, until_ms: u64) -> Output {
    Output::Lingering {
        waiter: w(id),
        until: at(until_ms),
    }
}

/// The machine with its index audited after every input, and outputs sorted so tests compare
/// them as sets.
pub(super) struct Rig {
    pub poll: LongPoll<u8>,
}

impl Rig {
    pub(super) fn new() -> Self {
        Self {
            poll: LongPoll::new(),
        }
    }

    pub(super) fn park(
        &mut self,
        now_ms: u64,
        id: u64,
        interests: &[u8],
        after_rev: Rev,
        deadline_ms: u64,
        linger: Linger,
    ) -> Vec<Output> {
        let mut out = vec![];
        self.poll.park(
            at(now_ms),
            w(id),
            Park {
                interests,
                after_rev,
                deadline: at(deadline_ms),
                linger,
            },
            &mut out,
        );
        self.poll.audit();
        out.sort();
        out
    }

    pub(super) fn commit(&mut self, now_ms: u64, rev: Rev, changes: &[(u8, u64)]) -> Vec<Output> {
        let mut out = vec![];
        self.poll
            .committed(at(now_ms), rev, changes.iter().copied(), &mut out);
        self.poll.audit();
        out.sort();
        out
    }

    pub(super) fn tick(&mut self, now_ms: u64) -> Vec<Output> {
        let mut out = vec![];
        self.poll.tick(at(now_ms), &mut out);
        self.poll.audit();
        out.sort();
        out
    }

    pub(super) fn cancel(&mut self, id: u64) {
        self.poll.cancelled(w(id));
        self.poll.audit();
    }
}
