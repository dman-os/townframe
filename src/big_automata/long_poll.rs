//! Long-polls without the herd: a parked poll is woken by a commit that concerns it, after a
//! short linger, and by nothing else.
//!
//! Our services prefer client-pull long-polls to server push: cursors live on the client,
//! backpressure is free and the server keeps no subscriber buffers. The naive handler parks
//! every poll on one shared `Notify` or `watch`, so every commit wakes every parked poll and
//! each re-queries the store whether or not the commit touched what it watches: `O(parked)`
//! wakes and about two queries per poll per commit. This module is the replacement.
//!
//! # Why `long_poll`
//!
//! It is the pattern's own name, it is not tied to replay (cabinet's datum poll and the
//! cartridges' watch are the same shape), and `watch` would collide with
//! [`RevWatch`](crate::rev::RevWatch), which is the opposite thing: one rev for everyone.
//!
//! # Parts
//!
//! | Part | What it is |
//! | --- | --- |
//! | [`LongPoll`] | the sans-IO machine: the interest index, per-waiter linger state, the deadline queue |
//! | [`LongPollHub`] | the driver the source's commit path and the polls share: one lock, one channel per waiter |
//! | [`Waiting`] | one parked poll's handle: await it, or drop it to cancel |
//! | [`Timer`] | the sleep a [`Waiting`] uses; [`RuntimeTimer`] is the runtime's own |
//!
//! # The protocol
//!
//! A poll reads its page. If the page is empty it parks with `after_rev`, the rev its read ran
//! through, and the interests it read. The source's commit path says
//! [`committed`](LongPoll::committed)`(rev, changes)`, `changes` being each interest the commit
//! touched with an `amount` (rows, say). When the machine says [`Output::Ready`] the poll reads
//! again, through the `through_rev` it carries, and parks again if that page is empty.
//!
//! Linger, like Kafka's `fetch.max.wait.ms` and `fetch.min.bytes`: after the first relevant
//! commit a poll does not answer at once but waits out a window, or until its `min_amount`
//! is reached (a full page), so a burst of commits is one response instead of one each. It is
//! not a debounce: later commits do not extend the window, so a steady stream cannot starve a
//! poll.
//!
//! # Laws
//!
//! 1. **Relevance.** A waiter starts to end only through a *relevant* commit: a
//!    [`committed`](LongPoll::committed) at a rev above its `after_rev` that changes one of its
//!    interests (or law 6). A commit at or below `after_rev`, however late it is told, and a
//!    commit on other interests, never touch it.
//! 2. **The window is fixed.** The first relevant commit starts the window; the waiter is due
//!    `window` later, or at its deadline if that is sooner. Later relevant commits add to its
//!    amount and extend nothing.
//! 3. **The amount ends it early.** When the amounts of a waiter's relevant commits sum to its
//!    `min_amount` it is ready at once, mid-window. A commit's amount is summed over every
//!    interest of the waiter that it changed.
//! 4. **The deadline caps everything.** No waiter is ready or timed out later than its
//!    deadline. At its deadline a waiter that has seen a relevant commit is `Ready` and one
//!    that has not is `TimedOut`: a poll never times out with news pending.
//! 5. **One terminal output.** Every parked waiter gets exactly one of `Ready` or `TimedOut`,
//!    or, if cancelled first, none. A terminal output removes the waiter.
//! 6. **No lost wakeup.** The machine tells the highest rev it has heard of. A poll parked
//!    with an `after_rev` behind it may have raced a commit, and the machine keeps no
//!    per-interest history to say whether that commit was relevant, so it treats the park as a
//!    relevant commit of amount 0 at that moment: ready at once or after the window. The poll
//!    costs one spurious read at worst, never a missed one.
//! 7. **Cancel.** [`cancelled`](LongPoll::cancelled) removes a waiter and outputs nothing. For
//!    a waiter that ended or never was it is a no-op.
//! 8. **The index holds exactly the parked waiters.** Every parked waiter is under each of
//!    its interests and in the deadline queue once, at its due time, and nothing else is.
//! 9. **A commit costs `O(changed interests + the waiters watching them)`.** Not the parked
//!    count: [`LongPoll::touched`] counts the waiters looked at, and a commit on an interest
//!    nobody watches looks at none.
//! 10. **Time.** Every input first ends the waiters due by its `now`, so a late tick cannot
//!     let a commit revive an expired waiter. A `now` that goes back is read as the latest seen.
//! 11. **Through rev.** `Ready.through_rev` is the machine's head, at least the relevant
//!     commit's rev, so above `after_rev`: reading through it is reading everything the
//!     waiter was woken for. (Commits are visible to reads before they are told, so every rev
//!     the machine knows is readable.)
//!
//! The driver adds:
//!
//! 12. **Parked on return.** [`LongPollHub::park`] has indexed the waiter when it returns, so a
//!     commit after it is never lost, whenever the future is first polled.
//! 13. **Dropping leaves nothing.** A dropped [`Waiting`], ended or not, leaves no waiter, no
//!     slot and no timer behind.
//! 14. **Wakes follow the machine.** A commit wakes exactly the waiters it concerned.
//!
//! # Testing
//!
//! Each law has a `law_N_...` test. `oracle` is a naive machine that scans every waiter on
//! every input; a differential proptest drives both with the same inputs, compares every
//! output and audits the index after each step. A cost test parks thousands of waiters on
//! disjoint interests and counts the waiters one commit touches.
//!
//! # Adopting it
//!
//! Embed the hub where the commits are. The commit path calls `committed` with the interests
//! its rows touched; the poll handler reads, parks on the interests it read, awaits, and
//! reads again. Replay's [`Responder`](crate::replay::responder::Responder) is the first
//! user (interests are its routes).

mod driver;
mod machine;
#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub use driver::LongPollHub;
pub use driver::Outcome;
pub use driver::RuntimeTimer;
pub use driver::Timer;
pub use driver::Waiting;
pub use machine::Linger;
pub use machine::LongPoll;
pub use machine::Output;
pub use machine::Park;
pub use machine::WaiterId;
