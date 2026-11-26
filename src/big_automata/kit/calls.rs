//! The skeleton every driver in `big_automata` and `big_hive` shares: how a sans-IO machine is
//! run against real I/O. The machines stay different (their commands, their inputs, what a
//! call is), and this module holds only what is identical in each loop.
//!
//! # The skeleton
//!
//! 1. **One loop owns the machine.** Every input reaches it from one future, one at a time, and
//!    the machine is never borrowed across an await. The loop is an `async fn` the `start`
//!    function of the driver returns beside its handle.
//! 2. **The driver spawns nothing.** The embedder spawns the loop future (`tokio::spawn`
//!    natively, `spawn_local` in the browser). Calls are futures polled inside the loop, in a
//!    [`Pool`], not tasks.
//! 3. **Inferred future types, never `dyn`-boxed.** A pool is generic over one concrete future
//!    type, which is the opaque type a function returns (`impl Future`). When a driver issues
//!    more than one kind of call, the kinds are one `async fn`, or an
//!    [`Either`](futures::future::Either) of async blocks, never a `Box<dyn Future>`.
//! 4. **No `Send` bounds on type parameters.** Whether the loop future is `Send` is auto-trait
//!    leakage from the embedder's concrete types, so a browser backend holding JS handles
//!    works and a native one spawns on tokio.
//! 5. **Every command is executed once and every call's outcome is reported once.** A `Cancel`
//!    drops the call's future and still reports `Cancelled` ([`Aborts::track`]), so the machine
//!    frees the slot. The one exception is a connection lost wholesale: the loop clears the
//!    pool and the machine that would hear the reports is gone.
//! 6. **The loop waits for the next input, the next call outcome or the machine's `next_due`,
//!    whichever is first** ([`Pool::wait`]), and flushes the machine after every batch of
//!    inputs, so the commands it owes are issued before the loop waits again.
//! 7. **The clock is injected** (`Clock`, built on `web_time`) and sleeps go through
//!    `n0_future::time`, never `tokio::time`, so the loop builds for
//!    `wasm32-unknown-unknown`.
//! 8. **Errors.** A transport or backend error is an outcome the machine retries; a store
//!    error or a broken invariant panics the loop, which the embedder's spawn surfaces.
//!
//! # Where the drivers differ
//!
//! - Extra input arms: the outbox driver also polls a [`RevStream`](crate::rev::RevStream) of
//!   its table; the peer driver also polls the closes of replaced sessions. Both are the
//!   `extra` arm of [`Pool::wait`].
//! - Work between flushes: the peer driver persists cursors through a write that must keep
//!   polling the pool meanwhile ([`Pool::next`]), and then flushes again.
//! - Call lifetime: a peer driver's calls die with their connection (the pool is cleared);
//!   an outbox driver's landings are never cancelled and are not tracked in [`Aborts`].
//! - Not loop-shaped: the long-poll driver has no loop. A parked poll is a future its caller
//!   awaits and that ticks a shared machine behind a lock when its own timer fires, so it
//!   shares laws 2, 4 and 7 with the others but nothing of this module.
//!
//! The four machines of `big_hive` are driven with these pieces and follow the same laws.

use std::collections::BTreeMap;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::AbortHandle;
use futures::future::Abortable;
use futures::stream::FuturesUnordered;

use super::scheduler::Instant;

/// What [`Pool::wait`] woke for.
#[derive(Debug)]
pub enum Event<I, O, X> {
    /// A call finished.
    Done(O),
    /// An input arrived, or `None` when every sender is gone: the loop's cue to end.
    Input(Option<I>),
    /// The driver's own arm finished.
    Extra(X),
    /// The machine's `next_due` has passed.
    Timer,
}

/// The calls in flight: futures of one concrete type, polled by the loop that owns them.
pub struct Pool<F>(FuturesUnordered<F>);

impl<F> Default for Pool<F> {
    fn default() -> Self {
        Self(FuturesUnordered::new())
    }
}

impl<F: Future> Pool<F> {
    pub fn extend(&mut self, calls: impl IntoIterator<Item = F>) {
        self.0.extend(calls);
    }

    /// Drop every call without a report.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// The next call outcome, pending while there are no calls. Cancel-safe.
    pub async fn next(&mut self) -> F::Output {
        if self.0.is_empty() {
            std::future::pending().await
        } else {
            self.0.next().await.expect("a non-empty set yields")
        }
    }

    /// Wait for the first of a call outcome, an input, `extra` and the machine's `next_due`,
    /// in that order of priority when several are ready. `extra` is the driver's own arm
    /// (pass `std::future::pending()` for none). A `due` of `None` never fires.
    pub async fn wait<I, X>(
        &mut self,
        inputs: &async_channel::Receiver<I>,
        extra: impl Future<Output = X>,
        now: Instant,
        due: Option<Instant>,
    ) -> Event<I, F::Output, X> {
        let call = self.next().fuse();
        let input = inputs.recv().fuse();
        let extra = extra.fuse();
        let timer = async {
            match due {
                Some(at) => n0_future::time::sleep(at.saturating_duration_since(now)).await,
                None => std::future::pending().await,
            }
        }
        .fuse();
        futures::pin_mut!(call, input, extra, timer);
        futures::select_biased! {
            done = call => Event::Done(done),
            input = input => Event::Input(input.ok()),
            extra = extra => Event::Extra(extra),
            () = timer => Event::Timer,
        }
    }
}

/// The abort handles of the cancellable calls of one machine, keyed by the machine's call id.
pub struct Aborts<Id>(BTreeMap<Id, AbortHandle>);

impl<Id> Default for Aborts<Id> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<Id: Ord> Aborts<Id> {
    /// Make `call` cancellable by `id`. The returned future resolves to `call`'s output, or to
    /// `cancelled` if [`cancel`](Self::cancel) is called first, in which case `call` is dropped
    /// unfinished. Either way it resolves once, and the loop reports it and then
    /// [`finish`](Self::finish)es the id.
    pub fn track<F: Future>(
        &mut self,
        id: Id,
        call: F,
        cancelled: F::Output,
    ) -> impl Future<Output = F::Output> + use<Id, F> {
        let (handle, registration) = AbortHandle::new_pair();
        assert!(
            self.0.insert(id, handle).is_none(),
            "a call id is issued once"
        );
        async move {
            Abortable::new(call, registration)
                .await
                .unwrap_or(cancelled)
        }
    }

    /// Cancel a call in flight: its future stops at its next poll and reports `cancelled`.
    pub fn cancel(&self, id: &Id) {
        self.0[id].abort();
    }

    /// The call's outcome was reported: forget its handle.
    pub fn finish(&mut self, id: &Id) {
        self.0
            .remove(id)
            .expect("a call's outcome is reported once");
    }

    /// How many tracked calls have not been finished.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
