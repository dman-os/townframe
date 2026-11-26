//! The responder: the server half of the replay protocol bound to a part store. It turns one
//! [`PageSession`](crate::replay::wire::PageSession) into one authorized read (laws 6 to 13)
//! and one [`UpdateSession`]'s additions into admission answers (law 4).
//!
//! The [`ReplayServer`](crate::replay::server::ReplayServer) is not here. Its caller owns it
//! (behind a lock, or inside an actor) and touches it only from the synchronous closures the
//! responder's calls take or return, so no lock on it is ever held across an await.
//!
//! Time is a [`Timer`]: the crate builds for `wasm32-unknown-unknown`, where there is no tokio
//! timer, so the embedder supplies the sleep its runtime has
//! ([`RuntimeTimer`](crate::long_poll::RuntimeTimer) is the runtime's own).
//!
//! A hold parks on the store's [long-poll hub](crate::long_poll), by the routes the round
//! names, so a commit wakes the round only if it changed one of them.

use crate::interlude::*;

use crate::long_poll::Linger;
use crate::long_poll::LongPollHub;
use crate::long_poll::Outcome;
pub use crate::long_poll::Timer;
use crate::part_store::AuthorizedPage;
use crate::part_store::LogInterest;
use crate::part_store::PartStoreServe;
use crate::replay::server::Round;
use crate::replay::wire::Route;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::UpdateSession;
use crate::rev::Rev;

#[cfg(test)]
mod tests;

/// The default longest a client's linger hint may hold a round open (see
/// [`Responder::with_linger_cap`]).
pub const DEFAULT_LINGER_CAP: Duration = Duration::from_millis(50);

/// Serves one client connection's replay requests from a part store, one at a time. It owns
/// every buffer a round needs and reuses them across rounds, so a round allocates only what
/// the page it hands out holds.
///
/// Every call is cancel safe: dropping its future part way leaves nothing behind. The calls keep
/// no state between requests (law 9), the buffers are refilled by the next call, and a dropped
/// hold drops its sleep and its parked poll with it.
pub struct Responder<S, T> {
    store: S,
    timer: T,
    /// The store's long-poll hub, held for the responder's life.
    polls: LongPollHub<LogInterest>,
    /// The longest a hold may linger after the first commit that concerns it, whatever the
    /// client's hint (see [`Responder::with_linger_cap`]).
    linger_cap: Duration,
    /// What the current round's hold watches.
    interests: Vec<LogInterest>,
    round: Round,
    read: AuthorizedPage,
    page: SessionPage,
    /// The parts an update adds, sorted; after [`Responder::admission`], the readable ones.
    admit_parts: Vec<(PartKey, Rev)>,
    /// The objects an update adds, sorted; after [`Responder::admission`], the readable ones.
    admit_objects: Vec<ObjKey>,
}

impl<S, T> Responder<S, T>
where
    S: PartStoreServe,
    T: Timer,
{
    pub fn new(store: S, timer: T) -> Self {
        let polls = store.long_poll();
        Self {
            store,
            timer,
            polls,
            linger_cap: DEFAULT_LINGER_CAP,
            interests: default(),
            round: default(),
            read: default(),
            page: SessionPage {
                events: default(),
                verdicts: default(),
            },
            admit_parts: default(),
            admit_objects: default(),
        }
    }

    /// Cap the linger a client may ask for at `cap`. A hold waits `min(hint, cap)` after the
    /// first commit that concerns it, or until its page is full, before it answers, so a burst
    /// of commits is one answer instead of one each. The window is fixed from that first commit
    /// and never extends past the hold's own deadline. A round with no hint does not linger: it
    /// answers as soon as a commit carries it a row. The default cap is
    /// [`DEFAULT_LINGER_CAP`].
    pub fn with_linger_cap(mut self, cap: Duration) -> Self {
        self.linger_cap = cap;
        self
    }

    /// Answer one round of `principal`'s.
    ///
    /// `resolve` fills the round from the request: the caller's
    /// [`ReplayServer::resolve_round`](crate::replay::server::ReplayServer::resolve_round)
    /// under whatever guards its server. Its error is the answer.
    ///
    /// The read runs to the store's latest rev. When it carries no row and the round holds
    /// (law 13), the responder parks on the store's long-poll hub, watching the round's routes
    /// (and access changes, which move verdicts), and reads again when a commit that changed
    /// one of them is ready, until a read carries rows or the hold runs out. A commit on other
    /// routes neither wakes the round nor costs it a read. The page is the last read's, so a
    /// held round answers with verdicts and progress as of its end. Every read decides the verdicts afresh
    /// (law 7), so a hold never carries a row access no longer allows.
    ///
    /// The `Err` of the outer result is the store's: the request goes unanswered.
    pub async fn page(
        &mut self,
        principal: &PeerKey,
        resolve: impl FnOnce(&mut Round) -> Result<(), SessionError>,
    ) -> Res<Result<&SessionPage, SessionError>> {
        if let Err(err) = resolve(&mut self.round) {
            return Ok(Err(err));
        }
        let mut until = self.store.latest_rev().await?;
        self.read_round(principal, until).await?;
        if self.read.rows.entries.is_empty() && !self.round.hold().is_zero() {
            // One deadline for the whole hold, fixed now: each park below waits to it, so a
            // wake that finds nothing does not restart the hold.
            let deadline = self.polls.now() + self.round.hold();
            self.interests.clear();
            self.round.routes().interests_into(&mut self.interests);
            let linger = Linger {
                window: self.round.linger().min(self.linger_cap),
                min_amount: self.round.limits().max_entries.get() as u64,
            };
            while self.read.rows.entries.is_empty() {
                // Parked from the rev this read ran to: a commit past it that raced the park
                // wakes the poll at once (law 6 of the long-poll).
                let waiting = self.polls.park(&self.interests, until, deadline, linger);
                match waiting.wait(&self.timer).await {
                    Outcome::TimedOut => break,
                    // The commit may only have touched a route's access or a part the page
                    // does not carry; the loop then parks again, past the new head, on the
                    // same deadline.
                    Outcome::Ready { through_rev } => {
                        until = through_rev;
                        self.read_round(principal, until).await?;
                    }
                }
            }
        }
        self.round.answer(&mut self.read, until, &mut self.page);
        Ok(Ok(&self.page))
    }

    /// The round's authorized read up to `until`.
    ///
    /// The read starts at the round's lowest cursor, capped at `until`: a cursor past the
    /// store's head reads the empty window and is answered with its verdict and its own
    /// cursor as `resume`.
    async fn read_round(&mut self, principal: &PeerKey, until: Rev) -> Res<()> {
        let round = &self.round;
        self.store
            .read_authorized(
                principal,
                round.routes(),
                round.after().min(until),
                until,
                round.limits(),
                &mut self.read,
            )
            .await
    }

    /// Law 4's admission for `update`'s additions, decided by `principal`'s access now: a part
    /// route is admitted while the part is known and readable, an object route while the object
    /// is a live member of a readable part. Only the additions are read, in two batched reads,
    /// before the caller takes its server's lock; the returned closure is the `admits` that
    /// [`ReplayServer::update`](crate::replay::server::ReplayServer::update) asks under it.
    ///
    /// The reads are not skipped for an update the server will not apply (law 2's repeats): only
    /// the server knows that, under its lock.
    pub async fn admission(
        &mut self,
        principal: &PeerKey,
        update: &UpdateSession,
    ) -> Res<impl FnMut(&Route) -> bool + '_> {
        let Self {
            store,
            admit_parts,
            admit_objects,
            ..
        } = self;
        admit_parts.clear();
        admit_objects.clear();
        for (_, route) in &update.additions {
            match route {
                // Only whether a summary comes back matters here, and a `since` past every row
                // asks for no dirt.
                Route::Part { part_key } => admit_parts.push((part_key.clone(), Rev::MAX)),
                Route::Object { obj_key } => admit_objects.push(obj_key.clone()),
            }
        }
        admit_parts.sort_unstable();
        admit_parts.dedup();
        admit_objects.sort_unstable();
        admit_objects.dedup();
        let summaries = store.part_summaries(principal, admit_parts).await?;
        let mut readable = summaries.iter().map(Option::is_some);
        admit_parts.retain(|_| readable.next().expect(ERROR_IMPOSSIBLE));
        let states = store.readable_obj_states(principal, admit_objects).await?;
        let mut readable = states.iter().map(Option::is_some);
        admit_objects.retain(|_| readable.next().expect(ERROR_IMPOSSIBLE));
        let (parts, objects) = (&*admit_parts, &*admit_objects);
        Ok(move |route: &Route| match route {
            Route::Part { part_key } => parts
                .binary_search_by(|(part, _)| part.cmp(part_key))
                .is_ok(),
            Route::Object { obj_key } => objects.binary_search(obj_key).is_ok(),
        })
    }
}
