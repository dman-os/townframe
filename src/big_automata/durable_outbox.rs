//! The durable outbox: wants that survive a crash, fetched on their own time.
//!
//! For a backend that answers `Done` from `record` and fetches later (big_blobs' transfer
//! worker is the first user), see [`SyncBackend`](crate::driver::SyncBackend). A want is "this
//! object's content, from one of these peers": it is written in the transaction that admits the
//! object, and the outbox fetches it until the content is durable, however many crashes, peers
//! and supersessions that takes.
//!
//! # Why a module of its own
//!
//! It is public and speaks peers and okv tables, which `kit` (crate-private, nothing in it
//! knows a peer or a table) does not. It is built from `kit`'s blocks (`KeyedScheduler`), from
//! okv and the rev stream, and is driven the way [`crate::driver`] drives the peer machine.
//!
//! # Parts
//!
//! | Part | What it is |
//! | --- | --- |
//! | [`WantsTable`] | the writer: `record`, `withdraw` and `land` inside the caller's okv transaction |
//! | [`Outbox`] | the sans-IO machine: the index of every live want, what to fetch, from whom, when |
//! | [`OutboxHandle::start`] | the loop that follows the table, runs the machine's commands and lands wants |
//!
//! # The row
//!
//! One okv row per object, keyed by the object: a [`Want`] holding the `version`, the
//! first-seen time `queued_at`, the `priority`, the `identity` and the advertising `providers`.
//! The table is the pending set: a newer want overwrites the key (supersession in the store), a
//! withdrawal or a landing is a tombstone, and a reader that was behind never sees a version
//! that was overwritten.
//!
//! # Laws
//!
//! **The table**
//!
//! 1. **A want is durable and atomic with its admission.** [`WantsTable::record`] writes in
//!    the caller's transaction, beside the backend's own writes: a crash after the commit
//!    keeps both, a crash before keeps neither. The backend may then answer `Done`.
//! 2. **An overwrite keeps what must survive.** `record` reads the row and writes the
//!    result: `queued_at` is the first-seen time and is never moved by an overwrite (a want
//!    that keeps being re-advertised does not fall behind); the `version` changes exactly when
//!    the `identity` does (every row is written with a fresh version, never reused, so a
//!    landing cannot mistake a later want for the one it fetched); under an equal identity the
//!    providers union and the newest record's priority wins; under a new identity the providers
//!    are the new advertiser alone, since the others advertised the old one. A record that
//!    changes nothing writes nothing (a rev wakes every follower).
//! 3. **Withdrawal is a tombstone**, written only if the row is live. A tombstone is also what
//!    a landing leaves.
//!
//! **The machine**
//!
//! 4. **The index holds every live want, and the stream is the only way in.** On start the
//!    driver follows the table from rev 0, so a restart reads every live row and every want is
//!    due: nothing durable says a fetch was in flight, and none is needed. The index is
//!    light (key, version, priority, age, identity, providers) and is not bounded: `M` and `C`
//!    bound work, never what the machine knows (a bounded window would let a high-priority
//!    want wait behind `M` low ones it never read). A tombstone for a want the index does not
//!    hold is a no-op. The stream is polled whatever the budgets say; they gate dispatch only.
//! 5. **One fetch per object, stopped before replaced.** An object has at most one fetch
//!    running or stopping. A fetch is ended by its terminal event, matched by call id: a
//!    cancelled call still reports (as `Cancelled`, or its real result if it raced), and its
//!    result is dropped. Until it reports, the object's key and the call's slot stay held
//!    (`KeyedScheduler`'s stopping state): a superseding or re-recorded want waits for it
//!    and then competes for a slot like any other.
//! 6. **An overwrite with an equal version never touches a running fetch.** Only a new version
//!    (a new identity) cancels the fetch; a change of priority or providers is applied to the
//!    want in place. A burst of re-advertisements cannot restart a fetch or reset its backoff.
//! 7. **Priority, then age, with aging.** Of the wants that may start, the one with the
//!    smallest `queued_at - priority * aging` starts first (ties by object key): higher
//!    priority first, older first among equals, and a want that has waited `aging` longer than
//!    another overtakes one level of priority. `Duration::MAX` is strict priority.
//! 8. **Bounded work.** At most `C` fetches are running or stopping; at most `M` fetches and
//!    landings are in flight, so a store too slow to land holds new fetches back. A fetch
//!    holds its slot until its terminal event.
//! 9. **Providers.** The providers of a fetch are the row's advertisers that are connected
//!    right now. A want with none is parked: it is not scheduled, holds no slot and costs
//!    nothing until a provider connects. Eligibility is computed from the current connections
//!    when a want is considered, never from a snapshot, so a connection between "no provider"
//!    and "parked" is not lost. A peer disconnecting cancels the fetches running against it;
//!    that is not a failure, and the want goes on to another provider.
//! 10. **Failure tries the next provider, then backs off.** A failed fetch (a network error:
//!     the backend reports it, the outbox does not retry inside it) marks its provider tried;
//!     the next connected provider not tried starts at once. When none is left the want backs
//!     off, `min` doubling per round to `max`, and every provider is tried again after it. A
//!     backoff holds no slot. It resets only with a new version, never by a re-advertisement,
//!     and it is not durable: after a restart every want is due (a thundering herd bounded by
//!     `C`). The rounds a want has been through are the poison signal
//!     ([`Outbox::rounds_of`]); each failure is logged.
//! 11. **Withdrawal cancels.** The tombstone cancels the want's fetch; the cancelled call
//!     reports before its slot frees (law 5).
//! 12. **Landing deletes the want only if its version is the one fetched, and only after the
//!     content is durable.** [`WantsTable::land`] reads the row and deletes it on a match, in
//!     one transaction: when the content store shares the table's database the backend writes
//!     the content in that same transaction and answers [`Fetched::Landed`]; otherwise the
//!     driver lands after the fetch's content is durable, and a crash between the two is one
//!     idempotent refetch. A landing that deleted forgets the want at once. One that found a
//!     different version or no row leaves the want stale: nothing refetches that version, and
//!     the stream's next row for the object (the newer version, or the tombstone) ends it.
//!     A newer version already seen is fetched on its own.
//! 13. **Landing is never cancelled.** Only the network phase is cancellable: a want
//!     withdrawn or superseded while it lands finds the row gone or newer, and the landing
//!     says so.
//! 14. **Pruning.** The outbox registers as an okv consumer and reports the cursor of every
//!     page once the index has taken it in, so the tombstones landings leave can be pruned
//!     ([`crate::okv`]): the driver prunes every `prune_every` revs of progress. Live rows are
//!     never pruned, which is why a restart from rev 0 loses nothing.
//! 15. **Errors.** A fetch error is a network error and backs off (law 10). A store error, a
//!     row that does not decode, or a call the driver never issued is a bug and crashes.
//!
//! # Testing
//!
//! Each law above has a `law_N_...` test. [`oracle`](self::oracle) is a naive machine that
//! recomputes every decision from the whole state; a differential proptest drives both
//! with the same inputs and compares every command and every want's phase. A deterministic
//! simulation runs the real driver on a paused clock over a memory table, with crashes
//! between record, fetch and land, providers connecting and leaving, and withdrawals and
//! supersessions racing fetches.

use std::collections::BTreeSet;

use crate::interlude::*;
use crate::okv::Mutation;
use crate::okv::OkvStore;

pub mod driver;
mod machine;
#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

pub use driver::DriverConfig;
pub use driver::OutboxBackend;
pub use driver::OutboxHandle;
pub use machine::CallId;
pub use machine::Command;
pub use machine::Config;
pub use machine::Ended;
pub use machine::Fetched;
pub use machine::Job;
pub use machine::Outbox;
pub use machine::Phase;

/// Changes exactly when a want's identity does. Every row gets a fresh one, so equality is the
/// only thing it is compared for.
pub type Version = u64;

/// Higher starts first (law 7).
pub type Priority = u8;

/// One object's want, the value of its row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Want {
    pub version: Version,
    /// When the want was first recorded, in the writer's milliseconds: its age.
    pub queued_at: u64,
    pub priority: Priority,
    /// What the backend fetches: opaque here, a payload digest or a content hash.
    pub identity: ByteKey,
    /// The peers that advertised the object at this identity.
    pub providers: BTreeSet<PeerKey>,
}

impl Want {
    pub fn encode(&self) -> Vec<u8> {
        let mut row = vec![];
        ciborium::into_writer(self, &mut row).expect(ERROR_IMPOSSIBLE);
        row
    }

    /// Law 2 as a function: the row after `advert` is recorded over `old`, `None` if it would
    /// not change.
    pub fn record(old: Option<&Want>, advert: Advert, now_ms: u64) -> Option<(Want, Recorded)> {
        let fresh = |advert: Advert, queued_at| Want {
            version: advert.version,
            queued_at,
            priority: advert.priority,
            identity: advert.identity,
            providers: [advert.provider].into(),
        };
        match old {
            None => Some((fresh(advert, now_ms), Recorded::Created)),
            Some(old) if old.identity == advert.identity => {
                let mut want = old.clone();
                want.priority = advert.priority;
                want.providers.insert(advert.provider);
                (want != *old).then_some((want, Recorded::Updated))
            }
            Some(old) => {
                assert_ne!(advert.version, old.version, "a version is never reused");
                Some((fresh(advert, old.queued_at), Recorded::Superseded))
            }
        }
    }

    /// Rows are only ever written by [`WantsTable`], so one that does not decode is a bug.
    pub fn decode(row: &[u8]) -> Self {
        ciborium::from_reader(row).expect("a want row does not decode")
    }
}

/// What one peer says about one object, for [`WantsTable::record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Advert {
    pub provider: PeerKey,
    pub priority: Priority,
    pub identity: ByteKey,
    /// The version the want gets if this advert changes its identity (law 2).
    pub version: Version,
}

impl Advert {
    /// An advert with a fresh version.
    pub fn new(provider: PeerKey, priority: Priority, identity: ByteKey) -> Self {
        Self {
            provider,
            priority,
            identity,
            version: rand::random(),
        }
    }
}

/// What [`WantsTable::record`] did to the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// No row: a new want.
    Created,
    /// A row at another identity: a new version replaced it.
    Superseded,
    /// The same identity with other providers or priority.
    Updated,
    /// Nothing changed, so nothing was written.
    Unchanged,
}

/// The wants table: an okv table keyed by object, written inside the caller's transaction.
#[derive(Clone)]
pub struct WantsTable<S> {
    store: S,
}

impl<S> WantsTable<S>
where
    S: OkvStore<Key = Vec<u8>, Value = Vec<u8>>,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// Law 1 and 2. `now_ms` is the writer's clock, used only for a new want's age.
    pub async fn record(
        &self,
        tx: &mut S::Tx,
        obj: &ObjKey,
        advert: Advert,
        now_ms: u64,
    ) -> Res<Recorded> {
        let key = obj.as_bytes().to_vec();
        let old = self
            .store
            .get(tx, &key)
            .await?
            .map(|row| Want::decode(&row));
        let Some((want, recorded)) = Want::record(old.as_ref(), advert, now_ms) else {
            return Ok(Recorded::Unchanged);
        };
        self.store
            .apply(
                tx,
                vec![Mutation::Put {
                    key,
                    value: want.encode(),
                }],
            )
            .await?;
        Ok(recorded)
    }

    /// Law 3. Whether a live row was withdrawn.
    pub async fn withdraw(&self, tx: &mut S::Tx, obj: &ObjKey) -> Res<bool> {
        let key = obj.as_bytes().to_vec();
        if self.store.get(tx, &key).await?.is_none() {
            return Ok(false);
        }
        self.store.apply(tx, vec![Mutation::Delete { key }]).await?;
        Ok(true)
    }

    /// Law 12: delete the row if it still holds `version`. Whether it did.
    pub async fn land(&self, tx: &mut S::Tx, obj: &ObjKey, version: Version) -> Res<bool> {
        let key = obj.as_bytes().to_vec();
        let Some(row) = self.store.get(tx, &key).await? else {
            return Ok(false);
        };
        if Want::decode(&row).version != version {
            return Ok(false);
        }
        self.store.apply(tx, vec![Mutation::Delete { key }]).await?;
        Ok(true)
    }
}
