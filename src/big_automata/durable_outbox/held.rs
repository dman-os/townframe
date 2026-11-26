//! Held wants: notifications of objects the local node is not a member of, parked until it is.
//!
//! A peer's replay of a group part tells us an object changed in a part. It is a notification,
//! never membership: a node's own membership comes from its own source (the group-part worker
//! following the local keyhive's change stream) and nothing a peer says changes it. The
//! notification can arrive before the source has made the object a member, and dropping it
//! then would leave the object in the part without a fetch, so the backend parks it here and
//! the source's "membership added" hook releases it.
//!
//! # Why a table of its own
//!
//! A held want is a [`Want`] (a peer advertising an object at an identity, with a priority and
//! a first-seen time) that must not be fetched yet. The [`Outbox`](super::Outbox) follows the
//! wants table and fetches every live row it finds, so a held want lives in a second table that
//! the machine never reads, and releasing it is recording its providers into the wants table
//! (in the same transaction as the membership, when both share a database). That keeps one row
//! type and one record law (law 2), and the machine unchanged.
//!
//! # Laws
//!
//! 16. **Held is not wanted.** A held want is not in the wants table: the machine holds
//!     nothing for it, schedules nothing, and a restart starts nothing. [`HeldTable::hold`]
//!     records into the held table by law 2, so a replay of a parked notification is the same
//!     row and writes nothing.
//! 17. **Per-peer cap, oldest first.** A peer is the provider of at most
//!     [`HeldConfig::max_per_peer`] held wants. A hold that takes a peer past the cap drops that
//!     peer's oldest (first-seen, then object key) from the row's providers; a row left with no
//!     provider is deleted, and another peer's advert of the same object survives. One peer's
//!     flood never costs another peer a want.
//! 18. **TTL.** A held want first seen [`HeldConfig::ttl`] ago or more is deleted, whatever its
//!     providers, by every hold and by [`HeldTable::expire`]. A want that keeps being
//!     re-advertised does not outlive it: its age is its first-seen time (law 2).
//! 19. **Release hands the want over exactly as held.** [`HeldTable::release`] deletes the row
//!     and returns it, once: a second release finds nothing. The caller records the providers
//!     into the wants table in the membership's own transaction, so a crash keeps either both
//!     or neither.
//! 20. **A drop is a drop.** A want dropped by the cap or the TTL is not remembered anywhere:
//!     it is the peer's next replay of the object that brings it back, and nothing rewinds a
//!     cursor for it (operator, 2026-11-26: parked).
//!
//! # The index
//!
//! The cap needs each peer's count and the TTL needs the oldest first, so the table keeps a
//! light index in memory (first-seen time and object key, by peer and overall), rebuilt from
//! the table on first use (a restart reads every row once). It follows the staged writes, not
//! the commits, because a transaction is the caller's. A rolled back transaction leaves the
//! index ahead of the table: an entry for a row that is not there. Every drop checks the row
//! before deleting it and discards such an entry, so the table is never wrong, and the stale
//! entry costs only a count that is too high until it is reached; the caller retrying a failed
//! transaction (as the machine retries a failed backend call) makes it true again.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Mutex;

use tokio::sync::OnceCell;

use super::Advert;
use super::Recorded;
use super::Want;
use super::WantsTable;
use crate::interlude::*;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::rev::Limits;
use crate::rev::Page;

/// What bounds the held wants. Fields, not constants: an embedder sizes them for its peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldConfig {
    /// The most held wants one peer is the provider of (law 17), at least one.
    pub max_per_peer: usize,
    /// How long a held want lives, from its first-seen time (law 18).
    pub ttl: Duration,
}

impl Default for HeldConfig {
    fn default() -> Self {
        Self {
            max_per_peer: 1024,
            ttl: Duration::from_secs(10 * 60),
        }
    }
}

/// What a hold or an expiry dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dropped {
    /// `peer` went past the cap and lost its oldest want, `obj` (law 17).
    Cap { obj: ObjKey, peer: PeerKey },
    /// `obj` outlived the TTL (law 18).
    Ttl { obj: ObjKey },
}

/// What [`HeldTable::hold`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    /// What recording the advert did to the row (law 2).
    pub recorded: Recorded,
    /// The wants the TTL and the cap dropped along the way.
    pub dropped: Vec<Dropped>,
}

#[derive(Default)]
struct Index {
    /// Every held want by first-seen time, then object.
    by_age: BTreeSet<(u64, ObjKey)>,
    /// Each provider's held wants by first-seen time, then object.
    by_peer: BTreeMap<PeerKey, BTreeSet<(u64, ObjKey)>>,
}

impl Index {
    fn insert(&mut self, obj: &ObjKey, want: &Want) {
        self.by_age.insert((want.queued_at, obj.clone()));
        for peer in &want.providers {
            self.by_peer
                .entry(peer.clone())
                .or_default()
                .insert((want.queued_at, obj.clone()));
        }
    }

    fn remove(&mut self, obj: &ObjKey, want: &Want) {
        self.by_age.remove(&(want.queued_at, obj.clone()));
        for peer in &want.providers {
            self.forget(peer, want.queued_at, obj);
        }
    }

    fn forget(&mut self, peer: &PeerKey, queued_at: u64, obj: &ObjKey) {
        let Some(held) = self.by_peer.get_mut(peer) else {
            return;
        };
        held.remove(&(queued_at, obj.clone()));
        if held.is_empty() {
            self.by_peer.remove(peer);
        }
    }
}

/// The held wants of a node: a [`WantsTable`] of its own plus the index its laws need.
pub struct HeldTable<S> {
    table: WantsTable<S>,
    config: HeldConfig,
    index: OnceCell<Mutex<Index>>,
}

impl<S> HeldTable<S>
where
    S: OkvStore<Key = Vec<u8>, Value = Vec<u8>>,
{
    pub fn new(store: S, config: HeldConfig) -> Self {
        assert!(config.max_per_peer >= 1, "a cap of at least one");
        Self {
            table: WantsTable::new(store),
            config,
            index: OnceCell::new(),
        }
    }

    pub fn config(&self) -> HeldConfig {
        self.config
    }

    pub fn store(&self) -> &S {
        self.table.store()
    }

    /// The index, read from the table on first use.
    async fn index(&self) -> Res<&Mutex<Index>> {
        self.index
            .get_or_try_init(async || {
                let store = self.table.store();
                let until = store.latest_rev().await?;
                let limits = Limits {
                    max_entries: NonZeroUsize::new(256).expect(ERROR_IMPOSSIBLE),
                    max_bytes: None,
                };
                let (mut index, mut after, mut page) = (Index::default(), 0, Page::default());
                while after < until {
                    store
                        .read(&Selector::All, after, until, limits, &mut page)
                        .await?;
                    for entry in &page.entries {
                        if let Some(row) = &entry.value {
                            index.insert(&ObjKey::new(&entry.key), &Want::decode(row));
                        }
                    }
                    after = page.through;
                }
                Ok::<_, eyre::Report>(Mutex::new(index))
            })
            .await
    }

    fn lock(index: &Mutex<Index>) -> std::sync::MutexGuard<'_, Index> {
        index.lock().expect("held index lock poisoned")
    }

    /// The held want of `obj`.
    pub async fn get(&self, tx: &mut S::Tx, obj: &ObjKey) -> Res<Option<Want>> {
        Ok(self
            .table
            .store()
            .get(tx, &obj.as_bytes().to_vec())
            .await?
            .map(|row| Want::decode(&row)))
    }

    /// Law 16, 17 and 18: park `advert` for `obj`, after dropping what the TTL has expired,
    /// then drop what the advertising peer's cap no longer allows. `now_ms` is the writer's
    /// clock, the same one that stamps first-seen times.
    pub async fn hold(
        &self,
        tx: &mut S::Tx,
        obj: &ObjKey,
        advert: Advert,
        now_ms: u64,
    ) -> Res<Held> {
        let mut dropped = self.expire(tx, now_ms).await?;
        let peer = advert.provider.clone();
        let old = self.get(tx, obj).await?;
        let recorded = self.table.record(tx, obj, advert, now_ms).await?;
        if recorded != Recorded::Unchanged {
            let new = self.get(tx, obj).await?.expect("a want was just recorded");
            let mut index = Self::lock(self.index().await?);
            if let Some(old) = &old {
                index.remove(obj, old);
            }
            index.insert(obj, &new);
        }
        loop {
            let oldest = {
                let index = Self::lock(self.index().await?);
                match index.by_peer.get(&peer) {
                    Some(held) if held.len() > self.config.max_per_peer => held.first().cloned(),
                    _ => None,
                }
            };
            let Some((queued_at, victim)) = oldest else {
                break;
            };
            let live = self
                .get(tx, &victim)
                .await?
                .filter(|row| row.queued_at == queued_at && row.providers.contains(&peer));
            {
                let mut index = Self::lock(self.index().await?);
                match &live {
                    Some(row) => {
                        index.remove(&victim, row);
                        let mut left = row.clone();
                        left.providers.remove(&peer);
                        if !left.providers.is_empty() {
                            index.insert(&victim, &left);
                        }
                    }
                    // an entry of a rolled back write: there is no such want to drop
                    None => index.forget(&peer, queued_at, &victim),
                }
            }
            if live.is_some() {
                self.table.drop_provider(tx, &victim, &peer).await?;
                dropped.push(Dropped::Cap {
                    obj: victim,
                    peer: peer.clone(),
                });
            }
        }
        Ok(Held { recorded, dropped })
    }

    /// Law 19: delete the held want of `obj` and hand it over.
    pub async fn release(&self, tx: &mut S::Tx, obj: &ObjKey) -> Res<Option<Want>> {
        let Some(want) = self.get(tx, obj).await? else {
            return Ok(None);
        };
        self.table.withdraw(tx, obj).await?;
        Self::lock(self.index().await?).remove(obj, &want);
        Ok(Some(want))
    }

    /// Law 18: delete every want first seen `ttl` ago or more.
    pub async fn expire(&self, tx: &mut S::Tx, now_ms: u64) -> Res<Vec<Dropped>> {
        let ttl_ms = u64::try_from(self.config.ttl.as_millis()).expect("a ttl in u64 ms");
        let mut dropped = vec![];
        loop {
            let oldest = Self::lock(self.index().await?)
                .by_age
                .first()
                .filter(|(queued_at, _)| queued_at.saturating_add(ttl_ms) <= now_ms)
                .cloned();
            let Some((queued_at, obj)) = oldest else {
                return Ok(dropped);
            };
            match self.get(tx, &obj).await? {
                Some(row) if row.queued_at == queued_at => {
                    self.table.withdraw(tx, &obj).await?;
                    Self::lock(self.index().await?).remove(&obj, &row);
                    dropped.push(Dropped::Ttl { obj });
                }
                // an entry of a rolled back write
                _ => {
                    Self::lock(self.index().await?)
                        .by_age
                        .remove(&(queued_at, obj));
                }
            }
        }
    }

    /// How many held wants `peer` is the provider of.
    pub async fn held_by(&self, peer: &PeerKey) -> Res<usize> {
        Ok(Self::lock(self.index().await?)
            .by_peer
            .get(peer)
            .map_or(0, BTreeSet::len))
    }
}
