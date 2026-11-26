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
//!     peer's oldest (first-seen, then object key) from the row's providers, and the parts
//!     that peer named with it; a row left with no provider is deleted, and another peer's
//!     advert of the same object survives. One peer's flood never costs another peer a want.
//! 18. **TTL.** A held want first seen [`HeldConfig::ttl`] ago or more is deleted, whatever its
//!     providers, by every hold and by [`HeldTable::expire`]. A want that keeps being
//!     re-advertised does not outlive it: its age is its first-seen time (law 2).
//! 19. **Release is by part, and hands the want over exactly as held.** A held want records
//!     the parts each provider's notifications named (`Want::named`). [`HeldTable::release`]
//!     for a membership added to `part` deletes the row and returns it, once, only if some
//!     provider named `part`: a membership added to a part nobody named releases nothing and
//!     leaves the row as it was. The caller records the providers into the wants table in the
//!     membership's own transaction, so a crash keeps either both or neither.
//! 20. **A drop is a drop.** A want dropped by the cap or the TTL is not remembered anywhere:
//!     it is the peer's next replay of the object that brings it back, and nothing rewinds a
//!     cursor for it (operator, 2026-11-26: parked).
//! 21. **The index follows commits.** The cap and the TTL are decided from an in-memory index
//!     that is a fold of the committed rows, so a transaction that rolls back, or is dropped,
//!     leaves it exactly as it was.
//!
//! # The index
//!
//! The cap needs each peer's count and the TTL needs the oldest first, so the table keeps a
//! light index in memory: each row's first-seen time and providers, by object, by age and by
//! peer. It is never written by an operation. Like the outbox machine's index (law 4) it is
//! the fold of the table's own rows, read through the rev source: before an operation decides
//! anything it reads the rows committed since the rev it has folded to (the table's watch says
//! whether there are any, so an unchanged table costs no read), and a fresh [`HeldTable`] over
//! a table with rows starts from rev 0, as a restart does. Two handles on one table therefore
//! agree, and nothing a rolled back transaction staged was ever in it.
//!
//! What the operation's own transaction has staged is not committed yet, so an operation keeps
//! that apart, as a [`Staged`] overlay of the rows it touched, and decides over the index and
//! the overlay together. The overlay is dropped with the operation. Run one held-table
//! operation per transaction: a second operation in the same transaction would not see the
//! first one's staged rows in its decisions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Mutex;

use super::Advert;
use super::Recorded;
use super::Want;
use super::WantsTable;
use crate::interlude::*;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;

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

/// What the index keeps of a row: its first-seen time and its providers.
type Row = (u64, BTreeSet<PeerKey>);

fn row_of(want: &Want) -> Row {
    (want.queued_at, want.providers.clone())
}

type ByAge = BTreeSet<(u64, ObjKey)>;

/// The fold of the committed rows.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
struct Index {
    /// The rev of the table the fold has read to.
    rev: Rev,
    rows: BTreeMap<ObjKey, Row>,
    /// Every held want by first-seen time, then object.
    by_age: ByAge,
    /// Each provider's held wants by first-seen time, then object.
    by_peer: BTreeMap<PeerKey, ByAge>,
}

impl Index {
    /// `obj`'s row is now `row`.
    fn set(&mut self, obj: &ObjKey, row: Option<Row>) {
        if let Some((queued_at, providers)) = self.rows.remove(obj) {
            self.by_age.remove(&(queued_at, obj.clone()));
            for peer in providers {
                let held = self.by_peer.get_mut(&peer).expect("an indexed peer");
                held.remove(&(queued_at, obj.clone()));
                if held.is_empty() {
                    self.by_peer.remove(&peer);
                }
            }
        }
        if let Some((queued_at, providers)) = row {
            self.by_age.insert((queued_at, obj.clone()));
            for peer in &providers {
                self.by_peer
                    .entry(peer.clone())
                    .or_default()
                    .insert((queued_at, obj.clone()));
            }
            self.rows.insert(obj.clone(), (queued_at, providers));
        }
    }
}

/// The rows one operation has staged and not committed: `obj` to its row as the operation's
/// transaction sees it. Questions about the table are answered over the index and this.
#[derive(Default)]
struct Staged(BTreeMap<ObjKey, Option<Row>>);

impl Staged {
    fn set(&mut self, obj: &ObjKey, row: Option<Row>) {
        self.0.insert(obj.clone(), row);
    }

    /// How many held wants `peer` is the provider of.
    fn count(&self, index: &Index, peer: &PeerKey) -> usize {
        let committed = index.by_peer.get(peer).map_or(0, BTreeSet::len);
        let has = |row: Option<&Row>| row.is_some_and(|(_, providers)| providers.contains(peer));
        let removed = self
            .0
            .keys()
            .filter(|obj| has(index.rows.get(*obj)))
            .count();
        let added = self.0.values().filter(|row| has(row.as_ref())).count();
        committed - removed + added
    }

    /// The first-seen time and object of the oldest want of `peer`, or of every peer.
    fn oldest(&self, index: &Index, peer: Option<&PeerKey>) -> Option<(u64, ObjKey)> {
        let committed = match peer {
            Some(peer) => index.by_peer.get(peer),
            None => Some(&index.by_age),
        }
        .and_then(|held| held.iter().find(|(_, obj)| !self.0.contains_key(obj)))
        .cloned();
        let staged = self
            .0
            .iter()
            .filter_map(|(obj, row)| {
                let (queued_at, providers) = row.as_ref()?;
                peer.is_none_or(|peer| providers.contains(peer))
                    .then(|| (*queued_at, obj.clone()))
            })
            .min();
        [committed, staged].into_iter().flatten().min()
    }
}

/// The held wants of a node: a [`WantsTable`] of its own plus the index its laws need.
pub struct HeldTable<S> {
    table: WantsTable<S>,
    config: HeldConfig,
    index: Mutex<Index>,
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
            index: default(),
        }
    }

    pub fn config(&self) -> HeldConfig {
        self.config
    }

    pub fn store(&self) -> &S {
        self.table.store()
    }

    fn index(&self) -> std::sync::MutexGuard<'_, Index> {
        self.index.lock().expect("held index lock poisoned")
    }

    /// Fold the rows committed since the index's rev into it (law 21).
    async fn catch_up(&self) -> Res<()> {
        let store = self.table.store();
        let limits = Limits {
            max_entries: NonZeroUsize::new(256).expect(ERROR_IMPOSSIBLE),
            max_bytes: None,
        };
        loop {
            let after = self.index().rev;
            let until = store.watch().latest();
            if until <= after {
                return Ok(());
            }
            let (mut changes, mut page, mut at) = (vec![], Page::default(), after);
            while at < until {
                store
                    .read(&Selector::All, at, until, limits, &mut page)
                    .await?;
                for entry in &page.entries {
                    changes.push((
                        ObjKey::new(&entry.key),
                        entry.value.as_ref().map(|row| row_of(&Want::decode(row))),
                    ));
                }
                at = page.through;
            }
            let mut index = self.index();
            if index.rev != after {
                // another operation folded the same rows first
                continue;
            }
            for (obj, row) in changes {
                index.set(&obj, row);
            }
            index.rev = until;
            return Ok(());
        }
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
    /// clock, the same one that stamps first-seen times. The advert names the parts the
    /// notification was about, which gate [`release`](Self::release).
    pub async fn hold(
        &self,
        tx: &mut S::Tx,
        obj: &ObjKey,
        advert: Advert,
        now_ms: u64,
    ) -> Res<Held> {
        self.catch_up().await?;
        let mut staged = Staged::default();
        let mut dropped = self.expire_staged(tx, now_ms, &mut staged).await?;
        let peer = advert.provider.clone();
        let recorded = self.table.record(tx, obj, advert, now_ms).await?;
        if recorded != Recorded::Unchanged {
            let new = self.get(tx, obj).await?.expect("a want was just recorded");
            staged.set(obj, Some(row_of(&new)));
        }
        loop {
            let victim = {
                let index = self.index();
                if staged.count(&index, &peer) <= self.config.max_per_peer {
                    break;
                }
                staged
                    .oldest(&index, Some(&peer))
                    .expect("a peer over the cap holds a want")
            };
            let (_, victim) = victim;
            self.table.drop_provider(tx, &victim, &peer).await?;
            let left = self.get(tx, &victim).await?.as_ref().map(row_of);
            staged.set(&victim, left);
            dropped.push(Dropped::Cap {
                obj: victim,
                peer: peer.clone(),
            });
        }
        Ok(Held { recorded, dropped })
    }

    /// Law 19: a membership was added to `part`. If a provider named `part` for `obj`, delete
    /// the held want and hand it over; otherwise leave it as it was.
    pub async fn release(&self, tx: &mut S::Tx, obj: &ObjKey, part: &PartKey) -> Res<Option<Want>> {
        let Some(want) = self.get(tx, obj).await? else {
            return Ok(None);
        };
        if !want.named.values().any(|named| named.contains(part)) {
            return Ok(None);
        }
        self.table.withdraw(tx, obj).await?;
        Ok(Some(want))
    }

    /// Law 18: delete every want first seen `ttl` ago or more.
    pub async fn expire(&self, tx: &mut S::Tx, now_ms: u64) -> Res<Vec<Dropped>> {
        self.catch_up().await?;
        self.expire_staged(tx, now_ms, &mut Staged::default()).await
    }

    async fn expire_staged(
        &self,
        tx: &mut S::Tx,
        now_ms: u64,
        staged: &mut Staged,
    ) -> Res<Vec<Dropped>> {
        let ttl_ms = u64::try_from(self.config.ttl.as_millis()).expect("a ttl in u64 ms");
        let mut dropped = vec![];
        loop {
            let due = staged
                .oldest(&self.index(), None)
                .filter(|(queued_at, _)| queued_at.saturating_add(ttl_ms) <= now_ms);
            let Some((_, obj)) = due else {
                return Ok(dropped);
            };
            self.table.withdraw(tx, &obj).await?;
            staged.set(&obj, None);
            dropped.push(Dropped::Ttl { obj });
        }
    }

    /// How many held wants `peer` is the provider of, committed.
    pub async fn held_by(&self, peer: &PeerKey) -> Res<usize> {
        self.catch_up().await?;
        Ok(self.index().by_peer.get(peer).map_or(0, BTreeSet::len))
    }

    /// The whole index, to compare two moments for equality.
    #[cfg(test)]
    pub(in crate::durable_outbox) fn index_snapshot(&self) -> String {
        format!("{:?}", *self.index())
    }
}
