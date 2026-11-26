//! The [`MinimizedTreeStore`] over the tables.
//!
//! Subduction keeps built trees in a store so that serving sync skips a storage read and a
//! minimization pass. Here storage *is* the minimized tree (law 1), so there is nothing to
//! derive and no second copy to keep right: [`MinimizedTreeStore::get`] reads the rows. Reading
//! them is still a decode per item, so a bounded cache keeps recent trees; an entry is served
//! only when the `rev` the same snapshot just read equals the `rev` it was built at, which is
//! the "validate against storage" scheme of subduction's design notes, the one that holds for
//! every node and every process.
//!
//! The rules of the notes' shared-store section, as they land here:
//!
//! 1. *`populate` is a compare-and-set.* The generation is the tree's `rev` and a process-wide
//!    epoch; `populate` compares both and keeps nothing, because `get` already fills the cache
//!    from the rows. A tree handed in by a node is never trusted over the database's.
//! 2. *`merge` and `remove` change the generation, even on a miss.* They advance the epoch,
//!    which every outstanding generation carries.
//! 3. *A generation never repeats.* `rev` is monotone for the hive, tombstones included (see
//!    the migration), and the epoch only grows. A tree never written has `rev` 0, and once
//!    written never has it again.
//! 4. *Operations are serialized per tree.* All four run under the cache's one mutex, which
//!    is never held across an await.
//! 5. *Store calls time out* is moot: no call leaves the process but the database read.
//! 6. *Fail safe.* `merge` and `remove` cannot fail. A hit is always checked against the
//!    database, so a write that this process's node never reported (a failed or dropped
//!    maintenance call, or another process) cannot hide behind a stale entry.
//! 7. *Never write back an entry.* A hit is a clone of the cached tree.
//! 8. *A hit means the tree exists.* A tree is a hit iff its row is live, empty trees included;
//!    a missing or deleted tree is a miss.
//!
//! `merge` ignores its update: the write that produced it already committed the new rows (and,
//! through [`SedimentreeStore::committed`], the new cache entry).

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use future_form::FutureForm;
use future_form::Local;
use future_form::Sendable;
use future_form::future_form;
use sedimentree_core::depth::DepthMetric;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::sedimentree::Sedimentree;
use sedimentree_core::sedimentree::minimized::MinimizedSedimentree;
use sqlite_rs::SqliteDb;
use subduction_core::minimized_tree_store::Lookup;
use subduction_core::minimized_tree_store::MinimizedTreeStore;
use subduction_core::minimized_tree_store::Populate;
use subduction_core::minimized_tree_store::SharedMinimizedTreeStore;

use super::Error;
use super::SedimentreeStore;
use super::write::Plant;
use super::write::Stored;
use super::write::planted;
use crate::sqlite::queries;

/// A tree's state as a [`MinimizedTreeStore`] miss saw it: its `rev` (0 for a tree never
/// written) and the process epoch. Not `Clone`, as the trait asks: a generation is used once.
#[derive(Debug, PartialEq, Eq)]
pub struct Generation {
    rev: i64,
    epoch: u64,
}

/// Minimized trees by id, each at the `rev` it was built at.
pub(super) struct Cache {
    capacity: usize,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    /// Bumped by `merge` and `remove`, so a generation from before one cannot populate.
    epoch: u64,
    trees: BTreeMap<SedimentreeId, (i64, Arc<MinimizedSedimentree>)>,
    /// Insertion order, for eviction.
    order: VecDeque<SedimentreeId>,
}

impl Cache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .expect("the cache mutex is never poisoned")
    }

    fn epoch(&self) -> u64 {
        self.lock().epoch
    }

    /// The tree at exactly `rev`.
    fn at(&self, id: SedimentreeId, rev: i64) -> Option<Arc<MinimizedSedimentree>> {
        self.lock()
            .trees
            .get(&id)
            .filter(|(cached, _)| *cached == rev)
            .map(|(_, tree)| Arc::clone(tree))
    }

    /// The tree at whatever `rev`: what a cache that does not validate would serve.
    fn latest(&self, id: SedimentreeId) -> Option<Arc<MinimizedSedimentree>> {
        self.lock().trees.get(&id).map(|(_, tree)| Arc::clone(tree))
    }

    /// Keep `tree` as of `rev`, unless a newer one is already here.
    pub(super) fn insert(&self, id: SedimentreeId, rev: i64, tree: Arc<MinimizedSedimentree>) {
        let mut state = self.lock();
        match state.trees.get_mut(&id) {
            Some(entry) if entry.0 >= rev => {}
            Some(entry) => *entry = (rev, tree),
            None => {
                state.trees.insert(id, (rev, tree));
                state.order.push_back(id);
                while state.trees.len() > self.capacity {
                    let oldest = state.order.pop_front().expect("a full cache has an order");
                    state.trees.remove(&oldest);
                }
            }
        }
    }

    pub(super) fn evict(&self, id: SedimentreeId) {
        let mut state = self.lock();
        if state.trees.remove(&id).is_some() {
            state.order.retain(|queued| *queued != id);
        }
    }

    /// Advance the epoch, and drop `evicting`'s entry if there is one.
    fn advance(&self, evicting: Option<SedimentreeId>) {
        let mut state = self.lock();
        state.epoch += 1;
        if let Some(id) = evicting
            && state.trees.remove(&id).is_some()
        {
            state.order.retain(|queued| *queued != id);
        }
    }
}

impl<D: SqliteDb, M: DepthMetric> SedimentreeStore<D, M> {
    /// The minimized tree `id`, from the cache if its entry is at the `rev` this snapshot reads,
    /// else from the rows. `None` for a tree that does not exist.
    pub async fn tree(&self, id: SedimentreeId) -> Result<Option<MinimizedSedimentree>, Error> {
        Ok(self.lookup(id).await?.1)
    }

    /// The heads of the tree `id`, or `None` if it does not exist.
    pub async fn heads(
        &self,
        id: SedimentreeId,
    ) -> Result<Option<Vec<sedimentree_core::loose_commit::id::CommitId>>, Error> {
        Ok(self
            .tree(id)
            .await?
            .map(|mut tree| tree.heads(&self.shared.depth)))
    }

    fn cached(&self, id: SedimentreeId, rev: i64) -> Option<Arc<MinimizedSedimentree>> {
        if planted(Plant::StaleCache) {
            return self.shared.cache.latest(id);
        }
        self.shared.cache.at(id, rev)
    }

    /// The tree's `rev`, 0 if never written.
    async fn rev_of(&self, id: SedimentreeId) -> Result<i64, Error> {
        let tx = self.db.begin_read().await?;
        let state = queries::tree_state(&tx, id.as_bytes().to_vec()).await;
        D::end_read(tx).await?;
        Ok(state?.map_or(0, |state| state.rev))
    }

    /// The tree's `rev` (0 if never written) and, if it is live, its tree, from one snapshot.
    async fn lookup(
        &self,
        id: SedimentreeId,
    ) -> Result<(i64, Option<MinimizedSedimentree>), Error> {
        let tx = self.db.begin_read().await?;
        let read = self.lookup_in(&tx, id).await;
        D::end_read(tx).await?;
        read
    }

    async fn lookup_in(
        &self,
        tx: &D::Read,
        id: SedimentreeId,
    ) -> Result<(i64, Option<MinimizedSedimentree>), Error> {
        let tree_id = id.as_bytes().to_vec();
        let Some(state) = queries::tree_state(tx, tree_id.clone()).await? else {
            return Ok((0, None));
        };
        if !state.live {
            return Ok((state.rev, None));
        }
        if let Some(tree) = self.cached(id, state.rev) {
            return Ok((state.rev, Some(MinimizedSedimentree::clone(&tree))));
        }
        let tree = Stored::read(tx, &tree_id).await?.into_tree();
        self.shared
            .cache
            .insert(id, state.rev, Arc::new(MinimizedSedimentree::clone(&tree)));
        Ok((state.rev, Some(tree)))
    }
}

#[future_form(Sendable, Local)]
impl<F: FutureForm, D: SqliteDb, M: DepthMetric + Send + Sync + 'static> MinimizedTreeStore<F>
    for SedimentreeStore<D, M>
{
    type Error = Error;
    type Generation = Generation;
    type Entry = Box<MinimizedSedimentree>;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> F::Future<'_, Result<Lookup<Self::Entry, Self::Generation>, Error>> {
        F::from_future(async move {
            // The epoch first: a `merge` between this read and the `populate` must show.
            let epoch = self.shared.cache.epoch();
            Ok(match self.lookup(id).await? {
                (_, Some(tree)) => Lookup::Hit(Box::new(tree)),
                (rev, None) => Lookup::Miss(Generation { rev, epoch }),
            })
        })
    }

    fn populate(
        &self,
        id: SedimentreeId,
        _tree: MinimizedSedimentree,
        seen: Generation,
    ) -> F::Future<'_, Result<Populate, Error>> {
        F::from_future(async move {
            let rev = self.rev_of(id).await?;
            // Nothing is kept: `get` fills the cache from the rows, and a tree handed in by a
            // node is never trusted over them. The answer is only whether the miss still holds.
            Ok(
                if rev == seen.rev && self.shared.cache.epoch() == seen.epoch {
                    Populate::Populated
                } else {
                    Populate::Stale
                },
            )
        })
    }

    fn merge(&self, _id: SedimentreeId, _update: Sedimentree) -> F::Future<'_, Result<(), Error>> {
        F::from_future(async move {
            self.shared.cache.advance(None);
            Ok(())
        })
    }

    fn remove(&self, id: SedimentreeId) -> F::Future<'_, Result<(), Error>> {
        F::from_future(async move {
            self.shared.cache.advance(Some(id));
            Ok(())
        })
    }
}

/// Clones share the cache and the epoch, and every operation checks the database, so the store
/// meets the shared-store rules (see the module docs).
impl<D: SqliteDb, M: DepthMetric + Send + Sync + 'static> SharedMinimizedTreeStore<Sendable>
    for SedimentreeStore<D, M>
{
}

impl<D: SqliteDb, M: DepthMetric + Send + Sync + 'static> SharedMinimizedTreeStore<Local>
    for SedimentreeStore<D, M>
{
}
