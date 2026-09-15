use crate::interlude::*;

use sedimentree_core::{
    fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit,
    sedimentree::minimized::MinimizedSedimentree,
};
use utils_rs::lru::KeyedLruPool;

/// Metadata-weighted capacity of the tree projection cache, in metadata
/// items (one per loose commit or fragment). SQLite remains authoritative;
/// eviction is always safe.
pub(crate) const TREE_CACHE_METADATA_CAPACITY: usize = 4096;

/// Bounded, metadata-only cache of the canonical durable sedimentree trees.
///
/// This synchronous mutex is intentional and SQLite-specific.
///
/// The tree projection cache is accessed only while holding a successfully
/// acquired `BEGIN IMMEDIATE` write transaction. SQLite serializes writers
/// before this mutex is reached, so lock acquisition is uncontended and never
/// waits for async work performed by another task. The mutex protects ordinary
/// in-memory access; it is not the write-serialization mechanism.
///
/// Do not reuse this design for a backend that permits concurrent write
/// transactions. Such a backend needs per-tree coordination or another cache
/// concurrency design.
///
/// This cache also assumes exactly one `SqliteBigRepoStore` instance per
/// (database, scope): clones share the `Arc` and are safe, but two
/// independently-constructed stores for the same scope would each hold an
/// incoherent projection cache. The runtime constructs one store per scope.
pub(crate) struct TreeCache {
    lru: KeyedLruPool<SedimentreeId>,
    pub(crate) entries: HashMap<SedimentreeId, MinimizedSedimentree>,
    epochs: HashMap<SedimentreeId, u64>,
    next_epoch: u64,
}

impl TreeCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            lru: KeyedLruPool::new(capacity),
            entries: HashMap::new(),
            epochs: HashMap::new(),
            next_epoch: 1,
        }
    }

    /// Look up an entry, marking it most-recently-used.
    pub(crate) fn get(&mut self, id: &SedimentreeId) -> Option<&MinimizedSedimentree> {
        self.lru.touch_key(id);
        self.entries.get(id)
    }

    /// Advance and return the installation epoch for `id`.
    pub(crate) fn bump_epoch(&mut self, id: SedimentreeId) -> u64 {
        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.epochs.insert(id, epoch);
        epoch
    }

    /// The current installation epoch for `id`, if present.
    pub(crate) fn current_epoch(&self, id: &SedimentreeId) -> Option<u64> {
        self.epochs.get(id).copied()
    }

    /// Insert an entry without touching the LRU. The caller derives heads
    /// first, then calls [`update_cost`](Self::update_cost) so eviction can
    /// never remove the entry before heads are captured.
    pub(crate) fn insert_no_evict(&mut self, id: SedimentreeId, tree: MinimizedSedimentree) -> u64 {
        let epoch = self.bump_epoch(id);
        self.entries.insert(id, tree);
        epoch
    }

    /// Recompute an entry's LRU cost from its (post-minimization) metadata
    /// weight and evict as needed. The entry itself may be evicted when its
    /// cost exceeds capacity — an oversized tree is used transiently for the
    /// transaction and then left uncached. Callers must have captured heads
    /// before calling this.
    pub(crate) fn update_cost(&mut self, id: &SedimentreeId) {
        let Some(tree) = self.entries.get(id) else {
            return;
        };
        let cost = Self::cost(tree);
        let pruned = self.lru.insert_key(id, cost);
        for key in pruned {
            self.entries.remove(&key);
            self.epochs.remove(&key);
        }
    }

    /// Remove an entry (whole-tree removal, delete-path rebuild, or
    /// speculative invalidation).
    pub(crate) fn remove(&mut self, id: &SedimentreeId) {
        self.lru.remove_key(id);
        self.entries.remove(id);
        self.epochs.remove(id);
    }

    /// Remove an entry only if its installation epoch matches `epoch`.
    pub(crate) fn remove_if_epoch(&mut self, id: &SedimentreeId, epoch: u64) {
        if self.epochs.get(id).copied() == Some(epoch) {
            self.remove(id);
        }
    }

    /// Apply a loose commit to the cached tree. No LRU update — the caller
    /// derives heads and calls [`update_cost`](Self::update_cost) after.
    pub(crate) fn apply_commit(&mut self, id: &SedimentreeId, commit: LooseCommit) -> u64 {
        let epoch = self.bump_epoch(*id);
        if let Some(tree) = self.entries.get_mut(id) {
            tree.add_commit(commit);
        }
        epoch
    }

    /// Apply a fragment to the cached tree. No LRU update — the caller
    /// derives heads and calls [`update_cost`](Self::update_cost) after.
    pub(crate) fn apply_fragment(&mut self, id: &SedimentreeId, fragment: Fragment) -> u64 {
        let epoch = self.bump_epoch(*id);
        if let Some(tree) = self.entries.get_mut(id) {
            tree.add_fragment(fragment);
        }
        epoch
    }

    /// Apply a whole batch to the cached tree. No LRU update — the caller
    /// derives heads and calls [`update_cost`](Self::update_cost) after.
    pub(crate) fn apply_batch(
        &mut self,
        id: &SedimentreeId,
        commits: Vec<LooseCommit>,
        fragments: Vec<Fragment>,
    ) -> u64 {
        let epoch = self.bump_epoch(*id);
        if let Some(tree) = self.entries.get_mut(id) {
            for commit in commits {
                tree.add_commit(commit);
            }
            for fragment in fragments {
                tree.add_fragment(fragment);
            }
        }
        epoch
    }

    /// Metadata weight of a tree: one per loose commit or fragment, plus one
    /// for the tree itself.
    pub(crate) fn cost(tree: &MinimizedSedimentree) -> usize {
        1 + tree.loose_commits().count() + tree.fragments().count()
    }
}

/// RAII invalidation guard for a speculative cache entry.
///
/// Armed after the entry is obtained or hydrated; the entry is evicted on
/// drop unless [`disarm`](Self::disarm) is called after the transaction
/// commits. This covers errors, commit failures, and dropped futures
/// (cancellation) uniformly: a speculative entry must never survive a
/// transaction that did not commit.
///
/// Guard eviction is epoch-aware: if a subsequent transaction installs a
/// fresh cache entry after this transaction released the SQLite writer lock,
/// dropping this guard will not evict that newer entry.
pub(crate) struct TreeCacheGuard<'a> {
    cache: &'a std::sync::Mutex<TreeCache>,
    id: SedimentreeId,
    pub(crate) epoch: u64,
    armed: bool,
}

impl<'a> TreeCacheGuard<'a> {
    pub(crate) fn arm(
        cache: &'a std::sync::Mutex<TreeCache>,
        id: SedimentreeId,
        epoch: u64,
    ) -> Self {
        Self {
            cache,
            id,
            epoch,
            armed: true,
        }
    }

    /// Mark the transaction as committed; the entry is retained.
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for TreeCacheGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // A poisoned mutex is an invariant break (a thread panicked
            // while holding it) — never swallow it.
            self.cache
                .lock()
                .expect(ERROR_MUTEX)
                .remove_if_epoch(&self.id, self.epoch);
        }
    }
}
