//! The hive's sedimentree storage in SQLite: loose commits and fragments, stored as the minimal
//! form of their tree, behind subduction's [`Storage`](subduction_core::storage::traits::Storage)
//! and [`MinimizedTreeStore`](subduction_core::minimized_tree_store::MinimizedTreeStore).
//!
//! # Laws
//!
//! 1. **Storage is the minimized tree.** The rows of a tree are exactly its minimal form under
//!    the store's depth metric: no loose commit a stored fragment covers, no fragment a deeper
//!    one dominates, and one payload per head (the lowest digest, as `Sedimentree` itself
//!    decides). Every mutation minimizes and deletes what the minimization covers *in its own
//!    transaction*, so no reader, in this process or another, sees a tree that is not minimal.
//!    Memory-only minimization would re-send, on every sync, the commits a concurrent fragment
//!    hides until storage pruned them (subduction discussion 284); here nothing is left to
//!    prune later. A commit already covered when it arrives is never stored.
//! 2. **A tree's `rev` moves exactly when its rows or its liveness do.** It is drawn from one
//!    sequence for the hive, inside the changing transaction, never repeats and never goes
//!    down, deleted trees included. A save that changes nothing (a duplicate, a covered
//!    commit) writes nothing, so it moves nothing.
//! 3. **Every mutation is atomic with the caller's transaction.** [`SedimentreeStore::apply_in`]
//!    writes through a transaction the caller owns and shares with other stores of the
//!    database (keyhive rows, Keyline certificates, part-store rows), and returns a [`Staged`]
//!    for [`SedimentreeStore::committed`] once the transaction commits. A rollback leaves no
//!    row, no `rev` and no feed entry; nothing outside the transaction ever saw them.
//!    [`SedimentreeStore::committed`] only warms the cache and wakes the feed: skipping it is
//!    safe, only slower.
//! 4. **Every read is one snapshot.** A tree and its `rev` are read together.
//! 5. **One writer per database.** Writers serialize on `BEGIN IMMEDIATE` (`SqliteDb::begin_write`),
//!    which is what makes "maximum `rev` plus one" a sequence. Other processes only read.
//! 6. **The `MinimizedTreeStore` never holds a tree the database does not.** A hit is read from
//!    the database, or from a cache entry whose `rev` the same snapshot just read (see
//!    [`tree`]).
//! 7. **Nothing a feed reader needs is in memory.** [`ChangeFeed`] is a cursor over `rev`.
//!
//! # What a mutation costs
//!
//! One transaction, and the statements it needs: a tree state read, the tree's rows (an
//! existing tree only), the `rev` bump, a delete of what the new item covers (only when it
//! covers something) and a multi-row insert. A duplicate save is the two reads.
//!
//! # Platforms
//!
//! Generic over [`sqlite_rs::SqliteDb`] like `keyline_sqlite` and `big_automata_sqlite`, with no
//! timers or threads of its own.

mod feed;
mod storage;
mod tree;
mod write;

#[cfg(test)]
mod suite;

use std::sync::Arc;

use sedimentree_core::codec::error::DecodeError;
use sedimentree_core::depth::DepthMetric;
use sqlite_rs::SqlError;
use sqlite_rs::SqliteDb;
use tokio::sync::watch;

pub use feed::Change;
pub use feed::ChangeFeed;
pub use tree::Generation;
pub use write::Mutation;
pub use write::Save;
pub use write::Staged;

/// How many minimized trees the cache keeps. A tree is small by construction (it is minimal),
/// and every entry is validated against the database before it is served.
const CACHE_TREES: usize = 256;

/// A failure of the database, or a stored row that is not what a writer put there.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sql(#[from] SqlError),

    /// A row's `signed` bytes do not decode: the file was modified outside this store.
    #[error("a stored row does not decode: {0}")]
    Stored(#[from] DecodeError),
}

/// The sedimentree tables of one hive's database.
///
/// Clones share the database handle, the cache and the change feed's wakeups. Attach to a
/// database registered with [`MIGRATIONS`](crate::MIGRATIONS) under
/// [`MIGRATION_TABLE`](crate::MIGRATION_TABLE).
pub struct SedimentreeStore<D: SqliteDb, M> {
    db: D,
    shared: Arc<Shared<M>>,
}

impl<D: SqliteDb, M> Clone for SedimentreeStore<D, M> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

struct Shared<M> {
    /// The depth metric the rows are minimal under. A store opened with another metric over the
    /// same rows would serve trees that are not minimal for it.
    depth: M,
    cache: tree::Cache,
    /// The highest `rev` this process has committed, for wakeups. Not a source of truth: a
    /// reader always asks the database what changed.
    committed: Arc<watch::Sender<i64>>,
}

impl<D: SqliteDb, M: DepthMetric> SedimentreeStore<D, M> {
    /// Attach to `db`, whose trees are minimal under `depth`.
    pub fn new(db: D, depth: M) -> Self {
        Self {
            db,
            shared: Arc::new(Shared {
                depth,
                cache: tree::Cache::new(CACHE_TREES),
                committed: Arc::new(watch::Sender::new(0)),
            }),
        }
    }
}
