//! In-memory okv tables.
//!
//! A [`MemoryDb`] is one `surelock` mutex over the state of all its tables, and every operation
//! is one synchronous critical section: a read copies one bounded page out and releases the
//! lock, a commit validates and publishes every table it touched and releases the lock. Nothing
//! holds the lock across an await, and tables own no lock of their own.
//!
//! Transactions are optimistic and may span tables. A [`MemoryTx`] records each table's rev the
//! first time it touches the table (`apply` or `get`) and stages its writes locally. `commit`
//! checks under the lock that every touched table is still at that base rev: if so it publishes
//! all of them at once and, after releasing the lock, moves the watch of each table it wrote; if
//! not it fails with [`Conflict`] and changes nothing. A table that was only read is validated
//! too, so a read-modify-write cannot lose an update.

use std::any::Any;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::Bound;

use tokio::sync::watch;

use crate::interlude::*;
use crate::okv::Mutation;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::okv::row_bytes;
use crate::rev::Entry;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevWatch;

type TableId = u64;

strike! {
    /// A set of okv tables behind one lock, written together by [`MemoryTx`]s. Clones share
    /// the db.
    #[derive(Clone)]
    pub struct MemoryDb {
        state: Arc<surelock::mutex::Mutex<
            #[derive(Default)]
            struct DbState {
                next_table: TableId,
                /// Each table is an [`OkvTable`] of its own key and value types, erased so one
                /// db holds them all. The handle that created a table knows its types.
                tables: BTreeMap<TableId, Box<dyn ErasedTable>>,
            }
        >>,
    }
}

impl Default for MemoryDb {
    fn default() -> Self {
        Self {
            state: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }
}

/// One table's committed rows. A plain value inside its owner's locked state: no lock, no
/// sharing. [`MemoryDb`] embeds these, and so can any store that keeps a revisioned table
/// beside its own state under one lock (the memory part store's log).
pub(crate) struct OkvTable<K, V> {
    rev: Rev,
    /// The rows, grouped by the rev that wrote them: a read walks this in `(rev, key)` order
    /// with no regrouping.
    rows_by_rev: BTreeMap<Rev, BTreeMap<K, Option<V>>>,
    /// Where each key's row lives in `rows_by_rev`.
    rev_by_key: BTreeMap<K, Rev>,
}

trait ErasedTable: Any + Send {
    fn rev(&self) -> Rev;
}

impl<K: Send + 'static, V: Send + 'static> ErasedTable for OkvTable<K, V> {
    fn rev(&self) -> Rev {
        self.rev
    }
}

/// A handle on one table of a [`MemoryDb`]. Clones share the table.
pub struct MemoryOkv<K, V> {
    db: MemoryDb,
    table: TableId,
    /// The table's watch. Every handle holds a clone of the one sender, so subscribing takes
    /// no lock, and a transaction takes its own clone to move it after commit.
    wakeups: watch::Sender<Rev>,
    _types: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for MemoryOkv<K, V> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            table: self.table,
            wakeups: self.wakeups.clone(),
            _types: PhantomData,
        }
    }
}

impl MemoryDb {
    /// A new, empty table in this db.
    pub fn table<K, V>(&self) -> MemoryOkv<K, V>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let table = surelock::key::lock_scope(|key| {
            let (mut state, _key) = key.lock(&self.state);
            let table = state.next_table;
            state.next_table += 1;
            let old = state
                .tables
                .insert(table, Box::new(OkvTable::<K, V>::default()));
            assert!(old.is_none(), "fishy: a table id was reused");
            table
        });
        let (wakeups, _) = watch::channel(0);
        MemoryOkv {
            db: self.clone(),
            table,
            wakeups,
            _types: PhantomData,
        }
    }

    /// Start a transaction. It takes no lock: conflicts are found at commit.
    pub fn begin(&self) -> MemoryTx {
        MemoryTx {
            db: self.clone(),
            staged: default(),
        }
    }
}

/// A commit lost a race: a table the transaction touched committed after the transaction first
/// touched it. Nothing was published, no rev was consumed and no watch moved; the caller may
/// retry the whole transaction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("okv transaction conflict: a touched table moved from rev {base} to {current}")]
pub struct Conflict {
    /// The table's rev when the transaction first touched it.
    pub base: Rev,
    /// The table's rev at commit.
    pub current: Rev,
}

/// An optimistic transaction over the tables of one [`MemoryDb`]. Dropping it without
/// [`commit`](Self::commit) rolls it back: nothing it staged becomes visible, no rev is consumed
/// and no watch moves.
pub struct MemoryTx {
    db: MemoryDb,
    /// Per touched table: its base rev and its staged writes, typed as the table is.
    staged: BTreeMap<TableId, Box<dyn ErasedStage>>,
}

/// One table's part of a transaction.
struct Stage<K, V> {
    /// The table's rev when the transaction first touched it.
    base: Rev,
    /// The rev of the last batch staged, or `base` before the first.
    rev: Rev,
    /// The last staged write of each key, at the rev of its batch: what the table's rows will
    /// be, and what `get` reads first.
    rows: BTreeMap<K, (Rev, Option<V>)>,
    wakeups: watch::Sender<Rev>,
}

trait ErasedStage: Any + Send {
    fn base(&self) -> Rev;
    /// Write the staged rows into `table`, which is this stage's table.
    fn publish(&mut self, table: &mut dyn ErasedTable);
    /// Move the table's watch, after the publishing lock is released.
    fn notify(&self);
}

impl<K, V> ErasedStage for Stage<K, V>
where
    K: Ord + Clone + Send + 'static,
    V: Send + 'static,
{
    fn base(&self) -> Rev {
        self.base
    }

    fn publish(&mut self, table: &mut dyn ErasedTable) {
        if self.rows.is_empty() {
            return;
        }
        let table = (table as &mut dyn Any)
            .downcast_mut::<OkvTable<K, V>>()
            .expect(ERROR_IMPOSSIBLE);
        for (key, (rev, value)) in std::mem::take(&mut self.rows) {
            table.write(rev, key, value);
        }
        table.publish(self.rev);
    }

    fn notify(&self) {
        if self.rev > self.base {
            self.wakeups.send_replace(self.rev);
        }
    }
}

impl MemoryTx {
    /// Publish every touched table at once, then move the watch of each table written. Fails
    /// with [`Conflict`], changing nothing, when any touched table committed since this
    /// transaction first touched it.
    pub fn commit(mut self) -> Result<(), Conflict> {
        surelock::key::lock_scope(|key| {
            let (mut state, _key) = key.lock(&self.db.state);
            for (table, stage) in &self.staged {
                let current = state.tables[table].rev();
                if current != stage.base() {
                    return Err(Conflict {
                        base: stage.base(),
                        current,
                    });
                }
            }
            for (table, stage) in &mut self.staged {
                let table = state.tables.get_mut(table).expect(ERROR_IMPOSSIBLE);
                stage.publish(table.as_mut());
            }
            Ok(())
        })?;
        // After the lock is released: a woken reader must find the commit in place, and must
        // not contend with the commit that woke it.
        for stage in self.staged.values() {
            stage.notify();
        }
        Ok(())
    }
}

impl<K, V> MemoryOkv<K, V>
where
    K: Ord + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Run `inspect` on the committed table inside one critical section.
    fn with_table<R>(&self, inspect: impl FnOnce(&OkvTable<K, V>) -> R) -> R {
        surelock::key::lock_scope(|key| {
            let (state, _key) = key.lock(&self.db.state);
            let table = (state.tables[&self.table].as_ref() as &dyn Any)
                .downcast_ref::<OkvTable<K, V>>()
                .expect(ERROR_IMPOSSIBLE);
            inspect(table)
        })
    }

    /// This table's stage in `tx`, recording the table's base rev on first touch.
    fn stage<'tx>(&self, tx: &'tx mut MemoryTx) -> &'tx mut Stage<K, V> {
        assert!(
            Arc::ptr_eq(&tx.db.state, &self.db.state),
            "a transaction was used on a table of another db"
        );
        let stage = tx.staged.entry(self.table).or_insert_with(|| {
            let base = self.with_table(OkvTable::rev);
            Box::new(Stage::<K, V> {
                base,
                rev: base,
                rows: default(),
                wakeups: self.wakeups.clone(),
            })
        });
        (stage.as_mut() as &mut dyn Any)
            .downcast_mut::<Stage<K, V>>()
            .expect(ERROR_IMPOSSIBLE)
    }
}

impl<K, V> Default for OkvTable<K, V> {
    fn default() -> Self {
        Self {
            rev: 0,
            rows_by_rev: default(),
            rev_by_key: default(),
        }
    }
}

impl<K, V> OkvTable<K, V>
where
    K: Ord + Clone,
{
    /// The rev of the last committed batch.
    pub(crate) fn rev(&self) -> Rev {
        self.rev
    }

    /// Write `key`'s row at `rev`, a rev past the committed one that [`Self::publish`] then
    /// commits. Within one rev the later write of a key wins.
    pub(crate) fn write(&mut self, rev: Rev, key: K, value: Option<V>) {
        assert!(
            rev > self.rev,
            "okv write at rev {rev}, not past the committed rev {committed}",
            committed = self.rev
        );
        match self.rev_by_key.get_mut(&key) {
            Some(old_rev) => {
                let old_rows = self.rows_by_rev.get_mut(old_rev).expect(ERROR_IMPOSSIBLE);
                assert!(old_rows.remove(&key).is_some(), "okv row index is stale");
                if old_rows.is_empty() {
                    self.rows_by_rev.remove(old_rev);
                }
                *old_rev = rev;
            }
            None => {
                self.rev_by_key.insert(key.clone(), rev);
            }
        }
        self.rows_by_rev.entry(rev).or_default().insert(key, value);
    }

    /// Commit the rows written at `rev`: reads now see them.
    pub(crate) fn publish(&mut self, rev: Rev) {
        assert!(rev > self.rev, "okv revs must strictly increase");
        self.rev = rev;
    }

    /// The key's row and the rev that wrote it: `None` for a key never written, `Some((rev,
    /// None))` for a tombstone.
    pub(crate) fn get(&self, key: &K) -> Option<(Rev, &Option<V>)> {
        let rev = *self.rev_by_key.get(key)?;
        Some((rev, &self.rows_by_rev[&rev][key]))
    }

    /// The rows of the keys in `range`, in key order, with the rev of each.
    pub(crate) fn rows_in(
        &self,
        range: impl std::ops::RangeBounds<K>,
    ) -> impl Iterator<Item = (&K, Rev, &Option<V>)> {
        self.rev_by_key
            .range(range)
            .map(|(key, rev)| (key, *rev, &self.rows_by_rev[rev][key]))
    }

    /// The bounded read of the [`RevSource`] contract over this table: the rows `selects`
    /// keeps with a rev in `(after, until]`, each row costing `cost` against the byte budget.
    ///
    /// Scans the rev index and tests each row. Each revision is sized before anything is
    /// cloned, so a revision that does not fit is never copied; the scan stops at the first
    /// revision that does not fit, which once the page is full is the next one that selects
    /// anything.
    pub(crate) fn read_into(
        &self,
        selects: impl Fn(&K, Rev) -> bool,
        cost: impl Fn(&K, Option<&V>) -> usize,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<K, V>,
    ) where
        V: Clone,
    {
        assert!(after < until, "empty read window ({after}, {until}]");
        assert!(
            until <= self.rev,
            "read until {until} past the committed rev {rev}",
            rev = self.rev
        );
        page.entries.clear();
        page.through = until;
        let max_bytes = limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get);
        let mut bytes = 0usize;
        for (&rev, rows) in self
            .rows_by_rev
            .range((Bound::Excluded(after), Bound::Included(until)))
        {
            let (rev_len, rev_bytes) = rows.iter().filter(|(key, _)| selects(key, rev)).fold(
                (0usize, 0usize),
                |(len, bytes), (key, value)| {
                    (len + 1, bytes.saturating_add(cost(key, value.as_ref())))
                },
            );
            if rev_len == 0 {
                continue;
            }
            let fits = page.entries.is_empty()
                || (page.entries.len() + rev_len <= limits.max_entries.get()
                    && bytes.saturating_add(rev_bytes) <= max_bytes);
            if !fits {
                page.through = page.entries.last().expect(ERROR_IMPOSSIBLE).rev;
                return;
            }
            bytes = bytes.saturating_add(rev_bytes);
            page.entries
                .extend(
                    rows.iter()
                        .filter(|(key, _)| selects(key, rev))
                        .map(|(key, value)| Entry {
                            key: key.clone(),
                            rev,
                            value: value.clone(),
                        }),
                );
        }
    }
}

#[async_trait]
impl<K, V> RevSource for MemoryOkv<K, V>
where
    K: Ord + Clone + AsRef<[u8]> + Send + Sync + 'static,
    V: Clone + AsRef<[u8]> + Send + Sync + 'static,
{
    type Key = K;
    type Value = V;
    type Selector = Selector<K>;

    async fn latest_rev(&self) -> Res<Rev> {
        Ok(self.with_table(OkvTable::rev))
    }

    /// Copies one bounded page out under the lock.
    async fn read(
        &self,
        selector: &Selector<K>,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<K, V>,
    ) -> Res<()> {
        self.with_table(|table| {
            table.read_into(
                |key, _rev| selector.matches(key),
                row_bytes,
                after,
                until,
                limits,
                page,
            );
        });
        Ok(())
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

#[async_trait]
impl<K, V> OkvStore for MemoryOkv<K, V>
where
    K: Ord + Clone + AsRef<[u8]> + Send + Sync + 'static,
    V: Clone + AsRef<[u8]> + Send + Sync + 'static,
{
    type Tx = MemoryTx;

    async fn apply<I>(&self, tx: &mut MemoryTx, mutations: I) -> Res<Rev>
    where
        I: IntoIterator<Item = Mutation<K, V>> + Send,
        I::IntoIter: Send,
    {
        let stage = self.stage(tx);
        let rev = stage.rev.checked_add(1).expect("okv rev overflow");
        let mut empty = true;
        for mutation in mutations {
            empty = false;
            let (key, value) = mutation.into_row();
            stage.rows.insert(key, (rev, value));
        }
        assert!(!empty, "an empty mutation batch has no rows to stamp");
        stage.rev = rev;
        Ok(rev)
    }

    async fn get(&self, tx: &mut MemoryTx, key: &K) -> Res<Option<V>> {
        let stage = self.stage(tx);
        if let Some((_, value)) = stage.rows.get(key) {
            return Ok(value.clone());
        }
        Ok(self.with_table(|table| table.get(key).and_then(|(_rev, value)| value.clone())))
    }
}
