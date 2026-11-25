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
use crate::okv::Pruned;
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
#[derive(Clone)]
pub(crate) struct OkvTable<K, V> {
    rev: Rev,
    /// The rows, grouped by the rev that wrote them: a read walks this in `(rev, key)` order
    /// with no regrouping.
    rows_by_rev: BTreeMap<Rev, BTreeMap<K, Option<V>>>,
    /// Where each key's row lives in `rows_by_rev`.
    rev_by_key: BTreeMap<K, Rev>,
    /// The registered consumers and the rev each has gone past (see [`crate::okv`]).
    consumers: BTreeMap<String, Rev>,
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

    /// Run `change` on the committed table inside one critical section.
    fn with_table_mut<R>(&self, change: impl FnOnce(&mut OkvTable<K, V>) -> R) -> R {
        surelock::key::lock_scope(|key| {
            let (mut state, _key) = key.lock(&self.db.state);
            let table = (state
                .tables
                .get_mut(&self.table)
                .expect(ERROR_IMPOSSIBLE)
                .as_mut() as &mut dyn Any)
                .downcast_mut::<OkvTable<K, V>>()
                .expect(ERROR_IMPOSSIBLE);
            change(table)
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
            consumers: default(),
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

    /// Register `consumer` and return its cursor, `0` if it is new.
    pub(crate) fn register_consumer(&mut self, consumer: &str) -> Rev {
        *self.consumers.entry(consumer.to_owned()).or_insert(0)
    }

    /// Move `consumer`'s cursor forward; a cursor at or below the recorded one changes nothing.
    pub(crate) fn report_cursor(&mut self, consumer: &str, cursor: Rev) {
        assert!(
            cursor <= self.rev,
            "okv cursor {cursor} past the committed rev {committed}",
            committed = self.rev
        );
        let held = self
            .consumers
            .get_mut(consumer)
            .unwrap_or_else(|| panic!("okv cursor reported by unregistered consumer {consumer:?}"));
        *held = (*held).max(cursor);
    }

    pub(crate) fn unregister_consumer(&mut self, consumer: &str) {
        self.consumers.remove(consumer);
    }

    pub(crate) fn consumers(&self) -> BTreeMap<String, Rev> {
        self.consumers.clone()
    }

    /// Drop every tombstone at or below the lowest registered cursor. Live rows stay, no rev
    /// is consumed and nothing wakes: a reader sees only fewer tombstones.
    pub(crate) fn prune(&mut self) -> Pruned {
        let floor = self.consumers.values().copied().min().unwrap_or(0);
        let mut tombstones = 0;
        for (_, rows) in self.rows_by_rev.range_mut(..=floor) {
            rows.retain(|key, value| {
                let live = value.is_some();
                if !live {
                    self.rev_by_key.remove(key).expect(ERROR_IMPOSSIBLE);
                    tombstones += 1;
                }
                live
            });
        }
        self.rows_by_rev.retain(|_, rows| !rows.is_empty());
        Pruned { floor, tombstones }
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
        selects: impl Fn(&K, Rev, Option<&V>) -> bool,
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
            let (rev_len, rev_bytes) = rows
                .iter()
                .filter(|(key, value)| selects(key, rev, value.as_ref()))
                .fold((0usize, 0usize), |(len, bytes), (key, value)| {
                    (len + 1, bytes.saturating_add(cost(key, value.as_ref())))
                });
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
            page.entries.extend(
                rows.iter()
                    .filter(|(key, value)| selects(key, rev, value.as_ref()))
                    .map(|(key, value)| Entry {
                        key: key.clone(),
                        rev,
                        value: value.clone(),
                    }),
            );
        }
    }
}

// Every operation is one synchronous critical section, so the memory backend does its work when
// called and hands back a ready future.
impl<K, V> RevSource for MemoryOkv<K, V>
where
    K: Ord + Clone + AsRef<[u8]> + Send + Sync + 'static,
    V: Clone + AsRef<[u8]> + Send + Sync + 'static,
{
    type Key = K;
    type Value = V;
    type Selector = Selector<K>;

    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        std::future::ready(Ok(self.with_table(OkvTable::rev)))
    }

    /// Copies one bounded page out under the lock.
    fn read(
        &self,
        selector: &Selector<K>,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<K, V>,
    ) -> impl Future<Output = Res<()>> + Send {
        self.with_table(|table| {
            table.read_into(
                |key, _rev, _value| selector.matches(key),
                row_bytes,
                after,
                until,
                limits,
                page,
            );
        });
        std::future::ready(Ok(()))
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

impl<K, V> OkvStore for MemoryOkv<K, V>
where
    K: Ord + Clone + AsRef<[u8]> + Send + Sync + 'static,
    V: Clone + AsRef<[u8]> + Send + Sync + 'static,
{
    type Tx = MemoryTx;

    fn apply(
        &self,
        tx: &mut MemoryTx,
        mutations: Vec<Mutation<K, V>>,
    ) -> impl Future<Output = Res<Rev>> + Send {
        let stage = self.stage(tx);
        let rev = stage.rev.checked_add(1).expect("okv rev overflow");
        assert!(
            !mutations.is_empty(),
            "an empty mutation batch has no rows to stamp"
        );
        for mutation in mutations {
            let (key, value) = mutation.into_row();
            stage.rows.insert(key, (rev, value));
        }
        stage.rev = rev;
        std::future::ready(Ok(rev))
    }

    fn get(&self, tx: &mut MemoryTx, key: &K) -> impl Future<Output = Res<Option<V>>> + Send {
        let stage = self.stage(tx);
        let value = match stage.rows.get(key) {
            Some((_, value)) => value.clone(),
            None => self.with_table(|table| table.get(key).and_then(|(_rev, value)| value.clone())),
        };
        std::future::ready(Ok(value))
    }

    fn register_consumer(&self, consumer: &str) -> impl Future<Output = Res<Rev>> + Send {
        std::future::ready(Ok(
            self.with_table_mut(|table| table.register_consumer(consumer))
        ))
    }

    fn report_cursor(&self, consumer: &str, cursor: Rev) -> impl Future<Output = Res<()>> + Send {
        self.with_table_mut(|table| table.report_cursor(consumer, cursor));
        std::future::ready(Ok(()))
    }

    fn unregister_consumer(&self, consumer: &str) -> impl Future<Output = Res<()>> + Send {
        self.with_table_mut(|table| table.unregister_consumer(consumer));
        std::future::ready(Ok(()))
    }

    fn consumers(&self) -> impl Future<Output = Res<BTreeMap<String, Rev>>> + Send {
        std::future::ready(Ok(self.with_table(OkvTable::consumers)))
    }

    fn prune(&self) -> impl Future<Output = Res<Pruned>> + Send {
        std::future::ready(Ok(self.with_table_mut(OkvTable::prune)))
    }
}
