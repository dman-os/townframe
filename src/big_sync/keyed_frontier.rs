//! In-memory and SQLite keyed-frontier implementations.

mod sqlite_frontier;
mod sqlite_read;
mod sqlite_write;

pub(crate) use sqlite_frontier::SqlitePartFrontier;
pub(crate) use sqlite_read::SqlitePartSelector;

use crate::interlude::*;
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierMutation, FrontierRead, FrontierReadLimits, FrontierRevision,
    KeyedFrontier, KeyedFrontierError, KeyedFrontierReader, KeyedFrontierResult,
    KeyedFrontierTransaction, TransactionIsolation,
};
use big_sync_core::{ObjId, PartId};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

/// The logical object and part routes represented by the keyed frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartFrontierKey {
    Object(ObjId),
    Part { obj_id: ObjId, part_id: PartId },
}

pub trait MemoryKeyedFrontierSelector<K>: Send + Sync + 'static {
    /// `None` excludes the key. `Some(revision)` includes the key during
    /// initial replay only when its latest revision is newer than this bound.
    fn lower_bound(&self, key: &K) -> Option<FrontierRevision>;
}

#[derive(Debug, Clone)]
pub enum MemoryKeySelector<K> {
    All { after: FrontierRevision },
    Keys(BTreeMap<K, FrontierRevision>),
}

impl<K> MemoryKeyedFrontierSelector<K> for MemoryKeySelector<K>
where
    K: Ord + Send + Sync + 'static,
{
    fn lower_bound(&self, key: &K) -> Option<FrontierRevision> {
        match self {
            Self::All { after } => Some(*after),
            Self::Keys(keys) => keys.get(key).copied(),
        }
    }
}

#[derive(Debug, Clone)]
struct MemoryKeyedFrontierRoot<K, V> {
    entries: BTreeMap<K, FrontierEntry<K, V>>,
    keys_by_revision: BTreeMap<FrontierRevision, BTreeSet<K>>,
}

impl<K, V> Default for MemoryKeyedFrontierRoot<K, V> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            keys_by_revision: BTreeMap::new(),
        }
    }
}

pub struct MemoryKeyedFrontierView<K, V> {
    root: Arc<MemoryKeyedFrontierRoot<K, V>>,
    through: FrontierRevision,
    wakeups: watch::Receiver<FrontierRevision>,
}

/// Lock-independent table which can be embedded in a larger memory
/// transaction domain. Replay readers retain immutable roots, so commits and
/// replay never hold one another's lock while entries are being delivered.
#[derive(Debug)]
pub struct MemoryKeyedFrontierTable<K, V> {
    revision: FrontierRevision,
    root: Arc<MemoryKeyedFrontierRoot<K, V>>,
    wakeups: watch::Sender<FrontierRevision>,
}

impl<K, V> Default for MemoryKeyedFrontierTable<K, V> {
    fn default() -> Self {
        let (wakeups, _) = watch::channel(0);
        Self {
            revision: 0,
            root: Arc::new(MemoryKeyedFrontierRoot::default()),
            wakeups,
        }
    }
}

impl<K, V> MemoryKeyedFrontierTable<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn revision(&self) -> FrontierRevision {
        self.revision
    }

    pub fn get(&self, key: &K) -> Option<&FrontierEntry<K, V>> {
        self.root.entries.get(key)
    }

    pub fn view(&self) -> MemoryKeyedFrontierView<K, V> {
        MemoryKeyedFrontierView {
            root: Arc::clone(&self.root),
            through: self.revision,
            wakeups: self.wakeups.subscribe(),
        }
    }

    pub fn apply_at(
        &mut self,
        revision: FrontierRevision,
        mutations: impl IntoIterator<Item = FrontierMutation<K, V>>,
    ) -> KeyedFrontierResult<()> {
        if revision <= self.revision {
            return Err(KeyedFrontierError::NonAdvancingRevision {
                current: self.revision,
                next: revision,
            });
        }
        let root = Arc::make_mut(&mut self.root);
        for mutation in mutations {
            let (key, value) = match mutation {
                FrontierMutation::Put { key, value } => (key, Some(value)),
                FrontierMutation::Delete { key } => (key, None),
            };
            if let Some(previous) = root.entries.get(&key) {
                let keys = root
                    .keys_by_revision
                    .get_mut(&previous.revision)
                    .expect("frontier revision index missing entry");
                assert!(keys.remove(&key));
                if keys.is_empty() {
                    root.keys_by_revision.remove(&previous.revision);
                }
            }
            root.keys_by_revision
                .entry(revision)
                .or_default()
                .insert(key.clone());
            root.entries.insert(
                key.clone(),
                FrontierEntry {
                    key,
                    revision,
                    value,
                },
            );
        }
        self.revision = revision;
        self.wakeups.send_replace(revision);
        Ok(())
    }
}

#[async_trait]
pub trait MemoryKeyedFrontierSource<K, V>: Send + Sync
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn view(&self) -> KeyedFrontierResult<MemoryKeyedFrontierView<K, V>>;
}

struct OwnedMemoryKeyedFrontierSource<K, V> {
    inner: Arc<Mutex<MemoryKeyedFrontierTable<K, V>>>,
}

#[async_trait]
impl<K, V> MemoryKeyedFrontierSource<K, V> for OwnedMemoryKeyedFrontierSource<K, V>
where
    K: Ord + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    async fn view(&self) -> KeyedFrontierResult<MemoryKeyedFrontierView<K, V>> {
        Ok(self.inner.lock().await.view())
    }
}

#[derive(Debug, Clone)]
pub struct MemoryKeyedFrontier<K, V> {
    inner: Arc<Mutex<MemoryKeyedFrontierTable<K, V>>>,
}

impl<K, V> Default for MemoryKeyedFrontier<K, V> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryKeyedFrontierTable::default())),
        }
    }
}

pub struct MemoryKeyedFrontierTxn<K, V> {
    guard: OwnedMutexGuard<MemoryKeyedFrontierTable<K, V>>,
    staged: BTreeMap<K, Option<V>>,
    context: (),
    reserved_revision: Option<FrontierRevision>,
}

#[async_trait]
impl<K, V> KeyedFrontierTransaction<K, V> for MemoryKeyedFrontierTxn<K, V>
where
    K: Ord + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    fn isolation(&self) -> TransactionIsolation {
        TransactionIsolation::Serializable
    }

    fn context_mut(&mut self) -> &mut Self::Context {
        &mut self.context
    }

    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        if let Some(revision) = self.reserved_revision {
            return Ok(revision);
        }
        let revision = self
            .guard
            .revision()
            .checked_add(1)
            .expect("frontier revision overflow");
        self.reserved_revision = Some(revision);
        Ok(revision)
    }

    async fn get(&mut self, key: &K) -> KeyedFrontierResult<Option<V>> {
        Ok(self
            .staged
            .get(key)
            .cloned()
            .unwrap_or_else(|| self.guard.get(key).and_then(|entry| entry.value.clone())))
    }

    async fn put(&mut self, key: K, value: V) -> KeyedFrontierResult<()> {
        self.staged.insert(key, Some(value));
        Ok(())
    }

    async fn delete(&mut self, key: K) -> KeyedFrontierResult<()> {
        self.staged.insert(key, None);
        Ok(())
    }

    type Context = ();

    async fn commit(mut self) -> KeyedFrontierResult<FrontierRevision> {
        if self.staged.is_empty() && self.reserved_revision.is_none() {
            return Ok(self.guard.revision());
        }
        let revision = self.revision().await?;
        let mutations =
            std::mem::take(&mut self.staged)
                .into_iter()
                .map(|(key, value)| match value {
                    Some(value) => FrontierMutation::Put { key, value },
                    None => FrontierMutation::Delete { key },
                });
        self.guard.apply_at(revision, mutations)?;
        Ok(revision)
    }

    async fn rollback(self) -> KeyedFrontierResult<()> {
        Ok(())
    }
}

struct MemoryKeyedFrontierReader<K, V, S> {
    source: Arc<dyn MemoryKeyedFrontierSource<K, V>>,
    selector: S,
    limits: FrontierReadLimits,
    initial_root: Option<Arc<MemoryKeyedFrontierRoot<K, V>>>,
    initial_through: FrontierRevision,
    after: FrontierRevision,
    replay_complete_pending: bool,
    wakeups: watch::Receiver<FrontierRevision>,
}

impl<K, V, S> MemoryKeyedFrontierReader<K, V, S>
where
    K: Ord + Clone,
    V: Clone,
    S: MemoryKeyedFrontierSelector<K>,
{
    fn read_root(
        &self,
        root: &MemoryKeyedFrontierRoot<K, V>,
        after: FrontierRevision,
        through: FrontierRevision,
        initial: bool,
    ) -> (Vec<FrontierEntry<K, V>>, FrontierRevision) {
        let mut entries = Vec::new();
        let mut scanned_through = after;
        for (&revision, keys) in root.keys_by_revision.range((
            std::ops::Bound::Excluded(after),
            std::ops::Bound::Included(through),
        )) {
            let revision_entries = keys
                .iter()
                .filter_map(|key| {
                    let lower_bound = self.selector.lower_bound(key)?;
                    let entry = root
                        .entries
                        .get(key)
                        .expect("frontier revision index is stale");
                    (!initial || entry.revision > lower_bound).then(|| entry.clone())
                })
                .collect::<Vec<_>>();
            if !entries.is_empty()
                && !revision_entries.is_empty()
                && entries.len() + revision_entries.len() > self.limits.max_entries
            {
                break;
            }
            entries.extend(revision_entries);
            scanned_through = revision;
            if entries.len() >= self.limits.max_entries {
                break;
            }
        }
        if scanned_through == after
            || root
                .keys_by_revision
                .range((
                    std::ops::Bound::Excluded(scanned_through),
                    std::ops::Bound::Included(through),
                ))
                .next()
                .is_none()
        {
            scanned_through = through;
        }
        (entries, scanned_through)
    }
}

#[async_trait]
impl<K, V, S> KeyedFrontierReader<K, V> for MemoryKeyedFrontierReader<K, V, S>
where
    K: Ord + Clone + Send + Sync,
    V: Clone + Send + Sync,
    S: MemoryKeyedFrontierSelector<K>,
{
    async fn next(&mut self) -> KeyedFrontierResult<FrontierRead<K, V>> {
        loop {
            if let Some(root) = self.initial_root.as_ref() {
                let (entries, through) =
                    self.read_root(root, self.after, self.initial_through, true);
                self.after = through;
                if !entries.is_empty() {
                    return Ok(FrontierRead::Entries { entries, through });
                }
                assert_eq!(through, self.initial_through);
                self.initial_root = None;
                self.replay_complete_pending = true;
            }
            if self.replay_complete_pending {
                self.replay_complete_pending = false;
                return Ok(FrontierRead::ReplayComplete {
                    through: self.initial_through,
                });
            }

            self.wakeups.borrow_and_update();
            let view = self.source.view().await?;
            let (entries, through) = self.read_root(&view.root, self.after, view.through, false);
            self.after = through;
            if !entries.is_empty() {
                return Ok(FrontierRead::Entries { entries, through });
            }
            self.wakeups
                .changed()
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        }
    }
}

impl<K, V> MemoryKeyedFrontier<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn open_with_selector<S>(
        &self,
        selector: S,
        limits: FrontierReadLimits,
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<K, V>>>
    where
        S: MemoryKeyedFrontierSelector<K>,
    {
        let source: Arc<dyn MemoryKeyedFrontierSource<K, V>> =
            Arc::new(OwnedMemoryKeyedFrontierSource {
                inner: Arc::clone(&self.inner),
            });
        open_memory_keyed_frontier(source, selector, limits).await
    }
}

pub async fn open_memory_keyed_frontier<K, V, S>(
    source: Arc<dyn MemoryKeyedFrontierSource<K, V>>,
    selector: S,
    limits: FrontierReadLimits,
) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<K, V>>>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: MemoryKeyedFrontierSelector<K>,
{
    if limits.max_entries == 0 {
        return Err(KeyedFrontierError::EmptyReadLimit);
    }
    let view = source.view().await?;
    Ok(Box::new(MemoryKeyedFrontierReader {
        source,
        selector,
        limits,
        initial_root: Some(view.root),
        initial_through: view.through,
        after: 0,
        replay_complete_pending: false,
        wakeups: view.wakeups,
    }))
}

#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;
    use std::collections::BTreeMap;

    /// Supplies a frontier and selector constructors to the reusable
    /// KeyedFrontier contract suite. Backends can implement this once and run
    /// [`assert_keyed_frontier_contract`] against their own storage.
    pub trait KeyedFrontierContractHarness: Sync {
        type Key: Ord + Clone + std::fmt::Debug + Send + Sync + 'static;
        type Value: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static;
        type Frontier: KeyedFrontier<Self::Key, Self::Value> + Sync;

        fn frontier(&self) -> &Self::Frontier;

        fn key(&self, index: u64) -> Self::Key;

        fn value(&self, index: u64) -> Self::Value;

        fn values_match(&self, expected: &Self::Value, actual: &Self::Value) -> bool {
            expected == actual
        }

        fn all_selector(
            &self,
            after: FrontierRevision,
        ) -> <Self::Frontier as KeyedFrontier<Self::Key, Self::Value>>::Selector;

        fn keys_selector(
            &self,
            bounds: BTreeMap<Self::Key, FrontierRevision>,
        ) -> <Self::Frontier as KeyedFrontier<Self::Key, Self::Value>>::Selector;
    }

    async fn replay<R, K, V>(reader: &mut R) -> Vec<FrontierEntry<K, V>>
    where
        R: KeyedFrontierReader<K, V> + ?Sized,
        K: Send + Sync,
        V: Send + Sync,
    {
        let mut entries = Vec::new();
        loop {
            match reader.next().await.expect("frontier read") {
                FrontierRead::Entries {
                    entries: page,
                    through: _,
                } => entries.extend(page),
                FrontierRead::ReplayComplete { .. } => return entries,
            }
        }
    }

    async fn commit<K, V>(
        frontier: &impl KeyedFrontier<K, V>,
        mutations: impl IntoIterator<Item = FrontierMutation<K, V>>,
    ) -> FrontierRevision
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        let mut transaction = frontier.begin().await.expect("begin frontier transaction");
        let expected_revision = transaction
            .revision()
            .await
            .expect("reserve frontier revision");
        assert_eq!(
            transaction
                .revision()
                .await
                .expect("read reserved frontier revision"),
            expected_revision
        );
        for mutation in mutations {
            match mutation {
                FrontierMutation::Put { key, value } => transaction
                    .put(key, value)
                    .await
                    .expect("stage frontier put"),
                FrontierMutation::Delete { key } => transaction
                    .delete(key)
                    .await
                    .expect("stage frontier delete"),
            }
        }
        let revision = transaction
            .commit()
            .await
            .expect("commit frontier transaction");
        assert_eq!(revision, expected_revision);
        revision
    }

    fn assert_entry<H>(
        harness: &H,
        entry: &FrontierEntry<H::Key, H::Value>,
        key: &H::Key,
        revision: FrontierRevision,
        value: Option<&H::Value>,
    ) where
        H: KeyedFrontierContractHarness,
    {
        assert_eq!(&entry.key, key);
        assert_eq!(entry.revision, revision);
        match (value, entry.value.as_ref()) {
            (Some(expected), Some(actual)) => assert!(harness.values_match(expected, actual)),
            (None, None) => {}
            (Some(_), None) => panic!("expected a frontier value"),
            (None, Some(_)) => panic!("expected a frontier tombstone"),
        }
    }

    /// Runs the portable transaction, replay and live-handoff guarantees.
    pub async fn assert_keyed_frontier_contract<H>(harness: &H)
    where
        H: KeyedFrontierContractHarness,
    {
        let frontier = harness.frontier();
        let key1 = harness.key(1);
        let key2 = harness.key(2);
        let key3 = harness.key(3);
        let key4 = harness.key(4);
        let key5 = harness.key(5);
        let key6 = harness.key(6);
        let key7 = harness.key(7);
        let key8 = harness.key(8);
        let key9 = harness.key(9);
        let value10 = harness.value(10);
        let value20 = harness.value(20);
        let value30 = harness.value(30);
        let value40 = harness.value(40);
        let value50 = harness.value(50);
        let value60 = harness.value(60);
        let value61 = harness.value(61);
        let value70 = harness.value(70);
        let value80 = harness.value(80);
        let value90 = harness.value(90);
        let mut transaction = frontier.begin().await.expect("begin transaction");
        assert!(matches!(
            transaction.isolation(),
            TransactionIsolation::ReadCommitted | TransactionIsolation::Serializable
        ));
        let rolled_back_revision = transaction
            .revision()
            .await
            .expect("reserve rollback revision");
        assert_eq!(
            transaction
                .revision()
                .await
                .expect("read rollback revision"),
            rolled_back_revision
        );
        transaction
            .put(key1.clone(), value10.clone())
            .await
            .expect("stage put");
        assert_eq!(
            transaction.get(&key1).await.expect("read your write"),
            Some(value10.clone())
        );
        transaction
            .delete(key1.clone())
            .await
            .expect("stage delete");
        assert_eq!(
            transaction.get(&key1).await.expect("read your tombstone"),
            None
        );
        transaction.rollback().await.expect("rollback transaction");

        let mut transaction = frontier.begin().await.expect("begin after rollback");
        assert_eq!(
            transaction
                .get(&key1)
                .await
                .expect("read rolled back value"),
            None
        );
        assert_eq!(transaction.commit().await.expect("empty commit"), 0);

        assert!(matches!(
            frontier
                .open(
                    harness.all_selector(0),
                    FrontierReadLimits { max_entries: 0 }
                )
                .await,
            Err(KeyedFrontierError::EmptyReadLimit)
        ));

        let revision = commit(
            frontier,
            [
                FrontierMutation::Put {
                    key: key1.clone(),
                    value: value10.clone(),
                },
                FrontierMutation::Put {
                    key: key2.clone(),
                    value: value20.clone(),
                },
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: value30.clone(),
                },
            ],
        )
        .await;
        assert_eq!(revision, 1);

        let mut precommit_reader = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open pre-commit reader");
        let mut uncommitted = frontier
            .begin()
            .await
            .expect("begin uncommitted transaction");
        uncommitted
            .put(key9.clone(), value90.clone())
            .await
            .expect("stage uncommitted put");
        assert!(
            replay(&mut *precommit_reader)
                .await
                .iter()
                .all(|entry| entry.key != key9)
        );
        uncommitted
            .rollback()
            .await
            .expect("rollback uncommitted put");

        let mut reader = frontier
            .open(
                harness.all_selector(0),
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .expect("open bounded replay");
        let mut pages = Vec::new();
        let mut replay_markers = 0;
        loop {
            match reader.next().await.expect("bounded replay read") {
                FrontierRead::Entries { entries, through } => pages.push((entries, through)),
                FrontierRead::ReplayComplete { through } => {
                    assert_eq!(through, revision);
                    replay_markers += 1;
                    break;
                }
            }
        }
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].0.len(),
            3,
            "an atomic revision may exceed the soft limit"
        );
        assert_eq!(pages[0].1, revision);
        assert!(pages[0].0.iter().all(|entry| entry.revision == revision));
        assert_eq!(replay_markers, 1);

        let following_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key4.clone(),
                value: value40.clone(),
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } =
            reader.next().await.expect("following entry after replay")
        else {
            panic!("expected following entry")
        };
        assert_eq!(through, following_revision);
        assert_eq!(entries.len(), 1);
        assert_entry(
            harness,
            &entries[0],
            &key4,
            following_revision,
            Some(&value40),
        );

        let selector_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key5.clone(),
                value: value50.clone(),
            }],
        )
        .await;
        let mut selected = frontier
            .open(
                harness.keys_selector(BTreeMap::from([
                    (key4.clone(), 0),
                    (key5.clone(), selector_revision),
                ])),
                FrontierReadLimits::default(),
            )
            .await
            .expect("open selected replay");
        let selected_entries = replay(&mut *selected).await;
        assert_eq!(selected_entries.len(), 1);
        assert_entry(
            harness,
            &selected_entries[0],
            &key4,
            following_revision,
            Some(&value40),
        );

        let overwrite_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key1.clone(),
                value: harness.value(11),
            }],
        )
        .await;
        let mut collapsed = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open collapsed replay");
        let collapsed_entries = replay(&mut *collapsed).await;
        let collapsed_key1 = collapsed_entries
            .iter()
            .filter(|entry| entry.key == key1)
            .collect::<Vec<_>>();
        assert_eq!(collapsed_key1.len(), 1);
        let value11 = harness.value(11);
        assert_entry(
            harness,
            collapsed_key1[0],
            &key1,
            overwrite_revision,
            Some(&value11),
        );

        let repeated_revision = commit(
            frontier,
            [
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: harness.value(31),
                },
                FrontierMutation::Delete { key: key3.clone() },
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: harness.value(32),
                },
            ],
        )
        .await;
        let mut repeated = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open repeated-write replay");
        let repeated_entries = replay(&mut *repeated)
            .await
            .into_iter()
            .filter(|entry| entry.key == key3)
            .collect::<Vec<_>>();
        assert_eq!(repeated_entries.len(), 1);
        let value32 = harness.value(32);
        assert_entry(
            harness,
            &repeated_entries[0],
            &key3,
            repeated_revision,
            Some(&value32),
        );

        let tombstone_revision =
            commit(frontier, [FrontierMutation::Delete { key: key2.clone() }]).await;
        let mut tombstones = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open tombstone replay");
        assert_eq!(
            replay(&mut *tombstones)
                .await
                .into_iter()
                .find(|entry| entry.key == key2),
            Some(FrontierEntry {
                key: key2.clone(),
                revision: tombstone_revision,
                value: None,
            })
        );

        let boundary_value_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key6.clone(),
                value: value60.clone(),
            }],
        )
        .await;
        let mut boundary = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open boundary replay");
        let overwritten_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key6.clone(),
                value: value61.clone(),
            }],
        )
        .await;
        let replayed = replay(&mut *boundary).await;
        assert!(
            replayed
                .iter()
                .all(|entry| entry.revision <= boundary_value_revision)
        );
        let FrontierRead::Entries { entries, through } =
            boundary.next().await.expect("following overwritten value")
        else {
            panic!("expected following overwritten value")
        };
        assert_eq!(through, overwritten_revision);
        assert_eq!(entries.len(), 1);
        assert_entry(
            harness,
            &entries[0],
            &key6,
            overwritten_revision,
            Some(&value61),
        );

        let mut idle = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open idle reader");
        replay(&mut *idle).await;
        let (read, committed) = tokio::join!(
            idle.next(),
            commit(
                frontier,
                [FrontierMutation::Put {
                    key: key7,
                    value: value70,
                }],
            )
        );
        let idle_revision = committed;
        let FrontierRead::Entries { entries, through } = read.expect("woken reader") else {
            panic!("expected woken entry")
        };
        assert_eq!(through, idle_revision);
        let key7 = harness.key(7);
        let value70 = harness.value(70);
        assert_entry(harness, &entries[0], &key7, idle_revision, Some(&value70));

        let handoff_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key8,
                value: value80,
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } = idle
            .next()
            .await
            .expect("entry after replay-to-live handoff")
        else {
            panic!("expected handoff entry")
        };
        assert_eq!(through, handoff_revision);
        let key8 = harness.key(8);
        let value80 = harness.value(80);
        assert_entry(
            harness,
            &entries[0],
            &key8,
            handoff_revision,
            Some(&value80),
        );

        let mut selected_live = frontier
            .open(
                harness.keys_selector(BTreeMap::from([(key1.clone(), 0)])),
                FrontierReadLimits::default(),
            )
            .await
            .expect("open selected live reader");
        replay(&mut *selected_live).await;
        let _nonmatching_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key9,
                value: value90,
            }],
        )
        .await;
        let matching_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key1.clone(),
                value: harness.value(12),
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } = selected_live
            .next()
            .await
            .expect("selected reader crossed nonmatching revision")
        else {
            panic!("expected selected live entry")
        };
        assert_eq!(through, matching_revision);
        let value12 = harness.value(12);
        assert_entry(
            harness,
            &entries[0],
            &key1,
            matching_revision,
            Some(&value12),
        );
    }
}

#[async_trait]
impl<K, V> KeyedFrontier<K, V> for MemoryKeyedFrontier<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    type Selector = MemoryKeySelector<K>;
    type Context<'a>
        = ()
    where
        Self: 'a;
    type Transaction<'a>
        = MemoryKeyedFrontierTxn<K, V>
    where
        Self: 'a;

    async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>> {
        Ok(MemoryKeyedFrontierTxn {
            guard: Arc::clone(&self.inner).lock_owned().await,
            staged: BTreeMap::new(),
            context: (),
            reserved_revision: None,
        })
    }

    async fn begin_with_context<'a>(
        &'a self,
        _context: Self::Context<'a>,
    ) -> KeyedFrontierResult<Self::Transaction<'a>> {
        Ok(MemoryKeyedFrontierTxn {
            guard: Arc::clone(&self.inner).lock_owned().await,
            staged: BTreeMap::new(),
            context: (),
            reserved_revision: None,
        })
    }

    async fn open(
        &self,
        selector: Self::Selector,
        limits: FrontierReadLimits,
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<K, V> + '_>> {
        self.open_with_selector(selector, limits).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::rpc::{ObjChanged, PartEvent};
    use big_sync_core::{BuckId, ObjId, PartId};
    use sqlx_utils_rs::SqlCtx;
    use std::sync::Arc;
    use tokio::sync::Notify;

    struct MemoryContractHarness {
        frontier: MemoryKeyedFrontier<u64, u64>,
    }

    impl contract::KeyedFrontierContractHarness for MemoryContractHarness {
        type Key = u64;
        type Value = u64;
        type Frontier = MemoryKeyedFrontier<u64, u64>;

        fn frontier(&self) -> &Self::Frontier {
            &self.frontier
        }

        fn key(&self, index: u64) -> u64 {
            index
        }

        fn value(&self, index: u64) -> u64 {
            index
        }

        fn all_selector(&self, after: FrontierRevision) -> MemoryKeySelector<u64> {
            MemoryKeySelector::All { after }
        }

        fn keys_selector(&self, bounds: BTreeMap<u64, FrontierRevision>) -> MemoryKeySelector<u64> {
            MemoryKeySelector::Keys(bounds)
        }
    }

    #[tokio::test]
    async fn memory_keyed_frontier_contract() {
        contract::assert_keyed_frontier_contract(&MemoryContractHarness {
            frontier: MemoryKeyedFrontier::default(),
        })
        .await;
    }

    struct SqliteContractHarness {
        _sql: SqlCtx,
        frontier: SqlitePartFrontier,
    }

    impl SqliteContractHarness {
        async fn new() -> Self {
            let sql = SqlCtx::memory()
                .await
                .expect("create sqlite contract database");
            crate::sqlite_core::SqliteCore::init_schema(&sql.write_pool, BuckId::MAX_LEVEL)
                .await
                .expect("initialize sqlite contract schema");
            let core = crate::sqlite_core::SqliteCore::new(
                sql.clone(),
                "keyed-frontier-contract",
                BuckId::MAX_LEVEL,
            )
            .await
            .expect("create sqlite contract core");
            let frontier = SqlitePartFrontier::new(
                sql.read_pool.clone(),
                sql.write_pool.clone(),
                core.scope_id,
                Arc::new(Notify::new()),
            );
            Self {
                _sql: sql,
                frontier,
            }
        }
    }

    impl contract::KeyedFrontierContractHarness for SqliteContractHarness {
        type Key = PartFrontierKey;
        type Value = PartEvent;
        type Frontier = SqlitePartFrontier;

        fn frontier(&self) -> &Self::Frontier {
            &self.frontier
        }

        fn key(&self, index: u64) -> PartFrontierKey {
            let obj_id = ObjId::new([index as u8; 32]);
            if index == 2 {
                PartFrontierKey::Part {
                    obj_id,
                    part_id: PartId::new([index as u8; 32]),
                }
            } else {
                PartFrontierKey::Object(obj_id)
            }
        }

        fn value(&self, index: u64) -> PartEvent {
            let object_index = (index / 10) as u8;
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: if object_index == 2 {
                    vec![PartId::new([object_index; 32])]
                } else {
                    Vec::new()
                },
                obj_id: ObjId::new([object_index; 32]),
                payload: serde_json::json!({ "value": index }),
            })
        }

        fn values_match(&self, expected: &PartEvent, actual: &PartEvent) -> bool {
            match (expected, actual) {
                (PartEvent::Changed(expected), PartEvent::Changed(actual)) => {
                    expected.part_ids == actual.part_ids
                        && expected.obj_id == actual.obj_id
                        && expected.payload == actual.payload
                }
                _ => expected == actual,
            }
        }

        fn all_selector(&self, after: FrontierRevision) -> SqlitePartSelector {
            let mut selector = SqlitePartSelector::default();
            for index in 1..=9 {
                match self.key(index) {
                    PartFrontierKey::Object(obj_id) => {
                        selector.objects.insert(obj_id, after);
                    }
                    PartFrontierKey::Part { part_id, .. } => {
                        selector.parts.insert(part_id, after);
                    }
                }
            }
            selector
        }

        fn keys_selector(
            &self,
            bounds: BTreeMap<PartFrontierKey, FrontierRevision>,
        ) -> SqlitePartSelector {
            let mut selector = SqlitePartSelector::default();
            for (key, bound) in bounds {
                match key {
                    PartFrontierKey::Object(obj_id) => {
                        selector.objects.insert(obj_id, bound);
                    }
                    PartFrontierKey::Part { part_id, .. } => {
                        selector.parts.insert(part_id, bound);
                    }
                }
            }
            selector
        }
    }

    #[tokio::test]
    async fn sqlite_part_frontier_contract() {
        contract::assert_keyed_frontier_contract(&SqliteContractHarness::new().await).await;
    }

    struct SqliteContextProbe;

    struct SqliteContextProbeTransaction<'a> {
        transaction: Option<sqlx::Transaction<'a, sqlx::Sqlite>>,
    }

    #[async_trait]
    impl<'a> KeyedFrontierTransaction<u64, u64> for SqliteContextProbeTransaction<'a> {
        type Context = sqlx::Transaction<'a, sqlx::Sqlite>;

        fn isolation(&self) -> TransactionIsolation {
            TransactionIsolation::Serializable
        }

        fn context_mut(&mut self) -> &mut Self::Context {
            self.transaction
                .as_mut()
                .expect("probe transaction present")
        }

        async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
            Ok(0)
        }

        async fn get(&mut self, _key: &u64) -> KeyedFrontierResult<Option<u64>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe does not implement frontier operations",
            ))))
        }

        async fn put(&mut self, _key: u64, _value: u64) -> KeyedFrontierResult<()> {
            self.get(&_key).await.map(|_| ())
        }

        async fn delete(&mut self, _key: u64) -> KeyedFrontierResult<()> {
            self.get(&_key).await.map(|_| ())
        }

        async fn commit(mut self) -> KeyedFrontierResult<FrontierRevision> {
            self.transaction
                .take()
                .expect("probe transaction present")
                .commit()
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
            Ok(0)
        }

        async fn rollback(mut self) -> KeyedFrontierResult<()> {
            self.transaction
                .take()
                .expect("probe transaction present")
                .rollback()
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
        }
    }

    #[async_trait]
    impl KeyedFrontier<u64, u64> for SqliteContextProbe {
        type Selector = MemoryKeySelector<u64>;
        type Context<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
        type Transaction<'a> = SqliteContextProbeTransaction<'a>;

        async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe requires caller-owned transaction",
            ))))
        }

        async fn begin_with_context<'a>(
            &'a self,
            context: Self::Context<'a>,
        ) -> KeyedFrontierResult<Self::Transaction<'a>> {
            Ok(SqliteContextProbeTransaction {
                transaction: Some(context),
            })
        }

        async fn open(
            &self,
            _selector: Self::Selector,
            _limits: FrontierReadLimits,
        ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<u64, u64> + '_>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe does not implement reads",
            ))))
        }
    }

    #[tokio::test]
    async fn gat_context_round_trips_a_real_sqlite_transaction() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("create sqlite pool");
        let transaction = pool.begin().await.expect("begin sqlite transaction");
        let probe = SqliteContextProbe;
        let mut transaction = probe
            .begin_with_context(transaction)
            .await
            .expect("adapt caller-owned sqlite transaction");
        sqlx::query("CREATE TABLE context_probe (value INTEGER NOT NULL)")
            .execute(&mut **transaction.context_mut())
            .await
            .expect("use caller-owned sqlite transaction");
        transaction
            .rollback()
            .await
            .expect("rollback sqlite transaction");
        let table_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'context_probe'",
        )
        .fetch_one(&pool)
        .await
        .expect("verify rollback through pool");
        assert_eq!(table_count, 0);
    }

    #[tokio::test]
    async fn bounded_replay_never_splits_an_atomic_revision_and_then_follows() {
        let frontier = MemoryKeyedFrontier::<u64, &'static str>::default();
        let mut tx = frontier.begin().await.unwrap();
        tx.put(1, "one").await.unwrap();
        tx.put(2, "two").await.unwrap();
        tx.put(3, "three").await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 1);

        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .unwrap();
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected entries");
        };
        assert_eq!(entries.len(), 3);
        assert_eq!(through, 1);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 1 }
        );

        let mut tx = frontier.begin().await.unwrap();
        tx.delete(2).await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 2);
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected following entries");
        };
        assert_eq!(through, 2);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value, None);
    }

    #[tokio::test]
    async fn replay_pages_are_bounded_across_revisions() {
        let frontier = MemoryKeyedFrontier::<u64, u64>::default();
        for key in 1..=3 {
            let mut tx = frontier.begin().await.unwrap();
            tx.put(key, key).await.unwrap();
            let _revision = tx.commit().await.unwrap();
        }
        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .unwrap();
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected first page");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(through, 2);
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected second page");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(through, 3);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 3 }
        );
    }

    #[tokio::test]
    async fn commits_during_replay_follow_the_captured_boundary() {
        let frontier = MemoryKeyedFrontier::<u64, u64>::default();
        for key in 1..=2 {
            let mut tx = frontier.begin().await.unwrap();
            tx.put(key, key).await.unwrap();
            let _revision = tx.commit().await.unwrap();
        }
        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 1 },
            )
            .await
            .unwrap();

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected first replay page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(through, 1);

        let mut tx = frontier.begin().await.unwrap();
        tx.put(3, 3).await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 3);

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected second replay page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [2]
        );
        assert_eq!(through, 2);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 2 }
        );

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected following page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [3]
        );
        assert_eq!(through, 3);
    }
}
