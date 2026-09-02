//! In-memory keyed-frontier implementation.

use crate::interlude::*;
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierMutation, FrontierRead, FrontierReadLimits, FrontierRevision,
    KeyedFrontier, KeyedFrontierError, KeyedFrontierReader, KeyedFrontierResult,
    KeyedFrontierTransaction, TransactionIsolation,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

pub trait MemoryKeyedFrontierSelector<K>: Send + Sync + 'static {
    /// `None` excludes the key. `Some(revision)` includes the key during
    /// initial replay only when its latest revision is newer than this bound.
    fn lower_bound(&self, key: &K) -> Option<FrontierRevision>;

    /// Whether a live reader should report source progress when every entry
    /// in the advanced range is filtered out.
    fn emit_empty_progress(&self) -> bool {
        false
    }
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
            let previous_after = self.after;
            let (entries, through) = self.read_root(&view.root, self.after, view.through, false);
            self.after = through;
            if !entries.is_empty() {
                return Ok(FrontierRead::Entries { entries, through });
            }
            if self.selector.emit_empty_progress() && through > previous_after {
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
