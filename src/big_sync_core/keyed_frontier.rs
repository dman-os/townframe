//! Contracts for keyed, collapsed change frontiers.
//!
//! Concrete backends own persistence and notification. The reader contract
//! owns bounded replay, the replay-complete boundary and live wakeup handoff.
//! Storage is a latest-row-per-key map: a commit publishes one revision for
//! all of its mutations, and an overwrite replaces the prior row (including
//! with a retained tombstone). This is not append-only history.
//!
//! `open` captures a replay revision. Rows still current at or below that
//! revision may be replayed, then `ReplayComplete` marks the handoff; rows
//! published later are delivered only after that marker. A backend may retain
//! an immutable root for replay, but consumers must not rely on a general
//! point-in-time snapshot beyond this handoff contract.

/// Globally ordered position assigned to one atomic mutation batch.
use std::num::NonZeroUsize;

pub type FrontierRevision = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionIsolation {
    ReadCommitted,
    Serializable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontierMutation<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontierEntry<K, V> {
    pub key: K,
    pub revision: FrontierRevision,
    /// `None` is a retained deletion tombstone.
    pub value: Option<V>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrontierReadLimits {
    /// Soft limit: one atomic revision is never split across batches.
    pub max_entries: NonZeroUsize,
}

impl Default for FrontierReadLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(256).expect("literal is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontierRead<K, V> {
    Entries {
        entries: Vec<FrontierEntry<K, V>>,
        /// Every matching entry at or below this revision was represented by
        /// this or an earlier read.
        through: FrontierRevision,
    },
    /// The fixed frontier captured by `open` has been fully replayed. Every
    /// later `Entries` item is from the following/live phase.
    ReplayComplete { through: FrontierRevision },
}

#[derive(Debug, thiserror::Error)]
pub enum KeyedFrontierError {
    #[error("frontier revision {next} did not advance beyond {current}")]
    NonAdvancingRevision {
        current: FrontierRevision,
        next: FrontierRevision,
    },
    #[error("keyed frontier backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

pub type KeyedFrontierResult<T> = Result<T, KeyedFrontierError>;

/// Transaction over a keyed frontier.
///
/// The portable contract is read-your-writes plus atomic publication of all
/// staged mutations. `isolation()` reports any stronger guarantee. The
/// associated context can borrow caller-owned backend state for the duration
/// of the transaction and is returned after commit or rollback.
#[async_trait::async_trait]
pub trait KeyedFrontierTransaction<K, V>: Send
where
    K: Send + Sync,
    V: Send + Sync,
{
    type Context: Send;

    fn context_mut(&mut self) -> &mut Self::Context;

    /// Reserves a stable revision inside the backend transaction.
    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision>;

    fn isolation(&self) -> TransactionIsolation;

    async fn get(&mut self, key: &K) -> KeyedFrontierResult<Option<V>>;

    async fn put(&mut self, key: K, value: V) -> KeyedFrontierResult<()>;

    async fn delete(&mut self, key: K) -> KeyedFrontierResult<()>;

    async fn commit(self) -> KeyedFrontierResult<FrontierRevision>;

    async fn rollback(self) -> KeyedFrontierResult<()>;
}

#[async_trait::async_trait]
pub trait KeyedFrontierReader<K, V>: Send
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Returns bounded entries, a one-time replay boundary, or waits for a
    /// commit which may make matching entries available.
    async fn next(&mut self, limits: FrontierReadLimits)
    -> KeyedFrontierResult<FrontierRead<K, V>>;
}

#[async_trait::async_trait]
pub trait KeyedFrontier<K, V>: Send + Sync
where
    K: Send + Sync,
    V: Send + Sync,
{
    type Selector: Send + Sync + 'static;

    /// Caller-owned state carried by a transaction, possibly borrowing `'a`.
    type Context<'a>: Send
    where
        Self: 'a;

    /// Concrete transaction state tied to the frontier borrow and context.
    type Transaction<'a>: KeyedFrontierTransaction<K, V, Context = Self::Context<'a>> + 'a
    where
        Self: 'a;

    async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>>;

    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> KeyedFrontierResult<Self::Transaction<'a>>;

    async fn open(
        &self,
        selector: Self::Selector,
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<K, V> + '_>>;
}
