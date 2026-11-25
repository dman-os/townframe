//! Ordered key-value store over the rev read contract.
//!
//! An okv store keeps one row per key: `key -> (rev, value)`, where `rev` is the store-wide
//! [`Rev`] of the batch that last wrote the key and a deleted key keeps its row as a tombstone.
//! Reads list rows in `(rev, key)` order through [`RevSource`], so a consumer that remembers
//! one rev can ask for everything that changed after it. This replaces the keyed frontier.
//!
//! The only write path is [`OkvStore::apply`], called inside a transaction the caller owns: one
//! call is one atomic batch with its own rev, and the rev counter lives in the same transaction,
//! so a rollback consumes no rev. [`OkvStore::get`] reads a key inside the same transaction. The store moves its [`RevWatch`](crate::rev::RevWatch) from
//! the transaction's commit, never by hand, and never for a rollback.
//!
//! # Pruning by consumer cursors
//!
//! A delete keeps its row as a tombstone, and a table whose rows are deleted as they are
//! handled (a queue of wants) would grow one tombstone per key ever written. Live rows are
//! state and are never dropped, but a tombstone only matters to a reader that has not yet gone
//! past its rev, so consumers say how far they have gone and the store drops what all of them
//! have passed:
//!
//! - a consumer [registers](OkvStore::register_consumer) under a name, which holds pruning back
//!   from cursor 0, and [reports](OkvStore::report_cursor) the rev it has gone past: every row
//!   at or below it has been taken in, the tombstones among them included. A consumer that
//!   never reports holds pruning back, visibly: [`OkvStore::consumers`] lists every cursor.
//! - [`OkvStore::prune`] drops the tombstones at or below the lowest registered cursor. With no
//!   consumer registered it drops nothing: an unknown reader may still need them.
//!
//! These verbs are not part of a transaction: a report is a hint that can only delay a prune
//! when lost, and a prune changes no live row, consumes no rev and wakes nobody. A registered
//! reader never misses a tombstone; one that reads from a rev below the floor without being
//! registered sees the live rows and only the tombstones above the floor.
//!
//! Byte budgets: an entry costs its key bytes plus its value bytes against
//! [`Limits::max_bytes`](crate::rev::Limits) (a tombstone's value costs nothing). Every backend
//! charges the same so their pages agree.

use std::ops::Bound;
use std::ops::RangeBounds;

use crate::interlude::*;
use crate::rev::Rev;
use crate::rev::RevSource;

pub mod memory;

#[cfg(any(test, feature = "test-support"))]
pub mod laws;
#[cfg(any(test, feature = "test-support"))]
pub mod oracle;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
}

impl<K, V> Mutation<K, V> {
    /// The row the mutation leaves: its key and its value, `None` for a delete.
    pub fn into_row(self) -> (K, Option<V>) {
        match self {
            Self::Put { key, value } => (key, Some(value)),
            Self::Delete { key } => (key, None),
        }
    }
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq)]]
    /// Which keys a read is about. Selectors richer than this (parts, principals) belong to the
    /// store built on okv, not to okv.
    pub enum Selector<K> {
        All,
        Keys(BTreeSet<K>),
        Range(pub struct KeyRange<K> {
            pub start: Bound<K>,
            pub end: Bound<K>,
        }),
    }
}

impl<K: Ord> Selector<K> {
    pub fn matches(&self, key: &K) -> bool {
        match self {
            Self::All => true,
            Self::Keys(keys) => keys.contains(key),
            Self::Range(KeyRange { start, end }) => (start.as_ref(), end.as_ref()).contains(key),
        }
    }
}

/// What [`OkvStore::prune`] dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pruned {
    /// The lowest registered cursor, which tombstones at or below were dropped for: `0` when
    /// no consumer is registered, which drops nothing.
    pub floor: Rev,
    /// How many tombstones this call dropped (not counting ones an earlier prune dropped).
    pub tombstones: usize,
}

/// An okv store: a [`RevSource`] over its rows, plus the transactional verbs.
///
/// Implementations assign `apply`'s rev inside the caller's transaction (the counter is part of
/// the transaction's writes), publish every batch of the transaction together at commit, and
/// move the watch only after the commit is visible to `read`.
///
/// Every rev `apply` returns is provisional: it is the batch's rev only if the transaction
/// commits. A rolled back transaction consumes no rev, and a backend whose commit can fail (the
/// memory backend's optimistic commit) consumes none either when it does. A backend that holds
/// its write lock for the whole transaction (SQLite) never fails this way, but callers must not
/// rely on that.
pub trait OkvStore: RevSource<Selector = Selector<<Self as RevSource>::Key>> {
    /// The caller-owned transaction `apply` and `get` work in.
    type Tx;

    /// Stage one atomic batch in `tx` and return its provisional rev, one past the previous
    /// batch's (committed, or staged earlier in the same transaction). Within a batch the last
    /// mutation of a key wins. An empty batch is a caller bug and panics: it has no rows to
    /// stamp.
    fn apply(
        &self,
        tx: &mut Self::Tx,
        mutations: Vec<Mutation<Self::Key, Self::Value>>,
    ) -> impl Future<Output = Res<Rev>> + Send;

    /// The key's value as `tx` sees it: its own staged write if it has one (a staged delete
    /// reads as `None`), otherwise the committed row. A tombstone and a key never written both
    /// read as `None`. The read is part of the transaction: a backend that validates at commit
    /// fails the commit if the key's table moved after the transaction first touched it.
    fn get(
        &self,
        tx: &mut Self::Tx,
        key: &Self::Key,
    ) -> impl Future<Output = Res<Option<Self::Value>>> + Send;

    /// Register `consumer` and return its cursor: `0` for a new consumer, which holds every
    /// tombstone back, and the stored cursor for one registered before (a restart registers
    /// again and resumes). Durable with the table where the table is.
    fn register_consumer(&self, consumer: &str) -> impl Future<Output = Res<Rev>> + Send;

    /// Record that `consumer` has gone past `cursor`. A cursor is monotone: one at or below the
    /// recorded one changes nothing. Panics for a consumer that is not registered, and for a
    /// cursor past the table's latest rev: both are caller bugs.
    fn report_cursor(&self, consumer: &str, cursor: Rev) -> impl Future<Output = Res<()>> + Send;

    /// Stop holding pruning back for `consumer`. A consumer that is not registered is a
    /// no-op.
    fn unregister_consumer(&self, consumer: &str) -> impl Future<Output = Res<()>> + Send;

    /// Every registered consumer and its cursor.
    fn consumers(&self) -> impl Future<Output = Res<BTreeMap<String, Rev>>> + Send;

    /// Drop the tombstones at or below the lowest registered cursor; see the module docs.
    fn prune(&self) -> impl Future<Output = Res<Pruned>> + Send;
}

/// What a row costs against a byte budget (its key bytes plus its value bytes); see the module docs.
pub fn row_bytes<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: &K, value: Option<&V>) -> usize {
    key.as_ref().len() + value.map_or(0, |value| value.as_ref().len())
}
