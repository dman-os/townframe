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
#[async_trait]
pub trait OkvStore: RevSource<Selector = Selector<<Self as RevSource>::Key>> {
    /// The caller-owned transaction `apply` and `get` work in.
    type Tx: Send;

    /// Stage one atomic batch in `tx` and return its provisional rev, one past the previous
    /// batch's (committed, or staged earlier in the same transaction). Within a batch the last
    /// mutation of a key wins. An empty batch is a caller bug and panics: it has no rows to
    /// stamp.
    async fn apply<I>(&self, tx: &mut Self::Tx, mutations: I) -> Res<Rev>
    where
        I: IntoIterator<Item = Mutation<Self::Key, Self::Value>> + Send,
        I::IntoIter: Send;

    /// The key's value as `tx` sees it: its own staged write if it has one (a staged delete
    /// reads as `None`), otherwise the committed row. A tombstone and a key never written both
    /// read as `None`. The read is part of the transaction: a backend that validates at commit
    /// fails the commit if the key's table moved after the transaction first touched it.
    async fn get(&self, tx: &mut Self::Tx, key: &Self::Key) -> Res<Option<Self::Value>>;
}

/// What a row costs against a byte budget; see the module docs.
pub(crate) fn row_bytes<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: &K, value: Option<&V>) -> usize {
    key.as_ref().len() + value.map_or(0, |value| value.as_ref().len())
}
