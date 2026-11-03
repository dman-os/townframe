//! Ordered key-value storage under durable Clay collections (ADR 025 §2).
//!
//! The store knows bytes and nothing else: no collections, rows, or schemas.
//! Callers encode `(collection, scope, local_key)` into a key so that a whole
//! collection, or one registration scope, is a contiguous key range.
//!
//! Beyond point reads, the contract is exactly two capabilities: a prefix scan
//! (load a collection or scope at bootstrap) and an atomic multi-key batch
//! (commit or remove a whole scope in one transaction). Keys order bytewise,
//! lexicographically, with a shorter key sorting before any longer key it is a
//! prefix of; every backend must agree, and the shared contract suite in
//! `tests.rs` pins that.
//!
//! Clay is single-threaded and the browser's IndexedDB futures are `!Send`, so
//! the trait's futures carry no `Send` bound.

mod memory;
pub use memory::MemoryStore;

#[cfg(not(target_arch = "wasm32"))]
use sqlite_rs as sqlite_rs_alias;
#[cfg(not(target_arch = "wasm32"))]
mod sqlite {
    pub(crate) mod queries {
        include!("sqlite/gen.rs");
    }
}
#[cfg(not(target_arch = "wasm32"))]
mod sqlite_store;
#[cfg(not(target_arch = "wasm32"))]
pub use sqlite_store::SqliteStore;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod indexed_db;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexed_db::IndexedDbStore;

#[cfg(test)]
mod tests;

/// One mutation inside an atomic [`OrderedStore::apply`] batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchOp {
    /// Insert or overwrite `key`.
    Put(Vec<u8>, Vec<u8>),
    /// Remove `key`; absent keys are not an error.
    Delete(Vec<u8>),
    /// Remove every key that starts with the prefix, the prefix itself included.
    DeletePrefix(Vec<u8>),
}

/// A backend I/O failure: disk, quota, or a browser transaction abort.
///
/// Misuse of the store is a programming error and panics instead.
#[derive(Debug, thiserror::Error)]
#[error("clay store backend failure: {0}")]
pub struct StoreError(String);

impl StoreError {
    pub(crate) fn backend(message: impl std::fmt::Display) -> Self {
        Self(message.to_string())
    }
}

// Native `async fn` in a public trait is deliberate: the futures must stay
// `!Send` for IndexedDB, and `async_fn_in_trait` only warns that callers cannot
// demand `Send`, which is the point here.
#[expect(async_fn_in_trait, reason = "futures must stay !Send for IndexedDB")]
pub trait OrderedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;

    /// Every entry whose key starts with `prefix`, ascending by key. The empty
    /// prefix scans the whole store.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError>;

    /// Applies `batch` in order as one transaction: afterwards either every op
    /// took effect or none did. Later ops observe earlier ones, so a
    /// `DeletePrefix` followed by a `Put` under that prefix keeps the put.
    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError>;
}

/// The smallest key greater than every key that starts with `prefix`, or `None`
/// when no such key exists (the empty prefix and all-`0xFF` prefixes): those
/// ranges are unbounded above. The scan range is `[prefix, successor)`.
#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_arch = "wasm32", target_os = "unknown")
))]
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|byte| *byte != 0xFF)?;
    let mut successor = prefix[..=last_incrementable].to_vec();
    successor[last_incrementable] += 1;
    Some(successor)
}
