use crate::interlude::*;

pub trait OkvTxn {}

/// An version ordered key-value store
///
/// key -> value,revid
///
/// where revid is is a store global, monotonically increasing
/// txn counter of the transaction that last wrote to the key.
/// It's then expected to be able to list keys ordered by their txnid.
///
/// To understand it's data model
/// and one can do range queries using the
#[async_trait]
pub trait OkvStore {
    async fn latest_rev() -> Res<u64>;
}
