//! Optional sparse per-consumer memory for delta-walker settlements.
//!
//! This capability is separate from the generic cursor and settlement contract,
//! but its transaction is the same transaction as the walker state transaction.
//! Implementations therefore retain atomicity between sparse memory writes and
//! upstream cursor advancement.

use crate::delta_walker_state::{
    DeltaWalkerStateRepo, DeltaWalkerStateResult, DeltaWalkerStateTransaction,
};
use async_trait::async_trait;

/// Sparse state operations available on a delta-walker transaction.
#[async_trait]
pub trait DeltaWalkerSparseStateTransaction: DeltaWalkerStateTransaction {
    async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()>;
    async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()>;
}

/// Optional sparse state repository capability for a delta-walker consumer.
#[async_trait]
pub trait DeltaWalkerSparseStateRepo: DeltaWalkerStateRepo {
    async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>>;
    async fn get_many(&self, keys: &[Vec<u8>]) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>>;
}
