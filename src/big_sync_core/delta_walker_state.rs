//! Durable state primitives for embedder-driven delta walkers.
//!
//! The repository stores only consumer progress and sparse opaque per-key
//! state. It does not read a source, schedule work, or own an event loop.

use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaWalkerProgress {
    pub upstream_revision: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DeltaWalkerStateError {
    #[error("delta walker revision {next} did not advance beyond {current}")]
    NonAdvancingRevision { current: u64, next: u64 },
    #[error("delta walker progress changed while advancing from {expected}")]
    StaleProgress { expected: u64 },
    #[error("delta walker state backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

pub type DeltaWalkerStateResult<T> = Result<T, DeltaWalkerStateError>;

/// Transaction over one stable consumer's durable walker state.
#[async_trait]
pub trait DeltaWalkerStateTransaction: Send {
    type Context: Send;

    fn context_mut(&mut self) -> &mut Self::Context;
    async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress>;
    async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()>;
    async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()>;
    async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()>;
    async fn commit(self) -> DeltaWalkerStateResult<()>;
    async fn rollback(self) -> DeltaWalkerStateResult<()>;
}

/// Passive durable state for one stable consumer in one namespace.
#[async_trait]
pub trait DeltaWalkerStateRepo: Send + Sync {
    type Context<'a>: Send
    where
        Self: 'a;
    type Transaction<'a>: DeltaWalkerStateTransaction<Context = Self::Context<'a>> + 'a
    where
        Self: 'a;

    async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress>;
    /// Read one sparse state value without opening a write transaction.
    async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>>;
    /// Read the present values for a bounded set of sparse keys. Missing keys
    /// are omitted; returned keys are the original opaque state keys.
    async fn get_many(&self, keys: &[Vec<u8>]) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
}
