//! Durable state primitives for embedder-driven delta walkers.
//!
//! The repository stores consumer progress for one durable delta walker. It
//! does not read a source, schedule work, or own an event loop. Consumers that
//! need sparse per-key memory opt into the separate sparse-state capability.

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
    async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
}
