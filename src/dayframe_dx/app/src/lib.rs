//! Shared Dayframe application grounding.
//!
//! This crate deliberately keeps the local data runtime independent of Dioxus. Platform roots
//! will inject an [`ApplicationBackend`] and a future UI layer will observe the data contracts
//! exposed here.

pub mod backend;
pub mod data;
pub mod features;

pub use backend::{
    ApplicationBackend, BackendError, BackendEvent, Command, CommandId, CommandOutcome,
    CommandReceipt, DataRow, MockApplicationBackend, PendingRefresh, QueryDelta, QueryKey,
    RefreshRequest, RefreshResponse, SourceCursor,
};
pub use data::{
    Collection, CollectionChange, CollectionError, CollectionEvent, LiveQuery, MutationError,
    RefreshCoordinator, RefreshDecision, RefreshError, RefreshOutcome, RefreshRegistry,
    RefreshToken, TransactionId, TransactionStatus,
};
pub use features::{
    BuildCapabilities, FeatureAttribute, FeatureContext, FeatureDefinition, FeatureId,
    FeatureRegistry, ReleaseChannel,
};
