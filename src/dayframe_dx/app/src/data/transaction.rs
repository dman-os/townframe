use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::backend::{BackendError, CommandId};

/// Identifies one optimistic intent independently of backend request IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TransactionId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransactionStatus {
    LocallyApplied,
    Accepted,
    RolledBack,
}

#[derive(Debug, Error, PartialEq)]
pub enum MutationError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error("command was rejected by the backend")]
    Rejected,
    #[error("backend receipt command {actual:?} did not match submitted command {expected:?}")]
    CommandIdMismatch {
        expected: CommandId,
        actual: CommandId,
    },
}
