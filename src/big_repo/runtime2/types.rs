// FIXME: cleanup/reintegrate the legacy items in here

use crate::interlude::*;

use crate::DocumentId;
use std::time::Duration;

// ─── Constants ─────────────────────────────────────────────────────────────────

const DEFAULT_DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
const DEFAULT_DOC_SYNC_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SUBDUCTION_NONCE_TTL: Duration = Duration::from_secs(60);
const DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(30);

// ─── SyncPolicy ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BigRepoSyncPolicy {
    pub(crate) doc_worker_idle_ttl: Duration,
    pub(crate) doc_sync_timeout: Duration,
    pub(crate) subduction_nonce_ttl: Duration,
    pub(crate) subduction_default_roundtrip_timeout: Duration,
}

impl Default for BigRepoSyncPolicy {
    fn default() -> Self {
        Self {
            doc_worker_idle_ttl: DEFAULT_DOC_WORKER_IDLE_TTL,
            doc_sync_timeout: DEFAULT_DOC_SYNC_TIMEOUT,
            subduction_nonce_ttl: DEFAULT_SUBDUCTION_NONCE_TTL,
            subduction_default_roundtrip_timeout: DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT,
        }
    }
}

// ─── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, displaydoc::Display)]
pub enum SyncDocPolicyError {
    /// The local or remote policy has no document definition.
    DocumentNotFound,
    /// The policy knows the document but denies the requested operation.
    InsufficientAccess,
    /// The policy rejected an identifier as malformed.
    InvalidIdentifier,
    /// The remote peer refused the request without exposing a finer reason.
    Unauthorized,
    /// The policy returned an implementation-specific rejection.
    Other(String),
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SyncDocError {
    /// Document not found
    NotFound,
    /// The remote peer refused the document request.
    Unauthorized,
    /// The storage policy rejected the sync: {0}
    Policy(SyncDocPolicyError),
    /// TransportError
    TransportError,
    /// IoError
    IoError(eyre::Report),
    /// Unexpected {0}
    Other(#[from] eyre::Report),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncDocOutcome {
    /// The active document worker merged the newly persisted state.
    Ready,
    /// State is durably stored, but no active worker was eager-materialized.
    Stored,
    /// State is stored but materialization is blocked by these dependencies.
    Pending(Vec<crate::runtime2::io::MaterializationBlocker>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncDocReceipt {
    pub outcome: SyncDocOutcome,
}

#[derive(Debug, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum GetDocError {
    /// document {0} is not found
    NotFound(DocumentId),
    /// document {0} is pending materialization
    PendingMaterialization(DocumentId),
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum CreateDocError {
    /// keyhive doc creation failed: {0}
    Keyhive(#[from] eyre::Report),
    /// storage put failed: {0}
    Put(#[from] PutDocError),
}

#[derive(thiserror::Error, displaydoc::Display, Debug)]
pub enum PutDocError {
    /// IdOccpuied {id}
    IdOccpuied { id: DocumentId },
    /// {0:}
    Other(#[from] eyre::Report),
}

// ─── DocLookup ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocLookup<T> {
    Missing,
    PendingMaterialization,
    Ready(T),
}

impl<T> DocLookup<T> {
    pub(crate) fn map_ready<U>(self, map: impl FnOnce(T) -> U) -> DocLookup<U> {
        match self {
            Self::Missing => DocLookup::Missing,
            Self::PendingMaterialization => DocLookup::PendingMaterialization,
            Self::Ready(value) => DocLookup::Ready(map(value)),
        }
    }

    pub fn into_ready(self, doc_id: DocumentId) -> Result<T, GetDocError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Missing => Err(GetDocError::NotFound(doc_id)),
            Self::PendingMaterialization => Err(GetDocError::PendingMaterialization(doc_id)),
        }
    }
}

// ─── LiveDocBundle ─────────────────────────────────────────────────────────────

/// Monotonic source of bundle ids. A global counter (not a per-worker one) so
/// a stale handle's id can never collide with a bundle served by a restarted
/// worker.
static NEXT_BUNDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    /// Unique id of this bundle instance. Commits carry it so the worker can
    /// reject commits originating from invalidated (broken) or replaced
    /// bundles.
    id: u64,
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: tokio::sync::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    partially_decrypted: std::sync::atomic::AtomicBool,
    #[educe(Debug(ignore))]
    broken: std::sync::atomic::AtomicBool,
    #[educe(Debug(ignore))]
    _runtime2_lease: Option<crate::runtime2::DocLease>,
}

impl LiveDocBundle {
    pub(crate) fn new(
        doc_id: DocumentId,
        doc: automerge::Automerge,
        lease: crate::runtime2::DocLease,
        partially_decrypted: bool,
    ) -> Self {
        Self {
            id: NEXT_BUNDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
            partially_decrypted: std::sync::atomic::AtomicBool::new(partially_decrypted),
            broken: std::sync::atomic::AtomicBool::new(false),
            _runtime2_lease: Some(lease),
        }
    }

    /// Identity used to correlate commit requests with this bundle instance.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Whether a commit from this bundle was rejected (no write access, key
    /// unavailable, ...). A broken bundle must be re-acquired to reload the
    /// last persisted state; further commits from it are rejected by the
    /// worker.
    pub fn is_broken(&self) -> bool {
        self.broken.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn mark_broken(&self) {
        self.broken.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether some locally stored Sedimentree heads are not represented in
    /// the materialized Automerge document because their keys are unavailable.
    pub fn is_partially_decrypted(&self) -> bool {
        self.partially_decrypted
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_partially_decrypted(&self, partial: bool) {
        self.partially_decrypted
            .store(partial, std::sync::atomic::Ordering::Release);
    }
}

// ─── Keyhive event type aliases ────────────────────────────────────────────────

pub(crate) type SignedAddKeyOp =
    keyhive_crypto::signed::Signed<keyhive_core::principal::individual::op::add_key::AddKeyOp>;
pub(crate) type SignedRotateKeyOp = keyhive_crypto::signed::Signed<
    keyhive_core::principal::individual::op::rotate_key::RotateKeyOp,
>;
pub(crate) type SignedCgkaOp = keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>;

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_lookup_into_ready() {
        let doc_id = DocumentId::new([23; 32]);

        let err = DocLookup::<()>::Missing
            .into_ready(doc_id)
            .expect_err("missing doc should fail");
        assert!(matches!(err, GetDocError::NotFound(id) if id == doc_id));

        let err = DocLookup::<()>::PendingMaterialization
            .into_ready(doc_id)
            .expect_err("pending doc should fail");
        assert!(matches!(err, GetDocError::PendingMaterialization(id) if id == doc_id));

        let ok = DocLookup::Ready(42usize)
            .into_ready(doc_id)
            .expect("ready doc should succeed");
        assert_eq!(ok, 42);
    }
}
