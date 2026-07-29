//! Support types migrated from the obsolete runtime.rs.
//!
//! These are the types that runtime2 and its peers (keyhive_listener, native
//! backend, lib.rs re-exports) still import from the old `crate::runtime`
//! module.  Kept in a separate file so the old runtime implementation can be
//! deleted without disrupting the consumer sites.

use crate::interlude::*;

use crate::keyhive_listener::BigRepoKeyhiveListener;
use crate::DocumentId;
use future_form::Sendable;
use keyhive_core::principal::identifier::Identifier;
use std::sync::Arc;
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

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: tokio::sync::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    partially_decrypted: std::sync::atomic::AtomicBool,
    #[educe(Debug(ignore))]
    _runtime2_lease: Option<crate::runtime2::DocLease>,
}

impl LiveDocBundle {
    pub(crate) fn new_runtime2(
        doc_id: DocumentId,
        doc: automerge::Automerge,
        lease: crate::runtime2::DocLease,
    ) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
            partially_decrypted: std::sync::atomic::AtomicBool::new(false),
            _runtime2_lease: Some(lease),
        }
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

// ─── Minimal RuntimeEvt (only variants still consumed by runtime2 / keyhive_listener) ──

/// Events emitted by the keyhive listener and forwarded to the runtime2 event loop.
///
/// **Only** variants that are still sent or consumed by `keyhive_listener` and
/// `runtime2/native` are retained here.  The remaining variants
/// (`SyncSessionObserved`, `ConnEstablishedIroh`, `ConnLostIroh`, …) belonged to
/// the old runtime and have been removed.
pub(crate) enum RuntimeEvt {
    KeyhiveSyncDone {
        peer_id: crate::PeerId,
        request_id: subduction_keyhive::message::RequestId,
        changed: bool,
    },
    PrekeyExpanded {
        new_prekey: Arc<SignedAddKeyOp>,
    },
    PrekeyRotated {
        rotate_key: Arc<SignedRotateKeyOp>,
    },
    CgkaOp {
        data: Arc<SignedCgkaOp>,
    },
    DelegationReceived {
        target: Identifier,
        data: Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::group::delegation::Delegation<
                    Sendable,
                    keyhive_crypto::signer::memory::MemorySigner,
                    Vec<u8>,
                    BigRepoKeyhiveListener,
                >,
            >,
        >,
    },
    RevocationReceived {
        target: Identifier,
        data: Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::group::revocation::Revocation<
                    Sendable,
                    keyhive_crypto::signer::memory::MemorySigner,
                    Vec<u8>,
                    BigRepoKeyhiveListener,
                >,
            >,
        >,
    },
}

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
