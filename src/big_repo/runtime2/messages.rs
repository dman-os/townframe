//! runtime2 messages. Uses `futures::channel::oneshot` for request/response;
//! no Tokio types.

use crate::interlude::*;
use crate::DocumentId;
use big_sync_core::PeerId;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Commands into the runtime hub (from `Runtime2Handle`).
#[derive(educe::Educe)]
#[educe(Debug)]
pub enum Runtime2Cmd {
    /// Create a document. The handle sends this; the hub asynchronously calls
    /// [`RuntimeIo::create_document`], then enqueues a [`PutDoc`] to itself.
    ///
    /// [`RuntimeIo::create_document`]: super::RuntimeIo::create_document
    /// [`PutDoc`]: Self::PutDoc
    CreateDoc {
        #[educe(Debug(ignore))]
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
    },
    /// Internal: persist a document whose doc_id is already resolved.
    /// The hub sends this to itself after `CreateDoc` completes.
    PutDoc {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        initial_content: Box<automerge::Automerge>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
    },
    GetDocHandle {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<
                crate::runtime2::types::DocLookup<Arc<crate::runtime2::types::LiveDocBundle>>,
            >,
        >,
    },
    CommitDelta {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        #[educe(Debug(ignore))]
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    DocHeadState {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
    },
    InspectDocHeadState {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Option<crate::runtime2::DocHeadState>>,
        >,
    },
    OpenConn {
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerId,
                Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    },
    AcceptConn {
        incoming: Box<dyn std::any::Any + Send>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerId,
                Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    },
    CloseConn {
        peer_id: PeerId,
        /// End flag of the specific connection being closed. Subduction
        /// tracks multiple connections per peer, so a close must identify
        /// WHICH connection it targets; the peer's registration is only
        /// torn down when this flag matches the peer's current connection.
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        #[educe(Debug(ignore))]
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<std::time::Duration>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
        >,
    },
    FinalizeDocSync {
        sync_id: u64,
        doc_id: DocumentId,
        peer_id: PeerId,
        #[educe(Debug(ignore))]
        transport: crate::runtime2::io::SyncDocAttempt,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
        >,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    WaitForKeyhiveReconciliation {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    CancelDocSyncWaiter {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
    },
    CancelKeyhiveSyncWaiter {
        peer_id: PeerId,
        waiter_id: u64,
    },
    RegisterDocLease {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        registered: futures::channel::oneshot::Sender<()>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
    ReleaseInternalLease {
        doc_id: DocumentId,
    },
    ContainsSedimentree {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    },
    HasLocalDocState {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    },
    #[cfg(test)]
    HasDocWorker {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    },
    InspectStoredDocBlobs {
        sed_id: sedimentree_core::id::SedimentreeId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
    },
    /// Wait until all finite runtime work currently admitted to the Hub and
    /// document workers has drained. Pending decryption is quiescent; this
    /// does not wait for unavailable keys.
    WaitForQuiescence {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
}

/// Events from background workers / keyhive listener / sync sessions / doc-workers.
#[derive(educe::Educe)]
#[educe(Debug)]
pub enum Runtime2Evt {
    SyncSessionObserved {
        #[educe(Debug(ignore))]
        cause: tracing::Span,
        session: subduction_core::sync_session::SyncSession,
    },
    ConnEstablished {
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    },
    ConnLost {
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
        error: Option<String>,
    },
    KeyhiveSyncDone {
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
        changed: bool,
    },
    /// Initiating a keyhive sync failed before the protocol could emit a
    /// completion event. The hub uses this to resolve the public waiter
    /// instead of allowing a network error to panic a child task.
    KeyhiveSyncFailed {
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
        error: String,
    },
    /// Completion of the cache refresh admitted by a quiescence barrier.
    QuiescenceCacheRefreshDone {
        result: eyre::Result<()>,
    },
    /// Event-log cursor captured when a quiescence barrier is admitted.
    QuiescenceGroupPartWatermark {
        barrier_id: u64,
        result: eyre::Result<u64>,
    },
    KeyhiveReconciliationCaptured {
        result: eyre::Result<u64>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    /// The persisted Keyhive-derived partition cursor advanced.
    GroupPartWorkerAdvanced {
        cursor: u64,
    },
    DocWorkerStopped {
        doc_id: DocumentId,
    },
    FatalWorkerError {
        doc_id: Option<DocumentId>,
        context: &'static str,
        error: String,
    },
    DocWorkerMaterializationPending {
        doc_id: DocumentId,
    },
    DocWorkerMaterializationReady {
        doc_id: DocumentId,
    },
    DocWorkerMaterializationRetryCompleted {
        doc_id: DocumentId,
        status: crate::runtime2::MaterializationStatus,
    },
    /// A document worker reached a quiescence barrier in mailbox order.
    DocWorkerQuiescent {
        doc_id: DocumentId,
        barrier_id: u64,
    },
    PrekeyExpanded {
        new_prekey: Arc<crate::runtime2::types::SignedAddKeyOp>,
    },
    PrekeyRotated {
        rotate_key: Arc<crate::runtime2::types::SignedRotateKeyOp>,
    },
    CgkaOp {
        data: Arc<crate::runtime2::types::SignedCgkaOp>,
    },
    DelegationReceived {
        target: keyhive_core::principal::identifier::Identifier,
        data: Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::group::delegation::Delegation<
                    future_form::Sendable,
                    keyhive_crypto::signer::memory::MemorySigner,
                    Vec<u8>,
                    crate::keyhive_listener::BigRepoKeyhiveListener,
                >,
            >,
        >,
    },
    RevocationReceived {
        target: keyhive_core::principal::identifier::Identifier,
        data: Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::group::revocation::Revocation<
                    future_form::Sendable,
                    keyhive_crypto::signer::memory::MemorySigner,
                    Vec<u8>,
                    crate::keyhive_listener::BigRepoKeyhiveListener,
                >,
            >,
        >,
    },
}

/// The doc-worker's mailbox. `_lease` fields keep the worker alive for the
/// duration of the op (see [`crate::runtime2::DocWorkerInternalLease`]).
#[derive(educe::Educe)]
#[educe(Debug)]
pub enum DocWorkerMsg {
    PutDoc {
        #[educe(Debug(ignore))]
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
    },
    AcquireHandle {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<
                crate::runtime2::types::DocLookup<Arc<crate::runtime2::types::LiveDocBundle>>,
            >,
        >,
    },
    CommitDelta {
        #[educe(Debug(ignore))]
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        #[educe(Debug(ignore))]
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    ApplyReceivedContent {
        peer_id: PeerId,
        commit_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
        fragment_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
    },
    FinalizeAfterSync {
        sync_id: u64,
        #[educe(Debug(ignore))]
        transport: crate::runtime2::io::SyncDocAttempt,
        peer_id: PeerId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
        >,
    },
    ReattemptMaterialization {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::MaterializationStatus, String>,
        >,
    },
    QueryHeadState {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
    },
    InspectHeadState {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Option<crate::runtime2::DocHeadState>>,
        >,
    },
    /// Mailbox-ordered runtime quiescence barrier.
    Quiesce {
        barrier_id: u64,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
}

/// Monotonic waiter-id counters (shared handle↔hub).
pub fn fresh_waiter_id(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
