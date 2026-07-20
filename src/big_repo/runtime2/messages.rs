//! runtime2 messages. Uses `futures::channel::oneshot` for request/response;
//! no Tokio types.

use crate::interlude::*;
use crate::DocumentId;
use big_sync_core::PeerId;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Commands into the runtime hub (from `Runtime2Handle`).
pub enum Runtime2Cmd {
    /// Create a document. The handle sends this; the hub asynchronously calls
    /// [`RuntimeIo::create_document`], then enqueues a [`PutDoc`] to itself.
    ///
    /// [`RuntimeIo::create_document`]: super::RuntimeIo::create_document
    /// [`PutDoc`]: Self::PutDoc
    CreateDoc {
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    },
    /// Internal: persist a document whose doc_id is already resolved.
    /// The hub sends this to itself after `CreateDoc` completes.
    PutDoc {
        doc_id: DocumentId,
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>,
        >,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    DocHeadState {
        doc_id: DocumentId,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
    },
    OpenConn {
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, Arc<std::sync::atomic::AtomicBool>)>,
        >,
    },
    AcceptConn {
        incoming: Box<dyn std::any::Any + Send>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, Arc<std::sync::atomic::AtomicBool>)>,
        >,
    },
    CloseConn {
        peer_id: PeerId,
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<std::time::Duration>,
        resp: futures::channel::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    SyncKeyhiveWithPeerInternal {
        peer_id: PeerId,
    },
    NoteLocalKeyhiveChanged {
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
    ReleaseDocLease {
        doc_id: DocumentId,
    },
    ReleaseInternalLease {
        doc_id: DocumentId,
    },
    CheckSedimentreeResident {
        doc_id: DocumentId,
        resp: futures::channel::oneshot::Sender<bool>,
    },
    InspectStoredDocBlobs {
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
    },
    CheckDocWorkerExists {
        doc_id: DocumentId,
        resp: futures::channel::oneshot::Sender<bool>,
    },
    /// Wait until all finite runtime work currently admitted to the Hub and
    /// document workers has drained. Pending decryption is quiescent; this
    /// does not wait for unavailable keys.
    WaitForQuiescence {
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
}

/// Events from background workers / keyhive listener / sync sessions / doc-workers.
pub enum Runtime2Evt {
    SyncSessionObserved {
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
    },
    /// Initiating a keyhive sync failed before the protocol could emit a
    /// completion event. The hub uses this to resolve the public waiter
    /// instead of allowing a network error to panic a child task.
    KeyhiveSyncFailed {
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
        error: String,
    },
    /// The keyhive protocol sync and local cache refresh both completed.
    KeyhiveCacheRefreshDone {
        peer_id: PeerId,
        round_id: Option<u64>,
        result: eyre::Result<()>,
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
    /// The persisted Keyhive-derived partition cursor advanced.
    GroupPartWorkerAdvanced {
        cursor: u64,
    },
    KeyhiveSyncRequested {
        peer_id: PeerId,
    },
    /// A document worker requested that the hub start a Subduction sync.
    DocSyncRequested {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
    },
    /// Completion of a hub-driven document sync.
    DocSyncCompleted {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        result: Result<(), crate::runtime::SyncDocError>,
    },
    DocWorkerHandleAcquired {
        bundle: Arc<crate::runtime::LiveDocBundle>,
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
    /// A document worker reached a quiescence barrier in mailbox order.
    DocWorkerQuiescent {
        doc_id: DocumentId,
        barrier_id: u64,
    },
    PrekeyExpanded {
        new_prekey: Arc<crate::runtime::SignedAddKeyOp>,
    },
    PrekeyRotated {
        rotate_key: Arc<crate::runtime::SignedRotateKeyOp>,
    },
    CgkaOp {
        data: Arc<crate::runtime::SignedCgkaOp>,
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
pub enum DocWorkerMsg {
    PutDoc {
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    },
    AcquireHandle {
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>,
        >,
    },
    CommitDelta {
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    ApplySyncSession {
        session: subduction_core::sync_session::SyncSession,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    SyncWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<std::time::Duration>,
        done: futures::channel::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    CancelSyncWithPeer {
        peer_id: PeerId,
        waiter_id: Option<u64>,
        reason: &'static str,
    },
    SyncWithPeerResult {
        peer_id: PeerId,
        waiter_id: u64,
        result: Result<(), crate::runtime::SyncDocError>,
    },
    ReleaseHandleLease,
    ReattemptMaterialization,
    QueryHeadState {
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
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
