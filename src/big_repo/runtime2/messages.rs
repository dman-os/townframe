//! runtime2 messages. Uses `futures::channel::oneshot` for request/response;
//! no Tokio types.

use crate::interlude::*;
use crate::DocumentId;
use big_sync_core::PeerId;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Response payload for `OpenConn`/`AcceptConn`: the peer id, the shared
/// connection-closed flag, and a receiver that completes when the connection
/// fully closes.
type ConnOpenResp = futures::channel::oneshot::Sender<
    eyre::Result<(
        PeerId,
        Arc<std::sync::atomic::AtomicBool>,
        futures::channel::oneshot::Receiver<(Arc<std::sync::atomic::AtomicBool>, eyre::Result<()>)>,
    )>,
>;

/// A signed keyhive delegation forwarded to the hub as an event.
type SignedDelegation = Arc<
    keyhive_crypto::signed::Signed<
        keyhive_core::principal::group::delegation::Delegation<
            future_form::Sendable,
            keyhive_crypto::signer::memory::MemorySigner,
            Vec<u8>,
            crate::keyhive_listener::BigRepoKeyhiveListener,
        >,
    >,
>;

/// A signed keyhive revocation forwarded to the hub as an event.
type SignedRevocation = Arc<
    keyhive_crypto::signed::Signed<
        keyhive_core::principal::group::revocation::Revocation<
            future_form::Sendable,
            keyhive_crypto::signer::memory::MemorySigner,
            Vec<u8>,
            crate::keyhive_listener::BigRepoKeyhiveListener,
        >,
    >,
>;

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
        /// Bundle id of the committing handle; the worker rejects commits from
        /// broken or replaced bundles.
        bundle_id: u64,
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
        resp:
            futures::channel::oneshot::Sender<eyre::Result<Option<crate::runtime2::DocHeadState>>>,
    },
    OpenConn {
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        #[educe(Debug(ignore))]
        resp: ConnOpenResp,
    },
    AcceptConn {
        incoming: Box<dyn std::any::Any + Send>,
        #[educe(Debug(ignore))]
        resp: ConnOpenResp,
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
    /// The doc-sync transport round finished successfully (no error).
    ///
    /// Sessions never resolve waiters (a rejected session must not look like
    /// success); the round completion is the authoritative receipt resolution:
    /// a worker reconsider over the fully-persisted tree.
    DocSyncRoundDone {
        request_id: subduction_core::connection::message::RequestId,
    },
    /// The doc-sync transport round failed; resolve the waiter (if any) with
    /// this error.
    DocSyncFailed {
        request_id: subduction_core::connection::message::RequestId,
        #[educe(Debug(ignore))]
        error: crate::runtime2::types::SyncDocError,
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
    #[cfg_attr(not(test), expect(dead_code))]
    InspectStoredDocBlobs {
        sed_id: sedimentree_core::id::SedimentreeId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
    },
    /// Wait until all finite runtime work currently admitted to the Hub and
    /// document workers has drained. Pending decryption is quiescent; this
    /// does not wait for unavailable keys.
    ///
    /// When `freeze` is set, the hub stops processing events (and holds all
    /// non-`Unfreeze` commands) once quiescence is reached, until a matching
    /// `Unfreeze`. Tests use this to run assertions against a quiescent
    /// snapshot with nothing able to slip past the barrier.
    #[cfg_attr(not(test), allow(dead_code))]
    WaitForQuiescence {
        freeze: bool,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    /// Resume event/command processing after a frozen `WaitForQuiescence`.
    /// No-op when the hub is not frozen.
    Unfreeze,
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
    /// A remote peer signalled that its Keyhive changed (keyhive-changes RPC
    /// notification). The hub starts a waiter-less keyhive sync round when the
    /// peer is connected and no round is active; the round participates in
    /// quiescence via `active_keyhive_syncs`. The notification itself is a
    /// hint, not the source of Keyhive state.
    KeyhiveChangeNotif {
        peer_id: PeerId,
    },
    /// The highest Keyhive state generation the group-part projection has
    /// reconciled. The hub bumps `keyhive_state_generation` on every state
    /// advance (KeyhiveSyncDone{changed}, delegation, revocation, cgka); the
    /// worker full-rebuilds on advance and acks the generation it covered.
    GroupPartWorkerAdvanced {
        generation: u64,
    },
    DocWorkerStopped {
        doc_id: DocumentId,
        error: Option<String>,
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
    /// A document worker replied to a quiescence fence in mailbox order.
    /// `barrier_id` filters acks from a superseded (restarted) probe.
    DocWorkerFenced {
        doc_id: DocumentId,
        barrier_id: u64,
    },
    /// A tracked finite background future completed; decrements the hub's
    /// in-flight counter (A2/A5 tracked-work seam). Ordered after the future's
    /// own emissions by the `spawn_tracked` wrapper.
    TrackedWorkDone {
        kind: TrackedWorkKind,
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
        data: SignedDelegation,
    },
    RevocationReceived {
        target: keyhive_core::principal::identifier::Identifier,
        data: SignedRevocation,
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
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    AcquireHandle {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<
                crate::runtime2::types::DocLookup<Arc<crate::runtime2::types::LiveDocBundle>>,
            >,
        >,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    CommitDelta {
        /// Bundle id of the committing handle; the worker rejects commits from
        /// broken or replaced bundles.
        bundle_id: u64,
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
    ApplySyncSession {
        peer_id: PeerId,
        commit_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
        fragment_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
        /// Resolve the caller's sync receipt with the outcome. `None` for
        /// passive (fire-and-forget) routing.
        #[educe(Debug(ignore))]
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    ReattemptMaterialization {
        /// Why the retry was triggered; reported to change listeners as the
        /// materialization origin (keyhive-driven retries must not surface as
        /// `Bootstrap`).
        origin: crate::changes::BigRepoChangeOrigin,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::MaterializationStatus, String>,
        >,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    QueryHeadState {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    InspectHeadState {
        #[educe(Debug(ignore))]
        resp:
            futures::channel::oneshot::Sender<eyre::Result<Option<crate::runtime2::DocHeadState>>>,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    /// Mailbox-ordered runtime quiescence barrier. The worker replies on
    /// `reply` once its in-flight work has drained (the mailbox is quiescent).
    Fence {
        reply: futures::channel::oneshot::Sender<()>,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
}

/// Monotonic waiter-id counters (shared handle↔hub).
pub fn fresh_waiter_id(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Classification of a tracked finite background future, for diagnostics.
#[derive(Debug, Clone, Copy)]
pub enum TrackedWorkKind {
    CreateDoc,
    SyncDoc,
    KeyhiveSync,
    CloseConn,
    ContainsSedimentree,
    HasLocalDocState,
    InspectStoredDocBlobs,
    EmitMembershipChange,
    MaterializationRetry,
    WorkerFence,
}
