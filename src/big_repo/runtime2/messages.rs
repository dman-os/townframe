//! runtime2 messages. Uses `futures::channel::oneshot` for request/response;
//! no Tokio types.

use crate::DocumentId;
use crate::interlude::*;
use big_sync_core::PeerKey;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Response payload for `OpenConn`/`AcceptConn`: the peer id, the shared
/// connection-closed flag, and a receiver that completes when the connection
/// fully closes.
type ConnOpenResp = futures::channel::oneshot::Sender<
    eyre::Result<(
        PeerKey,
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
    AllocateDoc {
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::DocumentId>>,
    },
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
        resp:
            futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::types::LiveDocHandle>>,
    },
    /// Internal: persist a document whose doc_id is already resolved.
    /// The hub sends this to itself after `CreateDoc` completes.
    PutDoc {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        initial_content: Box<automerge::Automerge>,
        #[educe(Debug(ignore))]
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        #[educe(Debug(ignore))]
        resp:
            futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::types::LiveDocHandle>>,
    },
    FinalizeAllocatedDoc {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        initial_content: Box<automerge::Automerge>,
        #[educe(Debug(ignore))]
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        pending_group: crate::keyhive::BigKeyhiveGroup,
        #[educe(Debug(ignore))]
        resp:
            futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::types::LiveDocHandle>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        lease: crate::runtime2::DocLeaseKind,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::DocLookup<crate::runtime2::types::LiveDocHandle>>,
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
    EnsureCausalCoverage {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<bool>>>,
    },
    InspectDocHeadState {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp:
            futures::channel::oneshot::Sender<eyre::Result<Option<crate::runtime2::DocHeadState>>>,
    },
    OpenConn {
        peer: PeerKey,
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
        peer_id: PeerKey,
        /// End flag of the specific connection being closed. Subduction
        /// tracks multiple connections per peer, so a close must identify
        /// WHICH connection it targets; the peer's registration is only
        /// torn down when this flag matches the peer's current connection.
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerKey,
        waiter_id: u64,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
        >,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerKey,
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
        peer_id: PeerKey,
        waiter_id: u64,
    },
    CancelKeyhiveSyncWaiter {
        peer_id: PeerKey,
        waiter_id: u64,
    },
    RegisterDocLease {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        registered: futures::channel::oneshot::Sender<()>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
        generation: u64,
    },
    ReleaseInternalLease {
        doc_id: DocumentId,
        generation: u64,
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
    /// Test-only: whether `peer_id` currently has a registered connection in
    /// the hub. Lets a lifecycle test assert the deregistration invariant
    /// directly instead of inferring it from an asynchronous sync failure.
    #[cfg(test)]
    HasConnectedPeer {
        peer_id: PeerKey,
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
    /// Test-only seam: stop handing events to the hub and queue them instead,
    /// so a test can drive the command/event ordering directly.
    ///
    /// The hub polls commands before events (`select_biased!`), which is
    /// invisible to a test unless it can hold event processing: a command whose
    /// own spawned work already emitted its events then races them. `ResumeEvents`
    /// reopens processing and replays the queued events in channel order, which
    /// is the order they would have been handled in without the hold.
    #[cfg(test)]
    HoldEvents {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<()>,
    },
    /// Test-only seam: reopen event processing and handle the events queued
    /// since [`Runtime2Cmd::HoldEvents`], in channel order.
    #[cfg(test)]
    ResumeEvents {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<()>,
    },
    /// Test-only seam: close the doc worker's mailbox for `doc_id` on the next
    /// content-carrying sync-session apply route, reproducing the
    /// worker-stopping race (handle resolved, then the receiver dropped)
    /// deterministically.
    #[cfg(test)]
    FailNextContentApplyRoute {
        doc_id: DocumentId,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<()>,
    },
    /// Test-only seam: deliver a synthetic keyhive sync completion for
    /// `peer_id`'s active round, stamped one seq ahead of the hub's current
    /// `admitted_head`. The reply carries that seq so the test can then inject
    /// the matching admission event and prove the completion was deferred.
    #[cfg(test)]
    InjectKeyhiveCompletionForTest {
        peer_id: PeerKey,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<u64>>,
    },
    /// Test-only seam: hand an event directly to the hub's event handler,
    /// bypassing the event channel so a test can drive event ordering
    /// deterministically (no peer, no scheduler, no hold).
    #[cfg(test)]
    InjectRuntime2EvtForTest {
        evt: Box<Runtime2Evt>,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    },
    /// Test-only seam: report the currently active quiescence probe's barrier
    /// id (`None` once resolved), so a test can assert a pending probe restarted
    /// rather than resolving over work routed behind its fence.
    #[cfg(test)]
    QuiescenceProbeBarrierForTest {
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<Option<u64>>,
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
    /// The doc-sync transport round finished successfully (no error).
    ///
    /// Sessions never resolve waiters (a rejected session must not look like
    /// success); the round completion is the authoritative receipt resolution:
    /// a worker reconsider over the fully-persisted tree.
    ///
    /// It rides the event channel, not the command channel, because it is a
    /// report *out of* the round's spawned work — and because the round emits
    /// its [`Runtime2Evt::SyncSessionObserved`] into that channel before
    /// returning, so channel order makes the receipt resolution follow the
    /// session apply it must not overtake. A command would be polled first by
    /// the hub (`select_biased!`), letting a loaded hub resolve a caller's
    /// receipt for a round whose received content it had not applied yet;
    /// `KeyhiveSyncDone` and `TrackedWorkDone` hold the same ordering rule.
    DocSyncRoundDone {
        request_id: subduction_core::connection::message::RequestId,
    },
    /// The doc-sync transport round failed; resolve the waiter (if any) with
    /// this error. Emitted on the event channel for the same reason as
    /// [`Runtime2Evt::DocSyncRoundDone`].
    DocSyncFailed {
        request_id: subduction_core::connection::message::RequestId,
        #[educe(Debug(ignore))]
        error: crate::runtime2::types::SyncDocError,
    },
    ConnEstablished {
        peer_id: PeerKey,
        closed: Arc<std::sync::atomic::AtomicBool>,
    },
    ConnLost {
        peer_id: PeerKey,
        closed: Arc<std::sync::atomic::AtomicBool>,
        error: Option<String>,
    },
    KeyhiveSyncDone {
        peer_id: PeerKey,
        request_id: subduction_keyhive::message::RequestId,
        changed: bool,
        /// The store's admission watermark when the exchange was acknowledged:
        /// the last admission-log seq durably committed before this completion.
        /// The hub resolves the round's waiters only once its own
        /// `admitted_head` has reached this seq, so a caller can never be told
        /// "reconciled" while this round's admissions are still unprojected.
        admitted_seq: u64,
    },
    /// Initiating a keyhive sync failed before the protocol could emit a
    /// completion event. The hub uses this to resolve the public waiter
    /// instead of allowing a network error to panic a child task.
    KeyhiveSyncFailed {
        peer_id: PeerKey,
        request_id: subduction_keyhive::message::RequestId,
        error: String,
    },
    /// A remote peer signalled that its Keyhive changed (keyhive-changes RPC
    /// notification). The hub starts a waiter-less keyhive sync round when the
    /// peer is connected and no round is active; the round participates in
    /// quiescence via `active_keyhive_syncs`. The notification itself is a
    /// hint, not the source of Keyhive state.
    KeyhiveChangeNotif {
        peer_id: PeerKey,
    },
    /// The durable incorporation-log head advanced: an incorporation hook
    /// appended a batch and everything through `seq` is now applied to the
    /// Keyhive graph. Emitted after each committed append.
    KeyhiveAdmissionAdvanced {
        seq: u64,
    },
    /// The highest admission-log seq the group-part projection has settled
    /// (persisted its cursor past). The hub compares it against the captured
    /// admission head so `WaitForKeyhiveReconciliation` can resolve.
    GroupPartWorkerSettled {
        seq: u64,
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
        #[educe(Debug(ignore))]
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        resp:
            futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::types::LiveDocHandle>>,
        #[educe(Debug(ignore))]
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    AcquireHandle {
        lease: crate::runtime2::DocLeaseKind,
        #[educe(Debug(ignore))]
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::DocLookup<crate::runtime2::types::LiveDocHandle>>,
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
        peer_id: PeerKey,
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
    ReconcileCausalCoverage {
        #[educe(Debug(ignore))]
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<bool>>>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    PrekeyStatePersist,
}
