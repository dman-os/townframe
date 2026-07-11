//! runtime2 messages.
//!
//! De-iroh'd vs today: connection verbs take a transport-agnostic `Runtime2Conn`
//! handle, not `iroh::Endpoint`. Added `DocHeadState` (the new walk-derived
//! heads query) and `QueryHeadState` on the doc-worker mailbox.

use crate::interlude::*;
use big_sync_core::PeerId;
use crate::DocumentId;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Commands into the runtime hub (from `Runtime2Handle`).
pub enum Runtime2Cmd {
    PutDoc {
        doc_id: DocumentId,
        initial_content: Box<automerge::Automerge>,
        resp: tokio::sync::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(sedimentree_core::loose_commit::id::CommitId, std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: tokio::sync::oneshot::Sender<eyre::Result<()>>,
    },
    /// NEW: walk-derived heads query. Returns sedimentree heads + materialized
    /// heads (None if pending/relay). The flake-detector's backing op.
    DocHeadState {
        doc_id: DocumentId,
        resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocHeadState>>,
    },
    // ── connections (transport-agnostic) ──────────────────────────────────
    OpenConn {
        conn: Box<dyn crate::runtime2::Runtime2Conn<future_form::Sendable>>,
        resp: tokio::sync::oneshot::Sender<eyre::Result<(PeerId, Arc<std::sync::atomic::AtomicBool>)>>,
    },
    AcceptConn {
        conn: Box<dyn crate::runtime2::Runtime2Conn<future_form::Sendable>>,
        resp: tokio::sync::oneshot::Sender<eyre::Result<(PeerId, Arc<std::sync::atomic::AtomicBool>)>>,
    },
    CloseConn {
        peer_id: PeerId,
        resp: Option<tokio::sync::oneshot::Sender<eyre::Result<()>>>,
    },
    // ── sync ───────────────────────────────────────────────────────────────
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<std::time::Duration>,
        resp: tokio::sync::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        resp: tokio::sync::oneshot::Sender<eyre::Result<()>>,
    },
    SyncKeyhiveWithPeerInternal { peer_id: PeerId },
    NoteLocalKeyhiveChanged {
        resp: tokio::sync::oneshot::Sender<eyre::Result<()>>,
    },
    CancelDocSyncWaiter { doc_id: DocumentId, peer_id: PeerId, waiter_id: u64 },
    CancelKeyhiveSyncWaiter { peer_id: PeerId, waiter_id: u64 },
    ReleaseDocLease { doc_id: DocumentId },
    ReleaseInternalLease { doc_id: DocumentId },
    CheckSedimentreeResident { doc_id: DocumentId, resp: tokio::sync::oneshot::Sender<bool> },
    CheckDocWorkerExists { doc_id: DocumentId, resp: tokio::sync::oneshot::Sender<bool> },
}

/// Events from background workers / keyhive listener / sync sessions / doc-workers.
/// Same shape as today's `RuntimeEvt`; kept here so the hub's `handle_evt` is
/// self-documenting.
pub enum Runtime2Evt {
    SyncSessionObserved { session: subduction_core::sync_session::SyncSession },
    ConnEstablished { peer_id: PeerId, closed: Arc<std::sync::atomic::AtomicBool> },
    ConnLost { peer_id: PeerId, error: Option<String> },
    KeyhiveSyncDone { peer_id: PeerId },
    KeyhiveSyncRequested { peer_id: PeerId },
    DocWorkerHandleAcquired { bundle: Arc<crate::runtime::LiveDocBundle> },
    DocWorkerStopped { doc_id: DocumentId },
    FatalWorkerError { doc_id: Option<DocumentId>, context: &'static str, error: String },
    DocWorkerMaterializationPending { doc_id: DocumentId },
    DocWorkerMaterializationReady { doc_id: DocumentId },
    // keyhive listener events (P1)
    PrekeyExpanded { new_prekey: Arc<crate::runtime::SignedAddKeyOp> },
    PrekeyRotated { rotate_key: Arc<crate::runtime::SignedRotateKeyOp> },
    CgkaOp { data: Arc<crate::runtime::SignedCgkaOp> },
    DelegationReceived { target: keyhive_core::principal::identifier::Identifier },
    RevocationReceived { target: keyhive_core::principal::identifier::Identifier },
}

/// The doc-worker's mailbox. `_lease` fields keep the worker alive for the
/// duration of the op (see [`crate::runtime2::DocWorkerInternalLease`]).
pub enum DocWorkerMsg {
    PutDoc {
        initial_content: Box<automerge::Automerge>,
        resp: tokio::sync::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    },
    AcquireHandle {
        resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>>,
    },
    CommitDelta {
        commits: Vec<(sedimentree_core::loose_commit::id::CommitId, std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: tokio::sync::oneshot::Sender<eyre::Result<()>>,
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
        done: tokio::sync::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
        _lease: crate::runtime2::DocWorkerInternalLease,
    },
    CancelSyncWithPeer { peer_id: PeerId, waiter_id: Option<u64>, reason: &'static str },
    SyncWithPeerResult { peer_id: PeerId, waiter_id: u64, result: Result<(), crate::runtime::SyncDocError> },
    ReleaseHandleLease,
    ReattemptMaterialization,
    /// NEW: back the `DocHeadState` runtime query.
    QueryHeadState { resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocHeadState>> },
}

/// Monotonic waiter-id counters (shared handle↔hub).
pub fn fresh_waiter_id(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
