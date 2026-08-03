use crate::interlude::*;
use future_form::FutureForm;

mod group_part_worker;
pub(crate) use group_part_worker::group_part_id;
mod io;
mod lease;
mod messages;
pub(crate) mod native;
pub(crate) mod support;
mod tasks;
pub(crate) mod types;

#[cfg(test)]
mod test_support;

pub use doc_worker::spawn_doc_worker;
pub use io::{
    CausalDecryptResult, Clock, DocIo, KeyhiveSyncOutcome, MaterializationBlocker,
    MaterializationStatus, RuntimeIo, SyncDocAttempt, Timer,
};
pub use lease::{
    DocLease, DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken,
};
pub use messages::{Runtime2Cmd, Runtime2Evt, TrackedWorkKind};
pub(crate) use native::KeyhiveChangeNotifier;
pub use tasks::{TaskRuntime, TaskSet, TokioTaskRuntime, TokioTimer};

mod doc_worker;
mod handle;
mod hub;

pub use handle::Runtime2Handle;
pub use hub::{spawn_runtime2, Runtime2StopToken};

/// Generic over `F: FutureForm` (Sendable native, Local wasm) and the task
/// runtime `R`. Concrete storage, keyhive, and transport are behind the
/// injected [`RuntimeIo`] / [`DocIo`] / [`TransportConnect`] traits.
pub struct Runtime2Config<F: FutureForm, R: TaskRuntime<F>> {
    /// Local peer identity (derived from signer by the caller).
    pub local_peer_id: big_sync_core::PeerId,
    /// Hub-level IO (keyhive CRUD, sedimentree presence, transport).
    pub runtime_io: std::sync::Arc<dyn RuntimeIo<F>>,
    /// IO surface shared by per-document workers (encrypt, decrypt, store).
    pub doc_io: std::sync::Arc<dyn DocIo<F>>,
    pub sync_policy: crate::runtime2::types::BigRepoSyncPolicy,
    pub change_manager: std::sync::Arc<crate::changes::ChangeListenerManager>,
    /// Concrete task runtime (native: [`TokioTaskRuntime`]). Controls how
    /// background tasks (keyhive syncs, lease waiters, doc-workers) are
    /// spawned and stopped.
    pub tasks: R,
    /// Injected timer/clock — the determinism levers. `TokioTimer` native;
    /// a step-timer in tests; wasm event loop.
    pub timer: std::sync::Arc<dyn Timer<F>>,
    pub clock: std::sync::Arc<dyn Clock>,
    /// Transport-agnostic connection factory. Given a peer id + opaque addr,
    /// produces an authenticated connection + handshake. `iroh` native; in-memory
    /// `ChannelTransport` in tests; websocket in wasm. The blocking-out carries
    /// the addr as `Box<dyn Any + Send>`; the implementing model pins the type.
    pub connect: std::sync::Arc<dyn TransportConnect<F>>,
    /// Event bus supplied by IO backends that receive protocol events from
    /// outside the runtime machine (for example Subduction's observer).
    /// When absent, runtime2 creates a private bus.
    pub event_channel: Option<(
        async_channel::Sender<Runtime2Evt>,
        async_channel::Receiver<Runtime2Evt>,
    )>,
}

/// Transport-agnostic connect/accept/close — the seam that replaces iroh baked
/// into the runtime. The hub calls these on `OpenConn`/`AcceptConn`/`CloseConn`.
///
/// The connect/accept results carry a connection lifecycle end-future that the
/// hub spawns to observe `ConnLost`.
pub trait TransportConnect<F: FutureForm>: Send + Sync {
    /// Connect to `expected_peer` at `addr_blob` (transport-specific).
    /// Returns the handshake-authoritative peer identity, a closed flag that
    /// transitions to `true` when the transport drops the connection, and an
    /// end-future that resolves when the connection lifecycle ends (the hub
    /// spawns this to emit [`Runtime2Evt::ConnLost`]).
    fn connect(
        &self,
        expected_peer: big_sync_core::PeerId,
        addr_blob: Box<dyn std::any::Any + Send>,
    ) -> F::Future<
        'static,
        eyre::Result<(
            big_sync_core::PeerId,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            F::Future<'static, eyre::Result<()>>,
        )>,
    >;

    /// Accept an inbound connection from `incoming` (transport-specific).
    /// Returns the same triple as [`connect`](Self::connect).
    fn accept(
        &self,
        incoming: Box<dyn std::any::Any + Send>,
    ) -> F::Future<
        'static,
        eyre::Result<(
            big_sync_core::PeerId,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            F::Future<'static, eyre::Result<()>>,
        )>,
    >;

    /// Asynchronously close one connection to `peer_id`, identified by its
    /// end flag. Subduction tracks multiple connections per peer, so the
    /// close is per-connection; the peer's registration (keyhive adapter)
    /// is only torn down when the closing connection is still the owner.
    /// Returns when the transport has finished tearing down.
    fn close(
        &self,
        peer_id: big_sync_core::PeerId,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> F::Future<
        'static,
        eyre::Result<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>>,
    >;
}

/// The result of the walk-derived heads query (`Runtime2Handle::doc_head_state`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocHeadState {
    /// Sedimentree heads — storage ground truth; always present for a doc in
    /// the big_sync partition (relays included). What `obj_payload.heads`
    /// stores after the heads fix.
    pub sedimentree_heads: std::sync::Arc<[automerge::ChangeHash]>,
    /// Materialized (automerge) heads — `None` when pending/relay (no live doc
    /// or undecryptable). Derived by walking the sedimentree + decrypting.
    pub materialized_heads: Option<std::sync::Arc<[automerge::ChangeHash]>>,
    pub state: MaterializationState,
}

/// Where a doc is in its materialization lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationState {
    /// No sedimentree content locally known.
    Missing,
    /// Sedimentree heads recorded, but not (yet) decryptable. Relay/pending path.
    Pending,
    /// Automerge state exists, but some stored branches remain undecryptable.
    PartiallyMaterialized,
    /// Live automerge doc with every stored branch materialized.
    Materialized,
}
