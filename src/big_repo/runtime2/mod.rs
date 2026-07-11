//! runtime2 — the tractable rewrite of `runtime.rs`.
//!
//! A FutureForm-generic, transport-agnostic rewrite of the 5k-line tokio+iroh
//! `runtime.rs`. Hub/worker actors with all IO centralized behind traits
//! ([`DocIo`], injected [`TaskRuntime`]/[`Timer`]/[`Clock`]). Parity with the old
//! runtime, with three bugs fixed (see `play.big_repo.current_fixes.md`):
//! - head-divergence flake — heads are derived, never cached in an overloaded
//!   payload field;
//! - noop-after-crash — `apply_sync_session` always re-derives sedimentree
//!   heads, even on an empty-refs sync;
//! - non-atomic local write — a single `store_commit` (big_sync is gone from
//!   the runtime, so there is no split write).
//!
//! Not yet wired into the build (`#[cfg(any())]` in `lib.rs`); green-up is a
//! separate pass. See `play.big_repo.runtime2.md`.
//!
//! # IO model (v1: async actor + centralized IO traits → D2 determinism)
//!
//! The hub and doc-worker are async actors driven by an injected [`TaskRuntime`].
//! All IO flows through traits — [`DocIo`], `Storage<F>`, `KeyhiveStorage<F>`
//! — so IO is *external* to the actor logic. Memory backends give D2
//! determinism (no disk/network jitter). Full step-determinism (samod-style
//! `IoTask` round-stepping) is a future evolution: the trait seam lets us wrap
//! [`DocIo`] in an `IoTask`-completing harness later without rewriting the
//! actor.
//!
//! # Avoiding message soup
//!
//! samod's subduction branch uses a sans-io `engine.handle(Input) -> Output`
//! + `HubResults::emit_io_action` (IO as out-param, completed by the harness).
//! We can't match that fully (current subduction is async, not sans-io), so we
//! avoid soup by: (a) centralizing every doc-worker IO through [`DocIo`],
//! (b) breaking mega-methods into small steps with explicit IO contracts,
//! (c) one mailbox per actor (no fan-out of IO futures).
//!
//! # FutureForm
//!
//! Everything is generic over `F: FutureForm` (`Sendable` native, `Local` wasm).
//! This is the wasm lever. subduction + keyhive + subduction_keyhive already
//! support both; big_repo currently hardcodes `Sendable`.

use crate::interlude::*; // brings PeerId, ObjId, Res, prelude
use future_form::FutureForm;

mod io;
mod lease;
mod messages;
mod tasks;

pub use io::{Clock, CausalDecryptResult, DocIo, EncryptedLooseCommit, Timer};
pub use lease::{DocLease, DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken};
pub use messages::{DocWorkerMsg, Runtime2Cmd, Runtime2Evt};
pub use tasks::{TaskRuntime, TaskSet, TokioTaskRuntime, TokioTaskSet};

mod doc_worker;
mod handle;
mod hub;

pub use doc_worker::{DocWorker2, spawn_doc_worker};
pub use handle::Runtime2Handle;
pub use hub::{Runtime2Hub, Runtime2StopToken, spawn_runtime2};

/// Re-export of the doc id (root type alias) so runtime2 modules agree.
pub use crate::DocumentId;

/// runtime2 config — all inputs that were once hardcoded in `spawn_big_repo_runtime`.
///
/// Generic over `F: FutureForm` (Sendable native, Local wasm) and the storage
/// backends `S` (subduction `Storage<F>`) + `K` (`KeyhiveStorage<F>`). The
/// unified SQL store (plan B) makes `S` and the `HostPartStore` share one KV so
/// local writes are one atomic transaction.
#[expect(clippy::type_complexity)]
pub struct Runtime2Config<F, R, S, K>
where
    F: FutureForm,
    R: TaskRuntime<F>,
    S: subduction_core::storage::traits::Storage<F> + Clone + Send + Sync + std::fmt::Debug + 'static,
    K: subduction_keyhive::storage::KeyhiveStorage<F> + Send + Sync + 'static,
{
    pub signer: subduction_crypto::signer::memory::MemorySigner,
    /// Unified SQL store backing subduction sedimentree storage (plan B).
    pub storage: S,
    pub keyhive_storage: std::sync::Arc<K>,
    /// IO surface shared by per-document workers. The implementation owns the
    /// concrete subduction/keyhive/storage wiring; workers remain generic.
    pub doc_io: std::sync::Arc<dyn DocIo<F>>,
    /// NOTE: no `part_store` / `HostPartStore` here — runtime2 does not touch
    /// big_sync. big_sync (sync routing, partitions) lives in a sibling layer.
    pub policy: std::sync::Arc<crate::runtime::BigRepoPolicy>,
    pub sync_policy: crate::runtime::BigRepoSyncPolicy,
    pub keyhive: crate::keyhive::BigKeyhiveHandle,
    pub change_manager: std::sync::Arc<crate::changes::ChangeListenerManager>,
    /// Concrete task runtime (native: [`TokioTaskRuntime`]). Controls how
    /// background tasks (keyhive syncs, lease waiters, doc-workers) are
    /// spawned and stopped.
    pub tasks: R,
    /// Injected timer/clock — the determinism levers. `TokioTimer` native;
    /// a step-timer in tests; wasm event loop.
    pub timer: std::sync::Arc<dyn Timer<F>>,
    pub clock: std::sync::Arc<dyn Clock>,
    pub rng: std::sync::Arc<futures::lock::Mutex<rand::rngs::StdRng>>,
    /// Transport-agnostic connection factory. Given a peer id + opaque addr,
    /// produces an authenticated connection + handshake. `iroh` native; in-memory
    /// `ChannelTransport` in tests; websocket in wasm. The blocking-out carries
    /// the addr as `Box<dyn Any + Send>`; the implementing model pins the type.
    pub connect: std::sync::Arc<dyn TransportConnect<F>>,
}

/// Transport-agnostic connect/accept — the seam that replaces iroh baked into
/// the runtime. The hub calls these on `OpenConn`/`AcceptConn`.
pub trait TransportConnect<F: FutureForm>: Send + Sync {
    /// Connect to `peer` at `addr_blob` (transport-specific); returns the
    /// authenticated subduction connection + peer id.
    fn connect(
        &self,
        peer: big_sync_core::PeerId,
        addr_blob: Box<dyn std::any::Any + Send>,
    ) -> F::Future<'static, eyre::Result<big_sync_core::PeerId>>;
}

// ─── NEW types (the heads-fix op) ──────────────────────────────────────────

/// The result of the walk-derived heads query (`Runtime2Handle::doc_head_state`).
/// This is the flake-detector's backing type: test2 Tier-0 asserts sedimentree
/// parity across all peers (incl. relays) and materialized parity across peers
/// with access.
#[derive(Debug, Clone)]
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
    /// Live automerge doc; `materialized_heads` is `Some`.
    Materialized,
}
