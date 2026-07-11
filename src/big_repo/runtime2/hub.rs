//! `Runtime2Hub` — the runtime actor (the "machine loop").
//!
//! Replaces `BigRepoRuntimeWorker`. Processes `Runtime2Cmd` (from the handle)
//! + `Runtime2Evt` (from background workers / keyhive listener / sync sessions
//! / doc-workers). Manages: connections, doc-worker lifecycle, keyhive sync,
//! big_sync integration, the janitor.
//!
//! Spawning is a top-standing free function [`spawn_runtime2`] (not a method),
//! mirroring the existing `spawn_big_repo_runtime`. It spawns the background
//! workers + the machine loop and returns the handle + stop token.

use crate::interlude::*;
use crate::runtime2::{
    messages::{Runtime2Cmd, Runtime2Evt},
    DocWorkerEntry, DocWorkerHandle, Runtime2Config, Runtime2Handle,
};
use big_sync_core::PeerId;
use crate::DocumentId;
use future_form::FutureForm;

/// The runtime actor. Owns the live state; driven by `handle_cmd` / `handle_evt`
/// in the machine loop. Generic over `F: FutureForm` (Sendable native, Local wasm).
pub struct Runtime2Hub<F: FutureForm> {
    // ── identity / config ──────────────────────────────────────────────────
    pub(crate) local_peer_id: PeerId,
    pub(crate) sync_policy: crate::runtime::BigRepoSyncPolicy,
    // ── IO + libraries ─────────────────────────────────────────────────────
    pub(crate) keyhive: crate::keyhive::BigKeyhiveHandle,
    pub(crate) keyhive_storage: Arc<crate::runtime::BigRepoKeyhiveStorage>,
    pub(crate) part_store: Arc<dyn crate::runtime2::HostPartStore>,
    pub(crate) change_manager: Arc<crate::changes::ChangeListenerManager>,
    pub(crate) spawner: Arc<dyn crate::runtime2::Spawner<F>>,
    // ── channels ───────────────────────────────────────────────────────────
    pub(crate) cmd_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Cmd>,
    pub(crate) evt_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Evt>,
    // ── connection state ───────────────────────────────────────────────────
    pub(crate) connected_peers: HashMap<PeerId, ConnDeets>,
    // ── keyhive sync bookkeeping ───────────────────────────────────────────
    pub(crate) pending_keyhive_syncs: HashMap<PeerId, u64>,
    pub(crate) active_keyhive_syncs: HashMap<PeerId, u64>,
    pub(crate) keyhive_dirty: std::collections::BTreeSet<PeerId>,
    // ── doc-worker registry ────────────────────────────────────────────────
    pub(crate) doc_workers: HashMap<DocumentId, DocWorkerEntry>,
    pub(crate) pending_materialization: std::collections::HashSet<DocumentId>,
    // ── waiter-id counters (shared with the handle) ────────────────────────
    pub(crate) doc_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
}

pub(crate) struct ConnDeets {
    pub closed: Arc<std::sync::atomic::AtomicBool>,
    pub cancel: tokio_util::sync::CancellationToken,
}

impl<F: FutureForm> Runtime2Hub<F> {
    // ─── command handlers ──────────────────────────────────────────────────
    pub(crate) fn handle_cmd(&mut self, cmd: Runtime2Cmd) -> eyre::Result<()> {
        match cmd {
            Runtime2Cmd::PutDoc { doc_id, initial_content, resp } => {
                // Ensure a doc-worker exists, forward PutDoc. (Mirrors today's
                // spawn_doc_worker + forward.)
                let _ = (doc_id, initial_content, resp);
                todo!("ensure doc-worker + forward PutDoc (see doc_worker::spawn_doc_worker)")
            }
            Runtime2Cmd::GetDocHandle { doc_id, resp } => {
                let _ = (doc_id, resp);
                todo!("ensure doc-worker + forward AcquireHandle")
            }
            Runtime2Cmd::CommitDelta { doc_id, commits, heads, patches, origin, resp } => {
                let _ = (doc_id, commits, heads, patches, origin, resp);
                todo!("ensure doc-worker + forward CommitDelta (with internal lease)")
            }
            Runtime2Cmd::DocHeadState { doc_id, resp } => {
                let _ = (doc_id, resp);
                todo!("ensure doc-worker + forward QueryHeadState")
            }
            Runtime2Cmd::OpenConn { conn, resp } => {
                let _ = (conn, resp);
                todo!("transport-agnostic open: handshake, add_connection, keyhive add_peer, emit ConnEstablished")
            }
            Runtime2Cmd::AcceptConn { conn, resp } => {
                let _ = (conn, resp);
                todo!("transport-agnostic accept: handshake, add_connection, keyhive add_peer, emit ConnEstablished")
            }
            Runtime2Cmd::CloseConn { peer_id, resp } => {
                let _ = (peer_id, resp);
                todo!("cancel pending syncs, remove peer, disconnect subduction, remove keyhive peer")
            }
            Runtime2Cmd::SyncDocWithPeer { doc_id, peer_id, waiter_id, timeout, resp } => {
                let _ = (doc_id, peer_id, waiter_id, timeout, resp);
                todo!("ensure doc-worker + forward SyncWithPeer (with internal lease)")
            }
            Runtime2Cmd::SyncKeyhiveWithPeer { peer_id, waiter_id, resp } => {
                let _ = (peer_id, waiter_id, resp);
                todo!("start_keyhive_sync: initiate_sync_with_peer, track waiter")
            }
            Runtime2Cmd::SyncKeyhiveWithPeerInternal { peer_id } => {
                let _ = peer_id;
                todo!("schedule_internal_keyhive_sync (dirty-gated)")
            }
            Runtime2Cmd::NoteLocalKeyhiveChanged { resp } => {
                let _ = resp;
                todo!("mark keyhive dirty for all connected peers; schedule syncs")
            }
            Runtime2Cmd::CancelDocSyncWaiter { doc_id, peer_id, waiter_id } => {
                let _ = (doc_id, peer_id, waiter_id);
                todo!("forward CancelSyncWithPeer to doc-worker")
            }
            Runtime2Cmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id } => {
                let _ = (peer_id, waiter_id);
                todo!("drop keyhive sync waiter")
            }
            Runtime2Cmd::ReleaseDocLease { doc_id } => {
                let _ = doc_id;
                todo!("decrement local_handles; schedule eviction if zero")
            }
            Runtime2Cmd::ReleaseInternalLease { doc_id } => {
                let _ = doc_id;
                todo!("decrement internal_leases; schedule eviction if zero")
            }
            Runtime2Cmd::CheckSedimentreeResident { doc_id, resp } => {
                let _ = (doc_id, resp);
                todo!("contains_sedimentree_id")
            }
            Runtime2Cmd::CheckDocWorkerExists { doc_id, resp } => {
                let _ = (doc_id, resp);
                todo!("doc_workers contains live worker")
            }
        }
    }

    // ─── event handlers ────────────────────────────────────────────────────
    pub(crate) fn handle_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        match evt {
            Runtime2Evt::SyncSessionObserved { session } => {
                let _ = session;
                todo!("route session to the doc-worker for session.sedimentree_id (ApplySyncSession + internal lease)")
            }
            Runtime2Evt::ConnEstablished { peer_id, closed } => {
                let _ = (peer_id, closed);
                todo!("register connected_peers; schedule initial keyhive sync")
            }
            Runtime2Evt::ConnLost { peer_id, error } => {
                let _ = (peer_id, error);
                todo!("cancel pending syncs; remove from connected_peers; re-evaluate doc-workers")
            }
            Runtime2Evt::KeyhiveSyncDone { peer_id } => {
                let _ = peer_id;
                todo!("finish_keyhive_sync: clear active, flush dirty, reattempt doc materialization")
            }
            Runtime2Evt::KeyhiveSyncRequested { peer_id } => {
                let _ = peer_id;
                todo!("start_keyhive_sync if not active")
            }
            Runtime2Evt::DocWorkerHandleAcquired { bundle } => {
                let _ = bundle;
                todo!("increment local_handles for bundle.doc_id")
            }
            Runtime2Evt::DocWorkerStopped { doc_id } => {
                let _ = doc_id;
                todo!("remove doc_workers entry; clear pending_materialization if present")
            }
            Runtime2Evt::FatalWorkerError { doc_id, context, error } => {
                let _ = (doc_id, context, error);
                todo!("log + remove worker; surface to caller (panic per AGENTS.md: don't swallow)")
            }
            Runtime2Evt::DocWorkerMaterializationPending { doc_id } => {
                let _ = doc_id;
                todo!("insert into pending_materialization")
            }
            Runtime2Evt::DocWorkerMaterializationReady { doc_id } => {
                let _ = doc_id;
                todo!("remove from pending_materialization; retry dependent syncs")
            }
            Runtime2Evt::PrekeyExpanded { new_prekey } => {
                let _ = new_prekey;
                todo!("persist prekey op to keyhive_storage; note_local_keyhive_changed")
            }
            Runtime2Evt::PrekeyRotated { rotate_key } => {
                let _ = rotate_key;
                todo!("persist rotate op; note_local_keyhive_changed")
            }
            Runtime2Evt::CgkaOp { data } => {
                let _ = data;
                todo!("persist cgka op (persist_cgka_update_op); note_local_keyhive_changed")
            }
            Runtime2Evt::DelegationReceived { target } | Runtime2Evt::RevocationReceived { target } => {
                let _ = target;
                todo!("keyhive already ingested; note_local_keyhive_changed; reattempt materialization for affected docs")
            }
        }
    }

    // ─── doc-worker lifecycle ──────────────────────────────────────────────
    pub(crate) fn ensure_doc_worker(&mut self, doc_id: DocumentId) -> eyre::Result<&DocWorkerHandle> {
        let _ = doc_id;
        todo!("if no live worker, call spawn_doc_worker (top-standing); reset eviction deadline; return handle")
    }

    /// Periodic eviction of idle doc-workers. Driven by the machine loop's
    /// `Timer::tick(doc_worker_idle_ttl)` (replaces `tokio::time::interval`).
    pub(crate) fn janitor_tick(&mut self) {
        todo!("evict doc-workers with local_handles==0 && internal_leases==0 past eviction_deadline")
    }
}

/// Stop token: cancels background workers + joins the machine loop.
/// Replaces `BigRepoRuntimeStopToken`.
pub struct Runtime2StopToken {
    pub(crate) cancel: tokio_util::sync::CancellationToken,
    pub(crate) machine_loop: tokio::task::JoinHandle<eyre::Result<()>>,
    pub(crate) spawner: Arc<dyn crate::runtime2::Spawner<F>>,
}

impl<F: FutureForm> Runtime2StopToken {
    pub async fn stop(self) -> eyre::Result<()> {
        self.cancel.cancel();
        // Children die first (spawner aborts), then the machine loop.
        self.machine_loop.await.map_err(|e| ferr!("machine loop join: {e:?}"))??;
        eyre::Ok(())
    }
}

/// Top-standing runtime spawn (mirrors `spawn_big_repo_runtime`).
///
/// Spawns the background workers (de-iroh'd transport-agnostic equivalents of
/// today's spawns) + the machine loop, returns the handle + stop token.
///
/// # Background workers spawned (via `config.spawner`, NOT `tokio::spawn`):
/// 1. subduction listener (from `SubductionBuilder::build`)
/// 2. subduction connection manager (from `SubductionBuilder::build`)
/// 3. keyhive maintenance loop (refresh + compact ticks, via `Timer::tick`)
/// 4. ephemeral switchboard
/// 5. keyhive-listener event forwarder → `Runtime2Evt`
/// 6. the machine loop (this hub's `handle_cmd`/`handle_evt` + `Timer::tick`
///    for the janitor) — NOT on the spawner (so children die first, as today)
///
/// Determinism: all spawns go through `config.spawner`, so a step-spawner
/// impl drives them deterministically in tests.
pub fn spawn_runtime2<F: FutureForm>(config: Runtime2Config<F>) -> eyre::Result<(Runtime2Handle, Runtime2StopToken)>
where
    F: 'static,
{
    let _ = config;
    todo! {
        "1. construct subduction (SubductionBuilder, transport-agnostic), wire SyncSessionObserver -> evt_tx\n\
         2. construct keyhive protocol + handler (de-iroh'd conn adapter factory)\n\
         3. construct ephemeral backend + switchboard\n\
         4. build the hub (Runtime2Hub) with all shared state\n\
         5. spawn background workers 1-5 via config.spawner\n\
         6. spawn the machine loop (select on cmd_rx / evt_rx / Timer::tick(janitor))\n\
         7. return (handle, stop_token)"
    }
}
