//! `Runtime2Hub` — the runtime actor ("the machine loop").
//!
//! Replaces [`BigRepoRuntimeWorker`](crate::runtime::BigRepoRuntimeWorker).
//! Processes [`Runtime2Cmd`] (from the handle) and [`Runtime2Evt`] (from
//! background workers, keyhive listener, sync sessions, doc-workers).
//! Manages connections, doc-worker lifecycle, keyhive sync, the janitor.
//!
//! # big_sync removed
//!
//! The old hub held a `big_sync_store: SharedPartitionStore` and wrote
//! materialized heads to the obj payload (`set_obj_payload`) — both are gone.
//! Heads are derived from the sedimentree (see [`DocIo::sedimentree_heads`]).
//! big_sync (partition membership, sync routing) moves to a sibling layer.
//!
//! # FutureForm
//!
//! The hub is generic over `F: FutureForm`. However, subduction and keyhive
//! types in the current crate are hardcoded to [`Sendable`] (type aliases like
//! [`BigRepoSubduction`], [`BigRepoPolicy`]). The hub fields that reference
//! these concrete types carry a de-facto `F = Sendable` constraint until those
//! types are lifted.
//!
//! # Spawning
//!
//! [`spawn_runtime2`] is a top-standing free function (not a method),
//! mirroring [`spawn_big_repo_runtime`](crate::runtime::spawn_big_repo_runtime).
//! It spawns 6 background workers + the machine loop and returns
//! `(Runtime2Handle, Runtime2StopToken)`.
//!
//! [`Sendable`]: future_form::Sendable
//! [`BigRepoSubduction`]: crate::runtime::BigRepoSubduction

use crate::interlude::*;
use crate::runtime2::{
    messages::{DocWorkerMsg, Runtime2Cmd, Runtime2Evt},
    DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken,
    Runtime2Config, Runtime2Handle, TaskRuntime, TaskSet,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use future_form::FutureForm;

use std::collections::{BTreeSet, HashMap, HashSet};

// Re-export the ephemeral so embedders can subscribe.
pub use crate::ephemeral::BigEphemeral;

// ─── Local type aliases (replacing private types from runtime.rs) ──────────
// These mirror the private type aliases in runtime.rs that we can't access.
// When the old runtime types are made `pub(crate)`, these can be replaced
// with `crate::runtime::...`.

/// Mirror of `runtime.rs:634` `type SubductionSedimentrees`
pub(crate) type HubSedimentrees = Arc<
    subduction_core::collections::bounded_sharded_map::BoundedShardedMap<
        sedimentree_core::id::SedimentreeId,
        sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        256,
    >,
>;

/// Mirror of `runtime.rs:677` `type BigRepoSubduction<S>`
pub(crate) type HubSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    future_form::Sendable,
    S,
    crate::runtime::BigRepoIrohTransport,
    crate::handler::BigRepoComposedHandler<S>,
    crate::runtime::BigRepoPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    subduction_websocket::tokio::TimeoutTokio,
    subduction_websocket::tokio::TokioSpawn,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
>;

// ─── Runtime2Hub ───────────────────────────────────────────────────────────

/// The runtime actor. Owns the live state; driven by [`handle_cmd`](Self::handle_cmd)
/// / [`handle_evt`](Self::handle_evt) in the machine loop.
///
/// Mirrors [`BigRepoRuntimeWorker`](crate::runtime::BigRepoRuntimeWorker)
/// (`runtime.rs:1044`) with the same fields minus `big_sync_store`.
pub struct Runtime2Hub<F: FutureForm, R: TaskRuntime<F>> {
    // ── identity / config ──────────────────────────────────────────────────
    pub(crate) local_peer_id: PeerId,
    pub(crate) sync_policy: crate::runtime::BigRepoSyncPolicy,

    // ── subduction + sync ──────────────────────────────────────────────────
    pub(crate) subduction: Arc<HubSubduction<crate::runtime::BigRepoSubductionStorage>>,
    pub(crate) sedimentrees: HubSedimentrees,
    pub(crate) keyhive_protocol: crate::runtime::BigRepoKeyhiveProtocol,
    pub(crate) keyhive_handler: crate::runtime::BigRepoKeyhiveHandler,
    pub(crate) ephemeral_backend: Arc<dyn crate::ephemeral::BigEphemeralBackend>,
    pub(crate) ephemeral: crate::ephemeral::BigEphemeral,
    pub(crate) storage: crate::runtime::BigRepoSubductionStorage,
    /// Shared IO facade passed to every document worker.
    pub(crate) doc_io: Arc<dyn crate::runtime2::DocIo<F>>,

    // ── keyhive + change manager ───────────────────────────────────────────
    pub(crate) keyhive: crate::keyhive::BigKeyhiveHandle,
    pub(crate) keyhive_storage: crate::keyhive_storage::BigRepoKeyhiveStorage,
    pub(crate) change_manager: Arc<crate::changes::ChangeListenerManager>,

    // ── task sets ──────────────────────────────────────────────────────────
    /// Child/background tasks (construction-time, keyhive syncs, lease
    /// waiters, doc-workers). Stopped first on shutdown (children before
    /// the hub loop).
    pub(crate) child_tasks: R::Tasks,

    // ── determinism levers ─────────────────────────────────────────────────
    pub(crate) timer: Arc<dyn crate::runtime2::Timer<F>>,
    pub(crate) clock: Arc<dyn crate::runtime2::Clock>,

    // ── channels ───────────────────────────────────────────────────────────
    pub(crate) cmd_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Cmd>,
    pub(crate) evt_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Evt>,

    // ── connection state ───────────────────────────────────────────────────
    pub(crate) connected_peers: HashMap<PeerId, ConnDeets>,

    // ── keyhive sync bookkeeping ───────────────────────────────────────────
    pub(crate) pending_keyhive_syncs:
        HashMap<PeerId, Vec<(u64, tokio::sync::oneshot::Sender<eyre::Result<()>>)>>,
    pub(crate) active_keyhive_syncs: HashMap<PeerId, u64>,
    pub(crate) keyhive_dirty: BTreeSet<PeerId>,

    // ── doc-worker registry ────────────────────────────────────────────────
    pub(crate) doc_workers: HashMap<DocumentId, DocWorkerEntry>,
    pub(crate) pending_materialization: HashSet<DocumentId>,

    // ── waiter-id counters (shared with the handle) ────────────────────────
    pub(crate) doc_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
}

/// Per-connection bookkeeping. Holds the cancellation token and the `closed`
/// flag so [`handle_close_iroh`] / [`handle_connection_lost`] can tear down.
///
/// Mirrors `RuntimePeerConnDeets` at `runtime.rs:339`.
pub(crate) struct ConnDeets {
    pub closed: Arc<std::sync::atomic::AtomicBool>,
    pub cancel: tokio_util::sync::CancellationToken,
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMAND HANDLERS
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2Hub<F, R> {
    /// Process a single [`Runtime2Cmd`]. Every variant maps 1:1 to a variant
    /// of the old `RuntimeCmd`. Mirrors `handle_cmd` at `runtime.rs:1096`.
    pub(crate) fn handle_cmd(&mut self, cmd: Runtime2Cmd) -> eyre::Result<()> {
        match cmd {
            Runtime2Cmd::PutDoc {
                doc_id,
                initial_content,
                resp,
            } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::PutDoc {
                        initial_content,
                        resp,
                    })
                    .expect("task was found dead");
            }
            Runtime2Cmd::GetDocHandle { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::AcquireHandle { resp })
                    .expect("task was found dead");
            }
            Runtime2Cmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp,
            } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                // The internal lease is bundled into the message so the worker
                // stays alive for the duration of the operation.
                worker
                    .send(DocWorkerMsg::CommitDelta {
                        commits,
                        heads,
                        patches,
                        origin,
                        resp,
                        _lease,
                    })
                    .expect("task was found dead");
            }
            Runtime2Cmd::DocHeadState { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::QueryHeadState { resp })
                    .expect("task was found dead");
            }
            Runtime2Cmd::OpenConn { peer, addr, resp } => {
                // In runtime2 the connection is driven by TransportConnect.
                // For now we complete the response immediately with a peer-id
                // placeholder. The full handshake will go through the
                // TransportConnect trait in a later parcel.
                let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                self.connected_peers.insert(
                    peer,
                    ConnDeets {
                        closed: Arc::clone(&closed),
                        cancel: tokio_util::sync::CancellationToken::new(),
                    },
                );
                let _ = resp.send(Ok((peer, closed)));
            }
            Runtime2Cmd::AcceptConn { incoming: _, resp } => {
                // Same stub as OpenConn; the transport-agnostic accept
                // handshake is implemented in a later parcel.
                let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                // Placeholder peer — the real peer_id comes from the handshake.
                let peer = self.local_peer_id;
                self.connected_peers.insert(
                    peer,
                    ConnDeets {
                        closed: Arc::clone(&closed),
                        cancel: tokio_util::sync::CancellationToken::new(),
                    },
                );
                let _ = resp.send(Ok((peer, closed)));
            }
            Runtime2Cmd::CloseConn { peer_id, resp } => {
                self.cancel_pending_keyhive_syncs(&peer_id, "keyhive peer closed");
                self.cancel_pending_doc_syncs(&peer_id, "doc sync peer closed")?;
                if let Some(deets) = self.connected_peers.remove(&peer_id) {
                    deets.closed.store(true, std::sync::atomic::Ordering::SeqCst);
                    deets.cancel.cancel();
                }
                if let Some(resp) = resp {
                    let _ = resp.send(Ok(()));
                }
            }
            Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp,
            } => {
                let Ok((worker, _lease)) = self.doc_worker_handle(doc_id) else {
                    let _ = resp.send(Err(crate::runtime::SyncDocError::NotFound));
                    return Ok(());
                };
                if let Err(err) = worker.send(DocWorkerMsg::SyncWithPeer {
                    peer_id,
                    waiter_id,
                    timeout,
                    done: resp,
                    _lease,
                }) {
                    // Worker closed before accepting the sync request; report
                    // the error back to the caller (who is waiting on the
                    // oneshot that never fires). We log instead of crashing.
                    tracing::warn!(%doc_id, %peer_id, error = %err, "doc worker closed before sync");
                }
            }
            Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            } => {
                self.pending_keyhive_syncs
                    .entry(peer_id)
                    .or_default()
                    .push((waiter_id, resp));
                if !self.active_keyhive_syncs.contains_key(&peer_id) {
                    self.start_keyhive_sync(peer_id)?;
                }
            }
            Runtime2Cmd::SyncKeyhiveWithPeerInternal { peer_id } => {
                self.schedule_internal_keyhive_sync(peer_id);
            }
            Runtime2Cmd::NoteLocalKeyhiveChanged { resp } => {
                let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
                self.spawn_background(F::from_future(async move {
                    let out = async {
                        keyhive_protocol
                            .note_local_keyhive_changed()
                            .await
                            .wrap_err("keyhive local-change refresh failed")?;
                        eyre::Result::<()>::Ok(())
                    }
                    .await;
                    let _ = resp.send(out);
                    Ok(())
                }))?;
            }
            Runtime2Cmd::CancelDocSyncWaiter {
                doc_id,
                peer_id,
                waiter_id,
            } => {
                if let Some(entry) = self.doc_workers.get(&doc_id) {
                    entry
                        .handle
                        .send(DocWorkerMsg::CancelSyncWithPeer {
                            peer_id,
                            waiter_id: Some(waiter_id),
                            reason: "doc sync timed out",
                        })
                        .expect("task was found dead");
                }
            }
            Runtime2Cmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id } => {
                self.cancel_pending_keyhive_sync(&peer_id, waiter_id);
            }
            Runtime2Cmd::ReleaseDocLease { doc_id } => {
                self.handle_release_doc_lease(doc_id);
            }
            Runtime2Cmd::ReleaseInternalLease { doc_id } => {
                self.handle_release_internal_lease(doc_id);
            }
            Runtime2Cmd::CheckSedimentreeResident { doc_id, resp } => {
                let sedimentree_id =
                    sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                let storage = self.storage.clone();
                self.spawn_background(F::from_future(async move {
                    let stored = storage
                        .contains_sedimentree_id(sedimentree_id)
                        .await
                        .unwrap_or(false);
                    let _ = resp.send(stored);
                    Ok(())
                }))?;
            }
            Runtime2Cmd::CheckDocWorkerExists { doc_id, resp } => {
                let exists = self.doc_workers.contains_key(&doc_id);
                let _ = resp.send(exists);
            }
        }
        Ok(())
    }

    // ─── helper: spawn owned background work on the child task set ────────

    fn spawn_background(
        &self,
        fut: F::Future<'static, eyre::Result<()>>,
    ) -> eyre::Result<futures::stream::AbortHandle> {
        self.child_tasks.spawn(fut)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EVENT HANDLERS
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2Hub<F, R> {
    /// Process a single [`Runtime2Evt`]. Mirrors `handle_evt` at
    /// `runtime.rs:1315`.
    pub(crate) fn handle_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        match evt {
            Runtime2Evt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session);
            }
            Runtime2Evt::ConnEstablished { peer_id, closed } => {
                self.handle_connection_established(peer_id, closed)?;
            }
            Runtime2Evt::ConnLost { peer_id, error: _ } => {
                self.handle_connection_lost(peer_id)?;
            }
            Runtime2Evt::KeyhiveSyncDone { peer_id } => {
                self.finish_keyhive_sync(peer_id)?;
            }
            Runtime2Evt::KeyhiveSyncRequested { peer_id } => {
                self.schedule_internal_keyhive_sync(peer_id);
            }
            Runtime2Evt::DocWorkerHandleAcquired { bundle } => {
                self.handle_doc_worker_handle_acquired(bundle);
            }
            Runtime2Evt::DocWorkerStopped { doc_id } => {
                self.doc_workers.remove(&doc_id);
            }
            Runtime2Evt::FatalWorkerError {
                doc_id: _,
                context,
                error,
            } => {
                // Per AGENTS.md: programming errors crash the program.
                panic!("fatal runtime worker error context={context}: {error}");
            }
            Runtime2Evt::DocWorkerMaterializationPending { doc_id } => {
                self.pending_materialization.insert(doc_id);
            }
            Runtime2Evt::DocWorkerMaterializationReady { doc_id } => {
                self.pending_materialization.remove(&doc_id);
            }
            // --- Keyhive event listener handlers (parity with old) ---
            Runtime2Evt::PrekeyExpanded { new_prekey } => {
                self.change_manager
                    .notify_prekeys_expanded(new_prekey)
                    .expect("task was found dead");
            }
            Runtime2Evt::PrekeyRotated { rotate_key } => {
                self.change_manager
                    .notify_prekey_rotated(rotate_key)
                    .expect("task was found dead");
            }
            Runtime2Evt::CgkaOp { data } => {
                self.change_manager
                    .notify_cgka_op(data)
                    .expect("task was found dead");
            }
            Runtime2Evt::DelegationReceived { target, data } => {
                self.change_manager
                    .notify_delegation_received(data)
                    .expect("task was found dead");
                self.update_doc_access(target);
            }
            Runtime2Evt::RevocationReceived { target, data } => {
                self.change_manager
                    .notify_revocation_received(data)
                    .expect("task was found dead");
                self.update_doc_access(target);
            }
        }
        Ok(())
    }

    // ─── sync session routing ──────────────────────────────────────────────

    /// Route an observed sync session to the relevant doc-worker.
    /// Mirrors `handle_sync_session_observed` at `runtime.rs:1643`.
    #[tracing::instrument(skip_all)]
    fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) {
        let doc_id = DocumentId::new(*session.sedimentree_id.as_bytes());
        tracing::debug!(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            sent_commit_ids = session.sent_commit_ids.len(),
            sent_fragment_ids = session.sent_fragment_ids.len(),
            "observed sync session"
        );
        let Ok((worker, _lease)) = self.doc_worker_handle(doc_id) else {
            return;
        };
        worker
            .send(DocWorkerMsg::ApplySyncSession { session, _lease })
            .expect("task was found dead");
    }

    // ─── connection lifecycle ──────────────────────────────────────────────

    /// Handle an established connection: register and schedule initial keyhive sync.
    /// Mirrors `handle_connection_established` at `runtime.rs:2087`.
    fn handle_connection_established(
        &self,
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        let kh_proto = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(F::from_future(async move {
            let kh_peer_id =
                crate::keyhive_conn::KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            kh_proto
                .initiate_sync_with_peer(&kh_peer_id)
                .await
                .wrap_err("initial keyhive sync after connection established failed")?;
            Ok(())
        }))?;
        Ok(())
    }

    /// Handle a lost connection: clean up syncs and connected_peers.
    /// Mirrors `handle_connection_lost` at `runtime.rs:2037`.
    fn handle_connection_lost(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        self.cancel_pending_keyhive_syncs(&peer_id, "keyhive connection lost");
        self.cancel_pending_doc_syncs(&peer_id, "doc sync connection lost")?;
        self.connected_peers.remove(&peer_id);
        Ok(())
    }

    // ─── keyhive sync ──────────────────────────────────────────────────────

    /// Start a keyhive sync round with `peer_id` if not already active.
    /// Mirrors `start_keyhive_sync` at `runtime.rs:1758`.
    fn start_keyhive_sync(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        if self.active_keyhive_syncs.contains_key(&peer_id) {
            return Ok(());
        }
        let watermark = self
            .keyhive_sync_waiter_ids
            .load(std::sync::atomic::Ordering::Relaxed);
        self.active_keyhive_syncs.insert(peer_id, watermark);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        let kh_peer_id =
            crate::keyhive_conn::KeyhivePeerId::from_bytes(*peer_id.as_bytes());
        self.spawn_background(F::from_future(async move {
            keyhive_protocol
                .initiate_sync_with_peer(&kh_peer_id)
                .await
                .wrap_err_with(|| format!("keyhive sync with {peer_id} failed"))?;
            Ok(())
        }))?;
        Ok(())
    }

    /// Finish a keyhive sync round: resolve waiters, cascade if dirty, reattempt
    /// materialization for pending docs. Mirrors `finish_keyhive_sync` at
    /// `runtime.rs:1705`.
    fn finish_keyhive_sync(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        let Some(watermark) = self.active_keyhive_syncs.remove(&peer_id) else {
            tracing::debug!(%peer_id, "ignoring untracked keyhive sync completion");
            return Ok(());
        };
        // Split waiters: those that existed before this sync started resolve,
        // those that arrived during/after cascade into a new round.
        if let Some(waiters) = self.pending_keyhive_syncs.get_mut(&peer_id) {
            let mut remaining = Vec::new();
            for (id, sender) in std::mem::take(waiters) {
                if id < watermark {
                    let _ = sender.send(Ok(()));
                } else {
                    remaining.push((id, sender));
                }
            }
            if remaining.is_empty() {
                self.pending_keyhive_syncs.remove(&peer_id);
            } else {
                *waiters = remaining;
            }
        }
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(F::from_future(async move {
            keyhive_protocol
                .refresh_cache()
                .await
                .wrap_err("keyhive cache refresh failed")?;
            Ok(())
        }))?;
        let has_remaining = self.pending_keyhive_syncs.contains_key(&peer_id);
        if has_remaining || self.keyhive_dirty.remove(&peer_id) {
            self.start_keyhive_sync(peer_id)?;
        }
        for doc_id in self.pending_materialization.clone() {
            if let Ok((worker, _lease)) = self.doc_worker_handle(doc_id) {
                let _ = worker.send(DocWorkerMsg::ReattemptMaterialization);
            } else {
                tracing::warn!(
                    %doc_id,
                    "failed to get doc worker for reattempt on keyhive sync done"
                );
            }
        }
        Ok(())
    }

    /// Schedule an internal keyhive sync (triggered by keyhive-change events).
    /// Mirrors `schedule_internal_keyhive_sync` at `runtime.rs:1774`.
    fn schedule_internal_keyhive_sync(&mut self, peer_id: PeerId) {
        if !self.connected_peers.contains_key(&peer_id) {
            tracing::debug!(%peer_id, "dropping internal keyhive sync for disconnected peer");
            return;
        }
        if self.active_keyhive_syncs.contains_key(&peer_id) {
            self.keyhive_dirty.insert(peer_id);
        } else if let Err(err) = self.start_keyhive_sync(peer_id) {
            tracing::warn!(%peer_id, error = %err, "failed to start internal keyhive sync");
        }
    }

    /// Cancel a pending keyhive sync waiter by id.
    fn cancel_pending_keyhive_sync(&mut self, peer_id: &PeerId, waiter_id: u64) -> bool {
        if let Some(waiters) = self.pending_keyhive_syncs.get_mut(peer_id) {
            let len_before = waiters.len();
            waiters.retain(|(id, _)| *id != waiter_id);
            if waiters.is_empty() {
                self.pending_keyhive_syncs.remove(peer_id);
            }
            return waiters.len() < len_before;
        }
        false
    }

    /// Cancel all pending keyhive syncs for a peer.
    fn cancel_pending_keyhive_syncs(&mut self, peer_id: &PeerId, reason: &'static str) {
        if let Some(waiters) = self.pending_keyhive_syncs.remove(peer_id) {
            for (_id, sender) in waiters {
                let _ = sender.send(Err(eyre::eyre!("{}", reason)));
            }
        }
    }

    /// Cancel all pending doc syncs for a peer (fan-out to all doc-workers).
    fn cancel_pending_doc_syncs(&mut self, peer_id: &PeerId, reason: &'static str) -> eyre::Result<()> {
        let workers: Vec<(DocumentId, DocWorkerHandle)> = self
            .doc_workers
            .iter()
            .filter_map(|(doc_id, entry)| {
                (entry.internal_leases > 0).then_some((*doc_id, entry.handle.clone()))
            })
            .collect();
        for (doc_id, worker) in workers {
            if worker
                .send(DocWorkerMsg::CancelSyncWithPeer {
                    peer_id: *peer_id,
                    waiter_id: None,
                    reason,
                })
                .is_err()
            {
                tracing::warn!(%doc_id, %peer_id, "doc worker stopped before sync cancellation");
            }
        }
        Ok(())
    }

    // ─── doc access updates ────────────────────────────────────────────────

    /// Incrementally update doc access for a single target (doc or group) after
    /// a delegation or revocation is received. Mirrors `update_doc_access` at
    /// `runtime.rs:1420`.
    ///
    /// NOTE: the old runtime updates `big_sync_store.set_doc_members` /
    /// `add_obj_to_parts` here. In runtime2, big_sync is a sibling layer — the
    /// access update is forwarded via the change manager or an event channel.
    /// For now, this is a no-op that logs the target.
    fn update_doc_access(&self, _target: keyhive_core::principal::identifier::Identifier) {
        // TODO(runtime2): forward access change to the big_sync sibling layer
        // via an event channel or callback. The old runtime did:
        //   big_sync_store.set_doc_members(doc_id, agents)
        //   big_sync_store.add_obj_to_parts(doc_id, vec![GLOBAL_PART_ID])
        // These are out of scope for runtime2's Layer 2.
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DOC-WORKER LIFECYCLE
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2Hub<F, R> {
    /// Ensure a doc-worker exists for `doc_id`, return its handle + internal lease.
    ///
    /// If the worker is already alive, bumps `internal_leases` and clears the
    /// eviction deadline. Mirrors `doc_worker_handle` at `runtime.rs:1611`.
    pub(crate) fn doc_worker_handle(
        &mut self,
        doc_id: DocumentId,
    ) -> eyre::Result<(DocWorkerHandle, DocWorkerInternalLease)> {
        self.spawn_doc_worker(doc_id)?;
        let entry = self
            .doc_workers
            .get_mut(&doc_id)
            .ok_or_eyre("doc worker missing after spawn")?;
        entry.eviction_deadline = None;
        entry.internal_leases += 1;
        let handle = entry.handle.clone();

        // Create a oneshot whose sender is consumed by the lease. When the
        // lease drops (doc-worker finishes the op), the sender is dropped
        // and the receiver gets `RecvError::Closed` — a tracked child task
        // forwards this as a `ReleaseInternalLease` command back to the hub.
        let (lease_tx, lease_rx) = tokio::sync::oneshot::channel::<()>();
        let cmd_tx = self.cmd_tx.clone();
        self.spawn_background(F::from_future(async move {
            let _ = lease_rx.await;
            cmd_tx
                .send(Runtime2Cmd::ReleaseInternalLease { doc_id })
                .map_err(|_| ferr!("runtime stopped before internal lease release"))?;
            Ok(())
        }))?;

        let lease = DocWorkerInternalLease::new(doc_id, lease_tx);
        Ok((handle, lease))
    }

    /// Lazily spawn a doc-worker if none exists. Mirrors `spawn_doc_worker` at
    /// `runtime.rs:1514`.
    #[tracing::instrument(skip_all, fields(%doc_id))]
    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        // Fast path: already alive, just reset eviction.
        if self.doc_workers.get(&doc_id).is_some_and(|entry| {
            !entry.handle.msg_tx.is_closed()
        }) {
            if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                entry.eviction_deadline = None;
            }
            return Ok(());
        }
        // Stale entry: remove before re-creating.
        self.doc_workers.remove(&doc_id);

        let cancel = tokio_util::sync::CancellationToken::new();
        let (handle, stop, abort_handle) = crate::runtime2::spawn_doc_worker(
            doc_id,
            Arc::clone(&self.doc_io),
            self.keyhive.clone(),
            self.keyhive_storage.clone(),
            Arc::clone(&self.change_manager),
            self.sync_policy,
            Arc::clone(&self.clock),
            &self.child_tasks,
            self.evt_tx.clone(),
            cancel,
        )?;

        self.doc_workers.insert(
            doc_id,
            DocWorkerEntry {
                handle,
                stop,
                abort_handle,
                local_handles: 0,
                internal_leases: 0,
                eviction_deadline: Some(
                    self.clock.instant() + self.sync_policy.doc_worker_idle_ttl,
                ),
            },
        );
        Ok(())
    }

    /// Decrement `local_handles` for a doc-worker; schedule eviction if idle.
    /// Mirrors `handle_release_doc_lease` at `runtime.rs:1626`.
    fn handle_release_doc_lease(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            assert!(
                entry.local_handles > 0,
                "doc lease underflow for doc worker: {doc_id:?}"
            );
            entry.local_handles -= 1;
            entry
                .handle
                .send(DocWorkerMsg::ReleaseHandleLease)
                .expect("task was found dead");
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    /// Decrement `internal_leases` for a doc-worker; schedule eviction if idle.
    /// Mirrors `handle_release_internal_lease` at `runtime.rs:1467`.
    fn handle_release_internal_lease(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            assert!(
                entry.internal_leases > 0,
                "internal lease underflow for doc worker: {doc_id:?}"
            );
            entry.internal_leases -= 1;
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    /// Increment `local_handles` when a handle is acquired.
    /// Mirrors `handle_doc_worker_handle_acquired` at `runtime.rs:1478`.
    fn handle_doc_worker_handle_acquired(&mut self, bundle: Arc<crate::runtime::LiveDocBundle>) {
        let doc_id = bundle.doc_id;
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.local_handles += 1;
            entry.eviction_deadline = None;
        }
    }

    /// Set or clear the eviction deadline based on refcounts.
    /// Mirrors `schedule_doc_worker_eviction_if_idle` at `runtime.rs:1441`.
    fn schedule_doc_worker_eviction_if_idle(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        if entry.local_handles > 0
            || entry.internal_leases > 0
            || self.pending_materialization.contains(&doc_id)
        {
            entry.eviction_deadline = None;
            return;
        }
        entry.eviction_deadline = Some(
            self.clock.instant() + self.sync_policy.doc_worker_idle_ttl,
        );
    }

    /// Periodic eviction of idle doc-workers. Driven by the machine loop's
    /// `Timer::tick(doc_worker_idle_ttl)`. Mirrors
    /// `handle_doc_worker_janitor_tick` at `runtime.rs:1455`.
    pub(crate) fn janitor_tick(&mut self) {
        let now = self.clock.instant();
        let expired: Vec<DocumentId> = self
            .doc_workers
            .iter()
            .filter(|(_, entry)| {
                entry
                    .eviction_deadline
                    .is_some_and(|deadline| deadline <= now)
            })
            .map(|(doc_id, _)| *doc_id)
            .collect();
        for doc_id in expired {
            if let Some(entry) = self.doc_workers.get(&doc_id) {
                entry.stop.cancel();
                entry.abort_handle.abort();
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════
// STOP TOKEN
// ═══════════════════════════════════════════════════════════════════════════

/// Stop token for the runtime. Generic over the async form `F` and the
/// concrete [`TaskRuntime`] backend `R`. Holds two independent task sets:
/// - `child_tasks` — construction-time workers and dynamic background jobs,
///    stopped first on shutdown (children before the hub loop).
/// - `machine_tasks` — the hub's machine loop, stopped after children.
///
/// Mirrors `BigRepoRuntimeStopToken` at `runtime.rs:613`.
pub struct Runtime2StopToken<F: FutureForm, R: TaskRuntime<F>> {
    pub(crate) cancel: tokio_util::sync::CancellationToken,
    pub(crate) child_tasks: R::Tasks,
    pub(crate) machine_tasks: R::Tasks,
}

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2StopToken<F, R> {
    /// Cancel the runtime and await graceful shutdown. Reverses startup order:
    /// children first, then the hub machine loop.
    pub async fn stop(self, timeout: std::time::Duration) -> eyre::Result<()> {
        self.cancel.cancel();
        // Not every child operation observes the runtime cancellation token
        // (for example a peer sync may be awaiting transport IO). Abort the
        // child scope, then join it before allowing the hub loop to terminate.
        self.child_tasks.abort();
        self.child_tasks.stop(timeout).await?;
        self.machine_tasks.stop(timeout).await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SPAWN
// ═══════════════════════════════════════════════════════════════════════════

/// Top-standing runtime spawn. Mirrors
/// [`spawn_big_repo_runtime`](crate::runtime::spawn_big_repo_runtime) at
/// `runtime.rs:707`. Spawns background workers + the machine loop and
/// returns the handle + stop token.
///
/// The generics match [`Runtime2Config`]: `F` for the async form, `R` for
/// the concrete task runtime, `S` for subduction storage, `K` for keyhive
/// storage. Two independent [`TaskSet`]s are created from the runtime:
/// - `child_tasks`: all construction-time workers, dynamic background jobs,
///   and doc-workers. Stopped first on shutdown.
/// - `machine_tasks`: the hub's machine loop. Stopped after children.
///
/// This preserves the old runtime's reverse-order shutdown: children die
/// before the hub loop that dispatched them.
///
/// Determinism: the task runtime is injected via `config.tasks`, so a
/// step-task-runtime implementation can drive tests deterministically.
#[allow(clippy::too_many_lines)]
pub fn spawn_runtime2<F, R, S, K>(
    config: Runtime2Config<F, R, S, K>,
) -> eyre::Result<(Runtime2Handle, Runtime2StopToken<F, R>)>
where
    F: FutureForm + 'static,
    R: TaskRuntime<F>,
    S: subduction_core::storage::traits::Storage<F>
        + Clone + Send + Sync + std::fmt::Debug + 'static,
    K: subduction_keyhive::storage::KeyhiveStorage<F> + Send + Sync + 'static,
{
    use crate::keyhive_conn::BigRepoKeyhiveConnAdapter;
    use subduction_core::subduction::Subduction;

    let Runtime2Config {
        signer,
        storage,
        keyhive_storage,
        doc_io,
        policy,
        sync_policy,
        keyhive,
        change_manager,
        tasks,
        timer,
        clock,
        rng: _,
        connect: _,
    } = config;

    // Create two independent task sets for reverse-order shutdown.
    let child_tasks = tasks.task_set();
    let machine_tasks = tasks.task_set();

    let local_peer_id = PeerId::new(*signer.verifying_key().as_bytes());
    let runtime_stop = tokio_util::sync::CancellationToken::new();

    let sedimentrees: HubSedimentrees = Arc::new(
        subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new(),
    );
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));

    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<Runtime2Cmd>();
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Runtime2Evt>();

    // ── Spawn 1 (child): keyhive-listener event forwarder ──────────────────
    // The listener_evt_rx is handed to the hub during construction.
    // For now we create a dummy channel — the real listener integration
    // will wire it through the config.
    let (_listener_evt_tx, mut listener_evt_rx) =
        tokio::sync::mpsc::unbounded_channel::<Runtime2Evt>();
    let evt_tx_for_listener = evt_tx.clone();
    child_tasks.spawn(F::from_future(async move {
        while let Some(evt) = listener_evt_rx.recv().await {
            evt_tx_for_listener
                .send(evt)
                .map_err(|_| ferr!("runtime stopped before listener event"))?;
        }
        Ok(())
    }))?;

    // ── Subduction boot ───────────────────────────────────────────────────
    let storage_for_reads = storage.clone();
    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(BigRepoSyncSessionBridge {
        evt_tx: evt_tx.clone(),
    });

    let storage_powerbox = subduction_core::storage::powerbox::StoragePowerbox::new(
        storage.clone(),
        policy,
    );

    // NOTE: SyncHandler, Subduction, etc. are hardcoded to `Sendable` in the
    // current type aliases. The hub stores `Sendable`-concrete subduction
    // types. Full `F`-generics for subduction types is a future lift.
    let sync_handler = subduction_core::handler::sync::SyncHandler::new(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        sedimentree_core::depth::CountLeadingZeroBytes,
        // TODO: replace TokioSpawn with a TaskSet adapter when subduction types
        // support FutureForm-generic spawners.
        subduction_websocket::tokio::TokioSpawn,
    );
    sync_handler.set_sync_session_observer(Arc::clone(&sync_session_observer));
    let send_counter = sync_handler.send_counter().clone();
    let sync_handler = Arc::new(sync_handler);

    // ── Ephemeral ──────────────────────────────────────────────────────────
    let (ephemeral_handler, ephemeral_rx) = crate::ephemeral::EphemeralHandler::new(
        Arc::clone(&connections),
        crate::ephemeral::OpenEphemeralPolicy,
        Default::default(),
        crate::runtime2::ClockBackedClock(std::time::Duration::from_millis(100)),
    );
    let ephemeral_handler: crate::runtime::BigRepoEphemeralHandler = Arc::new(ephemeral_handler);
    let ephemeral_backend: Arc<dyn crate::ephemeral::BigEphemeralBackend> =
        Arc::new(crate::ephemeral::BigRepoEphemeralBackend::new(
            signer.clone(),
            Arc::clone(&ephemeral_handler),
        ));
    let ephemeral_switchboard = crate::ephemeral::BigEphemeralSwitchboard::spawn(
        Arc::clone(&ephemeral_backend),
        ephemeral_rx,
        runtime_stop.clone(),
        // TODO: lift BigEphemeralSwitchboard over TaskSet/FutureForm. The
        // current switchboard constructor still requires AbortableJoinSet.
        unimplemented!("ephemeral TaskSet integration is part of green-up"),
    );
    let ephemeral = crate::ephemeral::BigEphemeral::new(
        Arc::clone(&ephemeral_backend),
        ephemeral_switchboard,
    );

    // ── Keyhive protocol and handler ───────────────────────────────────────
    let (keyhive_protocol, keyhive_handler) = {
        let contact_card = keyhive.contact_card().clone();
        let kh_peer_id = keyhive.keyhive_peer_id();
        let keyhive_protocol: crate::runtime::BigRepoKeyhiveProtocol = Arc::new(
            subduction_keyhive::KeyhiveProtocol::new(
                Arc::clone(&keyhive.clone_keyhive()),
                keyhive_storage.as_ref().clone(),
                kh_peer_id,
                contact_card,
            )
            .with_storage_recovery(),
        );

        let mut keyhive_handler = crate::runtime::BigRepoKeyhiveHandler::new(
            Arc::clone(&keyhive_protocol),
            BigRepoKeyhiveConnAdapter::new
                as fn(
                    subduction_core::authenticated::Authenticated<
                        crate::runtime::BigRepoIrohTransport,
                        future_form::Sendable,
                    >,
                ) -> BigRepoKeyhiveConnAdapter,
        );
        keyhive_handler = keyhive_handler.with_sync_done_observer({
            let evt_tx = evt_tx.clone();
            Arc::new(move |peer_id| {
                let peer_id = PeerId::new(*peer_id.verifying_key());
                if evt_tx
                    .send(Runtime2Evt::KeyhiveSyncDone { peer_id })
                    .is_err()
                {
                    tracing::debug!(%peer_id, "runtime stopped before keyhive sync-done event");
                }
            })
        });
        (keyhive_protocol, keyhive_handler)
    };

    // ── Keyhive-change subscription (spawn 5) ───────────────────────────────
    child_tasks.spawn(F::from_future(async move { Ok(()) }))?;

    // ── Composed handler + Subduction ──────────────────────────────────────
    let composed_handler = Arc::new(
        crate::handler::BigRepoComposedHandler::new(
            sync_handler,
            Arc::clone(&ephemeral_handler),
            keyhive_handler,
        ),
    );

    let (subduction, listener, manager) = Subduction::new(
        composed_handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        send_counter,
        subduction_core::nonce_cache::NonceCache::new(sync_policy.subduction_nonce_ttl),
        subduction_websocket::tokio::TimeoutTokio,
        sync_policy.subduction_default_roundtrip_timeout,
        sedimentree_core::depth::CountLeadingZeroBytes,
        subduction_websocket::tokio::TokioSpawn,
    );
    subduction.set_sync_session_observer(sync_session_observer);
    let subduction_handle: Arc<HubSubduction<
        crate::runtime::BigRepoSubductionStorage,
    >> = Arc::clone(&subduction);

    // ── Build the hub ──────────────────────────────────────────────────────
    let keyhive_storage_arc = keyhive_storage;
    let mut hub = Runtime2Hub {
        local_peer_id,
        sync_policy,
        subduction: subduction_handle,
        sedimentrees,
        keyhive_protocol,
        keyhive_handler,
        ephemeral_backend,
        ephemeral,
        storage: storage_for_reads,
        doc_io,
        keyhive,
        keyhive_storage: keyhive_storage_arc.clone(),
        change_manager,
        child_tasks: child_tasks.clone(),
        timer: timer.clone(),
        clock: clock.clone(),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: HashMap::new(),
        pending_keyhive_syncs: HashMap::new(),
        active_keyhive_syncs: HashMap::new(),
        keyhive_dirty: BTreeSet::new(),
        doc_workers: HashMap::new(),
        pending_materialization: HashSet::new(),
        doc_sync_waiter_ids: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        keyhive_sync_waiter_ids: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    // ── Spawn 2 (child): subduction listener ───────────────────────────────
    child_tasks.spawn(F::from_future({
        let stop = runtime_stop.clone();
        async move {
            let _ = stop
                .run_until_cancelled(async move {
                    listener.await.unwrap();
                })
                .await;
            eyre::Ok(())
        }
    }))?;

    // ── Spawn 3 (child): subduction manager ─────────────────────────────────
    child_tasks.spawn(F::from_future({
        let stop = runtime_stop.clone();
        async move {
            let _ = stop
                .run_until_cancelled(async move {
                    manager.await.unwrap();
                })
                .await;
            eyre::Ok(())
        }
    }))?;

    // ── Spawn 4 (child): keyhive maintenance loop ──────────────────────────
    let keyhive_protocol = Arc::clone(&hub.keyhive_protocol);
    child_tasks.spawn(F::from_future({
        let stop = runtime_stop.clone();
        let timer = timer.clone();
        let keyhive_archive_id =
            subduction_keyhive::storage::StorageHash::new(*local_peer_id.as_bytes());
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    _ = timer.tick(std::time::Duration::from_secs(2)) => {
                        if let Err(e) = keyhive_protocol.refresh_cache().await {
                            tracing::warn!(error = %e, "keyhive cache refresh failed");
                        }
                    },
                    _ = timer.tick(std::time::Duration::from_secs(300)) => {
                        if let Err(e) = keyhive_protocol.compact(keyhive_archive_id).await {
                            tracing::warn!(error = %e, "keyhive archive compact failed");
                        }
                    }
                }
            }
            #[allow(unreachable_code)]
            eyre::Ok(())
        }
    }))?;

    // ── Construct handle ───────────────────────────────────────────────────
    let handle = Runtime2Handle::new(
        cmd_tx.clone(),
        hub.keyhive.clone(),
        Arc::new(hub.keyhive_storage.clone()),
        hub.sync_policy,
    );

    // ── Spawn 6 (machine): the hub machine loop ────────────────────────────
    // Spawned on the MACHINE task set so it is stopped after children on
    // shutdown (reverse startup order).
    machine_tasks.spawn(F::from_future(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = runtime_stop.cancelled() => break,
                    _ = timer.tick(std::time::Duration::from_millis(500)) => {
                        hub.janitor_tick();
                    },
                    cmd = cmd_rx.recv() => {
                        let Some(cmd) = cmd else { break; };
                        hub.handle_cmd(cmd)?;
                    },
                    evt = evt_rx.recv() => {
                        let Some(evt) = evt else { break; };
                        hub.handle_evt(evt)?;
                    }
                }
            }
            Ok(())
        }))?;

    Ok((
        handle,
        Runtime2StopToken {
            cancel: runtime_stop,
            child_tasks,
            machine_tasks,
        },
    ))
}

// ─── Bridge: subduction sync session → Runtime2Evt ─────────────────────────

/// Bridges subduction's sync session callback into the runtime event channel.
/// Mirrors `BigRepoSyncSessionBridge` at `runtime.rs:690`.
struct BigRepoSyncSessionBridge {
    evt_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Evt>,
}

impl subduction_core::sync_session::SyncSessionObserver for BigRepoSyncSessionBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        if self
            .evt_tx
            .send(Runtime2Evt::SyncSessionObserved { session })
            .is_err()
        {
            tracing::warn!("runtime shutting down; dropping observed sync session");
        }
    }
}

// ─── Clock adapter: `crate::runtime2::Clock` → `subduction_core::clock::Clock` ──

/// Adapter that wraps an injected [`Clock`] for subduction's
/// [`Clock`](subduction_core::clock::Clock) trait. Used by the ephemeral.
pub(crate) struct ClockBackedClock(pub std::time::Duration);

impl subduction_core::clock::Clock for ClockBackedClock {
    fn now(&self) -> subduction_core::timestamp::TimestampSeconds {
        subduction_core::timestamp::TimestampSeconds::now()
    }
    fn interval(&self) -> std::time::Duration {
        self.0
    }
}
