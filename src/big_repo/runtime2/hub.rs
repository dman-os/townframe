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
    DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken, Runtime2Config,
    Runtime2Handle, TaskRuntime, TaskSet,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use future_form::{FutureForm, Local, Sendable};

use std::collections::{BTreeSet, HashMap, HashSet};

// Re-export the ephemeral so embedders can subscribe.
pub use crate::ephemeral::BigEphemeral;

// ─── Local type aliases (replacing private types from runtime.rs) ──────────
// These mirror the private type aliases in runtime.rs that we can't access.
// When the old runtime types are made `pub(crate)`, these can be replaced
// with `crate::runtime::...`.

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

    // ── injected IO facades ────────────────────────────────────────────────
    pub(crate) runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
    /// Transport-agnostic connection factory. Cloned for each spawn so
    /// background connect/accept/close tasks can drive IO without holding
    /// a borrow on the hub.
    pub(crate) connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
    /// Shared IO facade passed to every document worker.
    pub(crate) doc_io: Arc<dyn crate::runtime2::DocIo<F>>,

    // ── change manager ─────────────────────────────────────────────────────
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
    pub(crate) cmd_tx: async_channel::Sender<Runtime2Cmd>,
    pub(crate) evt_tx: async_channel::Sender<Runtime2Evt>,

    // ── connection state ───────────────────────────────────────────────────
    pub(crate) connected_peers: HashMap<PeerId, ConnDeets>,

    // ── keyhive sync bookkeeping ───────────────────────────────────────────
    pub(crate) pending_keyhive_syncs:
        HashMap<PeerId, Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>>,
    pub(crate) active_keyhive_syncs: HashMap<PeerId, KeyhiveSyncRound>,
    pub(crate) keyhive_round_ids: u64,
    pub(crate) keyhive_dirty: BTreeSet<PeerId>,

    // ── runtime-wide quiescence ────────────────────────────────────────────
    pub(crate) quiescence_waiters:
        Vec<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    pub(crate) quiescence_probe: Option<QuiescenceProbe>,
    pub(crate) quiescence_barrier_ids: u64,
    pub(crate) activity_generation: u64,

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
}

pub(crate) struct KeyhiveSyncRound {
    pub watermark: u64,
    pub round_id: u64,
    pub request_id: subduction_keyhive::message::RequestId,
    pub cache_refresh_started: bool,
}

pub(crate) struct QuiescenceProbe {
    barrier_id: u64,
    activity_generation: u64,
    pending_docs: HashSet<DocumentId>,
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMAND HANDLERS
// ═══════════════════════════════════════════════════════════════════════════

trait HubCommandFuture<F: FutureForm> {
    fn create_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn check_sedimentree(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<bool>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn inspect_stored_doc_blobs(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn note_local_keyhive_changed(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> HubCommandFuture<F> for F {
    fn create_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match runtime_io.create_document(parents, content_heads).await {
                Ok(doc_id) => {
                    if cmd_tx
                        .send(Runtime2Cmd::PutDoc {
                            doc_id,
                            initial_content,
                            resp,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!(?doc_id, "runtime stopped before PutDoc could be enqueued");
                    }
                }
                Err(err) => {
                    let _ = resp.send(Err(err));
                }
            }
            Ok(())
        })
    }

    fn check_sedimentree(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<bool>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let stored = runtime_io
                .contains_sedimentree(sed_id)
                .await
                .unwrap_or(false);
            let _ = resp.send(stored);
            Ok(())
        })
    }

    fn inspect_stored_doc_blobs(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = runtime_io.inspect_stored_doc_blobs(sed_id).await;
            let _ = resp.send(result);
            Ok(())
        })
    }

    fn note_local_keyhive_changed(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let out = runtime_io
                .note_local_keyhive_changed()
                .await
                .wrap_err("keyhive local-change refresh failed");
            let _ = resp.send(out);
            Ok(())
        })
    }
}

impl<
        F: FutureForm
            + HubCommandFuture<F>
            + HubBackgroundFuture<F>
            + HubIoFutures<F, R::Tasks>
            + crate::runtime2::doc_worker::DocWorkerLoop<F>,
        R: TaskRuntime<F>,
    > Runtime2Hub<F, R>
where
    F: 'static,
{
    fn note_activity(&mut self) {
        self.activity_generation = self.activity_generation.wrapping_add(1);
    }

    fn request_quiescence(
        &mut self,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> eyre::Result<()> {
        self.quiescence_waiters.push(resp);
        if self.quiescence_probe.is_none() {
            self.start_quiescence_probe()?;
        }
        self.try_resolve_quiescence()
    }

    fn start_quiescence_probe(&mut self) -> eyre::Result<()> {
        self.quiescence_barrier_ids = self.quiescence_barrier_ids.wrapping_add(1);
        let barrier_id = self.quiescence_barrier_ids;
        let generation = self.activity_generation;
        let doc_ids: Vec<_> = self.doc_workers.keys().copied().collect();
        self.quiescence_probe = Some(QuiescenceProbe {
            barrier_id,
            activity_generation: generation,
            pending_docs: doc_ids.iter().copied().collect(),
        });
        for doc_id in doc_ids {
            let (worker, lease) = self.doc_worker_handle(doc_id)?;
            worker
                .send(DocWorkerMsg::Quiesce {
                    barrier_id,
                    _lease: lease,
                })
                .expect("task was found dead");
        }
        Ok(())
    }

    fn handle_doc_worker_quiescent(
        &mut self,
        doc_id: DocumentId,
        barrier_id: u64,
    ) -> eyre::Result<()> {
        if let Some(probe) = self.quiescence_probe.as_mut() {
            if probe.barrier_id == barrier_id {
                probe.pending_docs.remove(&doc_id);
            }
        }
        self.try_resolve_quiescence()
    }

    fn try_resolve_quiescence(&mut self) -> eyre::Result<()> {
        let Some(probe) = self.quiescence_probe.as_ref() else {
            return Ok(());
        };
        if probe.activity_generation != self.activity_generation {
            self.start_quiescence_probe()?;
            return Ok(());
        }
        if !probe.pending_docs.is_empty()
            || !self.active_keyhive_syncs.is_empty()
            || !self.keyhive_dirty.is_empty()
        {
            return Ok(());
        }
        self.quiescence_probe = None;
        for waiter in std::mem::take(&mut self.quiescence_waiters) {
            waiter.send(Ok(())).expect("quiescence waiter receiver must remain open");
        }
        Ok(())
    }

    /// Process a single [`Runtime2Cmd`]. Every variant maps 1:1 to a variant
    /// of the old `RuntimeCmd`. Mirrors `handle_cmd` at `runtime.rs:1096`.
    pub(crate) fn handle_cmd(&mut self, cmd: Runtime2Cmd) -> eyre::Result<()> {
        if !matches!(&cmd, Runtime2Cmd::WaitForQuiescence { .. })
            && !matches!(
                &cmd,
                Runtime2Cmd::ReleaseDocLease { .. }
                    | Runtime2Cmd::ReleaseInternalLease { .. }
            )
        {
            self.note_activity();
        }
        match cmd {
            Runtime2Cmd::CreateDoc {
                initial_content,
                parents,
                content_heads,
                resp,
            } => {
                self.spawn_background(F::create_doc(
                    Arc::clone(&self.runtime_io),
                    self.cmd_tx.clone(),
                    initial_content,
                    parents,
                    content_heads,
                    resp,
                ))?;
            }
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
                let child_tasks = self.child_tasks.clone();
                self.spawn_background(F::open_connection_and_watch(
                    Arc::clone(&self.connect),
                    peer,
                    addr,
                    self.evt_tx.clone(),
                    child_tasks,
                    resp,
                ))?;
            }
            Runtime2Cmd::AcceptConn { incoming, resp } => {
                let child_tasks = self.child_tasks.clone();
                self.spawn_background(F::accept_connection_and_watch(
                    Arc::clone(&self.connect),
                    incoming,
                    self.evt_tx.clone(),
                    child_tasks,
                    resp,
                ))?;
            }
            Runtime2Cmd::CloseConn { peer_id, resp } => {
                self.cancel_pending_keyhive_syncs(&peer_id, "keyhive peer closed");
                self.cancel_pending_doc_syncs(&peer_id, "doc sync peer closed")?;
                if let Some(deets) = self.connected_peers.remove(&peer_id) {
                    deets
                        .closed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                self.spawn_background(F::close_connection_async(
                    Arc::clone(&self.connect),
                    peer_id,
                    resp,
                ))?;
            }
            Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp,
            } => {
                let Ok((worker, lease)) = self.doc_worker_handle(doc_id) else {
                    let _ = resp.send(Err(crate::runtime::SyncDocError::NotFound));
                    return Ok(());
                };
                let msg = DocWorkerMsg::SyncWithPeer {
                    peer_id,
                    waiter_id,
                    timeout,
                    done: resp,
                    _lease: lease,
                };
                if let Err(send_error) = worker.msg_tx.try_send(msg) {
                    let DocWorkerMsg::SyncWithPeer { done, .. } = send_error.into_inner() else {
                        unreachable!(
                            "worker returned a different message after SyncWithPeer send failure"
                        );
                    };
                    let _ = done.send(Err(crate::runtime::SyncDocError::IoError(ferr!(
                        "doc worker stopped before sync request"
                    ))));
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
                self.spawn_background(F::note_local_keyhive_changed(
                    Arc::clone(&self.runtime_io),
                    resp,
                ))?;
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
                let sedimentree_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_background(F::check_sedimentree(
                    Arc::clone(&self.runtime_io),
                    sedimentree_id,
                    resp,
                ))?;
            }
            Runtime2Cmd::InspectStoredDocBlobs { sed_id, resp } => {
                self.spawn_background(F::inspect_stored_doc_blobs(
                    Arc::clone(&self.runtime_io),
                    sed_id,
                    resp,
                ))?;
            }
            Runtime2Cmd::CheckDocWorkerExists { doc_id, resp } => {
                let exists = self.doc_workers.contains_key(&doc_id);
                let _ = resp.send(exists);
            }
            Runtime2Cmd::WaitForQuiescence { resp } => {
                self.request_quiescence(resp)?;
            }
        }
        self.try_resolve_quiescence()
    }
}

trait HubBackgroundFuture<F: FutureForm> {
    fn start_sync(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn refresh_cache(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn refresh_cache_and_notify(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerId,
        round_id: Option<u64>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn update_doc_access(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn release_lease(
        lease_rx: futures::channel::oneshot::Receiver<()>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>>;
}

/// Connection and doc-sync IO futures that need the concrete task set for
/// spawning end-futures. Defined as a separate trait so `#[future_form]`
/// can generate Sendable/Local implementations.
trait HubIoFutures<F: FutureForm, Tasks: crate::runtime2::TaskSet<F>> {
    fn open_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn accept_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        incoming: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn close_connection_async(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer_id: PeerId,
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn sync_doc_with_peer_and_notify(
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> HubBackgroundFuture<F> for F {
    fn start_sync(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            if let Err(error) = runtime_io.sync_keyhive_with_peer(peer_id, request_id.clone()).await {
                let error = format!("keyhive sync with {peer_id} failed: {error}");
                if evt_tx
                    .send(Runtime2Evt::KeyhiveSyncFailed {
                        peer_id,
                        request_id,
                        error,
                    })
                    .await
                    .is_err()
                {
                    tracing::debug!(%peer_id, "runtime stopped before keyhive sync failure event");
                }
            }
            Ok(())
        })
    }

    fn refresh_cache(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            runtime_io
                .refresh_keyhive_cache()
                .await
                .wrap_err("keyhive cache refresh failed")?;
            Ok(())
        })
    }

    fn refresh_cache_and_notify(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerId,
        round_id: Option<u64>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = runtime_io
                .refresh_keyhive_cache()
                .await
                .wrap_err("keyhive cache refresh failed");
            if evt_tx
                .send(Runtime2Evt::KeyhiveCacheRefreshDone {
                    peer_id,
                    round_id,
                    result,
                })
                .await
                .is_err()
            {
                tracing::debug!(%peer_id, "runtime stopped before keyhive cache completion");
            }
            Ok(())
        })
    }

    fn update_doc_access(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            runtime_io
                .update_doc_access(target)
                .await
                .wrap_err("big-sync document access update failed")
        })
    }

    fn release_lease(
        lease_rx: futures::channel::oneshot::Receiver<()>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let _ = lease_rx.await;
            if cmd_tx
                .send(Runtime2Cmd::ReleaseInternalLease { doc_id })
                .await
                .is_err()
            {
                tracing::debug!(?doc_id, "runtime stopped before internal lease release");
            }
            Ok(())
        })
    }
}

#[future_form::future_form(Sendable where Tasks: Send, Local)]
impl<F: FutureForm, Tasks: crate::runtime2::TaskSet<F>> HubIoFutures<F, Tasks> for F {
    fn open_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match connect.connect(peer, addr).await {
                Ok((handshake_peer, closed, end_fut)) => {
                    if handshake_peer != peer {
                        // The connector has already authenticated the peer;
                        // close that authenticated connection before rejecting
                        // the caller's expected-target mismatch.
                        connect.close(handshake_peer).await?;
                        let _ = resp.send(Err(ferr!(
                            "handshake peer mismatch: expected {peer}, got {handshake_peer}"
                        )));
                        return Ok(());
                    }
                    let watcher_closed = Arc::clone(&closed);
                    let watcher_peer = handshake_peer;
                    let watcher_evt_tx = evt_tx.clone();
                    let evt_tx_established = evt_tx.clone();
                    let closed_established = Arc::clone(&closed);
                    let watcher = child_tasks.spawn(F::from_future(async move {
                        let result = end_fut.await;
                        watcher_closed.store(true, std::sync::atomic::Ordering::SeqCst);
                        let error = result.as_ref().err().map(ToString::to_string);
                        if watcher_evt_tx
                            .send(Runtime2Evt::ConnLost {
                                peer_id: watcher_peer,
                                closed: Arc::clone(&watcher_closed),
                                error,
                            })
                            .await
                            .is_err()
                        {
                            tracing::debug!(
                                %watcher_peer,
                                "runtime stopped before connection-loss event"
                            );
                        }
                        Ok(())
                    }));
                    if let Err(error) = watcher {
                        connect.close(handshake_peer).await?;
                        let _ = resp.send(Err(error));
                        return Ok(());
                    }
                    if evt_tx_established
                        .send(Runtime2Evt::ConnEstablished {
                            peer_id: handshake_peer,
                            closed: closed_established,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!(%handshake_peer, "runtime stopped before connection-established event");
                        connect.close(handshake_peer).await?;
                        return Ok(());
                    }
                    let _ = resp.send(Ok((handshake_peer, closed)));
                }
                Err(error) => {
                    let _ = resp.send(Err(error));
                }
            }
            Ok(())
        })
    }

    fn accept_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        incoming: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match connect.accept(incoming).await {
                Ok((handshake_peer, closed, end_fut)) => {
                    let watcher_closed = Arc::clone(&closed);
                    let watcher_peer = handshake_peer;
                    let watcher_evt_tx = evt_tx.clone();
                    let evt_tx_established = evt_tx.clone();
                    let closed_established = Arc::clone(&closed);
                    let watcher = child_tasks.spawn(F::from_future(async move {
                        let result = end_fut.await;
                        watcher_closed.store(true, std::sync::atomic::Ordering::SeqCst);
                        let error = result.as_ref().err().map(ToString::to_string);
                        if watcher_evt_tx
                            .send(Runtime2Evt::ConnLost {
                                peer_id: watcher_peer,
                                closed: Arc::clone(&watcher_closed),
                                error,
                            })
                            .await
                            .is_err()
                        {
                            tracing::debug!(
                                %watcher_peer,
                                "runtime stopped before connection-loss event"
                            );
                        }
                        Ok(())
                    }));
                    if let Err(error) = watcher {
                        let _ = resp.send(Err(error));
                        return Ok(());
                    }
                    if evt_tx_established
                        .send(Runtime2Evt::ConnEstablished {
                            peer_id: handshake_peer,
                            closed: closed_established,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!(%handshake_peer, "runtime stopped before connection-established event");
                        connect.close(handshake_peer).await?;
                        return Ok(());
                    }
                    let _ = resp.send(Ok((handshake_peer, closed)));
                }
                Err(error) => {
                    let _ = resp.send(Err(error));
                }
            }
            Ok(())
        })
    }

    fn close_connection_async(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer_id: PeerId,
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = connect.close(peer_id).await;
            if let Some(resp) = resp {
                let _ = resp.send(result);
            }
            Ok(())
        })
    }

    fn sync_doc_with_peer_and_notify(
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = match runtime_io.sync_doc_with_peer(sed_id, peer_id).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(crate::runtime::SyncDocError::NotFound),
                Err(error) => Err(crate::runtime::SyncDocError::IoError(error)),
            };
            if evt_tx
                .send(Runtime2Evt::DocSyncCompleted {
                    doc_id,
                    peer_id,
                    waiter_id,
                    result,
                })
                .await
                .is_err()
            {
                tracing::debug!(?doc_id, %peer_id, "runtime stopped before document sync completion");
            }
            Ok(())
        })
    }
}

// ─── Shared helper: spawn owned background work on the child task set ────

impl<F: FutureForm + 'static, R: TaskRuntime<F>> Runtime2Hub<F, R> {
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

impl<
        F: FutureForm
            + HubCommandFuture<F>
            + HubBackgroundFuture<F>
            + HubIoFutures<F, R::Tasks>
            + crate::runtime2::doc_worker::DocWorkerLoop<F>,
        R: TaskRuntime<F>,
    > Runtime2Hub<F, R>
where
    F: 'static,
{
    /// Process a single [`Runtime2Evt`]. Mirrors `handle_evt` at
    /// `runtime.rs:1315`.
    pub(crate) fn handle_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        if !matches!(&evt, Runtime2Evt::DocWorkerQuiescent { .. }) {
            self.note_activity();
        }
        match evt {
            Runtime2Evt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session);
            }
            Runtime2Evt::ConnEstablished { peer_id, closed } => {
                self.handle_connection_established(peer_id, closed)?;
            }
            Runtime2Evt::ConnLost {
                peer_id,
                closed,
                error: _,
            } => {
                self.handle_connection_lost(peer_id, closed)?;
            }
            Runtime2Evt::KeyhiveSyncDone {
                peer_id,
                request_id,
            } => {
                self.finish_keyhive_sync(peer_id, request_id)?;
            }
            Runtime2Evt::KeyhiveSyncFailed {
                peer_id,
                request_id,
                error,
            } => {
                self.fail_keyhive_sync(peer_id, request_id, error)?;
            }
            Runtime2Evt::KeyhiveCacheRefreshDone {
                peer_id,
                round_id,
                result,
            } => {
                self.finish_keyhive_sync_after_cache(peer_id, round_id, result)?;
            }
            Runtime2Evt::DocSyncRequested {
                doc_id,
                peer_id,
                waiter_id,
            } => {
                let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_background(F::sync_doc_with_peer_and_notify(
                    Arc::clone(&self.runtime_io),
                    self.evt_tx.clone(),
                    doc_id,
                    peer_id,
                    waiter_id,
                    sed_id,
                ))?;
            }
            Runtime2Evt::DocSyncCompleted {
                doc_id,
                peer_id,
                waiter_id,
                result,
            } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::SyncWithPeerResult {
                        peer_id,
                        waiter_id,
                        result,
                    })
                    .expect("task was found dead");
            }
            Runtime2Evt::KeyhiveSyncRequested { peer_id } => {
                self.schedule_internal_keyhive_sync(peer_id);
            }
            Runtime2Evt::DocWorkerHandleAcquired { bundle } => {
                self.handle_doc_worker_handle_acquired(bundle);
            }
            Runtime2Evt::DocWorkerStopped { doc_id } => {
                self.doc_workers.remove(&doc_id);
                if let Some(probe) = self.quiescence_probe.as_mut() {
                    probe.pending_docs.remove(&doc_id);
                }
            }
            Runtime2Evt::DocWorkerQuiescent {
                doc_id,
                barrier_id,
            } => {
                self.handle_doc_worker_quiescent(doc_id, barrier_id)?;
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
                self.spawn_background(F::update_doc_access(
                    Arc::clone(&self.runtime_io),
                    target,
                ))?;
            }
            Runtime2Evt::RevocationReceived { target, data } => {
                self.change_manager
                    .notify_revocation_received(data)
                    .expect("task was found dead");
                self.spawn_background(F::update_doc_access(
                    Arc::clone(&self.runtime_io),
                    target,
                ))?;
            }
        }
        self.try_resolve_quiescence()
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

    /// Handle an established connection: register peer in `connected_peers`,
    /// then schedule the initial keyhive sync.
    /// Mirrors `handle_connection_established` at `runtime.rs:2087`.
    fn handle_connection_established(
        &mut self,
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        self.connected_peers.insert(
            peer_id,
            ConnDeets {
                closed: closed.clone(),
            },
        );
        self.start_keyhive_sync(peer_id)?;
        Ok(())
    }

    /// Handle a lost connection: clean up syncs and connected_peers.
    /// Mirrors `handle_connection_lost` at `runtime.rs:2037`.
    fn handle_connection_lost(
        &mut self,
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        let Some(current) = self.connected_peers.get(&peer_id) else {
            tracing::debug!(%peer_id, "ignoring connection loss for untracked connection");
            return Ok(());
        };
        if !Arc::ptr_eq(&current.closed, &closed) {
            tracing::debug!(%peer_id, "ignoring stale connection loss after reconnect");
            return Ok(());
        }
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
        self.keyhive_round_ids = self.keyhive_round_ids.wrapping_add(1);
        let round_id = self.keyhive_round_ids;
        let request_id = subduction_keyhive::message::RequestId {
            requestor: subduction_keyhive::KeyhivePeerId::from_bytes(
                *self.local_peer_id.as_bytes(),
            ),
            nonce: round_id,
        };
        self.active_keyhive_syncs.insert(
            peer_id,
            KeyhiveSyncRound {
                watermark,
                round_id,
                request_id: request_id.clone(),
                cache_refresh_started: false,
            },
        );
        self.spawn_background(F::start_sync(
            Arc::clone(&self.runtime_io),
            self.evt_tx.clone(),
            peer_id,
            request_id,
        ))?;
        Ok(())
    }

    /// Resolve a keyhive round whose initiation failed before the protocol
    /// could emit its normal completion event.
    fn fail_keyhive_sync(
        &mut self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
        error: String,
    ) -> eyre::Result<()> {
        let Some(round) = self.active_keyhive_syncs.get(&peer_id) else {
            tracing::debug!(%peer_id, ?request_id, "ignoring untracked keyhive sync failure");
            return Ok(());
        };
        if round.request_id != request_id {
            tracing::debug!(
                %peer_id,
                expected_request_id = ?round.request_id,
                request_id = ?request_id,
                "ignoring stale keyhive sync failure"
            );
            return Ok(());
        }

        self.active_keyhive_syncs
            .remove(&peer_id)
            .expect("active keyhive sync disappeared after failure validation");
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, sender) in waiters {
                sender
                    .send(Err(ferr!("{error}")))
                    .expect("keyhive sync waiter receiver must remain open");
            }
        }
        self.keyhive_dirty.remove(&peer_id);
        tracing::debug!(%peer_id, ?request_id, error, "keyhive sync initiation failed");
        Ok(())
    }

    /// Start the cache-refresh half of keyhive sync completion. Waiters stay
    /// pending until [`finish_keyhive_sync_after_cache`] observes that refresh.
    fn finish_keyhive_sync(
        &mut self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> eyre::Result<()> {
        let Some(round) = self.active_keyhive_syncs.get_mut(&peer_id) else {
            tracing::debug!(%peer_id, ?request_id, "refreshing untracked inbound keyhive completion");
            self.spawn_background(F::refresh_cache_and_notify(
                Arc::clone(&self.runtime_io),
                self.evt_tx.clone(),
                peer_id,
                None,
            ))?;
            return Ok(());
        };
        if round.request_id != request_id {
            tracing::debug!(
                %peer_id,
                expected_request_id = ?round.request_id,
                request_id = ?request_id,
                "processing inbound keyhive exchange without resolving waiter"
            );
            self.spawn_background(F::refresh_cache_and_notify(
                Arc::clone(&self.runtime_io),
                self.evt_tx.clone(),
                peer_id,
                None,
            ))?;
            return Ok(());
        }
        if round.cache_refresh_started {
            tracing::debug!(%peer_id, round_id = round.round_id, "ignoring duplicate keyhive sync completion");
            return Ok(());
        }
        round.cache_refresh_started = true;
        let round_id = round.round_id;
        self.spawn_background(F::refresh_cache_and_notify(
            Arc::clone(&self.runtime_io),
            self.evt_tx.clone(),
            peer_id,
            Some(round_id),
        ))?;
        Ok(())
    }

    /// Resolve keyhive waiters only after protocol sync *and* local cache
    /// refresh have completed. This is the synchronous public barrier.
    fn finish_keyhive_sync_after_cache(
        &mut self,
        peer_id: PeerId,
        round_id: Option<u64>,
        result: eyre::Result<()>,
    ) -> eyre::Result<()> {
        let Some(round_id) = round_id else {
            if let Err(error) = result {
                tracing::warn!(%peer_id, error = %error, "inbound keyhive cache refresh failed");
            } else {
                self.reattempt_pending_materialization();
            }
            return Ok(());
        };
        let Some(round) = self.active_keyhive_syncs.get(&peer_id) else {
            tracing::debug!(%peer_id, round_id, "ignoring untracked keyhive cache completion");
            return Ok(());
        };
        if round.round_id != round_id {
            tracing::debug!(
                %peer_id,
                expected_round_id = round.round_id,
                round_id,
                "ignoring stale keyhive cache completion"
            );
            return Ok(());
        }
        let round = self
            .active_keyhive_syncs
            .remove(&peer_id)
            .expect("active keyhive sync disappeared after round validation");
        let watermark = round.watermark;
        if let Err(error) = result {
            if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
                for (_, sender) in waiters {
                    sender
                        .send(Err(ferr!("keyhive cache refresh failed: {error}")))
                        .expect("keyhive sync waiter receiver must remain open");
                }
            }
            self.keyhive_dirty.remove(&peer_id);
            return Ok(());
        }

        // Split waiters: those that existed before this sync started resolve,
        // those that arrived during/after cascade into a new round.
        if let Some(waiters) = self.pending_keyhive_syncs.get_mut(&peer_id) {
            let mut remaining = Vec::new();
            for (id, sender) in std::mem::take(waiters) {
                if id < watermark {
                    sender.send(Ok(())).expect("keyhive sync waiter receiver must remain open");
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
        let has_remaining = self.pending_keyhive_syncs.contains_key(&peer_id);
        if has_remaining || self.keyhive_dirty.remove(&peer_id) {
            self.start_keyhive_sync(peer_id)?;
        }
        self.reattempt_pending_materialization();
        Ok(())
    }

    fn reattempt_pending_materialization(&mut self) {
        for doc_id in self.pending_materialization.clone() {
            if let Ok((worker, _lease)) = self.doc_worker_handle(doc_id) {
                worker
                    .send(DocWorkerMsg::ReattemptMaterialization)
                    .expect("pending doc worker must remain open");
            } else {
                tracing::warn!(
                    %doc_id,
                    "failed to get doc worker for reattempt on keyhive sync done"
                );
            }
        }
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
        let (removed, became_empty) =
            if let Some(waiters) = self.pending_keyhive_syncs.get_mut(peer_id) {
                let len_before = waiters.len();
                waiters.retain(|(id, _)| *id != waiter_id);
                (waiters.len() < len_before, waiters.is_empty())
            } else {
                return false;
            };
        if became_empty {
            self.pending_keyhive_syncs.remove(peer_id);
        }
        removed
    }

    /// Cancel all pending keyhive syncs for a peer.
    fn cancel_pending_keyhive_syncs(&mut self, peer_id: &PeerId, reason: &'static str) {
        self.active_keyhive_syncs.remove(peer_id);
        self.keyhive_dirty.remove(peer_id);
        if let Some(waiters) = self.pending_keyhive_syncs.remove(peer_id) {
            for (_id, sender) in waiters {
                let _ = sender.send(Err(eyre::eyre!("{}", reason)));
            }
        }
    }

    /// Cancel all pending doc syncs for a peer (fan-out to all doc-workers).
    fn cancel_pending_doc_syncs(
        &mut self,
        peer_id: &PeerId,
        reason: &'static str,
    ) -> eyre::Result<()> {
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

}

// ═══════════════════════════════════════════════════════════════════════════
// DOC-WORKER LIFECYCLE
// ═══════════════════════════════════════════════════════════════════════════

impl<
        F: FutureForm
            + HubBackgroundFuture<F>
            + crate::runtime2::doc_worker::DocWorkerLoop<F>
            + 'static,
        R: TaskRuntime<F>,
    > Runtime2Hub<F, R>
{
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
        let (lease_tx, lease_rx) = futures::channel::oneshot::channel::<()>();
        let cmd_tx = self.cmd_tx.clone();
        self.spawn_background(F::release_lease(lease_rx, cmd_tx, doc_id))?;

        let lease = DocWorkerInternalLease::new(doc_id, lease_tx);
        Ok((handle, lease))
    }

    /// Lazily spawn a doc-worker if none exists. Mirrors `spawn_doc_worker` at
    /// `runtime.rs:1514`.
    #[tracing::instrument(skip_all, fields(%doc_id))]
    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        // Fast path: already alive, just reset eviction.
        if self
            .doc_workers
            .get(&doc_id)
            .is_some_and(|entry| !entry.handle.msg_tx.is_closed())
        {
            if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                entry.eviction_deadline = None;
            }
            return Ok(());
        }
        // Stale entry: remove before re-creating.
        self.doc_workers.remove(&doc_id);

        let (handle, stop, _abort) = crate::runtime2::spawn_doc_worker(
            doc_id,
            Arc::clone(&self.doc_io),
            Arc::clone(&self.change_manager),
            self.sync_policy,
            Arc::clone(&self.clock),
            &self.child_tasks,
            self.evt_tx.clone(),
        )?;

        self.doc_workers.insert(
            doc_id,
            DocWorkerEntry {
                handle,
                stop,
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
        entry.eviction_deadline = Some(self.clock.instant() + self.sync_policy.doc_worker_idle_ttl);
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
/// - `child_tasks` — construction-time workers and dynamic background jobs.
/// - `machine_tasks` — the hub's dispatcher loop, stopped first so it cannot
///    dispatch work into an aborted child scope.
///
/// Mirrors `BigRepoRuntimeStopToken` at `runtime.rs:613`.
pub struct Runtime2StopToken<F: FutureForm, R: TaskRuntime<F>> {
    pub(crate) cancel: futures::future::AbortHandle,
    pub(crate) child_tasks: R::Tasks,
    pub(crate) machine_tasks: R::Tasks,
}

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2StopToken<F, R> {
    /// Cancel the runtime and await graceful shutdown.
    pub async fn stop(self, timeout: std::time::Duration) -> eyre::Result<()> {
        // Stop the dispatcher before aborting its children. Otherwise a late
        // connection-loss event can be consumed by the still-running hub and
        // try to spawn work into the already-aborted child task set.
        self.cancel.abort();
        self.machine_tasks.stop(timeout).await?;

        // Not every child operation observes the runtime cancellation token
        // (for example a peer sync may be awaiting transport IO). Abort the
        // child scope only after the dispatcher has stopped, then join it.
        self.child_tasks.abort();
        self.child_tasks.stop(timeout).await?;
        Ok(())
    }
}

trait HubMachineFuture<F: FutureForm + FutureForm, R: TaskRuntime<F>> {
    fn machine_loop(
        hub: Runtime2Hub<F, R>,
        cmd_rx: async_channel::Receiver<Runtime2Cmd>,
        evt_rx: async_channel::Receiver<Runtime2Evt>,
        timer: Arc<dyn crate::runtime2::Timer<F>>,
        registration: futures::future::AbortRegistration,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable where R::Tasks: Send, Local)]
impl<
        F: FutureForm + HubCommandFuture<F> + HubBackgroundFuture<F> + HubIoFutures<F, R::Tasks>,
        R: TaskRuntime<F>,
    > HubMachineFuture<F, R> for F
{
    fn machine_loop(
        mut hub: Runtime2Hub<F, R>,
        cmd_rx: async_channel::Receiver<Runtime2Cmd>,
        evt_rx: async_channel::Receiver<Runtime2Evt>,
        timer: Arc<dyn crate::runtime2::Timer<F>>,
        registration: futures::future::AbortRegistration,
    ) -> F::Future<'static, eyre::Result<()>> {
        use futures::FutureExt;
        F::from_future(async move {
            let result = futures::future::Abortable::new(
                async move {
                    loop {
                        let mut sleep =
                            Box::pin(timer.sleep(std::time::Duration::from_millis(500)).fuse());
                        let mut cmd = Box::pin(cmd_rx.recv().fuse());
                        let mut evt = Box::pin(evt_rx.recv().fuse());
                        futures::select_biased! {
                            _ = sleep.as_mut() => hub.janitor_tick(),
                            cmd = cmd.as_mut() => match cmd {
                                Ok(cmd) => hub.handle_cmd(cmd)?,
                                Err(_) => break,
                            },
                            evt = evt.as_mut() => match evt {
                                Ok(evt) => hub.handle_evt(evt)?,
                                Err(_) => break,
                            },
                        }
                    }
                    eyre::Ok(())
                },
                registration,
            )
            .await;
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    tracing::error!(error = %error, "runtime2 hub machine failed");
                    Err(error)
                }
                Err(_) => Ok(()),
            }
        })
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
/// the concrete task runtime. Two independent [`TaskSet`]s are created
/// from the runtime:
/// - `child_tasks`: all construction-time workers, dynamic background jobs,
///   and doc-workers.
/// - `machine_tasks`: the hub's machine loop. Stopped first on shutdown so
///   it cannot dispatch into an aborted child scope.
///
/// Determinism: the task runtime is injected via `config.tasks`, so a
/// step-task-runtime implementation can drive tests deterministically.
pub fn spawn_runtime2<F, R>(
    config: Runtime2Config<F, R>,
) -> eyre::Result<(Runtime2Handle<F>, Runtime2StopToken<F, R>)>
where
    F: FutureForm
        + HubCommandFuture<F>
        + HubBackgroundFuture<F>
        + HubIoFutures<F, R::Tasks>
        + HubMachineFuture<F, R>
        + crate::runtime2::doc_worker::DocWorkerLoop<F>
        + 'static,
    R: TaskRuntime<F>,
{
    let Runtime2Config {
        local_peer_id,
        runtime_io,
        doc_io,
        sync_policy,
        change_manager,
        tasks,
        timer,
        clock,
        connect,
        event_channel,
    } = config;

    // Create two independent task sets for reverse-order shutdown.
    let child_tasks = tasks.task_set();
    let machine_tasks = tasks.task_set();

    let (runtime_abort, runtime_registration) = futures::future::AbortHandle::new_pair();

    let (cmd_tx, cmd_rx) = async_channel::unbounded::<Runtime2Cmd>();
    let (evt_tx, evt_rx) = event_channel.unwrap_or_else(async_channel::unbounded::<Runtime2Evt>);

    // The hub and its public handle must share waiter counters. The hub uses
    // the current counter as a sync watermark; separate counters would leave
    // every request below that watermark and make it wait forever.
    let doc_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let keyhive_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));

    // ── Build the hub ──────────────────────────────────────────────────────
    let mut hub: Runtime2Hub<F, R> = Runtime2Hub {
        local_peer_id,
        sync_policy,
        runtime_io: Arc::clone(&runtime_io),
        connect,
        doc_io,
        change_manager,
        child_tasks: child_tasks.clone(),
        timer: timer.clone(),
        clock: clock.clone(),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: HashMap::new(),
        pending_keyhive_syncs: HashMap::new(),
        active_keyhive_syncs: HashMap::new(),
        keyhive_round_ids: 0,
        keyhive_dirty: BTreeSet::new(),
        quiescence_waiters: Vec::new(),
        quiescence_probe: None,
        quiescence_barrier_ids: 0,
        activity_generation: 0,
        doc_workers: HashMap::new(),
        pending_materialization: HashSet::new(),
        doc_sync_waiter_ids: Arc::clone(&doc_sync_waiter_ids),
        keyhive_sync_waiter_ids: Arc::clone(&keyhive_sync_waiter_ids),
    };

    // ── Construct handle ───────────────────────────────────────────────────
    let handle = Runtime2Handle::<F>::new(
        cmd_tx.clone(),
        hub.sync_policy,
        hub.timer.clone(),
        doc_sync_waiter_ids,
        keyhive_sync_waiter_ids,
    );

    // ── Spawn (machine): the hub machine loop ──────────────────────────────
    // The dispatcher is stopped before child work so it cannot accept late
    // events that would spawn into an aborted child task set.
    machine_tasks.spawn(F::machine_loop(
        hub,
        cmd_rx,
        evt_rx,
        timer,
        runtime_registration,
    ))?;

    Ok((
        handle,
        Runtime2StopToken {
            cancel: runtime_abort,
            child_tasks,
            machine_tasks,
        },
    ))
}
