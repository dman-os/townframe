//! `Runtime2Hub` — the runtime actor ("the machine loop").

use crate::interlude::*;

use crate::DocumentId;
use crate::runtime2::doc_worker::DocWorkerLoop;
use crate::runtime2::{
    DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, Runtime2Config, Runtime2Handle,
    TaskRuntime, TaskSet,
    messages::{DocWorkerMsg, Runtime2Cmd, Runtime2Evt},
};
use big_sync_core::PeerId;
use future_form::{FutureForm, Local, Sendable};
use std::collections::{HashMap, HashSet};
use tracing::Instrument;
// Re-export the ephemeral so embedders can subscribe.

pub(crate) struct Runtime2Hub<F: FutureForm, R: TaskRuntime<F>> {
    // ── identity / config ──────────────────────────────────────────────────
    local_peer_id: PeerId,
    sync_policy: crate::runtime2::types::BigRepoSyncPolicy,

    // ── injected IO facades ────────────────────────────────────────────────
    runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
    /// Transport-agnostic connection factory. Cloned for each spawn so
    /// background connect/accept/close tasks can drive IO without holding
    /// a borrow on the hub.
    connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
    /// Shared IO facade passed to every document worker.
    doc_io: Arc<dyn crate::runtime2::DocIo<F>>,

    // ── change manager ─────────────────────────────────────────────────────
    change_manager: Arc<crate::changes::ChangeListenerManager>,

    // ── task sets ──────────────────────────────────────────────────────────
    /// Child/background tasks (construction-time, keyhive syncs, lease
    /// waiters, doc-workers). Stopped first on shutdown (children before
    /// the hub loop).
    child_tasks: R::Tasks,

    // ── determinism levers ─────────────────────────────────────────────────
    timer: Arc<dyn crate::runtime2::Timer<F>>,
    clock: Arc<dyn crate::runtime2::Clock>,

    // ── channels ───────────────────────────────────────────────────────────
    cmd_tx: async_channel::Sender<Runtime2Cmd>,
    evt_tx: async_channel::Sender<Runtime2Evt>,

    // ── connection state ───────────────────────────────────────────────────
    connected_peers: HashMap<PeerId, ConnDeets>,

    // ── keyhive sync bookkeeping ───────────────────────────────────────────
    /// Keyhive sync waiters per peer. Each round snapshots the waiter ids it
    /// owns (`KeyhiveSyncRound::admitted_ids`); waiters admitted during the
    /// round stay queued and cascade to the next round. `ids` mirrors the vec
    /// for O(1) cancellation (dead waiters never trigger a follow-up round).
    keyhive_waiters: HashMap<PeerId, KeyhiveWaiters>,
    active_keyhive_syncs: HashMap<PeerId, KeyhiveSyncRound>,
    /// A `KeyhiveChangeNotif` that arrived while a round for the peer was
    /// already active. The in-flight exchange may have synced stale state;
    /// when the round completes, a follow-up round is started for the peer.
    keyhive_notif_pending: HashSet<PeerId>,
    keyhive_round_ids: u64,
    keyhive_reconciliation_waiters: Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>,
    /// Highest admission-log seq the group-part projection has settled
    /// (from worker announcements).
    group_part_settled_seq: u64,
    /// Highest admission-log seq known incorporated (from the admission
    /// writer). `WaitForKeyhiveReconciliation` captures this and resolves
    /// once `group_part_settled_seq` covers it.
    admitted_head: u64,
    /// Notifier for Keyhive event log changes.
    keyhive_event_notify: Arc<tokio::sync::Notify>,

    // ── doc sync bookkeeping ───────────────────────────────────────────────
    /// Waiters for caller-initiated doc sync rounds, keyed by waiter id.
    ///
    /// The waiter id doubles as the `RequestId` nonce (the requestor is the
    /// local peer id), so the round-completion commands (`DocSyncRoundDone` /
    /// `DocSyncFailed`) resolve exactly the right waiter — no single-flight
    /// needed for concurrent syncs of the same (doc, peer).
    pending_doc_syncs: HashMap<u64, PendingDocSyncWaiter>,
    // ── runtime-wide quiescence ────────────────────────────────────────────
    quiescence_waiters: Vec<futures::channel::oneshot::Sender<eyre::Result<()>>>,
    quiescence_probe: Option<QuiescenceProbe>,
    quiescence_barrier_ids: u64,
    /// Freeze the hub (B12): events are held, the janitor pauses, and all
    /// commands except `Unfreeze` are buffered until unfreeze.
    frozen: bool,
    /// Commands buffered while frozen, replayed FIFO on unfreeze.
    frozen_cmd_buffer: Vec<Runtime2Cmd>,
    /// A resolving `WaitForQuiescence { freeze: true }` freezes the hub.
    freeze_on_resolve: bool,
    /// Set once the commands channel closes (the stop token dropped its
    /// sender); no new background work is admitted and the machine loop
    /// exits once tracked work drains.
    cmd_closed: bool,
    activity_generation: u64,
    /// Finite background futures admitted via `spawn_tracked` that have not
    /// yet completed. The quiescence predicate waits for this to drain, so
    /// in-flight work started before a probe cannot resolve it early (A2/A5).
    tracked_in_flight: u64,
    // ── doc-worker registry ────────────────────────────────────────────────
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
    pending_materialization: HashSet<DocumentId>,
    /// Documents with a materialization retry in flight, mapped to the Keyhive
    /// state generation at retry start. A `Pending` completion whose walk ran
    /// against a generation older than the current one is re-verified (B6).
    materialization_retries_in_flight: HashMap<DocumentId, u64>,
    next_doc_worker_generation: u64,
}

struct ConnDeets {
    closed: Arc<std::sync::atomic::AtomicBool>,
}

struct KeyhiveSyncRound {
    round_id: u64,
    request_id: subduction_keyhive::message::RequestId,
    /// Ids of the waiters queued when this round started; the round resolves
    /// them on completion. Waiters admitted during the round cascade.
    admitted_ids: std::collections::HashSet<u64>,
}

/// Keyhive sync waiters for one peer: those owned by the active round plus
/// those cascading to the next. `ids` mirrors `waiters` for O(1) cancellation.
#[derive(Default)]
struct KeyhiveWaiters {
    waiters: Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>,
    ids: std::collections::HashSet<u64>,
}

struct PendingDocSyncWaiter {
    doc_id: DocumentId,
    peer_id: PeerId,
    resp: futures::channel::oneshot::Sender<
        Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
    >,
}

struct QuiescenceProbe {
    barrier_id: u64,
    activity_generation: u64,
    pending_docs: HashSet<DocumentId>,
    group_part_settled_seq: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMAND HANDLERS
// ═══════════════════════════════════════════════════════════════════════════

pub(crate) trait HubCommandFuture<F: FutureForm> {
    fn create_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn contains_sedimentree(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    ) -> F::Future<'static, eyre::Result<()>>;
    fn has_local_doc_state(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        doc_id: DocumentId,
        has_doc_worker: bool,
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn inspect_stored_doc_blobs(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<Vec<Vec<u8>>>>,
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
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match runtime_io.create_document(parents, content_heads).await {
                Ok(doc_id) => {
                    match cmd_tx
                        .send(Runtime2Cmd::PutDoc {
                            doc_id,
                            initial_content,
                            resp,
                        })
                        .await
                    {
                        Ok(()) => {}
                        Err(_) => {
                            // Shutdown: the hub's machine loop is gone. The
                            // caller's `resp` is dropped with the hub, so a
                            // send error is the signal — nothing to report.
                        }
                    }
                }
                Err(err) => {
                    resp.send(Err(err))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
            }
            Ok(())
        })
    }

    fn contains_sedimentree(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        sed_id: sedimentree_core::id::SedimentreeId,
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let stored = runtime_io.contains_sedimentree(sed_id).await;
            resp.send(stored)
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            Ok(())
        })
    }

    fn has_local_doc_state(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        doc_id: DocumentId,
        has_doc_worker: bool,
        resp: futures::channel::oneshot::Sender<eyre::Result<bool>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = if has_doc_worker {
                Ok(true)
            } else {
                runtime_io
                    .contains_sedimentree(sedimentree_core::id::SedimentreeId::new(
                        doc_id.into_bytes(),
                    ))
                    .await
            };
            resp.send(result)
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
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
            resp.send(result)
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            Ok(())
        })
    }
}

impl<
    F: FutureForm
        + HubCommandFuture<F>
        + HubBackgroundFuture<F>
        + HubIoFutures<F, R::Tasks>
        + DocWorkerLoop<F>,
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
        freeze: bool,
    ) -> eyre::Result<()> {
        self.freeze_on_resolve |= freeze;
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
        let group_part_settled_seq = self.group_part_settled_seq;
        debug!(
            barrier_id,
            local_peer_id = %self.local_peer_id,
            activity_generation = generation,
            doc_workers = doc_ids.len(),
            pending_materialization = self.pending_materialization.len(),
            group_part_settled_seq,
            "runtime2 quiescence probe started",
        );
        self.quiescence_probe = Some(QuiescenceProbe {
            barrier_id,
            activity_generation: generation,
            pending_docs: doc_ids.iter().copied().collect(),
            group_part_settled_seq,
        });
        for doc_id in doc_ids {
            let (worker, lease) = self.doc_worker_handle(doc_id)?;
            let (fence_reply, reply_rx) = futures::channel::oneshot::channel();
            worker
                .send(DocWorkerMsg::Fence {
                    reply: fence_reply,
                    _lease: lease,
                })
                .wrap_err(ERROR_CHANNEL)?;
            self.spawn_tracked(
                crate::runtime2::TrackedWorkKind::WorkerFence,
                F::await_worker_fence(barrier_id, doc_id, reply_rx, self.evt_tx.clone()),
            )?;
        }
        Ok(())
    }

    fn handle_doc_worker_fenced(
        &mut self,
        doc_id: DocumentId,
        barrier_id: u64,
    ) -> eyre::Result<()> {
        if let Some(probe) = self.quiescence_probe.as_mut()
            && probe.barrier_id == barrier_id
        {
            probe.pending_docs.remove(&doc_id);
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
            || self.tracked_in_flight > 0
            || !self.active_keyhive_syncs.is_empty()
            || !self.keyhive_waiters.is_empty()
            || self.group_part_settled_seq < probe.group_part_settled_seq
        {
            return Ok(());
        }
        debug!(
            local_peer_id = %self.local_peer_id,
            barrier_id = probe.barrier_id,
            activity_generation = probe.activity_generation,
            "runtime2 quiescence probe resolved",
        );
        let barrier_id = probe.barrier_id;
        self.quiescence_probe = None;
        if self.freeze_on_resolve {
            self.freeze_on_resolve = false;
            self.frozen = true;
            debug!(
                local_peer_id = %self.local_peer_id,
                barrier_id,
                "runtime2 quiescence probe resolved; hub frozen until unfreeze"
            );
        }
        for waiter in std::mem::take(&mut self.quiescence_waiters) {
            // A caller timeout drops the receiver; that cancellation is not
            // a runtime failure and must not crash the hub while resolving
            // other waiters.
            waiter
                .send(Ok(()))
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn handle_cmd(&mut self, cmd: Runtime2Cmd) -> eyre::Result<()> {
        trace!(?cmd, "runtime2 command received");
        if !matches!(
            &cmd,
            Runtime2Cmd::WaitForQuiescence { .. }
                | Runtime2Cmd::Unfreeze
                | Runtime2Cmd::RegisterDocLease { .. }
                | Runtime2Cmd::ReleaseDocLease { .. }
                | Runtime2Cmd::ReleaseInternalLease { .. }
                | Runtime2Cmd::EnsureCausalCoverage { .. }
        ) {
            self.note_activity();
        }
        match cmd {
            Runtime2Cmd::CreateDoc {
                initial_content,
                parents,
                content_heads,
                resp,
            } => {
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::CreateDoc,
                    F::create_doc(
                        Arc::clone(&self.runtime_io),
                        self.cmd_tx.clone(),
                        initial_content,
                        parents,
                        content_heads,
                        resp,
                    ),
                )?;
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
                        _lease,
                    })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::GetDocHandle { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::AcquireHandle { resp, _lease })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::CommitDelta {
                doc_id,
                bundle_id,
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
                        bundle_id,
                        commits,
                        heads,
                        patches,
                        origin,
                        resp,
                        _lease,
                    })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::DocHeadState { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::QueryHeadState { resp, _lease })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::EnsureCausalCoverage { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::ReconcileCausalCoverage { resp, _lease })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::InspectDocHeadState { doc_id, resp } => {
                if let Ok(Some((worker, _lease))) = self.acquire_existing_doc_worker_handle(doc_id)
                {
                    if let Err(err) = worker.send(DocWorkerMsg::InspectHeadState { resp, _lease }) {
                        debug!(%doc_id, ?err, "failed sending InspectHeadState to worker");
                    }
                } else {
                    resp.send(Ok(None))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
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
            Runtime2Cmd::CloseConn {
                peer_id,
                closed,
                resp,
            } => {
                // The connection is being closed by its consumer: mark it
                // dead up front so syncs through its handle fail fast. Only
                // the peer's *current* connection owns the peer's
                // registration — a superseded connection's close must not
                // disturb the replacement (the closed flag's pointer
                // identity is the connection id, matching
                // `handle_connection_lost`'s stale-conn gate).
                closed.store(true, std::sync::atomic::Ordering::SeqCst);
                let is_current = matches!(
                    self.connected_peers.get(&peer_id),
                    // FIXME: use a wrapper type on the atomic bool if we're going
                    // to use it like this and use internal uuids afterwards
                    Some(deets) if std::sync::Arc::ptr_eq(&deets.closed, &closed)
                );
                if is_current {
                    self.cancel_pending_keyhive_syncs(&peer_id, "keyhive peer closed");
                    self.connected_peers.remove(&peer_id);
                } else {
                    debug!(
                        peer_id = %peer_id,
                        "closing superseded connection; current registration left intact"
                    );
                }
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::CloseConn,
                    F::close_connection_async(
                        Arc::clone(&self.connect),
                        peer_id,
                        closed,
                        self.evt_tx.clone(),
                        resp,
                    ),
                )?;
            }
            Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                resp,
            } => {
                let request_id = subduction_core::connection::message::RequestId {
                    requestor: subduction_core::peer::id::PeerId::new(
                        *self.local_peer_id.as_bytes(),
                    ),
                    nonce: waiter_id,
                };
                self.pending_doc_syncs.insert(
                    waiter_id,
                    PendingDocSyncWaiter {
                        doc_id,
                        peer_id,
                        resp,
                    },
                );
                let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::SyncDoc,
                    F::sync_doc_with_peer(
                        request_id,
                        Arc::clone(&self.runtime_io),
                        peer_id,
                        sed_id,
                        self.cmd_tx.clone(),
                    ),
                )?;
            }
            Runtime2Cmd::DocSyncRoundDone { request_id } => {
                // Transport round succeeded: resolve the waiter (if still
                // pending) with a worker reconsider so the receipt reflects
                // the fully-persisted tree.
                let Some(waiter) = self.pending_doc_syncs.remove(&request_id.nonce) else {
                    return self.try_resolve_quiescence();
                };
                debug!(
                    request_nonce = request_id.nonce,
                    doc_id = %waiter.doc_id,
                    remote_peer_id = %waiter.peer_id,
                    "doc sync round resolved; resolving waiter",
                );
                self.route_sync_session_apply(
                    waiter.doc_id,
                    waiter.peer_id,
                    Vec::new(),
                    Vec::new(),
                    Some(waiter.resp),
                )?;
            }
            Runtime2Cmd::DocSyncFailed { request_id, error } => {
                let Some(waiter) = self.pending_doc_syncs.remove(&request_id.nonce) else {
                    return self.try_resolve_quiescence();
                };
                debug!(
                    ?error,
                    doc_id = %waiter.doc_id,
                    remote_peer_id = %waiter.peer_id,
                    "doc sync round failed; failing waiter",
                );
                waiter
                    .resp
                    .send(Err(error))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            } => {
                let entry = self.keyhive_waiters.entry(peer_id).or_default();
                entry.ids.insert(waiter_id);
                entry.waiters.push((waiter_id, resp));
                // A reconnect can expose the public connection handle before
                // the hub has processed its ConnEstablished event. Do not
                // initiate against the old/missing Keyhive peer in that gap;
                // the establishment handler will start the round once the
                // transport is registered.
                if self.connected_peers.contains_key(&peer_id)
                    && !self.active_keyhive_syncs.contains_key(&peer_id)
                {
                    self.start_keyhive_sync(peer_id)?;
                }
            }
            Runtime2Cmd::WaitForKeyhiveReconciliation { resp } => {
                let captured = self.admitted_head;
                if self.group_part_settled_seq >= captured {
                    resp.send(Ok(()))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                } else {
                    self.keyhive_reconciliation_waiters.push((captured, resp));
                }
            }
            Runtime2Cmd::CancelDocSyncWaiter { waiter_id, .. } => {
                // The caller's timeout dropped the response receiver; forget
                // the waiter so a late session / round-done cannot resolve it.
                self.pending_doc_syncs.remove(&waiter_id);
            }
            Runtime2Cmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id } => {
                self.cancel_pending_keyhive_sync(&peer_id, waiter_id);
            }
            Runtime2Cmd::RegisterDocLease { doc_id, registered } => {
                if !self.doc_workers.contains_key(&doc_id) {
                    self.spawn_doc_worker(doc_id)?;
                }
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.local_handles += 1;
                    entry.eviction_deadline = None;
                    registered
                        .send(())
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
            }
            Runtime2Cmd::ReleaseDocLease { doc_id, generation } => {
                self.handle_release_doc_lease(doc_id, generation);
            }
            Runtime2Cmd::ReleaseInternalLease { doc_id, generation } => {
                self.handle_release_internal_lease(doc_id, generation);
            }
            Runtime2Cmd::ContainsSedimentree { doc_id, resp } => {
                let sedimentree_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::ContainsSedimentree,
                    F::contains_sedimentree(Arc::clone(&self.runtime_io), sedimentree_id, resp),
                )?;
            }
            Runtime2Cmd::HasLocalDocState { doc_id, resp } => {
                let has_doc_worker = self.doc_workers.contains_key(&doc_id);
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::HasLocalDocState,
                    F::has_local_doc_state(
                        Arc::clone(&self.runtime_io),
                        doc_id,
                        has_doc_worker,
                        resp,
                    ),
                )?;
            }
            #[cfg(test)]
            Runtime2Cmd::HasDocWorker { doc_id, resp } => {
                resp.send(Ok(self.doc_workers.contains_key(&doc_id)))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            Runtime2Cmd::InspectStoredDocBlobs { sed_id, resp } => {
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::InspectStoredDocBlobs,
                    F::inspect_stored_doc_blobs(Arc::clone(&self.runtime_io), sed_id, resp),
                )?;
            }
            Runtime2Cmd::WaitForQuiescence { freeze, resp } => {
                self.request_quiescence(resp, freeze)?;
            }
            Runtime2Cmd::Unfreeze => {
                debug!(local_peer_id = %self.local_peer_id, "unfreeze: hub not frozen");
            }
        }
        self.try_resolve_quiescence()
    }
}

pub(crate) trait HubBackgroundFuture<F: FutureForm> {
    fn start_sync(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn emit_membership_change(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        change_manager: Arc<crate::changes::ChangeListenerManager>,
        target: keyhive_core::principal::identifier::Identifier,
        member_id: PeerId,
        access: crate::changes::BigRepoAccess,
        removed: bool,
        member_is_document: bool,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn forward_materialization_retry(
        result: futures::channel::oneshot::Receiver<
            Result<crate::runtime2::MaterializationStatus, String>,
        >,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>>;
    fn release_lease(
        lease_rx: futures::channel::oneshot::Receiver<()>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: DocumentId,
        generation: u64,
    ) -> F::Future<'static, eyre::Result<()>>;
    /// Await a doc-worker's fence reply and forward it as a `DocWorkerFenced`
    /// event so the hub can clear the probe's pending-doc set.
    fn await_worker_fence(
        barrier_id: u64,
        doc_id: DocumentId,
        reply: futures::channel::oneshot::Receiver<()>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>>;
    /// Wrap a finite background future so a `TrackedWorkDone` event is emitted
    /// after its own emissions (keeps channel order for the in-flight counter).
    fn track_work(
        kind: crate::runtime2::TrackedWorkKind,
        fut: F::Future<'static, eyre::Result<()>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>>;
}

/// Connection and doc-sync IO futures that need the concrete task set for
/// spawning end-futures. Defined as a separate trait so `#[future_form]`
/// can generate Sendable/Local implementations.
pub(crate) trait HubIoFutures<F: FutureForm, Tasks: crate::runtime2::TaskSet<F>> {
    #[expect(clippy::type_complexity)]
    fn open_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerId,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    std::sync::Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    #[expect(clippy::type_complexity)]
    fn accept_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        incoming: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerId,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    std::sync::Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn close_connection_async(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer_id: PeerId,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn sync_doc_with_peer(
        request_id: subduction_core::connection::message::RequestId,
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
        peer_id: PeerId,
        sed_id: sedimentree_core::id::SedimentreeId,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
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
        let span = tracing::debug_span!(
            "keyhive_sync_round",
            remote_peer_id = %peer_id,
            request_nonce = request_id.nonce,
        );
        F::from_future(
            async move {
                match runtime_io
                    .sync_keyhive_with_peer(peer_id, request_id.clone())
                    .await
                {
                    Ok(crate::runtime2::KeyhiveSyncOutcome::Initiated) => {}
                    Ok(crate::runtime2::KeyhiveSyncOutcome::PeerDisappeared) => {
                        evt_tx
                            .send(Runtime2Evt::KeyhiveSyncFailed {
                                peer_id,
                                request_id,
                                error: format!(
                                    "keyhive peer {peer_id} disappeared before sync could start"
                                ),
                            })
                            .await
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                    }
                    Err(error) => {
                        let error = format!("keyhive sync with {peer_id} failed: {error}");
                        evt_tx
                            .send(Runtime2Evt::KeyhiveSyncFailed {
                                peer_id,
                                request_id,
                                error,
                            })
                            .await
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                    }
                }
                Ok(())
            }
            .instrument(span),
        )
    }

    fn emit_membership_change(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        change_manager: Arc<crate::changes::ChangeListenerManager>,
        target: keyhive_core::principal::identifier::Identifier,
        member_id: PeerId,
        access: crate::changes::BigRepoAccess,
        removed: bool,
        member_is_document: bool,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            if runtime_io.is_document_membership_target(target).await? {
                if removed {
                    change_manager.notify_document_access_revoked(
                        DocumentId::new(target.to_bytes()),
                        member_id,
                    )?;
                } else {
                    change_manager.notify_document_access_changed(
                        DocumentId::new(target.to_bytes()),
                        member_id,
                        access,
                    )?;
                }
            } else if member_is_document {
                let group_id = crate::changes::GroupId::new(target.to_bytes());
                if removed {
                    change_manager.notify_document_removed_from_group(
                        DocumentId::new(member_id.0.into_bytes()),
                        group_id,
                    )?;
                } else {
                    change_manager.notify_document_added_to_group(
                        DocumentId::new(member_id.0.into_bytes()),
                        group_id,
                    )?;
                }
            } else if removed {
                change_manager.notify_member_removed_from_group(
                    crate::changes::GroupId::new(target.to_bytes()),
                    member_id,
                )?;
            } else {
                change_manager.notify_member_added_to_group(
                    crate::changes::GroupId::new(target.to_bytes()),
                    member_id,
                    access,
                )?;
            }
            Ok(())
        })
    }

    fn forward_materialization_retry(
        result: futures::channel::oneshot::Receiver<
            Result<crate::runtime2::MaterializationStatus, String>,
        >,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let status = result
                .await
                .map_err(|_| ferr!("materialization retry worker dropped its response"))?
                .map_err(|error| ferr!("materialization retry failed: {error}"))?;
            evt_tx
                .send(Runtime2Evt::DocWorkerMaterializationRetryCompleted { doc_id, status })
                .await
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            Ok(())
        })
    }
    fn release_lease(
        lease_rx: futures::channel::oneshot::Receiver<()>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: DocumentId,
        generation: u64,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            lease_rx.await.ok();
            // A closed commands channel means the runtime is draining; the
            // lease bookkeeping is moot then.
            cmd_tx
                .send(Runtime2Cmd::ReleaseInternalLease { doc_id, generation })
                .await
                .inspect_err(|_| warn_loc!(ERROR_CHANNEL))
                .ok();
            Ok(())
        })
    }
    fn await_worker_fence(
        barrier_id: u64,
        doc_id: DocumentId,
        reply: futures::channel::oneshot::Receiver<()>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            // The worker replies once its mailbox work has drained past the
            // fence. A dropped reply (worker evicted mid-fence) still acks:
            // `DocWorkerStopped` clears the doc from the probe anyway.
            reply.await.inspect_err(|_| warn_loc!(ERROR_CALLER)).ok();
            evt_tx
                .send(Runtime2Evt::DocWorkerFenced { doc_id, barrier_id })
                .await
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            Ok(())
        })
    }
    fn track_work(
        kind: crate::runtime2::TrackedWorkKind,
        fut: F::Future<'static, eyre::Result<()>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let _guard = crate::runtime2::TrackedWorkGuard::new(evt_tx, kind);
            fut.await
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
            eyre::Result<(
                PeerId,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    std::sync::Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match connect.connect(peer, addr).await {
                Ok((handshake_peer, closed, end_fut)) => {
                    if handshake_peer != peer {
                        // The connector has already authenticated the peer;
                        // close that authenticated connection before rejecting
                        // the caller's expected-target mismatch.
                        connect.close(handshake_peer, closed).await?;
                        resp.send(Err(ferr!(
                            "handshake peer mismatch: expected {peer}, got {handshake_peer}"
                        )))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                        return Ok(());
                    }
                    let (end_tx, end_rx) = futures::channel::oneshot::channel();
                    let watcher_closed = Arc::clone(&closed);
                    let watcher_peer = handshake_peer;
                    let watcher_evt_tx = evt_tx.clone();
                    let watcher_end_tx = end_tx;
                    let evt_tx_established = evt_tx.clone();
                    let closed_established = Arc::clone(&closed);
                    let watcher = child_tasks.spawn(F::from_future(async move {
                        let result = end_fut.await;
                        watcher_closed.store(true, std::sync::atomic::Ordering::SeqCst);
                        let error = result.as_ref().err().map(ToString::to_string);
                        // Surface the connection end to the caller alongside
                        // the runtime's own ConnLost event. The receiver is
                        // the caller's opt-in end channel; it is dropped when
                        // no end signal was requested (or the caller is
                        // gone), making a failed send here benign — ConnLost
                        // remains the runtime's source of truth.
                        // Send the end flag alongside the result so the
                        // caller can identify WHICH connection ended (peer
                        // ids can be reused across connections).
                        watcher_end_tx
                            .send((Arc::clone(&watcher_closed), result))
                            .ok();
                        if watcher_evt_tx
                            .send(Runtime2Evt::ConnLost {
                                peer_id: watcher_peer,
                                closed: Arc::clone(&watcher_closed),
                                error,
                            })
                            .await
                            .is_err()
                        {
                            debug!(%watcher_peer, "runtime stopped before connection-lost event");
                        }
                        Ok(())
                    }));
                    if let Err(error) = watcher {
                        connect.close(handshake_peer, closed).await?;
                        resp.send(Err(error))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
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
                        debug!(%handshake_peer, "runtime stopped before connection-established event");
                        connect.close(handshake_peer, closed).await?;
                        return Ok(());
                    }
                    resp.send(Ok((handshake_peer, closed, end_rx)))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
                Err(error) => {
                    resp.send(Err(error))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
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
            eyre::Result<(
                PeerId,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    std::sync::Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match connect.accept(incoming).await {
                Ok((handshake_peer, closed, end_fut)) => {
                    let (end_tx, end_rx) = futures::channel::oneshot::channel();
                    let watcher_closed = Arc::clone(&closed);
                    let watcher_peer = handshake_peer;
                    let watcher_evt_tx = evt_tx.clone();
                    let watcher_end_tx = end_tx;
                    let evt_tx_established = evt_tx.clone();
                    let closed_established = Arc::clone(&closed);
                    let watcher = child_tasks.spawn(F::from_future(async move {
                        let result = end_fut.await;
                        watcher_closed.store(true, std::sync::atomic::Ordering::SeqCst);
                        let error = result.as_ref().err().map(ToString::to_string);
                        // Surface the connection end to the caller alongside
                        // the runtime's own ConnLost event. The receiver is
                        // the caller's opt-in end channel; it is dropped when
                        // no end signal was requested (or the caller is
                        // gone), making a failed send here benign — ConnLost
                        // remains the runtime's source of truth.
                        // Send the end flag alongside the result so the
                        // caller can identify WHICH connection ended (peer
                        // ids can be reused across connections).
                        watcher_end_tx
                            .send((Arc::clone(&watcher_closed), result))
                            .ok();
                        if watcher_evt_tx
                            .send(Runtime2Evt::ConnLost {
                                peer_id: watcher_peer,
                                closed: Arc::clone(&watcher_closed),
                                error,
                            })
                            .await
                            .is_err()
                        {
                            debug!(%watcher_peer, "runtime stopped before connection-lost event");
                        }
                        Ok(())
                    }));
                    if let Err(error) = watcher {
                        resp.send(Err(error))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
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
                        debug!(%handshake_peer, "runtime stopped before connection-established event");
                        connect.close(handshake_peer, closed).await?;
                        return Ok(());
                    }
                    resp.send(Ok((handshake_peer, closed, end_rx)))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
                Err(error) => {
                    resp.send(Err(error))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
            }
            Ok(())
        })
    }

    fn close_connection_async(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer_id: PeerId,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = match connect.close(peer_id, closed).await {
                Ok(Some(replacement)) => {
                    evt_tx
                        .send(Runtime2Evt::ConnEstablished {
                            peer_id,
                            closed: replacement,
                        })
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            resp.send(result)
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            Ok(())
        })
    }

    fn sync_doc_with_peer(
        request_id: subduction_core::connection::message::RequestId,
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
        peer_id: PeerId,
        sed_id: sedimentree_core::id::SedimentreeId,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
    ) -> F::Future<'static, eyre::Result<()>> {
        let span = tracing::debug_span!(
            "document_sync",
            request_nonce = request_id.nonce,
            remote_peer_id = %peer_id,
            document_id = %DocumentId::new(*sed_id.as_bytes()),
        );
        F::from_future(
            async move {
                let result = match runtime_io
                    .sync_doc_with_peer(sed_id, peer_id, Some(request_id))
                    .await
                {
                    Ok(crate::runtime2::SyncDocAttempt::Exchanged) => Ok(()),
                    Ok(crate::runtime2::SyncDocAttempt::NotFound) => {
                        Err(crate::runtime2::types::SyncDocError::NotFound)
                    }
                    Ok(crate::runtime2::SyncDocAttempt::Unauthorized) => {
                        Err(crate::runtime2::types::SyncDocError::Unauthorized)
                    }
                    Ok(crate::runtime2::SyncDocAttempt::Policy(kind)) => {
                        let policy = match kind {
                            subduction_core::sync_session::SyncPolicyRejectionKind::DocumentNotFound => {
                                crate::runtime2::types::SyncDocPolicyError::DocumentNotFound
                            }
                            subduction_core::sync_session::SyncPolicyRejectionKind::InsufficientAccess => {
                                crate::runtime2::types::SyncDocPolicyError::InsufficientAccess
                            }
                            subduction_core::sync_session::SyncPolicyRejectionKind::InvalidIdentifier => {
                                crate::runtime2::types::SyncDocPolicyError::InvalidIdentifier
                            }
                            subduction_core::sync_session::SyncPolicyRejectionKind::Other => {
                                crate::runtime2::types::SyncDocPolicyError::Other(
                                    "local policy rejection".into(),
                                )
                            }
                        };
                        Err(crate::runtime2::types::SyncDocError::Policy(policy))
                    }
                    Err(error) => Err(crate::runtime2::types::SyncDocError::IoError(error)),
                };
                match result {
                    Ok(()) => {
                        // The emitted session(s) carry `request_id`; the hub
                        // resolves the waiter from the session or, if no
                        // session is observed, from this round-done signal.
                        // A closed commands channel means the runtime is
                        // draining; the waiter is dropped with the hub and
                        // the caller observes the closure.
                        cmd_tx
                            .send(Runtime2Cmd::DocSyncRoundDone { request_id })
                            .await
                            .inspect_err(|_| warn_loc!(ERROR_CHANNEL))
                            .ok();
                    }
                    Err(error) => {
                        cmd_tx
                            .send(Runtime2Cmd::DocSyncFailed { request_id, error })
                            .await
                            .inspect_err(|_| warn_loc!(ERROR_CHANNEL))
                            .ok();
                    }
                }
                Ok(())
            }
            .instrument(span),
        )
    }
}

// ─── Shared helper: spawn owned background work on the child task set ────

impl<F: FutureForm + HubBackgroundFuture<F> + 'static, R: TaskRuntime<F>> Runtime2Hub<F, R> {
    fn spawn_background(
        &self,
        fut: F::Future<'static, eyre::Result<()>>,
    ) -> eyre::Result<futures::stream::AbortHandle> {
        self.child_tasks.spawn(fut)
    }

    /// Spawn finite background work that participates in the quiescence
    /// predicate (B8 tracked-work seam). The future is wrapped so a
    /// `TrackedWorkDone` event is emitted *after* its own emissions, keeping
    /// channel order: the hub processes everything the future reported before
    /// it decrements the in-flight counter.
    fn spawn_tracked(
        &mut self,
        kind: crate::runtime2::TrackedWorkKind,
        fut: F::Future<'static, eyre::Result<()>>,
    ) -> eyre::Result<futures::stream::AbortHandle> {
        if self.cmd_closed {
            // The runtime is draining; dropping new background work is the
            // point of shutdown (its events could otherwise keep the drain
            // alive indefinitely, e.g. B11's follow-up sync or B6's
            // re-verification retry).
            debug!(
                local_peer_id = %self.local_peer_id,
                kind = ?kind,
                "discarding background work while shutting down"
            );
            let (abort, _) = futures::future::AbortHandle::new_pair();
            return Ok(abort);
        }
        self.tracked_in_flight = self.tracked_in_flight.wrapping_add(1);
        self.child_tasks
            .spawn(F::track_work(kind, fut, self.evt_tx.clone()))
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
        + DocWorkerLoop<F>,
    R: TaskRuntime<F>,
> Runtime2Hub<F, R>
where
    F: 'static,
{
    #[tracing::instrument(skip(self))]
    fn handle_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        trace!(?evt, "runtime2 event received");
        match &evt {
            Runtime2Evt::SyncSessionObserved { session, .. } => {
                debug!(
                    local_peer_id = %self.local_peer_id,
                    doc_id = %DocumentId::new(*session.sedimentree_id.as_bytes()),
                    peer_id = %session.peer_id,
                    kind = ?session.kind,
                    remote_rejection = ?&session.remote_rejection,
                    received_commits = session.received_commit_ids.len(),
                    received_fragments = session.received_fragment_ids.len(),
                    rejected_commits = session.rejected_commit_ids.len(),
                    rejected_fragments = session.rejected_fragment_ids.len(),
                    sent_commits = session.sent_commit_ids.len(),
                    sent_fragments = session.sent_fragment_ids.len(),
                    "runtime2 event: sync session observed",
                );
            }
            Runtime2Evt::KeyhiveSyncDone {
                peer_id,
                request_id,
                changed,
            } => debug!(
                local_peer_id = %self.local_peer_id,
                %peer_id,
                ?request_id,
                changed,
                "runtime2 event: Keyhive sync completed",
            ),
            Runtime2Evt::KeyhiveSyncFailed {
                peer_id,
                request_id,
                error,
            } => debug!(
                local_peer_id = %self.local_peer_id,
                %peer_id,
                ?request_id,
                %error,
                "runtime2 event: Keyhive sync failed",
            ),
            Runtime2Evt::ConnEstablished { peer_id, .. } => debug!(
                local_peer_id = %self.local_peer_id,
                %peer_id,
                "runtime2 event: connection established",
            ),
            Runtime2Evt::ConnLost { peer_id, error, .. } => debug!(
                local_peer_id = %self.local_peer_id,
                %peer_id,
                ?error,
                "runtime2 event: connection lost",
            ),
            _ => {}
        }
        // Lifecycle/completion events update dedicated barriers below; only
        // domain mutations restart a quiescence probe's activity generation.
        if !matches!(
            &evt,
            Runtime2Evt::DocWorkerFenced { .. }
                | Runtime2Evt::TrackedWorkDone { .. }
                | Runtime2Evt::GroupPartWorkerSettled { .. }
                | Runtime2Evt::KeyhiveAdmissionAdvanced { .. }
                | Runtime2Evt::ConnEstablished { .. }
                | Runtime2Evt::ConnLost { .. }
                | Runtime2Evt::KeyhiveSyncDone { .. }
                | Runtime2Evt::KeyhiveSyncFailed { .. }
                | Runtime2Evt::DocWorkerStopped { .. }
        ) {
            self.note_activity();
        }
        match evt {
            Runtime2Evt::SyncSessionObserved { cause, session } => {
                let span = tracing::debug_span!(
                    "apply_sync_session",
                    remote_peer_id = %session.peer_id,
                    document_id = %DocumentId::new(*session.sedimentree_id.as_bytes()),
                    kind = ?session.kind,
                );
                span.follows_from(cause);
                let _entered = span.enter();
                self.handle_sync_session_observed(session)?;
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
                changed,
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
            Runtime2Evt::KeyhiveChangeNotif { peer_id } => {
                self.handle_keyhive_change_notif(peer_id)?;
            }
            Runtime2Evt::KeyhiveAdmissionAdvanced { seq } => {
                self.admitted_head = self.admitted_head.max(seq);
                self.try_resolve_quiescence()?;
            }
            Runtime2Evt::GroupPartWorkerSettled { seq } => {
                self.group_part_settled_seq = self.group_part_settled_seq.max(seq);
                let mut pending = Vec::new();
                for (captured, waiter) in std::mem::take(&mut self.keyhive_reconciliation_waiters) {
                    if self.group_part_settled_seq >= captured {
                        waiter
                            .send(Ok(()))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                    } else {
                        pending.push((captured, waiter));
                    }
                }
                self.keyhive_reconciliation_waiters = pending;
                self.try_resolve_quiescence()?;
            }
            Runtime2Evt::DocWorkerStopped { doc_id, error } => {
                self.doc_workers.remove(&doc_id);
                self.pending_materialization.remove(&doc_id);
                self.materialization_retries_in_flight.remove(&doc_id);
                if let Some(probe) = self.quiescence_probe.as_mut() {
                    probe.pending_docs.remove(&doc_id);
                }
                if let Some(err) = error {
                    tracing::error!(%doc_id, error = %err, "doc worker stopped with error");
                }
            }
            Runtime2Evt::DocWorkerMaterializationPending { doc_id } => {
                self.pending_materialization.insert(doc_id);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    %doc_id,
                    pending_count = self.pending_materialization.len(),
                    worker_present = self.doc_workers.contains_key(&doc_id),
                    "document materialization entered pending set"
                );
            }
            Runtime2Evt::DocWorkerMaterializationReady { doc_id } => {
                self.pending_materialization.remove(&doc_id);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    %doc_id,
                    pending_count = self.pending_materialization.len(),
                    "document materialization left pending set"
                );
            }
            Runtime2Evt::DocWorkerMaterializationRetryCompleted { doc_id, status } => {
                let start_seq = self.materialization_retries_in_flight.remove(&doc_id);
                let stale = start_seq
                    .is_some_and(|start| self.admitted_head > start);
                match &status {
                    crate::runtime2::MaterializationStatus::Pending(blockers) => {
                        self.pending_materialization.insert(doc_id);
                        debug!(
                            %doc_id,
                            ?blockers,
                            stale,
                            "materialization remains dependency-blocked"
                        );
                        if stale {
                            // The Keyhive state advanced while the walk ran
                            // (e.g. a later CGKA op of the same rotation) —
                            // this Pending may be stale; re-verify with the
                            // fresher key state (B6).
                            debug!(%doc_id, "re-verifying stale materialization retry");
                            self.retry_doc_materialization(doc_id)?;
                        }
                    }
                    crate::runtime2::MaterializationStatus::Ready {
                        partially_decrypted: true,
                    } => {
                        self.pending_materialization.insert(doc_id);
                        debug!(
                            %doc_id,
                            "materialization retry remains partially decrypted"
                        );
                    }
                    crate::runtime2::MaterializationStatus::Missing
                    | crate::runtime2::MaterializationStatus::Ready {
                        partially_decrypted: false,
                    } => {
                        self.pending_materialization.remove(&doc_id);
                        debug!(%doc_id, ?status, "materialization retry reached a terminal state");
                    }
                }
            }
            // --- Keyhive event listener handlers ---
            // Translate raw Keyhive events into domain-level notifications.
            Runtime2Evt::PrekeyExpanded { .. } | Runtime2Evt::PrekeyRotated { .. } => {
                // Individual prekey operations are internal key management
                // and do not correspond to a BigRepo domain event.
            }
            // FIXME: this doesn't seem correct, I believe a key rotation can correspond
            // to multiple CGKA ops
            Runtime2Evt::CgkaOp { data } => {
                // Every CGKA op is a document key rotation.
                let doc_id = crate::DocumentId::new(*data.payload().doc_id().as_bytes());
                let was_pending = self.pending_materialization.contains(&doc_id);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    %doc_id,
                    was_pending,
                    pending_count = self.pending_materialization.len(),
                    "processing CGKA operation; retrying pending materialization after key update"
                );
                self.change_manager
                    .notify_document_key_rotated(doc_id)
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                // Targeted retry: only this doc's keys moved; live docs are
                // not re-walked (B6).
                if was_pending {
                    self.retry_doc_materialization(doc_id)?;
                }
            }
            Runtime2Evt::DelegationReceived { target, data } => {
                let member_id = PeerId::new(data.payload().delegate().id().to_bytes());
                let member_is_document = matches!(
                    data.payload().delegate(),
                    keyhive_core::principal::agent::Agent::Document(..)
                );
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::EmitMembershipChange,
                    F::emit_membership_change(
                        Arc::clone(&self.runtime_io),
                        Arc::clone(&self.change_manager),
                        target,
                        member_id,
                        crate::changes::BigRepoAccess::from(data.payload().can()),
                        false,
                        member_is_document,
                    ),
                )?;
                let pending: Vec<_> = self.pending_materialization.iter().copied().collect();
                for doc_id in pending {
                    self.retry_doc_materialization(doc_id)?;
                }
            }
            Runtime2Evt::RevocationReceived { target, data } => {
                let member_id = PeerId::new(data.payload().revoked_id().as_bytes());
                let member_is_document = matches!(
                    data.payload().revoked().payload().delegate(),
                    keyhive_core::principal::agent::Agent::Document(..)
                );
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::EmitMembershipChange,
                    F::emit_membership_change(
                        Arc::clone(&self.runtime_io),
                        Arc::clone(&self.change_manager),
                        target,
                        member_id,
                        crate::changes::BigRepoAccess::Relay,
                        true,
                        member_is_document,
                    ),
                )?;
            }
            Runtime2Evt::DocWorkerFenced { doc_id, barrier_id } => {
                self.handle_doc_worker_fenced(doc_id, barrier_id)?;
            }
            Runtime2Evt::TrackedWorkDone { kind } => {
                assert!(
                    self.tracked_in_flight > 0,
                    "TrackedWorkDone without a matching spawn_tracked increment"
                );
                self.tracked_in_flight -= 1;
                debug!(
                    local_peer_id = %self.local_peer_id,
                    kind = ?kind,
                    tracked_in_flight = self.tracked_in_flight,
                    "tracked background work completed",
                );
            }
        }
        self.try_resolve_quiescence()
    }

    /// Route an observed sync session to the relevant doc-worker.
    #[tracing::instrument(
        skip_all,
        fields(
            local_peer_id = %self.local_peer_id,
            doc_id = %DocumentId::new(*session.sedimentree_id.as_bytes()),
            remote_peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commits = session.received_commit_ids.len(),
            received_fragments = session.received_fragment_ids.len(),
        )
    )]
    fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> eyre::Result<()> {
        if self.cmd_closed {
            debug!("discarding observed sync session while shutting down");
            return Ok(());
        }
        let doc_id = DocumentId::new(*session.sedimentree_id.as_bytes());
        debug!(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            sent_commit_ids = session.sent_commit_ids.len(),
            sent_fragment_ids = session.sent_fragment_ids.len(),
            "observed sync session"
        );
        let received =
            !session.received_commit_ids.is_empty() || !session.received_fragment_ids.is_empty();
        if !received {
            // Empty sessions carry no content and never resolve a receipt:
            // the transport round's completion (or a later session) handles
            // the reconsider walk. Skipped as before B4.
            return Ok(());
        }
        let peer_id = PeerId::new(*session.peer_id.as_bytes());

        // Sessions are always routed fire-and-forget. Caller waiters are
        // resolved at round completion (`DocSyncRoundDone` /
        // `DocSyncFailed`) so the receipt reflects the authoritative
        // transport result — a rejected session (remote or policy) must not
        // resolve a waiter with a success receipt.
        debug!(
            doc_id = %doc_id,
            remote_peer_id = %peer_id,
            "routing received sync content to document worker"
        );
        self.route_sync_session_apply(
            doc_id,
            peer_id,
            session.received_commit_ids,
            session.received_fragment_ids,
            None,
        )
    }

    /// Route a sync-session apply to the doc worker, resolving the receipt
    /// directly when no (or no live) worker exists.
    ///
    /// `reply: Some` resolves a caller's sync receipt with the worker's
    /// outcome; `None` is fire-and-forget (passive sessions). Empty `commit_ids`
    /// / `fragment_ids` make the worker only reconsider a pending doc / report
    /// its current state.
    fn route_sync_session_apply(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        commit_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
        fragment_ids: Vec<sedimentree_core::loose_commit::id::CommitId>,
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
    ) -> eyre::Result<()> {
        // Received content must pass through the document worker even without
        // an application handle: writable overlap nodes use cold/transient
        // materialization to publish causal-key healing checkpoints.
        let (worker, _lease) = self.doc_worker_handle(doc_id)?;
        if let Err(err) = worker.send(DocWorkerMsg::ApplySyncSession {
            peer_id,
            commit_ids,
            fragment_ids,
            reply,
            _lease,
        }) {
            debug!(
                doc_id = %doc_id,
                ?err,
                "failed to route sync session to doc worker (worker closed or full)"
            );
        }
        Ok(())
    }

    // ─── connection lifecycle ──────────────────────────────────────────────

    /// Handle an established connection: register peer in `connected_peers`,
    /// then schedule the initial keyhive sync.
    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn handle_connection_established(
        &mut self,
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        self.connected_peers.insert(
            peer_id,
            ConnDeets {
                closed: Arc::clone(&closed),
            },
        );
        self.start_keyhive_sync(peer_id)?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn handle_connection_lost(
        &mut self,
        peer_id: PeerId,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        let Some(current) = self.connected_peers.get(&peer_id) else {
            debug!(%peer_id, "ignoring connection loss for untracked connection");
            return Ok(());
        };
        if !Arc::ptr_eq(&current.closed, &closed) {
            debug!(%peer_id, "ignoring stale connection loss after reconnect");
            return Ok(());
        }
        self.cancel_pending_keyhive_syncs(&peer_id, "keyhive connection lost");
        self.connected_peers.remove(&peer_id);
        Ok(())
    }

    // ─── keyhive sync ──────────────────────────────────────────────────────

    /// Start a keyhive sync round with `peer_id` if not already active.
    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn start_keyhive_sync(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        self.start_keyhive_sync_round(peer_id)
    }

    fn start_keyhive_sync_round(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        if self.active_keyhive_syncs.contains_key(&peer_id) {
            return Ok(());
        }
        let admitted_ids = self
            .keyhive_waiters
            .get(&peer_id)
            .map_or_else(std::collections::HashSet::new, |waiters| {
                waiters.ids.clone()
            });
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
                round_id,
                request_id: request_id.clone(),
                admitted_ids,
            },
        );
        debug!(
            %peer_id,
            round_id,
            ?request_id,
            admitted_waiters = self
                .active_keyhive_syncs
                .get(&peer_id)
                .map_or(0, |round| round.admitted_ids.len()),
            pending_waiters = self.keyhive_waiters.get(&peer_id).map_or(0, |waiters| waiters.waiters.len()),
            "starting Keyhive sync round"
        );
        self.spawn_tracked(
            crate::runtime2::TrackedWorkKind::KeyhiveSync,
            F::start_sync(
                Arc::clone(&self.runtime_io),
                self.evt_tx.clone(),
                peer_id,
                request_id,
            ),
        )?;
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
            debug!(%peer_id, ?request_id, "ignoring untracked keyhive sync failure");
            return Ok(());
        };
        if round.request_id != request_id {
            debug!(
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
        if let Some(waiters) = self.keyhive_waiters.remove(&peer_id) {
            for (_, sender) in waiters.waiters {
                sender
                    .send(Err(ferr!("{error}")))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
        }
        debug!(%peer_id, ?request_id, error, "keyhive sync initiation failed");
        if self.keyhive_notif_pending.remove(&peer_id) {
            debug!(
                %peer_id,
                "retrying latched change notification after failed keyhive sync"
            );
            self.start_keyhive_sync(peer_id)?;
        }
        Ok(())
    }

    /// A remote peer signalled a Keyhive change (keyhive-changes RPC
    /// notification). Start a waiter-less sync round so the peer's changes are
    /// pulled; the round is quiescence-visible via `active_keyhive_syncs`. If
    /// a round is already active, the change is latched and a follow-up round
    /// runs when the current one completes (the in-flight exchange may have
    /// synced stale state); if the peer is not connected, the notification is
    /// stale and ignored.
    fn handle_keyhive_change_notif(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        if !self.connected_peers.contains_key(&peer_id) {
            debug!(
                %peer_id,
                "keyhive change notification ignored: peer not connected"
            );
            return Ok(());
        }
        if self.active_keyhive_syncs.contains_key(&peer_id) {
            self.keyhive_notif_pending.insert(peer_id);
            debug!(
                %peer_id,
                "keyhive change notification latched; follow-up round after current completes"
            );
            return Ok(());
        }
        debug!(%peer_id, "keyhive change notification; starting sync round");
        self.start_keyhive_sync(peer_id)?;
        Ok(())
    }

    /// Complete a keyhive protocol round and resolve any eligible waiters.
    fn finish_keyhive_sync(
        &mut self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> eyre::Result<()> {
        let Some(round) = self.active_keyhive_syncs.get_mut(&peer_id) else {
            debug!(%peer_id, ?request_id, "processing untracked inbound keyhive completion");
            self.reattempt_pending_materialization()?;
            return Ok(());
        };
        if round.request_id != request_id {
            debug!(
                %peer_id,
                expected_request_id = ?round.request_id,
                request_id = ?request_id,
                "processing inbound keyhive exchange without resolving waiter"
            );
            self.reattempt_pending_materialization()?;
            return Ok(());
        }
        let round_id = round.round_id;
        self.complete_keyhive_sync_round(peer_id, round_id)
    }

    fn complete_keyhive_sync_round(&mut self, peer_id: PeerId, round_id: u64) -> eyre::Result<()> {
        let round = self
            .active_keyhive_syncs
            .remove(&peer_id)
            .expect("active keyhive sync disappeared before protocol completion");
        assert_eq!(round.round_id, round_id);
        let admitted_ids = round.admitted_ids;

        // Split waiters: those this round admitted resolve; those that arrived
        // during/after cascade into a new round.
        let mut resolved_waiters = 0usize;
        if let Some(waiters) = self.keyhive_waiters.get_mut(&peer_id) {
            let mut remaining = Vec::new();
            for (id, sender) in std::mem::take(&mut waiters.waiters) {
                if admitted_ids.contains(&id) {
                    waiters.ids.remove(&id);
                    sender
                        .send(Ok(()))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    resolved_waiters += 1;
                } else {
                    remaining.push((id, sender));
                }
            }
            waiters.waiters = remaining;
            if waiters.waiters.is_empty() {
                self.keyhive_waiters.remove(&peer_id);
            }
        }
        let has_remaining = self.keyhive_waiters.contains_key(&peer_id);
        debug!(
            %peer_id,
            round_id,
            ?round.request_id,
            admitted_waiters = admitted_ids.len(),
            resolved_waiters,
            remaining_waiters = self
                .keyhive_waiters
                .get(&peer_id)
                .map_or(0, |waiters| waiters.waiters.len()),
            has_remaining,
            "completing Keyhive sync round"
        );
        if has_remaining {
            self.start_keyhive_sync(peer_id)?;
        }
        if self.keyhive_notif_pending.remove(&peer_id) {
            debug!(
                %peer_id,
                round_id,
                "change notification latched during round; starting follow-up round"
            );
            self.start_keyhive_sync(peer_id)?;
        }
        self.reattempt_pending_materialization()?;
        Ok(())
    }

    /// Full sweep: retry materialization for every doc in the pending set
    /// (used by the exchange triggers — a keyhive sync may have unblocked any
    /// pending doc).
    fn reattempt_pending_materialization(&mut self) -> eyre::Result<()> {
        let pending = self.pending_materialization.clone();
        debug!(
            local_peer_id = %self.local_peer_id,
            pending_count = pending.len(),
            "reattempting pending document materialization"
        );
        for doc_id in pending {
            self.retry_doc_materialization(doc_id)?;
        }
        Ok(())
    }

    /// Targeted retry for one document (B6). The in-flight entry records the
    /// Keyhive state generation at retry start; `forward_materialization_retry`
    /// acks the worker's status back through `DocWorkerMaterializationRetryCompleted`,
    /// where a stale `Pending` (state advanced while the walk ran) re-verifies.
    fn retry_doc_materialization(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        if self.materialization_retries_in_flight.contains_key(&doc_id) {
            return Ok(());
        }
        let (worker, _lease) = match self.doc_worker_handle(doc_id) {
            Ok(pair) => pair,
            Err(_) => {
                debug!(%doc_id, "dropping pending materialization without document worker");
                self.pending_materialization.remove(&doc_id);
                self.schedule_doc_worker_eviction_if_idle(doc_id);
                return Ok(());
            }
        };
        let start_seq = self.admitted_head;
        self.materialization_retries_in_flight
            .insert(doc_id, start_seq);
        debug!(
            %doc_id,
            start_seq,
            "requesting targeted materialization retry from document worker"
        );
        let (resp, result) = futures::channel::oneshot::channel();
        if let Err(error) = worker.send(DocWorkerMsg::ReattemptMaterialization {
            origin: crate::changes::BigRepoChangeOrigin::Keyhive,
            resp,
            _lease,
        }) {
            self.materialization_retries_in_flight.remove(&doc_id);
            return Err(error).wrap_err(ERROR_CHANNEL);
        }
        self.spawn_tracked(
            crate::runtime2::TrackedWorkKind::MaterializationRetry,
            F::forward_materialization_retry(result, self.evt_tx.clone(), doc_id),
        )?;
        Ok(())
    }

    /// Cancel a pending keyhive sync waiter by id. The waiter is removed
    /// precisely (O(1) membership via the id set), so a timed-out caller's
    /// dead entry can never cascade a follow-up round.
    fn cancel_pending_keyhive_sync(&mut self, peer_id: &PeerId, waiter_id: u64) -> bool {
        let Some(waiters) = self.keyhive_waiters.get_mut(peer_id) else {
            return false;
        };
        if !waiters.ids.remove(&waiter_id) {
            return false;
        }
        waiters.waiters.retain(|(id, _)| *id != waiter_id);
        if waiters.waiters.is_empty() {
            self.keyhive_waiters.remove(peer_id);
        }
        true
    }

    /// Cancel all pending keyhive syncs for a peer.

    fn cancel_pending_keyhive_syncs(&mut self, peer_id: &PeerId, reason: &'static str) {
        self.active_keyhive_syncs.remove(peer_id);
        // A latched change notification is stale once the connection is gone;
        // reconnecting runs its own initial sync round.
        self.keyhive_notif_pending.remove(peer_id);
        if let Some(waiters) = self.keyhive_waiters.remove(peer_id) {
            for (_id, sender) in waiters.waiters {
                sender
                    .send(Err(eyre::Report::new(crate::KeyhiveSyncCancelled {
                        reason,
                    })))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DOC-WORKER LIFECYCLE
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm + HubBackgroundFuture<F> + DocWorkerLoop<F> + 'static, R: TaskRuntime<F>>
    Runtime2Hub<F, R>
{
    /// Ensure a doc-worker exists for `doc_id`, return its handle + internal lease.
    ///
    /// If the worker is already alive, bumps `internal_leases` and clears the
    /// eviction deadline.
    fn doc_worker_handle(
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
        let generation = entry.generation;

        // Create a oneshot whose sender is consumed by the lease. When the
        // lease drops (doc-worker finishes the op), the sender is dropped
        // and the receiver gets `RecvError::Closed` — a tracked child task
        // forwards this as a `ReleaseInternalLease` command back to the hub.
        let (lease_tx, lease_rx) = futures::channel::oneshot::channel::<()>();
        let cmd_tx = self.cmd_tx.clone();
        self.spawn_background(F::release_lease(lease_rx, cmd_tx, doc_id, generation))?;

        let lease = DocWorkerInternalLease::new(lease_tx);
        Ok((handle, lease))
    }

    /// Acquire handle + internal lease for an existing doc-worker without spawning a new one.
    fn acquire_existing_doc_worker_handle(
        &mut self,
        doc_id: DocumentId,
    ) -> eyre::Result<Option<(DocWorkerHandle, DocWorkerInternalLease)>> {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return Ok(None);
        };
        if entry.handle.is_closed() {
            return Ok(None);
        }
        entry.eviction_deadline = None;
        entry.internal_leases += 1;
        let handle = entry.handle.clone();
        let generation = entry.generation;

        let (lease_tx, lease_rx) = futures::channel::oneshot::channel::<()>();
        let cmd_tx = self.cmd_tx.clone();
        self.spawn_background(F::release_lease(lease_rx, cmd_tx, doc_id, generation))?;

        let lease = DocWorkerInternalLease::new(lease_tx);
        Ok(Some((handle, lease)))
    }

    /// Lazily spawn a doc-worker if none exists.
    #[tracing::instrument(skip_all, fields(%doc_id))]
    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        if self.cmd_closed {
            debug!(%doc_id, "discarding doc worker spawn while shutting down");
            return Ok(());
        }
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

        let generation = self.next_doc_worker_generation;
        self.next_doc_worker_generation += 1;

        let worker = crate::runtime2::spawn_doc_worker(
            doc_id,
            Arc::clone(&self.doc_io),
            Arc::clone(&self.change_manager),
            self.cmd_tx.clone(),
            self.evt_tx.clone(),
            generation,
        );
        let handle = worker.handle;
        let stop = worker.stop;
        self.child_tasks.spawn(worker.run)?;

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
                generation,
            },
        );
        Ok(())
    }

    /// Decrement `local_handles` for a doc-worker; schedule eviction if idle.
    ///
    /// Identifies worker incarnation by `generation`: stale releases from
    /// previous worker generations that were evicted or died are safely ignored.
    fn handle_release_doc_lease(&mut self, doc_id: DocumentId, generation: u64) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            if entry.generation == generation {
                entry.local_handles = entry
                    .local_handles
                    .checked_sub(1)
                    .expect("doc lease refcount underflow for active worker incarnation");
            } else {
                debug!(
                    %doc_id,
                    lease_generation = generation,
                    current_generation = entry.generation,
                    "ignoring stale doc lease release for superseded worker incarnation"
                );
                return;
            }
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    /// Decrement `internal_leases` for a doc-worker; schedule eviction if idle.
    ///
    /// Identifies worker incarnation by `generation` — see [`Self::handle_release_doc_lease`].
    fn handle_release_internal_lease(&mut self, doc_id: DocumentId, generation: u64) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            if entry.generation == generation {
                entry.internal_leases = entry
                    .internal_leases
                    .checked_sub(1)
                    .expect("internal lease refcount underflow for active worker incarnation");
            } else {
                debug!(
                    %doc_id,
                    lease_generation = generation,
                    current_generation = entry.generation,
                    "ignoring stale internal lease release for superseded worker incarnation"
                );
                return;
            }
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    /// Set or clear the eviction deadline based on refcounts.
    fn schedule_doc_worker_eviction_if_idle(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        if entry.local_handles > 0 || entry.internal_leases > 0 {
            entry.eviction_deadline = None;
            return;
        }
        entry.eviction_deadline = Some(self.clock.instant() + self.sync_policy.doc_worker_idle_ttl);
    }

    /// Periodic eviction of idle doc-workers. Driven by the machine loop's
    /// `Timer::tick(doc_worker_idle_ttl)`.
    fn janitor_tick(&mut self) {
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
            // Eviction requires both lease counts to be zero (the deadline is
            // only armed by `schedule_doc_worker_eviction_if_idle` in that
            // case); guard anyway so an in-flight operation is never cancelled
            // underneath itself.
            let idle = self
                .doc_workers
                .get(&doc_id)
                .is_some_and(|entry| entry.local_handles == 0 && entry.internal_leases == 0);
            if !idle {
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.eviction_deadline = None;
                }
                continue;
            }
            // Remove the entry *before* cancelling: the abort path of the
            // worker's mailbox loop does not emit `DocWorkerStopped` (that
            // event is only sent on normal mailbox completion), so a cancelled
            // worker would otherwise linger as a stale closed-sender entry —
            // re-cancelled by the janitor every tick and fenced by quiescence.
            let entry = self
                .doc_workers
                .remove(&doc_id)
                .expect("doc worker entry present when janitor evicted it");
            self.pending_materialization.remove(&doc_id);
            self.materialization_retries_in_flight.remove(&doc_id);
            if let Some(probe) = self.quiescence_probe.as_mut() {
                probe.pending_docs.remove(&doc_id);
            }
            entry.stop.cancel();
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
/// - `machine_tasks` — the hub's dispatcher loop, stopped last so it can
///   drain in-flight tracked work before exiting.
pub struct Runtime2StopToken<F: FutureForm, R: TaskRuntime<F>> {
    pub(crate) cancel: futures::future::AbortHandle,
    pub(crate) cmd_tx: async_channel::Sender<Runtime2Cmd>,
    pub(crate) child_tasks: R::Tasks,
    pub(crate) machine_tasks: R::Tasks,
    pub group_part_stop: Option<crate::runtime2::GroupPartWorkerStopToken>,
    pub causal_checkpoint_stop: Option<crate::runtime2::CausalCheckpointWorkerStopToken>,
    pub automerge_frontier_stop: Option<crate::runtime2::AutomergeFrontierWorkerStopToken>,
}

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2StopToken<F, R> {
    /// Cancel the runtime and await graceful shutdown.
    ///
    /// Closing the commands channel (via [`Sender::close`], so the closure
    /// is observed even while the hub, handle and in-flight children still
    /// hold sender clones) makes the machine loop stop admitting new
    /// background work, drain in-flight tracked work (processing their
    /// events, so no child ever reports into a closed channel) and exit.
    /// Senders that still hold a `cmd_tx` clone treat the closed channel as
    /// the shutdown signal. Only if the drain does not complete within
    /// `timeout` is the machine loop aborted outright.
    pub async fn stop(mut self, timeout: std::time::Duration) -> eyre::Result<()> {
        // Stop construction-time background workers first in reverse order:
        if let Some(stop) = self.automerge_frontier_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.causal_checkpoint_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.group_part_stop.take() {
            stop.cancel();
        }

        // Close the commands channel: no `Stop` message, no ack — the
        // receiver observes the closure after draining buffered commands.
        self.cmd_tx.close();

        // Wait for the machine loop to drain and exit. A timeout means the
        // drain is wedged (e.g. a child stuck on transport IO); `stop` has
        // already aborted the remaining tasks and waited a grace period for
        // them to terminate, so aborting the machine loop's registration
        // here makes its next poll return immediately.
        if self.machine_tasks.stop(timeout).await.is_err() {
            tracing::warn!("runtime2 graceful shutdown timed out; aborting machine loop");
            self.cancel.abort();
        }

        // Not every child operation observes the runtime cancellation token
        // (for example a peer sync may be awaiting transport IO). Abort the
        // child scope only after the machine loop has exited, then join it.
        self.child_tasks.abort();
        self.child_tasks.stop(timeout).await?;
        Ok(())
    }
}

pub(crate) trait HubMachineFuture<F: FutureForm + FutureForm, R: TaskRuntime<F>> {
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
        let _cancellation = registration.handle();
        let span = tracing::info_span!("runtime2_hub", local_peer_id = %hub.local_peer_id);
        F::from_future(
            async move {
                let result = futures::future::Abortable::new(
                    async move {
                        loop {
                            if hub.cmd_closed && hub.tracked_in_flight == 0 {
                                // Drain complete: the commands channel closed
                                // (the stop token dropped its sender) and all
                                // tracked work has reported. Exit; the stop
                                // token then aborts the child scope, which is
                                // safe because no child still has events to
                                // report (and the ones that outlive it handle a
                                // closed channel).
                                break;
                            }
                            if hub.frozen {
                                // B12 freeze: events and the janitor are held;
                                // only `Unfreeze` is processed — everything
                                // else is buffered and replayed on unfreeze.
                                match cmd_rx.recv().await {
                                    Ok(Runtime2Cmd::Unfreeze) => {
                                        hub.frozen = false;
                                        let buffered = std::mem::take(&mut hub.frozen_cmd_buffer);
                                        for cmd in buffered {
                                            hub.handle_cmd(cmd)?;
                                        }
                                    }
                                    Ok(cmd) => hub.frozen_cmd_buffer.push(cmd),
                                    Err(_) => {
                                        // Shutdown wins over a freeze: unfreeze
                                        // so queued TrackedWorkDone events can
                                        // drain, then fall through to the normal
                                        // drain path.
                                        hub.frozen = false;
                                        hub.frozen_cmd_buffer.clear();
                                        hub.cmd_closed = true;
                                    }
                                }
                                continue;
                            }
                            // Commands are polled before events: a command (e.g.
                            // a lease release or an unfreeze) can cancel the
                            // very work whose events would otherwise keep the
                            // loop busy, so it must not be starved by an event
                            // flood.
                            let mut sleep =
                                Box::pin(timer.sleep(std::time::Duration::from_millis(500)).fuse());
                            let mut evt = Box::pin(evt_rx.recv().fuse());
                            if hub.cmd_closed {
                                // Draining: only events (and the janitor) can
                                // make progress. Polling a closed command
                                // channel returns `Err` instantly, so a select
                                // that still polls it would fire its branch on
                                // every iteration and busy-spin the drain at
                                // 100% CPU until `tracked_in_flight` hits zero.
                                // Once the channel is closed it is never polled
                                // again.
                                futures::select_biased! {
                                    _ = sleep.as_mut() => hub.janitor_tick(),
                                    evt = evt.as_mut() => match evt {
                                        Ok(evt) => hub.handle_evt(evt)?,
                                        Err(_) => break,
                                    },
                                }
                            } else {
                                let mut cmd = Box::pin(cmd_rx.recv().fuse());
                                futures::select_biased! {
                                    _ = sleep.as_mut() => hub.janitor_tick(),
                                    cmd_res = cmd.as_mut() => match cmd_res {
                                        Ok(cmd) => hub.handle_cmd(cmd)?,
                                        Err(_) => {
                                            // Channel closed: no more commands.
                                            // Fall into the drain path (events
                                            // only) next iteration.
                                            hub.cmd_closed = true;
                                        }
                                    },
                                    evt = evt.as_mut() => match evt {
                                        Ok(evt) => hub.handle_evt(evt)?,
                                        Err(_) => break,
                                    },
                                }
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
                        error!(error = %error, "runtime2 hub machine failed");
                        Err(error)
                    }
                    Err(_) => Ok(()),
                }
            }
            .instrument(span),
        )
    }
}

/// Top-standing runtime spawn. Spawns background workers + the machine loop and
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
        + DocWorkerLoop<F>
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
        keyhive_event_notify,
    } = config;

    let keyhive_event_notify =
        keyhive_event_notify.unwrap_or_else(|| Arc::new(tokio::sync::Notify::new()));

    // Create two independent task sets for reverse-order shutdown.
    let child_tasks = tasks.task_set();
    let machine_tasks = tasks.task_set();

    let (runtime_abort, runtime_registration) = futures::future::AbortHandle::new_pair();

    let (cmd_tx, cmd_rx) = async_channel::unbounded::<Runtime2Cmd>();
    let (evt_tx, evt_rx) = event_channel.unwrap_or_else(async_channel::unbounded::<Runtime2Evt>);

    // The handle generates waiter ids; the hub tracks waiters per peer/doc
    // with its own round/request-id state (no hub-side watermark needed).
    let doc_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let keyhive_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));

    let hub: Runtime2Hub<F, R> = Runtime2Hub {
        local_peer_id,
        sync_policy,
        runtime_io: Arc::clone(&runtime_io),
        connect,
        doc_io,
        change_manager,
        child_tasks: child_tasks.clone(),
        timer: Arc::clone(&timer),
        clock: Arc::clone(&clock),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: HashMap::new(),
        keyhive_waiters: HashMap::new(),
        active_keyhive_syncs: HashMap::new(),
        keyhive_notif_pending: HashSet::new(),
        keyhive_round_ids: 0,
        group_part_settled_seq: 0,
        admitted_head: 0,
        keyhive_event_notify,
        keyhive_reconciliation_waiters: Vec::new(),
        pending_doc_syncs: HashMap::new(),
        quiescence_waiters: Vec::new(),
        quiescence_probe: None,
        quiescence_barrier_ids: 0,
        frozen: false,
        frozen_cmd_buffer: Vec::new(),
        freeze_on_resolve: false,
        cmd_closed: false,
        activity_generation: 0,
        tracked_in_flight: 0,
        doc_workers: HashMap::new(),
        pending_materialization: HashSet::new(),
        materialization_retries_in_flight: HashMap::new(),
        next_doc_worker_generation: 1,
    };

    let handle = Runtime2Handle::<F>::new(
        cmd_tx.clone(),
        hub.sync_policy,
        Arc::clone(&hub.timer),
        doc_sync_waiter_ids,
        keyhive_sync_waiter_ids,
    );

    // The dispatcher is stopped before child work so it cannot accept late
    // events that would spawn into an aborted child task set.
    machine_tasks.spawn(F::machine_loop(
        hub,
        cmd_rx,
        evt_rx,
        Arc::clone(&timer),
        runtime_registration,
    ))?;

    Ok((
        handle,
        Runtime2StopToken {
            cancel: runtime_abort,
            cmd_tx,
            child_tasks,
            machine_tasks,
            group_part_stop: None,
            causal_checkpoint_stop: None,
            automerge_frontier_stop: None,
        },
    ))
}
