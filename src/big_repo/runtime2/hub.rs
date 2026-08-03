//! `Runtime2Hub` — the runtime actor ("the machine loop").

use crate::interlude::*;

use crate::runtime2::doc_worker::DocWorkerLoop;
use crate::runtime2::{
    messages::{DocWorkerMsg, Runtime2Cmd, Runtime2Evt},
    DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, Runtime2Config, Runtime2Handle,
    TaskRuntime, TaskSet,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use future_form::{FutureForm, Local, Sendable};
use std::collections::{HashMap, HashSet};
use tracing::Instrument;
// Re-export the ephemeral so embedders can subscribe.

struct Runtime2Hub<F: FutureForm, R: TaskRuntime<F>> {
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
    pending_keyhive_syncs:
        HashMap<PeerId, Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>>,
    active_keyhive_syncs: HashMap<PeerId, KeyhiveSyncRound>,
    keyhive_round_ids: u64,
    keyhive_reconciliation_waiters: Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>,
    /// A quiescence probe is awaiting its event-log watermark.
    quiescence_group_part_watermark_pending: bool,

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
    activity_generation: u64,
    group_part_cursor: u64,

    // ── doc-worker registry ────────────────────────────────────────────────
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
    pending_materialization: HashSet<DocumentId>,
    materialization_retries_in_flight: HashSet<DocumentId>,
    // ── waiter-id counters (shared with the handle) ────────────────────────
    doc_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
    keyhive_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
}

struct ConnDeets {
    closed: Arc<std::sync::atomic::AtomicBool>,
}

struct KeyhiveSyncRound {
    watermark: u64,
    round_id: u64,
    request_id: subduction_keyhive::message::RequestId,
    changed: bool,
    validating: bool,
}

struct PendingDocSyncWaiter {
    doc_id: DocumentId,
    peer_id: PeerId,
    request_id: subduction_core::connection::message::RequestId,
    resp: futures::channel::oneshot::Sender<
        Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError>,
    >,
}

struct QuiescenceProbe {
    barrier_id: u64,
    activity_generation: u64,
    pending_docs: HashSet<DocumentId>,
    group_part_cursor: u64,
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
                    cmd_tx
                        .send(Runtime2Cmd::PutDoc {
                            doc_id,
                            initial_content,
                            resp,
                        })
                        .await
                        .expect(ERROR_CHANNEL);
                }
                Err(err) => {
                    resp.send(Err(err))
                        .inspect_err(|_| warn!(ERROR_CALLER))
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
            resp.send(stored).inspect_err(|_| warn!(ERROR_CALLER)).ok();
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
            resp.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();
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
            resp.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();
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
        debug!(
            barrier_id,
            local_peer_id = %self.local_peer_id,
            activity_generation = generation,
            doc_workers = doc_ids.len(),
            pending_materialization = self.pending_materialization.len(),
            group_part_cursor = self.group_part_cursor,
            "runtime2 quiescence probe started",
        );
        self.quiescence_probe = Some(QuiescenceProbe {
            barrier_id,
            activity_generation: generation,
            pending_docs: doc_ids.iter().copied().collect(),
            group_part_cursor: self.group_part_cursor,
        });
        self.quiescence_group_part_watermark_pending = true;
        self.spawn_background(F::capture_group_part_watermark(
            Arc::clone(&self.runtime_io),
            self.evt_tx.clone(),
            barrier_id,
        ))?;
        for doc_id in doc_ids {
            let (worker, lease) = self.doc_worker_handle(doc_id)?;
            worker
                .send(DocWorkerMsg::Quiesce {
                    barrier_id,
                    _lease: lease,
                })
                .wrap_err(ERROR_CHANNEL)?;
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
            || self.quiescence_group_part_watermark_pending
            || !self.active_keyhive_syncs.is_empty()
            || !self.pending_keyhive_syncs.is_empty()
            || self.group_part_cursor < probe.group_part_cursor
        {
            return Ok(());
        }
        debug!(
            local_peer_id = %self.local_peer_id,
            barrier_id = probe.barrier_id,
            activity_generation = probe.activity_generation,
            "runtime2 quiescence probe resolved",
        );
        self.quiescence_probe = None;
        for waiter in std::mem::take(&mut self.quiescence_waiters) {
            // A caller timeout drops the receiver; that cancellation is not
            // a runtime failure and must not crash the hub while resolving
            // other waiters.
            waiter
                .send(Ok(()))
                .inspect_err(|_| warn!(ERROR_CALLER))
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
                | Runtime2Cmd::RegisterDocLease { .. }
                | Runtime2Cmd::ReleaseDocLease { .. }
                | Runtime2Cmd::ReleaseInternalLease { .. }
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
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::GetDocHandle { doc_id, resp } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::AcquireHandle { resp })
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
                    .send(DocWorkerMsg::QueryHeadState { resp })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::InspectDocHeadState { doc_id, resp } => {
                let Some(entry) = self.doc_workers.get(&doc_id) else {
                    resp.send(Ok(None))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                    return self.try_resolve_quiescence();
                };
                entry
                    .handle
                    .send(DocWorkerMsg::InspectHeadState { resp })
                    .wrap_err(ERROR_CHANNEL)?;
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
                self.spawn_background(F::close_connection_async(
                    Arc::clone(&self.connect),
                    peer_id,
                    closed,
                    self.evt_tx.clone(),
                    resp,
                ))?;
            }
            Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout: _,
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
                        request_id: request_id.clone(),
                        resp,
                    },
                );
                let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_background(F::sync_doc_with_peer(
                    request_id,
                    Arc::clone(&self.runtime_io),
                    peer_id,
                    sed_id,
                    self.cmd_tx.clone(),
                ))?;
            }
            Runtime2Cmd::DocSyncRoundDone { request_id } => {
                // Transport round succeeded: resolve the waiter (if still
                // pending) with a worker reconsider so the receipt reflects
                // the fully-persisted tree.
                let Some(waiter) = self.pending_doc_syncs.remove(&request_id.nonce) else {
                    return self.try_resolve_quiescence();
                };
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
                waiter
                    .resp
                    .send(Err(error))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
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
                self.spawn_background(F::capture_keyhive_reconciliation(
                    Arc::clone(&self.runtime_io),
                    self.evt_tx.clone(),
                    resp,
                ))?;
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
                let entry = self
                    .doc_workers
                    .get_mut(&doc_id)
                    .expect("doc worker must exist before registering its bundle lease");
                entry.local_handles += 1;
                entry.eviction_deadline = None;
                registered.send(()).expect(ERROR_CHANNEL);
            }
            Runtime2Cmd::ReleaseDocLease { doc_id } => {
                self.handle_release_doc_lease(doc_id);
            }
            Runtime2Cmd::ReleaseInternalLease { doc_id } => {
                self.handle_release_internal_lease(doc_id);
            }
            Runtime2Cmd::ContainsSedimentree { doc_id, resp } => {
                let sedimentree_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
                self.spawn_background(F::contains_sedimentree(
                    Arc::clone(&self.runtime_io),
                    sedimentree_id,
                    resp,
                ))?;
            }
            Runtime2Cmd::HasLocalDocState { doc_id, resp } => {
                let has_doc_worker = self.doc_workers.contains_key(&doc_id);
                self.spawn_background(F::has_local_doc_state(
                    Arc::clone(&self.runtime_io),
                    doc_id,
                    has_doc_worker,
                    resp,
                ))?;
            }
            #[cfg(test)]
            Runtime2Cmd::HasDocWorker { doc_id, resp } => {
                resp.send(Ok(self.doc_workers.contains_key(&doc_id)))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
            Runtime2Cmd::InspectStoredDocBlobs { sed_id, resp } => {
                self.spawn_background(F::inspect_stored_doc_blobs(
                    Arc::clone(&self.runtime_io),
                    sed_id,
                    resp,
                ))?;
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

    fn capture_keyhive_reconciliation(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>>;
    fn capture_group_part_watermark(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        barrier_id: u64,
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
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
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
                            .expect(ERROR_CHANNEL);
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
                            .expect(ERROR_CHANNEL);
                    }
                }
                Ok(())
            }
            .instrument(span),
        )
    }

    fn capture_keyhive_reconciliation(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = runtime_io.keyhive_event_log_cursor().await;
            evt_tx
                .send(Runtime2Evt::KeyhiveReconciliationCaptured { result, resp })
                .await
                .expect(ERROR_CHANNEL);
            Ok(())
        })
    }

    fn capture_group_part_watermark(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        barrier_id: u64,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = runtime_io.keyhive_event_log_cursor().await;
            evt_tx
                .send(Runtime2Evt::QuiescenceGroupPartWatermark { barrier_id, result })
                .await
                .expect(ERROR_CHANNEL);
            Ok(())
        })
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
                .expect(ERROR_CHANNEL);
            Ok(())
        })
    }
    fn release_lease(
        lease_rx: futures::channel::oneshot::Receiver<()>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let _ = lease_rx.await;
            cmd_tx
                .send(Runtime2Cmd::ReleaseInternalLease { doc_id })
                .await
                .expect(ERROR_CHANNEL);
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
                        .inspect_err(|_| warn!(ERROR_CALLER))
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
                        let _ = watcher_end_tx.send((Arc::clone(&watcher_closed), result));
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
                            .inspect_err(|_| warn!(ERROR_CALLER))
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
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                }
                Err(error) => {
                    resp.send(Err(error))
                        .inspect_err(|_| warn!(ERROR_CALLER))
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
                        let _ = watcher_end_tx.send((Arc::clone(&watcher_closed), result));
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
                            .inspect_err(|_| warn!(ERROR_CALLER))
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
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                }
                Err(error) => {
                    resp.send(Err(error))
                        .inspect_err(|_| warn!(ERROR_CALLER))
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
        resp: Option<futures::channel::oneshot::Sender<eyre::Result<()>>>,
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
                        .expect(ERROR_CHANNEL);
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Some(resp) = resp {
                resp.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
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
                    .sync_doc_with_peer(sed_id, peer_id, Some(request_id.clone()))
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
                        cmd_tx
                            .send(Runtime2Cmd::DocSyncRoundDone { request_id })
                            .await
                            .expect(ERROR_CHANNEL);
                    }
                    Err(error) => {
                        cmd_tx
                            .send(Runtime2Cmd::DocSyncFailed { request_id, error })
                            .await
                            .expect(ERROR_CHANNEL);
                    }
                }
                Ok(())
            }
            .instrument(span),
        )
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
            Runtime2Evt::DocWorkerQuiescent { .. }
                | Runtime2Evt::QuiescenceGroupPartWatermark { .. }
                | Runtime2Evt::KeyhiveReconciliationCaptured { .. }
                | Runtime2Evt::GroupPartWorkerAdvanced { .. }
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
                self.finish_keyhive_sync(peer_id, request_id, changed)?;
            }
            Runtime2Evt::KeyhiveSyncFailed {
                peer_id,
                request_id,
                error,
            } => {
                self.fail_keyhive_sync(peer_id, request_id, error)?;
            }
            Runtime2Evt::QuiescenceGroupPartWatermark { barrier_id, result } => {
                let current = self
                    .quiescence_probe
                    .as_ref()
                    .is_some_and(|probe| probe.barrier_id == barrier_id);
                if current {
                    self.quiescence_group_part_watermark_pending = false;
                    match result {
                        Ok(cursor) => {
                            self.quiescence_probe
                                .as_mut()
                                .expect("quiescence probe disappeared")
                                .group_part_cursor = cursor;
                        }
                        Err(error) => {
                            self.quiescence_probe = None;
                            let message = error.to_string();
                            for waiter in std::mem::take(&mut self.quiescence_waiters) {
                                waiter
                                    .send(Err(ferr!(
                                        "group-part watermark capture failed: {message}"
                                    )))
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                        }
                    }
                }
            }
            Runtime2Evt::KeyhiveReconciliationCaptured { result, resp } => match result {
                Ok(watermark) if self.group_part_cursor >= watermark => {
                    resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                }
                Ok(watermark) => self.keyhive_reconciliation_waiters.push((watermark, resp)),
                Err(error) => {
                    resp.send(Err(error))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                }
            },
            Runtime2Evt::GroupPartWorkerAdvanced { cursor } => {
                self.group_part_cursor = self.group_part_cursor.max(cursor);
                let mut pending = Vec::new();
                for (watermark, waiter) in std::mem::take(&mut self.keyhive_reconciliation_waiters)
                {
                    if self.group_part_cursor >= watermark {
                        waiter
                            .send(Ok(()))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                    } else {
                        pending.push((watermark, waiter));
                    }
                }
                self.keyhive_reconciliation_waiters = pending;
            }
            Runtime2Evt::DocWorkerStopped { doc_id } => {
                self.doc_workers.remove(&doc_id);
                self.pending_materialization.remove(&doc_id);
                self.materialization_retries_in_flight.remove(&doc_id);
                if let Some(probe) = self.quiescence_probe.as_mut() {
                    probe.pending_docs.remove(&doc_id);
                }
            }
            Runtime2Evt::DocWorkerQuiescent { doc_id, barrier_id } => {
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
                debug!(
                    local_peer_id = %self.local_peer_id,
                    %doc_id,
                    pending_count = self.pending_materialization.len(),
                    worker_present = self.doc_workers.contains_key(&doc_id),
                    "document materialization entered pending set"
                );
                self.reattempt_pending_materialization()?;
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
                self.materialization_retries_in_flight.remove(&doc_id);
                match &status {
                    crate::runtime2::MaterializationStatus::Pending(blockers) => {
                        self.pending_materialization.insert(doc_id);
                        debug!(%doc_id, ?blockers, "materialization remains dependency-blocked");
                    }
                    crate::runtime2::MaterializationStatus::Missing
                    | crate::runtime2::MaterializationStatus::Ready { .. } => {
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
                    .expect(ERROR_CHANNEL);
                self.reattempt_pending_materialization()?;
            }
            Runtime2Evt::DelegationReceived { target, data } => {
                let member_id = PeerId::new(data.payload().delegate().id().to_bytes());
                let member_is_document = matches!(
                    data.payload().delegate(),
                    keyhive_core::principal::agent::Agent::Document(..)
                );
                self.spawn_background(F::emit_membership_change(
                    Arc::clone(&self.runtime_io),
                    Arc::clone(&self.change_manager),
                    target,
                    member_id,
                    crate::changes::BigRepoAccess::from(data.payload().can()),
                    false,
                    member_is_document,
                ))?;
            }
            Runtime2Evt::RevocationReceived { target, data } => {
                let member_id = PeerId::new(data.payload().revoked_id().as_bytes());
                let member_is_document = matches!(
                    data.payload().revoked().payload().delegate(),
                    keyhive_core::principal::agent::Agent::Document(..)
                );
                self.spawn_background(F::emit_membership_change(
                    Arc::clone(&self.runtime_io),
                    Arc::clone(&self.change_manager),
                    target,
                    member_id,
                    crate::changes::BigRepoAccess::Relay,
                    true,
                    member_is_document,
                ))?;
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
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            // Subduction has persisted the session. A later acquisition will
            // hydrate it directly when no worker currently exists.
            if let Some(reply) = reply {
                reply
                    .send(Ok(crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Stored,
                    }))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
            return Ok(());
        };
        entry.eviction_deadline = None;
        if entry.handle.is_closed() && entry.local_handles == 0 {
            debug!(
                doc_id = %doc_id,
                "discarding stale evicted document worker during sync routing"
            );
            self.doc_workers.remove(&doc_id);
            // The waiter was already removed from `pending_doc_syncs`;
            // resolve its receipt here so the caller does not hang.
            if let Some(reply) = reply {
                reply
                    .send(Ok(crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Stored,
                    }))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
            return Ok(());
        }
        entry
            .handle
            .send(DocWorkerMsg::ApplySyncSession {
                peer_id,
                commit_ids,
                fragment_ids,
                reply,
            })?;
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
                closed: closed.clone(),
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
        self.start_keyhive_sync_round(peer_id, false)
    }

    fn start_keyhive_sync_round(&mut self, peer_id: PeerId, validating: bool) -> eyre::Result<()> {
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
                changed: false,
                validating,
            },
        );
        debug!(
            %peer_id,
            round_id,
            ?request_id,
            watermark,
            validating,
            pending_waiters = self.pending_keyhive_syncs.get(&peer_id).map_or(0, Vec::len),
            "starting Keyhive sync round"
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
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, sender) in waiters {
                sender
                    .send(Err(ferr!("{error}")))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
        debug!(%peer_id, ?request_id, error, "keyhive sync initiation failed");
        Ok(())
    }

    /// Complete a keyhive protocol round and resolve any eligible waiters.
    fn finish_keyhive_sync(
        &mut self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
        changed: bool,
    ) -> eyre::Result<()> {
        let Some(round) = self.active_keyhive_syncs.get_mut(&peer_id) else {
            debug!(%peer_id, ?request_id, "processing untracked inbound keyhive completion");
            self.reattempt_pending_materialization()?;
            return Ok(());
        };
        // A concurrent inbound exchange can advance this peer's state while a
        // different request owns the explicit waiter. That progress still
        // invalidates the active round and requires an unchanged validation
        // round before its waiters may resolve.
        round.changed |= changed;
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
        let watermark = round.watermark;

        // Ingestion can change the pair view used by the just-completed
        // exchange. Keep every waiter pending until a subsequent unchanged
        // round validates the caller-sided fixed point.
        let has_preexisting_waiter = self
            .pending_keyhive_syncs
            .get(&peer_id)
            .is_some_and(|waiters| waiters.iter().any(|(id, _)| *id < watermark));
        if round.changed || (!round.validating && has_preexisting_waiter) {
            debug!(
                %peer_id,
                round_id,
                ?round.request_id,
                changed = round.changed,
                was_validation = round.validating,
                pending_waiters = self.pending_keyhive_syncs.get(&peer_id).map_or(0, Vec::len),
                "scheduling Keyhive validation round"
            );
            self.start_keyhive_sync_round(peer_id, true)?;
            self.reattempt_pending_materialization()?;
            return Ok(());
        }
        // Split waiters: those that existed before this sync started resolve,
        // those that arrived during/after cascade into a new round.
        let mut resolved_waiters = 0usize;
        if let Some(waiters) = self.pending_keyhive_syncs.get_mut(&peer_id) {
            let mut remaining = Vec::new();
            for (id, sender) in std::mem::take(waiters) {
                if id < watermark {
                    sender
                        .send(Ok(()))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                    resolved_waiters += 1;
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
        debug!(
            %peer_id,
            round_id,
            ?round.request_id,
            watermark,
            resolved_waiters,
            remaining_waiters = self.pending_keyhive_syncs.get(&peer_id).map_or(0, Vec::len),
            has_remaining,
            "completing Keyhive sync round"
        );
        if has_remaining {
            self.start_keyhive_sync(peer_id)?;
        }
        self.reattempt_pending_materialization()?;
        Ok(())
    }

    fn reattempt_pending_materialization(&mut self) -> eyre::Result<()> {
        let pending = self.pending_materialization.clone();
        debug!(
            local_peer_id = %self.local_peer_id,
            pending_count = pending.len(),
            "reattempting pending document materialization"
        );
        for doc_id in pending {
            if !self.materialization_retries_in_flight.insert(doc_id) {
                continue;
            }
            let Some(entry) = self.doc_workers.get(&doc_id) else {
                debug!(%doc_id, "dropping pending materialization without document worker");
                self.pending_materialization.remove(&doc_id);
                self.materialization_retries_in_flight.remove(&doc_id);
                self.schedule_doc_worker_eviction_if_idle(doc_id);
                continue;
            };
            debug!(
                %doc_id,
                local_handles = entry.local_handles,
                "requesting pending materialization retry from document worker"
            );
            let (resp, result) = futures::channel::oneshot::channel();
            if let Err(error) = entry
                .handle
                .send(DocWorkerMsg::ReattemptMaterialization {
                    origin: crate::changes::BigRepoChangeOrigin::Keyhive,
                    resp,
                })
            {
                self.materialization_retries_in_flight.remove(&doc_id);
                return Err(error).wrap_err(ERROR_CHANNEL);
            }
            self.spawn_background(F::forward_materialization_retry(
                result,
                self.evt_tx.clone(),
                doc_id,
            ))?;
        }
        Ok(())
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
        if let Some(waiters) = self.pending_keyhive_syncs.remove(peer_id) {
            for (_id, sender) in waiters {
                sender
                    .send(Err(eyre::eyre!("{reason}")))
                    .inspect_err(|_| warn!(ERROR_CALLER))
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

    /// Lazily spawn a doc-worker if none exists.
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

        let worker = crate::runtime2::spawn_doc_worker(
            doc_id,
            Arc::clone(&self.doc_io),
            Arc::clone(&self.change_manager),
            self.cmd_tx.clone(),
            self.evt_tx.clone(),
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
            },
        );
        Ok(())
    }

    /// Decrement `local_handles` for a doc-worker; schedule eviction if idle.
    fn handle_release_doc_lease(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            assert!(
                entry.local_handles > 0,
                "doc lease underflow for doc worker: {doc_id:?}"
            );
            entry.local_handles -= 1;
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    /// Decrement `internal_leases` for a doc-worker; schedule eviction if idle.
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
            let idle = self.doc_workers.get(&doc_id).is_some_and(|entry| {
                entry.local_handles == 0 && entry.internal_leases == 0
            });
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
/// - `machine_tasks` — the hub's dispatcher loop, stopped first so it cannot
///    dispatch work into an aborted child scope.
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
        let cancellation = registration.handle();
        let span = tracing::info_span!("runtime2_hub", local_peer_id = %hub.local_peer_id);
        F::from_future(
            async move {
                let result = futures::future::Abortable::new(
                    async move {
                        loop {
                            // FIXME: why do we need to allocate and box every loop?
                            let mut sleep =
                                Box::pin(timer.sleep(std::time::Duration::from_millis(500)).fuse());
                            let mut cmd = Box::pin(cmd_rx.recv().fuse());
                            let mut evt = Box::pin(evt_rx.recv().fuse());
                            futures::select_biased! {
                                _ = sleep.as_mut() => hub.janitor_tick(),
                                evt = evt.as_mut() => match evt {
                                    Ok(evt) => hub.handle_evt(evt)?,
                                    Err(_) => break,
                                },
                                cmd = cmd.as_mut() => match cmd {
                                    Ok(cmd) => hub.handle_cmd(cmd)?,
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

// ═══════════════════════════════════════════════════════════════════════════
// SPAWN
// ═══════════════════════════════════════════════════════════════════════════

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
    let hub: Runtime2Hub<F, R> = Runtime2Hub {
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
        keyhive_reconciliation_waiters: Vec::new(),
        quiescence_group_part_watermark_pending: false,
        pending_doc_syncs: HashMap::new(),
        quiescence_waiters: Vec::new(),
        quiescence_probe: None,
        quiescence_barrier_ids: 0,
        activity_generation: 0,
        group_part_cursor: 0,
        doc_workers: HashMap::new(),
        pending_materialization: HashSet::new(),
        materialization_retries_in_flight: HashSet::new(),
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
