//! `Runtime2Hub` — the runtime actor ("the machine loop").

use crate::interlude::*;

use crate::DocumentId;
use crate::runtime2::doc_worker::DocWorkerLoop;
use crate::runtime2::{
    DocWorkerEntry, DocWorkerHandle, DocWorkerInternalLease, Runtime2Config, Runtime2Handle,
    TaskRuntime, TaskSet,
    messages::{DocWorkerMsg, Runtime2Cmd, Runtime2Evt},
};
use big_sync_core::PeerKey;
use future_form::{FutureForm, Local, Sendable};
use std::collections::{HashMap, HashSet};
use tracing::Instrument;
// Re-export the ephemeral so embedders can subscribe.

pub(crate) struct Runtime2Hub<F: FutureForm, R: TaskRuntime<F>> {
    // ── identity / config ──────────────────────────────────────────────────
    local_peer_id: PeerKey,
    sync_policy: crate::runtime2::types::BigRepoSyncPolicy,
    /// When false, `ConnEstablished` skips the initial keyhive sync round
    /// (mirrors `BigRepoConfig::keyhive_change_notifs`; test-only).
    keyhive_sync_on_connect: bool,

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

    /// Span owned by this hub. Long-lived workers spawned by the hub parent
    /// their own spans here so their logs cannot masquerade as children of
    /// whichever command happened to spawn them.
    span: tracing::Span,

    // ── task sets ──────────────────────────────────────────────────────────
    /// Child/background tasks (construction-time, keyhive syncs, lease
    /// waiters, doc-workers). Stopped first on shutdown (children before
    /// the hub loop).
    child_tasks: R::Tasks,

    // ── determinism levers ─────────────────────────────────────────────────
    // timer: Arc<dyn crate::runtime2::Timer<F>>,
    clock: Arc<dyn crate::runtime2::Clock>,

    // ── channels ───────────────────────────────────────────────────────────
    cmd_tx: async_channel::Sender<Runtime2Cmd>,
    evt_tx: async_channel::Sender<Runtime2Evt>,

    // ── connection state ───────────────────────────────────────────────────
    connected_peers: HashMap<PeerKey, ConnDeets>,

    // ── keyhive sync bookkeeping ───────────────────────────────────────────
    /// Keyhive sync waiters per peer. Each round snapshots the waiter ids it
    /// owns (`KeyhiveSyncRound::admitted_ids`); waiters admitted during the
    /// round stay queued and cascade to the next round. `ids` mirrors the vec
    /// for O(1) cancellation (dead waiters never trigger a follow-up round).
    keyhive_waiters: HashMap<PeerKey, KeyhiveWaiters>,
    active_keyhive_syncs: HashMap<PeerKey, KeyhiveSyncRound>,
    /// A `KeyhiveChangeNotif` that arrived while a round for the peer was
    /// already active. The in-flight exchange may have synced stale state;
    /// when the round completes, a follow-up round is started for the peer.
    keyhive_notif_pending: HashSet<PeerKey>,
    keyhive_round_ids: u64,
    keyhive_reconciliation_waiters: Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>,
    /// Highest admission-log seq the group-part projection has settled
    /// (from worker announcements).
    group_part_settled_seq: u64,
    /// Highest admission-log seq known incorporated (from the admission
    /// writer). `WaitForKeyhiveReconciliation` captures this and resolves
    /// once `group_part_settled_seq` covers it.
    admitted_head: u64,

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
    /// Test-only: queue events instead of handling them (see
    /// [`Runtime2Cmd::HoldEvents`]). Zero production cost — absent from
    /// non-test builds along with its command arms.
    #[cfg(test)]
    hold_events: bool,
    /// Test-only: the events queued while `hold_events` is set, replayed in
    /// channel order by [`Runtime2Cmd::ResumeEvents`].
    #[cfg(test)]
    held_events: Vec<Runtime2Evt>,
    /// Test-only: fail the next content-carrying sync-session apply route for
    /// this document by closing the worker's mailbox, reproducing the
    /// worker-stopping race deterministically (see
    /// [`Runtime2Cmd::FailNextContentApplyRoute`]).
    #[cfg(test)]
    poisoned_content_apply: Option<DocumentId>,
    /// Set once the commands channel closes (the stop token dropped its
    /// sender); no new background work is admitted and the machine loop
    /// exits once tracked work drains.
    cmd_closed: bool,
    activity_generation: u64,
    /// Finite background futures admitted via `spawn_tracked` that have not
    /// yet completed. The quiescence predicate waits for this to drain, so
    /// in-flight work started before a probe cannot resolve it early (A2/A5).
    tracked_in_flight: u64,
    /// Per-kind breakdown of `tracked_in_flight`, so a stalled quiescence fence
    /// can name the work that never reported instead of only its count.
    tracked_work: HashMap<crate::runtime2::TrackedWorkKind, u64>,
    /// When the current quiescence wait started, and when to next report the
    /// stall. Both cleared as soon as quiescence resolves.
    quiescence_stall_since: Option<std::time::Instant>,
    next_quiescence_stall_report: Option<std::time::Instant>,
    // ── doc-worker registry ────────────────────────────────────────────────
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
    pending_materialization: HashSet<DocumentId>,
    /// Documents with a materialization retry in flight, mapped to the Keyhive
    /// state generation at retry start. A `Pending` completion whose walk ran
    /// against a generation older than the current one is re-verified (B6).
    materialization_retries_in_flight: HashMap<DocumentId, u64>,
    /// Documents whose materialization retry request arrived while another walk
    /// was already in flight. That walk had read its inputs before the request, so
    /// it cannot observe what triggered it; the request is latched here and
    /// re-issued when the in-flight walk completes. Dropping it instead leaves the
    /// document labelled one CGKA operation behind with no further wakeup.
    materialization_retries_requested: HashSet<DocumentId>,
    next_doc_worker_generation: u64,
}

struct ConnDeets {
    closed: Arc<std::sync::atomic::AtomicBool>,
}
/// Test-only orphan-round detector.
///
/// A round is retired by the protocol completion, protocol error, or
/// connection-loss paths. A round that outlives this bound means one of those
/// silently failed; a test must surface that instead of hanging on it.
#[cfg(any(test, feature = "test-support"))]
const KEYHIVE_SYNC_ROUND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Production slow-round reporting threshold.
///
/// There is deliberately no internal round timeout outside tests: we expect to
/// run on slow networks, and an application-level deadline belongs to the
/// caller, which can cancel a `sync_keyhive` call safely — the caller's waiter
/// guard removes the waiter, so a cancelled call never cascades a follow-up
/// round. A round that outlives this bound is suspicious, so report it once and
/// keep waiting rather than failing a transfer the peer may still complete.
#[cfg(not(any(test, feature = "test-support")))]
const KEYHIVE_SYNC_SLOW_ROUND_WARN: std::time::Duration = std::time::Duration::from_secs(300);

/// A Keyhive sync round that outlived its reporting threshold, captured so the
/// janitor can report it without holding a borrow on the round map.
struct UnresolvedKeyhiveRound {
    round_id: u64,
    request_id: subduction_keyhive::message::RequestId,
    elapsed_secs: u64,
    admitted_waiters: usize,
}

impl KeyhiveSyncRound {
    /// Latch and describe this round when it has outlived `threshold` and was
    /// not reported yet. Latching keeps a stuck round from being reported on
    /// every janitor tick; `None` means "not reportable".
    fn latch_if_unresolved(
        &mut self,
        now: std::time::Instant,
        threshold: std::time::Duration,
    ) -> Option<UnresolvedKeyhiveRound> {
        if self.slow_warned {
            return None;
        }
        let elapsed = now.saturating_duration_since(self.started_at);
        if elapsed < threshold {
            return None;
        }
        self.slow_warned = true;
        Some(UnresolvedKeyhiveRound {
            round_id: self.round_id,
            request_id: self.request_id.clone(),
            elapsed_secs: elapsed.as_secs(),
            admitted_waiters: self.admitted_ids.len(),
        })
    }
}

struct KeyhiveSyncRound {
    round_id: u64,
    started_at: std::time::Instant,
    request_id: subduction_keyhive::message::RequestId,
    /// Latches the one-shot slow-round report so a stuck round warns once
    /// instead of on every janitor tick. Tests read it too: a round that was
    /// already reported is not the orphan their panic detector looks for.
    slow_warned: bool,
    /// Ids of the waiters queued when this round started; the round resolves
    /// them on completion. Waiters admitted during the round cascade.
    admitted_ids: std::collections::HashSet<u64>,
    /// Set when a completion arrived whose admission watermark the hub's
    /// `admitted_head` had not yet reached. The round is held (waiters
    /// unresolved, no follow-up round) until `KeyhiveAdmissionAdvanced` brings
    /// `admitted_head` up to this seq, at which point the round finishes.
    pending_admission_seq: Option<u64>,
}

/// Keyhive sync waiters for one peer: those owned by the active round plus
/// those cascading to the next. `ids` mirrors `waiters` for O(1) cancellation.
#[derive(Default)]
struct KeyhiveWaiters {
    waiters: Vec<(u64, futures::channel::oneshot::Sender<eyre::Result<()>>)>,
    ids: std::collections::HashSet<u64>,
}

/// Select one follow-up round for demand that arrived while a round was
/// active. A queued waiter is already an explicit demand for that round, so a
/// notification latched for the same peer is consumed rather than creating a
/// second round.
fn coalesce_keyhive_demand(has_remaining_waiters: bool, notification_pending: &mut bool) -> bool {
    if has_remaining_waiters {
        *notification_pending = false;
        true
    } else {
        std::mem::take(notification_pending)
    }
}

struct PendingDocSyncWaiter {
    doc_id: DocumentId,
    peer_id: PeerKey,
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
    fn allocate_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::DocumentId>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn finalize_allocated_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: crate::DocumentId,
        initial_content: Box<automerge::Automerge>,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        pending_group: crate::keyhive::BigKeyhiveGroup,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::LiveDocHandle>,
        >,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn create_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::LiveDocHandle>,
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

/// How long a quiescence fence may stall before the hub reports what is still
/// in flight. Diagnostic only: the fence itself has no timeout.
const QUIESCENCE_STALL_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> HubCommandFuture<F> for F {
    fn allocate_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::DocumentId>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = runtime_io.allocate_document(parents).await;
            resp.send(result).map_err(|_| ferr!(ERROR_CHANNEL))?;
            Ok(())
        })
    }

    fn finalize_allocated_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        doc_id: crate::DocumentId,
        initial_content: Box<automerge::Automerge>,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        pending_group: crate::keyhive::BigKeyhiveGroup,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::LiveDocHandle>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = async {
                let content_heads = nonempty::NonEmpty::from_vec(
                    initial_content
                        .get_heads()
                        .into_iter()
                        .map(|head| head.0)
                        .collect(),
                )
                .ok_or_else(|| ferr!("automerge document has no content heads"))?;
                let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32());
                // Stage the plaintext before creating the Keyhive authority.
                // This is the recovery record for a crash in any later step.
                let already_persisted = runtime_io.contains_sedimentree(sed_id).await?;
                runtime_io
                    .stage_allocated_document(
                        doc_id.clone(),
                        initial_content.save(),
                        initial_keys.clone(),
                        already_persisted,
                    )
                    .await?;
                // The initial content is encrypted against the Keyhive document,
                // so authority creation precedes Sedimentree persistence.
                runtime_io
                    .finalize_document_authority(doc_id.clone(), content_heads.clone())
                    .await?;
                let handle = if runtime_io.contains_sedimentree(sed_id).await? {
                    let (handle_resp, handle_rx) = futures::channel::oneshot::channel();
                    cmd_tx
                        .send(Runtime2Cmd::GetDocHandle {
                            doc_id: doc_id.clone(),
                            lease: crate::runtime2::DocLeaseKind::Caller,
                            resp: handle_resp,
                        })
                        .await
                        .map_err(|_| ferr!(ERROR_ACTOR))?;
                    let lookup = handle_rx.await.map_err(|_| ferr!(ERROR_CHANNEL))??;
                    let handle = match lookup {
                        crate::runtime2::types::DocLookup::Ready(handle) => handle,
                        crate::runtime2::types::DocLookup::Missing => {
                            return Err(ferr!(
                                "persisted document has no materialized handle: {doc_id}"
                            ));
                        }
                        crate::runtime2::types::DocLookup::PendingMaterialization => {
                            return Err(ferr!(
                                "persisted document is pending materialization: {doc_id}"
                            ));
                        }
                    };
                    let (persisted_heads, persisted_content) = surelock::key::lock_scope(|key| {
                        let (doc, _key) = key.lock(&handle.bundle.doc);
                        (
                            doc.get_heads()
                                .into_iter()
                                .map(|head| head.0)
                                .collect::<std::collections::BTreeSet<_>>(),
                            doc.save(),
                        )
                    });
                    let requested_heads = content_heads
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>();
                    if persisted_heads != requested_heads
                        || persisted_content != initial_content.save()
                    {
                        return Err(ferr!(
                            "persisted document initial content mismatch: {doc_id}"
                        ));
                    }
                    handle
                } else {
                    let (put_resp, put_rx) = futures::channel::oneshot::channel();
                    cmd_tx
                        .send(Runtime2Cmd::PutDoc {
                            doc_id: doc_id.clone(),
                            initial_content,
                            initial_keys: initial_keys.clone(),
                            resp: put_resp,
                        })
                        .await
                        .map_err(|_| ferr!(ERROR_ACTOR))?;
                    put_rx.await.map_err(|_| ferr!(ERROR_CHANNEL))??
                };
                runtime_io
                    .complete_document_authority(doc_id, pending_group, content_heads)
                    .await?;
                eyre::Ok(handle)
            }
            .await;
            resp.send(result).map_err(|_| ferr!(ERROR_CHANNEL))?;
            Ok(())
        })
    }

    fn create_doc(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        initial_content: Box<automerge::Automerge>,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime2::types::LiveDocHandle>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            match runtime_io.create_document(parents, content_heads).await {
                Ok(doc_id) => {
                    match cmd_tx
                        .send(Runtime2Cmd::PutDoc {
                            doc_id,
                            initial_content,
                            initial_keys: Vec::new(),
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
            // The document id arrives from the sync backend, so its width is peer input:
            // derive the fixed-width sedimentree id fallibly and report the failure to the
            // caller's receipt instead of the hub loop.
            let result = async {
                if has_doc_worker {
                    return Ok(true);
                }
                runtime_io
                    .contains_sedimentree(sedimentree_core::id::SedimentreeId::new(
                        doc_id.try_to_bytes32()?,
                    ))
                    .await
            }
            .await;
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
        if self.quiescence_stall_since.is_none() {
            self.quiescence_stall_since = Some(self.clock.instant());
            self.next_quiescence_stall_report =
                Some(self.clock.instant() + QUIESCENCE_STALL_REPORT_INTERVAL);
        }
        if self.quiescence_probe.is_none() {
            self.start_quiescence_probe()?;
        }
        self.try_resolve_quiescence()
    }

    fn start_quiescence_probe(&mut self) -> eyre::Result<()> {
        self.quiescence_barrier_ids = self.quiescence_barrier_ids.wrapping_add(1);
        let barrier_id = self.quiescence_barrier_ids;
        let generation = self.activity_generation;
        let doc_ids: Vec<_> = self.doc_workers.keys().cloned().collect();
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
            pending_docs: doc_ids.iter().cloned().collect(),
            group_part_settled_seq,
        });
        for doc_id in doc_ids {
            let (worker, lease) = self.doc_worker_handle(doc_id.clone())?;
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
        self.quiescence_stall_since = None;
        self.next_quiescence_stall_report = None;
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
        let control_only = matches!(
            &cmd,
            Runtime2Cmd::WaitForQuiescence { .. }
                | Runtime2Cmd::Unfreeze
                | Runtime2Cmd::RegisterDocLease { .. }
                | Runtime2Cmd::ReleaseDocLease { .. }
                | Runtime2Cmd::ReleaseInternalLease { .. }
                // Read-only doc-worker queries: never restart a pending probe,
                // or a caller polling head state while waiting for quiescence
                // would restart it forever. Everything that routes *work* into
                // a worker — `PutDoc`, `CommitDelta`, `GetDocHandle`'s
                // `AcquireHandle`, `ApplySyncSession`, and
                // `EnsureCausalCoverage`'s `ReconcileCausalCoverage` — is
                // activity and must bump, so a probe placed before that routing
                // restarts and fences the work rather than resolving over it.
                | Runtime2Cmd::DocHeadState { .. }
                | Runtime2Cmd::InspectDocHeadState { .. }
        );
        // The test-only event-hold seam performs no domain work, so it must not
        // restart a quiescence probe either; the same goes for the test-only
        // state/barrier queries.
        #[cfg(test)]
        let control_only = control_only
            || matches!(
                &cmd,
                Runtime2Cmd::HoldEvents { .. }
                    | Runtime2Cmd::ResumeEvents { .. }
                    | Runtime2Cmd::HasDocWorker { .. }
                    | Runtime2Cmd::HasConnectedPeer { .. }
                    | Runtime2Cmd::QuiescenceProbeBarrierForTest { .. }
            );
        if !control_only {
            self.note_activity();
        }
        match cmd {
            Runtime2Cmd::AllocateDoc { parents, resp } => {
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::CreateDoc,
                    F::allocate_doc(Arc::clone(&self.runtime_io), parents, resp),
                )?;
            }
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
                initial_keys,
                resp,
            } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::PutDoc {
                        initial_content,
                        initial_keys,
                        resp,
                        _lease,
                    })
                    .wrap_err(ERROR_CHANNEL)?;
            }
            Runtime2Cmd::FinalizeAllocatedDoc {
                doc_id,
                initial_content,
                initial_keys,
                pending_group,
                resp,
            } => {
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::CreateDoc,
                    F::finalize_allocated_doc(
                        Arc::clone(&self.runtime_io),
                        self.cmd_tx.clone(),
                        doc_id,
                        initial_content,
                        initial_keys,
                        pending_group,
                        resp,
                    ),
                )?;
            }
            Runtime2Cmd::GetDocHandle {
                doc_id,
                lease,
                resp,
            } => {
                let (worker, _lease) = self.doc_worker_handle(doc_id)?;
                worker
                    .send(DocWorkerMsg::AcquireHandle {
                        lease,
                        resp,
                        _lease,
                    })
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
                if let Ok(Some((worker, _lease))) =
                    self.acquire_existing_doc_worker_handle(doc_id.clone())
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
                // The connection is being closed by its consumer: mark it dead
                // up front so syncs through its handle fail fast, and
                // deregister it *before* `close_connection_async` resolves the
                // caller's response. That ordering is the point — a sync issued
                // the instant `close_connection` returns must not start a round
                // against the connection this call closed. Only the peer's
                // *current* connection owns the peer's registration; a
                // superseded connection's close must not disturb the
                // replacement (the closed flag's pointer identity is the
                // connection id, matching `handle_connection_lost`'s stale-conn
                // gate).
                closed.store(true, std::sync::atomic::Ordering::SeqCst);
                if !self.deregister_connection(&peer_id, &closed, "keyhive peer closed") {
                    debug!(
                        peer_id = %peer_id,
                        "close left no current registration to tear down \
                         (superseded, or its establishment had not landed)"
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
                        self.local_peer_id.to_bytes32(),
                    ),
                    nonce: waiter_id,
                };
                // The document id reaches this command from the sync backend, so its width
                // is peer input: derive the fixed-width sedimentree id fallibly, and fail
                // this sync attempt rather than the hub loop when it does not fit.
                let doc_key = match doc_id.try_to_bytes32() {
                    Ok(doc_key) => doc_key,
                    Err(error) => {
                        resp.send(Err(crate::runtime2::types::SyncDocError::Other(error)))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                        return self.try_resolve_quiescence();
                    }
                };
                self.pending_doc_syncs.insert(
                    waiter_id,
                    PendingDocSyncWaiter {
                        doc_id: doc_id.clone(),
                        peer_id: peer_id.clone(),
                        resp,
                    },
                );
                let sed_id = sedimentree_core::id::SedimentreeId::new(doc_key);
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::SyncDoc,
                    F::sync_doc_with_peer(
                        request_id,
                        Arc::clone(&self.runtime_io),
                        peer_id,
                        sed_id,
                        self.evt_tx.clone(),
                    ),
                )?;
            }
            Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            } => {
                let entry = self.keyhive_waiters.entry(peer_id.clone()).or_default();
                entry.ids.insert(waiter_id);
                entry.waiters.push((waiter_id, resp));
                let queued_waiters = entry.waiters.len();
                tracing::debug!(
                    local_peer_id = %self.local_peer_id,
                    %peer_id,
                    waiter_id,
                    queued_waiters,
                    connected = self.connected_peers.contains_key(&peer_id),
                    active_round = self.active_keyhive_syncs.contains_key(&peer_id),
                    admitted_head = self.admitted_head,
                    group_part_settled_seq = self.group_part_settled_seq,
                    "keyhive sync waiter enqueued"
                );
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
                tracing::debug!(
                    local_peer_id = %self.local_peer_id,
                    captured,
                    group_part_settled_seq = self.group_part_settled_seq,
                    active_keyhive_syncs = self.active_keyhive_syncs.len(),
                    "keyhive reconciliation wait requested"
                );
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
                    self.spawn_doc_worker(doc_id.clone())?;
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
                let sedimentree_id = sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32());
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
            #[cfg(test)]
            Runtime2Cmd::HasConnectedPeer { peer_id, resp } => {
                resp.send(Ok(self.connected_peers.contains_key(&peer_id)))
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
            #[cfg(test)]
            Runtime2Cmd::HoldEvents { resp } => {
                self.hold_events = true;
                debug!(local_peer_id = %self.local_peer_id, "holding hub event processing (test seam)");
                resp.send(()).inspect_err(|_| warn_loc!(ERROR_CALLER)).ok();
            }
            #[cfg(test)]
            Runtime2Cmd::ResumeEvents { resp } => {
                self.hold_events = false;
                let held = std::mem::take(&mut self.held_events);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    held = held.len(),
                    "resuming hub event processing (test seam)"
                );
                for evt in held {
                    self.handle_evt(evt)?;
                }
                resp.send(()).inspect_err(|_| warn_loc!(ERROR_CALLER)).ok();
            }
            #[cfg(test)]
            Runtime2Cmd::FailNextContentApplyRoute { doc_id, resp } => {
                self.poisoned_content_apply = Some(doc_id);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    "arming next content-apply route failure (test seam)"
                );
                resp.send(()).inspect_err(|_| warn_loc!(ERROR_CALLER)).ok();
            }
            #[cfg(test)]
            Runtime2Cmd::InjectKeyhiveCompletionForTest { peer_id, resp } => {
                let result = match self.active_keyhive_syncs.get(&peer_id) {
                    Some(round) => {
                        let request_id = round.request_id.clone();
                        // One seq ahead of the hub's current head: the
                        // completion must be deferred until a matching
                        // admission event lands.
                        let admitted_seq = self.admitted_head.wrapping_add(1);
                        self.handle_keyhive_sync_done(peer_id, request_id, admitted_seq)
                            .map(|()| admitted_seq)
                    }
                    None => Err(ferr!("no active keyhive round for {peer_id}")),
                };
                resp.send(result)
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            #[cfg(test)]
            Runtime2Cmd::InjectRuntime2EvtForTest { evt, resp } => {
                let result = self.handle_evt(*evt);
                resp.send(result)
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            #[cfg(test)]
            Runtime2Cmd::QuiescenceProbeBarrierForTest { resp } => {
                resp.send(self.quiescence_probe.as_ref().map(|probe| probe.barrier_id))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
        }
        self.try_resolve_quiescence()
    }
}

pub(crate) trait HubBackgroundFuture<F: FutureForm> {
    fn start_sync(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerKey,
        request_id: subduction_keyhive::message::RequestId,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn emit_membership_change(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        change_manager: Arc<crate::changes::ChangeListenerManager>,
        target: keyhive_core::principal::identifier::Identifier,
        member_id: PeerKey,
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
    /// Persist a durable prekey-state snapshot (published membership ops +
    /// secret halves) via the shared runtime IO facade. Best-effort; failures
    /// are logged by the implementation.
    fn persist_prekey_state_snapshot(
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
    ) -> F::Future<'static, eyre::Result<()>>;
    /// Wrap a finite background future so a `TrackedWorkDone` event is emitted
    /// after its own emissions (keeps channel order for the in-flight counter).
    ///
    /// The wrapper owns the [`TrackedWorkGuard`](crate::runtime2::TrackedWorkGuard)
    /// from construction, not just from its first poll: the in-flight count is
    /// incremented in [`spawn_tracked`](Runtime2Hub::spawn_tracked) before this
    /// future exists, so the matching decrement must survive a task that is
    /// dropped or aborted before it is ever polled.
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
        peer: PeerKey,
        addr: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerKey,
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
                PeerKey,
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
        peer_id: PeerKey,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>>;

    fn sync_doc_with_peer(
        request_id: subduction_core::connection::message::RequestId,
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
        peer_id: PeerKey,
        sed_id: sedimentree_core::id::SedimentreeId,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> HubBackgroundFuture<F> for F {
    fn start_sync(
        runtime_io: Arc<dyn crate::runtime2::RuntimeIo<F>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        peer_id: PeerKey,
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
                    .sync_keyhive_with_peer(peer_id.clone(), request_id.clone())
                    .await
                {
                    Ok(crate::runtime2::KeyhiveSyncOutcome::Initiated) => {
                        // TEMP-DIAGNOSTIC: rounds completing with an empty exchange
                        // (serving side sends 0 for an explicit hash request) leave no
                        // trace of the round at default levels; log every round start,
                        // visible under `RUST_LOG_TEST=debug`.
                        tracing::debug!(
                            %peer_id,
                            nonce = request_id.nonce,
                            "KEYHIVE_DIAG keyhive sync round initiated"
                        );
                    }
                    Ok(crate::runtime2::KeyhiveSyncOutcome::PeerDisappeared) => {
                        evt_tx
                            .send(Runtime2Evt::KeyhiveSyncFailed {
                                peer_id: peer_id.clone(),
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
        member_id: PeerKey,
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
                        DocumentId::new(member_id.0.as_bytes()),
                        group_id,
                    )?;
                } else {
                    change_manager.notify_document_added_to_group(
                        DocumentId::new(member_id.0.as_bytes()),
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
    fn persist_prekey_state_snapshot(
        runtime_io: std::sync::Arc<dyn crate::runtime2::RuntimeIo<F>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            runtime_io
                .persist_prekey_state()
                .await
                .inspect_err(|err| {
                    warn_loc!("persisting prekey state snapshot failed: {err:#}");
                })
                .map(|_| ())
        })
    }
    fn track_work(
        kind: crate::runtime2::TrackedWorkKind,
        fut: F::Future<'static, eyre::Result<()>>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>> {
        // Construct the guard *before* wrapping the future and let the wrapper
        // own it, rather than creating it inside the async body on first poll.
        // The in-flight count is incremented in `spawn_tracked` before this
        // future exists, so the decrement must be structural: `Abortable` (the
        // task set's wrapper) drops an aborted task's future without polling
        // it, and a body-created guard would then never be dropped — leaking
        // the count so the shutdown drain never completes. Owning it here means
        // it drops on every path (completion, abort, or an unpolled drop), and
        // it still drops *after* `fut`, preserving `TrackedWorkDone` ordering
        // for the normal path.
        let guard = crate::runtime2::TrackedWorkGuard::new(evt_tx, kind);
        F::from_future(async move {
            let _guard = guard;
            fut.await
        })
    }
}

#[future_form::future_form(Sendable where Tasks: Send, Local)]
impl<F: FutureForm, Tasks: crate::runtime2::TaskSet<F>> HubIoFutures<F, Tasks> for F {
    fn open_connection_and_watch(
        connect: std::sync::Arc<dyn crate::runtime2::TransportConnect<F>>,
        peer: PeerKey,
        addr: Box<dyn std::any::Any + Send>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        child_tasks: Tasks,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<(
                PeerKey,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                futures::channel::oneshot::Receiver<(
                    std::sync::Arc<std::sync::atomic::AtomicBool>,
                    eyre::Result<()>,
                )>,
            )>,
        >,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let dial_started = std::time::Instant::now();
            tracing::debug!(%peer, "dialing peer");
            let dial_result = connect.connect(peer.clone(), addr).await;
            tracing::debug!(
                %peer,
                elapsed_ms = dial_started.elapsed().as_millis() as u64,
                ok = dial_result.is_ok(),
                "dial returned"
            );
            match dial_result {
                Ok((handshake_peer, closed, end_fut)) => {
                    if handshake_peer != peer {
                        // The connector has already authenticated the peer;
                        // close that authenticated connection before rejecting
                        // the caller's expected-target mismatch.
                        connect.close(handshake_peer.clone(), closed).await?;
                        resp.send(Err(ferr!(
                            "handshake peer mismatch: expected {peer}, got {handshake_peer}"
                        )))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                        return Ok(());
                    }
                    let (end_tx, end_rx) = futures::channel::oneshot::channel();
                    let watcher_closed = Arc::clone(&closed);
                    let watcher_peer = handshake_peer.clone();
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
                                peer_id: watcher_peer.clone(),
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
                            peer_id: handshake_peer.clone(),
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
                PeerKey,
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
                    let watcher_peer = handshake_peer.clone();
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
                                peer_id: watcher_peer.clone(),
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
                            peer_id: handshake_peer.clone(),
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
        peer_id: PeerKey,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        evt_tx: async_channel::Sender<Runtime2Evt>,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = match connect.close(peer_id.clone(), closed).await {
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
        peer_id: PeerKey,
        sed_id: sedimentree_core::id::SedimentreeId,
        evt_tx: async_channel::Sender<Runtime2Evt>,
    ) -> F::Future<'static, eyre::Result<()>> {
        let span = tracing::debug_span!(
            "document_sync",
            request_nonce = request_id.nonce,
            remote_peer_id = %peer_id,
            document_id = %DocumentId::new(sed_id.as_bytes()),
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
                        // Report the round's completion on the event channel:
                        // the round's session observation(s) are emitted into
                        // it before this signal is sent, so the hub cannot
                        // resolve the caller's receipt before the session that
                        // carries this round's content has been applied. A
                        // closed event channel means the runtime is draining;
                        // the waiter is dropped with the hub and the caller
                        // observes the closure.
                        evt_tx
                            .send(Runtime2Evt::DocSyncRoundDone { request_id })
                            .await
                            .inspect_err(|_| warn_loc!(ERROR_CHANNEL))
                            .ok();
                    }
                    Err(error) => {
                        evt_tx
                            .send(Runtime2Evt::DocSyncFailed { request_id, error })
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
        *self.tracked_work.entry(kind).or_insert(0) += 1;
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
    /// Hand `evt` to the hub, unless event processing is held by the test-only
    /// [`Runtime2Cmd::HoldEvents`] seam, in which case it is queued for
    /// [`Runtime2Cmd::ResumeEvents`]. One gate for the machine loop, so the
    /// hold covers every event path (including the shutdown drain).
    fn handle_or_hold_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        #[cfg(test)]
        if self.hold_events {
            self.held_events.push(evt);
            return Ok(());
        }
        self.handle_evt(evt)
    }

    #[tracing::instrument(skip(self))]
    fn handle_evt(&mut self, evt: Runtime2Evt) -> eyre::Result<()> {
        trace!(?evt, "runtime2 event received");
        match &evt {
            Runtime2Evt::SyncSessionObserved { session, .. } => {
                debug!(
                    local_peer_id = %self.local_peer_id,
                    doc_id = %DocumentId::new(session.sedimentree_id.as_bytes()),
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
                admitted_seq,
            } => debug!(
                local_peer_id = %self.local_peer_id,
                %peer_id,
                ?request_id,
                changed,
                admitted_seq,
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
                    document_id = %DocumentId::new(session.sedimentree_id.as_bytes()),
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
                changed: _,
                admitted_seq,
            } => {
                self.handle_keyhive_sync_done(peer_id, request_id, admitted_seq)?;
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
            Runtime2Evt::DocSyncRoundDone { request_id } => {
                // Transport round succeeded: resolve the waiter (if still
                // pending) with a worker reconsider so the receipt reflects
                // the fully-persisted tree. Reaching here means every session
                // observation this round emitted has already been routed (the
                // round sent them into this channel first), so a received
                // commit is applied to the document worker ahead of the
                // reconsider that resolves the caller's receipt.
                let Some(waiter) = self.pending_doc_syncs.remove(&request_id.nonce) else {
                    self.try_resolve_quiescence()?;
                    return Ok(());
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
            Runtime2Evt::DocSyncFailed { request_id, error } => {
                let Some(waiter) = self.pending_doc_syncs.remove(&request_id.nonce) else {
                    self.try_resolve_quiescence()?;
                    return Ok(());
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
            Runtime2Evt::KeyhiveAdmissionAdvanced { seq } => {
                self.admitted_head = self.admitted_head.max(seq);
                tracing::debug!(
                    local_peer_id = %self.local_peer_id,
                    seq,
                    admitted_head = self.admitted_head,
                    group_part_settled_seq = self.group_part_settled_seq,
                    "keyhive admission head advanced"
                );
                // Release every keyhive round whose completion was deferred
                // waiting for exactly this watermark. `finish_keyhive_sync`
                // runs the same resolution it would have run on the
                // completion, now that `admitted_head` covers the round's
                // admissions.
                let released: Vec<(PeerKey, subduction_keyhive::message::RequestId)> = self
                    .active_keyhive_syncs
                    .iter()
                    .filter(|(_, round)| {
                        round
                            .pending_admission_seq
                            .is_some_and(|watermark| watermark <= self.admitted_head)
                    })
                    .map(|(peer_id, round)| (peer_id.clone(), round.request_id.clone()))
                    .collect();
                for (peer_id, request_id) in released {
                    self.finish_keyhive_sync(peer_id, request_id)?;
                }
                self.try_resolve_quiescence()?;
                // Admitted Keyhive state (CGKA ops, delegations, keys) can
                // unblock a doc that entered the pending set before its
                // material landed. The exchange triggers cover sync rounds;
                // this covers admissions that arrive without one (ephemeral
                // notifications, peer pushes), which otherwise leave a doc
                // pending with nothing left to wake it.
                if !self.pending_materialization.is_empty() {
                    self.reattempt_pending_materialization()?;
                }
            }
            Runtime2Evt::GroupPartWorkerSettled { seq } => {
                self.group_part_settled_seq = self.group_part_settled_seq.max(seq);
                tracing::debug!(
                    local_peer_id = %self.local_peer_id,
                    seq,
                    admitted_head = self.admitted_head,
                    group_part_settled_seq = self.group_part_settled_seq,
                    waiters = self.keyhive_reconciliation_waiters.len(),
                    "group-part settled admission watermark"
                );
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
                self.materialization_retries_requested.remove(&doc_id);
                if let Some(probe) = self.quiescence_probe.as_mut() {
                    probe.pending_docs.remove(&doc_id);
                }
                if let Some(err) = error {
                    tracing::error!(%doc_id, error = %err, "doc worker stopped with error");
                }
            }
            Runtime2Evt::DocWorkerMaterializationPending { doc_id } => {
                self.pending_materialization.insert(doc_id.clone());
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
                let stale = start_seq.is_some_and(|start| self.admitted_head > start);
                match &status {
                    crate::runtime2::MaterializationStatus::Pending(blockers) => {
                        self.pending_materialization.insert(doc_id.clone());
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
                            self.retry_existing_doc_materialization(doc_id.clone())?;
                        }
                    }
                    crate::runtime2::MaterializationStatus::Ready {
                        partially_decrypted: true,
                    } => {
                        self.pending_materialization.insert(doc_id.clone());
                        debug!(
                            %doc_id,
                            stale,
                            "materialization retry remains partially decrypted"
                        );
                        if stale {
                            // The walk ran against an older Keyhive state: the
                            // operation admitted mid-walk may be exactly what
                            // supplied the missing document keys. Re-verify with
                            // the fresher state, otherwise the bundle's causal
                            // count stalls below the live CGKA count and the
                            // frontier publish barrier waits for a wakeup that
                            // never comes.
                            debug!(%doc_id, "re-verifying stale partial materialization retry");
                            self.retry_existing_doc_materialization(doc_id.clone())?;
                        }
                    }
                    crate::runtime2::MaterializationStatus::Missing
                    | crate::runtime2::MaterializationStatus::Ready {
                        partially_decrypted: false,
                    } => {
                        self.pending_materialization.remove(&doc_id);
                        debug!(
                            %doc_id,
                            ?status,
                            stale,
                            "materialization retry reached a terminal state"
                        );
                        if stale {
                            debug!(%doc_id, "re-verifying stale terminal materialization retry");
                            self.retry_existing_doc_materialization(doc_id.clone())?;
                        }
                    }
                }
                // A request that arrived while this walk was running could not be
                // coalesced into it: the walk had already read the projection before
                // that change landed. Re-issue it now, or the change is only observed
                // by whatever happens to run next — which, for a document that just
                // left the pending set, may be nothing at all.
                if self.materialization_retries_requested.remove(&doc_id) {
                    debug!(%doc_id, "re-issuing latched materialization retry");
                    self.retry_existing_doc_materialization(doc_id)?;
                }
            }
            // --- Keyhive event listener handlers ---
            // Translate raw Keyhive events into domain-level notifications.
            Runtime2Evt::PrekeyExpanded { .. } | Runtime2Evt::PrekeyRotated { .. } => {
                // Individual prekey operations are internal key management and do
                // not correspond to a BigRepo domain event, but every state change
                // must reach the durable prekey-state sidecar so restarts restore
                // published membership without requiring compaction. Best-effort:
                // a missed snapshot narrows to the ops since the last persisted
                // one, never breaks decryptability.
                self.spawn_tracked(
                    crate::runtime2::TrackedWorkKind::PrekeyStatePersist,
                    F::persist_prekey_state_snapshot(std::sync::Arc::clone(&self.runtime_io)),
                )?;
            }
            // FIXME: this doesn't seem correct, I believe a key rotation can correspond
            // to multiple CGKA ops
            Runtime2Evt::CgkaOp { data } => {
                // Every CGKA op is a document key rotation.
                let doc_id = crate::DocumentId::new(data.payload().doc_id().as_bytes());
                let worker_present = self.doc_workers.contains_key(&doc_id);
                debug!(
                    local_peer_id = %self.local_peer_id,
                    %doc_id,
                    worker_present,
                    pending_count = self.pending_materialization.len(),
                    "processing CGKA operation; routing document materialization update"
                );
                self.change_manager
                    .notify_document_key_rotated(doc_id.clone())
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                // Route the per-document key update to an existing worker. The
                // admission-driven AFW path handles cold documents; this path
                // must never create a worker merely because Keyhive changed.
                self.retry_existing_doc_materialization(doc_id)?;
            }
            Runtime2Evt::DelegationReceived { target, data } => {
                let member_id = PeerKey::new(data.payload().delegate().id().to_bytes());
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
                let pending: Vec<_> = self.pending_materialization.iter().cloned().collect();
                for doc_id in pending {
                    self.retry_doc_materialization(doc_id)?;
                }
            }
            Runtime2Evt::RevocationReceived { target, data } => {
                let member_id = PeerKey::new(data.payload().revoked_id().as_bytes());
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
                if let Some(count) = self.tracked_work.get_mut(&kind) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.tracked_work.remove(&kind);
                    }
                }
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
            doc_id = %DocumentId::new(session.sedimentree_id.as_bytes()),
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
        let doc_id = DocumentId::new(session.sedimentree_id.as_bytes());
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
        let peer_id = PeerKey::new(session.peer_id.as_bytes());

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

    /// Route a sync-session apply to the doc worker.
    ///
    /// `reply: Some` resolves a caller's sync receipt with the worker's
    /// outcome; `None` is fire-and-forget (passive sessions). Empty `commit_ids`
    /// / `fragment_ids` make the worker only reconsider a pending doc / report
    /// its current state. A document without a local worker is spawned lazily,
    /// which is intentional: a round can complete for a doc with no handle and
    /// its reconsider must still report the stored state.
    ///
    /// A send failure is never swallowed. The mailbox is unbounded, so a
    /// failure means the worker closed its receiver (it is stopping) and the
    /// apply — with any receipt it carried — was dropped. The waiting caller is
    /// failed directly; a dropped *content* apply additionally fails every
    /// pending round for the document, because the completion path resolves a
    /// waiter with an empty reconsider whose success would claim content that
    /// was never applied.
    fn route_sync_session_apply(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerKey,
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
        let content_carrying = !commit_ids.is_empty() || !fragment_ids.is_empty();
        // Received content must pass through the document worker even without
        // an application handle: writable overlap nodes use cold/transient
        // materialization to publish causal-key healing checkpoints. Failing to
        // resolve one is an apply-route failure, not a hub failure: surface it
        // to the waiter rather than aborting the loop. The lazy spawn is
        // intentional, so this only trips on a spawn error.
        let (worker, _lease) = match self.doc_worker_handle(doc_id.clone()) {
            Ok(resolved) => resolved,
            Err(error) => {
                warn!(
                    doc_id = %doc_id,
                    %peer_id,
                    ?error,
                    "sync session apply could not resolve a document worker"
                );
                self.fail_sync_session_apply_route(doc_id, peer_id, content_carrying, reply);
                return Ok(());
            }
        };

        // Test-only: close the resolved worker's mailbox so the send below
        // fails, standing in for the worker-stopping race that cannot be
        // reached deterministically from a test.
        #[cfg(test)]
        if content_carrying && self.poisoned_content_apply.as_ref() == Some(&doc_id) {
            self.poisoned_content_apply = None;
            worker.msg_tx.close();
        }

        let msg = DocWorkerMsg::ApplySyncSession {
            peer_id: peer_id.clone(),
            commit_ids,
            fragment_ids,
            reply,
            _lease,
        };
        let error = match worker.msg_tx.try_send(msg) {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        // Recover the reply before dropping the rest: the caller that sent it
        // must still learn the apply was dropped.
        let reply = match error {
            async_channel::TrySendError::Closed(DocWorkerMsg::ApplySyncSession {
                reply, ..
            })
            | async_channel::TrySendError::Full(DocWorkerMsg::ApplySyncSession { reply, .. }) => {
                reply
            }
            other => unreachable!("apply route sent a non-apply message: {other:?}"),
        };
        debug!(
            doc_id = %doc_id,
            %peer_id,
            content_carrying,
            "sync session apply dropped: document worker mailbox closed"
        );
        self.fail_sync_session_apply_route(doc_id, peer_id, content_carrying, reply);
        Ok(())
    }

    /// Surface a failed sync-session apply route to whoever was waiting.
    ///
    /// A caller awaiting a receipt is failed directly. A dropped *content*
    /// apply (fire-and-forget, no receipt) additionally fails and forgets every
    /// pending round for the document, so the completion path's empty
    /// reconsider cannot resolve them with a success receipt for content that
    /// was never applied.
    fn fail_sync_session_apply_route(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerKey,
        content_carrying: bool,
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
    ) {
        if let Some(reply) = reply {
            reply
                .send(Err(crate::runtime2::types::SyncDocError::WorkerUnavailable))
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
        } else if content_carrying {
            self.fail_pending_doc_syncs_for(doc_id, peer_id);
        }
    }

    /// Fail and forget every pending doc-sync round for `(doc_id, peer_id)`.
    ///
    /// A dropped content apply means those rounds' content never reached the
    /// live document, so the completion path's empty reconsider must not be
    /// allowed to resolve them with a success receipt — removing them here
    /// makes that completion a no-op.
    fn fail_pending_doc_syncs_for(&mut self, doc_id: DocumentId, peer_id: PeerKey) {
        let waiter_ids: Vec<u64> = self
            .pending_doc_syncs
            .iter()
            .filter(|(_, waiter)| waiter.doc_id == doc_id && waiter.peer_id == peer_id)
            .map(|(waiter_id, _)| *waiter_id)
            .collect();
        for waiter_id in waiter_ids {
            let waiter = self
                .pending_doc_syncs
                .remove(&waiter_id)
                .expect("waiter id collected from pending_doc_syncs");
            warn!(
                waiter_id,
                doc_id = %doc_id,
                %peer_id,
                "failing doc sync waiter: content apply was dropped"
            );
            waiter
                .resp
                .send(Err(crate::runtime2::types::SyncDocError::WorkerUnavailable))
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
        }
    }

    // ─── connection lifecycle ──────────────────────────────────────────────

    /// Handle an established connection: register peer in `connected_peers`,
    /// then schedule the initial keyhive sync and start a round for any waiter
    /// that was queued before this event was processed.
    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn handle_connection_established(
        &mut self,
        peer_id: PeerKey,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        if closed.load(std::sync::atomic::Ordering::SeqCst) {
            // Commands are polled ahead of events, so an explicit `CloseConn`
            // can be processed before the `ConnEstablished` it overtook.
            // Registering this connection now would make a closed connection
            // look live and start doomed sync rounds against it, so a sync
            // issued the instant `close_connection` returned could be told the
            // peer is gone by a spurious failure. The close path already marked
            // the connection dead; ignore its establishment.
            debug!(
                %peer_id,
                "ignoring establishment for an already-closed connection"
            );
            return Ok(());
        }
        self.connected_peers.insert(
            peer_id.clone(),
            ConnDeets {
                closed: Arc::clone(&closed),
            },
        );
        // `SyncKeyhiveWithPeer` is polled ahead of events, so it can be
        // processed before this one even though `open_connection_and_watch`
        // queues `ConnEstablished` before it resolves the caller's connect
        // response. That handler defers the round to this one while the peer is
        // unregistered, so a waiter queued in that gap must start its round
        // here even when connect-time syncing is disabled — otherwise nothing
        // ever resolves it and the connection stays healthy, so no `ConnLost`
        // backstop fires.
        let queued_waiters = self
            .keyhive_waiters
            .get(&peer_id)
            .is_some_and(|waiters| !waiters.waiters.is_empty());
        if self.keyhive_sync_on_connect || queued_waiters {
            self.start_keyhive_sync(peer_id)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn handle_connection_lost(
        &mut self,
        peer_id: PeerKey,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        if !self.deregister_connection(&peer_id, &closed, "keyhive connection lost") {
            // Already torn down by an explicit close, or superseded by a
            // replacement connection: a no-op, not a double teardown.
            debug!(
                %peer_id,
                "ignoring connection loss for an untracked or superseded connection"
            );
        }
        Ok(())
    }

    /// Tear down `peer_id`'s connection registration iff it still belongs to the
    /// connection identified by `closed`.
    ///
    /// Both the explicit close path (`CloseConn`) and the transport-loss path
    /// (`ConnLost`) route through here, so there is exactly one
    /// deregistration. It is `Arc::ptr_eq`-gated on the connection id, which
    /// keeps a superseded connection's teardown from disturbing its
    /// replacement (matching `handle_connection_lost`'s stale-conn gate) and
    /// makes a `ConnLost` that lands after an explicit close a no-op instead of
    /// a double-deregistration. Returns whether a registration was removed.
    fn deregister_connection(
        &mut self,
        peer_id: &PeerKey,
        closed: &Arc<std::sync::atomic::AtomicBool>,
        reason: &'static str,
    ) -> bool {
        let is_current = matches!(
            self.connected_peers.get(peer_id),
            // FIXME: use a wrapper type on the atomic bool if we're going
            // to use it like this and use internal uuids afterwards
            Some(deets) if Arc::ptr_eq(&deets.closed, closed)
        );
        if !is_current {
            return false;
        }
        self.cancel_pending_keyhive_syncs(peer_id, reason);
        self.connected_peers.remove(peer_id);
        true
    }

    // ─── keyhive sync ──────────────────────────────────────────────────────

    /// Start a keyhive sync round with `peer_id` if not already active.
    #[tracing::instrument(skip_all, fields(local_peer_id = %self.local_peer_id, %peer_id))]
    fn start_keyhive_sync(&mut self, peer_id: PeerKey) -> eyre::Result<()> {
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
                self.local_peer_id.to_bytes32(),
            ),
            nonce: round_id,
        };
        self.active_keyhive_syncs.insert(
            peer_id.clone(),
            KeyhiveSyncRound {
                round_id,
                started_at: self.clock.instant(),
                request_id: request_id.clone(),
                slow_warned: false,
                admitted_ids,
                pending_admission_seq: None,
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
        peer_id: PeerKey,
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
    fn handle_keyhive_change_notif(&mut self, peer_id: PeerKey) -> eyre::Result<()> {
        if !self.connected_peers.contains_key(&peer_id) {
            debug!(
                %peer_id,
                "keyhive change notification ignored: peer not connected"
            );
            return Ok(());
        }
        if self.active_keyhive_syncs.contains_key(&peer_id) {
            self.keyhive_notif_pending.insert(peer_id.clone());
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

    /// Handle a keyhive sync completion, deferring it until the hub's
    /// `admitted_head` covers the completion's admission watermark.
    ///
    /// A completion whose watermark is already reached behaves exactly as
    /// before (the common case: the peer admitted nothing new, or the
    /// admission event landed ahead of the completion on the shared channel).
    /// Otherwise the round is held — waiters unresolved and no follow-up round
    /// started — and [`Self::finish_keyhive_sync`] runs from the
    /// `KeyhiveAdmissionAdvanced` arm once `admitted_head` catches up. Without
    /// this the caller could return, capture a stale `admitted_head` in its
    /// follow-up reconciliation wait, and be told "reconciled" while this
    /// round's admissions were never projected.
    fn handle_keyhive_sync_done(
        &mut self,
        peer_id: PeerKey,
        request_id: subduction_keyhive::message::RequestId,
        admitted_seq: u64,
    ) -> eyre::Result<()> {
        let admitted_head = self.admitted_head;
        if let Some(round) = self.active_keyhive_syncs.get_mut(&peer_id)
            && round.request_id == request_id
        {
            // Deferral is sticky: once a watermark is required, a later
            // duplicate completion stamped lower cannot release the round.
            let required = round
                .pending_admission_seq
                .map_or(admitted_seq, |pending| pending.max(admitted_seq));
            if required > admitted_head {
                round.pending_admission_seq = Some(required);
                debug!(
                    %peer_id,
                    ?request_id,
                    admitted_seq,
                    required,
                    admitted_head,
                    "keyhive completion deferred until the admission watermark catches up"
                );
                return Ok(());
            }
        }
        self.finish_keyhive_sync(peer_id, request_id)
    }

    /// Complete a keyhive protocol round and resolve any eligible waiters.
    fn finish_keyhive_sync(
        &mut self,
        peer_id: PeerKey,
        request_id: subduction_keyhive::message::RequestId,
    ) -> eyre::Result<()> {
        let Some(round) = self.active_keyhive_syncs.get_mut(&peer_id) else {
            warn!(%peer_id, ?request_id, "processing untracked inbound keyhive completion");
            self.reattempt_pending_materialization()?;
            return Ok(());
        };
        if round.request_id != request_id {
            warn!(
                %peer_id,
                expected_request_id = ?round.request_id,
                request_id = ?request_id,
                "processing inbound keyhive exchange without resolving waiter"
            );
            self.reattempt_pending_materialization()?;
            return Ok(());
        }

        let admitted_ids = std::mem::take(&mut round.admitted_ids);
        let round_id = round.round_id;

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
        // The round is finished: retire it before any follow-up round can
        // start, so a latched change notification no longer sees this peer
        // as mid-sync (the original complete_keyhive_sync_round removed it
        // before resolving waiters).
        self.active_keyhive_syncs.remove(&peer_id);
        let mut notification_pending = self.keyhive_notif_pending.remove(&peer_id);
        let was_notification_pending = notification_pending;
        if coalesce_keyhive_demand(has_remaining, &mut notification_pending) {
            if has_remaining && was_notification_pending {
                debug!(
                    %peer_id,
                    round_id,
                    "coalescing change notification into waiter follow-up round"
                );
            } else if was_notification_pending {
                debug!(
                    %peer_id,
                    round_id,
                    "change notification latched during round; starting follow-up round"
                );
            }
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

    /// Targeted retries remember the Keyhive admission head at retry start.
    /// `DocWorkerMaterializationRetryCompleted` re-runs any result produced while
    /// admission advanced, because even an apparently complete snapshot may have
    /// materialized one fewer CGKA operation than the now-current document state.
    fn retry_doc_materialization(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        let (worker, lease) = self.doc_worker_handle(doc_id.clone())?;
        self.send_materialization_retry(doc_id, worker, lease)
    }

    /// CGKA notifications are routed only to workers that already exist. Cold
    /// documents are admitted and materialized by AFW instead of being spawned
    /// merely because a keyhive operation arrived.
    fn retry_existing_doc_materialization(&mut self, doc_id: DocumentId) -> eyre::Result<()> {
        let Some((worker, lease)) = self.acquire_existing_doc_worker_handle(doc_id.clone())? else {
            debug!(%doc_id, "skipping materialization retry without an existing document worker");
            return Ok(());
        };
        self.send_materialization_retry(doc_id, worker, lease)
    }

    fn send_materialization_retry(
        &mut self,
        doc_id: DocumentId,
        worker: DocWorkerHandle,
        lease: DocWorkerInternalLease,
    ) -> eyre::Result<()> {
        if self.materialization_retries_in_flight.contains_key(&doc_id) {
            self.materialization_retries_requested
                .insert(doc_id.clone());
            debug!(
                %doc_id,
                "latching materialization retry behind the in-flight walk"
            );
            return Ok(());
        }
        self.materialization_retries_requested.remove(&doc_id);
        let start_seq = self.admitted_head;
        self.materialization_retries_in_flight
            .insert(doc_id.clone(), start_seq);
        debug!(
            %doc_id,
            start_seq,
            "requesting targeted materialization retry from document worker"
        );
        let (resp, result) = futures::channel::oneshot::channel();
        if let Err(error) = worker.send(DocWorkerMsg::ReattemptMaterialization {
            origin: crate::changes::BigRepoChangeOrigin::Keyhive,
            resp,
            _lease: lease,
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
    fn cancel_pending_keyhive_sync(&mut self, peer_id: &PeerKey, waiter_id: u64) -> bool {
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
    fn cancel_pending_keyhive_syncs(&mut self, peer_id: &PeerKey, reason: &'static str) {
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
        self.spawn_doc_worker(doc_id.clone())?;
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
            doc_id.clone(),
            Arc::clone(&self.doc_io),
            Arc::clone(&self.change_manager),
            self.cmd_tx.clone(),
            self.evt_tx.clone(),
            generation,
            self.span.clone(),
        )?;
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
    /// Report what a stalled quiescence fence is waiting for, every
    /// [`QUIESCENCE_STALL_REPORT_INTERVAL`] while it stays stalled.
    ///
    /// Quiescence is the harness's universal "everything drained" fence, so a
    /// wedged fence otherwise only surfaces as an opaque test timeout. Naming
    /// the in-flight work, the docs still fenced, and the cursors that have not
    /// caught up is what makes the stall attributable.
    fn report_quiescence_stall(&mut self, now: std::time::Instant) {
        let Some(since) = self.quiescence_stall_since else {
            return;
        };
        let Some(next_report) = self.next_quiescence_stall_report else {
            return;
        };
        if now < next_report {
            return;
        }
        self.next_quiescence_stall_report = Some(now + QUIESCENCE_STALL_REPORT_INTERVAL);
        let probe = self.quiescence_probe.as_ref();
        let mut pending_docs: Vec<String> = self
            .pending_materialization
            .iter()
            .map(ToString::to_string)
            .collect();
        pending_docs.sort();
        // Which peer and round is holding the fence, not just how many. A count
        // cannot distinguish one stuck round from a revolving set.
        let mut active_keyhive_rounds: Vec<String> = self
            .active_keyhive_syncs
            .iter()
            .map(|(peer_id, round)| {
                format!(
                    "{}:round{}:{:.0}s",
                    &peer_id.to_string()[..8],
                    round.round_id,
                    round.started_at.elapsed().as_secs_f64(),
                )
            })
            .collect();
        active_keyhive_rounds.sort();
        let mut keyhive_waiter_peers: Vec<String> = self
            .keyhive_waiters
            .keys()
            .map(|peer_id| peer_id.to_string()[..8].to_string())
            .collect();
        keyhive_waiter_peers.sort();
        // Name the outstanding doc syncs the same way: a count cannot say which
        // peer or document the fence is waiting for.
        let mut pending_doc_sync_details: Vec<String> = self
            .pending_doc_syncs
            .iter()
            .map(|(waiter_id, waiter)| {
                format!(
                    "{waiter_id}:{}:{}",
                    &waiter.doc_id.to_string()[..10],
                    &waiter.peer_id.to_string()[..8],
                )
            })
            .collect();
        pending_doc_sync_details.sort();
        warn!(
            local_peer_id = %self.local_peer_id,
            stalled_secs = since.elapsed().as_secs(),
            quiescence_waiters = self.quiescence_waiters.len(),
            probe_barrier = probe.map(|probe| probe.barrier_id),
            probe_pending_docs = probe.map_or(0, |probe| probe.pending_docs.len()),
            probe_generation = probe.map(|probe| probe.activity_generation),
            activity_generation = self.activity_generation,
            frozen = self.frozen,
            tracked_in_flight = self.tracked_in_flight,
            tracked_work = ?self.tracked_work,
            doc_workers = self.doc_workers.len(),
            pending_materialization = pending_docs.len(),
            pending_docs = ?pending_docs,
            retries_in_flight = self.materialization_retries_in_flight.len(),
            pending_doc_syncs = self.pending_doc_syncs.len(),
            pending_doc_sync_details = ?pending_doc_sync_details,
            keyhive_waiters = self.keyhive_waiters.len(),
            active_keyhive_syncs = self.active_keyhive_syncs.len(),
            active_keyhive_rounds = ?active_keyhive_rounds,
            keyhive_waiter_peers = ?keyhive_waiter_peers,
            admitted_head = self.admitted_head,
            group_part_settled_seq = self.group_part_settled_seq,
            "quiescence fence stalled",
        );
    }

    /// Report unresolved Keyhive sync rounds.
    ///
    /// Production: report each round once when it outlives
    /// `KEYHIVE_SYNC_SLOW_ROUND_WARN` and keep waiting — we run on slow
    /// networks and an application-level deadline belongs to the caller, who
    /// can cancel `sync_keyhive` safely because the waiter guard removes the
    /// waiter (so a cancelled call never cascades a follow-up round).
    ///
    /// Tests: panic on `KEYHIVE_SYNC_ROUND_TIMEOUT` so a stress run surfaces a
    /// round that no completion, protocol error, or connection loss retired
    /// instead of hanging on it.
    fn report_unresolved_keyhive_round(&mut self, now: std::time::Instant) {
        #[cfg(any(test, feature = "test-support"))]
        let threshold = KEYHIVE_SYNC_ROUND_TIMEOUT;
        #[cfg(not(any(test, feature = "test-support")))]
        let threshold = KEYHIVE_SYNC_SLOW_ROUND_WARN;

        let unresolved = self
            .active_keyhive_syncs
            .iter_mut()
            .filter_map(|(peer_id, round)| {
                round
                    .latch_if_unresolved(now, threshold)
                    .map(|report| (peer_id.clone(), report))
            })
            .collect::<Vec<_>>();

        #[cfg(any(test, feature = "test-support"))]
        if let Some((peer_id, report)) = unresolved.first() {
            panic!(
                "Keyhive sync round timed out without response, protocol error, or connection loss: peer={peer_id} request={:?} round={} elapsed_secs={} admitted_waiters={}",
                report.request_id, report.round_id, report.elapsed_secs, report.admitted_waiters
            );
        }

        #[cfg(not(any(test, feature = "test-support")))]
        for (peer_id, report) in unresolved {
            warn!(
                %peer_id,
                round_id = report.round_id,
                request_id = ?report.request_id,
                elapsed_secs = report.elapsed_secs,
                slow_after_secs = threshold.as_secs(),
                connected = self.connected_peers.contains_key(&peer_id),
                admitted_waiters = report.admitted_waiters,
                pending_waiters = self
                    .keyhive_waiters
                    .get(&peer_id)
                    .map_or(0, |waiters| waiters.waiters.len()),
                "Keyhive sync round unresolved; still waiting"
            );
        }
    }

    fn janitor_tick(&mut self) {
        let now = self.clock.instant();
        self.report_quiescence_stall(now);
        self.report_unresolved_keyhive_round(now);
        let expired: Vec<DocumentId> = self
            .doc_workers
            .iter()
            .filter(|(_, entry)| {
                entry
                    .eviction_deadline
                    .is_some_and(|deadline| deadline <= now)
            })
            .map(|(doc_id, _)| doc_id.clone())
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
/// - `worker_tasks` — construction-time background workers, joined before the
///   commands channel closes so they never send into a dead hub.
/// - `child_tasks` — dynamic background jobs and long-lived support tasks.
/// - `machine_tasks` — the hub's dispatcher loop, stopped last so it can
///   drain in-flight tracked work before exiting.
pub struct Runtime2StopToken<F: FutureForm, R: TaskRuntime<F>> {
    pub(crate) cancel: futures::future::AbortHandle,
    pub(crate) cmd_tx: async_channel::Sender<Runtime2Cmd>,
    pub(crate) child_tasks: R::Tasks,
    pub(crate) machine_tasks: R::Tasks,
    /// Dedicated set for the construction-time background workers (group part,
    /// prekey janitor, causal checkpoint, automerge frontier, keyhive
    /// dispatcher). They are joined *before* the commands channel closes: the
    /// hub constructs that channel, so it must outlive every worker that sends
    /// into it, otherwise a worker cancelled mid-flight observes a dead actor
    /// and fails a task that is really being shut down.
    pub(crate) worker_tasks: R::Tasks,
    /// Keeps the event channel open until every child holding an `evt_tx`
    /// clone has been joined. The machine-loop future owns the other
    /// receiver, so without this guard the channel closes the moment that
    /// future is dropped — while children can still be mid-send. Never read;
    /// it exists to be dropped last by [`Self::stop`].
    _evt_rx_guard: async_channel::Receiver<Runtime2Evt>,
    pub group_part_stop: Option<crate::runtime2::GroupPartWorkerStopToken>,
    pub causal_checkpoint_stop: Option<crate::runtime2::CausalCheckpointWorkerStopToken>,
    pub automerge_frontier_stop: Option<crate::runtime2::AutomergeFrontierWorkerStopToken>,
    pub prekey_janitor_stop: Option<crate::runtime2::PrekeyJanitorWorkerStopToken>,
    pub(crate) keyhive_dispatcher_stop:
        Option<crate::runtime2::keyhive_dispatcher::KeyhiveDispatcherStopToken>,
}

impl<F: FutureForm, R: TaskRuntime<F>> Runtime2StopToken<F, R> {
    /// Cancel the runtime and await graceful shutdown.
    ///
    /// Closing the commands channel (via [`Sender::close`], so the closure
    /// is observed even while the hub, handle and in-flight children still
    /// hold sender clones) makes the machine loop stop admitting new
    /// background work, drain in-flight tracked work (processing their
    /// events) and exit. The event channel stays open until every child that
    /// holds an `evt_tx` clone has been joined (see `_evt_rx_guard`), so no
    /// child ever reports into a closed channel.
    /// Senders that still hold a `cmd_tx` clone treat the closed channel as
    /// the shutdown signal. Only if the drain does not complete within
    /// `timeout` is the machine loop aborted outright.
    pub async fn stop(mut self, timeout: std::time::Duration) -> eyre::Result<()> {
        // Stop construction-time background workers first in reverse order:
        // the dispatcher is spawned last, so it is cancelled first.
        if let Some(stop) = self.keyhive_dispatcher_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.automerge_frontier_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.causal_checkpoint_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.prekey_janitor_stop.take() {
            stop.cancel();
        }
        if let Some(stop) = self.group_part_stop.take() {
            stop.cancel();
        }

        // Join the cancelled construction-time workers before closing the
        // commands channel. The hub constructs that channel, so it must
        // outlive its senders: a worker aborted mid-flight (for example the
        // causal checkpoint worker awaiting `EnsureCausalCoverage`) would
        // otherwise observe a closed channel or an unanswered command and
        // fail a task that is only shutting down.
        if self.worker_tasks.stop(timeout).await.is_err() {
            tracing::warn!("runtime2 workers did not stop within the shutdown timeout; aborting");
            self.worker_tasks.abort();
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
        // `_evt_rx_guard` drops with `self` here — after every child holding
        // an `evt_tx` clone has been joined — so a child can never send into a
        // closed channel.
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
        let span = hub.span.clone();
        F::from_future(
            async move {
                let result = futures::future::Abortable::new(
                    async move {
                        let janitor_interval = std::time::Duration::from_millis(500);
                        let mut next_janitor = hub.clock.instant() + janitor_interval;
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
                            // Preserve the janitor deadline across busy loop iterations.
                            // Recreating a full-interval sleep after every command/event can
                            // postpone eviction forever; scanning every worker per event is
                            // equally undesirable. The cheap deadline check runs per turn,
                            // while the O(workers) scan remains bounded to twice per second.
                            let now = hub.clock.instant();
                            if now >= next_janitor {
                                hub.janitor_tick();
                                next_janitor = now + janitor_interval;
                            }
                            // Commands are polled before events: a command (e.g.
                            // a lease release or an unfreeze) can cancel the
                            // very work whose events would otherwise keep the
                            // loop busy, so it must not be starved by an event
                            // flood.
                            let sleep_for =
                                next_janitor.saturating_duration_since(hub.clock.instant());
                            let mut sleep = Box::pin(timer.sleep(sleep_for).fuse());
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
                                    _ = sleep.as_mut() => {}
                                    evt = evt.as_mut() => match evt {
                                        Ok(evt) => hub.handle_or_hold_evt(evt)?,
                                        Err(_) => break,
                                    },
                                }
                            } else {
                                let mut cmd = Box::pin(cmd_rx.recv().fuse());
                                futures::select_biased! {
                                    _ = sleep.as_mut() => {}
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
                                        Ok(evt) => hub.handle_or_hold_evt(evt)?,
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
        keyhive_sync_on_connect,
        event_channel,
    } = config;

    // Create two independent task sets for reverse-order shutdown.
    let child_tasks = tasks.task_set();
    let machine_tasks = tasks.task_set();
    let worker_tasks = tasks.task_set();

    let (runtime_abort, runtime_registration) = futures::future::AbortHandle::new_pair();

    let (cmd_tx, cmd_rx) = async_channel::unbounded::<Runtime2Cmd>();
    let (evt_tx, evt_rx) = event_channel.unwrap_or_else(async_channel::unbounded::<Runtime2Evt>);
    // The stop token keeps a receiver clone so the channel cannot close while
    // a child may still be reporting into it (see `Runtime2StopToken::stop`).
    let evt_rx_guard = evt_rx.clone();

    // The handle generates waiter ids; the hub tracks waiters per peer/doc
    // with its own round/request-id state (no hub-side watermark needed).
    let doc_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let keyhive_sync_waiter_ids = Arc::new(std::sync::atomic::AtomicU64::new(1));

    let hub: Runtime2Hub<F, R> = Runtime2Hub {
        local_peer_id: local_peer_id.clone(),
        span: tracing::info_span!("runtime2_hub", local_peer_id = %local_peer_id),
        sync_policy,
        runtime_io: Arc::clone(&runtime_io),
        connect,
        doc_io,
        keyhive_sync_on_connect,
        change_manager,
        child_tasks: child_tasks.clone(),
        // timer: Arc::clone(&timer),
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
        keyhive_reconciliation_waiters: Vec::new(),
        pending_doc_syncs: HashMap::new(),
        quiescence_waiters: Vec::new(),
        quiescence_probe: None,
        quiescence_barrier_ids: 0,
        frozen: false,
        frozen_cmd_buffer: Vec::new(),
        freeze_on_resolve: false,
        #[cfg(test)]
        hold_events: false,
        #[cfg(test)]
        held_events: Vec::new(),
        #[cfg(test)]
        poisoned_content_apply: None,
        cmd_closed: false,
        activity_generation: 0,
        tracked_in_flight: 0,
        doc_workers: HashMap::new(),
        tracked_work: HashMap::new(),
        quiescence_stall_since: None,
        next_quiescence_stall_report: None,
        pending_materialization: HashSet::new(),
        materialization_retries_in_flight: HashMap::new(),
        materialization_retries_requested: HashSet::new(),
        next_doc_worker_generation: 1,
    };

    let handle = Runtime2Handle::<F>::new(
        cmd_tx.clone(),
        hub.sync_policy,
        #[cfg(any(test, feature = "test-support"))]
        Arc::clone(&timer),
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
            worker_tasks,
            _evt_rx_guard: evt_rx_guard,
            group_part_stop: None,
            causal_checkpoint_stop: None,
            automerge_frontier_stop: None,
            prekey_janitor_stop: None,
            keyhive_dispatcher_stop: None,
        },
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn notification_overlapping_explicit_waiter_coalesces_to_one_follow_up_round() {
        let (reply, _reply_rx) = futures::channel::oneshot::channel();
        let mut waiters = super::KeyhiveWaiters::default();
        waiters.ids.insert(7);
        waiters.waiters.push((7, reply));
        let mut notification_pending = true;

        // The explicit/backend waiter remains queued when the active round
        // completes. It is sufficient demand for the next round; the
        // notification must not schedule another one.
        assert!(super::coalesce_keyhive_demand(
            !waiters.waiters.is_empty(),
            &mut notification_pending,
        ));
        assert!(!notification_pending);
        assert_eq!(waiters.waiters.len(), 1);

        // The consumed notification cannot create a second round after the
        // waiter-triggered follow-up has been admitted.
        assert!(!super::coalesce_keyhive_demand(
            waiters.waiters.is_empty(),
            &mut notification_pending,
        ));
    }

    #[test]
    fn unresolved_keyhive_round_is_reported_once() {
        let now = std::time::Instant::now();
        let threshold = std::time::Duration::from_secs(30);
        let round = |elapsed: std::time::Duration| super::KeyhiveSyncRound {
            round_id: 4,
            started_at: now - elapsed,
            request_id: subduction_keyhive::message::RequestId {
                requestor: subduction_keyhive::KeyhivePeerId::from_bytes([9; 32]),
                nonce: 4,
            },
            slow_warned: false,
            admitted_ids: std::collections::HashSet::from([7]),
            pending_admission_seq: None,
        };

        // A fresh round is not reportable, and must not be latched by asking.
        let mut fresh = round(std::time::Duration::from_secs(29));
        assert!(fresh.latch_if_unresolved(now, threshold).is_none());
        assert!(!fresh.slow_warned);

        // A stale round reports its age and admitted waiters exactly once: the
        // janitor ticks on a fixed cadence and must not repeat the report.
        let mut stale = round(std::time::Duration::from_secs(31));
        let report = stale
            .latch_if_unresolved(now, threshold)
            .expect("a stale round is reported");
        assert_eq!(report.round_id, 4);
        assert_eq!(report.elapsed_secs, 31);
        assert_eq!(report.admitted_waiters, 1);
        assert!(stale.latch_if_unresolved(now, threshold).is_none());
    }

    /// The in-flight count is incremented in `spawn_tracked` before the task
    /// exists, so the matching decrement must not depend on the task being
    /// polled. A tracked future aborted before its first poll must still emit
    /// `TrackedWorkDone`, or the count leaks and the shutdown drain never
    /// completes. `Abortable` (the task set's wrapper) drops an aborted task's
    /// future without polling it, which is the shape reproduced here.
    #[tokio::test]
    async fn tracked_work_aborted_before_first_poll_still_emits_done() {
        use crate::runtime2::{TaskRuntime, TaskSet, TokioTaskRuntime, TrackedWorkKind};
        use future_form::{FutureForm, Sendable};

        // Current-thread runtime: there is no await between `spawn` and
        // `abort`, so the task cannot have been polled yet.
        let set = TokioTaskRuntime.task_set();
        let (evt_tx, evt_rx) = async_channel::unbounded::<super::Runtime2Evt>();
        let tracked = <Sendable as super::HubBackgroundFuture<Sendable>>::track_work(
            TrackedWorkKind::SyncDoc,
            Sendable::from_future(async { crate::eyre::Ok(()) }),
            evt_tx,
        );
        let abort = set.spawn(tracked).expect("task set accepts work");
        abort.abort();
        set.stop(std::time::Duration::from_secs(5))
            .await
            .expect("task set stops");

        match evt_rx.try_recv() {
            Ok(super::Runtime2Evt::TrackedWorkDone { kind }) => {
                assert_eq!(kind, TrackedWorkKind::SyncDoc);
            }
            other => panic!(
                "a tracked future aborted before its first poll must still emit \
                 TrackedWorkDone, got: {other:?}"
            ),
        }
    }
}
