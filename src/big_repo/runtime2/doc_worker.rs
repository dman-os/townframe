//! `DocWorker2` — the per-doc actor.
//!
//! # State machine
//!
//! Four states
//! - [`Unloaded`](DocState::Unloaded) — nothing loaded; sedimentree heads may
//!   exist in storage (relay mode).
//! - [`Transient`](DocState::Transient) — automerge doc in memory, no external
//!   handle (client dropped all references).
//! - [`Live`](DocState::Live) — doc shared via `LiveDocBundle`; worker holds a
//!   `Weak` so eviction can reclaim when all handles drop.
//! - [`PendingMaterialization`](DocState::PendingMaterialization) — content
//!   exists in the sedimentree but is not yet decryptable (keyhive keys pending).

use crate::interlude::*;
use crate::runtime::stage_automerge_ingest;
use crate::runtime2::{
    messages::DocWorkerMsg, DocIo, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use future_form::{FutureForm, Local, Sendable};
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════
// STRUCT
// ═══════════════════════════════════════════════════════════════════════════

#[allow(clippy::too_many_arguments)]
pub fn spawn_doc_worker<F, T>(
    doc_id: DocumentId,
    io: Arc<dyn DocIo<F>>,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    sync_policy: crate::runtime::BigRepoSyncPolicy,
    clock: Arc<dyn crate::runtime2::Clock>,
    tasks: &T,
    runtime_evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
) -> eyre::Result<(
    DocWorkerHandle,
    DocWorkerStopToken,
    futures::stream::AbortHandle,
)>
where
    F: FutureForm + DocWorkerLoop<F> + 'static,
    T: crate::runtime2::TaskSet<F>,
{
    let (msg_tx, msg_rx) = async_channel::unbounded::<DocWorkerMsg>();
    let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());

    let mut worker = DocWorker2 {
        doc_id,
        sed_id,
        state: DocState::Unloaded,
        io,
        change_manager,
        sync_policy,
        clock,
        msg_tx: msg_tx.clone(),
        evt_tx: runtime_evt_tx.clone(),
        last_notified_heads: None,
        pending_fragment_requests: std::collections::BTreeSet::new(),
        pending_sync_jobs: HashMap::new(),
        active_doc_syncs: HashMap::new(),
        sync_completed: std::collections::HashSet::new(),
        sync_materialized: std::collections::HashSet::new(),
        quiescence_waiters: Vec::new(),
    };

    let (stop_abort, stop_registration) = futures::future::AbortHandle::new_pair();

    let abort = tasks.spawn(F::mailbox_loop(
        worker,
        msg_rx,
        stop_registration,
        runtime_evt_tx,
        doc_id,
    ))?;

    Ok((
        DocWorkerHandle { msg_tx },
        DocWorkerStopToken { abort: stop_abort },
        abort,
    ))
}

struct DocWorker2<F: FutureForm> {
    doc_id: DocumentId,
    sed_id: sedimentree_core::id::SedimentreeId,

    /// The 4-state machine (Unloaded / Transient / Live / PendingMaterialization).
    state: DocState,

    /// Centralized IO surface — replaces subduction + storage + ciphertext_store
    /// + big_sync_store from the old `DocWorker`.
    io: Arc<dyn DocIo<F>>,

    // ── change manager ─────────────────────────────────────────────────────
    change_manager: Arc<crate::changes::ChangeListenerManager>,

    // ── sync policy / clock ────────────────────────────────────────────────
    sync_policy: crate::runtime::BigRepoSyncPolicy,
    clock: Arc<dyn crate::runtime2::Clock>,

    // ── channels ───────────────────────────────────────────────────────────
    msg_tx: async_channel::Sender<DocWorkerMsg>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,

    // ── head-tracking ──────────────────────────────────────────────────────
    /// The last heads we notified via the change manager. Used for no-op
    /// detection (if the live doc's heads haven't changed, skip notification).
    last_notified_heads: Option<Arc<[automerge::ChangeHash]>>,

    // ── fragment bookkeeping ───────────────────────────────────────────────
    /// Fragment-boundary commits awaiting a corresponding `store_fragment` call.
    pending_fragment_requests:
        std::collections::BTreeSet<subduction_core::subduction::request::FragmentRequested>,

    // ── sync bookkeeping (parcel 4b) ───────────────────────────────────────
    /// In-flight sync jobs: peer → (waiter_id, response_sender, internal_lease).
    pending_sync_jobs: HashMap<
        PeerId,
        Vec<(
            u64,
            futures::channel::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
            DocWorkerInternalLease,
        )>,
    >,
    active_doc_syncs: HashMap<PeerId, u64>,
    /// The subduction sync task has completed for the peer. Success does not
    /// resolve waiters until the corresponding outbound session has also been
    /// applied; the two events arrive through independent runtime2 paths.
    sync_completed: std::collections::HashSet<PeerId>,
    /// An outbound sync session has finished applying for the peer.
    sync_materialized: std::collections::HashSet<PeerId>,
    /// Mailbox-ordered quiescence barriers waiting for active finite work.
    quiescence_waiters: Vec<(u64, DocWorkerInternalLease)>,
}

/// The doc's live state. `Unloaded` = relay/pending (sedimentree heads only,
/// no automerge). `Transient` / `Live` = materialized.
///
/// Mirrors `DocWorkerDocState` at `runtime.rs:2198`.
enum DocState {
    /// No automerge doc loaded. The sedimentree may have content (relay mode)
    /// or may be empty.
    Unloaded,
    /// Automerge doc in memory, no external [`LiveDocBundle`] handles.
    /// The worker can upgrade to `Live` when a handle is acquired, or be
    /// evicted when idle.
    Transient(Box<automerge::Automerge>),
    /// Doc shared via a live handle. The worker holds only a [`Weak`] reference,
    /// so the bundle can be reclaimed when all client references drop.
    Live(std::sync::Weak<crate::runtime::LiveDocBundle>),
    /// Sedimentree content exists but is not yet decryptable (keyhive keys
    /// not yet available — pending sync).
    PendingMaterialization,
}

// ═══════════════════════════════════════════════════════════════════════════
// MESSAGE LOOP
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Dispatch a single [`DocWorkerMsg`]. Called by the message loop.
    ///
    /// Mirrors the old `DocWorker::handle_msg` (implied at `runtime.rs:2314`
    /// where individual message handlers are defined). The `_lease` fields in
    /// certain messages keep the worker alive while the operation is in-flight
    /// (the hub's side of the lease is released when the message completes).
    async fn handle_msg(&mut self, msg: DocWorkerMsg) -> eyre::Result<()> {
        match msg {
            DocWorkerMsg::PutDoc {
                initial_content,
                resp,
            } => self.put_doc(initial_content, resp).await,
            DocWorkerMsg::AcquireHandle { resp } => self.acquire_handle(resp).await,
            DocWorkerMsg::CommitDelta {
                commits,
                heads,
                patches,
                origin,
                resp,
                _lease,
            } => {
                self.commit_delta(commits, heads, patches, origin, resp)
                    .await
            }
            DocWorkerMsg::ApplySyncSession { session, _lease } => {
                let peer_id = PeerId::new(*session.peer_id.as_bytes());
                let completes_sync_waiters = matches!(
                    session.kind,
                    subduction_core::sync_session::SyncSessionKind::OutboundBatch { .. }
                );
                let result = self.apply_sync_session(session).await;
                if !completes_sync_waiters {
                    return result;
                }
                match result {
                    Ok(()) => {
                        self.mark_sync_materialized(peer_id);
                        Ok(())
                    }
                    Err(error) => {
                        self.sync_completed.remove(&peer_id);
                        self.sync_materialized.remove(&peer_id);
                        let expected_sync_error = error
                            .downcast_ref::<crate::runtime::SyncDocError>()
                            .is_some();
                        let sync_error = if let Some(sync_error) =
                            error.downcast_ref::<crate::runtime::SyncDocError>()
                        {
                            match sync_error {
                                crate::runtime::SyncDocError::NotFound => {
                                    crate::runtime::SyncDocError::NotFound
                                }
                                crate::runtime::SyncDocError::Unauthorized => {
                                    crate::runtime::SyncDocError::Unauthorized
                                }
                                crate::runtime::SyncDocError::Policy(policy) => {
                                    crate::runtime::SyncDocError::Policy(policy.clone())
                                }
                                crate::runtime::SyncDocError::TransportError => {
                                    crate::runtime::SyncDocError::TransportError
                                }
                                crate::runtime::SyncDocError::IoError(error) => {
                                    crate::runtime::SyncDocError::IoError(eyre::eyre!("{error}"))
                                }
                                crate::runtime::SyncDocError::Other(error) => {
                                    crate::runtime::SyncDocError::Other(eyre::eyre!("{error}"))
                                }
                            }
                        } else {
                            crate::runtime::SyncDocError::IoError(eyre::eyre!(
                                "document materialization failed: {error}"
                            ))
                        };
                        self.resolve_sync_peer_result(peer_id, 0, Err(sync_error));
                        if expected_sync_error {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    }
                }
            }
            DocWorkerMsg::SyncWithPeer {
                peer_id,
                waiter_id,
                timeout,
                done,
                _lease,
            } => {
                self.sync_with_peer(peer_id, waiter_id, timeout, done, _lease)
                    .await
            }
            DocWorkerMsg::CancelSyncWithPeer {
                peer_id,
                waiter_id,
                reason,
            } => {
                self.cancel_sync_with_peer(&peer_id, waiter_id, reason);
                Ok(())
            }
            DocWorkerMsg::SyncWithPeerResult {
                peer_id,
                waiter_id,
                result,
            } => {
                match result {
                    Ok(()) => self.mark_sync_completed(peer_id, waiter_id),
                    Err(error) => {
                        self.sync_completed.remove(&peer_id);
                        self.sync_materialized.remove(&peer_id);
                        // Successful sync waiters are resolved only after both
                        // this completion and ApplySyncSession have arrived.
                        self.resolve_sync_peer_result(peer_id, waiter_id, Err(error));
                    }
                }
                Ok(())
            }
            DocWorkerMsg::ReleaseHandleLease => {
                // The hub owns refcounts now via `Runtime2Cmd::ReleaseDocLease`.
                // This message is kept for parity but is a no-op in runtime2;
                // the doc-worker's state is managed through the eviction model
                // in hub.rs.
                Ok(())
            }
            DocWorkerMsg::ReattemptMaterialization => {
                self.retry_materialization().await.map(|_| ())
            }
            DocWorkerMsg::QueryHeadState { resp } => self.query_head_state(resp).await,
            DocWorkerMsg::Quiesce { barrier_id, _lease } => {
                self.quiescence_waiters.push((barrier_id, _lease));
                Ok(())
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// PUT_DOC  (parity with handle_put_doc at runtime.rs:2316)
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Create a new document with initial content.
    ///
    /// 1. Check the worker is `Unloaded` (doc not already occupied).
    /// 2. Stage the automerge content into loose commits + fragments.
    /// 3. Encrypt each blob via `io.encrypt_loose_commit`.
    /// 4. Store each commit via `io.store_commit` (the single atomic write).
    ///    Fragment-boundary commits add to `pending_fragment_requests`.
    /// 5. Persist any CGKA update ops emitted by encryption.
    /// 6. Build a `LiveDocBundle`, transition to `Live`.
    /// 7. Notify `doc_created` + `local_doc_created` via the change manager.
    /// 8. Emit `DocWorkerHandleAcquired` to the hub.
    /// 9. Note local keyhive changed (frontier advanced).
    ///
    /// Mirrors `handle_put_doc` at `runtime.rs:2316`. The old runtime also
    /// called `set_obj_payload` (big_sync) — that is dropped in runtime2;
    /// heads are derived on query.
    async fn put_doc(
        &mut self,
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    ) -> eyre::Result<()> {
        // ── 1. Occupancy check ─────────────────────────────────────────────
        if !matches!(self.state, DocState::Unloaded) {
            let _ = resp.send(Err(ferr!("doc already occupied: {:?}", self.doc_id)));
            return Ok(());
        }

        // ── 2. Stage and encrypt the complete initial sedimentree ─────────
        // An initial fragment may contain its head only inside the fragment;
        // there need not be a loose-commit row from which to recover its
        // content key. The batch encryption path handles that case and keeps
        // the fragment/commit key chain intact.
        let staged = stage_automerge_ingest(&initial_content);
        let encrypted = self
            .io
            .encrypt_initial_sedimentree(self.sed_id, staged.clone())
            .await?;

        // ── 3. Persist the complete tree, then its encryption metadata ────
        let cgka_update_ops = encrypted.cgka_update_ops.clone();
        self.io
            .store_initial_sedimentree(self.sed_id, encrypted)
            .await?;
        for op in cgka_update_ops {
            self.io.persist_cgka_update_op(op).await?;
        }

        // ── 4. Build LiveDocBundle, transition to Live ─────────────────────
        let heads: Arc<[automerge::ChangeHash]> = Arc::from(initial_content.get_heads());

        // NOTE: the old `LiveDocBundle::new` takes a `RuntimeDocLease` which
        // fires `release_doc_lease` on drop. In runtime2, the hub manages
        // leases via `Runtime2Cmd::ReleaseDocLease`. The bundle's lease is a
        // no-op in runtime2 — refcounts live on the hub.
        let bundle = Arc::new(crate::runtime::LiveDocBundle::new_noop(
            self.doc_id,
            *initial_content,
        ));

        self.state = DocState::Live(Arc::downgrade(&bundle));

        // ── 6. Notify ──────────────────────────────────────────────────────
        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))
            .map_err(|_| ferr!("change manager notify_doc_created failed"))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, Arc::clone(&heads))
            .map_err(|_| ferr!("change manager notify_local_doc_created failed"))?;

        self.evt_tx
            .send(crate::runtime2::Runtime2Evt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            })
            .await
            .map_err(|_| ferr!("hub event channel closed"))?;

        // ── 9. Note keyhive changed (initial membership/CGKA state) ───────
        // The initial document membership and CGKA operations were persisted
        // during creation. Emit a note so the hub triggers keyhive sync.
        // NOTE: this uses `note_local_keyhive_changed` through the runtime handle
        // (which sends a cmd to the hub). In runtime2, this is synchronous
        // since the handle's cmd channel routes to the same machine loop.
        // For the blocking-out, we rely on the hub to pick this up.
        self.last_notified_heads = Some(Arc::clone(&heads));
        self.io.note_local_keyhive_changed().await?;

        let _ = resp.send(Ok(bundle));
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ACQUIRE_HANDLE  (parity with handle_acquire_handle at runtime.rs:2410)
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Acquire a live handle to the document.
    ///
    /// - `Live(bundle)` and bundle still alive → upgrade the `Weak`, return
    ///   `Ready(upgraded)`.
    /// - `Transient(doc)` → build a new `LiveDocBundle`, transition to `Live`,
    ///   emit `DocWorkerHandleAcquired`.
    /// - `Unloaded` / `PendingMaterialization` → attempt to load + decrypt
    ///   the sedimentree via `load_doc_snapshot` (hydrate + decrypt walk).
    ///   - Fully decryptable → `Ready` + `mark_materialization_ready`.
    ///   - Partially decryptable → `PendingMaterialization` +
    ///     `mark_materialization_pending`.
    ///
    /// Mirrors `handle_acquire_handle` at `runtime.rs:2410`.
    async fn acquire_handle(
        &mut self,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>,
        >,
    ) -> eyre::Result<()> {
        let result = match &self.state {
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    self.evt_tx
                        .send(crate::runtime2::Runtime2Evt::DocWorkerHandleAcquired {
                            bundle: Arc::clone(&bundle),
                        })
                        .await
                        .map_err(|_| ferr!("hub event channel closed"))?;
                    crate::runtime::DocLookup::Ready(bundle)
                } else {
                    // Weak reference expired — fall through to re-load below.
                    self.state = DocState::Unloaded;
                    self.take_or_load_transient_doc().await?
                }
            }
            DocState::Transient(_) => {
                // Take ownership and build a bundle.
                let doc = match std::mem::replace(&mut self.state, DocState::Unloaded) {
                    DocState::Transient(doc) => doc,
                    _ => unreachable!(),
                };
                let bundle = Arc::new(crate::runtime::LiveDocBundle::new_noop(self.doc_id, *doc));
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.evt_tx
                    .send(crate::runtime2::Runtime2Evt::DocWorkerHandleAcquired {
                        bundle: Arc::clone(&bundle),
                    })
                    .await
                    .map_err(|_| ferr!("hub event channel closed"))?;
                crate::runtime::DocLookup::Ready(bundle)
            }
            DocState::Unloaded | DocState::PendingMaterialization => {
                self.take_or_load_transient_doc().await?
            }
        };
        let _ = resp.send(Ok(result));
        Ok(())
    }

    /// Load or re-load the document from storage (hydrate + decrypt walk).
    ///
    /// Mirrors the old `load_doc_snapshot` at `runtime.rs:3610`.
    /// Returns:
    /// - `Ready(doc)` if fully decryptable.
    /// - `PendingMaterialization` if undecryptable.
    /// - `Missing` if no sedimentree content exists.
    async fn load_doc_snapshot(
        &self,
    ) -> eyre::Result<crate::runtime::DocLookup<automerge::Automerge>> {
        // Hydrate the tree from storage.
        let Some(mut tree) = self.io.hydrate_tree(self.sed_id).await? else {
            return Ok(crate::runtime::DocLookup::Missing);
        };

        // Ensure minimized (lazy dirty-gated — cheap if already clean).
        // `MinimizedSedimentree::ensure_minimized` takes a DepthMetric.
        tree.ensure_minimized(&sedimentree_core::depth::CountLeadingZeroBytes);

        // Walk and decrypt in the sedimentree's causal topological order.
        // This is the same order used by sync-session materialization; the
        // lookup/reload path must not have a second, unordered loader.
        let order = tree
            .topsorted_blob_order()
            .map_err(|e| ferr!("failed ordering document blobs: {e}"))?;
        let fragments: Vec<_> = tree.fragments().collect();
        let commits: Vec<_> = tree.loose_commits().collect();
        let mut plaintexts = std::collections::HashMap::<Vec<u8>, Vec<u8>>::new();

        for item in &order {
            let (kind, head) = match item {
                sedimentree_core::sedimentree::SedimentreeItem::Fragment(index) => (
                    crate::runtime::BigRepoCiphertextKind::Fragment,
                    fragments[*index].head(),
                ),
                sedimentree_core::sedimentree::SedimentreeItem::LooseCommit(index) => (
                    crate::runtime::BigRepoCiphertextKind::LooseCommit,
                    commits[*index].head(),
                ),
            };
            let locator = crate::runtime::BigRepoCiphertextLocator::new(kind, self.sed_id, head);
            let result = self.io.try_causal_decrypt(self.sed_id, locator).await?;
            plaintexts.extend(result.complete);
        }

        let mut doc = automerge::Automerge::new();
        let mut made_progress = false;
        for item in &order {
            let content_ref = match item {
                sedimentree_core::sedimentree::SedimentreeItem::Fragment(index) => {
                    fragments[*index].head().as_bytes().to_vec()
                }
                sedimentree_core::sedimentree::SedimentreeItem::LooseCommit(index) => {
                    commits[*index].head().as_bytes().to_vec()
                }
            };
            let Some(plaintext) = plaintexts.remove(&content_ref) else {
                // This branch is not decryptable with the currently available
                // entrypoint key. Other independent branches may still load.
                continue;
            };
            match doc.load_incremental(&plaintext) {
                Ok(_) => made_progress = true,
                Err(automerge::AutomergeError::MissingDeps) => {
                    return Err(ferr!(
                        "topologically ordered document blob has missing Automerge dependencies"
                    ));
                }
                Err(error) => return Err(ferr!("automerge load_incremental failed: {error}")),
            }
        }

        if !made_progress {
            return Ok(crate::runtime::DocLookup::PendingMaterialization);
        }

        Ok(crate::runtime::DocLookup::Ready(doc))
    }

    /// `take_or_load_transient_doc` — mirror of old `runtime.rs:3295`.
    ///
    /// Takes ownership of the current state (replacing with `Unloaded`), then
    /// either returns the existing `Transient` doc or attempts to load from
    /// storage. Handles the `was_pending` deduplication for the
    /// `mark_materialization_*` transition helpers.
    async fn take_or_load_transient_doc(
        &mut self,
    ) -> eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let out = match std::mem::replace(&mut self.state, DocState::Unloaded) {
            DocState::Transient(doc) => {
                // Already have a transient doc; wrap in Live bundle.
                let bundle = Arc::new(crate::runtime::LiveDocBundle::new_noop(self.doc_id, *doc));
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.evt_tx
                    .send(crate::runtime2::Runtime2Evt::DocWorkerHandleAcquired {
                        bundle: Arc::clone(&bundle),
                    })
                    .await
                    .map_err(|_| ferr!("hub event channel closed"))?;
                crate::runtime::DocLookup::Ready(bundle)
            }
            DocState::Unloaded | DocState::PendingMaterialization => {
                let loaded = self.load_doc_snapshot().await?;
                match loaded {
                    crate::runtime::DocLookup::Ready(doc) => {
                        let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                        self.last_notified_heads = Some(Arc::clone(&heads));
                        self.transition_to_ready(was_pending, Arc::clone(&heads))
                            .await?;
                        let bundle =
                            Arc::new(crate::runtime::LiveDocBundle::new_noop(self.doc_id, doc));
                        self.state = DocState::Live(Arc::downgrade(&bundle));
                        self.evt_tx
                            .send(crate::runtime2::Runtime2Evt::DocWorkerHandleAcquired {
                                bundle: Arc::clone(&bundle),
                            })
                            .await
                            .map_err(|_| ferr!("hub event channel closed"))?;
                        crate::runtime::DocLookup::Ready(bundle)
                    }
                    crate::runtime::DocLookup::PendingMaterialization => {
                        self.transition_to_pending(was_pending).await?;
                        self.state = DocState::PendingMaterialization;
                        crate::runtime::DocLookup::PendingMaterialization
                    }
                    crate::runtime::DocLookup::Missing => crate::runtime::DocLookup::Missing,
                }
            }
            DocState::Live(bundle) => {
                // Shouldn't be called when Live — restore and bail.
                self.state = DocState::Live(bundle);
                eyre::bail!("document already live")
            }
        };
        Ok(out)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMIT_DELTA  (parity with handle_commit_delta at runtime.rs:2446)
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Commit a set of changes locally.
    ///
    /// 1. [`encrypt_commits`](Self::encrypt_commits) — encrypt each loose commit
    ///    blob via `io.encrypt_loose_commit`, collect CGKA ops.
    /// 2. [`persist_commits_and_heads`](Self::persist_commits_and_heads) — store
    ///    each encrypted commit via `io.store_commit` (the single atomic write).
    /// 3. [`notify_heads_changed`](Self::notify_heads_changed) — emit change
    ///    notifications (materialized heads from the live doc, in-hand — no
    ///    big_sync `set_obj_payload`).
    /// 4. Process pending fragment requests (boundary commits awaiting fragments).
    /// 5. Note local keyhive changed if CGKA ops were emitted.
    ///
    /// Mirrors `handle_commit_delta` at `runtime.rs:2446`.
    async fn commit_delta(
        &mut self,
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> eyre::Result<()> {
        // ── 1. Encrypt ─────────────────────────────────────────────────────
        let (encrypted, cgka_ops) = self.encrypt_commits(&commits).await?;

        // ── 2. Persist (the single atomic write via DocIo) ─────────────────
        self.persist_commits_and_heads(&encrypted).await?;

        // ── 3. Notify heads changed ────────────────────────────────────────
        self.notify_heads_changed(Arc::from(heads.clone()), patches, origin)
            .await?;
        // Keep the query-facing materialized frontier synchronized with the
        // caller's actual Automerge heads even when notification coalescing
        // suppresses an event.
        self.last_notified_heads = Some(Arc::from(heads));

        // ── 4. Process pending fragment requests ───────────────────────────
        self.process_pending_fragment_requests().await?;

        // ── 5. Persist CGKA ops and note keyhive changed ───────────────────
        let keyhive_changed = !cgka_ops.is_empty();
        for op in &cgka_ops {
            self.io.persist_cgka_update_op(op.clone()).await?;
        }
        if keyhive_changed {
            // Refresh the protocol cache before acknowledging the document
            // write. Otherwise a caller can immediately initiate Keyhive sync
            // against the pre-commit cache and publish an encrypted commit
            // whose new epoch is not yet visible to the peer.
            self.io.note_local_keyhive_changed().await?;

            self.evt_tx
                .send(crate::runtime2::Runtime2Evt::CgkaOp {
                    data: Arc::new(cgka_ops.into_iter().next().unwrap()),
                })
                .await
                .map_err(|_| ferr!("hub event channel closed"))?;
        }

        let _ = resp.send(Ok(()));
        Ok(())
    }

    /// Step 1: Encrypt each loose commit blob.
    ///
    /// Calls [`DocIo::encrypt_loose_commit`] per commit, passing the raw
    /// plaintext blob. Collects any CGKA update ops emitted by keyhive for
    /// later persistence.
    ///
    /// Mirrors the encrypt loop in `handle_commit_delta` at `runtime.rs:2452-2474`.
    async fn encrypt_commits(
        &self,
        commits: &[(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )],
    ) -> eyre::Result<(
        Vec<crate::runtime2::EncryptedLooseCommit>,
        Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
    )> {
        let mut encrypted = Vec::with_capacity(commits.len());
        let mut cgka_ops = Vec::new();

        for (head, parents, blob) in commits {
            let result = self
                .io
                .encrypt_loose_commit(self.sed_id, *head, parents.clone(), blob.clone())
                .await?;
            if let Some(ref op) = result.cgka_update_op {
                cgka_ops.push(op.clone());
            }
            encrypted.push(result);
        }

        Ok((encrypted, cgka_ops))
    }

    /// Step 2: Persist encrypted commits atomically.
    ///
    /// Calls [`DocIo::store_commit`] for each commit. This is the single atomic
    /// write — subduction records the commit and advances the sedimentree
    /// frontier. Fragment-boundary commits add to `pending_fragment_requests`.
    ///
    /// Mirrors the store loop in `handle_commit_delta` at `runtime.rs:2476-2497`.
    async fn persist_commits_and_heads(
        &mut self,
        encrypted: &[crate::runtime2::EncryptedLooseCommit],
    ) -> eyre::Result<()> {
        for commit in encrypted {
            let maybe_request = self.io.store_commit(self.sed_id, commit.clone()).await?;
            if let Some(request) = maybe_request {
                self.pending_fragment_requests.insert(request);
            }
        }
        Ok(())
    }

    /// Step 3: Notify listeners that heads changed.
    ///
    /// Calls the change manager's `notify_doc_heads_changed` and
    /// `notify_local_doc_heads_changed` with the materialized heads (from the
    /// live automerge doc, in-hand). There is **no big_sync** `set_obj_payload`
    /// call — heads are derived on query.
    ///
    /// Mirrors `commit_delta_bookkeep` at `runtime.rs:3403` minus the
    /// `set_obj_payload` call.
    async fn notify_heads_changed(
        &mut self,
        heads: Arc<[automerge::ChangeHash]>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        // No-op detection: skip notification if heads haven't changed.
        if self.last_notified_heads.as_ref() == Some(&heads) {
            return Ok(());
        }

        self.change_manager
            .notify_doc_heads_changed(self.doc_id, Arc::clone(&heads), origin.clone())
            .map_err(|_| ferr!("change manager notify_doc_heads_changed failed"))?;

        // Fire patches even if heads didn't change (delta can have content
        // changes within the same head set — e.g. tombstone compaction).
        for patch in &patches {
            self.change_manager
                .notify_doc_changed(
                    self.doc_id,
                    Arc::new(patch.clone()),
                    Arc::clone(&heads),
                    origin.clone(),
                )
                .map_err(|_| ferr!("change manager notify_doc_changed failed"))?;
        }

        self.last_notified_heads = Some(heads);
        Ok(())
    }

    /// Process fragment-boundary commits stored during the write.
    /// Builds and stores fragments for each pending request.
    async fn process_pending_fragment_requests(&mut self) -> eyre::Result<()> {
        if self.pending_fragment_requests.is_empty() {
            return Ok(());
        }
        let bundle = match &self.state {
            DocState::Live(bundle) => bundle
                .upgrade()
                .ok_or_else(|| ferr!("live document expired while storing fragment"))?,
            _ => return Ok(()),
        };
        let requests = std::mem::take(&mut self.pending_fragment_requests);
        let sed_id = sedimentree_core::id::SedimentreeId::new(self.doc_id.into_bytes());
        for request in requests {
            let (boundary, checkpoints, raw_blob) = {
                let doc = bundle.doc.lock().await;
                let fragment = doc
                    .get_fragment(automerge::ChangeHash(*request.head().as_bytes()))
                    .ok_or_else(|| ferr!("requested Automerge fragment is unavailable"))?;
                let raw_blob = doc
                    .bundle(fragment.members.iter().cloned())
                    .wrap_err("unable to resolve bundle for fragment")?
                    .bytes()
                    .to_vec();
                let boundary = fragment
                    .boundary
                    .iter()
                    .map(|head| sedimentree_core::loose_commit::id::CommitId::new(head.0))
                    .collect();
                let checkpoints = fragment
                    .checkpoints
                    .iter()
                    .map(|head| sedimentree_core::loose_commit::id::CommitId::new(head.0))
                    .collect();
                (boundary, checkpoints, raw_blob)
            };
            self.io
                .store_fragment(sed_id, request.head(), boundary, checkpoints, raw_blob)
                .await?;
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STATE TRANSITIONS  (parity with mark_materialization_* at runtime.rs:3290)
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Transition to `PendingMaterialization`. Emits
    /// `DocWorkerMaterializationPending` only if `was_pending` is false (dedupe).
    ///
    /// Mirrors `mark_materialization_pending` at `runtime.rs:3290`.
    async fn transition_to_pending(&mut self, was_pending: bool) -> eyre::Result<()> {
        self.state = DocState::PendingMaterialization;
        if !was_pending {
            self.change_manager
                .notify_local_doc_materialization_pending(self.doc_id)
                .map_err(|_| ferr!("change manager materialization_pending failed"))?;
            self.evt_tx
                .send(
                    crate::runtime2::Runtime2Evt::DocWorkerMaterializationPending {
                        doc_id: self.doc_id,
                    },
                )
                .await
                .map_err(|_| ferr!("hub event channel closed"))?;
        }
        Ok(())
    }

    /// Transition from `PendingMaterialization` to materialized.
    /// Emits `DocWorkerMaterializationReady` only if `was_pending`.
    ///
    /// Mirrors `mark_materialization_ready` at `runtime.rs:3305`.
    async fn transition_to_ready(
        &mut self,
        was_pending: bool,
        heads: Arc<[automerge::ChangeHash]>,
    ) -> eyre::Result<()> {
        if was_pending {
            self.change_manager
                .notify_local_doc_materialization_ready(self.doc_id, heads)
                .map_err(|_| ferr!("change manager materialization_ready failed"))?;
            self.evt_tx
                .send(
                    crate::runtime2::Runtime2Evt::DocWorkerMaterializationReady {
                        doc_id: self.doc_id,
                    },
                )
                .await
                .map_err(|_| ferr!("hub event channel closed"))?;
        }
        Ok(())
    }

    /// Whether the fast-path (relay mode) applies: no change-listener interest
    /// for remote changes + the doc is not yet materialized.
    ///
    /// Mirrors the condition in `handle_apply_sync_session` at `runtime.rs:~2600`
    /// (`!wants_patches && state ∈ {Unloaded, PendingMaterialization}`).
    fn should_fast_path_pending(&self, peer_id: PeerId) -> bool {
        !self.change_manager.has_change_listener_interest(
            self.doc_id,
            &crate::changes::BigRepoChangeOrigin::Remote { peer_id },
        ) && matches!(
            self.state,
            DocState::Unloaded | DocState::PendingMaterialization
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUERY_HEAD_STATE  — NEW (the flake-detector op)
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Query the current head state (sedimentree heads + materialized heads).
    ///
    /// This is the NEW operation that replaces the overloaded
    /// `doc_payload_heads` query (which returned either sedimentree heads or
    /// automerge heads depending on state, causing the head-divergence flake).
    ///
    /// - `sedimentree_heads`: always derived via `io.sedimentree_heads()`.
    /// - `materialized_heads`: `Some(...)` when the doc is materialized
    ///   (`Live` or `Transient`), `None` otherwise.
    /// - `state`: mapped from [`DocState`] to [`MaterializationState`].
    async fn query_head_state(
        &self,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
    ) -> eyre::Result<()> {
        let sedimentree_heads_commit_ids = self.io.sedimentree_heads(self.sed_id).await?;

        // Convert CommitIds to ChangeHashes for the public API.
        let sedimentree_heads: Arc<[automerge::ChangeHash]> = sedimentree_heads_commit_ids
            .iter()
            .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
            .collect();

        let (materialized_heads, state) = match &self.state {
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    let doc = bundle.doc.lock().await;
                    let heads: Arc<[automerge::ChangeHash]> = self
                        .last_notified_heads
                        .clone()
                        .unwrap_or_else(|| Arc::from(doc.get_heads()));
                    (
                        Some(heads),
                        crate::runtime2::MaterializationState::Materialized,
                    )
                } else {
                    // Weak reference expired — treat as Unloaded.
                    (None, crate::runtime2::MaterializationState::Missing)
                }
            }
            DocState::Transient(doc) => {
                let heads: Arc<[automerge::ChangeHash]> = self
                    .last_notified_heads
                    .clone()
                    .unwrap_or_else(|| Arc::from(doc.get_heads()));
                (
                    Some(heads),
                    crate::runtime2::MaterializationState::Materialized,
                )
            }
            DocState::PendingMaterialization => {
                (None, crate::runtime2::MaterializationState::Pending)
            }
            DocState::Unloaded => {
                if sedimentree_heads.is_empty() {
                    (None, crate::runtime2::MaterializationState::Missing)
                } else {
                    (None, crate::runtime2::MaterializationState::Pending)
                }
            }
        };

        let _ = resp.send(Ok(crate::runtime2::DocHeadState {
            sedimentree_heads,
            materialized_heads,
            state,
        }));
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// APPLY_SYNC_SESSION  (parity with handle_apply_sync_session at runtime.rs:~2530)
//
// 🚨 NOOP-AFTER-CRASH FIX (bug #2, see play.big_repo.current_fixes.md)
//   The old handle_apply_sync_session early-returned when `received_refs` was
//   empty, skipping the sedimentree-heads refresh entirely. After a crash the
//   recorded heads went stale. This implementation ALWAYS re-derives heads
//   from the sedimentree, even on the empty-refs path, via
//   `refresh_heads_from_sedimentree`.
// ═══════════════════════════════════════════════════════════════════════════

impl<F: FutureForm> DocWorker2<F> {
    /// Apply a sync session from a peer.
    ///
    /// Step sequence:
    /// 1. [`hydrate_tree`] — get the minimized sedimentree via [`DocIo::hydrate_tree`].
    /// 2. Try to decrypt the session's blobs via
    ///    [`try_decrypt_session_blobs`] (causal decrypt loop).
    /// 3. **Decision branch:**
    ///    - **Undecryptable** → [`record_pending_heads`] + [`transition_to_pending`]
    ///      (uses sedimentree heads from the tree).
    ///    - **Decryptable** → [`materialize_from_blobs`] (load into Automerge),
    ///      snapshot `before_heads`, apply blobs, no-op detect, notify via
    ///      [`notify_heads_changed`] (materialized heads, in-hand — no write-back).
    /// 4. Returns whether materialization is still pending.
    ///
    /// Mirrors `handle_apply_sync_session` at `runtime.rs:~2530`.
    ///
    /// # Noop-after-crash fix
    ///
    /// Even when `received_refs` is empty (nothing new from the peer), we
    /// still hydrate the tree and refresh recorded heads from the sedimentree.
    /// This ensures that after a crash, the heads stored in the part-store are
    /// brought in sync with the sedimentree — the old code skipped this and
    /// left stale heads behind.
    #[tracing::instrument(skip_all)]
    async fn apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> eyre::Result<()> {
        let peer_id = PeerId::new(*session.peer_id.as_bytes());
        if let Some(rejection) = session.remote_rejection {
            return Err(match rejection {
                subduction_core::sync_session::SyncRemoteRejection::NotFound => {
                    crate::runtime::SyncDocError::NotFound.into()
                }
                subduction_core::sync_session::SyncRemoteRejection::Unauthorized => {
                    crate::runtime::SyncDocError::Unauthorized.into()
                }
                subduction_core::sync_session::SyncRemoteRejection::Policy(kind) => {
                    let policy = match kind {
                        subduction_core::sync_session::SyncPolicyRejectionKind::DocumentNotFound => {
                            crate::runtime::SyncDocPolicyError::DocumentNotFound
                        }
                        subduction_core::sync_session::SyncPolicyRejectionKind::InsufficientAccess => {
                            crate::runtime::SyncDocPolicyError::InsufficientAccess
                        }
                        subduction_core::sync_session::SyncPolicyRejectionKind::InvalidIdentifier => {
                            crate::runtime::SyncDocPolicyError::InvalidIdentifier
                        }
                        subduction_core::sync_session::SyncPolicyRejectionKind::Other => {
                            crate::runtime::SyncDocPolicyError::Other("remote policy rejection".into())
                        }
                    };
                    crate::runtime::SyncDocError::Policy(policy).into()
                }
            });
        }
        if let Some((_, rejection)) = session
            .rejected_commit_ids
            .first()
            .or_else(|| session.rejected_fragment_ids.first())
        {
            let kind = match rejection.kind {
                subduction_core::sync_session::SyncPolicyRejectionKind::DocumentNotFound => {
                    crate::runtime::SyncDocPolicyError::DocumentNotFound
                }
                subduction_core::sync_session::SyncPolicyRejectionKind::InsufficientAccess => {
                    crate::runtime::SyncDocPolicyError::InsufficientAccess
                }
                subduction_core::sync_session::SyncPolicyRejectionKind::InvalidIdentifier => {
                    crate::runtime::SyncDocPolicyError::InvalidIdentifier
                }
                subduction_core::sync_session::SyncPolicyRejectionKind::Other => {
                    crate::runtime::SyncDocPolicyError::Other(rejection.reason.clone())
                }
            };
            return Err(crate::runtime::SyncDocError::Policy(kind).into());
        }
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let wants_patches = self.change_manager.has_change_listener_interest(
            self.doc_id,
            &crate::changes::BigRepoChangeOrigin::Remote { peer_id },
        );

        // ── Collect received content refs (for decryption ordering) ──────
        let received_refs: std::collections::HashSet<Vec<u8>> = session
            .received_commit_ids
            .iter()
            .map(|cid| cid.as_bytes().to_vec())
            .chain(
                session
                    .received_fragment_ids
                    .iter()
                    .map(|fid| fid.as_bytes().to_vec()),
            )
            .collect();

        // 🚨 NOOP-AFTER-CRASH FIX (bug #2):
        // Even with empty received_refs, we MUST hydrate the tree and refresh
        // heads from the sedimentree. The old code early-returned here without
        // refreshing, letting recorded heads go stale after a crash.
        if received_refs.is_empty() {
            // Refresh the recorded frontier even when this transport session
            // transferred no blobs. Do not reload over a live bundle, though:
            // an empty inbound session can race a local commit whose encrypted
            // storage write has not become visible to the snapshot loader yet.
            // Reloading then would replace the live Automerge document with an
            // older snapshot and lose the local materialized frontier.
            self.refresh_heads_from_sedimentree(peer_id).await?;
            let needs_reload = match &self.state {
                DocState::Live(bundle) => bundle.upgrade().is_none(),
                DocState::Transient(_) | DocState::Unloaded | DocState::PendingMaterialization => {
                    true
                }
            };
            if needs_reload {
                return self.retry_materialization().await.map(|_| ());
            }
            return Ok(());
        }

        // ── Step 1: Hydrate the tree ────────────────────────────────────
        let Some(mut tree) = self.io.hydrate_tree(self.sed_id).await? else {
            // No sedimentree content — record pending with empty heads.
            self.record_pending_heads(None, peer_id).await?;
            self.transition_to_pending(was_pending).await?;
            return Ok(());
        };
        tree.ensure_minimized(&sedimentree_core::depth::CountLeadingZeroBytes);

        let fragments: Vec<_> = tree.fragments().collect();
        let commits: Vec<_> = tree.loose_commits().collect();

        // Topsorted blob order (for causal ordering).
        let order = tree
            .topsorted_blob_order()
            .map_err(|e| ferr!("failed ordering sync session blobs: {e}"))?;
        let received_all_ordered_blobs = received_refs.len() == order.len();

        // ── Fast path: relay mode (no change listener, not materialized) ──
        if self.should_fast_path_pending(peer_id) {
            self.record_pending_heads(Some(&mut tree), peer_id).await?;
            return Ok(());
        }

        // ── Step 2: Try to decrypt session blobs ─────────────────────────
        let (blobs, materialization_pending) = self
            .try_decrypt_session_blobs(&fragments, &commits, &order, &received_refs, &session)
            .await?;

        // ── Step 3: Decision branch ──────────────────────────────────────
        if blobs.is_empty() {
            self.record_pending_heads(Some(&mut tree), peer_id).await?;
            if materialization_pending {
                self.transition_to_pending(was_pending).await?;
            }
            return Ok(());
        }
        if materialization_pending {
            // A readable causal branch is available even though another
            // branch is still waiting for keys. Keep the branch materialized;
            // the next sync/keyhive event will retry the missing branch.
            self.record_pending_heads(Some(&mut tree), peer_id).await?;
        }

        // ── Decryptable: materialize ─────────────────────────────────────
        let maybe_delta = self
            .materialize_from_blobs(&blobs, received_all_ordered_blobs, peer_id, was_pending)
            .await?;

        let Some((after_heads, patches)) = maybe_delta else {
            return Ok(());
        };

        // Notify with materialized heads (in-hand — no write-back).
        let heads_arc = Arc::<[automerge::ChangeHash]>::from(after_heads);
        self.change_manager
            .notify_doc_heads_changed(
                self.doc_id,
                Arc::clone(&heads_arc),
                crate::changes::BigRepoChangeOrigin::Remote { peer_id },
            )
            .map_err(|_| ferr!("change manager notify_doc_heads_changed failed"))?;
        for patch in &patches {
            self.change_manager
                .notify_doc_changed(
                    self.doc_id,
                    Arc::new(patch.clone()),
                    Arc::clone(&heads_arc),
                    crate::changes::BigRepoChangeOrigin::Remote { peer_id },
                )
                .map_err(|_| ferr!("change manager notify_doc_changed failed"))?;
        }

        self.last_notified_heads = Some(heads_arc);
        Ok(())
    }

    /// Try to decrypt blobs from a sync session.
    ///
    /// Iterates the causal-decrypt loop until all blobs are decrypted or
    /// progress stalls (a key is missing). Returns `(blobs, stalled)` where
    /// `stalled` means materialization is pending (undecryptable).
    ///
    /// Mirrors the decrypt loop in `handle_apply_sync_session` at
    /// `runtime.rs:2646-2775`.
    async fn try_decrypt_session_blobs(
        &self,
        fragments: &[&sedimentree_core::fragment::Fragment],
        commits: &[&sedimentree_core::loose_commit::LooseCommit],
        order: &[sedimentree_core::sedimentree::SedimentreeItem],
        received_refs: &std::collections::HashSet<Vec<u8>>,
        session: &subduction_core::sync_session::SyncSession,
    ) -> eyre::Result<(Vec<Vec<u8>>, bool)> {
        // Filter the order to only received items.
        let received_order: Vec<(&sedimentree_core::sedimentree::SedimentreeItem, Vec<u8>)> = order
            .iter()
            .filter_map(|item| {
                let content_ref = match item {
                    sedimentree_core::sedimentree::SedimentreeItem::Fragment(idx) => {
                        fragments.get(*idx).map(|f| f.head().as_bytes().to_vec())
                    }
                    sedimentree_core::sedimentree::SedimentreeItem::LooseCommit(idx) => {
                        commits.get(*idx).map(|c| c.head().as_bytes().to_vec())
                    }
                };
                content_ref.and_then(|cr| received_refs.contains(&cr).then_some((item, cr)))
            })
            .collect();

        let expected = session.received_commit_ids.len() + session.received_fragment_ids.len();
        if received_order.len() != expected {
            eyre::bail!(
                "sync session received blobs are missing from sedimentree order: expected={expected} found={}",
                received_order.len()
            );
        }

        let mut plaintext_by_ref: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        let mut plaintext_by_index: Vec<Option<Vec<u8>>> = std::iter::repeat_with(|| None)
            .take(received_order.len())
            .collect();
        let mut made_progress = true;
        let mut materialization_pending = false;

        while made_progress && plaintext_by_index.iter().any(Option::is_none) {
            made_progress = false;
            for (idx, (item, content_ref)) in received_order.iter().enumerate() {
                if plaintext_by_index[idx].is_some() {
                    continue;
                }
                if let Some(plaintext) = plaintext_by_ref.get(content_ref).cloned() {
                    plaintext_by_index[idx] = Some(plaintext);
                    made_progress = true;
                    continue;
                }

                let locator = match item {
                    sedimentree_core::sedimentree::SedimentreeItem::Fragment(idx) => {
                        let f = fragments
                            .get(*idx)
                            .ok_or_else(|| ferr!("missing fragment at index {idx}"))?;
                        crate::runtime::BigRepoCiphertextLocator::new(
                            crate::runtime::BigRepoCiphertextKind::Fragment,
                            self.sed_id,
                            f.head(),
                        )
                    }
                    sedimentree_core::sedimentree::SedimentreeItem::LooseCommit(idx) => {
                        let c = commits
                            .get(*idx)
                            .ok_or_else(|| ferr!("missing loose commit at index {idx}"))?;
                        crate::runtime::BigRepoCiphertextLocator::new(
                            crate::runtime::BigRepoCiphertextKind::LooseCommit,
                            self.sed_id,
                            c.head(),
                        )
                    }
                };

                // Try entrypoint decrypt first.
                let entrypoint = self
                    .io
                    .try_decrypt_content_keyed(self.sed_id, locator)
                    .await?;
                let Some(entrypoint_raw) = entrypoint else {
                    // Key not found — skip; may resolve via causal chain.
                    continue;
                };

                // `DocIo::try_decrypt_content_keyed` already unwraps the
                // Keyhive envelope and returns the stored plaintext.
                let exact_plaintext = entrypoint_raw;
                plaintext_by_index[idx] = Some(exact_plaintext.clone());
                plaintext_by_ref.insert(content_ref.clone(), exact_plaintext);
                made_progress = true;

                // Then causal decrypt to unlock ancestors.
                let state = self.io.try_causal_decrypt(self.sed_id, locator).await?;
                for (ancestor_ref, ancestor_plaintext) in &state.complete {
                    if plaintext_by_ref
                        .insert(ancestor_ref.clone(), ancestor_plaintext.clone())
                        .is_none()
                    {
                        made_progress = true;
                    }
                }
            }
        }

        if !made_progress && plaintext_by_index.iter().any(Option::is_none) {
            materialization_pending = true;
        }

        let blobs: Vec<Vec<u8>> = plaintext_by_index.into_iter().flatten().collect();
        if materialization_pending {
            return Ok((blobs, true));
        }
        Ok((blobs, false))
    }

    /// Materialize decrypted blobs into the automerge document.
    ///
    /// Handles all `DocState` variants:
    /// - `Unloaded` / `PendingMaterialization`: build fresh Automerge doc,
    ///   compare before/after heads, emit patches.
    /// - `Transient(doc)`: mutate in-place, compare before/after.
    /// - `Live(bundle)`: lock the shared doc, mutate, compare.
    ///
    /// Returns `Some((after_heads, patches))` if the heads changed or there
    /// was no cached prior state, or `None` if the sync was a true no-op
    /// (same heads, cached state existed).
    ///
    /// Mirrors the materialization switch in `handle_apply_sync_session` at
    /// `runtime.rs:2808-2997`. No big_sync reads — before-heads are derived
    /// from the live doc state or [`DocIo::sedimentree_heads`].
    async fn materialize_from_blobs(
        &mut self,
        blobs: &[Vec<u8>],
        received_all_ordered_blobs: bool,
        peer_id: PeerId,
        was_pending: bool,
    ) -> eyre::Result<Option<(Vec<automerge::ChangeHash>, Vec<automerge::Patch>)>> {
        let wants_patches = self.change_manager.has_change_listener_interest(
            self.doc_id,
            &crate::changes::BigRepoChangeOrigin::Remote { peer_id },
        );
        let make_patches = |doc: &automerge::Automerge,
                            before: &[automerge::ChangeHash],
                            after: &[automerge::ChangeHash]| {
            if wants_patches {
                doc.diff(before, after)
            } else {
                Vec::new()
            }
        };

        // Determine before-heads based on state (no big_sync read).
        let (before_heads, had_cached_before) = self.before_heads_for_materialization().await?;
        // ── Per-state materialization ──────────────────────────────────────
        let maybe_delta = match std::mem::replace(&mut self.state, DocState::Unloaded) {
            DocState::Unloaded | DocState::PendingMaterialization => {
                if received_all_ordered_blobs {
                    // Fresh full sync: build doc from scratch.
                    let mut doc = automerge::Automerge::new();
                    for blob in blobs {
                        doc.load_incremental(blob)
                            .map_err(|e| ferr!("failed applying full sync blob: {e}"))?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if before_heads == after_heads {
                        if had_cached_before {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = make_patches(&doc, &before_heads, &after_heads);
                        Some((after_heads, patches))
                    };
                    self.transition_to_ready(was_pending, Arc::from(doc.get_heads()))
                        .await?;
                    self.state = DocState::Transient(Box::new(doc));
                    out
                } else {
                    // Partial sync: load existing snapshot, apply blobs.
                    let loaded = self.load_doc_snapshot().await?;
                    let mut doc = match loaded {
                        crate::runtime::DocLookup::Ready(doc) => {
                            let heads = doc.get_heads();
                            self.transition_to_ready(was_pending, Arc::from(heads))
                                .await?;
                            doc
                        }
                        crate::runtime::DocLookup::PendingMaterialization => {
                            self.transition_to_pending(was_pending).await?;
                            return Ok(None);
                        }
                        crate::runtime::DocLookup::Missing => automerge::Automerge::new(),
                    };
                    let loaded_heads = doc.get_heads();
                    // The loaded snapshot is the actual Automerge baseline.
                    // The storage frontier may already include the received
                    // commit even when the snapshot walk did not, so never
                    // skip `blobs` merely because `loaded_heads` differs from
                    // the cached/storage heads. Duplicate Automerge changes
                    // are harmless; missing newly received changes are not.
                    for blob in blobs {
                        doc.load_incremental(blob)
                            .map_err(|e| ferr!("failed applying sync blob: {e}"))?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if loaded_heads == after_heads {
                        if had_cached_before {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = make_patches(&doc, &loaded_heads, &after_heads);
                        Some((after_heads, patches))
                    };
                    self.state = DocState::Transient(Box::new(doc));
                    out
                }
            }
            DocState::Transient(mut doc) => {
                let out = {
                    let before = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(blob)
                            .map_err(|e| ferr!("failed applying sync blob: {e}"))?;
                    }
                    let after_heads = doc.get_heads();
                    if before == after_heads {
                        if had_cached_before {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = make_patches(&doc, &before, &after_heads);
                        Some((after_heads, patches))
                    }
                };
                self.state = DocState::Transient(doc);
                out
            }
            DocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    let mut doc = bundle.doc.lock().await;
                    let before = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(blob)
                            .map_err(|e| ferr!("failed applying sync blob: {e}"))?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if before == after_heads {
                        if had_cached_before {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = make_patches(&doc, &before, &after_heads);
                        Some((after_heads, patches))
                    };
                    drop(doc);
                    self.state = DocState::Live(Arc::downgrade(&bundle));
                    out
                }
                None => {
                    // Weak expired — same as Unloaded path.
                    if received_all_ordered_blobs {
                        let mut doc = automerge::Automerge::new();
                        for blob in blobs {
                            doc.load_incremental(blob)
                                .map_err(|e| ferr!("failed applying full sync blob: {e}"))?;
                        }
                        let after_heads = doc.get_heads();
                        let out = if before_heads == after_heads {
                            if had_cached_before {
                                None
                            } else {
                                Some((after_heads, Vec::new()))
                            }
                        } else {
                            let patches = make_patches(&doc, &before_heads, &after_heads);
                            Some((after_heads, patches))
                        };
                        self.transition_to_ready(was_pending, Arc::from(doc.get_heads()))
                            .await?;
                        self.state = DocState::Transient(Box::new(doc));
                        out
                    } else {
                        let loaded = self.load_doc_snapshot().await?;
                        let mut doc = match loaded {
                            crate::runtime::DocLookup::Ready(doc) => {
                                let heads = doc.get_heads();
                                self.transition_to_ready(was_pending, Arc::from(heads))
                                    .await?;
                                doc
                            }
                            crate::runtime::DocLookup::PendingMaterialization => {
                                self.transition_to_pending(was_pending).await?;
                                return Ok(None);
                            }
                            crate::runtime::DocLookup::Missing => automerge::Automerge::new(),
                        };
                        // The snapshot may already include the received
                        // blobs because storage is updated before materialization.
                        // Compare against the last notified frontier instead of
                        // the freshly hydrated snapshot so remote changes still
                        // emit notifications after the live handle is dropped.
                        for blob in blobs {
                            doc.load_incremental(blob)
                                .map_err(|e| ferr!("failed applying sync blob: {e}"))?;
                        }
                        let after_heads = doc.get_heads();
                        let out = if before_heads == after_heads {
                            if had_cached_before {
                                None
                            } else {
                                Some((after_heads, Vec::new()))
                            }
                        } else {
                            let patches = make_patches(&doc, &before_heads, &after_heads);
                            Some((after_heads, patches))
                        };
                        self.state = DocState::Transient(Box::new(doc));
                        out
                    }
                }
            },
        };
        Ok(maybe_delta)
    }

    /// Determine the "before" heads for the materialization no-op comparison.
    ///
    /// In the old runtime this read `partition_doc_heads_payload` (big_sync).
    /// In runtime2: if we have a live or transient doc, we use its current
    /// heads. If not, we use [`DocIo::sedimentree_heads`] (the storage
    /// frontier). There is no cached head store to go stale.
    async fn before_heads_for_materialization(
        &self,
    ) -> eyre::Result<(Vec<automerge::ChangeHash>, bool)> {
        let (heads, had_cached) = match &self.state {
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    let doc = bundle.doc.lock().await;
                    (doc.get_heads(), true)
                } else if let Some(heads) = &self.last_notified_heads {
                    // The storage frontier may already include the incoming
                    // changes. Keep the last materialized frontier as the
                    // notification baseline after a handle is dropped.
                    (heads.to_vec(), true)
                } else {
                    let commit_ids = self.io.sedimentree_heads(self.sed_id).await?;
                    let heads: Vec<automerge::ChangeHash> = commit_ids
                        .iter()
                        .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
                        .collect();
                    (heads, !commit_ids.is_empty())
                }
            }
            DocState::Transient(doc) => (doc.get_heads(), true),
            DocState::Unloaded | DocState::PendingMaterialization => {
                // There is no previously notified materialized frontier on a
                // fresh worker. If a listener requires materialization, compare
                // the hydrated document against an empty baseline so its
                // initial remote state is observable.
                (Vec::new(), false)
            }
        };
        Ok((heads, had_cached))
    }

    /// Record pending (sedimentree) heads when content exists but is
    /// undecryptable. In runtime2 this is a no-op with respect to storage:
    /// heads are derived from the sedimentree, never cached. The notification
    /// still fires so change listeners know the frontier advanced.
    ///
    /// Mirrors the `store_doc_heads_payload` + `notify_doc_pending_heads_changed`
    /// pattern at `runtime.rs:2785` but without the big_sync write.
    async fn record_pending_heads(
        &mut self,
        tree: Option<&mut sedimentree_core::sedimentree::minimized::MinimizedSedimentree>,
        peer_id: PeerId,
    ) -> eyre::Result<()> {
        let heads: Arc<[automerge::ChangeHash]> = if let Some(tree) = tree {
            let commit_ids = tree.heads(&sedimentree_core::depth::CountLeadingZeroBytes);
            commit_ids
                .iter()
                .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
                .collect()
        } else {
            let commit_ids = self.io.sedimentree_heads(self.sed_id).await?;
            commit_ids
                .iter()
                .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
                .collect()
        };

        // TODO(runtime2): the old runtime called `store_doc_heads_payload`
        // (big_sync `set_obj_payload`) here. In runtime2, heads are always
        // derived — no write needed. The notification fires so listeners can
        // react (e.g. the relay layer may want to track frontier progress).
        self.change_manager
            .notify_doc_pending_heads_changed(
                self.doc_id,
                heads,
                crate::changes::BigRepoChangeOrigin::Remote { peer_id },
            )
            .map_err(|_| ferr!("change manager notify_doc_pending_heads_changed failed"))?;
        Ok(())
    }

    /// 🚨 THE NOOP-AFTER-CRASH FIX (bug #2).
    ///
    /// Always re-derive sedimentree heads from the sedimentree and record them
    /// (notify change listeners). This is called even on the empty-refs path
    /// in `apply_sync_session`, ensuring the recorded heads never go stale
    /// after a crash. The old code skipped this entirely when there were no
    /// new incoming blobs.
    ///
    /// IO: [`DocIo::sedimentree_heads`]. No big_sync: heads are derived, not
    /// cached in a separate store.
    async fn refresh_heads_from_sedimentree(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        let commit_ids = self.io.sedimentree_heads(self.sed_id).await?;
        let heads: Arc<[automerge::ChangeHash]> = commit_ids
            .iter()
            .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
            .collect();

        self.change_manager
            .notify_doc_pending_heads_changed(
                self.doc_id,
                heads,
                crate::changes::BigRepoChangeOrigin::Remote { peer_id },
            )
            .map_err(|_| ferr!("change manager notify_doc_pending_heads_changed failed"))?;
        Ok(())
    }

    // ═════════════════════════════════════════════════════════════════════
    // RETRY_MATERIALIZATION  (parity with retry_pending_materialization
    //                         at runtime.rs:3055)
    // ═════════════════════════════════════════════════════════════════════

    /// Retry materialization (e.g. after keyhive sync delivers new keys).
    ///
    /// Hydrates the tree, attempts decrypt walk via [`load_doc_snapshot`].
    /// - If now fully decryptable → `transition_to_ready` + bootstrap notify
    ///   (no big_sync write).
    /// - If still undecryptable → `transition_to_pending` (deduped).
    /// - Returns `true` if still pending, `false` otherwise.
    ///
    /// Mirrors `retry_pending_materialization` at `runtime.rs:3055`.
    async fn retry_materialization(&mut self) -> eyre::Result<bool> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let live_bundle = match &self.state {
            DocState::Live(weak) => weak.upgrade(),
            _ => None,
        };
        match self.load_doc_snapshot().await? {
            crate::runtime::DocLookup::Ready(doc) => {
                if let Some(bundle) = live_bundle {
                    let before = bundle.doc.lock().await.get_heads();
                    let after_heads = doc.get_heads();
                    let patches = doc.diff(&before, &after_heads);
                    *bundle.doc.lock().await = doc;
                    let heads_arc = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    if before.as_slice() != heads_arc.as_ref() {
                        self.change_manager
                            .notify_doc_heads_changed(
                                self.doc_id,
                                Arc::clone(&heads_arc),
                                crate::changes::BigRepoChangeOrigin::Bootstrap,
                            )
                            .map_err(|_| ferr!("change manager notify_doc_heads_changed failed"))?;
                        for patch in &patches {
                            self.change_manager
                                .notify_doc_changed(
                                    self.doc_id,
                                    Arc::new(patch.clone()),
                                    Arc::clone(&heads_arc),
                                    crate::changes::BigRepoChangeOrigin::Bootstrap,
                                )
                                .map_err(|_| ferr!("change manager notify_doc_changed failed"))?;
                        }
                        self.last_notified_heads = Some(heads_arc);
                    }
                    self.state = DocState::Live(Arc::downgrade(&bundle));
                    return Ok(false);
                }

                let after_heads = doc.get_heads();
                self.transition_to_ready(was_pending, Arc::from(after_heads.clone()))
                    .await?;
                if was_pending {
                    let before: Arc<[automerge::ChangeHash]> =
                        self.last_notified_heads.clone().unwrap_or_default();
                    let patches = doc.diff(&before, &after_heads);
                    let heads_arc = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    self.change_manager
                        .notify_doc_heads_changed(
                            self.doc_id,
                            Arc::clone(&heads_arc),
                            crate::changes::BigRepoChangeOrigin::Bootstrap,
                        )
                        .map_err(|_| ferr!("change manager notify_doc_heads_changed failed"))?;
                    for patch in &patches {
                        self.change_manager
                            .notify_doc_changed(
                                self.doc_id,
                                Arc::new(patch.clone()),
                                Arc::clone(&heads_arc),
                                crate::changes::BigRepoChangeOrigin::Bootstrap,
                            )
                            .map_err(|_| ferr!("change manager notify_doc_changed failed"))?;
                    }
                    self.last_notified_heads = Some(heads_arc);
                }
                self.state = DocState::Transient(Box::new(doc));
                Ok(false)
            }
            crate::runtime::DocLookup::PendingMaterialization => {
                self.transition_to_pending(was_pending).await?;
                Ok(true)
            }
            crate::runtime::DocLookup::Missing => Ok(false),
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // SYNC_WITH_PEER  (parity with handle_sync_with_peer at runtime.rs:3199)
    // ═════════════════════════════════════════════════════════════════════

    /// Sync the doc's sedimentree with a peer via subduction.
    ///
    /// Registers the waiter, then starts a subduction sync for this
    /// (doc_id, peer) pair if none is active. The sync result arrives
    /// via [`SyncWithPeerResult`](DocWorkerMsg::SyncWithPeerResult).
    ///
    /// In runtime2, the doc-worker does not hold a direct subduction reference
    /// (subduction is owned by the hub). The actual sync is driven by sending
    /// a request through the event channel or by a callback passed at
    /// construction. For the blocking-out, we show the waiter management
    /// pattern; the subduction call is a documented `todo!()`.
    ///
    /// Mirrors `handle_sync_with_peer` at `runtime.rs:3199` and
    /// `spawn_doc_sync_for_peer` at `runtime.rs:3099`.
    fn is_quiescent(&self) -> bool {
        self.active_doc_syncs.is_empty()
            && self.pending_sync_jobs.is_empty()
            && self.sync_completed.is_empty()
            && self.sync_materialized.is_empty()
            && self.pending_fragment_requests.is_empty()
    }

    async fn resolve_quiescence_waiters(&mut self) -> eyre::Result<()> {
        if !self.is_quiescent() || self.quiescence_waiters.is_empty() {
            return Ok(());
        }
        let waiters = std::mem::take(&mut self.quiescence_waiters);
        for (barrier_id, _lease) in waiters {
            self.evt_tx
                .send(crate::runtime2::Runtime2Evt::DocWorkerQuiescent {
                    doc_id: self.doc_id,
                    barrier_id,
                })
                .await
                .map_err(|_| ferr!("hub event channel closed"))?;
        }
        Ok(())
    }

    async fn sync_with_peer(
        &mut self,
        peer_id: PeerId,
        waiter_id: u64,
        _timeout: Option<std::time::Duration>,
        done: futures::channel::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
        _lease: DocWorkerInternalLease,
    ) -> eyre::Result<()> {
        // Register the waiter immediately so it is visible to both
        // ApplySyncSession (on OutboundBatch) and SyncWithPeerResult
        // (on error) — whichever fires first resolves it.
        // The lease is stored alongside the waiter and dropped when
        // the waiter is resolved, releasing the internal lease counter.
        self.pending_sync_jobs
            .entry(peer_id)
            .or_default()
            .push((waiter_id, done, _lease));

        // If no sync is active for this peer, start one. The subduction
        // call must go through the hub since the doc-worker doesn't hold
        // a subduction reference. The old runtime called
        // `subduction.sync_with_peer(...)` directly.
        if !self.active_doc_syncs.contains_key(&peer_id) {
            // The watermark is the first waiter ID not present when this
            // round starts. Waiters arriving after this point cascade into a
            // subsequent round, matching the original runtime.
            let watermark = self
                .pending_sync_jobs
                .get(&peer_id)
                .and_then(|waiters| waiters.iter().map(|(id, _, _)| *id).max())
                .map_or(waiter_id, |id| id.saturating_add(1));
            self.active_doc_syncs.insert(peer_id, watermark);

            self.evt_tx
                .send(crate::runtime2::Runtime2Evt::DocSyncRequested {
                    doc_id: self.doc_id,
                    peer_id,
                    waiter_id,
                })
                .await
                .map_err(|_| ferr!("runtime stopped before document sync request"))?;
        }
        Ok(())
    }

    // ═════════════════════════════════════════════════════════════════════
    // HELPER: CancelSyncWithPeer handler
    // ═════════════════════════════════════════════════════════════════════

    /// Cancel pending sync waiters for `peer_id`. If `waiter_id` is `Some`,
    /// only that specific waiter is cancelled; otherwise all for that peer.
    ///
    /// Mirrors the cancellation logic in the old `handle_sync_session_observed`
    /// and `handle_connection_lost` paths at `runtime.rs:1504-1510`.
    fn cancel_sync_with_peer(
        &mut self,
        peer_id: &PeerId,
        waiter_id: Option<u64>,
        reason: &'static str,
    ) {
        if waiter_id.is_none() {
            self.active_doc_syncs.remove(peer_id);
        }
        self.sync_completed.remove(peer_id);
        self.sync_materialized.remove(peer_id);
        if let Some(waiters) = self.pending_sync_jobs.get_mut(peer_id) {
            if let Some(wid) = waiter_id {
                // Cancel a specific waiter.
                if let Some(pos) = waiters.iter().position(|(id, _, _)| *id == wid) {
                    let (_id, sender, _lease) = waiters.remove(pos);
                    let _ = sender.send(Err(crate::runtime::SyncDocError::IoError(eyre::eyre!(
                        "{}", reason
                    ))));
                    // _lease dropped here → releases internal lease counter.
                }
                if waiters.is_empty() {
                    self.pending_sync_jobs.remove(peer_id);
                    self.active_doc_syncs.remove(peer_id);
                }
            } else {
                // Cancel all waiters for this peer.
                if let Some(waiters) = self.pending_sync_jobs.remove(peer_id) {
                    for (_id, sender, _lease) in waiters {
                        let _ = sender.send(Err(crate::runtime::SyncDocError::IoError(
                            eyre::eyre!("{}", reason),
                        )));
                        // _lease dropped here → releases internal lease counter.
                    }
                }
            }
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // HELPER: SyncWithPeerResult handler
    // ═════════════════════════════════════════════════════════════════════

    /// Resolve a sync waiter for `peer_id` with the given `result`. Clears
    /// the active sync entry if the watermark matches.
    ///
    /// Mirrors the result resolution at `runtime.rs:3238`.
    fn mark_sync_completed(&mut self, peer_id: PeerId, _waiter_id: u64) {
        if !self.active_doc_syncs.contains_key(&peer_id) {
            tracing::debug!(%peer_id, "ignoring document sync completion without active sync");
            return;
        }
        self.sync_completed.insert(peer_id);
        self.finish_sync_if_ready(peer_id);
    }

    fn mark_sync_materialized(&mut self, peer_id: PeerId) {
        if !self.active_doc_syncs.contains_key(&peer_id) {
            tracing::debug!(%peer_id, "ignoring document materialization without active sync");
            return;
        }
        self.sync_materialized.insert(peer_id);
        self.finish_sync_if_ready(peer_id);
    }

    fn finish_sync_if_ready(&mut self, peer_id: PeerId) {
        if self.active_doc_syncs.contains_key(&peer_id)
            && self.sync_completed.contains(&peer_id)
            && self.sync_materialized.contains(&peer_id)
        {
            self.sync_completed.remove(&peer_id);
            self.sync_materialized.remove(&peer_id);
            let watermark = self
                .active_doc_syncs
                .get(&peer_id)
                .copied()
                .expect("active document sync disappeared before completion");
            self.resolve_sync_peer_result(peer_id, watermark, Ok(()));
        }
    }

    fn resolve_sync_peer_result(
        &mut self,
        peer_id: PeerId,
        watermark: u64,
        result: Result<(), crate::runtime::SyncDocError>,
    ) {
        self.active_doc_syncs.remove(&peer_id);
        let Some(waiters) = self.pending_sync_jobs.remove(&peer_id) else {
            return;
        };

        // Errors terminate the whole active round. Successful completion only
        // resolves waiters that existed when this round began; later waiters
        // require a fresh round, as in the original runtime.
        if result.is_err() {
            for (_wid, sender, _lease) in waiters {
                let _ = sender.send(match &result {
                    Err(crate::runtime::SyncDocError::NotFound) => {
                        Err(crate::runtime::SyncDocError::NotFound)
                    }
                    Err(crate::runtime::SyncDocError::Unauthorized) => {
                        Err(crate::runtime::SyncDocError::Unauthorized)
                    }
                    Err(crate::runtime::SyncDocError::Policy(error)) => {
                        Err(crate::runtime::SyncDocError::Policy(error.clone()))
                    }
                    Err(crate::runtime::SyncDocError::TransportError) => {
                        Err(crate::runtime::SyncDocError::TransportError)
                    }
                    Err(crate::runtime::SyncDocError::IoError(error)) => Err(
                        crate::runtime::SyncDocError::IoError(eyre::eyre!("{error}")),
                    ),
                    Err(crate::runtime::SyncDocError::Other(error)) => {
                        Err(crate::runtime::SyncDocError::Other(eyre::eyre!("{error}")))
                    }
                    Ok(()) => unreachable!("successful result entered error path"),
                });
            }
            return;
        }

        let mut remaining = Vec::new();
        for (waiter_id, sender, lease) in waiters {
            if waiter_id < watermark {
                sender
                    .send(Ok(()))
                    .expect("document sync waiter receiver must remain open");
            } else {
                remaining.push((waiter_id, sender, lease));
            }
        }
        if remaining.is_empty() {
            return;
        }

        let next_waiter_id = remaining
            .iter()
            .map(|(id, _, _)| *id)
            .min()
            .expect("remaining document waiters must be non-empty");
        let next_watermark = remaining
            .iter()
            .map(|(id, _, _)| *id)
            .max()
            .expect("remaining document waiters must be non-empty")
            .saturating_add(1);
        self.pending_sync_jobs.insert(peer_id, remaining);
        self.active_doc_syncs.insert(peer_id, next_watermark);
        self.evt_tx
            .try_send(crate::runtime2::Runtime2Evt::DocSyncRequested {
                doc_id: self.doc_id,
                peer_id,
                waiter_id: next_waiter_id,
            })
            .expect("runtime event channel must remain open");
    }
}

// ─── Mailbox loop trait (discharges from_send_future for wasm) ──────────

/// Trait that isolates the doc-worker\'s mailbox loop from the
/// `FutureForm::from_send_future` Send requirement.
///
/// The `#[future_form]` macro generates a `Sendable` impl where the async
/// block must be `Send` and a `Local` impl where it need not be.  This is the
/// wasm compatibility lever: `DocIo<Local>` returns non-Send futures, so the
/// `Local` variant of this trait does not require them to be `Send`.
pub(crate) trait DocWorkerLoop<F: FutureForm> {
    fn mailbox_loop(
        worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: futures::future::AbortRegistration,
        runtime_evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> DocWorkerLoop<F> for F {
    fn mailbox_loop(
        mut worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: futures::future::AbortRegistration,
        runtime_evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>> {
        F::from_future(async move {
            let result = futures::future::Abortable::new(
                async {
                    loop {
                        match msg_rx.recv().await {
                            Ok(msg) => {
                                worker.handle_msg(msg).await?;
                                worker.resolve_quiescence_waiters().await?;
                            }
                            Err(async_channel::RecvError) => break,
                        }
                    }
                    eyre::Ok(())
                },
                stop_registration,
            )
            .await;

            if matches!(&result, Ok(Ok(()))) {
                runtime_evt_tx
                    .send(crate::runtime2::Runtime2Evt::DocWorkerStopped { doc_id })
                    .await
                    .map_err(|_| ferr!("hub event channel closed"))?;
            }

            match result {
                Ok(Err(error)) if runtime_evt_tx.is_closed() => {
                    tracing::debug!(%doc_id, ?error, "doc worker stopped after runtime shutdown");
                    Ok(())
                }
                Ok(ok) => ok,
                Err(_) => Ok(()),
            }
        })
    }
}
