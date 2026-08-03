//! `DocWorker2` — the per-doc actor.

use crate::interlude::*;

use crate::changes::BigRepoChangeOrigin;
use crate::runtime2::support::{
    stage_automerge_ingest, BigRepoCiphertextKind, BigRepoCiphertextLocator,
};
use crate::runtime2::types::{DocLookup, LiveDocBundle};
use crate::runtime2::Runtime2Evt;
use crate::runtime2::{
    messages::DocWorkerMsg, DocIo, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken,
    MaterializationBlocker, MaterializationStatus,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use futures::future::AbortRegistration;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::sedimentree::SedimentreeItem;
use tracing::Instrument;
pub struct SpawnedDocWorker<F: FutureForm> {
    pub handle: DocWorkerHandle,
    pub stop: DocWorkerStopToken,
    pub run: F::Future<'static, eyre::Result<()>>,
}

pub fn spawn_doc_worker<F>(
    doc_id: DocumentId,
    io: Arc<dyn DocIo<F>>,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    runtime_cmd_tx: async_channel::Sender<crate::runtime2::Runtime2Cmd>,
    runtime_evt_tx: async_channel::Sender<Runtime2Evt>,
) -> SpawnedDocWorker<F>
where
    F: FutureForm + DocWorkerLoop<F> + 'static,
{
    let (msg_tx, msg_rx) = async_channel::unbounded::<DocWorkerMsg>();
    let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());

    let worker = DocWorker2 {
        doc_id,
        sed_id,
        state: DocState::Unloaded,
        partially_decrypted: false,
        io,
        change_manager,
        runtime_cmd_tx,
        evt_tx: runtime_evt_tx.clone(),
        pending_fragment_requests: std::collections::BTreeSet::new(),
        quiescence_waiters: Vec::new(),
    };

    let (stop_abort, stop_registration) = futures::future::AbortHandle::new_pair();
    let run = F::mailbox_loop(worker, msg_rx, stop_registration, runtime_evt_tx, doc_id);

    SpawnedDocWorker {
        handle: DocWorkerHandle { msg_tx },
        stop: DocWorkerStopToken { abort: stop_abort },
        run,
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
#[expect(private_interfaces)]
pub trait DocWorkerLoop<F: FutureForm> {
    fn mailbox_loop(
        worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: AbortRegistration,
        runtime_evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> DocWorkerLoop<F> for F {
    #[expect(private_interfaces)]
    fn mailbox_loop(
        mut worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: AbortRegistration,
        runtime_evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>> {
        let cancellation = stop_registration.handle();
        F::from_future(
            async move {
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

                if matches!(&result, Ok(Ok(())))
                    && runtime_evt_tx
                        .send(Runtime2Evt::DocWorkerStopped { doc_id })
                        .await
                        .is_err()
                {
                    debug!(%doc_id, "runtime stopped before doc worker stop event");
                }

                match result {
                    Ok(Err(_error)) if cancellation.is_aborted() => Ok(()),
                    Ok(Err(error)) if runtime_evt_tx.is_closed() => {
                        debug!(%doc_id, ?error, "doc worker stopped after runtime shutdown");
                        Ok(())
                    }
                    Ok(Err(error)) => {
                        runtime_evt_tx
                            .send(Runtime2Evt::FatalWorkerError {
                                doc_id: Some(doc_id),
                                context: "document worker failed",
                                error: format!("{error:?}"),
                            })
                            .await
                            .expect(ERROR_CHANNEL);
                        Err(error)
                    }
                    Ok(Ok(())) => Ok(()),
                    Err(_) => Ok(()),
                }
            }
            .instrument(tracing::info_span!("doc_worker mailbox loop", %doc_id)),
        )
    }
}

struct DocWorker2<F: FutureForm> {
    doc_id: DocumentId,
    sed_id: sedimentree_core::id::SedimentreeId,

    state: DocState,
    partially_decrypted: bool,
    change_manager: Arc<crate::changes::ChangeListenerManager>,

    io: Arc<dyn DocIo<F>>,

    runtime_cmd_tx: async_channel::Sender<crate::runtime2::Runtime2Cmd>,
    evt_tx: async_channel::Sender<Runtime2Evt>,

    // ── fragment bookkeeping ───────────────────────────────────────────────
    /// Fragment-boundary commits awaiting a corresponding `store_fragment` call.
    pending_fragment_requests:
        std::collections::BTreeSet<subduction_core::subduction::request::FragmentRequested>,

    /// Mailbox-ordered quiescence fences waiting for active finite work.
    /// Each fence replies directly on its oneshot once the worker is quiescent.
    quiescence_waiters: Vec<(futures::channel::oneshot::Sender<()>, DocWorkerInternalLease)>,
}

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
    Live(std::sync::Weak<LiveDocBundle>),
    /// Sedimentree content exists, but its keys, ciphertext closure, or
    /// Automerge dependency closure is not yet available.
    PendingMaterialization(Vec<MaterializationBlocker>),
}

enum LoadedDocSnapshot {
    Missing,
    Unavailable(Vec<MaterializationBlocker>),
    Ready {
        doc: automerge::Automerge,
        partially_decrypted: bool,
    },
}

impl LoadedDocSnapshot {
    fn from_materialized_doc(
        doc: automerge::Automerge,
        partially_decrypted: bool,
        blockers: Vec<MaterializationBlocker>,
    ) -> Self {
        if doc.get_heads().is_empty() {
            Self::Unavailable(blockers)
        } else {
            Self::Ready {
                doc,
                partially_decrypted,
            }
        }
    }

    fn from_decrypted_plaintexts(
        mut pending_plaintexts: Vec<Vec<u8>>,
        mut partially_decrypted: bool,
        doc_id: DocumentId,
        mut blockers: Vec<MaterializationBlocker>,
    ) -> eyre::Result<Self> {
        let mut doc = automerge::Automerge::new();
        loop {
            let mut deferred = Vec::new();
            let mut round_progress = false;
            for plaintext in pending_plaintexts {
                match doc.load_incremental(&plaintext) {
                    Ok(0) if doc.get_heads().is_empty() => deferred.push(plaintext),
                    Ok(applied) => round_progress |= applied > 0,
                    Err(automerge::AutomergeError::MissingDeps) => deferred.push(plaintext),
                    Err(error) => {
                        return Err(ferr!("automerge load_incremental failed: {error}"));
                    }
                }
            }
            if deferred.is_empty() {
                break;
            }
            if !round_progress {
                partially_decrypted = true;
                blockers.push(MaterializationBlocker::MissingAutomergeDependencies {
                    deferred_blobs: deferred.len(),
                });
                debug!(
                    %doc_id,
                    deferred_count = deferred.len(),
                    "document blobs remain blocked on unavailable Automerge dependencies"
                );
                break;
            }
            pending_plaintexts = deferred;
        }
        Ok(Self::from_materialized_doc(
            doc,
            partially_decrypted,
            blockers,
        ))
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Dispatch a single [`DocWorkerMsg`]. Called by the message loop.
    ///
    /// The `_lease` fields in certain messages keep the worker alive while
    /// the operation is in-flight (the hub's side of the lease is released
    /// when the message completes).
    #[tracing::instrument(skip(self))]
    async fn handle_msg(&mut self, msg: DocWorkerMsg) -> eyre::Result<()> {
        trace!(?msg, "doc worker message received");
        match msg {
            DocWorkerMsg::PutDoc {
                initial_content,
                resp,
            } => self.put_doc(initial_content, resp).await,
            DocWorkerMsg::AcquireHandle { resp } => self.acquire_handle(resp).await,
            DocWorkerMsg::CommitDelta {
                bundle_id,
                commits,
                heads,
                patches,
                origin,
                resp,
                _lease,
            } => {
                self.commit_delta(bundle_id, commits, heads, patches, origin, resp)
                    .await
            }
            DocWorkerMsg::ApplySyncSession {
                peer_id,
                commit_ids,
                fragment_ids,
                reply,
            } => self
                .apply_sync_session(peer_id, commit_ids, fragment_ids, reply)
                .await,
            DocWorkerMsg::ReattemptMaterialization { origin, resp } => {
                debug!(
                    doc_id = %self.doc_id,
                    pending = matches!(self.state, DocState::PendingMaterialization(_)),
                    "retrying document materialization after dependency update"
                );
                match self.retry_materialization(origin).await
                {
                    Ok(status) => {
                        debug!(%self.doc_id, ?status, "document materialization retry completed");
                        resp.send(Ok(status))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                        Ok(())
                    }
                    Err(error) => {
                        resp.send(Err(format!("{error:?}")))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                        Err(error)
                    }
                }
            }
            DocWorkerMsg::QueryHeadState { resp } => {
                resp.send(self.head_state().await)
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
                Ok(())
            }
            // FIXME: we have a duplicate here just to support
            // Option<> Senders
            DocWorkerMsg::InspectHeadState { resp } => {
                resp.send(self.head_state().await.map(Some))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
                Ok(())
            }
            DocWorkerMsg::Fence { reply, _lease } => {
                self.quiescence_waiters.push((reply, _lease));
                Ok(())
            }
        }
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Create a new document with initial content.
    async fn put_doc(
        &mut self,
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<eyre::Result<Arc<LiveDocBundle>>>,
    ) -> eyre::Result<()> {
        // ── 1. Occupancy check ─────────────────────────────────────────────
        if !matches!(self.state, DocState::Unloaded) {
            resp.send(Err(ferr!("doc already occupied: {:?}", self.doc_id)))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            return Ok(());
        }

        let staged = stage_automerge_ingest(&initial_content);
        self.io
            .persist_initial_document(self.sed_id, staged)
            .await?;

        // ── 4. Build LiveDocBundle, transition to Live ─────────────────────
        let heads: Arc<[automerge::ChangeHash]> = Arc::from(initial_content.get_heads());

        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            *initial_content,
            crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
            false,
        ));

        self.state = DocState::Live(Arc::downgrade(&bundle));

        // ── 6. Notify ──────────────────────────────────────────────────────
        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, Arc::clone(&heads))?;

        self.register_bundle_lease().await?;

        resp.send(Ok(bundle))
            .inspect_err(|_| warn!(ERROR_CALLER))
            .ok();
        Ok(())
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Acquire a live handle to the document.
    async fn acquire_handle(
        &mut self,
        resp: futures::channel::oneshot::Sender<eyre::Result<DocLookup<Arc<LiveDocBundle>>>>,
    ) -> eyre::Result<()> {
        let result = match &self.state {
            // - `Live(bundle)` and bundle still alive → upgrade the `Weak`, return
            //   `Ready(upgraded)`. A broken bundle (an earlier commit from it
            //   was rejected) reloads the last persisted state into a fresh
            //   bundle instead.
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    if bundle.is_broken() {
                        self.state = DocState::Unloaded;
                        self.take_or_load_transient_doc().await?
                    } else {
                        DocLookup::Ready(bundle)
                    }
                } else {
                    // Weak reference expired — fall through to re-load below.
                    self.state = DocState::Unloaded;
                    self.take_or_load_transient_doc().await?
                }
            }
            // - `Transient(doc)` → build a new `LiveDocBundle`, transition
            // to `Live`, emit `DocWorkerHandleAcquired`.
            DocState::Transient(_) => {
                // Take ownership and build a bundle.
                let DocState::Transient(doc) =
                    std::mem::replace(&mut self.state, DocState::Unloaded)
                else {
                    unreachable!();
                };
                let bundle = Arc::new(LiveDocBundle::new(
                    self.doc_id,
                    *doc,
                    crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
                    self.partially_decrypted,
                ));
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.register_bundle_lease().await?;
                DocLookup::Ready(bundle)
            }
            // - `Unloaded` / `PendingMaterialization` → attempt to load + decrypt
            //   the sedimentree via `load_doc_snapshot` (hydrate + decrypt walk).
            //   - Fully decryptable → `Ready` + `mark_materialization_ready`.
            //   - Partially decryptable → `PendingMaterialization` +
            //     `mark_materialization_pending`.
            DocState::Unloaded | DocState::PendingMaterialization(_) => {
                self.take_or_load_transient_doc().await?
            }
        };
        resp.send(Ok(result))
            .inspect_err(|_| warn!(ERROR_CALLER))
            .ok();
        Ok(())
    }

    async fn register_bundle_lease(&self) -> eyre::Result<()> {
        let (registered_tx, registered_rx) = futures::channel::oneshot::channel();
        self.runtime_cmd_tx
            .send(crate::runtime2::Runtime2Cmd::RegisterDocLease {
                doc_id: self.doc_id,
                registered: registered_tx,
            })
            .await
            .expect(ERROR_CHANNEL);
        registered_rx.await.expect(ERROR_CHANNEL);
        Ok(())
    }
    /// Load the materializable document state and retain whether any stored
    /// Sedimentree item could not be decrypted.
    ///
    /// This runs on initial handle acquisition and on every
    /// `ReattemptMaterialization` message while the worker is pending. The
    /// tree itself is cached by `DocIo::hydrate_tree`, but this method still
    /// walks and causally decrypts the full resident history; retrying that
    /// walk is the remaining materialization hot path.
    #[tracing::instrument(skip_all)]
    async fn load_doc_snapshot(&self) -> eyre::Result<LoadedDocSnapshot> {
        let Some(mut tree) = self.io.hydrate_tree(self.sed_id).await? else {
            return Ok(LoadedDocSnapshot::Missing);
        };
        tree.ensure_minimized(&sedimentree_core::depth::CountLeadingZeroBytes);

        let order = tree
            .topsorted_blob_order()
            .map_err(|error| ferr!("failed ordering document blobs: {error}"))?;
        let fragments: Vec<_> = tree.fragments().collect();
        let commits: Vec<_> = tree.loose_commits().collect();
        let mut plaintexts = HashMap::<Vec<u8>, Vec<u8>>::new();
        let mut blockers = Vec::new();

        for item in &order {
            let (kind, head) = match item {
                SedimentreeItem::Fragment(index) => {
                    (BigRepoCiphertextKind::Fragment, fragments[*index].head())
                }
                SedimentreeItem::LooseCommit(index) => {
                    (BigRepoCiphertextKind::LooseCommit, commits[*index].head())
                }
            };
            let locator = BigRepoCiphertextLocator::new(kind, self.sed_id, head);
            let result = self.io.try_causal_decrypt(self.sed_id, locator).await?;
            plaintexts.extend(result.complete);
            blockers.extend(result.blockers);
        }

        let mut partially_decrypted = false;
        let mut pending_plaintexts = Vec::new();
        for item in &order {
            let content_ref = match item {
                SedimentreeItem::Fragment(index) => fragments[*index].head().as_bytes().to_vec(),
                SedimentreeItem::LooseCommit(index) => commits[*index].head().as_bytes().to_vec(),
            };
            match plaintexts.remove(&content_ref) {
                Some(plaintext) => pending_plaintexts.push(plaintext),
                None => partially_decrypted = true,
            }
        }

        // Minimized fragments can hide loose ancestors that causal decryption
        // still returns. Include those dependencies in the same fixed-point
        // application pass. A topological sedimentree order does not guarantee
        // that every decrypted Automerge dependency precedes its child.
        pending_plaintexts.extend(plaintexts.into_values());
        LoadedDocSnapshot::from_decrypted_plaintexts(
            pending_plaintexts,
            partially_decrypted,
            self.doc_id,
            blockers,
        )
    }

    async fn take_or_load_transient_doc(&mut self) -> eyre::Result<DocLookup<Arc<LiveDocBundle>>> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization(_));
        let out = match std::mem::replace(&mut self.state, DocState::Unloaded) {
            DocState::Live(_) => unreachable!("document already live"),
            DocState::Transient(doc) => {
                let bundle = Arc::new(LiveDocBundle::new(
                    self.doc_id,
                    *doc,
                    crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
                    self.partially_decrypted,
                ));
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.register_bundle_lease().await?;
                DocLookup::Ready(bundle)
            }
            DocState::Unloaded | DocState::PendingMaterialization(_) => {
                match self.load_doc_snapshot().await? {
                    LoadedDocSnapshot::Ready {
                        doc,
                        partially_decrypted,
                    } => {
                        let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                        self.transition_to_ready(was_pending, Arc::clone(&heads))
                            .await?;
                        self.set_partially_decrypted(partially_decrypted).await?;
                        let bundle = Arc::new(LiveDocBundle::new(
                            self.doc_id,
                            doc,
                            crate::runtime2::DocLease::new(
                                self.runtime_cmd_tx.clone(),
                                self.doc_id,
                            ),
                            partially_decrypted,
                        ));
                        self.state = DocState::Live(Arc::downgrade(&bundle));
                        self.register_bundle_lease().await?;
                        DocLookup::Ready(bundle)
                    }
                    LoadedDocSnapshot::Unavailable(blockers) => {
                        self.transition_to_pending(was_pending, blockers).await?;
                        DocLookup::PendingMaterialization
                    }
                    LoadedDocSnapshot::Missing => DocLookup::Missing,
                }
            }
        };
        Ok(out)
    }

    async fn set_partially_decrypted(&mut self, partial: bool) -> eyre::Result<()> {
        if self.partially_decrypted == partial {
            return Ok(());
        }
        self.partially_decrypted = partial;
        if let DocState::Live(bundle) = &self.state {
            if let Some(bundle) = bundle.upgrade() {
                bundle.set_partially_decrypted(partial);
            }
        }
        let event = if partial {
            Runtime2Evt::DocWorkerMaterializationPending {
                doc_id: self.doc_id,
            }
        } else {
            Runtime2Evt::DocWorkerMaterializationReady {
                doc_id: self.doc_id,
            }
        };
        self.evt_tx.send(event).await.wrap_err(ERROR_CHANNEL)?;
        Ok(())
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Commit a set of changes locally.
    ///
    /// The authoritative write gate: a commit is rejected (and its bundle
    /// latched broken) when
    /// 1. it comes from a stale or broken bundle (an earlier commit from this
    ///    bundle was rejected, or the bundle was replaced by a reload), or
    /// 2. the local principal no longer holds write access (revoked or
    ///    Read-only), or
    /// 3. the encrypted commit cannot be persisted (key unavailable).
    /// A rejected commit never persists, so no partial history can form.
    async fn commit_delta(
        &mut self,
        bundle_id: u64,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> eyre::Result<()> {
        // ── 1. Handle validity gate ───────────────────────────────────────
        // Reject commits from broken bundles (an earlier commit from this
        // bundle was rejected) and from bundles the worker no longer serves
        // (reloaded after a break, or a freshly spawned worker with no
        // bundle). This makes concurrent commits from the same handle fail
        // atomically with the first rejection, instead of persisting a chain
        // whose parent — the rejected commit's ghost — was never stored.
        let current = match &self.state {
            DocState::Live(bundle) => bundle.upgrade(),
            _ => None,
        };
        let served_bundle_id = current.as_ref().map(|bundle| bundle.id());
        if served_bundle_id != Some(bundle_id)
            || current.as_ref().is_some_and(|bundle| bundle.is_broken())
        {
            let message = if current.as_ref().is_some_and(|b| b.is_broken()) {
                "document write rejected: handle invalidated by an earlier rejected commit; re-acquire the document"
            } else {
                "document write rejected: commit from a stale handle; re-acquire the document"
            };
            resp.send(Err(ferr!("{message}")))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            return Ok(());
        }
        // ── 2. Write-access gate ──────────────────────────────────────────
        match self.io.has_doc_write_access(self.doc_id).await {
            Ok(true) => {}
            Ok(false) => {
                current
                    .as_ref()
                    .expect("live bundle present for a valid commit")
                    .mark_broken();
                resp.send(Err(ferr!(
                    "document write rejected: local access is not writable (revoked or read-only)"
                )))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
                return Ok(());
            }
            Err(error) => {
                resp.send(Err(error))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
                return Ok(());
            }
        }
        let pending_fragment_requests =
            match self.io.persist_local_commits(self.sed_id, commits).await {
                Ok(requests) => requests,
                Err(error)
                    if error
                        .downcast_ref::<crate::runtime2::io::DocumentKeyUnavailable>()
                        .is_some() =>
                {
                    // The Automerge operation already ran against the live
                    // handle, but its encrypted commit cannot be accepted
                    // (key unavailable). Report the failure and invalidate the
                    // handle: the in-memory mutation cannot persist and would
                    // poison later commits, so the caller must re-acquire to
                    // reload the last persisted state.
                    current
                        .as_ref()
                        .expect("live bundle present for a valid commit")
                        .mark_broken();
                    resp.send(Err(error))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
        self.pending_fragment_requests
            .extend(pending_fragment_requests);

        // ── 3. Notify heads changed ────────────────────────────────────────

        let heads = Arc::from(heads);
        self.change_manager.notify_doc_heads_changed(
            self.doc_id,
            Arc::clone(&heads),
            origin.clone(),
        )?;

        // Fire patches even if heads didn't change (delta can have content
        // changes within the same head set — e.g. tombstone compaction).
        for patch in &patches {
            self.change_manager.notify_doc_changed(
                self.doc_id,
                Arc::new(patch.clone()),
                Arc::clone(&heads),
                origin.clone(),
            )?;
        }

        // ── 4. Process pending fragment requests ───────────────────────────
        self.process_pending_fragment_requests().await?;

        resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        Ok(())
    }

    /// Process fragment-boundary commits stored during the write.
    /// Builds and stores fragments for each pending request.
    async fn process_pending_fragment_requests(&mut self) -> eyre::Result<()> {
        if self.pending_fragment_requests.is_empty() {
            return Ok(());
        }
        let DocState::Live(bundle) = &self.state else {
            return Ok(());
        };
        let bundle = bundle
            .upgrade()
            .ok_or_else(|| ferr!("live document expired while storing fragment"))?;
        let requests = std::mem::take(&mut self.pending_fragment_requests);
        for request in requests {
            let (boundary, checkpoints, raw_blob) = {
                let doc = bundle.doc.lock().await;
                let head = automerge::ChangeHash(*request.head().as_bytes());
                let fragment = doc
                    .get_fragment(head)
                    .ok_or_else(|| ferr!("requested Automerge fragment is unavailable"))?;
                let boundary = fragment
                    .boundary
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect();
                let checkpoints = fragment
                    .checkpoints
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect();
                let raw_blob = doc
                    .bundle(fragment.members.iter().cloned())
                    .wrap_err("unable to resolve bundle for fragment")?
                    .bytes()
                    .to_vec();
                (boundary, checkpoints, raw_blob)
            };
            self.io
                .store_fragment(self.sed_id, request.head(), boundary, checkpoints, raw_blob)
                .await?;
        }
        Ok(())
    }
}

impl<F: FutureForm> DocWorker2<F> {
    #[tracing::instrument(skip_all, fields(was_pending))]
    async fn transition_to_pending(
        &mut self,
        was_pending: bool,
        blockers: Vec<MaterializationBlocker>,
    ) -> eyre::Result<()> {
        debug!(?blockers, "document materialization pending");
        self.state = DocState::PendingMaterialization(blockers);
        if !was_pending {
            self.change_manager
                .notify_local_doc_materialization_pending(self.doc_id)?;
        }
        self.set_partially_decrypted(true).await
    }

    #[tracing::instrument(
        skip_all,
        fields(was_pending, head_count = heads.len())
    )]
    async fn transition_to_ready(
        &mut self,
        was_pending: bool,
        heads: Arc<[automerge::ChangeHash]>,
    ) -> eyre::Result<()> {
        if was_pending {
            self.change_manager
                .notify_local_doc_materialization_ready(self.doc_id, heads)?;
        }
        Ok(())
    }
}

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
    async fn head_state(&self) -> eyre::Result<crate::runtime2::DocHeadState> {
        let mut sedimentree_heads: Vec<automerge::ChangeHash> = self
            .io
            .sedimentree_heads(self.sed_id)
            .await?
            .iter()
            .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
            .collect();
        sedimentree_heads.sort_unstable();
        let sedimentree_heads: Arc<[automerge::ChangeHash]> = sedimentree_heads.into();
        let (materialized_heads, state) = match &self.state {
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    let doc = bundle.doc.lock().await;
                    let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                    (
                        Some(heads),
                        if self.partially_decrypted {
                            crate::runtime2::MaterializationState::PartiallyMaterialized
                        } else {
                            crate::runtime2::MaterializationState::Materialized
                        },
                    )
                } else {
                    // Weak reference expired — treat as Unloaded.
                    (None, crate::runtime2::MaterializationState::Missing)
                }
            }
            DocState::Transient(doc) => {
                let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                (
                    Some(heads),
                    if self.partially_decrypted {
                        crate::runtime2::MaterializationState::PartiallyMaterialized
                    } else {
                        crate::runtime2::MaterializationState::Materialized
                    },
                )
            }
            DocState::PendingMaterialization(_) => {
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

        if sedimentree_heads.is_empty()
            && materialized_heads
                .as_ref()
                .is_some_and(|heads| !heads.is_empty())
        {
            error!(
                sed_id = ?self.sed_id,
                materialized_heads = materialized_heads.as_ref().map_or(0, |heads| heads.len()),
                ?state,
                "materialized document has heads while its Sedimentree frontier is empty"
            );
            unreachable!("materialized document has heads while its Sedimentree frontier is empty")
        }

        Ok(crate::runtime2::DocHeadState {
            sedimentree_heads,
            materialized_heads,
            state,
        })
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Apply content that Subduction has already persisted to the resident
    /// live Automerge document, then report the sync outcome.
    ///
    /// `commit_ids`/`fragment_ids` may be empty (an empty session): nothing
    /// is applied, but a pending doc is reconsidered and its current state
    /// reported. `reply: Some` resolves the caller's sync receipt; `None` is
    /// passive (fire-and-forget) routing.
    #[tracing::instrument(
        skip_all,
        fields(
            remote_peer_id = %peer_id,
            received_commits = commit_ids.len(),
            received_fragments = fragment_ids.len(),
        )
    )]
    async fn apply_sync_session(
        &mut self,
        peer_id: PeerId,
        commit_ids: Vec<CommitId>,
        fragment_ids: Vec<CommitId>,
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
    ) -> eyre::Result<()> {
        let received = !commit_ids.is_empty() || !fragment_ids.is_empty();
        let has_live = matches!(
            &self.state,
            DocState::Live(bundle) if bundle.strong_count() > 0
        );

        if received {
            // Incremental apply of the received content into the live
            // document. Content for documents without live handles never
            // reaches the live path; it is persisted by Subduction and
            // hydrated by the next acquisition (or by the walk below when
            // the doc is pending).
            if let Some(bundle) = match &self.state {
                DocState::Live(bundle) => bundle.upgrade(),
                _ => None,
            } {
                let received_refs: HashSet<Vec<u8>> = commit_ids
                    .iter()
                    .chain(&fragment_ids)
                    .map(|id| id.as_bytes().to_vec())
                    .collect();
                let Some(mut tree) = self.io.hydrate_tree(self.sed_id).await? else {
                    error!(
                        doc_id = %self.doc_id,
                        "received sync content has no persisted Sedimentree"
                    );
                    return Err(ferr!(
                        "received sync session has no persisted Sedimentree content"
                    ));
                };
                tree.ensure_minimized(&sedimentree_core::depth::CountLeadingZeroBytes);
                let (blobs, partially_decrypted) = self
                    .try_decrypt_received_blobs(&mut tree, &received_refs)
                    .await?;
                self.set_partially_decrypted(partially_decrypted).await?;

                if blobs.is_empty() {
                    self.notif_pending_heads(&mut tree, peer_id).await?;
                    return self
                        .report_sync_outcome(peer_id, has_live, true, reply)
                        .await;
                }
                if partially_decrypted {
                    self.notif_pending_heads(&mut tree, peer_id).await?;
                }

                let mut missing_deps = false;
                let mut changed;
                let (after_heads, patches) = {
                    let mut doc = bundle.doc.lock().await;
                    let before = doc.get_heads();
                    for blob in blobs {
                        match doc.load_incremental(&blob) {
                            Ok(_) => {}
                            Err(automerge::AutomergeError::MissingDeps) => {
                                missing_deps = true;
                                break;
                            }
                            Err(error) => {
                                return Err(ferr!("failed applying sync blob: {error}"));
                            }
                        }
                    }
                    let after = doc.get_heads();
                    changed = before != after;
                    if !changed {
                        (after, Vec::new())
                    } else {
                        let patches = if self
                            .change_manager
                            .has_change_listener_interest(
                                self.doc_id,
                                &BigRepoChangeOrigin::Remote { peer_id },
                            )
                        {
                            doc.diff(&before, &after)
                        } else {
                            Vec::new()
                        };
                        (after, patches)
                    }
                };
                if missing_deps {
                    self.set_partially_decrypted(true).await?;
                    self.notif_pending_heads(&mut tree, peer_id).await?;
                } else if changed {
                    // Notify only when heads actually advanced.
                    let heads = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    self.change_manager.notify_doc_heads_changed(
                        self.doc_id,
                        Arc::clone(&heads),
                        BigRepoChangeOrigin::Remote { peer_id },
                    )?;
                    for patch in patches {
                        self.change_manager.notify_doc_changed(
                            self.doc_id,
                            Arc::new(patch),
                            Arc::clone(&heads),
                            BigRepoChangeOrigin::Remote { peer_id },
                        )?;
                    }
                }
            }
        }

        self.report_sync_outcome(peer_id, has_live, received, reply).await
    }

    /// Determine and report the sync outcome after applying a session.
    ///
    /// The full walk (decrypt of the entire persisted tree) runs when the
    /// doc is pending, is partially decrypted, or received content — it is
    /// the honest evaluator: a session that decrypts cleanly must still
    /// reconsider previously-stored undecryptable content (A7), and the
    /// receipt must report `Pending` for a doc that is still blocked.
    async fn report_sync_outcome(
        &mut self,
        peer_id: PeerId,
        has_live: bool,
        received: bool,
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
    ) -> eyre::Result<()> {
        let pending = matches!(
            self.state,
            DocState::PendingMaterialization(_)
        );
        // A live doc that received content is walked unconditionally: the
        // session-scoped decrypt result can be stale relative to the full
        // tree (a clean session must still reconsider previously-stored
        // undecryptable content — A7).
        let walk = pending || self.partially_decrypted || (received && has_live);
        let result = if walk {
            match self
                .retry_materialization(BigRepoChangeOrigin::Remote { peer_id })
                .await
            {
                Ok(MaterializationStatus::Ready { .. }) => Ok(
                    crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Ready,
                    },
                ),
                Ok(MaterializationStatus::Pending(blockers)) => Ok(
                    crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Pending(blockers),
                    },
                ),
                Ok(MaterializationStatus::Missing) => Err(
                    crate::runtime2::types::SyncDocError::NotFound,
                ),
                Err(error) => {
                    let report = Err(crate::runtime2::types::SyncDocError::Other(ferr!(
                        "{error:?}"
                    )));
                    return self.finish_sync_outcome(reply, report, Err(error));
                }
            }
        } else if has_live {
            Ok(crate::runtime2::types::SyncDocReceipt {
                outcome: crate::runtime2::types::SyncDocOutcome::Ready,
            })
        } else {
            // No live bundle and not pending: state is persisted; the next
            // acquisition hydrates it.
            if matches!(self.state, DocState::Transient(_)) {
                debug!(
                    doc_id = %self.doc_id,
                    "invalidating inactive document after sync"
                );
                self.state = DocState::Unloaded;
                self.set_partially_decrypted(false).await?;
            }
            Ok(crate::runtime2::types::SyncDocReceipt {
                outcome: crate::runtime2::types::SyncDocOutcome::Stored,
            })
        };
        self.finish_sync_outcome(reply, result, Ok(()))
    }

    /// Send the receipt outcome to the caller and return the worker-level
    /// result (an error both reports `Other` on the receipt and fails the
    /// worker, preserving the pre-B4 fatal-walk behavior).
    fn finish_sync_outcome(
        &mut self,
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
        result: Result<
            crate::runtime2::types::SyncDocReceipt,
            crate::runtime2::types::SyncDocError,
        >,
        worker: eyre::Result<()>,
    ) -> eyre::Result<()> {
        if let Some(reply) = reply {
            reply
                .send(result)
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        }
        worker
    }

    /// Decrypt only the content received by this Subduction exchange.
    async fn try_decrypt_received_blobs(
        &self,
        tree: &mut sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        received_refs: &HashSet<Vec<u8>>,
    ) -> eyre::Result<(Vec<Vec<u8>>, bool)> {
        let fragments: Vec<_> = tree.fragments().collect();
        let commits: Vec<_> = tree.loose_commits().collect();
        let order = tree
            .topsorted_blob_order()
            .map_err(|error| ferr!("failed ordering sync session blobs: {error}"))?;

        // Filter the order to only received items.
        let received_order: Vec<(&SedimentreeItem, Vec<u8>)> = order
            .iter()
            .filter_map(|item| {
                let content_ref = match item {
                    SedimentreeItem::Fragment(idx) => {
                        fragments.get(*idx).map(|f| f.head().as_bytes().to_vec())
                    }
                    SedimentreeItem::LooseCommit(idx) => {
                        commits.get(*idx).map(|c| c.head().as_bytes().to_vec())
                    }
                };
                content_ref.and_then(|cr| received_refs.contains(&cr).then_some((item, cr)))
            })
            .collect();

        let expected = received_refs.len();
        if received_order.len() != expected {
            eyre::bail!(
                "sync session received blobs are missing from sedimentree order: expected={expected} found={}",
                received_order.len()
            );
        }

        let mut plaintext_by_ref: HashMap<Vec<u8>, Vec<u8>> = default();
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
                    SedimentreeItem::Fragment(idx) => {
                        let f = fragments
                            .get(*idx)
                            .ok_or_else(|| ferr!("missing fragment at index {idx}"))?;
                        BigRepoCiphertextLocator::new(
                            BigRepoCiphertextKind::Fragment,
                            self.sed_id,
                            f.head(),
                        )
                    }
                    SedimentreeItem::LooseCommit(idx) => {
                        let c = commits
                            .get(*idx)
                            .ok_or_else(|| ferr!("missing loose commit at index {idx}"))?;
                        BigRepoCiphertextLocator::new(
                            BigRepoCiphertextKind::LooseCommit,
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
        let unresolved_count = plaintext_by_index
            .iter()
            .filter(|value| value.is_none())
            .count();
        if unresolved_count != 0 {
            debug!(
                doc_id = %self.doc_id,
                sedimentree_id = %self.sed_id,
                received_count = received_refs.len(),
                resolved_count = plaintext_by_ref.len(),
                unresolved_count,
                "received content remains undecryptable during materialization"
            );
        }
        if unresolved_count != 0 {
            materialization_pending = true;
        }
        let blobs: Vec<Vec<u8>> = plaintext_by_index.into_iter().flatten().collect();
        if materialization_pending {
            return Ok((blobs, true));
        }
        Ok((blobs, false))
    }

    async fn notif_pending_heads(
        &mut self,
        tree: &mut sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        peer_id: PeerId,
    ) -> eyre::Result<()> {
        let commit_ids = tree.heads(&sedimentree_core::depth::CountLeadingZeroBytes);
        let heads = commit_ids
            .iter()
            .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
            .collect();

        self.change_manager.notify_doc_pending_heads_changed(
            self.doc_id,
            heads,
            BigRepoChangeOrigin::Remote { peer_id },
        )?;
        Ok(())
    }

    /// Retry materialization (e.g. after keyhive sync delivers new keys).
    ///
    /// Hydrates the tree, attempts decrypt walk via [`load_doc_snapshot`].
    /// - If now fully decryptable → `transition_to_ready` + bootstrap notify
    ///   (no big_sync write).
    /// - If still undecryptable → `transition_to_pending` (deduped).
    /// - Returns `true` if still pending, `false` otherwise.
    #[tracing::instrument(skip_all)]
    async fn retry_materialization(
        &mut self,
        origin: BigRepoChangeOrigin,
    ) -> eyre::Result<MaterializationStatus> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization(_));
        let live_bundle = match &self.state {
            DocState::Live(weak) => weak.upgrade(),
            _ => None,
        };
        match self.load_doc_snapshot().await? {
            LoadedDocSnapshot::Ready {
                mut doc,
                partially_decrypted,
            } => {
                if let Some(bundle) = live_bundle {
                    let (before, after_heads, patches) = {
                        let mut live = bundle.doc.lock().await;
                        let before = live.get_heads();
                        live.merge(&mut doc)?;
                        let after_heads = live.get_heads();
                        let patches = live.diff(&before, &after_heads);
                        (before, after_heads, patches)
                    };
                    let heads = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    debug!(
                        before_heads = before.len(),
                        after_heads = heads.len(),
                        changed = before.as_slice() != heads.as_ref(),
                        partially_decrypted,
                        "merged persisted snapshot into active document",
                    );
                    if before.as_slice() != heads.as_ref() {
                        self.change_manager.notify_doc_heads_changed(
                            self.doc_id,
                            Arc::clone(&heads),
                            origin.clone(),
                        )?;
                        for patch in patches {
                            self.change_manager.notify_doc_changed(
                                self.doc_id,
                                Arc::new(patch),
                                Arc::clone(&heads),
                                origin.clone(),
                            )?;
                        }
                    }
                    self.state = DocState::Live(Arc::downgrade(&bundle));
                    self.set_partially_decrypted(partially_decrypted).await?;
                    return Ok(MaterializationStatus::Ready {
                        partially_decrypted,
                    });
                }

                let after_heads = doc.get_heads();
                self.transition_to_ready(was_pending, Arc::from(after_heads.clone()))
                    .await?;
                if was_pending {
                    let patches = doc.diff(&[], &after_heads);
                    let heads = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    self.change_manager.notify_doc_heads_changed(
                        self.doc_id,
                        Arc::clone(&heads),
                        origin.clone(),
                    )?;
                    for patch in patches {
                        self.change_manager.notify_doc_changed(
                            self.doc_id,
                            Arc::new(patch),
                            Arc::clone(&heads),
                            origin.clone(),
                        )?;
                    }
                }
                self.state = DocState::Transient(Box::new(doc));
                self.set_partially_decrypted(partially_decrypted).await?;
                Ok(MaterializationStatus::Ready {
                    partially_decrypted,
                })
            }
            LoadedDocSnapshot::Unavailable(blockers) => {
                let status = MaterializationStatus::Pending(blockers.clone());
                if live_bundle.is_none() {
                    self.transition_to_pending(was_pending, blockers).await?;
                } else {
                    self.set_partially_decrypted(true).await?;
                }
                Ok(status)
            }
            LoadedDocSnapshot::Missing => {
                self.set_partially_decrypted(false).await?;
                Ok(MaterializationStatus::Missing)
            }
        }
    }

    fn is_quiescent(&self) -> bool {
        self.pending_fragment_requests.is_empty()
    }

    async fn resolve_quiescence_waiters(&mut self) -> eyre::Result<()> {
        if !self.is_quiescent() || self.quiescence_waiters.is_empty() {
            return Ok(());
        }
        let waiters = std::mem::take(&mut self.quiescence_waiters);
        for (reply, _lease) in waiters {
            // A dropped reply receiver means the hub-side fence awaiter was
            // aborted (shutdown) — benign.
            reply.send(()).inspect_err(|_| debug!(%self.doc_id, "worker fence reply dropped")).ok();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_applied_ops_with_heads_is_a_materialized_snapshot() {
        let mut source = automerge::AutoCommit::new();
        source.empty_change(automerge::transaction::CommitOptions::default());
        let expected_heads = source.get_heads();
        let bytes = source.save();

        let mut loaded = automerge::Automerge::new();
        assert_eq!(loaded.load_incremental(&bytes).unwrap(), 0);
        assert_eq!(loaded.get_heads(), expected_heads);

        assert!(matches!(
            LoadedDocSnapshot::from_materialized_doc(loaded, false, Vec::new()),
            LoadedDocSnapshot::Ready {
                partially_decrypted: false,
                ..
            }
        ));
    }

    #[test]
    fn decrypted_child_is_retried_after_its_parent() {
        use automerge::transaction::Transactable;

        let mut source = automerge::AutoCommit::new();
        source.put(automerge::ROOT, "parent", 1).unwrap();
        let parent_heads = source.get_heads();
        let parent = source.save();
        source.put(automerge::ROOT, "child", 2).unwrap();
        let expected_heads = source.get_heads();
        let child = source.save_after(&parent_heads);

        let snapshot = LoadedDocSnapshot::from_decrypted_plaintexts(
            vec![child, parent],
            false,
            DocumentId::new([1; 32]),
            Vec::new(),
        )
        .unwrap();
        let LoadedDocSnapshot::Ready { doc, .. } = snapshot else {
            panic!("parent and child plaintexts should materialize");
        };
        assert_eq!(doc.get_heads(), expected_heads);
    }
}
