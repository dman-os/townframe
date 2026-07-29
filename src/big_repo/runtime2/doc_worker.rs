//! `DocWorker2` — the per-doc actor.

use crate::interlude::*;

use crate::runtime2::support::stage_automerge_ingest;
use crate::runtime2::Runtime2Evt;
use crate::runtime2::{
    messages::DocWorkerMsg, DocIo, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken,
};
use crate::DocumentId;
use big_sync_core::PeerId;
use future_form::{FutureForm, Local, Sendable};
use sedimentree_core::fragment::Fragment;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::sedimentree::SedimentreeItem;

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
pub(crate) trait DocWorkerLoop<F: FutureForm> {
    fn mailbox_loop(
        worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: futures::future::AbortRegistration,
        runtime_evt_tx: async_channel::Sender<Runtime2Evt>,
        doc_id: DocumentId,
    ) -> F::Future<'static, eyre::Result<()>>;
}

#[future_form::future_form(Sendable, Local)]
impl<F: FutureForm> DocWorkerLoop<F> for F {
    fn mailbox_loop(
        mut worker: DocWorker2<F>,
        msg_rx: async_channel::Receiver<DocWorkerMsg>,
        stop_registration: futures::future::AbortRegistration,
        runtime_evt_tx: async_channel::Sender<Runtime2Evt>,
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

            if matches!(&result, Ok(Ok(())))
                && runtime_evt_tx
                    .send(Runtime2Evt::DocWorkerStopped { doc_id })
                    .await
                    .is_err()
            {
                tracing::debug!(%doc_id, "runtime stopped before doc worker stop event");
            }

            match result {
                Ok(Err(error)) if runtime_evt_tx.is_closed() => {
                    tracing::debug!(%doc_id, ?error, "doc worker stopped after runtime shutdown");
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
        })
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

    /// Mailbox-ordered quiescence barriers waiting for active finite work.
    quiescence_waiters: Vec<(u64, DocWorkerInternalLease)>,
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
    Live(std::sync::Weak<crate::runtime2::types::LiveDocBundle>),
    /// Sedimentree content exists but is not yet decryptable (keyhive keys
    /// not yet available — pending sync).
    PendingMaterialization,
}

enum LoadedDocSnapshot {
    Missing,
    Unavailable,
    Ready {
        doc: automerge::Automerge,
        partially_decrypted: bool,
    },
}

impl<F: FutureForm> DocWorker2<F> {
    /// Dispatch a single [`DocWorkerMsg`]. Called by the message loop.
    ///
    /// The `_lease` fields in certain messages keep the worker alive while
    /// the operation is in-flight (the hub's side of the lease is released
    /// when the message completes).
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
            DocWorkerMsg::ApplyReceivedContent {
                peer_id,
                commit_ids,
                fragment_ids,
            } => {
                self.apply_received_content(peer_id, commit_ids, fragment_ids)
                    .await
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

impl<F: FutureForm> DocWorker2<F> {
    /// Create a new document with initial content.
    async fn put_doc(
        &mut self,
        initial_content: Box<automerge::Automerge>,
        resp: futures::channel::oneshot::Sender<
            eyre::Result<Arc<crate::runtime2::types::LiveDocBundle>>,
        >,
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

        let bundle = Arc::new(crate::runtime2::types::LiveDocBundle::new_runtime2(
            self.doc_id,
            *initial_content,
            crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
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
        resp: futures::channel::oneshot::Sender<
            eyre::Result<
                crate::runtime2::types::DocLookup<Arc<crate::runtime2::types::LiveDocBundle>>,
            >,
        >,
    ) -> eyre::Result<()> {
        let result = match &self.state {
            // - `Live(bundle)` and bundle still alive → upgrade the `Weak`, return
            //   `Ready(upgraded)`.
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    crate::runtime2::types::DocLookup::Ready(bundle)
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
                let doc = match std::mem::replace(&mut self.state, DocState::Unloaded) {
                    DocState::Transient(doc) => doc,
                    _ => unreachable!(),
                };
                let bundle = Arc::new(crate::runtime2::types::LiveDocBundle::new_runtime2(
                    self.doc_id,
                    *doc,
                    crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
                ));
                bundle.set_partially_decrypted(self.partially_decrypted);
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.register_bundle_lease().await?;
                crate::runtime2::types::DocLookup::Ready(bundle)
            }
            // - `Unloaded` / `PendingMaterialization` → attempt to load + decrypt
            //   the sedimentree via `load_doc_snapshot` (hydrate + decrypt walk).
            //   - Fully decryptable → `Ready` + `mark_materialization_ready`.
            //   - Partially decryptable → `PendingMaterialization` +
            //     `mark_materialization_pending`.
            DocState::Unloaded | DocState::PendingMaterialization => {
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

        for item in &order {
            let (kind, head) = match item {
                SedimentreeItem::Fragment(index) => (
                    crate::runtime2::support::BigRepoCiphertextKind::Fragment,
                    fragments[*index].head(),
                ),
                SedimentreeItem::LooseCommit(index) => (
                    crate::runtime2::support::BigRepoCiphertextKind::LooseCommit,
                    commits[*index].head(),
                ),
            };
            let locator =
                crate::runtime2::support::BigRepoCiphertextLocator::new(kind, self.sed_id, head);
            plaintexts.extend(
                self.io
                    .try_causal_decrypt(self.sed_id, locator)
                    .await?
                    .complete,
            );
        }

        let mut doc = automerge::Automerge::new();
        let mut made_progress = false;
        let mut partially_decrypted = false;
        for item in &order {
            let content_ref = match item {
                SedimentreeItem::Fragment(index) => fragments[*index].head().as_bytes().to_vec(),
                SedimentreeItem::LooseCommit(index) => commits[*index].head().as_bytes().to_vec(),
            };
            let Some(plaintext) = plaintexts.remove(&content_ref) else {
                partially_decrypted = true;
                continue;
            };
            match doc.load_incremental(&plaintext) {
                Ok(applied) => made_progress |= applied > 0,
                Err(automerge::AutomergeError::MissingDeps) => {
                    partially_decrypted = true;
                    tracing::debug!(
                        doc_id = %self.doc_id,
                        "document blob is waiting for unavailable Automerge dependencies"
                    );
                }
                Err(error) => return Err(ferr!("automerge load_incremental failed: {error}")),
            }
        }

        // Minimized fragments can hide loose ancestors that causal decryption
        // still returns. Automerge still needs those plaintext dependencies.
        for (_, plaintext) in plaintexts {
            match doc.load_incremental(&plaintext) {
                Ok(applied) => made_progress |= applied > 0,
                Err(automerge::AutomergeError::MissingDeps) => {
                    partially_decrypted = true;
                    tracing::debug!(
                        doc_id = %self.doc_id,
                        "causal ancestor blob is waiting for unavailable Automerge dependencies"
                    );
                }
                Err(error) => return Err(ferr!("failed applying causal ancestor blob: {error}")),
            }
        }

        if !made_progress {
            return Ok(LoadedDocSnapshot::Unavailable);
        }
        Ok(LoadedDocSnapshot::Ready {
            doc,
            partially_decrypted,
        })
    }

    async fn take_or_load_transient_doc(
        &mut self,
    ) -> eyre::Result<crate::runtime2::types::DocLookup<Arc<crate::runtime2::types::LiveDocBundle>>>
    {
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let out = match std::mem::replace(&mut self.state, DocState::Unloaded) {
            DocState::Live(_) => unreachable!("document already live"),
            DocState::Transient(doc) => {
                let bundle = Arc::new(crate::runtime2::types::LiveDocBundle::new_runtime2(
                    self.doc_id,
                    *doc,
                    crate::runtime2::DocLease::new(self.runtime_cmd_tx.clone(), self.doc_id),
                ));
                bundle.set_partially_decrypted(self.partially_decrypted);
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.register_bundle_lease().await?;
                crate::runtime2::types::DocLookup::Ready(bundle)
            }
            DocState::Unloaded | DocState::PendingMaterialization => {
                match self.load_doc_snapshot().await? {
                    LoadedDocSnapshot::Ready {
                        doc,
                        partially_decrypted,
                    } => {
                        let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                        self.transition_to_ready(was_pending, Arc::clone(&heads))
                            .await?;
                        self.set_partially_decrypted(partially_decrypted).await?;
                        let bundle = Arc::new(crate::runtime2::types::LiveDocBundle::new_runtime2(
                            self.doc_id,
                            doc,
                            crate::runtime2::DocLease::new(
                                self.runtime_cmd_tx.clone(),
                                self.doc_id,
                            ),
                        ));
                        bundle.set_partially_decrypted(partially_decrypted);
                        self.state = DocState::Live(Arc::downgrade(&bundle));
                        self.register_bundle_lease().await?;
                        crate::runtime2::types::DocLookup::Ready(bundle)
                    }
                    LoadedDocSnapshot::Unavailable => {
                        self.transition_to_pending(was_pending).await?;
                        crate::runtime2::types::DocLookup::PendingMaterialization
                    }
                    LoadedDocSnapshot::Missing => crate::runtime2::types::DocLookup::Missing,
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
        self.evt_tx.send(event).await.expect(ERROR_CHANNEL);
        Ok(())
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Commit a set of changes locally.
    async fn commit_delta(
        &mut self,
        commits: Vec<(CommitId, std::collections::BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: futures::channel::oneshot::Sender<eyre::Result<()>>,
    ) -> eyre::Result<()> {
        self.pending_fragment_requests
            .extend(self.io.persist_local_commits(self.sed_id, commits).await?);

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
                    .map(|head| CommitId::new(head.0))
                    .collect();
                let checkpoints = fragment
                    .checkpoints
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect();
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
    async fn transition_to_pending(&mut self, was_pending: bool) -> eyre::Result<()> {
        self.state = DocState::PendingMaterialization;
        if !was_pending {
            self.change_manager
                .notify_local_doc_materialization_pending(self.doc_id)?;
        }
        self.set_partially_decrypted(true).await
    }

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
    async fn query_head_state(
        &self,
        resp: futures::channel::oneshot::Sender<eyre::Result<crate::runtime2::DocHeadState>>,
    ) -> eyre::Result<()> {
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

        if sedimentree_heads.is_empty()
            && materialized_heads
                .as_ref()
                .is_some_and(|heads| !heads.is_empty())
        {
            tracing::error!(
                sed_id = ?self.sed_id,
                materialized_heads = materialized_heads.as_ref().map_or(0, |heads| heads.len()),
                ?state,
                "materialized document has heads while its Sedimentree frontier is empty"
            );
            unreachable!("materialized document has heads while its Sedimentree frontier is empty")
        }

        resp.send(Ok(crate::runtime2::DocHeadState {
            sedimentree_heads,
            materialized_heads,
            state,
        }))
        .inspect_err(|_| warn!(ERROR_CALLER))
        .ok();
        Ok(())
    }
}

impl<F: FutureForm> DocWorker2<F> {
    /// Apply content that Subduction has already persisted to the resident live
    /// Automerge document. Sessions for documents without live handles never
    /// reach this worker.
    #[tracing::instrument(skip_all)]
    async fn apply_received_content(
        &mut self,
        peer_id: PeerId,
        commit_ids: Vec<CommitId>,
        fragment_ids: Vec<CommitId>,
    ) -> eyre::Result<()> {
        let received_refs: HashSet<Vec<u8>> = commit_ids
            .iter()
            .chain(&fragment_ids)
            .map(|id| id.as_bytes().to_vec())
            .collect();
        assert!(
            !received_refs.is_empty(),
            "empty sync sessions are not routed to doc workers"
        );
        let Some(bundle) = (match &self.state {
            DocState::Live(bundle) => bundle.upgrade(),
            _ => None,
        }) else {
            // The lease-release command can trail the final Arc drop on the
            // hub's separate command channel. Stored content will be loaded by
            // the next acquisition; there is no live document to update now.
            return Ok(());
        };
        let Some(mut tree) = self.io.hydrate_tree(self.sed_id).await? else {
            tracing::error!(
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
            return Ok(());
        }
        if partially_decrypted {
            self.notif_pending_heads(&mut tree, peer_id).await?;
        }

        let (after_heads, patches) = {
            let mut doc = bundle.doc.lock().await;
            let before = doc.get_heads();
            for blob in blobs {
                match doc.load_incremental(&blob) {
                    Ok(_) => {}
                    Err(automerge::AutomergeError::MissingDeps) => {
                        self.set_partially_decrypted(true).await?;
                        self.notif_pending_heads(&mut tree, peer_id).await?;
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(ferr!("failed applying sync blob: {error}"));
                    }
                }
            }
            let after = doc.get_heads();
            if before == after {
                return Ok(());
            }
            let patches = if self.change_manager.has_change_listener_interest(
                self.doc_id,
                &crate::changes::BigRepoChangeOrigin::Remote { peer_id },
            ) {
                doc.diff(&before, &after)
            } else {
                Vec::new()
            };
            (after, patches)
        };

        let heads = Arc::<[automerge::ChangeHash]>::from(after_heads);
        self.change_manager.notify_doc_heads_changed(
            self.doc_id,
            Arc::clone(&heads),
            crate::changes::BigRepoChangeOrigin::Remote { peer_id },
        )?;
        for patch in patches {
            self.change_manager.notify_doc_changed(
                self.doc_id,
                Arc::new(patch),
                Arc::clone(&heads),
                crate::changes::BigRepoChangeOrigin::Remote { peer_id },
            )?;
        }
        Ok(())
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
                        crate::runtime2::support::BigRepoCiphertextLocator::new(
                            crate::runtime2::support::BigRepoCiphertextKind::Fragment,
                            self.sed_id,
                            f.head(),
                        )
                    }
                    SedimentreeItem::LooseCommit(idx) => {
                        let c = commits
                            .get(*idx)
                            .ok_or_else(|| ferr!("missing loose commit at index {idx}"))?;
                        crate::runtime2::support::BigRepoCiphertextLocator::new(
                            crate::runtime2::support::BigRepoCiphertextKind::LooseCommit,
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
            crate::changes::BigRepoChangeOrigin::Remote { peer_id },
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
    async fn retry_materialization(&mut self) -> eyre::Result<bool> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
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
                    if before.as_slice() != heads.as_ref() {
                        self.change_manager.notify_doc_heads_changed(
                            self.doc_id,
                            Arc::clone(&heads),
                            crate::changes::BigRepoChangeOrigin::Bootstrap,
                        )?;
                        for patch in patches {
                            self.change_manager.notify_doc_changed(
                                self.doc_id,
                                Arc::new(patch),
                                Arc::clone(&heads),
                                crate::changes::BigRepoChangeOrigin::Bootstrap,
                            )?;
                        }
                    }
                    self.state = DocState::Live(Arc::downgrade(&bundle));
                    self.set_partially_decrypted(partially_decrypted).await?;
                    return Ok(partially_decrypted);
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
                        crate::changes::BigRepoChangeOrigin::Bootstrap,
                    )?;
                    for patch in patches {
                        self.change_manager.notify_doc_changed(
                            self.doc_id,
                            Arc::new(patch),
                            Arc::clone(&heads),
                            crate::changes::BigRepoChangeOrigin::Bootstrap,
                        )?;
                    }
                }
                self.state = DocState::Transient(Box::new(doc));
                self.set_partially_decrypted(partially_decrypted).await?;
                Ok(partially_decrypted)
            }
            LoadedDocSnapshot::Unavailable => {
                if live_bundle.is_none() {
                    self.transition_to_pending(was_pending).await?;
                } else {
                    self.set_partially_decrypted(true).await?;
                }
                Ok(true)
            }
            LoadedDocSnapshot::Missing => {
                self.set_partially_decrypted(false).await?;
                Ok(false)
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
        for (barrier_id, _lease) in waiters {
            self.evt_tx
                .send(Runtime2Evt::DocWorkerQuiescent {
                    doc_id: self.doc_id,
                    barrier_id,
                })
                .await
                .expect(ERROR_CHANNEL);
        }
        Ok(())
    }
}
