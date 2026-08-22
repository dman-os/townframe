//! `DocWorker2` — the per-doc actor.

use crate::interlude::*;

use crate::DocumentId;
use crate::changes::BigRepoChangeOrigin;
use crate::runtime2::Runtime2Evt;
use crate::runtime2::support::{
    BigRepoCiphertextKind, BigRepoCiphertextLocator, CausalCheckpoint, causal_checkpoint_id,
    is_causal_checkpoint_id, stage_automerge_ingest,
};
use crate::runtime2::types::{DocLookup, LiveDocBundle};
use crate::runtime2::{
    DocIo, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken, MaterializationBlocker,
    MaterializationStatus, messages::DocWorkerMsg,
};
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
    generation: u64,
) -> SpawnedDocWorker<F>
where
    F: FutureForm + DocWorkerLoop<F> + 'static,
{
    let (msg_tx, msg_rx) = async_channel::unbounded::<DocWorkerMsg>();
    let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());

    let worker = DocWorker2 {
        doc_id,
        sed_id,
        generation,
        state: DocState::Unloaded,
        partially_decrypted: false,
        blocked_refs: HashSet::new(),
        causal_checkpoints: HashMap::new(),
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
                        while let Ok(msg) = msg_rx.recv().await {
                            worker.handle_msg(msg).await?;
                            worker.resolve_quiescence_waiters().await?;
                        }
                        eyre::Ok(())
                    },
                    stop_registration,
                )
                .await;

                let error = match result {
                    Ok(Err(error)) if !cancellation.is_aborted() && !runtime_evt_tx.is_closed() => {
                        Some(format!("document worker failed: {error:?}"))
                    }
                    _ => None,
                };

                if !runtime_evt_tx.is_closed() {
                    let _res = runtime_evt_tx
                        .send(Runtime2Evt::DocWorkerStopped {
                            doc_id,
                            error: error.clone(),
                        })
                        .await;
                }

                if let Some(err) = error {
                    Err(eyre::eyre!("{err}"))
                } else {
                    Ok(())
                }
            }
            .instrument(tracing::info_span!("doc_worker mailbox loop", %doc_id)),
        )
    }
}

struct DocWorker2<F: FutureForm> {
    doc_id: DocumentId,
    sed_id: sedimentree_core::id::SedimentreeId,
    generation: u64,

    state: DocState,
    partially_decrypted: bool,
    /// Content refs (fragment/loose-commit heads) whose plaintext we could not
    /// decrypt or apply (missing key / missing Automerge dependency). The
    /// source of truth for `partially_decrypted`; retried precisely on
    /// session end and on keyhive-driven reattempts, so a live doc never
    /// needs a coarse full-tree rewalk (A7).
    blocked_refs: HashSet<(BigRepoCiphertextKind, CommitId)>,
    /// Decrypted key-only nodes, indexed by their physical Sedimentree head.
    causal_checkpoints: HashMap<CommitId, CausalCheckpoint>,

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
    quiescence_waiters: Vec<(
        futures::channel::oneshot::Sender<()>,
        DocWorkerInternalLease,
    )>,
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
    PendingMaterialization,
}

#[expect(clippy::large_enum_variant)]
enum LoadedDocSnapshot {
    Missing,
    Unavailable {
        blockers: Vec<MaterializationBlocker>,
        blocked_refs: Vec<(BigRepoCiphertextKind, CommitId)>,
    },
    Ready {
        doc: automerge::Automerge,
        partially_decrypted: bool,
        /// Content refs that could not be decrypted or applied; the source
        /// of truth for a live doc's blocked set when materialized cold.
        blocked_refs: Vec<(BigRepoCiphertextKind, CommitId)>,
        causal_checkpoints: Vec<(CommitId, CausalCheckpoint)>,
    },
}

impl LoadedDocSnapshot {
    fn from_materialized_doc(
        doc: automerge::Automerge,
        partially_decrypted: bool,
        blockers: Vec<MaterializationBlocker>,
        blocked_refs: Vec<(BigRepoCiphertextKind, CommitId)>,
        causal_checkpoints: Vec<(CommitId, CausalCheckpoint)>,
    ) -> Self {
        if doc.get_heads().is_empty() {
            Self::Unavailable {
                blockers,
                blocked_refs,
            }
        } else {
            Self::Ready {
                doc,
                partially_decrypted,
                blocked_refs,
                causal_checkpoints,
            }
        }
    }

    fn from_decrypted_plaintexts(
        mut pending: Vec<(BigRepoCiphertextKind, CommitId, Vec<u8>)>,
        mut partially_decrypted: bool,
        doc_id: DocumentId,
        mut blockers: Vec<MaterializationBlocker>,
        mut blocked_refs: Vec<(BigRepoCiphertextKind, CommitId)>,
    ) -> eyre::Result<Self> {
        let mut doc = automerge::Automerge::new();
        let mut causal_checkpoints = Vec::new();
        loop {
            let mut deferred = Vec::new();
            let mut round_progress = false;
            for (kind, ref_, plaintext) in pending {
                if let Some(checkpoint) = CausalCheckpoint::decode(&plaintext)? {
                    causal_checkpoints.push((ref_, checkpoint));
                    round_progress = true;
                    continue;
                }
                match doc.load_incremental(&plaintext) {
                    Ok(0) if doc.get_heads().is_empty() => deferred.push((kind, ref_, plaintext)),
                    Ok(applied) => round_progress |= applied > 0,
                    Err(automerge::AutomergeError::MissingDeps) => {
                        deferred.push((kind, ref_, plaintext))
                    }
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
                blocked_refs.extend(deferred.iter().map(|(kind, ref_, _)| (*kind, *ref_)));
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
            pending = deferred;
        }
        let mut snapshot = Self::from_materialized_doc(
            doc,
            partially_decrypted,
            blockers,
            blocked_refs,
            causal_checkpoints,
        );
        if let Self::Ready { blocked_refs, .. } = &mut snapshot {
            blocked_refs.sort();
            blocked_refs.dedup();
        }
        Ok(snapshot)
    }
}

impl<F: FutureForm> Drop for DocWorker2<F> {
    fn drop(&mut self) {
        if let DocState::Live(ref weak_bundle) = self.state
            && let Some(bundle) = weak_bundle.upgrade()
        {
            bundle.mark_broken();
        }
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
                _lease: _,
            } => self.put_doc(initial_content, resp).await,
            DocWorkerMsg::AcquireHandle { resp, _lease: _ } => self.acquire_handle(resp).await,
            DocWorkerMsg::CommitDelta {
                bundle_id,
                commits,
                heads,
                patches,
                origin,
                resp,
                _lease: _,
            } => {
                self.commit_delta(bundle_id, commits, heads, patches, origin, resp)
                    .await
            }
            DocWorkerMsg::ApplySyncSession {
                peer_id,
                commit_ids,
                fragment_ids,
                reply,
                _lease: _,
            } => {
                self.apply_sync_session(peer_id, commit_ids, fragment_ids, reply)
                    .await
            }
            DocWorkerMsg::ReconcileCausalCoverage { resp, _lease: _ } => {
                let result = self.reconcile_causal_coverage().await;
                if let Some(resp) = resp {
                    resp.send(result.as_ref().copied().map_err(|error| ferr!("{error:?}")))
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                }
                result.map(|_| ())
            }
            DocWorkerMsg::ReattemptMaterialization {
                origin,
                resp,
                _lease: _,
            } => {
                debug!(
                    doc_id = %self.doc_id,
                    pending = matches!(self.state, DocState::PendingMaterialization),
                    "retrying document materialization after dependency update"
                );
                match self.retry_materialization(origin).await {
                    Ok(status) => {
                        debug!(%self.doc_id, ?status, "document materialization retry completed");
                        resp.send(Ok(status))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                        Ok(())
                    }
                    Err(error) => {
                        resp.send(Err(format!("{error:?}")))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                        Err(error)
                    }
                }
            }
            DocWorkerMsg::QueryHeadState { resp, _lease: _ } => {
                resp.send(self.head_state().await)
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                Ok(())
            }
            DocWorkerMsg::InspectHeadState { resp, _lease: _ } => {
                resp.send(self.head_state().await.map(Some))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
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
        if !matches!(self.state, DocState::Unloaded)
            || !self
                .io
                .sedimentree_heads(self.sed_id)
                .await
                .map_err(|err| ferr!("failed checking sedimentree heads for put_doc: {err}"))?
                .is_empty()
        {
            resp.send(Err(ferr!("doc already occupied: {:?}", self.doc_id)))
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
            return Ok(());
        }

        let staged = stage_automerge_ingest(&initial_content);
        self.io
            .persist_initial_document(self.sed_id, staged)
            .await?;

        let heads: Arc<[automerge::ChangeHash]> = Arc::from(initial_content.get_heads());

        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            *initial_content,
            crate::runtime2::DocLease::new(
                self.runtime_cmd_tx.clone(),
                self.doc_id,
                self.generation,
            ),
            false,
        ));

        self.state = DocState::Live(Arc::downgrade(&bundle));

        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, Arc::clone(&heads))?;

        self.register_bundle_lease().await?;

        resp.send(Ok(bundle))
            .inspect_err(|_| warn_loc!(ERROR_CALLER))
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
                    crate::runtime2::DocLease::new(
                        self.runtime_cmd_tx.clone(),
                        self.doc_id,
                        self.generation,
                    ),
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
            DocState::Unloaded | DocState::PendingMaterialization => {
                self.take_or_load_transient_doc().await?
            }
        };
        resp.send(Ok(result))
            .inspect_err(|_| warn_loc!(ERROR_CALLER))
            .ok();
        Ok(())
    }

    async fn register_bundle_lease(&self) -> eyre::Result<()> {
        let (registered_tx, registered_rx) = futures::channel::oneshot::channel();
        if self
            .runtime_cmd_tx
            .send(crate::runtime2::Runtime2Cmd::RegisterDocLease {
                doc_id: self.doc_id,
                registered: registered_tx,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
        registered_rx.await.ok();
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
        let mut pending: Vec<(BigRepoCiphertextKind, CommitId, Vec<u8>)> = Vec::new();
        let mut blocked_refs = Vec::new();
        for item in &order {
            let (kind, head) = match item {
                SedimentreeItem::Fragment(index) => {
                    (BigRepoCiphertextKind::Fragment, fragments[*index].head())
                }
                SedimentreeItem::LooseCommit(index) => {
                    (BigRepoCiphertextKind::LooseCommit, commits[*index].head())
                }
            };
            let content_ref = match item {
                SedimentreeItem::Fragment(index) => fragments[*index].head().as_bytes().to_vec(),
                SedimentreeItem::LooseCommit(index) => commits[*index].head().as_bytes().to_vec(),
            };
            match plaintexts.remove(&content_ref) {
                Some(plaintext) => pending.push((kind, head, plaintext)),
                None => {
                    if kind == BigRepoCiphertextKind::LooseCommit && is_causal_checkpoint_id(head) {
                        continue;
                    }
                    partially_decrypted = true;
                    blocked_refs.push((kind, head));
                }
            }
        }

        // Minimized fragments can hide loose ancestors that causal decryption
        // still returns. Include those dependencies in the same fixed-point
        // application pass. A topological sedimentree order does not guarantee
        // that every decrypted Automerge dependency precedes its child.
        pending.extend(plaintexts.into_iter().filter_map(|(ref_bytes, plaintext)| {
            let Ok(array) = ref_bytes.as_slice().try_into() else {
                warn!(
                    doc_id = %self.doc_id,
                    len = ref_bytes.len(),
                    "skipping content ref with invalid length (expected 32 bytes)"
                );
                return None;
            };
            let commit = CommitId::new(array);
            Some((BigRepoCiphertextKind::LooseCommit, commit, plaintext))
        }));
        LoadedDocSnapshot::from_decrypted_plaintexts(
            pending,
            partially_decrypted,
            self.doc_id,
            blockers,
            blocked_refs,
        )
    }

    async fn take_or_load_transient_doc(&mut self) -> eyre::Result<DocLookup<Arc<LiveDocBundle>>> {
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let out = match std::mem::replace(&mut self.state, DocState::Unloaded) {
            DocState::Live(_) => unreachable!("document already live"),
            DocState::Transient(doc) => {
                let bundle = Arc::new(LiveDocBundle::new(
                    self.doc_id,
                    *doc,
                    crate::runtime2::DocLease::new(
                        self.runtime_cmd_tx.clone(),
                        self.doc_id,
                        self.generation,
                    ),
                    self.partially_decrypted,
                ));
                self.state = DocState::Live(Arc::downgrade(&bundle));
                self.register_bundle_lease().await?;
                DocLookup::Ready(bundle)
            }
            DocState::Unloaded | DocState::PendingMaterialization => {
                match self.load_doc_snapshot().await? {
                    LoadedDocSnapshot::Ready {
                        doc,
                        partially_decrypted,
                        blocked_refs,
                        causal_checkpoints,
                    } => {
                        let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
                        self.transition_to_ready(was_pending, Arc::clone(&heads))
                            .await?;
                        self.blocked_refs = blocked_refs.into_iter().collect();
                        self.causal_checkpoints.extend(causal_checkpoints);
                        self.sync_partial_state().await?;
                        let bundle = Arc::new(LiveDocBundle::new(
                            self.doc_id,
                            doc,
                            crate::runtime2::DocLease::new(
                                self.runtime_cmd_tx.clone(),
                                self.doc_id,
                                self.generation,
                            ),
                            partially_decrypted,
                        ));
                        self.state = DocState::Live(Arc::downgrade(&bundle));
                        self.register_bundle_lease().await?;
                        DocLookup::Ready(bundle)
                    }
                    LoadedDocSnapshot::Unavailable {
                        blockers,
                        blocked_refs,
                    } => {
                        self.blocked_refs = blocked_refs.into_iter().collect();
                        self.transition_to_pending(was_pending, blockers).await?;
                        DocLookup::PendingMaterialization
                    }
                    LoadedDocSnapshot::Missing => DocLookup::Missing,
                }
            }
        };
        Ok(out)
    }

    /// Recompute the partial-decryption flag from the held blocked-refs set
    /// and propagate the change: the live bundle's flag (so handles handed
    /// out before the state change observe it — tier9 regression) and the
    /// materialization pending/ready event.
    async fn sync_partial_state(&mut self) -> eyre::Result<()> {
        let partial = !self.blocked_refs.is_empty();
        if self.partially_decrypted == partial {
            return Ok(());
        }
        self.partially_decrypted = partial;
        if let DocState::Live(bundle) = &self.state
            && let Some(bundle) = bundle.upgrade()
        {
            bundle.set_partially_decrypted(partial);
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
    ///
    /// A rejected commit never persists, so no partial history can form.
    async fn commit_delta(
        &mut self,
        bundle_id: u64,
        mut commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
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
            let message = if current.as_ref().is_some_and(|bundle| bundle.is_broken()) {
                "document write rejected: handle invalidated by an earlier rejected commit; re-acquire the document"
            } else {
                "document write rejected: commit from a stale handle; re-acquire the document"
            };
            resp.send(Err(ferr!("{message}")))
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
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
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
                return Ok(());
            }
            Err(error) => {
                resp.send(Err(error))
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                return Ok(());
            }
        }
        // Automerge payload dependencies and the encryption/Sedimentree DAG
        // intentionally differ when key-only checkpoints are present. A
        // checkpoint can replace exactly the Automerge frontier it covers,
        // but must not make a write depend on arbitrary physical Sedimentree
        // heads: another partition may have produced a checkpoint whose key
        // this writer cannot decrypt.
        let batch_ids: HashSet<_> = commits.iter().map(|(head, _, _)| *head).collect();
        // Eager causal coverage on the origin: if the frontier being
        // extended spans an epoch boundary (any external parent predates
        // the current epoch), ensure a checkpoint covers it under the
        // current epoch — before the rewrite below rewires this batch's
        // parents onto that checkpoint. The bridge then rides the natural
        // sync round, and a receiver's reconcile finds the coverage already
        // present instead of minting its own.
        let distinct_frontiers: HashSet<BTreeSet<CommitId>> = commits
            .iter()
            .map(|(_, parents, _)| {
                parents
                    .iter()
                    .copied()
                    .filter(|parent| !batch_ids.contains(parent))
                    .collect()
            })
            .filter(|frontier: &BTreeSet<CommitId>| !frontier.is_empty())
            .collect();
        let mut frontier_to_checkpoint: HashMap<BTreeSet<CommitId>, CommitId> = HashMap::new();
        for frontier in distinct_frontiers {
            if let Some(checkpoint_head) = self.ensure_frontier_checkpointed(&frontier).await? {
                frontier_to_checkpoint.insert(frontier, checkpoint_head);
            }
        }
        for (_head, parents, _blob) in &mut commits {
            let external: BTreeSet<_> = parents
                .iter()
                .filter(|parent| !batch_ids.contains(parent))
                .copied()
                .collect();
            if let Some(&checkpoint_head) = frontier_to_checkpoint.get(&external) {
                parents.retain(|parent| batch_ids.contains(parent));
                parents.insert(checkpoint_head);
            }
        }
        debug!(
            %self.doc_id,
            commits = ?commits.iter().map(|(head, parents, _)| (*head, parents)).collect::<Vec<_>>(),
            "persisting local Automerge commits"
        );

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
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
        self.pending_fragment_requests
            .extend(pending_fragment_requests);

        // ── 3. Notify heads changed ────────────────────────────────────────

        let heads = Arc::from(heads);
        self.change_manager
            .notify_sedimentree_heads_changed(self.doc_id, Arc::clone(&heads), origin.clone())
            .inspect_err(|err| warn_loc!(ERROR_CALLER, ?err))
            .ok();

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
                .inspect_err(|err| warn_loc!(ERROR_CALLER, ?err))
                .ok();
        }

        // ── 4. Process pending fragment requests ───────────────────────────
        self.process_pending_fragment_requests().await?;

        resp.send(Ok(()))
            .inspect_err(|_| warn_loc!(ERROR_CALLER))
            .ok();
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
                let head = automerge::ChangeHash(*request.head().as_bytes());
                surelock::key::lock_scope(|key| {
                    let (doc, _key) = key.lock(&bundle.doc);
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
                    Ok::<_, eyre::Error>((boundary, checkpoints, raw_blob))
                })?
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
        drop(blockers); // logged above; the state only carries the pending flag
        self.state = DocState::PendingMaterialization;
        if !was_pending {
            self.change_manager
                .notify_local_doc_materialization_pending(self.doc_id)?;
        }
        // A pending doc is by definition partially decrypted: the blocked
        // set is whatever `transition_to_pending`'s caller captured from the
        // walk; the flag + pending event derive from it.
        self.sync_partial_state().await
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
            .durable_sedimentree_heads(self.sed_id)
            .await?
            .iter()
            .map(|cid| automerge::ChangeHash(*cid.as_bytes()))
            .collect();
        sedimentree_heads.sort_unstable();
        let sedimentree_heads: Arc<[automerge::ChangeHash]> = sedimentree_heads.into();
        let (materialized_heads, state) = match &self.state {
            DocState::Live(bundle) => {
                if let Some(bundle) = bundle.upgrade() {
                    let heads: Arc<[automerge::ChangeHash]> = surelock::key::lock_scope(|key| {
                        let (doc, _key) = key.lock(&bundle.doc);
                        Arc::from(doc.get_heads())
                    });
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
                let received_refs: HashSet<&[u8]> = commit_ids
                    .iter()
                    .chain(&fragment_ids)
                    .map(|id| id.as_bytes().as_slice())
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
                let (resolved, unresolved) = self
                    .try_decrypt_received_blobs(&mut tree, &received_refs)
                    .await?;
                // Hold every ref we could not decrypt: the precise A7 record.
                self.blocked_refs.extend(unresolved);
                self.sync_partial_state().await?;
                if resolved.is_empty() {
                    self.notif_pending_heads(&mut tree, peer_id).await?;
                    return self.report_sync_outcome(peer_id, has_live, reply).await;
                }
                if !self.blocked_refs.is_empty() {
                    self.notif_pending_heads(&mut tree, peer_id).await?;
                }

                let origin = BigRepoChangeOrigin::Remote { peer_id };
                // Apply the session's decrypted content incrementally; refs
                // whose Automerge dependencies are still missing stay blocked.
                let (missing_deps, changed, after_heads, patches) =
                    self.apply_blobs_to_live(&bundle, resolved, &origin).await?;
                self.blocked_refs.extend(missing_deps);
                self.sync_partial_state().await?;
                if changed {
                    self.notify_heads_advanced(after_heads, patches, &origin)?;
                }

                // A7: reconsider previously-held blocked refs — this session's
                // keys/deps may unlock content from an earlier session. Precise
                // retry of the held set; no coarse full-tree rewalk.
                self.retry_blocked_refs(&bundle, &origin).await?;
            }

            if !has_live {
                let origin = BigRepoChangeOrigin::Remote { peer_id };
                self.retry_materialization(origin).await?;
            }
            self.reconcile_causal_coverage().await?;
        } else {
            debug_assert!(
                commit_ids.is_empty() && fragment_ids.is_empty(),
                "non-received sync session path must have empty commit_ids and fragment_ids"
            );
        }

        self.report_sync_outcome(peer_id, has_live, reply).await
    }

    /// Attempt to reconcile the current BeeKEM epoch with the materialized
    /// Automerge frontier. The operation is idempotent: settled linear
    /// histories need no shadow node, while a missing epoch or uncovered fork
    /// publishes exactly one epoch-specific checkpoint.
    async fn reconcile_causal_coverage(&mut self) -> eyre::Result<bool> {
        if !self.io.has_doc_write_access(self.doc_id).await? {
            debug!(%self.doc_id, "causal coverage skipped: no write access");
            return Ok(true);
        }
        let needs_reload = matches!(
            self.state,
            DocState::Unloaded | DocState::PendingMaterialization
        ) || matches!(&self.state, DocState::Live(bundle) if bundle.upgrade().is_none());
        if needs_reload {
            self.state = DocState::Unloaded;
            self.retry_materialization(BigRepoChangeOrigin::Local)
                .await?;
        }
        if !self.blocked_refs.is_empty() {
            debug!(%self.doc_id, blocked = self.blocked_refs.len(), "causal coverage skipped: blocked content");
            return Ok(false);
        }
        let materialized_heads: Vec<automerge::ChangeHash> = match &self.state {
            DocState::Transient(doc) => doc.get_heads(),
            DocState::Live(bundle) => {
                let Some(bundle) = bundle.upgrade() else {
                    debug!(%self.doc_id, "causal coverage deferred: live bundle expired");
                    return Ok(false);
                };
                surelock::key::lock_scope(|key| {
                    let (doc, _key) = key.lock(&bundle.doc);
                    doc.get_heads()
                })
            }
            DocState::Unloaded | DocState::PendingMaterialization => {
                debug!(%self.doc_id, "causal coverage deferred: document did not materialize");
                return Ok(false);
            }
        };
        if materialized_heads.is_empty() {
            // An empty Automerge document has no historical application key
            // to carry across this epoch. The Keyhive event is therefore
            // fully classified as requiring no causal checkpoint.
            return Ok(true);
        }

        let covered_frontier: BTreeSet<CommitId> = materialized_heads
            .into_iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let current_epoch = self.io.current_causal_epoch(self.sed_id).await?;
        if let Some(epoch) = current_epoch {
            if self.causal_checkpoints.values().any(|checkpoint| {
                checkpoint.epoch == epoch && checkpoint.covered_frontier == covered_frontier
            }) {
                debug!(%self.doc_id, covered = covered_frontier.len(), "causal coverage already present for current epoch");
                return Ok(true);
            }
            // Per-head epoch check. The materialized heads are worker-owned
            // (the live doc), and each head's ciphertext epoch is an
            // immutable blob fact — no shared-frontier read, no TOCTOU
            // between a heads read and the current-epoch read. If every
            // materialized head is already under the current epoch, the
            // content is covered; a checkpoint is only needed to bridge
            // heads that predate the current epoch.
            let mut all_under_current_epoch = true;
            for head in &covered_frontier {
                match self.io.ciphertext_epoch(self.sed_id, *head).await? {
                    Some(head_epoch) if head_epoch == epoch => {}
                    _ => {
                        all_under_current_epoch = false;
                        break;
                    }
                }
            }
            if all_under_current_epoch {
                debug!(%self.doc_id, covered = covered_frontier.len(), "causal coverage satisfied: all materialized heads under current epoch");
                return Ok(true);
            }
        }

        debug!(%self.doc_id, ?current_epoch, ?covered_frontier, "persisting causal coverage checkpoint");
        let Some((head, checkpoint, _heads_observed)) = self
            .io
            .persist_causal_checkpoint(self.sed_id, covered_frontier)
            .await?
        else {
            return Ok(false);
        };
        self.causal_checkpoints.insert(head, checkpoint);
        Ok(true)
    }

    /// Eager causal coverage at write time: if the frontier being extended
    /// spans an epoch boundary (any external parent predates the current
    /// epoch), ensure a checkpoint covers it under the current epoch. The
    /// decision uses only immutable facts — the parents' ciphertext epochs
    /// (stamped in their blobs) and the current keyhive epoch — never a
    /// shared-frontier read, so it cannot be fooled by mid-arrival or
    /// stale-cache tree state. Idempotent: the checkpoint id is
    /// deterministic in (epoch, covered), so a re-mint after a worker
    /// respawn is a storage no-op.
    async fn ensure_frontier_checkpointed(
        &mut self,
        external_parents: &BTreeSet<CommitId>,
    ) -> eyre::Result<Option<CommitId>> {
        let Some(current_epoch) = self.io.current_causal_epoch(self.sed_id).await? else {
            return Ok(None);
        };
        let mut spans_boundary = false;
        for parent in external_parents {
            if let Some(epoch) = self.io.ciphertext_epoch(self.sed_id, *parent).await?
                && epoch != current_epoch
            {
                spans_boundary = true;
                break;
            }
        }
        if !spans_boundary {
            return Ok(None);
        }
        let checkpoint = CausalCheckpoint::new(current_epoch, external_parents.clone());
        let head = causal_checkpoint_id(&checkpoint);
        if self.causal_checkpoints.contains_key(&head) {
            return Ok(Some(head));
        }
        if let Some((head, checkpoint, _heads)) = self
            .io
            .persist_causal_checkpoint(self.sed_id, external_parents.clone())
            .await?
        {
            self.causal_checkpoints.insert(head, checkpoint);
            return Ok(Some(head));
        }
        Ok(None)
    }

    /// Apply decrypted plaintexts into the live bundle under the doc lock.
    /// Returns the refs that hit `MissingDeps` (they stay blocked), whether
    /// heads advanced, the resulting heads, and the patches to notify.
    #[allow(clippy::type_complexity)]
    async fn apply_blobs_to_live(
        &mut self,
        bundle: &Arc<LiveDocBundle>,
        blobs: Vec<(BigRepoCiphertextKind, CommitId, Vec<u8>)>,
        origin: &BigRepoChangeOrigin,
    ) -> eyre::Result<(
        Vec<(BigRepoCiphertextKind, CommitId)>,
        bool,
        Arc<[automerge::ChangeHash]>,
        Vec<automerge::Patch>,
    )> {
        let mut automerge_blobs = Vec::new();
        for (kind, ref_, plaintext) in blobs {
            if let Some(checkpoint) = CausalCheckpoint::decode(&plaintext)? {
                debug!(%self.doc_id, ?ref_, ?checkpoint.covered_frontier, "filtered causal checkpoint from Automerge");
                self.causal_checkpoints.insert(ref_, checkpoint);
            } else {
                debug!(%self.doc_id, ?ref_, ?kind, "applying decrypted Automerge blob");
                automerge_blobs.push((kind, ref_, plaintext));
            }
        }
        let (missing_deps, changed, after_heads, patches) = surelock::key::lock_scope(|key| {
            let (mut doc, _key) = key.lock(&bundle.doc);
            let before = doc.get_heads();
            let mut missing_deps = Vec::new();
            for (kind, ref_, plaintext) in automerge_blobs {
                match doc.load_incremental(&plaintext) {
                    Ok(_) => {}
                    Err(automerge::AutomergeError::MissingDeps) => {
                        missing_deps.push((kind, ref_));
                    }
                    Err(error) => {
                        return Err(ferr!("failed applying sync blob: {error}"));
                    }
                }
            }
            let after = doc.get_heads();
            debug!(%self.doc_id, ?before, ?after, ?missing_deps, "finished incremental Automerge application");
            let changed = before != after;
            let patches = if changed
                && self
                    .change_manager
                    .has_change_listener_interest(self.doc_id, origin)
            {
                doc.diff(&before, &after)
            } else {
                Vec::new()
            };
            Ok::<_, eyre::Error>((missing_deps, changed, Arc::from(after), patches))
        })?;
        Ok((missing_deps, changed, after_heads, patches))
    }

    /// Notify heads-changed + patches for a live-bundle advance.
    fn notify_heads_advanced(
        &self,
        after_heads: Arc<[automerge::ChangeHash]>,
        patches: Vec<automerge::Patch>,
        origin: &BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        self.change_manager.notify_sedimentree_heads_changed(
            self.doc_id,
            Arc::clone(&after_heads),
            origin.clone(),
        )?;
        for patch in patches {
            self.change_manager.notify_doc_changed(
                self.doc_id,
                Arc::new(patch),
                Arc::clone(&after_heads),
                origin.clone(),
            )?;
        }
        Ok(())
    }

    /// Precisely retry the held blocked refs: decrypt whatever the current key
    /// state unlocks and apply it into the live bundle. Iterative passes
    /// resolve Automerge dependency chains among the blocked refs. Returns
    /// true if heads advanced. This replaces the coarse full-tree rewalk for
    /// live docs (A7: content blocked in an earlier session becomes
    /// decryptable once its key/dependency arrives).
    async fn retry_blocked_refs(
        &mut self,
        bundle: &Arc<LiveDocBundle>,
        origin: &BigRepoChangeOrigin,
    ) -> eyre::Result<bool> {
        if self.blocked_refs.is_empty() {
            return Ok(false);
        }
        let mut remaining: HashSet<(BigRepoCiphertextKind, CommitId)> =
            std::mem::take(&mut self.blocked_refs);
        remaining.retain(|(kind, id)| {
            *kind != BigRepoCiphertextKind::LooseCommit || !is_causal_checkpoint_id(*id)
        });
        let mut applied_any = false;
        let mut made_progress = true;
        while made_progress && !remaining.is_empty() {
            made_progress = false;
            let mut to_apply = Vec::new();
            let mut still_blocked = HashSet::new();
            for (kind, ref_) in remaining.iter().copied() {
                let locator = BigRepoCiphertextLocator::new(kind, self.sed_id, ref_);
                match self
                    .io
                    .try_decrypt_content_keyed(self.sed_id, locator)
                    .await?
                {
                    Some(plaintext) => to_apply.push((kind, ref_, plaintext)),
                    None => {
                        still_blocked.insert((kind, ref_));
                    }
                }
            }
            let attempted = to_apply.len();
            let (missing_deps, changed, after_heads, patches) =
                self.apply_blobs_to_live(bundle, to_apply, origin).await?;
            let applied_count = attempted.saturating_sub(missing_deps.len());
            if applied_count > 0 {
                made_progress = true;
            }
            if changed {
                applied_any = true;
                self.notify_heads_advanced(after_heads, patches, origin)?;
            }
            still_blocked.extend(missing_deps);
            remaining = still_blocked;
        }
        self.blocked_refs = remaining;
        self.sync_partial_state().await?;
        Ok(applied_any)
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
        reply: Option<
            futures::channel::oneshot::Sender<
                Result<
                    crate::runtime2::types::SyncDocReceipt,
                    crate::runtime2::types::SyncDocError,
                >,
            >,
        >,
    ) -> eyre::Result<()> {
        let pending = matches!(self.state, DocState::PendingMaterialization);
        // Only a doc with no live bundle (cold: pending, or persisted-only)
        // runs the full walk here. A live doc's session path already applied
        // the received content incrementally and retried the held blocked
        // refs precisely, so the receipt is honest without a coarse rewalk.
        let walk = pending;
        let result = if walk {
            match self
                .retry_materialization(BigRepoChangeOrigin::Remote { peer_id })
                .await
            {
                Ok(MaterializationStatus::Ready { .. }) => {
                    Ok(crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Ready,
                    })
                }
                Ok(MaterializationStatus::Pending(blockers)) => {
                    Ok(crate::runtime2::types::SyncDocReceipt {
                        outcome: crate::runtime2::types::SyncDocOutcome::Pending(blockers),
                    })
                }
                Ok(MaterializationStatus::Missing) => {
                    Err(crate::runtime2::types::SyncDocError::NotFound)
                }
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
                // Unloaded means the next acquisition cold-walks and
                // repopulates the blocked set; drop stale refs.
                self.blocked_refs.clear();
                self.sync_partial_state().await?;
            }
            self.change_manager
                .notify_cold_sedimentree_heads_updated(
                    self.doc_id,
                    BigRepoChangeOrigin::Remote { peer_id },
                )
                .inspect_err(|err| warn_loc!(ERROR_CALLER, ?err))
                .ok();
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
                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                .ok();
        }
        worker
    }

    /// Decrypt only the content received by this Subduction exchange.
    /// Returns the resolved (ref, plaintext) pairs in topological order plus
    /// the refs that could not be decrypted — those are held as blocked refs
    /// so a later session or key arrival can retry them precisely (A7).
    async fn try_decrypt_received_blobs(
        &self,
        tree: &mut sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        received_refs: &HashSet<&[u8]>,
    ) -> eyre::Result<(
        Vec<(BigRepoCiphertextKind, CommitId, Vec<u8>)>,
        Vec<(BigRepoCiphertextKind, CommitId)>,
    )> {
        let fragments: Vec<_> = tree.fragments().collect();
        let commits: Vec<_> = tree.loose_commits().collect();
        let order = tree
            .topsorted_blob_order()
            .map_err(|error| ferr!("failed ordering sync session blobs: {error}"))?;

        let ordered_items: Vec<(BigRepoCiphertextKind, CommitId, Vec<u8>)> = order
            .iter()
            .filter_map(|item| {
                let (kind, head) = match item {
                    SedimentreeItem::Fragment(idx) => {
                        (BigRepoCiphertextKind::Fragment, fragments.get(*idx)?.head())
                    }
                    SedimentreeItem::LooseCommit(idx) => (
                        BigRepoCiphertextKind::LooseCommit,
                        commits.get(*idx)?.head(),
                    ),
                };
                let content_ref = head.as_bytes().to_vec();
                Some((kind, head, content_ref))
            })
            .collect();
        // Received items are the available causal entrypoints. Successful
        // entrypoint decryption may unlock older stored ancestors that were
        // not named by this particular Subduction receipt; those ancestors
        // must still be returned to the materializer below.
        let received_order: Vec<_> = ordered_items
            .iter()
            .filter(|(_, _, content_ref)| received_refs.contains(content_ref.as_slice()))
            .cloned()
            .collect();

        let expected = received_refs.len();
        if received_order.len() != expected {
            tracing::debug!(
                expected,
                found = received_order.len(),
                "some received refs are not loose entrypoints in sedimentree order (may be merged into fragment or already processed)"
            );
        }

        let mut plaintext_by_ref: HashMap<Vec<u8>, Vec<u8>> = default();
        let mut plaintext_by_index: Vec<Option<Vec<u8>>> = std::iter::repeat_with(|| None)
            .take(received_order.len())
            .collect();
        let mut made_progress = true;

        while made_progress && plaintext_by_index.iter().any(Option::is_none) {
            made_progress = false;
            for (idx, (kind, head, content_ref)) in received_order.iter().enumerate() {
                if plaintext_by_index[idx].is_some() {
                    continue;
                }
                if let Some(plaintext) = plaintext_by_ref.get(content_ref).cloned() {
                    plaintext_by_index[idx] = Some(plaintext);
                    made_progress = true;
                    continue;
                }

                let locator = BigRepoCiphertextLocator::new(*kind, self.sed_id, *head);

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

        let decrypted_refs: HashSet<_> = plaintext_by_ref.keys().cloned().collect();
        let resolved: Vec<_> = ordered_items
            .into_iter()
            .filter_map(|(kind, head, content_ref)| {
                plaintext_by_ref
                    .remove(&content_ref)
                    .map(|plaintext| (kind, head, plaintext))
            })
            .collect();
        let unresolved = received_order
            .into_iter()
            .filter(|(_, _, content_ref)| !decrypted_refs.contains(content_ref))
            .map(|(kind, head, _)| (kind, head))
            .filter(|(kind, head)| {
                *kind != BigRepoCiphertextKind::LooseCommit || !is_causal_checkpoint_id(*head)
            })
            .collect::<Vec<_>>();
        if !unresolved.is_empty() {
            debug!(
                doc_id = %self.doc_id,
                sedimentree_id = %self.sed_id,
                received_count = received_refs.len(),
                resolved_count = resolved.len(),
                unresolved_count = unresolved.len(),
                "received content remains undecryptable during materialization"
            );
        }
        Ok((resolved, unresolved))
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

        self.change_manager
            .notify_doc_pending_sedimentree_heads_changed(
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
        let was_pending = matches!(self.state, DocState::PendingMaterialization);
        let live_bundle = match &self.state {
            DocState::Live(weak) => weak.upgrade(),
            _ => None,
        };
        // A live doc never needs a coarse rewalk: precisely retry the held
        // blocked refs — a keyhive round or an earlier session may have
        // unlocked some (A7). The doc stays live; partial is a valid state.
        if let Some(bundle) = live_bundle {
            let advanced = self.retry_blocked_refs(&bundle, &origin).await?;
            if advanced {
                tracing::debug!(%self.doc_id, "live precise retry advanced doc heads");
            }
            let partially_decrypted = !self.blocked_refs.is_empty();
            return Ok(MaterializationStatus::Ready {
                partially_decrypted,
            });
        }

        match self.load_doc_snapshot().await? {
            LoadedDocSnapshot::Ready {
                doc,
                partially_decrypted,
                blocked_refs,
                causal_checkpoints,
            } => {
                self.blocked_refs = blocked_refs.into_iter().collect();
                self.causal_checkpoints.extend(causal_checkpoints);
                self.sync_partial_state().await?;
                let after_heads = doc.get_heads();
                self.transition_to_ready(was_pending, Arc::from(after_heads.clone()))
                    .await?;
                if was_pending {
                    let patches = doc.diff(&[], &after_heads);
                    let heads = Arc::<[automerge::ChangeHash]>::from(after_heads);
                    self.change_manager.notify_sedimentree_heads_changed(
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
                Ok(MaterializationStatus::Ready {
                    partially_decrypted,
                })
            }
            LoadedDocSnapshot::Unavailable {
                blockers,
                blocked_refs,
            } => {
                self.blocked_refs = blocked_refs.into_iter().collect();
                let status = MaterializationStatus::Pending(blockers.clone());
                self.transition_to_pending(was_pending, blockers).await?;
                Ok(status)
            }
            LoadedDocSnapshot::Missing => {
                self.blocked_refs.clear();
                self.sync_partial_state().await?;
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
            reply
                .send(())
                .inspect_err(|_| debug!(%self.doc_id, "worker fence reply dropped"))
                .ok();
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
            LoadedDocSnapshot::from_materialized_doc(
                loaded,
                false,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
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
            vec![
                (
                    BigRepoCiphertextKind::LooseCommit,
                    CommitId::new([0; 32]),
                    child,
                ),
                (
                    BigRepoCiphertextKind::LooseCommit,
                    CommitId::new([1; 32]),
                    parent,
                ),
            ],
            false,
            DocumentId::new([1; 32]),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let LoadedDocSnapshot::Ready { doc, .. } = snapshot else {
            panic!("parent and child plaintexts should materialize");
        };
        assert_eq!(doc.get_heads(), expected_heads);
    }

    #[test]
    fn non_automerge_shadow_node_contracts_out_of_materialized_history() {
        use automerge::{ReadDoc, transaction::Transactable};
        use sedimentree_core::{
            blob::{Blob, BlobMeta},
            depth::CountLeadingZeroBytes,
            id::SedimentreeId,
            loose_commit::LooseCommit,
            sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
        };

        let mut base = automerge::Automerge::new();
        base.transact(|tx| tx.put(automerge::ROOT, "base", 1))
            .unwrap();
        let base_heads = base.get_heads();
        assert_eq!(base_heads.len(), 1);
        let base_head = base_heads[0];
        let base_bytes = base.save();

        let mut left = base
            .fork()
            .with_actor(automerge::ActorId::from(b"shadow-left".as_slice()));
        left.transact(|tx| tx.put(automerge::ROOT, "left", 1))
            .unwrap();
        let left_heads = left.get_heads();
        assert_eq!(left_heads.len(), 1);
        let left_head = left_heads[0];
        let left_bytes = left.save_after(&[base_head]);

        let mut right = base
            .fork()
            .with_actor(automerge::ActorId::from(b"shadow-right".as_slice()));
        right
            .transact(|tx| tx.put(automerge::ROOT, "right", 1))
            .unwrap();
        let right_heads = right.get_heads();
        assert_eq!(right_heads.len(), 1);
        let right_head = right_heads[0];
        let right_bytes = right.save_after(&[base_head]);

        let sed_id = SedimentreeId::new([9; 32]);
        let base_id = CommitId::new(base_head.0);
        // Keep the synthetic ID away from CountLeadingZeroBytes boundaries so
        // it cannot accidentally become a fragment boundary.
        let shadow_id = CommitId::new([0x7f; 32]);
        let left_id = CommitId::new(left_head.0);
        let right_id = CommitId::new(right_head.0);
        let meta = |tag| BlobMeta::new(&Blob::new(vec![tag]));
        let commits = vec![
            LooseCommit::new(sed_id, base_id, BTreeSet::new(), meta(1)),
            LooseCommit::new(sed_id, shadow_id, BTreeSet::from([base_id]), meta(2)),
            // Sedimentree says these depend on the shadow node, while their
            // Automerge payloads still directly depend on `base_head`.
            LooseCommit::new(sed_id, left_id, BTreeSet::from([shadow_id]), meta(3)),
            LooseCommit::new(sed_id, right_id, BTreeSet::from([shadow_id]), meta(4)),
        ];
        let mut tree = MinimizedSedimentree::new(Sedimentree::new(Vec::new(), commits));
        tree.ensure_minimized(&CountLeadingZeroBytes);
        assert_eq!(
            tree.heads(&CountLeadingZeroBytes)
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([left_id, right_id])
        );

        let loose: Vec<_> = tree.loose_commits().collect();
        let order = tree.topsorted_blob_order().unwrap();
        let ordered_ids: Vec<_> = order
            .iter()
            .map(|item| match item {
                SedimentreeItem::LooseCommit(index) => loose[*index].head(),
                SedimentreeItem::Fragment(_) => unreachable!(),
            })
            .collect();
        let position = |id| {
            ordered_ids
                .iter()
                .position(|candidate| *candidate == id)
                .unwrap()
        };
        assert!(position(base_id) < position(shadow_id));
        assert!(position(shadow_id) < position(left_id));
        assert!(position(shadow_id) < position(right_id));

        let snapshot = LoadedDocSnapshot::from_decrypted_plaintexts(
            ordered_ids
                .into_iter()
                .filter_map(|id| match id {
                    id if id == base_id => {
                        Some((BigRepoCiphertextKind::LooseCommit, id, base_bytes.clone()))
                    }
                    id if id == left_id => {
                        Some((BigRepoCiphertextKind::LooseCommit, id, left_bytes.clone()))
                    }
                    id if id == right_id => {
                        Some((BigRepoCiphertextKind::LooseCommit, id, right_bytes.clone()))
                    }
                    id if id == shadow_id => None,
                    _ => unreachable!(),
                })
                .collect(),
            false,
            DocumentId::new(*sed_id.as_bytes()),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let LoadedDocSnapshot::Ready { doc, .. } = snapshot else {
            panic!("filtering the shadow node must leave a materializable Automerge DAG");
        };
        assert_eq!(
            doc.get_heads().into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([left_head, right_head])
        );
        assert_eq!(
            doc.get(automerge::ROOT, "base")
                .unwrap()
                .unwrap()
                .0
                .to_i64(),
            Some(1)
        );
        assert_eq!(
            doc.get(automerge::ROOT, "left")
                .unwrap()
                .unwrap()
                .0
                .to_i64(),
            Some(1)
        );
        assert_eq!(
            doc.get(automerge::ROOT, "right")
                .unwrap()
                .unwrap()
                .0
                .to_i64(),
            Some(1)
        );
    }
}
