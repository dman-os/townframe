//! `Runtime2Handle` — the public API handle.
//!
//! # Generics
//!
//! Generic over `F: FutureForm` to carry the injected [`Timer`] so timeout
//! behaviour is runtime-neutral. The hub and stop token carry the same `F`.
//!
//! [`Runtime2Cmd`]: super::Runtime2Cmd
//! [`Timer`]: super::Timer

use crate::DocumentId;
use crate::interlude::*;
#[cfg(any(test, feature = "test-support"))]
use crate::runtime2::Timer;
use crate::runtime2::messages::{Runtime2Cmd, fresh_waiter_id};
use big_sync_core::PeerKey;
use future_form::FutureForm;
use std::sync::Arc;

/// The handle embedders use to drive the runtime.
pub struct Runtime2Handle<F: FutureForm> {
    pub(crate) cmd_tx: async_channel::Sender<Runtime2Cmd>,
    pub(crate) sync_policy: crate::runtime2::types::BigRepoSyncPolicy,
    pub(crate) doc_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Injected runtime-neutral timer for timeout operations.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) timer: Arc<dyn Timer<F>>,
    #[cfg(not(any(test, feature = "test-support")))]
    pub(crate) _future_form: std::marker::PhantomData<F>,
}

impl<F: FutureForm> Clone for Runtime2Handle<F> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            sync_policy: self.sync_policy,
            doc_sync_waiter_ids: Arc::clone(&self.doc_sync_waiter_ids),
            keyhive_sync_waiter_ids: Arc::clone(&self.keyhive_sync_waiter_ids),
            #[cfg(any(test, feature = "test-support"))]
            timer: Arc::clone(&self.timer),
            #[cfg(not(any(test, feature = "test-support")))]
            _future_form: std::marker::PhantomData,
        }
    }
}

impl<F: FutureForm> Runtime2Handle<F> {
    pub(crate) fn is_stopped(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Construct a new handle. Called by `spawn_runtime2` in the hub.
    pub(crate) fn new(
        cmd_tx: async_channel::Sender<Runtime2Cmd>,
        sync_policy: crate::runtime2::types::BigRepoSyncPolicy,
        #[cfg(any(test, feature = "test-support"))] timer: Arc<dyn Timer<F>>,
        doc_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
        keyhive_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            cmd_tx,
            sync_policy,
            doc_sync_waiter_ids,
            keyhive_sync_waiter_ids,
            #[cfg(any(test, feature = "test-support"))]
            timer,
            #[cfg(not(any(test, feature = "test-support")))]
            _future_form: std::marker::PhantomData,
        }
    }

    // ── doc lifecycle ──────────────────────────────────────────────────────

    pub async fn allocate_doc(
        &self,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
    ) -> eyre::Result<DocumentId> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::AllocateDoc { parents, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    pub async fn finalize_allocated_doc(
        &self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        pending_group: crate::keyhive::BigKeyhiveGroup,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    ) -> eyre::Result<crate::runtime2::types::LiveDocHandle> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::FinalizeAllocatedDoc {
                doc_id,
                initial_content: Box::new(initial_content),
                initial_keys,
                pending_group,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Create a new document with `initial_content` and the given keyhive
    /// `parents` (co-creators).
    ///
    /// Sends a [`CreateDoc`] command to the hub, which asynchronously calls
    /// [`RuntimeIo::create_document`] then enqueues a [`PutDoc`] to itself.
    ///
    /// [`CreateDoc`]: Runtime2Cmd::CreateDoc
    /// [`PutDoc`]: Runtime2Cmd::PutDoc
    /// [`RuntimeIo::create_document`]: super::RuntimeIo::create_document
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
    ) -> eyre::Result<crate::runtime2::types::LiveDocHandle> {
        use nonempty::NonEmpty;
        let heads = initial_content.get_heads();
        let content_heads = NonEmpty::from_vec(heads.iter().map(|head| head.0).collect())
            .ok_or_else(|| eyre::eyre!("automerge doc has no heads"))?;
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CreateDoc {
                initial_content: Box::new(initial_content),
                parents,
                content_heads,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Get or spawn a live handle for an existing document.
    ///
    /// Returns [`DocLookup::Ready`] with a live automerge bundle,
    /// [`DocLookup::PendingMaterialization`] if the doc exists but is not yet
    /// decryptable, or [`DocLookup::Missing`] if unknown.
    ///
    /// [`DocLookup::Ready`]: crate::runtime2::types::DocLookup::Ready
    /// [`DocLookup::PendingMaterialization`]: crate::runtime2::types::DocLookup::PendingMaterialization
    /// [`DocLookup::Missing`]: crate::runtime2::types::DocLookup::Missing
    pub async fn get_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime2::types::DocLookup<crate::runtime2::types::LiveDocHandle>>
    {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::GetDocHandle {
                doc_id,
                lease: crate::runtime2::DocLeaseKind::Caller,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Acquire a live handle for background work that must not count as a
    /// live caller.
    ///
    /// Identical to [`Self::get_doc_handle`] except that the document is not
    /// marked as having a live caller: received content stays out of the
    /// materialized bundle and no user-visible change notification is emitted
    /// for a document nobody holds. Used by the Automerge frontier publisher.
    pub async fn acquire_internal_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime2::types::DocLookup<crate::runtime2::types::LiveDocHandle>>
    {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::GetDocHandle {
                doc_id,
                lease: crate::runtime2::DocLeaseKind::Internal,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Commit a delta (sets of encrypted commits) to a document.
    ///
    /// Each commit is a triple (head, parents, blob); the runtime encrypts
    /// and persists it atomically via the [`DocIo::store_commit`] seam.
    ///
    /// [`DocIo::store_commit`]: super::DocIo::store_commit
    pub async fn commit_delta(
        &self,
        doc_id: DocumentId,
        // Bundle id of the committing handle; the hub forwards it to the worker
        // which rejects commits from broken or replaced bundles.
        bundle_id: u64,
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CommitDelta {
                doc_id,
                bundle_id,
                commits,
                heads,
                patches,
                origin,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Query walk-derived storage and materialization heads for a document.
    pub async fn doc_head_state(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime2::DocHeadState> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::DocHeadState { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    pub(crate) async fn ensure_causal_coverage(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::EnsureCausalCoverage {
                doc_id,
                resp: Some(resp),
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_ACTOR))?
    }

    /// Inspect head state without creating a document worker.
    pub async fn inspect_doc_head_state(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<Option<crate::runtime2::DocHeadState>> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::InspectDocHeadState { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    // ── connections (transport-agnostic) ──────────────────────────────────

    /// Open an outbound connection to `peer` at a transport-specific `addr`.
    ///
    /// The `addr` is an opaque `Box<dyn Any + Send>` that the hub's
    /// [`TransportConnect`](super::TransportConnect) implementation
    /// interprets.
    ///
    /// The returned receiver resolves with `(closed, end_result)` once the
    /// hub's watcher observes the transport connection lifecycle ending —
    /// `closed` is the connection's end flag (shared with the runtime) so
    /// callers can tell which connection ended when ids are reused.
    pub async fn open_connection(
        &self,
        peer: PeerKey,
        addr: Box<dyn std::any::Any + Send>,
    ) -> eyre::Result<(
        PeerKey,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        futures::channel::oneshot::Receiver<(
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            eyre::Result<()>,
        )>,
    )> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::OpenConn { peer, addr, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Accept an inbound connection from the transport layer.
    ///
    /// `incoming` is an opaque handle the hub's
    /// [`TransportConnect`](super::TransportConnect) implementation
    /// uses to complete the handshake.
    ///
    /// The returned receiver resolves with `(closed, end_result)` once the
    /// hub's watcher observes the transport connection lifecycle ending —
    /// `closed` is the connection's end flag (shared with the runtime) so
    /// callers can tell which connection ended when ids are reused.
    pub async fn accept_connection(
        &self,
        incoming: Box<dyn std::any::Any + Send>,
    ) -> eyre::Result<(
        PeerKey,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        futures::channel::oneshot::Receiver<(
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            eyre::Result<()>,
        )>,
    )> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::AcceptConn { incoming, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Close one established connection to `peer_id`, identified by its end
    /// flag. Only the peer's current connection's registration is torn
    /// down; closing a superseded connection leaves the replacement intact.
    pub async fn close_connection(
        &self,
        peer_id: PeerKey,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CloseConn {
                peer_id,
                closed,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    // ── sync ───────────────────────────────────────────────────────────────
    //
    // Synchronous-wait contract for the sync APIs — what "returns" means.
    //
    // One rule decides all of it: a fact is only ordered across the hub if
    // both of its halves are enqueued by the same task (the hub polls
    // commands before events), so each API below names exactly which facts it
    // waits for.
    //
    // - `sync_doc_with_peer*` waits for ONE document's round with ONE peer:
    //   the exchange, plus that round's received content being routed into the
    //   document's worker — the receipt resolves from a worker reconsider that
    //   is FIFO-ordered after that round's own content apply. It does NOT wait
    //   for other documents, for this document's (re)materialization, for
    //   membership-change emission, or for Keyhive projection.
    // - `sync_keyhive_with_peer` waits for the peer's Keyhive round — the
    //   exchange plus the durable incorporation of that round's admissions —
    //   and then for `wait_for_keyhive_reconciliation` below. It does NOT wait
    //   for membership-change emission (delegations and revocations notify
    //   asynchronously) nor for document rematerialization (only documents
    //   already pending are retried).
    // - `wait_for_keyhive_reconciliation` waits for the projection watermark
    //   that admission updates.
    //
    // Work deliberately outside these waits is still tracked, so
    // `wait_for_quiescence` (tests) and shutdown drain it anyway.

    /// Sync one document's sedimentree with one peer, and wait for its result.
    ///
    /// Synchronously waits for this document's sync round with `peer_id`: the
    /// exchange, and the routing of the round's received content into the
    /// document's worker. The receipt resolves from a worker reconsider that
    /// the hub routes after that round's own content apply, so a returned `Ok`
    /// means the round's content reached the live document (or was already
    /// stored).
    ///
    /// Deliberately NOT waited for: other documents, this document's
    /// (re)materialization, membership-change emission, and Keyhive projection
    /// — use [`Runtime2Handle::sync_keyhive_with_peer`] for the latter. See the
    /// section contract above for the full table.
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerKey,
    ) -> Result<(), crate::runtime2::types::SyncDocError> {
        self.sync_doc_with_peer_receipt(doc_id, peer_id)
            .await
            .map(|_| ())
    }

    /// [`Runtime2Handle::sync_doc_with_peer`] with the receipt kept.
    ///
    /// Same synchronous wait as [`Runtime2Handle::sync_doc_with_peer`]; the
    /// receipt distinguishes an applied round from one whose content was
    /// already stored (`Stored`) and errors when the round failed or its
    /// content apply could not be routed to a live worker
    /// (`SyncDocError::WorkerUnavailable`).
    pub async fn sync_doc_with_peer_receipt(
        &self,
        doc_id: DocumentId,
        peer_id: PeerKey,
    ) -> Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError> {
        let waiter_id = fresh_waiter_id(&self.doc_sync_waiter_ids);
        debug!(
            sync_id = waiter_id,
            %doc_id,
            %peer_id,
            "document sync requested"
        );
        let mut guard = DocSyncWaiterGuard {
            cmd_tx: self.cmd_tx.clone(),
            doc_id: doc_id.clone(),
            peer_id: peer_id.clone(),
            waiter_id,
            completed: false,
        };
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                resp,
            })
            .await
            .map_err(|_| crate::runtime2::types::SyncDocError::IoError(eyre::eyre!(ERROR_ACTOR)))?;
        let res = rx
            .await
            .map_err(|_| crate::runtime2::types::SyncDocError::IoError(ferr!(ERROR_CHANNEL)))?;
        guard.completed = true;
        match &res {
            Ok(receipt) => debug!(
                sync_id = waiter_id,
                ?receipt.outcome,
                "document sync completed"
            ),
            Err(error) => debug!(sync_id = waiter_id, ?error, "document sync failed"),
        }
        res
    }

    /// Sync Keyhive state with a peer and wait for it to be applied AND projected.
    ///
    /// Two synchronous waits, in order:
    ///
    /// 1. The peer's Keyhive round: the exchange, plus the durable
    ///    incorporation of that round's admissions. The durable incorporation
    ///    hook is awaited before the protocol acknowledges the exchange, so
    ///    the resulting admission update is enqueued ahead of the round's
    ///    completion and the hub's admission watermark already reflects it
    ///    when the round resolves.
    /// 2. [`Runtime2Handle::wait_for_keyhive_reconciliation`], i.e. the
    ///    group-part projection watermark reaching the admission watermark
    ///    captured at that moment.
    ///
    /// Deliberately NOT waited for: membership-change emission (delegations
    /// and revocations are emitted asynchronously as tracked work) and
    /// document rematerialization (only documents already pending are
    /// retried). Both are drained by `wait_for_quiescence` and shutdown.
    pub async fn sync_keyhive_with_peer(&self, peer_id: PeerKey) -> eyre::Result<()> {
        let waiter_id = fresh_waiter_id(&self.keyhive_sync_waiter_ids);
        let mut guard = KeyhiveSyncWaiterGuard {
            cmd_tx: self.cmd_tx.clone(),
            peer_id: peer_id.clone(),
            waiter_id,
            completed: false,
        };
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let result = rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?;
        guard.completed = true;
        result.wrap_err("keyhive sync failed")?;
        let (resp, reconciled) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::WaitForKeyhiveReconciliation { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        reconciled
            .await
            .map_err(|_| eyre::eyre!("runtime dropped keyhive reconciliation response"))?
            .wrap_err("keyhive post-sync reconciliation failed")
    }

    /// Wait until every Keyhive admission already incorporated locally reaches
    /// durable projection settlement.
    ///
    /// Captures the current admission watermark (`admitted_head`) and returns
    /// once the group-part projection watermark (`group_part_settled_seq`) has
    /// reached it, so a returned `Ok` means the admitted Keyhive state is
    /// reflected in the parts peers are served.
    ///
    /// This may block for a long time while Keyhive synchronization and durable
    /// I/O complete. Applications that require a deadline should apply their
    /// timeout at the application boundary.
    pub async fn wait_for_keyhive_reconciliation(&self) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::WaitForKeyhiveReconciliation { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| ferr!(ERROR_CHANNEL))?
            .wrap_err("keyhive reconciliation failed")
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn wait_for_quiescence(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> eyre::Result<()> {
        self.wait_for_quiescence_freeze(timeout, false).await
    }

    /// Like [`Runtime2Handle::wait_for_quiescence`], but freezes the hub once
    /// quiescence is reached: no further events are processed and all
    /// non-`Unfreeze` commands are held until [`Runtime2Handle::unfreeze`].
    #[cfg(any(test, feature = "test-support"))]
    pub async fn wait_for_quiescence_freeze(
        &self,
        timeout: Option<std::time::Duration>,
        freeze: bool,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::WaitForQuiescence { freeze, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;

        if let Some(duration) = timeout {
            match self.race_timeout(rx, duration).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(ferr!(ERROR_CHANNEL)),
                Err(()) => Err(eyre::eyre!("quiescence wait timed out")),
            }
        } else {
            rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
        }
    }

    /// Resume event/command processing after a frozen quiescence wait.
    pub async fn unfreeze(&self) -> eyre::Result<()> {
        self.cmd_tx
            .send(Runtime2Cmd::Unfreeze)
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))
    }

    /// Test-only seam: queue events instead of handling them.
    ///
    /// The hub polls commands before events (`select_biased!`), so a command
    /// whose own spawned work already emitted its events can be handled first —
    /// an inversion a test cannot otherwise reach without loading the machine.
    /// While held, events accumulate in the hub (and the in-flight counter they
    /// carry stays unreleased, so no quiescence probe can resolve).
    /// `resume_events_for_test` reopens processing and replays them in channel
    /// order.
    #[cfg(test)]
    pub(crate) async fn hold_events_for_test(&self) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HoldEvents { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))
    }

    /// Test-only seam: reopen event processing and handle the events queued
    /// since [`Runtime2Handle::hold_events_for_test`], in channel order.
    #[cfg(test)]
    pub(crate) async fn resume_events_for_test(&self) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::ResumeEvents { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))
    }

    /// Test-only panic-safety net for [`Runtime2Handle::hold_events_for_test`]:
    /// reopen event processing from a `Drop`, so a failed assertion cannot
    /// leave the hub holding every later event (including the in-flight
    /// counter's release) for the rest of the test process.
    #[cfg(test)]
    pub(crate) fn resume_events_for_test_on_drop(&self) {
        let resume = self.cmd_tx.try_send(Runtime2Cmd::ResumeEvents {
            resp: futures::channel::oneshot::channel().0,
        });
        if let Err(err) = resume {
            tracing::debug!(
                ?err,
                "resuming hub event processing on drop failed (runtime draining)"
            );
        }
    }

    pub async fn contains_sedimentree_id(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::ContainsSedimentree { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    pub async fn inspect_stored_doc_blobs(&self, doc_id: DocumentId) -> eyre::Result<Vec<Vec<u8>>> {
        let (resp, rx) = futures::channel::oneshot::channel();
        let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32()?);
        self.cmd_tx
            .send(Runtime2Cmd::InspectStoredDocBlobs { sed_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    pub async fn has_local_doc_state(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HasLocalDocState { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    #[cfg(test)]
    pub(crate) async fn has_doc_worker(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HasDocWorker { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Test-only: whether `peer_id` currently has a registered connection in
    /// the hub. Lets a connection-lifecycle test assert the deregistration
    /// invariant directly instead of inferring it from a sync failure.
    #[cfg(test)]
    pub(crate) async fn has_connected_peer_for_test(&self, peer_id: PeerKey) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HasConnectedPeer { peer_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Test-only: deliver a synthetic keyhive sync completion for `peer_id`'s
    /// active round, stamped one seq ahead of the hub's admitted head. Returns
    /// the seq so the test can inject the matching admission event.
    #[cfg(test)]
    pub(crate) async fn inject_keyhive_completion_for_test(
        &self,
        peer_id: PeerKey,
    ) -> eyre::Result<u64> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::InjectKeyhiveCompletionForTest { peer_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Test-only: hand an event directly to the hub's event handler, bypassing
    /// the event channel so a test can drive event ordering deterministically.
    #[cfg(test)]
    pub(crate) async fn inject_runtime2_evt_for_test(
        &self,
        evt: crate::runtime2::Runtime2Evt,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::InjectRuntime2EvtForTest {
                evt: Box::new(evt),
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))?
    }

    /// Test-only: the active quiescence probe's barrier id (`None` if resolved).
    #[cfg(test)]
    pub(crate) async fn quiescence_probe_barrier_for_test(&self) -> eyre::Result<Option<u64>> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::QuiescenceProbeBarrierForTest { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))
    }

    /// Test-only seam: close the doc worker's mailbox for `doc_id` on the next
    /// content-carrying sync-session apply, so the route fails the way the
    /// worker-stopping race would.
    #[cfg(test)]
    pub(crate) async fn fail_next_content_apply_route_for_test(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::FailNextContentApplyRoute { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| ferr!(ERROR_CHANNEL))
    }

    // ── private helpers ──────────────────────────────────────────────────────

    /// Race a oneshot response against a timer sleep.
    ///
    /// Replaces the `timeout` combinator. Uses `select_biased!` so the sleep
    /// branch has priority when both are ready (parity with Tokio semantics
    /// where timeout always resolves first on simultaneity).
    ///
    /// Returns `Ok(Ok(value))` on response success, `Ok(Err(Canceled))` on
    /// caller-drop, and `Err(())` on timeout.
    #[cfg(any(test, feature = "test-support"))]
    async fn race_timeout<T>(
        &self,
        rx: futures::channel::oneshot::Receiver<T>,
        duration: std::time::Duration,
    ) -> Result<Result<T, futures::channel::oneshot::Canceled>, ()> {
        use futures::future::{Either, select};
        let sleep = Box::pin(self.timer.sleep(duration));
        match select(sleep, rx).await {
            Either::Left(_) => Err(()),
            Either::Right((result, _)) => Ok(result),
        }
    }
}

struct DocSyncWaiterGuard {
    cmd_tx: async_channel::Sender<Runtime2Cmd>,
    doc_id: DocumentId,
    peer_id: PeerKey,
    waiter_id: u64,
    completed: bool,
}

impl Drop for DocSyncWaiterGuard {
    fn drop(&mut self) {
        if !self.completed {
            drop(self.cmd_tx.try_send(Runtime2Cmd::CancelDocSyncWaiter {
                doc_id: self.doc_id.clone(),
                peer_id: self.peer_id.clone(),
                waiter_id: self.waiter_id,
            }));
        }
    }
}

struct KeyhiveSyncWaiterGuard {
    cmd_tx: async_channel::Sender<Runtime2Cmd>,
    peer_id: PeerKey,
    waiter_id: u64,
    completed: bool,
}

impl Drop for KeyhiveSyncWaiterGuard {
    fn drop(&mut self) {
        if !self.completed {
            drop(self.cmd_tx.try_send(Runtime2Cmd::CancelKeyhiveSyncWaiter {
                peer_id: self.peer_id.clone(),
                waiter_id: self.waiter_id,
            }));
        }
    }
}
