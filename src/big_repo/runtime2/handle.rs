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
use crate::runtime2::{
    Timer,
    messages::{Runtime2Cmd, fresh_waiter_id},
};
use big_sync_core::PeerId;
use future_form::FutureForm;
use std::sync::Arc;

/// The handle embedders use to drive the runtime.
pub struct Runtime2Handle<F: FutureForm> {
    pub(crate) cmd_tx: async_channel::Sender<Runtime2Cmd>,
    pub(crate) sync_policy: crate::runtime2::types::BigRepoSyncPolicy,
    pub(crate) doc_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Injected runtime-neutral timer for timeout operations.
    pub(crate) timer: Arc<dyn Timer<F>>,
}

impl<F: FutureForm> Clone for Runtime2Handle<F> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            sync_policy: self.sync_policy,
            doc_sync_waiter_ids: Arc::clone(&self.doc_sync_waiter_ids),
            keyhive_sync_waiter_ids: Arc::clone(&self.keyhive_sync_waiter_ids),
            timer: Arc::clone(&self.timer),
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
        timer: Arc<dyn Timer<F>>,
        doc_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
        keyhive_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            cmd_tx,
            sync_policy,
            doc_sync_waiter_ids,
            keyhive_sync_waiter_ids,
            timer,
        }
    }

    // ── doc lifecycle ──────────────────────────────────────────────────────

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
    ) -> eyre::Result<std::sync::Arc<crate::runtime2::types::LiveDocBundle>> {
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
    ) -> eyre::Result<
        crate::runtime2::types::DocLookup<std::sync::Arc<crate::runtime2::types::LiveDocBundle>>,
    > {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::GetDocHandle { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
    ) -> eyre::Result<(
        PeerId,
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
        PeerId,
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
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Close one established connection to `peer_id`, identified by its end
    /// flag. Only the peer's current connection's registration is torn
    /// down; closing a superseded connection leaves the replacement intact.
    pub async fn close_connection(
        &self,
        peer_id: PeerId,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CloseConn {
                peer_id,
                closed,
                resp: Some(resp),
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    // ── sync ───────────────────────────────────────────────────────────────

    /// Sync a document's sedimentree with a peer. Waits for completion or
    /// `timeout`.
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), crate::runtime2::types::SyncDocError> {
        self.sync_doc_with_peer_receipt(doc_id, peer_id, timeout)
            .await
            .map(|_| ())
    }

    pub async fn sync_doc_with_peer_receipt(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Result<crate::runtime2::types::SyncDocReceipt, crate::runtime2::types::SyncDocError> {
        let waiter_id = fresh_waiter_id(&self.doc_sync_waiter_ids);
        debug!(
            sync_id = waiter_id,
            %doc_id,
            %peer_id,
            "document sync requested"
        );
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp,
            })
            .await
            .map_err(|_| crate::runtime2::types::SyncDocError::IoError(eyre::eyre!(ERROR_ACTOR)))?;
        let result = if let Some(duration) = timeout {
            let duration = utils_rs::scale_timeout(duration);
            match self.race_timeout(rx, duration).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(crate::runtime2::types::SyncDocError::IoError(eyre::eyre!(
                    "caller dropped before response"
                ))),
                Err(()) => {
                    self.cmd_tx
                        .try_send(Runtime2Cmd::CancelDocSyncWaiter {
                            doc_id,
                            peer_id,
                            waiter_id,
                        })
                        .map_err(|err| match err {
                            async_channel::TrySendError::Closed(_) => {
                                crate::runtime2::types::SyncDocError::IoError(ferr!(
                                    "task was found dead"
                                ))
                            }
                            async_channel::TrySendError::Full(_) => {
                                crate::runtime2::types::SyncDocError::IoError(ferr!("mailbox full"))
                            }
                        })?;
                    Err(crate::runtime2::types::SyncDocError::IoError(eyre::eyre!(
                        "doc sync timed out"
                    )))
                }
            }
        } else {
            rx.await.map_err(|_| {
                crate::runtime2::types::SyncDocError::IoError(eyre::eyre!(
                    "caller dropped before response"
                ))
            })?
        };
        match &result {
            Ok(receipt) => debug!(
                sync_id = waiter_id,
                ?receipt.outcome,
                "document sync completed"
            ),
            Err(error) => debug!(sync_id = waiter_id, ?error, "document sync failed"),
        }
        result
    }

    /// Sync keyhive state with a peer. Waits for completion or `timeout`.
    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> eyre::Result<()> {
        let waiter_id = fresh_waiter_id(&self.keyhive_sync_waiter_ids);
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let timeout =
            utils_rs::scale_timeout(timeout.unwrap_or_else(|| std::time::Duration::from_secs(30)));
        let deadline = std::time::Instant::now() + timeout;
        match self.race_timeout(rx, timeout).await {
            Ok(Ok(result)) => {
                result.wrap_err("keyhive sync failed")?;
                let (resp, reconciled) = futures::channel::oneshot::channel();
                self.cmd_tx
                    .send(Runtime2Cmd::WaitForKeyhiveReconciliation { resp })
                    .await
                    .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                match self.race_timeout(reconciled, remaining).await {
                    Ok(Ok(result)) => result.wrap_err("keyhive post-sync reconciliation failed"),
                    Ok(Err(_)) => Err(eyre::eyre!(
                        "runtime dropped keyhive reconciliation response"
                    )),
                    Err(()) => Err(eyre::eyre!("keyhive post-sync reconciliation timed out")),
                }
            }
            Ok(Err(_)) => Err(eyre::eyre!("caller dropped before response")),
            Err(()) => {
                self.cmd_tx
                    .try_send(Runtime2Cmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id })
                    .map_err(|err| match err {
                        async_channel::TrySendError::Closed(_) => eyre::eyre!(ERROR_ACTOR),
                        async_channel::TrySendError::Full(_) => eyre::eyre!("mailbox full"),
                    })?;
                Err(eyre::eyre!("keyhive sync timed out"))
            }
        }
    }

    pub async fn wait_for_keyhive_reconciliation(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> eyre::Result<()> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::WaitForKeyhiveReconciliation { resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let timeout =
            utils_rs::scale_timeout(timeout.unwrap_or_else(|| std::time::Duration::from_secs(30)));
        match self.race_timeout(rx, timeout).await {
            Ok(Ok(result)) => result.wrap_err("keyhive reconciliation failed"),
            Ok(Err(_)) => Err(eyre::eyre!("caller dropped before response")),
            Err(()) => Err(eyre::eyre!("keyhive reconciliation timed out")),
        }
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
                Ok(Err(_)) => Err(eyre::eyre!("caller dropped before response")),
                Err(()) => Err(eyre::eyre!("quiescence wait timed out")),
            }
        } else {
            rx.await
                .map_err(|_| eyre::eyre!("caller dropped before response"))?
        }
    }

    /// Resume event/command processing after a frozen quiescence wait.
    pub async fn unfreeze(&self) -> eyre::Result<()> {
        self.cmd_tx
            .send(Runtime2Cmd::Unfreeze)
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))
    }

    pub async fn contains_sedimentree_id(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::ContainsSedimentree { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    #[cfg(test)]
    pub(crate) async fn inspect_stored_doc_blobs(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<Vec<Vec<u8>>> {
        let (resp, rx) = futures::channel::oneshot::channel();
        let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.into_bytes());
        self.cmd_tx
            .send(Runtime2Cmd::InspectStoredDocBlobs { sed_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    pub async fn has_local_doc_state(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HasLocalDocState { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    #[cfg(test)]
    pub(crate) async fn has_doc_worker(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::HasDocWorker { doc_id, resp })
            .await
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
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
