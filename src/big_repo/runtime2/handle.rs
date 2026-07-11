//! `Runtime2Handle` — the public API handle.
//!
//! Thin sender over [`Runtime2Cmd`]. Replaces
//! [`BigRepoRuntimeHandle`](crate::runtime::BigRepoRuntimeHandle). Every public
//! method mirrors a method on the old handle (`runtime.rs:380`) or is a new
//! heads-fix method (`doc_head_state`, `doc_payload_heads`).
//!
//! # Generics
//!
//! Unlike [`Runtime2Config`] and the hub, `Runtime2Handle` is **not** generic
//! over `F: FutureForm` — it only holds channel senders + `Arc` counters +
//! accessor handles (`BigKeyhiveHandle`, `BigRepoSyncPolicy`), none of which
//! depend on the async runtime flavour. The hub's `F` is erased at
//! construction.
//!
//! [`Runtime2Cmd`]: super::Runtime2Cmd

use crate::interlude::*;
use crate::runtime2::messages::{Runtime2Cmd, fresh_waiter_id};
use big_sync_core::PeerId;
use crate::DocumentId;

/// The handle embedders use to drive the runtime.
///
/// Cloneable (just a channel sender + shared counters). Does **not** own the
/// runtime — see [`crate::runtime2::Runtime2StopToken`].
///
/// Mirrors `BigRepoRuntimeHandle` at `runtime.rs:365` which has the same
/// `cmd_tx`, `keyhive`, `keyhive_storage`, `sync_policy`, and waiter-id
/// counters.
#[derive(Clone)]
pub struct Runtime2Handle {
    pub(crate) cmd_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Cmd>,
    pub(crate) keyhive: crate::keyhive::BigKeyhiveHandle,
    pub(crate) keyhive_storage: std::sync::Arc<crate::runtime::BigRepoKeyhiveStorage>,
    pub(crate) sync_policy: crate::runtime::BigRepoSyncPolicy,
    pub(crate) doc_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Runtime2Handle {
    /// Construct a new handle. Called by `spawn_runtime2` in the hub; the hub
    /// passes its own cmd channel sender, keyhive handle, waiter counters, etc.
    ///
    /// Mirrors `BigRepoRuntimeHandle` construction at `runtime.rs:1044` which
    /// takes the same constituents from `spawn_big_repo_runtime`.
    pub(crate) fn new(
        cmd_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Cmd>,
        keyhive: crate::keyhive::BigKeyhiveHandle,
        keyhive_storage: std::sync::Arc<crate::runtime::BigRepoKeyhiveStorage>,
        sync_policy: crate::runtime::BigRepoSyncPolicy,
    ) -> Self {
        Self {
            cmd_tx,
            keyhive,
            keyhive_storage,
            sync_policy,
            doc_sync_waiter_ids: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            keyhive_sync_waiter_ids: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    // ── accessors (mirror BigRepoRuntimeHandle fields) ──────────────────────

    /// The keyhive handle used for access control, CGKA, and encryption.
    /// Mirrors `BigRepoRuntimeHandle::keyhive` (`runtime.rs:367`).
    pub fn keyhive(&self) -> &crate::keyhive::BigKeyhiveHandle {
        &self.keyhive
    }

    /// The sync policy (timeouts, TTLs). Mirrors
    /// `BigRepoRuntimeHandle::sync_policy` (`runtime.rs:373`).
    pub fn sync_policy(&self) -> crate::runtime::BigRepoSyncPolicy {
        self.sync_policy
    }

    // ── doc lifecycle ──────────────────────────────────────────────────────

    /// Create a new document with `initial_content` and the given keyhive
    /// `parents` (co-creators). Mirrors
    /// [`BigRepoRuntimeHandle::create_doc`](crate::runtime::BigRepoRuntimeHandle::create_doc)
    /// at `runtime.rs:381`.
    ///
    /// The doc `DocumentId` is derived from a keyhive [`generate_doc`] call
    /// before sending the `PutDoc` command to the hub.
    ///
    /// [`generate_doc`]: keyhive_core::keyhive::Keyhive::generate_doc
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
    ) -> eyre::Result<std::sync::Arc<crate::runtime::LiveDocBundle>> {
        use nonempty::NonEmpty;
        let heads = initial_content.get_heads();
        let content_heads = NonEmpty::from_vec(heads.iter().map(|h| h.0).collect())
            .ok_or_else(|| eyre::eyre!("automerge doc has no heads"))?;
        let doc_id = self
            .keyhive
            .create_doc(parents, content_heads, &self.keyhive_storage)
            .await?;
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::PutDoc {
                doc_id,
                initial_content: Box::new(initial_content),
                resp,
            })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await
            .map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Get or spawn a live handle for an existing document.
    ///
    /// Mirrors `BigRepoRuntimeHandle::get_doc_handle` at `runtime.rs:405`.
    /// Returns [`DocLookup::Ready`] with a live automerge bundle,
    /// [`DocLookup::PendingMaterialization`] if the doc exists but is not yet
    /// decryptable, or [`DocLookup::Missing`] if unknown.
    ///
    /// [`DocLookup::Ready`]: crate::runtime::DocLookup::Ready
    /// [`DocLookup::PendingMaterialization`]: crate::runtime::DocLookup::PendingMaterialization
    /// [`DocLookup::Missing`]: crate::runtime::DocLookup::Missing
    pub async fn get_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime::DocLookup<std::sync::Arc<crate::runtime::LiveDocBundle>>> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::GetDocHandle { doc_id, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Commit a delta (sets of encrypted commits) to a document.
    ///
    /// Mirrors `BigRepoRuntimeHandle::commit_delta` at `runtime.rs:444`.
    /// Each commit is a triple (head, parents, blob); the runtime encrypts
    /// and persists it atomically via the [`DocIo::store_commit`] seam.
    ///
    /// [`DocIo::store_commit`]: super::DocIo::store_commit
    pub async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp,
            })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// NEW: query walk-derived head state for a document.
    ///
    /// Returns both the sedimentree heads (storage ground truth, always
    /// present if the doc is known) and the materialized automerge heads
    /// (`None` when pending or relay-only). Backs the test2 Tier-0 flake
    /// detector. This operation is new — no equivalent in the old
    /// `BigRepoRuntimeHandle`.
    pub async fn doc_head_state(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime2::DocHeadState> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::DocHeadState { doc_id, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Convenience: query only the sedimentree (payload) heads.
    ///
    /// After the heads fix this always returns `Some(sedimentree_heads)` for
    /// a known doc, never the old "materialized heads or sedimentree heads?"
    /// ambiguity. Returns `None` only if the doc is unknown to the runtime.
    ///
    /// Replaces `BigRepo::doc_payload_heads` (`lib.rs:658`) which returned
    /// the overloaded `obj_payload.heads` field that caused the
    /// head-divergence flake.
    pub async fn doc_payload_heads(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<Option<std::sync::Arc<[automerge::ChangeHash]>>> {
        let state = self.doc_head_state(doc_id).await?;
        match state.state {
            crate::runtime2::MaterializationState::Missing => Ok(None),
            _ => Ok(Some(state.sedimentree_heads)),
        }
    }

    // ── connections (transport-agnostic) ──────────────────────────────────

    /// Open an outbound connection to `peer` at a transport-specific `addr`.
    ///
    /// The `addr` is an opaque `Box<dyn Any + Send>` that the hub's
    /// [`TransportConnect`](super::TransportConnect) implementation
    /// interprets. Mirrors
    /// [`BigRepoRuntimeHandle::open_connection_iroh`](crate::runtime::BigRepoRuntimeHandle::open_connection_iroh)
    /// at `runtime.rs:467` but de-iroh'd — takes an abstract addr instead of
    /// `iroh::Endpoint` + `iroh::EndpointAddr`.
    pub async fn open_connection(
        &self,
        peer: PeerId,
        addr: Box<dyn std::any::Any + Send>,
    ) -> eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::OpenConn { peer, addr, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Accept an inbound connection from the transport layer.
    ///
    /// `incoming` is an opaque handle the hub's
    /// [`TransportConnect`](super::TransportConnect) implementation
    /// uses to complete the handshake. Mirrors
    /// [`BigRepoRuntimeHandle::accept_connection_iroh`](crate::runtime::BigRepoRuntimeHandle::accept_connection_iroh)
    /// at `runtime.rs:488` but de-iroh'd.
    pub async fn accept_connection(
        &self,
        incoming: Box<dyn std::any::Any + Send>,
    ) -> eyre::Result<(PeerId, std::sync::Arc<std::sync::atomic::AtomicBool>)> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::AcceptConn { incoming, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    /// Close an established peer connection.
    ///
    /// Mirrors `BigRepoRuntimeHandle::close_peer_connection` at `runtime.rs:507`.
    pub async fn close_connection(&self, peer_id: PeerId) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CloseConn {
                peer_id,
                resp: Some(resp),
            })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    // ── sync ───────────────────────────────────────────────────────────────

    /// Sync a document's sedimentree with a peer. Waits for completion or
    /// `timeout`.
    ///
    /// Mirrors `BigRepoRuntimeHandle::sync_doc_with_peer` at `runtime.rs:518`.
    /// The old handle applies a `tokio::time::timeout` at the handle level;
    /// runtime2 does the same (keeping the timeout-driven cancellation path
    /// for parity).
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), crate::runtime::SyncDocError> {
        let waiter_id = fresh_waiter_id(&self.doc_sync_waiter_ids);
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp,
            })
            .map_err(|_| crate::runtime::SyncDocError::IoError(eyre::eyre!("task was found dead")))?;
        // If no timeout, wait indefinitely (the old handle returns
        // immediately without timeout).
        let Some(timeout) = timeout else {
            return rx.await.map_err(|_| {
                crate::runtime::SyncDocError::IoError(eyre::eyre!("caller dropped before response"))
            })?;
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(crate::runtime::SyncDocError::IoError(eyre::eyre!(
                "caller dropped before response"
            ))),
            Err(_) => {
                // Send cancellation to the hub so it cleans up the waiter.
                let _ = self.cmd_tx.send(Runtime2Cmd::CancelDocSyncWaiter {
                    doc_id,
                    peer_id,
                    waiter_id,
                });
                Err(crate::runtime::SyncDocError::IoError(eyre::eyre!(
                    "doc sync timed out"
                )))
            },
        }
    }

    /// Sync keyhive state with a peer. Waits for completion or `timeout`.
    ///
    /// Mirrors `BigRepoRuntimeHandle::sync_keyhive_with_peer` at `runtime.rs:578`.
    /// The old handle applies a `tokio::time::timeout` (defaulting to 5s);
    /// runtime2 does the same.
    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> eyre::Result<()> {
        let waiter_id = fresh_waiter_id(&self.keyhive_sync_waiter_ids);
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        let timeout = timeout.unwrap_or_else(|| {
            utils_rs::scale_timeout(std::time::Duration::from_secs(5))
        });
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result.wrap_err("keyhive sync failed"),
            Ok(Err(_)) => Err(eyre::eyre!("caller dropped before response")),
            Err(_) => {
                let _ = self.cmd_tx.send(Runtime2Cmd::CancelKeyhiveSyncWaiter {
                    peer_id,
                    waiter_id,
                });
                Err(eyre::eyre!("keyhive sync timed out"))
            },
        }
    }

    /// Notify the runtime that the local keyhive state has changed (e.g. a
    /// delegation or membership update was received out-of-band).
    ///
    /// Mirrors `BigRepoRuntimeHandle::note_local_keyhive_changed` at
    /// `runtime.rs:415`.
    pub async fn note_local_keyhive_changed(&self) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::NoteLocalKeyhiveChanged { resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))?
    }

    // ── presence / introspection ───────────────────────────────────────────

    /// Check whether the sedimentree for `doc_id` is resident in storage.
    ///
    /// This is the authoritative presence check for the fetch gate: a doc
    /// that was never pulled subduction-side exists as a marker only. Mirrors
    /// `BigRepoRuntimeHandle::contains_sedimentree_id` at `runtime.rs:425`.
    pub async fn contains_sedimentree_id(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CheckSedimentreeResident { doc_id, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))
    }

    /// Check whether a doc-worker is currently alive for `doc_id`.
    ///
    /// Returns `true` if a worker is running (handles ongoing sync, commits,
    /// materialization). Mirrors `BigRepoRuntimeHandle::has_doc_worker` at
    /// `runtime.rs:435`.
    pub async fn has_doc_worker(&self, doc_id: DocumentId) -> eyre::Result<bool> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CheckDocWorkerExists { doc_id, resp })
            .map_err(|_| eyre::eyre!("task was found dead"))?;
        rx.await.map_err(|_| eyre::eyre!("caller dropped before response"))
    }
}
