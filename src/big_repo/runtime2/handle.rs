//! `Runtime2Handle` — the public API (transport-agnostic, FutureForm-generic).
//!
//! Thin sender over `Runtime2Cmd`. Replaces `BigRepoRuntimeHandle`. The runtime
//! is constructed via [`crate::runtime2::spawn_runtime2`]; this handle is what
//! embedders (and the doc-worker, for self-commands) hold.

use crate::interlude::*;
use crate::runtime2::{messages::Runtime2Cmd, Runtime2Config};
use big_sync_core::PeerId;
use crate::DocumentId;
use future_form::FutureForm;

/// The handle embedders use to drive the runtime. Cloneable (just a channel
/// sender + shared counters). Does NOT own the runtime — see
/// [`crate::runtime2::Runtime2StopToken`].
#[derive(Clone)]
pub struct Runtime2Handle {
    pub(crate) cmd_tx: tokio::sync::mpsc::UnboundedSender<Runtime2Cmd>,
    pub(crate) keyhive: crate::keyhive::BigKeyhiveHandle,
    pub(crate) keyhive_storage: Arc<crate::runtime::BigRepoKeyhiveStorage>,
    pub(crate) sync_policy: crate::runtime::BigRepoSyncPolicy,
    pub(crate) doc_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) keyhive_sync_waiter_ids: Arc<std::sync::atomic::AtomicU64>,
}

impl Runtime2Handle {
    pub fn keyhive(&self) -> &crate::keyhive::BigKeyhiveHandle {
        &self.keyhive
    }
    pub fn sync_policy(&self) -> crate::runtime::BigRepoSyncPolicy {
        self.sync_policy
    }

    // ── doc lifecycle ──────────────────────────────────────────────────────
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
    ) -> eyre::Result<Arc<crate::runtime::LiveDocBundle>> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        let doc_id = todo!("derive DocumentId from keyhive generate_doc (as runtime.rs create_doc does)");
        self.cmd_tx
            .send(Runtime2Cmd::PutDoc { doc_id, initial_content: Box::new(initial_content), resp })
            .map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    pub async fn get_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx.send(Runtime2Cmd::GetDocHandle { doc_id, resp }).map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    pub async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(sedimentree_core::loose_commit::id::CommitId, std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::CommitDelta { doc_id, commits, heads, patches, origin, resp })
            .map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    /// NEW: the walk-derived heads query. Returns sedimentree heads (always)
    /// and materialized heads (None when pending/relay). Backs the test2
    /// Tier-0 flake detector.
    pub async fn doc_head_state(&self, doc_id: DocumentId) -> eyre::Result<crate::runtime::DocHeadState> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx.send(Runtime2Cmd::DocHeadState { doc_id, resp }).map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    // ── connections (transport-agnostic) ──────────────────────────────────
    pub async fn open_connection<F: FutureForm>(
        &self,
        conn: Box<dyn crate::runtime2::Runtime2Conn<F>>,
    ) -> eyre::Result<(PeerId, Arc<std::sync::atomic::AtomicBool>)> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        // NOTE: the hub runs `Sendable`; a `Local` runtime is constructed with
        // `F = Local` and the cmd carries a `Local` conn. The blocking-out uses
        // `Sendable` here for concreteness; the `F`-generic construction is in
        // `spawn_runtime2`.
        self.cmd_tx.send(Runtime2Cmd::OpenConn { conn: todo!("erase conn to the hub's F"), resp }).map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    pub async fn close_connection(&self, peer_id: PeerId) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx.send(Runtime2Cmd::CloseConn { peer_id, resp: Some(resp) }).map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    // ── sync ───────────────────────────────────────────────────────────────
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), crate::runtime::SyncDocError> {
        let waiter_id = crate::runtime2::messages::fresh_waiter_id(&self.doc_sync_waiter_ids);
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncDocWithPeer { doc_id, peer_id, waiter_id, timeout, resp })
            .map_err(|_| crate::runtime::SyncDocError::IoError(ferr!("runtime stopped")))?;
        rx.await.map_err(|_| crate::runtime::SyncDocError::IoError(ferr!("runtime dropped response")))?
    }

    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> eyre::Result<()> {
        let waiter_id = crate::runtime2::messages::fresh_waiter_id(&self.keyhive_sync_waiter_ids);
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Runtime2Cmd::SyncKeyhiveWithPeer { peer_id, waiter_id, resp })
            .map_err(|_| ferr!("runtime stopped"))?;
        // NOTE: `timeout` is enforced in the hub (as today); ignored in the cmd.
        let _ = timeout;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    pub async fn note_local_keyhive_changed(&self) -> eyre::Result<()> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx.send(Runtime2Cmd::NoteLocalKeyhiveChanged { resp }).map_err(|_| ferr!("runtime stopped"))?;
        rx.await.map_err(|_| ferr!("runtime dropped response"))?
    }

    pub async fn doc_payload_heads(&self, doc_id: DocumentId) -> eyre::Result<Option<Arc<[automerge::ChangeHash]>>> {
        // After the heads fix this is ALWAYS sedimentree heads.
        let state = self.doc_head_state(doc_id).await?;
        Ok(state.sedimentree_heads)
    }
}
