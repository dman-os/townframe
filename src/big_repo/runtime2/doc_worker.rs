//! `DocWorker2` — the per-doc actor.
//!
//! Replaces the `DocWorker` mega-methods. Each former mega-method is broken
//! into small steps with an explicit IO contract via [`DocIo`], so the IO
//! needs are visible at the type level (the point of the blocking-out).
//!
//! Spawning is a top-standing free function [`spawn_doc_worker`] (not a method
//! on the hub), mirroring `spawn_doc_worker` today — the hub *calls* it.

use crate::interlude::*;
use crate::runtime2::{DocIo, DocWorkerHandle, DocWorkerInternalLease, DocWorkerStopToken, Runtime2Handle, messages::DocWorkerMsg};
use big_sync_core::PeerId;
use crate::DocumentId;
use future_form::FutureForm;
use std::sync::Arc;

/// The per-doc actor state. Generic over `F: FutureForm`.
pub struct DocWorker2<F: FutureForm> {
    pub(crate) doc_id: DocumentId,
    pub(crate) sed_id: sedimentree_core::id::SedimentreeId,
    pub(crate) state: DocState,
    /// The centralized IO surface — every IO goes through this.
    pub(crate) io: Arc<dyn DocIo<F>>,
    pub(crate) keyhive: crate::keyhive::BigKeyhiveHandle,
    pub(crate) change_manager: Arc<crate::changes::ChangeListenerManager>,
    pub(crate) runtime_handle: Runtime2Handle,
    pub(crate) msg_tx: tokio::sync::mpsc::UnboundedSender<DocWorkerMsg>,
    pub(crate) last_notified_heads: Option<Arc<[automerge::ChangeHash]>>,
    /// Fragment-boundary commits awaiting `add_fragment` (as today).
    pub(crate) pending_fragment_requests: std::collections::BTreeSet<crate::runtime::FragmentRequested>,
    /// In-flight sync jobs (peer → waiters + leases).
    pub(crate) pending_sync_jobs: HashMap<PeerId, Vec<(u64, tokio::sync::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>, DocWorkerInternalLease)>>,
    pub(crate) active_doc_syncs: HashMap<PeerId, u64>,
}

/// The doc's live state. `Unloaded` ⇒ relay/pending (sedimentree heads only,
/// no automerge). `Transient`/`Live` ⇒ materialized.
pub enum DocState {
    Unloaded,
    Transient(Box<automerge::Automerge>),
    Live(std::sync::Weak<crate::runtime::LiveDocBundle>),
    PendingMaterialization,
}

impl<F: FutureForm> DocWorker2<F> {
    // ═══════════════════════════════════════════════════════════════════════
    // MESSAGE LOOP
    // ═══════════════════════════════════════════════════════════════════════
    pub(crate) async fn handle_msg(&mut self, msg: DocWorkerMsg) -> eyre::Result<()> {
        match msg {
            DocWorkerMsg::PutDoc { initial_content, resp } => self.put_doc(initial_content, resp).await,
            DocWorkerMsg::AcquireHandle { resp } => self.acquire_handle(resp).await,
            DocWorkerMsg::CommitDelta { commits, heads, patches, origin, resp, _lease } => {
                self.commit_delta(commits, heads, patches, origin, resp).await
            }
            DocWorkerMsg::ApplySyncSession { session, _lease } => self.apply_sync_session(session).await,
            DocWorkerMsg::SyncWithPeer { peer_id, waiter_id, timeout, done, _lease } => {
                self.sync_with_peer(peer_id, waiter_id, timeout, done).await
            }
            DocWorkerMsg::CancelSyncWithPeer { peer_id, waiter_id, reason } => {
                let _ = (peer_id, waiter_id, reason);
                todo!("drop matching waiter(s) with cancelled error")
            }
            DocWorkerMsg::SyncWithPeerResult { peer_id, waiter_id, result } => {
                let _ = (peer_id, waiter_id, result);
                todo!("resolve the waiter; clear active_doc_syncs entry")
            }
            DocWorkerMsg::ReleaseHandleLease => {
                todo!("the hub owns refcounts now; this is a no-op or a re-eval trigger")
            }
            DocWorkerMsg::ReattemptMaterialization => self.retry_materialization().await,
            DocWorkerMsg::QueryHeadState { resp } => self.query_head_state(resp).await,
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PUT_DOC  (was part of handle_put_doc)
    // ═══════════════════════════════════════════════════════════════════════
    async fn put_doc(
        &mut self,
        initial_content: automerge::Automerge,
        resp: tokio::sync::oneshot::Sender<eyre::Result<Arc<crate::runtime::LiveDocBundle>>>,
    ) -> eyre::Result<()> {
        let _ = (initial_content, resp);
        todo! {
            "1. encrypt initial content (keyhive generate_doc + initial frontier)\n\
             2. io.store_commit_and_advance_heads (UNIFIED TXN — atomic)\n\
             3. build LiveDocBundle, transition to Live\n\
             4. notify_doc_created via change_manager"
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ACQUIRE_HANDLE  (was handle_acquire_handle)
    // IO: hydrate_tree (if Unloaded) + decrypt walk → materialize
    // ═══════════════════════════════════════════════════════════════════════
    async fn acquire_handle(
        &mut self,
        resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocLookup<Arc<crate::runtime::LiveDocBundle>>>>,
    ) -> eyre::Result<()> {
        let _ = resp;
        todo! {
            "match state:\n\
             - Live(bundle)     ⇒ Ready(upgrade)\n\
             - Transient(doc)   ⇒ build LiveDocBundle, Ready\n\
             - Unloaded/Pending ⇒ load_doc_snapshot (hydrate_tree + decrypt_walk);\n\
                                   Ready if fully decryptable, else PendingMaterialization"
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // COMMIT_DELTA  (was handle_commit_delta) — BROKEN INTO 3 STEPS
    // ═══════════════════════════════════════════════════════════════════════
    async fn commit_delta(
        &mut self,
        commits: Vec<(sedimentree_core::loose_commit::id::CommitId, std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
        resp: tokio::sync::oneshot::Sender<eyre::Result<()>>,
    ) -> eyre::Result<()> {
        let encrypted = self.encrypt_commits(&commits).await?;
        // THE UNIFIED ATOMIC WRITE (plan B): store_commit + advance heads in one txn.
        self.persist_commits_and_heads(encrypted, &heads).await?;
        self.notify_heads_changed(heads, patches, origin).await?;
        let _ = resp;
        todo!("resolve resp after notify; process pending_fragment_requests; note_local_keyhive_changed if keyhive changed")
    }

    /// Step 1: encrypt each loose commit (+ collect CGKA update ops).
    /// IO: keyhive try_encrypt_content_keyed.
    async fn encrypt_commits(
        &self,
        commits: &[(sedimentree_core::loose_commit::id::CommitId, std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>, Vec<u8>)],
    ) -> eyre::Result<Vec<EncryptedCommit>> {
        let _ = commits;
        todo!("encrypt_loose_commit_with_update_op per commit (as today); collect update_ops")
    }

    /// Step 2: the atomic local write. IO: DocIo::store_commit_and_advance_heads
    /// (one SQL txn: subduction store_commit + part_store set_obj_payload +
    /// bucket-summary update). This is the crash-resistance fix.
    async fn persist_commits_and_heads(
        &self,
        encrypted: Vec<EncryptedCommit>,
        heads: &[automerge::ChangeHash],
    ) -> eyre::Result<()> {
        let _ = (encrypted, heads);
        todo!("for each encrypted commit: io.store_commit_and_advance_heads(sed_id, head, parents, blob, sedimentree_heads). sedimentree_heads derived via io.sedimentree_heads (cheap). Fragment-boundary commits → io.store_fragment_and_advance_heads.")
    }

    /// Step 3: notify. IO: DocIo::notify_heads_changed.
    async fn notify_heads_changed(
        &mut self,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: crate::changes::BigRepoChangeOrigin,
    ) -> eyre::Result<()> {
        let _ = (heads, patches, origin);
        todo!("commit_delta_bookkeep equivalent: io.notify_heads_changed; update last_notified_heads; emit patches")
    }

    // ═══════════════════════════════════════════════════════════════════════
    // APPLY_SYNC_SESSION  (was the ~500-line handle_apply_sync_session) — BROKEN APART
    // ═══════════════════════════════════════════════════════════════════════
    async fn apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> eyre::Result<()> {
        let peer_id = PeerId::new(*session.peer_id.as_bytes());
        if session.received_commit_ids.is_empty() && session.received_fragment_ids.is_empty() {
            // Noop path. NOTE the fix (current_fixes §2): even on noop, refresh
            // part_store heads from the sedimentree so a prior crash can't leave
            // them stale. Today this returns without refreshing — the bug.
            return self.refresh_heads_from_sedimentree(peer_id).await;
        }
        let tree = self.hydrate_tree(self.sed_id).await?;
        let Some(tree) = tree else {
            return self.record_pending_heads(&tree, peer_id).await;
        };
        // Fast path: no change-listener interest + Unloaded ⇒ record sedimentree
        // heads only (relay mode). Correct by construction in runtime2.
        if self.should_fast_path_pending() {
            return self.record_pending_heads(&tree, peer_id).await;
        }
        let blobs = self.try_decrypt_session_blobs(&tree, &session).await?;
        if blobs.is_none() {
            // Decryption stalled ⇒ pending. Record sedimentree heads, mark pending.
            self.record_pending_heads(&tree, peer_id).await?;
            return self.transition_to_pending().await;
        }
        self.materialize_from_blobs(&tree, blobs.unwrap(), peer_id).await
    }

    /// IO: DocIo::hydrate_tree.
    async fn hydrate_tree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> eyre::Result<Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>> {
        self.io.hydrate_tree(sed_id).await
    }

    /// The causal-decrypt loop. IO: DocIo::keyhive_doc + load_ciphertext.
    /// Returns None if decryption stalls (materialization pending).
    async fn try_decrypt_session_blobs(
        &self,
        tree: &sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        session: &subduction_core::sync_session::SyncSession,
    ) -> eyre::Result<Option<Vec<Vec<u8>>>> {
        let _ = (tree, session);
        todo!("the try_decrypt_content_keyed + try_causal_decrypt_content loop from handle_apply_sync_session; None ⇒ stalled")
    }

    /// Apply decrypted blobs to automerge; transition to Transient/Live.
    /// IO: DocIo::set_doc_sedimentree_heads (now automerge heads, post-materialize)
    /// + notify_heads_changed. Pure-ish (load_incremental).
    async fn materialize_from_blobs(
        &mut self,
        tree: &sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        blobs: Vec<Vec<u8>>,
        peer_id: PeerId,
    ) -> eyre::Result<()> {
        let _ = (tree, blobs, peer_id);
        todo!("load_incremental each blob; diff for patches; transition_to_ready; set heads + notify")
    }

    /// The relay/pending path: record sedimentree heads, no automerge touch.
    /// IO: DocIo::set_doc_sedimentree_heads + notify_pending_heads_changed.
    async fn record_pending_heads(
        &mut self,
        tree: &Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>,
        peer_id: PeerId,
    ) -> eyre::Result<()> {
        let _ = (tree, peer_id);
        todo!("io.set_doc_sedimentree_heads(sedimentree_heads); io.notify_pending_heads_changed")
    }

    /// Refresh part_store heads from the sedimentree (the noop-after-crash fix).
    /// IO: DocIo::sedimentree_heads + set_doc_sedimentree_heads.
    async fn refresh_heads_from_sedimentree(&mut self, peer_id: PeerId) -> eyre::Result<()> {
        let _ = peer_id;
        todo!("io.sedimentree_heads(sed_id); if differs from io.doc_payload_heads, io.set_doc_sedimentree_heads")
    }

    async fn transition_to_pending(&mut self) -> eyre::Result<()> {
        self.state = DocState::PendingMaterialization;
        todo!("emit DocWorkerMaterializationPending evt")
    }

    async fn transition_to_ready(&mut self, heads: Arc<[automerge::ChangeHash]>) -> eyre::Result<()> {
        let _ = heads;
        todo!("if was pending: emit DocWorkerMaterializationReady")
    }

    fn should_fast_path_pending(&self) -> bool {
        todo!("!change_manager.has_change_listener_interest(doc_id, Remote) && matches!(state, Unloaded|Pending)")
    }

    // ═══════════════════════════════════════════════════════════════════════
    // RETRY MATERIALIZATION  (was retry_pending_materialization)
    // Called after keyhive sync delivers new CGKA keys.
    // IO: hydrate_tree + decrypt walk.
    // ═══════════════════════════════════════════════════════════════════════
    async fn retry_materialization(&mut self) -> eyre::Result<()> {
        todo!("load_doc_snapshot (hydrate + decrypt); Ready ⇒ transition_to_ready + commit_delta_bookkeep(Bootstrap); Pending ⇒ stay pending")
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SYNC_WITH_PEER  (was handle_sync_with_peer)
    // IO: subduction sync_with_peer / full_sync (via the runtime handle's
    // subduction, not DocIo — sync is orchestration, not storage).
    // ═══════════════════════════════════════════════════════════════════════
    async fn sync_with_peer(
        &mut self,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<std::time::Duration>,
        done: tokio::sync::oneshot::Sender<Result<(), crate::runtime::SyncDocError>>,
    ) -> eyre::Result<()> {
        let _ = (peer_id, waiter_id, timeout, done);
        todo!("drive subduction sync for (doc_id, peer); track waiter; resolve on completion/timeout")
    }

    // ═══════════════════════════════════════════════════════════════════════
    // QUERY_HEAD_STATE  — NEW (the flake-detector op)
    // IO: DocIo::sedimentree_heads + the live doc's automerge heads (if Live).
    // ═══════════════════════════════════════════════════════════════════════
    async fn query_head_state(
        &self,
        resp: tokio::sync::oneshot::Sender<eyre::Result<crate::runtime::DocHeadState>>,
    ) -> eyre::Result<()> {
        let _ = resp;
        todo!("sedimentree_heads = io.sedimentree_heads(sed_id); materialized_heads = match state { Live/Transient ⇒ Some(doc.get_heads()), _ ⇒ None }; state = (Pending/Unloaded/Materialized)")
    }
}

/// An encrypted loose commit + its CGKA update op (from `encrypt_commits`).
pub(crate) struct EncryptedCommit {
    pub head: sedimentree_core::loose_commit::id::CommitId,
    pub parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    pub blob: sedimentree_core::blob::Blob,
    pub cgka_update_op: Option<crate::runtime::SignedCgkaOp>,
}

// ─── top-standing spawn ────────────────────────────────────────────────────

/// Spawn a doc-worker. Top-standing free function (NOT a method on the hub) —
/// the hub calls this when ensuring a worker. Mirrors today's `spawn_doc_worker`
/// but lifted out so the doc-worker module owns its own lifecycle.
///
/// Returns the mailbox handle + stop token. The worker runs on `runtime_tasks`
/// (via the hub's spawner) and emits `Runtime2Evt` (DocWorkerStopped /
/// FatalWorkerError) when it exits.
#[allow(clippy::too_many_arguments)]
pub fn spawn_doc_worker<F: FutureForm>(
    doc_id: DocumentId,
    io: Arc<dyn DocIo<F>>,
    keyhive: crate::keyhive::BigKeyhiveHandle,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    runtime_handle: Runtime2Handle,
    runtime_tasks: Arc<dyn crate::runtime2::Spawner<F>>,
    runtime_evt_tx: tokio::sync::mpsc::UnboundedSender<crate::runtime2::Runtime2Evt>,
    cancel: tokio_util::sync::CancellationToken,
) -> (DocWorkerHandle, DocWorkerStopToken) {
    let _ = (io, keyhive, change_manager, runtime_handle, runtime_tasks, runtime_evt_tx, cancel);
    todo! {
        "1. build the mailbox (mpsc::unbounded)\n\
         2. construct DocWorker2 ( state: Unloaded, ... )\n\
         3. spawn the loop: cancel.run_until_cancelled(async ( while let Some(msg) = rx.recv().await { worker.handle_msg(msg).await? ) })\n\
         4. on exit: emit DocWorkerStopped (+ FatalWorkerError on Err, per AGENTS.md don't swallow)\n\
         5. return (DocWorkerHandle{msg_tx}, DocWorkerStopToken{cancel})"
    }
}
