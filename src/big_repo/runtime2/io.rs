//! IO seams for runtime2.
//!
//! These traits are where IO is *externalized*. The actor logic (hub, doc-worker)
//! depends on these traits, never on `tokio`/`iroh`/`redb` directly. Backends:
//! - native: [`TokioTaskRuntime`](crate::runtime2::TokioTaskRuntime) + real storage/keyhive.
//! - tests (D2): memory backends + a test task runtime (jitter-free; not step-deterministic).
//! - future (full determinism): a step task runtime + `IoTask`-completing `DocIo`.
//! - wasm: `FutureForm = Local`, wasm-bindgen task runtime, IndexedDB storage.
//!
//! # No big_sync in the runtime
//!
//! runtime2 does **not** touch `big_sync` / `HostPartStore`. The old runtime
//! wrote materialized heads into the big_sync obj payload and read them back
//! for no-op detection — both are gone here:
//! - **writes** are gone: materialized heads are derived (walk+decrypt / live
//!   `get_heads()`), never cached; sedimentree heads live in subduction. This
//!   also dissolves the split-write atomicity bug (there is now a single
//!   `store_commit` call — atomic by construction).
//! - **reads** are gone: no-op detection snapshots the live doc; the pending
//!   baseline reads `sedimentree_heads()`; the first-materialization heuristic
//!   is the `DocWorker2` state machine.
//!
//! big_sync (sync routing, part_store, partition membership) lives in a sibling
//! layer that owns its own sync and watches subduction for new content.

use crate::interlude::*;
use future_form::FutureForm;

// ─── Determinism levers: Timer / Clock ─────────────────────────────────────
//
// Spawning is handled by [`TaskRuntime`](crate::runtime2::TaskRuntime) /
// [`TaskSet`](crate::runtime2::TaskSet), not by a local trait.

/// Runtime-neutral sleep capability. Periodic workers recreate a sleep on
/// each iteration, which also lets deterministic tests advance time explicitly.
pub trait Timer<F: FutureForm>: Send + Sync {
    fn sleep(&self, duration: std::time::Duration) -> F::Future<'static, ()>;
}

/// Replaces `TimestampSeconds::now()` / `Instant::now()`. Injected so tests
/// control time.
pub trait Clock: Send + Sync {
    fn now(&self) -> subduction_core::timestamp::TimestampSeconds;
    fn instant(&self) -> std::time::Instant;
}

// ─── DocIo: the doc-worker's centralized IO surface ────────────────────────
// EVERY IO the doc-worker does goes through this trait — subduction sedimentree
// storage AND keyhive (encrypt/decrypt). Hiding keyhive behind DocIo (rather
// than exposing the keyhive doc handle) keeps the doc-worker free of keyhive's
// deep generics and makes the full IO surface visible in one place.
//
// A future `IoTaskDocIo` impl wraps these in samod-style round-stepping for
// full determinism, without rewriting the actor.

/// Result of a causal decrypt.
///
/// Mirrors `keyhive_core::store::ciphertext::CausalDecryptionState<Vec<u8>, Vec<u8>>`
/// from [`Document::try_causal_decrypt_content`](keyhive_core::principal::document::Document::try_causal_decrypt_content).
/// The doc-worker materializer iterates [`complete`](Self::complete) to collect
/// ancestor plaintexts, then continues the walk.
#[derive(Debug, Clone, Default)]
pub struct CausalDecryptResult {
    /// Successfully-decrypted (content_ref, plaintext) pairs, starting with the
    /// entrypoint ancestor chain. Consumed by the materializer to build the
    /// full automerge doc.
    pub complete: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Result of a transport-level document sync attempt before materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocAttempt {
    Exchanged,
    NotFound,
    Unauthorized,
    Policy(subduction_core::sync_session::SyncPolicyRejectionKind),
}

/// An encrypted loose commit ready for the atomic `store_commit` write.
///
/// Bundles the commit identity (head, parents) and the encrypted blob that
/// subduction's [`store_commit`](subduction_core::subduction::Subduction::store_commit)
/// persists. The [`cgka_update_op`](Self::cgka_update_op) is *metadata*
/// emitted during encryption — the hub routes it to the keyhive event listener;
/// `store_commit` itself operates only on the head/parents/blob.
#[derive(Clone)]
pub struct EncryptedLooseCommit {
    pub head: sedimentree_core::loose_commit::id::CommitId,
    pub parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    pub blob: sedimentree_core::blob::Blob,
    /// CGKA operation emitted by the encrypt (if the key epoch advanced).
    /// Routed to the keyhive event listener by the hub, never stored with the
    /// sedimentree.
    pub cgka_update_op: Option<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
}

/// The result of encrypting the complete initial Automerge state.
///
/// Initial state may already contain sedimentree fragments. Those fragments
/// do not necessarily have a loose-commit row for their head, so they cannot
/// use the incremental `store_fragment` path, which recovers a fragment key by
/// loading that row.
#[derive(Clone)]
pub struct EncryptedInitialSedimentree {
    pub sedimentree: sedimentree_core::sedimentree::Sedimentree,
    pub blobs: Vec<sedimentree_core::blob::Blob>,
    pub cgka_update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
}

/// The doc-worker's IO contract. All methods are `F::Future<'_>` so the same
/// logic runs `Sendable` (native) and `Local` (wasm).
///
/// # Single atomic write
/// [`DocIo::store_commit`] is the *only* local write. subduction records the
/// commit and advances the sedimentree frontier in one call — there is no
/// separate "set heads" step (the old split write was the atomicity bug; it is
/// gone because heads are derived, never cached).
///
/// # Parities with the old runtime
///
/// These methods mirror the subduction + keyhive calls the old runtime made
/// directly in `runtime.rs::DocWorker::handle_commit_delta` and the
/// materialization paths. The `EncryptedLooseCommit` bundles the (head,
/// parents, blob) tuple that `subduction.store_commit(id, head, parents, blob)`
/// accepts. The encrypt/decrypt methods wrap `keyhive.try_encrypt_content_keyed`,
/// `Document::try_decrypt_content_keyed`, and
/// `Document::try_causal_decrypt_content`.
pub trait DocIo<F: FutureForm>: Send + Sync {
    // ── sedimentree frontier (derivable; never cached) ────────────────────
    /// Cheap sedimentree frontier via subduction's
    /// [`heads_or_hydrate`](subduction_core::subduction::ingest::heads_or_hydrate)
    /// or the storage trait directly. Resident fast path; hydrates on miss.
    /// Replaces the old `partition_doc_heads_payload` read.
    fn sedimentree_heads(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<Vec<sedimentree_core::loose_commit::id::CommitId>>>;

    /// Hydrate the full minimized tree (for materialization / decrypt walks).
    /// Returns `None` if the tree has no stored commits or fragments.
    /// The returned [`MinimizedSedimentree`] provides
    /// [`heads`](sedimentree_core::sedimentree::Sedimentree::heads) and
    /// commit/fragment iteration for the decrypt walk.
    fn hydrate_tree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<
        '_,
        eyre::Result<Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>>,
    >;

    // ── initial state ─────────────────────────────────────────────────────
    /// Encrypt and assemble the complete initial sedimentree in one batch.
    /// This is distinct from incremental commit encryption because an initial
    /// fragment can have no corresponding loose-commit row yet.
    fn encrypt_initial_sedimentree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        staged: crate::runtime::StagedAutomergeIngest,
    ) -> F::Future<'_, eyre::Result<EncryptedInitialSedimentree>>;

    /// Persist the encrypted initial sedimentree durably.
    fn store_initial_sedimentree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        initial: EncryptedInitialSedimentree,
    ) -> F::Future<'_, eyre::Result<()>>;

    // ── the single local write (atomic) ───────────────────────────────────
    /// Store an encrypted loose commit. subduction records it AND advances the
    /// sedimentree frontier in one call. Returns any fragment-boundary request
    /// (as subduction's [`store_commit`](subduction_core::subduction::Subduction::store_commit)
    /// does — `Some(FragmentRequested)` when the commit sits at a fragment
    /// boundary depth).
    fn store_commit(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        commit: EncryptedLooseCommit,
    ) -> F::Future<'_, eyre::Result<Option<subduction_core::subduction::request::FragmentRequested>>>;

    /// Store a raw fragment bundle at a boundary commit.
    /// The implementation encrypts the bundle and constructs the persisted
    /// fragment metadata from the encrypted blob.
    fn store_fragment(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        head: sedimentree_core::loose_commit::id::CommitId,
        boundary: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
        checkpoints: Vec<sedimentree_core::loose_commit::id::CommitId>,
        raw_blob: Vec<u8>,
    ) -> F::Future<'_, eyre::Result<()>>;

    /// Refresh keyhive caches after local encryption advances its frontier.
    fn note_local_keyhive_changed(&self) -> F::Future<'_, eyre::Result<()>>;

    // ── keyhive encrypt/decrypt (hides the keyhive doc handle) ────────────
    /// Encrypt a loose commit's blob under the current keyhive epoch. Returns
    /// the encrypted blob + any CGKA op emitted. Mirrors
    /// [`Keyhive::try_encrypt_content_keyed`](keyhive_core::keyhive::Keyhive::try_encrypt_content_keyed)
    /// but takes the sedimentree identity directly (the impl looks up the
    /// keyhive document internally).
    fn encrypt_loose_commit(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        head: sedimentree_core::loose_commit::id::CommitId,
        parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
        blob: Vec<u8>,
    ) -> F::Future<'_, eyre::Result<EncryptedLooseCommit>>;

    /// Try to decrypt the blob at `locator`. Returns `None` if the key is not
    /// available (materialization pending). Mirrors
    /// [`Document::try_decrypt_content_keyed`](keyhive_core::principal::document::Document::try_decrypt_content_keyed)
    /// but abstracts loading the ciphertext from storage and looking up the
    /// keyhive document.
    fn try_decrypt_content_keyed(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<Option<Vec<u8>>>>;

    /// Causal decrypt: decrypt `locator` + any ancestors whose keys are now
    /// reachable. Mirrors
    /// [`Document::try_causal_decrypt_content`](keyhive_core::principal::document::Document::try_causal_decrypt_content).
    /// The returned [`CausalDecryptResult::complete`] includes the entrypoint
    /// plus any ancestors decrypted along the causal chain.
    /// Persist a CGKA update operation emitted during encryption.
    ///
    /// Mirrors `keyhive_storage::persist_cgka_update_op`. Extracted as a
    /// `DocIo` method so the doc-worker doesn't hold concrete keyhive storage.
    fn persist_cgka_update_op(
        &self,
        op: keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
    ) -> F::Future<'_, eyre::Result<()>>;

    fn try_causal_decrypt(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<CausalDecryptResult>>;
}

// ─── RuntimeIo: hub-level IO contract ──────────────────────────────────────

/// Hub-level IO that runtime2 currently hardcodes in the hub/handle.
///
/// Every method returns `F::Future` so the same logic works for `Sendable`
/// (native) and `Local` (wasm). No concrete keyhive protocol, storage, spawn,
/// or transport types appear in this trait.
pub trait RuntimeIo<F: FutureForm>: Send + Sync {
    /// Create a new keyhive document with the given parents and content heads.
    /// Returns the generated [`DocumentId`].
    fn create_document(
        &self,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
    ) -> F::Future<'_, eyre::Result<crate::DocumentId>>;

    /// Check whether the sedimentree for `sed_id` is resident in storage.
    fn contains_sedimentree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<bool>>;

    /// Test-support inspection of the raw stored blobs for a sedimentree.
    fn inspect_stored_doc_blobs(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<Vec<Vec<u8>>>>;

    /// Notify the runtime that the local keyhive state has changed.
    fn note_local_keyhive_changed(&self) -> F::Future<'_, eyre::Result<()>>;

    /// Refresh BigRepo's sync membership and partition indexes from Keyhive
    /// for a changed target.
    fn refresh_big_sync_doc_access(
        &self,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> F::Future<'_, eyre::Result<()>>;

    /// Classify a membership target without exposing Keyhive types to callers.
    fn is_document_membership_target(
        &self,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> F::Future<'_, eyre::Result<bool>>;

    /// Initiate a keyhive sync round with a peer.
    fn sync_keyhive_with_peer(
        &self,
        peer_id: big_sync_core::PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> F::Future<'_, eyre::Result<()>>;

    /// Refresh the keyhive cache (periodic maintenance).
    fn refresh_keyhive_cache(&self) -> F::Future<'_, eyre::Result<()>>;

    /// Compact the keyhive archive (periodic maintenance).
    fn compact_keyhive(&self) -> F::Future<'_, eyre::Result<()>>;

    /// Run a doc sync round with `peer_id` for the given sedimentree.
    /// Returns `true` if the sync exchange had success (commits/fragments
    /// were received or sent), `false` if the sync completed without
    /// meaningful exchange (no new content).
    fn sync_doc_with_peer(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        peer_id: big_sync_core::PeerId,
    ) -> F::Future<'_, eyre::Result<SyncDocAttempt>>;
}
