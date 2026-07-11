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

/// Replaces `tokio::time::interval` / `tokio::time::sleep`. The runtime's
/// periodic workers (keyhive refresh/compact, janitor) go through this so a
/// step-`Timer` can drive them deterministically.
pub trait Timer<F: FutureForm>: Send + Sync {
    /// A periodic tick; the determinism boundary for maintenance loops.
    fn tick(&self, period: std::time::Duration) -> F::Future<'static, ()>;
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

/// An encrypted loose commit ready for the atomic `store_commit` write.
///
/// Bundles the commit identity (head, parents) and the encrypted blob that
/// subduction's [`store_commit`](subduction_core::subduction::Subduction::store_commit)
/// persists. The [`cgka_update_op`](Self::cgka_update_op) is *metadata*
/// emitted during encryption — the hub routes it to the keyhive event listener;
/// `store_commit` itself operates only on the head/parents/blob.
pub struct EncryptedLooseCommit {
    pub head: sedimentree_core::loose_commit::id::CommitId,
    pub parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    pub blob: sedimentree_core::blob::Blob,
    /// CGKA operation emitted by the encrypt (if the key epoch advanced).
    /// Routed to the keyhive event listener by the hub, never stored with the
    /// sedimentree.
    pub cgka_update_op: Option<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
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
    ) -> F::Future<'_, eyre::Result<Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>>>;

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

    /// Store a fragment (companion to `store_commit` for boundary commits).
    /// Mirrors subduction's
    /// [`store_fragment`](subduction_core::subduction::Subduction::store_fragment)
    /// which takes `(id, head, boundary, checkpoints, blob)` — the
    /// [`Fragment`](sedimentree_core::fragment::Fragment) struct already
    /// bundles the head, boundary, and checkpoints.
    fn store_fragment(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        fragment: sedimentree_core::fragment::Fragment,
        blob: sedimentree_core::blob::Blob,
    ) -> F::Future<'_, eyre::Result<()>>;

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
    fn try_causal_decrypt(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<CausalDecryptResult>>;
}
