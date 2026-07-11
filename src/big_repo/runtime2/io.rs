//! IO seams for runtime2.
//!
//! These traits are where IO is *externalized*. The actor logic (hub, doc-worker)
//! depends on these traits, never on `tokio`/`iroh`/`redb` directly. Backends:
//! - native: `TokioSpawn`/`TokioTimer`/`StdClock` + real `Storage`/`HostPartStore`.
//! - tests (D2): memory backends + `TokioSpawn` (jitter-free; not step-deterministic).
//! - future (full determinism): a step-`Spawner` + `IoTask`-completing `DocIo`.
//! - wasm: `FutureForm = Local`, wasm-bindgen spawner, IndexedDB storage.

use crate::interlude::*;
use future_form::FutureForm;

// ─── Determinism levers: Spawner / Timer / Clock ───────────────────────────

/// Spawn a background future of form `F`. Replaces `tokio::spawn` /
/// `utils_rs::AbortableJoinSet` (which hardcode tokio). A step-spawner impl
/// drives background work deterministically in tests.
pub trait Spawner<F: FutureForm>: Send + Sync {
    /// Spawn a future; returns a handle that can await completion / abort.
    fn spawn(&self, fut: F::Future<'static, eyre::Result<()>>) -> SpawnHandle;
}

/// A handle to a spawned background task (replaces `AbortHandle`/`JoinHandle`).
pub struct SpawnHandle {
    pub abort: Box<dyn Fn() + Send + Sync>,
    pub join: futures::future::BoxFuture<'static, eyre::Result<()>>,
}

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

// ─── HostPartStore (big_sync) ──────────────────────────────────────────────
// Re-exported so the hub/doc-worker don't import big_sync directly here.
// The part_store stays as today (sqlite); unification (plan B) backs it with
// the same SQL KV as `Storage<F>` so `store_commit` + `set_obj_payload` are
// one transaction.
pub use big_sync::HostPartStore;

// ─── DocIo: the doc-worker's centralized IO surface ────────────────────────
// EVERY IO the doc-worker does goes through this trait — storage AND keyhive
// (encrypt/decrypt). Hiding keyhive behind DocIo (rather than exposing the
// keyhive doc handle) keeps the doc-worker free of keyhive's deep generics and
// makes the full IO surface visible in one place (the blocking-out deliverable).
//
// A future `IoTaskDocIo` impl wraps these in samod-style round-stepping for
// full determinism, without rewriting the actor.

/// Result of a causal decrypt: plaintext for the blob + any ancestors unlocked
/// along the way (see `try_causal_decrypt_content` in keyhive).
pub struct CausalDecryptResult {
    pub plaintext: Option<Vec<u8>>,
    pub ancestors: Vec<(Vec<u8>, Vec<u8>)>,
}

/// An encrypted loose commit ready for the unified atomic write.
pub struct EncryptedLooseCommit {
    pub head: sedimentree_core::loose_commit::id::CommitId,
    pub parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    pub blob: sedimentree_core::blob::Blob,
    /// CGKA op emitted by the encrypt (if the key epoch advanced).
    pub cgka_update_op: Option<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
}

/// The doc-worker's IO contract. All methods are `F::Future<'_>` so the same
/// logic runs `Sendable` (native) and `Local` (wasm).
///
/// # The unified transaction (plan B)
/// [`DocIo::store_commit_and_advance_heads`] is the atomic local write: it
/// stores the sedimentree commit AND updates the big_sync part_store heads in
/// ONE transaction. This is the crash-resistance fix (current_fixes §3) —
/// `subduction.store_commit` + `set_obj_payload` are no longer separate calls.
pub trait DocIo<F: FutureForm>: Send + Sync {
    // ── sedimentree (reads) ───────────────────────────────────────────────
    /// Cheap sedimentree frontier via subduction's `heads_or_hydrate`
    /// (resident fast path; hydrates on miss). Replaces big_repo's own
    /// full-hydration `get_or_hydrate_minimized_tree_from_storage`.
    fn sedimentree_heads(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<Vec<sedimentree_core::loose_commit::id::CommitId>>>;

    /// Hydrate the full minimized tree (for materialization / decrypt walks).
    fn hydrate_tree(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>>>;

    // ── unified local write (atomic; plan B) ─────────────────────────────
    /// Atomically: store the encrypted loose commit to subduction AND advance
    /// the big_sync part_store heads to `sedimentree_heads`. One SQL txn.
    /// Returns any fragment-boundary request (as subduction `store_commit` does).
    fn store_commit_and_advance_heads(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        commit: EncryptedLooseCommit,
        sedimentree_heads: Vec<sedimentree_core::loose_commit::id::CommitId>,
    ) -> F::Future<'_, eyre::Result<Option<subduction_core::subduction::request::FragmentRequested>>>;

    /// Atomically store a fragment + advance heads (companion for boundary commits).
    fn store_fragment_and_advance_heads(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        fragment: sedimentree_core::fragment::Fragment,
        blob: sedimentree_core::blob::Blob,
        sedimentree_heads: Vec<sedimentree_core::loose_commit::id::CommitId>,
    ) -> F::Future<'_, eyre::Result<()>>;

    // ── keyhive encrypt/decrypt (hides the keyhive doc handle) ────────────
    /// Encrypt a loose commit's blob under the current keyhive epoch. Returns
    /// the encrypted blob + any CGKA op emitted.
    fn encrypt_loose_commit(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        head: sedimentree_core::loose_commit::id::CommitId,
        parents: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
        blob: Vec<u8>,
    ) -> F::Future<'_, eyre::Result<EncryptedLooseCommit>>;

    /// Try to decrypt the blob at `locator` (`None` if we lack the key ⇒
    /// materialization pending). Internally loads ciphertext + keyhive decrypt.
    fn try_decrypt_content_keyed(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<Option<Vec<u8>>>>;

    /// Causal decrypt: decrypt `locator` + any ancestors whose keys are now
    /// reachable (see keyhive `try_causal_decrypt_content`).
    fn try_causal_decrypt(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<CausalDecryptResult>>;

    // ── big_sync part_store (heads) ───────────────────────────────────────
    /// Read the stored sedimentree heads (the part_store payload). After the
    /// heads fix, this is ALWAYS sedimentree heads.
    fn doc_payload_heads(
        &self,
        doc_id: crate::DocumentId,
    ) -> F::Future<'_, eyre::Result<Option<std::sync::Arc<[automerge::ChangeHash]>>>>;

    /// Write sedimentree heads to the part_store (the pending / relay path —
    /// no commit, just advance the recorded frontier).
    fn set_doc_sedimentree_heads(
        &self,
        doc_id: crate::DocumentId,
        heads: std::sync::Arc<[automerge::ChangeHash]>,
    ) -> F::Future<'_, eyre::Result<()>>;
}
