//! IO seams for runtime2.
//!
//! These traits are where IO is *externalized*. The actor logic (hub, doc-worker)
//! depends on these traits, never on `tokio`/`iroh` directly.

use crate::interlude::*;
use future_form::FutureForm;

/// Runtime-neutral sleep capability. Periodic workers recreate a sleep on
/// each iteration, which also lets deterministic tests advance time explicitly.
pub trait Timer<F: FutureForm>: Send + Sync {
    fn sleep(&self, duration: std::time::Duration) -> F::Future<'static, ()>;
}

/// Replaces `TimestampSeconds::now()` / `Instant::now()`. Injected so tests
/// control time.
pub trait Clock: Send + Sync {
    fn instant(&self) -> std::time::Instant;
}

/// Result of a causal decrypt.
///
/// Mirrors `keyhive_core::store::ciphertext::CausalDecryptionState<Vec<u8>, Vec<u8>>`
/// from [`Document::try_causal_decrypt_content`](keyhive_core::principal::document::Document::try_causal_decrypt_content).
/// The doc-worker materializer iterates [`complete`](Self::complete) to collect
/// ancestor plaintexts, then continues the walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationBlocker {
    DocumentNotInHive,
    MissingDocumentKeys { content_refs: Vec<Vec<u8>> },
    MissingCiphertexts { content_refs: Vec<Vec<u8>> },
    MissingAutomergeDependencies { deferred_blobs: usize },
}

#[derive(Debug, Clone, Default)]
pub struct CausalDecryptResult {
    /// Successfully-decrypted (content_ref, plaintext) pairs, starting with the
    /// entrypoint ancestor chain. Consumed by the materializer to build the
    /// full automerge doc.
    pub complete: Vec<(Vec<u8>, Vec<u8>)>,
    /// Concrete reasons why the causal closure could not be decrypted.
    pub blockers: Vec<MaterializationBlocker>,
}

/// Result of a transport-level document sync attempt before materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocAttempt {
    Exchanged,
    NotFound,
    Unauthorized,
    Policy(subduction_core::sync_session::SyncPolicyRejectionKind),
}

/// Outcome of initiating a keyhive sync round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyhiveSyncOutcome {
    /// The round was initiated; completion arrives via `KeyhiveSyncDone` /
    /// `KeyhiveSyncFailed`.
    Initiated,
    /// The peer's transport connection is already gone; the round cannot
    /// start. The hub must fail the round deterministically instead of waiting
    /// for a separate connection-loss event to clear it.
    PeerDisappeared,
}

/// A document's Keyhive encryption state cannot currently produce an application key.
#[derive(Debug, thiserror::Error)]
#[error(
    "document encryption key unavailable: {source} (document={document_id}, owner_secrets={owner_secret_count}, cgka_ops={cgka_operation_count}, has_pcs_key={has_pcs_key})"
)]
pub(crate) struct DocumentKeyUnavailable {
    #[source]
    pub(crate) source: eyre::Report,
    pub(crate) document_id: crate::DocumentId,
    pub(crate) owner_secret_count: usize,
    pub(crate) cgka_operation_count: usize,
    pub(crate) has_pcs_key: bool,
}

/// The doc-worker's IO contract. All methods are `F::Future<'_>` so the same
/// logic runs `Sendable` (native) and `Local` (wasm).
///
/// Local writes are exposed as document-level service operations. Their
/// implementations own Keyhive encryption/update persistence and Subduction
/// storage ordering; the worker only serializes document transitions.
///
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

    /// Durable (storage-backed) sedimentree frontier — the source of truth.
    /// [`sedimentree_heads`](Self::sedimentree_heads) is cache-first and can
    /// lag durable storage behind an eviction/re-hydration race (observed:
    /// the causal-coverage reconcile read a stale multi-head frontier while
    /// storage already held the linking commit, minting a spurious
    /// checkpoint). Correctness-sensitive decisions — like whether a
    /// checkpoint is needed — must read this.
    fn durable_sedimentree_heads(
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

    /// Encrypt and persist a newly created document, including any Keyhive
    /// updates produced by encryption. The service owns operation ordering and
    /// publishes the resulting Keyhive change.
    fn persist_initial_document(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        staged: crate::runtime2::support::StagedAutomergeIngest,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    ) -> F::Future<'_, eyre::Result<()>>;

    /// Encrypt and persist a batch of serialized local document transitions.
    /// Keyhive update operations never escape this service boundary. The caller
    /// must service every returned `FragmentRequested` through `store_fragment`.
    fn persist_local_commits(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
    ) -> F::Future<
        '_,
        eyre::Result<
            std::collections::BTreeSet<subduction_core::subduction::request::FragmentRequested>,
        >,
    >;

    /// Fingerprint of the currently usable BeeKEM epoch, or `None` when the
    /// settled operation history requires a causally subsequent Update.
    fn current_causal_epoch(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
    ) -> F::Future<'_, eyre::Result<Option<[u8; 32]>>>;

    /// Epoch fingerprint recorded in a persisted frontier ciphertext.
    fn ciphertext_epoch(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        head: sedimentree_core::loose_commit::id::CommitId,
    ) -> F::Future<'_, eyre::Result<Option<[u8; 32]>>>;

    /// Publish a key-only causal checkpoint covering the supplied encryption
    /// frontier. The implementation establishes and durably records a PCS
    /// root first when the healed Keyhive graph has none.
    #[expect(clippy::type_complexity)]
    fn persist_causal_checkpoint(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        covered_frontier: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    ) -> F::Future<
        '_,
        eyre::Result<
            Option<(
                sedimentree_core::loose_commit::id::CommitId,
                crate::runtime2::support::CausalCheckpoint,
                Vec<sedimentree_core::loose_commit::id::CommitId>,
            )>,
        >,
    >;

    /// Whether the local principal may write to this document (Edit access or
    /// better). The authoritative write gate: rejects revoked members and
    /// Read-only holders before a commit is persisted.
    fn has_doc_write_access(&self, doc_id: crate::DocumentId) -> F::Future<'_, eyre::Result<bool>>;

    /// Whether the local principal may fetch or sync this document (Fetch/Relay access
    /// or better). Used for early fail-fast validation prior to network sync.
    fn has_doc_fetch_access(&self, doc_id: crate::DocumentId) -> F::Future<'_, eyre::Result<bool>>;

    /// Store a raw fragment bundle at a boundary commit. The implementation
    /// encrypts the bundle and constructs the persisted fragment metadata from
    /// the encrypted blob.
    fn store_fragment(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        head: sedimentree_core::loose_commit::id::CommitId,
        boundary: std::collections::BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
        checkpoints: Vec<sedimentree_core::loose_commit::id::CommitId>,
        raw_blob: Vec<u8>,
    ) -> F::Future<'_, eyre::Result<()>>;

    // ── keyhive decrypt (hides the keyhive doc handle) ────────────────────

    /// Try to decrypt the blob at `locator`. Returns `None` if the key is not
    /// available (materialization pending). Mirrors
    /// [`Document::try_decrypt_content_keyed`](keyhive_core::principal::document::Document::try_decrypt_content_keyed)
    /// but abstracts loading the ciphertext from storage and looking up the
    /// keyhive document.
    fn try_decrypt_content_keyed(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime2::support::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<Option<Vec<u8>>>>;

    /// Causal decrypt: decrypt `locator` + any ancestors whose keys are now
    /// reachable. Mirrors
    /// [`Document::try_causal_decrypt_content`](keyhive_core::principal::document::Document::try_causal_decrypt_content).
    /// The returned [`CausalDecryptResult::complete`] includes the entrypoint
    /// plus any ancestors decrypted along the causal chain.
    fn try_causal_decrypt(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        locator: crate::runtime2::support::BigRepoCiphertextLocator,
    ) -> F::Future<'_, eyre::Result<CausalDecryptResult>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationStatus {
    Missing,
    Pending(Vec<MaterializationBlocker>),
    Ready { partially_decrypted: bool },
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

    fn allocate_document(
        &self,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
    ) -> F::Future<'_, eyre::Result<crate::DocumentId>>;

    fn stage_allocated_document(
        &self,
        doc_id: crate::DocumentId,
        initial_content: Vec<u8>,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        already_persisted: bool,
    ) -> F::Future<'_, eyre::Result<()>>;

    fn finalize_document_authority(
        &self,
        doc_id: crate::DocumentId,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
    ) -> F::Future<'_, eyre::Result<()>>;

    fn complete_document_authority(
        &self,
        doc_id: crate::DocumentId,
        pending_group: crate::keyhive::BigKeyhiveGroup,
        content_heads: nonempty::NonEmpty<[u8; 32]>,
    ) -> F::Future<'_, eyre::Result<()>>;

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

    /// Read the immutable Keyhive event-log watermark for quiescence barriers.
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
    ) -> F::Future<'_, eyre::Result<KeyhiveSyncOutcome>>;

    /// Run a doc sync round with `peer_id` for the given sedimentree.
    /// Returns `true` if the sync exchange had success (commits/fragments
    /// were received or sent), `false` if the sync completed without
    /// meaningful exchange (no new content).
    ///
    /// `request_id` (when `Some`) is threaded into the transport so the
    /// emitted sessions carry an ID the hub can correlate back to its waiter.
    fn sync_doc_with_peer(
        &self,
        sed_id: sedimentree_core::id::SedimentreeId,
        peer_id: big_sync_core::PeerId,
        request_id: Option<subduction_core::connection::message::RequestId>,
    ) -> F::Future<'_, eyre::Result<SyncDocAttempt>>;
}
