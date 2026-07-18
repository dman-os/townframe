//! Native Sendable IO backend for runtime2.
//!
//! Provides a concrete [`DocIo`] + [`RuntimeIo`] impl backed by real
//! subduction storage and keyhive. Generic over subduction storage `S`
//! and connection type `C` (default: [`BigRepoIrohTransport`]).
//!
//! # Design
//!
//! [`NativeBigRepoIo`] bundles all the state that runtime2's hub and
//! doc-workers need: the subduction handle, storage, sedimentree cache,
//! keyhive handle, keyhive storage, and keyhive protocol. It implements both
//! [`DocIo<Sendable>`] and [`RuntimeIo<Sendable>`] by adapting/copying the old
//! `runtime.rs` logic — no behavioral simplification.
//!
//! # Tokio boundary
//!
//! All methods are `async` and `Sendable`. Tokio type use (channels, tasks) is
//! confined to this backend; runtime2 actor code depends only on the trait
//! interfaces.
//!
//! [`BigRepoIrohTransport`]: crate::runtime::BigRepoIrohTransport

use crate::interlude::*;
use crate::keyhive_storage::BigRepoKeyhiveStorage;
use crate::runtime2::{
    CausalDecryptResult, Clock, DocIo, EncryptedInitialSedimentree, EncryptedLooseCommit,
    RuntimeIo, SyncDocAttempt, TaskSet, Timer,
};
use crate::{
    encrypted_blob::{decode_encrypted_blob, encode_encrypted_blob},
    ephemeral::{BigEphemeralBackend, BigEphemeralSwitchboard, BigRepoEphemeralBackend},
    handler::{
        BigRepoComposedHandler, BigRepoEphemeralHandler, BigRepoKeyhiveHandler,
        BigRepoKeyhiveProtocol,
    },
    keyhive_conn::BigRepoKeyhiveConnAdapter,
    runtime::{
        accept_incoming, connect_outgoing_to, encrypt_fragment_blob,
        encrypt_loose_commit_with_update_op, encrypt_staged_automerge_ingest,
        persist_cgka_update_op, sedimentree_heads_payload, BigRepoIrohTransport, BigRepoSubduction,
        BigRepoSubductionStorage, BigRepoSyncPolicy, IrohConnectResult, SubductionSedimentrees,
    },
    wire::BigRepoWireMessage,
    BigEphemeral, BigKeyhiveHandle, DocumentId,
};
use big_sync_core::PeerId;
use future_form::{FutureForm, Sendable};
use keyhive_core::{
    crypto::envelope::Envelope, event::static_event::StaticEvent,
    principal::document::id::DocumentId as KhDocumentId, principal::identifier::Identifier,
    store::ciphertext::CiphertextStore,
};
use keyhive_crypto::symmetric_key::SymmetricKey;
use nonempty::NonEmpty;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::id::CommitId,
    sedimentree::{minimized::MinimizedSedimentree, Sedimentree},
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use subduction_core::{
    authenticated::Authenticated,
    connection::{message::SyncMessage, Connection},
    handler::sync::SyncHandler,
    nonce_cache::NonceCache,
    storage::powerbox::StoragePowerbox,
    subduction::request::FragmentRequested,
    subduction::Subduction,
};
use subduction_ephemeral::{
    clock::std_clock::StdClock, config::EphemeralConfig, handler::EphemeralHandler,
    message::EphemeralMessage, policy::OpenEphemeralPolicy,
};
use subduction_keyhive::{KeyhiveConnection, KeyhivePeerId};
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};
use tokio::sync::mpsc;

// ═══════════════════════════════════════════════════════════════════════════
// CONTEXT
// ═══════════════════════════════════════════════════════════════════════════

/// Concrete native IO backend for runtime2.
///
/// Generic over subduction storage `S` and connection `C`. Both [`DocIo`] and
/// [`RuntimeIo`] are implemented on this single struct so the hub can inject
/// one `Arc<NativeBigRepoIo<S, C>>` for both trait slots.
#[derive(Clone)]
pub(crate) struct NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    /// Shared subduction handle — the core sync + storage engine.
    pub(crate) subduction: Arc<BigRepoSubduction<S>>,
    /// Clonable storage backend (for direct reads).
    pub(crate) storage: S,
    /// Shared sedimentree cache (minimized trees).
    pub(crate) sedimentrees: SubductionSedimentrees,
    /// Keyhive handle — document operations, content encryption.
    pub(crate) keyhive: BigKeyhiveHandle,
    /// Keyhive storage — CGKA ops, archives.
    pub(crate) keyhive_storage: BigRepoKeyhiveStorage,
    /// Keyhive protocol handle — sync initiation, cache refresh, compaction.
    pub(crate) keyhive_protocol: BigRepoKeyhiveProtocol,
    /// Local peer identity.
    pub(crate) local_peer_id: PeerId,
    /// Sync policy (timeouts, TTLs).
    pub(crate) sync_policy: BigRepoSyncPolicy,
    /// Ownership for the legacy ephemeral switchboard task. Dropping the
    /// runtime2 hub drops this set and therefore shuts the switchboard down.
    pub(crate) ephemeral_tasks: Arc<utils_rs::AbortableJoinSet>,
    /// Ephemeral publisher for application-level transient messages.
    pub(crate) ephemeral_backend: Arc<dyn BigEphemeralBackend>,
    /// Direct BigRepo RPC notification source for local Keyhive changes.
    pub(crate) keyhive_change_tx: tokio::sync::broadcast::Sender<()>,
    /// Compatibility bridge for the existing big-sync partition layer.
    pub(crate) big_sync_store: crate::SharedPartStore,
}

impl<S> std::fmt::Debug for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBigRepoIo")
            .field("local_peer_id", &self.local_peer_id)
            .finish_non_exhaustive()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CiphertextStore adapter (for causal decrypt)
// ═══════════════════════════════════════════════════════════════════════════

/// A [`CiphertextStore`] that reads encrypted blobs directly from
/// subduction storage. Used by [`NativeBigRepoIo::try_causal_decrypt`].
struct NativeCiphertextStore<S: BigRepoSubductionStorage> {
    storage: S,
    sed_id: SedimentreeId,
    /// In-memory cache of (content_ref -> encrypted content).
    cache: std::sync::Mutex<
        HashMap<Vec<u8>, Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
    >,
}

impl<S: BigRepoSubductionStorage> NativeCiphertextStore<S> {
    fn new(storage: S, sed_id: SedimentreeId) -> Self {
        Self {
            storage,
            sed_id,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn load_and_cache(
        &self,
        content_ref: &[u8],
    ) -> eyre::Result<Option<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>> {
        // Check cache first.
        {
            let cache = self.cache.lock().expect(ERROR_MUTEX);
            if let Some(encrypted) = cache.get(content_ref) {
                return Ok(Some(Arc::clone(encrypted)));
            }
        }

        // Try loading as loose commit, then fragment.
        let commit_id_bytes: [u8; 32] = content_ref
            .try_into()
            .map_err(|_| ferr!("content_ref must be 32 bytes, got {}", content_ref.len()))?;
        let commit_id = CommitId::new(commit_id_bytes);

        // Loose commit
        if let Some(verified) =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                &self.storage,
                self.sed_id,
                commit_id,
            )
            .await
            .map_err(|e| ferr!("failed loading loose commit for ciphertext: {e}"))?
        {
            let encrypted = decode_encrypted_blob(verified.blob().as_slice())
                .map_err(|e| ferr!("failed decoding loose commit encrypted blob: {e}"))?;
            let encrypted = Arc::new(encrypted);
            self.cache
                .lock()
                .expect(ERROR_MUTEX)
                .insert(content_ref.to_vec(), Arc::clone(&encrypted));
            return Ok(Some(encrypted));
        }

        // Fragment
        if let Some(verified) =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                &self.storage,
                self.sed_id,
                commit_id,
            )
            .await
            .map_err(|e| ferr!("failed loading fragment for ciphertext: {e}"))?
        {
            let encrypted = decode_encrypted_blob(verified.blob().as_slice())
                .map_err(|e| ferr!("failed decoding fragment encrypted blob: {e}"))?;
            let encrypted = Arc::new(encrypted);
            self.cache
                .lock()
                .expect(ERROR_MUTEX)
                .insert(content_ref.to_vec(), Arc::clone(&encrypted));
            return Ok(Some(encrypted));
        }

        Ok(None)
    }
}

impl<S: BigRepoSubductionStorage> CiphertextStore<Sendable, Vec<u8>, Vec<u8>>
    for NativeCiphertextStore<S>
{
    type GetCiphertextError = eyre::Report;
    type MarkDecryptedError = eyre::Report;

    fn get_ciphertext<'a>(
        &'a self,
        content_ref: &'a Vec<u8>,
    ) -> <Sendable as future_form::FutureForm>::Future<
        'a,
        std::result::Result<
            Option<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        Sendable::from_future(async move { self.load_and_cache(content_ref.as_slice()).await })
    }

    fn get_ciphertext_by_pcs_update<'a>(
        &'a self,
        _pcs_update: &'a keyhive_crypto::digest::Digest<
            keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
        >,
    ) -> <Sendable as future_form::FutureForm>::Future<
        'a,
        std::result::Result<
            Vec<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        // Not used in the decrypt paths we support; return empty.
        Sendable::from_future(async move { Ok(Vec::new()) })
    }

    fn mark_decrypted<'a>(
        &'a self,
        _content_ref: &'a Vec<u8>,
    ) -> <Sendable as future_form::FutureForm>::Future<
        'a,
        std::result::Result<(), Self::MarkDecryptedError>,
    > {
        Sendable::from_future(async move { Ok(()) })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Keyhive doc lookup helper
// ═══════════════════════════════════════════════════════════════════════════

/// Derive the Keyhive document ID from a sedimentree ID (same 32-byte
/// verifying key).
fn kh_doc_id_from_sed_id(sed_id: SedimentreeId) -> eyre::Result<KhDocumentId> {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sed_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    Ok(KhDocumentId::from(Identifier::from(vk)))
}

/// Get the keyhive document for a sedimentree ID, or return an error if
/// the keyhive document is not found locally.
async fn get_kh_doc(
    keyhive: &BigKeyhiveHandle,
    sed_id: SedimentreeId,
) -> eyre::Result<
    Arc<
        futures::lock::Mutex<
            keyhive_core::principal::document::Document<
                Sendable,
                keyhive_crypto::signer::memory::MemorySigner,
                Vec<u8>,
                crate::keyhive_listener::BigRepoKeyhiveListener,
            >,
        >,
    >,
> {
    let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
    keyhive
        .clone_keyhive()
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for sedimentree_id={sed_id:?}"))
}

// ═══════════════════════════════════════════════════════════════════════════
// DocIo<Sendable> impl
// ═══════════════════════════════════════════════════════════════════════════

impl<S> DocIo<Sendable> for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    fn encrypt_initial_sedimentree(
        &self,
        sed_id: SedimentreeId,
        staged: crate::runtime::StagedAutomergeIngest,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<EncryptedInitialSedimentree>>
    {
        Sendable::from_future(async move {
            let (sedimentree, blobs, cgka_update_ops) =
                encrypt_staged_automerge_ingest(&staged, &self.keyhive, sed_id)
                    .await
                    .wrap_err("failed encrypting initial sedimentree")?;
            Ok(EncryptedInitialSedimentree {
                sedimentree,
                blobs,
                cgka_update_ops,
            })
        })
    }

    fn store_initial_sedimentree(
        &self,
        sed_id: SedimentreeId,
        initial: EncryptedInitialSedimentree,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            self.subduction
                .store_sedimentree(sed_id, initial.sedimentree, initial.blobs)
                .await
                .map_err(|err| ferr!("failed storing initial sedimentree: {err}"))?;
            Ok(())
        })
    }

    // ── sedimentree_heads ──────────────────────────────────────────────────
    fn sedimentree_heads(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<Vec<CommitId>>> {
        Sendable::from_future(async move {
            // Try sedimentree cache first.
            if let Some(tree) = self.sedimentrees.get_cloned(&sed_id).await {
                let heads = sedimentree_heads_payload(&tree);
                return Ok(heads.iter().map(|h| CommitId::new(h.0)).collect());
            }

            // Hydrate from storage: load loose commits + fragments, build tree.
            let loose_commits =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commits(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading loose commits for heads")?;
            let fragments =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragments(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading fragments for heads")?;

            if loose_commits.is_empty() && fragments.is_empty() {
                return Ok(Vec::new());
            }

            let tree = MinimizedSedimentree::new(Sedimentree::new(
                fragments.iter().map(|v| v.payload().clone()).collect(),
                loose_commits.iter().map(|v| v.payload().clone()).collect(),
            ));

            let heads = sedimentree_heads_payload(&tree);
            Ok(heads.iter().map(|h| CommitId::new(h.0)).collect())
        })
    }

    // ── hydrate_tree ──────────────────────────────────────────────────────
    fn hydrate_tree(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<Option<MinimizedSedimentree>>>
    {
        Sendable::from_future(async move {
            // Check cache first.
            if let Some(tree) = self.sedimentrees.get_cloned(&sed_id).await {
                return Ok(Some(tree));
            }

            // Load from storage.
            let loose_commits =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commits(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading loose commits for hydrate")?;
            let fragments =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragments(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading fragments for hydrate")?;

            if loose_commits.is_empty() && fragments.is_empty() {
                return Ok(None);
            }

            let tree = MinimizedSedimentree::new(Sedimentree::new(
                fragments.iter().map(|v| v.payload().clone()).collect(),
                loose_commits.iter().map(|v| v.payload().clone()).collect(),
            ));

            Ok(Some(tree))
        })
    }

    // ── store_commit ──────────────────────────────────────────────────────
    fn store_commit(
        &self,
        sed_id: SedimentreeId,
        commit: EncryptedLooseCommit,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<Option<FragmentRequested>>>
    {
        Sendable::from_future(async move {
            let EncryptedLooseCommit {
                head,
                parents,
                blob,
                ..
            } = commit;

            // Wrap the blob for subduction's store_commit call.
            let maybe_request = self
                .subduction
                .store_commit(sed_id, head, parents, blob)
                .await
                .map_err(|err| ferr!("failed store_commit: {err}"))?;

            Ok(maybe_request)
        })
    }

    // ── store_fragment ────────────────────────────────────────────────────
    fn store_fragment(
        &self,
        sed_id: SedimentreeId,
        head: CommitId,
        boundary: std::collections::BTreeSet<CommitId>,
        checkpoints: Vec<CommitId>,
        raw_blob: Vec<u8>,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let raw_blob = Blob::new(raw_blob);
            let encrypted_blob = encrypt_fragment_blob(
                &self.keyhive,
                &self.storage,
                sed_id,
                head,
                &boundary,
                raw_blob.as_slice(),
            )
            .await
            .wrap_err("failed encrypting fragment blob")?;
            let fragment = sedimentree_core::fragment::Fragment::new(
                sed_id,
                head,
                boundary,
                &checkpoints,
                BlobMeta::new(&encrypted_blob),
            );

            self.subduction
                .add_fragment(
                    sed_id,
                    fragment.head(),
                    fragment.boundary().clone(),
                    &checkpoints,
                    encrypted_blob,
                )
                .await
                .map_err(|err| ferr!("failed add_fragment: {err}"))?;

            Ok(())
        })
    }

    // ── big-sync payload compatibility bridge ─────────────────────────────
    fn set_doc_heads_payload(
        &self,
        doc_id: DocumentId,
        heads: Arc<[automerge::ChangeHash]>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let payload = serde_json::json!({
                "heads": am_utils_rs::serialize_commit_heads(&heads),
            });
            self.big_sync_store
                .set_obj_payload(doc_id, payload)
                .await
                .map_err(|error| ferr!("failed updating big-sync payload: {error}"))
        })
    }

    // ── note local keyhive change ─────────────────────────────────────────
    fn note_local_keyhive_changed(&self) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            self.keyhive_protocol
                .note_local_keyhive_changed()
                .await
                .map(|_| ())
                .map_err(|error| ferr!("failed refreshing keyhive cache: {error}"))?;
            // This is a best-effort invalidation hint. The direct BigRepo RPC
            // stream carries it to connected peers; Keyhive sync carries the
            // actual state.
            let _ = self.keyhive_change_tx.send(());
            Ok(())
        })
    }

    // ── encrypt_loose_commit ──────────────────────────────────────────────
    fn encrypt_loose_commit(
        &self,
        sed_id: SedimentreeId,
        head: CommitId,
        parents: BTreeSet<CommitId>,
        blob: Vec<u8>,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<EncryptedLooseCommit>> {
        Sendable::from_future(async move {
            // Use the old helper to encrypt.
            let (encrypted_blob, _app_key, update_op) = encrypt_loose_commit_with_update_op(
                &self.keyhive,
                sed_id,
                head,
                &parents,
                &blob,
                &HashMap::new(), // batch_keys empty on first call; keyhive uses known_decryption_keys
            )
            .await
            .map_err(|e| ferr!("encrypt commit failed: {e}"))?;

            Ok(EncryptedLooseCommit {
                head,
                parents,
                blob: encrypted_blob,
                cgka_update_op: update_op,
            })
        })
    }

    // ── try_decrypt_content_keyed ─────────────────────────────────────────
    fn try_decrypt_content_keyed(
        &self,
        sed_id: SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<Option<Vec<u8>>>> {
        Sendable::from_future(async move {
            // Load the raw blob from storage.
            let raw = match locator.kind {
                crate::runtime::BigRepoCiphertextKind::LooseCommit => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|e| ferr!("failed loading loose commit: {e}"))?
                    .map(|v| v.blob().clone().into_contents())
                }
                crate::runtime::BigRepoCiphertextKind::Fragment => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|e| ferr!("failed loading fragment: {e}"))?
                    .map(|v| v.blob().clone().into_contents())
                }
            };
            let Some(raw) = raw else {
                return Ok(None);
            };

            // Decode the encrypted blob.
            let encrypted = decode_encrypted_blob(&raw)
                .map_err(|e| ferr!("failed decoding encrypted blob: {e}"))?;

            // A missing local Keyhive document means the content keys have
            // not arrived yet. This is a normal pending-materialization
            // state, not a fatal decryption error.
            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let Some(kh_doc) = self.keyhive.clone_keyhive().get_document(kh_doc_id).await else {
                return Ok(None);
            };
            let mut doc = kh_doc.lock().await;
            match doc.try_decrypt_content_keyed(&encrypted) {
                Ok((plaintext, key)) => {
                    // Keep the recovered key available for a later local
                    // child commit's causal envelope.
                    doc.remember_decryption_key(encrypted.content_ref.clone(), key);
                    // Deserialize the envelope to extract the actual payload.
                    let envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&plaintext)
                        .map_err(|e| ferr!("bincode decrypt result: {e}"))?;
                    Ok(Some(envelope.plaintext))
                }
                Err(keyhive_core::principal::document::DecryptError::KeyNotFound) => Ok(None),
                Err(err) => Err(ferr!("decrypt failed: {err}")),
            }
        })
    }

    // ── persist_cgka_update_op ────────────────────────────────────────────
    fn persist_cgka_update_op(
        &self,
        op: keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(
            async move { persist_cgka_update_op(&self.keyhive_storage, op).await },
        )
    }

    // ── try_causal_decrypt ────────────────────────────────────────────────
    fn try_causal_decrypt(
        &self,
        sed_id: SedimentreeId,
        locator: crate::runtime::BigRepoCiphertextLocator,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<CausalDecryptResult>> {
        Sendable::from_future(async move {
            // Load the raw blob from storage.
            let raw = match locator.kind {
                crate::runtime::BigRepoCiphertextKind::LooseCommit => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|e| ferr!("failed loading loose commit: {e}"))?
                    .map(|v| v.blob().clone().into_contents())
                }
                crate::runtime::BigRepoCiphertextKind::Fragment => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|e| ferr!("failed loading fragment: {e}"))?
                    .map(|v| v.blob().clone().into_contents())
                }
            };
            let Some(raw) = raw else {
                return Ok(CausalDecryptResult::default());
            };

            // Decode the encrypted blob.
            let encrypted = decode_encrypted_blob(&raw)
                .map_err(|e| ferr!("failed decoding encrypted blob: {e}"))?;

            // A missing local Keyhive document means the content keys have
            // not arrived yet. Return an incomplete causal result so the doc
            // worker records PendingMaterialization and retries after the
            // next Keyhive sync.
            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let Some(kh_doc) = self.keyhive.clone_keyhive().get_document(kh_doc_id).await else {
                return Ok(CausalDecryptResult::default());
            };

            // Set up a ciphertext store backed by our storage.
            let ct_store = NativeCiphertextStore::new(self.storage.clone(), sed_id);

            // The upstream causal walk returns the decrypted ancestors, but
            // not the entrypoint itself. The old runtime explicitly loaded
            // the entrypoint first; runtime2 must preserve that contract.
            let (entrypoint_raw, entrypoint_key) = {
                let mut doc = kh_doc.lock().await;
                match doc.try_decrypt_content_keyed(&encrypted) {
                    Ok((plaintext, key)) => (plaintext, key),
                    Err(keyhive_core::principal::document::DecryptError::KeyNotFound) => {
                        return Ok(CausalDecryptResult::default())
                    }
                    Err(error) => {
                        return Err(ferr!("entrypoint decrypt failed: {error}"));
                    }
                }
            };
            let entrypoint_envelope: Envelope<Vec<u8>, Vec<u8>> =
                bincode::deserialize(&entrypoint_raw)
                    .map_err(|error| ferr!("failed decoding entrypoint envelope: {error}"))?;

            // Attempt causal decrypt for the entrypoint's ancestors.
            let state = {
                let mut doc = kh_doc.lock().await;
                match doc.try_causal_decrypt_content(&encrypted, ct_store).await {
                    Ok(state) => state,
                    Err(error) => {
                        return Err(ferr!(
                            "causal decrypt failed; BigRepo envelope is not causally closed: {error}"
                        ));
                    }
                }
            };

            // The causal store returns the application keys it used, but the
            // generic Keyhive API does not persist them on the document. Keep
            // them there so a later local child commit can construct its own
            // causal envelope, including ancestors recovered through a grant.
            {
                let mut doc = kh_doc.lock().await;
                doc.remember_decryption_key(encrypted.content_ref.clone(), entrypoint_key);
                for (content_ref, key) in &state.keys {
                    doc.remember_decryption_key(content_ref.clone(), *key);
                }
            }

            // Return the entrypoint first, followed by the decrypted causal
            // ancestors. Consumers can therefore materialize the exact blob
            // requested as well as its closure.
            let mut complete = vec![(encrypted.content_ref.clone(), entrypoint_envelope.plaintext)];
            for (content_ref, ciphertext_or_plaintext) in state.complete {
                let envelope: Envelope<Vec<u8>, Vec<u8>> =
                    match bincode::deserialize(&ciphertext_or_plaintext) {
                        Ok(env) => env,
                        Err(_) => {
                            complete.push((content_ref, ciphertext_or_plaintext));
                            continue;
                        }
                    };
                complete.push((content_ref, envelope.plaintext));
            }

            Ok(CausalDecryptResult { complete })
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RuntimeIo<Sendable> impl
// ═══════════════════════════════════════════════════════════════════════════

impl<S> RuntimeIo<Sendable> for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    // ── create_document ────────────────────────────────────────────────────
    fn create_document(
        &self,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: NonEmpty<[u8; 32]>,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<DocumentId>> {
        Sendable::from_future(async move {
            let doc_id = self
                .keyhive
                .create_doc(parents, content_heads, &self.keyhive_storage)
                .await?;
            Ok(doc_id)
        })
    }

    // ── contains_sedimentree ──────────────────────────────────────────────
    fn contains_sedimentree(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<bool>> {
        Sendable::from_future(async move {
            self.storage
                .contains_sedimentree_id(sed_id)
                .await
                .map_err(|e| ferr!("failed checking sedimentree presence: {e}"))
        })
    }

    fn inspect_stored_doc_blobs(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<Vec<Vec<u8>>>> {
        Sendable::from_future(async move {
            let commits =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commits(
                    &self.storage,
                    sed_id,
                )
                .await
                .map_err(|error| ferr!("failed loading loose commits for inspection: {error}"))?;
            let fragments =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragments(
                    &self.storage,
                    sed_id,
                )
                .await
                .map_err(|error| ferr!("failed loading fragments for inspection: {error}"))?;
            Ok(commits
                .into_iter()
                .map(|commit| commit.blob().clone().into_contents())
                .chain(
                    fragments
                        .into_iter()
                        .map(|fragment| fragment.blob().clone().into_contents()),
                )
                .collect())
        })
    }

    // ── note_local_keyhive_changed ────────────────────────────────────────
    fn note_local_keyhive_changed(
        &self,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            self.keyhive_protocol
                .note_local_keyhive_changed()
                .await
                .wrap_err("keyhive local-change refresh failed")?;
            // Broadcast delivery is intentionally best effort; the event is
            // only a wake-up hint and is not the source of Keyhive state.
            let _ = self.keyhive_change_tx.send(());
            Ok(())
        })
    }

    fn is_document_membership_target(
        &self,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        Sendable::from_future(async move {
            let document_id = keyhive_core::principal::document::id::DocumentId::from(target);
            Ok(self
                .keyhive
                .clone_keyhive()
                .get_document(document_id)
                .await
                .is_some())
        })
    }

    // ── big-sync membership bridge ───────────────────────────────────────
    fn refresh_big_sync_doc_access(
        &self,
        target: keyhive_core::principal::identifier::Identifier,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let agents = self
                .keyhive
                .agents_for_membered(target)
                .await
                .into_iter()
                .map(|(id, access)| (PeerId::new(id), access))
                .collect::<HashMap<PeerId, keyhive_core::access::Access>>();
            let doc_id = DocumentId::new(target.to_bytes());
            self.big_sync_store
                .set_doc_members(doc_id, agents.clone())
                .await;

            let own_id = PeerId::new(self.keyhive.clone_keyhive().id().to_bytes());
            if agents
                .get(&own_id)
                .is_some_and(|access| access.is_fetcher())
            {
                self.big_sync_store
                    .add_obj_to_parts(doc_id, vec![crate::GLOBAL_PART_ID])
                    .await
                    .map_err(|error| {
                        ferr!("failed adding document to global partition: {error}")
                    })?;
            } else {
                self.big_sync_store
                    .remove_obj_from_part(doc_id, crate::GLOBAL_PART_ID)
                    .await
                    .map_err(|error| {
                        ferr!("failed removing document from global partition: {error}")
                    })?;
            }

            let group_ids: std::collections::HashSet<PeerId> = self
                .keyhive
                .clone_keyhive()
                .groups()
                .lock()
                .await
                .keys()
                .map(|id| PeerId::new(id.to_bytes()))
                .collect();
            for agent_id in agents.keys().filter(|id| group_ids.contains(id)) {
                let group_part_id = big_sync_core::PartId::new(*agent_id.as_bytes());
                self.big_sync_store
                    .ensure_part(group_part_id)
                    .await
                    .map_err(|error| ferr!("failed ensuring group partition: {error}"))?;
                self.big_sync_store
                    .add_obj_to_parts(doc_id, vec![group_part_id])
                    .await
                    .map_err(|error| ferr!("failed adding document to group partition: {error}"))?;
            }
            Ok(())
        })
    }

    // ── sync_keyhive_with_peer ────────────────────────────────────────────
    fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            match self
                .keyhive_protocol
                .initiate_sync_with_request(&kh_peer_id, request_id)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) if error.to_string().contains("unknown peer") => {
                    tracing::debug!(%peer_id, "dropping keyhive sync after peer teardown");
                    Ok(())
                }
                Err(error) => Err(ferr!("keyhive initiate_sync_with_peer failed: {error}")),
            }
        })
    }

    // ── refresh_keyhive_cache ─────────────────────────────────────────────
    fn refresh_keyhive_cache(
        &self,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            self.keyhive_protocol
                .refresh_cache()
                .await
                .wrap_err("keyhive cache refresh failed")
        })
    }

    // ── compact_keyhive ───────────────────────────────────────────────────
    fn compact_keyhive(
        &self,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let archive_id =
                subduction_keyhive::storage::StorageHash::new(*self.local_peer_id.as_bytes());
            self.keyhive_protocol
                .compact(archive_id)
                .await
                .wrap_err("keyhive archive compact failed")
        })
    }

    // ── sync_doc_with_peer ────────────────────────────────────────────────
    fn sync_doc_with_peer(
        &self,
        sed_id: SedimentreeId,
        peer_id: PeerId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, eyre::Result<SyncDocAttempt>> {
        Sendable::from_future(async move {
            let remote_peer_id = subduction_core::peer::id::PeerId::new(*peer_id.as_bytes());
            let result = self
                .subduction
                .sync_with_peer(
                    &remote_peer_id,
                    sed_id,
                    false,
                    subduction_core::timeout::call::CallTimeout::Default,
                )
                .await;

            match result {
                Ok((had_success, stats, conn_errs)) => {
                    if had_success {
                        Ok(SyncDocAttempt::Exchanged)
                    } else if let Some(rejection) = stats.remote_rejection {
                        Ok(match rejection {
                            subduction_core::sync_session::SyncRemoteRejection::NotFound => {
                                SyncDocAttempt::NotFound
                            }
                            subduction_core::sync_session::SyncRemoteRejection::Unauthorized => {
                                SyncDocAttempt::Unauthorized
                            }
                            subduction_core::sync_session::SyncRemoteRejection::Policy(kind) => {
                                SyncDocAttempt::Policy(kind)
                            }
                        })
                    } else if conn_errs.is_empty() {
                        // Sync completed with no content exchanged and no errors.
                        Ok(SyncDocAttempt::NotFound)
                    } else {
                        Err(ferr!("doc sync transport errors: {conn_errs:?}"))
                    }
                }
                Err(err) => Err(ferr!("doc sync failed: {err}")),
            }
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TransportConnect (Iroh native)
// ═══════════════════════════════════════════════════════════════════════════

/// Concrete [`TransportConnect`] for Iroh, using the old runtime's
/// [`connect_outgoing`] / [`accept_incoming`] helpers.
///
/// Owns the subduction handle, signer, nonce cache, ephemeral backend,
/// and keyhive protocol so that every `connect` / `accept` can register
/// the authenticated connection with all three subsystems before returning
/// the peer identity and connection lifecycle watcher.
pub(crate) struct IrohTransportConnect<S>
where
    S: BigRepoSubductionStorage,
{
    pub(crate) subduction: Arc<BigRepoSubduction<S>>,
    pub(crate) signer: subduction_crypto::signer::memory::MemorySigner,
    pub(crate) nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    pub(crate) local_peer_id: PeerId,
    pub(crate) ephemeral_backend: Arc<dyn BigEphemeralBackend>,
    pub(crate) keyhive_protocol: BigRepoKeyhiveProtocol,
}

impl<S> std::fmt::Debug for IrohTransportConnect<S>
where
    S: BigRepoSubductionStorage,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrohTransportConnect")
            .field("local_peer_id", &self.local_peer_id)
            .finish_non_exhaustive()
    }
}

impl<S> crate::runtime2::TransportConnect<Sendable> for IrohTransportConnect<S>
where
    S: BigRepoSubductionStorage,
{
    fn connect(
        &self,
        expected_peer: PeerId,
        addr_blob: Box<dyn std::any::Any + Send>,
    ) -> <Sendable as FutureForm>::Future<
        'static,
        eyre::Result<(
            PeerId,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            <Sendable as FutureForm>::Future<'static, eyre::Result<()>>,
        )>,
    > {
        let subduction = Arc::clone(&self.subduction);
        let signer = self.signer.clone();
        let ephemeral_backend = Arc::clone(&self.ephemeral_backend);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        Sendable::from_future(async move {
            let (endpoint, endpoint_addr): (iroh::Endpoint, iroh::EndpointAddr) = *addr_blob
                .downcast::<(iroh::Endpoint, iroh::EndpointAddr)>()
                .map_err(|_| ferr!("addr_blob must be (iroh::Endpoint, iroh::EndpointAddr)"))?;

            let result: IrohConnectResult = connect_outgoing_to(
                endpoint,
                endpoint_addr,
                &signer,
                subduction_core::handshake::audience::Audience::known(
                    subduction_core::peer::id::PeerId::new(*expected_peer.as_bytes()),
                ),
            )
            .await?;
            let peer_id = PeerId::new(*result.authenticated.peer_id().as_bytes());

            // Register with subduction.
            subduction
                .add_connection(result.authenticated.clone())
                .await
                .map_err(|err| ferr!("subduction add_connection failed: {err}"))?;

            // Register with ephemeral backend.
            ephemeral_backend
                .subscribe_peer(subduction_core::peer::id::PeerId::new(*peer_id.as_bytes()))
                .await;

            // Register with keyhive protocol.
            let adapter = BigRepoKeyhiveConnAdapter::<BigRepoIrohTransport>::new(
                result.authenticated.clone(),
            );
            keyhive_protocol.add_peer(adapter.peer_id(), adapter).await;

            let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

            // End future: race listener_task and sender_task. The first to
            // complete signals connection loss.
            let listener = result.listener_task;
            let sender = result.sender_task;
            let closed_end = std::sync::Arc::clone(&closed);
            let end_fut: <Sendable as FutureForm>::Future<'static, eyre::Result<()>> =
                Sendable::from_future(async move {
                    use futures::future::{select, Either};
                    match select(
                        Box::pin(async { listener.await.map_err(|e| eyre::eyre!("{e}")) }),
                        Box::pin(async { sender.await.map_err(|e| eyre::eyre!("{e}")) }),
                    )
                    .await
                    {
                        Either::Left((res, _)) | Either::Right((res, _)) => {
                            closed_end.store(true, std::sync::atomic::Ordering::SeqCst);
                            res
                        }
                    }
                });

            Ok((peer_id, closed, end_fut))
        })
    }

    fn accept(
        &self,
        incoming: Box<dyn std::any::Any + Send>,
    ) -> <Sendable as FutureForm>::Future<
        'static,
        eyre::Result<(
            PeerId,
            std::sync::Arc<std::sync::atomic::AtomicBool>,
            <Sendable as FutureForm>::Future<'static, eyre::Result<()>>,
        )>,
    > {
        let subduction = Arc::clone(&self.subduction);
        let signer = self.signer.clone();
        let nonce_cache = Arc::clone(&self.nonce_cache);
        let local_peer_id = self.local_peer_id;
        let ephemeral_backend = Arc::clone(&self.ephemeral_backend);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        Sendable::from_future(async move {
            let conn: iroh::endpoint::Connection = *incoming
                .downcast::<iroh::endpoint::Connection>()
                .map_err(|_| ferr!("incoming must be iroh::endpoint::Connection"))?;

            let subduction_peer_id =
                subduction_core::peer::id::PeerId::new(*local_peer_id.as_bytes());
            let result: IrohConnectResult =
                accept_incoming(conn, &signer, nonce_cache.as_ref(), subduction_peer_id).await?;
            let peer_id = PeerId::new(*result.authenticated.peer_id().as_bytes());

            // Register with subduction.
            subduction
                .add_connection(result.authenticated.clone())
                .await
                .map_err(|err| ferr!("subduction add_connection failed: {err}"))?;

            // Register with ephemeral backend.
            ephemeral_backend
                .subscribe_peer(subduction_core::peer::id::PeerId::new(*peer_id.as_bytes()))
                .await;

            // Register with keyhive protocol.
            let adapter = BigRepoKeyhiveConnAdapter::<BigRepoIrohTransport>::new(
                result.authenticated.clone(),
            );
            keyhive_protocol.add_peer(adapter.peer_id(), adapter).await;

            let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

            // End future races listener_task and sender_task.
            let listener = result.listener_task;
            let sender = result.sender_task;
            let closed_end = std::sync::Arc::clone(&closed);
            let end_fut: <Sendable as FutureForm>::Future<'static, eyre::Result<()>> =
                Sendable::from_future(async move {
                    use futures::future::{select, Either};
                    match select(
                        Box::pin(async { listener.await.map_err(|e| eyre::eyre!("{e}")) }),
                        Box::pin(async { sender.await.map_err(|e| eyre::eyre!("{e}")) }),
                    )
                    .await
                    {
                        Either::Left((res, _)) | Either::Right((res, _)) => {
                            closed_end.store(true, std::sync::atomic::Ordering::SeqCst);
                            res
                        }
                    }
                });

            Ok((peer_id, closed, end_fut))
        })
    }

    fn close(
        &self,
        peer_id: PeerId,
    ) -> <Sendable as FutureForm>::Future<'static, eyre::Result<()>> {
        let subduction = Arc::clone(&self.subduction);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        Sendable::from_future(async move {
            // Tear down keyhive protocol for this peer.
            keyhive_protocol
                .remove_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
                .await;

            // Disconnect from subduction.
            let remote_peer_id = subduction_core::peer::id::PeerId::new(*peer_id.as_bytes());
            subduction
                .disconnect_from_peer(&remote_peer_id)
                .await
                .map_err(|err| ferr!("subduction disconnect_from_peer failed: {err}"))?;

            Ok(())
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// NATIVE RUNTIME SPAWN
// ═══════════════════════════════════════════════════════════════════════════

/// Adapter that bridges the old runtime's event format into the new runtime2
/// event channel. Only keyhive listener events (PrekeyExpanded, PrekeyRotated,
/// CgkaOp, DelegationReceived, RevocationReceived) are forwarded -- connection
/// lifecycle and sync-session events are handled directly by runtime2.
struct Runtime2EvtBridge {
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
}

impl subduction_core::sync_session::SyncSessionObserver for Runtime2EvtBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        if self
            .evt_tx
            .try_send(crate::runtime2::Runtime2Evt::SyncSessionObserved { session })
            .is_err()
        {
            tracing::warn!("runtime2 shutting down; dropping observed sync session");
        }
    }
}

/// Spawn a native runtime2 instance.
///
/// Builds the full subduction / keyhive / ephemeral infrastructure and feeds
/// it into [`spawn_runtime2`]. Long-lived tasks (subduction listener/manager,
/// keyhive maintenance, keyhive change subscription, listener event forwarding)
/// are spawned on the stop token's `child_tasks` so they are stopped before
/// the hub machine loop (reverse-order shutdown).
///
/// # Returns
///
/// - [`Runtime2Handle<Sendable>`] — public API handle.
/// - [`BigEphemeral`] — the ephemeral pub/sub bus.
/// - `async_channel::Sender<Runtime2Evt>` — sender for external event injection.
/// - [`Runtime2StopToken<Sendable, TokioTaskRuntime>`] — stop token.
pub async fn spawn_native_runtime2<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    big_sync_store: crate::SharedPartStore,
    policy: Arc<crate::runtime::BigRepoPolicy>,
    sync_policy: BigRepoSyncPolicy,
    keyhive: BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    listener_evt_rx: mpsc::UnboundedReceiver<crate::runtime::RuntimeEvt>,
    keyhive_change_tx: tokio::sync::broadcast::Sender<()>,
) -> eyre::Result<(
    crate::runtime2::Runtime2Handle<Sendable>,
    BigEphemeral,
    async_channel::Sender<crate::runtime2::Runtime2Evt>,
    crate::runtime2::Runtime2StopToken<Sendable, crate::runtime2::TokioTaskRuntime>,
)>
where
    S: BigRepoSubductionStorage,
{
    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());

    // ── Shared infrastructure ─────────────────────────────────────────────
    let sedimentrees: SubductionSedimentrees =
        Arc::new(subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new());
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let storage_powerbox: StoragePowerbox<S, crate::runtime::BigRepoPolicy> =
        subduction_core::storage::powerbox::StoragePowerbox::new(
            storage.clone(),
            Arc::clone(&policy),
        );

    // ── Sync handler (with sync session observer) ─────────────────────────
    let (evt_tx, evt_rx) = async_channel::unbounded::<crate::runtime2::Runtime2Evt>();

    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(Runtime2EvtBridge {
        evt_tx: evt_tx.clone(),
    });

    let sync_handler: Arc<crate::runtime::BigRepoSyncHandler<S>> = Arc::new(SyncHandler::new(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        CountLeadingZeroBytes,
        TokioSpawn,
    ));
    sync_handler.set_sync_session_observer(Arc::clone(&sync_session_observer));
    let send_counter = sync_handler.send_counter().clone();

    // ── Ephemeral handler / backend ───────────────────────────────────────
    let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
        Arc::clone(&connections),
        OpenEphemeralPolicy,
        EphemeralConfig::default(),
        StdClock,
    );
    let ephemeral_handler: Arc<BigRepoEphemeralHandler> = Arc::new(ephemeral_handler);
    let ephemeral_backend: Arc<dyn BigEphemeralBackend> = Arc::new(BigRepoEphemeralBackend::new(
        signer.clone(),
        Arc::clone(&ephemeral_handler),
    ));

    // ── Keyhive protocol and handler ──────────────────────────────────────
    let keyhive_protocol: BigRepoKeyhiveProtocol = Arc::new(
        subduction_keyhive::KeyhiveProtocol::new(
            keyhive.clone_keyhive(),
            keyhive_storage.clone(),
            keyhive.keyhive_peer_id(),
            keyhive.contact_card().clone(),
        )
        .with_storage_recovery(),
    );

    let mut keyhive_handler = BigRepoKeyhiveHandler::new(
        Arc::clone(&keyhive_protocol),
        BigRepoKeyhiveConnAdapter::<BigRepoIrohTransport>::new
            as fn(
                Authenticated<BigRepoIrohTransport, Sendable>,
            ) -> BigRepoKeyhiveConnAdapter<BigRepoIrohTransport>,
    );
    {
        let evt_tx = evt_tx.clone();
        keyhive_handler = keyhive_handler.with_sync_done_observer(Arc::new(
            move |keyhive_peer_id, request_id| {
                let peer_id = PeerId::new(*keyhive_peer_id.verifying_key());
                if evt_tx
                    .try_send(crate::runtime2::Runtime2Evt::KeyhiveSyncDone {
                        peer_id,
                        request_id,
                    })
                    .is_err()
                {
                    tracing::debug!(
                        %peer_id,
                        "runtime2 stopped before keyhive sync-done event"
                    );
                }
            },
        ));
    }

    // ── Composed handler ─────────────────────────────────────────────────
    let composed_handler = Arc::new(BigRepoComposedHandler::new(
        sync_handler,
        Some(Arc::clone(&ephemeral_handler)),
        keyhive_handler,
    ));

    // ── Subduction instance ──────────────────────────────────────────────
    let (subduction, listener, manager) = Subduction::new(
        composed_handler,
        None,
        connect_signer.clone(),
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        send_counter,
        NonceCache::new(sync_policy.subduction_nonce_ttl),
        TimeoutTokio,
        sync_policy.subduction_default_roundtrip_timeout,
        CountLeadingZeroBytes,
        TokioSpawn,
    );
    subduction.set_sync_session_observer(sync_session_observer);
    let subduction_handle: Arc<BigRepoSubduction<S>> = Arc::clone(&subduction);

    // ── IO facades ─────────────────────────────────────────────────────────
    let native_io = Arc::new(NativeBigRepoIo {
        subduction: Arc::clone(&subduction_handle),
        storage: storage.clone(),
        sedimentrees: Arc::clone(&sedimentrees),
        keyhive: keyhive.clone(),
        keyhive_storage: keyhive_storage.clone(),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        sync_policy,
        ephemeral_tasks: Arc::new(utils_rs::AbortableJoinSet::new()),
        ephemeral_backend: Arc::clone(&ephemeral_backend),
        keyhive_change_tx: keyhive_change_tx.clone(),
        big_sync_store,
    });

    let iroh_connect = Arc::new(IrohTransportConnect {
        subduction: Arc::clone(&subduction_handle),
        signer: connect_signer,
        nonce_cache: Arc::new(NonceCache::new(sync_policy.subduction_nonce_ttl)),
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        ephemeral_backend: Arc::clone(&ephemeral_backend),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
    });

    let timer: Arc<dyn crate::runtime2::Timer<Sendable>> =
        Arc::new(crate::runtime2::TokioTimer::default());
    let clock: Arc<dyn crate::runtime2::Clock> =
        Arc::new(subduction_ephemeral::clock::std_clock::StdClock);

    // ── Spawn runtime2 ───────────────────────────────────────────────────
    let config = crate::runtime2::Runtime2Config {
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        runtime_io: native_io.clone() as Arc<dyn crate::runtime2::RuntimeIo<Sendable>>,
        doc_io: Arc::clone(&native_io) as Arc<dyn crate::runtime2::DocIo<Sendable>>,
        sync_policy,
        change_manager: Arc::clone(&change_manager),
        tasks: crate::runtime2::TokioTaskRuntime,
        timer: Arc::clone(&timer),
        clock: Arc::clone(&clock),
        connect: iroh_connect as Arc<dyn crate::runtime2::TransportConnect<Sendable>>,
        event_channel: Some((evt_tx.clone(), evt_rx)),
    };

    let (handle, stop_token) =
        crate::runtime2::spawn_runtime2::<Sendable, crate::runtime2::TokioTaskRuntime>(config)?;

    // ── Background tasks (owned by child_tasks for reverse-order shutdown) ─

    // Subduction listener.
    stop_token.child_tasks.spawn({
        let listener = listener;
        Sendable::from_future(async move {
            let _ = listener.await.unwrap();
            Ok(())
        })
    })?;

    // Subduction manager.
    stop_token.child_tasks.spawn({
        let manager = manager;
        Sendable::from_future(async move {
            // manager only returns abort signal on Subduction drop.
            manager.await.ok();
            Ok(())
        })
    })?;

    // Keyhive maintenance (cache refresh + archive compact).
    {
        let kh_proto = Arc::clone(&keyhive_protocol);
        let peer_id = PeerId::new(*local_peer_id.as_bytes());
        let keyhive_archive_id = subduction_keyhive::storage::StorageHash::new(*peer_id.as_bytes());
        stop_token.child_tasks.spawn({
            let timer = Arc::clone(&timer);
            Sendable::from_future(async move {
                loop {
                    timer.sleep(std::time::Duration::from_secs(2)).await;
                    let _ = kh_proto
                        .refresh_cache()
                        .await
                        .map_err(|e| tracing::warn!(error = %e, "keyhive cache refresh failed"));
                    timer.sleep(std::time::Duration::from_secs(298)).await;
                    let _ = kh_proto
                        .compact(keyhive_archive_id)
                        .await
                        .map_err(|e| tracing::warn!(error = %e, "keyhive archive compact failed"));
                }
            })
        })?;
    }

    // BigEphemeral remains available for application-level transient topics.
    // Keyhive invalidations use the direct BigRepo RPC stream instead of this
    // relay-capable bus.
    let ephemeral = {
        let switchboard = BigEphemeralSwitchboard::spawn(
            Arc::clone(&ephemeral_backend),
            ephemeral_rx,
            tokio_util::sync::CancellationToken::new(),
            Arc::clone(&native_io.ephemeral_tasks),
        );
        BigEphemeral::new(Arc::clone(&ephemeral_backend), switchboard)
    };

    // Listener event forwarding: old RuntimeEvt -> Runtime2Evt.
    {
        let evt_tx = evt_tx.clone();
        stop_token
            .child_tasks
            .spawn(Sendable::from_future(async move {
                let mut rx = listener_evt_rx;
                while let Some(evt) = rx.recv().await {
                    let evt2 = match evt {
                        crate::runtime::RuntimeEvt::PrekeyExpanded { new_prekey } => {
                            crate::runtime2::Runtime2Evt::PrekeyExpanded { new_prekey }
                        }
                        crate::runtime::RuntimeEvt::PrekeyRotated { rotate_key } => {
                            crate::runtime2::Runtime2Evt::PrekeyRotated { rotate_key }
                        }
                        crate::runtime::RuntimeEvt::CgkaOp { data } => {
                            crate::runtime2::Runtime2Evt::CgkaOp { data }
                        }
                        crate::runtime::RuntimeEvt::DelegationReceived { target, data } => {
                            crate::runtime2::Runtime2Evt::DelegationReceived { target, data }
                        }
                        crate::runtime::RuntimeEvt::RevocationReceived { target, data } => {
                            crate::runtime2::Runtime2Evt::RevocationReceived { target, data }
                        }
                        // Other RuntimeEvt variants (connection lifecycle,
                        // sync sessions, doc-worker) are handled directly by
                        // runtime2's hub or TransportConnect — skip them.
                        _ => continue,
                    };
                    if evt_tx.send(evt2).await.is_err() {
                        break;
                    }
                }
                Ok(())
            }))?;
    }

    Ok((handle, ephemeral, evt_tx, stop_token))
}

// ═══════════════════════════════════════════════════════════════════════════
// Clock impl using StdClock (re-export for convenience)
// ═══════════════════════════════════════════════════════════════════════════

impl crate::runtime2::Clock for subduction_ephemeral::clock::std_clock::StdClock {
    fn now(&self) -> subduction_core::timestamp::TimestampSeconds {
        subduction_core::timestamp::TimestampSeconds::now()
    }

    fn instant(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}
