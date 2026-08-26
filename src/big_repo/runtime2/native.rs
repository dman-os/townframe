//! Native Sendable IO backend for runtime2.
//!
//! Provides a concrete [`DocIo`] + [`RuntimeIo`] impl backed by real
//! subduction storage and keyhive. Generic over subduction storage `S`
//! and connection type `C` (default: [`BigRepoIrohTransport`]).
//!
//! # Tokio boundary
//!
//! All methods are `async` and `Sendable`. Tokio type use (channels, tasks) is
//! confined to this backend; runtime2 actor code depends only on the trait
//! interfaces.
//!
//! [`BigRepoIrohTransport`]: crate::runtime2::support::BigRepoIrohTransport

use crate::interlude::*;
use crate::keyhive_storage::BigRepoKeyhiveStorage;
use crate::runtime2::support::BigRepoCiphertextLocator;
use crate::runtime2::{
    CausalDecryptResult, DocIo, KeyhiveSyncOutcome, MaterializationBlocker, RuntimeIo,
    SyncDocAttempt, TaskSet,
};
use crate::store::sqlite::KeyhiveIncorporationSink;

/// Period between archive/prune/WAL maintenance passes.
pub(crate) const KEYHIVE_MAINTENANCE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(300);
use crate::{
    BigEphemeral, BigKeyhiveHandle, DocumentId,
    encrypted_blob::decode_encrypted_blob,
    ephemeral::{BigEphemeralBackend, BigEphemeralSwitchboard, BigRepoEphemeralBackend},
    handler::{
        BigRepoComposedHandler, BigRepoEphemeralHandler, BigRepoKeyhiveHandler,
        BigRepoKeyhiveProtocol,
    },
    keyhive_conn::BigRepoKeyhiveConnAdapter,
    runtime2::support::{
        BigRepoIrohTransport, BigRepoSubduction, BigRepoSubductionStorage, IrohConnectResult,
        SubductionSedimentrees, accept_incoming, connect_outgoing_to, encrypt_fragment_blob,
        encrypt_loose_commit_with_update_op, encrypt_staged_automerge_ingest,
        persist_cgka_updates_durably, sedimentree_heads_payload,
    },
    runtime2::types::BigRepoSyncPolicy,
};
use keyhive_core::principal::document::{DecryptError, EncryptError};
use keyhive_core::{
    crypto::envelope::Envelope, principal::document::id::DocumentId as KhDocumentId,
    principal::identifier::Identifier, store::ciphertext::CiphertextStore,
};
use nonempty::NonEmpty;
use sedimentree_core::{
    blob::BlobMeta,
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::id::CommitId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_core::{
    authenticated::Authenticated, handler::sync::SyncHandler, nonce_cache::NonceCache,
    storage::powerbox::StoragePowerbox, subduction::Subduction,
    subduction::request::FragmentRequested,
};
use subduction_ephemeral::{
    clock::std_clock::StdClock, config::EphemeralConfig, handler::EphemeralHandler,
    policy::OpenEphemeralPolicy,
};
use subduction_keyhive::{KeyhiveConnection, KeyhivePeerId};
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};
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
    subduction: Arc<BigRepoSubduction<S>>,
    /// Clonable storage backend (for direct reads).
    storage: S,
    /// Durable metadata index used by Keyhive causal decryption to discover
    /// every ciphertext encrypted under a particular CGKA Update.
    causal_ciphertext_store: crate::store::sqlite::SqliteBigRepoStore,
    /// Shared sedimentree cache (minimized trees).
    sedimentrees: SubductionSedimentrees,
    /// Keyhive handle — document operations, content encryption.
    keyhive: BigKeyhiveHandle,
    /// Keyhive storage — CGKA ops, archives.
    keyhive_storage: BigRepoKeyhiveStorage,
    /// Keyhive protocol handle — sync initiation, cache refresh, compaction.
    keyhive_protocol: BigRepoKeyhiveProtocol,
    /// Local peer identity.
    local_peer_id: PeerId,
    /// Ownership for the legacy ephemeral switchboard task. Dropping the
    /// runtime2 hub drops this set and therefore shuts the switchboard down.
    ephemeral_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl<S> std::fmt::Debug for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeBigRepoIo")
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
    causal_index: Option<crate::store::sqlite::SqliteBigRepoStore>,
    sed_id: SedimentreeId,
    /// In-memory cache of (content_ref -> encrypted content).
    #[expect(clippy::type_complexity)]
    cache: std::sync::Mutex<
        HashMap<Vec<u8>, Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
    >,
}

impl<S: BigRepoSubductionStorage> NativeCiphertextStore<S> {
    fn new(
        storage: S,
        causal_index: Option<crate::store::sqlite::SqliteBigRepoStore>,
        sed_id: SedimentreeId,
    ) -> Self {
        Self {
            storage,
            causal_index,
            sed_id,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn contains_content(&self, content_ref: &[u8]) -> Res<bool> {
        // Check cache first.
        {
            let cache = self.cache.lock().expect(ERROR_MUTEX);
            if cache.contains_key(content_ref) {
                return Ok(true);
            }
        }
        let commit_id_bytes: [u8; 32] = content_ref
            .try_into()
            .map_err(|_| ferr!("content_ref must be 32 bytes, got {}", content_ref.len()))?;
        let commit_id = CommitId::new(commit_id_bytes);
        let (frag_res, loose_res) = tokio::join!(
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                &self.storage,
                self.sed_id,
                commit_id,
            ),
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                &self.storage,
                self.sed_id,
                commit_id,
            )
        );
        if frag_res
            .wrap_err("failed loading fragment for ciphertext")?
            .is_some()
            || loose_res
                .wrap_err("failed loading loose commit for ciphertext")?
                .is_some()
        {
            return Ok(true);
        }

        Ok(false)
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

        let commit_id_bytes: [u8; 32] = content_ref
            .try_into()
            .map_err(|_| ferr!("content_ref must be 32 bytes, got {}", content_ref.len()))?;
        let commit_id = CommitId::new(commit_id_bytes);
        // Prefer fragments: a fragment and its head loose commit can share a
        // content reference, but the fragment is the causally complete form.
        if let Some(verified) =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                &self.storage,
                self.sed_id,
                commit_id,
            )
            .await
            .wrap_err("failed loading fragment for ciphertext")?
        {
            let encrypted = decode_encrypted_blob(verified.blob().as_slice())
                .wrap_err("failed decoding fragment encrypted blob")?;
            let encrypted = Arc::new(encrypted);
            self.cache
                .lock()
                .expect(ERROR_MUTEX)
                .insert(content_ref.to_vec(), Arc::clone(&encrypted));
            return Ok(Some(encrypted));
        }
        // Fall back to the loose commit when no fragment exists.
        if let Some(verified) =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                &self.storage,
                self.sed_id,
                commit_id,
            )
            .await
            .wrap_err("failed loading loose commit for ciphertext")?
        {
            let encrypted = decode_encrypted_blob(verified.blob().as_slice())
                .wrap_err("failed decoding loose commit encrypted blob")?;
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
    ) -> <Sendable as FutureForm>::Future<
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
        pcs_update: &'a keyhive_crypto::digest::Digest<
            keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
        >,
    ) -> <Sendable as FutureForm>::Future<
        'a,
        std::result::Result<
            Vec<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        Sendable::from_future(async move {
            let Some(causal_index) = &self.causal_index else {
                return Ok(Vec::new());
            };
            let blobs = causal_index
                .causal_ciphertexts_by_pcs_update(self.sed_id, pcs_update.raw.as_bytes())
                .await?;
            blobs
                .into_iter()
                .map(|blob| {
                    decode_encrypted_blob(&blob)
                        .map(Arc::new)
                        .map_err(|error| ferr!("failed decoding indexed ciphertext: {error}"))
                })
                .collect()
        })
    }

    fn mark_decrypted<'a>(
        &'a self,
        _content_ref: &'a Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<'a, std::result::Result<(), Self::MarkDecryptedError>>
    {
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

// ═══════════════════════════════════════════════════════════════════════════
// DocIo<Sendable> impl
// ═══════════════════════════════════════════════════════════════════════════

impl<S> NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    /// Compute the sedimentree frontier from durable storage alone (ignoring
    /// the resident cache and the incrementally-maintained heads table).
    /// Debug-only divergence cross-check: see [`Self::sedimentree_heads`]'s
    /// cache-vs-durable detector. O(tree) — never call on a hot path.
    #[expect(dead_code)]
    async fn durable_sedimentree_heads_full(
        storage: &S,
        sed_id: SedimentreeId,
    ) -> eyre::Result<Vec<CommitId>> {
        let loose_commits =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit_metas(
                storage, sed_id,
            )
            .await
            .wrap_err("failed loading loose commits for heads")?;
        let fragments =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment_metas(
                storage, sed_id,
            )
            .await
            .wrap_err("failed loading fragments for heads")?;
        if loose_commits.is_empty() && fragments.is_empty() {
            return Ok(Vec::new());
        }
        let tree = MinimizedSedimentree::new(Sedimentree::new(fragments, loose_commits));
        Ok(sedimentree_heads_payload(&tree)
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect())
    }
}

impl<S> DocIo<Sendable> for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    fn persist_initial_document(
        &self,
        sed_id: SedimentreeId,
        staged: crate::runtime2::support::StagedAutomergeIngest,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let (sedimentree, blobs, cgka_ops, local_secrets) =
                encrypt_staged_automerge_ingest(&staged, &self.keyhive, sed_id)
                    .await
                    .wrap_err("failed encrypting initial sedimentree")?;
            if !cgka_ops.is_empty() {
                persist_cgka_updates_durably(
                    &self.keyhive_protocol,
                    &self.keyhive_storage,
                    cgka_ops,
                    local_secrets,
                )
                .await?;
            }
            self.subduction
                .store_sedimentree(sed_id, sedimentree, blobs)
                .await
                .wrap_err("failed storing initial sedimentree")?;
            Ok(())
        })
    }

    fn persist_local_commits(
        &self,
        sed_id: SedimentreeId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<BTreeSet<FragmentRequested>>> {
        Sendable::from_future(async move {
            let mut fragment_requests = BTreeSet::new();
            let mut batch_keys = HashMap::new();
            for (head, parents, blob) in commits {
                let (encrypted_blob, app_key, update_op, local_secret) =
                    encrypt_loose_commit_with_update_op(
                        &self.keyhive,
                        sed_id,
                        head,
                        &parents,
                        &blob,
                        &batch_keys,
                    )
                    .await
                    .map_err(|error| {
                        if error
                            .downcast_ref::<crate::runtime2::io::DocumentKeyUnavailable>()
                            .is_some()
                        {
                            error
                        } else {
                            ferr!("encrypt commit failed: {error}")
                        }
                    })?;
                batch_keys.insert(head, app_key);
                if let Some(update_op) = update_op {
                    persist_cgka_updates_durably(
                        &self.keyhive_protocol,
                        &self.keyhive_storage,
                        vec![update_op],
                        local_secret.into_iter().collect(),
                    )
                    .await?;
                }
                let (request, _heads) = self
                    .subduction
                    .store_commit(sed_id, head, parents, encrypted_blob)
                    .await
                    .wrap_err("failed store_commit")?;
                if let Some(request) = request {
                    assert!(
                        fragment_requests.insert(request),
                        "duplicate fragment request"
                    );
                }
            }
            Ok(fragment_requests)
        })
    }

    fn persist_causal_checkpoint(
        &self,
        sed_id: SedimentreeId,
        covered_frontier: BTreeSet<CommitId>,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<
            Option<(
                CommitId,
                crate::runtime2::support::CausalCheckpoint,
                Vec<CommitId>,
            )>,
        >,
    > {
        Sendable::from_future(async move {
            use crate::runtime2::support::{CausalCheckpoint, causal_checkpoint_id};

            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let keyhive = self.keyhive.clone_keyhive();
            let kh_doc = keyhive
                .get_document(kh_doc_id)
                .await
                .ok_or_else(|| ferr!("Keyhive document missing for causal checkpoint"))?;

            if keyhive
                .try_pcs_key_hash(Arc::clone(&kh_doc))
                .await
                .is_none()
            {
                let (update, local_secret) = match keyhive
                    .force_pcs_update(Arc::clone(&kh_doc))
                    .await
                {
                    Ok(update) => update,
                    Err(EncryptError::UnableToPcsUpdate(
                        beekem::error::CgkaError::IdentifierNotFound,
                    )) => {
                        debug!(
                            ?sed_id,
                            "causal checkpoint deferred: local principal is absent from the current CGKA tree"
                        );
                        return Ok(None);
                    }
                    Err(error) => {
                        return Err(ferr!("failed establishing checkpoint PCS root: {error}"));
                    }
                };
                persist_cgka_updates_durably(
                    &self.keyhive_protocol,
                    &self.keyhive_storage,
                    vec![update],
                    vec![local_secret],
                )
                .await?;
            }

            let pcs_key_hash = match keyhive.try_pcs_key_hash(Arc::clone(&kh_doc)).await {
                Some(pcs_key_hash) => pcs_key_hash,
                None => {
                    // A concurrent task rotated/forked the CGKA between our
                    // preparation and this verification (remote ops ingested
                    // via ApplySyncSession). Defer like the other concurrent-
                    // mutation paths; the racing op re-triggers coverage.
                    debug!(
                        ?sed_id,
                        "causal checkpoint deferred: PCS root unavailable after concurrent CGKA update"
                    );
                    return Ok(None);
                }
            };
            let checkpoint =
                CausalCheckpoint::new(*pcs_key_hash.raw.as_bytes(), covered_frontier.clone());
            let head = causal_checkpoint_id(&checkpoint);
            let plaintext = checkpoint.encode()?;
            let encrypted = encrypt_loose_commit_with_update_op(
                &self.keyhive,
                sed_id,
                head,
                &covered_frontier,
                &plaintext,
                &HashMap::new(),
            )
            .await;
            let (encrypted_blob, _app_key, update_op, local_secret) = match encrypted {
                Ok(encrypted) => encrypted,
                Err(error)
                    if error
                        .downcast_ref::<crate::runtime2::io::DocumentKeyUnavailable>()
                        .is_some() =>
                {
                    debug!(
                        ?sed_id,
                        ?covered_frontier,
                        "causal checkpoint deferred: frontier application key is unavailable"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            // A concurrent task may have mutated the document CGKA between our
            // PCS preparation and this encryption (e.g. an ApplySyncSession
            // ingesting remote ops). In that case the encryption performs its
            // own rotation; persist it like any other local CGKA update
            // instead of treating it as an invariant violation.
            if update_op.is_some() || local_secret.is_some() {
                debug!(
                    ?sed_id,
                    ?covered_frontier,
                    rotated = update_op.is_some(),
                    "causal checkpoint encryption rotated the prepared PCS root"
                );
                persist_cgka_updates_durably(
                    &self.keyhive_protocol,
                    &self.keyhive_storage,
                    update_op.into_iter().collect(),
                    local_secret.into_iter().collect(),
                )
                .await?;
            }
            let (fragment_request, heads_observed) = self
                .subduction
                .store_commit(sed_id, head, covered_frontier, encrypted_blob)
                .await
                .wrap_err("failed storing causal checkpoint")?;
            assert!(
                fragment_request.is_none(),
                "depth-zero causal checkpoint requested Automerge fragmentation"
            );
            Ok(Some((head, checkpoint, heads_observed)))
        })
    }

    fn current_causal_epoch(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<[u8; 32]>>> {
        Sendable::from_future(async move {
            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let keyhive = self.keyhive.clone_keyhive();
            let Some(kh_doc) = keyhive.get_document(kh_doc_id).await else {
                return Ok(None);
            };
            Ok(keyhive
                .try_pcs_key_hash(kh_doc)
                .await
                .map(|hash| *hash.raw.as_bytes()))
        })
    }

    fn ciphertext_epoch(
        &self,
        sed_id: SedimentreeId,
        head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<[u8; 32]>>> {
        Sendable::from_future(async move {
            let loose =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                    &self.storage,
                    sed_id,
                    head,
                )
                .await
                .map_err(|error| ferr!("failed loading frontier commit: {error}"))?;
            let raw = if let Some(commit) = loose {
                commit.blob().clone().into_contents()
            } else {
                let fragment =
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                        &self.storage,
                        sed_id,
                        head,
                    )
                    .await
                    .map_err(|error| ferr!("failed loading frontier fragment: {error}"))?;
                let Some(fragment) = fragment else {
                    return Ok(None);
                };
                fragment.blob().clone().into_contents()
            };
            let encrypted = decode_encrypted_blob(&raw)
                .map_err(|error| ferr!("failed decoding frontier ciphertext: {error}"))?;
            Ok(Some(*encrypted.pcs_key_hash.raw.as_bytes()))
        })
    }

    fn has_doc_write_access(
        &self,
        doc_id: crate::DocumentId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        Sendable::from_future(async move {
            let local_ident = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(self.local_peer_id.as_bytes())
                    .map_err(|_| ferr!("local peer id is not a valid verifying key"))?,
            );
            let doc_ident = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
                    .map_err(|_| ferr!("doc id is not a valid verifying key"))?,
            );
            let access = self.keyhive.agent_access_on(&local_ident, doc_ident).await;
            if access.is_some_and(|access| access.is_editor()) {
                return Ok(true);
            }
            // Public-member path: a doc that grants editor access to the
            // well-known Public agent may be written by anyone (the writer
            // encrypts through Public's well-known keys).
            let public_ident = keyhive_core::principal::public::Public.id();
            let public_access = self.keyhive.agent_access_on(&public_ident, doc_ident).await;
            Ok(public_access.is_some_and(|access| access.is_editor()))
        })
    }

    fn has_doc_fetch_access(
        &self,
        doc_id: crate::DocumentId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        Sendable::from_future(async move {
            let local_ident = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(self.local_peer_id.as_bytes())
                    .map_err(|_| ferr!("local peer id is not a valid verifying key"))?,
            );
            let doc_ident = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
                    .map_err(|_| ferr!("doc id is not a valid verifying key"))?,
            );
            let access = self.keyhive.agent_access_on(&local_ident, doc_ident).await;
            if access.is_some_and(|access| access.is_fetcher()) {
                return Ok(true);
            }
            let public_ident = keyhive_core::principal::public::Public.id();
            let public_access = self.keyhive.agent_access_on(&public_ident, doc_ident).await;
            Ok(public_access.is_some_and(|access| access.is_fetcher()))
        })
    }

    fn sedimentree_heads(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Vec<CommitId>>> {
        Sendable::from_future(async move {
            // Try sedimentree cache first. If it reports an impossible empty
            // frontier, compare it with durable storage before returning so a
            // future failure identifies the layer that lost the metadata.
            let empty_cached_counts =
                if let Some(tree) = self.sedimentrees.get_cloned(&sed_id).await {
                    let heads = sedimentree_heads_payload(&tree);
                    if !heads.is_empty() {
                        let cached: Vec<CommitId> =
                            heads.iter().map(|head| CommitId::new(head.0)).collect();
                        return Ok(cached);
                    }
                    Some((tree.loose_commits().count(), tree.fragments().count()))
                } else {
                    None
                };

            // Hydrate from storage: load loose commits + fragments, build tree.
            let loose_commits =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit_metas(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading loose commits for heads")?;
            let fragments =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment_metas(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading fragments for heads")?;

            if loose_commits.is_empty() && fragments.is_empty() {
                if let Some((cached_loose, cached_fragments)) = empty_cached_counts {
                    tracing::warn!(
                        ?sed_id,
                        cached_loose,
                        cached_fragments,
                        durable_loose = 0,
                        durable_fragments = 0,
                        "cached and durable Sedimentree both report an empty frontier"
                    );
                }
                return Ok(Vec::new());
            }

            let durable_loose = loose_commits.len();
            let durable_fragments = fragments.len();
            let tree = MinimizedSedimentree::new(Sedimentree::new(fragments, loose_commits));
            let tree = self.sedimentrees.get_or_insert_with(sed_id, || tree).await;
            let heads = sedimentree_heads_payload(&tree);
            if let Some((cached_loose, cached_fragments)) = empty_cached_counts {
                tracing::warn!(
                    ?sed_id,
                    cached_loose,
                    cached_fragments,
                    durable_loose,
                    durable_fragments,
                    durable_heads = heads.len(),
                    "cached Sedimentree reported empty heads; compared durable state"
                );
                return Ok(heads.iter().map(|head| CommitId::new(head.0)).collect());
            }
            Ok(heads.iter().map(|head| CommitId::new(head.0)).collect())
        })
    }

    fn durable_sedimentree_heads(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Vec<CommitId>>> {
        Sendable::from_future(async move {
            // Recompute the frontier from the authoritative SQLite rows
            // (metadata only, no blobs). The store's projection cache is
            // write-only and never consulted on read paths.
            self.causal_ciphertext_store
                .durable_sedimentree_heads(sed_id)
                .await
                .map_err(|err| ferr!("failed reading durable sedimentree heads: {err}"))
        })
    }

    fn hydrate_tree(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<MinimizedSedimentree>>> {
        Sendable::from_future(async move {
            // Check cache first.
            if let Some(tree) = self.sedimentrees.get_cloned(&sed_id).await {
                return Ok(Some(tree));
            }

            // Load from storage.
            let loose_commits =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit_metas(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading loose commits for hydrate")?;
            let fragments =
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment_metas(
                    &self.storage,
                    sed_id,
                )
                .await
                .wrap_err("failed loading fragments for hydrate")?;

            if loose_commits.is_empty() && fragments.is_empty() {
                return Ok(None);
            }

            let tree = MinimizedSedimentree::new(Sedimentree::new(fragments, loose_commits));

            // Keep the hydrated tree resident: materialization retries can be
            // frequent while Keyhive operations arrive, and the durable store
            // should not be rescanned for every retry.
            let tree = self.sedimentrees.get_or_insert_with(sed_id, || tree).await;
            Ok(Some(tree))
        })
    }

    fn store_fragment(
        &self,
        sed_id: SedimentreeId,
        head: CommitId,
        boundary: std::collections::BTreeSet<CommitId>,
        checkpoints: Vec<CommitId>,
        raw_blob: Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        Sendable::from_future(async move {
            let encrypted_blob = encrypt_fragment_blob(
                &self.keyhive,
                &self.storage,
                sed_id,
                head,
                &boundary,
                &raw_blob,
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

    fn try_decrypt_content_keyed(
        &self,
        sed_id: SedimentreeId,
        locator: BigRepoCiphertextLocator,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<Vec<u8>>>> {
        Sendable::from_future(async move {
            // Load the raw blob from storage.
            let raw = match locator.kind {
                crate::runtime2::support::BigRepoCiphertextKind::LooseCommit => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|err| ferr!("failed loading loose commit: {err}"))?
                    .map(|frag| frag.blob().clone().into_contents())
                }
                crate::runtime2::support::BigRepoCiphertextKind::Fragment => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|err| ferr!("failed loading fragment: {err}"))?
                    .map(|frag| frag.blob().clone().into_contents())
                }
            };
            let Some(raw) = raw else {
                return Ok(None);
            };

            // Decode the encrypted blob.
            let encrypted = decode_encrypted_blob(&raw)
                .map_err(|err| ferr!("failed decoding encrypted blob: {err}"))?;

            // A missing local Keyhive document means the content keys have
            // not arrived yet. This is a normal pending-materialization
            // state, not a fatal decryption error.
            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let Some(kh_doc) = self.keyhive.clone_keyhive().get_document(kh_doc_id).await else {
                return Ok(None);
            };
            let mut doc = kh_doc.lock().await;
            tracing::debug!(%sed_id, "decrypt: acquired kh_doc lock");
            match doc.try_decrypt_content_keyed(&encrypted) {
                Ok((plaintext, key)) => {
                    // Keep the recovered key available for a later local
                    // child commit's causal envelope.
                    doc.remember_decryption_key(encrypted.content_ref.clone(), key);
                    // Deserialize the envelope to extract the actual payload.
                    let envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&plaintext)
                        .map_err(|err| ferr!("bincode decrypt result: {err}"))?;
                    Ok(Some(envelope.plaintext))
                }
                Err(DecryptError::KeyNotFound) => Ok(None),
                Err(err) => Err(ferr!("decrypt failed: {err}")),
            }
        })
    }

    #[tracing::instrument(skip(self))]
    fn try_causal_decrypt(
        &self,
        sed_id: SedimentreeId,
        locator: BigRepoCiphertextLocator,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<CausalDecryptResult>> {
        let fut = async move {
            // A missing local Keyhive document means the content keys have
            // not arrived yet. Return an incomplete causal result so the doc
            // worker records PendingMaterialization and retries after the
            // next Keyhive sync.
            let kh_doc_id = kh_doc_id_from_sed_id(sed_id)?;
            let Some(kh_doc) = self.keyhive.clone_keyhive().get_document(kh_doc_id).await else {
                return Ok(CausalDecryptResult {
                    complete: Vec::new(),
                    blockers: vec![MaterializationBlocker::DocumentNotInHive],
                });
            };
            // Load the raw blob from storage.
            let raw = match locator.kind {
                crate::runtime2::support::BigRepoCiphertextKind::LooseCommit => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|err| ferr!("failed loading loose commit: {err}"))?
                    .map(|frag| frag.blob().clone().into_contents())
                }
                crate::runtime2::support::BigRepoCiphertextKind::Fragment => {
                    <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
                        &self.storage,
                        locator.sedimentree_id,
                        locator.commit_id,
                    )
                    .await
                    .map_err(|err| ferr!("failed loading fragment: {err}"))?
                    .map(|frag| frag.blob().clone().into_contents())
                }
            };
            let Some(raw) = raw else {
                return Ok(CausalDecryptResult {
                    complete: Vec::new(),
                    blockers: vec![MaterializationBlocker::MissingCiphertexts {
                        content_refs: vec![locator.commit_id.as_bytes().to_vec()],
                    }],
                });
            };

            // Decode the encrypted blob.
            let encrypted =
                decode_encrypted_blob(&raw).wrap_err("failed decoding encrypted blob")?;
            let key_tag = |key: &keyhive_crypto::symmetric_key::SymmetricKey| {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                hasher.write(b"townframe_key_tag_domain_spec_v1");
                key.as_slice().hash(&mut hasher);
                format!("{:016x}", hasher.finish())
            };

            // Set up a ciphertext store backed by our storage.
            let ct_store = NativeCiphertextStore::new(
                self.storage.clone(),
                Some(self.causal_ciphertext_store.clone()),
                sed_id,
            );
            tracing::debug!(%sed_id, "causal: before entrypoint kh_doc lock");
            let (entrypoint_raw, entrypoint_key) = {
                let mut doc = kh_doc.lock().await;
                tracing::debug!(%sed_id, "causal: acquired entrypoint kh_doc lock");
                match doc.try_decrypt_content_keyed(&encrypted) {
                    Ok((plaintext, key)) => {
                        tracing::debug!(
                            content_ref = ?encrypted.content_ref,
                            entry_key_id = %key_tag(&key),
                            "decrypted sedimentree entrypoint"
                        );
                        (plaintext, key)
                    }
                    Err(DecryptError::KeyNotFound) => {
                        tracing::warn!(
                            content_ref = ?encrypted.content_ref,
                            "missing entry encryption key while materializing"
                        );
                        return Ok(CausalDecryptResult {
                            complete: Vec::new(),
                            blockers: vec![MaterializationBlocker::MissingDocumentKeys {
                                content_refs: vec![encrypted.content_ref.clone()],
                            }],
                        });
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
            tracing::debug!(%sed_id, "causal: before causal kh_doc lock (async IO across lock)");
            let state = {
                let mut doc = kh_doc.lock().await;
                tracing::debug!(%sed_id, "causal: acquired causal kh_doc lock");
                doc.try_causal_decrypt_content(&encrypted, &ct_store)
                    .await
                    .map_err(|err| {
                        ferr!(
                            "causal decrypt failed; BigRepo envelope is not causally closed: {err}"
                        )
                    })?
            };
            tracing::debug!(%sed_id, "causal: released causal kh_doc lock");
            let mut missing_ciphertexts = Vec::new();
            let mut missing_keys = Vec::new();
            let complete_refs: std::collections::HashSet<_> = state
                .complete
                .iter()
                .map(|(content_ref, _)| content_ref)
                .collect();
            for ancestor_ref in entrypoint_envelope.ancestors.keys() {
                if complete_refs.contains(ancestor_ref) {
                    continue;
                }
                if ct_store.contains_content(ancestor_ref).await? {
                    missing_keys.push(ancestor_ref.clone());
                } else {
                    missing_ciphertexts.push(ancestor_ref.clone());
                }
            }
            if !missing_ciphertexts.is_empty() || !missing_keys.is_empty() {
                tracing::warn!(
                    content_ref = ?encrypted.content_ref,
                    ancestor_count = entrypoint_envelope.ancestors.len(),
                    missing_ciphertexts = ?missing_ciphertexts,
                    missing_keys = ?missing_keys,
                    "causal decrypt returned an incomplete ancestor closure"
                );
            }
            // The causal store returns the application keys it used, but the
            // generic Keyhive API does not persist them on the document. Keep
            // them there so a later local child commit can construct its own
            // causal envelope, including ancestors recovered through a grant.
            {
                let mut doc = kh_doc.lock().await;
                doc.remember_decryption_key(encrypted.content_ref.clone(), entrypoint_key);
                for (content_ref, key) in &state.keys {
                    tracing::debug!(
                        content_ref = ?content_ref,
                        key_id = %key_tag(key),
                        "remembering causal decryption key"
                    );
                    doc.remember_decryption_key(content_ref.clone(), *key);
                }
            }

            // Return the entrypoint first, followed by the decrypted causal
            // ancestors. Consumers can therefore materialize the exact blob
            // requested as well as its closure.
            let mut complete = vec![(encrypted.content_ref.clone(), entrypoint_envelope.plaintext)];
            for (content_ref, ciphertext_or_plaintext) in state.complete {
                if complete
                    .iter()
                    .any(|(known_ref, _)| known_ref == &content_ref)
                {
                    continue;
                }
                match bincode::deserialize::<'_, Envelope<Vec<u8>, Vec<u8>>>(
                    &ciphertext_or_plaintext,
                ) {
                    Ok(env) => {
                        complete.push((content_ref, env.plaintext));
                    }
                    Err(_) => {
                        complete.push((content_ref, ciphertext_or_plaintext));
                    }
                };
            }

            let mut blockers = Vec::new();
            if !missing_ciphertexts.is_empty() {
                blockers.push(MaterializationBlocker::MissingCiphertexts {
                    content_refs: missing_ciphertexts,
                });
            }
            if !missing_keys.is_empty() {
                blockers.push(MaterializationBlocker::MissingDocumentKeys {
                    content_refs: missing_keys,
                });
            }
            Ok(CausalDecryptResult { complete, blockers })
        };
        Sendable::from_future(fut)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RuntimeIo<Sendable> impl
// ═══════════════════════════════════════════════════════════════════════════

impl<S> RuntimeIo<Sendable> for NativeBigRepoIo<S>
where
    S: BigRepoSubductionStorage,
{
    fn create_document(
        &self,
        parents: Vec<crate::keyhive::BigKeyhiveAuthority>,
        content_heads: NonEmpty<[u8; 32]>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<DocumentId>> {
        Sendable::from_future(async move {
            let uuid = Uuid::new_v4();
            info!(%uuid, "creating doc");
            let (doc_id, _hashes) = self
                .keyhive
                .create_doc(parents, content_heads, &self.keyhive_protocol)
                .await?;
            info!(%uuid, ?doc_id, "created doc");
            Ok(doc_id)
        })
    }

    fn contains_sedimentree(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        Sendable::from_future(async move {
            self.storage
                .contains_sedimentree_id(sed_id)
                .await
                .map_err(|err| ferr!("failed checking sedimentree presence: {err}"))
        })
    }

    fn inspect_stored_doc_blobs(
        &self,
        sed_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Vec<Vec<u8>>>> {
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

    fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        request_id: subduction_keyhive::message::RequestId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<KeyhiveSyncOutcome>> {
        Sendable::from_future(async move {
            let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            match self
                .keyhive_protocol
                .initiate_sync_with_request(&kh_peer_id, request_id)
                .await
            {
                Ok(()) => Ok(KeyhiveSyncOutcome::Initiated),
                Err(subduction_keyhive::ProtocolError::UnknownPeer(_)) => {
                    tracing::debug!(%peer_id, "keyhive peer disappeared before sync initiation");
                    Ok(KeyhiveSyncOutcome::PeerDisappeared)
                }
                Err(error) => Err(ferr!("keyhive initiate_sync_with_peer failed: {error}")),
            }
        })
    }

    fn sync_doc_with_peer(
        &self,
        sed_id: SedimentreeId,
        peer_id: PeerId,
        request_id: Option<subduction_core::connection::message::RequestId>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<SyncDocAttempt>> {
        Sendable::from_future(async move {
            let doc_id = crate::DocumentId::new(*sed_id.as_bytes());
            match self.has_doc_fetch_access(doc_id).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(%doc_id, %peer_id, "early fail-fast sync_doc_with_peer: local Keyhive does not know the document (no fetch access)"
                    );
                    return Ok(SyncDocAttempt::Policy(
                        subduction_core::sync_session::SyncPolicyRejectionKind::DocumentNotFound,
                    ));
                }
                Err(err) => {
                    return Err(ferr!("has_doc_fetch_access error for doc {doc_id}: {err}"));
                }
            }
            let remote_peer_id = subduction_core::peer::id::PeerId::new(*peer_id.as_bytes());
            let result = self
                .subduction
                .sync_with_peer(
                    &remote_peer_id,
                    sed_id,
                    false,
                    subduction_core::timeout::call::CallTimeout::Default,
                    request_id,
                )
                .await;

            match result {
                Ok((had_success, stats, conn_errs)) => {
                    if let Some(rejection) = stats.local_policy_rejections.first() {
                        Ok(SyncDocAttempt::Policy(rejection.kind))
                    } else if had_success {
                        Ok(SyncDocAttempt::Exchanged)
                    } else if let Some(rejection) = stats.remote_rejection {
                        Ok(match rejection {
                            subduction_core::sync_session::SyncRemoteRejection::NotFound => {
                                SyncDocAttempt::NotFound
                            }
                            subduction_core::sync_session::SyncRemoteRejection::Unauthorized => {
                                SyncDocAttempt::Unauthorized
                            }
                        })
                    } else if conn_errs.is_empty() {
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
/// Per-peer keyhive-changes RPC subscription wiring (native/iroh only).
///
/// When present, every `connect` / `accept` starts a `SubscribeKeyhiveChanges`
/// RPC subscription for the peer and emits [`Runtime2Evt::KeyhiveChangeNotif`]
/// events to the hub, which triggers a waiter-less keyhive sync round. The
/// subscription is cancelled when its connection ends or a newer connection
/// for the same peer replaces it.
#[derive(Clone)]
struct KeyhiveNotifWiring {
    /// Hub event sender for `KeyhiveChangeNotif` triggers.
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    /// Cancel token per peer; a new connection supersedes the previous
    /// subscription for the same peer.
    cancels: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<PeerId, tokio_util::sync::CancellationToken>>,
    >,
}

pub(crate) struct IrohTransportConnect<S>
where
    S: BigRepoSubductionStorage,
{
    /// Keyhive-changes RPC subscription wiring; `None` disables notif-driven
    /// syncs for every connection of this transport.
    keyhive_notif: Option<KeyhiveNotifWiring>,
    pub(crate) subduction: Arc<BigRepoSubduction<S>>,
    pub(crate) signer: subduction_crypto::signer::memory::MemorySigner,
    pub(crate) nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    pub(crate) local_peer_id: PeerId,
    pub(crate) ephemeral_backend: Arc<dyn BigEphemeralBackend>,
    pub(crate) keyhive_protocol: BigRepoKeyhiveProtocol,
    /// Live authenticated connections keyed by their end flag. Subduction
    /// tracks multiple connections per peer, so closing one connection must
    /// disconnect exactly that connection — the flag's pointer identity is
    /// the connection id.
    #[expect(clippy::type_complexity)]
    conns: std::sync::Arc<
        std::sync::Mutex<
            Vec<(
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                Authenticated<BigRepoIrohTransport, Sendable>,
            )>,
        >,
    >,
    /// Which connection (by end flag) last registered the keyhive protocol
    /// adapter for each peer. `add_peer` overwrites per peer id, so the
    /// close path must only `remove_peer` when the closing connection is
    /// still the registered owner — otherwise a superseded connection's
    /// close would nuke the replacement's adapter and syncpoint.
    keyhive_adapter_owner: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashMap<KeyhivePeerId, std::sync::Arc<std::sync::atomic::AtomicBool>>,
        >,
    >,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl<S> std::fmt::Debug for IrohTransportConnect<S>
where
    S: BigRepoSubductionStorage,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohTransportConnect")
            .field("local_peer_id", &self.local_peer_id)
            .finish_non_exhaustive()
    }
}

/// Start the keyhive-changes RPC subscription for `peer_id` and emit
/// [`Runtime2Evt::KeyhiveChangeNotif`] events to the hub on every remote
/// change. Cancelled via `cancel` (connection end) or when a newer connection
/// for the same peer supersedes it. Best-effort: subscription failures are
/// logged, never fatal to the connection.
async fn spawn_keyhive_change_subscription(
    wiring: KeyhiveNotifWiring,
    peer_id: PeerId,
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    cancel: tokio_util::sync::CancellationToken,
    tasks: &utils_rs::AbortableJoinSet,
) {
    // Supersede any earlier subscription for the same peer (reconnect).
    let mut cancels = wiring.cancels.lock().await;
    if let Some(previous) = cancels.insert(peer_id, cancel.clone()) {
        previous.cancel();
    }
    drop(cancels);

    drop(tasks.spawn(async move {
        let client = crate::rpc::IrohBigRepoRpcClient::new(endpoint, endpoint_addr);
        let mut changes = match client.subscribe_keyhive_changes(64).await {
            Ok(changes) => changes,
            Err(error) => {
                tracing::warn!(%peer_id, ?error, "keyhive RPC subscription failed");
                return;
            }
        };
        // The first event confirms subscription readiness.
        match tokio::time::timeout(std::time::Duration::from_secs(5), changes.recv()).await {
            Ok(Ok(Some(event))) if event.initial => {}
            Ok(Ok(Some(_))) => {
                tracing::debug!(%peer_id, "keyhive RPC stream sent a non-initial event first");
            }
            Ok(Ok(None)) => {
                tracing::warn!(%peer_id, "keyhive RPC stream closed before readiness");
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(%peer_id, ?error, "keyhive RPC stream failed before readiness");
                return;
            }
            Err(_) => {
                tracing::warn!(%peer_id, "timed out waiting for keyhive RPC subscription readiness");
                return;
            }
        }

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                event = changes.recv() => {
                    match event {
                        Ok(Some(_)) => {
                            wiring
                                .evt_tx
                                .send(crate::runtime2::Runtime2Evt::KeyhiveChangeNotif { peer_id })
                                .await
                                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                                .ok();
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
        }
    }));
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
        let conns = Arc::clone(&self.conns);
        let keyhive_adapter_owner = Arc::clone(&self.keyhive_adapter_owner);
        let keyhive_notif = self.keyhive_notif.clone();
        let subscription_tasks = Arc::clone(&self.subscription_tasks);
        Sendable::from_future(async move {
            let (endpoint, endpoint_addr): (iroh::Endpoint, iroh::EndpointAddr) = *addr_blob
                .downcast::<(iroh::Endpoint, iroh::EndpointAddr)>()
                .map_err(|_| ferr!("addr_blob must be (iroh::Endpoint, iroh::EndpointAddr)"))?;
            let (rpc_endpoint, rpc_addr) = (endpoint.clone(), endpoint_addr.clone());

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
            let adapter_peer = adapter.peer_id();
            keyhive_protocol
                .add_peer(adapter_peer.clone(), adapter)
                .await;

            let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            // Register this connection in the close registry (keyed by its
            // end flag) so a later close can disconnect exactly it.
            conns
                .lock()
                .unwrap()
                .push((Arc::clone(&closed), result.authenticated.clone()));
            // This connection owns the peer's keyhive adapter (add_peer
            // overwrites per peer id) until the next connection re-registers.
            keyhive_adapter_owner
                .lock()
                .unwrap()
                .insert(adapter_peer, Arc::clone(&closed));
            // End future: race listener_task and sender_task. The first to
            // complete signals connection loss.
            let listener = result.listener_task;
            let sender = result.sender_task;
            let closed_end = std::sync::Arc::clone(&closed);
            let sub_cancel = if let Some(wiring) = &keyhive_notif {
                let cancel = tokio_util::sync::CancellationToken::new();
                spawn_keyhive_change_subscription(
                    wiring.clone(),
                    peer_id,
                    rpc_endpoint,
                    rpc_addr,
                    cancel.clone(),
                    &subscription_tasks,
                )
                .await;
                Some(cancel)
            } else {
                None
            };

            let end_fut_inner = Sendable::from_future(async move {
                use futures::future::{Either, select};
                match select(
                    Box::pin(async { listener.await.map_err(|err| eyre::eyre!("{err}")) }),
                    Box::pin(async { sender.await.map_err(|err| eyre::eyre!("{err}")) }),
                )
                .await
                {
                    Either::Left((res, _)) | Either::Right((res, _)) => {
                        closed_end.store(true, std::sync::atomic::Ordering::SeqCst);
                        // Natural end: drop the close-registry entry so
                        // a later close of this connection is a clean no-op.
                        conns
                            .lock()
                            .unwrap()
                            .retain(|(flag, _)| !Arc::ptr_eq(flag, &closed_end));
                        res
                    }
                }
            });
            let end_fut: <Sendable as FutureForm>::Future<'static, eyre::Result<()>> =
                Sendable::from_future(async move {
                    let result = end_fut_inner.await;
                    if let Some(cancel) = &sub_cancel {
                        cancel.cancel();
                    }
                    result
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
        let conns = Arc::clone(&self.conns);
        let keyhive_adapter_owner = Arc::clone(&self.keyhive_adapter_owner);
        let keyhive_notif = self.keyhive_notif.clone();
        let subscription_tasks = Arc::clone(&self.subscription_tasks);
        Sendable::from_future(async move {
            let (conn, rpc_endpoint): (iroh::endpoint::Connection, Option<iroh::Endpoint>) =
                *incoming
                    .downcast::<(iroh::endpoint::Connection, Option<iroh::Endpoint>)>()
                    .map_err(|_| {
                        ferr!(
                            "incoming must be (iroh::endpoint::Connection, Option<iroh::Endpoint>)"
                        )
                    })?;

            // Capture before `accept_incoming` consumes the connection: the
            // subscription needs the remote endpoint id to derive its address.
            let remote_endpoint_id = conn.remote_id();
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
            let adapter_peer = adapter.peer_id();
            keyhive_protocol
                .add_peer(adapter_peer.clone(), adapter)
                .await;

            let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            // Register this connection in the close registry (keyed by its
            // end flag) so a later close can disconnect exactly it.
            conns
                .lock()
                .unwrap()
                .push((Arc::clone(&closed), result.authenticated.clone()));
            // This connection owns the peer's keyhive adapter (add_peer
            // overwrites per peer id) until the next connection re-registers.
            keyhive_adapter_owner
                .lock()
                .unwrap()
                .insert(adapter_peer, Arc::clone(&closed));
            // End future races listener_task and sender_task.
            let listener = result.listener_task;
            let sender = result.sender_task;
            let closed_end = std::sync::Arc::clone(&closed);
            // Keyhive-changes RPC subscription for inbound connections: the
            // caller supplies the local endpoint (needed to dial the peer's
            // RPC server); the remote address is derived like the daybook
            // accept path via remote_info.
            let sub_cancel = if let (Some(wiring), Some(endpoint)) = (&keyhive_notif, rpc_endpoint)
            {
                let cancel = tokio_util::sync::CancellationToken::new();
                let remote_addr = endpoint.remote_info(remote_endpoint_id).await.map(|info| {
                    iroh::EndpointAddr::from_parts(
                        info.id(),
                        info.into_addrs().map(|info| info.into_addr()),
                    )
                });
                match remote_addr {
                    Some(remote_addr) => {
                        spawn_keyhive_change_subscription(
                            wiring.clone(),
                            peer_id,
                            endpoint.clone(),
                            remote_addr,
                            cancel.clone(),
                            &subscription_tasks,
                        )
                        .await;
                        Some(cancel)
                    }
                    None => {
                        tracing::debug!(
                            %peer_id,
                            "skipping keyhive RPC subscription: remote info unavailable"
                        );
                        None
                    }
                }
            } else {
                None
            };

            let end_fut_inner = Sendable::from_future(async move {
                use futures::future::{Either, select};
                match select(
                    Box::pin(async { listener.await.map_err(|err| eyre::eyre!("{err}")) }),
                    Box::pin(async { sender.await.map_err(|err| eyre::eyre!("{err}")) }),
                )
                .await
                {
                    Either::Left((res, _)) | Either::Right((res, _)) => {
                        closed_end.store(true, std::sync::atomic::Ordering::SeqCst);
                        // Natural end: drop the close-registry entry so
                        // a later close of this connection is a clean no-op.
                        conns
                            .lock()
                            .unwrap()
                            .retain(|(flag, _)| !Arc::ptr_eq(flag, &closed_end));
                        res
                    }
                }
            });
            let end_fut: <Sendable as FutureForm>::Future<'static, eyre::Result<()>> =
                Sendable::from_future(async move {
                    let result = end_fut_inner.await;
                    if let Some(cancel) = &sub_cancel {
                        cancel.cancel();
                    }
                    result
                });

            Ok((peer_id, closed, end_fut))
        })
    }

    fn close(
        &self,
        peer_id: PeerId,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> <Sendable as FutureForm>::Future<
        'static,
        eyre::Result<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>>,
    > {
        let subduction = Arc::clone(&self.subduction);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        let conns = Arc::clone(&self.conns);
        let keyhive_adapter_owner = Arc::clone(&self.keyhive_adapter_owner);
        Sendable::from_future(async move {
            let peer_keyhive = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            let owns_adapter = keyhive_adapter_owner
                .lock()
                .expect(ERROR_MUTEX)
                .get(&peer_keyhive)
                .is_some_and(|owner| std::sync::Arc::ptr_eq(owner, &closed));

            // Disconnect exactly the transport represented by this handle.
            // Subduction removes the connection's paired multiplexer without
            // disturbing other live connections to the same peer.
            let mut auth = None;
            {
                let mut guard = conns.lock().expect(ERROR_MUTEX);
                guard.retain(|(flag, auth_conn)| {
                    if std::sync::Arc::ptr_eq(flag, &closed) {
                        auth = Some(auth_conn.clone());
                        false
                    } else {
                        true
                    }
                });
            }
            if let Some(auth) = auth {
                subduction
                    .disconnect(&auth)
                    .await
                    .map_err(|err| ferr!("subduction disconnect failed: {err}"))?;
            }

            // The Keyhive protocol has one adapter per peer. Closing an older
            // connection must not remove the adapter installed by its replacement.
            // If the owning connection closes while another transport remains,
            // immediately install that transport as the protocol adapter.
            let mut replacement = None;
            if owns_adapter {
                let fallback = conns
                    .lock()
                    .expect(ERROR_MUTEX)
                    .iter()
                    .rev()
                    .find(|(_, auth)| auth.peer_id().as_bytes() == peer_id.as_bytes())
                    .map(|(flag, auth)| (Arc::clone(flag), auth.clone()));
                if let Some((fallback_flag, fallback_auth)) = fallback {
                    let adapter =
                        BigRepoKeyhiveConnAdapter::<BigRepoIrohTransport>::new(fallback_auth);
                    keyhive_protocol
                        .add_peer(peer_keyhive.clone(), adapter)
                        .await;
                    keyhive_adapter_owner
                        .lock()
                        .expect(ERROR_MUTEX)
                        .insert(peer_keyhive, Arc::clone(&fallback_flag));
                    replacement = Some(fallback_flag);
                } else {
                    keyhive_protocol.remove_peer(&peer_keyhive).await;
                    keyhive_adapter_owner
                        .lock()
                        .expect(ERROR_MUTEX)
                        .remove(&peer_keyhive);
                }
            }
            Ok(replacement)
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
            .try_send(crate::runtime2::Runtime2Evt::SyncSessionObserved {
                cause: tracing::Span::current(),
                session,
            })
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
#[expect(clippy::too_many_arguments)]
pub async fn spawn_native_runtime2<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    group_part_store: crate::store::sqlite::SqliteBigRepoStore,
    frontier_store: Arc<dyn big_sync::HostPartStore>,
    storage: S,
    policy: Arc<crate::runtime2::support::BigRepoPolicy>,
    sync_policy: BigRepoSyncPolicy,
    keyhive: BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
    change_manager: Arc<crate::changes::ChangeListenerManager>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    evt_rx: async_channel::Receiver<crate::runtime2::Runtime2Evt>,
    automerge_frontier_group_scope: crate::runtime2::WorkerGroupScope,
    causal_checkpoint_group_scope: crate::runtime2::WorkerGroupScope,
    group_part_group_scope: crate::runtime2::WorkerGroupScope,
) -> eyre::Result<(
    crate::runtime2::Runtime2Handle<Sendable>,
    BigEphemeral,
    BigRepoKeyhiveProtocol,
    crate::runtime2::keyhive_dispatcher::KeyhiveChangeDispatcher,
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
    let storage_powerbox: StoragePowerbox<S, crate::runtime2::support::BigRepoPolicy> =
        subduction_core::storage::powerbox::StoragePowerbox::new(
            storage.clone(),
            Arc::clone(&policy),
        );

    // ── Sync handler (with sync session observer) ─────────────────────────
    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(Runtime2EvtBridge {
        evt_tx: evt_tx.clone(),
    });

    let sync_handler: Arc<crate::runtime2::support::BigRepoSyncHandler<S>> =
        Arc::new(SyncHandler::new(
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
        TokioSpawn,
    );
    let ephemeral_handler: Arc<BigRepoEphemeralHandler> = Arc::new(ephemeral_handler);
    let ephemeral_backend: Arc<dyn BigEphemeralBackend> = Arc::new(BigRepoEphemeralBackend::new(
        signer.clone(),
        Arc::clone(&ephemeral_handler),
    ));

    // ── Keyhive protocol and handler ──────────────────────────────────────
    // The incorporation sink is awaited inline: the exchange cannot complete
    // until the durable incorporation record commits. The same sink answers
    // boot's WAL-vs-admission diff for crash-window reconciliation.
    let (keyhive_events_tx, keyhive_events_rx) = tokio::sync::mpsc::channel(1024);
    let keyhive_reporter_weak = keyhive_events_tx.downgrade();
    let incorporation_sink = KeyhiveIncorporationSink::new(
        group_part_store.clone(),
        evt_tx.clone(),
        keyhive_reporter_weak,
    );
    let keyhive_protocol: BigRepoKeyhiveProtocol = Arc::new(
        subduction_keyhive::KeyhiveProtocol::new(
            keyhive.clone_keyhive(),
            keyhive_storage.clone(),
            keyhive.keyhive_peer_id(),
            keyhive.contact_card().clone(),
        )
        .with_storage_recovery()
        .with_durable_incorporation_sink(Arc::new(incorporation_sink.clone())),
    );
    keyhive_protocol
        .ingest_from_storage()
        .await
        .map_err(|error| ferr!("failed recovering keyhive event WAL: {error}"))?;
    // One dispatcher owns the debounced, classified fan-out of keyhive change
    // hints to subscribed peers. It stops when the events channel closes
    // (BigRepo drop) and is aborted on runtime shutdown (spawned on
    // `child_tasks` below, reverse-order with the other workers).
    let keyhive_dispatcher_subscriptions: crate::runtime2::keyhive_dispatcher::SubscriptionMap =
        Arc::new(surelock::mutex::Mutex::new(std::collections::HashMap::new()));
    let (keyhive_dispatcher, spawned_keyhive_dispatcher) =
        crate::runtime2::keyhive_dispatcher::spawn_keyhive_dispatcher(
            Arc::clone(&keyhive_protocol),
            group_part_store.clone(),
            keyhive_events_tx,
            keyhive_events_rx,
            keyhive_dispatcher_subscriptions,
            utils_rs::batching::DebouncePolicy {
                quiet_window: std::time::Duration::from_millis(100),
                max_latency: std::time::Duration::from_secs(1),
            },
        );
    let mut keyhive_handler = BigRepoKeyhiveHandler::new(
        Arc::clone(&keyhive_protocol),
        BigRepoKeyhiveConnAdapter::<BigRepoIrohTransport>::new
            as fn(
                Authenticated<BigRepoIrohTransport, Sendable>,
            ) -> BigRepoKeyhiveConnAdapter<BigRepoIrohTransport>,
    );
    {
        // Route sync completion through the same ordered event channel as
        // membership events. The Keyhive protocol invokes its sync observer
        // after applying events; sharing the channel with the keyhive listener
        // keeps `KeyhiveSyncDone` from overtaking a preceding delegation event
        // (single FIFO channel).
        let evt_tx = evt_tx.clone();
        keyhive_handler = keyhive_handler.with_sync_done_observer(Arc::new(
            move |keyhive_peer_id, request_id, changed| {
                let peer_id = PeerId::new(*keyhive_peer_id.verifying_key());
                if evt_tx
                    .try_send(crate::runtime2::Runtime2Evt::KeyhiveSyncDone {
                        peer_id,
                        request_id,
                        changed,
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
        causal_ciphertext_store: group_part_store.clone(),
        sedimentrees: Arc::clone(&sedimentrees),
        keyhive: keyhive.clone(),
        keyhive_storage: keyhive_storage.clone(),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        ephemeral_tasks: Arc::new(utils_rs::AbortableJoinSet::new()),
    });

    let iroh_connect = Arc::new(IrohTransportConnect {
        subduction: Arc::clone(&subduction_handle),
        signer: connect_signer,
        nonce_cache: Arc::new(NonceCache::new(sync_policy.subduction_nonce_ttl)),
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        ephemeral_backend: Arc::clone(&ephemeral_backend),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
        conns: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        keyhive_adapter_owner: std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        subscription_tasks: Arc::new(utils_rs::AbortableJoinSet::new()),
        keyhive_notif: Some(KeyhiveNotifWiring {
            evt_tx: evt_tx.clone(),
            cancels: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }),
    });

    let timer: Arc<dyn crate::runtime2::Timer<Sendable>> = Arc::new(crate::runtime2::TokioTimer);
    let clock: Arc<dyn crate::runtime2::Clock> =
        Arc::new(subduction_ephemeral::clock::std_clock::StdClock);

    // ── Spawn runtime2 ───────────────────────────────────────────────────
    let config = crate::runtime2::Runtime2Config {
        local_peer_id: PeerId::new(*local_peer_id.as_bytes()),
        runtime_io: Arc::clone(&native_io) as Arc<dyn crate::runtime2::RuntimeIo<Sendable>>,
        doc_io: Arc::clone(&native_io) as Arc<dyn crate::runtime2::DocIo<Sendable>>,
        sync_policy,
        change_manager: Arc::clone(&change_manager),
        tasks: crate::runtime2::TokioTaskRuntime,
        timer: Arc::clone(&timer),
        clock: Arc::clone(&clock),
        connect: iroh_connect as Arc<dyn crate::runtime2::TransportConnect<Sendable>>,
        event_channel: Some((evt_tx.clone(), evt_rx)),
    };

    let (handle, mut stop_token) =
        crate::runtime2::spawn_runtime2::<Sendable, crate::runtime2::TokioTaskRuntime>(config)?;

    // ── Background tasks (owned by child_tasks for reverse-order shutdown) ─

    // Subduction listener.
    let spawned_group_part = crate::runtime2::spawn_group_part_worker(
        group_part_store.clone(),
        keyhive.clone(),
        PeerId::new(*local_peer_id.as_bytes()),
        Arc::clone(&timer),
        evt_tx.clone(),
        group_part_group_scope,
    );
    stop_token.group_part_stop = Some(spawned_group_part.stop);
    stop_token.child_tasks.spawn(spawned_group_part.run)?;

    let spawned_causal_checkpoint = crate::runtime2::spawn_causal_checkpoint_worker(
        group_part_store.clone(),
        keyhive.clone(),
        handle.clone(),
        Arc::clone(&timer),
        evt_tx.clone(),
        causal_checkpoint_group_scope,
    );
    stop_token.causal_checkpoint_stop = Some(spawned_causal_checkpoint.stop);
    stop_token
        .child_tasks
        .spawn(spawned_causal_checkpoint.run)?;

    let spawned_automerge_frontier = crate::runtime2::spawn_automerge_frontier_worker(
        group_part_store.clone(),
        Arc::new(group_part_store.clone()) as Arc<dyn big_sync::HostPartStore>,
        frontier_store,
        handle.clone(),
        evt_tx.clone(),
        keyhive.clone(),
        automerge_frontier_group_scope,
    );
    stop_token.automerge_frontier_stop = Some(spawned_automerge_frontier.stop);
    stop_token
        .child_tasks
        .spawn(spawned_automerge_frontier.run)?;

    stop_token.child_tasks.spawn({
        let listener = listener;
        Sendable::from_future(async move {
            listener.await.unwrap();
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

    // Keyhive maintenance: cache warming and archive compaction have
    // independent schedules. Cache refresh is also single-flight and is
    // triggered on demand by the protocol when a sync needs fresh state.
    {
        let kh_proto = Arc::clone(&keyhive_protocol);
        stop_token.child_tasks.spawn({
            let timer = Arc::clone(&timer);
            Sendable::from_future(async move {
                loop {
                    timer.sleep(std::time::Duration::from_secs(2)).await;
                    kh_proto
                        .refresh_cache()
                        .await
                        .map_err(|error| ferr!("keyhive cache refresh failed: {error}"))?;
                }
            })
        })?;
    }
    {
        let kh_proto = Arc::clone(&keyhive_protocol);
        let store = group_part_store.clone();
        let keyhive_archive_id = subduction_keyhive::storage::StorageHash::new(
            *keyhive.keyhive_peer_id().verifying_key(),
        );
        stop_token.child_tasks.spawn({
            let timer = Arc::clone(&timer);
            Sendable::from_future(async move {
                loop {
                    timer.sleep(KEYHIVE_MAINTENANCE_INTERVAL).await;
                    let result = async {
                        kh_proto
                            .compact(keyhive_archive_id)
                            .await
                            .map_err(|error| ferr!("keyhive archive compaction failed: {error}"))?;
                        let watermark = store.keyhive_event_log_cursor().await?;
                        store.set_archived_through(watermark).await?;
                        store.run_maintenance().await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(%error, "keyhive SQLite maintenance failed; continuing");
                    }
                }
            })
        })?;
    }

    // Keyhive change dispatcher: spawned last on child_tasks so reverse-order
    // shutdown stops it first (its stop token is cancelled before the task set
    // is aborted). The task set's spawn unwraps the dispatcher's result, so an
    // unexpected error or panic brings down the process.
    stop_token.keyhive_dispatcher_stop = Some(spawned_keyhive_dispatcher.stop);
    stop_token
        .child_tasks
        .spawn(spawned_keyhive_dispatcher.run)?;

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

    Ok((
        handle,
        ephemeral,
        keyhive_protocol,
        keyhive_dispatcher,
        stop_token,
    ))
}

// ═══════════════════════════════════════════════════════════════════════════
// Clock impl using StdClock (re-export for convenience)
// ═══════════════════════════════════════════════════════════════════════════

impl crate::runtime2::Clock for subduction_ephemeral::clock::std_clock::StdClock {
    fn instant(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BigKeyhiveHandle;
    use crate::keyhive_listener::BigRepoKeyhiveListener;
    use crate::keyhive_storage::BigRepoKeyhiveStorage;
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_core::store::ciphertext::CiphertextStore;
    use sedimentree_core::blob::verified::VerifiedBlobMeta;
    use sedimentree_core::fragment::Fragment;
    use sedimentree_core::loose_commit::LooseCommit;
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use subduction_core::storage::memory::MemoryStorage;
    use subduction_core::storage::traits::Storage;
    use subduction_crypto::signer::memory::MemorySigner;
    use subduction_crypto::verified_meta::VerifiedMeta;

    fn kh_listener() -> (
        BigRepoKeyhiveListener,
        async_channel::Receiver<crate::runtime2::Runtime2Evt>,
    ) {
        let (evt_tx, evt_rx) = async_channel::unbounded();
        (
            BigRepoKeyhiveListener {
                evt_tx,
                storage: BigRepoKeyhiveStorage::memory(),
            },
            evt_rx,
        )
    }

    fn kh_protocol(
        keyhive: &BigKeyhiveHandle,
        storage: &BigRepoKeyhiveStorage,
    ) -> BigRepoKeyhiveProtocol {
        Arc::new(subduction_keyhive::KeyhiveProtocol::new(
            keyhive.clone_keyhive(),
            storage.clone(),
            keyhive.keyhive_peer_id(),
            keyhive.contact_card().clone(),
        ))
    }

    #[tokio::test]
    async fn ciphertext_store_durable_after_mark_decrypted() -> eyre::Result<()> {
        let (listener, _evt_rx) = kh_listener();
        let keyhive = BigKeyhiveHandle::new([9; 32], listener).await?;
        let kh_storage = BigRepoKeyhiveStorage::memory();
        let kh_protocol = kh_protocol(&keyhive, &kh_storage);
        let (doc_id, _hashes) = keyhive
            .create_doc(
                default(),
                nonempty::NonEmpty {
                    head: [1; 32],
                    tail: vec![],
                },
                &kh_protocol,
            )
            .await?;
        let sed_id = sedimentree_core::id::SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[42; 32]);

        let head = sedimentree_core::loose_commit::id::CommitId::new([7; 32]);
        let parents = BTreeSet::new();
        let (encrypted_blob, _app_key, _, _) =
            crate::runtime2::support::encrypt_loose_commit_with_update_op(
                &keyhive,
                sed_id,
                head,
                &parents,
                b"payload-bytes",
                &HashMap::new(),
            )
            .await?;

        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (sed_id, head, parents),
            VerifiedBlobMeta::new(encrypted_blob.clone()),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&storage, sed_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let ct_store = NativeCiphertextStore::new(storage.clone(), None, sed_id);
        let content_ref = head.as_bytes().to_vec();

        let indexed = ct_store
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        assert_eq!(indexed.content_ref, content_ref);

        ct_store
            .mark_decrypted(&head.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("mark_decrypted failed: {e}"))?;

        let after_mark = ct_store
            .get_ciphertext(&head.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("get_ciphertext after mark failed: {e}"))?
            .expect("ciphertext should remain loadable after mark_decrypted");
        assert_eq!(after_mark.content_ref, content_ref);
        Ok(())
    }

    #[tokio::test]
    async fn ciphertext_store_cache_identity() -> eyre::Result<()> {
        let (listener, _evt_rx) = kh_listener();
        let keyhive = BigKeyhiveHandle::new([14; 32], listener).await?;
        let kh_storage = BigRepoKeyhiveStorage::memory();
        let kh_protocol = kh_protocol(&keyhive, &kh_storage);
        let (doc_id, _hashes) = keyhive
            .create_doc(
                default(),
                nonempty::NonEmpty {
                    head: [1; 32],
                    tail: vec![],
                },
                &kh_protocol,
            )
            .await?;
        let sed_id = sedimentree_core::id::SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[47; 32]);

        let head = sedimentree_core::loose_commit::id::CommitId::new([17; 32]);
        let parents = BTreeSet::new();
        let (encrypted_blob, _app_key, _, _) =
            crate::runtime2::support::encrypt_loose_commit_with_update_op(
                &keyhive,
                sed_id,
                head,
                &parents,
                b"payload-bytes",
                &HashMap::new(),
            )
            .await?;

        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (sed_id, head, parents),
            VerifiedBlobMeta::new(encrypted_blob),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&storage, sed_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let ct_store = NativeCiphertextStore::new(storage.clone(), None, sed_id);
        let content_ref = head.as_bytes().to_vec();

        let first = ct_store
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("first get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        let second = ct_store
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("second get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");

        assert!(
            Arc::ptr_eq(&first, &second),
            "repeated lookup must return the same Arc"
        );
        Ok(())
    }
    #[tokio::test]
    async fn ciphertext_store_prefers_fragment_over_loose_commit() -> eyre::Result<()> {
        let (listener, _evt_rx) = kh_listener();
        let keyhive = BigKeyhiveHandle::new([11; 32], listener).await?;
        let kh_storage = BigRepoKeyhiveStorage::memory();
        let kh_protocol = kh_protocol(&keyhive, &kh_storage);
        let (doc_id, _hashes) = keyhive
            .create_doc(
                default(),
                nonempty::NonEmpty {
                    head: [1; 32],
                    tail: vec![],
                },
                &kh_protocol,
            )
            .await?;
        let sed_id = sedimentree_core::id::SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[44; 32]);
        let h1 = sedimentree_core::loose_commit::id::CommitId::new([7; 32]);
        let h2 = sedimentree_core::loose_commit::id::CommitId::new([8; 32]);
        let h1_parents = BTreeSet::new();
        let h2_parents = BTreeSet::from([h1]);
        let (h1_blob, h1_key, _, _) =
            crate::runtime2::support::encrypt_loose_commit_with_update_op(
                &keyhive,
                sed_id,
                h1,
                &h1_parents,
                b"parent-bytes",
                &HashMap::new(),
            )
            .await?;
        let h1_verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (sed_id, h1, h1_parents.clone()),
            VerifiedBlobMeta::new(h1_blob),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&storage, sed_id, h1_verified).await?;
        let (h2_blob, h2_key, _, _) =
            crate::runtime2::support::encrypt_loose_commit_with_update_op(
                &keyhive,
                sed_id,
                h2,
                &h2_parents,
                b"head-bytes",
                &HashMap::from([(h1, h1_key)]),
            )
            .await?;
        let h2_verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (sed_id, h2, h2_parents.clone()),
            VerifiedBlobMeta::new(h2_blob.clone()),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&storage, sed_id, h2_verified).await?;
        let fragment_blob = crate::runtime2::support::encrypt_fragment_blob(
            &keyhive,
            &storage,
            sed_id,
            h2,
            &h2_parents,
            b"head-bytes",
        )
        .await?;
        let fragment_verified = VerifiedMeta::<Fragment>::seal::<Sendable, _>(
            &signer,
            (sed_id, h2, h2_parents, vec![]),
            VerifiedBlobMeta::new(fragment_blob),
        )
        .await;
        Storage::<Sendable>::save_fragment(&storage, sed_id, fragment_verified).await?;
        let ct_store = NativeCiphertextStore::new(storage, None, sed_id);
        let encrypted = ct_store
            .get_ciphertext(&h2.as_bytes().to_vec())
            .await?
            .expect("fragment ciphertext should be found");
        let plaintext = encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting fragment: {e}"))?;
        let envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&plaintext)?;
        // Fragment compaction preserves the self-contained checkpoint payload
        // from its head loose commit; it must not replace it with a dependency-
        // incomplete Automerge fragment bundle.
        assert_eq!(envelope.plaintext, b"head-bytes");
        assert_eq!(envelope.ancestors.get(&h1.as_bytes()[..]), Some(&h1_key));
        Ok(())
    }
    #[tokio::test]
    async fn ciphertext_store_rejects_plaintext_blob() -> eyre::Result<()> {
        let sed_id = sedimentree_core::id::SedimentreeId::new([12; 32]);
        let head = sedimentree_core::loose_commit::id::CommitId::new([9; 32]);
        let parents = BTreeSet::new();
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[45; 32]);
        let plaintext = sedimentree_core::blob::Blob::new(b"plain commit bytes".to_vec());
        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (sed_id, head, parents),
            VerifiedBlobMeta::new(plaintext),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&storage, sed_id, verified)
            .await
            .wrap_err("save_loose_commit failed")?;
        let ct_store = NativeCiphertextStore::new(storage, None, sed_id);
        let error = ct_store
            .get_ciphertext(&head.as_bytes().to_vec())
            .await
            .expect_err("plaintext blob must not decode as ciphertext");
        assert!(
            error
                .to_string()
                .contains("failed decoding loose commit encrypted blob")
        );
        Ok(())
    }
}
