//! Support types, traits, type aliases, and helper functions that are still
//! imported by runtime2, its native backend, handler.rs, keyhive_conn.rs,
//! and ephemeral.rs.
//!
//! NOTE: we use bincode for the initial Envelope but this is safe/canonicalized
//! since it uses an ordered serializer internally

use crate::interlude::*;

use crate::{
    BigKeyhiveHandle,
    encrypted_blob::{decode_encrypted_blob, encode_encrypted_blob},
    keyhive_storage::BigRepoKeyhiveStorage,
};
use future_form::Sendable;
use futures::future::BoxFuture;
use keyhive_core::event::static_event::StaticEvent;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use std::collections::BTreeSet;
use std::time::Duration;
use subduction_core::{
    authenticated::Authenticated, collections::bounded_sharded_map::BoundedShardedMap,
    handler::sync::SyncHandler, storage::traits::Storage, subduction::Subduction,
    transport::message::MessageTransport,
};
use subduction_keyhive::message::EventHash;

// Re-exports that handler.rs / keyhive_conn.rs / ephemeral.rs / native.rs need.
// These type aliases are identical to the old `crate::runtime::*` definitions.
pub(crate) type SubductionSedimentrees =
    Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, 256>>;
pub(crate) type BigRepoIrohTransport = MessageTransport<subduction_iroh::transport::IrohTransport>;

pub(crate) type BigRepoPolicy = subduction_keyhive::policy::SubductionKeyhive<
    Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    crate::keyhive_listener::BigRepoKeyhiveListener,
    rand_08::rngs::OsRng,
>;

pub(crate) type BigRepoSyncHandler<S, C = BigRepoIrohTransport> = SyncHandler<
    Sendable,
    S,
    C,
    BigRepoPolicy,
    CountLeadingZeroBytes,
    subduction_websocket::tokio::TokioSpawn,
>;

pub(crate) type BigRepoNativeComposedHandler<S, C = BigRepoIrohTransport> =
    crate::handler::BigRepoComposedHandler<
        BigRepoSyncHandler<S, C>,
        crate::handler::BigRepoEphemeralHandler<C>,
        crate::handler::BigRepoKeyhiveHandler<C>,
    >;

/// The concrete Subduction type for BigRepo, using the composed handler.
pub(crate) type BigRepoSubduction<S, C = BigRepoIrohTransport> = Subduction<
    'static,
    Sendable,
    S,
    C,
    BigRepoNativeComposedHandler<S, C>,
    BigRepoPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    subduction_websocket::tokio::TimeoutTokio,
    subduction_websocket::tokio::TokioSpawn,
    CountLeadingZeroBytes,
    256,
>;

pub trait BigRepoSubductionStorage:
    Storage<Sendable, Error: std::fmt::Display + Send + Sync + 'static>
    + Clone
    + Send
    + Sync
    + std::fmt::Debug
    + 'static
{
}

impl<T> BigRepoSubductionStorage for T where
    T: Storage<Sendable, Error: std::fmt::Display + Send + Sync + 'static>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static
{
}

// ─── Iroh connection helpers ───────────────────────────────────────────────────

pub(crate) struct IrohConnectResult {
    pub(crate) authenticated:
        Authenticated<MessageTransport<subduction_iroh::transport::IrohTransport>, Sendable>,
    pub(crate) listener_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
    pub(crate) sender_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
}

pub(crate) async fn connect_outgoing_to(
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    audience: subduction_core::handshake::audience::Audience,
) -> Res<IrohConnectResult> {
    let connected = subduction_iroh::client::connect(&endpoint, endpoint_addr, signer, audience)
        .await
        .map_err(|err| ferr!("subduction iroh connect failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: connected.authenticated.map(MessageTransport::new),
        listener_task: connected.listener_task,
        sender_task: connected.sender_task,
    })
}

pub(crate) async fn accept_incoming(
    conn: iroh::endpoint::Connection,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    nonce_cache: &subduction_core::nonce_cache::NonceCache,
    local_peer_id: subduction_core::peer::id::PeerId,
) -> Res<IrohConnectResult> {
    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|err| ferr!("failed accepting subduction bidi stream: {err}"))?;
    let now = subduction_core::timestamp::TimestampSeconds::now();
    let handshake = subduction_iroh::handshake::IrohHandshake::new(send, recv);
    let (authenticated, (listener_task, sender_task)) = subduction_core::handshake::respond(
        handshake,
        move |handshake, peer_id| {
            let (send, recv) = handshake.into_parts();
            let (transport, outbound_rx) =
                subduction_iroh::transport::IrohTransport::new(peer_id, conn);
            let listener_transport = transport.clone();
            let listener_task = Box::pin(subduction_iroh::tasks::listener_task(
                listener_transport,
                recv,
            ));
            let sender_task = Box::pin(subduction_iroh::tasks::sender_task(send, outbound_rx));
            (transport, (listener_task, sender_task))
        },
        signer,
        nonce_cache,
        local_peer_id,
        Some(subduction_core::handshake::audience::Audience::discover(
            b"townframe-subduction",
        )),
        now,
        Duration::from_secs(600),
    )
    .await
    .map_err(|err| ferr!("subduction handshake respond failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: authenticated.map(MessageTransport::new),
        listener_task,
        sender_task,
    })
}

// ─── sedimentree_heads_payload ─────────────────────────────────────────────────

pub(crate) fn sedimentree_heads_payload(tree: &Sedimentree) -> Arc<[automerge::ChangeHash]> {
    doc_heads_from_commit_ids(tree.heads(&CountLeadingZeroBytes))
}

fn doc_heads_from_commit_ids(heads: Vec<CommitId>) -> Arc<[automerge::ChangeHash]> {
    heads
        .into_iter()
        .map(|id| automerge::ChangeHash(<[u8; 32]>::from(id)))
        .collect()
}

// ─── StagedAutomergeIngest & helpers ───────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct FragmentEntry {
    pub(crate) head: CommitId,
    pub(crate) boundary: BTreeSet<CommitId>,
    pub(crate) checkpoints: Vec<CommitId>,
}

#[derive(Clone)]
pub(crate) struct LooseEntry {
    pub(crate) head: CommitId,
    pub(crate) parents: BTreeSet<CommitId>,
}

/// Plaintext Automerge staging output.
///
/// These bytes are transient in-memory data and must be encrypted before they
/// are persisted through Subduction storage.
#[derive(Clone)]
pub(crate) struct StagedAutomergeIngest {
    pub(crate) blobs: Vec<Blob>,
    pub(crate) fragment_entries: Vec<FragmentEntry>,
    pub(crate) loose_entries: Vec<LooseEntry>,
}

/// Stage transient plaintext Automerge bundle bytes and provisional
/// sedimentree metadata.
pub(crate) fn stage_automerge_ingest(doc: &automerge::Automerge) -> StagedAutomergeIngest {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let snapshot_doc = doc.clone();
    let snapshot = snapshot_doc.save();

    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let mut fragment_entries = Vec::with_capacity(cached.len());
    let mut loose_entries = Vec::with_capacity(loose.len());
    let mut covered: sedimentree_core::collections::Set<CommitId> = default();
    for (fragment, raw) in cached.iter().zip(cached_bytes) {
        for member in &fragment.members {
            covered.insert(CommitId::new(member.0));
        }
        let head = CommitId::new(fragment.head.0);
        let boundary: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let checkpoints: Vec<CommitId> = fragment
            .checkpoints
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        blobs.push(Blob::new(raw));
        fragment_entries.push(FragmentEntry {
            head,
            boundary,
            checkpoints,
        });
    }

    for fragment in &loose {
        let head = CommitId::new(fragment.head.0);
        let parents: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|pp| CommitId::new(pp.0))
            .collect();
        blobs.push(Blob::new(snapshot.clone()));
        loose_entries.push(LooseEntry { head, parents });
    }

    StagedAutomergeIngest {
        blobs,
        fragment_entries,
        loose_entries,
    }
}

// ─── Ciphertext locator types ──────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum BigRepoCiphertextKind {
    Fragment,
    LooseCommit,
}

const CAUSAL_CHECKPOINT_MAGIC: &[u8] = b"townframe/causal-checkpoint/v1\0";
const CAUSAL_CHECKPOINT_ID_PREFIX: &[u8; 8] = b"TFCASL01";

/// Key-only node in the Sedimentree encryption DAG. Its plaintext is consumed
/// by BigRepo and never passed to Automerge.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CausalCheckpoint {
    pub(crate) logical_id: [u8; 32],
    pub(crate) epoch: [u8; 32],
    pub(crate) covered_frontier: BTreeSet<CommitId>,
}

impl CausalCheckpoint {
    pub(crate) fn new(epoch: [u8; 32], covered_frontier: BTreeSet<CommitId>) -> Self {
        let mut bytes = b"townframe/causal-checkpoint/logical/v2".to_vec();
        bytes.extend_from_slice(&epoch);
        for head in &covered_frontier {
            bytes.extend_from_slice(head.as_bytes());
        }
        Self {
            logical_id: *blake3::hash(&bytes).as_bytes(),
            epoch,
            covered_frontier,
        }
    }

    pub(crate) fn encode(&self) -> Res<Vec<u8>> {
        let mut encoded = CAUSAL_CHECKPOINT_MAGIC.to_vec();
        encoded.extend(bincode::serialize(self).wrap_err("encode causal checkpoint")?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Res<Option<Self>> {
        let Some(payload) = bytes.strip_prefix(CAUSAL_CHECKPOINT_MAGIC) else {
            return Ok(None);
        };
        Ok(Some(
            bincode::deserialize(payload).wrap_err("decode causal checkpoint")?,
        ))
    }
}

/// Derive a stable physical content reference for one logical checkpoint and
/// PCS epoch. Rejection sampling keeps checkpoints at Sedimentree depth zero,
/// so storing one can never request an Automerge fragment.
pub(crate) fn causal_checkpoint_id(checkpoint: &CausalCheckpoint) -> CommitId {
    let mut bytes = b"townframe/causal-checkpoint/physical/v1".to_vec();
    bytes.extend_from_slice(&checkpoint.logical_id);
    let mut candidate = *blake3::hash(&bytes).as_bytes();
    candidate[..CAUSAL_CHECKPOINT_ID_PREFIX.len()].copy_from_slice(CAUSAL_CHECKPOINT_ID_PREFIX);
    CommitId::new(candidate)
}

pub(crate) fn is_causal_checkpoint_id(id: CommitId) -> bool {
    id.as_bytes().starts_with(CAUSAL_CHECKPOINT_ID_PREFIX)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct BigRepoCiphertextLocator {
    pub(crate) kind: BigRepoCiphertextKind,
    pub(crate) sedimentree_id: SedimentreeId,
    pub(crate) commit_id: CommitId,
}

impl BigRepoCiphertextLocator {
    pub(crate) const fn new(
        kind: BigRepoCiphertextKind,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Self {
        Self {
            kind,
            sedimentree_id,
            commit_id,
        }
    }
}

// ─── persist_cgka_update_op ───────────────────────────────────────────────────

pub(crate) async fn persist_cgka_updates_durably(
    keyhive_protocol: &crate::handler::BigRepoKeyhiveProtocol,
    keyhive_storage: &BigRepoKeyhiveStorage,
    update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
    local_secrets: Vec<keyhive_core::cgka::LocalCgkaSecret>,
) -> Res<Vec<EventHash>> {
    if update_ops.len() != local_secrets.len() {
        return Err(ferr!(
            "local CGKA persistence invariant violated: {} updates but {} private deltas",
            update_ops.len(),
            local_secrets.len()
        ));
    }
    for (update, secret) in update_ops.iter().zip(&local_secrets) {
        if update.payload().doc_id() != &secret.tree_id() {
            return Err(ferr!(
                "local CGKA private delta belongs to a different document than its update"
            ));
        }
    }

    // The private leaf key must be durable before the public operation that
    // makes the new leaf current.
    for secret in local_secrets {
        subduction_keyhive::save_local_cgka_secret(keyhive_storage, &secret)
            .await
            .map_err(|error| ferr!("failed saving local CGKA secret: {error}"))?;
    }
    keyhive_protocol
        .persist_local_events(
            update_ops
                .into_iter()
                .map(|op| StaticEvent::CgkaOperation(Box::new(op)))
                .collect(),
        )
        .await
        .map_err(|err| ferr!("failed persisting keyhive cgka update ops: {err}"))
}

pub(crate) async fn encrypt_staged_automerge_ingest(
    staged_ingest: &StagedAutomergeIngest,
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    initial_keys: Vec<(Vec<u8>, [u8; 32])>,
) -> Res<(
    Sedimentree,
    Vec<Blob>,
    Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
    Vec<keyhive_core::cgka::LocalCgkaSecret>,
)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;

    let keyhive = keyhive_handle.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive.get_document(kh_doc_id).await.ok_or_else(|| {
        ferr!("keyhive doc not found in local keyhive; only the doc owner can call put_doc")
    })?;
    // Imported histories are encrypted with keys supplied by the source
    // document. Seed the new document's local key cache with those keys so
    // later local child commits can construct causal envelopes for them.
    if !initial_keys.is_empty() {
        let mut doc = kh_doc.lock().await;
        for (reference, key) in &initial_keys {
            doc.remember_decryption_key(reference.clone(), (*key).into());
        }
    }

    let mut encrypted_blobs: Vec<Blob> = Vec::with_capacity(staged_ingest.blobs.len());
    let mut new_fragments: Vec<Fragment> = Vec::with_capacity(staged_ingest.fragment_entries.len());
    let mut new_loose_commits: Vec<LooseCommit> =
        Vec::with_capacity(staged_ingest.loose_entries.len());
    let mut update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>> =
        Vec::with_capacity(
            staged_ingest.fragment_entries.len() + staged_ingest.loose_entries.len(),
        );
    let mut local_secrets = Vec::new();

    // Track content_ref -> SymmetricKey for building ancestor maps
    let mut key_index: std::collections::HashMap<Vec<u8>, SymmetricKey> = initial_keys
        .into_iter()
        .map(|(reference, bytes)| (reference, bytes.into()))
        .collect();

    // Encrypt fragment blobs
    for (entry, blob) in staged_ingest.fragment_entries.iter().zip(
        staged_ingest
            .blobs
            .iter()
            .take(staged_ingest.fragment_entries.len()),
    ) {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .boundary
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect();
        // Build ancestors map from all known predecessor keys
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|key| (pred.clone(), *key)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes = bincode::serialize(&envelope).wrap_err("bincode encode envelope")?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|err| ferr!("encrypt fragment failed: {err}"))?;
        if let Some(secret) = encrypted.local_cgka_secret().copied() {
            local_secrets.push(secret);
        }
        if let Some(update_op) = encrypted.update_op().cloned() {
            update_ops.push(update_op);
        }

        let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_fragments.push(Fragment::new(
            sedimentree_id,
            entry.head,
            entry.boundary.clone(),
            &entry.checkpoints,
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    // Encrypt loose commit blobs
    for (entry, blob) in staged_ingest.loose_entries.iter().zip(
        staged_ingest
            .blobs
            .iter()
            .skip(staged_ingest.fragment_entries.len()),
    ) {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .parents
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect();

        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|key| (pred.clone(), *key)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes = bincode::serialize(&envelope).wrap_err("bincode encode envelope")?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|err| ferr!("encrypt loose commit failed: {err}"))?;
        if let Some(secret) = encrypted.local_cgka_secret().copied() {
            local_secrets.push(secret);
        }
        if let Some(update_op) = encrypted.update_op().cloned() {
            update_ops.push(update_op);
        }

        let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_loose_commits.push(LooseCommit::new(
            sedimentree_id,
            entry.head,
            entry.parents.clone(),
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    Ok((
        Sedimentree::new(new_fragments, new_loose_commits),
        encrypted_blobs,
        update_ops,
        local_secrets,
    ))
}

pub(crate) async fn encrypt_loose_commit_with_update_op(
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    parents: &BTreeSet<CommitId>,
    blob: &[u8],
    batch_keys: &std::collections::HashMap<CommitId, keyhive_crypto::symmetric_key::SymmetricKey>,
) -> Res<(
    Blob,
    keyhive_crypto::symmetric_key::SymmetricKey,
    Option<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
    Option<keyhive_core::cgka::LocalCgkaSecret>,
)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;
    let keyhive = keyhive_handle.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for commit encryption"))?;
    let content_ref: Vec<u8> = head.as_bytes().to_vec();
    let pred_refs: Vec<Vec<u8>> = parents.iter().map(|id| id.as_bytes().to_vec()).collect();
    let (owner_secret_count, cgka_operation_count, has_pcs_key) = {
        let locked = kh_doc.lock().await;
        let cgka = locked
            .cgka()
            .map_err(|error| ferr!("failed inspecting document CGKA before encryption: {error}"))?;
        (cgka.owner_sks().len(), cgka.ops_count(), cgka.has_pcs_key())
    };
    let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = {
        let doc_keys = kh_doc.lock().await.known_decryption_keys().clone();
        parents
            .iter()
            .map(|parent| {
                let content_ref = parent.as_bytes().to_vec();
                let key = batch_keys
                    .get(parent)
                    .copied()
                    .or_else(|| doc_keys.get(&content_ref).copied())
                    .ok_or_else(|| {
                        eyre::Report::new(crate::runtime2::io::DocumentKeyUnavailable {
                            source: ferr!("missing causal encryption key for parent {parent}"),
                            document_id: crate::DocumentId::new(*sedimentree_id.as_bytes()),
                            owner_secret_count,
                            cgka_operation_count,
                            has_pcs_key,
                        })
                    })?;
                Ok((content_ref, key))
            })
            .collect::<Res<_>>()?
    };
    let envelope = Envelope {
        plaintext: blob.to_vec(),
        ancestors,
    };
    let envelope_bytes = bincode::serialize(&envelope).wrap_err("bincode encode envelope")?;

    let (encrypted, app_key) = keyhive
        .try_encrypt_content_keyed(
            Arc::clone(&kh_doc),
            &content_ref,
            &pred_refs,
            &envelope_bytes,
        )
        .await
        .map_err(|error| match error {
            keyhive_core::keyhive::EncryptContentError::EncryptError(
                keyhive_core::principal::document::EncryptError::FailedToMakeAppSecret(source),
            ) => eyre::Report::new(crate::runtime2::io::DocumentKeyUnavailable {
                source: eyre::Report::new(source),
                document_id: crate::DocumentId::new(*sedimentree_id.as_bytes()),
                owner_secret_count,
                cgka_operation_count,
                has_pcs_key,
            }),
            error => ferr!("encrypt commit failed: {error}"),
        })?;
    let update_op = encrypted.update_op().cloned();
    let local_secret = encrypted.local_cgka_secret().copied();
    tracing::debug!(
        %sedimentree_id,
        %head,
        parent_count = parents.len(),
        "generated application encryption key for loose commit"
    );

    let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;
    Ok((Blob::new(encrypted_bytes), app_key, update_op, local_secret))
}

pub(crate) async fn encrypt_fragment_blob<S>(
    keyhive_handle: &BigKeyhiveHandle,
    storage_for_reads: &S,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    boundary: &BTreeSet<CommitId>,
    fragment_bytes: &[u8],
) -> Res<Blob>
where
    S: BigRepoSubductionStorage,
{
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::siv::Siv;

    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let keyhive = keyhive_handle.clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for fragment encryption"))?;

    let mut known_decryption_keys = kh_doc.lock().await.known_decryption_keys().clone();

    let head_verified = <S as Storage<Sendable>>::load_loose_commit(
        storage_for_reads,
        sedimentree_id,
        head,
    )
    .await
    .map_err(|err| ferr!("failed loading loose commit for fragment encryption: {err}"))?
    .ok_or_else(|| {
        ferr!(
            "fragment head missing loose commit in storage: sedimentree_id={sedimentree_id:?} head={head:?}"
        )
    })?;
    let head_encrypted = decode_encrypted_blob(head_verified.blob().as_slice())?;
    let (_head_plaintext, head_key) = kh_doc
        .lock()
        .await
        .try_decrypt_content_keyed(&head_encrypted)
        .map_err(|error| ferr!("failed recovering fragment head snapshot: {error}"))?;

    let mut ancestors = std::collections::HashMap::with_capacity(boundary.len());
    for predecessor in boundary {
        let content_ref = predecessor.as_bytes().to_vec();
        let key = if let Some(key) = known_decryption_keys.get(&content_ref).copied() {
            key
        } else {
            let verified = <S as Storage<Sendable>>::load_loose_commit(
                storage_for_reads,
                sedimentree_id,
                *predecessor,
            )
            .await
            .map_err(|error| ferr!("failed loading fragment boundary commit: {error}"))?
            .ok_or_else(|| {
                ferr!(
                    "fragment boundary missing loose commit: sedimentree_id={sedimentree_id:?} head={predecessor:?}"
                )
            })?;
            let encrypted = decode_encrypted_blob(verified.blob().as_slice())?;
            let (_, key) = kh_doc
                .lock()
                .await
                .try_decrypt_content_keyed(&encrypted)
                .map_err(|error| ferr!("failed recovering fragment boundary key: {error}"))?;
            known_decryption_keys.insert(content_ref.clone(), key);
            key
        };
        ancestors.insert(content_ref, key);
    }

    let envelope = Envelope {
        plaintext: fragment_bytes.to_vec(),
        ancestors,
    };
    let envelope_bytes =
        bincode::serialize(&envelope).wrap_err("bincode encode fragment envelope")?;
    let nonce_context = fragment_nonce_context(sedimentree_id, head, boundary);
    let nonce = Siv::new(&head_key, &envelope_bytes, &nonce_context);
    let mut ciphertext = envelope_bytes;
    head_key
        .try_encrypt(nonce, &mut ciphertext)
        .map_err(|err| ferr!("encrypt fragment payload failed: {err}"))?;

    let encrypted = beekem::encrypted::EncryptedContent::new(
        nonce,
        ciphertext,
        head_encrypted.pcs_key_hash,
        head_encrypted.pcs_update_op_hash,
        head_encrypted.content_ref.clone(),
        head_encrypted.pred_refs,
    );
    let encrypted_bytes = encode_encrypted_blob(&encrypted)?;

    Ok(Blob::new(encrypted_bytes))
}

pub(crate) fn fragment_nonce_context(
    sedimentree_id: SedimentreeId,
    head: CommitId,
    boundary: &BTreeSet<CommitId>,
) -> Vec<u8> {
    let mut context = b"big_repo.fragment.v1".to_vec();
    context.extend_from_slice(sedimentree_id.as_bytes());
    context.extend_from_slice(head.as_bytes());
    for boundary_head in boundary {
        context.extend_from_slice(boundary_head.as_bytes());
    }
    context
}

#[cfg(test)]
mod causal_checkpoint_tests {
    use super::*;

    fn frontier(bytes: &[[u8; 32]]) -> BTreeSet<CommitId> {
        bytes.iter().copied().map(CommitId::new).collect()
    }

    #[test]
    fn checkpoint_wire_format_is_typed_and_round_trips() -> Res<()> {
        let checkpoint = CausalCheckpoint::new([9; 32], frontier(&[[1; 32], [2; 32]]));
        let encoded = checkpoint.encode()?;

        assert_eq!(CausalCheckpoint::decode(&encoded)?, Some(checkpoint));
        assert_eq!(CausalCheckpoint::decode(b"an automerge payload")?, None);
        Ok(())
    }

    #[test]
    fn physical_checkpoint_identity_is_epoch_specific_and_never_fragments() {
        let checkpoint = CausalCheckpoint::new([5; 32], frontier(&[[3; 32], [4; 32]]));
        let first = causal_checkpoint_id(&checkpoint);
        let repeated = causal_checkpoint_id(&checkpoint);
        let next_epoch = causal_checkpoint_id(&CausalCheckpoint::new(
            [6; 32],
            checkpoint.covered_frontier.clone(),
        ));

        assert_eq!(first, repeated);
        assert_ne!(first, next_epoch);
        assert!(is_causal_checkpoint_id(first));
        assert!(is_causal_checkpoint_id(next_epoch));
        assert!(!is_causal_checkpoint_id(CommitId::new([0; 32])));
    }

    #[test]
    fn causal_checkpoint_is_not_an_automerge_fragment_candidate() {
        let checkpoint = CausalCheckpoint::new([5; 32], frontier(&[[3; 32]]));
        let checkpoint_head = causal_checkpoint_id(&checkpoint);

        assert_eq!(checkpoint_head.as_bytes()[..8], *b"TFCASL01");
        assert!(
            is_causal_checkpoint_id(checkpoint_head),
            "a causal checkpoint is filtered before Automerge ingestion"
        );
    }

    #[test]
    fn logical_checkpoint_identity_only_depends_on_covered_frontier() {
        let first = CausalCheckpoint::new([5; 32], frontier(&[[7; 32], [8; 32]]));
        let same = CausalCheckpoint::new([5; 32], frontier(&[[8; 32], [7; 32]]));
        let different = CausalCheckpoint::new([5; 32], frontier(&[[7; 32], [9; 32]]));

        assert_eq!(first.logical_id, same.logical_id);
        assert_ne!(first.logical_id, different.logical_id);
        assert_ne!(
            first.logical_id,
            CausalCheckpoint::new([6; 32], first.covered_frontier.clone()).logical_id
        );
    }
}
