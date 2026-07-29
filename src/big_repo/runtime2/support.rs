//! Support definitions migrated from the obsolete runtime.rs.
//!
//! Support types, traits, type aliases, and helper functions that are still
//! imported by runtime2, its native backend, handler.rs, keyhive_conn.rs,
//! and ephemeral.rs.

use crate::interlude::*;

use crate::{
    encrypted_blob::{decode_encrypted_blob, encode_encrypted_blob},
    keyhive_storage::BigRepoKeyhiveStorage,
    BigKeyhiveHandle,
};
use future_form::Sendable;
use futures::future::BoxFuture;
use keyhive_core::event::static_event::StaticEvent;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::{minimized::MinimizedSedimentree, Sedimentree},
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use subduction_core::{
    authenticated::Authenticated, collections::bounded_sharded_map::BoundedShardedMap,
    handler::sync::SyncHandler, storage::traits::Storage, subduction::Subduction,
    transport::message::MessageTransport,
};

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

pub(crate) async fn connect_outgoing(
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    signer: &subduction_crypto::signer::memory::MemorySigner,
) -> Res<IrohConnectResult> {
    connect_outgoing_to(
        endpoint,
        endpoint_addr,
        signer,
        subduction_core::handshake::audience::Audience::discover(b"townframe-subduction"),
    )
    .await
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
    _change_count: usize,
    _covered_count: usize,
    _loose_count: usize,
    _fragment_count: usize,
}

/// Stage transient plaintext Automerge bundle bytes and provisional
/// sedimentree metadata.
pub(crate) fn stage_automerge_ingest(doc: &automerge::Automerge) -> StagedAutomergeIngest {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());

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

    for (fragment, raw) in loose.iter().zip(loose_bytes) {
        let head = CommitId::new(fragment.head.0);
        let parents: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|pp| CommitId::new(pp.0))
            .collect();
        blobs.push(Blob::new(raw));
        loose_entries.push(LooseEntry { head, parents });
    }

    let covered_count = covered.len();
    let fragment_count = fragment_entries.len();
    let loose_count = loose_entries.len();

    StagedAutomergeIngest {
        blobs,
        fragment_entries,
        loose_entries,
        _change_count: doc.get_changes_meta(&[]).len(),
        _covered_count: covered_count,
        _loose_count: loose_count,
        _fragment_count: fragment_count,
    }
}

// ─── Ciphertext locator types ──────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum BigRepoCiphertextKind {
    Fragment,
    LooseCommit,
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

pub(crate) async fn persist_cgka_update_op(
    keyhive_storage: &BigRepoKeyhiveStorage,
    update_op: keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
) -> Res<()> {
    let event = StaticEvent::CgkaOperation(Box::new(update_op));
    subduction_keyhive::save_event::<Vec<u8>, _, Sendable>(keyhive_storage, &event)
        .await
        .map_err(|e| ferr!("failed to save keyhive cgka update op: {e}"))?;
    Ok(())
}

// ─── encrypt_staged_automerge_ingest ───────────────────────────────────────────

pub(crate) async fn encrypt_staged_automerge_ingest(
    staged_ingest: &StagedAutomergeIngest,
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
) -> Res<(
    Sedimentree,
    Vec<Blob>,
    Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
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

    let mut encrypted_blobs: Vec<Blob> = Vec::with_capacity(staged_ingest.blobs.len());
    let mut new_fragments: Vec<Fragment> = Vec::with_capacity(staged_ingest.fragment_entries.len());
    let mut new_loose_commits: Vec<LooseCommit> =
        Vec::with_capacity(staged_ingest.loose_entries.len());
    let mut update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>> =
        Vec::with_capacity(
            staged_ingest.fragment_entries.len() + staged_ingest.loose_entries.len(),
        );

    // Track content_ref -> SymmetricKey for building ancestor maps
    let mut key_index: std::collections::HashMap<Vec<u8>, SymmetricKey> =
        std::collections::HashMap::new();

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
            .map(|c| c.as_bytes().to_vec())
            .collect();
        // Build ancestors map from all known predecessor keys
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|e| ferr!("encrypt fragment failed: {e}"))?;
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
            .map(|c| c.as_bytes().to_vec())
            .collect();

        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|e| ferr!("encrypt loose commit failed: {e}"))?;
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
    ))
}

// ─── encrypt_loose_commit_with_update_op ───────────────────────────────────────

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
    let pred_refs: Vec<Vec<u8>> = parents.iter().map(|c| c.as_bytes().to_vec()).collect();

    let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = {
        let doc_keys = kh_doc.lock().await.known_decryption_keys().clone();
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = parents
            .iter()
            .map(|p| {
                let pref = p.as_bytes().to_vec();
                let key = batch_keys
                    .get(p)
                    .copied()
                    .or_else(|| doc_keys.get(&pref).copied())
                    .ok_or_else(|| ferr!("missing causal encryption key for parent {p}"))?;
                Ok((pref, key))
            })
            .collect::<Res<_>>()?;
        ancestors
    };
    let envelope = Envelope {
        plaintext: blob.to_vec(),
        ancestors,
    };
    let envelope_bytes =
        bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

    let (encrypted, app_key) = keyhive
        .try_encrypt_content_keyed(
            Arc::clone(&kh_doc),
            &content_ref,
            &pred_refs,
            &envelope_bytes,
        )
        .await
        .map_err(|e| ferr!("encrypt commit failed: {e}"))?;
    let update_op = encrypted.update_op().cloned();

    let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

    Ok((Blob::new(encrypted_bytes), app_key, update_op))
}

// ─── encrypt_fragment_blob ─────────────────────────────────────────────────────

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
    use keyhive_crypto::{siv::Siv, symmetric_key::SymmetricKey};

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

    let head_ref: Vec<u8> = head.as_bytes().to_vec();
    let head_verified = <S as Storage<Sendable>>::load_loose_commit(
        storage_for_reads,
        sedimentree_id,
        head,
    )
    .await
    .map_err(|e| ferr!("failed loading loose commit for fragment encryption: {e}"))?
    .ok_or_else(|| {
        ferr!(
            "fragment head missing loose commit in storage: sedimentree_id={sedimentree_id:?} head={head:?}"
        )
    })?;
    let head_encrypted = decode_encrypted_blob(head_verified.blob().as_slice())?;
    let head_key: SymmetricKey = if let Some(key) = known_decryption_keys.get(&head_ref).copied() {
        key
    } else {
        let (_, key) = kh_doc
            .lock()
            .await
            .try_decrypt_content_keyed(&head_encrypted)
            .map_err(|e| ferr!("failed recovering fragment head key: {e}"))?;
        known_decryption_keys.insert(head_ref.clone(), key);
        key
    };

    let mut ancestors = std::collections::HashMap::with_capacity(boundary.len());
    for pred in boundary {
        let pred_ref: Vec<u8> = pred.as_bytes().to_vec();
        let pred_key = if let Some(key) = known_decryption_keys.get(&pred_ref).copied() {
            key
        } else {
            let pred_verified = <S as Storage<Sendable>>::load_loose_commit(storage_for_reads, sedimentree_id, *pred)
                .await
                .map_err(|e| {
                    ferr!("failed loading fragment boundary loose commit for encryption: {e}")
                })?
                .ok_or_else(|| {
                    ferr!(
                        "fragment boundary missing loose commit in storage: sedimentree_id={sedimentree_id:?} head={pred:?}"
                    )
                })?;
            let pred_encrypted = decode_encrypted_blob(pred_verified.blob().as_slice())?;
            let (_, key) = kh_doc
                .lock()
                .await
                .try_decrypt_content_keyed(&pred_encrypted)
                .map_err(|e| ferr!("failed recovering fragment boundary key: {e}"))?;
            known_decryption_keys.insert(pred_ref.clone(), key);
            key
        };
        ancestors.insert(pred_ref, pred_key);
    }

    let envelope = Envelope {
        plaintext: fragment_bytes.to_vec(),
        ancestors,
    };
    let envelope_bytes = bincode::serialize(&envelope)
        .map_err(|e| ferr!("bincode encode fragment envelope: {e}"))?;
    let nonce_context = fragment_nonce_context(sedimentree_id, head, boundary);
    let nonce = Siv::new(&head_key, &envelope_bytes, &nonce_context);
    let mut ciphertext = envelope_bytes;
    head_key
        .try_encrypt(nonce, &mut ciphertext)
        .map_err(|e| ferr!("encrypt fragment payload failed: {e}"))?;

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
