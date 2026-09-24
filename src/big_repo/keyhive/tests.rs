use super::*;
use keyhive_core::access::Access;
use keyhive_core::principal::individual::{Individual, op::KeyOp};
use keyhive_crypto::{share_key::ShareKey, share_key::ShareSecretKey};
use nonempty::nonempty;

#[tokio::test]
async fn prekey_rotation_secret_is_durable_before_public_event_and_survives_compaction() -> Res<()>
{
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (evt_tx, evt_rx) = async_channel::unbounded();
    let listener = BigRepoKeyhiveListener {
        evt_tx,
        storage: storage.clone(),
    };
    let seed = [40; 32];
    let owner = BigKeyhiveHandle::new(seed, listener.clone()).await?;
    let storage_id = subduction_keyhive::StorageHash::new(*owner.keyhive_peer_id().verifying_key());

    // Establish a baseline archive and discard private deltas produced while
    // constructing the initial contact card.
    subduction_keyhive::compact(owner.clone_keyhive().as_ref(), &storage, storage_id).await?;
    while evt_rx.try_recv().is_ok() {}

    let add_op = owner.clone_keyhive().expand_prekeys().await?;
    let persisted =
        subduction_keyhive::load_local_prekey_changes::<_, future_form::Sendable>(&storage).await?;
    assert_eq!(persisted.len(), 1);
    // The combined record carries the signed membership op AND the secret.
    match &persisted[0].1 {
        keyhive_core::principal::individual::op::KeyOp::Add(add) => {
            assert_eq!(add.payload.share_key, add_op.payload.share_key);
        }
        keyhive_core::principal::individual::op::KeyOp::Rotate(_) => {
            panic!("an expansion must be recorded as an Add op");
        }
    }
    assert!(
        matches!(
            evt_rx.try_recv(),
            Ok(crate::runtime2::Runtime2Evt::PrekeyExpanded { .. })
        ),
        "the public event must only become observable after its secret is durable"
    );

    let restored = BigKeyhiveHandle::restore_from_storage_archive(seed, &storage, listener.clone())
        .await?
        .ok_or_eyre("baseline archive is missing")?;
    subduction_keyhive::ingest_from_storage(restored.clone_keyhive().as_ref(), &storage).await?;
    let restored_pairs: BTreeMap<ShareKey, ShareSecretKey> =
        bincode::deserialize(&restored.clone_keyhive().export_prekey_secrets().await?)?;
    assert_eq!(
        restored_pairs.get(&add_op.payload.share_key),
        Some(&persisted[0].2)
    );

    subduction_keyhive::compact(owner.clone_keyhive().as_ref(), &storage, storage_id).await?;
    assert!(
        subduction_keyhive::load_local_prekey_secrets::<_, future_form::Sendable>(&storage)
            .await?
            .is_empty(),
        "compaction must absorb the prekey delta into the archive"
    );
    let compacted = BigKeyhiveHandle::restore_from_storage_archive(seed, &storage, listener)
        .await?
        .ok_or_eyre("compacted archive is missing")?;
    let compacted_pairs: BTreeMap<ShareKey, ShareSecretKey> =
        bincode::deserialize(&compacted.clone_keyhive().export_prekey_secrets().await?)?;
    assert_eq!(
        compacted_pairs.get(&add_op.payload.share_key),
        Some(&persisted[0].2)
    );

    Ok(())
}

#[tokio::test]
async fn pending_doc_finalization_removes_only_pending_group() -> Res<()> {
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (evt_tx, _evt_rx) = async_channel::unbounded();
    let owner = BigKeyhiveHandle::new(
        [43; 32],
        BigRepoKeyhiveListener {
            evt_tx,
            storage: storage.clone(),
        },
    )
    .await?;
    let protocol: BigRepoKeyhiveProtocol = Arc::new(subduction_keyhive::KeyhiveProtocol::new(
        owner.clone_keyhive(),
        storage.clone(),
        owner.keyhive_peer_id(),
        owner.contact_card().clone(),
    ));
    let (pending_group, _) = owner
        .create_group_with_parents(Vec::new(), &protocol)
        .await?;
    let (intended_group, _) = owner
        .create_group_with_parents(Vec::new(), &protocol)
        .await?;
    let doc_id = owner
        .reserve_doc_id(
            vec![pending_group.clone().into(), intended_group.clone().into()],
            &storage,
        )
        .await?;
    // A reservation is not yet a Keyhive authority: no document exists and
    // no group contains it.
    assert!(!owner.document_has_content(doc_id.clone()).await?);
    assert!(
        !owner
            .group_document_ids(&pending_group)
            .await
            .contains(&doc_id)
    );
    assert!(
        !owner
            .group_document_ids(&intended_group)
            .await
            .contains(&doc_id)
    );

    // Finalization creates the document under the reserved identity with the
    // real content heads and the reserved parents.
    owner
        .finalize_reserved_doc(
            doc_id.clone(),
            nonempty::nonempty!([7u8; 32]),
            &protocol,
            &storage,
        )
        .await?;
    assert!(owner.document_has_content(doc_id.clone()).await?);
    assert!(
        owner
            .group_document_ids(&pending_group)
            .await
            .contains(&doc_id)
    );
    assert!(
        owner
            .group_document_ids(&intended_group)
            .await
            .contains(&doc_id)
    );

    owner
        .revoke_group_from_doc(&pending_group, doc_id.clone(), vec![vec![7; 32]], &protocol)
        .await?;
    assert!(
        !owner
            .group_document_ids(&pending_group)
            .await
            .contains(&doc_id)
    );
    assert!(
        owner
            .group_document_ids(&intended_group)
            .await
            .contains(&doc_id)
    );
    Ok(())
}

#[tokio::test]
async fn authority_change_archive_immediately_restores_private_document_key() -> Res<()> {
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (evt_tx, _evt_rx) = async_channel::unbounded();
    let listener = BigRepoKeyhiveListener {
        evt_tx,
        storage: storage.clone(),
    };
    let owner_seed = [41; 32];
    let owner = BigKeyhiveHandle::new(owner_seed, listener.clone()).await?;
    owner.save_prekey_state(&storage).await?;
    let protocol: BigRepoKeyhiveProtocol = Arc::new(subduction_keyhive::KeyhiveProtocol::new(
        owner.clone_keyhive(),
        storage.clone(),
        owner.keyhive_peer_id(),
        owner.contact_card().clone(),
    ));

    let (repo_agents, _repo_hashes) = owner
        .create_group_with_parents(Vec::new(), &protocol)
        .await?;
    let (core_docs, _core_hashes) = owner
        .create_group_with_parents(Vec::new(), &protocol)
        .await?;
    let owner_agent = owner
        .get_agent_by_peer_id(&owner.keyhive_peer_id())
        .await?
        .ok_or_eyre("owner agent is missing")?;
    owner
        .add_member_to_group(
            owner_agent,
            &repo_agents,
            Access::Admin,
            BTreeMap::new(),
            &protocol,
        )
        .await?;
    owner
        .add_member_to_group(
            repo_agents.clone(),
            &core_docs,
            Access::Admin,
            BTreeMap::new(),
            &protocol,
        )
        .await?;

    let initial_ref = vec![7; 32];
    let (doc_id, _doc_hashes) = owner
        .create_doc(vec![core_docs.into()], nonempty![[7; 32]], &protocol)
        .await?;
    let keyhive = owner.clone_keyhive();
    let kh_doc_id = keyhive_doc_id(doc_id.clone())?;
    assert!(
        keyhive.get_document(kh_doc_id).await.is_some(),
        "new document is missing"
    );
    let encrypted = keyhive
        .try_encrypt_content(kh_doc_id, &initial_ref, &Vec::new(), b"initial")
        .await?;
    if let Some(update) = encrypted.update_op().cloned() {
        persist_cgka_update_ops(&protocol, vec![update]).await?;
    }
    subduction_keyhive::compact(
        owner.clone_keyhive().as_ref(),
        &storage,
        subduction_keyhive::StorageHash::new(*owner.keyhive_peer_id().verifying_key()),
    )
    .await?;

    let (clone_evt_tx, _clone_evt_rx) = async_channel::unbounded();
    let clone = BigKeyhiveHandle::new(
        [42; 32],
        BigRepoKeyhiveListener {
            evt_tx: clone_evt_tx,
            storage: crate::keyhive_storage::BigRepoKeyhiveStorage::memory(),
        },
    )
    .await?;
    owner
        .clone_keyhive()
        .receive_contact_card(clone.contact_card())
        .await?;
    let clone_agent = owner
        .get_agent_by_peer_id(&clone.keyhive_peer_id())
        .await?
        .ok_or_eyre("clone agent is missing")?;
    owner
        .add_member_to_group(
            clone_agent,
            &repo_agents,
            Access::Admin,
            BTreeMap::from([(doc_id, vec![initial_ref.clone()])]),
            &protocol,
        )
        .await?;

    let checkpoint_ref = vec![8; 32];
    assert!(
        keyhive.get_document(kh_doc_id).await.is_some(),
        "document disappeared before checkpoint"
    );
    let checkpoint = keyhive
        .try_encrypt_content(
            kh_doc_id,
            &checkpoint_ref,
            &vec![initial_ref.clone()],
            b"authority-checkpoint",
        )
        .await?;
    let update = checkpoint
        .update_op()
        .cloned()
        .ok_or_eyre("authority checkpoint must rotate the PCS key")?;
    let local_secret = checkpoint
        .local_cgka_secret()
        .copied()
        .ok_or_eyre("authority checkpoint must expose its private leaf key")?;
    crate::runtime2::support::persist_cgka_updates_durably(
        &protocol,
        &storage,
        vec![update],
        vec![Some(local_secret)],
    )
    .await?;
    let restored = BigKeyhiveHandle::restore_from_storage_archive(owner_seed, &storage, listener)
        .await?
        .ok_or_eyre("owner archive is missing")?;
    restored.import_prekey_state(&storage).await?;
    subduction_keyhive::ingest_from_storage(restored.clone_keyhive().as_ref(), &storage).await?;
    let restored_keyhive = restored.clone_keyhive();
    assert!(
        restored_keyhive.get_document(kh_doc_id).await.is_some(),
        "restored document is missing"
    );
    restored_keyhive
        .try_encrypt_content(
            kh_doc_id,
            &vec![9; 32],
            &vec![checkpoint_ref],
            b"after-authority-change",
        )
        .await?;

    subduction_keyhive::compact(
        restored_keyhive.as_ref(),
        &storage,
        subduction_keyhive::StorageHash::new(*restored.keyhive_peer_id().verifying_key()),
    )
    .await?;
    let remaining =
        <crate::keyhive_storage::BigRepoKeyhiveStorage as subduction_keyhive::KeyhiveStorage<
            future_form::Sendable,
        >>::load_local_secrets(&storage)
        .await?;
    assert!(
        remaining.is_empty(),
        "archive compaction must remove incorporated private deltas"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Coparent prekey material
//
// `Document::generate` picks a prekey for every coparent, so creating or
// finalizing a document that names a principal whose prekey ops this hive has
// never ingested cannot succeed. The field failure this pins is opaque — the
// caller sees "individual <id> has published no prekey to select from" and has
// to guess whether the publication is still in flight or was never pulled —
// which is why each site below is pinned in *both* directions: the refusal case
// states the precondition, and the positive control stops an implementation
// that always refused from passing on the refusal case alone.
//
// The peer here is a *real* hive. Its individual is rebuilt from its own signed
// prekey op (`expand_prekeys`), which is exactly the material a keyhive sync
// would deliver, and it is registered into the local hive only where a test
// says so. "Material absent" and "material present" therefore differ by one
// call, and nothing in these tests sleeps, retries, or touches the network.
// ---------------------------------------------------------------------------

/// A local hive plus an independent peer that the local hive has never heard
/// from, plus the protocol the local hive needs.
///
/// The event receivers are dropped deliberately: `BigRepoKeyhiveListener`
/// treats a closed channel as a dropped debug event rather than an error, so a
/// send from inside a keyhive operation cannot fail because of this helper.
async fn local_hive_and_unmaterialized_peer(
    owner_seed: u8,
    peer_seed: u8,
) -> Res<(
    BigKeyhiveHandle,
    crate::keyhive_storage::BigRepoKeyhiveStorage,
    BigRepoKeyhiveProtocol,
    Arc<futures::lock::Mutex<Individual>>,
    BigKeyhiveAuthority,
)> {
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (owner_tx, _owner_rx) = async_channel::unbounded();
    let owner = BigKeyhiveHandle::new(
        [owner_seed; 32],
        BigRepoKeyhiveListener {
            evt_tx: owner_tx,
            storage: storage.clone(),
        },
    )
    .await?;
    let protocol: BigRepoKeyhiveProtocol = Arc::new(subduction_keyhive::KeyhiveProtocol::new(
        owner.clone_keyhive(),
        storage.clone(),
        owner.keyhive_peer_id(),
        owner.contact_card().clone(),
    ));

    let peer_storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (peer_tx, _peer_rx) = async_channel::unbounded();
    let peer = BigKeyhiveHandle::new(
        [peer_seed; 32],
        BigRepoKeyhiveListener {
            evt_tx: peer_tx,
            storage: peer_storage.clone(),
        },
    )
    .await?;
    // The peer's own published prekey op: the material a sync would carry.
    let peer_prekey_op = peer.clone_keyhive().expand_prekeys().await?;
    let peer_individual = Arc::new(futures::lock::Mutex::new(Individual::new(KeyOp::Add(
        peer_prekey_op,
    ))));
    let peer_authority = {
        let id = peer_individual.lock().await.id();
        BigKeyhiveAuthority::Agent(BigKeyhiveAgent::Individual(id, peer_individual.clone()))
    };

    Ok((owner, storage, protocol, peer_individual, peer_authority))
}

#[tokio::test]
async fn document_creation_refuses_a_coparent_the_hive_has_not_materialized() -> Res<()> {
    let (owner, _storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(61, 62).await?;
    let peer_identifier: Identifier = peer_individual.lock().await.id().into();

    let err = owner
        .create_doc(vec![peer_authority], nonempty![[7u8; 32]], &protocol)
        .await
        .err()
        .ok_or_eyre("creation must not proceed without the coparent's material")?;
    assert!(
        err.to_string().contains(&peer_identifier.to_string()),
        "the refusal must name the coparent whose material is missing: {err}"
    );
    assert!(
        err.to_string().contains("is not known to this keyhive"),
        "a coparent that has published nothing here is reported as unknown to this hive: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn document_creation_succeeds_once_the_coparent_material_is_present() -> Res<()> {
    let (owner, _storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(63, 64).await?;
    assert!(
        owner
            .clone_keyhive()
            .register_individual(peer_individual.clone())
            .await,
        "the fixture must start without the peer registered"
    );

    let (doc_id, _hashes) = owner
        .create_doc(vec![peer_authority], nonempty![[7u8; 32]], &protocol)
        .await?;
    assert!(
        owner.document_has_content(doc_id).await?,
        "the created document must be materialized"
    );
    Ok(())
}

#[tokio::test]
async fn reserved_document_finalization_refuses_a_parent_the_hive_has_not_materialized() -> Res<()>
{
    let (owner, storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(65, 66).await?;
    let peer_identifier: Identifier = peer_individual.lock().await.id().into();

    // A reservation stores parent ids without resolving them, so reserving
    // succeeds here and the refusal has to come from finalization.
    let doc_id = owner.reserve_doc_id(vec![peer_authority], &storage).await?;
    let err = owner
        .finalize_reserved_doc(doc_id, nonempty![[7u8; 32]], &protocol, &storage)
        .await
        .err()
        .ok_or_eyre("finalization must not proceed without the parent's material")?;
    assert!(
        err.to_string()
            .contains("cannot resolve reserved parent authority"),
        "finalization refuses an unresolvable parent before selecting prekeys: {err}"
    );
    assert!(
        err.to_string().contains(&format!("{peer_identifier:?}")),
        "the refusal must name the parent it could not resolve: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn reserved_document_finalization_succeeds_once_the_parent_material_is_present() -> Res<()> {
    let (owner, storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(67, 68).await?;
    assert!(
        owner
            .clone_keyhive()
            .register_individual(peer_individual.clone())
            .await,
        "the fixture must start without the peer registered"
    );

    let doc_id = owner.reserve_doc_id(vec![peer_authority], &storage).await?;
    owner
        .finalize_reserved_doc(doc_id.clone(), nonempty![[7u8; 32]], &protocol, &storage)
        .await?;
    assert!(
        owner.document_has_content(doc_id).await?,
        "the finalized document must be materialized"
    );
    Ok(())
}

/// The same precondition, one call over: `generate_group` resolves its
/// coparents through the same `agent_by_id` lookup, so a group naming a
/// principal this hive has not materialized is refused too — and, unlike the
/// document sites, without any attempt to say why.
#[tokio::test]
async fn group_creation_refuses_a_coparent_the_hive_has_not_materialized() -> Res<()> {
    let (owner, _storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(69, 70).await?;
    let peer_identifier: Identifier = peer_individual.lock().await.id().into();

    let err = owner
        .create_group_with_parents(vec![peer_authority], &protocol)
        .await
        .err()
        .ok_or_eyre("group creation must not proceed without the coparent's material")?;
    assert!(
        err.to_string().contains(&peer_identifier.to_string()),
        "the refusal must name the coparent whose material is missing: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn group_creation_succeeds_once_the_coparent_material_is_present() -> Res<()> {
    let (owner, _storage, protocol, peer_individual, peer_authority) =
        local_hive_and_unmaterialized_peer(71, 72).await?;
    assert!(
        owner
            .clone_keyhive()
            .register_individual(peer_individual.clone())
            .await,
        "the fixture must start without the peer registered"
    );

    let (group, _hashes) = owner
        .create_group_with_parents(vec![peer_authority], &protocol)
        .await?;
    assert!(
        owner.group_document_ids(&group).await.is_empty(),
        "a group created with no documents yet must still be a live handle"
    );
    Ok(())
}
