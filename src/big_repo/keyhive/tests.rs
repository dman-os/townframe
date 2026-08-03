use super::*;
use keyhive_core::access::Access;
use nonempty::nonempty;

#[tokio::test]
async fn authority_change_archive_immediately_restores_private_document_key() -> Res<()> {
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::memory();
    let (evt_tx, _evt_rx) = async_channel::unbounded();
    let listener = BigRepoKeyhiveListener { evt_tx };
    let owner_seed = [41; 32];
    let owner = BigKeyhiveHandle::new(owner_seed, listener.clone()).await?;
    owner.save_prekey_secrets(&storage).await?;

    let repo_agents = owner
        .create_group_with_parents(Vec::new(), &storage)
        .await?;
    let core_docs = owner
        .create_group_with_parents(Vec::new(), &storage)
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
            &storage,
        )
        .await?;
    owner
        .add_member_to_group(
            repo_agents.clone(),
            &core_docs,
            Access::Admin,
            BTreeMap::new(),
            &storage,
        )
        .await?;

    let initial_ref = vec![7; 32];
    let doc_id = owner
        .create_doc(vec![core_docs.into()], nonempty![[7; 32]], &storage)
        .await?;
    let keyhive = owner.clone_keyhive();
    let kh_doc_id = keyhive_doc_id(doc_id)?;
    let doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_eyre("new document is missing")?;
    let encrypted = keyhive
        .try_encrypt_content(doc, &initial_ref, &Vec::new(), b"initial")
        .await?;
    if let Some(update) = encrypted.update_op().cloned() {
        persist_cgka_update_ops(&storage, vec![update]).await?;
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
            &storage,
        )
        .await?;

    let checkpoint_ref = vec![8; 32];
    let doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_eyre("document disappeared before checkpoint")?;
    let checkpoint = keyhive
        .try_encrypt_content(
            doc,
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
        &storage,
        vec![update],
        vec![local_secret],
    )
    .await?;
    let restored = BigKeyhiveHandle::restore_from_storage_archive(owner_seed, &storage, listener)
        .await?
        .ok_or_eyre("owner archive is missing")?;
    restored.import_prekey_secrets(&storage).await?;
    restored.ingest_from_storage(&storage).await?;
    let restored_keyhive = restored.clone_keyhive();
    let restored_doc = restored_keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_eyre("restored document is missing")?;
    restored_keyhive
        .try_encrypt_content(
            restored_doc,
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
