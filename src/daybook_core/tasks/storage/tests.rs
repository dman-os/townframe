use super::*;
use crate::blobs::encrypt::decrypt_bytes;
use automerge::{ReadDoc, transaction::Transactable};
use daybook_types::doc::*;
fn seed() -> automerge::Automerge {
    let mut doc = automerge::Automerge::new();
    let mut tx = doc.transaction();
    tx.put(automerge::ROOT, "version", "0").unwrap();
    tx.commit();
    doc
}
fn jwk(key: &MasterKey) -> FacetRaw {
    serde_json::to_value(JwkOct::from_master_key(key)).unwrap()
}

fn write_jwk(
    doc: &mut automerge::Automerge,
    facet: &FacetKey,
    key: &MasterKey,
) -> Res<ChangeHashSet> {
    let mut tx = doc.transaction();
    let facets = match tx.get(automerge::ROOT, "facets")? {
        Some((automerge::Value::Object(automerge::ObjType::Map), object)) => object,
        None => tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?,
        Some(_) => panic!("fixture facets must be a map"),
    };
    autosurgeon::reconcile_prop(
        &mut tx,
        &facets,
        autosurgeon::Prop::Key(facet.to_string().into()),
        am_utils_rs::codecs::ThroughJson(jwk(key)),
    )?;
    tx.commit();
    Ok(ChangeHashSet(doc.get_heads().into()))
}
fn rep(
    snapshot: &big_sync_core::encrypted_register::RegisterSnapshot,
) -> &big_sync_core::encrypted_register::Representation {
    let LaneState::Current { representation } = snapshot.lanes.values().next().unwrap() else {
        panic!("current lane")
    };
    representation
}
#[tokio::test(flavor = "multi_thread")]
async fn pinned_keys_sequence_gaps_replay_and_consumer_settlement() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let (repo, _sync, stop) = crate::test_support::boot_disk_repo(dir.path().join("repo")).await?;
    let key1 = MasterKey::random();
    let key2 = MasterKey::random();
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let group = repo.create_group_with_parents(vec![]).await?;
    repo.add_admin_member_to_group(repo.local_keyhive_agent().await?, &group)
        .await?;
    let mut pool_doc = seed();
    write_jwk(&mut pool_doc, &facet, &key1)?;
    let key_doc_handle = repo
        .create_doc_with_parents(pool_doc, vec![group.clone().into()])
        .await?;
    let key_doc = DocId::from(key_doc_handle.document_id().to_string());
    let key_ref = daybook_types::url::build_facet_ref(&key_doc, &facet)?;
    let heads1 = key_doc_handle
        .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
        .await;
    repo.wait_for_quiescence(None).await?;
    let authority = repo
        .coordination_authority(key_doc_handle.document_id(), group.id().to_bytes())
        .await?;
    let parts = Arc::new(SqlitePartStore::new(repo.sql_ctx(), "register-host-smoke", 4).await?);
    let binding = |heads| RegisterBinding {
        scope: b"pool/smoke".to_vec(),
        part: PartKey::new(b"register-host-smoke/active"),
        authority: authority.clone(),
        allowed_key_refs: [key_ref.clone()].into(),
        publication_key: PinnedJwk {
            key_ref: key_ref.clone(),
            heads,
        },
        incarnation: [1; 32],
    };
    let directory = repo.create_doc(seed()).await?;
    let pools = crate::tasks::pool::PoolRepo::new(
        Arc::clone(&repo),
        directory.document_id(),
        automerge::ActorId::from(b"storage-provisioner".as_slice()),
    );
    let descriptor = crate::tasks::pool::TaskPoolDescriptor {
        pool_id: crate::tasks::test_util::pool(),
        authority_group: group.id().to_bytes(),
        active_task_part: PartKey::new(b"register-host-smoke/active"),
        register_scope: b"pool/smoke".to_vec(),
        register_incarnation: [1; 32],
        allowed_key_refs: [key_ref.clone()].into(),
        publication_key: PinnedJwk {
            key_ref: key_ref.clone(),
            heads: heads1,
        },
        archive_part: None,
        router_slot: ObjKey::new(b"router"),
        router_heartbeat_topic: big_repo::BigEphemeralTopic::new([5; 32]),
        allowed_rpc_transports: [crate::tasks::pool::RpcTransport::IrpcIroh].into(),
        retention_class: crate::tasks::pool::RetentionClass::UntilAuthoritativeRemoval,
        routing_defaults: crate::tasks::pool::RoutingDefaults::SharedElection,
    };
    let reference = pools
        .register(key_doc_handle.document_id(), descriptor.clone())
        .await?;
    let crate::tasks::pool::PoolDiscovery::Ready(snapshot) = pools
        .load_descriptor(&reference, &descriptor.pool_id, descriptor.authority_group)
        .await?
    else {
        eyre::bail!("provisioned native descriptor unavailable");
    };
    let checked_binding = snapshot.register_binding(&repo).await?;
    // Native document publication shares this store and can advance its global
    // revision concurrently. Rejection must leave this pool publication locus absent.
    let control = ObjKey::new(descriptor.active_task_part.as_bytes());
    assert!(!HostPartStore::obj_exists(&*parts, control.clone()).await?);
    let mut invalid_binding = snapshot.register_binding(&repo).await?;
    invalid_binding.publication_key.heads = Default::default();
    assert!(
        RegisterStore::open(Arc::clone(&parts), Arc::clone(&repo), invalid_binding)
            .await
            .is_err()
    );
    let mut invalid_binding = snapshot.register_binding(&repo).await?;
    invalid_binding
        .allowed_key_refs
        .insert(daybook_types::url::build_facet_ref(
            &big_repo::DocumentId::new([23; 32]).to_string(),
            &facet,
        )?);
    assert!(
        RegisterStore::open(Arc::clone(&parts), Arc::clone(&repo), invalid_binding)
            .await
            .is_err()
    );
    assert!(!HostPartStore::obj_exists(&*parts, control).await?);
    assert_eq!(
        HostPartStore::member_count(&*parts, descriptor.active_task_part.clone()).await?,
        0
    );
    let store = RegisterStore::open(Arc::clone(&parts), Arc::clone(&repo), checked_binding).await?;
    store
        .publish_local(b"slot", vec![], b"before rotation".to_vec())
        .await?;
    let old = store.current(b"slot").await?.unwrap();
    let original = store.open_original(rep(&old)).await?;
    assert_eq!(original.body, b"before rotation");
    assert!(decrypt_bytes(&key2, &rep(&old).ciphertext).is_err());
    assert_eq!(store.reserve_sequence(rep(&old).original.writer).await?, 2);
    let heads2 = key_doc_handle
        .with_document(|doc| write_jwk(doc, &facet, &key2))
        .await??;
    let reopened = RegisterStore::open(
        Arc::clone(&parts),
        Arc::clone(&repo),
        binding(heads2.clone()),
    )
    .await?;
    assert_eq!(reopened.open_original(rep(&old)).await?, original);
    reopened
        .publish_local(b"slot", vec![], b"after rotation".to_vec())
        .await?;
    let latest = reopened.current(b"slot").await?.unwrap();
    assert_eq!(rep(&latest).original.writer_seq, 3);
    assert_eq!(
        reopened.open_original(rep(&latest)).await?.body,
        b"after rotation"
    );
    let outcome = reopened
        .receive(
            &big_sync_core::ObjKey::new(reopened.key(b"slot")?.encode()),
            std::slice::from_ref(reopened.part()),
            serde_json::to_value(old)?,
        )
        .await?;
    assert_eq!(outcome, MergeOutcome::Unchanged);
    let obj = big_sync_core::ObjKey::new(reopened.key(b"slot")?.encode());
    let mut tx = parts.begin_obj_write(obj.clone()).await?;
    assert_eq!(tx.payload().await?, Some(serde_json::to_value(&latest)?));
    tx.commit().await?;
    // Part hints must never create membership in an unrelated processor partition.
    assert!(
        reopened
            .receive(
                &obj,
                &[big_sync_core::PartKey::new(b"other processor")],
                serde_json::to_value(&latest)?
            )
            .await
            .is_err()
    );
    let mut invalid: RegisterSnapshot = serde_json::from_value(serde_json::to_value(&latest)?)?;
    invalid.key.slot = b"forged-slot".to_vec();
    let LaneState::Current { representation } = invalid.lanes.values_mut().next().unwrap() else {
        panic!("current lane")
    };
    representation.original.key = invalid.key.clone();
    let forged = big_sync_core::ObjKey::new(invalid.key.encode());
    assert!(
        reopened
            .receive(
                &forged,
                std::slice::from_ref(reopened.part()),
                serde_json::to_value(invalid)?
            )
            .await
            .is_err()
    );
    let mut tx = parts.begin_obj_write(forged.clone()).await?;
    let allocated: i64 =
        sqlx::query_scalar("SELECT count(*) FROM big_sync_objs WHERE scope_id = ? AND obj_id = ?")
            .bind(tx.scope_id())
            .bind(forged.as_bytes())
            .fetch_one(&mut **tx.context_mut())
            .await?;
    assert_eq!(allocated, 0);
    assert_eq!(tx.payload().await?, None);
    tx.commit().await?;
    let through = big_sync::HostPartStore::latest_revision(&*parts).await?;
    let mut settlement = reopened
        .begin_consumer_settlement(b"projector", through)
        .await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS register_test_projection (value TEXT NOT NULL)")
        .execute(&mut **settlement.context_mut())
        .await?;
    sqlx::query("INSERT INTO register_test_projection VALUES ('committed')")
        .execute(&mut **settlement.context_mut())
        .await?;
    settlement.commit().await?;
    assert_eq!(reopened.consumer_checkpoint(b"projector").await?, through);
    let mut rollback = reopened
        .begin_consumer_settlement(b"projector", through)
        .await?;
    sqlx::query("INSERT INTO register_test_projection VALUES ('rolled back')")
        .execute(&mut **rollback.context_mut())
        .await?;
    drop(rollback);
    let mut tx = parts.begin_obj_write(obj).await?;
    let rows: Vec<String> = sqlx::query_scalar("SELECT value FROM register_test_projection")
        .fetch_all(&mut **tx.context_mut())
        .await?;
    assert_eq!(rows, vec!["committed"]);
    tx.commit().await?;
    // Real signed/encrypted task publication shares the same disk transaction path.
    drop(store);
    let tasks = crate::tasks::store::TaskStore::new(reopened, crate::tasks::test_util::pool());
    let mut declaration = crate::tasks::test_util::declaration("durable-task");
    declaration.producer = Some(crate::tasks::NodePubkey::new(
        tasks.register().local_writer().await?,
    ));
    declaration.placement = crate::tasks::Preference::AnyNode;
    let task = declaration.task_id;
    tasks.submit(declaration.clone(), Vec::new()).await?;
    let unsupported = crate::tasks::test_util::declaration("unsupported-schema");
    let unsupported_task = unsupported.task_id;
    tasks
        .register()
        .publish_local(
            &unsupported_task.to_bytes32(),
            vec![],
            serde_json::to_vec(&serde_json::json!({
                "schema": u16::MAX, "declaration": unsupported, "terminal": null
            }))?,
        )
        .await?;
    assert!(
        tasks.ticket(unsupported_task).await.is_err(),
        "unknown retained schemas must not become runnable task declarations"
    );
    let mut collision = declaration;
    collision.input = b"different immutable execution".to_vec();
    assert!(tasks.submit(collision, Vec::new()).await.is_err());
    let cancelled = crate::tasks::test_util::cancelled_fact("cancelled before publication");
    tasks.record_terminal(task, cancelled.clone()).await?;
    let stale_task = tasks.register().current(&task.to_bytes32()).await?.unwrap();
    let success = crate::tasks::test_util::success_fact(71, "durably incorporated effects");
    tasks.record_terminal(task, success.clone()).await?;
    assert_eq!(
        tasks.record_terminal(task, cancelled).await?,
        MergeOutcome::Unchanged
    );
    let task_obj = big_sync_core::ObjKey::new(tasks.register().key(&task.to_bytes32())?.encode());
    assert_eq!(
        tasks
            .register()
            .receive(
                &task_obj,
                std::slice::from_ref(tasks.register().part()),
                serde_json::to_value(stale_task)?
            )
            .await?,
        MergeOutcome::Unchanged
    );
    drop(tasks);
    let reopened =
        RegisterStore::open(Arc::clone(&parts), Arc::clone(&repo), binding(heads2)).await?;
    let recovered_tasks =
        crate::tasks::store::TaskStore::new(reopened, crate::tasks::test_util::pool());
    let recovered = recovered_tasks.ticket(task).await?.unwrap();
    assert_eq!(recovered.terminal.values().next().unwrap().fact, success);
    assert!(recovered.is_success());
    drop(recovered_tasks);
    stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_relay_retains_ciphertext_without_payload_key_or_read_access() -> Res<()> {
    use crate::tasks::test_util::TaskTestNode;
    let owner = TaskTestNode::boot(191).await?;
    let relay = TaskTestNode::boot(192).await?;
    let peer = owner
        .repo
        .receive_keyhive_contact_card(&relay.repo.local_keyhive_contact_card())
        .await?;
    relay
        .repo
        .receive_keyhive_contact_card(&owner.repo.local_keyhive_contact_card())
        .await?;
    let group = owner.repo.create_group_with_parents(vec![]).await?;
    owner
        .repo
        .add_admin_member_to_group(owner.repo.local_keyhive_agent().await?, &group)
        .await?;
    owner
        .repo
        .add_member_to_group(peer.clone(), &group, Access::Relay)
        .await?;
    let key1 = MasterKey::random();
    let key2 = MasterKey::random();
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let mut pool_doc = seed();
    write_jwk(&mut pool_doc, &facet, &key1)?;
    let key_doc_handle = owner
        .repo
        .create_doc_with_parents(pool_doc, vec![group.clone().into()])
        .await?;
    let document = key_doc_handle.document_id();
    let key_doc = DocId::from(document.to_string());
    let key_ref = daybook_types::url::build_facet_ref(&key_doc, &facet)?;
    let heads1 = key_doc_handle
        .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
        .await;
    let connection = relay
        .repo
        .open_connection_iroh(
            relay.endpoint.clone(),
            owner.endpoint.addr(),
            owner.repo.local_peer_id(),
            None,
        )
        .await?;
    relay
        .repo
        .sync_keyhive_with_peer(owner.repo.local_peer_id())
        .await?;
    owner.repo.wait_for_quiescence(None).await?;
    relay.repo.wait_for_quiescence(None).await?;
    let authority = relay
        .repo
        .coordination_authority(document.clone(), group.id().to_bytes())
        .await?;
    assert_eq!(authority.local_access(), Some(Access::Relay));
    let parts =
        Arc::new(SqlitePartStore::new(relay.repo.sql_ctx(), "relay-register-test", 4).await?);
    // The relay knows the reference but cannot decrypt the pool document or its JWK.
    let store = RegisterStore::open(
        Arc::clone(&parts),
        Arc::clone(&relay.repo),
        RegisterBinding {
            scope: b"relay-domain".to_vec(),
            part: PartKey::new(b"relay-domain/active"),
            authority,
            allowed_key_refs: [key_ref.clone()].into(),
            publication_key: PinnedJwk {
                key_ref: key_ref.clone(),
                heads: heads1.clone(),
            },
            incarnation: [3; 32],
        },
    )
    .await?;
    let authority = owner
        .repo
        .coordination_authority(document, group.id().to_bytes())
        .await?;
    let (statement, header) = owner
        .repo
        .with_coordination_signer(&authority, |signer| {
            OriginalStatement::sign(
                store.key(b"slot").unwrap(),
                1,
                vec![],
                b"private body".to_vec(),
                signer.verifying_key(),
                signer,
            )
            .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
        })
        .await?;
    let framing = EncodingParams::DEFAULT;
    let ciphertext = encrypt_with_rs(
        &key1,
        &serde_json::to_vec(&statement)?,
        framing.record_size,
        framing.padding,
    );
    let representation = owner
        .repo
        .with_coordination_signer(&authority, |signer| {
            Representation::sign(
                header,
                RepresentationBinding {
                    incarnation: [3; 32],
                    key_ref: key_ref.as_str().as_bytes().to_vec(),
                    key_heads: heads1.0.iter().map(|head| head.0).collect(),
                    encoding: CONTENT_ENCODING_AES128GCM.as_bytes().to_vec(),
                    parameters: serde_json::to_vec(&framing).unwrap(),
                },
                ciphertext,
                signer.verifying_key(),
                signer,
            )
            .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
        })
        .await?;
    let mut register = EncryptedRegister::new(store.limits(store.key(b"slot")?));
    register.merge_local(representation)?;
    let snapshot = serde_json::to_value(register.snapshot())?;
    let obj = ObjKey::new(store.key(b"slot")?.encode());
    store
        .receive(&obj, std::slice::from_ref(store.part()), snapshot.clone())
        .await?;
    let current = store.current(b"slot").await?.unwrap();
    assert_eq!(serde_json::to_value(&current)?, snapshot);
    assert!(matches!(
        store
            .open_original(rep(&current))
            .await
            .unwrap_err()
            .downcast_ref::<big_repo::CoordinationError>(),
        Some(big_repo::CoordinationError::Unauthorized)
    ));
    assert!(matches!(
        store
            .publish_local(b"slot", vec![], b"unauthorized".to_vec())
            .await
            .unwrap_err()
            .downcast_ref::<big_repo::CoordinationError>(),
        Some(big_repo::CoordinationError::Unauthorized)
    ));
    let mut write = parts.begin_obj_write(obj).await?;
    assert_eq!(write.payload().await?, Some(snapshot));
    write.commit().await?;
    key_doc_handle
        .with_document(|doc| write_jwk(doc, &facet, &key2))
        .await??;
    assert!(decrypt_bytes(&key2, &rep(&current).ciphertext).is_err());
    owner
        .repo
        .add_member_to_group(peer.clone(), &group, Access::Read)
        .await?;
    relay
        .repo
        .sync_keyhive_with_peer(owner.repo.local_peer_id())
        .await?;
    relay
        .repo
        .sync_doc_with_peer(key_doc.parse()?, owner.repo.local_peer_id())
        .await?;
    owner.repo.wait_for_quiescence(None).await?;
    relay.repo.wait_for_quiescence(None).await?;
    assert_eq!(store.open_original(rep(&current)).await?, statement);
    assert_eq!(
        serde_json::to_value(store.current(b"slot").await?.unwrap())?,
        serde_json::to_value(&current)?
    );
    drop(store);
    connection.stop().await?;
    relay.stop().await?;
    owner.stop().await
}
