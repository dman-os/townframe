use super::*;

fn descriptor(group: [u8; 32]) -> TaskPoolDescriptor {
    let key_ref = daybook_types::url::build_facet_ref(
        &DocumentId::new([3; 32]).to_string(),
        &daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::Jwk),
    )
    .unwrap();
    TaskPoolDescriptor {
        pool_id: TaskPoolId::for_processor("example/plug", "processor"),
        authority_group: group,
        active_task_part: PartKey::new(b"task-part/first"),
        register_scope: b"pool/processor".to_vec(),
        register_incarnation: [7; 32],
        allowed_key_refs: [key_ref.clone()].into(),
        publication_key: PinnedJwk {
            key_ref,
            heads: daybook_types::doc::ChangeHashSet(vec![automerge::ChangeHash([8; 32])].into()),
        },
        archive_part: None,
        router_slot: ObjKey::new(b"router/processor"),
        router_heartbeat_topic: BigEphemeralTopic::new([2; 32]),
        allowed_rpc_transports: BTreeSet::from([RpcTransport::IrpcIroh]),
        retention_class: RetentionClass::UntilAuthoritativeRemoval,
        routing_defaults: RoutingDefaults::SharedElection,
    }
}

fn bind_document(descriptor: &mut TaskPoolDescriptor, document: DocumentId) {
    let reference =
        daybook_types::url::parse_facet_ref(&descriptor.publication_key.key_ref).unwrap();
    let key_ref =
        daybook_types::url::build_facet_ref(&document.to_string(), &reference.facet_key).unwrap();
    descriptor.allowed_key_refs = [key_ref.clone()].into();
    descriptor.publication_key.key_ref = key_ref;
}

fn actor(label: &str) -> ActorId {
    ActorId::from(label.as_bytes())
}

fn seed() -> automerge::Automerge {
    let mut doc = automerge::Automerge::new();
    let mut tx = doc.transaction();
    tx.put(automerge::ROOT, "unrelated", "retained").unwrap();
    tx.commit();
    doc
}

#[test]
fn processor_mapping_separates_tuple_boundaries() {
    assert_ne!(
        TaskPoolId::for_processor("a/b", "c"),
        TaskPoolId::for_processor("a", "b/c")
    );
}

#[test]
fn concurrent_directory_root_and_pool_maps_preserve_independent_references() {
    for shared_root in [false, true] {
        let mut base = seed();
        if shared_root {
            let mut tx = base.transaction();
            tx.put_object(automerge::ROOT, DIRECTORY, ObjType::Map)
                .unwrap();
            tx.commit();
        }
        let pool = descriptor([1; 32]).pool_id;
        let left_reference = PoolReference {
            document: DocumentId::new([3; 32]),
            authority_group: [1; 32],
        };
        let right_reference = PoolReference {
            document: DocumentId::new([4; 32]),
            authority_group: [1; 32],
        };
        let mut left = base.fork();
        let mut right = base.fork();
        write_reference(
            &mut left,
            &actor("left"),
            &pool,
            &left_reference.key().unwrap(),
        )
        .unwrap();
        write_reference(
            &mut right,
            &actor("right"),
            &pool,
            &right_reference.key().unwrap(),
        )
        .unwrap();
        left.merge(&mut right).unwrap();
        assert_eq!(
            read_directory(&left, &pool).unwrap(),
            BTreeSet::from([left_reference, right_reference])
        );
    }
}

#[test]
fn concurrent_complete_descriptors_remain_conflicting_not_fieldwise_combined() {
    let mut base = seed();
    let first = descriptor([1; 32]);
    write_descriptor(
        &mut base,
        &actor("initial"),
        &first.pool_id,
        &first.encode().unwrap(),
    )
    .unwrap();
    let mut left = base.fork();
    let mut right = base.fork();
    let mut left_descriptor = first.clone();
    left_descriptor.active_task_part = PartKey::new(b"part/left");
    let mut right_descriptor = first.clone();
    right_descriptor.router_slot = ObjKey::new(b"slot/right");
    write_descriptor(
        &mut left,
        &actor("left"),
        &first.pool_id,
        &left_descriptor.encode().unwrap(),
    )
    .unwrap();
    write_descriptor(
        &mut right,
        &actor("right"),
        &first.pool_id,
        &right_descriptor.encode().unwrap(),
    )
    .unwrap();
    left.merge(&mut right).unwrap();
    let values = read_descriptor(&left, &first.pool_id).unwrap();
    assert!(values.contains(&left_descriptor));
    assert!(values.contains(&right_descriptor));
    let before = left.get_heads();
    assert!(
        write_descriptor(
            &mut left,
            &actor("overwrite"),
            &first.pool_id,
            &first.encode().unwrap()
        )
        .is_err()
    );
    assert_eq!(left.get_heads(), before);
}

#[test]
fn unknown_remote_policy_and_transport_are_explicit_rejections() {
    let original = descriptor([1; 32]).encode().unwrap();
    for (field, value, expected) in [
        (
            "protocol",
            serde_json::json!("daybook/task-pool/v99"),
            PoolMetadataRejection::UnsupportedProtocol("daybook/task-pool/v99".into()),
        ),
        (
            "allowed_rpc_transports",
            serde_json::json!(["future-transport"]),
            PoolMetadataRejection::UnsupportedTransport("future-transport".into()),
        ),
        (
            "retention_class",
            serde_json::json!("future-retention"),
            PoolMetadataRejection::UnsupportedRetentionClass("future-retention".into()),
        ),
        (
            "routing_defaults",
            serde_json::json!("future-routing"),
            PoolMetadataRejection::UnsupportedRoutingDefaults("future-routing".into()),
        ),
    ] {
        let mut json: serde_json::Value = serde_json::from_str(&original).unwrap();
        json[field] = value;
        assert_eq!(TaskPoolDescriptor::decode(&json.to_string()), Err(expected));
    }
    assert!(matches!(
        TaskPoolDescriptor::decode("not JSON"),
        Err(PoolMetadataRejection::Malformed(_))
    ));
}

#[test]
fn provisioned_binding_roundtrip_and_rejections() {
    let original = descriptor([1; 32]);
    assert_eq!(
        TaskPoolDescriptor::decode(&original.encode().unwrap()).unwrap(),
        original
    );
    for field in [
        "register_scope",
        "register_incarnation",
        "allowed_key_refs",
        "publication_key",
    ] {
        let mut wire: serde_json::Value =
            serde_json::from_str(&original.encode().unwrap()).unwrap();
        wire.as_object_mut().unwrap().remove(field);
        assert!(matches!(
            TaskPoolDescriptor::decode(&wire.to_string()),
            Err(PoolMetadataRejection::Malformed(_))
        ));
    }
    for invalid in 0..8 {
        let mut candidate = original.clone();
        match invalid {
            0 => candidate.register_scope.clear(),
            1 => candidate.publication_key.heads = Default::default(),
            2 => candidate.allowed_key_refs.clear(),
            3 | 5 | 6 => {
                candidate
                    .publication_key
                    .key_ref
                    .set_query(Some(match invalid {
                        3 => "branch=main",
                        5 => "at=pinned",
                        6 => "unknown=ignored-by-parser",
                        _ => unreachable!(),
                    }));
                candidate.allowed_key_refs = [candidate.publication_key.key_ref.clone()].into();
            }
            4 => {
                let foreign = daybook_types::url::build_facet_ref(
                    &DocumentId::new([9; 32]).to_string(),
                    &daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::Jwk),
                )
                .unwrap();
                candidate.allowed_key_refs.insert(foreign);
            }
            7 => {
                let reference =
                    daybook_types::url::parse_facet_ref(&candidate.publication_key.key_ref)
                        .unwrap();
                candidate.publication_key.key_ref = daybook_types::url::build_facet_ref(
                    reference.doc_id.as_str(),
                    &daybook_types::doc::FacetKey::from(
                        daybook_types::doc::WellKnownFacetTag::Note,
                    ),
                )
                .unwrap();
                candidate.allowed_key_refs = [candidate.publication_key.key_ref.clone()].into();
            }
            _ => unreachable!(),
        }
        // Remote inputs must be rejected too, not only locally encoded values.
        let mut wire: serde_json::Value =
            serde_json::from_str(&original.encode().unwrap()).unwrap();
        wire["register_scope"] = serde_json::to_value(&candidate.register_scope).unwrap();
        wire["allowed_key_refs"] = serde_json::to_value(&candidate.allowed_key_refs).unwrap();
        wire["publication_key"] = serde_json::to_value(&candidate.publication_key).unwrap();
        assert!(matches!(
            TaskPoolDescriptor::decode(&wire.to_string()),
            Err(PoolMetadataRejection::Malformed(_))
        ));
    }
}

#[tokio::test]
async fn register_watch_reload_and_change_physical_part_without_changing_logical_pool() -> Res<()> {
    let (big_repo, _sync, stop) = crate::test_support::boot_repo().await?;
    let group = big_repo.create_group_with_parents(Vec::new()).await?;
    big_repo
        .add_admin_member_to_group(big_repo.local_keyhive_agent().await?, &group)
        .await?;
    let group_id = group.id().to_bytes();
    let directory = big_repo.create_doc(seed()).await?;
    let metadata = big_repo
        .create_doc_with_parents(seed(), vec![group.into()])
        .await?;
    let repo = PoolRepo::new(
        Arc::clone(&big_repo),
        directory.document_id(),
        actor("pool"),
    );
    let mut first = descriptor(group_id);
    bind_document(&mut first, metadata.document_id());
    let mut watch = repo.watch(first.pool_id.clone(), group_id).await?;
    assert_eq!(watch.initial(), &PoolDiscovery::Absent);
    let reference = repo.register(metadata.document_id(), first.clone()).await?;
    let PoolDiscovery::Ready(snapshot) = watch.changed().await? else {
        eyre::bail!("registered pool was not Ready");
    };
    assert_eq!(snapshot.descriptor, first);
    assert_eq!(snapshot.reference, reference);
    let unchanged_heads = metadata.with_document_read(|doc| doc.get_heads()).await;
    repo.register(reference.document.clone(), first.clone())
        .await?;
    assert_eq!(
        metadata.with_document_read(|doc| doc.get_heads()).await,
        unchanged_heads
    );
    let mut rotated = first.clone();
    rotated.active_task_part = PartKey::new(b"task-part/rotated");
    repo.register(reference.document.clone(), rotated.clone())
        .await?;
    let reloaded = PoolRepo::new(
        Arc::clone(&big_repo),
        directory.document_id(),
        actor("reloaded"),
    );
    let PoolDiscovery::Ready(snapshot) = reloaded.discover(&first.pool_id, group_id).await? else {
        eyre::bail!("reloaded pool was not Ready");
    };
    assert_eq!(snapshot.descriptor, rotated);
    assert_eq!(snapshot.reference, reference);
    let mut second = first.clone();
    second.pool_id = TaskPoolId::for_processor("example/plug", "another-processor");
    second.router_slot = ObjKey::new(b"router/another-processor");
    repo.register(reference.document.clone(), second.clone())
        .await?;
    let PoolDiscovery::Ready(second_snapshot) = repo.discover(&second.pool_id, group_id).await?
    else {
        eyre::bail!("shared-document second pool was not Ready");
    };
    assert_eq!(second_snapshot.descriptor, second);
    let PoolDiscovery::Ready(first_snapshot) = repo.discover(&first.pool_id, group_id).await?
    else {
        eyre::bail!("shared-document first pool was lost");
    };
    assert_eq!(first_snapshot.descriptor, rotated);
    assert_eq!(
        directory
            .with_document_read(|doc| doc
                .get(automerge::ROOT, "unrelated")
                .unwrap()
                .unwrap()
                .0
                .as_str()
                .map(str::to_owned))
            .await,
        Some("retained".into())
    );
    drop(watch);
    stop().await
}

#[tokio::test]
async fn known_missing_reference_is_pending_and_duplicate_hints_are_conflict() -> Res<()> {
    let (big_repo, _sync, stop) = crate::test_support::boot_repo().await?;
    let directory = big_repo.create_doc(seed()).await?;
    let repo = PoolRepo::new(big_repo, directory.document_id(), actor("pool"));
    let pool = descriptor([1; 32]).pool_id;
    let missing = PoolReference {
        document: DocumentId::new([8; 32]),
        authority_group: [1; 32],
    };
    directory
        .with_document(|doc| {
            write_reference(doc, &actor("publisher"), &pool, &missing.key().unwrap())
        })
        .await??;
    assert_eq!(
        repo.discover(&pool, [1; 32]).await?,
        PoolDiscovery::Pending {
            documents: BTreeSet::from([missing.document.clone()])
        }
    );
    assert_eq!(
        repo.discover(&pool, [2; 32]).await?,
        PoolDiscovery::Rejected(PoolMetadataRejection::WrongAuthority)
    );
    let duplicate = PoolReference {
        document: DocumentId::new([9; 32]),
        authority_group: [1; 32],
    };
    directory
        .with_document(|doc| {
            write_reference(doc, &actor("other"), &pool, &duplicate.key().unwrap())
        })
        .await??;
    assert_eq!(
        repo.discover(&pool, [1; 32]).await?,
        PoolDiscovery::Conflict {
            references: BTreeSet::from([missing, duplicate])
        }
    );
    stop().await
}

#[tokio::test]
async fn lookup_binding_is_neutral_and_conflicts_across_authority_groups() -> Res<()> {
    let (big_repo, _sync, stop) = crate::test_support::boot_repo().await?;
    let directory = big_repo.create_doc(seed()).await?;
    let repo = PoolRepo::new(
        Arc::clone(&big_repo),
        directory.document_id(),
        actor("owner"),
    );
    let pool = TaskPoolId::for_processor("explicit/plug", "managed-processor");
    assert_eq!(repo.lookup_binding(&pool).await?, PoolBindingLookup::Absent);
    let first = PoolReference {
        document: DocumentId::new([21; 32]),
        authority_group: [1; 32],
    };
    directory
        .with_document(|doc| write_reference(doc, &actor("first"), &pool, &first.key().unwrap()))
        .await??;
    assert_eq!(
        repo.lookup_binding(&pool).await?,
        PoolBindingLookup::Hint(first.clone())
    );
    // The lookup does not require descriptor bytes or confer authority to read them.
    assert_eq!(
        repo.load_descriptor(&first, &pool, first.authority_group)
            .await?,
        PoolDiscovery::Pending {
            documents: BTreeSet::from([first.document.clone()])
        }
    );
    let other_group = PoolReference {
        document: first.document.clone(),
        authority_group: [2; 32],
    };
    directory
        .with_document(|doc| {
            write_reference(doc, &actor("second"), &pool, &other_group.key().unwrap())
        })
        .await??;
    assert_eq!(
        repo.lookup_binding(&pool).await?,
        PoolBindingLookup::Conflict {
            references: BTreeSet::from([first, other_group])
        }
    );
    let unknown_directory = PoolRepo::new(big_repo, DocumentId::new([22; 32]), actor("unknown"));
    assert_eq!(
        unknown_directory.lookup_binding(&pool).await?,
        PoolBindingLookup::Pending {
            document: DocumentId::new([22; 32])
        }
    );
    stop().await
}

#[tokio::test]
async fn lookup_binding_rejects_malformed_directory_metadata() -> Res<()> {
    let (big_repo, _sync, stop) = crate::test_support::boot_repo().await?;
    let mut malformed_directory = seed();
    let mut tx = malformed_directory.transaction();
    tx.put(automerge::ROOT, DIRECTORY, "not a map")?;
    tx.commit();
    let directory = big_repo.create_doc(malformed_directory).await?;
    let repo = PoolRepo::new(big_repo, directory.document_id(), actor("owner"));
    let pool = TaskPoolId::for_processor("explicit/plug", "managed-processor");
    assert!(matches!(
        repo.lookup_binding(&pool).await?,
        PoolBindingLookup::Rejected(PoolMetadataRejection::Malformed(_))
    ));
    stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_group_revocation_wakes_ready_watch_while_direct_document_read_survives() -> Res<()>
{
    use big_repo::keyhive_core::access::Access;
    let owner = crate::tasks::test_util::TaskTestNode::boot(171).await?;
    let reader = crate::tasks::test_util::TaskTestNode::boot(172).await?;
    let reader_agent = owner
        .repo
        .receive_keyhive_contact_card(&reader.repo.local_keyhive_contact_card())
        .await?;
    reader
        .repo
        .receive_keyhive_contact_card(&owner.repo.local_keyhive_contact_card())
        .await?;
    let local = owner.repo.local_keyhive_agent().await?;
    let group = owner.repo.create_group_with_parents(Vec::new()).await?;
    owner
        .repo
        .add_admin_member_to_group(local.clone(), &group)
        .await?;
    let nested = owner.repo.create_group_with_parents(Vec::new()).await?;
    owner.repo.add_admin_member_to_group(local, &nested).await?;
    owner
        .repo
        .add_member_to_group(reader_agent.clone(), &nested, Access::Read)
        .await?;
    owner
        .repo
        .add_member_to_group(nested.clone(), &group, Access::Read)
        .await?;
    let directory = owner.repo.create_doc(seed()).await?;
    let metadata = owner
        .repo
        .create_doc_with_parents(seed(), vec![group.clone().into()])
        .await?;
    owner
        .repo
        .grant_doc_access(directory.document_id(), reader_agent.clone(), Access::Read)
        .await?;
    owner
        .repo
        .grant_doc_access(metadata.document_id(), reader_agent.clone(), Access::Read)
        .await?;
    let mut descriptor = descriptor(group.id().to_bytes());
    bind_document(&mut descriptor, metadata.document_id());
    let publisher = PoolRepo::new(
        Arc::clone(&owner.repo),
        directory.document_id(),
        actor("publisher"),
    );
    let reference = publisher
        .register(metadata.document_id(), descriptor.clone())
        .await?;
    let connection = reader
        .repo
        .open_connection_iroh(
            reader.endpoint.clone(),
            owner.endpoint.addr(),
            owner.repo.local_peer_id(),
            /*end_signal_tx*/ None,
        )
        .await?;
    reader
        .repo
        .sync_keyhive_with_peer(owner.repo.local_peer_id())
        .await?;
    reader
        .repo
        .sync_doc_with_peer(directory.document_id(), owner.repo.local_peer_id())
        .await?;
    reader
        .repo
        .sync_doc_with_peer(metadata.document_id(), owner.repo.local_peer_id())
        .await?;
    owner.repo.wait_for_quiescence(/*timeout*/ None).await?;
    reader.repo.wait_for_quiescence(/*timeout*/ None).await?;

    let consumer = PoolRepo::new(
        Arc::clone(&reader.repo),
        directory.document_id(),
        actor("consumer"),
    );
    let mut watch = consumer
        .watch(descriptor.pool_id.clone(), descriptor.authority_group)
        .await?;
    let PoolDiscovery::Ready(initial) = watch.initial() else {
        eyre::bail!("nested Read did not authorize initial descriptor");
    };
    assert_eq!(initial.descriptor, descriptor);
    let admitted_snapshot = initial.clone();
    admitted_snapshot.register_binding(&reader.repo).await?;
    let (_domain_ticket, mut domain_rx) = reader
        .repo
        .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
        .await?;

    owner
        .repo
        .revoke_member_from_group_for_test(reader_agent, &nested)
        .await?;
    reader
        .repo
        .sync_keyhive_with_peer(owner.repo.local_peer_id())
        .await?;
    owner.repo.wait_for_quiescence(/*timeout*/ None).await?;
    reader.repo.wait_for_quiescence(/*timeout*/ None).await?;
    let mut removed_nested = false;
    while let Ok(batch) = domain_rx.try_recv() {
        for event in batch {
            match event {
                big_repo::BigRepoDomainNotification::MemberRemovedFromGroup {
                    group_id, ..
                } => {
                    assert_ne!(group_id.to_bytes(), descriptor.authority_group);
                    removed_nested |= group_id.to_bytes() == nested.id().to_bytes();
                }
                big_repo::BigRepoDomainNotification::MemberAddedToGroup { group_id, .. } => {
                    assert_ne!(group_id.to_bytes(), descriptor.authority_group)
                }
                big_repo::BigRepoDomainNotification::DocumentAccessChanged { doc_id, .. }
                | big_repo::BigRepoDomainNotification::DocumentAccessRevoked { doc_id, .. }
                | big_repo::BigRepoDomainNotification::DocumentKeyRotated { doc_id } => {
                    assert_ne!(doc_id, reference.document)
                }
                big_repo::BigRepoDomainNotification::DocumentAddedToGroup { .. }
                | big_repo::BigRepoDomainNotification::DocumentRemovedFromGroup { .. } => {}
            }
        }
    }
    assert!(
        removed_nested,
        "real nested-target revocation was not delivered"
    );
    let direct = reader
        .repo
        .get_doc(&reference.document)
        .await?
        .into_ready(reference.document.clone())?;
    assert_eq!(
        direct
            .with_document_read(|doc| read_descriptor(doc, &descriptor.pool_id))
            .await?,
        vec![descriptor.clone()]
    );
    assert!(
        admitted_snapshot
            .register_binding(&reader.repo)
            .await
            .is_err(),
        "cached Read snapshot must not authorize a binding after group revocation"
    );
    assert_eq!(
        watch.changed().await?,
        PoolDiscovery::Rejected(PoolMetadataRejection::Unauthorized)
    );
    assert_eq!(
        consumer
            .discover(&descriptor.pool_id, descriptor.authority_group)
            .await?,
        PoolDiscovery::Rejected(PoolMetadataRejection::Unauthorized)
    );
    drop(watch);
    drop(_domain_ticket);
    connection.stop().await?;
    reader.stop().await?;
    owner.stop().await
}
