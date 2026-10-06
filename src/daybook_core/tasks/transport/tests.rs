use super::*;
use crate::blobs::encrypt::{JwkOct, MasterKey};
use crate::repo::{RepoCtx, RepoOpenOptions};
use crate::tasks::pool::{
    PoolDiscovery, PoolRepo, RetentionClass, RoutingDefaults, RpcTransport, TaskPoolDescriptor,
};
use crate::tasks::storage::PinnedJwk;
use crate::tasks::{NodePubkey, Preference};
use automerge::transaction::Transactable;
use big_sync::rpc::{ScopedRequest, WireBigSyncRpcClient};
use big_sync_core::encrypted_register::{LaneState, Representation};
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits, RevisionedStore};
use big_sync_core::rpc::{
    PartEvent, ReplayPageRequest, ReplayRequestId, ReplaySessionId, SubPartsRequest,
    SubscriptionTarget, TargetVerdict,
};
use daybook_types::doc::{ChangeHashSet, FacetKey, UserPathBuf, WellKnownFacetTag};
use keyhive_crypto::verifiable::Verifiable;

struct NativeNode {
    ctx: Arc<RepoCtx>,
    sync: Arc<crate::sync::IrohSyncRepo>,
    sync_stop: crate::sync::IrohSyncRepoStopToken,
    config_stop: crate::repos::RepoStopToken,
    plugs_stop: crate::repos::RepoStopToken,
}

impl NativeNode {
    async fn open(path: &std::path::Path) -> Res<Self> {
        let ctx = RepoCtx::open(
            path,
            RepoOpenOptions::default(),
            "task-transport-test".into(),
        )
        .await?;
        let blobs = crate::blobs::BlobsRepo::new(
            ctx.layout.blobs_root.clone(),
            ctx.local_user_path.clone(),
        )
        .await?;
        let (plugs, plugs_stop) = crate::plugs::PlugsRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&blobs),
            ctx.doc_config.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            Arc::clone(&ctx.sqlite_local_state_repo),
        )
        .await?;
        let (config, config_stop) = crate::config::ConfigRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            plugs,
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
        )
        .await?;
        let (sync, sync_stop) =
            crate::sync::IrohSyncRepo::boot(Arc::clone(&ctx), config, blobs, None).await?;
        Ok(Self {
            ctx,
            sync,
            sync_stop,
            config_stop,
            plugs_stop,
        })
    }

    async fn stop(self) -> Res<()> {
        // The native router's BlobsProtocol owns FsStore shutdown. A second
        // BlobsRepo::shutdown would send to the already-closed actor.
        self.sync_stop.stop().await?;
        drop(self.sync);
        self.config_stop.stop().await?;
        self.plugs_stop.stop().await?;
        self.ctx.shutdown().await
    }
}

async fn live_boundary(reader: &mut dyn big_sync::LocalPartRevisionReader) -> Res<()> {
    loop {
        if matches!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { .. }
        ) {
            return Ok(());
        }
    }
}

async fn task_change(
    reader: &mut dyn big_sync::LocalPartRevisionReader,
    object: &ObjKey,
) -> Res<u64> {
    loop {
        match reader.next(RevisionReadLimits::default()).await? {
            RevisionRead::Entries { revision, entries } => {
                if entries.iter().any(|entry| matches!(entry, PartEvent::Changed(changed) if &changed.obj_id == object)) {
                    return Ok(revision);
                }
            }
            RevisionRead::ReplayComplete { .. } => {}
        }
    }
}

async fn object_synced(
    events: &mut tokio::sync::broadcast::Receiver<big_sync_core::SyncStatEvent>,
    object: &ObjKey,
    peer: &PeerKey,
) -> Res<()> {
    loop {
        if matches!(events.recv().await?, big_sync_core::SyncStatEvent::ObjectSynced { obj_id, peer_id } if &obj_id == object && &peer_id == peer)
        {
            return Ok(());
        }
    }
}

fn replay_request(scope: &str, part: &PartKey, id: u64) -> ScopedRequest<ReplayPageRequest> {
    ScopedRequest {
        scope_key: Arc::from(scope),
        inner: ReplayPageRequest {
            session_id: ReplaySessionId(771),
            request_id: ReplayRequestId(id),
            supersede: None,
            targets: vec![SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: 0,
            }],
            limit: 256,
            hold_ms: 0,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn opaque_processor_domain_relay_retains_exact_slot_without_runtime_or_read() -> Res<()> {
    use crate::rt::triage::domain::{
        ProcessorDomainLoad, ProcessorDomainReference, ProcessorDomainRepo,
    };
    use crate::rt::triage::slots::{ProcessorCapture, ProcessorDesired, ProcessorSlotKey};
    use crate::tasks::pool::PoolReference;
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let a_path = temp.path().join("owner");
    let b_path = temp.path().join("relay");
    let initialized = RepoCtx::init(
        &a_path,
        RepoOpenOptions::default(),
        "processor-relay".into(),
        "owner".into(),
    )
    .await?;
    initialized.shutdown().await?;
    let a = NativeNode::open(&a_path).await?;
    crate::sync::clone_repo_init_from_url(
        &a.sync.get_clone_ticket_url().await?,
        &b_path,
        Default::default(),
    )
    .await?;
    let b = NativeNode::open(&b_path).await?;
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    let agent = a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&b.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    let group = a.ctx.big_repo.create_group_with_parents(vec![]).await?;
    a.ctx
        .big_repo
        .add_admin_member_to_group(a.ctx.big_repo.local_keyhive_agent().await?, &group)
        .await?;
    a.ctx
        .big_repo
        .add_member_to_group(agent.clone(), &group, Access::Relay)
        .await?;
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let mut seed = automerge::Automerge::new();
    let mut tx = seed.transaction();
    let facets = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
    autosurgeon::reconcile_prop(
        &mut tx,
        &facets,
        autosurgeon::Prop::Key(facet.to_string().into()),
        am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
            &MasterKey::random(),
        ))?),
    )?;
    tx.commit();
    let pool = a
        .ctx
        .big_repo
        .create_doc_with_parents(seed.clone(), vec![group.clone().into()])
        .await?;
    let mut tx = seed.transaction();
    autosurgeon::reconcile_prop(
        &mut tx,
        &facets,
        autosurgeon::Prop::Key(facet.to_string().into()),
        am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
            &MasterKey::random(),
        ))?),
    )?;
    tx.commit();
    let domain = a
        .ctx
        .big_repo
        .create_doc_with_parents(seed, vec![group.clone().into()])
        .await?;
    let reference = ProcessorDomainReference {
        document: domain.document_id(),
        authority_group: group.id().to_bytes(),
    };
    let processor = "@test/opaque-relay/process";
    let domains =
        ProcessorDomainRepo::new(Arc::clone(&a.ctx.big_repo), a.ctx.local_actor_id.clone());
    domains
        .provision(
            &reference,
            processor.into(),
            PoolReference {
                document: pool.document_id(),
                authority_group: group.id().to_bytes(),
            },
            PinnedJwk {
                key_ref: daybook_types::url::build_facet_ref(
                    &reference.document.to_string(),
                    &facet,
                )?,
                heads: domain
                    .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                    .await,
            },
        )
        .await?;
    let ProcessorDomainLoad::Ready(snapshot) = domains.load(&reference, processor).await? else {
        eyre::bail!("owner domain unavailable");
    };
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    let authority = b
        .ctx
        .big_repo
        .coordination_authority(reference.document.clone(), reference.authority_group)
        .await?;
    assert_eq!(authority.local_access(), Some(Access::Relay));
    assert!(matches!(
        b.ctx.big_repo.admit_coordination_read(&authority).await,
        Err(CoordinationError::Unauthorized)
    ));
    a.sync.attach_processor_domain(&snapshot).await?;
    b.sync.attach_processor_domain(&snapshot).await?;
    let owner = a.ctx.attach_processor_slot_store(&snapshot).await?;
    let relay = b.ctx.attach_processor_slot_store(&snapshot).await?;
    let mut reader = b
        .sync
        .task_backend()
        .shared_store()
        .open_revision_reader(SubPartsRequest {
            lower_bound: 0,
            targets: [SubscriptionTarget::Part {
                part_id: snapshot.register_binding.part.clone(),
                cursor: 0,
            }]
            .into(),
        })
        .await?
        .map_err(|error| ferr!("processor relay observer: {error:?}"))?;
    live_boundary(&mut *reader).await?;
    let slot = ProcessorSlotKey {
        document_id: "captured-source".into(),
        branch_path: "main".into(),
        processor_full_id: processor.into(),
    };
    let desired = ProcessorDesired {
        capture: ProcessorCapture::new(
            ChangeHashSet(vec![automerge::ChangeHash([41; 32])].into()),
            None,
            [42; 32],
            [43; 32],
        ),
        matches: true,
    };
    let object = ObjKey::new(owner.register().key(&slot.id())?.encode());
    let (received, published) = tokio::join!(
        task_change(&mut *reader, &object),
        owner.evaluate(&slot, desired.clone())
    );
    published?;
    received?;
    let retained = relay.register().current(&slot.id()).await?.unwrap();
    assert_eq!(
        serde_json::to_value(&retained)?,
        serde_json::to_value(owner.register().current(&slot.id()).await?.unwrap())?
    );
    let LaneState::Current { representation } = retained.lanes.values().next().unwrap() else {
        unreachable!("valid owner lane");
    };
    assert!(matches!(
        relay
            .register()
            .open_original(representation)
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    assert!(matches!(
        relay
            .register()
            .publish_local(
                &slot.id(),
                vec![],
                b"not an authorized publication".to_vec()
            )
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    domain
        .with_document(|doc| {
            let mut tx = doc.transaction();
            autosurgeon::reconcile_prop(
                &mut tx,
                &facets,
                autosurgeon::Prop::Key(facet.to_string().into()),
                am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
                    &MasterKey::random(),
                ))?),
            )?;
            tx.commit();
            eyre::Ok(())
        })
        .await??;
    let rotated = PinnedJwk {
        key_ref: snapshot.register_binding.publication_key.key_ref.clone(),
        heads: domain
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await,
    };
    domains
        .provision(&reference, processor.into(), snapshot.pool.clone(), rotated)
        .await?;
    let ProcessorDomainLoad::Ready(rotated) = domains.load(&reference, processor).await? else {
        eyre::bail!("rotated domain unavailable");
    };
    b.sync.attach_processor_domain(&rotated).await?;
    assert_eq!(
        serde_json::to_value(relay.register().current(&slot.id()).await?.unwrap())?,
        serde_json::to_value(&retained)?
    );
    assert!(matches!(
        relay
            .register()
            .open_original(representation)
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    a.ctx
        .big_repo
        .add_member_to_group(agent, &group, Access::Read)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    b.ctx
        .big_repo
        .sync_doc_with_peer(reference.document.clone(), a.ctx.big_repo.local_peer_id())
        .await?;
    let decoded = relay.slot(&slot).await?;
    assert_eq!(decoded.desired().collect::<Vec<_>>(), vec![&desired]);
    assert_eq!(
        serde_json::to_value(relay.register().current(&slot.id()).await?.unwrap())?,
        serde_json::to_value(&retained)?
    );
    b.stop().await?;
    a.stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn native_task_transport_commits_before_revision_delivery_and_revokes_serving() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let a_path = temp.path().join("a");
    let b_path = temp.path().join("b");
    let initialized = RepoCtx::init(
        &a_path,
        RepoOpenOptions::default(),
        "task-pool-repo".into(),
        "producer".into(),
    )
    .await?;
    initialized.shutdown().await?;
    let a = NativeNode::open(&a_path).await?;
    crate::sync::clone_repo_init_from_url(
        &a.sync.get_clone_ticket_url().await?,
        &b_path,
        Default::default(),
    )
    .await?;
    let b = NativeNode::open(&b_path).await?;
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    let a_peer = a.ctx.big_repo.local_peer_id();
    let b_peer = b.ctx.big_repo.local_peer_id();
    let b_agent = a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&b.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    let group = a.ctx.big_repo.create_group_with_parents(vec![]).await?;
    a.ctx
        .big_repo
        .add_admin_member_to_group(a.ctx.big_repo.local_keyhive_agent().await?, &group)
        .await?;
    // Relay-only B receives ciphertext without descriptor/JWK plaintext access.
    a.ctx
        .big_repo
        .add_member_to_group(b_agent.clone(), &group, Access::Relay)
        .await?;
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let key = MasterKey::random();
    let mut document = automerge::Automerge::new();
    let mut tx = document.transaction();
    let facets = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
    autosurgeon::reconcile_prop(
        &mut tx,
        &facets,
        autosurgeon::Prop::Key(facet.to_string().into()),
        am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(&key))?),
    )?;
    tx.commit();
    let metadata = a
        .ctx
        .big_repo
        .create_doc_with_parents(document, vec![group.clone().into()])
        .await?;
    let heads = metadata
        .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
        .await;
    let key_ref = daybook_types::url::build_facet_ref(&metadata.document_id().to_string(), &facet)?;
    let descriptor = TaskPoolDescriptor {
        pool_id: crate::tasks::test_util::pool(),
        authority_group: group.id().to_bytes(),
        active_task_part: PartKey::new(b"native-task-transport/active"),
        register_scope: b"native-task-transport/pool".to_vec(),
        register_incarnation: [61; 32],
        allowed_key_refs: [key_ref.clone()].into(),
        publication_key: PinnedJwk { key_ref, heads },
        archive_part: None,
        router_slot: ObjKey::new(b"native-task-transport/router"),
        router_heartbeat_topic: big_repo::BigEphemeralTopic::new([62; 32]),
        allowed_rpc_transports: [RpcTransport::IrpcIroh].into(),
        retention_class: RetentionClass::UntilAuthoritativeRemoval,
        routing_defaults: RoutingDefaults::SharedElection,
    };
    let a_pools = PoolRepo::new(
        Arc::clone(&a.ctx.big_repo),
        a.ctx.doc_config.document_id(),
        automerge::ActorId::from(b"provisioner".as_slice()),
    );
    let reference = a_pools
        .register(metadata.document_id(), descriptor.clone())
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a_peer.clone())
        .await?;
    b.ctx
        .big_repo
        .sync_doc_with_peer(a.ctx.doc_config.document_id(), a_peer.clone())
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    let b_pools = PoolRepo::new(
        Arc::clone(&b.ctx.big_repo),
        a.ctx.doc_config.document_id(),
        automerge::ActorId::from(b"consumer".as_slice()),
    );
    let PoolDiscovery::Ready(a_snapshot) = a_pools
        .load_descriptor(&reference, &descriptor.pool_id, descriptor.authority_group)
        .await?
    else {
        eyre::bail!("producer descriptor unavailable");
    };
    assert!(!matches!(
        b_pools
            .load_descriptor(&reference, &descriptor.pool_id, descriptor.authority_group)
            .await?,
        PoolDiscovery::Ready(_)
    ));
    // The provisioner supplies routing/key-reference metadata, not key material.
    let b_snapshot = a_snapshot.clone();
    assert!(b_snapshot.register_binding(&b.ctx.big_repo).await.is_err());
    // Attach after the real native connection exists: both peer directions must
    // receive dynamic task routes, not only routes captured at next connection.
    let a_tasks = a.sync.attach_task_pool(a_snapshot.clone()).await?;
    let b_backend = b.sync.task_backend();
    let (committed, release) = b_backend.pause_permission_commit();
    let retained = {
        let attach = b.sync.attach_task_pool(b_snapshot.clone());
        tokio::pin!(attach);
        tokio::select! {
            result = &mut attach => { result?; panic!("attachment escaped its commit gate"); }
            result = committed => result?,
        }
        let retained = b_backend.get(&descriptor.pool_id).unwrap();
        // The real serving rows have committed while the attachment caller is
        // still suspended. Dropping that caller must not orphan the owner.
        let rpc = a.sync.native_rpc_client(b.sync.endpoint_addr());
        let page = rpc
            .replay_page(replay_request(
                TASK_SCOPE_KEY,
                &descriptor.active_task_part,
                800,
            ))
            .await??;
        assert!(matches!(
            page.verdict(&SubscriptionTarget::Part {
                part_id: descriptor.active_task_part.clone(),
                cursor: 0,
            }),
            Some(TargetVerdict::Events { .. }),
        ));
        retained
    };
    drop(release);
    let (b_first, b_second) = tokio::join!(
        b.sync.attach_task_pool(b_snapshot.clone()),
        b.sync.attach_task_pool(b_snapshot.clone())
    );
    let b_tasks = b_first?;
    assert!(Arc::ptr_eq(&b_tasks, &b_second?));
    assert!(Arc::ptr_eq(&retained, &b_tasks));
    drop(retained);
    {
        let mut permission_updates = b_backend.permission_updates.subscribe();
        b_backend.force_pending_once();
        let (denied, release) = b_backend.pause_permission_commit();
        let repair = b.sync.attach_task_pool(b_snapshot.clone());
        tokio::pin!(repair);
        tokio::select! {
            result = &mut repair => { result?; panic!("Pending attachment escaped its commit gate"); }
            result = denied => result?,
        }
        let rpc = a.sync.native_rpc_client(b.sync.endpoint_addr());
        let page = rpc
            .replay_page(replay_request(
                TASK_SCOPE_KEY,
                &descriptor.active_task_part,
                801,
            ))
            .await??;
        assert_eq!(
            page.verdict(&SubscriptionTarget::Part {
                part_id: descriptor.active_task_part.clone(),
                cursor: 0,
            }),
            Some(&TargetVerdict::Unauthorized),
        );
        release.send(()).expect(ERROR_CHANNEL);
        assert!(Arc::ptr_eq(&repair.await?, &b_tasks));
        // No new native mutation/delta is generated: the retained Pending
        // obligation itself must recover fresh serving permissions.
        while permission_updates.recv().await? {}
        let page = rpc
            .replay_page(replay_request(
                TASK_SCOPE_KEY,
                &descriptor.active_task_part,
                802,
            ))
            .await??;
        assert!(matches!(
            page.verdict(&SubscriptionTarget::Part {
                part_id: descriptor.active_task_part.clone(),
                cursor: 0,
            }),
            Some(TargetVerdict::Events { .. }),
        ));
        drop(rpc);
    }
    assert!(Arc::ptr_eq(
        &a_tasks,
        &a.sync.task_store(&descriptor.pool_id).unwrap()
    ));
    let mut changed_binding = b_snapshot;
    changed_binding.descriptor.register_incarnation[0] ^= 1;
    assert!(b.sync.attach_task_pool(changed_binding).await.is_err());
    let a_backend = a.sync.task_backend();
    assert!(Arc::ptr_eq(
        &b_backend.get(&descriptor.pool_id).unwrap(),
        &b_tasks
    ));
    let mut a_reader = a_tasks
        .register()
        .open_consumer_reader(b"producer-domain")
        .await?;
    // Relay owns opaque durable transport, not the Read-gated domain checkpoint.
    let mut b_reader = b_backend
        .shared_store()
        .open_revision_reader(SubPartsRequest {
            lower_bound: 0,
            targets: [SubscriptionTarget::Part {
                part_id: descriptor.active_task_part.clone(),
                cursor: 0,
            }]
            .into(),
        })
        .await?
        .map_err(|error| ferr!("task transport reader: {error:?}"))?;
    live_boundary(&mut *a_reader).await?;
    live_boundary(&mut *b_reader).await?;
    let mut declaration = crate::tasks::test_util::declaration("native-task-exact-input");
    let writer = NodePubkey::new(a_tasks.register().local_writer().await?);
    declaration.producer = Some(writer);
    declaration.placement = Preference::AnyNode;
    let task = declaration.task_id;
    let object = ObjKey::new(a_tasks.register().key(&task.to_bytes32())?.encode());
    let (received, submitted) = tokio::join!(
        task_change(&mut *b_reader, &object),
        a_tasks.submit(declaration.clone(), Vec::new())
    );
    submitted?;
    let b_declaration_revision = received?;
    let a_declaration_revision = task_change(&mut *a_reader, &object).await?;
    assert_eq!(
        serde_json::to_value(b_tasks.register().current(&task.to_bytes32()).await?)?,
        serde_json::to_value(a_tasks.register().current(&task.to_bytes32()).await?)?
    );
    assert!(b_tasks.ticket(task).await.is_err());
    // A real native Read grant now permits exact-head key materialization and
    // projection of the already-retained original; it does not republish T.
    a.ctx
        .big_repo
        .add_member_to_group(b_agent.clone(), &group, Access::Read)
        .await?;
    // Independent document Read survives later group revocation, without being
    // enough to keep task-part serving permission alive.
    a.ctx
        .big_repo
        .grant_doc_access(metadata.document_id(), b_agent.clone(), Access::Read)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a_peer.clone())
        .await?;
    b.ctx
        .big_repo
        .sync_doc_with_peer(metadata.document_id(), a_peer.clone())
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    let PoolDiscovery::Ready(read_snapshot) = b_pools
        .load_descriptor(&reference, &descriptor.pool_id, descriptor.authority_group)
        .await?
    else {
        eyre::bail!("Read grant did not materialize the pool descriptor");
    };
    assert_eq!(read_snapshot.descriptor, descriptor);
    let a_ticket = a_tasks.ticket(task).await?.unwrap();
    assert_eq!(b_tasks.ticket(task).await?, Some(a_ticket.clone()));
    assert_eq!(a_ticket.declaration.declaration, declaration);
    assert_eq!(
        a_ticket
            .declaration
            .publishers
            .iter()
            .map(|evidence| evidence.publisher)
            .collect::<Vec<_>>(),
        vec![writer]
    );
    let stale = a_tasks
        .register()
        .current(&task.to_bytes32())
        .await?
        .unwrap();
    b_tasks
        .register()
        .begin_consumer_settlement(b"consumer-domain", b_declaration_revision)
        .await?
        .commit()
        .await?;
    // A real durable domain SQL commit precedes publishing the terminal fact.
    let mut effects = a_tasks
        .register()
        .begin_consumer_settlement(b"producer-domain", a_declaration_revision)
        .await?;
    sqlx::query(
        "CREATE TABLE task_transport_effects (task_id BLOB PRIMARY KEY, result TEXT NOT NULL)",
    )
    .execute(&mut **effects.context_mut())
    .await?;
    sqlx::query("INSERT INTO task_transport_effects VALUES (?, ?)")
        .bind(task.to_bytes32().as_slice())
        .bind("native-result")
        .execute(&mut **effects.context_mut())
        .await?;
    effects.commit().await?;
    let success = crate::tasks::test_util::success_fact(701, "native-result");
    let (received, completed) = tokio::join!(
        task_change(&mut *b_reader, &object),
        a_tasks.record_terminal(task, success.clone())
    );
    completed?;
    let b_terminal_revision = received?;
    let terminal = a_tasks.ticket(task).await?.unwrap();
    assert_eq!(b_tasks.ticket(task).await?, Some(terminal.clone()));
    assert_eq!(
        terminal.terminal.get(&writer),
        Some(&crate::tasks::SignedTerminalFact {
            writer,
            writer_seq: 2,
            fact: success
        })
    );
    b_tasks
        .register()
        .begin_consumer_settlement(b"consumer-domain", b_terminal_revision)
        .await?
        .commit()
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_transport_effects WHERE task_id = ?"
        )
        .bind(task.to_bytes32().as_slice())
        .fetch_one(&a.ctx.big_repo.sql_ctx().read_pool)
        .await?,
        "native-result"
    );
    assert_eq!(a.ctx.part_store.obj_payload(object.clone()).await?, None);
    assert_eq!(b.ctx.part_store.obj_payload(object.clone()).await?, None);
    assert_eq!(
        a.ctx.derived_part_store.obj_payload(object.clone()).await?,
        None
    );
    let current = b_tasks
        .register()
        .current(&task.to_bytes32())
        .await?
        .unwrap();
    let payload = serde_json::to_value(&current)?;
    assert!(matches!(
        b_backend
            .sync_obj(
                a_peer.clone(),
                object.clone(),
                vec![descriptor.active_task_part.clone()],
                None
            )
            .await?,
        SyncTaskRunOutcome::Stale
    ));
    assert!(
        b_backend
            .sync_obj(
                a_peer.clone(),
                object.clone(),
                vec![PartKey::new(b"unbound-pool")],
                Some(payload.clone())
            )
            .await
            .is_err()
    );
    assert!(
        b_backend
            .sync_obj(
                a_peer.clone(),
                object.clone(),
                vec![
                    descriptor.active_task_part.clone(),
                    PartKey::new(b"ambiguous-pool")
                ],
                Some(payload.clone())
            )
            .await
            .is_err()
    );
    assert!(
        b_backend
            .remove_obj_from_parts(object.clone(), vec![descriptor.active_task_part.clone()])
            .await
            .is_err()
    );
    assert_eq!(
        b_backend.shared_store().obj_payload(object.clone()).await?,
        Some(payload.clone())
    );
    // Replay old retained bytes through actual native BigSync, not B.receive.
    let mut b_events = b.sync.task_sync_events();
    a_backend
        .shared_store()
        .set_obj_payload(object.clone(), serde_json::to_value(stale)?)
        .await?;
    object_synced(&mut b_events, &object, &a_peer).await?;
    assert_eq!(b_tasks.ticket(task).await?, Some(terminal.clone()));
    assert_eq!(
        b_tasks
            .register()
            .consumer_checkpoint(b"consumer-domain")
            .await?,
        b_terminal_revision
    );
    let good = a_tasks
        .register()
        .current(&task.to_bytes32())
        .await?
        .unwrap();
    a_backend
        .shared_store()
        .set_obj_payload(object.clone(), serde_json::to_value(&good)?)
        .await?;
    object_synced(&mut b_events, &object, &a_peer).await?;
    // A native authorized writer signs an invalid representation incarnation.
    // Publish adversarial remote bytes at A; B's actual worker must reject them
    // before its state, part membership or domain cursor changes.
    let mut invalid: big_sync_core::encrypted_register::RegisterSnapshot =
        serde_json::from_value(serde_json::to_value(&good)?)?;
    let LaneState::Current { representation } = invalid.lanes.values_mut().next().unwrap() else {
        panic!("expected current representation");
    };
    let mut binding = representation.binding.clone();
    binding.incarnation[0] ^= 1;
    let authority = a
        .ctx
        .big_repo
        .coordination_authority(metadata.document_id(), descriptor.authority_group)
        .await?;
    *representation = a
        .ctx
        .big_repo
        .with_coordination_signer(&authority, |signer| {
            Representation::sign(
                representation.original.clone(),
                binding,
                representation.ciphertext.clone(),
                signer.verifying_key(),
                signer,
            )
            .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
        })
        .await?;
    let mut rejection = b_backend.admission_errors();
    a_backend
        .shared_store()
        .set_obj_payload(object.clone(), serde_json::to_value(invalid)?)
        .await?;
    let rejected_object = rejection.recv().await?;
    assert_eq!(rejected_object, object);
    assert_eq!(
        b_backend.shared_store().obj_payload(object.clone()).await?,
        Some(payload)
    );
    assert_eq!(
        b_tasks
            .register()
            .consumer_checkpoint(b"consumer-domain")
            .await?,
        b_terminal_revision
    );
    a_backend
        .shared_store()
        .set_obj_payload(object.clone(), serde_json::to_value(&good)?)
        .await?;
    object_synced(&mut b_events, &object, &a_peer).await?;
    // Exercise the reverse native path after a real group Edit grant.
    a.ctx
        .big_repo
        .add_member_to_group(b_agent.clone(), &group, Access::Edit)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a_peer.clone())
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    let mut reverse = crate::tasks::test_util::declaration("native-reverse-path");
    reverse.producer = Some(NodePubkey::new(b_tasks.register().local_writer().await?));
    reverse.placement = Preference::AnyNode;
    let reverse_task = reverse.task_id;
    let reverse_object = ObjKey::new(b_tasks.register().key(&reverse_task.to_bytes32())?.encode());
    let (received, submitted) = tokio::join!(
        task_change(&mut *a_reader, &reverse_object),
        b_tasks.submit(reverse, Vec::new())
    );
    submitted?;
    received?;
    assert_eq!(
        a_tasks.ticket(reverse_task).await?,
        b_tasks.ticket(reverse_task).await?
    );
    // Equivalent meaning can be published by both native writers with different
    // exact-history witnesses; neither witness changes the immutable task identity.
    let mut shared = crate::tasks::test_util::declaration("native-equivalent-witnesses");
    shared.producer = Some(writer);
    shared.placement = Preference::AnyNode;
    let shared_id = shared.task_id;
    let shared_object = ObjKey::new(a_tasks.register().key(&shared_id.to_bytes32())?.encode());
    let (received, submitted) = tokio::join!(
        task_change(&mut *b_reader, &shared_object),
        a_tasks.submit(shared.clone(), b"manifest-head-a".to_vec())
    );
    submitted?;
    received?;
    let b_writer = NodePubkey::new(b_tasks.register().local_writer().await?);
    let mut a_events = a.sync.task_sync_events();
    b_tasks
        .submit(shared.clone(), b"manifest-head-b".to_vec())
        .await?;
    // ObjectSynced observes the completed native pull, unlike a current-frontier
    // Changed event that can belong to A's earlier publication of the same key.
    object_synced(&mut a_events, &shared_object, &b_peer).await?;
    let ticket = a_tasks.ticket(shared_id).await?.unwrap();
    assert_eq!(ticket.declaration.declaration, shared);
    let witnesses: std::collections::BTreeMap<_, _> = ticket
        .declaration
        .publishers
        .iter()
        .map(|evidence| (evidence.publisher, evidence.input_witness.as_slice()))
        .collect();
    assert_eq!(witnesses.get(&writer), Some(&b"manifest-head-a".as_slice()));
    assert_eq!(
        witnesses.get(&b_writer),
        Some(&b"manifest-head-b".as_slice())
    );
    assert_eq!(b_tasks.ticket(shared_id).await?, Some(ticket));
    // Per-node slots stay local even if local part rows grant remote readers.
    let local_slot_part = crate::part_id_from_label(crate::rt::triage::slots::LOCAL_SLOT_PART);
    let slot_key = crate::rt::triage::slots::ProcessorSlotKey {
        document_id: "local-document".into(),
        branch_path: "main".into(),
        processor_full_id: "@test/local-processor".into(),
    };
    let local_slot_object = ObjKey::new(slot_key.id());
    crate::rt::triage::slots::ProcessorSlotStore::local(
        Arc::clone(&a.ctx.derived_part_store),
        [1; 32],
    )
    .evaluate(
        &slot_key,
        crate::rt::triage::slots::ProcessorDesired {
            capture: crate::rt::triage::slots::ProcessorCapture::new(
                daybook_types::doc::ChangeHashSet::default(),
                None,
                [1; 32],
                [2; 32],
            ),
            matches: true,
        },
    )
    .await?;
    a.ctx
        .derived_part_store
        .set_part_members(
            local_slot_part.clone(),
            [(b_peer.clone(), Access::Read)].into(),
        )
        .await?;
    let rpc = b.sync.native_rpc_client(a.sync.endpoint_addr());
    for (request, scope) in ["daybook-core", "daybook-core:derived", TASK_SCOPE_KEY]
        .into_iter()
        .enumerate()
    {
        let page = rpc
            .replay_page(replay_request(scope, &local_slot_part, request as u64))
            .await??;
        assert_eq!(
            page.verdict(&SubscriptionTarget::Part {
                part_id: local_slot_part.clone(),
                cursor: 0
            }),
            Some(&TargetVerdict::UnknownPart)
        );
        assert!(page.events.is_empty());
    }
    assert_eq!(
        b.ctx
            .derived_part_store
            .obj_payload(local_slot_object)
            .await?,
        None
    );
    let target = SubscriptionTarget::Part {
        part_id: descriptor.active_task_part.clone(),
        cursor: 0,
    };
    let before = rpc
        .replay_page(replay_request(
            TASK_SCOPE_KEY,
            &descriptor.active_task_part,
            10,
        ))
        .await??;
    assert!(matches!(
        before.verdict(&target),
        Some(TargetVerdict::Events { .. })
    ));
    assert!(
        before
            .events
            .iter()
            .any(|event| matches!(event, PartEvent::Changed(changed) if changed.obj_id == object))
    );
    let mut progress = a.sync.task_permission_progress();
    a.ctx
        .big_repo
        .revoke_member_from_group_for_test(b_agent, &group)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    let access = big_repo::KeyhiveAccessRevisionStore::<big_sync::SqliteDeltaWalkerStateRepo>::new(
        &a.ctx.big_repo,
    );
    let revoke_revision = access.latest_revision().await?;
    progress
        .wait_for(|through| *through >= revoke_revision)
        .await?;
    let denied = rpc
        .replay_page(replay_request(
            TASK_SCOPE_KEY,
            &descriptor.active_task_part,
            11,
        ))
        .await??;
    assert_eq!(denied.verdict(&target), Some(&TargetVerdict::Unauthorized));
    assert!(denied.events.is_empty());
    assert_eq!(
        a_backend.shared_store().obj_parts(object.clone()).await?,
        vec![descriptor.active_task_part.clone()]
    );
    // Revocation does not pretend physical retirement of retained signed evidence.
    assert_eq!(
        serde_json::to_value(a_tasks.register().current(&task.to_bytes32()).await?)?,
        serde_json::to_value(Some(&good))?
    );
    drop(a_reader);
    drop(b_reader);
    drop(a_tasks);
    drop(b_tasks);
    drop(a_backend);
    drop(b_backend);
    drop(rpc);
    b.stop().await?;
    a.stop().await?;
    let reopened = NativeNode::open(&a_path).await?;
    let mut mismatched = a_snapshot.clone();
    mismatched.descriptor.register_incarnation[0] ^= 1;
    assert!(reopened.sync.attach_task_pool(mismatched).await.is_err());
    let recovered = reopened.sync.attach_task_pool(a_snapshot).await?;
    assert_eq!(recovered.ticket(task).await?, Some(terminal));
    assert_eq!(
        recovered
            .register()
            .consumer_checkpoint(b"producer-domain")
            .await?,
        a_declaration_revision
    );
    drop(recovered);
    reopened.stop().await
}
