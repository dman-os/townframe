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
    owners: crate::test_support::sync_node::Owners,
}

impl NativeNode {
    async fn open(path: &std::path::Path) -> Res<Self> {
        // Native replay is recursive; do not poll it below a large scenario frame.
        let path = path.to_owned();
        tokio::spawn(async move { Self::open_inner(&path).await })
            .await
            .unwrap()
    }

    async fn open_inner(path: &std::path::Path) -> Res<Self> {
        let ctx = RepoCtx::open(
            path,
            RepoOpenOptions::default(),
            "task-transport-test".into(),
        )
        .await?;
        let mut owners = crate::test_support::sync_node::Owners::new(Arc::clone(&ctx));
        let blobs = crate::blobs::BlobsRepo::new(
            ctx.layout.blobs_root.clone(),
            ctx.local_user_path.clone(),
        )
        .await?;
        owners.blobs = Some(Arc::clone(&blobs));
        let (plugs, plugs_stop) = crate::plugs::PlugsRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&blobs),
            ctx.doc_config.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            Arc::clone(&ctx.sqlite_local_state_repo),
        )
        .await?;
        owners.plugs = Some(plugs_stop);
        let (config, config_stop) = crate::config::ConfigRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            plugs,
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
        )
        .await?;
        owners.config = Some(config_stop);
        let (sync, sync_stop) =
            crate::sync::IrohSyncRepo::boot(Arc::clone(&ctx), config, blobs, None).await?;
        owners.sync = Some(sync_stop);
        Ok(Self { ctx, sync, owners })
    }

    async fn stop(self) -> Res<()> {
        // The shared owner drains only services booted here: no Daybook runtime
        // or plaintext capability is introduced into the Relay-only fixture.
        let Self {
            ctx,
            sync,
            mut owners,
        } = self;
        drop((sync, ctx));
        owners.stop(/*runtime_first*/ false).await
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
    Box::pin(opaque_processor_rotation_scenario(false)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn opaque_processor_rotation_recovers_cancelled_admission_before_permissions() -> Res<()> {
    Box::pin(opaque_processor_rotation_scenario(true)).await
}

async fn opaque_processor_rotation_scenario(interrupt_admission: bool) -> Res<()> {
    use crate::rt::triage::domain::{
        ProcessorDomainLoad, ProcessorDomainReference, ProcessorDomainRepo,
    };
    use crate::rt::triage::slots::{ProcessorCapture, ProcessorDesired, ProcessorSlotKey};
    use crate::tasks::pool::PoolReference;
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let a_path = temp.path().join("owner");
    let b_path = temp.path().join("relay");
    tokio::spawn({
        let path = a_path.clone();
        async move {
            RepoCtx::init(
                &path,
                RepoOpenOptions::default(),
                "processor-relay".into(),
                "owner".into(),
            )
            .await?
            .shutdown()
            .await
        }
    })
    .await
    .unwrap()?;
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
    let domain = pool.clone();
    let reference = ProcessorDomainReference {
        document: domain.document_id(),
        authority_group: group.id().to_bytes(),
    };
    let processor = "@test/opaque-relay/process";
    let configuration = crate::rt::triage::domain::ProcessorConfiguration {
        // Explicit one-hour lifetime for this finite native fixture.
        task_lifetime_secs: std::num::NonZeroU64::new(3600).unwrap(),
        manifest: daybook_types::url::build_facet_ref(
            &reference.document.to_string(),
            &FacetKey::from(WellKnownFacetTag::PlugManifest),
        )?,
        config_documents: [("@test/opaque-relay".into(), reference.document.to_string())].into(),
    };
    let domains =
        ProcessorDomainRepo::new(Arc::clone(&a.ctx.big_repo), a.ctx.local_actor_id.clone());
    let pool_reference = PoolReference {
        rotation_generation: 0,
        document: pool.document_id(),
        authority_group: group.id().to_bytes(),
    };
    let pinned_key = PinnedJwk {
        key_ref: daybook_types::url::build_facet_ref(&reference.document.to_string(), &facet)?,
        heads: domain
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await,
    };
    crate::test_support::sync_node::native_admission(|| async {
        domains
            .provision(
                &reference,
                processor.into(),
                configuration.clone(),
                pool_reference.clone(),
                pinned_key.clone(),
            )
            .await
            .map_err(|error| match error {
                crate::rt::triage::domain::ProcessorDomainProvisionError::Authority(error) => {
                    eyre::Report::from(error)
                }
                error => eyre::Report::from(error),
            })
    })
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
    if interrupt_admission {
        b.sync.attach_processor_domain(&snapshot).await?;
    } else {
        crate::test_support::sync_node::native_admission(|| async {
            b.sync
                .discover_processor_domain(
                    a.sync.endpoint_addr(),
                    processor,
                    reference.authority_group,
                )
                .await
        })
        .await?;
    }
    let owner = a
        .ctx
        .attach_processor_slot_store(&snapshot.transport(&a.ctx.big_repo).await?, None)
        .await?;
    let relay = Arc::clone(
        &b.ctx
            .processor_slot_stores
            .lock()
            .await
            .get(processor)
            .unwrap()
            .store,
    );
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
    tokio::try_join!(
        task_change(&mut *reader, &object),
        crate::test_support::sync_node::native_admission(async || {
            owner.evaluate(&slot, desired.clone()).await
        })
    )?;
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
        .provision(
            &reference,
            processor.into(),
            configuration,
            snapshot.pool.clone(),
            rotated,
        )
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
    // Stop the certificate-free attachment only after automatic exchange has
    // retained authenticated control. Recovery itself must not dial the owner.
    let mut progress = b.sync.task_permission_progress();
    let control = loop {
        if let Some(control) = b
            .sync
            .task_backend()
            .cached_processor_control(processor)
            .await?
        {
            break control;
        }
        progress.changed().await?;
    };
    assert!(
        control
            .admit(
                &b.ctx.big_repo,
                "@test/unattached/process",
                reference.authority_group,
                None
            )
            .await
            .is_err()
    );
    let mut forged = serde_json::to_value(&control)?;
    let component = &mut forged["signature"]["r"][0];
    *component = serde_json::json!(component.as_u64().unwrap() ^ 1);
    let forged: crate::rt::triage::domain::ProcessorRotationControl =
        serde_json::from_value(forged)?;
    assert!(
        b.sync
            .task_backend()
            .accept_processor_control(processor, &a.ctx.big_repo.local_peer_id(), &forged)
            .await
            .is_err()
    );
    drop(reader);
    drop(relay);
    b.stop().await?;
    let b = NativeNode::open(&b_path).await?;
    crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .resume_processor_domain(processor, reference.authority_group)
            .await
    })
    .await?;
    let recovered = Arc::clone(
        &b.ctx
            .processor_slot_stores
            .lock()
            .await
            .get(processor)
            .unwrap()
            .store,
    );
    assert_eq!(
        serde_json::to_value(recovered.register().current(&slot.id()).await?.unwrap())?,
        serde_json::to_value(&retained)?
    );
    assert!(matches!(
        recovered
            .register()
            .open_original(representation)
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    let forwarded = crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .task_backend()
            .current_processor_control(processor, &a.ctx.big_repo.local_peer_id())
            .await
            .map_err(Into::into)
    })
    .await?;
    assert_eq!(
        serde_json::to_value(&forwarded)?,
        serde_json::to_value(&control)?
    );

    // Create a genuinely fresh authority document and JWK while the Relay is
    // offline. Its configuration remains private, outside current control.
    let successor = a
        .ctx
        .big_repo
        .create_doc_with_parents(seed, vec![group.clone().into()])
        .await?;
    successor
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
    let successor_reference = ProcessorDomainReference {
        document: successor.document_id(),
        authority_group: reference.authority_group,
    };
    let successor_key = PinnedJwk {
        key_ref: daybook_types::url::build_facet_ref(
            &successor_reference.document.to_string(),
            &facet,
        )?,
        heads: successor
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await,
    };
    crate::test_support::sync_node::native_admission(|| async {
        domains
            .provision_successor(
                &rotated.recovery_metadata(),
                &successor_reference,
                PoolReference {
                    rotation_generation: 1,
                    document: successor_reference.document.clone(),
                    authority_group: reference.authority_group,
                },
                successor_key.clone(),
            )
            .await
            .map_err(|error| match error {
                crate::rt::triage::domain::ProcessorDomainProvisionError::Authority(error) => {
                    eyre::Report::from(error)
                }
                error => eyre::Report::from(error),
            })
    })
    .await?;
    let ProcessorDomainLoad::Ready(successor_snapshot) =
        domains.load(&successor_reference, processor).await?
    else {
        eyre::bail!("successor processor unavailable");
    };
    crate::test_support::sync_node::native_admission(|| async {
        a.sync.attach_processor_domain(&successor_snapshot).await
    })
    .await?;
    let mut progress = b.sync.task_permission_progress();
    let b = if interrupt_admission {
        let (committed, release) = b
            .sync
            .task_backend()
            .pause_permission_commit(Some(successor_snapshot.register_binding.part.clone()));
        b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
        committed.await?;
        let control = b
            .sync
            .task_backend()
            .cached_processor_control(processor)
            .await?
            .unwrap();
        let selected = control
            .admit(&b.ctx.big_repo, processor, reference.authority_group, None)
            .await?;
        assert_eq!(selected.reference, successor_reference);
        let rpc = a.sync.native_rpc_client(b.sync.endpoint_addr());
        let denied = rpc
            .replay_page(replay_request(
                TASK_SCOPE_KEY,
                &successor_snapshot.register_binding.part,
                707,
            ))
            .await??;
        assert_eq!(
            denied.verdict(&SubscriptionTarget::Part {
                part_id: successor_snapshot.register_binding.part.clone(),
                cursor: 0
            }),
            Some(&TargetVerdict::Unauthorized)
        );
        drop(rpc);
        drop(recovered);
        b.stop().await?;
        assert!(
            release.send(()).is_err(),
            "shutdown must cancel blocked admission"
        );
        let b = NativeNode::open(&b_path).await?;
        crate::test_support::sync_node::native_admission(|| async {
            b.sync
                .resume_processor_domain(processor, reference.authority_group)
                .await
        })
        .await?;
        b
    } else {
        b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
        b
    };
    let relay = loop {
        let selected = Arc::clone(
            &b.ctx
                .processor_slot_stores
                .lock()
                .await
                .get(processor)
                .unwrap()
                .store,
        );
        if selected.register().generation() == 1 {
            break selected;
        }
        progress.changed().await?;
    };
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
    assert!(
        owner
            .register()
            .publish_local(b"retired-owner", vec![], b"must be fenced".to_vec())
            .await
            .is_err()
    );
    let selected_control = b
        .sync
        .task_backend()
        .cached_processor_control(processor)
        .await?
        .unwrap();
    let admitted = selected_control
        .admit(&b.ctx.big_repo, processor, reference.authority_group, None)
        .await?;
    assert_eq!(admitted.reference, successor_reference);

    if interrupt_admission {
        b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    }
    a.ctx
        .big_repo
        .add_member_to_group(agent.clone(), &group, Access::Read)
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
    let local_domains =
        ProcessorDomainRepo::new(Arc::clone(&b.ctx.big_repo), b.ctx.local_actor_id.clone());
    let candidate = local_domains
        .selected_candidate(&b.ctx.doc_config.document_id(), processor)
        .await?
        .unwrap();
    assert_eq!(candidate.generation, 1);
    assert_eq!(candidate.reference, successor_reference);

    // Make the next generation's original certificate author a leaf editor.
    // Native revocation of a group creator has different delegation semantics.
    a.ctx
        .big_repo
        .add_member_to_group(agent.clone(), &group, Access::Edit)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    crate::test_support::sync_node::native_admission(|| async {
        domains
            .provision_successor(
                &successor_snapshot.recovery_metadata(),
                &successor_reference,
                PoolReference {
                    rotation_generation: 2,
                    document: successor_reference.document.clone(),
                    authority_group: reference.authority_group,
                },
                successor_key.clone(),
            )
            .await
            .map_err(|error| match error {
                crate::rt::triage::domain::ProcessorDomainProvisionError::Authority(error) => {
                    eyre::Report::from(error)
                }
                error => eyre::Report::from(error),
            })
    })
    .await?;
    b.ctx
        .big_repo
        .sync_doc_with_peer(
            successor_reference.document.clone(),
            a.ctx.big_repo.local_peer_id(),
        )
        .await?;
    let ProcessorDomainLoad::Ready(next) =
        local_domains.load(&successor_reference, processor).await?
    else {
        eyre::bail!("next processor generation unavailable");
    };
    crate::test_support::sync_node::native_admission(|| async {
        b.sync.attach_processor_domain(&next).await
    })
    .await?;
    let selected_control = crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .task_backend()
            .current_processor_control(processor, &a.ctx.big_repo.local_peer_id())
            .await
            .map_err(Into::into)
    })
    .await?;
    let current = selected_control
        .admit(&b.ctx.big_repo, processor, reference.authority_group, None)
        .await?;
    crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .task_backend()
            .attach_processor(Arc::clone(&current), Some(&selected_control))
            .await
    })
    .await?;
    crate::test_support::sync_node::native_admission(|| async {
        a.sync
            .task_backend()
            .accept_processor_control(
                processor,
                &b.ctx.big_repo.local_peer_id(),
                &selected_control,
            )
            .await
            .map_err(Into::into)
    })
    .await?;
    let a_selected = Arc::clone(
        &a.ctx
            .processor_slot_stores
            .lock()
            .await
            .get(processor)
            .unwrap()
            .store,
    );
    assert_eq!(a_selected.register().generation(), 2);
    assert_eq!(
        serde_json::to_value(a_selected.register().current(&slot.id()).await?.unwrap())?,
        serde_json::to_value(&retained)?
    );
    a.ctx
        .big_repo
        .revoke_member_from_group_for_test(agent.clone(), &group)
        .await?;
    a.ctx
        .big_repo
        .add_member_to_group(agent, &group, Access::Read)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    assert!(matches!(
        selected_control
            .admit(&b.ctx.big_repo, processor, reference.authority_group, None)
            .await
            .err()
            .unwrap()
            .downcast::<CoordinationError>()
            .unwrap(),
        CoordinationError::Unauthorized
    ));
    let replacement = crate::test_support::sync_node::native_admission(|| async {
        a.sync
            .task_backend()
            .current_processor_control(processor, &b.ctx.big_repo.local_peer_id())
            .await
            .map_err(Into::into)
    })
    .await?;
    crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .task_backend()
            .accept_processor_control(processor, &a.ctx.big_repo.local_peer_id(), &replacement)
            .await
            .map_err(Into::into)
    })
    .await?;
    let forwarded = crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .task_backend()
            .current_processor_control(processor, &a.ctx.big_repo.local_peer_id())
            .await
            .map_err(Into::into)
    })
    .await?;
    assert_eq!(
        serde_json::to_value(forwarded)?,
        serde_json::to_value(replacement)?
    );
    assert_eq!(
        relay.slot(&slot).await?.desired().collect::<Vec<_>>(),
        vec![&desired]
    );
    if !interrupt_admission {
        let c_path = temp.path().join("fresh-processor-relay");
        tokio::spawn({
            let ticket = a.sync.get_clone_ticket_url().await?;
            let path = c_path.clone();
            async move {
                crate::sync::clone_repo_init_from_url(&ticket, &path, Default::default()).await
            }
        })
        .await
        .unwrap()?;
        let c = NativeNode::open(&c_path).await?;
        let contact = a
            .ctx
            .big_repo
            .receive_keyhive_contact_card(&c.ctx.big_repo.local_keyhive_contact_card())
            .await?;
        a.ctx
            .big_repo
            .add_member_to_group(contact, &group, Access::Relay)
            .await?;
        c.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
        c.ctx
            .big_repo
            .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
            .await?;
        // No processor template or source proof is provisioned to C. B already
        // retains the original authority's certificate and exact slot ciphertext.
        a.stop().await?;
        c.sync.connect_endpoint_addr(b.sync.endpoint_addr()).await?;
        c.ctx
            .big_repo
            .sync_keyhive_with_peer(b.ctx.big_repo.local_peer_id())
            .await?;
        crate::test_support::sync_node::native_admission(|| async {
            c.sync
                .discover_processor_domain(
                    b.sync.endpoint_addr(),
                    processor,
                    reference.authority_group,
                )
                .await
        })
        .await?;
        let fresh = Arc::clone(
            &c.ctx
                .processor_slot_stores
                .lock()
                .await
                .get(processor)
                .unwrap()
                .store,
        );
        let mut observed = c
            .sync
            .task_backend()
            .shared_store()
            .open_revision_reader(SubPartsRequest {
                lower_bound: 0,
                targets: [SubscriptionTarget::Part {
                    part_id: fresh.register().part().clone(),
                    cursor: 0,
                }]
                .into(),
            })
            .await?
            .map_err(|error| ferr!("fresh processor Relay observer: {error:?}"))?;
        let object = ObjKey::new(fresh.register().key(&slot.id())?.encode());
        task_change(&mut *observed, &object).await?;
        let received = fresh.register().current(&slot.id()).await?.unwrap();
        assert_eq!(
            serde_json::to_value(&received)?,
            serde_json::to_value(&retained)?
        );
        let LaneState::Current { representation } = received.lanes.values().next().unwrap() else {
            unreachable!("original slot lane")
        };
        assert!(matches!(
            fresh
                .register()
                .open_original(representation)
                .await
                .unwrap_err()
                .downcast_ref::<CoordinationError>(),
            Some(CoordinationError::Unauthorized)
        ));
        assert_eq!(
            c.sync
                .task_backend()
                .shared_store()
                .obj_parts(object)
                .await?,
            vec![fresh.register().part().clone()]
        );
        drop(observed);
        drop(fresh);
        c.stop().await?;
        let c = NativeNode::open(&c_path).await?;
        crate::test_support::sync_node::native_admission(|| async {
            c.sync
                .resume_processor_domain(processor, reference.authority_group)
                .await
        })
        .await?;
        let fresh = Arc::clone(
            &c.ctx
                .processor_slot_stores
                .lock()
                .await
                .get(processor)
                .unwrap()
                .store,
        );
        assert_eq!(
            serde_json::to_value(fresh.register().current(&slot.id()).await?.unwrap())?,
            serde_json::to_value(&retained)?
        );
        drop(fresh);
        c.stop().await?;
        b.stop().await
    } else {
        b.stop().await?;
        a.stop().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_task_transport_commits_before_revision_delivery_and_revokes_serving() -> Res<()> {
    // Box the large scenario storage; NativeNode::open separately boots children
    // so native replay does not inherit this scenario's asynchronous poll stack.
    Box::pin(native_task_transport_scenario()).await
}

async fn native_task_transport_scenario() -> Res<()> {
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
        predecessor_parts: Default::default(),
        historical_key_authorities: Default::default(),
        pool_id: crate::tasks::test_util::pool(),
        rotation_generation: 0,
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
    // This template is only for repeated-open ownership checks below; initial
    // Relay attachment obtains authenticated current control over native RPC.
    let b_snapshot = a_snapshot.clone();
    assert!(b_snapshot.register_binding(&b.ctx.big_repo).await.is_err());
    // Attach after the real native connection exists: both peer directions must
    // receive dynamic task routes, not only routes captured at next connection.
    let a_tasks = a.sync.attach_task_pool(a_snapshot.clone()).await?;
    let control = crate::test_support::sync_node::native_admission(|| async {
        a.sync
            .task_backend()
            .current_control(&descriptor.pool_id, &b.ctx.big_repo.local_peer_id())
            .await
            .map_err(Into::into)
    })
    .await?;
    let admitted = crate::test_support::sync_node::native_admission(|| async {
        control
            .admit(
                &b.ctx.big_repo,
                &descriptor.pool_id,
                descriptor.authority_group,
            )
            .await
    })
    .await?;
    assert_eq!(admitted.reference, a_snapshot.reference);
    assert!(
        control
            .admit(&b.ctx.big_repo, &descriptor.pool_id, [0; 32])
            .await
            .is_err()
    );
    let mut forged = serde_json::to_value(&control)?;
    let transcript = forged["statement"].as_array_mut().unwrap();
    let last = transcript.last_mut().unwrap();
    *last = serde_json::json!(last.as_u64().unwrap() ^ 1);
    let forged: crate::tasks::pool::PoolRotationControl = serde_json::from_value(forged)?;
    assert!(
        forged
            .admit(
                &b.ctx.big_repo,
                &descriptor.pool_id,
                descriptor.authority_group
            )
            .await
            .is_err()
    );
    let mut oversized = serde_json::to_value(&control)?;
    oversized["statement"] = serde_json::json!(vec![0_u8; 16 * 1024 + 1]);
    assert!(serde_json::from_value::<crate::tasks::pool::PoolRotationControl>(oversized).is_err());
    let b_backend = b.sync.task_backend();
    assert!(matches!(
        b_backend
            .accept_current_control(&descriptor.pool_id, &a_peer, &control)
            .await,
        Err(CoordinationError::Invalid("pool is not attached"))
    ));
    assert!(b_backend.get(&descriptor.pool_id).is_none());
    let (committed, release) = b_backend.pause_permission_commit(None);
    let retained = {
        let attach = crate::test_support::sync_node::native_admission(|| async {
            b.sync
                .discover_task_pool(
                    a.sync.endpoint_addr(),
                    descriptor.pool_id.clone(),
                    descriptor.authority_group,
                )
                .await
        });
        tokio::pin!(attach);
        tokio::select! {
            result = &mut attach => { result?; panic!("attachment escaped its commit gate"); }
            result = committed => result?,
        }
        let retained = b_backend.get(&descriptor.pool_id).unwrap();
        let retained_control = crate::test_support::sync_node::native_admission(|| async {
            b_backend
                .current_control(&descriptor.pool_id, &a_peer)
                .await
                .map_err(Into::into)
        })
        .await?;
        assert_eq!(
            serde_json::to_value(&retained_control)?,
            serde_json::to_value(&control)?
        );
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
    let via_relay = crate::test_support::sync_node::native_admission(|| async {
        a.sync
            .discover_task_pool(
                b.sync.endpoint_addr(),
                descriptor.pool_id.clone(),
                descriptor.authority_group,
            )
            .await
    })
    .await?;
    assert!(Arc::ptr_eq(&via_relay, &a_tasks));
    drop(via_relay);
    // Close the native publisher connection and reopen only the Relay. Cached
    // signed control must survive without a dial, metadata Read, or Edit grant.
    drop(b_tasks);
    drop(b_backend);
    drop(b_pools);
    b.stop().await?;
    tracing::warn!(root = %b_path.display(), "control acceptance: Relay reopen beginning");
    let b = NativeNode::open(&b_path).await?;
    tracing::warn!(
        "control acceptance: Relay bootstrap complete; cached-control recovery beginning"
    );
    let b_tasks = crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .resume_task_pool(descriptor.pool_id.clone(), descriptor.authority_group)
            .await
    })
    .await?;
    tracing::warn!("control acceptance: cached-control recovery complete");
    let b_backend = b.sync.task_backend();
    let forwarded_after_reopen = crate::test_support::sync_node::native_admission(|| async {
        b_backend
            .current_control(&descriptor.pool_id, &a_peer)
            .await
            .map_err(Into::into)
    })
    .await?;
    assert_eq!(
        serde_json::to_value(&forwarded_after_reopen)?,
        serde_json::to_value(&control)?
    );
    assert_eq!(
        b.ctx
            .big_repo
            .coordination_authority(reference.document.clone(), descriptor.authority_group,)
            .await?
            .local_access(),
        Some(Access::Relay)
    );
    let b_pools = PoolRepo::new(
        Arc::clone(&b.ctx.big_repo),
        a.ctx.doc_config.document_id(),
        automerge::ActorId::from(b"consumer".as_slice()),
    );
    tracing::warn!("control acceptance: Relay reconnect beginning");
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    tracing::warn!("control acceptance: Relay reconnect complete");
    {
        let mut permission_updates = b_backend.permission_updates.subscribe();
        b_backend.force_pending_once();
        let (denied, release) = b_backend.pause_permission_commit(None);
        let repair = b.sync.attach_task_pool(b_snapshot.clone());
        tokio::pin!(repair);
        tokio::select! {
            result = &mut repair => { result?; panic!("Pending attachment escaped its commit gate"); }
            result = denied => result?,
        }
        tracing::warn!("control acceptance: Pending permission commit paused");
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
        tracing::warn!("control acceptance: explicit permission repair complete");
        // No new native mutation/delta is generated: the retained Pending
        // obligation itself must recover fresh serving permissions.
        while permission_updates.recv().await? {}
        tracing::warn!("control acceptance: Pending permission refresh complete");
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
    tracing::warn!("control acceptance: native task readers opening boundary");
    live_boundary(&mut *a_reader).await?;
    live_boundary(&mut *b_reader).await?;
    tracing::warn!("control acceptance: native task readers at boundary");
    let mut declaration = crate::tasks::test_util::declaration("native-task-exact-input");
    let writer = NodePubkey::new(
        crate::test_support::sync_node::native_admission(|| async {
            a_tasks.register().local_writer().await
        })
        .await?,
    );
    declaration.producer = Some(writer);
    declaration.placement = Preference::AnyNode;
    let task = declaration.task_id;
    let object = ObjKey::new(a_tasks.register().key(&task.to_bytes32())?.encode());
    let (b_declaration_revision, _) = tokio::try_join!(
        task_change(&mut *b_reader, &object),
        crate::test_support::sync_node::native_admission(async || {
            a_tasks.submit(declaration.clone(), Vec::new()).await
        })
    )?;
    let a_declaration_revision = task_change(&mut *a_reader, &object).await?;
    tracing::warn!("control acceptance: exact task declaration replicated");
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
    let (b_terminal_revision, _) = tokio::try_join!(
        task_change(&mut *b_reader, &object),
        crate::test_support::sync_node::native_admission(async || {
            a_tasks.record_terminal(task, success.clone()).await
        })
    )?;
    let terminal = a_tasks.ticket(task).await?.unwrap();
    assert_eq!(b_tasks.ticket(task).await?, Some(terminal.clone()));
    let signed_terminal = terminal.terminal.get(&writer).unwrap();
    assert_eq!(signed_terminal.writer, writer);
    assert_eq!(signed_terminal.fact, success);
    let LaneState::Current {
        representation: declaration_representation,
    } = stale.lanes.values().next().unwrap()
    else {
        unreachable!("the captured declaration is a current lane");
    };
    // Failed admission may consume a durable reservation. The terminal must
    // advance its original declaration, not assume contiguous writer sequences.
    assert!(signed_terminal.writer_seq > declaration_representation.original.writer_seq);
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
    reverse.producer = Some(NodePubkey::new(
        crate::test_support::sync_node::native_admission(|| async {
            b_tasks.register().local_writer().await
        })
        .await?,
    ));
    reverse.placement = Preference::AnyNode;
    let reverse_task = reverse.task_id;
    let reverse_object = ObjKey::new(b_tasks.register().key(&reverse_task.to_bytes32())?.encode());
    tokio::try_join!(
        task_change(&mut *a_reader, &reverse_object),
        crate::test_support::sync_node::native_admission(async || {
            b_tasks.submit(reverse.clone(), Vec::new()).await
        })
    )?;
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
    tokio::try_join!(
        task_change(&mut *b_reader, &shared_object),
        crate::test_support::sync_node::native_admission(async || {
            a_tasks
                .submit(shared.clone(), b"manifest-head-a".to_vec())
                .await
        })
    )?;
    let b_writer = NodePubkey::new(
        crate::test_support::sync_node::native_admission(|| async {
            b_tasks.register().local_writer().await
        })
        .await?,
    );
    crate::test_support::sync_node::native_admission(|| async {
        b_tasks
            .submit(shared.clone(), b"manifest-head-b".to_vec())
            .await
    })
    .await?;
    // A same-object completion may describe the earlier A-only capture. Wait
    // on durable revisions until B’s exact witness is in the projected ticket.
    let ticket = loop {
        let ticket = a_tasks.ticket(shared_id).await?.unwrap();
        if ticket.declaration.publishers.iter().any(|evidence| {
            evidence.publisher == b_writer && evidence.input_witness == b"manifest-head-b"
        }) {
            break ticket;
        }
        task_change(&mut *a_reader, &shared_object).await?;
    };
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
    let control_denied = crate::test_support::sync_node::native_admission(|| async {
        b.sync
            .discover_task_pool(
                a.sync.endpoint_addr(),
                descriptor.pool_id.clone(),
                descriptor.authority_group,
            )
            .await
    })
    .await;
    assert!(matches!(
        control_denied
            .err()
            .expect("revoked sponsorship must reject control")
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
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

#[tokio::test(flavor = "multi_thread")]
async fn native_rotated_pool_recovers_late_offline_predecessor_ciphertext() -> Res<()> {
    Box::pin(late_offline_predecessor_scenario()).await
}

async fn late_offline_predecessor_scenario() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let a_path = temp.path().join("a");
    let b_path = temp.path().join("b");
    let c_path = temp.path().join("relay");
    let d_path = temp.path().join("fresh-relay");
    // Keep recursive native initialization below a small task frame, not the
    // multi-replica recovery scenario's retained captures and certificates.
    tokio::spawn({
        let path = a_path.clone();
        async move {
            RepoCtx::init(
                &path,
                RepoOpenOptions::default(),
                "late-source".into(),
                "a".into(),
            )
            .await?
            .shutdown()
            .await
        }
    })
    .await
    .unwrap()?;
    let a = NativeNode::open(&a_path).await?;
    crate::sync::clone_repo_init_from_url(
        &a.sync.get_clone_ticket_url().await?,
        &b_path,
        Default::default(),
    )
    .await?;
    crate::sync::clone_repo_init_from_url(
        &a.sync.get_clone_ticket_url().await?,
        &c_path,
        Default::default(),
    )
    .await?;
    crate::sync::clone_repo_init_from_url(
        &a.sync.get_clone_ticket_url().await?,
        &d_path,
        Default::default(),
    )
    .await?;
    let d = NativeNode::open(&d_path).await?;
    let d_agent = a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&d.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    let c = NativeNode::open(&c_path).await?;
    let c_agent = a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&c.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    let b = NativeNode::open(&b_path).await?;
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    let a_peer = a.ctx.big_repo.local_peer_id();
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
    a.ctx
        .big_repo
        .add_member_to_group(b_agent.clone(), &group, Access::Edit)
        .await?;
    a.ctx
        .big_repo
        .add_member_to_group(c_agent, &group, Access::Relay)
        .await?;
    a.ctx
        .big_repo
        .add_member_to_group(d_agent.clone(), &group, Access::Relay)
        .await?;
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let mut document = automerge::Automerge::new();
    let mut transaction = document.transaction();
    let facets = transaction.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
    autosurgeon::reconcile_prop(
        &mut transaction,
        &facets,
        autosurgeon::Prop::Key(facet.to_string().into()),
        am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
            &MasterKey::random(),
        ))?),
    )?;
    transaction.commit();
    let metadata = a
        .ctx
        .big_repo
        .create_doc_with_parents(document, vec![group.clone().into()])
        .await?;
    let heads = metadata
        .with_document_read(|document| ChangeHashSet(document.get_heads().into()))
        .await;
    let key_ref = daybook_types::url::build_facet_ref(&metadata.document_id().to_string(), &facet)?;
    let descriptor = TaskPoolDescriptor {
        predecessor_parts: Default::default(),
        historical_key_authorities: Default::default(),
        pool_id: crate::tasks::test_util::pool(),
        rotation_generation: 0,
        authority_group: group.id().to_bytes(),
        active_task_part: PartKey::new(b"offline-predecessor/initial"),
        register_scope: b"offline-predecessor/register".to_vec(),
        register_incarnation: [81; 32],
        allowed_key_refs: [key_ref.clone()].into(),
        publication_key: PinnedJwk { key_ref, heads },
        archive_part: None,
        router_slot: ObjKey::new(b"offline-predecessor/router"),
        router_heartbeat_topic: big_repo::BigEphemeralTopic::new([82; 32]),
        allowed_rpc_transports: [RpcTransport::IrpcIroh].into(),
        retention_class: RetentionClass::UntilAuthoritativeRemoval,
        routing_defaults: RoutingDefaults::SharedElection,
    };
    let pools = PoolRepo::new(
        Arc::clone(&a.ctx.big_repo),
        a.ctx.doc_config.document_id(),
        automerge::ActorId::from(b"late-source-provisioner".as_slice()),
    );
    let reference = pools
        .register(metadata.document_id(), descriptor.clone())
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    b.ctx
        .big_repo
        .sync_keyhive_with_peer(a_peer.clone())
        .await?;
    b.ctx
        .big_repo
        .sync_doc_with_peer(reference.document.clone(), a_peer)
        .await?;
    b.ctx.big_repo.wait_for_quiescence(None).await?;
    c.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    c.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    // Retain native sponsorship, but never attach a pool or acquire its
    // certificate before going offline. Reopen starts without a publisher.
    c.stop().await?;
    d.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    d.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    d.stop().await?;
    let PoolDiscovery::Ready(original) = pools
        .load_descriptor(&reference, &descriptor.pool_id, descriptor.authority_group)
        .await?
    else {
        eyre::bail!("original pool unavailable");
    };
    let a_tasks = a.sync.attach_task_pool(original.clone()).await?;
    let b_tasks = b.sync.attach_task_pool(original.clone()).await?;
    drop(metadata);
    drop(pools);
    drop(a_tasks);
    a.stop().await?;

    // B is the sole durable holder of this task while A advances independently.
    let mut declaration = crate::tasks::test_util::declaration("late-offline-predecessor");
    declaration.producer = Some(NodePubkey::new(b_tasks.register().local_writer().await?));
    declaration.placement = Preference::AnyNode;
    b_tasks
        .submit(declaration.clone(), b"exact offline capture".to_vec())
        .await?;
    let expected = serde_json::to_value(
        b_tasks
            .register()
            .current(&declaration.task_id.to_bytes32())
            .await?
            .unwrap(),
    )?;
    drop(b_tasks);
    b.stop().await?;

    let a = NativeNode::open(&a_path).await?;
    let pools = PoolRepo::new(
        Arc::clone(&a.ctx.big_repo),
        a.ctx.doc_config.document_id(),
        automerge::ActorId::from(b"late-source-successor".as_slice()),
    );
    let intermediate = pools.propose_successor(&original).await?;
    drop(a.sync.attach_task_pool(intermediate.clone()).await?);
    let successor = pools.propose_successor(&intermediate).await?;
    let selected = a.sync.attach_task_pool(successor.clone()).await?;
    assert!(selected.ticket(declaration.task_id).await?.is_none());
    let object = ObjKey::new(
        selected
            .register()
            .key(&declaration.task_id.to_bytes32())?
            .encode(),
    );
    let mut reader = selected
        .register()
        .open_consumer_reader(b"late-source-recovery")
        .await?;
    live_boundary(&mut *reader).await?;

    let c = NativeNode::open(&c_path).await?;
    let relay_old = c.sync.attach_task_pool(original.clone()).await?;
    assert!(
        c.sync
            .task_backend()
            .cached_current_control(&descriptor.pool_id)
            .await?
            .is_none()
    );
    assert!(matches!(
        relay_old
            .register()
            .local_writer()
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    let mut relay_progress = c.sync.task_permission_progress();
    c.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    let relay = loop {
        let store = c.sync.task_backend().get(&descriptor.pool_id).unwrap();
        if store.register().generation() == successor.descriptor.rotation_generation {
            break store;
        }
        relay_progress.changed().await?;
    };
    assert!(
        relay
            .register()
            .current(&declaration.task_id.to_bytes32())
            .await?
            .is_none()
    );
    let mut relay_reader = c
        .sync
        .task_backend()
        .shared_store()
        .open_revision_reader(SubPartsRequest {
            lower_bound: 0,
            targets: [SubscriptionTarget::Part {
                part_id: successor.descriptor.active_task_part.clone(),
                cursor: 0,
            }]
            .into(),
        })
        .await?
        .map_err(|error| ferr!("passive Relay observer: {error:?}"))?;
    live_boundary(&mut *relay_reader).await?;
    let b = NativeNode::open(&b_path).await?;
    let old = b.sync.attach_task_pool(original).await?;
    let mut adoption_progress = b.sync.task_permission_progress();
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    // An old publisher must be allowed to subscribe without first adopting the
    // successor; its source transport remains authenticated by the selected pool.
    let rpc = b.sync.native_rpc_client(a.sync.endpoint_addr());
    let page = rpc
        .replay_page(replay_request(
            TASK_SCOPE_KEY,
            &descriptor.active_task_part,
            901,
        ))
        .await??;
    assert!(matches!(
        page.verdict(&SubscriptionTarget::Part {
            part_id: descriptor.active_task_part.clone(),
            cursor: 0,
        }),
        Some(TargetVerdict::Events { .. })
    ));
    task_change(&mut *reader, &object).await?;
    assert_eq!(
        serde_json::to_value(
            selected
                .register()
                .current(&declaration.task_id.to_bytes32())
                .await?
                .unwrap()
        )?,
        expected
    );
    task_change(&mut *relay_reader, &object).await?;
    let memberships = a
        .sync
        .task_backend()
        .shared_store()
        .obj_parts(object)
        .await?;
    assert!(memberships.contains(&successor.descriptor.active_task_part));
    assert!(!memberships.contains(&descriptor.active_task_part));
    assert_eq!(
        selected.register().generation(),
        successor.descriptor.rotation_generation
    );
    // Connection and native authority progress exchange signed current control
    // without an explicit discovery call or an executor session. B retains its
    // own authenticated source provenance rather than receiving A's history.
    let adopted = loop {
        let selected = b.sync.task_backend().get(&descriptor.pool_id).unwrap();
        if selected.register().generation() == successor.descriptor.rotation_generation {
            break selected;
        }
        adoption_progress.changed().await?;
    };
    assert_eq!(
        adopted.register().generation(),
        successor.descriptor.rotation_generation
    );
    assert_eq!(
        serde_json::to_value(
            adopted
                .register()
                .current(&declaration.task_id.to_bytes32())
                .await?
                .unwrap()
        )?,
        expected
    );
    assert!(
        old.register()
            .publish_local(b"fenced-old-producer", vec![], b"must not publish".to_vec())
            .await
            .is_err()
    );
    let relay_ciphertext = relay
        .register()
        .current(&declaration.task_id.to_bytes32())
        .await?
        .unwrap();
    assert_eq!(serde_json::to_value(&relay_ciphertext)?, expected);
    let LaneState::Current { representation } = relay_ciphertext.lanes.values().next().unwrap()
    else {
        unreachable!("original producer lane remains current");
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
    assert!(
        c.sync
            .task_backend()
            .cached_current_control(&descriptor.pool_id)
            .await?
            .is_some()
    );
    drop(relay_reader);
    drop(relay);
    drop(relay_old);
    c.stop().await?;
    drop(adopted);
    drop(rpc);
    drop(reader);
    drop(selected);
    drop(pools);
    a.stop().await?;
    // Both editors are disconnected from this pair. A fresh Relay knows only
    // sponsorship; current discovery cannot smuggle old keys or transport parts.
    let c = NativeNode::open(&c_path).await?;
    crate::test_support::sync_node::native_admission(|| async {
        c.sync
            .resume_task_pool(descriptor.pool_id.clone(), descriptor.authority_group)
            .await
    })
    .await?;
    let d = NativeNode::open(&d_path).await?;
    d.sync.connect_endpoint_addr(c.sync.endpoint_addr()).await?;
    d.ctx
        .big_repo
        .sync_keyhive_with_peer(c.ctx.big_repo.local_peer_id())
        .await?;
    let fresh = crate::test_support::sync_node::native_admission(|| async {
        d.sync
            .discover_task_pool(
                c.sync.endpoint_addr(),
                descriptor.pool_id.clone(),
                descriptor.authority_group,
            )
            .await
    })
    .await?;
    let mut fresh_reader = d
        .sync
        .task_backend()
        .shared_store()
        .open_revision_reader(SubPartsRequest {
            lower_bound: 0,
            targets: [SubscriptionTarget::Part {
                part_id: successor.descriptor.active_task_part.clone(),
                cursor: 0,
            }]
            .into(),
        })
        .await?
        .map_err(|error| ferr!("fresh Relay observer: {error:?}"))?;
    let fresh_object = ObjKey::new(
        fresh
            .register()
            .key(&declaration.task_id.to_bytes32())?
            .encode(),
    );
    task_change(&mut *fresh_reader, &fresh_object).await?;
    let retained = fresh
        .register()
        .current(&declaration.task_id.to_bytes32())
        .await?
        .unwrap();
    assert_eq!(serde_json::to_value(&retained)?, expected);
    let LaneState::Current { representation } = retained.lanes.values().next().unwrap() else {
        unreachable!("original lane")
    };
    assert!(matches!(
        fresh
            .register()
            .open_original(representation)
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    assert_eq!(
        d.sync
            .task_backend()
            .shared_store()
            .obj_parts(fresh_object)
            .await?,
        vec![successor.descriptor.active_task_part.clone()]
    );
    let forwarded = crate::test_support::sync_node::native_admission(|| async {
        fresh
            .register()
            .source_control(
                &c.ctx.big_repo.local_peer_id(),
                fresh.register().authority_document(),
                &descriptor.publication_key.key_ref,
            )
            .await
    })
    .await?;
    let relay_control = crate::test_support::sync_node::native_admission(|| async {
        c.sync
            .task_backend()
            .get(&descriptor.pool_id)
            .unwrap()
            .register()
            .source_control(
                &d.ctx.big_repo.local_peer_id(),
                &successor.reference.document,
                &descriptor.publication_key.key_ref,
            )
            .await
    })
    .await?;
    assert_eq!(
        serde_json::to_value(&forwarded)?,
        serde_json::to_value(&relay_control)?
    );
    let (_, source) = forwarded.decode(descriptor.authority_group)?;
    for corrupt_scope in [true, false] {
        let mut mismatched = source.clone();
        if corrupt_scope {
            mismatched.scope.push(0);
        } else {
            mismatched.incarnation[0] ^= 1;
        }
        let bad = crate::test_support::sync_node::native_admission(|| async {
            let authority = b
                .ctx
                .big_repo
                .coordination_authority(
                    successor.reference.document.clone(),
                    descriptor.authority_group,
                )
                .await?;
            let authority = b
                .ctx
                .big_repo
                .admit_coordination_access(&authority, Access::Edit)
                .await?;
            crate::tasks::RegisterSourceControl::sign(
                &b.ctx.big_repo,
                &authority,
                &d.ctx.big_repo.local_peer_id(),
                &mismatched,
            )
            .await
        })
        .await?;
        assert!(matches!(
            fresh
                .register()
                .import_source_control(
                    &bad,
                    &descriptor.publication_key.key_ref,
                    &b.ctx.big_repo.local_peer_id()
                )
                .await
                .unwrap_err()
                .downcast_ref::<CoordinationError>(),
            Some(CoordinationError::Invalid(_))
        ));
    }
    assert!(matches!(
        fresh
            .register()
            .import_source_control(
                &forwarded,
                &successor.descriptor.publication_key.key_ref,
                &c.ctx.big_repo.local_peer_id()
            )
            .await
            .unwrap_err()
            .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Invalid(_))
    ));
    let still_forwarded = crate::test_support::sync_node::native_admission(|| async {
        fresh
            .register()
            .source_control(
                &c.ctx.big_repo.local_peer_id(),
                fresh.register().authority_document(),
                &descriptor.publication_key.key_ref,
            )
            .await
    })
    .await?;
    assert_eq!(
        serde_json::to_value(&still_forwarded)?,
        serde_json::to_value(&forwarded)?
    );
    drop(fresh_reader);
    drop(fresh);
    d.stop().await?;
    c.stop().await?;
    let d = NativeNode::open(&d_path).await?;
    let recovered = crate::test_support::sync_node::native_admission(|| async {
        d.sync
            .resume_task_pool(descriptor.pool_id.clone(), descriptor.authority_group)
            .await
    })
    .await?;
    assert_eq!(
        serde_json::to_value(
            recovered
                .register()
                .current(&declaration.task_id.to_bytes32())
                .await?
                .unwrap()
        )?,
        expected
    );
    let a = NativeNode::open(&a_path).await?;
    // Native handles belong to their repository instance, not merely its ID.
    let group = a
        .ctx
        .big_repo
        .get_group_by_id(descriptor.authority_group)
        .await
        .expect("reopened authority group");
    let d_agent = a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&d.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    b.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    let rpc = b.sync.native_rpc_client(a.sync.endpoint_addr());
    // Neither a retained source nor its successor may serve cached grants before attachment.
    for (index, part) in [
        &descriptor.active_task_part,
        &successor.descriptor.active_task_part,
    ]
    .into_iter()
    .enumerate()
    {
        let denied = rpc
            .replay_page(replay_request(TASK_SCOPE_KEY, part, 904 + index as u64))
            .await??;
        assert_eq!(
            denied.verdict(&SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: 0
            }),
            Some(&TargetVerdict::Unauthorized)
        );
        assert!(denied.events.is_empty());
    }
    let selected = a.sync.attach_task_pool(successor.clone()).await?;
    a.ctx
        .big_repo
        .add_member_to_group(d_agent, &group, Access::Read)
        .await?;
    d.sync.connect_endpoint_addr(a.sync.endpoint_addr()).await?;
    d.ctx
        .big_repo
        .sync_keyhive_with_peer(a.ctx.big_repo.local_peer_id())
        .await?;
    let original_key_doc =
        daybook_types::url::parse_facet_ref(&descriptor.publication_key.key_ref)?.doc_id;
    d.ctx
        .big_repo
        .sync_doc_with_peer(
            original_key_doc.as_str().parse()?,
            a.ctx.big_repo.local_peer_id(),
        )
        .await?;
    d.ctx
        .big_repo
        .sync_doc_with_peer(
            successor.reference.document.clone(),
            a.ctx.big_repo.local_peer_id(),
        )
        .await?;
    let historical = crate::test_support::sync_node::native_admission(|| async {
        old.register().open_original(representation).await
    })
    .await?;
    assert_eq!(
        crate::test_support::sync_node::native_admission(async || recovered
            .register()
            .open_original(representation)
            .await)
        .await?,
        historical
    );
    assert_eq!(
        crate::test_support::sync_node::native_admission(async || recovered
            .ticket(declaration.task_id)
            .await)
        .await?
        .unwrap()
        .declaration
        .declaration,
        declaration
    );
    drop(recovered);
    d.stop().await?;
    let leaf_source = crate::test_support::sync_node::native_admission(|| async {
        let authority = b
            .ctx
            .big_repo
            .coordination_authority(
                successor.reference.document.clone(),
                descriptor.authority_group,
            )
            .await?;
        let authority = b
            .ctx
            .big_repo
            .admit_coordination_access(&authority, Access::Edit)
            .await?;
        crate::tasks::RegisterSourceControl::sign(
            &b.ctx.big_repo,
            &authority,
            &a.ctx.big_repo.local_peer_id(),
            &source,
        )
        .await
    })
    .await?;
    crate::test_support::sync_node::native_admission(|| async {
        selected
            .register()
            .import_source_control(
                &leaf_source,
                &descriptor.publication_key.key_ref,
                &b.ctx.big_repo.local_peer_id(),
            )
            .await
    })
    .await?;
    let mut progress = a.sync.task_permission_progress();
    a.ctx
        .big_repo
        .revoke_member_from_group_for_test(b_agent, &group)
        .await?;
    a.ctx.big_repo.wait_for_quiescence(None).await?;
    let access = big_repo::KeyhiveAccessRevisionStore::<big_sync::SqliteDeltaWalkerStateRepo>::new(
        &a.ctx.big_repo,
    );
    let revision = access.latest_revision().await?;
    progress.wait_for(|through| *through >= revision).await?;
    assert!(matches!(
        crate::test_support::sync_node::native_admission(async || selected
            .register()
            .import_source_control(
                &leaf_source,
                &descriptor.publication_key.key_ref,
                &a.ctx.big_repo.local_peer_id()
            )
            .await)
        .await
        .unwrap_err()
        .downcast_ref::<CoordinationError>(),
        Some(CoordinationError::Unauthorized)
    ));
    let source_replacement = crate::test_support::sync_node::native_admission(|| async {
        selected
            .register()
            .source_control(
                &a.ctx.big_repo.local_peer_id(),
                &successor.reference.document,
                &descriptor.publication_key.key_ref,
            )
            .await
    })
    .await?;
    crate::test_support::sync_node::native_admission(|| async {
        selected
            .register()
            .import_source_control(
                &source_replacement,
                &descriptor.publication_key.key_ref,
                &a.ctx.big_repo.local_peer_id(),
            )
            .await
    })
    .await?;
    assert_eq!(
        crate::test_support::sync_node::native_admission(async || selected
            .register()
            .open_original(representation)
            .await)
        .await?,
        historical
    );
    for (index, part) in [
        &descriptor.active_task_part,
        &successor.descriptor.active_task_part,
    ]
    .into_iter()
    .enumerate()
    {
        let denied = rpc
            .replay_page(replay_request(TASK_SCOPE_KEY, part, 902 + index as u64))
            .await??;
        assert_eq!(
            denied.verdict(&SubscriptionTarget::Part {
                part_id: part.clone(),
                cursor: 0
            }),
            Some(&TargetVerdict::Unauthorized)
        );
        assert!(denied.events.is_empty());
    }
    drop(rpc);
    drop(old);
    drop(selected);
    b.stop().await?;
    a.stop().await
}
