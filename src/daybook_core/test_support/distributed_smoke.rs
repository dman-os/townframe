use super::sync_node::SyncTestNode as Node;
use crate::{
    blobs::encrypt::{JwkOct, MasterKey},
    plugs::OciImportOptions,
    repo::{RepoCtx, RepoOpenOptions},
    repos::{Repo, SubscribeOpts},
    rt::{
        DispatchArgs,
        dispatch::RoutineInvocation,
        task_adapter::{
            COMMAND_DOMAIN, CapturedRoutineAdapter, CapturedRoutineInput, ResolvedRoutineInput,
        },
        triage::{
            ActivationStatus,
            domain::{ProcessorConfiguration, ProcessorDomainReference, ProcessorDomainRepo},
        },
    },
    sync::{CloneRepoInitOptions, clone_repo_init_from_url},
    tasks::{driver::SchedulingTiming, storage::PinnedJwk, *},
};
use automerge::transaction::Transactable;
use big_repo::{DocumentId, keyhive_core::access::Access};
use big_sync_core::{ObjKey, PartKey, revisioned_store::RevisionReadLimits};
use daybook_types::{doc::*, manifest::*};
use futures::StreamExt;
use std::{path::Path, sync::Arc};
use utils_rs::prelude::*;
use wflow::wflow_core::partition::job_events::{JobRunResult, JobWaitResultDeets};
use wflow::wflow_core::partition::log::PartitionLogEntry;
use wflow::wflow_tokio::partition::PartitionLogRef;

impl Node {
    fn pools(&self) -> PoolRepo {
        PoolRepo::new(
            Arc::clone(&self.ctx.big_repo),
            self.ctx.doc_config.document_id(),
            self.ctx.local_actor_id.clone(),
        )
    }

    async fn enable(&self, reference: &url::Url) -> Res<()> {
        let activation = self.rt.plugs_repo.enable_plug(reference).await?;
        let status = self
            .rt
            .triage_worker
            .wait_for_activation(&self.rt.plugs_repo, activation)
            .await?;
        eyre::ensure!(
            matches!(status, ActivationStatus::Active(_)),
            "activation failed: {status:?}"
        );
        Ok(())
    }
}

async fn key_document(
    node: &Node,
    group: &big_repo::BigKeyhiveGroup,
) -> Res<(DocumentId, PinnedJwk)> {
    let mut doc = automerge::Automerge::new();
    let facet = FacetKey::from(WellKnownFacetTag::Jwk);
    let mut tx = doc.transaction();
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
    let handle = node
        .ctx
        .big_repo
        .create_doc_with_parents(doc, vec![group.clone().into()])
        .await?;
    let heads = handle
        .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
        .await;
    let id = handle.document_id();
    let key_ref = daybook_types::url::build_facet_ref(&id.to_string(), &facet)?;
    Ok((id, PinnedJwk { key_ref, heads }))
}

async fn snapshot(
    node: &Node,
    reference: &PoolReference,
    pool: &TaskPoolId,
) -> Res<PoolDescriptorSnapshot> {
    match node
        .pools()
        .load_descriptor(reference, pool, reference.authority_group)
        .await?
    {
        PoolDiscovery::Ready(snapshot) => Ok(snapshot),
        status => eyre::bail!("pool unavailable: {status:?}"),
    }
}

fn descriptor(
    pool: TaskPoolId,
    group: [u8; 32],
    key: PinnedJwk,
    label: &str,
) -> TaskPoolDescriptor {
    TaskPoolDescriptor {
        pool_id: pool,
        rotation_generation: 0,
        authority_group: group,
        active_task_part: PartKey::new(format!("{label}/tasks").into_bytes()),
        register_scope: format!("{label}/register").into_bytes(),
        register_incarnation: *blake3::hash(label.as_bytes()).as_bytes(),
        allowed_key_refs: [key.key_ref.clone()].into(),
        publication_key: key,
        predecessor_parts: Default::default(),
        historical_key_authorities: Default::default(),
        archive_part: None,
        router_slot: ObjKey::new(format!("{label}/router").into_bytes()),
        router_heartbeat_topic: big_repo::BigEphemeralTopic::new(
            *blake3::hash(format!("{label}/heartbeat").as_bytes()).as_bytes(),
        ),
        allowed_rpc_transports: [RpcTransport::IrpcIroh].into(),
        retention_class: RetentionClass::UntilAuthoritativeRemoval,
        routing_defaults: RoutingDefaults::SharedElection,
    }
}

async fn pull(
    source: &Node,
    destination: &Node,
    documents: impl IntoIterator<Item = DocumentId>,
) -> Res<()> {
    source.ctx.big_repo.wait_for_quiescence(None).await?;
    let peer = source.ctx.big_repo.local_peer_id();
    destination
        .ctx
        .big_repo
        .sync_keyhive_with_peer(peer.clone())
        .await?;
    for document in documents {
        destination
            .ctx
            .big_repo
            .sync_doc_with_peer(document, peer.clone())
            .await?;
    }
    destination.ctx.big_repo.wait_for_quiescence(None).await?;
    Ok(())
}

pub async fn run() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let a_root = temp.path().join("producer");
    let b_root = temp.path().join("executor");
    RepoCtx::init(
        &a_root,
        RepoOpenOptions::default(),
        "producer".into(),
        "smoke".into(),
    )
    .await?
    .shutdown()
    .await?;
    let producer = Node::open(&a_root, RepoOpenOptions::default(), "producer").await?;
    let artifact = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/oci/@daybook/test");
    let imported = producer
        .rt
        .plugs_repo
        .import_from_oci_layout(&artifact, OciImportOptions::default())
        .await?;
    let manifest_doc = imported.doc_id.ok_or_eyre("OCI import has no manifest")?;
    let reference: url::Url =
        format!("db+facet:///{manifest_doc}/org.example.daybook.plugManifest/main?branch=main")
            .parse()?;
    producer.enable(&reference).await?;
    let ticket = producer.sync_repo.get_clone_ticket_url().await?;
    clone_repo_init_from_url(&ticket, &b_root, CloneRepoInitOptions::default()).await?;
    let mut executor = Node::open(&b_root, RepoOpenOptions::default(), "executor").await?;
    executor.sync_repo.connect_url(&ticket).await?;
    let b_peer = executor.ctx.big_repo.local_peer_id();
    let group = producer
        .ctx
        .big_repo
        .create_group_with_parents(vec![])
        .await?;
    producer
        .ctx
        .big_repo
        .add_admin_member_to_group(producer.ctx.big_repo.local_keyhive_agent().await?, &group)
        .await?;
    let agent = producer
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&executor.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    producer
        .ctx
        .big_repo
        .add_member_to_group(agent, &group, Access::Edit)
        .await?;

    let command_pool = TaskPoolId::from_label("smoke.command");
    let (command_doc, command_key) = key_document(&producer, &group).await?;
    let command_ref = producer
        .pools()
        .register(
            command_doc.clone(),
            descriptor(
                command_pool.clone(),
                group.id().to_bytes(),
                command_key,
                "smoke.command",
            ),
        )
        .await?;
    let doc = producer
        .rt
        .drawer
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::LabelGeneric),
                WellKnownFacet::LabelGeneric("runtime-smoke".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    pull(
        &producer,
        &executor,
        [
            producer.ctx.doc_config.document_id(),
            producer.ctx.doc_drawer.document_id(),
            command_doc,
            manifest_doc.parse()?,
            doc.parse()?,
        ],
    )
    .await?;
    let a_snapshot = snapshot(&producer, &command_ref, &command_pool).await?;
    let b_snapshot = snapshot(&executor, &command_ref, &command_pool).await?;
    let a_tasks = producer
        .sync_repo
        .attach_task_pool(a_snapshot.clone())
        .await?;
    let mut b_tasks = executor
        .sync_repo
        .discover_task_pool(
            producer.sync_repo.endpoint_addr(),
            command_pool.clone(),
            group.id().to_bytes(),
        )
        .await?;
    let adapter = Arc::new(CapturedRoutineAdapter::new(
        Arc::clone(&producer.rt),
        a_snapshot.clone(),
        a_tasks,
    ));
    let (_, heads) = producer
        .rt
        .drawer
        .get_with_heads(&doc, BranchPath::new("main"), None)
        .await?
        .unwrap();
    let input = adapter
        .capture(
            "@daybook/test",
            "report-full-command",
            DispatchArgs::DocRoutine {
                doc_id: doc.clone(),
                branch_path: "main".into(),
                heads,
                invocation: RoutineInvocation::Command,
                changed_facet_keys: vec![],
                wflow_args_json: Some("{\"await_message\":true}".into()),
            },
        )
        .await?;
    let a_driver = producer
        .sync_repo
        .attach_scheduling_pool(
            a_snapshot.clone(),
            adapter,
            CapabilitySummary::empty(),
            Capacity::new(1),
            SchedulingTiming {
                heartbeat_interval: std::time::Duration::from_millis(100),
                router_settle: std::time::Duration::from_millis(100),
                router_takeover_after: std::time::Duration::from_secs(1),
            },
        )
        .await?;
    executor
        .sync_repo
        .attach_scheduling_pool(
            b_snapshot.clone(),
            Arc::new(CapturedRoutineAdapter::new(
                Arc::clone(&executor.rt),
                b_snapshot.clone(),
                Arc::clone(&b_tasks),
            )),
            CapabilitySummary::empty(),
            Capacity::new(1),
            SchedulingTiming {
                heartbeat_interval: std::time::Duration::from_millis(100),
                router_settle: std::time::Duration::from_millis(100),
                router_takeover_after: std::time::Duration::from_secs(1),
            },
        )
        .await?;
    let task_id = PoolTaskId::new(*blake3::hash(b"runtime-smoke-command").as_bytes());
    let declaration = TaskDeclaration {
        task_id,
        pool: command_pool.clone(),
        domain: DomainId::from_label(COMMAND_DOMAIN),
        producer: Some(NodePubkey::new(
            producer.ctx.big_repo.local_peer_id().to_bytes32()?,
        )),
        handler: HandlerRef::from_label(COMMAND_DOMAIN),
        input: serde_json::to_vec(&input)?,
        capabilities: CapabilitySummary::empty(),
        coordination_ref: None,
        placement: Preference::Only(NodePubkey::new(b_peer.to_bytes32()?)),
        effect_policy: EffectPolicy::AuthoritativePlacement,
        not_before_secs: None,
        // Explicit finite origin authorization for this command fixture.
        not_after_secs: jiff::Timestamp::now()
            .as_second()
            .checked_add(3600)
            .unwrap(),
        result_retention: ResultRetention::TicketAuthoritative { retain_until: None },
    };
    let digest = declaration.canonical_digest();
    let events = executor.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
    a_driver.submit(declaration, vec![]).await?;
    drop(a_driver);
    let attempt = loop {
        if let Some(attempt) = executor
            .rt
            .dispatch_repo
            .task_attempts(&command_pool)
            .await?
            .into_iter()
            .find(|attempt| attempt.request.key.task_id == task_id && attempt.job_id.is_some())
        {
            break attempt;
        }
        match events.recv_async().await {
            Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
            Err(crate::repos::RecvError::Closed) => eyre::bail!("command dispatch observer closed"),
        }
    };
    let job = attempt.job_id.as_deref().unwrap();
    let log = PartitionLogRef::new(Arc::clone(&executor.rt.wcx.logstore));
    let mut entries = log.tail(1);
    loop {
        let (_, entry) = entries
            .next()
            .await
            .ok_or_eyre("workflow log closed before guest message wait")??;
        if let Some(PartitionLogEntry::JobEffectResult(event)) = entry
            && event.job_id.as_ref() == job
            && matches!(event.result, JobRunResult::StepWait(wait)
                if matches!(wait.deets, JobWaitResultDeets::Message { .. }))
        {
            break;
        }
    }
    drop(entries);
    let original = super::sync_node::native_admission(|| b_tasks.ticket(task_id))
        .await?
        .unwrap()
        .declaration;
    let pools = producer.pools();
    let successor =
        super::sync_node::native_admission(|| pools.propose_successor(&a_snapshot)).await?;
    super::sync_node::native_admission(|| producer.sync_repo.attach_task_pool(successor.clone()))
        .await?;
    // Control supplies selection/provenance, not readable document bytes.
    // Deliver the successor key through the initial fixture’s native document
    // path before releasing the waiting guest; never substitute a newer key.
    pull(&producer, &executor, [successor.reference.document.clone()]).await?;
    b_tasks = super::sync_node::native_admission(|| {
        executor.sync_repo.discover_task_pool(
            producer.sync_repo.endpoint_addr(),
            command_pool.clone(),
            group.id().to_bytes(),
        )
    })
    .await?;
    eyre::ensure!(
        b_tasks.ticket(task_id).await?.unwrap().declaration == original,
        "rotation changed original command declaration"
    );
    let mut revisions = b_tasks
        .register()
        .open_consumer_reader(b"runtime-smoke-command-observer")
        .await?;
    executor
        .rt
        .wflow_ingress
        .send_message(
            Arc::from(job),
            Arc::from(format!("resume-{job}")),
            "{\"resume\":true}".into(),
        )
        .await?;
    println!(
        "running command rotated to generation {}; original attempt retained: {:?}",
        successor.descriptor.rotation_generation, attempt.request.key.attempt_id
    );
    let terminal = loop {
        if let Some(ticket) = super::sync_node::native_admission(|| b_tasks.ticket(task_id)).await?
            && let Some(terminal) = ticket.terminal_summary()
        {
            break terminal;
        }
        revisions.next(RevisionReadLimits::default()).await?;
    };
    eyre::ensure!(
        matches!(terminal, TerminalSummary::Succeeded { .. }),
        "command failed: {terminal:?}"
    );
    eyre::ensure!(
        executor
            .rt
            .dispatch_repo
            .incorporation_ack(&task_id, digest, &terminal)
            .await?
            .unwrap()
            .incorporated
            == terminal,
        "command receipt differs"
    );
    let report_file = executor
        .rt
        .sqlite_local_state_repo
        .get_sqlite_file_path("@daybook/test/capability-report")
        .await?;
    let sql = sqlx_utils_rs::SqlCtx::url(&format!("sqlite://{}", report_file.display())).await?;
    let report: String =
        sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?")
            .bind(&doc)
            .fetch_one(&sql.read_pool)
            .await?;
    println!("command durable effect: {report}; signed terminal: {terminal:?}");
    drop(sql);
    drop(revisions);
    drop(b_tasks);
    executor.stop().await?;
    executor = Node::open(&b_root, RepoOpenOptions::default(), "executor").await?;
    executor.sync_repo.connect_url(&ticket).await?;
    let recovered = executor
        .sync_repo
        .resume_task_pool(command_pool.clone(), group.id().to_bytes())
        .await?;
    eyre::ensure!(
        recovered.ticket(task_id).await?.unwrap().terminal_summary() == Some(terminal.clone()),
        "signed terminal lost on restart"
    );
    eyre::ensure!(
        executor
            .rt
            .dispatch_repo
            .incorporation_ack(&task_id, digest, &terminal)
            .await?
            .unwrap()
            .incorporated
            == terminal,
        "durable receipt lost on restart"
    );
    println!("executor recovery: original signed command terminal and effect receipt retained");

    let (_, loaded) = producer
        .rt
        .plugs_repo
        .materialize_active("@daybook/test", &reference)
        .await?
        .unwrap();
    let mut manifest = (*loaded).clone();
    // Changed processor policy is a new manifest revision, not an equal-version import.
    manifest.version.major = manifest
        .version
        .major
        .checked_add(1)
        .expect("manifest version exhausted");
    // This executable exercises distributed labeling, not the test plug
    // suite's independently configured OCR/embedding/indexing consumers.
    manifest
        .processors
        .retain(|name, _| name.as_str() == "test-label");
    Arc::make_mut(manifest.processors.get_mut("test-label").unwrap()).coordination =
        ProcessorCoordination::Distributed(DistributedProcessorPolicy {
            placement: ProcessorPlacement::Only(
                iroh::PublicKey::from_bytes(&b_peer.to_bytes32()?)?.to_string(),
            ),
            duplicates: ProcessorDuplicatePolicy::Idempotent,
        });
    producer
        .rt
        .drawer
        .update_at_heads(
            DocPatch {
                id: manifest_doc.clone(),
                facets_set: [(
                    FacetKey::from(WellKnownFacetTag::PlugManifest),
                    WellKnownFacet::PlugManifest(manifest).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            BranchPath::new("main"),
            None,
        )
        .await?;
    producer.enable(&reference).await?;
    let processor = "@daybook/test/test-label";
    let pool_id = TaskPoolId::for_processor("@daybook/test", "test-label");
    let (pool_doc, pool_key) = key_document(&producer, &group).await?;
    let pool_ref = producer
        .pools()
        .register(
            pool_doc.clone(),
            descriptor(
                pool_id.clone(),
                group.id().to_bytes(),
                pool_key.clone(),
                "smoke.triage",
            ),
        )
        .await?;
    let domain_doc = pool_doc;
    let domain = ProcessorDomainReference {
        document: domain_doc.clone(),
        authority_group: group.id().to_bytes(),
    };
    let (_, _, mut config_documents) = producer.rt.plugs_repo.capture_config_bindings().await?;
    let routine = &loaded.routines["test-label"];
    config_documents.retain(|owner, _| {
        owner == "@daybook/test"
            || routine
                .config_facet_acl()
                .iter()
                .any(|access| access.owner_plug_id.as_ref() == Some(owner))
    });
    ProcessorDomainRepo::new(
        Arc::clone(&producer.ctx.big_repo),
        producer.ctx.local_actor_id.clone(),
    )
    .provision(
        &domain,
        processor.into(),
        ProcessorConfiguration {
            // Explicit one-hour lifetime for this finite native fixture.
            task_lifetime_secs: std::num::NonZeroU64::new(3600).unwrap(),
            manifest: producer
                .rt
                .plugs_repo
                .enabled_ref("@daybook/test")
                .await?
                .unwrap(),
            config_documents,
        },
        pool_ref.clone(),
        pool_key,
    )
    .await?;
    pull(
        &producer,
        &executor,
        [
            producer.ctx.doc_config.document_id(),
            manifest_doc.parse()?,
            domain_doc,
        ],
    )
    .await?;
    executor.enable(&reference).await?;
    for node in [&producer, &executor] {
        let snapshot = snapshot(node, &pool_ref, &pool_id).await?;
        let adopted = node
            .sync_repo
            .attach_distributed_processor(
                Arc::clone(&node.rt),
                processor.into(),
                domain.clone(),
                snapshot,
                CapabilitySummary::empty(),
                Capacity::new(1),
                SchedulingTiming {
                    heartbeat_interval: std::time::Duration::from_millis(100),
                    router_settle: std::time::Duration::from_millis(100),
                    router_takeover_after: std::time::Duration::from_secs(1),
                },
            )
            .await?;
        eyre::ensure!(
            adopted
                || node
                    .sync_repo
                    .wait_for_processor_adoption(processor, &domain)
                    .await?,
            "processor not adopted"
        );
    }
    let tasks = executor.sync_repo.task_store(&pool_id).unwrap();
    let mut revisions = tasks
        .register()
        .open_consumer_reader(b"runtime-smoke-triage-observer")
        .await?;
    let events = executor.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
    let target = producer
        .rt
        .drawer
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note(Note {
                    mime: "text/plain".into(),
                    content: "two-node runtime triage".into(),
                })
                .into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    pull(
        &producer,
        &executor,
        [producer.ctx.doc_drawer.document_id(), target.parse()?],
    )
    .await?;
    let attempt = loop {
        if let Some(attempt) = executor.rt.dispatch_repo.task_attempts(&pool_id).await?.into_iter().find(|attempt| {
            matches!(serde_json::from_slice::<ResolvedRoutineInput>(&attempt.request.invocation.args).unwrap().input,
                CapturedRoutineInput::V1 { processor: Some(capture), .. } if capture.slot.document_id == target)
        }) { break attempt; }
        tokio::select! { result = revisions.next(RevisionReadLimits::default()) => { result?; }, result = events.recv_async() => { match result { Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}, Err(crate::repos::RecvError::Closed) => eyre::bail!("smoke dispatch observer closed") } } }
    };
    let terminal = loop {
        if let Some(ticket) =
            super::sync_node::native_admission(|| tasks.ticket(attempt.request.key.task_id)).await?
            && let Some(terminal) = ticket.terminal_summary()
        {
            break terminal;
        }
        let dispatch = executor
            .rt
            .dispatch_repo
            .get_any(&attempt.dispatch_id)
            .await
            .unwrap();
        eyre::ensure!(
            dispatch.status != crate::rt::dispatch::DispatchStatus::Failed,
            "native processor attempt failed: {:?}",
            executor
                .rt
                .dispatch_repo
                .task_local_failure(&attempt.request.key)
                .await?
        );
        tokio::select! { result = revisions.next(RevisionReadLimits::default()) => { result?; }, result = events.recv_async() => { match result { Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}, Err(crate::repos::RecvError::Closed) => eyre::bail!("smoke dispatch observer closed") } } }
    };
    eyre::ensure!(
        matches!(terminal, TerminalSummary::Succeeded { .. }),
        "triage failed: {terminal:?}"
    );
    let CapturedRoutineInput::V1 {
        processor: Some(capture),
        ..
    } = serde_json::from_slice::<ResolvedRoutineInput>(&attempt.request.invocation.args)?.input
    else {
        unreachable!()
    };
    let slots = executor
        .rt
        .processor_slot_store(processor, Some(&domain))
        .await?
        .unwrap();
    eyre::ensure!(
        super::sync_node::native_admission(|| slots.slot(&capture.slot))
            .await?
            .settled(&capture.capture.generation),
        "terminal preceded slot settlement"
    );
    let receipt = executor
        .rt
        .dispatch_repo
        .incorporation_ack(
            &attempt.request.key.task_id,
            attempt.request.declaration_digest,
            &terminal,
        )
        .await?
        .unwrap();
    eyre::ensure!(
        receipt.incorporated == terminal,
        "triage durable receipt differs"
    );
    let heads = executor
        .rt
        .drawer
        .get_branch_heads_for_path(&target, BranchPath::new("main"))
        .await?
        .unwrap();
    let effect = executor
        .rt
        .drawer
        .get_doc_with_facets_at_branch_heads(&target, BranchPath::new("main"), &heads, None)
        .await?
        .unwrap();
    eyre::ensure!(
        effect.facets[&FacetKey::from(WellKnownFacetTag::LabelGeneric)]
            == FacetRaw::from(WellKnownFacet::LabelGeneric("test_label".into())),
        "triage label effect absent"
    );
    println!(
        "triage durable effect receipt: {}; slot generation settled: {:?}; signed terminal: {terminal:?}",
        attempt.dispatch_id, capture.capture.generation
    );
    drop(slots);
    drop(revisions);
    drop(tasks);
    drop(recovered);
    executor.stop().await?;
    producer.stop().await?;
    Ok(())
}
