use super::*;
use crate::repos::{Repo as _, SubscribeOpts};
use crate::tasks::driver::{
    AdapterFuture, AttemptKey, AttemptOutcome, AttemptRequest, AttemptSubscription, DomainReceipt,
    PreparedAttempt, RecoveredAttempt, TaskExecutionAdapter,
};
use crate::tasks::{
    DomainCoordinationRef, EffectPolicy, OpaqueReason, OpaqueResultRef, Preference, ReadinessWatch,
    ResolvedInvocation, TaskClassification, TaskDeclaration, TaskPoolId, TaskTicket, TerminalFact,
    TerminalSummary,
};

pub const COMMAND_DOMAIN: &str = "daybook.command.v1";
pub const PROCESSOR_DOMAIN: &str = "daybook.processor.v1";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CapturedPoolBinding {
    document: String,
    authority_group: [u8; 32],
    pool: TaskPoolId,
}
impl CapturedPoolBinding {
    pub(crate) fn from_snapshot(snapshot: &crate::tasks::PoolDescriptorSnapshot) -> Self {
        Self {
            document: snapshot.reference.document.to_string(),
            authority_group: snapshot.reference.authority_group,
            pool: snapshot.descriptor.pool_id.clone(),
        }
    }
}

/// Exact domain work capture retained with an executable invocation. Configuration
/// documents include the processor owner even when its routine grants no config ACL.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CapturedProcessorInput {
    pub slot: triage::slots::ProcessorSlotKey,
    pub capture: triage::slots::ProcessorCapture,
    pub domain: triage::domain::ProcessorDomainReference,
    pub configuration: std::collections::BTreeMap<String, ChangeHashSet>,
}

/// Native invocation meaning, independent of the executor's attempt identity.
/// Configuration heads bind the native owner-to-document association, not rights.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CapturedRoutineInput {
    V1 {
        plug_id: String,
        routine_name: String,
        bundle_name: String,
        args: ActiveDispatchArgs,
        execution: dispatch::CapturedWflowExecution,
        configuration_document: String,
        configuration_heads: ChangeHashSet,
        pool_binding: Option<CapturedPoolBinding>,
        processor: Option<CapturedProcessorInput>,
    },
}

/// Stable execution meaning; native manifest/configuration heads are witnesses.
#[derive(serde::Serialize, serde::Deserialize)]
struct ProcessorTaskInput<'a> {
    slot: Cow<'a, triage::slots::ProcessorSlotKey>,
    capture: Cow<'a, triage::slots::ProcessorCapture>,
    domain: Cow<'a, triage::domain::ProcessorDomainReference>,
    plug_id: Cow<'a, str>,
    routine_name: Cow<'a, str>,
    bundle_name: Cow<'a, str>,
    manifest_document: Cow<'a, str>,
    configuration_document: Cow<'a, str>,
    pool: Cow<'a, CapturedPoolBinding>,
    changed_facet_keys: Cow<'a, [String]>,
    wflow_args_json: Option<Cow<'a, str>>,
}

impl CapturedRoutineInput {
    pub(super) fn task_input(&self) -> Res<Vec<u8>> {
        let Self::V1 {
            plug_id,
            routine_name,
            bundle_name,
            args,
            execution,
            configuration_document,
            pool_binding,
            processor,
            ..
        } = self;
        let Some(processor) = processor else {
            return Ok(serde_json::to_vec(self)?);
        };
        let ActiveDispatchArgs::FacetRoutine(args) = args;
        let dispatch::RoutineInvocation::Processor(invocation) = &args.invocation else {
            unreachable!("processor capture carries a processor invocation");
        };
        let dispatch::CapturedWflowExecution::V1 {
            manifest_doc_id, ..
        } = execution;
        Ok(serde_json::to_vec(&ProcessorTaskInput {
            slot: Cow::Borrowed(&processor.slot),
            capture: Cow::Borrowed(&processor.capture),
            domain: Cow::Borrowed(&processor.domain),
            plug_id: Cow::Borrowed(plug_id),
            routine_name: Cow::Borrowed(routine_name),
            bundle_name: Cow::Borrowed(bundle_name),
            manifest_document: Cow::Borrowed(manifest_doc_id),
            configuration_document: Cow::Borrowed(configuration_document),
            pool: Cow::Borrowed(pool_binding.as_ref().expect(ERROR_IMPOSSIBLE)),
            changed_facet_keys: Cow::Borrowed(&invocation.changed_facet_keys),
            wflow_args_json: args.wflow_args_json.as_deref().map(Cow::Borrowed),
        })?)
    }
}

/// Actual checked publisher retained with the exact invocation for rights rechecks.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ResolvedRoutineInput {
    publisher: crate::tasks::NodePubkey,
    pub(crate) input: CapturedRoutineInput,
}

pub(super) struct PreparedRoutine {
    pub dispatch_id: String,
    pub input: CapturedRoutineInput,
}

#[derive(Clone)]
pub struct CapturedRoutineAdapter {
    rt: Arc<Rt>,
    pool: CapturedPoolBinding,
    tasks: Option<Arc<crate::tasks::store::TaskStore>>,
    processor_domain: Option<(String, triage::domain::ProcessorDomainReference)>,
}

impl CapturedRoutineAdapter {
    pub fn new(
        rt: Arc<Rt>,
        snapshot: crate::tasks::PoolDescriptorSnapshot,
        tasks: Arc<crate::tasks::store::TaskStore>,
    ) -> Self {
        Self {
            rt,
            tasks: Some(tasks),
            pool: CapturedPoolBinding {
                document: snapshot.reference.document.to_string(),
                authority_group: snapshot.reference.authority_group,
                pool: snapshot.descriptor.pool_id,
            },
            processor_domain: None,
        }
    }

    pub(crate) fn for_processor(
        rt: Arc<Rt>,
        snapshot: crate::tasks::PoolDescriptorSnapshot,
        processor: String,
        domain: triage::domain::ProcessorDomainReference,
        tasks: Arc<crate::tasks::store::TaskStore>,
    ) -> Self {
        let mut adapter = Self::new(rt, snapshot, tasks);
        adapter.processor_domain = Some((processor, domain));
        adapter
    }

    /// The origin captures through the same preparation used by local callers.
    /// This performs no dispatch persistence or workflow admission.
    pub async fn capture(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
    ) -> Res<CapturedRoutineInput> {
        let mut input = self
            .rt
            .prepare_routine(plug_id, routine_name, args, None)
            .await?
            .input;
        let CapturedRoutineInput::V1 { pool_binding, .. } = &mut input;
        *pool_binding = Some(self.pool.clone());
        Ok(input)
    }

    fn watch_for(
        declaration: &TaskDeclaration,
        witness: Option<(&[u8], crate::tasks::NodePubkey)>,
    ) -> ReadinessWatch {
        ReadinessWatch::new(DomainCoordinationRef::from_label(
            serde_json::to_string(&(declaration, witness)).expect(ERROR_JSON),
        ))
    }

    async fn classify_declaration(&self, declaration: &TaskDeclaration) -> Res<TaskClassification> {
        if let Some((identity, domain)) = &self.processor_domain {
            let input: ProcessorTaskInput<'_> = match serde_json::from_slice(&declaration.input) {
                Ok(input) => input,
                Err(_) => return Ok(TaskClassification::Invalid),
            };
            if input.slot.processor_full_id != *identity || *input.domain != *domain {
                return Ok(TaskClassification::Invalid);
            }
            if let Some(store) = self.rt.processor_slot_store(identity, Some(domain)).await?
                && store
                    .slot(&input.slot)
                    .await?
                    .settled(&input.capture.generation)
            {
                return Ok(TaskClassification::Obsolete);
            }
        }
        let tasks = self
            .tasks
            .as_ref()
            .expect("ticket classification requires an attached task store");
        let Some(ticket) = tasks.ticket(declaration.task_id).await? else {
            return Ok(TaskClassification::NotReady(Self::watch_for(
                declaration,
                None,
            )));
        };
        if ticket.declaration.declaration.canonical_digest() != declaration.canonical_digest() {
            return Ok(TaskClassification::Invalid);
        }
        let mut pending = None;
        for evidence in &ticket.declaration.publishers {
            let bytes = if self.processor_domain.is_some() {
                evidence.input_witness.as_slice()
            } else {
                declaration.input.as_slice()
            };
            match self
                .classify_input(declaration, bytes, evidence.publisher)
                .await?
            {
                outcome @ (TaskClassification::Runnable(_) | TaskClassification::Obsolete) => {
                    return Ok(outcome);
                }
                outcome @ (TaskClassification::NotReady(_)
                | TaskClassification::CoordinationIncomplete) => pending = Some(outcome),
                TaskClassification::Invalid => {}
            }
        }
        Ok(pending.unwrap_or(TaskClassification::Invalid))
    }

    async fn classify_attempt(&self, request: &AttemptRequest) -> Res<TaskClassification> {
        let resolved: ResolvedRoutineInput = serde_json::from_slice(&request.invocation.args)?;
        self.classify_input(
            &request.declaration,
            &serde_json::to_vec(&resolved.input)?,
            resolved.publisher,
        )
        .await
    }
    async fn classify_input(
        &self,
        declaration: &TaskDeclaration,
        input_bytes: &[u8],
        publisher: crate::tasks::NodePubkey,
    ) -> Res<TaskClassification> {
        let domain = if self.processor_domain.is_some() {
            PROCESSOR_DOMAIN
        } else {
            COMMAND_DOMAIN
        };
        if declaration.domain.as_str() != domain || declaration.handler.as_str() != domain {
            return Ok(TaskClassification::Invalid);
        }
        let now = jiff::Timestamp::now().as_second();
        if declaration
            .not_after_secs
            .is_some_and(|deadline| now > deadline)
        {
            return Ok(TaskClassification::Invalid);
        }
        if declaration.not_before_secs.is_some_and(|start| now < start) {
            return Ok(TaskClassification::NotReady(Self::watch_for(
                declaration,
                Some((input_bytes, publisher)),
            )));
        }
        let local =
            crate::tasks::NodePubkey::new(self.rt.rcx.big_repo.local_peer_id().to_bytes32()?);
        if matches!(declaration.placement, Preference::Only(node) if node != local) {
            return Ok(TaskClassification::NotReady(Self::watch_for(
                declaration,
                Some((input_bytes, publisher)),
            )));
        }
        match declaration.effect_policy {
            EffectPolicy::AuthoritativePlacement
                if !matches!(declaration.placement, Preference::Only(_)) =>
            {
                return Ok(TaskClassification::Invalid);
            }
            // No external-idempotency mechanism is manufactured for arbitrary
            // components. Such a command needs an actual domain contract first.
            EffectPolicy::ExternalIdempotencyKey | EffectPolicy::Idempotent
                if self.processor_domain.is_none() =>
            {
                return Ok(TaskClassification::Invalid);
            }
            EffectPolicy::AuthoritativePlacement
            | EffectPolicy::AcceptsDuplicates
            | EffectPolicy::ExternalIdempotencyKey
            | EffectPolicy::Idempotent => {}
        }
        match self.pool_rights(declaration, publisher).await? {
            Some(true) => {}
            Some(false) => return Ok(TaskClassification::Invalid),
            None => {
                return Ok(TaskClassification::NotReady(Self::watch_for(
                    declaration,
                    Some((input_bytes, publisher)),
                )));
            }
        }
        let input: CapturedRoutineInput = match serde_json::from_slice(input_bytes) {
            Ok(input) => input,
            Err(_) => return Ok(TaskClassification::Invalid),
        };
        let CapturedRoutineInput::V1 {
            plug_id,
            routine_name,
            bundle_name,
            args,
            execution,
            configuration_document,
            configuration_heads,
            pool_binding,
            processor,
        } = &input;
        let dispatch::CapturedWflowExecution::V1 {
            manifest_doc_id,
            manifest_branch,
            manifest_heads,
            component_blobs,
            ..
        } = execution;
        let ActiveDispatchArgs::FacetRoutine(args) = args;
        if !encoded_eq(pool_binding, &Some(self.pool.clone()))? {
            return Ok(TaskClassification::Invalid);
        }
        if manifest_branch != "main"
            || manifest_heads.is_empty()
            || args.heads.is_empty()
            || configuration_heads.is_empty()
            || configuration_document != self.rt.plugs_repo.configuration_document_id()
            || args.primary_doc.doc_id != args.doc_id
            || args.primary_doc.branch_path != args.branch_path
            || args.primary_doc.heads != args.heads
        {
            return Ok(TaskClassification::Invalid);
        }
        let Some(manifest) = self
            .rt
            .plugs_repo
            .read_manifest_doc(manifest_doc_id, manifest_heads)
            .await?
        else {
            return Ok(TaskClassification::NotReady(Self::watch_for(
                declaration,
                Some((input_bytes, publisher)),
            )));
        };
        if manifest.id() != *plug_id {
            return Ok(TaskClassification::Invalid);
        }
        let Some(routine) = manifest.routines.get(routine_name.as_str()) else {
            return Ok(TaskClassification::Invalid);
        };
        match (&self.processor_domain, processor, &args.invocation) {
            (None, None, dispatch::RoutineInvocation::Command) => {
                if !manifest.commands.values().any(|command| {
                    let manifest::CommandDeets::DocCommand {
                        routine_name: command_routine,
                    } = &command.deets;
                    command_routine.0 == *routine_name
                }) {
                    return Ok(TaskClassification::Invalid);
                }
            }
            (
                Some((identity, domain)),
                Some(processor),
                dispatch::RoutineInvocation::Processor(invocation),
            ) => {
                if processor.slot.processor_full_id != *identity
                    || processor.domain != *domain
                    || processor.slot.document_id != args.doc_id
                    || processor.slot.branch_path != args.branch_path
                    || processor.capture.heads != args.heads
                    || invocation.trigger_doc_id != args.doc_id
                    || invocation.task_id.as_deref()
                        != Some(declaration.task_id.to_string().as_str())
                {
                    return Ok(TaskClassification::Invalid);
                }
                let Some(policy) = manifest.processors.iter().find_map(|(name, policy)| {
                    (format!("{plug_id}/{name}") == *identity).then_some(policy)
                }) else {
                    return Ok(TaskClassification::Invalid);
                };
                let manifest::ProcessorDeets::DocProcessor {
                    routine_name: expected,
                    ..
                } = &policy.deets;
                if expected.0 != *routine_name {
                    return Ok(TaskClassification::Invalid);
                }
                let manifest::ProcessorCoordination::Distributed(distributed) =
                    &policy.coordination
                else {
                    return Ok(TaskClassification::Invalid);
                };
                let (placement, effects) = processor_policy(distributed, declaration.producer)?;
                if placement != declaration.placement || effects != declaration.effect_policy {
                    return Ok(TaskClassification::Invalid);
                }
                let keys: Vec<_> = invocation
                    .changed_facet_keys
                    .iter()
                    .map(|key| daybook_types::doc::FacetKey::from(key.as_str()))
                    .collect();
                let Some(origin) = self
                    .rt
                    .drawer
                    .facet_source_origin_at_heads(
                        &args.doc_id,
                        &args.branch_path,
                        &args.heads,
                        &keys,
                    )
                    .await?
                else {
                    return Ok(TaskClassification::NotReady(Self::watch_for(
                        declaration,
                        Some((input_bytes, publisher)),
                    )));
                };
                if origin != declaration.producer {
                    return Ok(TaskClassification::Invalid);
                }
                let mut config_values = std::collections::BTreeMap::new();
                for (document, heads) in &processor.configuration {
                    let Some(doc) = self
                        .rt
                        .drawer
                        .get_doc_with_facets_at_branch_heads(
                            document,
                            daybook_types::doc::BranchPath::new("main"),
                            heads,
                            None,
                        )
                        .await?
                    else {
                        return Ok(TaskClassification::NotReady(Self::watch_for(
                            declaration,
                            Some((input_bytes, publisher)),
                        )));
                    };
                    config_values.insert(document.clone(), doc.facets.clone());
                }
                let artifact = triage::semantic_generation(
                    &(&manifest, component_blobs),
                    b"daybook/processor-artifact/v1",
                )?;
                let configuration = triage::semantic_generation(
                    &config_values,
                    b"daybook/processor-configuration/v1",
                )?;
                let expected = triage::slots::ProcessorCapture::new(
                    args.heads.clone(),
                    processor.capture.execution_baseline.clone(),
                    artifact,
                    configuration,
                );
                if expected != processor.capture {
                    return Ok(TaskClassification::Invalid);
                }
                let mut hash = blake3::Hasher::new();
                hash.update(b"daybook/processor-task/v1\0");
                hash.update(&processor.slot.id());
                hash.update(&expected.generation);
                if crate::tasks::PoolTaskId::new(*hash.finalize().as_bytes()) != declaration.task_id
                {
                    return Ok(TaskClassification::Invalid);
                }
                let Some(store) = self.rt.processor_slot_store(identity, Some(domain)).await?
                else {
                    return Ok(TaskClassification::NotReady(Self::watch_for(
                        declaration,
                        Some((input_bytes, publisher)),
                    )));
                };
                let slot = store.slot(&processor.slot).await?;
                if slot.settled(&processor.capture.generation) {
                    return Ok(TaskClassification::Obsolete);
                }
                if !slot
                    .desired()
                    .any(|desired| desired.matches && desired.capture == processor.capture)
                {
                    return Ok(TaskClassification::NotReady(Self::watch_for(
                        declaration,
                        Some((input_bytes, publisher)),
                    )));
                }
            }
            _ => return Ok(TaskClassification::Invalid),
        }
        if input.task_input()? != declaration.input {
            return Ok(TaskClassification::Invalid);
        }
        let manifest::RoutineImpl::Wflow { key, bundle } = &routine.r#impl;
        if bundle.0 != *bundle_name {
            return Ok(TaskClassification::Invalid);
        }
        let Some(bundle_manifest) = manifest.wflow_bundles.get(bundle_name.as_str()) else {
            return Ok(TaskClassification::Invalid);
        };
        // Imported manifests already normalize file/OCI components into owned
        // digest URLs. A raw pathname is not authority for remote blob bytes.
        if bundle_manifest
            .component_urls
            .iter()
            .any(|url| url.scheme() != crate::blobs::BLOB_SCHEME)
        {
            return Ok(TaskClassification::Invalid);
        }
        let expected = capture_wflow_execution(
            &self.rt.blobs_repo,
            plug_id,
            bundle_name,
            &key.0,
            manifest_doc_id,
            manifest_heads.clone(),
            bundle_manifest,
        )
        .await?;
        if !encoded_eq(&expected, execution)?
            || !encoded_eq(&routine.facet_acl(), &args.primary_doc.facet_acl)?
            || !encoded_eq(&routine.local_state_acl, &args.local_state_acl)?
            || !encoded_eq(
                routine.command_invoke_acl(),
                &args.command_invoke_acl_snapshot,
            )?
        {
            return Ok(TaskClassification::Invalid);
        }
        let Some(bindings) = self
            .rt
            .plugs_repo
            .config_bindings_at(configuration_document, configuration_heads)
            .await?
        else {
            return Ok(TaskClassification::NotReady(Self::watch_for(
                declaration,
                Some((input_bytes, publisher)),
            )));
        };
        if let Some(processor) = processor {
            let Some(owner) = bindings.get(plug_id) else {
                return Ok(TaskClassification::Invalid);
            };
            let mut expected: std::collections::BTreeMap<_, _> = args
                .config_docs
                .iter()
                .map(|doc| (doc.doc_id.clone(), doc.heads.clone()))
                .collect();
            let Some(heads) = processor.configuration.get(owner) else {
                return Ok(TaskClassification::Invalid);
            };
            expected
                .entry(owner.clone())
                .or_insert_with(|| heads.clone());
            if expected != processor.configuration {
                return Ok(TaskClassification::Invalid);
            }
        }
        let mut config_acl =
            std::collections::BTreeMap::<&str, Vec<&manifest::RoutineFacetAccess>>::new();
        for access in routine.config_facet_acl() {
            config_acl
                .entry(access.owner_plug_id.as_deref().unwrap_or(plug_id))
                .or_default()
                .push(access);
        }
        if args.config_docs.len() != config_acl.len() {
            return Ok(TaskClassification::Invalid);
        }
        for (captured, (owner, acl)) in args.config_docs.iter().zip(config_acl) {
            if bindings.get(owner) != Some(&captured.doc_id)
                || captured.branch_path.as_str() != "main"
                || captured.staging_branch_path.as_str() != "main"
                || captured.heads.is_empty()
                || !encoded_eq(&acl, &captured.facet_acl)?
            {
                return Ok(TaskClassification::Invalid);
            }
        }
        match self.current_rights(&input, publisher).await? {
            Some(true) => {}
            Some(false) => return Ok(TaskClassification::Invalid),
            None => {
                return Ok(TaskClassification::NotReady(Self::watch_for(
                    declaration,
                    Some((input_bytes, publisher)),
                )));
            }
        }
        for doc in std::iter::once(&args.primary_doc).chain(&args.config_docs) {
            if self
                .rt
                .drawer
                .get_doc_with_facets_at_branch_heads(
                    &doc.doc_id,
                    &doc.branch_path,
                    &doc.heads,
                    None,
                )
                .await?
                .is_none()
            {
                return Ok(TaskClassification::NotReady(Self::watch_for(
                    declaration,
                    Some((input_bytes, publisher)),
                )));
            }
        }
        for blob in component_blobs {
            if !self.rt.blobs_repo.has_blob_on_disk(blob.clone()).await?
                && self
                    .rt
                    .blobs_repo
                    .ensure_hash_materialized(blob.clone())
                    .await
                    .is_err()
            {
                return Ok(TaskClassification::NotReady(Self::watch_for(
                    declaration,
                    Some((input_bytes, publisher)),
                )));
            }
            let bytes = self.rt.blobs_repo.get_bytes(blob.clone()).await?;
            if crate::blobs::BlobId::new(*blake3::hash(&bytes).as_bytes()) != *blob {
                return Ok(TaskClassification::Invalid);
            }
        }
        Ok(TaskClassification::Runnable(ResolvedInvocation {
            args: serde_json::to_vec(&ResolvedRoutineInput { publisher, input })?,
        }))
    }

    async fn pool_rights(
        &self,
        declaration: &TaskDeclaration,
        publisher: crate::tasks::NodePubkey,
    ) -> Res<Option<bool>> {
        use big_repo::CoordinationError;
        if self.pool.pool != declaration.pool {
            return Ok(Some(false));
        }
        let reference = crate::tasks::PoolReference {
            document: self.pool.document.parse()?,
            authority_group: self.pool.authority_group,
        };
        let pools = crate::tasks::PoolRepo::new(
            Arc::clone(&self.rt.rcx.big_repo),
            self.rt.rcx.doc_config.document_id(),
            self.rt.rcx.local_actor_id.clone(),
        );
        match pools
            .load_descriptor(&reference, &self.pool.pool, self.pool.authority_group)
            .await?
        {
            crate::tasks::PoolDiscovery::Ready(_) => {}
            crate::tasks::PoolDiscovery::Pending { .. } => return Ok(None),
            _ => return Ok(Some(false)),
        }
        let admitted = async {
            let sampled = self
                .rt
                .rcx
                .big_repo
                .coordination_authority(reference.document, reference.authority_group)
                .await?;
            self.rt
                .rcx
                .big_repo
                .admit_coordination_access(&sampled, big_repo::keyhive_core::access::Access::Edit)
                .await
        }
        .await;
        match admitted {
            Ok(authority) => Ok(Some(
                authority.edit_agents().contains(&publisher.to_bytes32()),
            )),
            Err(CoordinationError::Pending) => Ok(None),
            Err(CoordinationError::Unauthorized | CoordinationError::Invalid(_)) => Ok(Some(false)),
            Err(CoordinationError::Other(error)) => Err(error),
        }
    }

    async fn current_rights(
        &self,
        input: &CapturedRoutineInput,
        publisher: crate::tasks::NodePubkey,
    ) -> Res<Option<bool>> {
        let CapturedRoutineInput::V1 {
            args,
            execution,
            configuration_document,
            processor,
            ..
        } = input;
        let dispatch::CapturedWflowExecution::V1 {
            manifest_doc_id,
            manifest_heads,
            ..
        } = execution;
        if !self
            .native_rights(configuration_document.parse()?, publisher, false)
            .await?
        {
            return Ok(Some(false));
        }
        let Some(manifest) = self
            .rt
            .drawer
            .native_document_at_branch_heads(
                manifest_doc_id,
                daybook_types::doc::BranchPath::new("main"),
                manifest_heads,
            )
            .await?
        else {
            return Ok(None);
        };
        if !self.native_rights(manifest, publisher, false).await? {
            return Ok(Some(false));
        }
        let ActiveDispatchArgs::FacetRoutine(args) = args;
        for doc in std::iter::once(&args.primary_doc).chain(&args.config_docs) {
            let Some(native) = self
                .rt
                .drawer
                .native_document_at_branch_heads(&doc.doc_id, &doc.branch_path, &doc.heads)
                .await?
            else {
                return Ok(None);
            };
            let edit = doc
                .facet_acl
                .iter()
                .any(|access| access.write || access.create || access.delete);
            if !self.native_rights(native, publisher, edit).await? {
                return Ok(Some(false));
            }
        }
        if let Some(processor) = processor {
            use big_repo::CoordinationError;
            let admission = async {
                let sampled = self
                    .rt
                    .rcx
                    .big_repo
                    .coordination_authority(
                        processor.domain.document.clone(),
                        processor.domain.authority_group,
                    )
                    .await?;
                self.rt
                    .rcx
                    .big_repo
                    .admit_coordination_access(
                        &sampled,
                        big_repo::keyhive_core::access::Access::Edit,
                    )
                    .await
            }
            .await;
            match admission {
                Ok(authority) if authority.edit_agents().contains(&publisher.to_bytes32()) => {}
                Ok(_) | Err(CoordinationError::Unauthorized | CoordinationError::Invalid(_)) => {
                    return Ok(Some(false));
                }
                Err(CoordinationError::Pending) => return Ok(None),
                Err(CoordinationError::Other(error)) => return Err(error),
            }
            for (document, heads) in &processor.configuration {
                let Some(native) = self
                    .rt
                    .drawer
                    .native_document_at_branch_heads(
                        document,
                        daybook_types::doc::BranchPath::new("main"),
                        heads,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                if !self.native_rights(native, publisher, false).await? {
                    return Ok(Some(false));
                }
            }
        }
        Ok(Some(true))
    }

    /// Authorization is checked now on the resolved native object. Captured
    /// associations/heads prove identity; they do not grant historical rights.
    async fn native_rights(
        &self,
        document: big_repo::DocumentId,
        publisher: crate::tasks::NodePubkey,
        edit: bool,
    ) -> Res<bool> {
        use big_repo::keyhive_core::principal::{identifier::Identifier, public::Public};
        let document = Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(
            &document.to_bytes32()?,
        )?);
        let publisher = Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(
            &publisher.to_bytes32(),
        )?);
        let executor = self.rt.rcx.big_repo.local_keyhive_agent().await?.id();
        let keyhive = self.rt.rcx.big_repo.keyhive();
        let public = keyhive.agent_access_on(&Public.id(), document).await;
        for principal in [publisher, executor] {
            let access = keyhive.agent_access_on(&principal, document).await;
            let allowed = |access: big_repo::keyhive_core::access::Access| {
                if edit {
                    access.is_editor()
                } else {
                    access.is_reader()
                }
            };
            if !access.is_some_and(allowed) && !public.is_some_and(allowed) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn persist_inner(&self, request: AttemptRequest) -> Res<PreparedAttempt> {
        eyre::ensure!(
            request.key.task_id == request.declaration.task_id
                && request.key.pool == request.declaration.pool
                && request.declaration_digest == request.declaration.canonical_digest(),
            "attempt request detached from declaration"
        );
        let TaskClassification::Runnable(invocation) = self.classify_attempt(&request).await?
        else {
            eyre::bail!("captured command is not runnable at durable preparation");
        };
        eyre::ensure!(
            invocation == request.invocation,
            "resolved invocation changed before persistence"
        );
        let ResolvedRoutineInput { input, .. } = serde_json::from_slice(&invocation.args)?;
        let CapturedRoutineInput::V1 {
            plug_id,
            routine_name,
            bundle_name,
            mut args,
            execution,
            processor,
            ..
        } = input;
        let encoded_key = serde_json::to_vec(&request.key)?;
        let dispatch_id = format!(
            "pool-command/{}",
            utils_rs::hash::blake3_hash_bytes_multibase(&encoded_key)
        );
        let job_id = format!("{dispatch_id}-{:032x}", request.key.attempt_id.to_u128());
        let staging = format!("/tmp/{job_id}");
        let ActiveDispatchArgs::FacetRoutine(facet_args) = &mut args;
        facet_args.staging_branch_path = staging.clone().into();
        facet_args.primary_doc.staging_branch_path = staging.clone().into();
        let attempt = PreparedAttempt {
            capture_digest: *blake3::hash(&request.invocation.args).as_bytes(),
            workflow_partition: self.rt.local_wflow_part_id.clone(),
            dispatch_id: dispatch_id.clone(),
            job_id: Some(job_id.clone()),
            staging_id: Some(staging),
            request,
        };
        let dispatch = Arc::new(dispatch::DispatchAttempt::new(dispatch::ActiveDispatch {
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: None,
                entry_id: None,
                plug_id,
                routine_name,
                bundle_name,
                wflow_job_id: Some(job_id),
            },
            args,
            execution,
            status: dispatch::DispatchStatus::Active,
            waiting_on_dispatch_ids: vec![],
            on_success_hooks: processor
                .into_iter()
                .map(|processor| DispatchOnSuccessHook::ProcessorSettlement {
                    slot: processor.slot,
                    capture: processor.capture,
                    domain: Some(processor.domain),
                })
                .collect(),
        }));
        let _admission = self.rt.task_admission.lock().await;
        let resolved: ResolvedRoutineInput =
            serde_json::from_slice(&attempt.request.invocation.args)?;
        let input = resolved.input;
        eyre::ensure!(
            self.pool_rights(&attempt.request.declaration, resolved.publisher)
                .await?
                == Some(true)
                && self.current_rights(&input, resolved.publisher).await? == Some(true),
            "captured command authority changed before durable preparation"
        );
        let attempt = self
            .rt
            .dispatch_repo
            .persist_task_attempt(attempt, dispatch)
            .await?;
        let job = attempt
            .job_id
            .as_deref()
            .expect("native prepared attempt has a job");
        let admitted = self
            .rt
            .wflow_part_state
            .read_jobs()
            .await
            .active
            .contains_key(job);
        if !admitted {
            self.rt.execution_gate.hold(Arc::from(job));
        }
        Ok(attempt)
    }

    async fn retained(&self, supplied: &PreparedAttempt) -> Res<PreparedAttempt> {
        let stored = self
            .rt
            .dispatch_repo
            .task_attempt(&supplied.request.key)
            .await?
            .ok_or_eyre("pool attempt is not durably prepared")?;
        eyre::ensure!(
            encoded_eq(&stored, supplied)?,
            "pool attempt differs from durable preparation"
        );
        Ok(stored)
    }

    async fn outcome(&self, attempt: &PreparedAttempt) -> Res<Option<AttemptOutcome>> {
        let dispatch = self
            .rt
            .dispatch_repo
            .get_any(&attempt.dispatch_id)
            .await
            .ok_or_eyre("retained task mapping has no dispatch")?;
        if dispatch.status.is_terminal()
            && dispatch.status != dispatch::DispatchStatus::Succeeded
            && let Some(reason) = self
                .rt
                .dispatch_repo
                .task_local_failure(&attempt.request.key)
                .await?
        {
            return Ok(Some(AttemptOutcome::Failed(OpaqueReason::from_label(
                reason,
            ))));
        }
        let outcome = match dispatch.status {
            dispatch::DispatchStatus::Succeeded => {
                let fact = TerminalFact::Succeeded {
                    attempt_id: attempt.request.key.attempt_id,
                    result_ref: Some(OpaqueResultRef::from_label(attempt.dispatch_id.clone())),
                };
                Some(AttemptOutcome::Finished(fact))
            }
            dispatch::DispatchStatus::Cancelled => {
                Some(AttemptOutcome::Finished(TerminalFact::Cancelled {
                    reason: OpaqueReason::from_label("native dispatch durably cancelled"),
                }))
            }
            dispatch::DispatchStatus::Failed => Some(AttemptOutcome::Failed(
                OpaqueReason::from_label("native captured dispatch failed"),
            )),
            dispatch::DispatchStatus::Waiting | dispatch::DispatchStatus::Active => None,
        };
        if let Some(AttemptOutcome::Finished(fact)) = &outcome {
            self.rt
                .dispatch_repo
                .incorporate_task_receipt(
                    DomainReceipt {
                        task_id: attempt.request.key.task_id,
                        summary: summary(fact),
                    },
                    attempt.request.declaration_digest,
                )
                .await?;
        }
        Ok(outcome)
    }

    pub(super) async fn authorize_retained_boot(rt: Arc<Rt>) -> Res<()> {
        for (dispatch_id, _) in rt.dispatch_repo.list_unsettled().await {
            let Some(attempt) = rt
                .dispatch_repo
                .task_attempt_for_dispatch(&dispatch_id)
                .await?
            else {
                continue;
            };
            let resolved: ResolvedRoutineInput =
                serde_json::from_slice(&attempt.request.invocation.args)?;
            let input = resolved.input;
            let CapturedRoutineInput::V1 {
                pool_binding,
                processor,
                ..
            } = &input;
            let Some(pool) = pool_binding else {
                Self::fail_local(&rt, &attempt, "retained command has no native pool binding")
                    .await?;
                continue;
            };
            let adapter = Self {
                rt: Arc::clone(&rt),
                pool: pool.clone(),
                tasks: None,
                processor_domain: processor.as_ref().map(|capture| {
                    (
                        capture.slot.processor_full_id.clone(),
                        capture.domain.clone(),
                    )
                }),
            };
            if rt
                .dispatch_repo
                .task_local_failure(&attempt.request.key)
                .await?
                .is_some()
            {
                Self::fail_local(&rt, &attempt, "retained local task failure").await?;
                continue;
            }
            match adapter.classify_attempt(&attempt.request).await? {
                TaskClassification::Runnable(_) => {
                    let job = attempt
                        .job_id
                        .as_deref()
                        .ok_or_eyre("retained native task has no job")?;
                    let admitted = rt
                        .wflow_part_state
                        .read_jobs()
                        .await
                        .active
                        .contains_key(job);
                    if admitted {
                        let dispatch = rt
                            .dispatch_repo
                            .get_any(&attempt.dispatch_id)
                            .await
                            .ok_or_eyre("retained command dispatch vanished")?;
                        let ActiveDispatchDeets::Wflow {
                            plug_id,
                            bundle_name,
                            ..
                        } = &dispatch.deets;
                        if let Err(error) = ensure_bundle_workload_running(
                            &rt.wash_host,
                            &rt.blobs_repo,
                            plug_id,
                            bundle_name,
                            &dispatch.execution,
                        )
                        .await
                        {
                            Self::fail_local(
                                &rt,
                                &attempt,
                                &format!("retained component refused: {error:#}"),
                            )
                            .await?;
                            continue;
                        }
                        if adapter
                            .pool_rights(&attempt.request.declaration, resolved.publisher)
                            .await?
                            == Some(true)
                            && adapter.current_rights(&input, resolved.publisher).await?
                                == Some(true)
                        {
                            rt.execution_gate.release(job);
                        }
                    }
                }
                TaskClassification::NotReady(_) | TaskClassification::CoordinationIncomplete => {}
                TaskClassification::Invalid | TaskClassification::Obsolete => {
                    Self::fail_local(&rt, &attempt, "retained command is no longer authorized")
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn fail_local(rt: &Rt, attempt: &PreparedAttempt, reason: &str) -> Res<()> {
        let _admission = rt.task_admission.lock().await;
        let current = rt
            .dispatch_repo
            .get_any(&attempt.dispatch_id)
            .await
            .ok_or_eyre("retained pool attempt has no native dispatch")?;
        let ActiveDispatchDeets::Wflow { entry_id, .. } = &current.deets;
        let prefix = match entry_id {
            Some(entry) => *entry,
            None => rt.wcx.logstore.latest_idx().await?,
        };
        // Absence and archived outcomes are meaningful only after the exact
        // durable admission prefix has reached the reducer. Cancellation shares
        // admission ownership so it cannot miss a concurrently appended JobInit.
        rt.wflow_part_state.wait_for_prefix(prefix).await?;
        rt.reconcile_retained_dispatch(attempt.dispatch_id.clone(), current, true)
            .await?;
        let current = rt
            .dispatch_repo
            .get_any(&attempt.dispatch_id)
            .await
            .ok_or_eyre("retained pool attempt has no native dispatch")?;
        if current.status.is_terminal() {
            return Ok(());
        }
        rt.dispatch_repo
            .fail_task_locally(&attempt.request.key, reason)
            .await?;
        let job = attempt
            .job_id
            .as_deref()
            .ok_or_eyre("retained native task has no job")?;
        let admitted = {
            let state = rt.wflow_part_state.read_jobs().await;
            state
                .active
                .get(job)
                .or_else(|| state.archive.get(job))
                .map(|job| job.init_entry_id)
        };
        if let Some(entry) = admitted {
            rt.record_job_admission(&attempt.dispatch_id, &current, entry)
                .await?;
            rt.dispatch_repo
                .cancel(&attempt.dispatch_id, &current)
                .await?;
            let cancellation = rt
                .wflow_ingress
                .cancel_job(Arc::from(job), reason.to_owned())
                .await?;
            rt.wflow_part_state.wait_for_prefix(cancellation).await?;
            // The incorporated cancellation is a real control outcome. Native
            // cleanup settles it; the local failure record prevents a global
            // cancellation fact from being manufactured for revoked authority.
            rt.handle_wflow_result(cancellation, job, &JobRunResult::Aborted)
                .await?;
            rt.execution_gate.release(job);
        } else {
            rt.dispatch_repo
                .complete(
                    attempt.dispatch_id.clone(),
                    dispatch::DispatchStatus::Failed,
                    &current,
                )
                .await?;
            rt.execution_gate.release(job);
        }
        Ok(())
    }

    async fn start_inner(&self, attempt: PreparedAttempt) -> Res<AttemptSubscription> {
        let attempt = self.retained(&attempt).await?;
        let subscription = self.subscription(attempt.clone());
        let current = self
            .rt
            .dispatch_repo
            .get_any(&attempt.dispatch_id)
            .await
            .ok_or_eyre("retained command dispatch vanished")?;
        self.rt
            .reconcile_retained_dispatch(attempt.dispatch_id.clone(), current, true)
            .await?;
        loop {
            if let Some(outcome) = self.outcome(&attempt).await? {
                if let AttemptOutcome::Failed(reason) = outcome {
                    eyre::bail!("retained native attempt failed: {reason}");
                }
                return Ok(subscription);
            }
            match self.classify_attempt(&attempt.request).await? {
                TaskClassification::NotReady(watch) => {
                    let ready = self.watch(watch).await?;
                    let dispatches = self.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
                    if self.outcome(&attempt).await?.is_some() {
                        continue;
                    }
                    tokio::select! {
                        result = ready => { result?; }
                        result = dispatches.recv_async() => {
                            match result {
                                Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
                                Err(crate::repos::RecvError::Closed) => eyre::bail!("native start listener closed"),
                            }
                        }
                    }
                    continue;
                }
                TaskClassification::Runnable(_) => {}
                TaskClassification::Invalid
                | TaskClassification::Obsolete
                | TaskClassification::CoordinationIncomplete => {
                    Self::fail_local(&self.rt, &attempt, "captured command refused at start")
                        .await?;
                    eyre::bail!("captured command is not authorized at start");
                }
            }
            let admission = self.rt.task_admission.lock().await;
            let resolved: ResolvedRoutineInput =
                serde_json::from_slice(&attempt.request.invocation.args)?;
            let input = resolved.input;
            if self
                .pool_rights(&attempt.request.declaration, resolved.publisher)
                .await?
                != Some(true)
                || self.current_rights(&input, resolved.publisher).await? != Some(true)
            {
                drop(admission);
                continue;
            }
            let current = self
                .rt
                .dispatch_repo
                .get_any(&attempt.dispatch_id)
                .await
                .ok_or_eyre("prepared dispatch vanished")?;
            if current.status.is_terminal() {
                continue;
            }
            let ActiveDispatchDeets::Wflow {
                entry_id,
                plug_id,
                bundle_name,
                ..
            } = &current.deets;
            // Prefer the retained JobInit. Otherwise sample the live reservation
            // prefix before proving absence. Append errors surface from their
            // owning admission; newer live holes are never silently skipped.
            let prefix = match entry_id {
                Some(entry) => *entry,
                None => self.rt.wcx.logstore.latest_idx().await?,
            };
            self.rt.wflow_part_state.wait_for_prefix(prefix).await?;
            let job = attempt
                .job_id
                .as_deref()
                .ok_or_eyre("native task has no retained job identity")?;
            let retained = {
                let state = self.rt.wflow_part_state.read_jobs().await;
                state
                    .active
                    .get(job)
                    .or_else(|| state.archive.get(job))
                    .map(|job| job.init_entry_id)
            };
            if let Err(error) = ensure_bundle_workload_running(
                &self.rt.wash_host,
                &self.rt.blobs_repo,
                plug_id,
                bundle_name,
                &current.execution,
            )
            .await
            {
                drop(admission);
                Self::fail_local(
                    &self.rt,
                    &attempt,
                    &format!("captured component refused: {error:#}"),
                )
                .await?;
                return Err(error);
            }
            if let Some(entry) = retained {
                self.rt
                    .record_job_admission(&attempt.dispatch_id, &current, entry)
                    .await?;
            } else {
                self.rt
                    .start_active_dispatch(&attempt.dispatch_id, &current)
                    .await?;
                let admitted = self
                    .rt
                    .dispatch_repo
                    .get_any(&attempt.dispatch_id)
                    .await
                    .ok_or_eyre("admitted dispatch vanished")?;
                let ActiveDispatchDeets::Wflow { entry_id, .. } = &admitted.deets;
                let entry = entry_id.ok_or_eyre("start did not durably retain its JobInit")?;
                self.rt.wflow_part_state.wait_for_prefix(entry).await?;
            }
            if attempt
                .request
                .declaration
                .not_after_secs
                .is_some_and(|deadline| jiff::Timestamp::now().as_second() > deadline)
                || self
                    .pool_rights(&attempt.request.declaration, resolved.publisher)
                    .await?
                    != Some(true)
                || self.current_rights(&input, resolved.publisher).await? != Some(true)
            {
                drop(admission);
                continue;
            }
            self.rt.execution_gate.release(job);
            return Ok(subscription);
        }
    }

    fn subscription(&self, attempt: PreparedAttempt) -> AttemptSubscription {
        let adapter = self.clone();
        let events = self.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
        Box::pin(async move {
            loop {
                if let Some(outcome) = adapter.outcome(&attempt).await? {
                    return Ok(outcome);
                }
                match events.recv_async().await {
                    Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
                    Err(crate::repos::RecvError::Closed) => {
                        eyre::bail!("native dispatch outcome listener closed");
                    }
                }
            }
        })
    }
}

impl TaskExecutionAdapter for CapturedRoutineAdapter {
    fn classify(&self, ticket: TaskTicket) -> AdapterFuture<TaskClassification> {
        let adapter = self.clone();
        Box::pin(async move {
            if ticket.task_id != ticket.declaration.declaration.task_id {
                return Ok(TaskClassification::Invalid);
            }
            if adapter
                .rt
                .dispatch_repo
                .task_receipt(
                    &ticket.task_id,
                    ticket.declaration.declaration.canonical_digest(),
                )
                .await?
                .is_some()
            {
                return Ok(TaskClassification::Obsolete);
            }
            adapter
                .classify_declaration(&ticket.declaration.declaration)
                .await
        })
    }

    fn persist(&self, request: AttemptRequest) -> AdapterFuture<PreparedAttempt> {
        let adapter = self.clone();
        Box::pin(async move { adapter.persist_inner(request).await })
    }

    fn start(&self, attempt: PreparedAttempt) -> AdapterFuture<AttemptSubscription> {
        let adapter = self.clone();
        Box::pin(async move { adapter.start_inner(attempt).await })
    }

    fn cancel(&self, key: AttemptKey, _reason: OpaqueReason) -> AdapterFuture<()> {
        let adapter = self.clone();
        Box::pin(async move {
            if let Some(attempt) = adapter.rt.dispatch_repo.task_attempt(&key).await? {
                Self::fail_local(&adapter.rt, &attempt, _reason.as_str()).await?;
            }
            Ok(())
        })
    }

    fn incorporate(
        &self,
        declaration: TaskDeclaration,
        summary: TerminalSummary,
        attempt: Option<AttemptKey>,
    ) -> AdapterFuture<DomainReceipt> {
        let adapter = self.clone();
        Box::pin(async move {
            if let Some(key) = attempt {
                eyre::ensure!(
                    key.task_id == declaration.task_id && key.pool == declaration.pool,
                    "terminal incorporation names a different task"
                );
                let local = adapter
                    .rt
                    .dispatch_repo
                    .task_attempt(&key)
                    .await?
                    .ok_or_eyre("terminal incorporation has no retained attempt")?;
                eyre::ensure!(
                    local.request.declaration_digest == declaration.canonical_digest(),
                    "terminal incorporation changes the retained declaration"
                );
                let own_success = matches!(summary, TerminalSummary::Succeeded { attempt_id, .. } if attempt_id == key.attempt_id);
                if own_success {
                    eyre::ensure!(
                        matches!(
                            adapter.outcome(&local).await?,
                            Some(AttemptOutcome::Finished(TerminalFact::Succeeded { .. }))
                        ),
                        "task success precedes actual native settlement"
                    );
                } else if adapter.outcome(&local).await?.is_none() {
                    Self::fail_local(
                        &adapter.rt,
                        &local,
                        "terminal task superseded this local attempt",
                    )
                    .await?;
                }
            }
            if matches!(summary, TerminalSummary::Succeeded { .. })
                && adapter.processor_domain.is_some()
            {
                let processor: ProcessorTaskInput<'_> = serde_json::from_slice(&declaration.input)?;
                // Task and slot parts may arrive in either order. A remote
                // terminal cannot become a domain receipt before the exact
                // durable incorporation proof is materialized locally.
                use big_sync::HostPartStore as _;
                loop {
                    let Some(store) = adapter
                        .rt
                        .processor_slot_store(
                            &processor.slot.processor_full_id,
                            Some(&processor.domain),
                        )
                        .await?
                    else {
                        adapter
                            .watch(Self::watch_for(&declaration, None))
                            .await?
                            .await?;
                        continue;
                    };
                    let parts = adapter.rt.rcx.coordination_part_store().await?;
                    let cursor = parts.latest_revision().await?;
                    let mut changes = parts
                        .open_revision_reader(big_sync_core::rpc::SubPartsRequest {
                            lower_bound: cursor,
                            targets: [big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: store.register().part().clone(),
                                cursor,
                            }]
                            .into(),
                        })
                        .await?
                        .map_err(|error| ferr!("processor settlement reader: {error:?}"))?;
                    if store
                        .slot(&processor.slot)
                        .await?
                        .settled(&processor.capture.generation)
                    {
                        break;
                    }
                    loop {
                        if matches!(
                            changes
                                .next(
                                    big_sync_core::revisioned_store::RevisionReadLimits::default()
                                )
                                .await?,
                            big_sync_core::revisioned_store::RevisionRead::Entries { .. }
                        ) {
                            break;
                        }
                    }
                }
            }
            adapter
                .rt
                .dispatch_repo
                .incorporate_task_receipt(
                    DomainReceipt {
                        task_id: declaration.task_id,
                        summary,
                    },
                    declaration.canonical_digest(),
                )
                .await
        })
    }

    fn watch(&self, watch: ReadinessWatch) -> AdapterFuture<AdapterFuture<()>> {
        let adapter = self.clone();
        Box::pin(async move {
            let (declaration, witness): (
                TaskDeclaration,
                Option<(Vec<u8>, crate::tasks::NodePubkey)>,
            ) = serde_json::from_str(watch.coordination_ref().as_str())?;
            let mut documents = adapter.rt.drawer.subscribe_metadata_events();
            let mut configuration = adapter.rt.plugs_repo.subscribe_events();
            let mut blobs = adapter.rt.blobs_repo.input_changes();
            let (change_ticket, mut changes) = adapter
                .rt
                .rcx
                .big_repo
                .subscribe_change_listener(big_repo::BigRepoChangeFilter {
                    doc_id: None,
                    origin: None,
                    path: vec![],
                })
                .await?;
            let (local_ticket, mut local) = adapter
                .rt
                .rcx
                .big_repo
                .subscribe_local_listener(big_repo::BigRepoLocalFilter { doc_id: None })
                .await?;
            let (domain_ticket, mut domain) = adapter
                .rt
                .rcx
                .big_repo
                .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
                .await?;
            let mut slot_changes = if let Some((processor, reference)) = &adapter.processor_domain {
                if let Some(store) = adapter
                    .rt
                    .processor_slot_store(processor, Some(reference))
                    .await?
                {
                    use big_sync::HostPartStore as _;
                    let parts = adapter.rt.rcx.coordination_part_store().await?;
                    let cursor = parts.latest_revision().await?;
                    Some(
                        parts
                            .open_revision_reader(big_sync_core::rpc::SubPartsRequest {
                                lower_bound: cursor,
                                targets: [big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: store.register().part().clone(),
                                    cursor,
                                }]
                                .into(),
                            })
                            .await?
                            .map_err(|error| ferr!("processor readiness reader: {error:?}"))?,
                    )
                } else {
                    None
                }
            } else {
                None
            };
            let ready: AdapterFuture<()> = Box::pin(async move {
                let _registrations = (change_ticket, local_ticket, domain_ticket);
                if !matches!(
                    match &witness {
                        Some((bytes, publisher)) =>
                            adapter
                                .classify_input(&declaration, bytes, *publisher)
                                .await?,
                        None => adapter.classify_declaration(&declaration).await?,
                    },
                    TaskClassification::NotReady(_) | TaskClassification::CoordinationIncomplete
                ) {
                    return Ok(());
                }
                let now = jiff::Timestamp::now().as_second();
                let wake_at = [
                    declaration.not_before_secs,
                    declaration
                        .not_after_secs
                        .map(|deadline| deadline.saturating_add(1)),
                ]
                .into_iter()
                .flatten()
                .filter(|deadline| *deadline > now)
                .min();
                let time = async move {
                    if let Some(wake_at) = wake_at {
                        tokio::time::sleep(Duration::from_secs(wake_at.saturating_sub(now) as u64))
                            .await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let slot_ready = async {
                    let Some(reader) = &mut slot_changes else {
                        return std::future::pending::<Res<()>>().await;
                    };
                    loop {
                        if matches!(
                            reader
                                .next(
                                    big_sync_core::revisioned_store::RevisionReadLimits::default()
                                )
                                .await?,
                            big_sync_core::revisioned_store::RevisionRead::Entries { .. }
                        ) {
                            return Ok(());
                        }
                    }
                };
                tokio::select! {
                    event = documents.recv() => {
                        match event {
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => eyre::bail!("native metadata readiness listener closed"),
                        }
                    }
                    event = configuration.recv() => {
                        match event {
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => eyre::bail!("native configuration readiness listener closed"),
                        }
                    }
                    event = blobs.changed() => { event?; }
                    event = changes.recv() => { event.ok_or_eyre("native document readiness listener closed")?; }
                    event = local.recv() => { event.ok_or_eyre("native local readiness listener closed")?; }
                    event = domain.recv() => { event.ok_or_eyre("native authority readiness listener closed")?; }
                    result = slot_ready => { result?; }
                    _ = time => {}
                }
                Ok(())
            });
            Ok(ready)
        })
    }

    fn recover(&self, pool: TaskPoolId) -> AdapterFuture<Vec<RecoveredAttempt>> {
        let adapter = self.clone();
        Box::pin(async move {
            let mut recovered = Vec::new();
            for attempt in adapter.rt.dispatch_repo.task_attempts(&pool).await? {
                if let Some(outcome) = adapter.outcome(&attempt).await? {
                    recovered.push(RecoveredAttempt::Finishing { attempt, outcome });
                    continue;
                }
                match adapter.classify_attempt(&attempt.request).await? {
                    TaskClassification::Runnable(_) => {
                        let job = attempt
                            .job_id
                            .as_deref()
                            .ok_or_eyre("native prepared attempt has no workflow identity")?;
                        let admitted = adapter
                            .rt
                            .wflow_part_state
                            .read_jobs()
                            .await
                            .active
                            .contains_key(job);
                        if !admitted {
                            recovered.push(RecoveredAttempt::Prepared(attempt));
                            continue;
                        }
                        let admission = adapter.rt.task_admission.lock().await;
                        let dispatch = adapter
                            .rt
                            .dispatch_repo
                            .get_any(&attempt.dispatch_id)
                            .await
                            .ok_or_eyre("retained recovery dispatch vanished")?;
                        let ActiveDispatchDeets::Wflow {
                            plug_id,
                            bundle_name,
                            ..
                        } = &dispatch.deets;
                        if let Err(error) = ensure_bundle_workload_running(
                            &adapter.rt.wash_host,
                            &adapter.rt.blobs_repo,
                            plug_id,
                            bundle_name,
                            &dispatch.execution,
                        )
                        .await
                        {
                            drop(admission);
                            Self::fail_local(
                                &adapter.rt,
                                &attempt,
                                &format!("retained component refused: {error:#}"),
                            )
                            .await?;
                            let outcome = adapter
                                .outcome(&attempt)
                                .await?
                                .ok_or_eyre("failed recovery attempt was not settled")?;
                            recovered.push(RecoveredAttempt::Finishing { attempt, outcome });
                            continue;
                        }
                        let resolved: ResolvedRoutineInput =
                            serde_json::from_slice(&attempt.request.invocation.args)?;
                        let input = resolved.input;
                        if adapter
                            .pool_rights(&attempt.request.declaration, resolved.publisher)
                            .await?
                            == Some(true)
                            && adapter.current_rights(&input, resolved.publisher).await?
                                == Some(true)
                        {
                            adapter.rt.execution_gate.release(job);
                            recovered.push(RecoveredAttempt::Running(attempt));
                        } else {
                            // A fresh permission race keeps the exact preparation
                            // held; start owns its readiness and final admission.
                            recovered.push(RecoveredAttempt::Prepared(attempt));
                        }
                    }
                    TaskClassification::NotReady(_)
                    | TaskClassification::CoordinationIncomplete => {
                        recovered.push(RecoveredAttempt::Prepared(attempt));
                    }
                    TaskClassification::Invalid | TaskClassification::Obsolete => {
                        Self::fail_local(
                            &adapter.rt,
                            &attempt,
                            "retained command refused during recovery",
                        )
                        .await?;
                        let outcome = adapter
                            .outcome(&attempt)
                            .await?
                            .ok_or_eyre("refused native attempt was not settled")?;
                        recovered.push(RecoveredAttempt::Finishing { attempt, outcome });
                    }
                }
            }
            Ok(recovered)
        })
    }

    fn is_proven_obsolete(&self, declaration: TaskDeclaration) -> AdapterFuture<bool> {
        let adapter = self.clone();
        Box::pin(async move {
            if adapter
                .rt
                .dispatch_repo
                .task_receipt(&declaration.task_id, declaration.canonical_digest())
                .await?
                .is_some()
            {
                return Ok(true);
            }
            Ok(matches!(
                adapter.classify_declaration(&declaration).await?,
                TaskClassification::Obsolete
            ))
        })
    }
}

pub(crate) fn processor_policy(
    policy: &manifest::DistributedProcessorPolicy,
    origin: Option<crate::tasks::NodePubkey>,
) -> Res<(Preference, EffectPolicy)> {
    let placement = match &policy.placement {
        manifest::ProcessorPlacement::AnyNode => Preference::AnyNode,
        manifest::ProcessorPlacement::PreferOrigin => origin
            .map(Preference::PreferOrigin)
            .unwrap_or(Preference::AnyNode),
        manifest::ProcessorPlacement::Only(node) => Preference::Only(
            crate::tasks::NodePubkey::new(*node.parse::<iroh::PublicKey>()?.as_bytes()),
        ),
    };
    let effects = match policy.duplicates {
        manifest::ProcessorDuplicatePolicy::Idempotent => EffectPolicy::Idempotent,
        manifest::ProcessorDuplicatePolicy::ExternalIdempotencyKey => {
            EffectPolicy::ExternalIdempotencyKey
        }
        manifest::ProcessorDuplicatePolicy::AuthoritativePlacement => {
            EffectPolicy::AuthoritativePlacement
        }
        manifest::ProcessorDuplicatePolicy::AcceptsDuplicates => EffectPolicy::AcceptsDuplicates,
    };
    Ok((placement, effects))
}
fn encoded_eq<A: serde::Serialize + ?Sized, B: serde::Serialize + ?Sized>(
    left: &A,
    right: &B,
) -> Res<bool> {
    Ok(serde_json::to_vec(left)? == serde_json::to_vec(right)?)
}

fn summary(fact: &TerminalFact) -> TerminalSummary {
    match fact {
        TerminalFact::Succeeded {
            attempt_id,
            result_ref,
        } => TerminalSummary::Succeeded {
            attempt_id: *attempt_id,
            result_ref: result_ref.clone(),
        },
        TerminalFact::Cancelled { reason } => TerminalSummary::Cancelled {
            reason: reason.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_source_origin_does_not_prefer_the_observer() -> Res<()> {
        let mut policy = manifest::DistributedProcessorPolicy {
            placement: manifest::ProcessorPlacement::PreferOrigin,
            duplicates: manifest::ProcessorDuplicatePolicy::Idempotent,
        };
        let source = crate::tasks::NodePubkey::new([7; 32]);
        assert_eq!(processor_policy(&policy, None)?.0, Preference::AnyNode);
        assert_eq!(
            processor_policy(&policy, Some(source))?.0,
            Preference::PreferOrigin(source)
        );
        let restricted = iroh::SecretKey::from_bytes(&[8; 32]).public();
        policy.placement = manifest::ProcessorPlacement::Only(restricted.to_string());
        assert_eq!(
            processor_policy(&policy, None)?.0,
            Preference::Only(crate::tasks::NodePubkey::new(*restricted.as_bytes()))
        );
        Ok(())
    }
}
