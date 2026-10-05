pub mod domain;
pub mod slots;

use crate::interlude::*;

use crate::index::facet_delta::{FacetDelta, FacetRouteKey, FacetSnapshot};
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::plugs::PlugsRepo;
use crate::rt::dispatch::DispatchOnSuccessHook;
use crate::rt::{DispatchArgs, Rt};
use big_sync::DeltaWalkerStateRepo as _;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use daybook_types::doc::{BranchId, BranchPathBuf, ChangeHashSet, Doc, DocId, FacetKey};

use daybook_types::manifest::{
    ChangeOriginDeets, DocChangeKind, DocPredicateEvalMode, DocPredicateEvalRequirement,
    DocPredicateEvalResolved, FacetReferenceManifest, KeyGeneric, NodePredicate, ProcessorDeets,
    ProcessorEventPredicate, ProcessorManifest,
};
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

/// Installed configuration, not upstream indexing catch-up or historical processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationAck {
    pub target: crate::plugs::PlugActivationTarget,
    pub processors: BTreeMap<String, [u8; 32]>,
    pub source_boundary: u64,
    pub source_policy: SourceConsumptionPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceConsumptionPolicy {
    FutureObservedDeltasOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationStatus {
    Pending,
    Active(ActivationAck),
    Disabled,
    Rejected(String),
    Superseded(crate::plugs::PlugActivationTarget),
}

#[derive(Clone)]
pub struct TriageWorker {
    state: tokio::sync::watch::Sender<HashMap<String, ActivationAck>>,
    stopped: CancellationToken,
    coordination_ready: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    wait_query_gate: Arc<tokio::sync::Mutex<Option<super::DispatchTestGate>>>,
    #[cfg(test)]
    install_gate: Arc<tokio::sync::Mutex<Option<(String, super::DispatchTestGate)>>>,
    #[cfg(test)]
    source_gate: Arc<tokio::sync::Mutex<Option<SourceTestGate>>>,
}

impl TriageWorker {
    pub(crate) fn new() -> Self {
        Self {
            state: tokio::sync::watch::channel(HashMap::new()).0,
            stopped: CancellationToken::new(),
            coordination_ready: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            wait_query_gate: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(test)]
            install_gate: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(test)]
            source_gate: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub(crate) fn wake_coordination(&self) {
        self.coordination_ready.notify_one();
    }

    pub async fn query_activation(
        &self,
        plugs: &PlugsRepo,
        target: &crate::plugs::PlugActivationTarget,
    ) -> Res<ActivationStatus> {
        eyre::ensure!(!self.stopped.is_cancelled(), "triage worker stopped");
        let desired = plugs.processor_activation_snapshot().await?;
        if let Some(reason) = desired.rejected.get(&target.plug_id) {
            return Ok(ActivationStatus::Rejected(reason.clone()));
        }
        let Some(current) = desired.targets.get(&target.plug_id) else {
            return Ok(ActivationStatus::Disabled);
        };
        if current != target {
            return Ok(ActivationStatus::Superseded(current.clone()));
        }
        Ok(match self.state.borrow().get(&target.plug_id) {
            Some(ack) if &ack.target == target => ActivationStatus::Active(ack.clone()),
            Some(_) | None => ActivationStatus::Pending,
        })
    }

    pub async fn wait_for_activation(
        &self,
        plugs: &PlugsRepo,
        target: crate::plugs::PlugActivationTarget,
    ) -> Res<ActivationStatus> {
        let mut changes = self.state.subscribe();
        #[cfg(test)]
        let gate = self.wait_query_gate.lock().await.take();
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.reached
                .send(())
                .map_err(|_| ferr!("activation query barrier closed"))?;
            gate.resume.await?;
        }
        loop {
            let status = self.query_activation(plugs, &target).await?;
            if status != ActivationStatus::Pending {
                return Ok(status);
            }
            tokio::select! {
                _ = self.stopped.cancelled() => return Err(ferr!("triage worker stopped")),
                result = changes.changed() => result.map_err(|_| ferr!("triage worker closed"))?,
            }
        }
    }
}

#[cfg(test)]
struct SourceTestGate {
    facet_key: FacetKey,
    selected: tokio::sync::mpsc::UnboundedSender<DocId>,
    resume: tokio::sync::mpsc::UnboundedReceiver<()>,
    settled: tokio::sync::mpsc::UnboundedSender<bool>,
    defer_once: bool,
}

pub(crate) struct DistributedProcessor {
    pub domain: domain::ProcessorDomainReference,
    pub driver: crate::tasks::driver::PoolDriverHandle,
    pub pool: crate::tasks::PoolDescriptorSnapshot,
}

impl Rt {
    pub(crate) async fn processor_slot_store(
        &self,
        processor: &str,
        domain: Option<&domain::ProcessorDomainReference>,
    ) -> Res<Option<Arc<slots::ProcessorSlotStore>>> {
        let Some(reference) = domain else {
            return Ok(Some(Arc::clone(&self.processor_slots)));
        };
        let repo = domain::ProcessorDomainRepo::new(
            Arc::clone(&self.rcx.big_repo),
            self.rcx.local_actor_id.clone(),
        );
        let snapshot = match repo.load(reference, processor).await? {
            domain::ProcessorDomainLoad::Ready(snapshot) => snapshot,
            domain::ProcessorDomainLoad::Pending { .. } => return Ok(None),
            domain::ProcessorDomainLoad::Rejected(error) => return Err(error.into()),
        };
        Ok(Some(self.rcx.attach_processor_slot_store(&snapshot).await?))
    }
}

impl crate::repo::RepoCtx {
    pub(crate) async fn attach_processor_slot_store(
        &self,
        snapshot: &domain::ProcessorDomainSnapshot,
    ) -> Res<Arc<slots::ProcessorSlotStore>> {
        let mut stores = self.processor_slot_stores.lock().await;
        let binding = snapshot.register_binding(&self.big_repo).await?;
        if let Some((bound, store)) = stores.get(&snapshot.processor_full_id) {
            eyre::ensure!(
                bound == &snapshot.reference,
                "processor authority binding changed; explicit migration is required"
            );
            store.register().refresh_publication_key(&binding).await?;
            return Ok(Arc::clone(store));
        }
        let register = crate::tasks::storage::RegisterStore::open(
            self.coordination_part_store().await?,
            Arc::clone(&self.big_repo),
            binding,
        )
        .await?;
        let store = Arc::new(slots::ProcessorSlotStore::distributed(register));
        stores.insert(
            snapshot.processor_full_id.clone(),
            (snapshot.reference.clone(), Arc::clone(&store)),
        );
        Ok(store)
    }
}

struct PreparedProcessor {
    processor_full_id: String,
    plug_id: String,
    routine_name: KeyGeneric,
    processor_manifest: Arc<ProcessorManifest>,
    plug_manifest: Arc<daybook_types::manifest::PlugManifest>,
    event_predicate: ProcessorEventPredicate,
    /// Tag-level: any facet with this tag counts as read.
    read_tags: HashSet<String>,
    /// Key-level: only this tag+id counts as read.
    read_keys: HashSet<FacetKey>,
    source_boundary: u64,
}

struct DocProcessorTriageListener {
    rt: Arc<Rt>,
    cached_processors: Vec<PreparedProcessor>,
    triage_read_tags: HashSet<String>,
    triage_read_keys: HashSet<FacetKey>,
    facet_reference_specs: Arc<HashMap<String, Vec<FacetReferenceManifest>>>,
    predicate_requirements: HashSet<DocPredicateEvalRequirement>,
    predicate_resolved: HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessorDispatchPlan {
    slot: slots::ProcessorSlotKey,
    capture: slots::ProcessorCapture,
    plug_id: String,
    routine_name: String,
    dispatch_id: String,
    input: crate::rt::task_adapter::CapturedRoutineInput,
    distributed_policy: Option<daybook_types::manifest::DistributedProcessorPolicy>,
    #[serde(default)]
    source_origin: Option<crate::tasks::NodePubkey>,
}

impl DocProcessorTriageListener {
    #[tracing::instrument(skip_all)]
    async fn refresh_processors(
        &mut self,
        plugs: Vec<Arc<daybook_types::manifest::PlugManifest>>,
        boundaries: &HashMap<String, ActivationAck>,
    ) -> Res<()> {
        self.cached_processors.clear();
        let mut triage_read_tags = HashSet::new();
        let mut triage_read_keys = HashSet::new();
        let mut facet_reference_specs: HashMap<String, Vec<FacetReferenceManifest>> =
            HashMap::new();
        for plug in plugs {
            let plug_id = plug.id();
            for facet in &plug.facets {
                if facet.references.is_empty() {
                    continue;
                }
                facet_reference_specs
                    .entry(facet.key_tag.to_string())
                    .or_default()
                    .extend(facet.references.iter().cloned());
            }
            for (processor_name, processor) in &plug.processors {
                match &processor.deets {
                    ProcessorDeets::DocProcessor {
                        event_predicate,
                        predicate,
                        routine_name,
                    } => {
                        let routine = plug.routines.get(routine_name).ok_or_else(|| {
                            ferr!(
                                "routine {} not found in plug {} manifest",
                                routine_name,
                                plug_id
                            )
                        })?;
                        let mut read_tags: HashSet<String> = predicate
                            .referenced_tags()
                            .iter()
                            .map(|tag| tag.0.clone())
                            .collect();
                        let mut read_keys: HashSet<FacetKey> = HashSet::new();
                        event_predicate
                            .doc_change_predicate
                            .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
                        let (acl_tags, acl_keys) = routine.read_facet_set();
                        read_tags.extend(acl_tags);
                        read_keys.extend(acl_keys);
                        triage_read_tags.extend(read_tags.iter().cloned());
                        triage_read_keys.extend(read_keys.iter().cloned());
                        self.cached_processors.push(PreparedProcessor {
                            processor_full_id: format!("{plug_id}/{processor_name}"),
                            plug_id: plug_id.clone(),
                            routine_name: routine_name.clone(),
                            processor_manifest: Arc::clone(processor),
                            plug_manifest: Arc::clone(&plug),
                            event_predicate: event_predicate.clone(),
                            read_tags,
                            read_keys,
                            source_boundary: boundaries[&plug_id].source_boundary,
                        });
                    }
                }
            }
        }
        self.triage_read_tags = triage_read_tags;
        self.triage_read_keys = triage_read_keys;
        self.facet_reference_specs = Arc::new(facet_reference_specs);
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, doc, doc_heads))]
    async fn plan_doc(
        &mut self,
        doc_id: &DocId,
        doc_heads: &ChangeHashSet,
        doc: &Doc,
        branch_path: daybook_types::doc::BranchPathBuf,
        change_kind: DocChangeKind,
        changed_facet_keys: Option<&HashSet<FacetKey>>,
        added_facet_keys: Option<&HashSet<FacetKey>>,
        removed_facet_keys: Option<&HashSet<FacetKey>>,
        local_changed_facet_keys: Option<&HashSet<FacetKey>>,
        source_revision: u64,
    ) -> Res<Option<Vec<ProcessorDispatchPlan>>> {
        let rt = &self.rt;
        debug!(
            processor_count = self.cached_processors.len(),
            "triaging doc"
        );
        let mut plans = Vec::new();
        let mut full_doc_for_reference_predicates: Option<Option<Arc<Doc>>> = None;
        for processor in &self.cached_processors {
            if source_revision <= processor.source_boundary {
                continue;
            }
            let is_local_for_processor = local_changed_facet_keys
                .map(|changed| {
                    changed_intersects_read_set(changed, &processor.read_tags, &processor.read_keys)
                })
                .unwrap_or(false);
            if change_kind != DocChangeKind::Deleted
                && !should_processor_run_for_event(
                    &processor.event_predicate.node_predicate,
                    &processor.event_predicate.doc_change_predicate,
                    is_local_for_processor,
                    change_kind,
                    changed_facet_keys,
                    added_facet_keys,
                    removed_facet_keys,
                    &processor.read_tags,
                    &processor.read_keys,
                )
            {
                continue;
            }
            let slot_key = slots::ProcessorSlotKey {
                document_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                processor_full_id: processor.processor_full_id.clone(),
            };
            let distributed = match &processor.processor_manifest.coordination {
                daybook_types::manifest::ProcessorCoordination::PerNode => None,
                daybook_types::manifest::ProcessorCoordination::Distributed(_) => {
                    let Some(managed) = rt
                        .distributed_processors
                        .read()
                        .await
                        .get(&processor.processor_full_id)
                        .cloned()
                    else {
                        return Ok(None);
                    };
                    Some(managed)
                }
            };
            let refreshed = if let Some(managed) = &distributed {
                let Some(store) = rt
                    .processor_slot_store(&processor.processor_full_id, Some(&managed.domain))
                    .await?
                else {
                    return Ok(None);
                };
                Some(store)
            } else {
                None
            };
            let store = refreshed.as_deref().unwrap_or(rt.processor_slots.as_ref());
            let slot = store.slot(&slot_key).await?;
            if change_kind == DocChangeKind::Deleted {
                // Deletion invalidates observed work regardless of the execution
                // trigger or origin filter. No source/configuration invocation is
                // prepared: the false receipt retains an observed fingerprint and
                // causally supersedes all desires visible in this slot.
                if let Some(desired) = slot.desired().next() {
                    let capture = slots::ProcessorCapture::new(
                        ChangeHashSet::default(),
                        desired.capture.execution_baseline.clone(),
                        desired.capture.processor_generation,
                        desired.capture.configuration_generation,
                    );
                    store
                        .evaluate(
                            &slot_key,
                            slots::ProcessorDesired {
                                capture,
                                matches: false,
                            },
                        )
                        .await?;
                }
                continue;
            }
            let predicate = match &processor.processor_manifest.deets {
                ProcessorDeets::DocProcessor { predicate, .. } => predicate,
            };
            self.predicate_requirements.clear();
            predicate.append_requirements(&mut self.predicate_requirements);

            let needs_full_doc = self.predicate_requirements.iter().any(|req| {
                matches!(
                    req,
                    DocPredicateEvalRequirement::FullDoc
                        | DocPredicateEvalRequirement::FacetsOfTag(_)
                )
            });

            let predicate_doc_arc = if needs_full_doc {
                if full_doc_for_reference_predicates.is_none() {
                    full_doc_for_reference_predicates = Some(
                        rt.drawer
                            .get_if_latest(doc_id, &branch_path, doc_heads, None)
                            .await
                            .wrap_err("error loading full doc for reference predicate")?,
                    );
                }
                match full_doc_for_reference_predicates
                    .as_ref()
                    .and_then(|opt| opt.as_ref())
                {
                    Some(doc) => Some(doc),
                    None => return Ok(None),
                }
            } else {
                None
            };
            // Key predicates use complete source-head metadata. Predicates that
            // inspect values use the materialized document snapshot above.
            let predicate_doc = predicate_doc_arc.map(|doc| doc.as_ref()).unwrap_or(doc);

            self.predicate_resolved.clear();
            for requirement in &self.predicate_requirements {
                match requirement {
                    DocPredicateEvalRequirement::FullDoc => {
                        let predicate_doc_arc = predicate_doc_arc
                            .expect("FullDoc requirement implies full doc was loaded and cached");
                        self.predicate_resolved.insert(
                            requirement.clone(),
                            DocPredicateEvalResolved::FullDoc(Arc::clone(predicate_doc_arc)),
                        );
                    }
                    DocPredicateEvalRequirement::FacetsOfTag(tag) => {
                        let facets = predicate_doc
                            .facets
                            .iter()
                            .filter(|(facet_key, _)| facet_key.tag.to_string() == tag.0)
                            .map(|(facet_key, facet_raw)| (facet_key.clone(), facet_raw.clone()))
                            .collect();
                        self.predicate_resolved.insert(
                            requirement.clone(),
                            DocPredicateEvalResolved::FacetsOfTag(facets),
                        );
                    }
                    DocPredicateEvalRequirement::FacetManifest => {
                        self.predicate_resolved.insert(
                            requirement.clone(),
                            DocPredicateEvalResolved::FacetManifest(Arc::clone(
                                &self.facet_reference_specs,
                            )),
                        );
                    }
                }
            }

            let predicate_match = predicate.evaluate(
                predicate_doc,
                DocPredicateEvalMode::Exact,
                &self.predicate_resolved,
            );
            let baseline = match processor.processor_manifest.input {
                daybook_types::manifest::ProcessorInput::Snapshot => None,
                daybook_types::manifest::ProcessorInput::Delta => slot.execution_baseline(),
            };
            let changed = match processor.processor_manifest.input {
                // Snapshot changed keys describe complete captured membership,
                // not replica-local delta coalescing. Other native capture
                // witnesses are retained separately in the prepared invocation.
                daybook_types::manifest::ProcessorInput::Snapshot => {
                    doc.facets.keys().cloned().collect::<HashSet<_>>()
                }
                daybook_types::manifest::ProcessorInput::Delta => {
                    let Some(changed) = rt
                        .drawer
                        .facet_keys_changed_between_branch_heads(
                            doc_id,
                            &branch_path,
                            baseline.as_ref(),
                            doc_heads,
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    changed
                }
            };
            let changed: Vec<_> = changed
                .into_iter()
                .filter(|key| {
                    processor.read_tags.contains(&key.tag.to_string())
                        || processor.read_keys.contains(key)
                })
                .collect();
            let source_origin = if distributed.is_some() && predicate_match {
                let Some(origin) = rt
                    .drawer
                    .facet_source_origin_at_heads(doc_id, &branch_path, doc_heads, &changed)
                    .await?
                else {
                    return Ok(None);
                };
                origin
            } else {
                None
            };
            let mut changed_facet_keys: Vec<_> =
                changed.into_iter().map(|key| key.to_string()).collect();
            changed_facet_keys.sort_unstable();
            let args = DispatchArgs::DocRoutine {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: doc_heads.clone(),
                invocation: crate::rt::dispatch::RoutineInvocation::Processor(
                    crate::rt::dispatch::ProcessorInvocation {
                        trigger_doc_id: doc_id.clone(),
                        changed_facet_keys: changed_facet_keys.clone(),
                        task_id: None,
                    },
                ),
                changed_facet_keys,
                wflow_args_json: None,
            };
            let mut prepared = rt
                .prepare_routine(&processor.plug_id, &processor.routine_name.0, args, None)
                .await?;
            let crate::rt::task_adapter::CapturedRoutineInput::V1 {
                args,
                execution,
                configuration_document,
                configuration_heads,
                ..
            } = &prepared.input;
            let crate::rt::dispatch::CapturedWflowExecution::V1 {
                component_blobs,
                manifest_doc_id,
                manifest_heads,
                ..
            } = execution;
            let Some(captured_manifest) = rt
                .plugs_repo
                .read_manifest_doc(manifest_doc_id, manifest_heads)
                .await?
            else {
                return Ok(None);
            };
            // Activation may change while preparation awaits native documents. Never
            // combine the evaluator's old policy with a newly captured artifact.
            if serde_json::to_value(processor.plug_manifest.as_ref())?
                != serde_json::to_value(&captured_manifest)?
            {
                return Ok(None);
            }
            let processor_generation = semantic_generation(
                &(&captured_manifest, component_blobs),
                b"daybook/processor-artifact/v1",
            )?;
            let crate::rt::dispatch::ActiveDispatchArgs::FacetRoutine(facet_args) = args;
            let mut config_values = BTreeMap::new();
            let mut configuration = BTreeMap::new();
            for config in &facet_args.config_docs {
                configuration.insert(config.doc_id.clone(), config.heads.clone());
                let Some(doc) = rt
                    .drawer
                    .get_doc_with_facets_at_branch_heads(
                        &config.doc_id,
                        &config.branch_path,
                        &config.heads,
                        None,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                config_values.insert(config.doc_id.clone(), doc.facets.clone());
            }
            let Some(bindings) = rt
                .plugs_repo
                .config_bindings_at(configuration_document, configuration_heads)
                .await?
            else {
                return Ok(None);
            };
            let own_config = bindings
                .get(&processor.plug_id)
                .cloned()
                .ok_or_eyre("enabled processor has no captured configuration association")?;
            if let std::collections::btree_map::Entry::Vacant(entry) =
                config_values.entry(own_config)
            {
                let Some(heads) = rt
                    .drawer
                    .get_branch_heads_for_path(entry.key(), &BranchPathBuf::from("main"))
                    .await?
                else {
                    return Ok(None);
                };
                let Some(doc) = rt
                    .drawer
                    .get_doc_with_facets_at_branch_heads(
                        entry.key(),
                        &BranchPathBuf::from("main"),
                        &heads,
                        None,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                configuration.insert(entry.key().clone(), heads);
                entry.insert(doc.facets.clone());
            }
            let capture = slots::ProcessorCapture::new(
                doc_heads.clone(),
                baseline,
                processor_generation,
                semantic_generation(&config_values, b"daybook/processor-configuration/v1")?,
            );

            if predicate_match
                && slot.settlements().any(|settlement| {
                    settlement.capture.heads == capture.heads
                        && settlement.capture.processor_generation == capture.processor_generation
                        && settlement.capture.configuration_generation
                            == capture.configuration_generation
                })
            {
                continue;
            }
            store
                .evaluate(
                    &slot_key,
                    slots::ProcessorDesired {
                        capture: capture.clone(),
                        matches: predicate_match,
                    },
                )
                .await?;
            if !predicate_match || slot.settled(&capture.generation) {
                continue;
            }
            let mut hash = blake3::Hasher::new();
            hash.update(b"daybook/processor-task/v1\0");
            hash.update(&slot_key.id());
            hash.update(&capture.generation);
            let dispatch_id =
                utils_rs::hash::blake3_hash_bytes_multibase(hash.finalize().as_bytes());
            if let Some(distributed) = &distributed {
                let crate::rt::task_adapter::CapturedRoutineInput::V1 {
                    processor: input,
                    pool_binding,
                    args,
                    ..
                } = &mut prepared.input;
                *input = Some(crate::rt::task_adapter::CapturedProcessorInput {
                    slot: slot_key.clone(),
                    capture: capture.clone(),
                    domain: distributed.domain.clone(),
                    configuration,
                });
                *pool_binding = Some(crate::rt::task_adapter::CapturedPoolBinding::from_snapshot(
                    &distributed.pool,
                ));
                let crate::rt::dispatch::ActiveDispatchArgs::FacetRoutine(args) = args;
                let crate::rt::dispatch::RoutineInvocation::Processor(invocation) =
                    &mut args.invocation
                else {
                    unreachable!("prepared processor invocation")
                };
                invocation.task_id =
                    Some(crate::tasks::PoolTaskId::new(*hash.finalize().as_bytes()).to_string());
            }
            plans.push(ProcessorDispatchPlan {
                slot: slot_key,
                capture,
                dispatch_id,
                input: prepared.input,
                plug_id: processor.plug_id.clone(),
                routine_name: processor.routine_name.0.clone(),
                source_origin,
                distributed_policy: match &processor.processor_manifest.coordination {
                    daybook_types::manifest::ProcessorCoordination::PerNode => None,
                    daybook_types::manifest::ProcessorCoordination::Distributed(policy) => {
                        Some(policy.clone())
                    }
                },
            });
        }
        Ok(Some(plans))
    }
}

/// Returns true if any changed key matches this processor's read set (by tag or by full key).
fn changed_intersects_read_set(
    changed: &HashSet<FacetKey>,
    read_tags: &HashSet<String>,
    read_keys: &HashSet<FacetKey>,
) -> bool {
    changed
        .iter()
        .any(|key| read_tags.contains(&key.tag.to_string()) || read_keys.contains(key))
}

#[expect(clippy::too_many_arguments)]
fn should_processor_run_for_event(
    node_predicate: &NodePredicate,
    doc_change_predicate: &daybook_types::manifest::DocChangePredicate,
    is_local_for_processor: bool,
    change_kind: DocChangeKind,
    changed_facet_keys: Option<&HashSet<FacetKey>>,
    added_facet_keys: Option<&HashSet<FacetKey>>,
    removed_facet_keys: Option<&HashSet<FacetKey>>,
    read_tags: &HashSet<String>,
    read_keys: &HashSet<FacetKey>,
) -> bool {
    if !evaluate_node_predicate(node_predicate, is_local_for_processor) {
        return false;
    }
    if !doc_change_predicate.evaluate_change(
        change_kind,
        changed_facet_keys,
        added_facet_keys,
        removed_facet_keys,
    ) {
        return false;
    }
    let mut all_changed = HashSet::new();
    if let Some(changed) = changed_facet_keys {
        all_changed.extend(changed.iter().cloned());
    }
    if let Some(added) = added_facet_keys {
        all_changed.extend(added.iter().cloned());
    }
    if let Some(removed) = removed_facet_keys {
        all_changed.extend(removed.iter().cloned());
    }
    if !all_changed.is_empty() && !changed_intersects_read_set(&all_changed, read_tags, read_keys) {
        return false;
    }
    true
}

fn evaluate_node_predicate(predicate: &NodePredicate, is_local_for_processor: bool) -> bool {
    match predicate {
        NodePredicate::ChangeOrigin(ChangeOriginDeets::Local) => is_local_for_processor,
    }
}

async fn enqueue_processor_plan(rt: &Rt, plan: &ProcessorDispatchPlan) -> Res<()> {
    if let Some(policy) = &plan.distributed_policy {
        let managed = rt
            .distributed_processors
            .read()
            .await
            .get(&plan.slot.processor_full_id)
            .cloned()
            .ok_or_eyre("captured distributed processor is no longer attached")?;
        let origin = plan.source_origin;
        let (placement, effect_policy) = crate::rt::task_adapter::processor_policy(policy, origin)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"daybook/processor-task/v1\0");
        hash.update(&plan.slot.id());
        hash.update(&plan.capture.generation);
        let coordination =
            crate::tasks::DomainCoordinationRef::from_label(plan.dispatch_id.clone());
        managed
            .driver
            .submit(
                crate::tasks::TaskDeclaration {
                    task_id: crate::tasks::PoolTaskId::new(*hash.finalize().as_bytes()),
                    pool: managed.pool.descriptor.pool_id.clone(),
                    domain: crate::tasks::DomainId::from_label(
                        crate::rt::task_adapter::PROCESSOR_DOMAIN,
                    ),
                    producer: origin,
                    handler: crate::tasks::HandlerRef::from_label(
                        crate::rt::task_adapter::PROCESSOR_DOMAIN,
                    ),
                    input: plan.input.task_input()?,
                    capabilities: crate::tasks::CapabilitySummary::empty(),
                    coordination_ref: Some(coordination.clone()),
                    placement,
                    effect_policy,
                    not_before_secs: None,
                    not_after_secs: None,
                    result_retention: crate::tasks::ResultRetention::ExternalSettlement(
                        coordination,
                    ),
                },
                serde_json::to_vec(&plan.input)?,
            )
            .await?;
        return Ok(());
    }
    if rt.dispatch_repo.get_any(&plan.dispatch_id).await.is_some() {
        return Ok(());
    }
    rt.dispatch_prepared_no_gate(
        &plan.plug_id,
        &plan.routine_name,
        crate::rt::task_adapter::PreparedRoutine {
            dispatch_id: plan.dispatch_id.clone(),
            input: plan.input.clone(),
        },
        vec![DispatchOnSuccessHook::ProcessorSettlement {
            slot: plan.slot.clone(),
            capture: plan.capture.clone(),
            domain: None,
        }],
        Vec::new(),
        false,
    )
    .await?;
    Ok(())
}

fn processor_meta_doc(doc_id: &DocId, keys: &HashSet<FacetKey>) -> Doc {
    Doc {
        id: doc_id.clone(),
        facets: keys
            .iter()
            .cloned()
            .map(|key| (key, serde_json::Value::Null))
            .collect(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessorDocState {
    heads: Option<ChangeHashSet>,
}

struct ProcessorFacetGroup {
    document_id: DocId,
    branch_id: BranchId,
    deltas: Vec<FacetDelta>,
}

fn processor_facet_state_key(key: &FacetRouteKey) -> Vec<u8> {
    let mut encoded = b"facet:".to_vec();
    encoded.extend(serde_json::to_vec(key).expect(ERROR_JSON));
    encoded
}

fn processor_doc_state_key(document_id: &DocId, branch_id: &str) -> Vec<u8> {
    let mut encoded = b"doc:".to_vec();
    encoded.extend(serde_json::to_vec(&(document_id, branch_id)).expect(ERROR_JSON));
    encoded
}

fn processor_snapshot_changed(previous: &FacetSnapshot, current: &FacetSnapshot) -> bool {
    previous.facet_heads != current.facet_heads || previous.author != current.author
}

async fn plan_processor_group(
    triage: &mut DocProcessorTriageListener,
    group: ProcessorFacetGroup,
    previous: &HashMap<Vec<u8>, FacetSnapshot>,
    previous_doc: Option<&ProcessorDocState>,
    source_revision: u64,
) -> Res<Option<(Vec<ProcessorDispatchPlan>, Vec<(Vec<u8>, Option<Vec<u8>>)>)>> {
    let branch_path = BranchPathBuf::from("main");
    let mut changed = HashSet::new();
    let mut added = HashSet::new();
    let mut removed = HashSet::new();
    let mut local_candidates = HashSet::new();
    let mut current_heads = None;
    let dmeta_key = FacetKey::from(daybook_types::doc::WellKnownFacetTag::Dmeta);
    for delta in &group.deltas {
        let is_dmeta = delta.key.facet_key == dmeta_key;
        if let Some(snapshot) = &delta.current {
            current_heads = Some(snapshot.branch_heads.clone());
            let state_key = processor_facet_state_key(&delta.key);
            match previous.get(&state_key) {
                Some(prior) if processor_snapshot_changed(prior, snapshot) => {
                    if !is_dmeta {
                        changed.insert(delta.key.facet_key.clone());
                        local_candidates.insert(delta.key.facet_key.clone());
                    }
                }
                Some(_) => {}
                None => {
                    if !is_dmeta {
                        added.insert(delta.key.facet_key.clone());
                        local_candidates.insert(delta.key.facet_key.clone());
                    }
                }
            }
        } else if previous.contains_key(&processor_facet_state_key(&delta.key)) && !is_dmeta {
            removed.insert(delta.key.facet_key.clone());
            if delta.removed_local {
                local_candidates.insert(delta.key.facet_key.clone());
            }
        }
    }
    let current_heads = current_heads.or_else(|| {
        group
            .deltas
            .iter()
            .find_map(|delta| delta.current_branch_heads.clone())
    });
    let change_kind = if current_heads.is_none() {
        DocChangeKind::Deleted
    } else if previous_doc.is_some_and(|state| state.heads.is_some()) {
        DocChangeKind::Updated
    } else {
        DocChangeKind::Added
    };
    let mut local_changed = if let Some(current_heads) = &current_heads {
        let mut keys = local_candidates.iter().cloned().collect::<Vec<_>>();
        keys.sort();
        let Some(local_changed) = triage
            .rt
            .drawer
            .facet_keys_touched_by_local_author(
                &group.document_id,
                &branch_path,
                current_heads,
                &keys,
            )
            .await?
        else {
            // The doc/branch is not resolvable at these heads yet (entry,
            // branch ref or handle not materialized). Classifying now would
            // mislabel local changes as remote, so defer the whole revision:
            // the driver parks on the materialization wake and reprocesses it.
            return Ok(None);
        };
        local_changed
    } else {
        HashSet::new()
    };
    if current_heads.is_none() {
        local_changed.extend(local_candidates);
    }
    // Delta keys describe this event, not the document predicate. HasTag and
    // Not(HasTag) must see every key at the captured source heads; otherwise
    // adding an embedding makes an unchanged Blob disappear from classification.
    let predicate_keys = if let Some(heads) = &current_heads {
        let Some(keys) = triage
            .rt
            .drawer
            .facet_keys_at_branch_heads(&group.document_id, &branch_path, heads)
            .await?
        else {
            return Ok(None);
        };
        keys
    } else {
        removed.clone()
    };
    let doc = Arc::new(processor_meta_doc(&group.document_id, &predicate_keys));
    let Some(plans) = triage
        .plan_doc(
            &group.document_id,
            &current_heads.clone().unwrap_or_default(),
            &doc,
            branch_path,
            change_kind,
            (!changed.is_empty()).then_some(&changed),
            (!added.is_empty()).then_some(&added),
            (!removed.is_empty()).then_some(&removed),
            current_heads.as_ref().map(|_| &local_changed),
            source_revision,
        )
        .await?
    else {
        return Ok(None);
    };
    let mut state_updates = Vec::new();
    for delta in group.deltas {
        let state_key = processor_facet_state_key(&delta.key);
        if let Some(snapshot) = delta.current {
            state_updates.push((
                state_key,
                Some(serde_json::to_vec(&snapshot).expect(ERROR_JSON)),
            ));
        } else {
            state_updates.push((state_key, None));
        }
    }
    state_updates.push((
        processor_doc_state_key(&group.document_id, &group.branch_id.0),
        Some(
            serde_json::to_vec(&ProcessorDocState {
                heads: current_heads,
            })
            .expect(ERROR_JSON),
        ),
    ));
    Ok(Some((plans, state_updates)))
}

async fn apply_processor_revision(
    triage: &mut DocProcessorTriageListener,
    state: &big_sync::SqliteDeltaWalkerStateRepo,
    walker: &mut SerialDeltaWalker<'_, FacetSetRevisionStore, big_sync::SqliteDeltaWalkerStateRepo>,
    source_revision: u64,
    entries: Vec<FacetDelta>,
) -> Res<bool> {
    let mut groups = BTreeMap::<(DocId, String), ProcessorFacetGroup>::new();
    for delta in entries {
        if delta.key.branch_id.0 != delta.key.document_id {
            continue;
        }
        groups
            .entry((delta.key.document_id.clone(), delta.key.branch_id.0.clone()))
            .or_insert_with(|| ProcessorFacetGroup {
                document_id: delta.key.document_id.clone(),
                branch_id: delta.key.branch_id.clone(),
                deltas: Vec::new(),
            })
            .deltas
            .push(delta);
    }
    let mut lookup_keys = Vec::new();
    for group in groups.values() {
        lookup_keys.push(processor_doc_state_key(
            &group.document_id,
            &group.branch_id.0,
        ));
        lookup_keys.extend(
            group
                .deltas
                .iter()
                .map(|delta| processor_facet_state_key(&delta.key)),
        );
    }
    let prior_rows =
        big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateRepo::get_many(
            state,
            &lookup_keys,
        )
        .await
        .map_err(|error| ferr!("reading DocProcessor sparse state: {error}"))?;
    let mut prior_snapshots = HashMap::new();
    let mut prior_docs = HashMap::new();
    for (key, value) in prior_rows {
        if key.starts_with(b"facet:") {
            prior_snapshots.insert(key, serde_json::from_slice::<FacetSnapshot>(&value)?);
        } else {
            prior_docs.insert(key, serde_json::from_slice::<ProcessorDocState>(&value)?);
        }
    }
    let mut plans = Vec::new();
    let mut state_updates = Vec::new();
    for group in groups.into_values() {
        let doc_key = processor_doc_state_key(&group.document_id, &group.branch_id.0);
        let Some((group_plans, group_updates)) = plan_processor_group(
            triage,
            group,
            &prior_snapshots,
            prior_docs.get(&doc_key),
            source_revision,
        )
        .await?
        else {
            return Ok(false);
        };
        plans.extend(group_plans);
        state_updates.extend(group_updates);
    }
    for plan in &plans {
        enqueue_processor_plan(&triage.rt, plan).await?;
    }
    let mut settlement = walker
        .begin_settlement(source_revision)
        .await
        .map_err(|error| ferr!("beginning DocProcessor FacetSet settlement: {error}"))?;
    for (key, value) in state_updates {
        match value {
            Some(value) => settlement
                .put(key, value)
                .await
                .map_err(|error| ferr!("persisting DocProcessor sparse state: {error}"))?,
            None => settlement
                .delete(&key)
                .await
                .map_err(|error| ferr!("deleting DocProcessor sparse state: {error}"))?,
        }
    }
    settlement
        .settle()
        .await
        .map_err(|error| ferr!("settling DocProcessor FacetSet revision: {error}"))?;
    Ok(true)
}

fn new_doc_processor_listener(rt: Arc<Rt>) -> DocProcessorTriageListener {
    DocProcessorTriageListener {
        rt,
        cached_processors: Vec::new(),
        triage_read_tags: HashSet::new(),
        triage_read_keys: HashSet::new(),
        facet_reference_specs: Arc::new(HashMap::new()),
        predicate_requirements: HashSet::new(),
        predicate_resolved: HashMap::new(),
    }
}

pub(crate) fn semantic_generation(value: &impl serde::Serialize, domain: &[u8]) -> Res<[u8; 32]> {
    fn canonicalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                object.sort_keys();
                for value in object.values_mut() {
                    canonicalize(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    canonicalize(value);
                }
            }
            _ => {}
        }
    }
    let mut value = serde_json::to_value(value)?;
    canonicalize(&mut value);
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&serde_json::to_vec(&value)?);
    Ok(*hash.finalize().as_bytes())
}

async fn install_activation_snapshot(
    triage: &mut DocProcessorTriageListener,
    source: &FacetSetRevisionStore,
    acknowledgements: &mut HashMap<String, ActivationAck>,
    config_documents: &mut HashSet<String>,
    changed_documents: &HashSet<String>,
) -> Res<()> {
    let rt = Arc::clone(&triage.rt);
    let desired = if config_documents.is_empty() {
        rt.plugs_repo.processor_activation_snapshot().await?
    } else {
        let cached_heads = acknowledgements
            .values()
            .filter(|ack| !changed_documents.contains(&ack.target.config_doc_id))
            .map(|ack| {
                (
                    ack.target.config_doc_id.clone(),
                    ack.target.config_doc_heads.clone(),
                )
            })
            .collect();
        rt.plugs_repo
            .cached_processor_activation_snapshot(&cached_heads)
            .await?
    };
    *config_documents = desired
        .targets
        .values()
        .map(|target| target.config_doc_id.clone())
        .collect();
    config_documents.insert(rt.plugs_repo.config_doc_id());
    #[cfg(test)]
    {
        let gate = {
            let mut slot = rt.triage_worker.install_gate.lock().await;
            let changed = slot.as_ref().is_some_and(|(id, _)| {
                desired.targets.get(id) != acknowledgements.get(id).map(|ack| &ack.target)
            });
            if changed { slot.take() } else { None }
        };
        if let Some((_, gate)) = gate {
            gate.reached
                .send(())
                .map_err(|_| ferr!("activation install barrier closed"))?;
            tokio::select! {
                result = gate.resume => result?,
                _ = rt.cancel_token.cancelled() => return Ok(()),
            }
        }
    }
    let boundary = source.latest_revision().await?;
    let mut installed = HashMap::new();
    for (plug_id, manifest) in &desired.manifests {
        let target = &desired.targets[plug_id];
        let previous = acknowledgements.get(plug_id);
        let ack = if let Some(previous) = previous
            && &previous.target == target
        {
            previous.clone()
        } else {
            let processors = if let Some(previous) = previous
                && previous.target.enabled_ref == target.enabled_ref
            {
                previous.processors.clone()
            } else {
                let generation =
                    semantic_generation(manifest.as_ref(), b"daybook/processor-manifest/v1")?;
                manifest
                    .processors
                    .keys()
                    .map(|key| (format!("{plug_id}/{key}"), generation))
                    .collect()
            };
            ActivationAck {
                target: target.clone(),
                processors,
                source_boundary: boundary,
                source_policy: SourceConsumptionPolicy::FutureObservedDeltasOnly,
            }
        };
        installed.insert(plug_id.clone(), ack);
    }
    triage
        .refresh_processors(desired.manifests.into_values().collect(), &installed)
        .await?;
    triage.predicate_requirements.clear();
    triage.predicate_resolved.clear();
    if &installed != acknowledgements {
        let mut tx = rt.rcx.sql.write_pool.begin().await?;
        sqlx::query("DELETE FROM triage_activation")
            .execute(&mut *tx)
            .await?;
        for (plug_id, ack) in &installed {
            sqlx::query("INSERT INTO triage_activation (plug_id, ack_json) VALUES (?, ?)")
                .bind(plug_id)
                .bind(serde_json::to_vec(ack)?)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
    }
    *acknowledgements = installed;
    // Wake even rejected/disabled waits; queries check current desired state, not stale receipts.
    rt.triage_worker
        .state
        .send_replace(acknowledgements.clone());
    Ok(())
}

pub(crate) struct DocProcessorStopToken {
    cancel_token: CancellationToken,
    stopped: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DocProcessorStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        self.stopped.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    err(Debug),
    fields(worker = "doc-processor")
)]
pub(crate) async fn spawn_doc_processor_driver(
    rt: Arc<Rt>,
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    parent_cancel_token: CancellationToken,
) -> Res<DocProcessorStopToken> {
    let facet_state = big_sync::SqliteDeltaWalkerStateRepo::new(
        rt.rcx.sql.read_pool.clone(),
        rt.rcx.sql.write_pool.clone(),
        "@daybook/core/doc-processor",
        "facets",
    )
    .await?;
    let wake = rt.drawer.subscribe_materialization_wake(None).await?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let stopped = rt.triage_worker.stopped.clone();
    let worker_handle = tokio::spawn(async move {
        let stopped = rt.triage_worker.stopped.clone();
        run_doc_processor_driver(
            rt,
            facet_set_store,
            plugs_repo,
            facet_state,
            worker_cancel_token,
            wake,
        )
        .await
        .unwrap();
        stopped.cancel();
    });
    Ok(DocProcessorStopToken {
        cancel_token,
        stopped,
        worker_handle: Some(worker_handle),
    })
}

#[derive(Default)]
struct ActivationChanges {
    config_changed: bool,
    documents: HashSet<String>,
    source_ready: bool,
}

impl ActivationChanges {
    fn observe(
        &mut self,
        change: crate::drawer::MaterializationChange,
        config_documents: &HashSet<String>,
    ) {
        self.source_ready |= change.heads.is_some();
        if config_documents.contains(&change.branch_id.0) {
            self.documents.insert(change.branch_id.0);
        }
    }

    fn plug_event(
        &mut self,
        event: Result<crate::plugs::PlugsEvent, tokio::sync::broadcast::error::RecvError>,
    ) -> Res<()> {
        match event {
            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                self.config_changed = true
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err(ferr!("DocProcessor plugs event broadcast closed"));
            }
        }
        Ok(())
    }

    fn metadata_event(
        &mut self,
        event: Result<Vec<DocId>, tokio::sync::broadcast::error::RecvError>,
        config_documents: &HashSet<String>,
    ) -> Res<()> {
        match event {
            Ok(ids) => self
                .documents
                .extend(ids.into_iter().filter(|id| config_documents.contains(id))),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                self.documents.extend(config_documents.iter().cloned())
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err(ferr!("DocProcessor Drawer metadata broadcast closed"));
            }
        }
        Ok(())
    }
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    err(Debug),
    fields(worker = "doc-processor")
)]
async fn run_doc_processor_driver(
    rt: Arc<Rt>,
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    facet_state: big_sync::SqliteDeltaWalkerStateRepo,
    cancel_token: CancellationToken,
    mut wake: crate::drawer::MaterializationWake,
) -> Res<()> {
    let mut triage = new_doc_processor_listener(Arc::clone(&rt));
    let mut plugs_events = plugs_repo.subscribe_events();
    let mut metadata_events = rt.drawer.subscribe_metadata_events();
    let facet_durable = facet_state.progress().await?.upstream_revision;
    let facet_reader = facet_set_store
        .open(FacetSetSelector::All, facet_durable)
        .await
        .map_err(|error| ferr!("opening DocProcessor FacetSet reader: {error}"))?;
    let mut facet_walker = SerialDeltaWalker::open(facet_reader, &facet_state)
        .await
        .map_err(|error| ferr!("opening DocProcessor FacetSet walker: {error}"))?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS triage_activation (plug_id TEXT PRIMARY KEY NOT NULL, ack_json BLOB NOT NULL)",
    ).execute(&rt.rcx.sql.write_pool).await?;
    let persisted: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT plug_id, ack_json FROM triage_activation")
            .fetch_all(&rt.rcx.sql.read_pool)
            .await?;
    let mut acknowledgements = persisted
        .into_iter()
        .map(|(id, bytes)| Ok((id, serde_json::from_slice::<ActivationAck>(&bytes)?)))
        .collect::<Res<HashMap<_, _>>>()?;
    #[cfg(any(test, feature = "test-support"))]
    {
        let gate = rt.config.triage_startup_gate.lock().await.take();
        if let Some((reached, resume)) = gate {
            reached
                .send(())
                .map_err(|_| ferr!("triage startup observer closed"))?;
            tokio::select! {
                result = resume => result?,
                _ = cancel_token.cancelled() => return Ok(()),
            }
        }
    }
    let mut config_documents = HashSet::new();
    install_activation_snapshot(
        &mut triage,
        &facet_set_store,
        &mut acknowledgements,
        &mut config_documents,
        &HashSet::new(),
    )
    .await?;
    let mut deferred: Option<(u64, Vec<FacetDelta>)> = None;
    let mut parked = false;
    loop {
        if cancel_token.is_cancelled() {
            return Ok(());
        }
        let mut changes = ActivationChanges::default();
        let read = tokio::select! {
            _ = cancel_token.cancelled() => return Ok(()),
            _ = rt.triage_worker.coordination_ready.notified() => {
                changes.source_ready = true;
                None
            }
            event = plugs_events.recv() => {
                changes.plug_event(event)?;
                None
            }
            event = metadata_events.recv() => {
                changes.metadata_event(event, &config_documents)?;
                None
            }
            change = wake.changed() => {
                changes.observe(change?, &config_documents);
                None
            }
            read = async {
                if let Some((revision, entries)) = deferred.take() {
                    Ok(RevisionRead::Entries { revision, entries })
                } else {
                    facet_walker.next().await.map_err(|error| ferr!("reading DocProcessor FacetSet walker: {error:?}"))
                }
            }, if !parked => Some(read?),
        };
        #[cfg(test)]
        let mut source_gate = {
            let mut slot = rt.triage_worker.source_gate.lock().await;
            if let Some(gate) = slot.as_ref()
                && let Some(RevisionRead::Entries { entries, .. }) = &read
                && entries
                    .iter()
                    .any(|entry| entry.key.facet_key == gate.facet_key)
            {
                slot.take()
            } else {
                None
            }
        };
        #[cfg(test)]
        if let Some(gate) = &mut source_gate {
            let Some(RevisionRead::Entries { entries, .. }) = &read else {
                unreachable!()
            };
            let document = entries
                .iter()
                .find(|entry| entry.key.facet_key == gate.facet_key)
                .expect(ERROR_IMPOSSIBLE)
                .key
                .document_id
                .clone();
            gate.selected
                .send(document)
                .map_err(|_| ferr!("source selection barrier closed"))?;
            tokio::select! {
                result = gate.resume.recv() => result.ok_or_eyre("source selection resume closed")?,
                _ = cancel_token.cancelled() => return Ok(()),
            }
        }
        // A selected source (including deferred work) must not bypass config
        // notifications already admitted to these receivers. Capture finite
        // prefixes so new unrelated arrivals cannot prolong this fence.
        let plug_prefix = plugs_events.len();
        for _ in 0..plug_prefix {
            match plugs_events.try_recv() {
                Ok(event) => changes.plug_event(Ok(event))?,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    changes.config_changed = true
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    return Err(ferr!("DocProcessor plugs event broadcast closed"));
                }
            }
        }
        let metadata_prefix = metadata_events.len();
        for _ in 0..metadata_prefix {
            match metadata_events.try_recv() {
                Ok(ids) => changes.metadata_event(Ok(ids), &config_documents)?,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    changes.documents.extend(config_documents.iter().cloned())
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    return Err(ferr!("DocProcessor Drawer metadata broadcast closed"));
                }
            }
        }
        wake.drain_ready(|change| changes.observe(change, &config_documents))?;
        // Facet content updates can precede physical materialization wakes.
        // Logical main mapping changes arrive separately from the Drawer owner.
        if let Some(RevisionRead::Entries { entries, .. }) = &read {
            for entry in entries {
                if config_documents.contains(&entry.key.document_id) {
                    changes.documents.insert(entry.key.document_id.clone());
                }
            }
        }
        if changes.config_changed || !changes.documents.is_empty() {
            install_activation_snapshot(
                &mut triage,
                &facet_set_store,
                &mut acknowledgements,
                &mut config_documents,
                &changes.documents,
            )
            .await?;
        }
        if changes.source_ready {
            parked = false;
        }
        let Some(read) = read else { continue };
        match read {
            RevisionRead::ReplayComplete { .. } => {}
            RevisionRead::Entries { revision, entries } => {
                #[cfg(test)]
                let defer = source_gate.as_ref().is_some_and(|gate| gate.defer_once);
                #[cfg(not(test))]
                let defer = false;
                let settled = !defer
                    && apply_processor_revision(
                        &mut triage,
                        &facet_state,
                        &mut facet_walker,
                        revision,
                        entries.clone(),
                    )
                    .await?;
                #[cfg(test)]
                if let Some(mut gate) = source_gate {
                    gate.settled
                        .send(settled)
                        .map_err(|_| ferr!("source settlement barrier closed"))?;
                    if !settled {
                        gate.defer_once = false;
                        *rt.triage_worker.source_gate.lock().await = Some(gate);
                    }
                }
                if !settled {
                    deferred = Some((revision, entries));
                    parked = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_types::manifest::DocChangePredicate;

    #[tokio::test(flavor = "multi_thread")]
    async fn deleted_source_invalidates_existing_desire_without_local_change_keys() -> Res<()> {
        use daybook_types::doc::AddDocArgs;
        use daybook_types::manifest::{DocPredicateClause, ProcessorCoordination, ProcessorInput};
        let ctx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
        let document = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: default(),
                user_path: None,
            })
            .await?;
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&document, daybook_types::doc::BranchPath::new("main"))
            .await?
            .unwrap();
        let key = slots::ProcessorSlotKey {
            document_id: document.clone(),
            branch_path: "main".into(),
            processor_full_id: "@test/deletion/process".into(),
        };
        let capture = slots::ProcessorCapture::new(heads, None, [1; 32], [2; 32]);
        ctx.rt
            .processor_slots
            .evaluate(
                &key,
                slots::ProcessorDesired {
                    capture: capture.clone(),
                    matches: true,
                },
            )
            .await?;
        ctx.rt
            .processor_slots
            .settle(
                &key,
                slots::ProcessorSettlement {
                    capture: capture.clone(),
                    attempt_id: "completed".into(),
                },
            )
            .await?;
        assert!(
            ctx.drawer_repo
                .delete_branch(&document, daybook_types::doc::BranchPath::new("main"), None)
                .await?
        );
        let processor_manifest = Arc::new(ProcessorManifest {
            desc: "Deletion invalidation".into(),
            input: ProcessorInput::Snapshot,
            coordination: ProcessorCoordination::PerNode,
            effects: default(),
            deets: ProcessorDeets::DocProcessor {
                event_predicate: default(),
                routine_name: "process".into(),
                predicate: DocPredicateClause::And(vec![]),
            },
        });
        let manifest = Arc::new(daybook_types::manifest::PlugManifest {
            namespace: "test".into(),
            name: "deletion".into(),
            version: "0.1.0".parse()?,
            title: "Deletion".into(),
            desc: "Deletion".into(),
            facets: vec![],
            local_states: default(),
            dependencies: default(),
            views: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            inits: default(),
            processors: default(),
        });
        let mut listener = new_doc_processor_listener(Arc::clone(&ctx.rt));
        listener.cached_processors.push(PreparedProcessor {
            processor_full_id: key.processor_full_id.clone(),
            plug_id: "@test/deletion".into(),
            routine_name: "process".into(),
            processor_manifest,
            plug_manifest: manifest,
            event_predicate: default(),
            read_tags: default(),
            read_keys: default(),
            source_boundary: 0,
        });
        let doc = processor_meta_doc(&document, &HashSet::new());
        let plans = listener
            .plan_doc(
                &document,
                &ChangeHashSet::default(),
                &doc,
                "main".into(),
                DocChangeKind::Deleted,
                None,
                None,
                None,
                None,
                1,
            )
            .await?
            .unwrap();
        assert!(
            plans.is_empty(),
            "deleted source must not create an executable obligation"
        );
        let slot = ctx.rt.processor_slots.slot(&key).await?;
        let desired = slot.desired().collect::<Vec<_>>();
        assert_eq!(desired.len(), 1);
        assert!(
            !desired[0].matches,
            "deletion must invalidate the observed matching desire"
        );
        assert!(desired[0].capture.heads.is_empty());
        assert!(slot.settled(&capture.generation));
        assert_eq!(slot.execution_baseline(), Some(capture.heads));
        ctx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activation_wait_observes_ack_between_subscription_and_query() -> Res<()> {
        let ctx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
        let manifest = daybook_types::manifest::PlugManifest {
            namespace: "test".into(),
            name: "activation-gap".into(),
            version: "0.1.0".parse()?,
            title: "Activation gap".into(),
            desc: "Activation subscription ordering".into(),
            facets: vec![],
            local_states: default(),
            dependencies: default(),
            views: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            inits: default(),
            processors: default(),
        };
        let document = ctx.rt.plugs_repo.add(manifest).await?;
        let reference =
            format!("db+facet:///{document}/org.example.daybook.plugManifest/main?branch=main")
                .parse()?;
        let (install_reached, install_ready) = tokio::sync::oneshot::channel();
        let (install_resume, install_continue) = tokio::sync::oneshot::channel();
        *ctx.rt.triage_worker.install_gate.lock().await = Some((
            "@test/activation-gap".into(),
            super::super::DispatchTestGate {
                reached: install_reached,
                resume: install_continue,
            },
        ));
        let target = ctx.rt.plugs_repo.enable_plug(&reference).await?;
        install_ready.await?;
        assert_eq!(
            ctx.rt
                .triage_worker
                .query_activation(&ctx.rt.plugs_repo, &target)
                .await?,
            ActivationStatus::Pending
        );
        let (query_reached, query_ready) = tokio::sync::oneshot::channel();
        let (query_resume, query_continue) = tokio::sync::oneshot::channel();
        *ctx.rt.triage_worker.wait_query_gate.lock().await = Some(super::super::DispatchTestGate {
            reached: query_reached,
            resume: query_continue,
        });
        let waiter = tokio::spawn({
            let rt = Arc::clone(&ctx.rt);
            let target = target.clone();
            async move {
                rt.triage_worker
                    .wait_for_activation(&rt.plugs_repo, target)
                    .await
            }
        });
        query_ready.await?;
        let mut installed = ctx.rt.triage_worker.state.subscribe();
        install_resume
            .send(())
            .map_err(|_| ferr!("installer disappeared"))?;
        let expected = loop {
            if let Some(ack) = installed.borrow().get(&target.plug_id)
                && ack.target == target
            {
                break ack.clone();
            }
            installed.changed().await?;
        };
        query_resume
            .send(())
            .map_err(|_| ferr!("waiter disappeared"))?;
        assert_eq!(waiter.await??, ActivationStatus::Active(expected));
        ctx.stop().await?;
        Ok(())
    }

    enum SourceSelection {
        Fresh,
        Deferred,
    }

    async fn source_disable_fence_case(selection: SourceSelection) -> Res<()> {
        use daybook_types::doc::{AddDocArgs, WellKnownFacet, WellKnownFacetTag};
        let ctx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
        crate::test_support::import_and_enable_test_plug(&ctx).await?;
        let note_key = FacetKey::from(WellKnownFacetTag::Note);
        let (selected_tx, mut selected_rx) = tokio::sync::mpsc::unbounded_channel();
        let (resume_tx, resume_rx) = tokio::sync::mpsc::unbounded_channel();
        let (settled_tx, mut settled_rx) = tokio::sync::mpsc::unbounded_channel();
        let deferred = matches!(selection, SourceSelection::Deferred);
        *ctx.rt.triage_worker.source_gate.lock().await = Some(SourceTestGate {
            facet_key: note_key.clone(),
            selected: selected_tx,
            resume: resume_rx,
            settled: settled_tx,
            defer_once: deferred,
        });
        let document = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    note_key,
                    WellKnownFacet::Note("Activation fence source".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        assert_eq!(
            selected_rx.recv().await.ok_or_eyre("source gate closed")?,
            document
        );
        if deferred {
            // Deliberately park this real source once to exercise the driver's
            // deferred-consumption transition, independent of storage timing.
            resume_tx
                .send(())
                .map_err(|_| ferr!("source resume closed"))?;
            assert!(
                !settled_rx
                    .recv()
                    .await
                    .ok_or_eyre("source settlement closed")?
            );
            ctx.drawer_repo
                .add(AddDocArgs {
                    branch_path: BranchPathBuf::from("main"),
                    facets: HashMap::new(),
                    user_path: None,
                })
                .await?;
            assert_eq!(
                selected_rx
                    .recv()
                    .await
                    .ok_or_eyre("deferred source gate closed")?,
                document
            );
        }
        // Unrelated materialization arrives ahead of the relevant disable.
        ctx.drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: HashMap::new(),
                user_path: None,
            })
            .await?;
        ctx.rt.plugs_repo.disable_plug("@daybook/test").await?;
        resume_tx
            .send(())
            .map_err(|_| ferr!("source resume closed"))?;
        assert!(
            settled_rx
                .recv()
                .await
                .ok_or_eyre("source settlement closed")?
        );
        assert!(
            ctx.dispatch_repo
                .get_any_by_wflow_key("test-label")
                .await
                .is_none(),
            "source selected before disable must not dispatch with the stale processor set"
        );
        let doc = ctx
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &document,
                daybook_types::doc::BranchPath::new("main"),
                Some(vec![FacetKey::from(WellKnownFacetTag::LabelGeneric)]),
            )
            .await?
            .ok_or_eyre("source document missing")?;
        assert!(
            !doc.facets
                .contains_key(&FacetKey::from(WellKnownFacetTag::LabelGeneric))
        );
        ctx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activation_fences_selected_source_after_admitted_disable() -> Res<()> {
        source_disable_fence_case(SourceSelection::Fresh).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activation_fences_deferred_source_after_admitted_disable() -> Res<()> {
        source_disable_fence_case(SourceSelection::Deferred).await
    }

    fn fk(tag: &str, id: &str) -> FacetKey {
        FacetKey {
            tag: tag.into(),
            id: id.into(),
        }
    }

    #[test]
    fn node_predicate_change_origin_local() {
        let predicate = NodePredicate::ChangeOrigin(ChangeOriginDeets::Local);
        assert!(evaluate_node_predicate(&predicate, true));
        assert!(!evaluate_node_predicate(&predicate, false));
    }

    #[test]
    fn doc_change_predicate_changed_facet_tags() {
        use daybook_types::manifest::{DocChangeKind, DocChangePredicate};
        let mut changed = HashSet::new();
        changed.insert(FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        });
        changed.insert(FacetKey {
            tag: "org.example.todo".into(),
            id: "x".into(),
        });
        let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()]);
        assert!(pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
        let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.unknown".into()]);
        assert!(!pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
        assert!(!pred.evaluate_change(DocChangeKind::Updated, None, None, None));
    }

    #[test]
    fn doc_change_predicate_added_deleted_and_removed_tags() {
        use daybook_types::manifest::{DocChangeKind, DocChangePredicate};
        let mut removed = HashSet::new();
        removed.insert(FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        });

        assert!(DocChangePredicate::Added.evaluate_change(DocChangeKind::Added, None, None, None,));
        assert!(!DocChangePredicate::Added.evaluate_change(
            DocChangeKind::Updated,
            None,
            None,
            None,
        ));
        assert!(DocChangePredicate::Deleted.evaluate_change(
            DocChangeKind::Deleted,
            None,
            None,
            None,
        ));
        assert!(
            DocChangePredicate::RemovedFacetTags(vec!["org.example.note".into()]).evaluate_change(
                DocChangeKind::Deleted,
                Some(&removed),
                None,
                Some(&removed),
            )
        );
    }

    #[test]
    fn processor_event_gate_is_specific_and_rejects_adjacent_unsatisfying_events() {
        let local_node = NodePredicate::ChangeOrigin(ChangeOriginDeets::Local);
        let read_tags: HashSet<String> = ["org.example.note".to_string()].into();
        let read_keys: HashSet<FacetKey> = HashSet::new();
        let mut changed_note = HashSet::new();
        changed_note.insert(fk("org.example.note", "main"));
        let mut changed_todo = HashSet::new();
        changed_todo.insert(fk("org.example.todo", "main"));
        let mut removed_note = HashSet::new();
        removed_note.insert(fk("org.example.note", "main"));

        struct Case {
            name: &'static str,
            predicate: DocChangePredicate,
            is_local_for_processor: bool,
            kind: DocChangeKind,
            changed: Option<HashSet<FacetKey>>,
            added: Option<HashSet<FacetKey>>,
            removed: Option<HashSet<FacetKey>>,
            expect: bool,
        }

        let cases = vec![
            Case {
                name: "added matches exact added event",
                predicate: DocChangePredicate::Added,
                is_local_for_processor: true,
                kind: DocChangeKind::Added,
                changed: Some(changed_note.clone()),
                added: Some(changed_note.clone()),
                removed: None,
                expect: true,
            },
            Case {
                name: "added does not match adjacent updated",
                predicate: DocChangePredicate::Added,
                is_local_for_processor: true,
                kind: DocChangeKind::Updated,
                changed: Some(changed_note.clone()),
                added: None,
                removed: None,
                expect: false,
            },
            Case {
                name: "added does not match adjacent deleted",
                predicate: DocChangePredicate::Added,
                is_local_for_processor: true,
                kind: DocChangeKind::Deleted,
                changed: Some(changed_note.clone()),
                added: None,
                removed: Some(changed_note.clone()),
                expect: false,
            },
            Case {
                name: "deleted matches exact deleted event",
                predicate: DocChangePredicate::Deleted,
                is_local_for_processor: true,
                kind: DocChangeKind::Deleted,
                changed: Some(changed_note.clone()),
                added: None,
                removed: Some(changed_note.clone()),
                expect: true,
            },
            Case {
                name: "deleted does not match adjacent updated",
                predicate: DocChangePredicate::Deleted,
                is_local_for_processor: true,
                kind: DocChangeKind::Updated,
                changed: Some(changed_note.clone()),
                added: None,
                removed: None,
                expect: false,
            },
            Case {
                name: "changed tag matches only matching tag",
                predicate: DocChangePredicate::ChangedFacetTags(vec!["org.example.note".into()]),
                is_local_for_processor: true,
                kind: DocChangeKind::Updated,
                changed: Some(changed_note.clone()),
                added: None,
                removed: None,
                expect: true,
            },
            Case {
                name: "changed tag rejects adjacent non-matching tag",
                predicate: DocChangePredicate::ChangedFacetTags(vec!["org.example.note".into()]),
                is_local_for_processor: true,
                kind: DocChangeKind::Updated,
                changed: Some(changed_todo.clone()),
                added: None,
                removed: None,
                expect: false,
            },
            Case {
                name: "removed tag matches removed set",
                predicate: DocChangePredicate::RemovedFacetTags(vec!["org.example.note".into()]),
                is_local_for_processor: true,
                kind: DocChangeKind::Deleted,
                changed: Some(removed_note.clone()),
                added: None,
                removed: Some(removed_note.clone()),
                expect: true,
            },
            Case {
                name: "removed tag rejects deleted with other tag",
                predicate: DocChangePredicate::RemovedFacetTags(vec!["org.example.note".into()]),
                is_local_for_processor: true,
                kind: DocChangeKind::Deleted,
                changed: Some(changed_todo.clone()),
                added: None,
                removed: Some(changed_todo.clone()),
                expect: false,
            },
            Case {
                name: "local node predicate rejects when processor has no local overlap",
                predicate: DocChangePredicate::ChangedFacetTags(vec!["org.example.note".into()]),
                is_local_for_processor: false,
                kind: DocChangeKind::Updated,
                changed: Some(changed_note.clone()),
                added: None,
                removed: None,
                expect: false,
            },
            Case {
                name: "read-set gating rejects unrelated changed keys",
                predicate: DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()]),
                is_local_for_processor: true,
                kind: DocChangeKind::Updated,
                changed: Some(changed_todo),
                added: None,
                removed: None,
                expect: false,
            },
        ];

        for case in cases {
            let changed_ref = case.changed.as_ref();
            let added_ref = case.added.as_ref();
            let removed_ref = case.removed.as_ref();
            let got = should_processor_run_for_event(
                &local_node,
                &case.predicate,
                case.is_local_for_processor,
                case.kind,
                changed_ref,
                added_ref,
                removed_ref,
                &read_tags,
                &read_keys,
            );
            assert_eq!(got, case.expect, "case={}", case.name);
        }
    }

    #[test]
    fn changed_intersects_read_set_matches_by_tag_or_exact_key() {
        let note_main = fk("org.example.note", "main");
        let note_alt = fk("org.example.note", "alt");
        let todo_main = fk("org.example.todo", "main");

        let changed: HashSet<FacetKey> = [todo_main.clone()].into();
        let read_tags: HashSet<String> = ["org.example.todo".to_string()].into();
        let read_keys: HashSet<FacetKey> = HashSet::new();
        assert!(changed_intersects_read_set(
            &changed, &read_tags, &read_keys
        ));

        let changed: HashSet<FacetKey> = [note_alt].into();
        let read_tags: HashSet<String> = HashSet::new();
        let read_keys: HashSet<FacetKey> = [note_main].into();
        assert!(!changed_intersects_read_set(
            &changed, &read_tags, &read_keys
        ));
    }
}
