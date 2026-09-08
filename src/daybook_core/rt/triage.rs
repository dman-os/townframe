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

struct PreparedProcessor {
    processor_full_id: String,
    plug_id: String,
    routine_name: KeyGeneric,
    processor_manifest: Arc<ProcessorManifest>,
    event_predicate: ProcessorEventPredicate,
    /// Tag-level: any facet with this tag counts as read.
    read_tags: HashSet<String>,
    /// Key-level: only this tag+id counts as read.
    read_keys: HashSet<FacetKey>,
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
    doc_id: DocId,
    branch_path: BranchPathBuf,
    heads: ChangeHashSet,
    plug_id: String,
    routine_name: String,
    processor_full_id: String,
    changed_facet_keys: Vec<String>,
    done_token: String,
    dispatch_id: String,
}

impl DocProcessorTriageListener {
    #[tracing::instrument(skip(self, rt))]
    async fn refresh_processors(&mut self, rt: &Arc<Rt>) -> Res<()> {
        // ADR 007 §6: processors register for active plugs only.
        let plugs = rt.plugs_repo.list_active_plugs().await;
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
                            event_predicate: event_predicate.clone(),
                            read_tags,
                            read_keys,
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
    ) -> Res<Option<Vec<ProcessorDispatchPlan>>> {
        let rt = &self.rt;
        debug!(
            processor_count = self.cached_processors.len(),
            "triaging doc"
        );
        let mut plans = Vec::new();
        let mut full_doc_for_reference_predicates: Option<Option<Arc<Doc>>> = None;
        for processor in &self.cached_processors {
            let is_local_for_processor = local_changed_facet_keys
                .map(|changed| {
                    changed_intersects_read_set(changed, &processor.read_tags, &processor.read_keys)
                })
                .unwrap_or(false);
            if !should_processor_run_for_event(
                &processor.event_predicate.node_predicate,
                &processor.event_predicate.doc_change_predicate,
                is_local_for_processor,
                change_kind,
                changed_facet_keys,
                added_facet_keys,
                removed_facet_keys,
                &processor.read_tags,
                &processor.read_keys,
            ) {
                continue;
            }
            let predicate = match &processor.processor_manifest.deets {
                ProcessorDeets::DocProcessor { predicate, .. } => predicate,
            };
            self.predicate_requirements.clear();
            predicate.append_requirements(&mut self.predicate_requirements);

            let needs_full_doc = change_kind != DocChangeKind::Deleted
                && self.predicate_requirements.iter().any(|req| {
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
            // Predicates like `HasTag(A) AND HasTag(B)` where A and B land in
            // different batches can never match the batch-only meta doc. When
            // the full doc is already loaded (any processor needed it),
            // evaluate every predicate against it; fall back to the meta doc
            // only when the full doc is unavailable (deleted/unreadable).
            let predicate_doc = match full_doc_for_reference_predicates
                .as_ref()
                .and_then(|opt| opt.as_ref())
            {
                Some(full) => full.as_ref(),
                None => predicate_doc_arc.map(|doc| doc.as_ref()).unwrap_or(doc),
            };

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
            if !predicate_match {
                continue;
            }
            info!(
                plug_id = %processor.plug_id,
                routine_name = %processor.routine_name,
                processor_full_id = %processor.processor_full_id,
                ?doc_id,
                branch_path = %branch_path,
                heads = ?am_utils_rs::serialize_commit_heads(doc_heads.as_ref()),
                "planning processor dispatch"
            );
            let changed_facet_keys: Vec<String> = {
                let mut keys = std::collections::BTreeSet::new();
                let mut extend_keys = |facet_keys: Option<&HashSet<FacetKey>>| {
                    if let Some(facet_keys) = facet_keys {
                        keys.extend(facet_keys.iter().filter_map(|key| {
                            if processor.read_tags.contains(&key.tag.to_string())
                                || processor.read_keys.contains(key)
                            {
                                Some(key.to_string())
                            } else {
                                None
                            }
                        }));
                    }
                };
                extend_keys(changed_facet_keys);
                extend_keys(added_facet_keys);
                extend_keys(removed_facet_keys);
                keys.into_iter().collect()
            };
            let done_token = make_processor_done_token(
                doc_id,
                &processor.processor_full_id,
                &branch_path,
                doc_heads,
            );
            let dispatch_id = processor_dispatch_id(
                doc_id,
                &branch_path,
                doc_heads,
                &processor.plug_id,
                &processor.routine_name.0,
            );
            plans.push(ProcessorDispatchPlan {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: doc_heads.clone(),
                plug_id: processor.plug_id.clone(),
                routine_name: processor.routine_name.0.clone(),
                processor_full_id: processor.processor_full_id.clone(),
                changed_facet_keys,
                done_token,
                dispatch_id,
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

fn make_processor_done_token(
    doc_id: &DocId,
    processor_full_id: &str,
    branch_path: &daybook_types::doc::BranchPathBuf,
    heads: &ChangeHashSet,
) -> String {
    let mut fingerprint = String::new();
    use std::fmt::Write as _;
    write!(
        &mut fingerprint,
        "{}|{}|{}|{}",
        doc_id,
        processor_full_id,
        branch_path.as_str(),
        serde_json::to_string(heads).expect(ERROR_JSON)
    )
    .expect("writing to string should never fail");
    utils_rs::hash::blake3_hash_bytes_multibase(fingerprint.as_bytes())
}

fn processor_dispatch_id(
    doc_id: &DocId,
    branch_path: &BranchPathBuf,
    heads: &ChangeHashSet,
    plug_id: &str,
    routine_name: &str,
) -> String {
    let mut identity = String::new();
    use std::fmt::Write as _;
    write!(
        &mut identity,
        "{}|{}|{}|{}|{}|processor",
        doc_id,
        branch_path,
        am_utils_rs::serialize_commit_heads(heads.as_ref()).join(","),
        plug_id,
        routine_name,
    )
    .expect("writing to string should never fail");
    let digest = utils_rs::hash::blake3_hash_bytes_multibase(identity.as_bytes());
    format!("{plug_id}/{routine_name}/{branch_path}-{digest}")
}

async fn enqueue_processor_plan(rt: &Rt, plan: &ProcessorDispatchPlan) -> Res<()> {
    if rt.dispatch_repo.get_any(&plan.dispatch_id).await.is_some() {
        return Ok(());
    }
    let args = DispatchArgs::DocRoutine {
        doc_id: plan.doc_id.clone(),
        branch_path: plan.branch_path.clone(),
        heads: plan.heads.clone(),
        invocation: crate::rt::dispatch::RoutineInvocation::Processor(
            crate::rt::dispatch::ProcessorInvocation {
                trigger_doc_id: plan.doc_id.clone(),
                changed_facet_keys: plan.changed_facet_keys.clone(),
            },
        ),
        changed_facet_keys: plan.changed_facet_keys.clone(),
        wflow_args_json: None,
    };
    rt.dispatch_raw(
        &plan.plug_id,
        &plan.routine_name,
        args,
        vec![DispatchOnSuccessHook::ProcessorRunLog {
            doc_id: plan.doc_id.clone(),
            processor_full_id: plan.processor_full_id.clone(),
            done_token: plan.done_token.clone(),
        }],
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
    previous.facet_heads != current.facet_heads || previous.actor_id != current.actor_id
}

async fn plan_processor_group(
    triage: &mut DocProcessorTriageListener,
    group: ProcessorFacetGroup,
    previous: &HashMap<Vec<u8>, FacetSnapshot>,
    previous_doc: Option<&ProcessorDocState>,
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
        triage
            .rt
            .drawer
            .facet_keys_touched_by_local_actor(
                &group.document_id,
                &branch_path,
                current_heads,
                &keys,
            )
            .await?
    } else {
        HashSet::new()
    };
    if current_heads.is_none() {
        local_changed.extend(local_candidates);
    }
    let all_changed = changed
        .iter()
        .chain(added.iter())
        .chain(removed.iter())
        .cloned()
        .collect::<HashSet<_>>();
    let doc = Arc::new(processor_meta_doc(&group.document_id, &all_changed));
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
        let Some((group_plans, group_updates)) =
            plan_processor_group(triage, group, &prior_snapshots, prior_docs.get(&doc_key)).await?
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

pub(crate) struct DocProcessorStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DocProcessorStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

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
    let worker_handle = tokio::spawn(async move {
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
    });
    Ok(DocProcessorStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

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
    // The plugs repository warms its cache before attaching to the runtime,
    // so this snapshot is the complete initial processor set. Subscribe
    // first so an enablement cannot fall between the snapshot and the live
    // event stream.
    triage.refresh_processors(&rt).await?;
    let facet_durable = facet_state.progress().await?.upstream_revision;
    let facet_reader = facet_set_store
        .open(FacetSetSelector::All, facet_durable)
        .await
        .map_err(|error| ferr!("opening DocProcessor FacetSet reader: {error}"))?;
    let mut facet_walker = SerialDeltaWalker::open(facet_reader, &facet_state)
        .await
        .map_err(|error| ferr!("opening DocProcessor FacetSet walker: {error}"))?;
    let mut deferred = None;
    loop {
        let read = if let Some((revision, entries)) = deferred.take() {
            RevisionRead::Entries { revision, entries }
        } else {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                plug_event = plugs_events.recv() => {
                    match plug_event {
                        Ok(_) => triage.refresh_processors(&rt).await?,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                            warn!(missed, "DocProcessor plugs event broadcast lagged; refreshing processor set");
                            triage.refresh_processors(&rt).await?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(ferr!("DocProcessor plugs event broadcast closed"));
                        }
                    }
                    continue;
                }
                read = facet_walker.next() => read.map_err(|error| ferr!("reading DocProcessor FacetSet walker: {error:?}"))?,
            }
        };
        match read {
            RevisionRead::ReplayComplete { .. } => {}
            RevisionRead::Entries { revision, entries } => {
                if !apply_processor_revision(
                    &mut triage,
                    &facet_state,
                    &mut facet_walker,
                    revision,
                    entries.clone(),
                )
                .await?
                {
                    deferred = Some((revision, entries));
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => return Ok(()),
                        result = wake.wait() => result?,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_types::manifest::DocChangePredicate;

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
