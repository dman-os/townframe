use crate::interlude::*;

use crate::drawer::DrawerEvent;
use crate::rt::dispatch::{DispatchEvent, DispatchOnSuccessHook, DispatchStatus};
use crate::rt::switch::{
    facet_keys_set_to_meta_doc, SwitchEvent, SwitchSink, SwitchSinkCtx, SwitchSinkOutcome,
    SwtchSinkInterest,
};
use crate::rt::{DispatchArgs, Rt};
use daybook_types::doc::BranchPath;
use daybook_types::doc::{Doc, DocId, FacetKey, WellKnownFacetTag};

use daybook_types::manifest::{
    ChangeOriginDeets, DocChangeKind, DocPredicateEvalMode, DocPredicateEvalRequirement,
    DocPredicateEvalResolved, FacetReferenceManifest, KeyGeneric, NodePredicate, ProcessorDeets,
    ProcessorEventPredicate, ProcessorManifest,
};

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

#[derive(Default)]
struct DocProcessorTriageListener {
    cached_processors: Vec<PreparedProcessor>,
    triage_read_tags: HashSet<String>,
    triage_read_keys: HashSet<FacetKey>,
    facet_reference_specs: Arc<HashMap<String, Vec<FacetReferenceManifest>>>,
    predicate_requirements: HashSet<DocPredicateEvalRequirement>,
    predicate_resolved: HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
    dispatch_to_job: HashMap<String, String>,
    job_to_dispatch: HashMap<String, String>,
}

impl DocProcessorTriageListener {
    fn inflight_job_key(
        doc_id: &DocId,
        processor_full_id: &str,
        branch_path: &BranchPath,
        doc_heads: &ChangeHashSet,
    ) -> String {
        format!(
            "{}:{}:{}:{}",
            doc_id,
            processor_full_id,
            branch_path,
            am_utils_rs::serialize_commit_heads(doc_heads.as_ref()).join(",")
        )
    }

    fn clear_inflight_dispatch(&mut self, dispatch_id: &str) {
        if let Some(job_key) = self.dispatch_to_job.remove(dispatch_id) {
            self.job_to_dispatch.remove(&job_key);
        }
    }

    #[tracing::instrument(skip(self, rt))]
    async fn refresh_processors(&mut self, rt: &Arc<Rt>) -> Res<()> {
        let plugs = rt.plugs_repo.list_plugs().await;
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

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, doc, doc_heads, ctx))]
    async fn triage_doc(
        &mut self,
        ctx: &SwitchSinkCtx<'_>,
        doc_id: &DocId,
        doc_heads: &ChangeHashSet,
        doc: &Doc,
        branch_path: daybook_types::doc::BranchPath,
        _event_origin: &crate::event_origin::SwitchEventOrigin,
        change_kind: DocChangeKind,
        changed_facet_keys: Option<&HashSet<FacetKey>>,
        added_facet_keys: Option<&HashSet<FacetKey>>,
        removed_facet_keys: Option<&HashSet<FacetKey>>,
        local_changed_facet_keys: Option<&HashSet<FacetKey>>,
    ) -> Res<()> {
        let rt = ctx
            .rt
            .ok_or_else(|| ferr!("triage listener context missing rt"))?;
        debug!(
            processor_count = self.cached_processors.len(),
            "triaging doc"
        );
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
                    None => continue,
                }
            } else {
                None
            };
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
                "dispatching job"
            );
            let changed_facet_keys: Vec<String> = changed_facet_keys
                .map(|keys| {
                    keys.iter()
                        .filter(|key| {
                            processor.read_tags.contains(&key.tag.to_string())
                                || processor.read_keys.contains(key)
                        })
                        .map(|key| key.to_string())
                        .collect()
                })
                .unwrap_or_default();
            let args = DispatchArgs::DocRoutine {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: doc_heads.clone(),
                changed_facet_keys,
                wflow_args_json: None,
            };
            let job_key = Self::inflight_job_key(
                doc_id,
                &processor.processor_full_id,
                &branch_path,
                doc_heads,
            );
            let old_dispatch = self.job_to_dispatch.get(&job_key).cloned();
            if let Some(dispatch_id) = old_dispatch {
                info!(
                    ?dispatch_id,
                    "inflight job already exists; skipping redispatch"
                );
                continue;
            }
            let done_token = make_processor_done_token(
                doc_id,
                &processor.processor_full_id,
                &branch_path,
                doc_heads,
            );
            let dispatch_id = rt
                .dispatch_raw(
                    &processor.plug_id,
                    &processor.routine_name.0,
                    args,
                    vec![DispatchOnSuccessHook::ProcessorRunLog {
                        doc_id: doc_id.clone(),
                        processor_full_id: processor.processor_full_id.clone(),
                        done_token,
                    }],
                )
                .await?;
            self.job_to_dispatch
                .insert(job_key.clone(), dispatch_id.clone());
            self.dispatch_to_job.insert(dispatch_id, job_key);
        }
        Ok(())
    }
}

#[async_trait]
impl SwitchSink for DocProcessorTriageListener {
    fn interest(&self) -> SwtchSinkInterest {
        SwtchSinkInterest {
            consume_drawer: true,
            consume_plugs: true,
            consume_dispatch: true,
            consume_config: true,
            drawer_predicate: None,
        }
    }

    async fn on_event(
        &mut self,
        event: &SwitchEvent,
        ctx: &SwitchSinkCtx<'_>,
    ) -> Res<SwitchSinkOutcome> {
        match event {
            SwitchEvent::Plugs(_) => {
                let rt = ctx
                    .rt
                    .ok_or_else(|| ferr!("triage listener context missing rt"))?;
                self.refresh_processors(rt).await?;
            }
            SwitchEvent::Config(_) => {}
            SwitchEvent::Dispatch(event) => match &**event {
                DispatchEvent::DispatchDeleted { id, .. } => {
                    self.clear_inflight_dispatch(id);
                }
                DispatchEvent::DispatchUpdated { id, .. } => {
                    let rt = ctx
                        .rt
                        .ok_or_else(|| ferr!("triage listener context missing rt"))?;
                    let Some(dispatch) = rt.dispatch_repo.get_any(id).await else {
                        self.clear_inflight_dispatch(id);
                        return Ok(SwitchSinkOutcome::default());
                    };
                    if matches!(
                        dispatch.status,
                        DispatchStatus::Succeeded
                            | DispatchStatus::Failed
                            | DispatchStatus::Cancelled
                    ) {
                        self.clear_inflight_dispatch(id);
                    }
                }
                DispatchEvent::DispatchAdded { .. } => {}
            },
            SwitchEvent::Drawer(event) => match &**event {
                DrawerEvent::DocDeleted {
                    id,
                    deleted_facet_keys,
                    drawer_heads,
                    origin,
                    ..
                } => {
                    let deleted_set: HashSet<FacetKey> =
                        deleted_facet_keys.iter().cloned().collect();
                    if !changed_intersects_read_set(
                        &deleted_set,
                        &self.triage_read_tags,
                        &self.triage_read_keys,
                    ) {
                        return Ok(SwitchSinkOutcome::default());
                    }
                    let meta_doc = facet_keys_set_to_meta_doc(id, &deleted_set);
                    let pseudo_branch = BranchPath::from("main");
                    self.triage_doc(
                        ctx,
                        id,
                        drawer_heads,
                        &meta_doc,
                        pseudo_branch,
                        origin,
                        DocChangeKind::Deleted,
                        Some(&deleted_set),
                        None,
                        Some(&deleted_set),
                        None,
                    )
                    .await
                    .wrap_err("error triaging deleted doc")?;
                }
                DrawerEvent::DocAdded {
                    id,
                    entry,
                    drawer_heads: _,
                    origin,
                } => {
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPath::from(branch_name.as_str());
                        if branch_path.to_string().starts_with("/tmp/") {
                            continue;
                        }
                        let rt = ctx
                            .rt
                            .ok_or_else(|| ferr!("triage listener context missing rt"))?;
                        let Some(facet_keys_set) = rt
                            .drawer
                            .get_facet_keys_if_latest(id, &branch_path, heads)
                            .await?
                        else {
                            continue;
                        };
                        let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
                        let changed_facet_keys_set: HashSet<FacetKey> =
                            facet_keys_set.iter().cloned().collect();
                        let local_changed_facet_keys_set = rt
                            .drawer
                            .facet_keys_touched_by_local_actor(
                                id,
                                &branch_path,
                                heads,
                                &changed_facet_keys_set
                                    .iter()
                                    .filter(|key| **key != dmeta_key)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                            .await?;
                        let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                        self.triage_doc(
                            ctx,
                            id,
                            heads,
                            &meta_doc,
                            branch_path,
                            origin,
                            DocChangeKind::Added,
                            Some(&changed_facet_keys_set),
                            Some(&changed_facet_keys_set),
                            None,
                            Some(&local_changed_facet_keys_set),
                        )
                        .await
                        .wrap_err("error triaging doc")?;
                    }
                }
                DrawerEvent::DocUpdated {
                    id,
                    entry,
                    diff,
                    drawer_heads: _,
                    origin,
                } => {
                    let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
                    let has_non_dmeta_change = diff
                        .changed_facet_keys
                        .iter()
                        .any(|facet_key| facet_key != &dmeta_key);
                    let moved_main = diff.moved_branch_names.iter().any(|name| name == "main");
                    let changed_facet_keys_set: Option<HashSet<FacetKey>> = if has_non_dmeta_change
                    {
                        let changed_set: HashSet<FacetKey> =
                            diff.changed_facet_keys.iter().cloned().collect();
                        if !changed_intersects_read_set(
                            &changed_set,
                            &self.triage_read_tags,
                            &self.triage_read_keys,
                        ) {
                            return Ok(SwitchSinkOutcome::default());
                        }
                        Some(changed_set)
                    } else if moved_main {
                        None
                    } else {
                        return Ok(SwitchSinkOutcome::default());
                    };
                    let added_facet_keys_set: Option<HashSet<FacetKey>> =
                        if diff.added_facet_keys.is_empty() {
                            None
                        } else {
                            Some(diff.added_facet_keys.iter().cloned().collect())
                        };
                    let removed_facet_keys_set: Option<HashSet<FacetKey>> =
                        if diff.removed_facet_keys.is_empty() {
                            None
                        } else {
                            Some(diff.removed_facet_keys.iter().cloned().collect())
                        };
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPath::from(branch_name.as_str());
                        if branch_path.to_string().starts_with("/tmp/") {
                            continue;
                        }
                        if branch_name != "main"
                            && !diff
                                .moved_branch_names
                                .iter()
                                .any(|name| name == branch_name)
                        {
                            continue;
                        }
                        let rt = ctx
                            .rt
                            .ok_or_else(|| ferr!("triage listener context missing rt"))?;
                        let Some(facet_keys_set) = rt
                            .drawer
                            .get_facet_keys_if_latest(id, &branch_path, heads)
                            .await?
                        else {
                            debug!(?id, ?branch_path, "skipping triage for stale heads");
                            continue;
                        };
                        let local_changed_facet_keys_set = rt
                            .drawer
                            .facet_keys_touched_by_local_actor(
                                id,
                                &branch_path,
                                heads,
                                &diff
                                    .changed_facet_keys
                                    .iter()
                                    .cloned()
                                    .chain(diff.added_facet_keys.iter().cloned())
                                    .chain(diff.removed_facet_keys.iter().cloned())
                                    .filter(|key| *key != FacetKey::from(WellKnownFacetTag::Dmeta))
                                    .collect::<Vec<_>>(),
                            )
                            .await?;
                        if changed_facet_keys_set
                            .as_ref()
                            .is_none_or(HashSet::is_empty)
                            && added_facet_keys_set.as_ref().is_none_or(HashSet::is_empty)
                            && removed_facet_keys_set
                                .as_ref()
                                .is_none_or(HashSet::is_empty)
                        {
                            continue;
                        }
                        let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                        self.triage_doc(
                            ctx,
                            id,
                            heads,
                            &meta_doc,
                            branch_path,
                            origin,
                            DocChangeKind::Updated,
                            changed_facet_keys_set.as_ref(),
                            added_facet_keys_set.as_ref(),
                            removed_facet_keys_set.as_ref(),
                            Some(&local_changed_facet_keys_set),
                        )
                        .await
                        .wrap_err("error triaging doc")?;
                    }
                }
            },
        }
        Ok(SwitchSinkOutcome::default())
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

#[allow(clippy::too_many_arguments)]
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
    branch_path: &daybook_types::doc::BranchPath,
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
    utils_rs::hash::blake3_hash_bytes(fingerprint.as_bytes())
}

pub fn doc_processor_triage_listener() -> Box<dyn SwitchSink + Send + Sync> {
    Box::<DocProcessorTriageListener>::default()
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
