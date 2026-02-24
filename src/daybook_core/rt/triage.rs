use crate::interlude::*;
use crate::plugs::reference::select_json_path_values;

use crate::drawer::DrawerEvent;
use crate::rt::dispatch::DispatchEvent;
use crate::rt::switch::{
    facet_keys_set_to_meta_doc, SwitchEvent, SwitchSink, SwitchSinkCtx, SwitchSinkOutcome,
    SwtchSinkInterest,
};
use crate::rt::{DispatchArgs, Rt};
use daybook_types::doc::BranchPath;
use daybook_types::doc::{Doc, DocId, FacetKey, WellKnownFacetTag};

use crate::plugs::manifest::{
    FacetReferenceKind, FacetReferenceManifest, KeyGeneric, ProcessorDeets, ProcessorManifest,
    RoutineManifest, RoutineManifestDeets,
};

struct PreparedProcessor {
    plug_id: String,
    routine_name: KeyGeneric,
    processor_manifest: Arc<ProcessorManifest>,
    routine_manifest: Arc<RoutineManifest>,
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
    facet_reference_specs: HashMap<String, Vec<FacetReferenceManifest>>,
}

impl DocProcessorTriageListener {
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
            for processor in plug.processors.values() {
                match &processor.deets {
                    ProcessorDeets::DocProcessor {
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
                        let (acl_tags, acl_keys) = routine.read_facet_set();
                        read_tags.extend(acl_tags);
                        read_keys.extend(acl_keys);
                        triage_read_tags.extend(read_tags.iter().cloned());
                        triage_read_keys.extend(read_keys.iter().cloned());
                        self.cached_processors.push(PreparedProcessor {
                            plug_id: plug_id.clone(),
                            routine_name: routine_name.clone(),
                            processor_manifest: Arc::clone(processor),
                            routine_manifest: Arc::clone(routine),
                            read_tags,
                            read_keys,
                        });
                    }
                }
            }
        }
        self.triage_read_tags = triage_read_tags;
        self.triage_read_keys = triage_read_keys;
        self.facet_reference_specs = facet_reference_specs;
        Ok(())
    }

    #[tracing::instrument(skip(self, doc, doc_heads, ctx))]
    async fn triage_doc(
        &mut self,
        ctx: &SwitchSinkCtx<'_>,
        doc_id: &DocId,
        doc_heads: &ChangeHashSet,
        doc: &Doc,
        branch_path: daybook_types::doc::BranchPath,
        changed_facet_keys: Option<&HashSet<FacetKey>>,
    ) -> Res<()> {
        let rt = ctx
            .rt
            .ok_or_else(|| ferr!("triage listener context missing rt"))?;
        let store = ctx
            .store
            .ok_or_else(|| ferr!("triage listener context missing store"))?;
        debug!(
            processor_count = self.cached_processors.len(),
            "triaging doc"
        );
        let mut full_doc_for_reference_predicates: Option<Option<Arc<Doc>>> = None;
        for processor in &self.cached_processors {
            if let Some(changed) = changed_facet_keys {
                if !changed_intersects_read_set(changed, &processor.read_tags, &processor.read_keys)
                {
                    continue;
                }
            }
            let predicate = match &processor.processor_manifest.deets {
                ProcessorDeets::DocProcessor { predicate, .. } => predicate,
            };
            let predicate_doc = if predicate_contains_reference_clause(predicate) {
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
                    .and_then(|opt| opt.as_deref())
                {
                    Some(doc) => doc,
                    None => continue,
                }
            } else {
                doc
            };
            let predicate_match = doc_predicate_matches_with_reference_specs(
                predicate,
                predicate_doc,
                &self.facet_reference_specs,
            );
            if !predicate_match {
                continue;
            }
            info!(
                plug_id = %processor.plug_id,
                routine_name = %processor.routine_name,
                ?doc_id,
                "dispatching job"
            );
            let args = match &processor.routine_manifest.deets {
                RoutineManifestDeets::DocInvoke {} => DispatchArgs::DocInvoke {
                    doc_id: doc_id.clone(),
                    branch_path: branch_path.clone(),
                    heads: doc_heads.clone(),
                },
                RoutineManifestDeets::DocFacet { .. } => DispatchArgs::DocFacet {
                    doc_id: doc_id.clone(),
                    branch_path: branch_path.clone(),
                    heads: doc_heads.clone(),
                    facet_key: None,
                },
            };
            let job_key = format!(
                "{}:{}:{}",
                doc_id, processor.plug_id, processor.routine_name.0
            );
            let old_dispatch = store
                .query_sync(|store| store.job_to_dispatch.get(&job_key).cloned())
                .await;
            if let Some(dispatch_id) = old_dispatch {
                info!(
                    ?dispatch_id,
                    "inflight job already exists; skipping redispatch"
                );
                continue;
            }
            let dispatch_id = rt
                .dispatch(&processor.plug_id, &processor.routine_name.0, args)
                .await?;
            store
                .mutate_sync(|store| {
                    store.job_to_dispatch.insert(job_key, dispatch_id.clone());
                    store.dispatch_to_job.insert(
                        dispatch_id,
                        (
                            doc_id.clone(),
                            processor.plug_id.clone(),
                            processor.routine_name.0.clone(),
                        ),
                    );
                })
                .await?;
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
            SwitchEvent::Dispatch(event) => {
                if let DispatchEvent::DispatchDeleted { id, .. } = &**event {
                    let store = ctx
                        .store
                        .ok_or_else(|| ferr!("triage listener context missing store"))?;
                    store
                        .mutate_sync(|store| {
                            if let Some(job) = store.dispatch_to_job.remove(id) {
                                let job_key = format!("{}:{}:{}", job.0, job.1, job.2);
                                store.job_to_dispatch.remove(&job_key);
                            }
                        })
                        .await?;
                }
            }
            SwitchEvent::Drawer(event) => match &**event {
                DrawerEvent::ListChanged { .. } => {}
                DrawerEvent::DocDeleted { .. } => {}
                DrawerEvent::DocAdded {
                    id,
                    entry,
                    drawer_heads,
                } => {
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPath::from(branch_name.as_str());
                        if branch_path.to_string_lossy().starts_with("/tmp/") {
                            continue;
                        }
                        let rt = ctx
                            .rt
                            .ok_or_else(|| ferr!("triage listener context missing rt"))?;
                        let Some(facet_keys_set) = rt
                            .drawer
                            .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                            .await?
                        else {
                            continue;
                        };
                        let changed_facet_keys_set: HashSet<FacetKey> =
                            facet_keys_set.iter().cloned().collect();
                        let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                        self.triage_doc(
                            ctx,
                            id,
                            heads,
                            &meta_doc,
                            branch_path,
                            Some(&changed_facet_keys_set),
                        )
                        .await
                        .wrap_err("error triaging doc")?;
                    }
                }
                DrawerEvent::DocUpdated {
                    id,
                    entry,
                    diff,
                    drawer_heads,
                    ..
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
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPath::from(branch_name.as_str());
                        if branch_path.to_string_lossy().starts_with("/tmp/") {
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
                            .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                            .await?
                        else {
                            debug!(?id, ?branch_path, "skipping triage for stale heads");
                            continue;
                        };
                        let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                        self.triage_doc(
                            ctx,
                            id,
                            heads,
                            &meta_doc,
                            branch_path,
                            changed_facet_keys_set.as_ref(),
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

fn doc_predicate_matches_with_reference_specs(
    predicate: &crate::plugs::manifest::DocPredicateClause,
    doc: &Doc,
    facet_reference_specs: &HashMap<String, Vec<FacetReferenceManifest>>,
) -> bool {
    use crate::plugs::manifest::DocPredicateClause;

    match predicate {
        DocPredicateClause::HasTag(tag) => {
            doc.facets.keys().any(|key| key.tag.to_string() == tag.0)
        }
        DocPredicateClause::HasReferenceToTag {
            source_tag,
            target_tag,
        } => doc_has_manifest_declared_reference_to_tag(
            doc,
            &source_tag.0,
            &target_tag.0,
            facet_reference_specs,
        ),
        DocPredicateClause::Or(clauses) => clauses.iter().any(|clause| {
            doc_predicate_matches_with_reference_specs(clause, doc, facet_reference_specs)
        }),
        DocPredicateClause::And(clauses) => clauses.iter().all(|clause| {
            doc_predicate_matches_with_reference_specs(clause, doc, facet_reference_specs)
        }),
        DocPredicateClause::Not(clause) => {
            !doc_predicate_matches_with_reference_specs(clause, doc, facet_reference_specs)
        }
    }
}

fn predicate_contains_reference_clause(
    predicate: &crate::plugs::manifest::DocPredicateClause,
) -> bool {
    use crate::plugs::manifest::DocPredicateClause;
    match predicate {
        DocPredicateClause::HasReferenceToTag { .. } => true,
        DocPredicateClause::HasTag(_) => false,
        DocPredicateClause::Or(clauses) | DocPredicateClause::And(clauses) => {
            clauses.iter().any(predicate_contains_reference_clause)
        }
        DocPredicateClause::Not(clause) => predicate_contains_reference_clause(clause),
    }
}

fn doc_has_manifest_declared_reference_to_tag(
    doc: &Doc,
    source_tag: &str,
    target_tag: &str,
    facet_reference_specs: &HashMap<String, Vec<FacetReferenceManifest>>,
) -> bool {
    let Some(reference_specs) = facet_reference_specs.get(source_tag) else {
        return false;
    };

    for (facet_key, facet_raw) in &doc.facets {
        if facet_key.tag.to_string() != source_tag {
            continue;
        }
        if facet_has_reference_to_tag(facet_raw, target_tag, reference_specs) {
            return true;
        }
    }
    false
}

fn facet_has_reference_to_tag(
    facet_raw: &serde_json::Value,
    target_tag: &str,
    reference_specs: &[FacetReferenceManifest],
) -> bool {
    for reference_spec in reference_specs {
        match reference_spec.reference_kind {
            FacetReferenceKind::UrlFacet => {}
        }

        let selected_values = match select_json_path_values(facet_raw, &reference_spec.json_path) {
            Ok(values) => values,
            Err(err) => {
                debug!(error = %err, json_path = %reference_spec.json_path, "invalid facet reference json_path");
                continue;
            }
        };

        for selected in selected_values {
            let url_strings: Vec<&str> = match selected {
                serde_json::Value::String(value) => vec![value.as_str()],
                serde_json::Value::Array(items) => {
                    items.iter().filter_map(|item| item.as_str()).collect()
                }
                _ => Vec::new(),
            };

            for url_str in url_strings {
                let Ok(parsed_url) = url::Url::parse(url_str) else {
                    continue;
                };
                let Ok(parsed_ref) = daybook_types::url::parse_facet_ref(&parsed_url) else {
                    continue;
                };
                if parsed_ref.facet_key.tag.to_string() == target_tag {
                    return true;
                }
            }
        }
    }
    false
}

pub fn doc_processor_triage_listener() -> Box<dyn SwitchSink + Send + Sync> {
    Box::<DocProcessorTriageListener>::default()
}
