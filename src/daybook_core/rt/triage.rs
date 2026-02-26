use crate::interlude::*;

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
    DocPredicateEvalMode, DocPredicateEvalRequirement, DocPredicateEvalResolved,
    FacetReferenceManifest, KeyGeneric, ProcessorDeets, ProcessorManifest, RoutineManifest,
    RoutineManifestDeets,
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
    facet_reference_specs: Arc<HashMap<String, Vec<FacetReferenceManifest>>>,
    predicate_requirements: HashSet<DocPredicateEvalRequirement>,
    predicate_resolved: HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
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
        self.facet_reference_specs = Arc::new(facet_reference_specs);
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
                        let predicate_doc_arc = predicate_doc_arc.expect(
                            "FullDoc requirement implies full doc was loaded and cached",
                        );
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

pub fn doc_processor_triage_listener() -> Box<dyn SwitchSink + Send + Sync> {
    Box::<DocProcessorTriageListener>::default()
}
