use crate::interlude::*;
use big_repo::BigRepoLocalFilter;

use super::{DrawerRepo, MaterializationWake};

use crate::drawer::{
    ExactFacetHydration, ExactFacetValueHydration, dmeta, facet_recovery,
    types::{DocBundle, DocEntry, DocNBranches},
};
use crate::index::doc_delta::{BranchIdentity, BranchIdentityResolution};

use automerge::ReadDoc;
use daybook_types::doc::{
    BranchId, ChangeHashSet, Doc, DocId, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};

// queries
impl DrawerRepo {
    pub(crate) async fn subscribe_materialization_wake(
        &self,
        physical_branch_id: Option<&BranchId>,
    ) -> Res<MaterializationWake> {
        let doc_id = physical_branch_id
            .map(|branch_id| branch_id.0.parse::<big_repo::DocumentId>())
            .transpose()?;
        let (registration, receiver) = self
            .big_repo
            .subscribe_local_listener(BigRepoLocalFilter {
                doc_id: doc_id.map(big_repo::BigRepoDocIdFilter::new),
            })
            .await?;
        Ok(MaterializationWake {
            _registration: registration,
            receiver,
        })
    }

    /// Resolve only the system Branch facet at exact heads.
    ///
    /// This is intentionally narrower than the ordinary facet hydration APIs:
    /// DocDelta tracking must not hydrate dmeta or user facets.
    pub(crate) async fn resolve_system_branch_identity_at_heads(
        &self,
        physical_branch_id: &BranchId,
        heads: &ChangeHashSet,
    ) -> Res<BranchIdentityResolution> {
        let physical_id = physical_branch_id.0.parse::<big_repo::DocumentId>()?;
        let handle = match self.big_repo.get_doc(&physical_id).await? {
            big_repo::DocLookup::Ready(handle) => handle,
            big_repo::DocLookup::Missing | big_repo::DocLookup::PendingMaterialization => {
                return Ok(BranchIdentityResolution::Deferred);
            }
        };
        let branch_key = FacetKey::from(WellKnownFacetTag::Branch).to_string();
        let path = vec![
            "facets".into(),
            autosurgeon::Prop::Key(branch_key.clone().into()),
        ];
        let Some(raw) = handle
            .hydrate_path_at_heads::<ThroughJson<FacetRaw>>(&heads.0, automerge::ROOT, path)
            .await
            .wrap_err("hydrate system Branch facet at exact heads")?
        else {
            return Ok(BranchIdentityResolution::Ignored);
        };
        let branch = match WellKnownFacet::from_json(raw.0, WellKnownFacetTag::Branch)
            .wrap_err("decode Branch facet")?
        {
            WellKnownFacet::Branch(value) => value,
            _ => unreachable!("Branch facet decoded to another well-known variant"),
        };
        if branch.branch_id != *physical_branch_id {
            // Merging one branch into another imports the source Automerge changes
            // into the destination sedimentree. Historical events for those imported
            // changes still resolve to the source Branch facet until the destination's
            // identity-restoration commit is reached. They are known foreign history,
            // not unresolved materialization: skip them and project the complete merged
            // state when the restoration commit arrives.
            tracing::debug!(
                physical_branch_id = %physical_branch_id.0,
                imported_branch_id = %branch.branch_id.0,
                "ignoring imported branch-history event in destination sedimentree"
            );
            return Ok(BranchIdentityResolution::ImportedHistory);
        }
        Ok(BranchIdentityResolution::Found(BranchIdentity {
            document_id: branch.document_id,
            branch_id: branch.branch_id,
        }))
    }

    /// Hydrate the dmeta-derived current facet state at exact heads.
    ///
    /// Only the Branch and Dmeta system facets are read as values. User facet
    /// values are deliberately not hydrated: dmeta is the source of truth for
    /// current membership, while `facet_snapshot_metadata` supplies the
    /// exact heads and provenance needed by downstream projections.
    pub(crate) async fn hydrate_dmeta_state_at_heads(
        &self,
        physical_branch_id: &BranchId,
        document_id: &DocId,
        branch_heads: ChangeHashSet,
    ) -> Res<Option<crate::drawer::ExactDmetaState>> {
        let physical_id = physical_branch_id.0.parse::<big_repo::DocumentId>()?;
        let handle = match self.big_repo.get_doc(&physical_id).await? {
            big_repo::DocLookup::Ready(handle) => handle,
            big_repo::DocLookup::Missing | big_repo::DocLookup::PendingMaterialization => {
                return Ok(None);
            }
        };

        let branch_key = FacetKey::from(WellKnownFacetTag::Branch);
        let branch_raw = handle
            .hydrate_path_at_heads::<ThroughJson<FacetRaw>>(
                &branch_heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(branch_key.to_string().into()),
                ],
            )
            .await
            .wrap_err("hydrate Branch facet at exact heads")?
            .ok_or_else(|| ferr!("missing mandatory Branch facet"))?;
        let branch = match WellKnownFacet::from_json(branch_raw.0, WellKnownFacetTag::Branch)
            .wrap_err("decode Branch facet")?
        {
            WellKnownFacet::Branch(value) => value,
            _ => unreachable!("Branch facet decoded to another well-known variant"),
        };
        if branch.branch_id != *physical_branch_id {
            return Err(ferr!("physical branch id does not match Branch facet"));
        }
        if branch.document_id != *document_id {
            return Err(ferr!("logical document id does not match Branch facet"));
        }

        let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
        let dmeta_raw = handle
            .hydrate_path_at_heads::<ThroughJson<FacetRaw>>(
                &branch_heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(dmeta_key.to_string().into()),
                ],
            )
            .await
            .wrap_err("hydrate Dmeta facet at exact heads")?
            .ok_or_else(|| ferr!("missing mandatory Dmeta facet"))?;
        let dmeta = match WellKnownFacet::from_json(dmeta_raw.0, WellKnownFacetTag::Dmeta)
            .wrap_err("decode Dmeta facet")?
        {
            WellKnownFacet::Dmeta(value) => value,
            _ => unreachable!("Dmeta facet decoded to another well-known variant"),
        };
        if dmeta.id != *document_id {
            return Err(ferr!("dmeta document id does not match Branch facet"));
        }
        let dmeta_id = dmeta.id.clone();

        let all_facet_keys = dmeta
            .facets
            .keys()
            .filter(|key| {
                **key != branch_key
                    && **key != FacetKey::from(WellKnownFacetTag::Branches)
                    && **key != dmeta_key
            })
            .cloned()
            .collect();
        let mut facets = HashMap::new();
        for (key, meta) in dmeta.facets {
            if key == branch_key
                || key == FacetKey::from(WellKnownFacetTag::Branches)
                || !meta.deleted_at.is_empty()
            {
                continue;
            }
            let (facet_heads, actor_id) = handle
                .with_document_read(|doc| {
                    crate::drawer::facet_snapshot_metadata(doc, &key, &branch_heads.0)
                })
                .await?;
            facets.insert(key, (facet_heads, actor_id));
        }
        let dmeta_actor_id = handle
            .with_document_read(|doc| {
                doc.get_changes(&[])
                    .last()
                    .map(|change| change.actor_id().clone())
                    .ok_or_else(|| ferr!("dmeta facet has no write point"))
            })
            .await?;
        let dmeta_heads = branch_heads.clone();
        facets.insert(dmeta_key, (dmeta_heads, dmeta_actor_id));
        Ok(Some(crate::drawer::ExactDmetaState {
            document_id: dmeta_id,
            branch_id: branch.branch_id,
            branch_heads,
            facets,
            all_facet_keys,
        }))
    }

    /// Adapt dmeta-only exact-head hydration to the legacy metadata result
    /// consumed by the in-flight projection code. No user facet value is read.
    #[allow(dead_code)]
    pub(crate) async fn hydrate_facet_at_heads(
        &self,
        physical_branch_id: &BranchId,
        document_id: &DocId,
        branch_heads: ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<ExactFacetHydration> {
        let Some(state) = self
            .hydrate_dmeta_state_at_heads(physical_branch_id, document_id, branch_heads.clone())
            .await?
        else {
            return Ok(ExactFacetHydration::Deferred);
        };
        let Some((facet_heads, actor_id)) = state.facets.get(facet_key).cloned() else {
            return Ok(ExactFacetHydration::Absent);
        };
        Ok(ExactFacetHydration::Present {
            branch_heads,
            facet_heads,
            actor_id,
        })
    }

    /// Hydrate one user-facet value at exact physical branch heads. The
    /// drawer owns the BigRepo handle and exposes materialization separately
    /// so consumers can retain their revision and retry after a wakeup.
    pub(crate) async fn hydrate_facet_value_at_heads(
        &self,
        physical_branch_id: &BranchId,
        branch_heads: &ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<ExactFacetValueHydration> {
        let physical_id = physical_branch_id.0.parse::<big_repo::DocumentId>()?;
        let handle = match self.big_repo.get_doc(&physical_id).await? {
            big_repo::DocLookup::Ready(handle) => handle,
            big_repo::DocLookup::Missing | big_repo::DocLookup::PendingMaterialization => {
                return Ok(ExactFacetValueHydration::Deferred);
            }
        };
        let value = handle
            .hydrate_path_at_heads::<ThroughJson<FacetRaw>>(
                &branch_heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(facet_key.to_string().into()),
                ],
            )
            .await
            .wrap_err("hydrate facet value at exact heads")?;
        Ok(value.map_or(ExactFacetValueHydration::Absent, |value| {
            ExactFacetValueHydration::Present(value.0)
        }))
    }

    /// Hydrate one physical document at exact heads for DocDelta projection.
    pub(crate) async fn hydrate_physical_doc_at_heads(
        &self,
        physical_id: big_repo::DocumentId,
        heads: ChangeHashSet,
    ) -> Res<Option<HashMap<FacetKey, FacetRaw>>> {
        let handle = match self.big_repo.get_doc(&physical_id).await? {
            big_repo::DocLookup::Ready(handle) => handle,
            big_repo::DocLookup::Missing | big_repo::DocLookup::PendingMaterialization => {
                return Ok(None);
            }
        };
        handle
            .hydrate_path_at_heads::<ThroughJson<HashMap<FacetKey, FacetRaw>>>(
                &heads.0,
                automerge::ROOT,
                vec!["facets".into()],
            )
            .await
            .wrap_err("hydrate physical document facets at exact heads")
            .map(|value| value.map(|value| value.0))
    }

    pub fn get_drawer_heads(&self) -> ChangeHashSet {
        surelock::key::lock_scope(|key| {
            let (heads, _key) = key.lock(&self.current_heads);
            heads.clone()
        })
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn list_just_ids(&self) -> Res<(ChangeHashSet, Vec<String>)> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (drawer_heads, entries) = self.current_drawer_entries().await?;
        {
            surelock::key::lock_scope(|key| {
                key.lock_with(
                    &(&self.entry_pool, &self.entry_cache),
                    |(mut pool, mut cache)| {
                        for (doc_id, entry) in &entries {
                            let pruned = pool.insert_key(doc_id, 1);
                            for pkey in pruned {
                                cache.remove(&pkey);
                            }
                            cache.insert(doc_id.clone(), entry.clone());
                        }
                    },
                );
            });
        }
        let mut results = entries
            .into_iter()
            .map(|(doc_id, _)| doc_id.to_string())
            .collect::<Vec<_>>();
        results.sort();
        Ok((drawer_heads, results))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn list(&self) -> Res<Vec<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (_drawer_heads, entries) = self.current_drawer_entries().await?;
        let mut results = Vec::with_capacity(entries.len());
        for (doc_id, entry) in entries {
            results.push(
                self.current_doc_branches_from_entry(&doc_id, &entry)
                    .await?,
            );
        }
        Ok(results)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%doc_id))]
    pub async fn get_entry_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let current_heads = surelock::key::lock_scope(|key| {
            let (heads, _key) = key.lock(&self.current_heads);
            heads.clone()
        });
        if heads == &current_heads {
            return self.get_entry(doc_id).await;
        }
        self.hydrate_entry_at_heads(doc_id, heads).await
    }

    #[tracing::instrument(skip_all, fields(%doc_id))]
    pub async fn get_entry(&self, doc_id: &DocId) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        if let Some(cached) = surelock::key::lock_scope(|key| {
            key.lock_with(
                &(&self.entry_pool, &self.entry_cache),
                |(mut pool, cache)| {
                    cache.get(doc_id).map(|entry| {
                        pool.touch_key(doc_id);
                        entry.clone()
                    })
                },
            )
            .0
        }) {
            return Ok(Some(cached));
        }

        let heads = surelock::key::lock_scope(|key| {
            let (heads, _key) = key.lock(&self.current_heads);
            heads.clone()
        });
        debug!(%doc_id, "presence probe: entry cache miss, hydrating from drawer doc");
        let mut entry = self.hydrate_entry_at_heads(doc_id, &heads).await?;
        if entry.is_none() {
            let live_heads = self
                .drawer_doc_handle
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await;
            if live_heads != heads {
                entry = self.hydrate_entry_at_heads(doc_id, &live_heads).await?;
                if entry.is_some() {
                    surelock::key::lock_scope(|key| {
                        let (mut current_heads, _key) = key.lock(&self.current_heads);
                        if *current_heads == heads {
                            *current_heads = live_heads;
                        }
                    });
                }
            }
        }
        debug!(%doc_id, found = entry.is_some(), "presence probe: hydrated entry");

        if let Some(entry) = entry {
            surelock::key::lock_scope(|key| {
                key.lock_with(
                    &(&self.entry_pool, &self.entry_cache),
                    |(mut pool, mut cache)| {
                        let pruned = pool.insert_key(doc_id, 1);
                        for pkey in pruned {
                            cache.remove(&pkey);
                        }
                        cache.insert(doc_id.clone(), entry.clone());
                    },
                );
            });
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
    /// Fetch facets at branch heads with Arc-backed values to avoid deep-cloning on cache hits.
    pub(crate) async fn get_at_branch_heads_with_facets_arc(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<
        Option<(
            HashMap<FacetKey, daybook_types::doc::ArcFacetRaw>,
            HashMap<FacetKey, ChangeHashSet>,
        )>,
    > {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let Some(handle) = self
            .resolve_handle_for_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            return Ok(None);
        };

        // Facet hydration must happen under the doc lock, but the facet cache
        // lives in separate surelock mutexes -- surelock forbids nesting a
        // second scope on the same thread, so cache probes cannot run while
        // the doc scope is active. Plan: pass 1 under the doc lock resolves
        // uuids/heads and hydrates the facets that aren't cache-eligible;
        // cache probes run unlocked; a second doc pass hydrates the misses.
        let (mut facets, facet_heads_by_key, to_probe) = handle
            .with_document_read(|am_doc| {
                let mut facets = HashMap::new();
                let mut facet_heads_by_key = HashMap::new();
                let mut to_probe = Vec::new();
                let facets_obj =
                    match automerge::ReadDoc::get_at(am_doc, automerge::ROOT, "facets", heads)? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ if facet_keys.is_none() => {
                            return eyre::Ok((facets, facet_heads_by_key, to_probe));
                        }
                        _ => {
                            eyre::bail!("facets object not found in content doc");
                        }
                    };

                let selected_keys: Vec<FacetKey> = match &facet_keys {
                    Some(keys) => keys.clone(),
                    None => automerge::ReadDoc::map_range_at(am_doc, &facets_obj, .., heads)
                        .map(|item| {
                            let key_str = item.key.to_string();
                            FacetKey::from(key_str.as_str())
                        })
                        .collect(),
                };

                for key in selected_keys {
                    let facet_uuid = dmeta::facet_uuid_for_key_at(am_doc, &key, heads)?;
                    let facet_heads = if facet_uuid.is_some() {
                        Some(dmeta::facet_heads_for_key_at(am_doc, &key, heads)?)
                    } else {
                        None
                    };
                    if let Some(meta_heads) = &facet_heads {
                        facet_heads_by_key.insert(key.clone(), meta_heads.clone());
                    }

                    if let (Some(uuid), Some(meta_heads)) = (facet_uuid, facet_heads) {
                        // Cache-eligible: probe the cache outside the doc scope.
                        to_probe.push((key, uuid, meta_heads));
                        continue;
                    }

                    let key_str = key.to_string();
                    if automerge::ReadDoc::get_at(am_doc, &facets_obj, &*key_str, heads)?.is_some()
                    {
                        let value: Option<ThroughJson<FacetRaw>> =
                            autosurgeon::hydrate_prop_at(am_doc, &facets_obj, &*key_str, heads)?;
                        if let Some(facet_value) = value {
                            facets.insert(key, Arc::new(facet_value.0));
                        }
                    }
                }
                eyre::Ok((facets, facet_heads_by_key, to_probe))
            })
            .await?;

        // Cache probes (no doc scope active).
        let mut misses: Vec<(FacetKey, Uuid, ChangeHashSet)> = Vec::new();
        for (key, uuid, meta_heads) in to_probe {
            if let Some(cached) = self.facet_cache_get(doc_id, &uuid, &meta_heads) {
                facets.insert(key, cached);
            } else {
                misses.push((key, uuid, meta_heads));
            }
        }

        // Second doc pass: hydrate only the cache misses.
        let mut to_cache = Vec::new();
        if !misses.is_empty() {
            let (hydrated, hydrated_to_cache) = handle
                .with_document_read(|am_doc| {
                    let facets_obj =
                        match automerge::ReadDoc::get_at(am_doc, automerge::ROOT, "facets", heads)?
                        {
                            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                            _ => {
                                eyre::bail!("facets object not found in content doc");
                            }
                        };
                    let mut hydrated = HashMap::new();
                    let mut hydrated_to_cache = Vec::new();
                    for (key, uuid, meta_heads) in &misses {
                        let key_str = key.to_string();
                        let value: Option<ThroughJson<FacetRaw>> =
                            autosurgeon::hydrate_prop_at(am_doc, &facets_obj, &*key_str, heads)?;
                        if let Some(facet_value) = value {
                            let facet_value = Arc::new(facet_value.0);
                            hydrated.insert(key.clone(), Arc::clone(&facet_value));
                            hydrated_to_cache.push((*uuid, meta_heads.clone(), facet_value));
                        }
                    }
                    eyre::Ok((hydrated, hydrated_to_cache))
                })
                .await?;
            facets.extend(hydrated);
            to_cache.extend(hydrated_to_cache);
        }

        for (uuid, heads, value) in to_cache {
            self.facet_cache_put(doc_id, uuid, heads, value);
        }

        Ok(Some((facets, facet_heads_by_key)))
    }
    /// Get a doc at specific branch.
    #[tracing::instrument(level = "trace", skip_all, fields(%doc_id, %branch_path))]
    pub async fn get_doc_with_facets_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let Some(branch_heads) = self.get_branch_heads_for_path(doc_id, branch_path).await? else {
            return Ok(None);
        };

        self.get_doc_with_facets_at_branch_heads(doc_id, branch_path, &branch_heads, facet_keys)
            .await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%doc_id))]
    pub async fn get_doc_branches(&self, doc_id: &DocId) -> Res<Option<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.current_doc_branches(doc_id).await
    }

    /// Get a doc at specific branch heads (exact version).
    #[tracing::instrument(level = "trace", skip_all, fields(%id, %branch_path))]
    pub async fn get_doc_with_facets_at_branch_heads(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let facets = self
            .get_at_branch_heads_with_facets_arc(id, branch_path, heads, facet_keys)
            .await?
            .map(|(facets, _)| facets);
        let Some(facets) = facets else {
            return Ok(None);
        };
        let facets = facets
            .into_iter()
            .map(|(key, value)| (key, value.as_ref().clone()))
            .collect();
        Ok(Some(Arc::new(Doc {
            id: id.clone(),
            facets,
        })))
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%doc_id, %branch_path))]
    pub async fn get_doc_bundle_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<DocBundle>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(entry) = self.get_entry(doc_id).await? else {
            return Ok(None);
        };
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(branch_ref.branch_doc_id)
            .await?
        else {
            return Ok(None);
        };
        let branch_heads = handle
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await;
        let Some((facets, facet_heads_by_key)) = self
            .get_at_branch_heads_with_facets_arc(doc_id, branch_path, &branch_heads, facet_keys)
            .await?
        else {
            return Ok(None);
        };
        let doc = Doc {
            id: doc_id.clone(),
            facets: facets
                .into_iter()
                .map(|(key, value)| (key, value.as_ref().clone()))
                .collect(),
        };
        Ok(Some(DocBundle {
            doc,
            entry,
            branch_heads,
            facet_heads_by_key,
        }))
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%doc_id, %branch_path))]
    pub async fn get_with_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_heads) = self.get_branch_heads_for_path(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let doc = self
            .get_doc_with_facets_at_branch_heads(doc_id, branch_path, &branch_heads, facet_keys)
            .await?;
        Ok(doc.map(|doc| (doc, branch_heads)))
    }

    pub async fn get_if_latest(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_heads) = self.get_branch_heads_for_path(doc_id, branch_path).await? else {
            return Ok(None);
        };
        if &branch_heads != heads {
            return Ok(None);
        }
        self.get_doc_with_facets_at_branch_heads(doc_id, branch_path, heads, facet_keys)
            .await
    }

    /// Returns the set of facet keys present for the doc at branch heads, without hydrating facet values.
    pub async fn facet_keys_at_branch_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self
            .resolve_handle_for_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            return Ok(None);
        };
        let keys = handle
            .with_document_read(|am_doc| {
                let facets_obj =
                    match automerge::ReadDoc::get_at(am_doc, automerge::ROOT, "facets", heads)? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => return Ok::<HashSet<FacetKey>, eyre::Report>(HashSet::new()),
                    };
                let mut out = HashSet::new();
                for item in automerge::ReadDoc::map_range_at(am_doc, &facets_obj, .., heads) {
                    let key_str = item.key.to_string();
                    out.insert(FacetKey::from(key_str.as_str()));
                }
                Ok(out)
            })
            .await?;
        Ok(Some(keys))
    }

    /// Like get_if_latest but returns only facet keys (no facet values). Returns None if branch heads are stale.
    pub async fn get_facet_keys_if_latest(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_heads) = self.get_branch_heads_for_path(doc_id, branch_path).await? else {
            return Ok(None);
        };
        if &branch_heads == heads {
            return self
                .facet_keys_at_branch_heads(doc_id, branch_path, heads)
                .await;
        }
        Ok(None)
    }

    pub async fn get_facet_heads_at_branch_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<Vec<automerge::ChangeHash>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self
            .resolve_handle_for_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            eyre::bail!("doc not found");
        };
        handle
            .with_document_read(|am_doc| {
                facet_recovery::recover_facet_heads_at(am_doc, facet_key, heads)
            })
            .await
    }

    pub async fn get_facet_heads_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_key: &FacetKey,
    ) -> Res<Option<Vec<automerge::ChangeHash>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_heads) = self.get_branch_heads_for_path(doc_id, branch_path).await? else {
            return Ok(None);
        };
        self.get_facet_heads_at_branch_heads(doc_id, branch_path, &branch_heads, facet_key)
            .await
            .map(Some)
    }

    pub async fn facet_keys_touched_by_local_actor(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: &[FacetKey],
    ) -> Res<HashSet<FacetKey>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self
            .resolve_handle_for_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            return Ok(HashSet::new());
        };
        let branch_doc_id = handle.document_id();
        let local_user_path = self.local_user_path.clone();
        let mut local_actor_ids = HashSet::from([
            self.local_actor_id.clone(),
            self.content_actor_id(None, branch_doc_id),
        ]);
        if let Some(doc) = self
            .get_doc_with_facets_at_branch_heads(
                doc_id,
                branch_path,
                heads,
                Some(vec![FacetKey::from(
                    daybook_types::doc::WellKnownFacetTag::Dmeta,
                )]),
            )
            .await?
            && let Some(raw) = doc.facets.get(&FacetKey::from(
                daybook_types::doc::WellKnownFacetTag::Dmeta,
            ))
            && let Ok(WellKnownFacet::Dmeta(dmeta)) =
                serde_json::from_value::<WellKnownFacet>(raw.clone())
        {
            let local_segments: Vec<&str> = local_user_path
                .as_str()
                .trim_start_matches('/')
                .split('/')
                .collect();
            for user_meta in dmeta.actors.values() {
                let user_segments: Vec<&str> = user_meta
                    .user_path
                    .as_str()
                    .trim_start_matches('/')
                    .split('/')
                    .collect();
                if local_segments.first() == user_segments.first()
                    && local_segments.get(1) == user_segments.get(1)
                {
                    local_actor_ids
                        .insert(self.content_actor_id(Some(&user_meta.user_path), branch_doc_id));
                }
            }
        }
        let mut out = HashSet::new();
        for key in facet_keys {
            let local_actor_ids = local_actor_ids.clone();
            let facet_heads = self
                .get_facet_heads_at_branch_heads(doc_id, branch_path, heads, key)
                .await?;
            let is_local = handle
                .with_document_read(|am_doc| {
                    for head in &facet_heads {
                        if let Some(change) = am_doc.get_change_by_hash(head)
                            && local_actor_ids.contains(change.actor_id())
                        {
                            return true;
                        }
                    }
                    false
                })
                .await;
            if is_local {
                out.insert(key.clone());
            }
        }
        Ok(out)
    }

    /// ADR 007 §7: the write points (heads + author) of a facet between two
    /// head sets, oldest first. Each write's facet content and dmeta marker
    /// live in the same change, so hydrating at a write point's heads yields
    /// a consistent snapshot; the author enables local-change filtering.
    pub(crate) async fn get_facet_write_points(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_key: &FacetKey,
        from: &[automerge::ChangeHash],
        to: &[automerge::ChangeHash],
    ) -> Res<Vec<(ChangeHashSet, ActorId)>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(vec![]);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(branch_ref.branch_doc_id)
            .await?
        else {
            return Ok(vec![]);
        };
        handle
            .with_document_read(|am_doc| {
                crate::drawer::facet_recovery::facet_write_points(am_doc, facet_key, from, to)
            })
            .await
    }
}
