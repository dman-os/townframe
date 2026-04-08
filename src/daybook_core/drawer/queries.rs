use crate::interlude::*;

use super::DrawerRepo;

use crate::drawer::{
    dmeta, facet_recovery,
    types::{DocBundle, DocEntry, DocNBranches},
};

use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, Doc, DocId, FacetKey, FacetRaw};

// queries
impl DrawerRepo {
    pub fn get_drawer_heads(&self) -> ChangeHashSet {
        self.current_heads.lock().expect(ERROR_MUTEX).clone()
    }

    pub async fn list_just_ids(&self) -> Res<(ChangeHashSet, Vec<String>)> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (drawer_heads, entries) = self.current_drawer_entries()?;
        {
            let mut pool = self.entry_pool.lock().unwrap();
            for (doc_id, entry) in &entries {
                let pruned = pool.insert_key(doc_id, 1);
                for pkey in pruned {
                    self.entry_cache.remove(&pkey);
                }
                self.entry_cache.insert(doc_id.clone(), entry.clone());
            }
        }
        let mut results = entries
            .into_iter()
            .map(|(doc_id, _)| doc_id.to_string())
            .collect::<Vec<_>>();
        results.sort();
        Ok((drawer_heads, results))
    }
    pub async fn list(&self) -> Res<Vec<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (_drawer_heads, doc_ids) = self.list_just_ids().await?;
        let mut results = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(entry) = self.current_doc_branches(&DocId::from(doc_id)).await? {
                results.push(entry);
            }
        }
        Ok(results)
    }

    pub async fn get_entry_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let current_heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
        if heads == &current_heads {
            return self.get_entry(doc_id).await;
        }
        self.hydrate_entry_at_heads(doc_id, heads).await
    }

    pub async fn get_entry(&self, doc_id: &DocId) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        if let Some(cached) = self.entry_cache.get(doc_id) {
            let mut pool = self.entry_pool.lock().unwrap();
            pool.touch_key(doc_id);
            return Ok(Some(cached.clone()));
        }

        let heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
        let entry = self.hydrate_entry_at_heads(doc_id, &heads).await?;

        if let Some(entry) = entry {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(doc_id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(doc_id.clone(), entry.clone());
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
    /// Fetch facets at given heads with Arc-backed values to avoid deep-cloning on cache hits.
    pub(crate) async fn get_at_heads_with_facets_arc(
        &self,
        doc_id: &DocId,
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

        let Some(handle) = self.resolve_handle_for_heads(doc_id, heads).await? else {
            return Ok(None);
        };

        let (facets, facet_heads_by_key, to_cache) = handle
            .with_document_local(|am_doc| {
                let mut facets = HashMap::new();
                let mut facet_heads_by_key = HashMap::new();
                let mut to_cache = Vec::new();

                match &facet_keys {
                    None => {
                        let full: ThroughJson<Doc> = autosurgeon::hydrate_at(am_doc, heads)?;
                        for (key, value) in full.0.facets {
                            let value = Arc::new(value);
                            facets.insert(key.clone(), Arc::clone(&value));
                            if let Some(facet_uuid) =
                                dmeta::facet_uuid_for_key_at(am_doc, &key, heads)?
                            {
                                let facet_heads =
                                    dmeta::facet_heads_for_key_at(am_doc, &key, heads)?;
                                facet_heads_by_key.insert(key, facet_heads.clone());
                                to_cache.push((facet_uuid, facet_heads, value));
                            }
                        }
                    }
                    Some(keys) => {
                        let facets_obj = match automerge::ReadDoc::get_at(
                            am_doc,
                            automerge::ROOT,
                            "facets",
                            heads,
                        )? {
                            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                            _ => eyre::bail!("facets object not found in content doc"),
                        };
                        for key in keys {
                            let facet_uuid = dmeta::facet_uuid_for_key_at(am_doc, key, heads)?;
                            let facet_heads = if facet_uuid.is_some() {
                                Some(dmeta::facet_heads_for_key_at(am_doc, key, heads)?)
                            } else {
                                None
                            };
                            if let Some(meta_heads) = &facet_heads {
                                facet_heads_by_key.insert(key.clone(), meta_heads.clone());
                            }

                            if let (Some(uuid), Some(heads)) = (facet_uuid, &facet_heads) {
                                if let Some(cached) = self.facet_cache_get(doc_id, &uuid, heads) {
                                    facets.insert(key.clone(), cached);
                                    continue;
                                }
                            }

                            let key_str = key.to_string();
                            let value: Option<ThroughJson<FacetRaw>> =
                                autosurgeon::hydrate_prop_at(
                                    am_doc,
                                    &facets_obj,
                                    &*key_str,
                                    heads,
                                )?;
                            if let Some(facet_value) = value {
                                let facet_value = Arc::new(facet_value.0);
                                facets.insert(key.clone(), Arc::clone(&facet_value));
                                if let (Some(uuid), Some(heads)) = (facet_uuid, facet_heads) {
                                    to_cache.push((uuid, heads, facet_value));
                                }
                            }
                        }
                    }
                }
                eyre::Ok((facets, facet_heads_by_key, to_cache))
            })
            .await??;

        for (uuid, heads, value) in to_cache {
            self.facet_cache_put(doc_id, uuid, heads, value);
        }

        Ok(Some((facets, facet_heads_by_key)))
    }

    /// Get a doc at specific branch.
    pub async fn get_doc_with_facets_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };

        self.get_doc_with_facets_at_heads(doc_id, &branch_state.latest_heads, facet_keys)
            .await
    }

    pub async fn get_doc_branches_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let current_heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
        if heads != &current_heads {
            let entry = self.get_entry_at_heads(doc_id, heads).await?;
            return Ok(entry.map(|entry_value| DocNBranches {
                doc_id: doc_id.clone(),
                branches: entry_value
                    .branches
                    .into_keys()
                    .map(|branch_name| (branch_name, ChangeHashSet::default()))
                    .collect(),
            }));
        }
        self.current_doc_branches(doc_id).await
    }

    pub async fn get_doc_branches(&self, doc_id: &DocId) -> Res<Option<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.current_doc_branches(doc_id).await
    }

    /// Get a doc at specific heads (exact version). Delegates to get_at_heads_with_facets.
    pub async fn get_doc_with_facets_at_heads(
        &self,
        id: &DocId,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let facets = self
            .get_at_heads_with_facets_arc(id, heads, facet_keys)
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
        let Some(branch_ref) = entry.branches.get(&branch_path.to_string()) else {
            return Ok(None);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(&branch_ref.branch_doc_id)
            .await?
        else {
            return Ok(None);
        };
        let branch_heads = handle
            .with_document_local(|doc| ChangeHashSet(doc.get_heads().into()))
            .await?;
        let Some((facets, facet_heads_by_key)) = self
            .get_at_heads_with_facets_arc(doc_id, &branch_heads, facet_keys)
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

    pub async fn get_with_heads_at_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        _heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };

        let doc = self
            .get_doc_with_facets_at_heads(doc_id, &branch_state.latest_heads, facet_keys)
            .await?;
        Ok(doc.map(|doc_value| (doc_value, branch_state.latest_heads)))
    }

    pub async fn get_with_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let doc = self
            .get_doc_with_facets_at_heads(doc_id, &branch_state.latest_heads, facet_keys)
            .await?;
        Ok(doc.map(|doc| (doc, branch_state.latest_heads)))
    }

    pub async fn get_if_latest_at_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        _drawer_heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };
        if &branch_state.latest_heads == heads {
            return self
                .get_doc_with_facets_at_heads(doc_id, heads, facet_keys)
                .await;
        }
        Ok(None)
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
        let drawer_heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
        self.get_if_latest_at_heads(doc_id, branch_path, heads, &drawer_heads, facet_keys)
            .await
    }

    /// Returns the set of facet keys present for the doc at the given heads, without hydrating facet values.
    pub async fn facet_keys_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self.resolve_handle_for_heads(doc_id, heads).await? else {
            return Ok(None);
        };
        let keys = handle
            .with_document_local(|am_doc| {
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
            .await??;
        Ok(Some(keys))
    }

    /// Like get_if_latest but returns only facet keys (no facet values). Returns None if branch heads are stale.
    pub async fn get_facet_keys_if_latest(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        _drawer_heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };
        if &branch_state.latest_heads == heads {
            return self.facet_keys_at_heads(doc_id, heads).await;
        }
        Ok(None)
    }

    pub async fn get_facet_heads_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<Vec<automerge::ChangeHash>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self.resolve_handle_for_heads(doc_id, heads).await? else {
            eyre::bail!("doc not found");
        };
        handle
            .with_document_local(|am_doc| {
                facet_recovery::recover_facet_heads_at(am_doc, facet_key, heads)
            })
            .await?
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
        let Some(branch_state) = self.get_branch_state(doc_id, branch_path).await? else {
            return Ok(None);
        };
        self.get_facet_heads_at_heads(doc_id, &branch_state.latest_heads, facet_key)
            .await
            .map(Some)
    }

    pub async fn facet_keys_touched_by_local_actor(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facet_keys: &[FacetKey],
    ) -> Res<HashSet<FacetKey>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self.resolve_handle_for_heads(doc_id, heads).await? else {
            return Ok(HashSet::new());
        };
        let local_actor_id = self.local_actor_id.clone();
        let local_branch_actor_id = self.content_actor_id(None, &handle.document_id().to_string());
        let mut out = HashSet::new();
        for key in facet_keys {
            let facet_heads = self.get_facet_heads_at_heads(doc_id, heads, key).await?;
            let is_local = handle
                .with_document_local(|am_doc| {
                    for head in &facet_heads {
                        if let Some(change) = am_doc.get_change_by_hash(head) {
                            if change.actor_id() == &local_actor_id
                                || change.actor_id() == &local_branch_actor_id
                            {
                                return true;
                            }
                        }
                    }
                    false
                })
                .await?;
            if is_local {
                out.insert(key.clone());
            }
        }
        Ok(out)
    }
}
