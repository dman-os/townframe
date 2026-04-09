use crate::interlude::*;

use super::{BranchKind, DrawerRepo};

use crate::drawer::{
    dmeta,
    types::{
        BranchDeleteTombstone, DocDeleteTombstone, DocEntry, DocEntryDiff, DocNBranches,
        DrawerError, DrawerEvent, StoredBranchRef, UpdateDocArgsV2, UpdateDocBatchErrV2,
    },
};

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use daybook_types::doc::{AddDocArgs, ChangeHashSet, DocId, DocPatch, FacetKey};

struct PreparedAddDoc {
    doc_id: DocId,
    handle: am_utils_rs::repo::BigDocHandle,
    entry: DocEntry,
    branch_heads: ChangeHashSet,
    branch_doc_id: String,
}

// mutations
impl DrawerRepo {
    async fn prepare_add_doc(&self, args: AddDocArgs) -> Result<PreparedAddDoc, DrawerError> {
        if args.branch_path != "main" {
            return Err(ferr!("new docs must be created on main"))?;
        }
        let doc_am = automerge::Automerge::new();
        let handle = self.big_repo.create_doc(doc_am).await?;
        let doc_id = DocId::from(Uuid::new_v4().bs58());
        let branch_doc_id = handle.document_id().to_string();
        let mutation_actor_id = self.content_actor_id(args.user_path.as_ref(), &branch_doc_id);
        let now = Timestamp::now();

        let facet_keys: Vec<_> = args.facets.keys().cloned().collect();

        let heads = handle
            .with_document_local(|am_doc| {
                am_doc.set_actor(mutation_actor_id.clone());
                let mut tx = am_doc.transaction();
                tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
                tx.put(automerge::ROOT, "id", &doc_id)?;

                let facets_obj =
                    tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;

                for (key, value) in &args.facets {
                    let key_str = key.to_string();
                    autosurgeon::reconcile_prop(
                        &mut tx,
                        &facets_obj,
                        &*key_str,
                        ThroughJson(value.clone()),
                    )?;
                }

                dmeta::ensure_for_add(
                    &mut tx,
                    &facets_obj,
                    &facet_keys,
                    now,
                    args.user_path.as_ref(),
                    &mutation_actor_id,
                )?;

                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok(ChangeHashSet(Arc::from([heads])))
            })
            .await??;

        let entry = DocEntry {
            branches: [(
                args.branch_path.to_string(),
                StoredBranchRef {
                    branch_doc_id: branch_doc_id.clone(),
                },
            )]
            .into(),
            branches_deleted: HashMap::new(),
            vtag: VersionTag::mint(self.local_actor_id.clone()),
            previous_version_heads: None,
        };

        Ok(PreparedAddDoc {
            doc_id,
            handle,
            entry,
            branch_heads: heads,
            branch_doc_id,
        })
    }

    pub async fn batch_add(&self, args_batch: Vec<AddDocArgs>) -> Result<Vec<DocId>, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(ferr!("repo is stopped"))?;
        }

        if args_batch.is_empty() {
            return Ok(Vec::new());
        }

        for args in &args_batch {
            let resulting_keys: HashSet<FacetKey> = args.facets.keys().cloned().collect();
            self.validate_facets(&args.facets, &resulting_keys).await?;
        }

        let mut prepared_docs = Vec::with_capacity(args_batch.len());
        for args in args_batch {
            prepared_docs.push(self.prepare_add_doc(args).await?);
        }

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            doc.set_actor(self.local_actor_id.clone());
            let mut tx = doc.transaction();
            let docs_obj = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(automerge::ROOT, "docs", automerge::ObjType::Map)?,
            };
            let map_id = match tx.get(&docs_obj, "map")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(&docs_obj, "map", automerge::ObjType::Map)?,
            };
            for prepared in &prepared_docs {
                autosurgeon::reconcile_prop(
                    &mut tx,
                    &map_id,
                    autosurgeon::Prop::Key((&prepared.doc_id[..]).into()),
                    &prepared.entry,
                )?;
            }
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;

        let mut doc_ids = Vec::with_capacity(prepared_docs.len());
        let mut events = Vec::with_capacity(prepared_docs.len());

        {
            let mut pool = self.entry_pool.lock().unwrap();
            for prepared in &prepared_docs {
                let pruned = pool.insert_key(&prepared.doc_id, 1);
                for pkey in pruned {
                    self.entry_cache.remove(&pkey);
                }
                self.entry_cache
                    .insert(prepared.doc_id.clone(), prepared.entry.clone());
            }
        }

        for prepared in prepared_docs {
            self.add_branch_to_partitions_if_needed(
                BranchKind::Replicated,
                &prepared.branch_doc_id,
            )
            .await?;
            doc_ids.push(prepared.doc_id.clone());
            self.branch_handles
                .insert(prepared.branch_doc_id.clone(), prepared.handle);
            events.push(DrawerEvent::DocAdded {
                id: prepared.doc_id.clone(),
                entry: DocNBranches {
                    doc_id: prepared.doc_id,
                    branches: [("main".to_string(), prepared.branch_heads)].into(),
                },
                drawer_heads: drawer_heads.clone(),
                origin: self.local_origin(),
            });
        }
        *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
        self.registry.notify(events);

        Ok(doc_ids)
    }

    pub async fn add(&self, args: AddDocArgs) -> Result<DocId, DrawerError> {
        let mut created = self.batch_add(vec![args]).await?;
        if created.len() != 1 {
            return Err(ferr!(
                "batch_add returned invalid result for single add call"
            ))?;
        }
        Ok(created.pop().expect("checked above"))
    }

    pub async fn update_at_heads(
        &self,
        patch: DocPatch,
        branch_path: daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(ferr!("repo is stopped"))?;
        }
        if patch.is_empty() {
            return Ok(());
        }

        let existing_branch_ref = self.get_branch_ref(&patch.id, &branch_path).await?;
        let heads = match (heads, existing_branch_ref.as_ref()) {
            (Some(selected_heads), _) => selected_heads,
            (None, Some(branch_ref)) => self
                .get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
                .await?
                .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?,
            (None, None) => {
                return Err(DrawerError::BranchNotFound {
                    name: branch_path.to_string(),
                })
            }
        };

        let now = Timestamp::now();
        let facet_keys_set: Vec<_> = patch.facets_set.keys().cloned().collect();
        let facet_keys_remove = patch.facets_remove.clone();

        let (handle, branch_doc_id) = if let Some(branch_ref) = existing_branch_ref {
            (
                self.get_handle_by_branch_doc_id(&branch_ref.branch_doc_id)
                    .await?
                    .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?,
                branch_ref.branch_doc_id,
            )
        } else {
            return Err(DrawerError::BranchNotFound {
                name: branch_path.to_string(),
            });
        };
        let mutation_actor_id = self.content_actor_id(patch.user_path.as_ref(), &branch_doc_id);
        let existing_facet_keys = handle
            .with_document_local(|am_doc| {
                let facets_obj =
                    match automerge::ReadDoc::get_at(am_doc, automerge::ROOT, "facets", &heads)? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => return Ok::<HashSet<FacetKey>, eyre::Report>(HashSet::new()),
                    };
                let mut out = HashSet::new();
                for item in automerge::ReadDoc::map_range_at(am_doc, &facets_obj, .., &heads) {
                    out.insert(FacetKey::from(item.key.to_string().as_str()));
                }
                Ok(out)
            })
            .await??;
        let mut resulting_keys = existing_facet_keys;
        for facet_key in patch.facets_set.keys() {
            resulting_keys.insert(facet_key.clone());
        }
        for facet_key in &patch.facets_remove {
            resulting_keys.remove(facet_key);
        }
        self.validate_facets(&patch.facets_set, &resulting_keys)
            .await?;

        // 1. Update content doc
        let (_new_heads, invalidated_uuids) = handle
            .with_document_local(|am_doc| {
                am_doc.set_actor(mutation_actor_id.clone());
                let mut tx = am_doc.transaction_at(automerge::PatchLog::null(), &heads);

                let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                    _ => eyre::bail!("facets object not found in content doc"),
                };

                for (key, value) in &patch.facets_set {
                    let key_str = key.to_string();
                    autosurgeon::reconcile_prop(
                        &mut tx,
                        &facets_obj,
                        &*key_str,
                        ThroughJson(value.clone()),
                    )?;
                }
                for key in &patch.facets_remove {
                    let key_str = key.to_string();
                    tx.delete(&facets_obj, &*key_str)?;
                }

                let invalidated_uuids = dmeta::apply_update(
                    &mut tx,
                    &facets_obj,
                    &facet_keys_set,
                    &facet_keys_remove,
                    now,
                    patch.user_path.as_ref(),
                    &mutation_actor_id,
                )?;

                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok((ChangeHashSet(Arc::from([heads])), invalidated_uuids))
            })
            .await??;

        let drawer_heads = self.get_drawer_heads();

        // 3. Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(&patch.id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
        }

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(&patch.id, &uuid);
        }

        *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
        self.branch_handles
            .insert(handle.document_id().to_string(), handle);

        Ok(())
    }

    pub async fn create_branch_at_heads_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        if self.get_branch_ref(id, to_branch).await?.is_some() {
            return Err(DrawerError::BranchAlreadyExists {
                name: to_branch.to_string(),
            });
        }
        debug!(
            ?id,
            to_branch = %to_branch,
            from_branch = %from_branch,
            heads = ?am_utils_rs::serialize_commit_heads(from_heads.as_ref()),
            "create_branch_at_heads_from_branch: starting"
        );
        let Some(from_handle) = self
            .resolve_handle_for_branch_heads(id, from_branch, from_heads)
            .await?
        else {
            return Err(DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            });
        };
        let branch_doc = from_handle
            .with_document_local(|am_doc| {
                let current_heads = am_doc.get_heads();
                let current_heads_serialized = am_utils_rs::serialize_commit_heads(&current_heads);
                let from_heads_serialized =
                    am_utils_rs::serialize_commit_heads(from_heads.as_ref());
                let missing_before_fork: Vec<String> = from_heads
                    .iter()
                    .filter(|head| am_doc.get_change_by_hash(head).is_none())
                    .map(ToString::to_string)
                    .collect();
                if current_heads.as_slice() == &from_heads[..] {
                    debug!(
                        ?id,
                        to_branch = %to_branch,
                        from_branch = %from_branch,
                        current_heads = ?current_heads_serialized,
                        from_heads = ?from_heads_serialized,
                        "create_branch_at_heads_from_branch: fast-path clone"
                    );
                    Ok(am_doc.clone())
                } else {
                    debug!(
                        ?id,
                        to_branch = %to_branch,
                        from_branch = %from_branch,
                        current_heads = ?current_heads_serialized,
                        from_heads = ?from_heads_serialized,
                        ?missing_before_fork,
                        "create_branch_at_heads_from_branch: attempting fork_at"
                    );
                    if !missing_before_fork.is_empty() {
                        eyre::bail!(
                            "invariant break before fork_at: from branch is missing requested heads: doc_id={} to_branch={} from_branch={} from_heads={:?} current_heads={:?} missing={:?}",
                            id,
                            to_branch,
                            from_branch,
                            from_heads_serialized,
                            current_heads_serialized,
                            missing_before_fork
                        );
                    }
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        am_doc.fork_at(from_heads)
                    })) {
                        Ok(res) => res.map_err(eyre::Report::from),
                        Err(payload) => {
                            let missing_after_panic: Vec<String> = from_heads
                                .iter()
                                .filter(|head| am_doc.get_change_by_hash(head).is_none())
                                .map(ToString::to_string)
                                .collect();
                            let panic_payload = if let Some(msg) = payload.downcast_ref::<&str>() {
                                (*msg).to_string()
                            } else if let Some(msg) = payload.downcast_ref::<String>() {
                                msg.clone()
                            } else {
                                "non-string panic payload".to_string()
                            };
                            eyre::bail!(
                                "fork_at panicked while materializing branch snapshot: doc_id={} to_branch={} from_branch={} from_heads={:?} current_heads={:?} panic={} missing_after_panic={:?}",
                                id,
                                to_branch,
                                from_branch,
                                from_heads_serialized,
                                current_heads_serialized,
                                panic_payload,
                                missing_after_panic
                            );
                        }
                    }
                }
            })
            .await??;
        let handle = self.big_repo.create_doc(branch_doc).await?;
        let branch_doc_id = handle.document_id().to_string();
        let branch_kind = self.branch_kind_for_path(to_branch)?;
        self.add_branch_to_partitions_if_needed(branch_kind, &branch_doc_id)
            .await?;

        let _ = user_path;
        let drawer_heads = if branch_kind == BranchKind::Local {
            let vtag = VersionTag::update(self.local_actor_id.clone());
            self.upsert_local_branch_ref(id, to_branch, &branch_doc_id, &vtag)
                .await?;
            self.invalidate_entry_cache(id);
            self.get_drawer_heads()
        } else {
            let latest_drawer_heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
            let entry = self
                .get_entry_at_heads(id, &latest_drawer_heads)
                .await?
                .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;
            let mut new_entry = entry.clone();
            new_entry.branches.insert(
                to_branch.to_string(),
                StoredBranchRef {
                    branch_doc_id: branch_doc_id.clone(),
                },
            );
            new_entry.vtag = VersionTag::update(self.local_actor_id.clone());

            let drawer_heads = self.drawer_am_handle.with_document(|doc| {
                let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
                new_entry.previous_version_heads = Some(current_drawer_heads);

                let mut tx = doc.transaction();
                let map_id = match tx.get(automerge::ROOT, "docs")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                        match tx.get(&docs_id, "map")? {
                            Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => {
                                map_id
                            }
                            _ => eyre::bail!("drawer map not found"),
                        }
                    }
                    _ => eyre::bail!("drawer docs not found"),
                };

                autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok(ChangeHashSet(Arc::from([heads])))
            })?;

            self.invalidate_entry_cache(id);
            *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
            drawer_heads
        };
        let updated_entry = self
            .current_doc_branches(id)
            .await?
            .ok_or_eyre("branch state missing after create_branch_at_heads_from_branch")?;
        self.registry.notify([DrawerEvent::DocUpdated {
            id: id.clone(),
            entry: updated_entry,
            diff: DocEntryDiff {
                changed_facet_keys: Vec::new(),
                added_facet_keys: Vec::new(),
                removed_facet_keys: Vec::new(),
                moved_branch_names: vec![to_branch.to_string()],
            },
            drawer_heads,
            origin: self.local_origin(),
        }]);
        self.branch_handles.insert(branch_doc_id, handle);
        Ok(())
    }

    pub async fn ensure_branch_at_heads_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        match self
            .create_branch_at_heads_from_branch(id, to_branch, from_branch, from_heads, user_path)
            .await
        {
            Ok(()) | Err(DrawerError::BranchAlreadyExists { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn merge_from_heads(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        let to_branch_ref = self.get_branch_ref(id, to_branch).await?.ok_or_else(|| {
            DrawerError::BranchNotFound {
                name: to_branch.to_string(),
            }
        })?;
        let handle = self
            .get_handle_by_branch_doc_id(&to_branch_ref.branch_doc_id)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;
        let mutation_actor_id =
            self.content_actor_id(user_path.as_ref(), &to_branch_ref.branch_doc_id);
        let from_branch_ref = self.get_branch_ref(id, from_branch).await?.ok_or_else(|| {
            DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            }
        })?;
        let from_handle = self
            .get_handle_by_branch_doc_id(&from_branch_ref.branch_doc_id)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        // 1. Merge content docs
        let user_path_for_dmeta = user_path.clone();
        let mut am_from = from_handle
            .with_document_local(|from_doc| {
                let current_heads = from_doc.get_heads();
                let current_heads_serialized = am_utils_rs::serialize_commit_heads(&current_heads);
                let from_heads_serialized =
                    am_utils_rs::serialize_commit_heads(from_heads.as_ref());
                let missing_before_fork: Vec<String> = from_heads
                    .iter()
                    .filter(|head| from_doc.get_change_by_hash(head).is_none())
                    .map(ToString::to_string)
                    .collect();
                if current_heads.as_slice() == &from_heads[..] {
                    Ok(from_doc.clone())
                } else {
                    debug!(
                        ?id,
                        to_branch = %to_branch,
                        from_branch = %from_branch,
                        from_branch_doc_id = %from_branch_ref.branch_doc_id,
                        current_heads = ?current_heads_serialized,
                        from_heads = ?from_heads_serialized,
                        ?missing_before_fork,
                        "merge_from_heads: attempting fork_at for source branch snapshot"
                    );
                    if !missing_before_fork.is_empty() {
                        eyre::bail!(
                            "invariant break before merge_from_heads fork_at: source branch is missing requested heads: doc_id={} to_branch={} source_branch_doc_id={} from_heads={:?} current_heads={:?} missing={:?}",
                            id,
                            to_branch,
                            from_branch_ref.branch_doc_id,
                            from_heads_serialized,
                            current_heads_serialized,
                            missing_before_fork
                        );
                    }
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        from_doc.fork_at(from_heads)
                    })) {
                        Ok(res) => res.map_err(eyre::Report::from),
                        Err(payload) => {
                            let missing_after_panic: Vec<String> = from_heads
                                .iter()
                                .filter(|head| from_doc.get_change_by_hash(head).is_none())
                                .map(ToString::to_string)
                                .collect();
                            let panic_payload = if let Some(msg) = payload.downcast_ref::<&str>() {
                                (*msg).to_string()
                            } else if let Some(msg) = payload.downcast_ref::<String>() {
                                msg.clone()
                            } else {
                                "non-string panic payload".to_string()
                            };
                            eyre::bail!(
                                "merge_from_heads fork_at panicked: doc_id={} to_branch={} source_branch_doc_id={} from_heads={:?} current_heads={:?} panic={} missing_after_panic={:?}",
                                id,
                                to_branch,
                                from_branch_ref.branch_doc_id,
                                from_heads_serialized,
                                current_heads_serialized,
                                panic_payload,
                                missing_after_panic
                            );
                        }
                    }
                }
            })
            .await??;
        let (_new_heads, _modified_facets, invalidated_uuids) = handle
            .with_document_local(move |am_doc| {
                am_doc.set_actor(mutation_actor_id.clone());
                let (patches, new_heads) = match std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| -> Res<(Vec<automerge::Patch>, ChangeHashSet)> {
                        let mut patch_log = automerge::PatchLog::active();
                        am_doc.merge_and_log_patches(&mut am_from, &mut patch_log)?;
                        let patches = am_doc.make_patches(&mut patch_log);
                        let heads = am_doc.get_heads();
                        let new_heads = ChangeHashSet(heads.into());
                        Ok((patches, new_heads))
                    }),
                ) {
                    Ok(res) => res?,
                    Err(payload) => {
                        let panic_payload = if let Some(msg) = payload.downcast_ref::<&str>() {
                            (*msg).to_string()
                        } else if let Some(msg) = payload.downcast_ref::<String>() {
                            msg.clone()
                        } else {
                            "non-string panic payload".to_string()
                        };
                        eyre::bail!(
                            "merge_from_heads panicked during merge_and_log_patches: doc_id={} to_branch={} from_branch={} target_branch_doc_id={} source_branch_doc_id={} from_heads={:?} panic={}",
                            id,
                            to_branch,
                            from_branch,
                            to_branch_ref.branch_doc_id,
                            from_branch_ref.branch_doc_id,
                            am_utils_rs::serialize_commit_heads(from_heads.as_ref()),
                            panic_payload
                        );
                    }
                };

                // Identify modified facets from patches
                let mut modified_facets = HashSet::new();
                for patch in patches {
                    if patch.path.len() >= 2 {
                        if let (_, automerge::Prop::Map(ref p0)) = &patch.path[0] {
                            if p0 == "facets" {
                                if let (_, automerge::Prop::Map(ref facet_key_str)) = &patch.path[1]
                                {
                                    modified_facets.insert(facet_key_str.to_string());
                                }
                            }
                        }
                    }
                }

                let invalidated_uuids = if modified_facets.is_empty() {
                    Vec::new()
                } else {
                    // Work around Automerge fork_at instability after merge+patch generation by
                    // performing the follow-up merge bookkeeping write via transaction_at on
                    // current heads with an inactive patch log.
                    let heads_now = am_doc.get_heads();
                    let mut tx =
                        am_doc.transaction_at(automerge::PatchLog::inactive(), &heads_now);
                    let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("facets object not found in content doc"),
                    };
                    let now = Timestamp::now();
                    let invalidated = dmeta::apply_merge(
                        &mut tx,
                        &facets_obj,
                        &modified_facets,
                        now,
                        user_path_for_dmeta.as_ref(),
                        &mutation_actor_id,
                    )?;
                    tx.commit();
                    invalidated
                };

                eyre::Ok((new_heads, modified_facets, invalidated_uuids))
            })
            .await??;

        let drawer_heads = self.get_drawer_heads();

        // 3. Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.remove(id);
        }

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(id, &uuid);
        }

        *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();

        Ok(())
    }

    pub async fn del(&self, id: &DocId) -> Result<bool, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }

        let current_entry = self.get_entry(id).await?;
        let Some(current_entry) = current_entry else {
            return Ok(false);
        };
        let deleted_branch_snapshots = self
            .non_tmp_branch_snapshots_for_entry(id, &current_entry)
            .await?;
        let mut deleted_facet_keys_set = HashSet::new();
        for snapshot in deleted_branch_snapshots.values() {
            deleted_facet_keys_set.extend(self.facet_keys_at_branch_snapshot(id, snapshot).await?);
        }
        let mut deleted_facet_keys: Vec<FacetKey> = deleted_facet_keys_set.into_iter().collect();
        deleted_facet_keys.sort();

        let res = self.drawer_am_handle.with_document(|doc| {
            let docs_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => docs_id,
                _ => eyre::bail!("drawer docs not found"),
            };
            let map_id = match doc.get(&docs_id, "map")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                _ => eyre::bail!("drawer map not found"),
            };

            let entry: Option<DocEntry> = autosurgeon::hydrate_prop(doc, &map_id, &**id)?;
            let Some(entry) = entry else {
                return Ok((false, ChangeHashSet::default(), None));
            };

            let mut tx = doc.transaction();
            let map_deleted_id = match tx.get(&docs_id, "map_deleted")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(&docs_id, "map_deleted", automerge::ObjType::Map)?,
            };
            let mut deleted_tags: Vec<DocDeleteTombstone> = match tx.get(&map_deleted_id, &**id)? {
                Some((automerge::Value::Object(automerge::ObjType::List), _)) => {
                    autosurgeon::hydrate_prop::<_, Vec<DocDeleteTombstone>, _, _>(
                        &tx,
                        &map_deleted_id,
                        &**id,
                    )?
                }
                Some((other, _)) => eyre::bail!("invalid map_deleted entry shape: {other:?}"),
                None => Vec::new(),
            };
            deleted_tags.push(DocDeleteTombstone {
                vtag: VersionTag::update(self.local_actor_id.clone()),
                branches: deleted_branch_snapshots.clone(),
            });
            autosurgeon::reconcile_prop(&mut tx, &map_deleted_id, &**id, deleted_tags)?;
            tx.delete(&map_id, &**id)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            Ok((true, ChangeHashSet(Arc::from([heads])), Some(entry)))
        });

        let (existed, drawer_heads, entry) = res?;

        if existed {
            let Some(entry) = &entry else {
                return Err(ferr!(
                    "deleted drawer entry must be returned with deletion result"
                ))?;
            };
            let local_branch_refs = self.list_local_branch_refs(id).await?;
            for (branch_path, branch_ref) in &entry.branches {
                self.remove_branch_from_partitions_if_needed(
                    self.branch_kind_for_path(&daybook_types::doc::BranchPath::from(
                        branch_path.clone(),
                    ))?,
                    &branch_ref.branch_doc_id,
                )
                .await?;
            }
            self.invalidate_entry_cache(id);
            for branch_ref in entry.branches.values() {
                self.branch_handles.remove(&branch_ref.branch_doc_id);
            }
            for (branch_path, branch_doc_id) in local_branch_refs {
                let branch_path = daybook_types::doc::BranchPath::from(branch_path);
                let branch_heads = self
                    .get_branch_heads_by_doc_id(&branch_doc_id)
                    .await?
                    .unwrap_or_default();
                self.delete_local_branch_ref_with_tombstone(
                    id,
                    &branch_path,
                    &branch_doc_id,
                    &branch_heads,
                )
                .await?;
                self.branch_handles.remove(&branch_doc_id);
            }
            self.invalidate_facet_cache_doc(id);
            *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
            self.registry.notify([DrawerEvent::DocDeleted {
                id: id.clone(),
                entry: Some(entry.clone()),
                drawer_heads: drawer_heads.clone(),
                deleted_facet_keys: deleted_facet_keys.clone(),
                origin: self.local_origin(),
            }]);
        }

        Ok(existed)
    }

    pub async fn update_batch(
        &self,
        patches: Vec<UpdateDocArgsV2>,
    ) -> Result<(), UpdateDocBatchErrV2> {
        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;
        let mut stream = futures::stream::iter(patches.into_iter().enumerate().map(
            |(ii, args)| async move {
                self.update_at_heads(args.patch, args.branch_path, args.heads)
                    .await
                    .map_err(|err| (ii, err))
            },
        ))
        .buffered_unordered(16);

        let mut errors = HashMap::new();
        while let Some(res) = stream.next().await {
            if let Err((ii, err)) = res {
                errors.insert(ii as u64, err);
            }
        }

        if !errors.is_empty() {
            Err(UpdateDocBatchErrV2 { map: errors })
        } else {
            Ok(())
        }
    }

    pub async fn merge_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        let from_branch_state = self
            .get_branch_heads_for_path(id, from_branch)
            .await?
            .ok_or_else(|| DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            })?;

        self.merge_from_heads(id, to_branch, from_branch, &from_branch_state, user_path)
            .await
    }

    pub async fn delete_branch(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        _user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<bool, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }

        let branch_name = branch_path.to_string();
        let Some(branch_ref) = self.get_branch_ref(id, branch_path).await? else {
            return Ok(false);
        };
        let branch_heads = self
            .get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
            .await?
            .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?;

        self.remove_branch_from_partitions_if_needed(
            branch_ref.branch_kind,
            &branch_ref.branch_doc_id,
        )
        .await?;
        self.branch_handles.remove(&branch_ref.branch_doc_id);

        if branch_ref.branch_kind == BranchKind::Local {
            self.delete_local_branch_ref_with_tombstone(
                id,
                branch_path,
                &branch_ref.branch_doc_id,
                &branch_heads,
            )
            .await?;
            self.invalidate_entry_cache(id);
            let drawer_heads = self.get_drawer_heads();
            let updated_entry = self
                .current_doc_branches(id)
                .await?
                .ok_or_eyre("branch state missing after local delete_branch")?;
            self.registry.notify([DrawerEvent::DocUpdated {
                id: id.clone(),
                entry: updated_entry,
                diff: DocEntryDiff {
                    changed_facet_keys: Vec::new(),
                    added_facet_keys: Vec::new(),
                    removed_facet_keys: Vec::new(),
                    moved_branch_names: vec![branch_name.clone()],
                },
                drawer_heads,
                origin: self.local_origin(),
            }]);
            return Ok(true);
        }

        let latest_drawer_heads = self.current_heads.lock().expect(ERROR_MUTEX).clone();
        let entry = self
            .get_entry_at_heads(id, &latest_drawer_heads)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;
        let mut new_entry = entry.clone();
        let removed_branch = new_entry
            .branches
            .remove(&branch_name)
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;
        new_entry
            .branches_deleted
            .entry(branch_name.clone())
            .or_default()
            .push(BranchDeleteTombstone {
                vtag: VersionTag::update(self.local_actor_id.clone()),
                branch_doc_id: removed_branch.branch_doc_id,
                branch_heads,
            });
        new_entry.vtag = VersionTag::update(self.local_actor_id.clone());

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
            new_entry.previous_version_heads = Some(current_drawer_heads);

            let mut tx = doc.transaction();
            let map_id = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match tx.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("drawer map not found"),
                    }
                }
                _ => eyre::bail!("drawer docs not found"),
            };

            autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;
        let diff = DocEntryDiff::new(&entry, &new_entry, Vec::new());

        // Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.remove(id);
        }

        *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
        let updated_entry = self
            .current_doc_branches(id)
            .await?
            .ok_or_eyre("branch state missing after delete_branch")?;
        self.registry.notify([DrawerEvent::DocUpdated {
            id: id.clone(),
            entry: updated_entry,
            diff,
            drawer_heads,
            origin: self.local_origin(),
        }]);

        Ok(true)
    }
}
