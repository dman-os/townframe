use crate::interlude::*;

use super::outbox::{self, OutboxState};
use super::{BranchKind, DrawerRepo, FacetRaw, FacetWriteScope};

use crate::drawer::{
    dmeta,
    types::{
        BranchDeleteTombstone, DocDeleteTombstone, DocEntry, DrawerError, StoredBranchRef,
        UpdateDocArgsV2, UpdateDocBatchErrV2,
    },
};

use automerge::ReadDoc;
use automerge::transaction::Transactable;
use daybook_types::doc::{
    AddDocArgs, AuthorityScope, Branch, BranchDeclaration, BranchId, BranchPublication,
    BranchVersion, Branches, ChangeHashSet, DocId, DocPatch, FacetKey, WellKnownFacet,
    WellKnownFacetTag,
};

/// The receipt of [`DrawerRepo::add_temporary`]: a document staged purely
/// locally. Nothing about the staging is replicated — no coparent grants at
/// genesis mean no group reaches the staged doc, so its events cannot leave
/// the node — and nothing registers it yet: the caller either
/// [`DrawerRepo::commit_temporary`]s it (the advertising grants plus the
/// `docs.map` registration) or [`DrawerRepo::discard_temporary`]s it, and a
/// crash leaves the reservation to the next boot's sweep (ADR 003 §19: the
/// cipherBlob claim decides which).
pub struct StagedAdd {
    pub doc_id: DocId,
    pub handle: big_repo::BigDocHandle,
    /// The registration write the commit replays into the drawer document.
    pub(crate) entry: DocEntry,
    /// The staged content's branch heads — what a cipherBlob `keyRef` pins
    /// while the document is still staging.
    pub branch_heads: ChangeHashSet,
    pub branch_doc_id: DocumentId,
    /// The idempotency key the outbox row rides on (from the caller's
    /// `AddDocArgs`); a commit or discard updates that row.
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy)]
enum AddValidation {
    Registered,
    Bootstrap,
}

// mutations
impl DrawerRepo {
    /// Stage a new document's content in big_repo and return the receipt whose
    /// commit registers it. Validation of the caller's own facets belongs to
    /// the caller (batch validation validates first so a bad batch allocates
    /// nothing); this only validates the system facets it authors.
    /// `temporary` marks an `add_temporary` staging receipt: its outbox row is
    /// write-ahead bookkeeping too, but its registration is NOT owed by the
    /// row — the durable `cipherBlob` claim decides it (ADR 003 §19,
    /// `drawer/reconciliation.rs`). Ordinary adds' rows are the intent to
    /// register.
    async fn prepare_add_doc(
        &self,
        args: AddDocArgs,
        temporary: bool,
    ) -> Result<StagedAdd, DrawerError> {
        if args.branch_path != "main" {
            Err(ferr!("new docs must be created on main"))?;
        }
        if args.idempotency_key.is_empty() {
            Err(ferr!("add requires a non-empty idempotency key"))?;
        }
        // Retry contract (drawer_add_outbox): the same key with a `done` row
        // returns the same doc, no re-execution; an in-flight key is refused.
        // After the TTL purge a late retry re-executes as a fresh add.
        if let Some(row) = outbox::get_row(&self.meta_store_sql, &args.idempotency_key).await? {
            match row.state {
                OutboxState::Done => {
                    let doc_id = DocId::from(row.branch_doc_id.to_string());
                    let handle = self
                        .big_repo
                        .get_doc(&row.branch_doc_id)
                        .await?
                        .into_ready(row.branch_doc_id.clone())
                        .wrap_err_with(|| {
                            eyre::eyre!(
                                "deduped add for key '{}' cannot materialize its doc",
                                args.idempotency_key
                            )
                        })?;
                    return Ok(StagedAdd {
                        doc_id,
                        handle,
                        entry: row.entry,
                        branch_heads: row.staged_branch_heads,
                        branch_doc_id: row.branch_doc_id,
                        idempotency_key: args.idempotency_key,
                    });
                }
                OutboxState::PendingAdd | OutboxState::CommittedStaged => {
                    Err(ferr!(
                        "idempotency key '{}' is already in flight; \
                         retry after the boot pass resolves it",
                        args.idempotency_key
                    ))?;
                }
            }
        }
        // The staging facility: allocation and commit touch no group — a
        // genesis with no coparents leaves the document visible to the local
        // agent only, so nothing between allocation and registration
        // advertises the document. The advertising groups are granted by the
        // registration half (`commit_staged_adds`), not here.
        let branch_doc_id = self
            .big_repo
            .allocate_id()
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error allocating doc in big repo")?;
        let doc_id = DocId::from(branch_doc_id.to_string());
        let branch_id = BranchId::from(branch_doc_id.to_string());
        let mutation_actor_id =
            self.content_actor_id(args.user_path.as_deref(), branch_doc_id.clone());
        let now = Timestamp::now();

        let branch_key = FacetKey::from(WellKnownFacetTag::Branch);
        let branches_key = FacetKey::from(WellKnownFacetTag::Branches);
        let branch_facet: serde_json::Value = WellKnownFacet::Branch(Branch {
            document_id: doc_id.clone(),
            branch_id,
            created_from: None,
        })
        .into();
        let branches_facet: serde_json::Value = WellKnownFacet::Branches(Branches {
            by_name: HashMap::new(),
            by_id: HashMap::new(),
        })
        .into();
        let system_facets = [
            (branch_key.clone(), branch_facet.clone()),
            (branches_key.clone(), branches_facet.clone()),
        ]
        .into();
        let resulting_keys: HashSet<_> = args
            .facets
            .keys()
            .cloned()
            .chain([branch_key.clone(), branches_key.clone()])
            .collect();
        self.validate_facets(
            &system_facets,
            &[],
            &resulting_keys,
            FacetWriteScope::System,
        )
        .await?;

        let facet_keys: Vec<_> = resulting_keys.iter().cloned().collect();
        let mut doc_am = automerge::Automerge::new();

        let heads = (|| -> Result<ChangeHashSet, eyre::Report> {
            doc_am.set_actor(mutation_actor_id.clone());
            let mut tx = doc_am.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
            tx.put(automerge::ROOT, "id", &doc_id)?;

            let facets_obj = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;

            for (key, value) in args.facets {
                let key_str = key.to_string();
                autosurgeon::reconcile_prop(&mut tx, &facets_obj, &*key_str, ThroughJson(value))?;
            }
            for (key, value) in system_facets {
                let key_str = key.to_string();
                autosurgeon::reconcile_prop(&mut tx, &facets_obj, &*key_str, ThroughJson(value))?;
            }

            dmeta::ensure_for_add(
                &mut tx,
                &facets_obj,
                &facet_keys,
                now,
                args.user_path.as_deref(),
                &mutation_actor_id,
            )?;

            let (heads, _) = tx.commit();
            Ok(ChangeHashSet(Arc::from([heads.expect("commit failed")])))
        })()?;
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
        // Write-ahead (ADR 003 §19): the outbox row is durable BEFORE
        // `commit_id` runs, so any crash mid-commit is replayable by the next
        // boot's pass or resolved on caller retry.
        outbox::insert_pending(
            &self.meta_store_sql,
            &args.idempotency_key,
            &branch_doc_id,
            &entry,
            &heads,
            temporary,
        )
        .await?;
        let handle = self
            .big_repo
            .commit_id(branch_doc_id.clone(), doc_am, Vec::new(), &[])
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error committing allocated doc in big repo")?;
        outbox::mark_committed_staged(&self.meta_store_sql, &args.idempotency_key).await?;

        Ok(StagedAdd {
            doc_id,
            handle,
            entry,
            branch_heads: heads,
            branch_doc_id,
            idempotency_key: args.idempotency_key,
        })
    }

    pub async fn batch_add(&self, args_batch: Vec<AddDocArgs>) -> Result<Vec<DocId>, DrawerError> {
        self.batch_add_inner(args_batch, AddValidation::Registered)
            .await
    }

    /// ADR 007 §4: the first manifest write (the core manifest doc at repo init)
    /// is the single write in the system that must skip facet validation — no
    /// manifest is registered yet. Everything after validates normally.
    pub async fn add_unchecked(&self, args: AddDocArgs) -> Result<DocId, DrawerError> {
        let mut created = self
            .batch_add_inner(vec![args], AddValidation::Bootstrap)
            .await?;
        if created.len() != 1 {
            Err(ferr!(
                "batch_add returned invalid result for single add call"
            ))?;
        }
        Ok(created.pop().expect("checked above"))
    }

    async fn batch_add_inner(
        &self,
        args_batch: Vec<AddDocArgs>,
        validation: AddValidation,
    ) -> Result<Vec<DocId>, DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }

        if args_batch.is_empty() {
            return Ok(Vec::new());
        }

        for args in &args_batch {
            Self::validate_facet_write_scope(&args.facets, &[], FacetWriteScope::User)?;
        }

        if matches!(validation, AddValidation::Registered) {
            for args in &args_batch {
                let resulting_keys: HashSet<FacetKey> = args.facets.keys().cloned().collect();
                self.validate_facets(&args.facets, &[], &resulting_keys, FacetWriteScope::User)
                    .await?;
            }
        }

        let mut staged_docs = Vec::with_capacity(args_batch.len());
        for args in args_batch {
            staged_docs.push(self.prepare_add_doc(args, false).await?);
        }
        self.commit_staged_adds(&staged_docs).await
    }

    /// The registration half of the drawer's add flow, shared by `batch_add`
    /// (one commit for a batch) and `commit_temporary` (one receipt): the
    /// finalize grants and the `docs.map` commit that registers the
    /// documents. One code path, two spellings.
    pub(crate) async fn commit_staged_adds(
        &self,
        staged_docs: &[StagedAdd],
    ) -> Result<Vec<DocId>, DrawerError> {
        // Finalize grants — the registration sequence: the advertising groups
        // become members before the docs.map commit that registers the
        // documents — the same shape `register_existing_doc` grants at
        // registration. From commit_id until this point nothing advertised
        // the documents: a genesis with no coparents leaves them visible to
        // the local agent only. The drawer-group grant below is unconditional
        // for content docs (local branches skip it).
        for staged in staged_docs {
            self.big_repo
                .add_admin_member_to_doc(
                    staged.branch_doc_id.clone(),
                    self.content_docs_group.clone(),
                )
                .await?;
            self.big_repo
                .add_admin_member_to_doc(
                    staged.branch_doc_id.clone(),
                    self.encrypted_blob_docs_group.clone(),
                )
                .await?;
            self.big_repo
                .add_admin_member_to_doc(staged.branch_doc_id.clone(), self.drawer_group.clone())
                .await?;
        }

        let drawer_heads = self
            .drawer_doc_handle
            .with_document(|doc| {
                // Test-only: refuse before the commit. Every allocation in this batch has
                // already committed (keyhive authority created; the reservation row is
                // gone) and received its finalize grants, so failing here pins the node
                // in the window between the grants and the registration write, where the
                // grants are the only thing that names the documents still nothing has
                // registered.
                #[cfg(test)]
                if self.take_fail_next_drawer_doc_commit() {
                    eyre::bail!("injected drawer-doc commit failure (test only)");
                }
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
                for staged in staged_docs {
                    autosurgeon::reconcile_prop(
                        &mut tx,
                        &map_id,
                        autosurgeon::Prop::Key((&staged.doc_id[..]).into()),
                        &staged.entry,
                    )?;
                }
                let (heads, _) = tx.commit();
                // A replay over an already-durable identical entry (the boot
                // loop re-derives residuals) writes nothing: commit() answers
                // None and the drawer doc stays at its current heads.
                let heads = heads.map_or_else(
                    || ChangeHashSet(Arc::from(doc.get_heads())),
                    |head| ChangeHashSet(Arc::from([head])),
                );
                eyre::Ok(heads)
            })
            .await??;

        let mut doc_ids = Vec::with_capacity(staged_docs.len());

        {
            surelock::key::lock_scope(|key| {
                key.lock_with(
                    &(&self.entry_pool, &self.entry_cache),
                    |(mut pool, mut cache)| {
                        for staged in staged_docs {
                            let pruned = pool.insert_key(&staged.doc_id, 1);
                            for pkey in pruned {
                                cache.remove(&pkey);
                            }
                            cache.insert(staged.doc_id.clone(), staged.entry.clone());
                        }
                    },
                );
            });
        }

        for staged in staged_docs {
            self.add_branch_to_partitions_if_needed(
                BranchKind::Replicated,
                staged.branch_doc_id.clone(),
                &staged.branch_heads,
            )
            .await?;
            doc_ids.push(staged.doc_id.clone());
            surelock::key::lock_scope(|key| {
                let (mut handles, _key) = key.lock(&self.branch_handles);
                handles.insert(staged.branch_doc_id.clone(), staged.handle.clone());
            });
            // The `docs.map` entry committed above is what registers the
            // document and the partitions are re-derived, so the row goes
            // `done`: aged out by TTL and never replayed by the boot loop.
            outbox::mark_done(&self.meta_store_sql, &staged.idempotency_key).await?;
        }
        surelock::key::lock_scope(|key| {
            let (mut heads, _key) = key.lock(&self.current_heads);
            *heads = drawer_heads.clone();
        });

        Ok(doc_ids)
    }

    /// The purely local half of the transactional add an external system uses
    /// (ADR 003 §19): stage a new document on `main` without granting any
    /// advertising group or writing any `docs.map` entry. A coparentless
    /// genesis means the staged document's events cannot leave the node; the
    /// reservation row is consumed by `commit_id` itself, so a crash before
    /// the caller's registration leaves the doc committed-but-unregistered
    /// (enumerated at boot by the drawer outbox pass, which reads the durable
    /// claim — e.g. a cipherBlob facet naming the staging doc — and replays
    /// the commit). The caller comes back with the receipt and either
    /// [`DrawerRepo::commit_temporary`]s it or [`DrawerRepo::discard_temporary`]s
    /// it.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn add_temporary(&self, args: AddDocArgs) -> Result<StagedAdd, DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }
        // The staging caller is the repo's own machinery, so it validates at
        // system scope: system-managed facets are exactly what it may author
        // (the encryption worker's JWK). Ordinary `add`/`batch_add` callers
        // keep their user-scope gate in `batch_add_inner`.
        let resulting_keys: HashSet<FacetKey> = args.facets.keys().cloned().collect();
        self.validate_facets(&args.facets, &[], &resulting_keys, FacetWriteScope::System)
            .await?;
        self.prepare_add_doc(args, true).await
    }

    /// Commit a [`StagedAdd`]: the finalize grants (content docs, encrypted
    /// blob docs, drawer) and the `docs.map` registration write — the same
    /// sequence `batch_add` runs, through the same code path.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn commit_temporary(&self, staged: &StagedAdd) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }
        self.commit_staged_adds(std::slice::from_ref(staged))
            .await?;
        Ok(())
    }

    /// Discard a [`StagedAdd`] without ever registering it. Idempotent
    /// (ADR 003 §19): the reservation row goes; nothing else happens — the
    /// staging doc never received a coparent grant (a crashed commit happens
    /// after the registration write began, and a registered doc is a delete,
    /// not a discard), so there is no grant to revert and no pending coparent
    /// to revoke. The doc's events, sedimentree and bytes are never deleted;
    /// the id becomes uncommittable.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn discard_temporary(&self, staged: &StagedAdd) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }
        if self
            .get_branch_ref(&staged.doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .is_some()
        {
            tracing::debug!(
                doc_id = %staged.doc_id,
                "discard of an already committed temporary add is a no-op"
            );
            return Ok(());
        }
        // Terminal: the row goes out of the outbox with the reservation, so a
        // claimed staging doc cannot be replayed after its caller picked
        // discard. The staging doc itself is untouched — no revoke, no event
        // or byte delete (an invisible memberless orphan at most).
        outbox::delete_row(&self.meta_store_sql, &staged.idempotency_key).await?;
        self.big_repo
            .abandon_allocation(staged.branch_doc_id.clone())
            .await
            .map_err(|err| DrawerError::Other {
                inner: eyre::eyre!(err),
            })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn add(&self, args: AddDocArgs) -> Result<DocId, DrawerError> {
        let mut created = self.batch_add(vec![args]).await?;
        if created.len() != 1 {
            Err(ferr!(
                "batch_add returned invalid result for single add call"
            ))?;
        }
        Ok(created.pop().expect("checked above"))
    }

    /// ADR 007 §2: register a doc that was created outside the drawer (e.g. the
    /// repo config doc at init) so the drawer serves it like any content doc.
    /// Grants the drawer groups admin access and adds a `docs.map` entry.
    pub async fn register_existing_doc(
        &self,
        doc_id: &DocId,
        branch_doc_id: DocumentId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }
        if self.get_branch_ref(doc_id, branch_path).await?.is_some() {
            return Ok(());
        }
        self.big_repo
            .add_admin_member_to_doc(branch_doc_id.clone(), self.content_docs_group.clone())
            .await?;
        self.big_repo
            .add_admin_member_to_doc(
                branch_doc_id.clone(),
                self.encrypted_blob_docs_group.clone(),
            )
            .await?;
        self.big_repo
            .add_admin_member_to_doc(branch_doc_id.clone(), self.drawer_group.clone())
            .await?;
        let entry = DocEntry {
            branches: [(
                branch_path.to_string(),
                StoredBranchRef {
                    branch_doc_id: branch_doc_id.clone(),
                },
            )]
            .into(),
            branches_deleted: HashMap::new(),
            vtag: VersionTag::mint(self.local_actor_id.clone()),
            previous_version_heads: None,
        };
        let drawer_heads = self
            .drawer_doc_handle
            .with_document(|doc| {
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
                autosurgeon::reconcile_prop(
                    &mut tx,
                    &map_id,
                    autosurgeon::Prop::Key((&doc_id[..]).into()),
                    &entry,
                )?;
                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok(ChangeHashSet(Arc::from([heads])))
            })
            .await??;
        surelock::key::lock_scope(|key| {
            let (mut heads, _key) = key.lock(&self.current_heads);
            *heads = drawer_heads;
        });
        surelock::key::lock_scope(|key| {
            let (mut cache, _key) = key.lock(&self.entry_cache);
            cache.insert(doc_id.clone(), entry);
        });

        // Adopted content docs (e.g. the repo config doc) are created outside
        // the drawer's `add` flow, so they lack the root `id` and the dmeta
        // and branch facets that every content doc carries. Bootstrap them
        // together so later drawer writes (update_at_heads etc.) validate
        // normally.
        let branch_handle = match self.big_repo.get_doc(&branch_doc_id).await? {
            big_repo::DocLookup::Ready(handle) => handle,
            big_repo::DocLookup::PendingMaterialization => {
                return Err(ferr!("adopted doc branch pending materialization: {doc_id}").into());
            }
            big_repo::DocLookup::Missing => {
                return Err(ferr!("adopted doc branch missing: {doc_id}").into());
            }
        };
        let mutation_actor_id = self.content_actor_id(None, branch_doc_id.clone());
        let now = Timestamp::now();
        let branch_key = FacetKey::from(WellKnownFacetTag::Branch);
        let branches_key = FacetKey::from(WellKnownFacetTag::Branches);
        let branch_facet: serde_json::Value = WellKnownFacet::Branch(Branch {
            document_id: doc_id.clone(),
            branch_id: BranchId::from(branch_doc_id.to_string()),
            created_from: None,
        })
        .into();
        let branches_facet: serde_json::Value = WellKnownFacet::Branches(Branches {
            by_name: HashMap::new(),
            by_id: HashMap::new(),
        })
        .into();
        let dmeta_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Dmeta);
        branch_handle
            .with_document(|am_doc| {
                let has_dmeta = dmeta::facet_meta_obj(am_doc, &dmeta_key)?.is_some();
                am_doc.set_actor(mutation_actor_id.clone());
                let mut tx = am_doc.transaction();
                if tx.get(automerge::ROOT, "id")?.is_none() {
                    tx.put(automerge::ROOT, "id", doc_id)?;
                }
                let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                    _ => tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?,
                };
                for (key, value) in [
                    (branch_key.clone(), branch_facet.clone()),
                    (branches_key.clone(), branches_facet.clone()),
                ] {
                    let key_str = key.to_string();
                    autosurgeon::reconcile_prop(
                        &mut tx,
                        &facets_obj,
                        &*key_str,
                        ThroughJson(value),
                    )?;
                }
                if !has_dmeta {
                    dmeta::ensure_for_add(
                        &mut tx,
                        &facets_obj,
                        &[branch_key.clone(), branches_key.clone()],
                        now,
                        None,
                        &mutation_actor_id,
                    )?;
                }
                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok(ChangeHashSet(Arc::from([heads])))
            })
            .await??;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%patch.id, %branch_path))]
    pub async fn update_at_heads(
        &self,
        patch: DocPatch,
        branch_path: &daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), DrawerError> {
        self.update_at_heads_with_scope(patch, branch_path, heads, FacetWriteScope::User)
            .await
    }

    /// System-scope writes bypass the "ordinary writes cannot modify a
    /// system-managed facet" rule (drawer.rs `validate_facet_write_scope`).
    /// The maintenance workers that own those facets (the encryption worker's
    /// cipherBlob/JWK writes) need this seam; tests of anything downstream of
    /// such a facet need it to stage the facet at all.
    pub(crate) async fn update_at_heads_with_scope(
        &self,
        patch: DocPatch,
        branch_path: &daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
        write_scope: FacetWriteScope,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            Err(ferr!("repo is stopped"))?;
        }
        if patch.is_empty() {
            return Ok(());
        }

        let existing_branch_ref = self.get_branch_ref(&patch.id, branch_path).await?;
        // Existence stays existence: the create/register guards above resolve a
        // branch ref alone, so creating a branch whose name is taken is still
        // refused as already existing even when this node cannot reach the old
        // branch doc. But *using* the branch is not merely existence: a peer's
        // delete revokes this repo's access to the branch doc on the keyhive
        // channel, while the tombstone that drops the branch from the entry
        // travels on the doc channel, so this node can still resolve the ref to a
        // branch doc it can no longer reach. Letting the write through would reach
        // the doc worker and be refused as a local access failure, which misstates
        // the situation: this node is not losing permission on a live branch, the
        // branch is gone as far as it can tell.
        if let Some(branch_ref) = existing_branch_ref.as_ref()
            && !self.branch_doc_reachable(&branch_ref.branch_doc_id).await?
        {
            return Err(DrawerError::BranchNotFound {
                name: branch_path.to_string(),
            });
        }
        let heads = match (heads, existing_branch_ref.as_ref()) {
            (Some(selected_heads), _) => selected_heads,
            (None, Some(branch_ref)) => self
                .get_branch_heads_by_doc_id(branch_ref.branch_doc_id.clone())
                .await?
                .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?,
            (None, None) => {
                return Err(DrawerError::BranchNotFound {
                    name: branch_path.to_string(),
                });
            }
        };

        let now = Timestamp::now();
        let facet_keys_set: Vec<_> = patch.facets_set.keys().cloned().collect();
        let facet_keys_remove = patch.facets_remove.clone();

        let (handle, branch_doc_id, branch_kind) = if let Some(branch_ref) = existing_branch_ref {
            (
                self.get_handle_by_branch_doc_id(branch_ref.branch_doc_id.clone())
                    .await?
                    .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?,
                branch_ref.branch_doc_id,
                branch_ref.branch_kind,
            )
        } else {
            return Err(DrawerError::BranchNotFound {
                name: branch_path.to_string(),
            });
        };
        let mutation_actor_id =
            self.content_actor_id(patch.user_path.as_deref(), branch_doc_id.clone());
        let existing_facet_keys = handle
            .with_document_read(|am_doc| {
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
            .await?;
        let mut resulting_keys = existing_facet_keys;
        for facet_key in patch.facets_set.keys() {
            resulting_keys.insert(facet_key.clone());
        }
        for facet_key in &patch.facets_remove {
            resulting_keys.remove(facet_key);
        }
        self.validate_facets(
            &patch.facets_set,
            &patch.facets_remove,
            &resulting_keys,
            write_scope,
        )
        .await?;

        // 1. Update content doc
        let (new_heads, invalidated_uuids) = handle
            .with_document(|am_doc| {
                am_doc.set_actor(mutation_actor_id.clone());
                let mut tx = am_doc
                    .transaction_at(automerge::PatchLog::null(), &heads)
                    .expect(ERROR_IMPOSSIBLE);

                let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                    _ => {
                        eyre::bail!("facets object not found in content doc");
                    }
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
                    patch.user_path.as_deref(),
                    &mutation_actor_id,
                )?;

                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok((ChangeHashSet(Arc::from([heads])), invalidated_uuids))
            })
            .await??;
        // 2. Update partition store
        self.add_branch_to_partitions_if_needed(branch_kind, branch_doc_id, &new_heads)
            .await?;

        // 3. Update caches and notify
        self.invalidate_entry_cache(&patch.id);

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(&patch.id, &uuid);
        }

        surelock::key::lock_scope(|key| {
            let (mut handles, _key) = key.lock(&self.branch_handles);
            handles.insert(handle.document_id(), handle);
        });

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%id, %to_branch, %from_branch))]
    pub async fn create_branch_at_heads_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<&daybook_types::doc::UserPath>,
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
        let branch_kind = self.branch_kind_for_path(to_branch)?;
        let Some(from_handle) = self
            .resolve_handle_for_branch_heads(id, from_branch, from_heads)
            .await?
        else {
            return Err(DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            });
        };
        let mut branch_doc = from_handle
            .with_document_read(|am_doc| {
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
            .await?;
        // The staging facility: allocation and commit touch no group — a
        // genesis with no coparents leaves the branch document visible to the
        // local agent only, so nothing between allocation and the registration
        // writes below advertises the branch document. The advertising groups
        // are granted at registration, in the sequence below.
        let branch_doc_id = self
            .big_repo
            .allocate_id()
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error allocating branch doc in big repo")?;
        let branch_key = FacetKey::from(WellKnownFacetTag::Branch);
        let branches_key = FacetKey::from(WellKnownFacetTag::Branches);
        let created_from = BranchVersion {
            branch_id: BranchId::from(from_handle.document_id().to_string()),
            heads: from_heads.clone(),
        };
        let branch_facet: serde_json::Value = WellKnownFacet::Branch(Branch {
            document_id: id.clone(),
            branch_id: BranchId::from(branch_doc_id.to_string()),
            created_from: Some(created_from.clone()),
        })
        .into();
        let inherited_facet_keys = (|| -> Result<HashSet<FacetKey>, eyre::Report> {
            let Some((automerge::Value::Object(automerge::ObjType::Map), facets_obj)) =
                branch_doc.get(automerge::ROOT, "facets")?
            else {
                return Ok(HashSet::new());
            };
            let mut keys = HashSet::new();
            for item in automerge::ReadDoc::map_range(&branch_doc, &facets_obj, ..) {
                keys.insert(FacetKey::from(item.key.to_string().as_str()));
            }
            Ok(keys)
        })()?;
        let branches_present = inherited_facet_keys.contains(&branches_key);
        let facet_keys_remove = if branches_present {
            vec![branches_key.clone()]
        } else {
            Vec::new()
        };
        let mut resulting_facet_keys = inherited_facet_keys;
        resulting_facet_keys.insert(branch_key.clone());
        for key in &facet_keys_remove {
            resulting_facet_keys.remove(key);
        }
        let system_facets = HashMap::from([(branch_key.clone(), branch_facet.clone())]);
        self.validate_facets(
            &system_facets,
            &facet_keys_remove,
            &resulting_facet_keys,
            FacetWriteScope::System,
        )
        .await?;
        let mutation_actor_id = self.content_actor_id(user_path, branch_doc_id.clone());
        let heads = (|| -> Result<ChangeHashSet, eyre::Report> {
            branch_doc.set_actor(mutation_actor_id.clone());
            let mut tx = branch_doc.transaction();
            let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => eyre::bail!("facets object not found in branch doc"),
            };
            let branch_key_str = branch_key.to_string();
            autosurgeon::reconcile_prop(
                &mut tx,
                &facets_obj,
                &*branch_key_str,
                ThroughJson(branch_facet),
            )?;
            if branches_present {
                let branches_key_str = branches_key.to_string();
                tx.delete(&facets_obj, &*branches_key_str)?;
            }
            dmeta::apply_update(
                &mut tx,
                &facets_obj,
                std::slice::from_ref(&branch_key),
                &facet_keys_remove,
                Timestamp::now(),
                user_path,
                &mutation_actor_id,
            )?;
            let (heads, _) = tx.commit();
            Ok(ChangeHashSet(Arc::from([heads.expect("commit failed")])))
        })()?;
        // The branch forks `from_handle`'s encrypted causal history, so the
        // initial content is encrypted against keys this node already holds for
        // the source document; they seed the branch's sedimentree.
        let handle = self
            .big_repo
            .commit_id(
                branch_doc_id.clone(),
                branch_doc,
                from_handle.content_keys().await?,
                &[],
            )
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error committing allocated branch doc in big repo")?;
        // Finalize grants — the registration sequence: the advertising groups
        // become members here, before any write that names the branch
        // (the Branches facet below, the partition, the branch-ref registration) —
        // the same shape `register_existing_doc` grants at registration. From
        // commit_id until this point nothing advertised the branch document. A
        // replicated branch grants the drawer group; a local branch does not.
        self.big_repo
            .add_admin_member_to_doc(branch_doc_id.clone(), self.content_docs_group.clone())
            .await?;
        self.big_repo
            .add_admin_member_to_doc(
                branch_doc_id.clone(),
                self.encrypted_blob_docs_group.clone(),
            )
            .await?;
        if branch_kind == BranchKind::Replicated {
            self.big_repo
                .add_admin_member_to_doc(branch_doc_id.clone(), self.drawer_group.clone())
                .await?;
        }
        if branch_kind == BranchKind::Replicated {
            let mut branches = self
                .get_doc_with_facets_at_branch(
                    id,
                    daybook_types::doc::BranchPath::new("main"),
                    Some(vec![branches_key.clone()]),
                )
                .await?
                .ok_or_else(|| ferr!("main branch missing for document '{id}'"))?
                .facets
                .clone()
                .remove(&branches_key)
                .map(serde_json::from_value::<WellKnownFacet>)
                .transpose()
                .map_err(|err| eyre::eyre!(err))?
                .map(|facet| match facet {
                    WellKnownFacet::Branches(branches) => branches,
                    other => panic!("main branches facet has wrong type: {:?}", other.tag()),
                })
                .unwrap_or(Branches {
                    by_name: HashMap::new(),
                    by_id: HashMap::new(),
                });
            let branch_id = BranchId::from(branch_doc_id.to_string());
            branches.insert_declaration(
                branch_id,
                BranchDeclaration {
                    name: Some(to_branch.to_string()),
                    publication: BranchPublication::Shared,
                    scope: AuthorityScope::InheritDocument,
                    created_from: Some(created_from),
                },
            );
            self.update_at_heads_with_scope(
                DocPatch {
                    id: id.clone(),
                    facets_set: [(branches_key, WellKnownFacet::Branches(branches).into())].into(),
                    facets_remove: vec![],
                    user_path: user_path.map(ToOwned::to_owned),
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
                FacetWriteScope::System,
            )
            .await?;
        }
        self.add_branch_to_partitions_if_needed(branch_kind, branch_doc_id.clone(), &heads)
            .await?;

        let _user_path = user_path;
        let _drawer_heads = if branch_kind == BranchKind::Local {
            let vtag = VersionTag::update(self.local_actor_id.clone());
            self.upsert_local_branch_ref(id, to_branch, branch_doc_id.clone(), &vtag)
                .await?;
            self.invalidate_entry_cache(id);
            self.get_drawer_heads()
        } else {
            let latest_drawer_heads = surelock::key::lock_scope(|key| {
                let (heads, _key) = key.lock(&self.current_heads);
                heads.clone()
            });
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

            let drawer_heads = self
                .drawer_doc_handle
                .with_document(|doc| {
                    let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
                    new_entry.previous_version_heads = Some(current_drawer_heads);

                    let mut tx = doc.transaction();
                    let map_id = match tx.get(automerge::ROOT, "docs")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                            match tx.get(&docs_id, "map")? {
                                Some((
                                    automerge::Value::Object(automerge::ObjType::Map),
                                    map_id,
                                )) => map_id,
                                _ => {
                                    eyre::bail!("drawer map not found");
                                }
                            }
                        }
                        _ => {
                            eyre::bail!("drawer docs not found");
                        }
                    };

                    autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
                    let (heads, _) = tx.commit();
                    let heads = heads.expect("commit failed");
                    eyre::Ok(ChangeHashSet(Arc::from([heads])))
                })
                .await??;

            self.invalidate_entry_cache(id);
            surelock::key::lock_scope(|key| {
                let (mut heads, _key) = key.lock(&self.current_heads);
                *heads = drawer_heads.clone();
            });
            drawer_heads
        };
        // The branch-ref write above is what registers the branch (durable in
        // the drawer document or the local-branch table), and the finalize
        // grants landed before everything that names the branch, so nothing
        // ever points at a branch its readers cannot yet authorize. There are
        // no allocation records left to release here: commit_id already deleted
        // the reservation row.
        surelock::key::lock_scope(|key| {
            let (mut handles, _key) = key.lock(&self.branch_handles);
            handles.insert(branch_doc_id, handle);
        });
        Ok(())
    }

    pub async fn ensure_branch_at_heads_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<&daybook_types::doc::UserPath>,
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
        user_path: Option<&daybook_types::doc::UserPath>,
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
        // Using the branch is not merely resolving its ref, exactly as for the write gate
        // above: a peer's delete revokes this repo's access to the branch doc on the
        // keyhive channel while the tombstone that drops the branch from the entry travels
        // on the doc channel, so this node can still resolve the ref to a branch doc it can
        // no longer reach. Merging through such a branch would reach the doc worker and be
        // refused as a local access failure, which misstates the situation: this node is
        // not losing permission on a live branch, the branch is gone as far as it can tell.
        // The target is gated first because it is the branch this call mutates and the one
        // resolved here; gating before the handle lookup also keeps an unreachable branch
        // from being reported as a missing document.
        if !self
            .branch_doc_reachable(&to_branch_ref.branch_doc_id)
            .await?
        {
            return Err(DrawerError::BranchNotFound {
                name: to_branch.to_string(),
            });
        }
        let handle = self
            .get_handle_by_branch_doc_id(to_branch_ref.branch_doc_id.clone())
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;
        let mutation_actor_id =
            self.content_actor_id(user_path, to_branch_ref.branch_doc_id.clone());
        let from_branch_ref = self.get_branch_ref(id, from_branch).await?.ok_or_else(|| {
            DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            }
        })?;
        // The source is a use site too: a merge reads the source branch's content to replay
        // it into the target, so a source this node cannot reach cannot be merged from — the
        // content is not this node's to read. Same reasoning as the target gate above.
        if !self
            .branch_doc_reachable(&from_branch_ref.branch_doc_id)
            .await?
        {
            return Err(DrawerError::BranchNotFound {
                name: from_branch.to_string(),
            });
        }
        let from_handle = self
            .get_handle_by_branch_doc_id(from_branch_ref.branch_doc_id.clone())
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        // 1. Merge content docs
        let user_path_for_dmeta = user_path;
        let mut am_from = from_handle
            .with_document_read(|from_doc| {
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
            .await?;
        let (_new_heads, _modified_facets, invalidated_uuids) = handle
            .with_document(move |am_doc| {
                am_doc.set_actor(mutation_actor_id.clone());
                // A branch merge imports the source history, but branch identity belongs
                // to the physical destination document. Snapshot it before the CRDT merge
                // so a concurrent source `Branch` facet cannot win Automerge's conflict
                // resolution and relabel the destination as the source branch.
                let branch_key = FacetKey::from(WellKnownFacetTag::Branch).to_string();
                let target_branch_facet: ThroughJson<FacetRaw> = {
                    let facets_obj = match am_doc.get(automerge::ROOT, "facets")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("facets object not found in target content doc"),
                    };
                    let target: Option<ThroughJson<FacetRaw>> =
                        autosurgeon::hydrate_prop(am_doc, &facets_obj, &*branch_key)?;
                    target.ok_or_else(|| ferr!("target content doc is missing its Branch facet"))?
                };
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
                    if patch.path.len() >= 2
                        && let (_, automerge::Prop::Map(p0)) = &patch.path[0]
                            && p0 == "facets"
                                && let (_, automerge::Prop::Map(facet_key_str)) = &patch.path[1]
                                {
                                    modified_facets.insert(facet_key_str.to_string());
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
                        am_doc.transaction_at(automerge::PatchLog::inactive(), &heads_now).expect(ERROR_IMPOSSIBLE);
                    let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => { eyre::bail!("facets object not found in content doc"); }
                    };
                    autosurgeon::reconcile_prop(
                        &mut tx,
                        &facets_obj,
                        &*branch_key,
                        target_branch_facet,
                    )?;
                    let now = Timestamp::now();
                    let invalidated = dmeta::apply_merge(
                        &mut tx,
                        &facets_obj,
                        &modified_facets,
                        now,
                        user_path_for_dmeta,
                        &mutation_actor_id,
                    )?;
                    let (heads_after_merge, _) = tx.commit();
                    heads_after_merge.expect("commit failed");
                    invalidated
                };

                eyre::Ok((new_heads, modified_facets, invalidated_uuids))
            })
            .await??;

        let drawer_heads = self.get_drawer_heads();

        // 3. Update caches and notify
        self.invalidate_entry_cache(id);

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(id, &uuid);
        }

        surelock::key::lock_scope(|key| {
            let (mut heads, _key) = key.lock(&self.current_heads);
            *heads = drawer_heads.clone();
        });

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
            .non_tmp_branch_snapshots_for_entry(current_entry.branches)
            .await?;
        let mut deleted_facet_keys_set = HashSet::new();
        for snapshot in deleted_branch_snapshots.values() {
            if let Some(keys) = self.facet_keys_at_branch_snapshot(id, snapshot).await? {
                deleted_facet_keys_set.extend(keys);
            }
        }
        let mut deleted_facet_keys: Vec<FacetKey> = deleted_facet_keys_set.into_iter().collect();
        deleted_facet_keys.sort();

        let res = self
            .drawer_doc_handle
            .with_document(|doc| {
                let docs_id = match doc.get(automerge::ROOT, "docs")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => docs_id,
                    _ => {
                        eyre::bail!("drawer docs not found");
                    }
                };
                let map_id = match doc.get(&docs_id, "map")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                    _ => {
                        eyre::bail!("drawer map not found");
                    }
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
                let mut deleted_tags: Vec<DocDeleteTombstone> =
                    match tx.get(&map_deleted_id, &**id)? {
                        Some((automerge::Value::Object(automerge::ObjType::List), _)) => {
                            autosurgeon::hydrate_prop::<_, Vec<DocDeleteTombstone>, _, _>(
                                &tx,
                                &map_deleted_id,
                                &**id,
                            )?
                        }
                        Some((other, _)) => {
                            eyre::bail!("invalid map_deleted entry shape: {other:?}");
                        }
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
            })
            .await?;

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
                    self.branch_kind_for_path(daybook_types::doc::BranchPath::new(
                        &branch_path[..],
                    ))?,
                    branch_ref.branch_doc_id.clone(),
                )
                .await?;
            }
            self.invalidate_entry_cache(id);
            surelock::key::lock_scope(|key| {
                let (mut handles, _key) = key.lock(&self.branch_handles);
                for branch_ref in entry.branches.values() {
                    handles.remove(&branch_ref.branch_doc_id);
                }
            });
            for (branch_path, branch_doc_id) in local_branch_refs {
                let branch_path = daybook_types::doc::BranchPath::new(&branch_path);
                self.remove_branch_from_partitions_if_needed(
                    self.branch_kind_for_path(branch_path)?,
                    branch_doc_id.clone(),
                )
                .await?;
                let branch_heads = self
                    .get_branch_heads_by_doc_id(branch_doc_id.clone())
                    .await?
                    .unwrap_or_default();
                self.delete_local_branch_ref_with_tombstone(
                    id,
                    branch_path,
                    branch_doc_id.clone(),
                    &branch_heads,
                )
                .await?;
                surelock::key::lock_scope(|key| {
                    let (mut handles, _key) = key.lock(&self.branch_handles);
                    handles.remove(&branch_doc_id);
                });
            }
            self.invalidate_facet_cache_doc(id);
            surelock::key::lock_scope(|key| {
                let (mut heads, _key) = key.lock(&self.current_heads);
                *heads = drawer_heads.clone();
            });
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
                self.update_at_heads(args.patch, &args.branch_path, args.heads)
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
        user_path: Option<&daybook_types::doc::UserPath>,
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

    #[tracing::instrument(level = "trace", skip_all, fields(%id, %branch_path))]
    pub async fn delete_branch(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        _user_path: Option<&daybook_types::doc::UserPath>,
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
            .get_branch_heads_by_doc_id(branch_ref.branch_doc_id.clone())
            .await?
            .ok_or_else(|| ferr!("missing branch doc '{}'", branch_ref.branch_doc_id))?;
        // TEMP-INSTRUMENTATION: trace replicated branch deletion lifecycle.
        tracing::warn!(
            doc = %id,
            branch = %branch_name,
            bdoc = %branch_ref.branch_doc_id,
            kind = ?branch_ref.branch_kind,
            "delete_branch: initiated"
        );
        self.remove_branch_from_partitions_if_needed(
            branch_ref.branch_kind,
            branch_ref.branch_doc_id.clone(),
        )
        .await?;
        surelock::key::lock_scope(|key| {
            let (mut handles, _key) = key.lock(&self.branch_handles);
            handles.remove(&branch_ref.branch_doc_id);
        });

        if branch_ref.branch_kind == BranchKind::Local {
            self.delete_local_branch_ref_with_tombstone(
                id,
                branch_path,
                branch_ref.branch_doc_id,
                &branch_heads,
            )
            .await?;
            self.invalidate_entry_cache(id);
            return Ok(true);
        }

        let latest_drawer_heads = surelock::key::lock_scope(|key| {
            let (heads, _key) = key.lock(&self.current_heads);
            heads.clone()
        });
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

        let drawer_heads = self
            .drawer_doc_handle
            .with_document(|doc| {
                // Test-only: refuse before the commit. The keyhive-channel
                // revocation above has already been applied, so failing here pins
                // the node in the window between the two channels and leaves
                // nothing written: the entry still lists the branch, and a later
                // delete can re-run by name.
                #[cfg(test)]
                if self.take_fail_next_drawer_doc_commit() {
                    eyre::bail!("injected drawer-doc commit failure (test only)");
                }
                let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
                new_entry.previous_version_heads = Some(current_drawer_heads);

                let mut tx = doc.transaction();
                let map_id = match tx.get(automerge::ROOT, "docs")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                        match tx.get(&docs_id, "map")? {
                            Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => {
                                map_id
                            }
                            _ => {
                                eyre::bail!("drawer map not found");
                            }
                        }
                    }
                    _ => {
                        eyre::bail!("drawer docs not found");
                    }
                };

                autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
                let (heads, _) = tx.commit();
                let heads = heads.expect("commit failed");
                eyre::Ok(ChangeHashSet(Arc::from([heads])))
            })
            .await??;

        // Update caches and notify
        self.invalidate_entry_cache(id);

        surelock::key::lock_scope(|key| {
            let (mut heads, _key) = key.lock(&self.current_heads);
            *heads = drawer_heads.clone();
        });

        // TEMP-INSTRUMENTATION: confirm the meta-doc commit that carries the tombstone.
        tracing::warn!(
            doc = %id,
            branch = %branch_name,
            bdoc = %branch_ref.branch_doc_id,
            drawer_heads = %drawer_heads.iter().next().map(ToString::to_string).unwrap_or_default(),
            "delete_branch: tombstone committed to drawer doc"
        );
        Ok(true)
    }
}
