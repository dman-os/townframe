use crate::interlude::*;

pub mod version_updates {
    use crate::interlude::*;
    use automerge::ROOT;
    use automerge::transaction::Transactable;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| {
            tx.put(ROOT, "version", "0")?;
            tx.put(ROOT, "$schema", "daybook.drawer")?;
            Ok::<_, automerge::AutomergeError>(())
        })
        .map_err(|err| ferr!("{err:?}"))?;
        Ok(doc.save_nocompress())
    }
}

pub mod doc_version_updates {
    use crate::interlude::*;
    use automerge::ROOT;
    use automerge::transaction::Transactable;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| {
            tx.put(ROOT, "version", "0")?;
            tx.put(ROOT, "$schema", "daybook.doc")?;
            tx.put_object(ROOT, "facets", automerge::ObjType::Map)?;
            Ok::<_, automerge::AutomergeError>(())
        })
        .map_err(|err| ferr!("{err:?}"))?;
        Ok(doc.save_nocompress())
    }
}

#[cfg(test)]
use super::BranchStateRow;
use super::{BranchKind, BranchRefRow, DrawerRepo};
use crate::drawer::types::{DocEntry, DocNBranches, StoredBranchRef};
use crate::stores::VersionTag;
use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, DocId};

/// Create the local-branch SQL tables if the db predates them. Idempotent;
/// also called by the boot sweep's registration read, which can run before
/// a drawer boot ever created the schema.
pub(crate) async fn ensure_local_branch_schema(sql: &SqlCtx) -> Res<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS drawer_local_branches (
            doc_id TEXT NOT NULL,
            branch_path TEXT NOT NULL,
            branch_doc_id BLOB NOT NULL,
            vtag_version TEXT NOT NULL,
            vtag_actor_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (doc_id, branch_path)
        ) STRICT
        "#,
    )
    .execute(&sql.write_pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS drawer_local_branches_deleted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            branch_path TEXT NOT NULL,
            branch_doc_id BLOB NOT NULL,
            branch_heads_json TEXT NOT NULL,
            vtag_version TEXT NOT NULL,
            vtag_actor_id TEXT NOT NULL,
            deleted_at INTEGER NOT NULL
        ) STRICT
        "#,
    )
    .execute(&sql.write_pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_doc_id ON drawer_local_branches(doc_id)",
    )
    .execute(&sql.write_pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_deleted_doc_path ON drawer_local_branches_deleted(doc_id, branch_path, deleted_at DESC)",
    )
    .execute(&sql.write_pool)
    .await?;
    Ok(())
}

impl DrawerRepo {
    pub(super) async fn ensure_local_branch_schema(&self) -> Res<()> {
        ensure_local_branch_schema(&self.meta_store_sql).await
    }
    pub(super) async fn upsert_local_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        branch_doc_id: DocumentId,
        vtag: &VersionTag,
    ) -> Res<()> {
        let updated_at = jiff::Timestamp::now().as_microsecond();
        sqlx::query(
            r#"
            INSERT INTO "drawer_local_branches" (
                doc_id, branch_path, branch_doc_id, vtag_version, vtag_actor_id, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(doc_id, branch_path) DO UPDATE SET
                branch_doc_id = excluded.branch_doc_id,
                vtag_version = excluded.vtag_version,
                vtag_actor_id = excluded.vtag_actor_id,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(doc_id)
        .bind(branch_path.to_string())
        .bind(branch_doc_id.as_bytes())
        .bind(vtag.version.to_string())
        .bind(vtag.actor_id.to_string())
        .bind(updated_at)
        .execute(&self.meta_store_sql.write_pool)
        .await?;
        Ok(())
    }

    pub(super) async fn get_local_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<DocumentId>> {
        let rec = sqlx::query_scalar::<_, Vec<u8>>(
            r#"SELECT branch_doc_id FROM "drawer_local_branches" WHERE doc_id = ?1 AND branch_path = ?2"#
        )
        .bind(doc_id)
        .bind(branch_path.to_string())
        .fetch_optional(&self.meta_store_sql.write_pool)
        .await?;

        Ok(rec.map(DocumentId::new))
    }

    pub(super) async fn list_local_branch_refs(
        &self,
        doc_id: &DocId,
    ) -> Res<Vec<(String, DocumentId)>> {
        Ok(sqlx::query_as::<_, (String, Vec<u8>)>(
            r#"SELECT branch_path, branch_doc_id 
                FROM "drawer_local_branches" 
                WHERE doc_id = ?1 ORDER BY branch_path ASC"#,
        )
        .bind(doc_id)
        .fetch_all(&self.meta_store_sql.write_pool)
        .await?
        .into_iter()
        .map(|(path, id)| (path, DocumentId::new(id)))
        .collect())
    }

    pub(super) async fn delete_local_branch_ref_with_tombstone(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        branch_doc_id: DocumentId,
        branch_heads: &ChangeHashSet,
    ) -> Res<()> {
        let deleted_at = jiff::Timestamp::now().as_microsecond();
        let vtag = VersionTag::update(self.local_actor_id.clone());
        let branch_heads_json =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(branch_heads.as_ref()))
                .expect(ERROR_JSON);

        let mut tx = self
            .meta_store_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        sqlx::query(
            r#"
            INSERT INTO "drawer_local_branches_deleted" (
                doc_id, branch_path, branch_doc_id, branch_heads_json, vtag_version, vtag_actor_id, deleted_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#
        )
        .bind(doc_id)
        .bind(branch_path.to_string())
        .bind(branch_doc_id.as_bytes())
        .bind(branch_heads_json)
        .bind(vtag.version.to_string())
        .bind(vtag.actor_id.to_string())
        .bind(deleted_at)
        .execute(tx.as_mut())
        .await?;
        sqlx::query(
            r#"DELETE FROM "drawer_local_branches" WHERE doc_id = ?1 AND branch_path = ?2"#,
        )
        .bind(doc_id)
        .bind(branch_path.to_string())
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn get_entry_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<(StoredBranchRef, BranchKind)>> {
        let branch_kind = self.branch_kind_for_path(branch_path)?;
        if branch_kind == BranchKind::Local {
            let Some(branch_doc_id) = self.get_local_branch_ref(doc_id, branch_path).await? else {
                return Ok(None);
            };
            return Ok(Some((StoredBranchRef { branch_doc_id }, branch_kind)));
        }

        let Some(entry) = self.get_entry(doc_id).await? else {
            return Ok(None);
        };
        let branch_path_str = branch_path.as_str();
        let Some(branch_ref) = entry.branches.get(branch_path_str) else {
            return Ok(None);
        };
        Ok(Some((branch_ref.clone(), branch_kind)))
    }

    pub(crate) async fn get_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<BranchRefRow>> {
        let Some((branch_ref, branch_kind)) =
            self.get_entry_branch_ref(doc_id, branch_path).await?
        else {
            return Ok(None);
        };
        Ok(Some(BranchRefRow {
            branch_doc_id: branch_ref.branch_doc_id,
            branch_kind,
        }))
    }

    #[cfg(test)]
    pub(super) async fn get_branch_state(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<BranchStateRow>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let Some(latest_heads) = self
            .get_branch_heads_by_doc_id(branch_ref.branch_doc_id.clone())
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(BranchStateRow {
            branch_path: branch_path.to_string(),
            branch_doc_id: branch_ref.branch_doc_id,
            latest_heads,
            branch_kind: branch_ref.branch_kind,
        }))
    }

    pub(super) async fn current_doc_branches_from_entry(
        &self,
        doc_id: &DocId,
        entry: &DocEntry,
    ) -> Res<DocNBranches> {
        let mut branch_names = entry.branches.keys().cloned().collect::<Vec<_>>();
        branch_names.sort();
        let mut branches = HashMap::new();
        for branch_name in branch_names {
            let branch_path = daybook_types::doc::BranchPath::new(branch_name.as_str());
            if self.branch_kind_for_path(branch_path)? == BranchKind::Local {
                continue;
            }
            let Some(branch_ref) = entry.branches.get(&branch_name) else {
                continue;
            };
            // A branch this node cannot reach is not part of its view: a peer's
            // delete revokes this repo's access to the branch doc on the keyhive
            // channel while the tombstone that drops the branch from the entry
            // travels on the doc channel, so the entry can list a branch whose
            // branch doc this node can no longer reach. Presenting it as a live
            // branch would surface a write that can only be refused.
            if !self.branch_doc_reachable(&branch_ref.branch_doc_id).await? {
                continue;
            }
            let Some(latest_heads) = self
                .get_branch_heads_by_doc_id(branch_ref.branch_doc_id.clone())
                .await?
            else {
                // TEMP-INSTRUMENTATION: warn so convergence hangs name the offender.
                tracing::warn!(
                    %doc_id,
                    %branch_name,
                    bdoc_id = %branch_ref.branch_doc_id,
                    "branch doc not ready yet during current_doc_branches_from_entry"
                );
                continue;
            };
            branches.insert(branch_name, latest_heads);
        }
        for (branch_path, branch_doc_id) in self.list_local_branch_refs(doc_id).await? {
            let Some(latest_heads) = self
                .get_branch_heads_by_doc_id(branch_doc_id.clone())
                .await?
            else {
                debug!(
                    %doc_id,
                    %branch_path,
                    %branch_doc_id,
                    "local branch doc not ready yet during current_doc_branches_from_entry"
                );
                continue;
            };
            branches.insert(branch_path, latest_heads);
        }
        Ok(DocNBranches {
            doc_id: doc_id.clone(),
            branches,
        })
    }

    pub(super) async fn current_doc_branches(&self, doc_id: &DocId) -> Res<Option<DocNBranches>> {
        let Some(entry) = self.get_entry(doc_id).await? else {
            return Ok(None);
        };
        self.current_doc_branches_from_entry(doc_id, &entry)
            .await
            .map(Some)
    }

    pub(super) async fn current_drawer_entries(
        &self,
    ) -> Res<(ChangeHashSet, Vec<(DocId, DocEntry)>)> {
        self.drawer_doc_handle
            .with_document_read(|doc| {
                let drawer_heads = ChangeHashSet(doc.get_heads().into());
                let map_id = match doc.get(automerge::ROOT, "docs")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => {
                        match doc.get(&id, "map")? {
                            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                            _ => {
                                eyre::bail!("invalid drawer shape");
                            }
                        }
                    }
                    None => return eyre::Ok((drawer_heads, Vec::new())),
                    _ => {
                        eyre::bail!("invalid drawer shape");
                    }
                };

                let mut entries = Vec::new();
                for item in doc.map_range(&map_id, ..) {
                    let doc_id = DocId::from(item.key.clone());
                    let entry: Option<DocEntry> =
                        autosurgeon::hydrate_prop(doc, &map_id, item.key)?;
                    if let Some(entry) = entry {
                        entries.push((doc_id, entry));
                    }
                }
                eyre::Ok((drawer_heads, entries))
            })
            .await
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn hydrate_entry_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocEntry>> {
        let path = vec![
            "docs".into(),
            "map".into(),
            autosurgeon::Prop::Key(doc_id.to_string().into()),
        ];
        let entry = self
            .drawer_doc_handle
            .hydrate_path_at_heads::<DocEntry>(heads, automerge::ROOT, path)
            .await?;
        Ok(entry)
    }
}

/// The registration shape of a pending allocation as the drawer's durable
/// surfaces record it — the surface is the kind: content docs are `docs.map`
/// keys, replicated branches ride an entry's branch refs, and a local branch
/// ref never travels on the drawer document at all (it lives in the
/// `drawer_local_branches` SQL table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisteredAllocationShape {
    ContentDoc,
    ReplicatedBranch,
    LocalBranch,
}

/// Read every pending allocation's registration off the drawer's durable
/// surfaces in one pass: one read of the drawer document's `docs.map` plus one
/// query over the local-branch SQL (whose schema may not exist yet — the sweep
/// can run before a drawer boot created it).
///
/// Returns `Ok(None)` when the drawer document is not readable at this boot
/// stage (missing, or still pending materialization on a fresh clone): the
/// boot sweep then keeps every reservation instead of classifying, since a
/// registered-but-unreadable allocation must never be discarded.
pub(crate) async fn registered_allocation_shapes(
    sql: &SqlCtx,
    big_repo: &big_repo::SharedBigRepo,
    drawer_doc_id: &big_repo::DocumentId,
) -> Res<Option<HashMap<big_repo::DocumentId, RegisteredAllocationShape>>> {
    let big_repo::DocLookup::Ready(drawer_handle) = big_repo.get_doc(drawer_doc_id).await? else {
        tracing::warn!(
            %drawer_doc_id,
            "pending allocations keep their boot sweep: the drawer document is not readable here"
        );
        return Ok(None);
    };

    let mut shapes = HashMap::new();
    drawer_handle
        .with_document_read(|doc| {
            let map_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match doc.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("invalid drawer shape"),
                    }
                }
                None => return eyre::Ok(()),
                _ => eyre::bail!("invalid drawer shape"),
            };
            for item in doc.map_range(&map_id, ..) {
                let doc_id = DocId::from(item.key.clone());
                let entry: Option<DocEntry> = autosurgeon::hydrate_prop(doc, &map_id, item.key)?;
                let Some(entry) = entry else {
                    continue;
                };
                let doc_id = doc_id
                    .to_string()
                    .parse::<big_repo::DocumentId>()
                    .map_err(|err| ferr!("drawer docs.map key is not a document id: {err}"))?;
                for branch_ref in entry.branches.values() {
                    shapes.insert(
                        branch_ref.branch_doc_id.clone(),
                        RegisteredAllocationShape::ReplicatedBranch,
                    );
                }
                shapes.insert(doc_id, RegisteredAllocationShape::ContentDoc);
            }
            eyre::Ok(())
        })
        .await?;

    ensure_local_branch_schema(sql).await?;
    let local_rows: Vec<(Vec<u8>,)> =
        sqlx::query_as("SELECT branch_doc_id FROM drawer_local_branches")
            .fetch_all(&sql.write_pool)
            .await?;
    for (branch_doc_id,) in local_rows {
        let branch_doc_id: [u8; 32] = branch_doc_id
            .try_into()
            .map_err(|err| ferr!("drawer local branch doc id is not 32 bytes: {err:?}"))?;
        shapes.insert(
            big_repo::DocumentId::new(branch_doc_id),
            RegisteredAllocationShape::LocalBranch,
        );
    }
    Ok(Some(shapes))
}
