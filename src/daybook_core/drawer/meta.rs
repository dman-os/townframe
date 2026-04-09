use crate::interlude::*;

#[cfg(test)]
use super::BranchStateRow;
use super::{BranchKind, BranchRefRow, DrawerRepo};
use crate::drawer::types::{DocEntry, DocEntryDiff, DocNBranches, StoredBranchRef};
use crate::stores::VersionTag;
use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, DocId};

const LOCAL_BRANCH_TABLE: &str = "drawer_local_branches";
const LOCAL_BRANCH_DELETED_TABLE: &str = "drawer_local_branches_deleted";

impl DrawerRepo {
    pub(super) async fn ensure_local_branch_schema(&self) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS drawer_local_branches (
                doc_id TEXT NOT NULL,
                branch_path TEXT NOT NULL,
                branch_doc_id TEXT NOT NULL,
                vtag_version TEXT NOT NULL,
                vtag_actor_id TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (doc_id, branch_path)
            )
            "#,
        )
        .execute(&self.meta_db_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS drawer_local_branches_deleted (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                branch_path TEXT NOT NULL,
                branch_doc_id TEXT NOT NULL,
                branch_heads_json TEXT NOT NULL,
                vtag_version TEXT NOT NULL,
                vtag_actor_id TEXT NOT NULL,
                deleted_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.meta_db_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_doc_id ON drawer_local_branches(doc_id)",
        )
        .execute(&self.meta_db_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_deleted_doc_path ON drawer_local_branches_deleted(doc_id, branch_path, deleted_at DESC)",
        )
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    pub(super) async fn upsert_local_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        branch_doc_id: &str,
        vtag: &VersionTag,
    ) -> Res<()> {
        let updated_at = jiff::Timestamp::now().as_microsecond();
        sqlx::query(&format!(
            r#"
            INSERT INTO {LOCAL_BRANCH_TABLE} (
                doc_id, branch_path, branch_doc_id, vtag_version, vtag_actor_id, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(doc_id, branch_path) DO UPDATE SET
                branch_doc_id = excluded.branch_doc_id,
                vtag_version = excluded.vtag_version,
                vtag_actor_id = excluded.vtag_actor_id,
                updated_at = excluded.updated_at
            "#
        ))
        .bind(doc_id)
        .bind(branch_path.to_string())
        .bind(branch_doc_id)
        .bind(vtag.version.to_string())
        .bind(vtag.actor_id.to_string())
        .bind(updated_at)
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    pub(super) async fn get_local_branch_ref(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<String>> {
        let rec = sqlx::query_scalar::<_, String>(&format!(
            "SELECT branch_doc_id FROM {LOCAL_BRANCH_TABLE} WHERE doc_id = ?1 AND branch_path = ?2"
        ))
        .bind(doc_id)
        .bind(branch_path.to_string())
        .fetch_optional(&self.meta_db_pool)
        .await?;
        Ok(rec)
    }

    pub(super) async fn list_local_branch_refs(
        &self,
        doc_id: &DocId,
    ) -> Res<Vec<(String, String)>> {
        let rows = sqlx::query_as::<_, (String, String)>(&format!(
            "SELECT branch_path, branch_doc_id FROM {LOCAL_BRANCH_TABLE} WHERE doc_id = ?1 ORDER BY branch_path ASC"
        ))
        .bind(doc_id)
        .fetch_all(&self.meta_db_pool)
        .await?;
        Ok(rows)
    }

    pub(super) async fn delete_local_branch_ref_with_tombstone(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        branch_doc_id: &str,
        branch_heads: &ChangeHashSet,
    ) -> Res<()> {
        let deleted_at = jiff::Timestamp::now().as_microsecond();
        let vtag = VersionTag::update(self.local_actor_id.clone());
        let branch_heads_json =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(branch_heads.as_ref()))
                .expect(ERROR_JSON);

        let mut tx = self.meta_db_pool.begin().await?;
        sqlx::query(&format!(
            r#"
            INSERT INTO {LOCAL_BRANCH_DELETED_TABLE} (
                doc_id, branch_path, branch_doc_id, branch_heads_json, vtag_version, vtag_actor_id, deleted_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#
        ))
        .bind(doc_id)
        .bind(branch_path.to_string())
        .bind(branch_doc_id)
        .bind(branch_heads_json)
        .bind(vtag.version.to_string())
        .bind(vtag.actor_id.to_string())
        .bind(deleted_at)
        .execute(tx.as_mut())
        .await?;
        sqlx::query(&format!(
            "DELETE FROM {LOCAL_BRANCH_TABLE} WHERE doc_id = ?1 AND branch_path = ?2"
        ))
        .bind(doc_id)
        .bind(branch_path.to_string())
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn migrate_legacy_local_branches_from_drawer_map(&self) -> Res<()> {
        let mut migrated_rows: Vec<(DocId, daybook_types::doc::BranchPath, String)> = Vec::new();
        let (changed, drawer_heads) = self.drawer_am_handle.with_document(|doc| {
            let current_heads = ChangeHashSet(doc.get_heads().into());
            let map_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => {
                    match doc.get(&id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("invalid drawer shape"),
                    }
                }
                None => return eyre::Ok((false, current_heads)),
                _ => eyre::bail!("invalid drawer shape"),
            };

            let mut updates: Vec<(DocId, DocEntry)> = Vec::new();
            for item in doc.map_range(&map_id, ..) {
                let doc_id = DocId::from(item.key.clone());
                let entry: DocEntry =
                    autosurgeon::hydrate_prop::<_, DocEntry, _, _>(doc, &map_id, item.key)?;
                let mut next_entry = entry.clone();
                let mut moved_any = false;
                let mut branch_names: Vec<String> = next_entry.branches.keys().cloned().collect();
                branch_names.sort();
                for branch_name in branch_names {
                    let branch_path: daybook_types::doc::BranchPath =
                        daybook_types::doc::BranchPath::from(branch_name.as_str());
                    if branch_path == "/tmp" || branch_path.starts_with("/tmp/") {
                        let Some(branch_ref) = next_entry.branches.remove(&branch_name) else {
                            continue;
                        };
                        migrated_rows.push((doc_id.clone(), branch_path, branch_ref.branch_doc_id));
                        moved_any = true;
                    }
                }
                if moved_any {
                    next_entry.vtag = VersionTag::update(self.local_actor_id.clone());
                    next_entry.previous_version_heads = Some(current_heads.clone());
                    updates.push((doc_id, next_entry));
                }
            }

            if updates.is_empty() {
                return eyre::Ok((false, current_heads));
            }

            let mut tx = doc.transaction();
            let map_id = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => {
                    match tx.get(&id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("invalid drawer shape"),
                    }
                }
                _ => eyre::bail!("invalid drawer shape"),
            };
            for (doc_id, entry) in updates {
                autosurgeon::reconcile_prop(&mut tx, &map_id, &*doc_id, entry)?;
            }
            let (heads, _) = tx.commit();
            let head = heads.expect("commit failed");
            eyre::Ok((true, ChangeHashSet(Arc::from([head]))))
        })?;

        if migrated_rows.is_empty() {
            return Ok(());
        }

        let vtag = VersionTag::update(self.local_actor_id.clone());
        for (doc_id, branch_path, branch_doc_id) in &migrated_rows {
            self.upsert_local_branch_ref(doc_id, branch_path, branch_doc_id, &vtag)
                .await?;
        }

        if changed {
            *self.current_heads.lock().expect(ERROR_MUTEX) = drawer_heads.clone();
            let mut changed_docs: HashSet<DocId> = HashSet::new();
            for (doc_id, _, _) in &migrated_rows {
                changed_docs.insert(doc_id.clone());
            }
            for doc_id in changed_docs {
                let Some(entry) = self.current_doc_branches(&doc_id).await? else {
                    tracing::warn!(
                        ?doc_id,
                        "missing doc branches while notifying migrated local branches"
                    );
                    continue;
                };
                self.registry
                    .notify([crate::drawer::DrawerEvent::DocUpdated {
                        id: doc_id,
                        entry,
                        diff: DocEntryDiff {
                            changed_facet_keys: Vec::new(),
                            added_facet_keys: Vec::new(),
                            removed_facet_keys: Vec::new(),
                            moved_branch_names: Vec::new(),
                        },
                        drawer_heads: drawer_heads.clone(),
                        origin: self.local_origin(),
                    }]);
            }
        }
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
        let branch_path_str = branch_path.to_string();
        let Some(branch_ref) = entry.branches.get(&branch_path_str) else {
            return Ok(None);
        };
        Ok(Some((branch_ref.clone(), branch_kind)))
    }

    pub(super) async fn get_branch_ref(
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
    #[allow(dead_code)]
    pub(super) async fn get_branch_state(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<BranchStateRow>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let Some(latest_heads) = self
            .get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
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
            let branch_path = daybook_types::doc::BranchPath::from(branch_name.as_str());
            if self.branch_kind_for_path(&branch_path)? == BranchKind::Local {
                continue;
            }
            let Some(branch_ref) = entry.branches.get(&branch_name) else {
                continue;
            };
            let Some(latest_heads) = self
                .get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
                .await?
            else {
                tracing::warn!(
                    branch_name = %branch_name,
                    branch_doc_id = %branch_ref.branch_doc_id,
                    "missing branch heads for drawer branch ref"
                );
                continue;
            };
            branches.insert(branch_name, latest_heads);
        }
        for (branch_path, branch_doc_id) in self.list_local_branch_refs(doc_id).await? {
            let Some(latest_heads) = self.get_branch_heads_by_doc_id(&branch_doc_id).await? else {
                tracing::warn!(
                    branch_path = %branch_path,
                    branch_doc_id = %branch_doc_id,
                    "missing branch heads for local branch ref"
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

    pub(super) fn current_drawer_entries(&self) -> Res<(ChangeHashSet, Vec<(DocId, DocEntry)>)> {
        self.drawer_am_handle.with_document(|doc| {
            let drawer_heads = ChangeHashSet(doc.get_heads().into());
            let map_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => {
                    match doc.get(&id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("invalid drawer shape"),
                    }
                }
                None => return eyre::Ok((drawer_heads, Vec::new())),
                _ => eyre::bail!("invalid drawer shape"),
            };

            let mut entries = Vec::new();
            for item in doc.map_range(&map_id, ..) {
                let doc_id = DocId::from(item.key.clone());
                let entry: Option<DocEntry> = autosurgeon::hydrate_prop(doc, &map_id, item.key)?;
                if let Some(entry) = entry {
                    entries.push((doc_id, entry));
                }
            }
            eyre::Ok((drawer_heads, entries))
        })
    }

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
            .big_repo
            .hydrate_path_at_heads::<DocEntry>(&self.drawer_doc_id, heads, automerge::ROOT, path)
            .await?;
        Ok(entry.map(|(entry_value, _)| entry_value))
    }
}
