use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::repos::Repo;
use daybook_types::doc::{BranchPath, ChangeHashSet, DocId, WellKnownFacetTag};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tokio_util::sync::CancellationToken;

const FACET_SET_LOCAL_STATE_ID: &str = "@daybook/wip/doc-facet-set-index";

#[derive(Debug, Clone)]
pub struct DocFacetTagMembership {
    pub doc_id: DocId,
    pub facet_tag: String,
    pub origin_heads: ChangeHashSet,
}

#[derive(Debug, Clone)]
pub enum DocFacetSetIndexEvent {
    Updated { doc_id: DocId },
    Deleted { doc_id: DocId },
}

pub struct DocFacetSetIndexRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub cancel_token: CancellationToken,
    drawer_repo: Arc<DrawerRepo>,
    work_tx: tokio::sync::mpsc::UnboundedSender<DocFacetSetIndexWorkItem>,
    db_pool: SqlitePool,
}

impl Repo for DocFacetSetIndexRepo {
    type Event = DocFacetSetIndexEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

pub struct DocFacetSetIndexStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DocFacetSetIndexStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            utils_rs::wait_on_handle_with_timeout(handle, 10000).await?;
        }
        Ok(())
    }
}

impl DocFacetSetIndexRepo {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, DocFacetSetIndexStopToken)> {
        let (_sqlite_file_path, db_pool) = sqlite_local_state_repo
            .ensure_sqlite_pool(FACET_SET_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&db_pool).await?;
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            work_tx,
            db_pool,
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.clone();
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => break,
                        item = work_rx.recv() => {
                            let Some(item) = item else {
                                break;
                            };
                            repo.handle_worker_item(item).await.unwrap_or_log();
                        }
                    }
                }
            }
        });

        Ok((
            repo,
            DocFacetSetIndexStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(db_pool: &SqlitePool) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS facet_set_docs (
                doc_id TEXT PRIMARY KEY
            )
            "#,
        )
        .execute(db_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS facet_set_tags (
                tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
                facet_tag TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(db_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS facet_set_doc_tags (
                doc_id TEXT NOT NULL,
                tag_id INTEGER NOT NULL,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(doc_id, tag_id),
                FOREIGN KEY(doc_id) REFERENCES facet_set_docs(doc_id) ON DELETE CASCADE,
                FOREIGN KEY(tag_id) REFERENCES facet_set_tags(tag_id)
            )
            "#,
        )
        .execute(db_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_set_doc_tags_tag_id ON facet_set_doc_tags(tag_id)",
        )
        .execute(db_pool)
        .await?;

        Ok(())
    }

    async fn handle_worker_item(&self, item: DocFacetSetIndexWorkItem) -> Res<()> {
        match item {
            DocFacetSetIndexWorkItem::Upsert { doc_id, heads } => {
                let Some(facet_keys) = self
                    .drawer_repo
                    .facet_keys_at_heads(&doc_id, &heads)
                    .await?
                else {
                    return Ok(());
                };
                self.reindex_doc_with_keys(&doc_id, &heads, &facet_keys)
                    .await?;
                self.registry
                    .notify([DocFacetSetIndexEvent::Updated { doc_id }]);
            }
            DocFacetSetIndexWorkItem::DeleteDoc { doc_id } => {
                self.delete_doc(&doc_id).await?;
                self.registry
                    .notify([DocFacetSetIndexEvent::Deleted { doc_id }]);
            }
        }
        Ok(())
    }

    async fn ensure_tag_id(tx: &mut Transaction<'_, Sqlite>, facet_tag: &str) -> Res<i64> {
        sqlx::query("INSERT OR IGNORE INTO facet_set_tags (facet_tag) VALUES (?1)")
            .bind(facet_tag)
            .execute(tx.as_mut())
            .await?;

        let tag_id: i64 =
            sqlx::query_scalar("SELECT tag_id FROM facet_set_tags WHERE facet_tag = ?1")
                .bind(facet_tag)
                .fetch_one(tx.as_mut())
                .await?;
        Ok(tag_id)
    }

    pub async fn reindex_doc_with_keys(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facet_keys: &HashSet<daybook_types::doc::FacetKey>,
    ) -> Res<()> {
        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);
        let mut desired_tags: HashSet<String> = facet_keys
            .iter()
            .map(|facet_key| facet_key.tag.to_string())
            .collect();
        desired_tags.remove(WellKnownFacetTag::Dmeta.as_str());

        let mut tx = self.db_pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO facet_set_docs (doc_id) VALUES (?1)")
            .bind(doc_id)
            .execute(tx.as_mut())
            .await?;

        let mut desired_tag_ids: HashSet<i64> = HashSet::new();
        for facet_tag in &desired_tags {
            let tag_id = Self::ensure_tag_id(&mut tx, facet_tag).await?;
            desired_tag_ids.insert(tag_id);
            sqlx::query(
                r#"
                INSERT INTO facet_set_doc_tags (doc_id, tag_id, origin_heads)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(doc_id, tag_id)
                DO UPDATE SET origin_heads = excluded.origin_heads
                "#,
            )
            .bind(doc_id)
            .bind(tag_id)
            .bind(&serialized_heads)
            .execute(tx.as_mut())
            .await?;
        }

        let existing_tag_ids: Vec<i64> =
            sqlx::query_scalar("SELECT tag_id FROM facet_set_doc_tags WHERE doc_id = ?1")
                .bind(doc_id)
                .fetch_all(tx.as_mut())
                .await?;

        for existing_tag_id in existing_tag_ids {
            if !desired_tag_ids.contains(&existing_tag_id) {
                sqlx::query("DELETE FROM facet_set_doc_tags WHERE doc_id = ?1 AND tag_id = ?2")
                    .bind(doc_id)
                    .bind(existing_tag_id)
                    .execute(tx.as_mut())
                    .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        sqlx::query("DELETE FROM facet_set_doc_tags WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&self.db_pool)
            .await?;
        sqlx::query("DELETE FROM facet_set_docs WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&self.db_pool)
            .await?;
        Ok(())
    }

    pub async fn list_tags_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        let tags: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT t.facet_tag
            FROM facet_set_doc_tags dt
            JOIN facet_set_tags t ON t.tag_id = dt.tag_id
            WHERE dt.doc_id = ?1
            ORDER BY t.facet_tag ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;
        Ok(tags)
    }

    pub async fn list_docs_for_tag(&self, facet_tag: &str) -> Res<Vec<DocFacetTagMembership>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT dt.doc_id, dt.origin_heads
            FROM facet_set_tags t
            JOIN facet_set_doc_tags dt ON dt.tag_id = t.tag_id
            WHERE t.facet_tag = ?1
            ORDER BY dt.doc_id ASC
            "#,
        )
        .bind(facet_tag)
        .fetch_all(&self.db_pool)
        .await?;

        rows.into_iter()
            .map(|(doc_id, origin_heads)| {
                let head_strings: Vec<String> = serde_json::from_str(&origin_heads)?;
                Ok(DocFacetTagMembership {
                    doc_id,
                    facet_tag: facet_tag.to_string(),
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }

    pub async fn has_tag(&self, doc_id: &DocId, facet_tag: &str) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT 1
            FROM facet_set_doc_tags dt
            JOIN facet_set_tags t ON t.tag_id = dt.tag_id
            WHERE dt.doc_id = ?1 AND t.facet_tag = ?2
            LIMIT 1
            "#,
        )
        .bind(doc_id)
        .bind(facet_tag)
        .fetch_optional(&self.db_pool)
        .await?;
        Ok(exists.is_some())
    }

    pub fn triage_listener(
        self: &Arc<Self>,
    ) -> Box<dyn crate::rt::switch::SwitchSink + Send + Sync> {
        Box::new(FacetSetTriageListener {
            drawer_repo: Arc::clone(&self.drawer_repo),
            index_repo: Arc::clone(self),
        })
    }

    pub fn enqueue_upsert(&self, doc_id: DocId, heads: ChangeHashSet) -> Res<()> {
        self.work_tx
            .send(DocFacetSetIndexWorkItem::Upsert { doc_id, heads })
            .map_err(|err| ferr!("doc_facet_set_index work queue closed: {err}"))?;
        Ok(())
    }

    pub fn enqueue_delete(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(DocFacetSetIndexWorkItem::DeleteDoc { doc_id })
            .map_err(|err| ferr!("doc_facet_set_index work queue closed: {err}"))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum DocFacetSetIndexWorkItem {
    Upsert { doc_id: DocId, heads: ChangeHashSet },
    DeleteDoc { doc_id: DocId },
}

struct FacetSetTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    index_repo: Arc<DocFacetSetIndexRepo>,
}

#[async_trait]
impl crate::rt::switch::SwitchSink for FacetSetTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_drawer: true,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            drawer_predicate: None,
        }
    }

    async fn on_event(
        &mut self,
        event: &crate::rt::switch::SwitchEvent,
        _ctx: &crate::rt::switch::SwitchSinkCtx<'_>,
    ) -> Res<crate::rt::switch::SwitchSinkOutcome> {
        let outcome = crate::rt::switch::SwitchSinkOutcome::default();
        let crate::rt::switch::SwitchEvent::Drawer(event) = event else {
            return Ok(outcome);
        };
        match &**event {
            crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                self.index_repo.enqueue_delete(id.clone())?;
            }
            crate::drawer::DrawerEvent::DocAdded {
                id,
                entry,
                drawer_heads,
            } => {
                let Some(heads) = entry.branches.get("main") else {
                    return Ok(outcome);
                };
                let branch_path = BranchPath::from("main");
                let Some(_keys) = self
                    .drawer_repo
                    .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                    .await?
                else {
                    return Ok(outcome);
                };
                self.index_repo.enqueue_upsert(id.clone(), heads.clone())?;
            }
            crate::drawer::DrawerEvent::DocUpdated {
                id,
                entry,
                diff,
                drawer_heads,
                ..
            } => {
                if diff
                    .changed_facet_keys
                    .iter()
                    .all(|facet_key| facet_key.tag == WellKnownFacetTag::Dmeta.into())
                {
                    return Ok(outcome);
                }
                if !diff
                    .moved_branch_names
                    .iter()
                    .any(|branch_name| branch_name == "main")
                {
                    return Ok(outcome);
                }
                let Some(heads) = entry.branches.get("main") else {
                    self.index_repo.enqueue_delete(id.clone())?;
                    return Ok(outcome);
                };
                let branch_path = BranchPath::from("main");
                let Some(_keys) = self
                    .drawer_repo
                    .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                    .await?
                else {
                    self.index_repo.enqueue_delete(id.clone())?;
                    return Ok(outcome);
                };
                self.index_repo.enqueue_upsert(id.clone(), heads.clone())?;
            }
            crate::drawer::DrawerEvent::ListChanged { .. } => {}
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::test_cx;
    use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet};

    async fn wait_for_doc_tag(
        repo: &DocFacetSetIndexRepo,
        doc_id: &DocId,
        facet_tag: &str,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            if repo.has_tag(doc_id, facet_tag).await? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!("timeout waiting for condition")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_facet_set_index_tracks_tags() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_facet_set_index_repo);

        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPath::from("main"),
                facets: [
                    (
                        FacetKey::from(WellKnownFacetTag::Note),
                        FacetRaw::from(WellKnownFacet::Note("hello".to_string().into())),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::LabelGeneric),
                        FacetRaw::from(WellKnownFacet::LabelGeneric("x".to_string())),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_doc_tag(&repo, &doc_id, WellKnownFacetTag::Note.as_str()).await?;

        let tags = repo.list_tags_for_doc(&doc_id).await?;
        assert!(tags.contains(&WellKnownFacetTag::Note.as_str().to_string()));
        assert!(tags.contains(&WellKnownFacetTag::LabelGeneric.as_str().to_string()));
        assert!(!tags.contains(&WellKnownFacetTag::Dmeta.as_str().to_string()));

        test_context.stop().await?;
        Ok(())
    }
}
