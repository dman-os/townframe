use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::interlude::*;
use crate::repos::Repo;
use crate::stores::Store;
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

#[derive(Default, Debug, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct DocFacetSetIndexWorkerStateStore {
    pub drawer_heads: Option<ChangeHashSet>,
}

#[async_trait]
impl Store for DocFacetSetIndexWorkerStateStore {
    fn prop() -> Cow<'static, str> {
        "index_worker/doc_facet_set".into()
    }
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
    worker: Option<DocFacetSetIndexWorkerHandle>,
}

impl DocFacetSetIndexStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(worker) = self.worker.take() {
            worker.stop().await?;
        }
        if let Some(handle) = self.worker_handle.take() {
            utils_rs::wait_on_handle_with_timeout(handle, 10000).await?;
        }
        Ok(())
    }
}

impl DocFacetSetIndexRepo {
    pub async fn boot(
        acx: AmCtx,
        app_doc_id: DocumentId,
        drawer_repo: Arc<DrawerRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, DocFacetSetIndexStopToken)> {
        let (_sqlite_file_path, db_pool) = sqlite_local_state_repo
            .ensure_sqlite_pool(FACET_SET_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&db_pool).await?;

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            db_pool,
        });

        let worker_store = DocFacetSetIndexWorkerStateStore::load(&acx, &app_doc_id)
            .await
            .unwrap_or_default();
        let worker_store =
            crate::stores::StoreHandle::new(worker_store, acx, app_doc_id, local_actor_id);

        let (worker, mut work_rx) =
            spawn_doc_facet_set_index_worker(Arc::clone(&drawer_repo), worker_store).await?;

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
                worker: Some(worker),
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
                let Some(doc) = self
                    .drawer_repo
                    .get_doc_with_facets_at_heads(&doc_id, &heads, None)
                    .await?
                else {
                    return Ok(());
                };
                self.reindex_doc(&doc_id, &heads, &doc).await?;
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

    pub async fn reindex_doc(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        doc: &daybook_types::doc::Doc,
    ) -> Res<()> {
        let serialized_heads =
            serde_json::to_string(&utils_rs::am::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);
        let mut desired_tags: HashSet<String> = doc
            .facets
            .keys()
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
                    origin_heads: ChangeHashSet(utils_rs::am::parse_commit_heads(&head_strings)?),
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
}

#[derive(Debug, Clone)]
enum DocFacetSetIndexWorkItem {
    Upsert { doc_id: DocId, heads: ChangeHashSet },
    DeleteDoc { doc_id: DocId },
}

pub struct DocFacetSetIndexWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: CancellationToken,
}

impl DocFacetSetIndexWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5000).await?;
        Ok(())
    }
}

async fn spawn_doc_facet_set_index_worker(
    drawer_repo: Arc<DrawerRepo>,
    store: crate::stores::StoreHandle<DocFacetSetIndexWorkerStateStore>,
) -> Res<(
    DocFacetSetIndexWorkerHandle,
    tokio::sync::mpsc::UnboundedReceiver<DocFacetSetIndexWorkItem>,
)> {
    let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<Arc<DrawerEvent>>();

    let listener = drawer_repo.register_listener({
        let event_tx = event_tx.clone();
        move |event| {
            let _ = event_tx.send(event);
        }
    });

    let initial_drawer_heads = store.query_sync(|store| store.drawer_heads.clone()).await;
    if let Some(known_heads) = initial_drawer_heads {
        let events = drawer_repo.diff_events(known_heads, None).await?;
        for event in events {
            if event_tx.send(Arc::new(event)).is_err() {
                break;
            }
        }
    } else {
        let events = drawer_repo
            .diff_events(ChangeHashSet(Vec::new().into()), None)
            .await?;
        let mut current_heads: Option<ChangeHashSet> = None;
        for event in &events {
            if let DrawerEvent::ListChanged { drawer_heads } = event {
                current_heads = Some(drawer_heads.clone());
            }
        }
        let current_heads = current_heads.unwrap_or_else(|| ChangeHashSet(Vec::new().into()));
        for doc in drawer_repo.list().await? {
            for (branch_name, heads) in doc.branches {
                let branch_path = BranchPath::from(branch_name.as_str());
                if branch_path.to_string_lossy().starts_with("/tmp/") {
                    continue;
                }
                let Some(_facet_keys_set) = drawer_repo
                    .get_facet_keys_if_latest(&doc.doc_id, &branch_path, &heads, &current_heads)
                    .await?
                else {
                    continue;
                };
                if work_tx
                    .send(DocFacetSetIndexWorkItem::Upsert {
                        doc_id: doc.doc_id.clone(),
                        heads,
                    })
                    .is_err()
                {
                    break;
                }
                break;
            }
        }
        store
            .mutate_sync(|store| {
                store.drawer_heads = Some(current_heads);
            })
            .await?;
    }

    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let _listener = listener;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        match &*event {
                            DrawerEvent::ListChanged { drawer_heads } => {
                                store.mutate_sync(|store| {
                                    store.drawer_heads = Some(drawer_heads.clone());
                                }).await?;
                            }
                            DrawerEvent::DocDeleted { id, drawer_heads, .. } => {
                                if work_tx
                                    .send(DocFacetSetIndexWorkItem::DeleteDoc { doc_id: id.clone() })
                                    .is_err()
                                {
                                    break;
                                }
                                store.mutate_sync(|store| {
                                    store.drawer_heads = Some(drawer_heads.clone());
                                }).await?;
                            }
                            DrawerEvent::DocAdded { id, entry, drawer_heads } => {
                                let mut matched_heads: Option<ChangeHashSet> = None;
                                for (branch_name, heads) in &entry.branches {
                                    let branch_path = BranchPath::from(branch_name.as_str());
                                    if branch_path.to_string_lossy().starts_with("/tmp/") {
                                        continue;
                                    }
                                    let Some(_facet_keys_set) = drawer_repo
                                        .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                                        .await?
                                    else {
                                        continue;
                                    };
                                    matched_heads = Some(heads.clone());
                                    if branch_name == "main" {
                                        break;
                                    }
                                }
                                if let Some(heads) = matched_heads {
                                    if work_tx
                                        .send(DocFacetSetIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                store
                                    .mutate_sync(|store| {
                                        store.drawer_heads = Some(drawer_heads.clone());
                                    })
                                    .await?;
                            }
                            DrawerEvent::DocUpdated {
                                id,
                                entry,
                                diff,
                                drawer_heads,
                                ..
                            } => {
                                if diff.changed_facet_keys.iter().all(|facet_key| {
                                    facet_key.tag == WellKnownFacetTag::Dmeta.into()
                                }) {
                                    store
                                        .mutate_sync(|store| {
                                            store.drawer_heads = Some(drawer_heads.clone());
                                        })
                                        .await?;
                                    continue;
                                }

                                let moved_branch_names: HashSet<&str> =
                                    diff.moved_branch_names.iter().map(String::as_str).collect();

                                let mut matched_heads: Option<ChangeHashSet> = None;
                                let mut evaluated_latest_branch = false;

                                for (branch_name, heads) in &entry.branches {
                                    if !moved_branch_names.contains(branch_name.as_str()) {
                                        continue;
                                    }
                                    let branch_path = BranchPath::from(branch_name.as_str());
                                    if branch_path.to_string_lossy().starts_with("/tmp/") {
                                        continue;
                                    }
                                    evaluated_latest_branch = true;
                                    let Some(_facet_keys_set) = drawer_repo
                                        .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                                        .await?
                                    else {
                                        continue;
                                    };

                                    matched_heads = Some(heads.clone());
                                    if branch_name == "main" {
                                        break;
                                    }
                                }

                                if let Some(heads) = matched_heads {
                                    if work_tx
                                        .send(DocFacetSetIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                } else if evaluated_latest_branch
                                    && work_tx
                                        .send(DocFacetSetIndexWorkItem::DeleteDoc { doc_id: id.clone() })
                                        .is_err()
                                {
                                    break;
                                }

                                store
                                    .mutate_sync(|store| {
                                        store.drawer_heads = Some(drawer_heads.clone());
                                    })
                                    .await?;
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };

    let join_handle = tokio::spawn(async move {
        fut.await.unwrap_or_log();
    });

    Ok((
        DocFacetSetIndexWorkerHandle {
            join_handle: Some(join_handle),
            cancel_token,
        },
        work_rx,
    ))
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
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
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
