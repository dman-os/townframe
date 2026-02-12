use crate::interlude::*;

use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::plugs::manifest::DocPredicateClause;
use crate::repos::Repo;
use crate::stores::Store;
use daybook_types::doc::{BranchPath, ChangeHashSet, Doc, DocId, FacetKey, WellKnownFacetTag};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct VectorHit {
    pub doc_id: DocId,
    pub facet_key: String,
    pub heads: ChangeHashSet,
    pub distance: f32,
}

#[derive(Debug, Clone)]
pub struct DocEmbeddingIndexRecord {
    pub facet_uuid: Uuid,
    pub origin_doc_id: DocId,
    pub origin_heads: ChangeHashSet,
    pub facet_key: FacetKey,
    pub vector: Vec<u8>,
    pub dim: u32,
}

#[derive(Default, Debug, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct DocIndexWorkerStateStore {
    pub drawer_heads: Option<ChangeHashSet>,
}

#[async_trait]
impl Store for DocIndexWorkerStateStore {
    fn prop() -> Cow<'static, str> {
        "index_worker/default".into()
    }
}

#[derive(Debug, Clone)]
pub enum DocEmbeddingIndexEvent {
    Updated { doc_id: DocId },
    Deleted { doc_id: DocId },
}

pub struct DocEmbeddingIndexRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub cancel_token: CancellationToken,
    drawer_repo: Arc<DrawerRepo>,
    db_pool: SqlitePool,
}

impl Repo for DocEmbeddingIndexRepo {
    type Event = DocEmbeddingIndexEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

pub struct DocEmbeddingIndexStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
    worker: Option<DocIndexWorkerHandle>,
}

impl DocEmbeddingIndexStopToken {
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

impl DocEmbeddingIndexRepo {
    pub async fn boot(
        acx: AmCtx,
        app_doc_id: DocumentId,
        drawer_repo: Arc<DrawerRepo>,
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, DocEmbeddingIndexStopToken)> {
        crate::init_sqlite_vec();

        let db_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str("sqlite::memory:")?
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_secs(5))
                    .create_if_missing(true),
            )
            .await
            .wrap_err("error initializing embedding index sqlite db")?;

        sqlx::query("select vec_version()")
            .execute(&db_pool)
            .await
            .wrap_err("sqlite-vec extension not available")?;

        sqlx::query(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS doc_embedding_vec
            USING vec0(embedding float[768])
            "#,
        )
        .execute(&db_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_embedding_meta (
                rowid INTEGER PRIMARY KEY,
                facet_uuid TEXT NOT NULL UNIQUE,
                origin_doc_id TEXT NOT NULL,
                origin_heads TEXT NOT NULL,
                facet_key TEXT NOT NULL
            )
            "#,
        )
        .execute(&db_pool)
        .await?;

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            db_pool,
        });

        let worker_prop = "index_worker/doc_embedding".to_string();
        let worker_store: DocIndexWorkerStateStore = DocIndexWorkerStateStore::load_from_prop(
            &acx,
            &app_doc_id,
            Cow::Owned(worker_prop.clone()),
        )
        .await
        .unwrap_or_default();
        let worker_store = crate::stores::StoreHandle::new_with_prop(
            worker_store,
            acx,
            app_doc_id,
            Some(worker_prop),
            local_actor_id,
        );

        let (worker, mut work_rx) = spawn_doc_index_worker(
            Arc::clone(&drawer_repo),
            worker_store,
            DocPredicateClause::HasTag(WellKnownFacetTag::Embedding.into()),
        )
        .await?;

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
            DocEmbeddingIndexStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
                worker: Some(worker),
            },
        ))
    }

    async fn handle_worker_item(&self, item: DocIndexWorkItem) -> Res<()> {
        match item {
            DocIndexWorkItem::Upsert { doc_id, heads } => {
                debug!(?doc_id, ?heads, "doc embedding index worker upsert");
                let Some(doc) = self
                    .drawer_repo
                    .get_doc_with_facets_at_heads(&doc_id, &heads, None)
                    .await?
                else {
                    debug!(
                        ?doc_id,
                        ?heads,
                        "doc not found at requested heads for indexing"
                    );
                    return Ok(());
                };
                self.reindex_doc(&doc_id, &heads, &doc).await?;
                self.registry
                    .notify([DocEmbeddingIndexEvent::Updated { doc_id }]);
            }
            DocIndexWorkItem::DeleteDoc { doc_id } => {
                debug!(?doc_id, "doc embedding index worker delete");
                self.delete_all_for_doc(&doc_id).await?;
                self.registry
                    .notify([DocEmbeddingIndexEvent::Deleted { doc_id }]);
            }
        }
        Ok(())
    }

    async fn reindex_doc(&self, doc_id: &DocId, heads: &ChangeHashSet, doc: &Doc) -> Res<()> {
        let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
        let Some(dmeta_raw) = doc.facets.get(&dmeta_key).cloned() else {
            debug!(
                ?doc_id,
                facet_keys = ?doc.facets.keys().collect::<Vec<_>>(),
                "doc missing dmeta facet during index reindex; deleting rows"
            );
            self.delete_all_for_doc(doc_id).await?;
            return Ok(());
        };
        let dmeta = match daybook_types::doc::WellKnownFacet::from_json(
            dmeta_raw.clone(),
            WellKnownFacetTag::Dmeta,
        ) {
            Ok(daybook_types::doc::WellKnownFacet::Dmeta(dmeta)) => dmeta,
            Ok(_) => {
                debug!(
                    ?doc_id,
                    "dmeta facet parsed as wrong well-known type; deleting rows"
                );
                self.delete_all_for_doc(doc_id).await?;
                return Ok(());
            }
            Err(err) => {
                debug!(
                    ?doc_id,
                    ?err,
                    dmeta_raw = %serde_json::to_string(&dmeta_raw).unwrap_or_else(|_| "<non-json>".to_string()),
                    "unable to parse dmeta facet during index reindex; deleting rows"
                );
                self.delete_all_for_doc(doc_id).await?;
                return Ok(());
            }
        };

        let mut keep_uuids: HashSet<Uuid> = HashSet::new();
        for (facet_key, raw) in &doc.facets {
            if facet_key.tag != WellKnownFacetTag::Embedding.into() {
                continue;
            }
            let parsed = daybook_types::doc::WellKnownFacet::from_json(
                raw.clone(),
                WellKnownFacetTag::Embedding,
            )?;
            let daybook_types::doc::WellKnownFacet::Embedding(embedding) = parsed else {
                continue;
            };
            if embedding.dtype != daybook_types::doc::EmbeddingDtype::F32
                || embedding.compression.is_some()
            {
                continue;
            }
            let Some((facet_uuid, _)) = dmeta.facet_uuids.iter().find(|(_, key)| *key == facet_key)
            else {
                continue;
            };
            keep_uuids.insert(*facet_uuid);

            self.upsert_record(DocEmbeddingIndexRecord {
                facet_uuid: *facet_uuid,
                origin_doc_id: doc_id.clone(),
                origin_heads: heads.clone(),
                facet_key: facet_key.clone(),
                vector: embedding.vector,
                dim: embedding.dim,
            })
            .await?;
        }
        debug!(
            ?doc_id,
            keep_count = keep_uuids.len(),
            "completed doc embedding reindex pass"
        );

        let existing: Vec<String> = sqlx::query_scalar(
            "SELECT facet_uuid FROM doc_embedding_meta WHERE origin_doc_id = ?1",
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;

        for existing_uuid in existing {
            let parsed = Uuid::parse_str(&existing_uuid)?;
            if !keep_uuids.contains(&parsed) {
                self.delete_by_facet_uuid(parsed).await?;
            }
        }
        Ok(())
    }

    pub async fn upsert_record(&self, record: DocEmbeddingIndexRecord) -> Res<()> {
        if record.dim != 768 {
            eyre::bail!("expected embedding dimension 768, got {}", record.dim);
        }
        let vector_json =
            daybook_types::doc::embedding_f32_bytes_to_json(&record.vector, record.dim)?;
        let serialized_heads = serde_json::to_string(&utils_rs::am::serialize_commit_heads(
            &record.origin_heads.0,
        ))
        .expect(ERROR_JSON);
        let facet_uuid = record.facet_uuid.to_string();
        let existing_rowid: Option<i64> =
            sqlx::query_scalar("SELECT rowid FROM doc_embedding_meta WHERE facet_uuid = ?1")
                .bind(&facet_uuid)
                .fetch_optional(&self.db_pool)
                .await?;

        let rowid = if let Some(existing_rowid) = existing_rowid {
            sqlx::query("UPDATE doc_embedding_vec SET embedding = ?1 WHERE rowid = ?2")
                .bind(&vector_json)
                .bind(existing_rowid)
                .execute(&self.db_pool)
                .await?;
            sqlx::query(
                "UPDATE doc_embedding_meta SET origin_doc_id = ?1, origin_heads = ?2, facet_key = ?3 WHERE rowid = ?4",
            )
            .bind(&record.origin_doc_id)
            .bind(&serialized_heads)
            .bind(record.facet_key.to_string())
            .bind(existing_rowid)
            .execute(&self.db_pool)
            .await?;
            existing_rowid
        } else {
            let result = sqlx::query("INSERT INTO doc_embedding_vec (embedding) VALUES (?1)")
                .bind(&vector_json)
                .execute(&self.db_pool)
                .await?;
            let inserted_rowid = result.last_insert_rowid();
            sqlx::query(
                "INSERT INTO doc_embedding_meta (rowid, facet_uuid, origin_doc_id, origin_heads, facet_key) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(inserted_rowid)
            .bind(&facet_uuid)
            .bind(&record.origin_doc_id)
            .bind(&serialized_heads)
            .bind(record.facet_key.to_string())
            .execute(&self.db_pool)
            .await?;
            inserted_rowid
        };

        debug!(rowid, facet_uuid, "upserted embedding index record");
        Ok(())
    }

    pub async fn delete_by_facet_uuid(&self, facet_uuid: Uuid) -> Res<()> {
        let facet_uuid = facet_uuid.to_string();
        let rowid: Option<i64> =
            sqlx::query_scalar("SELECT rowid FROM doc_embedding_meta WHERE facet_uuid = ?1")
                .bind(&facet_uuid)
                .fetch_optional(&self.db_pool)
                .await?;
        let Some(rowid) = rowid else {
            return Ok(());
        };
        sqlx::query("DELETE FROM doc_embedding_vec WHERE rowid = ?1")
            .bind(rowid)
            .execute(&self.db_pool)
            .await?;
        sqlx::query("DELETE FROM doc_embedding_meta WHERE rowid = ?1")
            .bind(rowid)
            .execute(&self.db_pool)
            .await?;
        Ok(())
    }

    pub async fn delete_all_for_doc(&self, doc_id: &DocId) -> Res<()> {
        let rowids: Vec<i64> =
            sqlx::query_scalar("SELECT rowid FROM doc_embedding_meta WHERE origin_doc_id = ?1")
                .bind(doc_id)
                .fetch_all(&self.db_pool)
                .await?;
        for rowid in rowids {
            sqlx::query("DELETE FROM doc_embedding_vec WHERE rowid = ?1")
                .bind(rowid)
                .execute(&self.db_pool)
                .await?;
            sqlx::query("DELETE FROM doc_embedding_meta WHERE rowid = ?1")
                .bind(rowid)
                .execute(&self.db_pool)
                .await?;
        }
        Ok(())
    }

    pub async fn list_by_doc(&self, doc_id: &DocId) -> Res<Vec<DocEmbeddingIndexRecord>> {
        let rows = sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT facet_uuid, origin_doc_id, origin_heads, facet_key FROM doc_embedding_meta WHERE origin_doc_id = ?1",
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;
        rows.into_iter()
            .map(|(facet_uuid, origin_doc_id, origin_heads, facet_key)| {
                let head_strings: Vec<String> = serde_json::from_str(&origin_heads)?;
                let heads = ChangeHashSet(utils_rs::am::parse_commit_heads(&head_strings)?);
                Ok(DocEmbeddingIndexRecord {
                    facet_uuid: Uuid::parse_str(&facet_uuid)?,
                    origin_doc_id,
                    origin_heads: heads,
                    facet_key: FacetKey::from(facet_key),
                    vector: Vec::new(),
                    dim: 768,
                })
            })
            .collect::<Res<Vec<_>>>()
    }

    pub async fn query_text(&self, text: &str, num_neighbors: u32) -> Res<Vec<VectorHit>> {
        let cache_dir = std::path::absolute(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/models/.fastembed_cache"),
        )?;
        let mltools_ctx = mltools::Ctx {
            config: mltools::Config {
                ocr: mltools::OcrConfig { backends: vec![] },
                embed: mltools::EmbedConfig {
                    backends: vec![mltools::EmbedBackendConfig::LocalFastembedNomic { cache_dir }],
                },
                llm: mltools::LlmConfig { backends: vec![] },
            },
        };
        let embedded = mltools::embed_text(&mltools_ctx, text).await?;
        let vector_bytes = daybook_types::doc::embedding_f32_slice_to_le_bytes(&embedded.vector);
        let vector_json =
            daybook_types::doc::embedding_f32_bytes_to_json(&vector_bytes, embedded.dimensions)?;
        let knn_rows = sqlx::query_as::<_, (i64, f32)>(
            r#"
            SELECT rowid, distance
            FROM doc_embedding_vec
            WHERE embedding MATCH ?1 AND k = ?2
            ORDER BY distance ASC
            "#,
        )
        .bind(&vector_json)
        .bind(num_neighbors as i64)
        .fetch_all(&self.db_pool)
        .await?;

        let mut out = Vec::with_capacity(knn_rows.len());
        for (rowid, distance) in knn_rows {
            let (doc_id, facet_key, origin_heads) = sqlx::query_as::<_, (String, String, String)>(
                "SELECT origin_doc_id, facet_key, origin_heads FROM doc_embedding_meta WHERE rowid = ?1",
            )
            .bind(rowid)
            .fetch_one(&self.db_pool)
            .await?;
            let heads_json: Vec<String> = serde_json::from_str(&origin_heads)?;
            out.push(VectorHit {
                doc_id,
                facet_key,
                heads: ChangeHashSet(utils_rs::am::parse_commit_heads(&heads_json)?),
                distance,
            });
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
enum DocIndexWorkItem {
    Upsert { doc_id: DocId, heads: ChangeHashSet },
    DeleteDoc { doc_id: DocId },
}

pub struct DocIndexWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: CancellationToken,
}

impl DocIndexWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5000).await?;
        Ok(())
    }
}

async fn spawn_doc_index_worker(
    drawer_repo: Arc<DrawerRepo>,
    store: crate::stores::StoreHandle<DocIndexWorkerStateStore>,
    predicate: DocPredicateClause,
) -> Res<(
    DocIndexWorkerHandle,
    tokio::sync::mpsc::UnboundedReceiver<DocIndexWorkItem>,
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
                let Some(facet_keys_set) = drawer_repo
                    .get_facet_keys_if_latest(&doc.doc_id, &branch_path, &heads, &current_heads)
                    .await?
                else {
                    continue;
                };
                let facets: HashMap<FacetKey, daybook_types::doc::FacetRaw> = facet_keys_set
                    .iter()
                    .map(|key| (key.clone(), serde_json::Value::Null))
                    .collect();
                let meta_doc = Doc {
                    id: doc.doc_id.clone(),
                    facets,
                };
                if predicate.matches(&meta_doc)
                    && work_tx
                        .send(DocIndexWorkItem::Upsert {
                            doc_id: doc.doc_id.clone(),
                            heads,
                        })
                        .is_err()
                {
                    break;
                }
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
                                    .send(DocIndexWorkItem::DeleteDoc { doc_id: id.clone() })
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
                                let mut evaluated_latest_branch = false;
                                for (branch_name, heads) in &entry.branches {
                                    let branch_path = BranchPath::from(branch_name.as_str());
                                    if branch_path.to_string_lossy().starts_with("/tmp/") {
                                        continue;
                                    }
                                    let Some(facet_keys_set) = drawer_repo
                                        .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                                        .await? else {
                                        continue;
                                    };
                                    evaluated_latest_branch = true;
                                    let facets: HashMap<FacetKey, daybook_types::doc::FacetRaw> = facet_keys_set
                                        .iter()
                                        .map(|key| (key.clone(), serde_json::Value::Null))
                                        .collect();
                                    let meta_doc = Doc {
                                        id: id.clone(),
                                        facets,
                                    };
                                    if predicate.matches(&meta_doc) {
                                        matched_heads = Some(heads.clone());
                                        if branch_name == "main" {
                                            break;
                                        }
                                    }
                                }

                                if let Some(heads) = matched_heads {
                                    if work_tx
                                        .send(DocIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                } else if evaluated_latest_branch
                                    && work_tx
                                        .send(DocIndexWorkItem::DeleteDoc { doc_id: id.clone() })
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
                            DrawerEvent::DocUpdated { id, entry, drawer_heads, .. } => {
                                let previous_entry = match &entry.previous_version_heads {
                                    Some(previous_heads) => {
                                        drawer_repo.get_entry_at_heads(id, previous_heads).await?
                                    }
                                    None => None,
                                };

                                let mut matched_heads: Option<ChangeHashSet> = None;
                                let mut evaluated_latest_branch = false;
                                for (branch_name, heads) in &entry.branches {
                                    let branch_path = BranchPath::from(branch_name.as_str());
                                    if branch_path.to_string_lossy().starts_with("/tmp/") {
                                        continue;
                                    }
                                    if let Some(previous_entry) = &previous_entry {
                                        let previous_heads = previous_entry.branches.get(branch_name);
                                        if previous_heads == Some(heads) {
                                            continue;
                                        }
                                    }
                                    let Some(facet_keys_set) = drawer_repo
                                        .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                                        .await? else {
                                        continue;
                                    };
                                    evaluated_latest_branch = true;
                                    let facets: HashMap<FacetKey, daybook_types::doc::FacetRaw> = facet_keys_set
                                        .iter()
                                        .map(|key| (key.clone(), serde_json::Value::Null))
                                        .collect();
                                    let meta_doc = Doc {
                                        id: id.clone(),
                                        facets,
                                    };
                                    if predicate.matches(&meta_doc) {
                                        matched_heads = Some(heads.clone());
                                        if branch_name == "main" {
                                            break;
                                        }
                                    }
                                }

                                if let Some(heads) = matched_heads {
                                    if work_tx
                                        .send(DocIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                } else if evaluated_latest_branch
                                    && work_tx
                                        .send(DocIndexWorkItem::DeleteDoc { doc_id: id.clone() })
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
        DocIndexWorkerHandle {
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

    fn zero_vector_bytes(dim: u32) -> Vec<u8> {
        vec![0u8; dim as usize * std::mem::size_of::<f32>()]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_embedding_index_repo_crud() -> Res<()> {
        let test_ctx = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_ctx.rt.doc_embedding_index_repo);
        let doc_id = "doc-crud".to_string();
        let facet_uuid = Uuid::new_v4();
        let facet_key = FacetKey::from(WellKnownFacetTag::Embedding);

        repo.upsert_record(DocEmbeddingIndexRecord {
            facet_uuid,
            origin_doc_id: doc_id.clone(),
            origin_heads: ChangeHashSet(Vec::new().into()),
            facet_key: facet_key.clone(),
            vector: zero_vector_bytes(768),
            dim: 768,
        })
        .await?;

        let records = repo.list_by_doc(&doc_id).await?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].facet_uuid, facet_uuid);
        assert_eq!(records[0].facet_key, facet_key);

        repo.delete_by_facet_uuid(facet_uuid).await?;
        let records_after_delete = repo.list_by_doc(&doc_id).await?;
        assert!(records_after_delete.is_empty());

        test_ctx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_embedding_index_repo_upsert_replaces_existing() -> Res<()> {
        let test_ctx = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_ctx.rt.doc_embedding_index_repo);
        let facet_uuid = Uuid::new_v4();
        let facet_key = FacetKey::from(WellKnownFacetTag::Embedding);

        repo.upsert_record(DocEmbeddingIndexRecord {
            facet_uuid,
            origin_doc_id: "doc-a".to_string(),
            origin_heads: ChangeHashSet(Vec::new().into()),
            facet_key: facet_key.clone(),
            vector: zero_vector_bytes(768),
            dim: 768,
        })
        .await?;
        repo.upsert_record(DocEmbeddingIndexRecord {
            facet_uuid,
            origin_doc_id: "doc-b".to_string(),
            origin_heads: ChangeHashSet(Vec::new().into()),
            facet_key,
            vector: zero_vector_bytes(768),
            dim: 768,
        })
        .await?;

        let doc_a_records = repo.list_by_doc(&"doc-a".to_string()).await?;
        let doc_b_records = repo.list_by_doc(&"doc-b".to_string()).await?;
        assert!(doc_a_records.is_empty());
        assert_eq!(doc_b_records.len(), 1);
        assert_eq!(doc_b_records[0].facet_uuid, facet_uuid);

        test_ctx.stop().await?;
        Ok(())
    }
}
