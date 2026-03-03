use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::plugs::manifest::{DocPredicateClause, FacetReferenceKind, FacetReferenceManifest};
use crate::plugs::reference::select_json_path_values;
use crate::plugs::PlugsRepo;
use crate::repos::Repo;
use daybook_types::doc::{BranchPath, ChangeHashSet, DocId, FacetKey};
use daybook_types::url::{parse_facet_ref, FACET_SELF_DOC_ID};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

const FACET_REF_LOCAL_STATE_ID: &str = "@daybook/wip/doc-facet-ref-index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocFacetRefEdge {
    pub origin_doc_id: DocId,
    pub origin_facet_key: FacetKey,
    pub target_doc_id: DocId,
    pub target_facet_key: FacetKey,
    pub reference_kind: FacetReferenceKind,
    pub origin_heads: ChangeHashSet,
}

#[derive(Debug, Clone)]
pub enum DocFacetRefIndexEvent {
    Updated { doc_id: DocId },
    Deleted { doc_id: DocId },
    Reindexed,
}

pub struct DocFacetRefIndexRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub cancel_token: CancellationToken,
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    work_tx: tokio::sync::mpsc::UnboundedSender<DocFacetRefIndexWorkItem>,
    db_pool: SqlitePool,
    reference_specs: tokio::sync::RwLock<HashMap<String, Vec<FacetReferenceManifest>>>,
}

impl Repo for DocFacetRefIndexRepo {
    type Event = DocFacetRefIndexEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

pub struct DocFacetRefIndexStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DocFacetRefIndexStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            utils_rs::wait_on_handle_with_timeout(handle, Duration::from_secs(2)).await?;
        }
        Ok(())
    }
}

impl DocFacetRefIndexRepo {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, DocFacetRefIndexStopToken)> {
        let (_sqlite_file_path, db_pool) = sqlite_local_state_repo
            .ensure_sqlite_pool(FACET_REF_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&db_pool).await?;
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            plugs_repo: Arc::clone(&plugs_repo),
            work_tx,
            db_pool,
            reference_specs: tokio::sync::RwLock::new(HashMap::new()),
        });

        repo.refresh_reference_specs().await?;

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
            DocFacetRefIndexStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(db_pool: &SqlitePool) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS facet_ref_edges (
                origin_doc_id TEXT NOT NULL,
                origin_facet_key TEXT NOT NULL,
                target_doc_id TEXT NOT NULL,
                target_facet_key TEXT NOT NULL,
                reference_kind TEXT NOT NULL,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(origin_doc_id, origin_facet_key, target_doc_id, target_facet_key, reference_kind)
            )
            "#,
        )
        .execute(db_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_target ON facet_ref_edges(target_doc_id, target_facet_key)",
        )
        .execute(db_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_origin ON facet_ref_edges(origin_doc_id, origin_facet_key)",
        )
        .execute(db_pool)
        .await?;

        Ok(())
    }

    async fn handle_worker_item(&self, item: DocFacetRefIndexWorkItem) -> Res<()> {
        match item {
            DocFacetRefIndexWorkItem::Upsert { doc_id, heads } => {
                self.reindex_doc(&doc_id, &heads).await?;
                self.registry
                    .notify([DocFacetRefIndexEvent::Updated { doc_id }]);
            }
            DocFacetRefIndexWorkItem::DeleteDoc { doc_id } => {
                self.delete_doc(&doc_id).await?;
                self.registry
                    .notify([DocFacetRefIndexEvent::Deleted { doc_id }]);
            }
            DocFacetRefIndexWorkItem::RefreshSpecsAndReindexAll => {
                self.refresh_reference_specs().await?;
                self.reindex_all_docs().await?;
                self.registry.notify([DocFacetRefIndexEvent::Reindexed]);
            }
        }
        Ok(())
    }

    async fn refresh_reference_specs(&self) -> Res<()> {
        let plugs = self.plugs_repo.list_plugs().await;
        let mut next_specs: HashMap<String, Vec<FacetReferenceManifest>> = HashMap::new();
        for plug in plugs {
            for facet in &plug.facets {
                if facet.references.is_empty() {
                    continue;
                }
                next_specs
                    .entry(facet.key_tag.to_string())
                    .or_default()
                    .extend(facet.references.iter().cloned());
            }
        }

        let mut guard = self.reference_specs.write().await;
        *guard = next_specs;
        Ok(())
    }

    async fn reindex_all_docs(&self) -> Res<()> {
        sqlx::query("DELETE FROM facet_ref_edges")
            .execute(&self.db_pool)
            .await?;

        let specs = self.reference_specs.read().await.clone();
        let reference_tags: HashSet<String> = specs.keys().cloned().collect();
        drop(specs);
        if reference_tags.is_empty() {
            return Ok(());
        }

        let docs = self.drawer_repo.list().await?;
        for doc in docs {
            let Some(branch_path) = doc.main_branch_path() else {
                continue;
            };
            if branch_path.to_string_lossy().starts_with("/tmp/") {
                continue;
            }

            let Some(heads) = doc
                .branches
                .get(&branch_path.to_string_lossy().to_string())
                .cloned()
            else {
                continue;
            };
            let Some(facet_keys) = self
                .drawer_repo
                .facet_keys_at_heads(&doc.doc_id, &heads)
                .await?
            else {
                continue;
            };
            let selected_keys: Vec<FacetKey> = facet_keys
                .into_iter()
                .filter(|facet_key| reference_tags.contains(&facet_key.tag.to_string()))
                .collect();
            let facets = self
                .drawer_repo
                .get_at_heads_with_facets_arc(&doc.doc_id, &heads, Some(selected_keys))
                .await?
                .unwrap_or_default();
            self.reindex_doc_from_facets(&doc.doc_id, &heads, &facets)
                .await?;
        }

        Ok(())
    }

    pub async fn reindex_doc(&self, doc_id: &DocId, heads: &ChangeHashSet) -> Res<()> {
        let specs = self.reference_specs.read().await.clone();
        let reference_tags: HashSet<String> = specs.keys().cloned().collect();
        drop(specs);
        if reference_tags.is_empty() {
            self.delete_doc(doc_id).await?;
            return Ok(());
        }

        let Some(facet_keys) = self.drawer_repo.facet_keys_at_heads(doc_id, heads).await? else {
            self.delete_doc(doc_id).await?;
            return Ok(());
        };
        let selected_keys: Vec<FacetKey> = facet_keys
            .into_iter()
            .filter(|facet_key| reference_tags.contains(&facet_key.tag.to_string()))
            .collect();
        if selected_keys.is_empty() {
            self.delete_doc(doc_id).await?;
            return Ok(());
        }
        let facets = self
            .drawer_repo
            .get_at_heads_with_facets_arc(doc_id, heads, Some(selected_keys))
            .await?
            .unwrap_or_default();
        self.reindex_doc_from_facets(doc_id, heads, &facets).await
    }

    async fn reindex_doc_from_facets(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facets: &HashMap<FacetKey, daybook_types::doc::ArcFacetRaw>,
    ) -> Res<()> {
        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);

        sqlx::query("DELETE FROM facet_ref_edges WHERE origin_doc_id = ?1")
            .bind(doc_id)
            .execute(&self.db_pool)
            .await?;

        let specs = self.reference_specs.read().await.clone();
        for (facet_key, facet_value) in facets {
            let facet_tag = facet_key.tag.to_string();
            let Some(tag_specs) = specs.get(&facet_tag) else {
                continue;
            };

            for spec in tag_specs {
                let references = extract_references(spec, facet_value.as_ref(), doc_id, facet_key)?;
                for reference in references {
                    sqlx::query(
                        r#"
                        INSERT INTO facet_ref_edges (
                            origin_doc_id,
                            origin_facet_key,
                            target_doc_id,
                            target_facet_key,
                            reference_kind,
                            origin_heads
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                        ON CONFLICT(origin_doc_id, origin_facet_key, target_doc_id, target_facet_key, reference_kind)
                        DO UPDATE SET origin_heads = excluded.origin_heads
                        "#,
                    )
                    .bind(doc_id)
                    .bind(facet_key.to_string())
                    .bind(reference.target_doc_id)
                    .bind(reference.target_facet_key.to_string())
                    .bind(reference_kind_to_db_value(&spec.reference_kind))
                    .bind(&serialized_heads)
                    .execute(&self.db_pool)
                    .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        sqlx::query("DELETE FROM facet_ref_edges WHERE origin_doc_id = ?1 OR target_doc_id = ?1")
            .bind(doc_id)
            .execute(&self.db_pool)
            .await?;
        Ok(())
    }

    pub async fn list_outgoing(&self, doc_id: &DocId) -> Res<Vec<DocFacetRefEdge>> {
        let rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            r#"
            SELECT
                origin_doc_id,
                origin_facet_key,
                target_doc_id,
                target_facet_key,
                reference_kind,
                origin_heads
            FROM facet_ref_edges
            WHERE origin_doc_id = ?1
            ORDER BY origin_facet_key, target_doc_id, target_facet_key
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;

        rows.into_iter().map(row_to_edge).collect()
    }

    pub async fn list_incoming(
        &self,
        target_doc_id: &DocId,
        target_facet_key: &FacetKey,
    ) -> Res<Vec<DocFacetRefEdge>> {
        let rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            r#"
            SELECT
                origin_doc_id,
                origin_facet_key,
                target_doc_id,
                target_facet_key,
                reference_kind,
                origin_heads
            FROM facet_ref_edges
            WHERE target_doc_id = ?1 AND target_facet_key = ?2
            ORDER BY origin_doc_id, origin_facet_key
            "#,
        )
        .bind(target_doc_id)
        .bind(target_facet_key.to_string())
        .fetch_all(&self.db_pool)
        .await?;

        rows.into_iter().map(row_to_edge).collect()
    }

    pub fn triage_listener(
        self: &Arc<Self>,
    ) -> Box<dyn crate::rt::switch::SwitchSink + Send + Sync> {
        Box::new(FacetRefTriageListener {
            drawer_repo: Arc::clone(&self.drawer_repo),
            plugs_repo: Arc::clone(&self.plugs_repo),
            index_repo: Arc::clone(self),
        })
    }

    pub fn enqueue_upsert(&self, doc_id: DocId, heads: ChangeHashSet) -> Res<()> {
        self.work_tx
            .send(DocFacetRefIndexWorkItem::Upsert { doc_id, heads })
            .map_err(|err| ferr!("doc_facet_ref_index work queue closed: {err}"))?;
        Ok(())
    }

    pub fn enqueue_delete(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(DocFacetRefIndexWorkItem::DeleteDoc { doc_id })
            .map_err(|err| ferr!("doc_facet_ref_index work queue closed: {err}"))?;
        Ok(())
    }

    pub fn enqueue_refresh_specs_and_reindex_all(&self) -> Res<()> {
        self.work_tx
            .send(DocFacetRefIndexWorkItem::RefreshSpecsAndReindexAll)
            .map_err(|err| ferr!("doc_facet_ref_index work queue closed: {err}"))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ExtractedReference {
    target_doc_id: DocId,
    target_facet_key: FacetKey,
}

fn extract_references(
    spec: &FacetReferenceManifest,
    facet_value: &serde_json::Value,
    origin_doc_id: &DocId,
    origin_facet_key: &FacetKey,
) -> Res<Vec<ExtractedReference>> {
    let selected_values = select_json_path_values(facet_value, &spec.json_path)?;
    let mut out = Vec::new();
    for selected_value in selected_values {
        match &spec.reference_kind {
            FacetReferenceKind::UrlFacet => {
                append_url_references(
                    &mut out,
                    selected_value,
                    origin_doc_id,
                    origin_facet_key,
                    &spec.json_path,
                )?;
            }
        }
    }
    Ok(out)
}

fn append_url_references(
    out: &mut Vec<ExtractedReference>,
    selected_value: &serde_json::Value,
    origin_doc_id: &DocId,
    origin_facet_key: &FacetKey,
    json_path: &str,
) -> Res<()> {
    match selected_value {
        serde_json::Value::String(url_value) => {
            out.push(parse_url_reference(
                url_value,
                origin_doc_id,
                origin_facet_key,
                json_path,
            )?);
        }
        serde_json::Value::Array(values) => {
            for item in values {
                let serde_json::Value::String(url_value) = item else {
                    eyre::bail!(
                        "expected array of URL strings at path '{}' for facet '{}'",
                        json_path,
                        origin_facet_key
                    );
                };
                out.push(parse_url_reference(
                    url_value,
                    origin_doc_id,
                    origin_facet_key,
                    json_path,
                )?);
            }
        }
        other => {
            eyre::bail!(
                "expected URL string or array of URL strings at path '{}' for facet '{}' but found {}",
                json_path,
                origin_facet_key,
                other
            );
        }
    }
    Ok(())
}

fn parse_url_reference(
    url_value: &str,
    origin_doc_id: &DocId,
    origin_facet_key: &FacetKey,
    json_path: &str,
) -> Res<ExtractedReference> {
    let parsed_url = url::Url::parse(url_value).wrap_err_with(|| {
        format!(
            "invalid URL '{}' at path '{}' for facet '{}'",
            url_value, json_path, origin_facet_key
        )
    })?;
    let parsed_ref = parse_facet_ref(&parsed_url).wrap_err_with(|| {
        format!(
            "invalid facet reference URL '{}' at path '{}' for facet '{}'",
            url_value, json_path, origin_facet_key
        )
    })?;

    let target_doc_id = if parsed_ref.doc_id == FACET_SELF_DOC_ID {
        origin_doc_id.clone()
    } else {
        parsed_ref.doc_id
    };

    Ok(ExtractedReference {
        target_doc_id,
        target_facet_key: parsed_ref.facet_key,
    })
}

fn reference_kind_to_db_value(reference_kind: &FacetReferenceKind) -> &'static str {
    match reference_kind {
        FacetReferenceKind::UrlFacet => "urlFacet",
    }
}

fn reference_kind_from_db_value(value: &str) -> Res<FacetReferenceKind> {
    match value {
        "urlFacet" => Ok(FacetReferenceKind::UrlFacet),
        _ => eyre::bail!("unsupported reference kind '{}'", value),
    }
}

fn row_to_edge(row: (String, String, String, String, String, String)) -> Res<DocFacetRefEdge> {
    let (origin_doc_id, origin_facet_key, target_doc_id, target_facet_key, reference_kind, heads) =
        row;
    let heads = am_utils_rs::parse_commit_heads(
        &serde_json::from_str::<Vec<String>>(&heads).expect(ERROR_JSON),
    )
    .expect(ERROR_JSON);

    Ok(DocFacetRefEdge {
        origin_doc_id,
        origin_facet_key: FacetKey::from(origin_facet_key),
        target_doc_id,
        target_facet_key: FacetKey::from(target_facet_key),
        reference_kind: reference_kind_from_db_value(&reference_kind)?,
        origin_heads: ChangeHashSet(heads),
    })
}

enum DocFacetRefIndexWorkItem {
    Upsert { doc_id: DocId, heads: ChangeHashSet },
    DeleteDoc { doc_id: DocId },
    RefreshSpecsAndReindexAll,
}

struct FacetRefTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    index_repo: Arc<DocFacetRefIndexRepo>,
}

impl FacetRefTriageListener {
    async fn build_drawer_predicate(&self) -> Res<Option<DocPredicateClause>> {
        let plugs = self.plugs_repo.list_plugs().await;
        let mut clauses = Vec::new();
        for plug in plugs {
            for facet in &plug.facets {
                if facet.references.is_empty() {
                    continue;
                }
                clauses.push(DocPredicateClause::HasTag(facet.key_tag.clone()));
            }
        }
        if clauses.is_empty() {
            return Ok(None);
        }
        Ok(Some(DocPredicateClause::Or(clauses)))
    }
}

#[async_trait]
impl crate::rt::switch::SwitchSink for FacetRefTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_drawer: true,
            consume_plugs: true,
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
        let mut outcome = crate::rt::switch::SwitchSinkOutcome::default();
        match event {
            crate::rt::switch::SwitchEvent::Plugs(_) => {
                outcome.drawer_predicate_update = self.build_drawer_predicate().await?;
                self.index_repo.enqueue_refresh_specs_and_reindex_all()?;
                return Ok(outcome);
            }
            crate::rt::switch::SwitchEvent::Drawer(event) => match &**event {
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
            },
            _ => {}
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::test_cx;
    use daybook_types::doc::{AddDocArgs, FacetRaw, WellKnownFacet, WellKnownFacetTag};

    async fn wait_for_outgoing(
        repo: &DocFacetRefIndexRepo,
        doc_id: &DocId,
        expected_len: usize,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            let outgoing = repo.list_outgoing(doc_id).await?;
            if outgoing.len() == expected_len {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!("timeout waiting for outgoing references")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_facet_ref_index_tracks_embedding_references() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_facet_ref_index_repo);

        let src_doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPath::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note("hello".to_string().into())),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let src_facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let facet_ref = daybook_types::url::build_facet_ref(&src_doc_id, &src_facet_key)?;

        let embedding_doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPath::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Embedding),
                    FacetRaw::from(WellKnownFacet::Embedding(daybook_types::doc::Embedding {
                        facet_ref,
                        ref_heads: ChangeHashSet(Vec::new().into()),
                        model_tag: "test-model".into(),
                        vector: vec![],
                        dim: 0,
                        dtype: daybook_types::doc::EmbeddingDtype::F32,
                        compression: None,
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_outgoing(&repo, &embedding_doc_id, 1).await?;
        let outgoing = repo.list_outgoing(&embedding_doc_id).await?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_doc_id, src_doc_id);
        assert_eq!(outgoing[0].target_facet_key, src_facet_key);

        test_context.stop().await?;
        Ok(())
    }
}
