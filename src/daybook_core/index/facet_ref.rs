use crate::drawer::{DrawerEvent, DrawerRepo};
use crate::interlude::*;
use crate::plugs::manifest::{FacetReferenceKind, FacetReferenceManifest};
use crate::plugs::reference::select_json_path_values;
use crate::plugs::{PlugsEvent, PlugsRepo};
use crate::repos::Repo;
use crate::stores::Store;
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

#[derive(Default, Debug, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct DocFacetRefIndexWorkerStateStore {
    pub drawer_heads: Option<ChangeHashSet>,
    pub plugs_heads: Option<ChangeHashSet>,
}

#[async_trait]
impl Store for DocFacetRefIndexWorkerStateStore {
    fn prop() -> Cow<'static, str> {
        "index_worker/doc_facet_ref".into()
    }
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
    worker: Option<DocFacetRefIndexWorkerHandle>,
}

impl DocFacetRefIndexStopToken {
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

impl DocFacetRefIndexRepo {
    pub async fn boot(
        acx: AmCtx,
        app_doc_id: DocumentId,
        drawer_repo: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, DocFacetRefIndexStopToken)> {
        let (_sqlite_file_path, db_pool) = sqlite_local_state_repo
            .ensure_sqlite_pool(FACET_REF_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&db_pool).await?;

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            plugs_repo: Arc::clone(&plugs_repo),
            db_pool,
            reference_specs: tokio::sync::RwLock::new(HashMap::new()),
        });

        repo.refresh_reference_specs().await?;

        let worker_store = DocFacetRefIndexWorkerStateStore::load(&acx, &app_doc_id)
            .await
            .unwrap_or_default();
        let worker_store =
            crate::stores::StoreHandle::new(worker_store, acx, app_doc_id, local_actor_id);

        let (worker, mut work_rx) = spawn_doc_facet_ref_index_worker(
            Arc::clone(&drawer_repo),
            Arc::clone(&plugs_repo),
            worker_store,
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
            DocFacetRefIndexStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
                worker: Some(worker),
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
                let Some(doc) = self
                    .drawer_repo
                    .get_doc_with_facets_at_heads(&doc_id, &heads, None)
                    .await?
                else {
                    return Ok(());
                };
                self.reindex_doc(&doc_id, &heads, &doc).await?;
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

        let docs = self.drawer_repo.list().await?;
        for doc in docs {
            let mut selected_branch: Option<BranchPath> = doc.main_branch_path();
            if selected_branch.is_none() {
                selected_branch = doc
                    .branches
                    .keys()
                    .next()
                    .map(|name| BranchPath::from(name.as_str()));
            }
            let Some(branch_path) = selected_branch else {
                continue;
            };
            if branch_path.to_string_lossy().starts_with("/tmp/") {
                continue;
            }

            let Some((doc_value, heads)) = self
                .drawer_repo
                .get_with_heads(&doc.doc_id, &branch_path, None)
                .await?
            else {
                continue;
            };
            self.reindex_doc(&doc.doc_id, &heads, &doc_value).await?;
        }

        Ok(())
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

        sqlx::query("DELETE FROM facet_ref_edges WHERE origin_doc_id = ?1")
            .bind(doc_id)
            .execute(&self.db_pool)
            .await?;

        let specs = self.reference_specs.read().await.clone();
        for (facet_key, facet_value) in &doc.facets {
            let facet_tag = facet_key.tag.to_string();
            let Some(tag_specs) = specs.get(&facet_tag) else {
                continue;
            };

            for spec in tag_specs {
                let references = extract_references(spec, facet_value, doc_id, facet_key)?;
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
    let heads = utils_rs::am::parse_commit_heads(
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

struct DocFacetRefIndexWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: CancellationToken,
}

impl DocFacetRefIndexWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5000).await?;
        Ok(())
    }
}

async fn spawn_doc_facet_ref_index_worker(
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    store: crate::stores::StoreHandle<DocFacetRefIndexWorkerStateStore>,
) -> Res<(
    DocFacetRefIndexWorkerHandle,
    tokio::sync::mpsc::UnboundedReceiver<DocFacetRefIndexWorkItem>,
)> {
    let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
    let (drawer_event_tx, mut drawer_event_rx) =
        tokio::sync::mpsc::unbounded_channel::<Arc<DrawerEvent>>();
    let (plugs_event_tx, mut plugs_event_rx) =
        tokio::sync::mpsc::unbounded_channel::<Arc<PlugsEvent>>();

    let drawer_listener = drawer_repo.register_listener({
        let drawer_event_tx = drawer_event_tx.clone();
        move |event| {
            let _ = drawer_event_tx.send(event);
        }
    });
    let plugs_listener = plugs_repo.register_listener({
        let plugs_event_tx = plugs_event_tx.clone();
        move |event| {
            let _ = plugs_event_tx.send(event);
        }
    });

    let (initial_drawer_heads, initial_plugs_heads) = store
        .query_sync(|state| (state.drawer_heads.clone(), state.plugs_heads.clone()))
        .await;

    let empty_heads = ChangeHashSet(Vec::new().into());

    let drawer_events = drawer_repo
        .diff_events(
            initial_drawer_heads.unwrap_or_else(|| empty_heads.clone()),
            None,
        )
        .await?;
    for event in drawer_events {
        if drawer_event_tx.send(Arc::new(event)).is_err() {
            break;
        }
    }

    let plugs_events = plugs_repo
        .diff_events(initial_plugs_heads.unwrap_or(empty_heads), None)
        .await?;
    for event in plugs_events {
        if plugs_event_tx.send(Arc::new(event)).is_err() {
            break;
        }
    }

    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let _drawer_listener = drawer_listener;
            let _plugs_listener = plugs_listener;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    event = plugs_event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        let heads = match &*event {
                            PlugsEvent::ListChanged { heads } => heads.clone(),
                            PlugsEvent::PlugAdded { heads, .. } => heads.clone(),
                            PlugsEvent::PlugChanged { heads, .. } => heads.clone(),
                            PlugsEvent::PlugDeleted { heads, .. } => heads.clone(),
                        };
                        store.mutate_sync(|state| {
                            state.plugs_heads = Some(heads);
                        }).await?;

                        if work_tx
                            .send(DocFacetRefIndexWorkItem::RefreshSpecsAndReindexAll)
                            .is_err()
                        {
                            break;
                        }
                    }
                    event = drawer_event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        match &*event {
                            DrawerEvent::ListChanged { drawer_heads } => {
                                store
                                    .mutate_sync(|state| {
                                        state.drawer_heads = Some(drawer_heads.clone());
                                    })
                                    .await?;
                            }
                            DrawerEvent::DocDeleted { id, drawer_heads, .. } => {
                                if work_tx
                                    .send(DocFacetRefIndexWorkItem::DeleteDoc { doc_id: id.clone() })
                                    .is_err()
                                {
                                    break;
                                }
                                store
                                    .mutate_sync(|state| {
                                        state.drawer_heads = Some(drawer_heads.clone());
                                    })
                                    .await?;
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
                                        .send(DocFacetRefIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                store
                                    .mutate_sync(|state| {
                                        state.drawer_heads = Some(drawer_heads.clone());
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
                                        .send(DocFacetRefIndexWorkItem::Upsert {
                                            doc_id: id.clone(),
                                            heads,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                } else if evaluated_latest_branch
                                    && work_tx
                                        .send(DocFacetRefIndexWorkItem::DeleteDoc { doc_id: id.clone() })
                                        .is_err()
                                {
                                    break;
                                }

                                store
                                    .mutate_sync(|state| {
                                        state.drawer_heads = Some(drawer_heads.clone());
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
        DocFacetRefIndexWorkerHandle {
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
    use daybook_types::doc::{AddDocArgs, FacetRaw, WellKnownFacet, WellKnownFacetTag};

    async fn wait_for_outgoing(
        repo: &DocFacetRefIndexRepo,
        doc_id: &DocId,
        expected_len: usize,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
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
