use crate::interlude::*;

use crate::drawer::DrawerRepo;
use crate::index::facet_delta::{FacetDelta, FacetRouteKey};
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::plugs::PlugsRepo;
use big_sync::DeltaWalkerStateTransaction;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo as _;
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use daybook_types::doc::{ArcFacetRaw, ChangeHashSet, DocId, FacetKey, FacetRef};
use daybook_types::manifest::{FacetReferenceKind, FacetReferenceManifest};
use daybook_types::reference::select_json_path_values;
use daybook_types::url::{FACET_SELF_DOC_ID, parse_facet_ref};
use sqlx::{Sqlite, Transaction};
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio_util::sync::CancellationToken;

const FACET_REF_LOCAL_STATE_ID: &str = "@daybook/core/doc-facet-ref-index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocFacetRefEdge {
    pub origin_doc_id: DocId,
    pub origin_facet_key: FacetKey,
    pub target_doc_id: DocId,
    pub target_facet_key: FacetKey,
    pub reference_kind: FacetReferenceKind,
    pub origin_heads: ChangeHashSet,
}

pub struct DocFacetRefIndexRepo {
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sql: SqlCtx,
    reference_specs: tokio::sync::RwLock<HashMap<String, Vec<FacetReferenceManifest>>>,
}

enum FacetSetPreparation {
    Ready(Vec<FacetSetBranchPreparation>),
}

enum FacetSetBranchPreparation {
    Live {
        document_id: DocId,
        heads: ChangeHashSet,
        facets: HashMap<FacetKey, ArcFacetRaw>,
    },
    Tombstone {
        document_id: DocId,
    },
}

impl DocFacetRefIndexRepo {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        facet_set_store: Arc<FacetSetRevisionStore>,
        parent_cancel_token: CancellationToken,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(FACET_REF_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&sql).await?;

        let repo = Arc::new(Self {
            drawer_repo: Arc::clone(&drawer_repo),
            plugs_repo: Arc::clone(&plugs_repo),
            sql,
            reference_specs: tokio::sync::RwLock::new(HashMap::new()),
        });

        repo.refresh_reference_specs().await?;

        let cancel_token = parent_cancel_token.child_token();
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let drawer = Arc::clone(&drawer_repo);
            let facet_set_store = Arc::clone(&facet_set_store);
            let cancel_token = cancel_token.clone();
            async move {
                repo.run_machine(drawer, facet_set_store, cancel_token)
                    .await
                    .expect("facet-ref machine error")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
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
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_target ON facet_ref_edges(target_doc_id, target_facet_key)",
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_origin ON facet_ref_edges(origin_doc_id, origin_facet_key)",
        )
        .execute(&sql.write_pool)
        .await?;

        Ok(())
    }

    async fn refresh_reference_specs_and_reindex_all(&self) -> Res<()> {
        self.refresh_reference_specs().await?;
        self.reindex_all_docs().await?;
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

    async fn reference_tags(&self) -> Vec<String> {
        let mut tags = self
            .reference_specs
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        tags.sort();
        tags
    }

    async fn reindex_all_docs(&self) -> Res<()> {
        sqlx::query("DELETE FROM facet_ref_edges")
            .execute(&self.sql.write_pool)
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
            if branch_path.to_string().starts_with("/tmp/") {
                continue;
            }

            let Some(heads) = doc.branches.get(&branch_path.to_string()).cloned() else {
                continue;
            };
            let Some(facet_keys) = self
                .drawer_repo
                .facet_keys_at_branch_heads(&doc.doc_id, &branch_path, &heads)
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
                .get_at_branch_heads_with_facets_arc(
                    &doc.doc_id,
                    &branch_path,
                    &heads,
                    Some(selected_keys),
                )
                .await?
                .map(|(facets, _)| facets)
                .unwrap_or_default();
            self.reindex_doc_from_facets(&doc.doc_id, &heads, &facets)
                .await?;
        }

        Ok(())
    }

    async fn reindex_doc_from_facets(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facets: &HashMap<FacetKey, ArcFacetRaw>,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        self.replace_outgoing_edges_in_tx(&mut tx, doc_id, heads, facets)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn replace_outgoing_edges_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facets: &HashMap<FacetKey, ArcFacetRaw>,
    ) -> Res<()> {
        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);

        sqlx::query("DELETE FROM facet_ref_edges WHERE origin_doc_id = ?1")
            .bind(doc_id)
            .execute(&mut **tx)
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
                    .bind(reference_kind_to_db_value(&spec.reference_kind()))
                    .bind(&serialized_heads)
                    .execute(&mut **tx)
                    .await?;
                }
            }
        }

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
        .fetch_all(&self.sql.read_pool)
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
        .fetch_all(&self.sql.read_pool)
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
    let selected_values = select_json_path_values(facet_value, spec.json_path())?;
    let mut out = Vec::new();
    for selected_value in selected_values {
        match spec {
            FacetReferenceManifest::UrlString { .. }
            | FacetReferenceManifest::UrlStringSplit { .. }
            | FacetReferenceManifest::UrlStringMany { .. } => {
                append_url_references(
                    &mut out,
                    selected_value,
                    origin_doc_id,
                    origin_facet_key,
                    spec.json_path(),
                )?;
            }
            FacetReferenceManifest::UrlObject { .. }
            | FacetReferenceManifest::UrlObjectMany { .. } => {
                append_object_references(
                    &mut out,
                    selected_value,
                    origin_doc_id,
                    origin_facet_key,
                    spec.json_path(),
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

/// Keyed execution budget for the facet-ref machine; mirrors the frontier
/// worker's concurrent budget.
const FACET_REF_TASK_BUDGET: usize = 64;

/// Scheduling key for one physical branch. Hash of the facet route's
/// (document, branch) pair: collisions only over-serialize a key, never
/// break correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct FacetRefKey(u64);

fn facet_ref_key(route: &FacetRouteKey) -> FacetRefKey {
    facet_ref_key_from_branch(&route.branch_id)
}

fn facet_ref_key_from_branch(branch_id: &daybook_types::doc::BranchId) -> FacetRefKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    FacetRefKey(hasher.finish())
}

/// Why the facet-ref machine's inner loop stopped. The machine's facet
/// walker is keyed and held open, but its selector depends on inputs that
/// change (reference specs, plug revisions); those inputs force
/// a reopen with fresh tags rather than an in-loop merge.
enum FacetRefMachineSignal {
    /// The machine is stopping (cancel token fired).
    Stop,
    /// The selector inputs changed: the facet walker must be reopened.
    Reopen,
}

/// The merged keyed command for one branch: the newest delta with the source
/// cursor it must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FacetRefTask {
    key: FacetRefKey,
    cursor: u64,
    delta: FacetDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FacetRefTaskOutput {
    Applied,
}

async fn run_facet_ref_task(
    task: FacetRefTask,
    repo: Arc<DocFacetRefIndexRepo>,
    facet_state: big_sync::SqliteDeltaWalkerStateRepo,
) -> Res<FacetRefTaskOutput> {
    // All hydration completes before the effect transaction opens; the
    // edge rewrite and its commit are then one SQLite unit.
    let FacetSetPreparation::Ready(prepared) = repo
        .prepare_facet_set_revision(std::slice::from_ref(&task.delta))
        .await?;
    let mut tx = facet_state
        .begin()
        .await
        .map_err(|error| ferr!("beginning facet-ref settlement: {error}"))?;
    repo.apply_facet_set_revision_in_tx(tx.context_mut(), &prepared)
        .await?;
    tx.commit()
        .await
        .map_err(|error| ferr!("committing facet-ref settlement: {error}"))?;
    Ok(FacetRefTaskOutput::Applied)
}

/// The keyed facet-ref machine for one reopen epoch: a `ConcurrentDeltaWalker`
/// over the facet-set source, keyed by branch, feeding per-branch edge tasks;
/// plug revisions break out to a fresh epoch. The epoch's mutable machine
/// state (walker, scheduler, pending, and parked keys) lives as stack locals in
/// `run_machine`; helpers take `&self` plus `&mut` references to it.

impl DocFacetRefIndexRepo {
    async fn on_task_completion(
        self: &Arc<Self>,
        walker: &mut ConcurrentDeltaWalker<
            '_,
            FacetSetRevisionStore,
            big_sync::SqliteDeltaWalkerStateRepo,
            FacetRefKey,
        >,
        tasks: &mut TokioKeyedScheduler<FacetRefKey, FacetRefTask, FacetRefTaskOutput>,
        pending: &mut HashMap<FacetRefKey, FacetRefTask>,
        completion: TokioTaskCompletion<FacetRefTask, FacetRefTaskOutput>,
    ) -> Res<()> {
        let task = completion.command;
        match completion.result {
            FacetRefTaskOutput::Applied => {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                walker.ack(task.key, task.cursor).await?;
                if pending
                    .get(&task.key)
                    .is_some_and(|t| t.cursor == task.cursor)
                {
                    pending.remove(&task.key);
                }
            }
            Err(error) => panic!("facet-ref task failed: {error:?}"),
        }
        Ok(())
    }

    fn on_delta(
        self: &Arc<Self>,
        facet_state: &big_sync::SqliteDeltaWalkerStateRepo,
        tasks: &mut TokioKeyedScheduler<FacetRefKey, FacetRefTask, FacetRefTaskOutput>,
        pending: &mut HashMap<FacetRefKey, FacetRefTask>,
        delta: ConcurrentDelta<FacetRefKey, FacetDelta>,
    ) -> Res<()> {
        let task = FacetRefTask {
            key: delta.key,
            cursor: delta.cursor,
            delta: delta.entry,
        };
        match pending.get(&delta.key) {
            Some(existing) if existing.cursor >= task.cursor => return Ok(()),
            _ => {}
        }
        pending.insert(task.key, task.clone());
        self.start_task(facet_state, tasks, task)
    }

    fn start_task(
        self: &Arc<Self>,
        facet_state: &big_sync::SqliteDeltaWalkerStateRepo,
        tasks: &mut TokioKeyedScheduler<FacetRefKey, FacetRefTask, FacetRefTaskOutput>,
        task: FacetRefTask,
    ) -> Res<()> {
        let future = run_facet_ref_task(
            task.clone(),
            Arc::clone(self),
            facet_state.clone(),
        );
        tasks.replace(task.key, task.clone(), future)?;
        Ok(())
    }

}

impl DocFacetRefIndexRepo {
    async fn prepare_facet_set_revision(&self, entries: &[FacetDelta]) -> Res<FacetSetPreparation> {
        let reference_tags = self.reference_tags().await;
        if reference_tags.is_empty() {
            return Ok(FacetSetPreparation::Ready(Vec::new()));
        }
        let mut branches = BTreeSet::new();
        for delta in entries {
            if delta.key.branch_id.0 != delta.key.document_id {
                continue;
            }
            branches.insert((delta.key.document_id.clone(), delta.key.branch_id.clone()));
        }
        let mut prepared = Vec::with_capacity(branches.len());
        for (document_id, branch_id) in branches {
            let branch_entries = entries.iter().filter(|delta| {
                delta.key.document_id == document_id && delta.key.branch_id == branch_id
            });
            let mut removed = false;
            let mut heads = None;
            for delta in branch_entries {
                match &delta.current_branch_heads {
                    Some(current_heads) => {
                        if heads.as_ref().is_some_and(|seen| seen != current_heads) {
                            return Err(ferr!(
                                "FacetRef facet deltas disagree on branch heads in one revision"
                            ));
                        }
                        heads = Some(current_heads.clone());
                    }
                    None => removed = true,
                }
            }
            if removed && heads.is_none() {
                prepared.push(FacetSetBranchPreparation::Tombstone { document_id });
                continue;
            }
            let heads = heads.expect("live FacetRef route has branch heads");
            let physical_id = branch_id.0.parse::<big_repo::DocumentId>()?;
            let Some(facets) = self
                .drawer_repo
                .hydrate_physical_doc_at_heads(physical_id, heads.clone())
                .await?
            else {
                eyre::bail!(
                    "facet-ref source heads are not materialized for branch {}",
                    branch_id.0
                );
            };
            prepared.push(FacetSetBranchPreparation::Live {
                document_id,
                heads,
                facets: facets
                    .into_iter()
                    .map(|(key, value)| (key, Arc::new(value)))
                    .collect(),
            });
        }
        Ok(FacetSetPreparation::Ready(prepared))
    }

    async fn run_machine(
        self: Arc<Self>,
        drawer: Arc<DrawerRepo>,
        facet_set_store: Arc<FacetSetRevisionStore>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let facet_state = big_sync::SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            FACET_REF_LOCAL_STATE_ID,
            "facets",
        )
        .await
        .map_err(|error| ferr!("initializing facet-ref FacetSet walker state: {error}"))?;
        // Table renamed off "plugs": the old state tracked the retired plugs
        // frontier's revision numbering, which is unrelated to the facet
        // revisions the plug event store replays.
        let plugs_state = big_sync::SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            FACET_REF_LOCAL_STATE_ID,
            "plug-events",
        )
        .await
        .map_err(|error| ferr!("initializing facet-ref Plugs walker state: {error}"))?;
        let plugs_durable = plugs_state.progress().await?.upstream_revision;
        let plugs_events = Arc::new(crate::plugs::PlugsConfigEventStore::new(
            Arc::clone(&facet_set_store),
            Arc::clone(&drawer),
            &self.plugs_repo,
        ));
        let plugs_reader = plugs_events
            .open((), plugs_durable)
            .await
            .map_err(|error| ferr!("opening facet-ref Plugs reader: {error}"))?;
        let mut plugs_walker = SerialDeltaWalker::open(plugs_reader, &plugs_state)
            .await
            .map_err(|error| ferr!("opening facet-ref Plugs walker: {error}"))?;
        'reopen: loop {
            // The epoch's mutable machine state lives on the stack.
            let tags = self.reference_tags().await;
            let source = facet_set_store.as_ref();
            let selector = FacetSetSelector::FacetTags(tags);
            let durable = facet_state.progress().await?.upstream_revision;
            let reader = source
                .open(selector, durable)
                .await
                .map_err(|error| ferr!("opening facet-ref FacetSet reader: {error}"))?;
            let mut walker = ConcurrentDeltaWalker::open(
                reader,
                facet_state.clone(),
                |entry: &FacetDelta| facet_ref_key(&entry.key),
            )
            .await
            .map_err(|error| ferr!("opening facet-ref FacetSet walker: {error}"))?;
            let mut tasks = TokioKeyedScheduler::new(FACET_REF_TASK_BUDGET);
            // The newest unacked delta per key.
            let mut pending: HashMap<FacetRefKey, FacetRefTask> = HashMap::new();
            // Exit signal from a select arm; arms never return mid-select so
            // the stack state is never moved while its futures hold borrows.
            let signal: FacetRefMachineSignal = loop {
                let available = FACET_REF_TASK_BUDGET.saturating_sub(tasks.active_count());
                let next_deadline = tasks.next_deadline();
                tokio::select! {
                    biased;
                    // The default `signal` is already `Stop`.
                    _ = cancel_token.cancelled() => break FacetRefMachineSignal::Stop,
                    completion = tasks.next_completion() => {
                        self.on_task_completion(
                            &mut walker,
                            &mut tasks,
                            &mut pending,
                            completion?,
                        )
                        .await?;
                    }
                    plug_read = plugs_walker.next() => {
                        match plug_read
                            .map_err(|error| ferr!("reading facet-ref Plugs walker: {error}"))?
                        {
                            RevisionRead::ReplayComplete { .. } => continue,
                            RevisionRead::Entries { revision, entries } => {
                                let touched = entries
                                    .iter()
                                    .any(|entry| !entry.events.is_empty());
                                if touched {
                                    self.refresh_reference_specs_and_reindex_all().await?;
                                }
                                plugs_walker
                                    .settle(revision)
                                    .await
                                    .map_err(|error| ferr!("settling facet-ref Plugs walker: {error}"))?;
                                if touched {
                                    break FacetRefMachineSignal::Reopen;
                                }
                            }
                        }
                    }
                    read = async {
                        if available == 0 {
                            std::future::pending().await
                        } else {
                            walker.next(available).await
                        }
                    } => match read? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {}
                        ConcurrentDeltaRead::Entries { entries, .. } => {
                            for delta in entries {
                                self.on_delta(
                                    &facet_state,
                                    &mut tasks,
                                    &mut pending,
                                    delta,
                                )?;
                            }
                        }
                    },
                    _ = async {
                        if let Some(deadline) = next_deadline {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        tasks.tick(std::time::Instant::now())?;
                    }
                }
            };
            match signal {
                FacetRefMachineSignal::Stop => return Ok(()),
                // The selector inputs changed (plug revision or refreshed
                // tags): reopen the facet walker with fresh tags.
                FacetRefMachineSignal::Reopen => continue 'reopen,
            }
        }
    }


    async fn apply_facet_set_revision_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prepared: &[FacetSetBranchPreparation],
    ) -> Res<()> {
        for preparation in prepared {
            match preparation {
                FacetSetBranchPreparation::Tombstone { document_id } => {
                    sqlx::query(
                        "DELETE FROM facet_ref_edges WHERE origin_doc_id = ? OR target_doc_id = ?",
                    )
                    .bind(document_id)
                    .bind(document_id)
                    .execute(&mut **tx)
                    .await?;
                }
                FacetSetBranchPreparation::Live {
                    document_id,
                    heads,
                    facets,
                } => {
                    self.replace_outgoing_edges_in_tx(tx, document_id, heads, facets)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

fn append_object_references(
    out: &mut Vec<ExtractedReference>,
    selected_value: &serde_json::Value,
    origin_doc_id: &DocId,
    origin_facet_key: &FacetKey,
    json_path: &str,
) -> Res<()> {
    let facet_ref: FacetRef =
        serde_json::from_value(selected_value.clone()).wrap_err_with(|| {
            format!(
                "expected reference object at path '{}' for facet '{}' but found {}",
                json_path, origin_facet_key, selected_value
            )
        })?;
    out.push(parse_object_reference(
        facet_ref,
        origin_doc_id,
        origin_facet_key,
        json_path,
    )?);
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

fn parse_object_reference(
    facet_ref: FacetRef,
    origin_doc_id: &DocId,
    origin_facet_key: &FacetKey,
    json_path: &str,
) -> Res<ExtractedReference> {
    let parsed_ref = parse_facet_ref(&facet_ref.r#ref).wrap_err_with(|| {
        format!(
            "invalid facet reference URL '{}' at path '{}' for facet '{}'",
            facet_ref.r#ref, json_path, origin_facet_key
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
        _ => {
            eyre::bail!("unsupported reference kind '{}'", value);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use daybook_types::doc::{AddDocArgs, FacetRaw, Note, WellKnownFacet, WellKnownFacetTag};

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
        eyre::bail!("timeout waiting for outgoing references");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_facet_ref_index_tracks_embedding_references() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_facet_ref_index_repo);

        let src_doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note(Note {
                        mime: "text/plain".into(),
                        content: "hello".into(),
                    })),
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
                branch_path: BranchPathBuf::from("main"),
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
