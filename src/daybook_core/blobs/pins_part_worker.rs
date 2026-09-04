use crate::drawer::DrawerRepo;
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::interlude::*;
use big_repo::SharedPartStore;
use big_sync::{DeltaWalkerStateRepo as _, DeltaWalkerStateTransaction};
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use daybook_types::doc::{BranchId, BranchPathBuf, ChangeHashSet, DocId, WellKnownFacetTag};
use sqlx::{QueryBuilder, Row, Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use tokio_util::sync::CancellationToken;
}

#[cfg(test)]
use daybook_types::doc::FacetKey;

pub const DOC_BLOB_PINS_LOCAL_STATE_ID: &str = "@daybook/core/doc-blob-pins-index";

/// Spawn the blob-pins-part worker: one keyed machine reconciling the
/// blob-pin facets of every document branch into the document's
/// blob-inventory part. Private shared state; no public surface —
/// observers read the `doc_blob_pins` SQLite projection or the part store.
pub async fn spawn_blob_pins_part_worker(
    part_store: SharedPartStore,
    sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    drawer: Arc<DrawerRepo>,
    facet_set_store: Arc<FacetSetRevisionStore>,
    parent_cancel_token: CancellationToken,
) -> Res<crate::repos::RepoStopToken> {
    let sql = sqlite_local_state_repo
        .ensure_sqlite_ctx(DOC_BLOB_PINS_LOCAL_STATE_ID)
        .await?;
    Ctx::init_schema(&sql).await?;

    let ctx = Arc::new(Ctx { part_store, sql });
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let mut worker = Worker::new(Arc::clone(&ctx));
    let worker_handle = tokio::spawn(async move {
        worker
            .run_facet_machine(drawer, facet_set_store, worker_cancel_token)
            .await
            .unwrap();
    });
    Ok(crate::repos::RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

/// Shared state of the blob-pins-part worker: the machine and its reconcile
/// tasks hold this Arc.
struct Ctx {
    part_store: SharedPartStore,
    sql: SqlCtx,
}

/// Private machine owner. Mutable walker state stays local to the machine;
/// task futures share only the context needed for reconciliation.
struct Worker {
    ctx: Arc<Ctx>,
}

impl Worker {
    fn new(ctx: Arc<Ctx>) -> Self {
        Self { ctx }
    }
}

impl std::ops::Deref for Worker {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Ctx {

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_blob_pins (
                doc_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL DEFAULT 0,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(doc_id, branch_id, blob_hash)
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        let has_branch_id_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('doc_blob_pins') WHERE name = 'branch_id'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_branch_id_col.is_none() {
            let mut tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("ALTER TABLE doc_blob_pins RENAME TO doc_blob_pins_legacy")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"CREATE TABLE doc_blob_pins (
                    doc_id TEXT NOT NULL
                  , branch_id TEXT NOT NULL
                  , blob_hash TEXT NOT NULL
                  , length_octets INTEGER NOT NULL DEFAULT 0
                  , origin_heads TEXT NOT NULL
                  , PRIMARY KEY(doc_id, branch_id, blob_hash)
                ) STRICT"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"INSERT INTO doc_blob_pins(
                    doc_id, branch_id, blob_hash, length_octets, origin_heads
                )
                SELECT doc_id
                     , CASE WHEN branch_path = 'main' THEN doc_id ELSE branch_path END
                     , blob_hash
                     , length_octets
                     , origin_heads
                  FROM doc_blob_pins_legacy"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE doc_blob_pins_legacy")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_doc_branch ON doc_blob_pins(doc_id, branch_id)",
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_blob_hash ON doc_blob_pins(blob_hash)",
        )
        .execute(&sql.write_pool)
        .await?;

        Ok(())
    }

    async fn load_branch_states_in<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        branches: &[(DocId, String)],
    ) -> Res<BTreeMap<(DocId, String), BTreeMap<String, u64>>> {
        if branches.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT doc_id, branch_id, blob_hash, length_octets FROM doc_blob_pins WHERE ",
        );
        for (index, (doc_id, branch_id)) in branches.iter().enumerate() {
            if index != 0 {
                query.push(" OR ");
            }
            query
                .push("(doc_id = ")
                .push_bind(doc_id)
                .push(" AND branch_id = ")
                .push_bind(branch_id)
                .push(")");
        }
        let mut out = BTreeMap::<(DocId, String), BTreeMap<String, u64>>::new();
        for row in query.build().fetch_all(&mut **tx).await? {
            let doc_id: DocId = row.try_get("doc_id")?;
            let branch_id: String = row.try_get("branch_id")?;
            let hash: String = row.try_get("blob_hash")?;
            let length = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            out.entry((doc_id, branch_id))
                .or_default()
                .insert(hash, length);
        }
        Ok(out)
    }

    async fn load_doc_states(
        &self,
        documents: &BTreeSet<DocId>,
    ) -> Res<BTreeMap<(DocId, String), BTreeMap<String, u64>>> {
        if documents.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut tx = self.sql.read_pool.begin().await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT doc_id, branch_id, blob_hash, length_octets FROM doc_blob_pins WHERE doc_id IN (",
        );
        let mut values = query.separated(", ");
        for document_id in documents {
            values.push_bind(document_id);
        }
        values.push_unseparated(")");
        let mut out = BTreeMap::<(DocId, String), BTreeMap<String, u64>>::new();
        for row in query.build().fetch_all(&mut *tx).await? {
            let document_id: DocId = row.try_get("doc_id")?;
            let branch_id: String = row.try_get("branch_id")?;
            let hash: String = row.try_get("blob_hash")?;
            let length = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            out.entry((document_id, branch_id))
                .or_default()
                .insert(hash, length);
        }
        tx.rollback().await?;
        Ok(out)
    }

    async fn prepare_facet_deltas(
        &self,
        drawer: &DrawerRepo,
        entries: &[FacetDelta],
    ) -> Res<Preparation> {
        let mut grouped = BTreeMap::<(DocId, String), Vec<&FacetDelta>>::new();
        for delta in entries {
            grouped
                .entry((delta.key.document_id.clone(), delta.key.branch_id.0.clone()))
                .or_default()
                .push(delta);
        }
        if grouped.is_empty() {
            return Ok(Preparation::Ready(Vec::new()));
        }

        let branches = grouped.keys().cloned().collect::<Vec<_>>();
        let mut read_tx = self.sql.read_pool.begin().await?;
        let prior_states = self.load_branch_states_in(&mut read_tx, &branches).await?;
        read_tx.rollback().await?;

        let mut prepared = Vec::with_capacity(grouped.len());
        for ((document_id, branch_id), deltas) in grouped {
            let branch_id = BranchId(branch_id);
            let prior = prior_states
                .get(&(document_id.clone(), branch_id.0.clone()))
                .cloned()
                .unwrap_or_default();
            let mut next = prior.clone();
            let mut seen_branch_heads = None;
            for delta in deltas {
                if let Some(seen) = &seen_branch_heads {
                    if seen != &delta.current_branch_heads {
                        return Err(ferr!(
                            "BlobPin facet deltas disagree on branch heads in one revision"
                        ));
                    }
                } else {
                    seen_branch_heads = Some(delta.current_branch_heads.clone());
                }
                let Some(current) = &delta.current else {
                    next.remove(&delta.key.facet_key.id);
                    continue;
                };
                let value = match drawer
                    .hydrate_facet_value_at_heads(
                        &delta.key.branch_id,
                        &current.branch_heads,
                        &delta.key.facet_key,
                    )
                    .await?
                {
                    crate::drawer::ExactFacetValueHydration::Deferred => {
                        eyre::bail!(
                            "blob-pin source heads are not materialized for branch {}",
                            delta.key.branch_id.0
                        );
                    }
                    crate::drawer::ExactFacetValueHydration::Absent => {
                        next.remove(&delta.key.facet_key.id);
                        continue;
                    }
                    crate::drawer::ExactFacetValueHydration::Present(value) => value,
                };
                let pin = match daybook_types::doc::WellKnownFacet::from_json(
                    value,
                    WellKnownFacetTag::BlobPin,
                )
                .wrap_err("decode BlobPin facet")?
                {
                    daybook_types::doc::WellKnownFacet::BlobPin(pin) => pin,
                    other => eyre::bail!("expected BlobPin facet, got {:?}", other.tag()),
                };
                delta
                    .key
                    .facet_key
                    .id
                    .parse::<crate::blobs::BlobId>()
                    .map_err(|_| ferr!("BlobPin facet id is not a valid blob id"))?;
                next.insert(delta.key.facet_key.id.clone(), pin.length_octets);
            }
            prepared.push(PreparedBranchDelta {
                document_id,
                branch_id,
                branch_heads: seen_branch_heads.expect("grouped BlobPin deltas non-empty"),
                prior,
                next,
            });
        }
        Ok(Preparation::Ready(prepared))
    }

    async fn replace_branch_state_in<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        prepared: &PreparedBranchDelta,
    ) -> Res<()> {
        // FIXME: this file is noto using query macros
        sqlx::query("DELETE FROM doc_blob_pins WHERE doc_id = ? AND branch_id = ?")
            .bind(&prepared.document_id)
            .bind(&prepared.branch_id.0)
            .execute(&mut **tx)
            .await?;
        let Some(branch_heads) = &prepared.branch_heads else {
            return Ok(());
        };
        if prepared.next.is_empty() {
            return Ok(());
        }
        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&branch_heads.0))
                .expect(ERROR_JSON);
        let rows: Vec<(&str, i64)> = prepared
            .next
            .iter()
            .map(|(hash, length)| {
                Ok((
                    hash.as_str(),
                    i64::try_from(*length)
                        .map_err(|_| ferr!("BlobPin length exceeds SQLite INTEGER range"))?,
                ))
            })
            .collect::<Res<_>>()?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO doc_blob_pins(doc_id, branch_id, blob_hash, length_octets, origin_heads) ",
        );
        query.push_values(rows.iter(), |mut row, (hash, length)| {
            row.push_bind(&prepared.document_id)
                .push_bind(&prepared.branch_id.0)
                .push_bind(hash)
                .push_bind(*length)
                .push_bind(&serialized_heads);
        });
        query.push(" ON CONFLICT(doc_id, branch_id, blob_hash) DO UPDATE SET");
        query.push(" length_octets = excluded.length_octets, origin_heads = excluded.origin_heads");
        query.build().execute(&mut **tx).await?;
        Ok(())
    }

    async fn apply_prepared_in_context<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        prepared: &[PreparedBranchDelta],
    ) -> Res<()> {
        let branches = prepared
            .iter()
            .map(|branch| (branch.document_id.clone(), branch.branch_id.0.clone()))
            .collect::<Vec<_>>();
        let authoritative = self.load_branch_states_in(tx, &branches).await?;
        for branch in prepared {
            let actual_prior = authoritative
                .get(&(branch.document_id.clone(), branch.branch_id.0.clone()))
                .cloned()
                .unwrap_or_default();
            if actual_prior != branch.prior {
                return Err(ferr!(
                    "blob-pins-part branch state changed while preparing revision"
                ));
            }
            self.replace_branch_state_in(tx, branch).await?;
        }
        Ok(())
    }

    async fn reconcile_part_store(
        &self,
        before: &BTreeMap<(DocId, String), BTreeMap<String, u64>>,
        prepared: &[PreparedBranchDelta],
    ) -> Res<()> {
        let documents = prepared
            .iter()
            .map(|branch| branch.document_id.clone())
            .collect::<BTreeSet<_>>();
        let mut after = before.clone();
        for branch in prepared {
            let key = (branch.document_id.clone(), branch.branch_id.0.clone());
            if branch.branch_heads.is_some() {
                after.insert(key, branch.next.clone());
            } else {
                after.remove(&key);
            }
        }
        for document_id in &documents {
            let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(document_id);
            self.part_store.ensure_part(part_id).await?;
            let old_hashes = before
                .iter()
                .filter(|((doc_id, _), _)| doc_id == document_id)
                .flat_map(|(_, state)| state.keys().cloned())
                .collect::<BTreeSet<_>>();
            let new_hashes = after
                .iter()
                .filter(|((doc_id, _), _)| doc_id == document_id)
                .flat_map(|(_, state)| state.keys().cloned())
                .collect::<BTreeSet<_>>();
            for branch in prepared
                .iter()
                .filter(|branch| &branch.document_id == document_id)
            {
                for (hash, length) in &branch.next {
                    self.part_store
                        .set_obj_payload(
                            crate::blobs::blob_id_from_hash(hash),
                            serde_json::json!({ "lengthOctets": length }),
                        )
                        .await?;
                }
            }
            for hash in new_hashes.difference(&old_hashes) {
                self.part_store
                    .add_obj_to_parts(crate::blobs::blob_id_from_hash(hash), vec![part_id])
                    .await?;
            }
            for hash in old_hashes.difference(&new_hashes) {
                self.part_store
                    .remove_obj_from_part(crate::blobs::blob_id_from_hash(hash), part_id)
                    .await?;
            }
        }
        Ok(())
    }

}

enum Preparation {
    Ready(Vec<PreparedBranchDelta>),
}

struct PreparedBranchDelta {
    document_id: DocId,
    branch_id: BranchId,
    branch_heads: Option<ChangeHashSet>,
    prior: BTreeMap<String, u64>,
    next: BTreeMap<String, u64>,
}


/// Keyed execution budget for the blob-pins-part machine; mirrors the
/// frontier worker's concurrent budget.
const BLOB_PINS_PART_TASK_BUDGET: usize = 64;

/// Scheduling key for one physical branch. Hash of the branch id: collisions
/// only over-serialize a key, never break correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BlobPinsPartKey(u64);

fn blob_pins_part_key(branch_id: &BranchId) -> BlobPinsPartKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    BlobPinsPartKey(hasher.finish())
}

/// The merged keyed command for one branch: the newest delta with the source
/// cursor it must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobPinsPartTask {
    key: BlobPinsPartKey,
    cursor: u64,
    /// The merged batch for the key: one delta per facet route, newest
    /// arrival per route. A revision may carry several BlobPin routes of the
    /// same branch, and the walker delivers newer revisions of a key while an
    /// older one is still un-acked, so siblings must accumulate — dropping
    /// them loses facet routes.
    deltas: Vec<FacetDelta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobPinsPartTaskOutput {
    Applied,
}

async fn run_blob_pins_part_task(
    task: BlobPinsPartTask,
    ctx: Arc<Ctx>,
    drawer: Arc<DrawerRepo>,
    state: big_sync::SqliteDeltaWalkerStateRepo,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
) -> Res<BlobPinsPartTaskOutput> {
    // All hydration completes before the reconcile section opens; the
    // reindex write and its commit are then one SQLite unit.
    let Preparation::Ready(prepared) = ctx
        .prepare_facet_deltas(&drawer, &task.deltas)
        .await?;
    // The part-store inventory is per document: concurrent branch tasks of
    // one document reconcile against fresh states inside this section, so
    // their inventory diffs cannot interleave.
    let _guard = reconcile_lock.lock().await;
    let documents = prepared
        .iter()
        .map(|branch| branch.document_id.clone())
        .collect::<BTreeSet<_>>();
    let before = ctx.load_doc_states(&documents).await?;
    ctx.reconcile_part_store(&before, &prepared).await?;
    let mut tx = state
        .begin()
        .await
        .map_err(|error| ferr!("beginning blob-pins-part settlement: {error}"))?;
    ctx.apply_prepared_in_context(tx.context_mut(), &prepared)
        .await?;
    tx.commit()
        .await
        .map_err(|error| ferr!("committing blob-pins-part settlement: {error}"))?;
    Ok(BlobPinsPartTaskOutput::Applied)
}

impl Worker {
    /// The keyed blob-pins-part machine: a `ConcurrentDeltaWalker` over the
/// facet-set source, keyed by branch, with per-branch reconcile tasks.
/// Mutable machine state lives as stack locals here.
async fn run_facet_machine(
    &mut self,
    drawer: Arc<DrawerRepo>,
    facet_set_store: Arc<FacetSetRevisionStore>,
    cancel_token: CancellationToken,
) -> Res<()> {
    let state = big_sync::SqliteDeltaWalkerStateRepo::new(
        self.sql.read_pool.clone(),
        self.sql.write_pool.clone(),
        "@daybook/core/blob-pins-part-worker",
        "facets",
    )
    .await
    .map_err(|error| ferr!("initializing blob-pins-part FacetSet state: {error}"))?;
    let durable = state.progress().await?.upstream_revision;
    let reader = facet_set_store
        .open(FacetSetSelector::Tag(WellKnownFacetTag::BlobPin), durable)
        .await
        .map_err(|error| ferr!("opening blob-pins-part FacetSet reader: {error}"))?;
    let mut walker = ConcurrentDeltaWalker::open(
        reader,
        state.clone(),
        |entry: &FacetDelta| blob_pins_part_key(&entry.key.branch_id),
    )
    .await
    .map_err(|error| ferr!("opening blob-pins-part FacetSet walker: {error}"))?;
    let mut tasks = TokioKeyedScheduler::new(BLOB_PINS_PART_TASK_BUDGET);
    // Per-document reconcile lock: concurrent branch tasks of one document
    // reconcile against fresh states inside the task's lock section.
    let reconcile_lock = Arc::new(tokio::sync::Mutex::new(()));
    // The newest unacked delta per key.
    let mut pending: HashMap<BlobPinsPartKey, BlobPinsPartTask> = HashMap::new();
    loop {
        let available = BLOB_PINS_PART_TASK_BUDGET.saturating_sub(tasks.active_count());
        let next_deadline = tasks.next_deadline();
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Ok(()),
            completion = tasks.next_completion() => {
                self.on_task_completion(
                    &mut walker,
                    &mut tasks,
                    &mut pending,
                    completion?,
                )
                .await?;
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
                            &state,
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
    }

    async fn on_task_completion(
        &mut self,
        walker: &mut ConcurrentDeltaWalker<
        '_,
        FacetSetRevisionStore,
        big_sync::SqliteDeltaWalkerStateRepo,
        BlobPinsPartKey,
    >,
    tasks: &mut TokioKeyedScheduler<BlobPinsPartKey, BlobPinsPartTask, BlobPinsPartTaskOutput>,
    pending: &mut HashMap<BlobPinsPartKey, BlobPinsPartTask>,
    completion: TokioTaskCompletion<BlobPinsPartTask, BlobPinsPartTaskOutput>,
) -> Res<()> {
    let task = completion.command;
    match completion.result {
        BlobPinsPartTaskOutput::Applied => {
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
        Err(error) => panic!("blob-pins-part task failed: {error:?}"),
    }
    Ok(())
}

/// Merge a delta into the key's pending batch.
///
/// One revision may carry several BlobPin routes of the same branch (all
/// sharing the branch key), and the walker delivers a newer revision of a
/// key while an older one is still un-acked. Arrivals therefore accumulate
/// per route — a stale-cursor arrival for a key whose batch already covers
/// a newer revision is the only drop. If a task is already in flight,
/// `start_task` replaces it with the union: hydration is read-only and the
/// reconcile write commits in one transaction, so re-running a delta is
/// safe. The cursor advances to the highest batch covered, and the walker
/// ack happens strictly after the union is durable.
fn on_delta(
    &mut self,
    drawer: &Arc<DrawerRepo>,
    state: &big_sync::SqliteDeltaWalkerStateRepo,
    reconcile_lock: &Arc<tokio::sync::Mutex<()>>,
    tasks: &mut TokioKeyedScheduler<BlobPinsPartKey, BlobPinsPartTask, BlobPinsPartTaskOutput>,
    pending: &mut HashMap<BlobPinsPartKey, BlobPinsPartTask>,
    delta: ConcurrentDelta<BlobPinsPartKey, FacetDelta>,
) -> Res<()> {
    // Take the pending batch out of the map and merge in place: no
    // per-arrival clone of the accumulated work.
    let task = match pending.remove(&delta.key) {
        Some(existing) if existing.cursor > delta.cursor => {
            pending.insert(existing.key, existing);
            return Ok(());
        }
        Some(mut existing) => {
            existing.deltas.retain(|d| d.key != delta.entry.key);
            existing.deltas.push(delta.entry.clone());
            existing.cursor = existing.cursor.max(delta.cursor);
            existing
        }
        None => BlobPinsPartTask {
            key: delta.key,
            cursor: delta.cursor,
            deltas: vec![delta.entry],
        },
    };
    pending.insert(task.key, task.clone());
    self.start_task(drawer, state, reconcile_lock, tasks, task)
}

fn start_task(
    &mut self,
    drawer: &Arc<DrawerRepo>,
    state: &big_sync::SqliteDeltaWalkerStateRepo,
    reconcile_lock: &Arc<tokio::sync::Mutex<()>>,
    tasks: &mut TokioKeyedScheduler<BlobPinsPartKey, BlobPinsPartTask, BlobPinsPartTaskOutput>,
    task: BlobPinsPartTask,
) -> Res<()> {
    let future = run_blob_pins_part_task(
        task.clone(),
        Arc::clone(&self.ctx),
        Arc::clone(drawer),
        state.clone(),
        Arc::clone(reconcile_lock),
    );
    tasks.replace(task.key, task.clone(), future)?;
    Ok(())
}

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync::DeltaWalkerStateRepo;
    use daybook_types::doc::{AddDocArgs, BlobPin, BranchPath, DocPatch, FacetRaw, WellKnownFacet};

    async fn wait_for_partition_member_count(
        part_store: &SharedPartStore,
        partition_id: PartId,
        expected: u64,
    ) -> Res<()> {
        loop {
            let count = part_store.member_count(partition_id).await?;
            if count == expected {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn facet_walker_progress(sql: &SqlCtx) -> Res<u64> {
        let state = big_sync::SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            "@daybook/core/blob-pins-part-worker",
            "facets",
        )
        .await?;
        Ok(state.progress().await?.upstream_revision)
    }

    /// Distinct blob hashes pinned for one document (the machine's
    /// `doc_blob_pins` SQLite projection).
    async fn list_hashes_for_doc(sql: &SqlCtx, doc_id: &DocId) -> Res<Vec<String>> {
        Ok(sqlx::query_scalar(
            r#"SELECT DISTINCT blob_hash
                 FROM doc_blob_pins
                WHERE doc_id = ?
                ORDER BY blob_hash ASC"#,
        )
        .bind(doc_id)
        .fetch_all(&sql.read_pool)
        .await?)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pins_part_worker_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let sql = test_context
            .rt
            .sqlite_local_state_repo
            .ensure_sqlite_ctx(DOC_BLOB_PINS_LOCAL_STATE_ID)
            .await?;
        let blob_part_store = &test_context.rt.rcx.blob_part_store;

        let blob_id_1 = crate::blobs::BlobId::random();
        let blob_id_2 = crate::blobs::BlobId::random();
        let hash_1 = blob_id_1.to_string();
        let hash_2 = blob_id_2.to_string();

        let key_pin_1 = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_1.clone(),
        };
        let key_pin_2 = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_2.clone(),
        };

        // 1. Add document with two BlobPin facets
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        key_pin_1.clone(),
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 150 })),
                    ),
                    (
                        key_pin_2.clone(),
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 250 })),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(&doc_id);

        wait_for_partition_member_count(blob_part_store, part_id, 2).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            vec![part_id]
        );
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_2))
                .await?,
            vec![part_id]
        );

        let hashes = list_hashes_for_doc(&sql, &doc_id).await?;
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&hash_1));
        assert!(hashes.contains(&hash_2));
        let initial_progress = facet_walker_progress(&sql).await?;
        assert!(initial_progress > 0);

        // 2. Update document: remove pin 2
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_2],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;

        wait_for_partition_member_count(blob_part_store, part_id, 1).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            vec![part_id]
        );
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_2))
                .await?,
            Vec::<PartId>::new()
        );

        let hashes_after_update = list_hashes_for_doc(&sql, &doc_id).await?;
        assert_eq!(hashes_after_update, vec![hash_1.clone()]);
        let update_progress = facet_walker_progress(&sql).await?;
        assert!(update_progress > initial_progress);

        // Keep an independent branch so removing main's pin exercises
        // branch-scoped state rather than removing shared physical membership.
        let main_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, BranchPath::new("main"))
            .await?
            .ok_or_eyre("missing main branch heads")?;
        let branch_path = BranchPathBuf::from("/test/blob-pins-part-branch");
        test_context
            .drawer_repo
            .create_branch_at_heads_from_branch(
                &doc_id,
                &branch_path,
                BranchPath::new("main"),
                &main_heads,
                None,
            )
            .await?;
        wait_for_partition_member_count(blob_part_store, part_id, 1).await?;
        assert_eq!(
            list_hashes_for_doc(&sql, &doc_id).await?,
            vec![hash_1.clone()]
        );

        // Removing main's pin must not remove the branch's physical
        // membership.
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_1.clone()],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        wait_for_partition_member_count(blob_part_store, part_id, 1).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            vec![part_id]
        );
        assert_eq!(
            list_hashes_for_doc(&sql, &doc_id).await?,
            vec![hash_1.clone()]
        );

        let branch_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch_path)
            .await?
            .ok_or_eyre("missing blob-pins-part branch heads")?;
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_1],
                    user_path: None,
                },
                BranchPath::new(branch_path.as_str()),
                Some(branch_heads),
            )
            .await?;
        wait_for_partition_member_count(blob_part_store, part_id, 0).await?;
        assert_eq!(
            list_hashes_for_doc(&sql, &doc_id).await?,
            Vec::<String>::new()
        );

        // 3. Delete document: the branch-scoped tombstones are already
        // empty, so the document deletion remains an idempotent no-op for
        // physical membership.
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_partition_member_count(blob_part_store, part_id, 0).await?;
        assert!(list_hashes_for_doc(&sql, &doc_id).await?.is_empty());

        test_context.stop().await?;
        Ok(())
    }
}
