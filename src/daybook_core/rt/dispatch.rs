use tokio_util::sync::CancellationToken;

use crate::interlude::*;
/// Immutable execution meaning admitted before dispatch persistence. Unknown
/// durable schema tags must never fall back to the currently enabled component.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CapturedWflowExecution {
    V1 {
        manifest_doc_id: String,
        manifest_branch: String,
        manifest_heads: ChangeHashSet,
        wflow: wflow::wflow_core::metastore::WflowMeta,
        component_blobs: Vec<crate::blobs::BlobId>,
        handler_keys: Vec<String>,
    },
}

impl CapturedWflowExecution {
    pub fn wflow(&self) -> &wflow::wflow_core::metastore::WflowMeta {
        let Self::V1 { wflow, .. } = self;
        wflow
    }

    pub fn workload_id(&self) -> &str {
        let wflow::wflow_core::metastore::WflowServiceMeta::Wasmcloud(service) =
            &self.wflow().service
        else {
            panic!("captured Daybook execution must use WASM");
        };
        &service.workload_id
    }

    #[cfg(test)]
    pub(crate) fn fixture() -> Self {
        use wflow::wflow_core::metastore::*;
        Self::V1 {
            manifest_doc_id: "fixture-manifest".into(),
            manifest_branch: "main".into(),
            manifest_heads: ChangeHashSet(vec![automerge::ChangeHash([1; 32])].into()),
            wflow: WflowMeta {
                key: "key".into(),
                service: WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                    workload_id: "fixture-workload".into(),
                }),
            },
            component_blobs: vec![crate::blobs::BlobId::new([1; 32])],
            handler_keys: vec!["key".into()],
        }
    }
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
    pub args: ActiveDispatchArgs,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub execution: CapturedWflowExecution,
    #[serde(default = "dispatch_status_active")]
    pub status: DispatchStatus,
    #[serde(default)]
    pub waiting_on_dispatch_ids: Vec<String>,
    #[serde(default)]
    pub on_success_hooks: Vec<DispatchOnSuccessHook>,
}

fn dispatch_status_active() -> DispatchStatus {
    DispatchStatus::Active
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DispatchStatus {
    Waiting,
    Active,
    Succeeded,
    Failed,
    Cancelled,
}

impl DispatchStatus {
    /// A dispatch in a terminal status has settled its target effects; nothing
    /// may publish on its behalf or roll it back afterwards.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum DispatchOnSuccessHook {
    InitMarkDone {
        init_id: String,
        run_mode: daybook_types::manifest::InitRunMode,
    },
    ProcessorSettlement {
        #[autosurgeon(with = "am_utils_rs::codecs::json")]
        slot: super::triage::slots::ProcessorSlotKey,
        #[autosurgeon(with = "am_utils_rs::codecs::json")]
        capture: super::triage::slots::ProcessorCapture,
        #[autosurgeon(with = "am_utils_rs::codecs::json")]
        domain: Option<super::triage::domain::ProcessorDomainReference>,
    },
    CommandInvokeReply {
        parent_wflow_job_id: String,
        request_id: String,
    },
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum ActiveDispatchDeets {
    Wflow {
        #[serde(default)]
        wflow_partition_id: Option<String>,
        #[serde(default)]
        entry_id: Option<u64>,
        plug_id: String,
        #[serde(default)]
        routine_name: String,
        bundle_name: String,
        #[serde(default)]
        wflow_job_id: Option<String>,
    },
}

impl ActiveDispatchDeets {
    pub fn routine_name(&self) -> &str {
        match self {
            Self::Wflow { routine_name, .. } => routine_name,
        }
    }
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum ActiveDispatchArgs {
    FacetRoutine(FacetRoutineArgs),
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct ProcessorInvocation {
    pub trigger_doc_id: daybook_types::doc::DocId,
    pub changed_facet_keys: Vec<String>,
    /// Stable distributed obligation identity; use as the external idempotency key.
    pub task_id: Option<String>,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum RoutineInvocation {
    Processor(ProcessorInvocation),
    Command,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct DocFacetTokens {
    pub doc_id: daybook_types::doc::DocId,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub branch_path: daybook_types::doc::BranchPathBuf,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub staging_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct FacetRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub branch_path: daybook_types::doc::BranchPathBuf,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub staging_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    pub invocation: RoutineInvocation,
    pub primary_doc: DocFacetTokens,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub config_docs: Vec<DocFacetTokens>,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub local_state_acl: Vec<daybook_types::manifest::RoutineLocalStateAccess>,
    #[serde(default)]
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub command_invoke_acl_snapshot: Vec<url::Url>,
    #[serde(default)]
    pub wflow_args_json: Option<String>,
}

pub(crate) fn facet_routine_args_fingerprint(args: &FacetRoutineArgs) -> String {
    // FIXME: use drisl
    let bytes = serde_json::to_vec(args).expect(ERROR_JSON);
    utils_rs::hash::blake3_hash_bytes_multibase(&bytes)
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
    DispatchAdded {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::EventOrigin,
    },
    DispatchUpdated {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::EventOrigin,
    },
    DispatchDeleted {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::EventOrigin,
    },
}

/// An in-process snapshot of one durable dispatch attempt. Metadata snapshots
/// share the arbitration cell; changing the workflow job creates a new attempt.
#[derive(Debug, Clone)]
pub struct DispatchAttempt {
    dispatch: ActiveDispatch,
    arbitration: Arc<std::sync::atomic::AtomicU8>,
}

impl std::ops::Deref for DispatchAttempt {
    type Target = ActiveDispatch;

    fn deref(&self) -> &Self::Target {
        &self.dispatch
    }
}

const ATTEMPT_ACTIVE: u8 = 0;
const ATTEMPT_CANCEL_REQUESTED: u8 = 1;
const ATTEMPT_FINALIZING: u8 = 2;
const ATTEMPT_SETTLED: u8 = 3;
const ATTEMPT_PUBLICATION_FAILED: u8 = 4;

impl DispatchAttempt {
    pub fn new(dispatch: ActiveDispatch) -> Self {
        let state = if dispatch.status.is_terminal() {
            ATTEMPT_SETTLED
        } else {
            ATTEMPT_ACTIVE
        };
        Self {
            dispatch,
            arbitration: Arc::new(std::sync::atomic::AtomicU8::new(state)),
        }
    }

    pub(crate) fn same_attempt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.arbitration, &other.arbitration)
    }
    pub(crate) fn cancellation_requested(&self) -> bool {
        self.arbitration.load(std::sync::atomic::Ordering::Acquire) == ATTEMPT_CANCEL_REQUESTED
    }

    fn claim_active(&self, outcome: u8) -> bool {
        self.arbitration
            .compare_exchange(
                ATTEMPT_ACTIVE,
                outcome,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }
}

/// A non-waiting publication claim. Dropping an unfinished successful claim
/// seals the attempt against cancellation: effects may already have published.
/// Recovery uses the existing durable dispatch on restart, not an in-process
/// retry that could mistake partial publication for a rollback.
pub(crate) struct DispatchFinalization {
    attempt: Arc<DispatchAttempt>,
    pub(crate) cancelled: bool,
    finished: bool,
}

impl DispatchFinalization {
    pub(crate) fn finish(mut self) {
        self.attempt
            .arbitration
            .store(ATTEMPT_SETTLED, std::sync::atomic::Ordering::Release);
        self.finished = true;
    }
}

impl Drop for DispatchFinalization {
    fn drop(&mut self) {
        if !self.finished {
            self.attempt.arbitration.store(
                if self.cancelled {
                    ATTEMPT_CANCEL_REQUESTED
                } else {
                    ATTEMPT_PUBLICATION_FAILED
                },
                std::sync::atomic::Ordering::Release,
            );
        }
    }
}

#[derive(Default)]
struct DispatchState {
    dispatches: HashMap<String, Arc<DispatchAttempt>>,
    active_dispatches: HashMap<String, Arc<DispatchAttempt>>,
    wflow_to_dispatch: HashMap<String, String>,
    cancelled_dispatches: HashSet<String>,
    wflow_partition_frontier: HashMap<String, u64>,
    dispatch_head_index: HashMap<automerge::ChangeHash, String>,
}

pub struct DispatchRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,

    repo_sql: SqlCtx,
    state: tokio::sync::Mutex<DispatchState>,
    transition_mutex: tokio::sync::Mutex<()>,
    cancel_token: CancellationToken,
    local_actor_id: ActorId,
}

struct CancellationWrite {
    arbitration: Arc<std::sync::atomic::AtomicU8>,
    committed: bool,
}

impl Drop for CancellationWrite {
    fn drop(&mut self) {
        if !self.committed {
            self.arbitration
                .store(ATTEMPT_ACTIVE, std::sync::atomic::Ordering::Release);
        }
    }
}

impl crate::repos::Repo for DispatchRepo {
    type Event = DispatchEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl DispatchRepo {
    fn local_origin(&self) -> crate::event_origin::EventOrigin {
        crate::event_origin::EventOrigin::Local {
            actor_id: self.local_actor_id.to_string(),
        }
    }

    /// Claim this exact attempt without waiting over target publication.
    /// The short transition lock fences an accepted cancellation's durable mark.
    pub(crate) async fn claim_finalization(
        &self,
        id: &str,
        attempt: &Arc<DispatchAttempt>,
    ) -> Res<Option<DispatchFinalization>> {
        use std::sync::atomic::Ordering;
        let state = self.state.lock().await;
        let Some(current) = state.dispatches.get(id) else {
            return Ok(None);
        };
        if !current.same_attempt(attempt) || current.status.is_terminal() {
            return Ok(None);
        }
        if attempt.claim_active(ATTEMPT_FINALIZING) {
            return Ok(Some(DispatchFinalization {
                attempt: Arc::clone(attempt),
                cancelled: false,
                finished: false,
            }));
        }
        drop(state);
        // Cancellation won the CAS. Wait only for its short mark transaction,
        // never for publication, and revalidate replacement after that fence.
        let _transition_guard = self.transition_mutex.lock().await;
        let state = self.state.lock().await;
        let Some(current) = state.dispatches.get(id) else {
            return Ok(None);
        };
        if !current.same_attempt(attempt) || current.status.is_terminal() {
            return Ok(None);
        }
        let previous = attempt.arbitration.load(Ordering::Acquire);
        match previous {
            ATTEMPT_ACTIVE | ATTEMPT_CANCEL_REQUESTED => {
                if attempt
                    .arbitration
                    .compare_exchange(
                        previous,
                        ATTEMPT_FINALIZING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    return Ok(None);
                }
                Ok(Some(DispatchFinalization {
                    attempt: Arc::clone(attempt),
                    cancelled: previous == ATTEMPT_CANCEL_REQUESTED,
                    finished: false,
                }))
            }
            ATTEMPT_PUBLICATION_FAILED => {
                eyre::bail!("dispatch {id} publication failed; restart required")
            }
            _ => Ok(None),
        }
    }

    /// Persist cancellation for this exact attempt before acknowledging it.
    /// Finalization that has already claimed publication is immediately too late.
    pub(crate) async fn cancel(&self, id: &str, attempt: &Arc<DispatchAttempt>) -> Res<bool> {
        use std::sync::atomic::Ordering;
        // Fast refusal never waits for publication or unrelated persistence.
        if attempt.arbitration.load(Ordering::Acquire) >= ATTEMPT_FINALIZING {
            return Ok(false);
        }
        let id = id.to_string();
        let _transition_guard = self.transition_mutex.lock().await;
        let state = self.state.lock().await;
        let Some(dispatch) = state.dispatches.get(&id) else {
            eyre::bail!("dispatch not found under {id}");
        };
        if !dispatch.same_attempt(attempt) || dispatch.status.is_terminal() {
            return Ok(false);
        }
        if state.cancelled_dispatches.contains(&id) {
            return Ok(false);
        }
        drop(state);
        if !attempt.claim_active(ATTEMPT_CANCEL_REQUESTED) {
            return Ok(false);
        }
        // An interrupted/failed mark write releases the unacknowledged claim.
        // The transition mutex prevents a finalizer consuming it before commit.
        let mut cancellation = CancellationWrite {
            arbitration: Arc::clone(&attempt.arbitration),
            committed: false,
        };

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO dispatch_cancelled_marks(dispatch_id, created_at)\n             VALUES (?1, unixepoch())",
        )
        .bind(&id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        tx.commit().await?;
        cancellation.committed = true;

        if inserted {
            self.state.lock().await.cancelled_dispatches.insert(id);
        }
        Ok(inserted)
    }

    pub async fn load(
        _big_repo: SharedBigRepo,
        _app_doc_id: DocumentId,
        local_user_path: UserPathBuf,
        repo_sql: SqlCtx,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        init_schema(&repo_sql).await?;

        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "dispatch-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let state = load_state(&repo_sql).await?;
        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();

        let repo = Arc::new(Self {
            registry,
            repo_sql,
            state: tokio::sync::Mutex::new(state),
            transition_mutex: tokio::sync::Mutex::new(()),
            cancel_token: cancel_token.clone(),
            local_actor_id,
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: None,
            },
        ))
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<DispatchEvent>> {
        let state = self.state.lock().await;
        let from_set: HashSet<automerge::ChangeHash> = from.0.iter().copied().collect();
        let to_heads = to.unwrap_or_else(|| dispatch_heads_for_dispatches(state.dispatches.iter()));
        let to_set: HashSet<automerge::ChangeHash> = to_heads.0.iter().copied().collect();
        let mut added_ids = Vec::new();
        for hash in to_set.difference(&from_set) {
            let Some(id) = state.dispatch_head_index.get(hash) else {
                warn!(head = ?hash, "dispatch head missing from index for add diff");
                continue;
            };
            added_ids.push(id.clone());
        }
        added_ids.sort();
        let mut removed_ids = Vec::new();
        for hash in from_set.difference(&to_set) {
            let Some(id) = state.dispatch_head_index.get(hash) else {
                warn!(head = ?hash, "dispatch head missing from index for remove diff");
                continue;
            };
            removed_ids.push(id.clone());
        }
        removed_ids.sort();
        let mut events = Vec::with_capacity(added_ids.len() + removed_ids.len());
        for id in added_ids {
            events.push(DispatchEvent::DispatchAdded {
                id,
                heads: to_heads.clone(),
                origin: self.local_origin(),
            });
        }
        for id in removed_ids {
            events.push(DispatchEvent::DispatchDeleted {
                id,
                heads: to_heads.clone(),
                origin: self.local_origin(),
            });
        }
        Ok(events)
    }

    pub async fn events_for_init(&self) -> Res<Vec<DispatchEvent>> {
        let state = self.state.lock().await;
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        let mut events = Vec::with_capacity(state.dispatches.len());
        for id in state.dispatches.keys() {
            events.push(DispatchEvent::DispatchAdded {
                id: id.clone(),
                heads: heads.clone(),
                origin: self.local_origin(),
            });
        }
        Ok(events)
    }

    pub async fn get_dispatch_heads(&self) -> ChangeHashSet {
        let state = self.state.lock().await;
        dispatch_heads_for_dispatches(state.dispatches.iter())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<DispatchAttempt>> {
        self.state.lock().await.dispatches.get(id).map(Arc::clone)
    }

    pub async fn get_active(&self, id: &str) -> Option<Arc<DispatchAttempt>> {
        self.state
            .lock()
            .await
            .active_dispatches
            .get(id)
            .map(Arc::clone)
    }

    pub async fn get_any(&self, id: &str) -> Option<Arc<DispatchAttempt>> {
        self.get(id).await
    }

    pub async fn get_wflow_part_frontier(&self, wflow_part_id: &str) -> Option<u64> {
        self.state
            .lock()
            .await
            .wflow_partition_frontier
            .get(wflow_part_id)
            .copied()
    }

    pub async fn set_wflow_part_frontier(&self, wflow_part_id: String, frontier: u64) -> Res<()> {
        sqlx::query(
            "INSERT INTO wflow_partition_frontier(wflow_partition_id, frontier, updated_at)\n             VALUES (?1, ?2, unixepoch())\n             ON CONFLICT(wflow_partition_id) DO UPDATE SET\n                 frontier = excluded.frontier,\n                 updated_at = excluded.updated_at",
        )
        .bind(&wflow_part_id)
        .bind(i64::try_from(frontier).expect("frontier exceeds sqlite INTEGER range"))
        .execute(&self.repo_sql.write_pool)
        .await?;

        let mut state = self.state.lock().await;
        state
            .wflow_partition_frontier
            .insert(wflow_part_id, frontier);
        Ok(())
    }

    pub async fn get_by_wflow_job(&self, job_id: &str) -> Option<Arc<DispatchAttempt>> {
        let state = self.state.lock().await;
        let dispatch_id = state.wflow_to_dispatch.get(job_id)?;
        let found = state.active_dispatches.get(dispatch_id).map(Arc::clone);
        if let Some(dispatch) = found.as_ref() {
            let ActiveDispatchArgs::FacetRoutine(args) = &dispatch.args;
            debug!(
                ?job_id,
                ?dispatch_id,
                arg_fingerprint = %facet_routine_args_fingerprint(args),
                doc_id = ?args.doc_id,
                branch_path = %args.branch_path,
                staging_branch_path = %args.staging_branch_path,
                heads = ?am_utils_rs::serialize_commit_heads(args.heads.as_ref()),
                "dispatch_repo get_by_wflow_job"
            );
        }
        found
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn get_any_by_wflow_key(
        &self,
        wflow_key: &str,
    ) -> Option<(String, Arc<DispatchAttempt>)> {
        let state = self.state.lock().await;

        if let Some((dispatch_id, dispatch)) = state
            .active_dispatches
            .iter()
            .find(|(_, dispatch)| dispatch.execution.wflow().key == wflow_key)
        {
            return Some((dispatch_id.clone(), Arc::clone(dispatch)));
        }

        state.dispatches.iter().find_map(|(dispatch_id, dispatch)| {
            (dispatch.execution.wflow().key == wflow_key)
                .then(|| (dispatch_id.clone(), Arc::clone(dispatch)))
        })
    }

    pub async fn add(&self, id: String, dispatch: Arc<DispatchAttempt>) -> Res<()> {
        self.add_with_task_attempt(id, dispatch, None).await?;
        Ok(())
    }

    pub(crate) async fn persist_task_attempt(
        &self,
        attempt: crate::tasks::driver::PreparedAttempt,
        dispatch: Arc<DispatchAttempt>,
    ) -> Res<crate::tasks::driver::PreparedAttempt> {
        self.add_with_task_attempt(attempt.dispatch_id.clone(), dispatch, Some(attempt))
            .await?
            .ok_or_eyre("prepared task admission did not retain its mapping")
    }

    async fn add_with_task_attempt(
        &self,
        id: String,
        dispatch: Arc<DispatchAttempt>,
        attempt: Option<crate::tasks::driver::PreparedAttempt>,
    ) -> Res<Option<crate::tasks::driver::PreparedAttempt>> {
        debug!(?id, "adding dispatch to repo");
        let _transition_guard = self.transition_mutex.lock().await;
        if let Some(candidate) = &attempt
            && let Some(existing) = self.task_attempt(&candidate.request.key).await?
        {
            eyre::ensure!(
                existing.capture_digest == candidate.capture_digest
                    && serde_json::to_vec(&existing.request)?
                        == serde_json::to_vec(&candidate.request)?,
                "pool attempt identity reused with different captured execution"
            );
            return Ok(Some(existing));
        }
        if self.state.lock().await.dispatches.contains_key(&id) {
            eyre::bail!("dispatch already exists: {id}");
        }
        let ActiveDispatchArgs::FacetRoutine(_args) = &dispatch.args;

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, &id, &dispatch).await?;
        clear_cancelled_mark_tx(&mut tx, &id).await?;
        if let Some(attempt) = &attempt {
            sqlx::query(
                "INSERT INTO dispatch_task_attempts (key_json, pool_json, dispatch_id, payload_json)
                 VALUES (?, ?, ?, ?)",
            )
            .bind(serde_json::to_string(&attempt.request.key)?)
            .bind(serde_json::to_string(&attempt.request.key.pool)?)
            .bind(&id)
            .bind(serde_json::to_string(attempt)?)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        let mut state = self.state.lock().await;
        state.cancelled_dispatches.remove(&id);
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(&id, &dispatch))
            .or_insert_with(|| id.clone());

        if let Some(old) = state.dispatches.insert(id.clone(), Arc::clone(&dispatch))
            && let ActiveDispatchDeets::Wflow {
                wflow_job_id: Some(job),
                ..
            } = &old.deets
        {
            state.wflow_to_dispatch.remove(job);
        }

        match dispatch.status {
            DispatchStatus::Active => {
                state
                    .active_dispatches
                    .insert(id.clone(), Arc::clone(&dispatch));
            }
            _ => {
                state.active_dispatches.remove(&id);
            }
        }

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &dispatch.deets
        {
            state.wflow_to_dispatch.insert(job.clone(), id.clone());
        }

        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        drop(state);

        self.registry.notify([DispatchEvent::DispatchAdded {
            id,
            heads,
            origin: self.local_origin(),
        }]);
        Ok(attempt)
    }

    pub(crate) async fn task_attempt(
        &self,
        key: &crate::tasks::driver::AttemptKey,
    ) -> Res<Option<crate::tasks::driver::PreparedAttempt>> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload_json FROM dispatch_task_attempts WHERE key_json = ?",
        )
        .bind(serde_json::to_string(key)?)
        .fetch_optional(&self.repo_sql.read_pool)
        .await?;
        payload
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    pub(crate) async fn task_attempts(
        &self,
        pool: &crate::tasks::TaskPoolId,
    ) -> Res<Vec<crate::tasks::driver::PreparedAttempt>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT payload_json FROM dispatch_task_attempts WHERE pool_json = ? ORDER BY key_json",
        )
        .bind(serde_json::to_string(pool)?)
        .fetch_all(&self.repo_sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn task_attempt_for_dispatch(
        &self,
        dispatch_id: &str,
    ) -> Res<Option<crate::tasks::driver::PreparedAttempt>> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload_json FROM dispatch_task_attempts WHERE dispatch_id = ?",
        )
        .bind(dispatch_id)
        .fetch_optional(&self.repo_sql.read_pool)
        .await?;
        payload
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    pub(crate) async fn task_local_failure(
        &self,
        key: &crate::tasks::driver::AttemptKey,
    ) -> Res<Option<String>> {
        let row: Option<Option<String>> = sqlx::query_scalar(
            "SELECT local_failure FROM dispatch_task_attempts WHERE key_json = ?",
        )
        .bind(serde_json::to_string(key)?)
        .fetch_optional(&self.repo_sql.read_pool)
        .await?;
        Ok(row.flatten())
    }

    pub(crate) async fn fail_task_locally(
        &self,
        key: &crate::tasks::driver::AttemptKey,
        reason: &str,
    ) -> Res<()> {
        let changed = sqlx::query(
            "UPDATE dispatch_task_attempts SET local_failure = COALESCE(local_failure, ?) WHERE key_json = ?",
        ).bind(reason).bind(serde_json::to_string(key)?).execute(&self.repo_sql.write_pool).await?;
        eyre::ensure!(
            changed.rows_affected() == 1,
            "local failure names no durable task attempt"
        );
        Ok(())
    }

    pub(crate) async fn task_receipt(
        &self,
        task_id: &crate::tasks::PoolTaskId,
        declaration_digest: [u8; 32],
    ) -> Res<Option<crate::tasks::driver::DomainReceipt>> {
        let row: Option<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT payload_json, declaration_digest FROM dispatch_task_receipts WHERE task_json = ?",
        )
        .bind(serde_json::to_string(task_id)?)
        .fetch_optional(&self.repo_sql.read_pool)
        .await?;
        row.map(|(payload, digest)| {
            eyre::ensure!(
                digest == declaration_digest,
                "task settlement belongs to a different declaration"
            );
            serde_json::from_str(&payload).map_err(Into::into)
        })
        .transpose()
    }

    pub(crate) async fn incorporate_task_receipt(
        &self,
        receipt: crate::tasks::driver::DomainReceipt,
        declaration_digest: [u8; 32],
    ) -> Res<crate::tasks::driver::DomainReceipt> {
        let _transition = self.transition_mutex.lock().await;
        if let Some(existing) = self
            .task_receipt(&receipt.task_id, declaration_digest)
            .await?
            && (matches!(
                existing.summary,
                crate::tasks::TerminalSummary::Succeeded { .. }
            ) || existing.summary == receipt.summary)
        {
            return Ok(existing);
        }
        sqlx::query(
            "INSERT INTO dispatch_task_receipts (task_json, payload_json, declaration_digest) VALUES (?, ?, ?)
             ON CONFLICT(task_json) DO UPDATE SET payload_json = excluded.payload_json",
        )
            .bind(serde_json::to_string(&receipt.task_id)?)
            .bind(serde_json::to_string(&receipt)?)
            .bind(declaration_digest.as_slice())
            .execute(&self.repo_sql.write_pool)
            .await?;
        Ok(receipt)
    }

    pub async fn complete(
        &self,
        id: String,
        status: DispatchStatus,
        expected: &DispatchAttempt,
    ) -> Res<Option<Arc<DispatchAttempt>>> {
        assert!(matches!(
            status,
            DispatchStatus::Succeeded | DispatchStatus::Failed | DispatchStatus::Cancelled
        ));
        let _transition_guard = self.transition_mutex.lock().await;

        let old = self.state.lock().await.dispatches.get(&id).map(Arc::clone);
        let Some(old_dispatch) = old.clone() else {
            return Ok(None);
        };
        if !old_dispatch.same_attempt(expected) || old_dispatch.status.is_terminal() {
            return Ok(None);
        }

        let mut next = (*old_dispatch).clone();
        next.dispatch.status = status;
        let next = Arc::new(next);

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, &id, &next).await?;
        clear_cancelled_mark_tx(&mut tx, &id).await?;
        tx.commit().await?;
        next.arbitration
            .store(ATTEMPT_SETTLED, std::sync::atomic::Ordering::Release);

        let mut state = self.state.lock().await;
        state.cancelled_dispatches.remove(&id);

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &old_dispatch.deets
        {
            state.wflow_to_dispatch.remove(job);
        }

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &next.deets
            && next.status == DispatchStatus::Active
        {
            state.wflow_to_dispatch.insert(job.clone(), id.clone());
        }

        let next_head = dispatch_head_for_dispatch(&id, &next);
        state.active_dispatches.remove(&id);
        state.dispatches.insert(id.clone(), next);
        state
            .dispatch_head_index
            .entry(next_head)
            .or_insert_with(|| id.clone());
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        drop(state);

        self.registry.notify([DispatchEvent::DispatchUpdated {
            id,
            heads,
            origin: self.local_origin(),
        }]);

        Ok(old)
    }

    pub async fn list_unsettled(&self) -> Vec<(String, Arc<DispatchAttempt>)> {
        self.state
            .lock()
            .await
            .dispatches
            .iter()
            .filter(|(_, dispatch)| !dispatch.status.is_terminal())
            .map(|(id, dispatch)| (id.clone(), Arc::clone(dispatch)))
            .collect()
    }

    pub async fn list_waiting_on(
        &self,
        dependency_dispatch_id: &str,
    ) -> Vec<(String, Arc<DispatchAttempt>)> {
        let dependency_dispatch_id = dependency_dispatch_id.to_string();
        self.state
            .lock()
            .await
            .dispatches
            .iter()
            .filter_map(|(id, dispatch)| {
                if dispatch.status == DispatchStatus::Waiting
                    && dispatch
                        .waiting_on_dispatch_ids
                        .iter()
                        .any(|dep| dep == &dependency_dispatch_id)
                {
                    Some((id.clone(), Arc::clone(dispatch)))
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn remove_waiting_dependency(
        &self,
        dispatch_id: &str,
        dependency_dispatch_id: &str,
    ) -> Res<Option<Arc<DispatchAttempt>>> {
        let _transition_guard = self.transition_mutex.lock().await;
        let cur = self
            .state
            .lock()
            .await
            .dispatches
            .get(dispatch_id)
            .map(Arc::clone)
            .ok_or_else(|| eyre::eyre!("dispatch not found under {dispatch_id}"))?;
        if cur.status != DispatchStatus::Waiting {
            return Ok(None);
        }

        let mut updated = (*cur).clone();
        updated
            .dispatch
            .waiting_on_dispatch_ids
            .retain(|dep| dep != dependency_dispatch_id);
        let ready = updated.waiting_on_dispatch_ids.is_empty();
        let updated = Arc::new(updated);

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, dispatch_id, &updated).await?;
        tx.commit().await?;

        let mut state = self.state.lock().await;
        state
            .dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(dispatch_id, &updated))
            .or_insert_with(|| dispatch_id.to_string());
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        drop(state);

        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id.to_string(),
            heads,
            origin: self.local_origin(),
        }]);

        if ready { Ok(Some(updated)) } else { Ok(None) }
    }

    /// Activate this exact ready attempt. Cancellation, terminal settlement, or
    /// replacement returns `None`; missing rows and invalid dependency/state
    /// transitions remain errors. Metadata refresh preserves the arbitration cell.
    pub async fn activate_waiting(
        &self,
        dispatch_id: &str,
        expected: &DispatchAttempt,
        deets: ActiveDispatchDeets,
    ) -> Res<Option<Arc<DispatchAttempt>>> {
        let _transition_guard = self.transition_mutex.lock().await;
        let cur = self
            .state
            .lock()
            .await
            .dispatches
            .get(dispatch_id)
            .map(Arc::clone)
            .ok_or_else(|| eyre::eyre!("dispatch not found under {dispatch_id}"))?;
        // A ready snapshot may outlive cancellation or exact-attempt replacement.
        // Those contenders own this transition; refusing activation is ordinary,
        // not a malformed dependency graph or lifecycle invariant violation.
        if !cur.same_attempt(expected)
            || cur.status.is_terminal()
            || cur.arbitration.load(std::sync::atomic::Ordering::Acquire)
                == ATTEMPT_CANCEL_REQUESTED
        {
            return Ok(None);
        }
        if cur.status != DispatchStatus::Waiting {
            eyre::bail!("dispatch is not waiting: {dispatch_id}");
        }
        if !cur.waiting_on_dispatch_ids.is_empty() {
            eyre::bail!("dispatch still has unresolved dependencies: {dispatch_id}");
        }
        if cur.arbitration.load(std::sync::atomic::Ordering::Acquire) != ATTEMPT_ACTIVE {
            eyre::bail!("cannot activate claimed dispatch: {dispatch_id}");
        }

        let mut updated = (*cur).clone();
        updated.dispatch.status = DispatchStatus::Active;
        updated.dispatch.deets = deets;
        let updated = Arc::new(updated);

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, dispatch_id, &updated).await?;
        tx.commit().await?;

        let mut state = self.state.lock().await;
        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &cur.deets
        {
            state.wflow_to_dispatch.remove(job);
        }
        state
            .dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));
        state
            .active_dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));
        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &updated.deets
        {
            state
                .wflow_to_dispatch
                .insert(job.clone(), dispatch_id.to_string());
        }
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(dispatch_id, &updated))
            .or_insert_with(|| dispatch_id.to_string());
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        drop(state);

        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id.to_string(),
            heads,
            origin: self.local_origin(),
        }]);

        Ok(Some(updated))
    }

    pub async fn update_active_deets(
        &self,
        dispatch_id: &str,
        expected: &DispatchAttempt,
        deets: ActiveDispatchDeets,
    ) -> Res<Option<Arc<DispatchAttempt>>> {
        let _transition_guard = self.transition_mutex.lock().await;
        // Keep the snapshot stable during this short persistence transition.
        // An active finalizer claims under the state lock, so replacement cannot
        // commit between its identity validation and publication CAS.
        let mut state = self.state.lock().await;
        let cur = state
            .dispatches
            .get(dispatch_id)
            .map(Arc::clone)
            .ok_or_else(|| eyre::eyre!("dispatch not found under {dispatch_id}"))?;
        if !cur.same_attempt(expected) || cur.status.is_terminal() {
            return Ok(None);
        }
        if cur.status != DispatchStatus::Active {
            eyre::bail!("dispatch is not active: {dispatch_id}");
        }

        let mut updated = (*cur).clone();
        let ActiveDispatchDeets::Wflow {
            wflow_job_id: old_job,
            ..
        } = &cur.deets;
        let ActiveDispatchDeets::Wflow {
            wflow_job_id: new_job,
            ..
        } = &deets;
        let replaced = old_job != new_job;
        if replaced {
            if cur.arbitration.load(std::sync::atomic::Ordering::Acquire) == ATTEMPT_FINALIZING {
                eyre::bail!("cannot replace finalizing dispatch: {dispatch_id}");
            }
            updated.arbitration = Arc::new(std::sync::atomic::AtomicU8::new(ATTEMPT_ACTIVE));
        }
        updated.dispatch.deets = deets;
        let updated = Arc::new(updated);

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, dispatch_id, &updated).await?;
        if replaced {
            clear_cancelled_mark_tx(&mut tx, dispatch_id).await?;
        }
        tx.commit().await?;
        if replaced {
            cur.arbitration
                .store(ATTEMPT_SETTLED, std::sync::atomic::Ordering::Release);
        }

        if replaced {
            state.cancelled_dispatches.remove(dispatch_id);
        }

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &cur.deets
        {
            state.wflow_to_dispatch.remove(job);
        }

        state
            .dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));
        state
            .active_dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &updated.deets
        {
            state
                .wflow_to_dispatch
                .insert(job.clone(), dispatch_id.to_string());
        }
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(dispatch_id, &updated))
            .or_insert_with(|| dispatch_id.to_string());
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());

        drop(state);

        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id.to_string(),
            heads,
            origin: self.local_origin(),
        }]);

        Ok(Some(updated))
    }

    pub async fn set_waiting_failed(&self, dispatch_id: &str) -> Res<()> {
        let _transition_guard = self.transition_mutex.lock().await;
        let Some(cur) = self
            .state
            .lock()
            .await
            .dispatches
            .get(dispatch_id)
            .map(Arc::clone)
        else {
            return Ok(());
        };

        let mut updated = (*cur).clone();
        updated.dispatch.status = DispatchStatus::Failed;
        let updated = Arc::new(updated);

        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        persist_dispatch_tx(&mut tx, dispatch_id, &updated).await?;
        clear_cancelled_mark_tx(&mut tx, dispatch_id).await?;
        tx.commit().await?;
        updated
            .arbitration
            .store(ATTEMPT_SETTLED, std::sync::atomic::Ordering::Release);

        let mut state = self.state.lock().await;
        state.cancelled_dispatches.remove(dispatch_id);
        state.active_dispatches.remove(dispatch_id);

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &cur.deets
        {
            state.wflow_to_dispatch.remove(job);
        }

        state
            .dispatches
            .insert(dispatch_id.to_string(), Arc::clone(&updated));
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(dispatch_id, &updated))
            .or_insert_with(|| dispatch_id.to_string());
        let heads = dispatch_heads_for_dispatches(state.dispatches.iter());
        drop(state);

        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id.to_string(),
            heads,
            origin: self.local_origin(),
        }]);

        Ok(())
    }
}

fn dispatch_head_for_dispatch(id: &str, dispatch: &ActiveDispatch) -> automerge::ChangeHash {
    use sha2::Digest;
    let payload = serde_json::to_vec(dispatch).expect(ERROR_JSON);
    let mut hasher = sha2::Sha256::new();
    hasher.update(id.as_bytes());
    hasher.update([0_u8]);
    hasher.update(payload);
    let digest = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&digest[..]);
    automerge::ChangeHash(bytes)
}

fn dispatch_heads_for_dispatches<'a>(
    dispatches: impl Iterator<Item = (&'a String, &'a Arc<DispatchAttempt>)>,
) -> ChangeHashSet {
    let mut items = dispatches.collect::<Vec<_>>();
    items.sort_unstable_by_key(|(lhs_id, _)| *lhs_id);
    let mut heads = Vec::with_capacity(items.len());
    for (id, dispatch) in items {
        heads.push(dispatch_head_for_dispatch(id, dispatch));
    }
    ChangeHashSet(Arc::from(heads))
}

async fn init_schema(repo_sql: &SqlCtx) -> Res<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatches (
            id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            wflow_job_id TEXT,
            updated_at INTEGER NOT NULL
        ) STRICT",
    )
    .execute(&repo_sql.write_pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_dispatches_wflow_job_id
         ON dispatches(wflow_job_id)",
    )
    .execute(&repo_sql.write_pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatch_cancelled_marks (
            dispatch_id TEXT PRIMARY KEY NOT NULL,
            created_at INTEGER NOT NULL
        ) STRICT",
    )
    .execute(&repo_sql.write_pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS wflow_partition_frontier (
            wflow_partition_id TEXT PRIMARY KEY NOT NULL,
            frontier INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        ) STRICT",
    )
    .execute(&repo_sql.write_pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatch_task_attempts (
             key_json TEXT PRIMARY KEY NOT NULL
           , pool_json TEXT NOT NULL
           , dispatch_id TEXT NOT NULL UNIQUE REFERENCES dispatches(id)
           , payload_json TEXT NOT NULL
           , local_failure TEXT
         ) STRICT",
    )
    .execute(&repo_sql.write_pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_dispatch_task_attempts_pool
         ON dispatch_task_attempts(pool_json, key_json)",
    )
    .execute(&repo_sql.write_pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatch_task_receipts (
             task_json TEXT PRIMARY KEY NOT NULL
           , payload_json TEXT NOT NULL
           , declaration_digest BLOB NOT NULL
         ) STRICT",
    )
    .execute(&repo_sql.write_pool)
    .await?;

    Ok(())
}

async fn load_state(repo_sql: &SqlCtx) -> Res<DispatchState> {
    let mut state = DispatchState::default();

    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, payload_json FROM dispatches ORDER BY id")
            .fetch_all(&repo_sql.write_pool)
            .await?;

    for (id, payload_json) in rows {
        let dispatch: ActiveDispatch = serde_json::from_str(&payload_json).wrap_err_with(|| {
            format!("dispatch {id} has unsupported or invalid captured execution state; legacy dispatches cannot be resumed safely without an explicit migration")
        })?;
        let dispatch = Arc::new(DispatchAttempt::new(dispatch));
        state
            .dispatch_head_index
            .insert(dispatch_head_for_dispatch(&id, &dispatch), id.clone());

        if dispatch.status == DispatchStatus::Active {
            state
                .active_dispatches
                .insert(id.clone(), Arc::clone(&dispatch));
        }

        if let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job),
            ..
        } = &dispatch.deets
        {
            state.wflow_to_dispatch.insert(job.clone(), id.clone());
        }

        state.dispatches.insert(id, dispatch);
    }

    let cancelled_ids: Vec<String> =
        sqlx::query_scalar("SELECT dispatch_id FROM dispatch_cancelled_marks")
            .fetch_all(&repo_sql.write_pool)
            .await?;
    state.cancelled_dispatches = cancelled_ids.into_iter().collect();
    for id in &state.cancelled_dispatches {
        if let Some(dispatch) = state.dispatches.get(id)
            && !dispatch.status.is_terminal()
        {
            dispatch.arbitration.store(
                ATTEMPT_CANCEL_REQUESTED,
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    let frontier_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT wflow_partition_id, frontier FROM wflow_partition_frontier")
            .fetch_all(&repo_sql.write_pool)
            .await?;
    for (part_id, frontier) in frontier_rows {
        let frontier = match u64::try_from(frontier) {
            Ok(value) => value,
            Err(_) => {
                eyre::bail!(
                    "invalid negative frontier row in sqlite: part_id={part_id} frontier={frontier}"
                );
            }
        };
        state.wflow_partition_frontier.insert(part_id, frontier);
    }

    Ok(state)
}

async fn persist_dispatch_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    dispatch: &ActiveDispatch,
) -> Res<()> {
    let ActiveDispatchArgs::FacetRoutine(args) = &dispatch.args;
    debug!(
        dispatch_id = %id,
        arg_fingerprint = %facet_routine_args_fingerprint(args),
        doc_id = ?args.doc_id,
        branch_path = %args.branch_path,
        staging_branch_path = %args.staging_branch_path,
        heads = ?am_utils_rs::serialize_commit_heads(args.heads.as_ref()),
        "dispatch_repo persist_dispatch"
    );
    let payload_json = serde_json::to_string(dispatch).expect(ERROR_JSON);
    let status = format!("{:?}", dispatch.status);
    let wflow_job_id = match &dispatch.deets {
        ActiveDispatchDeets::Wflow { wflow_job_id, .. } => wflow_job_id.clone(),
    };

    sqlx::query(
        "INSERT INTO dispatches(id, status, payload_json, wflow_job_id, updated_at)
         VALUES (?1, ?2, ?3, ?4, unixepoch())
         ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            payload_json = excluded.payload_json,
            wflow_job_id = excluded.wflow_job_id,
            updated_at = excluded.updated_at",
    )
    .bind(id)
    .bind(status)
    .bind(payload_json)
    .bind(wflow_job_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn clear_cancelled_mark_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    dispatch_id: &str,
) -> Res<()> {
    sqlx::query("DELETE FROM dispatch_cancelled_marks WHERE dispatch_id = ?1")
        .bind(dispatch_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use automerge::transaction::Transactable;

    use super::*;
    use crate::repos::{Repo, SubscribeOpts};

    async fn setup_repo_with_sql(
        repo_sql: SqlCtx,
    ) -> Res<(Arc<DispatchRepo>, daybook_types::doc::UserPathBuf)> {
        let local_user_path = daybook_types::doc::UserPathBuf::from("/test-user/test-device");
        let (big_repo, _part_store, _acx_stop) = crate::test_support::boot_repo().await?;
        let mut app_doc = automerge::Automerge::new();
        {
            let mut tx = app_doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
        }
        let app_doc = big_repo.create_doc(app_doc).await?;
        let app_doc_id = app_doc.document_id();
        let (repo, _stop) =
            DispatchRepo::load(big_repo, app_doc_id, local_user_path.clone(), repo_sql).await?;
        Ok((repo, local_user_path))
    }

    fn active_dispatch(job_id: &str) -> Arc<DispatchAttempt> {
        Arc::new(DispatchAttempt::new(ActiveDispatch {
            execution: CapturedWflowExecution::fixture(),
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some("part-a".into()),
                entry_id: None,
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_job_id: Some(job_id.to_string()),
            },
            args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id: "doc-1".into(),
                branch_path: "main".into(),
                staging_branch_path: "/tmp/stage".into(),
                heads: ChangeHashSet(Vec::new().into()),
                invocation: RoutineInvocation::Command,
                primary_doc: DocFacetTokens {
                    doc_id: "doc-1".into(),
                    branch_path: "main".into(),
                    staging_branch_path: "/tmp/stage".into(),
                    heads: ChangeHashSet(Vec::new().into()),
                    facet_acl: vec![],
                },
                config_docs: vec![],
                local_state_acl: vec![],
                command_invoke_acl_snapshot: vec![],
                wflow_args_json: None,
            }),
            status: DispatchStatus::Active,
            waiting_on_dispatch_ids: vec![],
            on_success_hooks: vec![],
        }))
    }

    fn waiting_dispatch(job_id: &str, waits_on: &[&str]) -> Arc<DispatchAttempt> {
        Arc::new(DispatchAttempt::new(ActiveDispatch {
            execution: CapturedWflowExecution::fixture(),
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: None,
                entry_id: None,
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_job_id: Some(job_id.to_string()),
            },
            args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id: "doc-1".into(),
                branch_path: "main".into(),
                staging_branch_path: "/tmp/stage".into(),
                heads: ChangeHashSet(Vec::new().into()),
                invocation: RoutineInvocation::Command,
                primary_doc: DocFacetTokens {
                    doc_id: "doc-1".into(),
                    branch_path: "main".into(),
                    staging_branch_path: "/tmp/stage".into(),
                    heads: ChangeHashSet(Vec::new().into()),
                    facet_acl: vec![],
                },
                config_docs: vec![],
                local_state_acl: vec![],
                command_invoke_acl_snapshot: vec![],
                wflow_args_json: None,
            }),
            status: DispatchStatus::Waiting,
            waiting_on_dispatch_ids: waits_on.iter().map(|value| value.to_string()).collect(),
            on_success_hooks: vec![],
        }))
    }

    #[tokio::test]
    async fn sqlite_dispatch_lifecycle_and_event_parity() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
        let sub = repo.subscribe(SubscribeOpts::new(8));

        repo.add("disp-1".into(), active_dispatch("job-1")).await?;
        let event = sub
            .recv_async()
            .await
            .map_err(|err| eyre::eyre!("listener closed: {err:?}"))?;
        assert!(matches!(
            &*event,
            DispatchEvent::DispatchAdded { id, origin, .. }
            if id == "disp-1"
                && matches!(origin, crate::event_origin::EventOrigin::Local { .. })
        ));
        assert!(repo.get_active("disp-1").await.is_some());
        assert!(matches!(
            repo.get_any("disp-1")
                .await
                .as_ref()
                .map(|dispatch| &dispatch.deets),
            Some(ActiveDispatchDeets::Wflow { entry_id: None, .. })
        ));
        assert!(repo.get_by_wflow_job("job-1").await.is_some());

        let attempt = repo.get_any("disp-1").await.unwrap();
        assert!(repo.cancel("disp-1", &attempt).await?);
        assert!(!repo.cancel("disp-1", &attempt).await?);

        repo.complete("disp-1".into(), DispatchStatus::Cancelled, &attempt)
            .await?;
        let event = sub
            .recv_async()
            .await
            .map_err(|err| eyre::eyre!("listener closed: {err:?}"))?;
        assert!(matches!(
            &*event,
            DispatchEvent::DispatchUpdated { id, origin, .. }
            if id == "disp-1"
                && matches!(origin, crate::event_origin::EventOrigin::Local { .. })
        ));
        assert!(repo.get_active("disp-1").await.is_none());
        assert!(repo.get_by_wflow_job("job-1").await.is_none());
        assert!(matches!(
            repo.get_any("disp-1").await.as_ref().map(|d| &d.status),
            Some(DispatchStatus::Cancelled)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_waiting_dependency_flow() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;

        repo.add("wait-1".into(), waiting_dispatch("job-wait-1", &["dep-1"]))
            .await?;
        let waiting = repo.list_waiting_on("dep-1").await;
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].0, "wait-1");

        let ready = repo
            .remove_waiting_dependency("wait-1", "dep-1")
            .await?
            .ok_or_else(|| eyre::eyre!("waiting dispatch should become ready"))?;
        assert!(ready.waiting_on_dispatch_ids.is_empty());

        repo.activate_waiting(
            "wait-1",
            &ready,
            ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some("part-b".into()),
                entry_id: Some(9),
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_job_id: Some("job-wait-1".into()),
            },
        )
        .await?
        .ok_or_eyre("ready waiting dispatch should activate")?;
        assert!(repo.get_active("wait-1").await.is_some());
        assert!(repo.get_by_wflow_job("job-wait-1").await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn waiting_activation_refuses_cancelled_or_settled_ready_snapshots() -> Res<()> {
        for settled in [false, true] {
            let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
            let (repo, _) = setup_repo_with_sql(sql).await?;
            let blocked = waiting_dispatch("waiting-job", &["dependency"]);
            repo.add("waiting".into(), Arc::clone(&blocked)).await?;
            assert!(
                repo.activate_waiting("waiting", &blocked, blocked.deets.clone())
                    .await
                    .is_err()
            );
            let ready = repo
                .remove_waiting_dependency("waiting", "dependency")
                .await?
                .unwrap();
            assert!(repo.cancel("waiting", &ready).await?);
            if settled {
                repo.complete("waiting".into(), DispatchStatus::Cancelled, &ready)
                    .await?;
            }
            assert!(
                repo.activate_waiting("waiting", &ready, ready.deets.clone())
                    .await?
                    .is_none()
            );
            assert!(repo.get_active("waiting").await.is_none());
            assert!(repo.get_by_wflow_job("waiting-job").await.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn waiting_activation_fences_replacement_but_keeps_invalid_transitions_errors() -> Res<()>
    {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql).await?;
        let waiting = waiting_dispatch("old-job", &[]);
        repo.add("waiting".into(), Arc::clone(&waiting)).await?;
        assert!(
            repo.activate_waiting("missing", &waiting, waiting.deets.clone())
                .await
                .is_err()
        );
        let active = repo
            .activate_waiting("waiting", &waiting, waiting.deets.clone())
            .await?
            .unwrap();
        assert!(
            repo.activate_waiting("waiting", &active, active.deets.clone())
                .await
                .is_err()
        );
        // Waiting→Active is the same attempt, so the retained waiting snapshot
        // still participates in cancellation after the metadata transition.
        assert!(repo.cancel("waiting", &waiting).await?);
        let mut deets = active.deets.clone();
        let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &mut deets;
        *wflow_job_id = Some("replacement-job".into());
        let replacement = repo
            .update_active_deets("waiting", &active, deets)
            .await?
            .unwrap();
        assert!(
            repo.activate_waiting("waiting", &waiting, waiting.deets.clone())
                .await?
                .is_none()
        );
        assert!(!repo.cancel("waiting", &waiting).await?);
        assert!(repo.cancel("waiting", &replacement).await?);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_reload_persists_dispatch_rows_and_frontier() -> Res<()> {
        let temp = tempfile::tempdir()?;
        let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));

        let sql = crate::app::open_sql_ctx(sql_cfg.clone()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
        repo.add("disp-a".into(), active_dispatch("job-a")).await?;
        repo.set_wflow_part_frontier("part-1".into(), 44).await?;
        let attempt = repo.get_any("disp-a").await.unwrap();
        assert!(repo.cancel("disp-a", &attempt).await?);
        drop(repo);
        drop(sql);

        let sql = crate::app::open_sql_ctx(sql_cfg.clone()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
        let loaded = repo
            .get_any("disp-a")
            .await
            .ok_or_else(|| eyre::eyre!("missing persisted dispatch"))?;
        assert_eq!(loaded.status, DispatchStatus::Active);
        assert_eq!(
            serde_json::to_value(&loaded.execution).expect(ERROR_JSON),
            serde_json::to_value(&attempt.execution).expect(ERROR_JSON),
        );
        assert_eq!(repo.get_wflow_part_frontier("part-1").await, Some(44));
        assert!(!repo.cancel("disp-a", &loaded).await?);
        let claim = repo.claim_finalization("disp-a", &loaded).await?.unwrap();
        assert!(claim.cancelled);
        assert!(matches!(
            repo.events_for_init().await?.first(),
            Some(DispatchEvent::DispatchAdded { id, origin, .. })
                if id == "disp-a"
                    && matches!(origin, crate::event_origin::EventOrigin::Local { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reopening_unverifiable_capture_rejects_legacy_and_unknown_schema() -> Res<()> {
        for unknown_schema in [false, true] {
            let temp = tempfile::tempdir()?;
            let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
            let sql = crate::app::open_sql_ctx(sql_cfg.clone()).await?;
            init_schema(&sql).await?;
            let mut payload = serde_json::to_value(&**active_dispatch("retained-job"))?;
            if unknown_schema {
                payload["execution"] = serde_json::json!({"V2": {}});
            } else {
                payload.as_object_mut().unwrap().remove("execution");
            }
            sqlx::query(
                "INSERT INTO dispatches(id, status, payload_json, wflow_job_id, updated_at)
                 VALUES (?1, 'Active', ?2, 'retained-job', 0)",
            )
            .bind("unverifiable-dispatch")
            .bind(payload.to_string())
            .execute(&sql.write_pool)
            .await?;
            drop(sql);
            let sql = crate::app::open_sql_ctx(sql_cfg).await?;
            let error = match load_state(&sql).await {
                Ok(_) => panic!("unsupported captured execution must not reopen"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("dispatch unverifiable-dispatch has unsupported or invalid captured execution state"),
                "reopen must identify the unresumable dispatch: {error:?}"
            );
            assert!(error.to_string().contains("explicit migration"));
        }
        Ok(())
    }

    #[test]
    fn concurrent_snapshots_choose_one_terminal_owner() {
        let first = active_dispatch("job");
        let second = (*first).clone();
        let barrier = std::sync::Barrier::new(2);
        let (cancelled, finalizing) = std::thread::scope(|scope| {
            let cancel = scope.spawn(|| {
                barrier.wait();
                first.claim_active(ATTEMPT_CANCEL_REQUESTED)
            });
            let finalize = scope.spawn(|| {
                barrier.wait();
                second.claim_active(ATTEMPT_FINALIZING)
            });
            (cancel.join().unwrap(), finalize.join().unwrap())
        });
        assert_ne!(
            cancelled, finalizing,
            "exactly one CAS must own the outcome"
        );
        assert_eq!(
            first.arbitration.load(std::sync::atomic::Ordering::Acquire),
            if cancelled {
                ATTEMPT_CANCEL_REQUESTED
            } else {
                ATTEMPT_FINALIZING
            },
        );
    }

    #[tokio::test]
    async fn metadata_snapshots_share_claim_but_replacement_retires_old_attempt() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql).await?;
        let old = active_dispatch("old-job");
        repo.add("dispatch".into(), Arc::clone(&old)).await?;
        let mut deets = old.deets.clone();
        let ActiveDispatchDeets::Wflow { entry_id, .. } = &mut deets;
        *entry_id = Some(42);
        let metadata = repo
            .update_active_deets("dispatch", &old, deets)
            .await?
            .unwrap();
        let claim = repo.claim_finalization("dispatch", &old).await?.unwrap();
        assert!(!repo.cancel("dispatch", &metadata).await?);
        assert!(
            repo.claim_finalization("dispatch", &metadata)
                .await?
                .is_none()
        );
        drop(claim);
        assert!(
            repo.claim_finalization("dispatch", &metadata)
                .await
                .is_err()
        );

        // Explicit replacement is a new attempt, not an implicit publication retry.
        let mut deets = metadata.deets.clone();
        let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &mut deets;
        *wflow_job_id = Some("new-job".into());
        let new = repo
            .update_active_deets("dispatch", &metadata, deets)
            .await?
            .unwrap();
        assert!(!repo.cancel("dispatch", &old).await?);
        assert!(repo.claim_finalization("dispatch", &old).await?.is_none());
        assert!(
            repo.complete("dispatch".into(), DispatchStatus::Succeeded, &old)
                .await?
                .is_none()
        );
        assert_eq!(
            repo.get_any("dispatch").await.unwrap().status,
            DispatchStatus::Active
        );
        assert!(repo.cancel("dispatch", &new).await?);
        let claim = repo.claim_finalization("dispatch", &new).await?.unwrap();
        assert!(claim.cancelled);
        drop(claim);
        let resumed = repo.claim_finalization("dispatch", &new).await?.unwrap();
        assert!(
            resumed.cancelled,
            "cancelled cleanup may resume without publishing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_cancellation_write_releases_unacknowledged_claim() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
        let attempt = active_dispatch("job");
        repo.add("dispatch".into(), Arc::clone(&attempt)).await?;
        sqlx::query("CREATE TRIGGER reject_cancel BEFORE INSERT ON dispatch_cancelled_marks BEGIN SELECT RAISE(ABORT, 'reject cancellation'); END")
            .execute(&sql.write_pool).await?;
        assert!(repo.cancel("dispatch", &attempt).await.is_err());
        let claim = repo
            .claim_finalization("dispatch", &attempt)
            .await?
            .unwrap();
        assert!(
            !claim.cancelled,
            "failed persistence never acknowledges cancellation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_acknowledgments_and_cleanup_wait_for_durable_mark() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
        let attempt = active_dispatch("job");
        repo.add("dispatch".into(), Arc::clone(&attempt)).await?;
        let tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut first = std::pin::pin!(repo.cancel("dispatch", &attempt));
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert_eq!(
            attempt
                .arbitration
                .load(std::sync::atomic::Ordering::Acquire),
            ATTEMPT_CANCEL_REQUESTED,
        );
        let mut duplicate = std::pin::pin!(repo.cancel("dispatch", &attempt));
        let mut cleanup = std::pin::pin!(repo.claim_finalization("dispatch", &attempt));
        assert!(futures::poll!(duplicate.as_mut()).is_pending());
        assert!(futures::poll!(cleanup.as_mut()).is_pending());
        tx.rollback().await?;
        assert!(first.await?);
        assert!(!duplicate.await?);
        assert!(cleanup.await?.unwrap().cancelled);
        Ok(())
    }
}
