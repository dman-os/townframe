use tokio_util::sync::CancellationToken;

use crate::interlude::*;

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
    pub args: ActiveDispatchArgs,
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

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum DispatchOnSuccessHook {
    InitMarkDone {
        init_id: String,
        run_mode: daybook_types::manifest::InitRunMode,
    },
    ProcessorRunLog {
        doc_id: String,
        processor_full_id: String,
        done_token: String,
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
        wflow_key: String,
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
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum RoutineInvocation {
    Processor(ProcessorInvocation),
    Command,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct FacetRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub branch_path: daybook_types::doc::BranchPath,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub staging_branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub invocation: RoutineInvocation,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub config_facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub local_state_acl: Vec<daybook_types::manifest::RoutineLocalStateAccess>,
    #[serde(default)]
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub command_invoke_acl_snapshot: Vec<url::Url>,
    #[serde(default)]
    pub wflow_args_json: Option<String>,
}

pub(crate) fn facet_routine_args_fingerprint(args: &FacetRoutineArgs) -> String {
    let bytes = serde_json::to_vec(args).expect(ERROR_JSON);
    utils_rs::hash::blake3_hash_bytes(&bytes)
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
    DispatchAdded {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    DispatchUpdated {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    DispatchDeleted {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
}

#[derive(Default)]
struct DispatchState {
    dispatches: HashMap<String, Arc<ActiveDispatch>>,
    active_dispatches: HashMap<String, Arc<ActiveDispatch>>,
    wflow_to_dispatch: HashMap<String, String>,
    cancelled_dispatches: HashSet<String>,
    wflow_partition_frontier: HashMap<String, u64>,
    dispatch_head_index: HashMap<automerge::ChangeHash, String>,
}

pub struct DispatchRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,

    db_pool: sqlx::SqlitePool,
    state: tokio::sync::Mutex<DispatchState>,
    transition_mutex: tokio::sync::Mutex<()>,
    cancel_token: CancellationToken,
    local_actor_id: ActorId,
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
    fn local_origin(&self) -> crate::event_origin::SwitchEventOrigin {
        crate::event_origin::SwitchEventOrigin::Local {
            actor_id: self.local_actor_id.to_string(),
        }
    }

    pub async fn load(
        _big_repo: SharedBigRepo,
        _app_doc_id: DocumentId,
        local_user_path: daybook_types::doc::UserPath,
        db_pool: sqlx::SqlitePool,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        init_schema(&db_pool).await?;

        let local_user_path =
            daybook_types::doc::user_path::for_repo(&local_user_path, "dispatch-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let state = load_state(&db_pool).await?;
        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();

        let repo = Arc::new(Self {
            registry,
            db_pool,
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

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.state.lock().await.dispatches.get(id).map(Arc::clone)
    }

    pub async fn get_active(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.state
            .lock()
            .await
            .active_dispatches
            .get(id)
            .map(Arc::clone)
    }

    pub async fn get_any(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
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
        .execute(&self.db_pool)
        .await?;

        let mut state = self.state.lock().await;
        state
            .wflow_partition_frontier
            .insert(wflow_part_id, frontier);
        Ok(())
    }

    pub async fn get_by_wflow_job(&self, job_id: &str) -> Option<Arc<ActiveDispatch>> {
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

    pub async fn add(&self, id: String, dispatch: Arc<ActiveDispatch>) -> Res<()> {
        debug!(?id, "adding dispatch to repo");
        let _transition_guard = self.transition_mutex.lock().await;
        let ActiveDispatchArgs::FacetRoutine(_args) = &dispatch.args;

        let mut tx = self.db_pool.begin().await?;
        persist_dispatch_tx(&mut tx, &id, &dispatch).await?;
        clear_cancelled_mark_tx(&mut tx, &id).await?;
        tx.commit().await?;

        let mut state = self.state.lock().await;
        state.cancelled_dispatches.remove(&id);
        state
            .dispatch_head_index
            .entry(dispatch_head_for_dispatch(&id, &dispatch))
            .or_insert_with(|| id.clone());

        if let Some(old) = state.dispatches.insert(id.clone(), Arc::clone(&dispatch)) {
            if let ActiveDispatchDeets::Wflow {
                wflow_job_id: Some(job),
                ..
            } = &old.deets
            {
                state.wflow_to_dispatch.remove(job);
            }
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
        Ok(())
    }

    pub async fn complete(
        &self,
        id: String,
        status: DispatchStatus,
    ) -> Res<Option<Arc<ActiveDispatch>>> {
        assert!(matches!(
            status,
            DispatchStatus::Succeeded | DispatchStatus::Failed | DispatchStatus::Cancelled
        ));
        let _transition_guard = self.transition_mutex.lock().await;

        let old = self.state.lock().await.dispatches.get(&id).map(Arc::clone);
        let Some(old_dispatch) = old.clone() else {
            return Ok(None);
        };

        let mut next = (*old_dispatch).clone();
        next.status = status;
        let next = Arc::new(next);

        let mut tx = self.db_pool.begin().await?;
        persist_dispatch_tx(&mut tx, &id, &next).await?;
        clear_cancelled_mark_tx(&mut tx, &id).await?;
        tx.commit().await?;

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
        {
            if next.status == DispatchStatus::Active {
                state.wflow_to_dispatch.insert(job.clone(), id.clone());
            }
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

    pub async fn list(&self) -> Vec<(String, Arc<ActiveDispatch>)> {
        self.state
            .lock()
            .await
            .active_dispatches
            .iter()
            .map(|(id, dispatch)| (id.clone(), Arc::clone(dispatch)))
            .collect()
    }

    pub async fn mark_cancelled(&self, id: &str) -> Res<bool> {
        let _transition_guard = self.transition_mutex.lock().await;
        let state = self.state.lock().await;
        let Some(dispatch) = state.dispatches.get(id) else {
            eyre::bail!("dispatch not found under {id}");
        };
        if !matches!(
            dispatch.status,
            DispatchStatus::Active | DispatchStatus::Waiting
        ) {
            eyre::bail!("dispatch not active/waiting under {id}");
        }
        if state.cancelled_dispatches.contains(id) {
            return Ok(false);
        }
        drop(state);

        let mut tx = self.db_pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO dispatch_cancelled_marks(dispatch_id, created_at)\n             VALUES (?1, unixepoch())",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        tx.commit().await?;

        if inserted {
            self.state
                .lock()
                .await
                .cancelled_dispatches
                .insert(id.to_string());
        }
        Ok(inserted)
    }

    pub async fn list_waiting_on(
        &self,
        dependency_dispatch_id: &str,
    ) -> Vec<(String, Arc<ActiveDispatch>)> {
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
    ) -> Res<Option<Arc<ActiveDispatch>>> {
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
            .waiting_on_dispatch_ids
            .retain(|dep| dep != dependency_dispatch_id);
        let ready = updated.waiting_on_dispatch_ids.is_empty();
        let updated = Arc::new(updated);

        let mut tx = self.db_pool.begin().await?;
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

        if ready {
            Ok(Some(updated))
        } else {
            Ok(None)
        }
    }

    pub async fn activate_waiting(
        &self,
        dispatch_id: &str,
        deets: ActiveDispatchDeets,
    ) -> Res<Arc<ActiveDispatch>> {
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
            eyre::bail!("dispatch is not waiting: {dispatch_id}");
        }
        if !cur.waiting_on_dispatch_ids.is_empty() {
            eyre::bail!("dispatch still has unresolved dependencies: {dispatch_id}");
        }

        let mut updated = (*cur).clone();
        updated.status = DispatchStatus::Active;
        updated.deets = deets;
        let updated = Arc::new(updated);

        let mut tx = self.db_pool.begin().await?;
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

        Ok(updated)
    }

    pub async fn update_active_deets(
        &self,
        dispatch_id: &str,
        deets: ActiveDispatchDeets,
    ) -> Res<Arc<ActiveDispatch>> {
        let _transition_guard = self.transition_mutex.lock().await;
        let cur = self
            .state
            .lock()
            .await
            .dispatches
            .get(dispatch_id)
            .map(Arc::clone)
            .ok_or_else(|| eyre::eyre!("dispatch not found under {dispatch_id}"))?;
        if cur.status != DispatchStatus::Active {
            eyre::bail!("dispatch is not active: {dispatch_id}");
        }

        let mut updated = (*cur).clone();
        updated.deets = deets;
        let updated = Arc::new(updated);

        let mut tx = self.db_pool.begin().await?;
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

        Ok(updated)
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
        updated.status = DispatchStatus::Failed;
        let updated = Arc::new(updated);

        let mut tx = self.db_pool.begin().await?;
        persist_dispatch_tx(&mut tx, dispatch_id, &updated).await?;
        clear_cancelled_mark_tx(&mut tx, dispatch_id).await?;
        tx.commit().await?;

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
    dispatches: impl Iterator<Item = (&'a String, &'a Arc<ActiveDispatch>)>,
) -> ChangeHashSet {
    let mut items = dispatches.collect::<Vec<_>>();
    items.sort_unstable_by(|(lhs_id, _), (rhs_id, _)| lhs_id.cmp(rhs_id));
    let mut heads = Vec::with_capacity(items.len());
    for (id, dispatch) in items {
        heads.push(dispatch_head_for_dispatch(id, dispatch));
    }
    ChangeHashSet(Arc::from(heads))
}

async fn init_schema(db_pool: &sqlx::SqlitePool) -> Res<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatches (
            id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            wflow_job_id TEXT,
            updated_at INTEGER NOT NULL
        )",
    )
    .execute(db_pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_dispatches_wflow_job_id
         ON dispatches(wflow_job_id)",
    )
    .execute(db_pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dispatch_cancelled_marks (
            dispatch_id TEXT PRIMARY KEY NOT NULL,
            created_at INTEGER NOT NULL
        )",
    )
    .execute(db_pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS wflow_partition_frontier (
            wflow_partition_id TEXT PRIMARY KEY NOT NULL,
            frontier INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )",
    )
    .execute(db_pool)
    .await?;

    Ok(())
}

async fn load_state(db_pool: &sqlx::SqlitePool) -> Res<DispatchState> {
    let mut state = DispatchState::default();

    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, payload_json FROM dispatches ORDER BY id")
            .fetch_all(db_pool)
            .await?;

    for (id, payload_json) in rows {
        let dispatch: ActiveDispatch = serde_json::from_str(&payload_json)?;
        let dispatch = Arc::new(dispatch);
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
            .fetch_all(db_pool)
            .await?;
    state.cancelled_dispatches = cancelled_ids.into_iter().collect();

    let frontier_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT wflow_partition_id, frontier FROM wflow_partition_frontier")
            .fetch_all(db_pool)
            .await?;
    for (part_id, frontier) in frontier_rows {
        let frontier = match u64::try_from(frontier) {
            Ok(value) => value,
            Err(_) => eyre::bail!(
                "invalid negative frontier row in sqlite: part_id={part_id} frontier={frontier}"
            ),
        };
        state.wflow_partition_frontier.insert(part_id, frontier);
    }

    Ok(state)
}

async fn persist_dispatch_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    dispatch: &Arc<ActiveDispatch>,
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
    use super::*;
    use crate::repos::{Repo, SubscribeOpts};

    async fn setup_repo_with_pool(
        db_pool: sqlx::SqlitePool,
    ) -> Res<(Arc<DispatchRepo>, daybook_types::doc::UserPath)> {
        let local_user_path = daybook_types::doc::UserPath::from("/test-user/test-device");
        let (big_repo, _acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
            peer_id: "test-dispatch".into(),
            storage: am_utils_rs::repo::StorageConfig::Memory,
        })
        .await?;
        let app_doc = big_repo.add_doc(automerge::Automerge::new()).await?;
        let app_doc_id = app_doc.document_id().clone();
        let (repo, _stop) =
            DispatchRepo::load(big_repo, app_doc_id, local_user_path.clone(), db_pool).await?;
        Ok((repo, local_user_path))
    }

    fn active_dispatch(job_id: &str) -> Arc<ActiveDispatch> {
        Arc::new(ActiveDispatch {
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some("part-a".into()),
                entry_id: Some(1),
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_key: "key".into(),
                wflow_job_id: Some(job_id.to_string()),
            },
            args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id: "doc-1".into(),
                branch_path: "main".into(),
                staging_branch_path: "/tmp/stage".into(),
                heads: ChangeHashSet(Vec::new().into()),
                invocation: RoutineInvocation::Command,
                facet_acl: vec![],
                config_facet_acl: vec![],
                local_state_acl: vec![],
                command_invoke_acl_snapshot: vec![],
                wflow_args_json: None,
            }),
            status: DispatchStatus::Active,
            waiting_on_dispatch_ids: vec![],
            on_success_hooks: vec![],
        })
    }

    fn waiting_dispatch(job_id: &str, waits_on: &[&str]) -> Arc<ActiveDispatch> {
        Arc::new(ActiveDispatch {
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: None,
                entry_id: None,
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_key: "key".into(),
                wflow_job_id: Some(job_id.to_string()),
            },
            args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id: "doc-1".into(),
                branch_path: "main".into(),
                staging_branch_path: "/tmp/stage".into(),
                heads: ChangeHashSet(Vec::new().into()),
                invocation: RoutineInvocation::Command,
                facet_acl: vec![],
                config_facet_acl: vec![],
                local_state_acl: vec![],
                command_invoke_acl_snapshot: vec![],
                wflow_args_json: None,
            }),
            status: DispatchStatus::Waiting,
            waiting_on_dispatch_ids: waits_on.iter().map(|value| value.to_string()).collect(),
            on_success_hooks: vec![],
        })
    }

    #[tokio::test]
    async fn sqlite_dispatch_lifecycle_and_event_parity() -> Res<()> {
        let sql = crate::app::SqlCtx::new("sqlite::memory:").await?;
        let (repo, _) = setup_repo_with_pool(sql.db_pool.clone()).await?;
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
                && matches!(origin, crate::event_origin::SwitchEventOrigin::Local { .. })
        ));
        assert!(repo.get_active("disp-1").await.is_some());
        assert!(repo.get_by_wflow_job("job-1").await.is_some());

        assert!(repo.mark_cancelled("disp-1").await?);
        assert!(!repo.mark_cancelled("disp-1").await?);

        repo.complete("disp-1".into(), DispatchStatus::Succeeded)
            .await?;
        let event = sub
            .recv_async()
            .await
            .map_err(|err| eyre::eyre!("listener closed: {err:?}"))?;
        assert!(matches!(
            &*event,
            DispatchEvent::DispatchUpdated { id, origin, .. }
            if id == "disp-1"
                && matches!(origin, crate::event_origin::SwitchEventOrigin::Local { .. })
        ));
        assert!(repo.get_active("disp-1").await.is_none());
        assert!(repo.get_by_wflow_job("job-1").await.is_none());
        assert!(matches!(
            repo.get_any("disp-1").await.as_ref().map(|d| &d.status),
            Some(DispatchStatus::Succeeded)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_waiting_dependency_flow() -> Res<()> {
        let sql = crate::app::SqlCtx::new("sqlite::memory:").await?;
        let (repo, _) = setup_repo_with_pool(sql.db_pool.clone()).await?;

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
            ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some("part-b".into()),
                entry_id: Some(9),
                plug_id: "@test/plug".into(),
                routine_name: "routine".into(),
                bundle_name: "bundle".into(),
                wflow_key: "key".into(),
                wflow_job_id: Some("job-wait-1".into()),
            },
        )
        .await?;
        assert!(repo.get_active("wait-1").await.is_some());
        assert!(repo.get_by_wflow_job("job-wait-1").await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_reload_persists_dispatch_rows_and_frontier() -> Res<()> {
        let temp = tempfile::tempdir()?;
        let db_url = format!("sqlite://{}", temp.path().join("dispatch.sqlite").display());

        let sql = crate::app::SqlCtx::new(&db_url).await?;
        let (repo, _) = setup_repo_with_pool(sql.db_pool.clone()).await?;
        repo.add("disp-a".into(), active_dispatch("job-a")).await?;
        repo.set_wflow_part_frontier("part-1".into(), 44).await?;
        assert!(repo.mark_cancelled("disp-a").await?);
        drop(repo);
        drop(sql);

        let sql = crate::app::SqlCtx::new(&db_url).await?;
        let (repo, _) = setup_repo_with_pool(sql.db_pool.clone()).await?;
        let loaded = repo
            .get_any("disp-a")
            .await
            .ok_or_else(|| eyre::eyre!("missing persisted dispatch"))?;
        assert_eq!(loaded.status, DispatchStatus::Active);
        assert_eq!(repo.get_wflow_part_frontier("part-1").await, Some(44));
        assert!(!repo.mark_cancelled("disp-a").await?);
        assert!(matches!(
            repo.events_for_init().await?.first(),
            Some(DispatchEvent::DispatchAdded { id, origin, .. })
                if id == "disp-a"
                    && matches!(origin, crate::event_origin::SwitchEventOrigin::Local { .. })
        ));
        Ok(())
    }

    #[test]
    fn processor_invocation_serializes_with_changed_facet_keys() {
        let proc = ProcessorInvocation {
            trigger_doc_id: "doc-1".into(),
            changed_facet_keys: vec![
                "org.example.note/main".into(),
                "org.example.blob/main".into(),
            ],
        };
        let json = serde_json::to_value(&proc).expect("serialize");
        assert_eq!(json["trigger_doc_id"], "doc-1");
        let keys = json["changed_facet_keys"].as_array().unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k == "org.example.note/main"));
        assert!(keys.iter().any(|k| k == "org.example.blob/main"));
    }

    #[test]
    fn routine_invocation_command_roundtrips() {
        let inv = RoutineInvocation::Command;
        let json = serde_json::to_value(&inv).expect("serialize");
        assert_eq!(json, serde_json::json!("Command"));
        let roundtrip: RoutineInvocation = serde_json::from_value(json).expect("deserialize");
        assert!(matches!(roundtrip, RoutineInvocation::Command));
    }

    #[test]
    fn routine_invocation_processor_roundtrips() {
        let inv = RoutineInvocation::Processor(ProcessorInvocation {
            trigger_doc_id: "doc-1".into(),
            changed_facet_keys: vec!["org.example.note/main".into()],
        });
        let json = serde_json::to_value(&inv).expect("serialize");
        assert!(json.get("Processor").is_some());
        let roundtrip: RoutineInvocation = serde_json::from_value(json).expect("deserialize");
        assert!(matches!(
            roundtrip,
            RoutineInvocation::Processor(ProcessorInvocation {
                trigger_doc_id,
                changed_facet_keys,
            }) if trigger_doc_id == "doc-1" && changed_facet_keys == vec!["org.example.note/main"]
        ));
    }

    #[test]
    fn facet_routine_args_fingerprint_is_stable_for_same_input() {
        let args = FacetRoutineArgs {
            doc_id: "doc-1".into(),
            branch_path: "main".into(),
            staging_branch_path: "/tmp/stage".into(),
            heads: ChangeHashSet(Vec::new().into()),
            invocation: RoutineInvocation::Processor(ProcessorInvocation {
                trigger_doc_id: "doc-1".into(),
                changed_facet_keys: vec!["org.example.note/main".into()],
            }),
            facet_acl: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl_snapshot: vec![],
            wflow_args_json: None,
        };
        let fp1 = facet_routine_args_fingerprint(&args);
        let fp2 = facet_routine_args_fingerprint(&args);
        assert_eq!(fp1, fp2, "fingerprint should be stable for identical args");
    }
}
