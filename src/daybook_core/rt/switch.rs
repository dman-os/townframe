//! Probably better named a Sequencer but that's too long
//! FIXME: this is spaghettifying and also has inefficencies
use crate::interlude::*;
use std::collections::BTreeMap;

use crate::drawer::DrawerEvent;
use crate::plugs::PlugsEvent;
use crate::rt::Rt;
use crate::rt::dispatch::DispatchEvent;
use big_sync_core::rpc::{SubEvent, SubPartsRequest};
use daybook_types::doc::BranchPathBuf;
use daybook_types::doc::{Doc, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag};
use daybook_types::manifest::{
    DocPredicateEvalMode, DocPredicateEvalRequirement, DocPredicateEvalResolved,
};
use sqlx::Row;

const SUBSCRIPTION_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
struct SwitchDocState {
    doc_id: DocId,
    // FIXME: use BranchPathBuf instead?
    branch_name: String,
    present: bool,
    last_heads: Option<ChangeHashSet>,
}

#[derive(Debug, Clone)]
struct BranchRef {
    doc_id: DocId,
    branch_name: String,
}

#[derive(Clone)]
pub struct SwitchStore {
    repo_sql: SqlCtx,
}

impl SwitchStore {
    async fn load(repo_sql: SqlCtx) -> Res<Self> {
        init_schema(&repo_sql).await?;
        Ok(Self { repo_sql })
    }

    async fn get_partition_cursor(&self, partition_id: &str) -> Res<u64> {
        let row = sqlx::query("SELECT cursor FROM switch_partition_cursor WHERE partition_id = ?1")
            .bind(partition_id)
            .fetch_optional(&self.repo_sql.write_pool)
            .await?;
        Ok(row
            .map(|rowv| rowv.get::<i64, _>("cursor"))
            .unwrap_or_default()
            .max(0) as u64)
    }

    async fn commit_partition_event(
        &self,
        partition_id: &str,
        cursor: u64,
        branch_doc_id: Option<&str>,
        state: Option<&SwitchDocState>,
    ) -> Res<()> {
        let mut tx = self
            .repo_sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        sqlx::query(
            r#"
            INSERT INTO switch_partition_cursor(partition_id, cursor, updated_at)
                VALUES (?1, ?2, unixepoch())
                ON CONFLICT(partition_id)
                DO UPDATE SET
                    cursor = excluded.cursor
                    ,updated_at = excluded.updated_at
            "#,
        )
        .bind(partition_id)
        .bind(i64::try_from(cursor).expect("cursor exceeds sqlite INTEGER range"))
        .execute(&mut *tx)
        .await?;
        if let (Some(branch_doc_id), Some(state)) = (branch_doc_id, state) {
            let heads_json = state.last_heads.as_ref().map(|heads| {
                serde_json::to_string(&am_utils_rs::serialize_commit_heads(heads.as_ref()))
                    .expect(ERROR_JSON)
            });
            sqlx::query(
                r#"INSERT INTO switch_doc_state(
                        branch_doc_id, doc_id
                        ,branch_name, present
                        ,last_heads_json, updated_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())
                    ON CONFLICT(branch_doc_id) 
                    DO UPDATE SET
                        doc_id = excluded.doc_id
                        ,branch_name = excluded.branch_name
                        ,present = excluded.present
                        ,last_heads_json = excluded.last_heads_json
                        ,updated_at = excluded.updated_at
                        "#,
            )
            .bind(branch_doc_id)
            .bind(state.doc_id.to_string())
            .bind(&state.branch_name)
            .bind(if state.present { 1_i64 } else { 0_i64 })
            .bind(heads_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_doc_state_by_branch_doc_id(
        &self,
        branch_doc_id: &str,
    ) -> Res<Option<SwitchDocState>> {
        let row = sqlx::query(
            "SELECT doc_id, branch_name, present, last_heads_json FROM switch_doc_state WHERE branch_doc_id = ?1",
        )
        .bind(branch_doc_id)
        .fetch_optional(&self.repo_sql.write_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let heads_json: Option<String> = row.get("last_heads_json");
        let last_heads = match heads_json {
            Some(json) => {
                let raw: Vec<String> = serde_json::from_str(&json)?;
                Some(ChangeHashSet(am_utils_rs::parse_commit_heads(&raw)?))
            }
            None => None,
        };
        Ok(Some(SwitchDocState {
            doc_id: DocId::from(row.get::<String, _>("doc_id")),
            branch_name: row.get("branch_name"),
            present: row.get::<i64, _>("present") != 0,
            last_heads,
        }))
    }
}

async fn init_schema(repo_sql: &SqlCtx) -> Res<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS switch_partition_cursor (
            partition_id TEXT PRIMARY KEY
            ,cursor INTEGER NOT NULL
            ,updated_at INTEGER NOT NULL
        ) STRICT"#,
    )
    .execute(&repo_sql.write_pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS switch_doc_state (
            branch_doc_id TEXT PRIMARY KEY
            ,doc_id TEXT NOT NULL
            ,branch_name TEXT NOT NULL
            ,present INTEGER NOT NULL
            ,last_heads_json TEXT
            ,updated_at INTEGER NOT NULL
        ) STRICT
        "#,
    )
    .execute(&repo_sql.write_pool)
    .await?;
    Ok(())
}

/// Worker that listens to drawer events and schedules workflows
pub struct SwitchWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

/// FIXME: use a stop token insntead
impl SwitchWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, Duration::from_secs(5)).await?;
        Ok(())
    }
}

// FIXME: adopt this pattern for handles/stop tokens across the codebase
impl Drop for SwitchWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort()
        }
    }
}

fn switch_worker_is_shutting_down(
    worker_cancel_token: &tokio_util::sync::CancellationToken,
    rt_cancel_token: &tokio_util::sync::CancellationToken,
) -> bool {
    worker_cancel_token.is_cancelled() || rt_cancel_token.is_cancelled()
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct SwitchDocEvent {
    pub doc_id: DocId,
    pub branch_name: String,
    pub prev_heads: Option<ChangeHashSet>,
    pub new_heads: ChangeHashSet,
    pub diff: Option<crate::drawer::DocEntryDiff>,
    pub drawer_heads: Option<ChangeHashSet>,
    pub origin: crate::event_origin::SwitchEventOrigin,
}

#[derive(Debug, Clone)]
pub enum SwitchEvent {
    Doc(Arc<SwitchDocEvent>),
    Drawer(Arc<DrawerEvent>),
    Plugs(Arc<PlugsEvent>),
    Dispatch(Arc<DispatchEvent>),
    Config(Arc<crate::config::ConfigEvent>),
}

#[derive(Debug, Clone)]
pub struct SwtchSinkInterest {
    pub consume_doc: bool,
    pub consume_drawer: bool,
    pub consume_plugs: bool,
    pub consume_dispatch: bool,
    pub consume_config: bool,
    pub drawer_predicate: Option<daybook_types::manifest::DocPredicateClause>,
}

#[derive(Default, Debug, Clone)]
pub struct SwitchSinkOutcome {
    pub drawer_predicate_update: Option<daybook_types::manifest::DocPredicateClause>,
}

#[derive(Clone, Copy)]
pub struct SwitchSinkCtx<'a> {
    // FIXME: why are these optional?
    pub rt: Option<&'a Arc<Rt>>,
    pub store: Option<&'a SwitchStore>,
}

#[async_trait]
pub trait SwitchSink {
    fn interest(&self) -> SwtchSinkInterest;
    async fn on_event(
        &mut self,
        event: &SwitchEvent,
        ctx: &SwitchSinkCtx<'_>,
    ) -> Res<SwitchSinkOutcome>;
}

struct PreparedSwitchSink {
    name: String,
    listener: Box<dyn SwitchSink + Send + Sync>,
    consume_doc: bool,
    consume_drawer: bool,
    consume_plugs: bool,
    consume_dispatch: bool,
    consume_config: bool,
    drawer_predicate: Option<daybook_types::manifest::DocPredicateClause>,
}

pub async fn spawn_switch_worker(
    rt: Arc<Rt>,
    repo_sql: SqlCtx,
    sinks: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>>,
) -> Res<SwitchWorkerHandle> {
    use crate::repos::{Repo, SubscribeOpts};

    let store = SwitchStore::load(repo_sql).await?;

    let drawer_listener = rt
        .drawer
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let plug_listener = rt
        .plugs_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let config_listener = rt
        .config_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let dispatch_listener = rt
        .dispatch_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));

    let mut worker = SwitchWorker {
        store,
        rt,
        prepared_sinks: prepare_sinks(sinks),
        predicate_requirements: HashSet::new(),
        predicate_resolved: HashMap::new(),
        branch_index: HashMap::new(),
    };

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let rt_cancel_token = worker.rt.cancel_token.clone();
    let fut = {
        let cancel_token = cancel_token.clone();
        let rt_cancel_token = rt_cancel_token.clone();
        async move {
            worker.refresh_branch_index().await?;
            let events = worker.rt.plugs_repo.events_for_init().await?;
            for event in events {
                let event = Arc::new(event);
                worker
                    .track_event_heads(&SwitchEvent::Plugs(Arc::clone(&event)))
                    .await?;
                worker
                    .dispatch_to_listeners(&SwitchEvent::Plugs(event))
                    .await?;
            }

            let events = worker.rt.drawer.events_for_init().await?;
            for event in events {
                let event = Arc::new(event);
                worker
                    .track_event_heads(&SwitchEvent::Drawer(Arc::clone(&event)))
                    .await?;
                worker
                    .dispatch_to_listeners(&SwitchEvent::Drawer(event))
                    .await?;
            }

            let events = worker.rt.dispatch_repo.events_for_init().await?;
            for event in events {
                let event = Arc::new(event);
                worker
                    .track_event_heads(&SwitchEvent::Dispatch(Arc::clone(&event)))
                    .await?;
                worker
                    .dispatch_to_listeners(&SwitchEvent::Dispatch(event))
                    .await?;
            }

            let events = worker.rt.config_repo.events_for_init().await?;
            for event in events {
                let event = Arc::new(event);
                worker
                    .track_event_heads(&SwitchEvent::Config(Arc::clone(&event)))
                    .await?;
                worker
                    .dispatch_to_listeners(&SwitchEvent::Config(event))
                    .await?;
            }

            let docs_partition_id = big_repo::automerge_docs_part_id();
            let docs_partition_id_text = docs_partition_id.to_string();
            let mut cursor = worker
                .store
                .get_partition_cursor(&docs_partition_id_text)
                .await?;
            let partition_listener = worker
                .rt
                .rcx
                .frontier_part_store
                .subscribe_local(SubPartsRequest {
                    lower_bound: cursor,
                    targets: std::collections::HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: docs_partition_id,
                            cursor,
                        },
                    ]),
                })
                .await??;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        debug!("cancel token lit");
                        break;
                    }
                    event = plug_listener.recv_lossy_async() => {
                        let event = event.map_err(|_| ferr!("SwitchWorker plug_listener recv closed"))?;
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Plugs(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Plugs(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    event = config_listener.recv_lossy_async() => {
                        let event = event.map_err(|_| ferr!("SwitchWorker config_listener recv closed"))?;
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Config(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Config(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    event = drawer_listener.recv_lossy_async() => {
                        let event = event.map_err(|_| ferr!("SwitchWorker drawer_listener recv closed"))?;
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Drawer(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Drawer(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.refresh_branch_index().await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    part_event = partition_listener.recv() => {
                        let part_event = part_event.wrap_err( "SwitchWorker partition_listener recv closed")?;
                        let commit = worker.handle_partition_doc_event(&part_event).await?;
                        cursor = match &part_event {
                            SubEvent::Added(inner) => inner.cursor,
                            SubEvent::Changed(inner) => inner.cursor,
                            SubEvent::Removed(inner) => inner.cursor,
                            SubEvent::ReplayComplete => cursor,
                        };
                        worker
                            .store
                            .commit_partition_event(
                                &docs_partition_id_text,
                                cursor,
                                commit
                                    .as_ref()
                                    .map(|(branch_doc_id, _)| &branch_doc_id[..]),
                                commit.as_ref().map(|(_, state)| state),
                            )
                            .await?;
                    }
                    event = dispatch_listener.recv_lossy_async() => {
                        let event = event.map_err(|_| ferr!("SwitchWorker dispatch_listener recv closed"))?;
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Dispatch(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Dispatch(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_cancel_token = cancel_token.clone();
    let join_rt_cancel_token = rt_cancel_token.clone();
    let join_handle = tokio::spawn(async move {
        if let Err(err) = fut.await {
            if switch_worker_is_shutting_down(&join_cancel_token, &join_rt_cancel_token) {
                debug!(?err, "SwitchWorker exiting during shutdown");
            } else {
                error!(?err, "SwitchWorker failed");
            }
        }
    });

    Ok(SwitchWorkerHandle {
        join_handle: Some(join_handle),
        cancel_token,
    })
}

fn prepare_sinks(
    listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>>,
) -> Vec<PreparedSwitchSink> {
    listeners
        .into_iter()
        .map(|(name, listener)| {
            let interest = listener.interest();
            PreparedSwitchSink {
                name,
                listener,
                consume_doc: interest.consume_doc,
                consume_drawer: interest.consume_drawer,
                consume_plugs: interest.consume_plugs,
                consume_dispatch: interest.consume_dispatch,
                consume_config: interest.consume_config,
                drawer_predicate: interest.drawer_predicate,
            }
        })
        .collect()
}

struct SwitchWorker {
    rt: Arc<Rt>,
    store: SwitchStore,
    prepared_sinks: Vec<PreparedSwitchSink>,
    predicate_requirements: HashSet<DocPredicateEvalRequirement>,
    predicate_resolved: HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
    branch_index: HashMap<String, BranchRef>,
}

impl SwitchWorker {
    async fn refresh_branch_index(&mut self) -> Res<()> {
        let (_, doc_ids) = self.rt.drawer.list_just_ids().await?;
        let mut out = HashMap::new();
        for raw_doc_id in doc_ids {
            let doc_id = DocId::from(raw_doc_id);
            let Some(entry) = self.rt.drawer.get_entry(&doc_id).await? else {
                continue;
            };
            for (branch_name, branch_ref) in entry.branches {
                out.insert(
                    branch_ref.branch_doc_id.to_string(),
                    BranchRef {
                        doc_id: doc_id.clone(),
                        branch_name,
                    },
                );
            }
        }
        self.branch_index = out;
        Ok(())
    }

    // FIXME: this shouldn't be optional

    async fn resolve_branch_ref(&mut self, branch_doc_id: &str) -> Res<Option<BranchRef>> {
        if let Some(found) = self.branch_index.get(branch_doc_id) {
            return Ok(Some(found.clone()));
        }
        self.refresh_branch_index().await?;
        Ok(self.branch_index.get(branch_doc_id).cloned())
    }

    async fn handle_partition_doc_event(
        &mut self,
        event: &SubEvent,
    ) -> Res<Option<(Arc<str>, SwitchDocState)>> {
        let obj_id = match event {
            SubEvent::Added(inner) => inner.obj_id,
            SubEvent::Changed(inner) => inner.obj_id,
            SubEvent::Removed(inner) => inner.obj_id,
            SubEvent::ReplayComplete => return Ok(None),
        };
        let branch_doc_id: Arc<str> = big_repo::automerge_obj_to_doc_id(obj_id).to_string().into();
        info!(%branch_doc_id, ?event, "SwitchWorker handle_partition_doc_event received");
        let stored_state = self
            .store
            .get_doc_state_by_branch_doc_id(&branch_doc_id)
            .await?;
        let resolved_branch = self.resolve_branch_ref(&branch_doc_id).await?;
        info!(%branch_doc_id, ?resolved_branch, ?stored_state, "SwitchWorker resolved branch");
        let (doc_id, branch_name) = if let Some(branch) = resolved_branch {
            (branch.doc_id, branch.branch_name)
        } else if let Some(state) = &stored_state {
            (state.doc_id.clone(), state.branch_name.clone())
        } else {
            info!(%branch_doc_id, "SwitchWorker ignoring unresolved branch_doc_id");
            return Ok(None);
        };
        let mut next_state = stored_state.unwrap_or(SwitchDocState {
            doc_id: doc_id.clone(),
            branch_name: branch_name.clone(),
            present: false,
            last_heads: None,
        });
        next_state.doc_id = doc_id.clone();
        next_state.branch_name = branch_name.clone();

        match event {
            SubEvent::Added(_) | SubEvent::Changed(_) => {
                let Some(handle) = self
                    .rt
                    .drawer
                    .get_handle_by_branch_doc_id(branch_doc_id.parse()?)
                    .await?
                else {
                    info!(%branch_doc_id, "SwitchWorker get_handle_by_branch_doc_id returned None");
                    return Ok(None);
                };

                let new_heads = ChangeHashSet(
                    handle
                        .with_document_read(|doc| doc.get_heads())
                        .await
                        .into_iter()
                        .collect(),
                );

                let prev_heads = next_state.last_heads.clone();
                info!(%branch_doc_id, ?prev_heads, ?new_heads, present = next_state.present, "SwitchWorker heads comparison");
                if next_state.present && prev_heads.as_ref() == Some(&new_heads) {
                    info!(%branch_doc_id, "SwitchWorker heads unchanged; skipping");
                    return Ok(Some((branch_doc_id, next_state)));
                }
                let branch_path = BranchPathBuf::from(branch_name.as_str());
                let (diff, origin, _deleted_facet_keys) = self
                    .compute_partition_doc_diff(
                        &doc_id,
                        &branch_path,
                        prev_heads.as_ref(),
                        Some(&new_heads),
                    )
                    .await?;
                let doc_evt = Arc::new(SwitchDocEvent {
                    doc_id: doc_id.clone(),
                    branch_name: branch_name.clone(),
                    prev_heads: prev_heads.clone(),
                    new_heads: new_heads.clone(),
                    diff: Some(diff),
                    drawer_heads: None,
                    origin,
                });
                info!(%doc_id, %branch_doc_id, ?doc_evt.diff, "SwitchWorker dispatching SwitchDocEvent from partition event");
                let switch_evt = SwitchEvent::Doc(Arc::clone(&doc_evt));
                self.track_event_heads(&switch_evt).await?;
                self.dispatch_to_listeners(&switch_evt).await?;
                self.rt.registry.notify([doc_evt]);
                next_state.present = true;
                next_state.last_heads = Some(new_heads);
            }
            SubEvent::Removed(_) => {
                if !next_state.present {
                    return Ok(Some((branch_doc_id, next_state)));
                }
                let branch_path = BranchPathBuf::from(branch_name.as_str());
                let (_diff, origin, deleted_facet_keys) = self
                    .compute_partition_doc_diff(
                        &doc_id,
                        &branch_path,
                        next_state.last_heads.as_ref(),
                        None,
                    )
                    .await?;
                let evt = Arc::new(DrawerEvent::DocDeleted {
                    id: doc_id.clone(),
                    deleted_facet_keys,
                    entry: self.rt.drawer.get_entry(&doc_id).await?,
                    drawer_heads: ChangeHashSet::default(),
                    origin,
                });
                self.track_event_heads(&SwitchEvent::Drawer(Arc::clone(&evt)))
                    .await?;
                self.dispatch_to_listeners(&SwitchEvent::Drawer(evt))
                    .await?;
                next_state.present = false;
                next_state.last_heads = None;
            }
            SubEvent::ReplayComplete => return Ok(None),
        }
        Ok(Some((branch_doc_id, next_state)))
    }

    async fn compute_partition_doc_diff(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        prev_heads: Option<&ChangeHashSet>,
        next_heads: Option<&ChangeHashSet>,
    ) -> Res<(
        crate::drawer::DocEntryDiff,
        crate::event_origin::SwitchEventOrigin,
        Vec<FacetKey>,
    )> {
        let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
        let (old_keys, old_updated_at) = if let Some(heads) = prev_heads {
            if let Some(doc) = self
                .rt
                .drawer
                .get_doc_with_facets_at_branch_heads(
                    doc_id,
                    branch_path,
                    heads,
                    Some(vec![dmeta_key.clone()]),
                )
                .await?
            {
                if let Some(dmeta_raw) = doc.facets.get(&dmeta_key) {
                    let dmeta = match WellKnownFacet::from_json(
                        dmeta_raw.clone(),
                        WellKnownFacetTag::Dmeta,
                    )? {
                        WellKnownFacet::Dmeta(dmeta) => dmeta,
                        other => {
                            eyre::bail!("expected dmeta facet, got {:?}", other.tag());
                        }
                    };
                    let mut keys = HashSet::new();
                    let mut updated_at = HashMap::new();
                    for (key, meta) in dmeta.facets {
                        if key == dmeta_key {
                            continue;
                        }
                        if !meta.deleted_at.is_empty() {
                            continue;
                        }
                        keys.insert(key.clone());
                        updated_at.insert(key, meta.updated_at);
                    }
                    (keys, updated_at)
                } else {
                    (HashSet::new(), HashMap::new())
                }
            } else {
                (HashSet::new(), HashMap::new())
            }
        } else {
            (HashSet::new(), HashMap::new())
        };
        let (new_keys, new_updated_at) = if let Some(heads) = next_heads {
            if let Some(doc) = self
                .rt
                .drawer
                .get_doc_with_facets_at_branch_heads(
                    doc_id,
                    branch_path,
                    heads,
                    Some(vec![dmeta_key.clone()]),
                )
                .await?
            {
                if let Some(dmeta_raw) = doc.facets.get(&dmeta_key) {
                    let dmeta = match WellKnownFacet::from_json(
                        dmeta_raw.clone(),
                        WellKnownFacetTag::Dmeta,
                    )? {
                        WellKnownFacet::Dmeta(dmeta) => dmeta,
                        other => {
                            eyre::bail!("expected dmeta facet, got {:?}", other.tag());
                        }
                    };
                    let mut keys = HashSet::new();
                    let mut updated_at = HashMap::new();
                    for (key, meta) in dmeta.facets {
                        if key == dmeta_key {
                            continue;
                        }
                        if !meta.deleted_at.is_empty() {
                            continue;
                        }
                        keys.insert(key.clone());
                        updated_at.insert(key, meta.updated_at);
                    }
                    (keys, updated_at)
                } else {
                    (HashSet::new(), HashMap::new())
                }
            } else {
                (HashSet::new(), HashMap::new())
            }
        } else {
            (HashSet::new(), HashMap::new())
        };
        let mut added: Vec<FacetKey> = new_keys.difference(&old_keys).cloned().collect();
        let mut removed: Vec<FacetKey> = old_keys.difference(&new_keys).cloned().collect();
        let mut changed = Vec::new();
        if let (Some(prev), Some(next)) = (prev_heads, next_heads) {
            for key in old_keys.intersection(&new_keys) {
                if old_updated_at.get(key) != new_updated_at.get(key) {
                    changed.push(key.clone());
                } else {
                    let old_h = self
                        .rt
                        .drawer
                        .get_facet_heads_at_branch_heads(doc_id, branch_path, prev, key)
                        .await?;
                    let new_h = self
                        .rt
                        .drawer
                        .get_facet_heads_at_branch_heads(doc_id, branch_path, next, key)
                        .await?;
                    if old_h != new_h {
                        changed.push(key.clone());
                    }
                }
            }
        }
        added.sort();
        removed.sort();
        changed.sort();
        // Partition events do not encode authoritative local/remote intent at processor granularity.
        // Keep origin neutral here; processor-locality is evaluated later during triage.
        let origin = crate::event_origin::SwitchEventOrigin::Remote {
            peer_id: crate::peer_id_from_label("partition").to_string(),
        };
        Ok((
            crate::drawer::DocEntryDiff {
                changed_facet_keys: changed,
                added_facet_keys: added,
                removed_facet_keys: removed.clone(),
                moved_branch_names: vec![branch_path.to_string()],
            },
            origin,
            removed,
        ))
    }

    #[tracing::instrument(skip(self, event))]
    async fn dispatch_to_listeners(&mut self, event: &SwitchEvent) -> Res<()> {
        for index in 0..self.prepared_sinks.len() {
            if self.listener_interested_in_event(index, event).await? {
                let ctx = SwitchSinkCtx {
                    rt: Some(&self.rt),
                    store: Some(&self.store),
                };
                let outcome = self.prepared_sinks[index]
                    .listener
                    .on_event(event, &ctx)
                    .await?;
                if let Some(next_predicate) = outcome.drawer_predicate_update {
                    self.prepared_sinks[index].drawer_predicate = Some(next_predicate);
                }
                debug!(listener = %self.prepared_sinks[index].name, "switch listener handled event");
            }
        }

        Ok(())
    }

    async fn listener_interested_in_event(
        &mut self,
        index: usize,
        event: &SwitchEvent,
    ) -> Res<bool> {
        match event {
            SwitchEvent::Doc(event) => {
                if !self.prepared_sinks[index].consume_doc {
                    return Ok(false);
                }
                let predicate = self.prepared_sinks[index].drawer_predicate.clone();
                self.doc_event_matches_listener(event, predicate.as_ref())
                    .await
            }
            SwitchEvent::Drawer(event) => {
                if !self.prepared_sinks[index].consume_drawer {
                    return Ok(false);
                }
                let predicate = self.prepared_sinks[index].drawer_predicate.clone();
                self.drawer_event_matches_listener(event, predicate.as_ref())
                    .await
            }
            SwitchEvent::Plugs(_) => Ok(self.prepared_sinks[index].consume_plugs),
            SwitchEvent::Dispatch(_) => Ok(self.prepared_sinks[index].consume_dispatch),
            SwitchEvent::Config(_) => Ok(self.prepared_sinks[index].consume_config),
        }
    }

    async fn doc_event_matches_listener(
        &mut self,
        event: &Arc<SwitchDocEvent>,
        predicate: Option<&daybook_types::manifest::DocPredicateClause>,
    ) -> Res<bool> {
        let Some(predicate) = predicate else {
            return Ok(true);
        };
        let Some(diff) = &event.diff else {
            return Ok(true);
        };
        let referenced_tags = predicate.referenced_tags();
        let union_changed: HashSet<FacetKey> = diff
            .changed_facet_keys
            .iter()
            .cloned()
            .chain(diff.added_facet_keys.iter().cloned())
            .chain(diff.removed_facet_keys.iter().cloned())
            .collect();
        if !union_changed.iter().any(|facet_key| {
            referenced_tags
                .iter()
                .any(|tag| tag.0 == facet_key.tag.to_string())
        }) {
            return Ok(false);
        }
        let branch_path = BranchPathBuf::from(event.branch_name.as_str());
        let Some(facet_keys_set) = self
            .rt
            .drawer
            .get_facet_keys_if_latest(&event.doc_id, &branch_path, &event.new_heads)
            .await?
        else {
            return Ok(false);
        };
        let meta_doc = facet_keys_set_to_meta_doc(&event.doc_id, &facet_keys_set);
        self.predicate_requirements.clear();
        predicate.append_requirements(&mut self.predicate_requirements);
        Self::resolve_meta_predicate_requirements(
            &self.predicate_requirements,
            &meta_doc,
            &mut self.predicate_resolved,
        );
        Ok(predicate.evaluate(
            &meta_doc,
            DocPredicateEvalMode::ApproxInterest,
            &self.predicate_resolved,
        ))
    }

    fn resolve_meta_predicate_requirements(
        requirements: &HashSet<DocPredicateEvalRequirement>,
        meta_doc: &Doc,
        out: &mut HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
    ) {
        out.clear();
        for requirement in requirements {
            match requirement {
                DocPredicateEvalRequirement::FacetsOfTag(tag) => {
                    let source_facets = meta_doc
                        .facets
                        .iter()
                        .filter(|(facet_key, _)| facet_key.tag.to_string() == tag.0)
                        .map(|(facet_key, facet_raw)| (facet_key.clone(), facet_raw.clone()))
                        .collect::<Vec<_>>();
                    out.insert(
                        requirement.clone(),
                        DocPredicateEvalResolved::FacetsOfTag(source_facets),
                    );
                }
                DocPredicateEvalRequirement::FullDoc
                | DocPredicateEvalRequirement::FacetManifest => {
                    // Switch prefilter stays cheap by default. Missing resolved requirements
                    // are handled conservatively by predicate evaluation in ApproxInterest mode.
                }
            }
        }
    }

    async fn drawer_event_matches_listener(
        &mut self,
        event: &Arc<DrawerEvent>,
        predicate: Option<&daybook_types::manifest::DocPredicateClause>,
    ) -> Res<bool> {
        let Some(predicate) = predicate else {
            return Ok(true);
        };
        match &**event {
            DrawerEvent::DocDeleted {
                id,
                deleted_facet_keys,
                ..
            } => {
                let deleted_set: HashSet<FacetKey> = deleted_facet_keys.iter().cloned().collect();
                let meta_doc = facet_keys_set_to_meta_doc(id, &deleted_set);
                self.predicate_requirements.clear();
                predicate.append_requirements(&mut self.predicate_requirements);
                Self::resolve_meta_predicate_requirements(
                    &self.predicate_requirements,
                    &meta_doc,
                    &mut self.predicate_resolved,
                );
                Ok(predicate.evaluate(
                    &meta_doc,
                    DocPredicateEvalMode::ApproxInterest,
                    &self.predicate_resolved,
                ))
            }
            DrawerEvent::DocAdded { id, entry, .. } => {
                let Some((branch_name, heads)) = entry
                    .branches
                    .get_key_value("main")
                    .or_else(|| entry.branches.iter().next())
                else {
                    return Ok(false);
                };
                let branch_path = BranchPathBuf::from(branch_name.as_str());
                let Some(facet_keys_set) = self
                    .rt
                    .drawer
                    .get_facet_keys_if_latest(id, &branch_path, heads)
                    .await?
                else {
                    return Ok(false);
                };
                let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                self.predicate_requirements.clear();
                predicate.append_requirements(&mut self.predicate_requirements);
                Self::resolve_meta_predicate_requirements(
                    &self.predicate_requirements,
                    &meta_doc,
                    &mut self.predicate_resolved,
                );
                Ok(predicate.evaluate(
                    &meta_doc,
                    DocPredicateEvalMode::ApproxInterest,
                    &self.predicate_resolved,
                ))
            }
        }
    }

    async fn track_event_heads(&self, event: &SwitchEvent) -> Res<()> {
        let _event = event;
        Ok(())
    }
}

pub fn facet_keys_set_to_meta_doc(doc_id: &DocId, facet_keys_set: &HashSet<FacetKey>) -> Doc {
    let facets: HashMap<FacetKey, daybook_types::doc::FacetRaw> = facet_keys_set
        .iter()
        .map(|key| (key.clone(), serde_json::Value::Null))
        .collect();
    Doc {
        id: doc_id.clone(),
        facets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::dispatch::ActiveDispatch;
    use crate::test_support::test_cx;
    use daybook_types::doc::{AddDocArgs, DocPatch, WellKnownFacetTag};
    use std::sync::{Arc as StdArc, Mutex};

    async fn test_switch_store() -> Res<SwitchStore> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        SwitchStore::load(sql).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_store_commit_partition_event_persists_cursor_and_doc_state() -> Res<()> {
        let store = test_switch_store().await?;
        let partition_id = "drawer.replicated";
        let branch_doc_id = "doc-branch-1";
        let heads = ChangeHashSet::default();
        let state = SwitchDocState {
            doc_id: DocId::from("doc-1"),
            branch_name: "main".into(),
            present: true,
            last_heads: Some(heads.clone()),
        };
        store
            .commit_partition_event(partition_id, 42, Some(branch_doc_id), Some(&state))
            .await?;
        assert_eq!(store.get_partition_cursor(partition_id).await?, 42);
        let loaded = store
            .get_doc_state_by_branch_doc_id(branch_doc_id)
            .await?
            .ok_or_eyre("missing stored doc state")?;
        assert_eq!(loaded.doc_id, state.doc_id);
        assert_eq!(loaded.branch_name, "main");
        assert!(loaded.present);
        assert_eq!(loaded.last_heads, Some(heads));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_store_commit_partition_event_cursor_only() -> Res<()> {
        let store = test_switch_store().await?;
        let partition_id = "drawer.replicated";
        store
            .commit_partition_event(partition_id, 9, None, None)
            .await?;
        assert_eq!(store.get_partition_cursor(partition_id).await?, 9);
        Ok(())
    }

    struct TestListener {
        name: String,
        calls: StdArc<Mutex<Vec<String>>>,
        interest: SwtchSinkInterest,
        outcome: Option<SwitchSinkOutcome>,
    }

    #[async_trait]
    impl SwitchSink for TestListener {
        fn interest(&self) -> SwtchSinkInterest {
            self.interest.clone()
        }

        async fn on_event(
            &mut self,
            _event: &SwitchEvent,
            _ctx: &SwitchSinkCtx<'_>,
        ) -> Res<SwitchSinkOutcome> {
            self.calls
                .lock()
                .expect("switch test call lock poisoned")
                .push(self.name.clone());
            Ok(self.outcome.clone().unwrap_or_default())
        }
    }

    struct OriginCaptureListener {
        seen: StdArc<Mutex<Vec<crate::event_origin::SwitchEventOrigin>>>,
    }

    #[async_trait]
    impl SwitchSink for OriginCaptureListener {
        fn interest(&self) -> SwtchSinkInterest {
            SwtchSinkInterest {
                consume_doc: true,
                consume_drawer: true,
                consume_plugs: true,
                consume_dispatch: true,
                consume_config: true,
                drawer_predicate: None,
            }
        }

        async fn on_event(
            &mut self,
            event: &SwitchEvent,
            _ctx: &SwitchSinkCtx<'_>,
        ) -> Res<SwitchSinkOutcome> {
            let origin = match event {
                SwitchEvent::Doc(event) => event.origin.clone(),
                SwitchEvent::Drawer(event) => match &**event {
                    DrawerEvent::DocAdded { origin, .. }
                    | DrawerEvent::DocDeleted { origin, .. } => origin.clone(),
                },
                SwitchEvent::Plugs(event) => match &**event {
                    PlugsEvent::PlugAdded { origin, .. }
                    | PlugsEvent::PlugChanged { origin, .. }
                    | PlugsEvent::PlugDeleted { origin, .. }
                    | PlugsEvent::ConfigDocsChanged { origin, .. } => origin.clone(),
                },
                SwitchEvent::Dispatch(event) => match &**event {
                    DispatchEvent::DispatchAdded { origin, .. }
                    | DispatchEvent::DispatchUpdated { origin, .. }
                    | DispatchEvent::DispatchDeleted { origin, .. } => origin.clone(),
                },
                SwitchEvent::Config(event) => match &**event {
                    crate::config::ConfigEvent::Changed { origin, .. }
                    | crate::config::ConfigEvent::SyncDevicesChanged { origin, .. } => {
                        origin.clone()
                    }
                },
            };
            self.seen
                .lock()
                .expect("switch test origin lock poisoned")
                .push(origin);
            Ok(SwitchSinkOutcome::default())
        }
    }

    async fn dispatch_test_event(
        listeners: &mut [PreparedSwitchSink],
        event: &SwitchEvent,
    ) -> Res<()> {
        for listener in listeners.iter_mut() {
            let is_interested = match event {
                SwitchEvent::Doc(_) => listener.consume_doc,
                SwitchEvent::Drawer(_) => listener.consume_drawer,
                SwitchEvent::Plugs(_) => listener.consume_plugs,
                SwitchEvent::Dispatch(_) => listener.consume_dispatch,
                SwitchEvent::Config(_) => listener.consume_config,
            };
            if !is_interested {
                continue;
            }
            let outcome = listener
                .listener
                .on_event(
                    event,
                    &SwitchSinkCtx {
                        rt: None,
                        store: None,
                    },
                )
                .await?;
            if let Some(next_predicate) = outcome.drawer_predicate_update {
                listener.drawer_predicate = Some(next_predicate);
            }
        }
        Ok(())
    }

    fn count_dispatches_with_wflow_key(
        dispatches: &[(String, std::sync::Arc<ActiveDispatch>)],
        key: &str,
    ) -> usize {
        dispatches
            .iter()
            .filter(|(_, d)| {
                matches!(
                    &d.deets,
                    crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == key
                )
            })
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_worker_smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_smoke").await?;
        crate::test_support::import_test_plug_oci(&ctx).await?;

        // Add a doc that should trigger the test-label processor
        let _doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hello world".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // Wait for the dispatch to be created
        let mut dispatch_id: Option<String> = None;
        for _ in 0..300 {
            if let Some((id, _dispatch)) =
                ctx.dispatch_repo.get_any_by_wflow_key("test-label").await
            {
                dispatch_id = Some(id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch not found")?;

        // Wait for the dispatch to complete
        ctx.rt
            .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
            .await?;

        ctx.stop().await?;
        Ok(())
    }

    /// Global early-out: when only facets outside any processor's read set change, switch does not load the doc or schedule any processor.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_skip_when_no_processor_read_set_changed() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_skip_unrelated").await?;
        crate::test_support::import_test_plug_oci(&ctx).await?;

        let doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hello world".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // Wait for test-label dispatch from the add and wait for completion
        let mut initial_dispatch_id: Option<String> = None;
        let mut initial_done_seen = false;
        for _ in 0..300 {
            if let Some((dispatch_id, _dispatch)) =
                ctx.dispatch_repo.get_any_by_wflow_key("test-label").await
            {
                initial_dispatch_id = Some(dispatch_id.clone());
                break;
            }
            if ctx
                .rt
                .get_processor_runlog_done(&doc_id, "@daybook/test/test-label")
                .await?
                .is_some()
            {
                initial_done_seen = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if let Some(initial_dispatch_id) = initial_dispatch_id {
            ctx.rt
                .wait_for_dispatch_end(&initial_dispatch_id, std::time::Duration::from_secs(90))
                .await?;
        } else if !initial_done_seen {
            eyre::bail!("initial test-label dispatch/runlog not found");
        }

        let dispatches_before = ctx.dispatch_repo.list().await;
        let test_label_count_before =
            count_dispatches_with_wflow_key(&dispatches_before, "test-label");

        // Update only Title (no processor in default plugs has Title in read set for switch)
        ctx.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        WellKnownFacetTag::TitleGeneric.into(),
                        daybook_types::doc::WellKnownFacet::TitleGeneric("A title".into()).into(),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        let dispatches_after = ctx.dispatch_repo.list().await;
        let test_label_count_after =
            count_dispatches_with_wflow_key(&dispatches_after, "test-label");

        assert_eq!(
            test_label_count_before, test_label_count_after,
            "switch should skip when only unrelated facet (Title) changed; test-label count should not increase"
        );

        ctx.stop().await?;
        Ok(())
    }

    /// DocAdded still triggers switch using facet-key view (no full doc load).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_doc_added_facet_key_matching() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_doc_added").await?;
        crate::test_support::import_test_plug_oci(&ctx).await?;

        let _doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hi".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let mut dispatch_id: Option<String> = None;
        for _ in 0..300 {
            if let Some((id, _dispatch)) =
                ctx.dispatch_repo.get_any_by_wflow_key("test-label").await
            {
                dispatch_id = Some(id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch not found")?;
        ctx.rt
            .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
            .await?;

        ctx.stop().await?;
        Ok(())
    }

    /// DocUpdated on a non-main branch (e.g. "draft") triggers switch and processor dispatch.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_doc_updated_on_custom_branch_triggers_event() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_custom_branch").await?;
        crate::test_support::import_test_plug_oci(&ctx).await?;

        let doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    WellKnownFacetTag::TitleGeneric.into(),
                    daybook_types::doc::WellKnownFacet::TitleGeneric("Initial title".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let main_heads = ctx
            .drawer_repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("doc branches missing")?
            .branches
            .get("main")
            .ok_or_eyre("main heads missing")?
            .clone();

        ctx.drawer_repo
            .create_branch_at_heads_from_branch(
                &doc_id,
                daybook_types::doc::BranchPath::new("/user/draft"),
                daybook_types::doc::BranchPath::new("main"),
                &main_heads,
                None,
            )
            .await?;

        // Update the /user/draft branch with a Note facet (which matches the test-label processor)
        ctx.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        WellKnownFacetTag::Note.into(),
                        daybook_types::doc::WellKnownFacet::Note("Hi on draft branch".into())
                            .into(),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::new("/user/draft"),
                None,
            )
            .await?;

        let mut dispatch_id: Option<String> = None;
        for _ in 0..300 {
            if let Some((id, _dispatch)) =
                ctx.dispatch_repo.get_any_by_wflow_key("test-label").await
            {
                dispatch_id = Some(id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch not found")?;
        ctx.rt
            .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
            .await?;

        ctx.stop().await?;
        Ok(())
    }

    /// Tests that SwitchStore correctly persists cursors and doc states across worker lifecycle.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_persists_cursor_and_doc_state() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_cursor_resume").await?;
        crate::test_support::import_test_plug_oci(&ctx).await?;

        let doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Persisted note".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // Wait for processor dispatch to confirm switch processed the partition event
        let mut dispatch_seen = false;
        for _ in 0..300 {
            if ctx
                .dispatch_repo
                .get_any_by_wflow_key("test-label")
                .await
                .is_some()
            {
                dispatch_seen = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dispatch_seen, "dispatch should be recorded");

        let entry = ctx
            .drawer_repo
            .get_entry(&doc_id)
            .await?
            .ok_or_eyre("doc entry missing")?;
        let branch_doc_id = entry
            .branches
            .get("main")
            .ok_or_eyre("main branch ref missing")?
            .branch_doc_id
            .to_string();
        let switch_store = SwitchStore::load(ctx.rt.rcx.sql.clone()).await?;
        let mut state = None;
        for _ in 0..300 {
            if let Some(s) = switch_store
                .get_doc_state_by_branch_doc_id(&branch_doc_id)
                .await?
            {
                state = Some(s);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        assert!(
            state.is_some(),
            "doc state must be recorded in switch_store"
        );
        let state = state.unwrap();
        assert_eq!(state.doc_id, doc_id);
        assert!(state.last_heads.is_some(), "last_heads must be recorded");

        let part_id = big_repo::automerge_docs_part_id().to_string();
        let cursor = switch_store.get_partition_cursor(&part_id).await?;
        assert!(
            cursor > 0,
            "partition cursor must be advanced in switch_store"
        );

        ctx.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_order_is_deterministic() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [
            (
                "zeta".to_string(),
                Box::new(TestListener {
                    name: "zeta".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_doc: false,
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
            (
                "alpha".to_string(),
                Box::new(TestListener {
                    name: "alpha".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_doc: false,
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
        ]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Dispatch(Arc::new(DispatchEvent::DispatchDeleted {
                id: "hello".into(),
                heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: "test-actor".into(),
                },
            })),
        )
        .await?;
        let called = calls
            .lock()
            .expect("switch test call lock poisoned")
            .clone();
        assert_eq!(called, vec!["alpha".to_string(), "zeta".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_routes_by_interest() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [
            (
                "dispatch_only".to_string(),
                Box::new(TestListener {
                    name: "dispatch_only".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_doc: false,
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
            (
                "config_only".to_string(),
                Box::new(TestListener {
                    name: "config_only".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_doc: false,
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: false,
                        consume_config: true,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
        ]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Dispatch(Arc::new(DispatchEvent::DispatchDeleted {
                id: "hello".into(),
                heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: "test-actor".into(),
                },
            })),
        )
        .await?;
        let called = calls
            .lock()
            .expect("switch test call lock poisoned")
            .clone();
        assert_eq!(called, vec!["dispatch_only".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_predicate_update_applies() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [(
            "predicated".to_string(),
            Box::new(TestListener {
                name: "predicated".to_string(),
                calls: StdArc::clone(&calls),
                interest: SwtchSinkInterest {
                    consume_doc: true,
                    consume_drawer: true,
                    consume_plugs: true,
                    consume_dispatch: false,
                    consume_config: false,
                    drawer_predicate: None,
                },
                outcome: Some(SwitchSinkOutcome {
                    drawer_predicate_update: Some(
                        daybook_types::manifest::DocPredicateClause::HasTag(
                            daybook_types::manifest::ManifestFacetTag("example.tag".into()),
                        ),
                    ),
                }),
            }) as Box<dyn SwitchSink + Send + Sync>,
        )]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Plugs(Arc::new(PlugsEvent::PlugAdded {
                id: "id".into(),
                heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: "test-actor".into(),
                },
            })),
        )
        .await?;
        let predicate = runtime_listeners[0].drawer_predicate.clone();
        assert!(matches!(
            predicate,
            Some(daybook_types::manifest::DocPredicateClause::HasTag(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_origin_metadata_propagates() -> Res<()> {
        let seen = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [(
            "origin".to_string(),
            Box::new(OriginCaptureListener {
                seen: StdArc::clone(&seen),
            }) as Box<dyn SwitchSink + Send + Sync>,
        )]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);

        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Drawer(Arc::new(DrawerEvent::DocAdded {
                id: "d1".into(),
                entry: crate::drawer::DocNBranches {
                    doc_id: "d1".into(),
                    branches: HashMap::new(),
                },
                drawer_heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: crate::peer_id_from_label("peer-a").to_string(),
                },
            })),
        )
        .await?;
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Plugs(Arc::new(PlugsEvent::ConfigDocsChanged {
                heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Bootstrap,
            })),
        )
        .await?;
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Dispatch(Arc::new(DispatchEvent::DispatchDeleted {
                id: "d".into(),
                heads: ChangeHashSet(Vec::new().into()),
                origin: crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: "actor-a".into(),
                },
            })),
        )
        .await?;
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Config(Arc::new(crate::config::ConfigEvent::SyncDevicesChanged {
                origin: crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: crate::peer_id_from_label("peer-b").to_string(),
                },
            })),
        )
        .await?;

        let got = seen
            .lock()
            .expect("switch test origin lock poisoned")
            .clone();
        assert_eq!(
            got,
            vec![
                crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: crate::peer_id_from_label("peer-a").to_string()
                },
                crate::event_origin::SwitchEventOrigin::Bootstrap,
                crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: "actor-a".into()
                },
                crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: crate::peer_id_from_label("peer-b").to_string()
                }
            ]
        );
        Ok(())
    }
}
