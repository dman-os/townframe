use crate::config::ConfigRepo;
use crate::index::{DocBlobsIndexRepo, DocFacetRefIndexRepo, DocFacetSetIndexRepo};
use crate::interlude::*;
use crate::local_state::SqliteLocalStateRepo;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use daybook_types::manifest;

use daybook_types::doc::{FacetKey, FacetTag};
use std::collections::BTreeMap;
use wash_runtime::{
    host::{Host as WashHost, HostApi},
    types::Component,
    wit::WitInterface,
};
use wflow::{
    wflow_core::partition::{
        job_events::{JobError, JobRunResult},
        log::PartitionLogEntry,
        RetryPolicy,
    },
    wflow_tokio::partition::{
        state::PartitionWorkingState, PartitionLogRef, TokioPartitionWorkerHandle,
    },
};

pub mod dispatch;
pub mod init;
pub mod switch;
pub mod triage;
pub mod wash_plugin;

use dispatch::{
    facet_routine_args_fingerprint, ActiveDispatch, ActiveDispatchArgs, ActiveDispatchDeets,
    DispatchOnSuccessHook, DispatchRepo, FacetRoutineArgs,
};
use init::InitRepo;

pub const PROCESSOR_RUNLOG_PARTITION_ID: &str = "processor-runlog/v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessorRunlogDone {
    pub done_by_peer_id: String,
    pub done_token: String,
    pub done_at: String,
}

pub struct RtConfig {
    pub device_id: String,
    pub startup_progress_task_id: Option<String>,
}

pub struct Rt {
    pub config: RtConfig,
    pub cancel_token: tokio_util::sync::CancellationToken,
    pub plugs_repo: Arc<PlugsRepo>,
    pub drawer: Arc<DrawerRepo>,
    pub config_repo: Arc<ConfigRepo>,
    pub wflow_ingress: Arc<dyn wflow::WflowIngress>,
    pub dispatch_repo: Arc<dispatch::DispatchRepo>,
    pub init_repo: Arc<InitRepo>,
    pub progress_repo: Arc<crate::progress::ProgressRepo>,
    pub big_repo: SharedBigRepo,
    pub wflow_part_state: Arc<PartitionWorkingState>,
    pub wcx: wflow::Ctx,
    pub wash_host: Arc<WashHost>,
    pub wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    pub daybook_plugin: Arc<wash_plugin::DaybookPlugin>,
    pub utils_plugin: Arc<wash_plugin_utils::UtilsPlugin>,
    pub mltools_plugin: Arc<wash_plugin_mltools::MltoolsPlugin>,
    pub blobs_repo: Arc<BlobsRepo>,
    pub doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    pub doc_facet_set_index_repo: Arc<DocFacetSetIndexRepo>,
    pub doc_facet_ref_index_repo: Arc<DocFacetRefIndexRepo>,
    pub sqlite_local_state_repo: Arc<SqliteLocalStateRepo>,
    pub local_actor_id: ActorId,
    local_wflow_part_id: String,
}

pub struct RtStopToken {
    wflow_part_handle: TokioPartitionWorkerHandle,
    rt: Arc<Rt>,
    partition_watcher: tokio::task::JoinHandle<()>,
    switch_worker: switch::SwitchWorkerHandle,
    doc_blobs_index_stop: crate::index::DocBlobsIndexStopToken,
    doc_facet_set_index_stop: crate::index::DocFacetSetIndexStopToken,
    doc_facet_ref_index_stop: crate::index::DocFacetRefIndexStopToken,
    sqlite_local_state_stop: crate::repos::RepoStopToken,
    init_repo_stop: crate::repos::RepoStopToken,
}

impl RtStopToken {
    pub async fn stop(self) -> Res<()> {
        self.rt.cancel_token.cancel();

        // Stop triage worker first to prevent new dispatches from being created
        if let Err(err) = self.switch_worker.stop().await {
            warn!(
                ?err,
                "error stopping switch_worker during shutdown - continuing"
            );
        }

        if let Err(err) = self.doc_facet_set_index_stop.stop().await {
            warn!(
                ?err,
                "error stopping doc_facet_set_index_repo during shutdown - continuing"
            );
        }

        if let Err(err) = self.doc_facet_ref_index_stop.stop().await {
            warn!(
                ?err,
                "error stopping doc_facet_ref_index_repo during shutdown - continuing"
            );
        }

        // FIXME: this is wrong, dispatches are allowed
        // to resume on reboot
        //
        // Wait for all active dispatches to complete
        // let active_dispatches: Vec<String> = self
        //     .rt
        //     .dispatch_repo
        //     .list()
        //     .await
        //     .iter()
        //     .map(|(id, _)| id.clone())
        //     .collect();
        // if !active_dispatches.is_empty() {
        //     info!(
        //         count = active_dispatches.len(),
        //         "waiting for active dispatches to complete"
        //     );
        //     for dispatch_id in active_dispatches {
        //         let _ = self
        //             .rt
        //             .wait_for_dispatch_end(&dispatch_id, Duration::from_secs(30))
        //             .await;
        //     }
        // }

        // Stop wflow partition worker
        if let Err(err) = self.wflow_part_handle.stop().await {
            warn!(
                ?err,
                "error stopping wflow_part_handle during shutdown - continuing"
            );
        }

        if let Err(err) = Arc::clone(&self.rt.wash_host).stop().await.to_eyre() {
            warn!(
                ?err,
                "error stopping wash_host during shutdown - continuing"
            );
        }
        if let Err(err) = self.doc_blobs_index_stop.stop().await {
            warn!(
                ?err,
                "error stopping doc_blobs_index_repo during shutdown - continuing"
            );
        }
        if let Err(err) = self.sqlite_local_state_stop.stop().await {
            warn!(
                ?err,
                "error stopping sqlite_local_state_repo during shutdown - continuing"
            );
        }
        if let Err(err) = self.init_repo_stop.stop().await {
            warn!(
                ?err,
                "error stopping init_repo during shutdown - continuing"
            );
        }

        if let Err(err) =
            utils_rs::wait_on_handle_with_timeout(self.partition_watcher, Duration::from_secs(10))
                .await
        {
            warn!(
                ?err,
                "error waiting for partition_watcher during shutdown - continuing"
            );
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum DispatchArgs {
    DocInvoke {
        doc_id: String,
        branch_path: daybook_types::doc::BranchPath,
        heads: ChangeHashSet,
    },
    DocFacet {
        doc_id: String,
        branch_path: daybook_types::doc::BranchPath,
        heads: ChangeHashSet,
        facet_key: Option<String>,
        wflow_args_json: Option<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum InvokeCommandFromWflowError {
    #[error("{0}")]
    Denied(String),
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

impl Rt {
    async fn emit_startup_progress_status(
        progress_repo: &Arc<crate::progress::ProgressRepo>,
        startup_progress_task_id: Option<&str>,
        message: String,
    ) -> Res<()> {
        let Some(task_id) = startup_progress_task_id else {
            return Ok(());
        };
        progress_repo
            .add_update(
                task_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: Some("App startup".to_string()),
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message,
                    },
                },
            )
            .await
    }

    fn startup_timing_note(
        stage_started: std::time::Instant,
        total_started: std::time::Instant,
    ) -> String {
        let stage_ms = stage_started.elapsed().as_millis();
        let total_ms = total_started.elapsed().as_millis();
        let from_app_start = format!(" from_app_start_ms={}", utils_rs::app_startup_elapsed_ms());
        format!("stage_ms={stage_ms} total_ms={total_ms}{from_app_start}")
    }

    pub fn processor_runlog_item_id(doc_id: &str, processor_full_id: &str) -> String {
        format!("v1|doc:{doc_id}|proc:{processor_full_id}")
    }

    pub async fn get_processor_runlog_done(
        &self,
        doc_id: &str,
        processor_full_id: &str,
    ) -> Res<Option<ProcessorRunlogDone>> {
        let item_id = Self::processor_runlog_item_id(doc_id, processor_full_id);
        let payload = self
            .big_repo
            .partition_store()
            .item_payload(&PROCESSOR_RUNLOG_PARTITION_ID.to_string(), &item_id)
            .await?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let done = serde_json::from_value::<ProcessorRunlogDone>(payload)?;
        Ok(Some(done))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn boot(
        config: RtConfig,
        app_doc_id: DocumentId,
        wflow_db_url: String,
        sql_pool: sqlx::SqlitePool,
        big_repo: SharedBigRepo,
        drawer: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        dispatch_repo: Arc<DispatchRepo>,
        progress_repo: Arc<crate::progress::ProgressRepo>,
        blobs_repo: Arc<BlobsRepo>,
        config_repo: Arc<ConfigRepo>,
        local_actor_id: ActorId,
        local_state_root: PathBuf,
    ) -> Res<(Arc<Self>, RtStopToken)> {
        let total_started = std::time::Instant::now();
        let startup_progress_task_id = config.startup_progress_task_id.clone();
        crate::repo::ensure_expected_partitions_for_docs(
            &big_repo,
            &app_doc_id,
            drawer.drawer_doc_id(),
        )
        .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: ensured partitions".to_string(),
        )
        .await?;

        let wcx = wflow::Ctx::init(&wflow_db_url).await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: initialized wflow ctx".to_string(),
        )
        .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            SqliteLocalStateRepo::boot(local_state_root).await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: loaded sqlite local state".to_string(),
        )
        .await?;

        let stage_started = std::time::Instant::now();
        let (init_repo, init_repo_stop) = InitRepo::load(
            Arc::clone(&big_repo),
            app_doc_id.clone(),
            local_actor_id.clone(),
            sql_pool,
            Arc::clone(&progress_repo),
            startup_progress_task_id.clone(),
        )
        .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded init repo ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;

        let stage_started = std::time::Instant::now();
        let (doc_facet_set_index_repo, doc_facet_set_index_stop) =
            crate::index::DocFacetSetIndexRepo::boot(
                Arc::clone(&drawer),
                Arc::clone(&sqlite_local_state_repo),
            )
            .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded facet-set index ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;
        let stage_started = std::time::Instant::now();
        let (doc_blobs_index_repo, doc_blobs_index_stop) = crate::index::DocBlobsIndexRepo::boot(
            Arc::clone(&drawer),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
        )
        .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded doc-blobs index ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;
        let stage_started = std::time::Instant::now();
        let (doc_facet_ref_index_repo, doc_facet_ref_index_stop) =
            crate::index::DocFacetRefIndexRepo::boot(
                Arc::clone(&drawer),
                Arc::clone(&plugs_repo),
                Arc::clone(&sqlite_local_state_repo),
            )
            .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded facet-ref index ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;

        let wflow_plugin = Arc::new(wash_plugin_wflow::WflowPlugin::new(Arc::clone(
            &wcx.metastore,
        )));
        let daybook_plugin = Arc::new(wash_plugin::DaybookPlugin::new(
            Arc::clone(&drawer),
            Arc::clone(&dispatch_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
            Arc::clone(&config_repo),
            Arc::clone(&plugs_repo),
        ));
        let utils_plugin = wash_plugin_utils::UtilsPlugin::new(wash_plugin_utils::Config {})
            .wrap_err("error creating utils plugin")?;
        let mltools_plugin =
            wash_plugin_mltools::MltoolsPlugin::new(wash_plugin_mltools::Config {})
                .wrap_err("error creating mltools plugin")?;

        let wash_host = wflow::build_wash_host(vec![
            #[allow(clippy::clone_on_ref_ptr)]
            wflow_plugin.clone(),
            #[allow(clippy::clone_on_ref_ptr)]
            daybook_plugin.clone(),
            #[allow(clippy::clone_on_ref_ptr)]
            utils_plugin.clone(),
            #[allow(clippy::clone_on_ref_ptr)]
            mltools_plugin.clone(),
        ])
        .await?;

        let wash_host = wash_host
            .start()
            .await
            .to_eyre()
            .wrap_err("error starting wash host")?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: wash host started".to_string(),
        )
        .await?;

        let mut bundles_to_load: HashSet<(String, String)> = default();
        for (_dispatch_id, dispach) in dispatch_repo.list().await {
            match &dispach.deets {
                ActiveDispatchDeets::Wflow {
                    plug_id,
                    bundle_name,
                    ..
                } => {
                    bundles_to_load.insert((plug_id.clone(), bundle_name.clone()));
                }
            }
        }
        for (plug_id, bundle_name) in bundles_to_load {
            let plug_id_for_log = plug_id.clone();
            let bundle_name_for_log = bundle_name.clone();
            let plug_man = plugs_repo.get(&plug_id).await.ok_or_else(|| {
                ferr!("plug with active dispatch not found in repo: plug={plug_id} bundle={bundle_name}")
            })?;
            let bundle_man = plug_man.wflow_bundles.get(&bundle_name[..]).ok_or_else(|| {
                ferr!("bundle with active dispatch not found in repo: plug={plug_id} bundle={bundle_name}")
            })?;

            let _workload_id = ensure_bundle_workload_running(
                &wcx,
                &wash_host,
                &blobs_repo,
                plug_id,
                bundle_name,
                bundle_man,
            )
            .await?;
            Self::emit_startup_progress_status(
                &progress_repo,
                startup_progress_task_id.as_deref(),
                format!(
                    "rt boot: resumed workload plug={plug_id_for_log} bundle={bundle_name_for_log}"
                ),
            )
            .await?;
        }

        let part_idx = 0;
        let (wflow_part_handle, wflow_part_state) =
            wflow::start_partition_worker(&wcx, Arc::clone(&wflow_plugin), part_idx).await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: partition worker started".to_string(),
        )
        .await?;
        let part_log = PartitionLogRef::new(Arc::clone(&wcx.logstore));
        let wflow_ingress = Arc::new(wflow::ingress::PartitionLogIngress::new(
            part_log,
            Arc::clone(&wcx.metastore),
        ));
        let local_wflow_part_id = format!("{}/{part_idx}", config.device_id);

        let rt = Arc::new(Self {
            config,
            local_wflow_part_id,
            cancel_token: default(),
            plugs_repo,
            drawer,
            big_repo,
            wflow_ingress,
            dispatch_repo,
            init_repo,
            progress_repo,
            wcx,
            wash_host,
            wflow_plugin,
            daybook_plugin,
            utils_plugin,
            mltools_plugin,
            blobs_repo,
            doc_blobs_index_repo: Arc::clone(&doc_blobs_index_repo),
            doc_facet_set_index_repo: Arc::clone(&doc_facet_set_index_repo),
            doc_facet_ref_index_repo: Arc::clone(&doc_facet_ref_index_repo),
            sqlite_local_state_repo,
            config_repo,
            wflow_part_state,
            local_actor_id,
        });
        rt.daybook_plugin.attach_rt(Arc::downgrade(&rt));

        // Ensure init routines are queued at boot according to each init run mode.
        let mut plug_ids = rt
            .plugs_repo
            .list_plugs()
            .await
            .into_iter()
            .map(|plug| plug.id())
            .collect::<Vec<_>>();
        plug_ids.sort();
        for plug_id in plug_ids {
            let _ = rt
                .ensure_plug_init_dispatches(
                    &plug_id,
                    startup_progress_task_id.as_deref(),
                    Some(total_started),
                )
                .await?;
        }
        Self::emit_startup_progress_status(
            &rt.progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: plug init queue complete ({})",
                Self::startup_timing_note(total_started, total_started)
            ),
        )
        .await?;

        // Start the DocTriageWorker to automatically queue jobs when docs are added
        let switch_sinks: BTreeMap<String, Box<dyn crate::rt::switch::SwitchSink + Send + Sync>> =
            [
                // FIXME: rename the methods to switch sinks
                (
                    "doc_processor".to_string(),
                    crate::rt::triage::doc_processor_triage_listener(),
                ),
                (
                    "doc_blobs".to_string(),
                    doc_blobs_index_repo.triage_listener(),
                ),
                (
                    "facet_set".to_string(),
                    doc_facet_set_index_repo.triage_listener(),
                ),
                (
                    "facet_ref".to_string(),
                    doc_facet_ref_index_repo.triage_listener(),
                ),
            ]
            .into();
        let switch_worker =
            crate::rt::switch::spawn_switch_worker(Arc::clone(&rt), app_doc_id, switch_sinks)
                .await?;

        let partition_watcher = tokio::spawn({
            let repo = Arc::clone(&rt);
            async move { repo.keep_up_with_partition().await.unwrap_or_log() }
        });

        Ok((
            Arc::clone(&rt),
            RtStopToken {
                rt,
                partition_watcher,
                switch_worker,
                doc_blobs_index_stop,
                doc_facet_set_index_stop,
                doc_facet_ref_index_stop,
                sqlite_local_state_stop,
                init_repo_stop,
                wflow_part_handle,
            },
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn keep_up_with_partition(&self) -> Res<()> {
        use futures::StreamExt;

        // let dispatch = self
        //     .dispatch_repo
        //     .get(dispatch_id)
        //     .await
        //     .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        //
        // match &dispatch.deets {
        //     ActiveDispatchDeets::Wflow {
        //         entry_id,
        //         wflow_job_id,
        //         ..
        //     } => {}
        // }
        let part_log = PartitionLogRef::new(Arc::clone(&self.wcx.logstore));
        let last_seen_idx = self
            .dispatch_repo
            .get_wflow_part_frontier(&self.local_wflow_part_id)
            .await
            .unwrap_or(0);
        let mut stream = part_log.tail(last_seen_idx);

        loop {
            let entry = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    break;
                }
                entry = stream.next() => {
                    entry
                }
            };
            let Some(entry) = entry else {
                warn!("log stream closed");
                // Stream ended
                break;
            };
            let (idx, entry) = entry?;
            if let Some(entry) = entry {
                if let Err(err) = self.handle_wflow_entry(idx, entry).await {
                    if self.cancel_token.is_cancelled() {
                        debug!(error = %err, "ignoring wflow entry error during shutdown");
                        break;
                    }
                    return Err(err);
                }
            };
            if let Err(err) = self
                .dispatch_repo
                .set_wflow_part_frontier(self.local_wflow_part_id.clone(), idx)
                .await
            {
                if self.cancel_token.is_cancelled() {
                    debug!(error = %err, "ignoring frontier write error during shutdown");
                    break;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    fn ensure_rt_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("rt is shutting down")
        }
        Ok(())
    }

    async fn collect_plug_and_dependency_order(&self, plug_id: &str) -> Res<Vec<String>> {
        fn dep_base_id(dep_id_full: &str) -> Res<String> {
            if dep_id_full.starts_with('@') {
                let without_prefix = dep_id_full
                    .strip_prefix('@')
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                let base = without_prefix
                    .split('@')
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                Ok(format!("@{base}"))
            } else {
                let base = dep_id_full
                    .split('@')
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                Ok(base.to_string())
            }
        }

        let mut permanent = HashSet::new();
        let mut temporary = HashSet::new();
        let mut out = vec![];
        let mut stack: Vec<(String, bool)> = vec![(plug_id.to_string(), false)];
        while let Some((cur, expanded)) = stack.pop() {
            if permanent.contains(&cur) {
                continue;
            }
            if expanded {
                temporary.remove(&cur);
                permanent.insert(cur.clone());
                out.push(cur);
                continue;
            }
            if !temporary.insert(cur.clone()) {
                eyre::bail!("circular plug dependencies detected around {cur}");
            }
            stack.push((cur.clone(), true));
            let plug = self
                .plugs_repo
                .get(&cur)
                .await
                .ok_or_else(|| ferr!("plug not found in repo: {cur}"))?;
            let mut deps = plug
                .dependencies
                .keys()
                .map(|raw| dep_base_id(raw))
                .collect::<Res<Vec<_>>>()?;
            deps.sort();
            for dep in deps.into_iter().rev() {
                if !permanent.contains(&dep) {
                    stack.push((dep, false));
                }
            }
        }
        Ok(out)
    }

    async fn ensure_plug_init_dispatches(
        &self,
        plug_id: &str,
        startup_progress_task_id: Option<&str>,
        total_started: Option<std::time::Instant>,
    ) -> Res<Vec<String>> {
        let stage_started = std::time::Instant::now();
        let order = self.collect_plug_and_dependency_order(plug_id).await?;
        let mut unresolved_init_dispatch_ids = vec![];
        for plug_id in order {
            let plug = self
                .plugs_repo
                .get(&plug_id)
                .await
                .ok_or_else(|| ferr!("plug not found in repo: {plug_id}"))?;
            let mut init_keys = plug.inits.keys().cloned().collect::<Vec<_>>();
            init_keys.sort();

            for init_key in init_keys {
                let init_manifest = plug.inits.get(&init_key).ok_or_else(|| {
                    ferr!("init not found in plug manifest: {plug_id}/{init_key}")
                })?;
                let init_id = init::InitRepo::init_id(&plug_id, &plug.version, &init_key.0);
                if self
                    .init_repo
                    .is_done(&init_manifest.run_mode, &init_id)
                    .await?
                {
                    self.init_repo
                        .report_boot_init_stage(
                            &init_manifest.run_mode,
                            &plug_id,
                            &init_key.0,
                            "already done",
                            init::BootInitProgressContext {
                                startup_progress_task_id_override: startup_progress_task_id
                                    .map(str::to_owned),
                                stage_started,
                                total_started,
                            },
                        )
                        .await?;
                    continue;
                }
                if let Some(running_dispatch_id) =
                    self.init_repo.get_running_dispatch(&init_id).await
                {
                    unresolved_init_dispatch_ids.push(running_dispatch_id);
                    self.init_repo
                        .report_boot_init_stage(
                            &init_manifest.run_mode,
                            &plug_id,
                            &init_key.0,
                            "running",
                            init::BootInitProgressContext {
                                startup_progress_task_id_override: startup_progress_task_id
                                    .map(str::to_owned),
                                stage_started,
                                total_started,
                            },
                        )
                        .await?;
                    continue;
                }
                let daybook_types::manifest::InitDeets::InvokeRoutine { routine_name } =
                    &init_manifest.deets;
                let config_doc_id = self
                    .plugs_repo
                    .get_or_init_plug_config_doc_id(&plug_id, &self.drawer)
                    .await?;
                let config_heads = self
                    .drawer
                    .get_doc_branches(&config_doc_id)
                    .await?
                    .and_then(|doc| doc.branches.get("main").cloned())
                    .ok_or_else(|| ferr!("config doc missing main branch for plug {plug_id}"))?;
                let dispatch_id = self
                    .dispatch_no_gate(
                        &plug_id,
                        &routine_name.0,
                        DispatchArgs::DocFacet {
                            doc_id: config_doc_id,
                            branch_path: daybook_types::doc::BranchPath::from("main"),
                            heads: config_heads,
                            facet_key: None,
                            wflow_args_json: None,
                        },
                        vec![DispatchOnSuccessHook::InitMarkDone {
                            init_id: init_id.clone(),
                            run_mode: init_manifest.run_mode.clone(),
                        }],
                        unresolved_init_dispatch_ids.clone(),
                    )
                    .await?;
                self.init_repo
                    .set_running_dispatch(&init_id, &dispatch_id)
                    .await?;
                unresolved_init_dispatch_ids.push(dispatch_id);
                self.init_repo
                    .report_boot_init_stage(
                        &init_manifest.run_mode,
                        &plug_id,
                        &init_key.0,
                        "queued",
                        init::BootInitProgressContext {
                            startup_progress_task_id_override: startup_progress_task_id
                                .map(str::to_owned),
                            stage_started,
                            total_started,
                        },
                    )
                    .await?;
            }
        }
        Ok(unresolved_init_dispatch_ids)
    }
    #[tracing::instrument(skip(self, entry))]
    async fn handle_wflow_entry(&self, entry_id: u64, entry: PartitionLogEntry) -> Res<()> {
        let PartitionLogEntry::JobEffectResult(event) = entry else {
            return Ok(());
        };
        let Some((dispatch_id, _)) = event.job_id.rsplit_once('-') else {
            return Ok(());
        };
        let Some(dispatch) = self.dispatch_repo.get_active(dispatch_id).await else {
            return Ok(());
        };
        let ActiveDispatchDeets::Wflow {
            wflow_job_id,
            wflow_key,
            ..
        } = &dispatch.deets;
        let Some(wflow_job_id) = wflow_job_id.as_ref() else {
            return Ok(());
        };
        if wflow_job_id.as_str() != event.job_id.as_ref() {
            debug!(
                %dispatch_id,
                active_job_id = %wflow_job_id,
                completed_job_id = %event.job_id,
                wflow_key = %wflow_key,
                "ignoring stale job result for replaced dispatch"
            );
            return Ok(());
        }

        // Get staging branch path from dispatch
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            staging_branch_path,
            ..
        }) = &dispatch.args;

        let is_done = match &event.result {
            JobRunResult::Success { value_json } => {
                info!(?value_json, "success on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Info,
                                message: "dispatch completed successfully".to_string(),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::Aborted => {
                info!("dispatch wflow aborted");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: "dispatch aborted".to_string(),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WorkerErr(err) => {
                error!(?err, "worker error on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Error,
                                message: format!("worker error: {err:?}"),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WflowErr(JobError::Terminal { error_json }) => {
                error!(?error_json, "terminal error on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Error,
                                message: format!("terminal error: {error_json}"),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WflowErr(JobError::Transient {
                error_json,
                retry_policy: None,
            })
            | JobRunResult::WflowErr(JobError::Transient {
                error_json,
                retry_policy: Some(RetryPolicy::Immediate),
            }) => {
                warn!("transient error on dispatch wflow: {error_json:?}");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: format!("transient error, retrying: {error_json}"),
                            },
                        },
                    )
                    .await?;
                false
            }
            JobRunResult::StepEffect(..) | JobRunResult::StepWait(..) => false,
        };
        if is_done {
            // Handle staging branch cleanup based on success/failure
            let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path: target_branch_path,
                ..
            }) = &dispatch.args;

            let mut merged_successfully = matches!(&event.result, JobRunResult::Success { .. });

            if merged_successfully {
                // Merge staging branch into target branch
                info!(
                    %dispatch_id,
                    %entry_id,
                    ?doc_id,
                    ?staging_branch_path,
                    ?target_branch_path,
                    "merging staging branch into target"
                );
                match self
                    .drawer
                    .merge_from_branch(doc_id, target_branch_path, staging_branch_path, None)
                    .await
                {
                    Ok(()) => {}
                    Err(crate::drawer::types::DrawerError::BranchNotFound { name }) => {
                        warn!(
                            %dispatch_id,
                            %entry_id,
                            ?doc_id,
                            ?name,
                            ?staging_branch_path,
                            ?target_branch_path,
                            "staging branch missing during merge; treating dispatch as failed"
                        );
                        merged_successfully = false;
                    }
                    Err(err) => {
                        error!(
                            %dispatch_id,
                            %entry_id,
                            ?doc_id,
                            ?staging_branch_path,
                            ?target_branch_path,
                            ?err,
                            "staging merge returned error"
                        );
                        return Err(eyre::eyre!(err).wrap_err("error merging staging branch"));
                    }
                }

                if merged_successfully {
                    // Delete the staging branch after successful merge
                    info!(
                        %dispatch_id,
                        %entry_id,
                        ?doc_id,
                        ?staging_branch_path,
                        "deleting staging branch after successful merge"
                    );
                    self.drawer
                        .delete_branch(doc_id, staging_branch_path, None)
                        .await
                        .or_else(|err| match err {
                            crate::drawer::types::DrawerError::BranchNotFound { .. } => {
                                debug!(
                                    %dispatch_id,
                                    %entry_id,
                                    ?doc_id,
                                    ?staging_branch_path,
                                    "staging branch already removed after successful merge"
                                );
                                Ok(false)
                            }
                            other => Err(other),
                        })
                        .wrap_err("error deleting staging branch after merge")?;
                }
            }
            if !merged_successfully {
                // Delete staging branch on failure if it exists.
                info!(
                    %dispatch_id,
                    %entry_id,
                    ?doc_id,
                    ?staging_branch_path,
                    "deleting staging branch due to failure"
                );
                self.drawer
                    .delete_branch(doc_id, staging_branch_path, None)
                    .await
                    .or_else(|err| match err {
                        crate::drawer::types::DrawerError::BranchNotFound { .. } => Ok(false),
                        other => Err(other),
                    })
                    .wrap_err("error deleting staging branch")?;
                for hook in &dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, .. } => {
                            self.init_repo
                                .clear_running_dispatch(init_id, dispatch_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorRunLog { .. } => {}
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let reply = {
                                let (status, value_json, error_json) =
                                    command_invoke_reply_from_result(
                                        &event.result,
                                        dispatch_id,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
            } else {
                for hook in &dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, run_mode } => {
                            self.init_repo.mark_done(run_mode, init_id).await?;
                            self.init_repo
                                .clear_running_dispatch(init_id, dispatch_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorRunLog {
                            doc_id,
                            processor_full_id,
                            done_token,
                        } => {
                            self.record_processor_runlog_done(
                                doc_id,
                                processor_full_id,
                                done_token,
                            )
                            .await?;
                        }
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let reply = {
                                let (status, value_json, error_json) =
                                    command_invoke_reply_from_result(
                                        &event.result,
                                        dispatch_id,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
            }

            let final_status = if merged_successfully {
                dispatch::DispatchStatus::Succeeded
            } else if matches!(&event.result, JobRunResult::Aborted) {
                dispatch::DispatchStatus::Cancelled
            } else {
                dispatch::DispatchStatus::Failed
            };
            self.progress_repo
                .add_update(
                    dispatch_id,
                    crate::progress::ProgressUpdate {
                        at: jiff::Timestamp::now(),
                        title: None,
                        deets: crate::progress::ProgressUpdateDeets::Completed {
                            state: if matches!(final_status, dispatch::DispatchStatus::Succeeded) {
                                crate::progress::ProgressFinalState::Succeeded
                            } else if matches!(final_status, dispatch::DispatchStatus::Cancelled) {
                                crate::progress::ProgressFinalState::Cancelled
                            } else {
                                crate::progress::ProgressFinalState::Failed
                            },
                            message: None,
                        },
                    },
                )
                .await?;
            self.dispatch_repo
                .complete(dispatch_id.into(), final_status.clone())
                .await?;
            self.release_waiting_dispatches(
                dispatch_id,
                matches!(final_status, dispatch::DispatchStatus::Succeeded),
            )
            .await?;
        }
        Ok(())
    }

    async fn release_waiting_dispatches(
        &self,
        completed_dispatch_id: &str,
        is_success: bool,
    ) -> Res<()> {
        let waiting_dispatches = self
            .dispatch_repo
            .list_waiting_on(completed_dispatch_id)
            .await;
        for (waiting_id, waiting_dispatch) in waiting_dispatches {
            if !is_success {
                self.dispatch_repo.set_waiting_failed(&waiting_id).await?;
                self.progress_repo
                    .add_update(
                        &waiting_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Completed {
                                state: crate::progress::ProgressFinalState::Failed,
                                message: Some(
                                    "dependency dispatch failed; waiting dispatch cancelled"
                                        .to_string(),
                                ),
                            },
                        },
                    )
                    .await?;
                for hook in &waiting_dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, .. } => {
                            self.init_repo
                                .clear_running_dispatch(init_id, &waiting_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorRunLog { .. } => {}
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let error_json = serde_json::json!({
                                "kind": "dependency-failed",
                                "dispatch_id": waiting_id,
                                "message": "dependency dispatch failed before command invocation could run",
                            });
                            let reply = daybook_pdk::InvokeCommandReply {
                                request_id: request_id.clone(),
                                status: daybook_pdk::InvokeCommandStatus::Failed,
                                value_json: None,
                                error_json: Some(error_json.to_string()),
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending failed command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
                continue;
            }
            if let Some(ready_dispatch) = self
                .dispatch_repo
                .remove_waiting_dependency(&waiting_id, completed_dispatch_id)
                .await?
            {
                self.start_waiting_dispatch(waiting_id, ready_dispatch)
                    .await?;
            }
        }
        Ok(())
    }

    async fn record_processor_runlog_done(
        &self,
        doc_id: &str,
        processor_full_id: &str,
        done_token: &str,
    ) -> Res<()> {
        upsert_processor_runlog_item(
            self.big_repo.partition_store().as_ref(),
            &self.config.device_id,
            doc_id,
            processor_full_id,
            done_token,
        )
        .await
    }

    async fn start_waiting_dispatch(
        &self,
        dispatch_id: String,
        waiting_dispatch: Arc<ActiveDispatch>,
    ) -> Res<()> {
        let ActiveDispatchDeets::Wflow {
            plug_id,
            routine_name,
            bundle_name,
            wflow_key,
            wflow_job_id,
            ..
        } = &waiting_dispatch.deets;
        let Some(job_id) = wflow_job_id.as_ref() else {
            eyre::bail!("waiting dispatch missing job id: {dispatch_id}");
        };
        let plug_man = self
            .plugs_repo
            .get(plug_id)
            .await
            .ok_or_else(|| ferr!("plug not found in repo: {plug_id}"))?;
        let bundle_man = plug_man
            .wflow_bundles
            .get(bundle_name.as_str())
            .ok_or_else(|| {
                ferr!("bundle not found in plug manifest: plug={plug_id} bundle={bundle_name}")
            })?;
        let key: daybook_types::manifest::KeyGeneric = wflow_key.clone().into();
        let job_args_json = match &waiting_dispatch.args {
            ActiveDispatchArgs::FacetRoutine(facet_args) => facet_args
                .wflow_args_json
                .clone()
                .unwrap_or_else(|| serde_json::to_string(&()).expect(ERROR_JSON)),
        };
        let _workload_id = ensure_bundle_workload_running(
            &self.wcx,
            &self.wash_host,
            &self.blobs_repo,
            plug_id.clone(),
            bundle_name.clone(),
            bundle_man,
        )
        .await?;
        let initial_deets = ActiveDispatchDeets::Wflow {
            wflow_partition_id: None,
            entry_id: None,
            plug_id: plug_id.clone(),
            routine_name: routine_name.clone(),
            bundle_name: bundle_name.clone(),
            wflow_key: wflow_key.clone(),
            wflow_job_id: Some(job_id.clone()),
        };
        self.dispatch_repo
            .activate_waiting(&dispatch_id, initial_deets)
            .await?;
        let entry_id = match self
            .wflow_ingress
            .add_job(job_id.clone().into(), &key, job_args_json, None)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                self.dispatch_repo
                    .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                    .await?;
                return Err(err).wrap_err_with(|| {
                    format!("error scheduling deferred job for dispatch {dispatch_id}")
                });
            }
        };
        let deets = ActiveDispatchDeets::Wflow {
            wflow_partition_id: Some(self.local_wflow_part_id.clone()),
            entry_id: Some(entry_id),
            plug_id: plug_id.clone(),
            routine_name: routine_name.clone(),
            bundle_name: bundle_name.clone(),
            wflow_key: wflow_key.clone(),
            wflow_job_id: Some(job_id.clone()),
        };
        if let Err(err) = self
            .dispatch_repo
            .update_active_deets(&dispatch_id, deets)
            .await
        {
            let _ = self
                .wflow_ingress
                .cancel_job(
                    Arc::from(job_id.as_ref()),
                    format!("rollback scheduling for dispatch {dispatch_id}"),
                )
                .await;
            return Err(err);
        }
        self.progress_repo
            .add_update(
                &dispatch_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: None,
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message: "dependencies resolved, dispatch queued".to_string(),
                    },
                },
            )
            .await?;
        Ok(())
    }

    pub async fn dispatch(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
    ) -> Res<String> {
        self.dispatch_raw(plug_id, routine_name, args, vec![]).await
    }

    pub async fn invoke_command_from_wflow_job(
        &self,
        parent_wflow_job_id: &str,
        target_command_url: &str,
        request: daybook_pdk::InvokeCommandRequest,
    ) -> Result<String, InvokeCommandFromWflowError> {
        self.ensure_rt_live()?;
        let parent_dispatch = self
            .dispatch_repo
            .get_by_wflow_job(parent_wflow_job_id)
            .await
            .ok_or_else(|| {
                ferr!("no active dispatch found for parent job: {parent_wflow_job_id}")
            })?;
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            doc_id,
            staging_branch_path,
            command_invoke_acl_snapshot,
            facet_key: _,
            ..
        }) = &parent_dispatch.args;
        let ActiveDispatchDeets::Wflow {
            plug_id: _,
            routine_name,
            ..
        } = &parent_dispatch.deets;

        let target_ref = daybook_pdk::parse_command_url_str(target_command_url)
            .map_err(|err| ferr!("invalid command URL in invoke token: {err}"))?;
        let mut is_allowed = false;
        for allowlisted_url in command_invoke_acl_snapshot {
            let parsed = daybook_pdk::parse_command_url(allowlisted_url).map_err(|err| {
                ferr!(
                    "invalid command_invoke_acl_snapshot entry '{}': {err}",
                    allowlisted_url
                )
            })?;
            if parsed.plug_id == target_ref.plug_id
                && parsed.command_name == target_ref.command_name
            {
                is_allowed = true;
                break;
            }
        }
        if !is_allowed {
            return Err(InvokeCommandFromWflowError::Denied(format!(
                "command target '{}' is not allowlisted by routine '{}'",
                target_command_url, routine_name
            )));
        }

        let target_plug_manifest =
            self.plugs_repo
                .get(&target_ref.plug_id)
                .await
                .ok_or_else(|| {
                    ferr!(
                        "target plug not found in command URL: {}",
                        target_ref.plug_id
                    )
                })?;
        let target_command_manifest = target_plug_manifest
            .commands
            .get(target_ref.command_name.as_str())
            .ok_or_else(|| {
                ferr!(
                    "target command not found in command URL: {}/{}",
                    target_ref.plug_id,
                    target_ref.command_name
                )
            })?;
        let manifest::CommandDeets::DocCommand {
            routine_name: target_routine_name,
        } = &target_command_manifest.deets;

        let fixed_dispatch_id = {
            let mut identity = String::new();
            use std::fmt::Write as _;
            write!(
                &mut identity,
                "{}|{}|{}",
                dispatch_stable_identity(&parent_dispatch),
                target_command_url,
                request.request_id
            )
            .expect("writing to string should never fail");
            let encoded = utils_rs::hash::blake3_hash_bytes(identity.as_bytes());
            format!("cmdinvoke-{encoded}")
        };
        let waiting_on_dispatch_ids = self
            .ensure_plug_init_dispatches(&target_ref.plug_id, None, None)
            .await?;
        let staging_heads = self
            .drawer
            .get_doc_branches(doc_id)
            .await?
            .and_then(|entry| entry.branches.get(staging_branch_path.as_str()).cloned())
            .ok_or_else(|| {
                ferr!(
                    "missing staging branch heads for command invoke: doc_id={doc_id} branch={}",
                    staging_branch_path.as_str()
                )
            })?;

        self.dispatch_no_gate_internal(
            &target_ref.plug_id,
            &target_routine_name.0,
            DispatchArgs::DocFacet {
                doc_id: doc_id.clone(),
                branch_path: staging_branch_path.clone(),
                heads: staging_heads,
                facet_key: None,
                wflow_args_json: Some(request.args_json.clone()),
            },
            vec![DispatchOnSuccessHook::CommandInvokeReply {
                parent_wflow_job_id: parent_wflow_job_id.to_string(),
                request_id: request.request_id,
            }],
            waiting_on_dispatch_ids,
            Some(fixed_dispatch_id),
            true,
        )
        .await
        .map_err(InvokeCommandFromWflowError::Other)
    }

    async fn dispatch_raw(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
    ) -> Res<String> {
        self.ensure_rt_live()?;
        let waiting_on_dispatch_ids = self
            .ensure_plug_init_dispatches(plug_id, None, None)
            .await?;
        self.dispatch_no_gate_internal(
            plug_id,
            routine_name,
            args,
            on_success_hooks,
            waiting_on_dispatch_ids,
            None,
            false,
        )
        .await
    }

    async fn dispatch_no_gate(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
        waiting_on_dispatch_ids: Vec<String>,
    ) -> Res<String> {
        self.dispatch_no_gate_internal(
            plug_id,
            routine_name,
            args,
            on_success_hooks,
            waiting_on_dispatch_ids,
            None,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_no_gate_internal(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
        waiting_on_dispatch_ids: Vec<String>,
        fixed_dispatch_id: Option<String>,
        reuse_terminal_on_match: bool,
    ) -> Res<String> {
        self.ensure_rt_live()?;

        let plug_man = self
            .plugs_repo
            .get(plug_id)
            .await
            .ok_or_else(|| ferr!("plug not found in repo: {plug_id}/{routine_name}"))?;
        let routine_man = plug_man
            .routines
            .get(routine_name)
            .ok_or_else(|| ferr!("routine not found in plug manifest: {plug_id}/{routine_name}"))?;

        use daybook_types::manifest::RoutineManifestDeets;
        let (mut dispatch_id, mut args) = match (&routine_man.deets, args) {
            (
                RoutineManifestDeets::DocFacet {
                    working_facet_tag, ..
                },
                DispatchArgs::DocFacet {
                    doc_id,
                    heads,
                    branch_path,
                    facet_key,
                    wflow_args_json,
                },
            ) => {
                let tag: FacetTag = working_facet_tag.0.clone().into();
                let facet_key = match facet_key {
                    Some(id) => FacetKey { tag, id },
                    None => tag.into(),
                };
                let facet_key = facet_key.to_string();
                let dispatch_id = {
                    let mut identity = String::new();
                    use std::fmt::Write as _;
                    write!(
                        &mut identity,
                        "{}|{}|{}|{}",
                        doc_id,
                        am_utils_rs::serialize_commit_heads(heads.as_ref()).join(","),
                        plug_id,
                        routine_name
                    )
                    .expect("writing to string should never fail");
                    utils_rs::hash::blake3_hash_bytes(identity.as_bytes())
                };
                let dispatch_id = fixed_dispatch_id
                    .clone()
                    .unwrap_or_else(|| format!("{plug_id}/{routine_name}-{dispatch_id}"));
                (
                    dispatch_id,
                    ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                        doc_id,
                        branch_path,
                        heads,
                        facet_key,
                        facet_acl: routine_man.facet_acl().to_vec(),
                        config_facet_acl: routine_man.config_facet_acl().to_vec(),
                        local_state_acl: routine_man.local_state_acl.clone(),
                        command_invoke_acl_snapshot: routine_man.command_invoke_acl().to_vec(),
                        wflow_args_json,
                        staging_branch_path: daybook_types::doc::BranchPath::from(
                            "/tmp/placeholder",
                        ), // Will be set when job is created
                    }),
                )
            }
            (deets, args) => {
                return Err(ferr!(
                    "routine type and args don't match: {deets:?}, {args:?}"
                ));
            }
        };
        if let Some(existing) = self.dispatch_repo.get_any(&dispatch_id).await {
            let can_reuse = serde_json::to_string(&existing.on_success_hooks).expect(ERROR_JSON)
                == serde_json::to_string(&on_success_hooks).expect(ERROR_JSON)
                && existing.waiting_on_dispatch_ids == waiting_on_dispatch_ids;
            let reuse_status_ok = matches!(
                existing.status,
                dispatch::DispatchStatus::Waiting | dispatch::DispatchStatus::Active
            );
            if can_reuse && reuse_status_ok {
                warn!(?dispatch_id, "dispatch already exists with same identity");
                return Ok(dispatch_id);
            }
            if can_reuse && reuse_terminal_on_match {
                debug!(
                    ?dispatch_id,
                    status = ?existing.status,
                    "skipping terminal dispatch reuse without CommandInvokeReply replay"
                );
            }
            dispatch_id = format!("{dispatch_id}-{}", Uuid::new_v4().bs58());
        }

        let is_waiting = !waiting_on_dispatch_ids.is_empty();

        let deets = match &routine_man.r#impl {
            manifest::RoutineImpl::Wflow {
                key,
                bundle: bundle_name,
            } => {
                // let fqk = format!("{workload_id}/{key}");
                let job_id = format!("{dispatch_id}-{id}", id = Uuid::new_v4().bs58());
                let staging_branch_path =
                    daybook_types::doc::BranchPath::from(format!("/tmp/{}", job_id));

                // Update args with staging branch path
                let ActiveDispatchArgs::FacetRoutine(ref mut facet_args) = args;
                facet_args.staging_branch_path = staging_branch_path.clone();

                let mut entry_id = None;
                let mut wflow_partition_id = None;
                if !is_waiting {
                    let bundle_man = plug_man.wflow_bundles.get(bundle_name).ok_or_else(|| {
                        ferr!(
                            "bundle not found in plug manifest: routine={plug_id}/{routine_name} bundle={bundle_name} key={key}"
                        )
                    })?;
                    let _workload_id = ensure_bundle_workload_running(
                        &self.wcx,
                        &self.wash_host,
                        &self.blobs_repo,
                        plug_id.into(),
                        bundle_name.0.clone(),
                        bundle_man,
                    )
                    .await?;

                    let wflow_args_json = {
                        let ActiveDispatchArgs::FacetRoutine(ref facet_args) = args;
                        facet_args
                            .wflow_args_json
                            .clone()
                            .unwrap_or_else(|| serde_json::to_string(&()).expect(ERROR_JSON))
                    };
                    entry_id = Some(
                        self.wflow_ingress
                            .add_job(job_id.clone().into(), key, wflow_args_json, None)
                            .await
                            .wrap_err_with(|| {
                                format!("error scheduling job for {plug_id}/{routine_name}")
                            })?,
                    );
                    wflow_partition_id = Some(self.local_wflow_part_id.clone());
                }

                ActiveDispatchDeets::Wflow {
                    wflow_partition_id,
                    entry_id,
                    plug_id: plug_id.into(),
                    routine_name: routine_name.to_string(),
                    bundle_name: bundle_name.0.clone(),
                    wflow_key: key.0.clone(),
                    wflow_job_id: Some(job_id),
                }
            }
        };
        let active_dispatch = Arc::new(ActiveDispatch {
            args,
            deets,
            status: if is_waiting {
                dispatch::DispatchStatus::Waiting
            } else {
                dispatch::DispatchStatus::Active
            },
            waiting_on_dispatch_ids,
            on_success_hooks,
        });
        let ActiveDispatchArgs::FacetRoutine(args) = &active_dispatch.args;
        debug!(
            %dispatch_id,
            arg_fingerprint = %facet_routine_args_fingerprint(args),
            doc_id = ?args.doc_id,
            branch_path = %args.branch_path,
            staging_branch_path = %args.staging_branch_path,
            heads = ?am_utils_rs::serialize_commit_heads(args.heads.as_ref()),
            "dispatch_no_gate_internal prepared dispatch args"
        );
        if let Err(add_err) = self
            .dispatch_repo
            .add(dispatch_id.clone(), Arc::clone(&active_dispatch))
            .await
        {
            let ActiveDispatchDeets::Wflow {
                wflow_job_id,
                entry_id,
                ..
            } = &active_dispatch.deets;
            if entry_id.is_some() {
                if let Some(wflow_job_id) = wflow_job_id.as_ref() {
                    if let Err(cancel_err) = self
                        .wflow_ingress
                        .cancel_job(
                            Arc::from(wflow_job_id.as_ref()),
                            format!(
                                "rollback scheduling for dispatch {dispatch_id} after dispatch store failure"
                            ),
                        )
                        .await
                    {
                        warn!(
                            %dispatch_id,
                            %wflow_job_id,
                            ?cancel_err,
                            "failed to rollback queued wflow job after dispatch add failure"
                        );
                    }
                }
            }
            return Err(add_err);
        }
        let mut tags = vec![
            "/type/dispatch".to_string(),
            format!("/dispatch/{dispatch_id}"),
        ];
        let title = match &active_dispatch.args {
            ActiveDispatchArgs::FacetRoutine(facet_args) => {
                tags.push(format!("/docs/{}", facet_args.doc_id));
                dispatch_id.clone()
            }
        };
        self.progress_repo
            .upsert_task(crate::progress::CreateProgressTaskArgs {
                id: dispatch_id.clone(),
                tags,
                retention: crate::progress::ProgressRetentionPolicy::UserDismissable,
            })
            .await?;
        self.progress_repo
            .add_update(
                &dispatch_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: Some(title),
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message: if is_waiting {
                            "dispatch waiting on dependencies".to_string()
                        } else {
                            "dispatch queued".to_string()
                        },
                    },
                },
            )
            .await?;
        Ok(dispatch_id)
    }

    pub async fn cancel_dispatch(&self, dispatch_id: &str) -> Res<()> {
        self.ensure_rt_live()?;

        let dispatch = self
            .dispatch_repo
            .get_any(dispatch_id)
            .await
            .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        if matches!(
            dispatch.status,
            dispatch::DispatchStatus::Succeeded
                | dispatch::DispatchStatus::Failed
                | dispatch::DispatchStatus::Cancelled
        ) {
            return Ok(());
        }
        if matches!(dispatch.status, dispatch::DispatchStatus::Waiting) {
            self.dispatch_repo
                .complete(dispatch_id.into(), dispatch::DispatchStatus::Cancelled)
                .await?;
            self.release_waiting_dispatches(dispatch_id, false).await?;
            self.progress_repo
                .add_update(
                    dispatch_id,
                    crate::progress::ProgressUpdate {
                        at: jiff::Timestamp::now(),
                        title: None,
                        deets: crate::progress::ProgressUpdateDeets::Completed {
                            state: crate::progress::ProgressFinalState::Cancelled,
                            message: Some("dispatch cancelled while waiting".to_string()),
                        },
                    },
                )
                .await?;
            return Ok(());
        }
        let marked_now = self.dispatch_repo.mark_cancelled(dispatch_id).await?;
        if !marked_now {
            debug!(%dispatch_id, "cancel already requested; skipping duplicate cancel");
            return Ok(());
        }
        match &dispatch.deets {
            ActiveDispatchDeets::Wflow {
                wflow_job_id,
                entry_id,
                ..
            } => {
                let Some(wflow_job_id) = wflow_job_id.as_ref() else {
                    return Ok(());
                };
                if entry_id.is_none() {
                    return Ok(());
                }
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: "cancellation requested".to_string(),
                            },
                        },
                    )
                    .await?;
                self.wflow_ingress
                    .cancel_job(
                        Arc::from(wflow_job_id.as_ref()),
                        format!("cancel requested for dispatch {dispatch_id}"),
                    )
                    .await
                    .wrap_err_with(|| format!("error cancelling dispatch {dispatch_id}"))?;
            }
        }
        Ok(())
    }

    /// Wait until a log entry matches the provided condition
    /// The callback receives (entry_id, log_entry) and should return true when the condition is met
    pub async fn wait_for_dispatch_end(
        &self,
        dispatch_id: &str,
        timeout: std::time::Duration,
    ) -> Res<()> {
        self.ensure_rt_live()?;

        use crate::repos::{Repo, SubscribeOpts};

        let listener_handle = self.dispatch_repo.subscribe(SubscribeOpts::new(128));

        // check if the dispatch exists first
        let Some(dispatch) = self.dispatch_repo.get_any(dispatch_id).await else {
            return Ok(());
        };
        if matches!(
            dispatch.status,
            dispatch::DispatchStatus::Succeeded
                | dispatch::DispatchStatus::Failed
                | dispatch::DispatchStatus::Cancelled
        ) {
            return Ok(());
        }

        tokio::time::timeout(timeout, async {
            loop {
                let event = listener_handle
                    .recv_async()
                    .await
                    .map_err(|err| eyre::eyre!("dispatch listener closed: {err:?}"))?;
                match &*event {
                    dispatch::DispatchEvent::DispatchDeleted { id, .. } if id == dispatch_id => {
                        return Ok::<(), eyre::Report>(());
                    }
                    dispatch::DispatchEvent::DispatchUpdated { id, .. }
                    | dispatch::DispatchEvent::DispatchAdded { id, .. }
                        if id == dispatch_id =>
                    {
                        if let Some(cur) = self.dispatch_repo.get_any(dispatch_id).await {
                            if matches!(
                                cur.status,
                                dispatch::DispatchStatus::Succeeded
                                    | dispatch::DispatchStatus::Failed
                                    | dispatch::DispatchStatus::Cancelled
                            ) {
                                return Ok::<(), eyre::Report>(());
                            }
                        } else {
                            return Ok::<(), eyre::Report>(());
                        }
                    }
                    _ => {}
                }
            }
        })
        .await??;

        Ok(())
    }
}

async fn upsert_processor_runlog_item(
    partition_store: &am_utils_rs::partition::PartitionStore,
    done_by_peer_id: &str,
    doc_id: &str,
    processor_full_id: &str,
    done_token: &str,
) -> Res<()> {
    let item_id = Rt::processor_runlog_item_id(doc_id, processor_full_id);
    let payload = serde_json::json!({
        "done_by_peer_id": done_by_peer_id,
        "done_token": done_token,
        "done_at": jiff::Timestamp::now().to_string(),
    });
    partition_store
        .record_item_change(
            &PROCESSOR_RUNLOG_PARTITION_ID.to_string(),
            &item_id,
            &payload,
        )
        .await
}

fn dispatch_stable_identity(dispatch: &ActiveDispatch) -> String {
    match (&dispatch.deets, &dispatch.args) {
        (
            ActiveDispatchDeets::Wflow {
                plug_id,
                routine_name,
                ..
            },
            ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path,
                heads,
                facet_key,
                ..
            }),
        ) => format!(
            "{plug_id}/{routine_name}|{doc_id}|{}|{}|{facet_key}",
            branch_path.as_str(),
            serde_json::to_string(heads).expect(ERROR_JSON)
        ),
    }
}

fn command_invoke_reply_from_result(
    result: &JobRunResult,
    dispatch_id: &str,
    merged_successfully: bool,
) -> (
    daybook_pdk::InvokeCommandStatus,
    Option<String>,
    Option<String>,
) {
    if !merged_successfully {
        let error_json = serde_json::json!({
            "kind": "merge-failed",
            "dispatch_id": dispatch_id,
            "message": "workflow run succeeded but post-run branch merge failed",
        });
        return (
            daybook_pdk::InvokeCommandStatus::Failed,
            None,
            Some(error_json.to_string()),
        );
    }
    match result {
        JobRunResult::Success { value_json } => (
            daybook_pdk::InvokeCommandStatus::Succeeded,
            Some(value_json.to_string()),
            None,
        ),
        JobRunResult::Aborted => (daybook_pdk::InvokeCommandStatus::Cancelled, None, None),
        JobRunResult::WflowErr(JobError::Terminal { error_json }) => (
            daybook_pdk::InvokeCommandStatus::Failed,
            None,
            Some(error_json.to_string()),
        ),
        JobRunResult::WorkerErr(err) => {
            let error_json = serde_json::json!({
                "kind": "worker-error",
                "dispatch_id": dispatch_id,
                "error": format!("{err:?}"),
            });
            (
                daybook_pdk::InvokeCommandStatus::Failed,
                None,
                Some(error_json.to_string()),
            )
        }
        JobRunResult::WflowErr(JobError::Transient { error_json, .. }) => {
            let wrapped = serde_json::json!({
                "kind": "transient-exhausted",
                "dispatch_id": dispatch_id,
                "error_json": error_json,
            });
            (
                daybook_pdk::InvokeCommandStatus::Failed,
                None,
                Some(wrapped.to_string()),
            )
        }
        JobRunResult::StepEffect(_) | JobRunResult::StepWait(_) => {
            unreachable!("non-terminal result reached terminal invoke reply")
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    async fn make_partition_store() -> Res<am_utils_rs::partition::PartitionStore> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let (events_tx, _) = broadcast::channel(1024);
        let store = am_utils_rs::partition::PartitionStore::new(
            pool,
            events_tx,
            CancellationToken::new(),
            std::sync::Arc::new(utils_rs::AbortableJoinSet::new()),
        );
        store.ensure_schema().await?;
        store
            .ensure_partition(&PROCESSOR_RUNLOG_PARTITION_ID.to_string())
            .await?;
        Ok(store)
    }

    #[tokio::test]
    async fn processor_runlog_upsert_is_bounded_for_same_doc_processor() -> Res<()> {
        let store = make_partition_store().await?;
        let item_id = Rt::processor_runlog_item_id("doc-1", "@daybook/plabels/label-note");

        upsert_processor_runlog_item(
            &store,
            "peer-a",
            "doc-1",
            "@daybook/plabels/label-note",
            "token-1",
        )
        .await?;
        upsert_processor_runlog_item(
            &store,
            "peer-a",
            "doc-1",
            "@daybook/plabels/label-note",
            "token-2",
        )
        .await?;

        let count = store
            .item_row_count(&PROCESSOR_RUNLOG_PARTITION_ID.to_string(), &item_id)
            .await?;
        assert_eq!(count, 1, "runlog should overwrite in-place for same key");

        let payload = store
            .item_payload(&PROCESSOR_RUNLOG_PARTITION_ID.to_string(), &item_id)
            .await?
            .ok_or_eyre("expected runlog payload row")?;
        assert_eq!(payload["done_by_peer_id"], serde_json::json!("peer-a"));
        assert_eq!(payload["done_token"], serde_json::json!("token-2"));

        Ok(())
    }
}

async fn ensure_bundle_workload_running(
    wcx: &wflow::Ctx,
    wash_host: &WashHost,
    blobs_repo: &BlobsRepo,
    plug_id: String,
    bundle_name: String,
    bundle_man: &manifest::WflowBundleManifest,
) -> Res<String> {
    let workload_id = format!("{plug_id}/{bundle_name}");
    for key in &bundle_man.keys {
        use wflow::wflow_core::metastore::*;
        match wcx.metastore.get_wflow(key).await? {
            Some(meta) => {
                if let WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                    workload_id: meta_workload_id,
                }) = &meta.service
                {
                    if meta_workload_id != &workload_id {
                        eyre::bail!(
                            "wflow under key '{key}' in metatstore '{meta:?}' doesn't match workload id '{workload_id}'",
                        );
                    }
                } else {
                    eyre::bail!(
                        "wflow under key '{key}' in metatstore '{meta:?}' doesn't match workload type for workload '{workload_id}'",
                    );
                }
            }
            None => {
                wcx.metastore
                    .set_wflow(
                        &key[..],
                        &WflowMeta {
                            key: key.0.clone(),
                            service: WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                                workload_id: workload_id.clone(),
                            }),
                        },
                    )
                    .await?;
            }
        }
    }
    let has_workload = wash_host
        .workload_status(wash_runtime::types::WorkloadStatusRequest {
            workload_id: workload_id.clone(),
        })
        .await
        .ok()
        .map(|status| match &status.workload_status.workload_state {
            wash_runtime::types::WorkloadState::Starting
            | wash_runtime::types::WorkloadState::Running => true,
            wash_runtime::types::WorkloadState::NotFound => false,
            wash_runtime::types::WorkloadState::Unspecified
            | wash_runtime::types::WorkloadState::Completed
            | wash_runtime::types::WorkloadState::Stopping
            | wash_runtime::types::WorkloadState::Error => {
                panic!("unexpected workload status: {status:?}")
            }
        })
        .unwrap_or_default();
    if !has_workload {
        start_bundle_workload(
            wash_host,
            blobs_repo,
            workload_id.clone(),
            plug_id,
            bundle_name,
            bundle_man,
        )
        .await
        .wrap_err("error starting bundle wflow")?;
    }
    Ok(workload_id)
}

async fn start_bundle_workload(
    wash_host: &WashHost,
    blobs_repo: &BlobsRepo,
    workload_id: String,
    plug_id: String,
    bundle_name: String,
    bundle_man: &manifest::WflowBundleManifest,
) -> Res<()> {
    // Load wasm bytes from component URLs
    let mut components = Vec::new();
    for url in &bundle_man.component_urls {
        let wasm_bytes = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| eyre::eyre!("invalid file path in url: {}", url))?;
                tokio::fs::read(&path).await.wrap_err_with(|| {
                    format!("failed to read component file: {}", path.display())
                })?
            }
            scheme if scheme == crate::blobs::BLOB_SCHEME => {
                let hash = url.path().trim_start_matches('/');
                let path = blobs_repo
                    .get_path(hash)
                    .await
                    .wrap_err_with(|| format!("blob not found in BlobsRepo: {}", hash))?;
                tokio::fs::read(&path)
                    .await
                    .wrap_err_with(|| format!("failed to read blob file: {}", path.display()))?
            }
            _ => {
                return Err(eyre::eyre!(
                    "Unsupported URL scheme for component: {}",
                    url.scheme()
                ));
            }
        };
        components.push(Component {
            bytes: wasm_bytes.into(),
            ..default()
        });
    }

    let _resp = wash_host
        .workload_start(wash_runtime::types::WorkloadStartRequest {
            workload_id,
            workload: wash_runtime::types::Workload {
                namespace: plug_id,
                name: bundle_name,
                annotations: HashMap::new(),
                service: None,
                components,
                host_interfaces: vec![
                    WitInterface {
                        config: [(
                            "wflow_keys".to_owned(),
                            bundle_man
                                .keys
                                // .iter()
                                // .map(|key| format!("{workload_id}/{key}"))
                                // .collect::<Vec<_>>()
                                .join(","),
                        )]
                        .into(),
                        ..WitInterface::from("townframe:wflow/bundle")
                    },
                    // FIXME: the following syntax is not supported here
                    // WitInterface::from("townframe:daybook/drawer,capabilities,facet-routine"),
                    WitInterface::from("townframe:daybook/drawer"),
                    WitInterface::from("townframe:daybook/capabilities"),
                    WitInterface::from("townframe:daybook/facet-routine"),
                    WitInterface::from("townframe:daybook/sqlite-connection"),
                    WitInterface::from("townframe:daybook/mltools-ocr"),
                    WitInterface::from("townframe:daybook/mltools-embed"),
                    WitInterface::from("townframe:daybook/mltools-llm-chat"),
                    // WitInterface::from("wasi:keyvalue/store"),
                ],
                volumes: vec![],
            },
        })
        .await
        .to_eyre()?;
    Ok(())
}
