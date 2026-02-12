use crate::config::ConfigRepo;
use crate::index::DocEmbeddingIndexRepo;
use crate::interlude::*;
use crate::local_state::SqliteLocalStateRepo;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::plugs::manifest;
use crate::plugs::PlugsRepo;

use daybook_types::doc::{FacetKey, FacetTag};
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
pub mod triage;
pub mod wash_plugin;

use dispatch::{
    ActiveDispatch, ActiveDispatchArgs, ActiveDispatchDeets, DispatchRepo, FacetRoutineArgs,
};

pub struct RtConfig {
    pub device_id: String,
}

pub struct Rt {
    pub config: RtConfig,
    pub cancel_token: tokio_util::sync::CancellationToken,
    pub plugs_repo: Arc<PlugsRepo>,
    pub drawer: Arc<DrawerRepo>,
    pub config_repo: Arc<ConfigRepo>,
    pub wflow_ingress: Arc<dyn wflow::WflowIngress>,
    pub dispatch_repo: Arc<dispatch::DispatchRepo>,
    pub acx: AmCtx,
    pub wflow_part_state: Arc<PartitionWorkingState>,
    pub wcx: wflow::Ctx,
    pub wash_host: Arc<WashHost>,
    pub wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    pub daybook_plugin: Arc<wash_plugin::DaybookPlugin>,
    pub utils_plugin: Arc<wash_plugin_utils::UtilsPlugin>,
    pub mltools_plugin: Arc<wash_plugin_mltools::MltoolsPlugin>,
    pub blobs_repo: Arc<BlobsRepo>,
    pub doc_embedding_index_repo: Arc<DocEmbeddingIndexRepo>,
    pub sqlite_local_state_repo: Arc<SqliteLocalStateRepo>,
    pub local_actor_id: automerge::ActorId,
    local_wflow_part_id: String,
}

pub struct RtStopToken {
    wflow_part_handle: Option<TokioPartitionWorkerHandle>,
    rt: Arc<Rt>,
    partition_watcher: Option<tokio::task::JoinHandle<()>>,
    doc_changes_worker: Option<triage::DocTriageWorkerHandle>,
    doc_embedding_index_stop: Option<crate::index::DocEmbeddingIndexStopToken>,
    sqlite_local_state_stop: Option<crate::repos::RepoStopToken>,
}

impl RtStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.rt.cancel_token.cancel();

        // Stop triage worker first to prevent new dispatches from being created
        if let Some(worker) = self.doc_changes_worker.take() {
            if let Err(err) = worker.stop().await {
                warn!(
                    ?err,
                    "error stopping doc_changes_worker during shutdown - continuing"
                );
            }
        }

        if let Some(stop) = self.doc_embedding_index_stop.take() {
            if let Err(err) = stop.stop().await {
                warn!(
                    ?err,
                    "error stopping doc_embedding_index_repo during shutdown - continuing"
                );
            }
        }
        if let Some(stop) = self.sqlite_local_state_stop.take() {
            if let Err(err) = stop.stop().await {
                warn!(
                    ?err,
                    "error stopping sqlite_local_state_repo during shutdown - continuing"
                );
            }
        }

        // Wait for all active dispatches to complete
        let active_dispatches: Vec<String> = self
            .rt
            .dispatch_repo
            .list()
            .await
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        if !active_dispatches.is_empty() {
            info!(
                count = active_dispatches.len(),
                "waiting for active dispatches to complete"
            );
            for dispatch_id in active_dispatches {
                let _ = self
                    .rt
                    .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(30))
                    .await;
            }
        }

        // Stop wflow partition worker
        if let Some(handle) = self.wflow_part_handle.take() {
            if let Err(err) = handle.stop().await {
                warn!(
                    ?err,
                    "error stopping wflow_part_handle during shutdown - continuing"
                );
            }
        }

        if let Err(err) = Arc::clone(&self.rt.wash_host).stop().await.to_eyre() {
            warn!(
                ?err,
                "error stopping wash_host during shutdown - continuing"
            );
        }

        if let Some(watcher) = self.partition_watcher.take() {
            if let Err(err) = utils_rs::wait_on_handle_with_timeout(watcher, 10 * 1000).await {
                warn!(
                    ?err,
                    "error waiting for partition_watcher during shutdown - continuing"
                );
            }
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
    },
}

impl Rt {
    #[allow(clippy::too_many_arguments)]
    pub async fn boot(
        config: RtConfig,
        app_doc_id: DocumentId,
        wflow_db_url: String,
        acx: AmCtx,
        drawer: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        dispatch_repo: Arc<DispatchRepo>,
        blobs_repo: Arc<BlobsRepo>,
        config_repo: Arc<ConfigRepo>,
        local_actor_id: automerge::ActorId,
        local_state_root: PathBuf,
    ) -> Res<(Arc<Self>, RtStopToken)> {
        let wcx = wflow::Ctx::init(&wflow_db_url).await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            SqliteLocalStateRepo::boot(local_state_root).await?;

        let (doc_embedding_index_repo, doc_embedding_index_stop) =
            crate::index::DocEmbeddingIndexRepo::boot(
                acx.clone(),
                app_doc_id.clone(),
                Arc::clone(&drawer),
                local_actor_id.clone(),
            )
            .await?;

        let wflow_plugin = Arc::new(wash_plugin_wflow::WflowPlugin::new(Arc::clone(
            &wcx.metastore,
        )));
        let daybook_plugin = Arc::new(wash_plugin::DaybookPlugin::new(
            Arc::clone(&drawer),
            Arc::clone(&dispatch_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_embedding_index_repo),
            Arc::clone(&sqlite_local_state_repo),
        ));
        let utils_plugin = wash_plugin_utils::UtilsPlugin::new(wash_plugin_utils::Config {
            ollama_url: utils_rs::get_env_var("OLLAMA_URL")?,
            ollama_model: utils_rs::get_env_var("OLLAMA_MODEL")?,
        })
        .wrap_err("error creating utils plugin")?;
        let mltools_plugin = wash_plugin_mltools::MltoolsPlugin::new(wash_plugin_mltools::Config {
            ollama_url: utils_rs::get_env_var("OLLAMA_URL")?,
            ollama_model: utils_rs::get_env_var("OLLAMA_MODEL")?,
        })
        .wrap_err("error creating utils plugin")?;

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
        }

        let part_idx = 0;
        let (wflow_part_handle, wflow_part_state) =
            wflow::start_partition_worker(&wcx, Arc::clone(&wflow_plugin), part_idx).await?;
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
            acx,
            wflow_ingress,
            dispatch_repo,
            wcx,
            wash_host,
            wflow_plugin,
            daybook_plugin,
            utils_plugin,
            mltools_plugin,
            blobs_repo,
            doc_embedding_index_repo,
            sqlite_local_state_repo,
            config_repo,
            wflow_part_state,
            local_actor_id,
        });

        // Start the DocTriageWorker to automatically queue jobs when docs are added
        let doc_changes_worker =
            crate::rt::triage::spawn_doc_triage_worker(Arc::clone(&rt), app_doc_id).await?;

        let partition_watcher = tokio::spawn({
            let repo = Arc::clone(&rt);
            async move { repo.keep_up_with_partition().await.unwrap_or_log() }
        });

        Ok((
            Arc::clone(&rt),
            RtStopToken {
                rt,
                partition_watcher: Some(partition_watcher),
                doc_changes_worker: Some(doc_changes_worker),
                doc_embedding_index_stop: Some(doc_embedding_index_stop),
                sqlite_local_state_stop: Some(sqlite_local_state_stop),
                wflow_part_handle: Some(wflow_part_handle),
            },
        ))
    }

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
                self.handle_wflow_entry(idx, entry).await?;
            };
            self.dispatch_repo
                .set_wflow_part_frontier(self.local_wflow_part_id.clone(), idx)
                .await?;
        }
        Ok(())
    }

    fn ensure_rt_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("rt is shutting down")
        }
        Ok(())
    }
    #[tracing::instrument(skip(self, entry))]
    async fn handle_wflow_entry(&self, entry_id: u64, entry: PartitionLogEntry) -> Res<()> {
        let PartitionLogEntry::JobEffectResult(event) = entry else {
            return Ok(());
        };
        let Some((dispatch_id, _)) = event.job_id.rsplit_once('-') else {
            return Ok(());
        };
        let Some(dispatch) = self.dispatch_repo.get(dispatch_id).await else {
            return Ok(());
        };
        let ActiveDispatchDeets::Wflow {
            wflow_job_id,
            wflow_key,
            ..
        } = &dispatch.deets;
        if wflow_job_id != event.job_id.as_ref() {
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
                true
            }
            JobRunResult::Aborted => {
                info!("dispatch wflow aborted");
                true
            }
            JobRunResult::WorkerErr(err) => {
                error!(?err, "worker error on dispatch wflow");
                true
            }
            JobRunResult::WflowErr(JobError::Terminal { error_json }) => {
                error!(?error_json, "terminal error on dispatch wflow");
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
                false
            }
            JobRunResult::StepEffect(..) => false,
        };
        if is_done {
            // Handle staging branch cleanup based on success/failure
            let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path: target_branch_path,
                ..
            }) = &dispatch.args;

            let is_success = matches!(&event.result, JobRunResult::Success { .. });

            if is_success {
                // Merge staging branch into target branch
                info!(
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
                            ?doc_id,
                            ?name,
                            "staging branch missing during merge; skipping merge/delete"
                        );
                        self.dispatch_repo.remove(dispatch_id.into()).await?;
                        return Ok(());
                    }
                    Err(err) => {
                        return Err(eyre::eyre!(err).wrap_err("error merging staging branch"));
                    }
                }

                // Delete the staging branch after successful merge
                info!(
                    ?doc_id,
                    ?staging_branch_path,
                    "deleting staging branch after successful merge"
                );
                self.drawer
                    .delete_branch(doc_id, staging_branch_path, None)
                    .await
                    .wrap_err("error deleting staging branch after merge")?;
            } else {
                // Delete staging branch on failure
                info!(
                    ?doc_id,
                    ?staging_branch_path,
                    "deleting staging branch due to failure"
                );
                self.drawer
                    .delete_branch(doc_id, staging_branch_path, None)
                    .await
                    .wrap_err("error deleting staging branch")?;
            }

            self.dispatch_repo.remove(dispatch_id.into()).await?;
        }
        Ok(())
    }

    pub async fn dispatch(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
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

        use crate::plugs::manifest::RoutineManifestDeets;
        let (dispatch_id, mut args) = match (&routine_man.deets, args) {
            (
                RoutineManifestDeets::DocFacet { working_facet_tag },
                DispatchArgs::DocFacet {
                    doc_id,
                    heads,
                    branch_path,
                    facet_key,
                },
            ) => {
                let tag: FacetTag = working_facet_tag.0.clone().into();
                let facet_key = match facet_key {
                    Some(id) => FacetKey { tag, id },
                    None => tag.into(),
                };
                let facet_key = facet_key.to_string();
                let dispatch_id = {
                    use std::hash::{Hash, Hasher};
                    // FIXME: we probably want to use a stable hasher impl
                    let mut hasher = std::hash::DefaultHasher::default();
                    doc_id.hash(&mut hasher);
                    heads.hash(&mut hasher);
                    plug_id.hash(&mut hasher);
                    routine_name.hash(&mut hasher);
                    let hash = hasher.finish();
                    utils_rs::hash::encode_base58_multibase(hash.to_le_bytes())
                };
                let dispatch_id = format!("{plug_id}/{routine_name}-{dispatch_id}");
                (
                    dispatch_id,
                    ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                        doc_id,
                        branch_path,
                        heads,
                        facet_key,
                        facet_acl: routine_man.facet_acl.clone(),
                        local_state_acl: routine_man.local_state_acl.clone(),
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
        if let Some(_disp) = self.dispatch_repo.get(&dispatch_id).await {
            warn!(?dispatch_id, "dispatch already active");
            return Ok(dispatch_id);
        }

        let deets = match &routine_man.r#impl {
            manifest::RoutineImpl::Wflow {
                key,
                bundle: bundle_name,
            } => {
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

                // let fqk = format!("{workload_id}/{key}");
                let job_id = format!("{dispatch_id}-{id}", id = Uuid::new_v4().bs58());
                let staging_branch_path =
                    daybook_types::doc::BranchPath::from(format!("/tmp/{}", job_id));

                // Update args with staging branch path
                let ActiveDispatchArgs::FacetRoutine(ref mut facet_args) = args;
                facet_args.staging_branch_path = staging_branch_path.clone();

                let entry_id = self
                    .wflow_ingress
                    .add_job(
                        job_id.clone().into(),
                        key,
                        // &fqk,
                        serde_json::to_string(&()).expect(ERROR_JSON),
                        None,
                    )
                    .await
                    .wrap_err_with(|| {
                        format!("error scheduling job for {plug_id}/{routine_name}")
                    })?;
                ActiveDispatchDeets::Wflow {
                    wflow_partition_id: self.local_wflow_part_id.clone(),
                    entry_id,
                    plug_id: plug_id.into(),
                    bundle_name: bundle_name.0.clone(),
                    wflow_key: key.0.clone(),
                    wflow_job_id: job_id,
                }
            }
        };
        self.dispatch_repo
            .add(
                dispatch_id.clone(),
                Arc::new(ActiveDispatch { args, deets }),
            )
            .await?;
        Ok(dispatch_id)
    }

    pub async fn cancel_dispatch(&self, dispatch_id: &str) -> Res<()> {
        self.ensure_rt_live()?;

        let dispatch = self
            .dispatch_repo
            .get(dispatch_id)
            .await
            .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        let marked_now = self.dispatch_repo.mark_cancelled(dispatch_id).await?;
        if !marked_now {
            debug!(%dispatch_id, "cancel already requested; skipping duplicate cancel");
            return Ok(());
        }
        match &dispatch.deets {
            ActiveDispatchDeets::Wflow { wflow_job_id, .. } => {
                self.wflow_ingress
                    .cancel_job(
                        Arc::from(wflow_job_id.as_str()),
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

        use crate::repos::Repo;

        let (notif_tx, mut notif_rx) = tokio::sync::watch::channel(());
        let _listener_handle = self.dispatch_repo.register_listener({
            let dispatch_id = dispatch_id.to_string();
            move |msg| {
                if let dispatch::DispatchEvent::DispatchDeleted { id, .. } = &*msg {
                    if *id == dispatch_id {
                        notif_tx.send(()).expect(ERROR_CHANNEL)
                    }
                }
            }
        });

        // check if the dispatch exists first
        let Some(_dispatch) = self.dispatch_repo.get(dispatch_id).await else {
            return Ok(());
        };

        tokio::time::timeout(timeout, notif_rx.changed())
            .await?
            .expect(ERROR_CHANNEL);

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
                    WitInterface::from("townframe:am-repo/repo"),
                    // FIXME: the following syntax is not supported here
                    // WitInterface::from("townframe:daybook/drawer,capabilities,facet-routine"),
                    WitInterface::from("townframe:daybook/drawer"),
                    WitInterface::from("townframe:daybook/capabilities"),
                    WitInterface::from("townframe:daybook/facet-routine"),
                    WitInterface::from("townframe:daybook/sqlite-connection"),
                    WitInterface::from("townframe:daybook/mltools-ocr"),
                    WitInterface::from("townframe:daybook/mltools-embed"),
                    WitInterface::from("townframe:daybook/mltools-llm-chat"),
                    WitInterface::from("townframe:daybook/index-vector"),
                    // WitInterface::from("wasi:keyvalue/store"),
                ],
                volumes: vec![],
            },
        })
        .await
        .to_eyre()?;
    Ok(())
}
