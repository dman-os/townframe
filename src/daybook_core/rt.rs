use crate::interlude::*;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::plugs::manifest;
use crate::plugs::PlugsRepo;

use daybook_types::doc::{DocPropKey, DocPropTag};
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
    ActiveDispatch, ActiveDispatchArgs, ActiveDispatchDeets, DispatchRepo, PropRoutineArgs,
};

pub struct RtConfig {
    pub device_id: String,
}

pub struct Rt {
    pub config: RtConfig,
    pub cancel_token: tokio_util::sync::CancellationToken,
    pub plugs_repo: Arc<PlugsRepo>,
    pub drawer: Arc<DrawerRepo>,
    pub wflow_ingress: Arc<dyn wflow::WflowIngress>,
    pub dispatch_repo: Arc<dispatch::DispatchRepo>,
    pub acx: AmCtx,
    pub wflow_part_state: Arc<PartitionWorkingState>,
    pub wcx: wflow::Ctx,
    pub wash_host: Arc<WashHost>,
    pub blobs_repo: Arc<BlobsRepo>,
    local_wflow_part_id: String,
}

pub struct RtStopToken {
    wflow_part_handle: Option<TokioPartitionWorkerHandle>,
    rt: Arc<Rt>,
    partition_watcher: tokio::task::JoinHandle<()>,
    doc_changes_worker: triage::DocTriageWorkerHandle,
}

impl RtStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.rt.cancel_token.cancel();
        // TODO: use a cancel token to shutdown
        // rt if stop token is dropped
        self.wflow_part_handle.take().unwrap().stop().await?;
        self.rt.wash_host.clone().stop().await.to_eyre()?;
        self.doc_changes_worker.stop().await?;
        utils_rs::wait_on_handle_with_timeout(self.partition_watcher, 5 * 1000).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum DispatchArgs {
    DocInvoke {
        doc_id: String,
        branch_name: String,
        heads: ChangeHashSet,
    },
    DocProp {
        doc_id: String,
        branch_name: String,
        heads: ChangeHashSet,
        prop_id: Option<String>,
    },
}

impl Rt {
    pub async fn boot(
        config: RtConfig,
        app_doc_id: DocumentId,
        wcx: wflow::Ctx,
        acx: AmCtx,
        drawer: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        dispatch_repo: Arc<DispatchRepo>,
        blobs_repo: Arc<BlobsRepo>,
    ) -> Res<(Arc<Self>, RtStopToken)> {
        let wflow_plugin = Arc::new(wash_plugin_wflow::WflowPlugin::new(Arc::clone(
            &wcx.metastore,
        )));
        let daybook_plugin = Arc::new(wash_plugin::DaybookPlugin::new(
            Arc::clone(&drawer),
            Arc::clone(&dispatch_repo),
        ));
        let utils_plugin = wash_plugin_utils::UtilsPlugin::new(wash_plugin_utils::Config {
            ollama_url: utils_rs::get_env_var("OLLAMA_URL")?,
            ollama_model: utils_rs::get_env_var("OLLAMA_MODEL")?,
        })
        .wrap_err("error creating utils plugin")?;

        let wash_host =
            wflow::build_wash_host(vec![wflow_plugin.clone(), daybook_plugin, utils_plugin])
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
                &bundle_man,
            )
            .await?;
        }

        let wash_host = wash_host
            .start()
            .await
            .to_eyre()
            .wrap_err("error starting wash host")?;

        let part_idx = 0;
        let (wflow_part_handle, wflow_part_state) =
            wflow::start_partition_worker(&wcx, wflow_plugin, part_idx).await?;
        let part_log = PartitionLogRef::new(Arc::clone(&wcx.logstore));
        let wflow_ingress = Arc::new(wflow::ingress::PartitionLogIngress::new(
            part_log,
            wcx.metastore.clone(),
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
            blobs_repo,
            wflow_part_state,
        });

        // Start the DocTriageWorker to automatically queue jobs when docs are added
        let doc_changes_worker =
            crate::rt::triage::spawn_doc_triage_worker(rt.clone(), app_doc_id).await?;

        let partition_watcher = tokio::spawn({
            let repo = rt.clone();
            async move { repo.keep_up_with_partition().await.unwrap_or_log() }
        });

        Ok((
            rt.clone(),
            RtStopToken {
                rt,
                partition_watcher,
                doc_changes_worker,
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
        let Some(_dispatch) = self.dispatch_repo.get(dispatch_id).await else {
            return Ok(());
        };
        let is_done = match &event.result {
            JobRunResult::Success { value_json } => {
                info!(?value_json, "success on dispatch wflow");
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
            .get(&routine_name[..])
            .ok_or_else(|| ferr!("routine not found in plug manifest: {plug_id}/{routine_name}"))?;

        use crate::plugs::manifest::RoutineManifestDeets;
        let (dispatch_id, args) = match (&routine_man.deets, args) {
            (
                RoutineManifestDeets::DocProp { working_prop_tag },
                DispatchArgs::DocProp {
                    doc_id,
                    heads,
                    branch_name,
                    prop_id,
                },
            ) => {
                let tag: DocPropTag = working_prop_tag.0.clone().into();
                let prop_key = match prop_id {
                    Some(id) => DocPropKey::TagAndId { tag, id },
                    None => DocPropKey::Tag(tag),
                };
                let prop_key = prop_key.to_string();
                let dispatch_id = {
                    use std::hash::{Hash, Hasher};
                    // FIXME: we probably want to use a stable hasher impl
                    let mut hasher = std::hash::DefaultHasher::default();
                    doc_id.hash(&mut hasher);
                    heads.hash(&mut hasher);
                    plug_id.hash(&mut hasher);
                    routine_name.hash(&mut hasher);
                    let hash = hasher.finish();
                    utils_rs::hash::encode_base58_multibase(&hash.to_le_bytes())
                };
                let dispatch_id = format!("{plug_id}/{routine_name}-{dispatch_id}");
                (
                    dispatch_id,
                    ActiveDispatchArgs::PropRoutine(PropRoutineArgs {
                        doc_id,
                        branch_name,
                        heads,
                        prop_key,
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
                    &bundle_man,
                )
                .await?;

                // let fqk = format!("{workload_id}/{key}");
                let job_id = format!("{dispatch_id}-{id}", id = Uuid::new_v4().bs58());
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

    pub async fn cancel_dispatch(&self, _dispatch_id: &str) -> Res<()> {
        todo!()
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
            move |msg| match &*msg {
                dispatch::DispatchEvent::DispatchDeleted { id, .. } => {
                    if *id == dispatch_id {
                        notif_tx.send(()).expect(ERROR_CHANNEL)
                    }
                }
                _ => {}
            }
        });

        // check if the dispatch exists first
        let Some(_dispatch) = self.dispatch_repo.get(dispatch_id).await else {
            return Ok(());
        };

        let _event = tokio::time::timeout(timeout, notif_rx.changed())
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
            plug_id.into(),
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
                    // WitInterface::from("townframe:daybook/drawer,capabilities,prop-routine"),
                    WitInterface::from("townframe:daybook/drawer"),
                    WitInterface::from("townframe:daybook/capabilities"),
                    WitInterface::from("townframe:daybook/prop-routine"),
                    WitInterface::from("townframe:utils/llm-chat"),
                    // WitInterface::from("wasi:keyvalue/store"),
                ],
                volumes: vec![],
            },
        })
        .await
        .to_eyre()?;
    Ok(())
}
