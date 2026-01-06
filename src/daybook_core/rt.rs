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

// pub struct RtConfig {}

pub struct Rt {
    // _config: RtConfig,
    // drawer: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    wflow_ingress: Arc<dyn wflow::WflowIngress>,
    dispatch_repo: Arc<dispatch::DispatchRepo>,
    _wflow_part_state: Arc<PartitionWorkingState>,
    wcx: wflow::Ctx,
    _wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    wash_host: Arc<WashHost>,
    blobs_repo: Arc<BlobsRepo>,
}

pub struct RtStopToken {
    wflow_part_handle: Option<TokioPartitionWorkerHandle>,
}

impl RtStopToken {
    pub async fn stop(mut self) -> Res<()> {
        // TODO: use a cancel token to shutdown
        // rt if stop token is dropped
        self.wflow_part_handle.take().unwrap().stop().await?;
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
        wcx: wflow::Ctx,
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

        let part_log = PartitionLogRef::new(Arc::clone(&wcx.logstore));
        let wflow_ingress = Arc::new(wflow::ingress::PartitionLogIngress::new(
            part_log,
            wcx.metastore.clone(),
        ));
        let (wflow_part_handle, wflow_part_state) =
            wflow::start_partition_worker(&wcx, Arc::clone(&wflow_plugin), 0).await?;
        Ok((
            Arc::new(Self {
                plugs_repo,
                wflow_ingress,
                dispatch_repo,
                wcx,
                _wflow_plugin: wflow_plugin,
                wash_host,
                blobs_repo,
                _wflow_part_state: wflow_part_state,
            }),
            RtStopToken {
                wflow_part_handle: Some(wflow_part_handle),
            },
        ))
    }

    pub async fn dispatch(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
    ) -> Res<String> {
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
            (RoutineManifestDeets::DocInvoke {}, DispatchArgs::DocInvoke { .. }) => {
                todo!();
                // ActiveDispatchDeets::PropRoutine{}
            }
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

    /// Wait until a log entry matches the provided condition
    /// The callback receives (entry_id, log_entry) and should return true when the condition is met
    pub async fn wait_for_dispatch(
        &self,
        dispatch_id: &str,
        timeout_secs: u64,
    ) -> Res<serde_json::Value> {
        use futures::StreamExt;
        use tokio::time::{Duration, Instant};

        let dispatch = self
            .dispatch_repo
            .get(dispatch_id)
            .await
            .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;

        match &dispatch.deets {
            ActiveDispatchDeets::Wflow {
                entry_id,
                wflow_job_id,
                ..
            } => {
                // FIXME: this shuold be a method on the PartitionLogRef
                let start = Instant::now();
                let timeout_duration = Duration::from_secs(timeout_secs);
                let mut stream = self.wcx.logstore.tail(*entry_id).await;

                loop {
                    // Calculate remaining time
                    let elapsed = start.elapsed();
                    let remaining = timeout_duration.saturating_sub(elapsed);
                    if remaining.is_zero() {
                        return Err(ferr!(
                            "timeout waiting for log entry condition after {} seconds",
                            timeout_secs
                        ));
                    }

                    // Wait for next entry with timeout
                    match tokio::time::timeout(remaining, stream.next()).await {
                        Ok(Some(Ok(wflow::wflow_core::log::TailLogEntry {
                            idx: _,
                            val: Some(bytes),
                        }))) => {
                            // FIXME: hide this impl detail
                            let log_entry: PartitionLogEntry =
                                serde_json::from_slice(&bytes[..])
                                    .wrap_err("failed to parse log entry")?;
                            // info!(?log_entry, wflow_job_id, "XXX");
                            let PartitionLogEntry::JobEffectResult(event) = log_entry else {
                                continue;
                            };

                            if &event.job_id[..] == wflow_job_id {
                                match &event.result {
                                    JobRunResult::Success { value_json } => {
                                        self.dispatch_repo.remove(dispatch_id).await?;
                                        return Ok(serde_json::from_str(&value_json[..])
                                            .expect(ERROR_JSON));
                                    }
                                    JobRunResult::WorkerErr(err) => {
                                        self.dispatch_repo.remove(dispatch_id).await?;
                                        return Err(ferr!(
                                            "worker error on dispatch wflow: {err:?}"
                                        ));
                                    }
                                    JobRunResult::WflowErr(JobError::Terminal { error_json }) => {
                                        self.dispatch_repo.remove(dispatch_id).await?;
                                        return Err(ferr!(
                                            "terminal error on dispatch wflow: {error_json:?}"
                                        ));
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
                                    }
                                    JobRunResult::StepEffect(..) => {}
                                }
                            }
                        }
                        Ok(Some(Ok(_val))) => {
                            // this is a hole, keep going
                        }
                        Ok(Some(Err(err))) => {
                            return Err(err).wrap_err("log stream error waiting on dispatch wflow");
                        }
                        Ok(None) => {
                            // Stream ended, wait a bit and retry
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Err(_) => {
                            // Timeout reached
                            return Err(ferr!(
                                "timeout waiting for log entry condition after {timeout_secs} seconds",
                            ));
                        }
                    }
                }
            }
        }
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
                if let WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta { workload_id }) =
                    &meta.service
                {
                    if workload_id != workload_id {
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
