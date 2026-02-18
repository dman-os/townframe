mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::RwLock;

use anyhow::Context;
use utils_rs::prelude::tokio::sync::oneshot;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

use wflow_core::gen::metastore::{WasmcloudWflowServiceMeta, WflowServiceMeta};
use wflow_core::metastore::MetdataStore;
use wflow_core::partition::{job_events, service, state};

pub mod binds_partition_host {
    wash_runtime::wasmtime::component::bindgen!({
        world: "rt-partition-host",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}
use binds_partition_host::townframe::wflow::partition_host;

pub mod binds_metastore {
    wash_runtime::wasmtime::component::bindgen!({
        world: "rt-metastore",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}
use binds_metastore::townframe::wflow::metastore;

mod binds_service {
    wash_runtime::wasmtime::component::bindgen!({
        world: "service",
        path: "../wash_plugin_wflow/wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}
use binds_service::exports::townframe::wflow::bundle;
use binds_service::townframe::wflow::host;
use binds_service::townframe::wflow::types;

#[derive(educe::Educe)]
#[educe(Debug)]
struct ActiveJobCtx {
    #[educe(Debug(ignore))]
    trap_tx: tokio::sync::Mutex<Option<oneshot::Sender<JobTrap>>>,
    cur_step: AtomicU64,
    active_step: std::sync::Mutex<Option<ActiveStepCtx>>,
    journal: state::JobState,
}

impl ActiveJobCtx {
    /// Set a trap and block forever (async). This should be called when the trap
    /// is set, as the `run` invocation will be dropped when a trap is set.
    async fn set_trap_and_block_forever(
        &self,
        trap: JobTrap,
    ) -> anyhow::Result<futures::never::Never> {
        let trap_tx = { self.trap_tx.lock().await.take() };
        let Some(trap_tx) = trap_tx else {
            anyhow::bail!("run has already trapped");
        };
        if trap_tx.send(trap).is_err() {
            anyhow::bail!("run has been abandoned");
        }
        // Block forever since the run invocation will be dropped
        futures::future::pending::<futures::never::Never>().await;
        unreachable!()
    }
}

#[derive(Debug)]
struct ActiveStepCtx {
    attempt_id: u64,
    step_id: u64,
    start_at: Timestamp,
}

enum JobTrap {
    PersistStep {
        step_id: u64,
        start_at: Timestamp,
        end_at: Timestamp,
        value_json: Arc<str>,
        attempt_id: u64,
    },
    RunComplete(Result<String, types::JobError>),
}

impl host::Host for SharedWashCtx {
    async fn next_step(
        &mut self,
        job_id: partition_host::JobId,
    ) -> wasmtime::Result<Result<host::StepState, String>> {
        let plugin = WflowPlugin::from_ctx(self);
        let Some(job) = plugin
            .active_jobs
            .read()
            .expect(ERROR_MUTEX)
            .get(job_id.as_str())
            .cloned()
        else {
            anyhow::bail!("job not active");
        };
        let mut active_step = job.active_step.lock().expect(ERROR_MUTEX);
        if active_step.is_some() {
            // TODO: should be possible to implement this
            anyhow::bail!("concurrent steps not allowed");
        }
        let step_id = job.cur_step.load(std::sync::atomic::Ordering::Relaxed);
        let attempt_id = if let Some(state) = job.journal.steps.get(step_id as usize) {
            use wflow_core::partition::job_events::JobEffectResultDeets;
            use wflow_core::partition::state::JobStepState;
            match state {
                JobStepState::Effect { attempts } => {
                    if let Some(attempt) = attempts.last() {
                        match &attempt.deets {
                            JobEffectResultDeets::Success { value_json } => {
                                job.cur_step
                                    .compare_exchange(
                                        step_id,
                                        step_id + 1,
                                        std::sync::atomic::Ordering::SeqCst,
                                        std::sync::atomic::Ordering::Relaxed,
                                    )
                                    .expect("impossible: wasm is single threaded");
                                return Ok(Ok(host::StepState::Completed(
                                    host::CompletedStepState {
                                        id: step_id,
                                        value_json: value_json.to_string(),
                                    },
                                )));
                            }
                            JobEffectResultDeets::EffectErr(_) => attempts.len(),
                        }
                    } else {
                        0
                    }
                }
            }
        } else {
            0
        };
        let start_at = Timestamp::now();
        active_step.replace(ActiveStepCtx {
            attempt_id: attempt_id as u64,
            step_id,
            start_at,
        });
        Ok(Ok(host::StepState::Active(host::ActiveStepState {
            id: step_id,
        })))
    }

    async fn persist_step(
        &mut self,
        job_id: partition_host::JobId,
        step_id: host::StepId,
        value_json: String,
    ) -> wasmtime::Result<Result<(), String>> {
        let plugin = WflowPlugin::from_ctx(self);
        let Some(job) = plugin
            .active_jobs
            .read()
            .expect(ERROR_MUTEX)
            .get(job_id.as_str())
            .cloned()
        else {
            anyhow::bail!("job not active");
        };
        let trap = {
            let mut active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.take() else {
                anyhow::bail!("step not active");
            };
            if active_step.step_id != step_id {
                anyhow::bail!("given step_id is not active");
            }
            let end_at = Timestamp::now();
            JobTrap::PersistStep {
                step_id,
                value_json: value_json.into(),
                attempt_id: active_step.attempt_id,
                start_at: active_step.start_at,
                end_at,
            }
        };

        job.cur_step
            .compare_exchange(
                step_id,
                step_id + 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            )
            .expect("impossible: wasm is single threaded");

        // Set trap and block forever - the run invocation will be dropped
        job.set_trap_and_block_forever(trap)
            .await
            .map_err(|err| wasmtime::Error::msg(format!("{err:?}")))?;

        unreachable!()
    }
}

impl partition_host::Host for SharedWashCtx {
    async fn add_job(
        &mut self,
        _id: partition_host::PartitionId,
        _args: partition_host::AddJobArgs,
    ) -> wasmtime::Result<()> {
        todo!()
    }
}

impl metastore::Host for SharedWashCtx {
    async fn get_wflow(&mut self, key: String) -> wasmtime::Result<Option<metastore::WflowMeta>> {
        let plugin = WflowPlugin::from_ctx(self);
        let meta = plugin.metastore.get_wflow(&key).await.to_anyhow()?;
        Ok(meta.map(|meta| metastore::WflowMeta {
            key: meta.key,
            service: match meta.service {
                WflowServiceMeta::Wasmcloud(wflow) => {
                    metastore::WflowServiceMeta::Wasmcloud(metastore::WasmcloudWflowServiceMeta {
                        workload_id: wflow.workload_id,
                    })
                }
                WflowServiceMeta::LocalNative => metastore::WflowServiceMeta::LocalNative,
            },
        }))
    }

    async fn get_partitions(&mut self) -> wasmtime::Result<metastore::PartitionsMeta> {
        let plugin = WflowPlugin::from_ctx(self);
        let meta = plugin.metastore.get_partitions().await.to_anyhow()?;
        Ok(metastore::PartitionsMeta {
            version: meta.version,
            partition_count: meta.partition_count,
        })
    }
}

pub struct WflowPlugin {
    pending_workloads: DHashMap<Arc<str>, HashSet<Arc<str>>>,

    // workload_id -> workload
    active_workloads: RwLock<HashMap<Arc<str>, Arc<WflowWorkload>>>,
    // wflow key -> workload_id
    active_keys: DHashMap<Arc<str>, Arc<str>>,
    // job id ->
    active_jobs: RwLock<HashMap<Arc<str>, Arc<ActiveJobCtx>>>,
    // ctx id -> job id
    active_contexts: DHashMap<Arc<str>, Arc<str>>,

    metastore: Arc<dyn MetdataStore>,
}

impl WflowPlugin {
    pub fn new(metastore: Arc<dyn MetdataStore>) -> Self {
        Self {
            active_workloads: default(),
            pending_workloads: default(),
            active_keys: default(),
            active_jobs: default(),
            active_contexts: default(),
            metastore,
        }
    }

    const ID: &str = "townframe:wflow";

    pub fn try_from_ctx(wcx: &SharedWashCtx) -> Option<Arc<Self>> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        let Some(this) = wcx.active_ctx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
    }

    pub fn job_id_of_ctx(&self, wcx: &SharedWashCtx) -> Option<Arc<str>> {
        self.active_contexts
            .get(&wcx.active_ctx.id[..])
            .map(|val| Arc::clone(val.value()))
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct WflowWorkload {
    wflow_keys: HashSet<Arc<str>>,
    resolved_handle: wash_runtime::engine::workload::ResolvedWorkload,
    component_id: String,
    #[educe(Debug(ignore))]
    instance_pre: binds_service::ServicePre<SharedWashCtx>,
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for WflowPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::from([
                //
                WitInterface::from("townframe:wflow/bundle"),
            ]),
            imports: std::collections::HashSet::from([
                //
                WitInterface::from("townframe:wflow/host,partition-host,metadata-store"),
            ]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        interface_configs: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(iface) = interface_configs
            .iter()
            .find(|iface| iface.namespace == "townframe" && iface.package == "wflow")
        else {
            unreachable!();
        };
        if !iface.interfaces.contains("bundle") {
            anyhow::bail!("unupported component {interface_configs:?}");
        }
        let Some(wflow_keys_raw) = iface.config.get("wflow_keys") else {
            anyhow::bail!("no wflow_keys defined for townframe:wflow component");
        };
        let wflow_keys: HashSet<Arc<str>> = wflow_keys_raw
            .split(",")
            .map(|key| key.trim().into())
            .collect();
        // FIXME: regex for valid job keys
        if wflow_keys.is_empty() {
            anyhow::bail!("wflow_keys is empty: \"{wflow_keys_raw}\"");
        }
        for key in &wflow_keys {
            if let Some(occupied) = self.metastore.get_wflow(key).await.to_anyhow()? {
                if let WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta { workload_id }) =
                    &occupied.service
                {
                    if workload_id != workload.id() {
                        anyhow::bail!(
                            "wflow under key '{key}' in metatstore '{occupied:?}' doesn't match workload id '{}'",
                            workload.id()
                        );
                    }
                } else {
                    anyhow::bail!(
                        "wflow under key '{key}' in metatstore '{occupied:?}' doesn't match workload type for workload '{}'",
                        workload.id()
                    );
                }
            }
        }
        let workload_id: Arc<str> = workload.id().into();
        self.pending_workloads.insert(workload_id, wflow_keys);
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "wflow" {
                if iface.interfaces.contains("host") {
                    host::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("partition-host") {
                    partition_host::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("metadata-store") {
                    metastore::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let Some((workload_id, wflow_keys)) = self.pending_workloads.remove(resolved.id()) else {
            anyhow::bail!("unrecognized workload was resolved");
        };
        let instance_pre = resolved.instantiate_pre(component_id).await?;
        let instance_pre = binds_service::ServicePre::new(instance_pre)
            .context("error pre instantiating service component")?;

        // Handle workload restarts/re-resolves deterministically by clearing any
        // prior registration for this workload ID before inserting fresh keys.
        if let Some(previous_workload) = self
            .active_workloads
            .write()
            .expect(ERROR_MUTEX)
            .remove(&workload_id)
        {
            for key in &previous_workload.wflow_keys {
                self.active_keys.remove(key);
            }
        }

        for key in &wflow_keys {
            let old = self
                .active_keys
                .insert(Arc::clone(key), Arc::clone(&workload_id));
            if let Some(old_workload_id) = old {
                if old_workload_id != workload_id {
                    anyhow::bail!(
                        "wflow key '{key}' already mapped to workload '{old_workload_id}', cannot remap to '{workload_id}'"
                    );
                }
            }
        }
        let wflow = WflowWorkload {
            wflow_keys,
            instance_pre,
            resolved_handle: resolved.clone(),
            component_id: component_id.into(),
        };
        let wflow = Arc::new(wflow);
        self.active_workloads
            .write()
            .expect(ERROR_MUTEX)
            .insert(workload_id, wflow);
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        if let Some(wflow) = self
            .active_workloads
            .write()
            .expect(ERROR_MUTEX)
            .remove(workload_id)
        {
            for key in &wflow.wflow_keys {
                self.active_keys.remove(key);
            }
        }
        // FIXME: cleaanup from meta store
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl service::WflowServiceHost for WflowPlugin {
    type ExtraArgs = WasmcloudWflowServiceMeta;
    async fn run(
        &self,
        job_id: Arc<str>,
        journal: state::JobState,
        args: &Self::ExtraArgs,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        let Some(workload) = self
            .active_workloads
            .read()
            .expect(ERROR_MUTEX)
            .get(&args.workload_id[..])
            .cloned()
        else {
            return Err(job_events::JobRunResult::WorkerErr(
                job_events::JobRunWorkerError::WflowNotFound,
            ));
        };
        let mut store = workload
            .resolved_handle
            .new_store(&workload.component_id)
            .await
            .to_eyre()
            .wrap_err("error creating component store")?;

        let instance = workload
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .to_eyre()
            .wrap_err("error creating component store")?;
        let bundle_args = bundle::RunArgs {
            ctx: types::JobCtx {
                job_id: job_id.to_string(),
            },
            wflow_key: journal.wflow.key.clone(),
            args_json: journal.init_args_json.to_string(),
        };

        let ctx_id: Arc<str> = store.data().active_ctx.id.clone().into();
        let fut = instance
            .townframe_wflow_bundle()
            .call_run(&mut store, &bundle_args);

        let (trap_tx, trap_rx) = oneshot::channel();
        let trap_tx = tokio::sync::Mutex::new(Some(trap_tx));
        let _old = self.active_jobs.write().expect(ERROR_MUTEX).insert(
            Arc::clone(&job_id),
            ActiveJobCtx {
                trap_tx,
                journal,
                cur_step: default(),
                active_step: None.into(),
            }
            .into(),
        );
        assert!(_old.is_none(), "fishy");

        self.active_contexts
            .insert(Arc::clone(&ctx_id), Arc::clone(&job_id));
        scopeguard::defer! {
            let _ = self.active_contexts.remove(&ctx_id);
            let _ = self.active_jobs
                .write()
                .expect(ERROR_MUTEX)
                .remove(&job_id);
        }

        // TODO: timeout
        let trap = tokio::select! {
            trap = trap_rx => {
                trap.expect("trap channel dropped without use")
            },
            res = fut => {
                JobTrap::RunComplete(
                    res
                        .to_eyre()
                        .wrap_err("wasm error")?
                )
            }
        };
        // FIXME: unite type hierarichies

        match trap {
            JobTrap::RunComplete(Err(err)) => match err {
                types::JobError::Transient(err) => Err(job_events::JobError::Transient {
                    error_json: err.error_json.into(),
                    retry_policy: err.retry_policy.map(|policy| match policy {
                        types::RetryPolicy::Immediate => {
                            wflow_core::partition::RetryPolicy::Immediate
                        }
                    }),
                }
                .into()),
                types::JobError::Terminal(err_json) => Err(job_events::JobError::Terminal {
                    error_json: err_json.into(),
                }
                .into()),
            },
            JobTrap::PersistStep {
                step_id,
                value_json,
                start_at,
                end_at,
                attempt_id,
            } => Ok(job_events::JobRunResult::StepEffect(
                job_events::JobEffectResult {
                    step_id,
                    attempt_id,
                    start_at,
                    end_at,
                    deets: job_events::JobEffectResultDeets::Success { value_json },
                },
            )),
            JobTrap::RunComplete(Ok(value_json)) => Ok(job_events::JobRunResult::Success {
                value_json: value_json.into(),
            }),
        }
    }
}
