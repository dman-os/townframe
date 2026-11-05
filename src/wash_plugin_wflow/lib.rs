mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use std::collections::HashSet;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use tokio_util::sync::CancellationToken;
use utils_rs::prelude::tokio::sync::mpsc;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::WitInterfaces;
use wash_runtime::wit::WitInterface;
use wash_runtime::wit::WitWorld;

use wasmtime::component::Accessor;
use wasmtime::component::HasSelf;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;

mod authority_session;
mod workload_registry;

mod binds_service {
    wash_runtime::wasmtime::component::bindgen!({
        world: "service",
        path: "../wash_plugin_wflow/wit/main.wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [PartialEq, Eq],
    });
}
use binds_service::exports::townframe::wflow::bundle;
use binds_service::townframe::wflow::host;
pub use binds_service::townframe::wflow::host as service_host;
use binds_service::townframe::wflow::types;

#[derive(educe::Educe)]
#[educe(Debug)]
struct ActiveJobCtx {
    job_id: Arc<str>,
    #[educe(Debug(ignore))]
    yield_tx: mpsc::UnboundedSender<JobTrap>,
    #[educe(Debug(ignore))]
    resume_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<SessionResume>>,
    #[educe(Debug(ignore))]
    pause_cancel: CancellationToken,
    cur_step: AtomicU64,
    active_step: std::sync::Mutex<Option<ActiveStepCtx>>,
    replay: std::sync::Mutex<Vec<ReplayStep>>,
    ctx_id: Arc<str>,
}

#[derive(Debug)]
struct ActiveStepCtx {
    step_id: u64,
    start_at: Timestamp,
    /// The boundary the guest declared for this step, carried to the trap so
    /// the authority records the durable kind and name it commits against.
    meta: host::StepMeta,
}

enum JobTrap {
    PersistStep {
        step_id: u64,
        start_at: Timestamp,
        end_at: Timestamp,
        value_json: Arc<str>,
        meta: host::StepMeta,
    },
    WaitStep {
        step_id: u64,
        start_at: Timestamp,
        deets: WaitTrapDeets,
        meta: host::StepMeta,
    },
    RunComplete(Result<String, types::JobError>),
}

#[derive(Debug)]
enum WaitTrapDeets {
    Timer { wait_id: u64, fire_at: Timestamp },
    Message { wait_id: u64 },
}

#[derive(Debug, Clone)]
enum SessionResume {
    Continue,
    Deliver(Arc<str>),
}

fn lookup_active_job<T>(
    accessor: &Accessor<T, HasSelf<SharedWashCtx>>,
    job_id: &str,
) -> Result<(Arc<WflowPlugin>, Arc<ActiveJobCtx>), host::HostError> {
    accessor.with(|mut access| {
        let wcx = access.get();
        let plugin = WflowPlugin::from_ctx(wcx);
        let ctx_id = &wcx.active_ctx.id;
        let active_job = plugin
            .active_jobs
            .read()
            .expect(ERROR_MUTEX)
            .get(ctx_id)
            .cloned()
            .ok_or_else(|| host::HostError::Protocol("job not active".into()))?;
        if active_job.job_id.as_ref() != job_id {
            return Err(host::HostError::Protocol(format!(
                "job_id mismatch for active context: expected {}, got {job_id}",
                active_job.job_id
            )));
        }
        Ok((plugin, active_job))
    })
}

impl ActiveJobCtx {
    /// Parks the session on `trap` until the host resumes it. Cancellation or
    /// channel closure tears down the invocation and surfaces as a protocol error.
    async fn yield_and_wait_resume(&self, trap: JobTrap) -> Result<SessionResume, host::HostError> {
        if self.yield_tx.send(trap).is_err() {
            if self.pause_cancel.is_cancelled() {
                return Err(host::HostError::Protocol("session cancelled".into()));
            }
            return Err(host::HostError::Protocol("session parent dropped".into()));
        }

        let mut resume_rx = self.resume_rx.lock().await;
        let cmd = tokio::select! {
            biased;
            _ = self.pause_cancel.cancelled() => {
                return Err(host::HostError::Protocol("session cancelled".into()));
            }
            cmd = resume_rx.recv() => cmd
        };
        match cmd {
            Some(cmd) => Ok(cmd),
            None => Err(host::HostError::Protocol("session stopped".into())),
        }
    }
}

struct SessionHandle {
    ctx_id: Arc<str>,
    resume_tx: mpsc::UnboundedSender<SessionResume>,
    yield_rx: mpsc::UnboundedReceiver<JobTrap>,
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    jobs: Arc<RwLock<HashMap<Arc<str>, Arc<ActiveJobCtx>>>>,
}

/// The spawned run task watches `cancel_token` and panics if its yield channel
/// is closed while the token is not lit (a dropped owner without cancellation).
/// Teardown can abort the effect worker while a run is in flight, dropping this
/// handle without going through `drop_session_handle`, so the token must always
/// be lit (and the run task aborted) on drop to keep that invariant.
impl Drop for SessionHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.join_handle.abort();
        self.jobs.write().expect(ERROR_MUTEX).remove(&self.ctx_id);
    }
}

impl host::Host for SharedWashCtx {}

impl<T: 'static + Send> host::HostWithStore<T> for HasSelf<SharedWashCtx> {
    async fn next_step(
        accessor: &Accessor<T, Self>,
        job_id: host::JobId,
        meta: host::StepMeta,
    ) -> wasmtime::Result<Result<host::StepState, host::HostError>> {
        let (_plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        if job.pause_cancel.is_cancelled() {
            return Ok(Err(host::HostError::Protocol("session cancelled".into())));
        }
        let mut active_step = job.active_step.lock().expect(ERROR_MUTEX);
        if let Some(stale_active) = active_step.as_ref() {
            let stale_step_id = stale_active.step_id;
            let replay = job.replay.lock().expect(ERROR_MUTEX);
            let stale_completed = replay
                .get(stale_step_id as usize)
                .is_some_and(|step| step.value_json.is_some());
            drop(replay);
            if stale_completed {
                active_step.take();
                job.cur_step
                    .compare_exchange(
                        stale_step_id,
                        stale_step_id + 1,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .expect("impossible: wasm is single threaded");
            } else {
                return Ok(Err(host::HostError::Protocol(
                    "concurrent steps not allowed".into(),
                )));
            }
        }
        let step_id = job.cur_step.load(std::sync::atomic::Ordering::Relaxed);
        let replay = job.replay.lock().expect(ERROR_MUTEX);
        let step = replay.get(step_id as usize);
        if let Err(err) = WflowPlugin::validate_replay_meta(step, step_id, &meta) {
            return Ok(Err(err));
        }
        if let Some(ReplayStep {
            value_json: Some(value),
            ..
        }) = step
        {
            job.cur_step
                .store(step_id + 1, std::sync::atomic::Ordering::SeqCst);
            return Ok(Ok(host::StepState::Completed(host::CompletedStepState {
                id: step_id,
                value_json: value.to_string(),
            })));
        }
        drop(replay);
        let start_at = Timestamp::now();
        active_step.replace(ActiveStepCtx {
            step_id,
            start_at,
            meta,
        });
        Ok(Ok(host::StepState::Active(host::ActiveStepState {
            id: step_id,
        })))
    }

    async fn persist_step(
        accessor: &Accessor<T, Self>,
        job_id: host::JobId,
        step_id: host::StepId,
        value_json: String,
    ) -> wasmtime::Result<Result<(), host::HostError>> {
        let (_plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let trap = {
            let mut active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.take() else {
                return Ok(Err(host::HostError::Protocol("step not active".into())));
            };
            if active_step.step_id != step_id {
                return Ok(Err(host::HostError::Protocol(
                    "given step_id is not active".into(),
                )));
            }
            let end_at = Timestamp::now();
            JobTrap::PersistStep {
                step_id,
                value_json: value_json.into(),
                start_at: active_step.start_at,
                end_at,
                meta: active_step.meta,
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

        Ok(match job.yield_and_wait_resume(trap).await {
            Ok(SessionResume::Continue) => Ok(()),
            Ok(SessionResume::Deliver(_)) => Err(host::HostError::Protocol(
                "persisted step resumed with a delivered value".into(),
            )),
            Err(err) => Err(err),
        })
    }

    async fn sleep(
        accessor: &Accessor<T, Self>,
        job_id: host::JobId,
        step_id: host::StepId,
        duration_ms: u64,
    ) -> wasmtime::Result<Result<(), host::HostError>> {
        let (_plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let (start_at, meta) = {
            let active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.as_ref() else {
                return Ok(Err(host::HostError::Protocol("step not active".into())));
            };
            if active_step.step_id != step_id {
                return Ok(Err(host::HostError::Protocol(
                    "given step_id is not active".into(),
                )));
            }
            (active_step.start_at, active_step.meta.clone())
        };

        let fire_at = start_at
            .checked_add(Duration::from_millis(duration_ms))
            .expect("ts overflow");
        let trap = JobTrap::WaitStep {
            step_id,
            start_at,
            deets: WaitTrapDeets::Timer {
                wait_id: step_id,
                fire_at,
            },
            meta,
        };

        // A timer resolves to no value the workflow observes.
        let resumed = match job.yield_and_wait_resume(trap).await {
            Ok(resumed) => resumed,
            Err(err) => return Ok(Err(err)),
        };
        match resumed {
            SessionResume::Continue => {}
            SessionResume::Deliver(_) => {
                return Ok(Err(host::HostError::Protocol(
                    "sleep resumed with a delivered value".into(),
                )));
            }
        }
        job.active_step.lock().expect(ERROR_MUTEX).take();
        job.cur_step
            .store(step_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(Ok(()))
    }

    async fn recv_message(
        accessor: &Accessor<T, Self>,
        job_id: host::JobId,
        step_id: host::StepId,
    ) -> wasmtime::Result<Result<String, host::HostError>> {
        let (_plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let (start_at, meta) = {
            let active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.as_ref() else {
                return Ok(Err(host::HostError::Protocol("step not active".into())));
            };
            if active_step.step_id != step_id {
                return Ok(Err(host::HostError::Protocol(
                    "given step_id is not active".into(),
                )));
            }
            (active_step.start_at, active_step.meta.clone())
        };

        let trap = JobTrap::WaitStep {
            step_id,
            start_at,
            deets: WaitTrapDeets::Message { wait_id: step_id },
            meta,
        };

        let resumed = match job.yield_and_wait_resume(trap).await {
            Ok(resumed) => resumed,
            Err(err) => return Ok(Err(err)),
        };
        let delivered = match resumed {
            SessionResume::Deliver(value) => Some(value),
            SessionResume::Continue => job
                .replay
                .lock()
                .expect(ERROR_MUTEX)
                .get(step_id as usize)
                .and_then(|step| step.value_json.clone()),
        };
        let Some(value) = delivered else {
            return Ok(Err(host::HostError::Protocol(
                "resumed message has no value".into(),
            )));
        };
        job.active_step.lock().expect(ERROR_MUTEX).take();
        job.cur_step
            .store(step_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(Ok(value.to_string()))
    }
}

#[derive(Default)]
pub struct WflowPlugin {
    registry: workload_registry::WorkloadRegistry<WflowWorkload>,
    // ctx id -> active job
    active_jobs: Arc<RwLock<HashMap<Arc<str>, Arc<ActiveJobCtx>>>>,
}

impl WflowPlugin {
    pub fn new() -> Self {
        Self {
            registry: default(),
            active_jobs: default(),
        }
    }

    const ID: &str = "townframe:wflow";

    pub fn try_from_ctx(wcx: &SharedWashCtx) -> Option<Arc<Self>> {
        wcx.active_ctx.try_get_plugin::<Self>(Self::ID).ok()
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    /// Whether the authority can resolve a submitted key to a live workload.
    pub fn contains_workflow(&self, key: &str) -> bool {
        self.registry.lookup(key).is_some()
    }

    pub fn job_id_of_ctx(&self, wcx: &SharedWashCtx) -> Option<Arc<str>> {
        self.active_jobs
            .read()
            .expect(ERROR_MUTEX)
            .get(&wcx.active_ctx.id[..])
            .map(|job| Arc::clone(&job.job_id))
    }

    fn validate_replay_meta(
        replay: Option<&ReplayStep>,
        step_id: u64,
        meta: &host::StepMeta,
    ) -> Result<(), host::HostError> {
        let Some(replay) = replay else { return Ok(()) };
        let expected_kind = replay.kind.map(|kind| match kind {
            wflow_wdk::StepKind::Effect => host::StepKind::Effect,
            wflow_wdk::StepKind::Sleep => host::StepKind::Sleep,
            wflow_wdk::StepKind::Recv => host::StepKind::Recv,
        });
        let expected_name = replay
            .name
            .as_ref()
            .map(|name| name.as_ref().map(|name| name.to_string()));
        if expected_kind.is_some_and(|kind| kind != meta.kind)
            || expected_name
                .as_ref()
                .is_some_and(|name| name != &meta.name)
        {
            return Err(host::HostError::NondeterministicReplay(
                host::NondeterministicReplayError {
                    step_id,
                    expected: host::StepMeta {
                        kind: expected_kind.unwrap_or(meta.kind),
                        name: expected_name.unwrap_or_else(|| meta.name.clone()),
                    },
                    found: meta.clone(),
                },
            ));
        }
        Ok(())
    }

    /// Instantiates the workload and starts `wflow_key` as `job_id`, replaying
    /// completed boundaries before any step closure runs.
    async fn start_session(
        &self,
        workload: &Arc<WflowWorkload>,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Res<SessionHandle> {
        let mut store = workload
            .resolved_handle
            .new_store(&workload.component_id)
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error creating component store")?;
        let instance = workload
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error creating component store")?;
        let bundle_args = bundle::RunArgs {
            ctx: types::JobCtx {
                job_id: job_id.to_string(),
            },
            wflow_key: wflow_key.to_owned(),
            args_json,
        };
        let ctx_id = Arc::clone(&store.data().active_ctx.id);
        let (yield_tx, yield_rx) = mpsc::unbounded_channel();
        let (resume_tx, resume_rx) = mpsc::unbounded_channel();
        let pause_cancel = CancellationToken::new();
        let active_job = Arc::new(ActiveJobCtx {
            job_id: Arc::clone(&job_id),
            yield_tx: yield_tx.clone(),
            resume_rx: resume_rx.into(),
            pause_cancel: pause_cancel.clone(),
            replay: std::sync::Mutex::new(replay),
            ctx_id: Arc::clone(&ctx_id),
            cur_step: default(),
            active_step: None.into(),
        });
        let _old = self
            .active_jobs
            .write()
            .expect(ERROR_MUTEX)
            .insert(Arc::clone(&ctx_id), Arc::clone(&active_job));
        assert!(_old.is_none(), "fishy");

        let join_handle = tokio::spawn({
            let cancel_token = pause_cancel.clone();
            async move {
                cancel_token
                    .clone()
                    .run_until_cancelled(async move {
                        let trap = match store
                            .run_concurrent(async move |accessor| {
                                instance
                                    .townframe_wflow_bundle()
                                    .call_run(accessor, bundle_args)
                                    .await
                            })
                            .await
                            .and_then(|res| res)
                        {
                            Ok(res) => JobTrap::RunComplete(res),
                            Err(err) => {
                                let terminal =
                                    types::JobError::Terminal(format!("wasm error: {err:?}"));
                                JobTrap::RunComplete(Err(terminal))
                            }
                        };
                        if yield_tx.send(trap).is_err() && !cancel_token.is_cancelled() {
                            panic!("{}", ERROR_CALLER);
                        }
                    })
                    .await;
            }
        });

        Ok(SessionHandle {
            ctx_id,
            resume_tx,
            yield_rx,
            cancel_token: pause_cancel,
            join_handle,
            jobs: Arc::clone(&self.active_jobs),
        })
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct WflowWorkload {
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
                WitInterface::from("townframe:wflow/host"),
            ]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        interface_configs: WitInterfaces<'_>,
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
        let workload_id: Arc<str> = workload.id().into();
        self.registry.bind_pending(workload_id, wflow_keys)?;
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "wflow"
                && iface.interfaces.contains("host")
            {
                host::add_to_linker::<_, HasSelf<SharedWashCtx>>(item.linker(), |ctx| ctx)?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let instance_pre = resolved.instantiate_pre(component_id).await?;
        let instance_pre = binds_service::ServicePre::new(instance_pre)
            .map_err(|err| anyhow::anyhow!("error pre instantiating service component: {err}"))?;
        let workload_id: Arc<str> = resolved.id().into();

        let wflow = Arc::new(WflowWorkload {
            instance_pre,
            resolved_handle: resolved.clone(),
            component_id: component_id.into(),
        });
        self.registry.publish(workload_id, wflow)
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.registry.unbind(workload_id);
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
