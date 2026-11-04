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
use wflow_core::r#gen::metastore::WasmcloudWflowServiceMeta;
use wflow_core::r#gen::metastore::WflowServiceMeta;
use wflow_core::metastore::MetdataStore;
use wflow_core::partition::effects;
use wflow_core::partition::job_events;
use wflow_core::partition::state;
use wflow_tokio::partition::service;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types::CleanupCause;

mod authority_session;

pub mod binds_partition_host {
    wash_runtime::wasmtime::component::bindgen!({
        world: "rt-partition-host",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [serde::Serialize, serde::Deserialize],
        anyhow: false
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
        additional_derives: [PartialEq, Eq],
    });
}
pub use crate::binds_metastore::townframe::wflow::metastore as service_metastore;
pub use crate::binds_partition_host::townframe::wflow::partition_host as service_partition_host;
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
    /// Set once the job entered cleanup; every forward boundary reached after
    /// that returns it as `host-error.interrupted` unless a committed replay
    /// value exists for the step, which replays as usual so the workflow
    /// re-registers the defers it derived from it. `Defer` steps never see it.
    cleanup: std::sync::Mutex<Option<CleanupCause>>,
    ctx_id: Arc<str>,
}

#[derive(Debug)]
struct ActiveStepCtx {
    attempt_id: u64,
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
        attempt_id: u64,
        meta: host::StepMeta,
    },
    WaitStep {
        step_id: u64,
        attempt_id: u64,
        start_at: Timestamp,
        deets: WaitTrapDeets,
        meta: host::StepMeta,
    },
    RunComplete(Result<String, types::JobError>),
    /// Forward work ended in the terminal `error_json` with compensation
    /// registered; the host records the cause durably and resumes with it.
    BeginCleanup {
        error_json: Arc<str>,
    },
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
    /// Resumes a parked boundary with the durable cause the job entered
    /// cleanup under: a committed `PersistStep` continues, a wait or a
    /// `BeginCleanup` returns the cause to the workflow.
    Interrupt(CleanupCause),
    Stop,
}

fn wasmtime_err(msg: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(msg.to_string())
}

fn to_wit_cleanup_cause(cause: CleanupCause) -> host::CleanupCause {
    match cause {
        CleanupCause::Cancelled { reason } => host::CleanupCause::Cancelled(reason),
        CleanupCause::Failed { error } => host::CleanupCause::Failed(error),
        CleanupCause::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        } => host::CleanupCause::RetriesExhausted(host::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        }),
    }
}

fn wait_id_for(step_id: u64, attempt_id: u64) -> u64 {
    let lo = step_id & 0xFFFF_FFFF;
    let hi = (attempt_id & 0xFFFF_FFFF) << 32;
    hi | lo
}

fn replay_from_job_state(journal: &state::JobState) -> Vec<ReplayStep> {
    let mut replay: Vec<_> = journal
        .steps
        .iter()
        .map(|step| {
            let state::JobStepState::Effect { attempts } = step;
            match attempts.last().map(|attempt| &attempt.deets) {
                Some(job_events::JobEffectResultDeets::Success { value_json }) => {
                    ReplayStep::completed(Arc::clone(value_json))
                }
                _ => ReplayStep {
                    failed_attempts: attempts.len() as u64,
                    ..default()
                },
            }
        })
        .collect();
    for run in &journal.runs {
        if let job_events::JobRunResult::StepWait(wait) = &run.result {
            let index = wait.step_id as usize;
            if replay.len() <= index {
                replay.resize_with(index + 1, default);
            }
            replay[index].kind = Some(match wait.deets {
                job_events::JobWaitResultDeets::Timer { .. } => wflow_core::StepKind::Sleep,
                job_events::JobWaitResultDeets::Message { .. } => wflow_core::StepKind::Recv,
            });
        }
    }
    replay
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
    /// Parks the session on `trap` until the host resumes it. A hard stop
    /// (the session was cancelled or its channels closed) is not cooperative
    /// cleanup: the invocation is being torn down, so it surfaces as a
    /// protocol error rather than an interruption that would run defers.
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
            Some(SessionResume::Stop) | None => {
                Err(host::HostError::Protocol("session stopped".into()))
            }
            Some(cmd) => Ok(cmd),
        }
    }

    /// Records the durable cause the job entered cleanup under so every later
    /// forward boundary is interrupted with it. The authority owns which cause
    /// wins (an earlier cancel over a later failure); the host only mirrors
    /// what it was resumed with.
    fn record_cleanup(&self, cause: CleanupCause) {
        *self.cleanup.lock().expect(ERROR_MUTEX) = Some(cause);
    }
}

struct SessionHandle {
    job_id: Arc<str>,
    ctx_id: Arc<str>,
    workload_id: Arc<str>,
    component_id: String,
    next_run_id: u64,
    last_effect_id: effects::EffectId,
    active_job: Arc<ActiveJobCtx>,
    resume_tx: mpsc::UnboundedSender<SessionResume>,
    yield_rx: mpsc::UnboundedReceiver<JobTrap>,
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    jobs: Arc<RwLock<HashMap<Arc<str>, Arc<ActiveJobCtx>>>>,
}

impl SessionHandle {
    fn request_cancel(&self) {
        self.resume_tx
            .send(SessionResume::Stop)
            .inspect_err(|err| warn_loc!(ERROR_CHANNEL, ?err))
            .ok();
        self.cancel_token.cancel();
    }

    /// Moves the parked session into cleanup under `cause`: the flag makes
    /// every later forward boundary return `interrupted`, and the resume wakes
    /// whichever boundary is parked right now (a committed persist continues,
    /// a wait or `begin-cleanup` returns the cause). `false` means the session
    /// was already stopped.
    #[must_use]
    fn interrupt(&self, cause: CleanupCause) -> bool {
        self.active_job.record_cleanup(cause.clone());
        self.resume_tx.send(SessionResume::Interrupt(cause)).is_ok()
    }
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

struct WasmRunSession {
    session: Option<SessionHandle>,
}

impl service::WflowServiceSession for WasmRunSession {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl host::Host for SharedWashCtx {}

impl<T: 'static + Send> host::HostWithStore<T> for HasSelf<SharedWashCtx> {
    async fn next_step(
        accessor: &Accessor<T, Self>,
        job_id: partition_host::JobId,
        meta: host::StepMeta,
    ) -> wasmtime::Result<Result<host::StepState, host::HostError>> {
        let (plugin, job) = match lookup_active_job(accessor, &job_id) {
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
        // A job in cleanup only moves through committed forward steps (so the
        // workflow re-registers the defers it derived from them); the first
        // uncommitted forward boundary is where it is interrupted.
        if meta.kind != host::StepKind::Defer
            && step.is_none_or(|step| step.value_json.is_none())
            && let Some(cause) = job.cleanup.lock().expect(ERROR_MUTEX).clone()
        {
            return Ok(Err(host::HostError::Interrupted(to_wit_cleanup_cause(
                cause,
            ))));
        }
        if let Err(err) = plugin.validate_and_record_meta(&job, step, step_id, &meta) {
            return Ok(Err(err));
        }
        let attempt_id = match step {
            Some(ReplayStep {
                value_json: Some(value),
                ..
            }) => {
                job.cur_step
                    .store(step_id + 1, std::sync::atomic::Ordering::SeqCst);
                return Ok(Ok(host::StepState::Completed(host::CompletedStepState {
                    id: step_id,
                    value_json: value.to_string(),
                })));
            }
            Some(step) => step.failed_attempts,
            None => 0,
        };
        drop(replay);
        let start_at = Timestamp::now();
        active_step.replace(ActiveStepCtx {
            attempt_id,
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
        job_id: partition_host::JobId,
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
                attempt_id: active_step.attempt_id,
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
            // The host only interrupts a parked persist after its commit
            // landed, so the step is durable and the workflow observes the
            // cause at its next forward boundary.
            Ok(SessionResume::Interrupt(cause)) => {
                job.record_cleanup(cause);
                Ok(())
            }
            Ok(SessionResume::Deliver(_)) => Err(host::HostError::Protocol(
                "persisted step resumed with a delivered value".into(),
            )),
            Ok(SessionResume::Stop) => unreachable!(),
            Err(err) => Err(err),
        })
    }

    async fn sleep(
        accessor: &Accessor<T, Self>,
        job_id: partition_host::JobId,
        step_id: host::StepId,
        duration_ms: u64,
    ) -> wasmtime::Result<Result<(), host::HostError>> {
        let (plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let (attempt_id, start_at, meta) = {
            let active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.as_ref() else {
                return Ok(Err(host::HostError::Protocol("step not active".into())));
            };
            if active_step.step_id != step_id {
                return Ok(Err(host::HostError::Protocol(
                    "given step_id is not active".into(),
                )));
            }
            (
                active_step.attempt_id,
                active_step.start_at,
                active_step.meta.clone(),
            )
        };

        let fire_at = start_at
            .checked_add(Duration::from_millis(duration_ms))
            .expect("ts overflow");
        let trap = JobTrap::WaitStep {
            step_id,
            attempt_id,
            start_at,
            deets: WaitTrapDeets::Timer {
                wait_id: wait_id_for(step_id, attempt_id),
                fire_at,
            },
            meta,
        };

        // A timer resolves to no value the workflow observes.
        let resumed = match job.yield_and_wait_resume(trap).await {
            Ok(resumed) => resumed,
            Err(err) => return Ok(Err(err)),
        };
        if let SessionResume::Interrupt(cause) = resumed {
            plugin.abandon_active_step(&job, cause.clone());
            return Ok(Err(host::HostError::Interrupted(to_wit_cleanup_cause(
                cause,
            ))));
        }
        job.active_step.lock().expect(ERROR_MUTEX).take();
        job.cur_step
            .store(step_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(Ok(()))
    }

    async fn recv_message(
        accessor: &Accessor<T, Self>,
        job_id: partition_host::JobId,
        step_id: host::StepId,
    ) -> wasmtime::Result<Result<String, host::HostError>> {
        let (plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let (attempt_id, start_at, meta) = {
            let active_step = job.active_step.lock().expect(ERROR_MUTEX);
            let Some(active_step) = active_step.as_ref() else {
                return Ok(Err(host::HostError::Protocol("step not active".into())));
            };
            if active_step.step_id != step_id {
                return Ok(Err(host::HostError::Protocol(
                    "given step_id is not active".into(),
                )));
            }
            (
                active_step.attempt_id,
                active_step.start_at,
                active_step.meta.clone(),
            )
        };

        let trap = JobTrap::WaitStep {
            step_id,
            attempt_id,
            start_at,
            deets: WaitTrapDeets::Message {
                wait_id: wait_id_for(step_id, attempt_id),
            },
            meta,
        };

        let resumed = match job.yield_and_wait_resume(trap).await {
            Ok(resumed) => resumed,
            Err(err) => return Ok(Err(err)),
        };
        let delivered = match resumed {
            SessionResume::Deliver(value) => Some(value),
            // The legacy partition worker delivers by rewriting the replay
            // before a plain resume.
            SessionResume::Continue => job
                .replay
                .lock()
                .expect(ERROR_MUTEX)
                .get(step_id as usize)
                .and_then(|step| step.value_json.clone()),
            SessionResume::Interrupt(cause) => {
                plugin.abandon_active_step(&job, cause.clone());
                return Ok(Err(host::HostError::Interrupted(to_wit_cleanup_cause(
                    cause,
                ))));
            }
            SessionResume::Stop => unreachable!(),
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

    async fn begin_cleanup(
        accessor: &Accessor<T, Self>,
        job_id: partition_host::JobId,
        error_json: String,
    ) -> wasmtime::Result<Result<host::CleanupCause, host::HostError>> {
        let (plugin, job) = match lookup_active_job(accessor, &job_id) {
            Ok(pair) => pair,
            Err(err) => return Ok(Err(err)),
        };
        let trap = JobTrap::BeginCleanup {
            error_json: error_json.into(),
        };
        // The only valid resume carries the cause the host recorded durably;
        // the guest never gets to proceed to a defer on a plain continue.
        Ok(match job.yield_and_wait_resume(trap).await {
            // Forward work is over: the effect whose closure failed terminally
            // (if any) is still the active step; it never persisted, so its
            // slot is freed for the first defer instead of reporting a
            // concurrent step.
            Ok(SessionResume::Interrupt(cause)) => {
                plugin.abandon_active_step(&job, cause.clone());
                Ok(to_wit_cleanup_cause(cause))
            }
            Ok(SessionResume::Continue | SessionResume::Deliver(_)) => Err(
                host::HostError::Protocol("begin-cleanup resumed without a cleanup cause".into()),
            ),
            Ok(SessionResume::Stop) => unreachable!(),
            Err(err) => Err(err),
        })
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

    async fn send_message(
        &mut self,
        _id: partition_host::PartitionId,
        _job_id: partition_host::JobId,
        _payload_json: String,
    ) -> wasmtime::Result<()> {
        todo!()
    }
}

impl metastore::Host for SharedWashCtx {
    async fn get_wflow(&mut self, key: String) -> wasmtime::Result<Option<metastore::WflowMeta>> {
        let plugin = WflowPlugin::from_ctx(self);
        let meta = plugin
            .metastore
            .get_wflow(&key)
            .await
            .map_err(wasmtime_err)?;
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
        let meta = plugin
            .metastore
            .get_partitions()
            .await
            .map_err(wasmtime_err)?;
        Ok(metastore::PartitionsMeta {
            version: meta.version,
            partition_count: meta.partition_count,
        })
    }
}

#[derive(Default)]
struct JobStepMetas {
    ctx_id: Arc<str>,
    steps: Vec<host::StepMeta>,
}

pub struct WflowPlugin {
    pending_workloads: DHashMap<Arc<str>, HashSet<Arc<str>>>,

    // workload_id -> workload
    active_workloads: RwLock<HashMap<Arc<str>, Arc<WflowWorkload>>>,
    // wflow key -> workload_id
    active_keys: DHashMap<Arc<str>, Arc<str>>,
    // ctx id -> active job
    active_jobs: Arc<RwLock<HashMap<Arc<str>, Arc<ActiveJobCtx>>>>,
    job_step_metas: std::sync::Mutex<HashMap<Arc<str>, JobStepMetas>>,
    metastore: Arc<dyn MetdataStore>,
}

impl WflowPlugin {
    pub fn new(metastore: Arc<dyn MetdataStore>) -> Self {
        Self {
            active_workloads: default(),
            pending_workloads: default(),
            active_keys: default(),
            active_jobs: default(),
            job_step_metas: default(),
            metastore,
        }
    }

    const ID: &str = "townframe:wflow";

    pub fn try_from_ctx(wcx: &SharedWashCtx) -> Option<Arc<Self>> {
        wcx.active_ctx.try_get_plugin::<Self>(Self::ID).ok()
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    pub fn job_id_of_ctx(&self, wcx: &SharedWashCtx) -> Option<Arc<str>> {
        self.active_jobs
            .read()
            .expect(ERROR_MUTEX)
            .get(&wcx.active_ctx.id[..])
            .map(|job| Arc::clone(&job.job_id))
    }

    fn validate_and_record_meta(
        &self,
        job: &ActiveJobCtx,
        replay: Option<&ReplayStep>,
        step_id: u64,
        meta: &host::StepMeta,
    ) -> Result<(), host::HostError> {
        let expected_kind = replay.and_then(|step| step.kind).map(|kind| match kind {
            wflow_core::StepKind::Effect => host::StepKind::Effect,
            wflow_core::StepKind::Sleep => host::StepKind::Sleep,
            wflow_core::StepKind::Recv => host::StepKind::Recv,
            wflow_core::StepKind::Defer => host::StepKind::Defer,
        });
        if let Some(kind) = expected_kind
            && kind != meta.kind
            && !matches!(
                (kind, meta.kind),
                (host::StepKind::Effect, host::StepKind::Defer)
            )
        {
            return Err(host::HostError::NondeterministicReplay(
                host::NondeterministicReplayError {
                    step_id,
                    expected: host::StepMeta {
                        kind,
                        name: meta.name.clone(),
                    },
                    found: meta.clone(),
                },
            ));
        }
        let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
        let Some(metas) = all_metas.get_mut(&job.job_id) else {
            return Ok(());
        };
        if let Some(expected) = metas.steps.get(step_id as usize) {
            if expected != meta {
                return Err(host::HostError::NondeterministicReplay(
                    host::NondeterministicReplayError {
                        step_id,
                        expected: expected.clone(),
                        found: meta.clone(),
                    },
                ));
            }
        } else if metas.ctx_id == job.ctx_id && metas.steps.len() == step_id as usize {
            metas.steps.push(meta.clone());
        }
        Ok(())
    }

    /// Abandons `job`'s uncommitted active step for cleanup: a wait the host
    /// interrupted, or an effect whose closure failed terminally. It never
    /// persisted, so its step slot is the one the first defer takes: the
    /// cursor stays put and the boundary identity recorded for the slot is
    /// dropped, matching what a cold replay in cleanup records there.
    fn abandon_active_step(&self, job: &ActiveJobCtx, cause: CleanupCause) {
        job.record_cleanup(cause);
        let Some(active) = job.active_step.lock().expect(ERROR_MUTEX).take() else {
            return;
        };
        let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
        if let Some(metas) = all_metas.get_mut(&job.job_id)
            && metas.ctx_id == job.ctx_id
            && metas.steps.len() == active.step_id as usize + 1
        {
            metas.steps.truncate(active.step_id as usize);
        }
    }

    fn drop_session_handle(&self, session: SessionHandle) {
        drop(session);
    }

    fn trap_to_result(trap: JobTrap) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        match trap {
            JobTrap::RunComplete(Err(err)) => match err {
                types::JobError::Transient(err) => Err(job_events::JobError::Transient {
                    error_json: err.error_json.into(),
                    retry_policy: err.retry_policy.map(|policy| match policy {
                        types::RetryPolicy::Immediate => {
                            wflow_core::partition::RetryPolicy::Immediate
                        }
                        types::RetryPolicy::Fixed(fixed) => {
                            wflow_core::partition::RetryPolicy::Fixed {
                                delay_ms: fixed.delay_ms,
                            }
                        }
                        types::RetryPolicy::CappedExponential(exp) => {
                            wflow_core::partition::RetryPolicy::CappedExponential {
                                base_ms: exp.base_ms,
                                cap_ms: exp.cap_ms,
                            }
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
                ..
            } => Ok(job_events::JobRunResult::StepEffect(
                job_events::JobEffectResult {
                    step_id,
                    attempt_id,
                    start_at,
                    end_at,
                    deets: job_events::JobEffectResultDeets::Success { value_json },
                },
            )),
            JobTrap::WaitStep {
                step_id,
                attempt_id,
                start_at,
                deets,
                ..
            } => Ok(job_events::JobRunResult::StepWait(
                job_events::JobWaitResult {
                    step_id,
                    attempt_id,
                    start_at,
                    deets: match deets {
                        WaitTrapDeets::Timer { wait_id, fire_at } => {
                            job_events::JobWaitResultDeets::Timer { wait_id, fire_at }
                        }
                        WaitTrapDeets::Message { wait_id } => {
                            job_events::JobWaitResultDeets::Message { wait_id }
                        }
                    },
                },
            )),
            JobTrap::RunComplete(Ok(value_json)) => Ok(job_events::JobRunResult::Success {
                value_json: value_json.into(),
            }),
            JobTrap::BeginCleanup { .. } => {
                unreachable!("begin-cleanup is acknowledged in wait_for_session_yield")
            }
        }
    }

    /// Instantiates the workload and starts `wflow_key` as `job_id`, replaying
    /// `replay` before any step closure runs. `cleanup` seeds the session of a
    /// job that already entered cleanup durably: its committed forward steps
    /// replay and the first uncommitted forward boundary is interrupted.
    async fn start_session(
        &self,
        workload: &Arc<WflowWorkload>,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
        cleanup: Option<CleanupCause>,
    ) -> Result<SessionHandle, job_events::JobRunResult> {
        let mut store = workload
            .resolved_handle
            .new_store(&workload.component_id)
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error creating component store")
            .map_err(Into::<job_events::JobRunResult>::into)?;
        let instance = workload
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|err| eyre::eyre!("{err}"))
            .wrap_err("error creating component store")
            .map_err(Into::<job_events::JobRunResult>::into)?;
        let bundle_args = bundle::RunArgs {
            ctx: types::JobCtx {
                job_id: job_id.to_string(),
            },
            wflow_key: wflow_key.to_owned(),
            args_json,
        };
        let ctx_id = Arc::clone(&store.data().active_ctx.id);
        {
            let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
            let metas = all_metas.entry(Arc::clone(&job_id)).or_default();
            metas.ctx_id = Arc::clone(&ctx_id);
            metas.steps.truncate(replay.len());
        }
        let (yield_tx, yield_rx) = mpsc::unbounded_channel();
        let (resume_tx, resume_rx) = mpsc::unbounded_channel();
        let pause_cancel = CancellationToken::new();
        let active_job = Arc::new(ActiveJobCtx {
            job_id: Arc::clone(&job_id),
            yield_tx: yield_tx.clone(),
            resume_rx: resume_rx.into(),
            pause_cancel: pause_cancel.clone(),
            replay: std::sync::Mutex::new(replay),
            cleanup: std::sync::Mutex::new(cleanup),
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
            job_id: Arc::clone(&job_id),
            ctx_id,
            workload_id: Arc::from(workload.resolved_handle.id()),
            component_id: workload.component_id.clone(),
            next_run_id: 0,
            last_effect_id: effects::EffectId {
                entry_id: 0,
                effect_idx: 0,
            },
            active_job,
            resume_tx,
            yield_rx,
            cancel_token: pause_cancel,
            join_handle,
            jobs: Arc::clone(&self.active_jobs),
        })
    }

    async fn wait_for_session_yield(
        &self,
        session: &mut SessionHandle,
        cancel_token: &CancellationToken,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        loop {
            let trap = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    session.request_cancel();
                    return Ok(job_events::JobRunResult::Aborted);
                }
                trap = session.yield_rx.recv() => trap
            };
            let Some(trap) = trap else {
                return Err(job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::Other {
                        msg: "session loop closed without yielding".into(),
                    },
                ));
            };
            // The legacy partition journal has no cleanup phase to record the
            // cause in, so it is acknowledged NON-durably: the defers still run
            // on a terminal error, but a crash mid-cleanup re-runs forward work
            // on the next run instead of resuming compensation. Only the
            // authority runtime makes this durable; this path is slated for
            // obsolete-path review.
            if let JobTrap::BeginCleanup { error_json } = trap {
                if !session.interrupt(CleanupCause::Failed {
                    error: error_json.to_string(),
                }) {
                    return Err(job_events::JobRunResult::WorkerErr(
                        job_events::JobRunWorkerError::Other {
                            msg: "session stopped before cleanup was acknowledged".into(),
                        },
                    ));
                }
                continue;
            }
            return Self::trap_to_result(trap);
        }
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
        item: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "wflow" {
                if iface.interfaces.contains("host") {
                    host::add_to_linker::<_, HasSelf<SharedWashCtx>>(item.linker(), |ctx| ctx)?;
                }
                if iface.interfaces.contains("partition-host") {
                    partition_host::add_to_linker::<_, HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("metadata-store") {
                    metastore::add_to_linker::<_, HasSelf<SharedWashCtx>>(item.linker(), |ctx| {
                        ctx
                    })?;
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
            .map_err(|err| anyhow::anyhow!("error pre instantiating service component: {err}"))?;

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
            if let Some(old_workload_id) = old
                && old_workload_id != workload_id
            {
                anyhow::bail!(
                    "wflow key '{key}' already mapped to workload '{old_workload_id}', cannot remap to '{workload_id}'"
                );
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
        _interfaces: WitInterfaces<'_>,
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
        run_ctx: &service::RunJobCtx,
        job_id: Arc<str>,
        journal: state::JobState,
        mut session: Option<Box<dyn service::WflowServiceSession>>,
        cancel_token: CancellationToken,
        args: &Self::ExtraArgs,
    ) -> service::RunJobReply {
        let Some(workload) = self
            .active_workloads
            .read()
            .expect(ERROR_MUTEX)
            .get(&args.workload_id[..])
            .cloned()
        else {
            return service::RunJobReply {
                result: Err(job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::WflowNotFound,
                )),
                session: None,
            };
        };
        let start_session = |journal: state::JobState| {
            let workload = &workload;
            let job_id = Arc::clone(&job_id);
            async move {
                self.start_session(
                    workload,
                    job_id,
                    &journal.wflow.key,
                    journal.init_args_json.to_string(),
                    replay_from_job_state(&journal),
                    None,
                )
                .await
            }
        };
        let mut session = if let Some(mut session_box) = session.take() {
            let session = session_box
                .as_any_mut()
                .downcast_mut::<WasmRunSession>()
                .expect("invalid session type for wasm host")
                .session
                .take()
                .expect("wasm session already taken");
            let session_valid = session.next_run_id == run_ctx.run_id
                && session.job_id == job_id
                && session.last_effect_id != run_ctx.effect_id
                && session.workload_id.as_ref() == args.workload_id
                && session.component_id == workload.component_id
                && self
                    .active_jobs
                    .read()
                    .expect(ERROR_MUTEX)
                    .contains_key(&session.ctx_id);
            if !session_valid {
                self.drop_session_handle(session);
                match start_session(journal.clone()).await {
                    Ok(session) => session,
                    Err(result) => {
                        return service::RunJobReply {
                            result: Err(result),
                            session: None,
                        };
                    }
                }
            } else {
                *session.active_job.replay.lock().expect(ERROR_MUTEX) =
                    replay_from_job_state(&journal);
                if session.resume_tx.send(SessionResume::Continue).is_err() {
                    self.drop_session_handle(session);
                    match start_session(journal.clone()).await {
                        Ok(session) => session,
                        Err(result) => {
                            return service::RunJobReply {
                                result: Err(result),
                                session: None,
                            };
                        }
                    }
                } else {
                    session
                }
            }
        } else {
            match start_session(journal.clone()).await {
                Ok(session) => session,
                Err(result) => {
                    return service::RunJobReply {
                        result: Err(result),
                        session: None,
                    };
                }
            }
        };

        let result = self
            .wait_for_session_yield(&mut session, &cancel_token)
            .await;
        match &result {
            Ok(_) | Err(_) => {
                session.next_run_id = run_ctx.run_id + 1;
                session.last_effect_id = run_ctx.effect_id.clone();
            }
        }
        service::RunJobReply {
            result,
            session: Some(Box::new(WasmRunSession {
                session: Some(session),
            })),
        }
    }

    fn drop_session(&self, mut session: Box<dyn service::WflowServiceSession>) {
        let session = session
            .as_any_mut()
            .downcast_mut::<WasmRunSession>()
            .expect("invalid session type for wasm host")
            .session
            .take()
            .expect("wasm session already taken");
        self.drop_session_handle(session);
    }
}
