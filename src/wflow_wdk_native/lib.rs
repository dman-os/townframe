mod interlude {
    pub(crate) use serde::de::DeserializeOwned;
    pub(crate) use std::collections::HashMap;
    pub(crate) use std::rc::Rc;
    pub(crate) use std::sync::Mutex;
    pub(crate) use std::sync::RwLock;
    pub(crate) use std::sync::atomic::AtomicBool;
    pub(crate) use std::sync::atomic::Ordering;
    pub(crate) use utils_rs::prelude::*;
}

#[cfg(test)]
mod tests;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use wflow_core::partition::effects;
use wflow_core::partition::job_events;
use wflow_core::partition::state;
use wflow_wdk::ActiveStepState;
use wflow_wdk::BackendError;
use wflow_wdk::CompletedStepState;
use wflow_wdk::JobErrorX;
use wflow_wdk::LocalBoxFuture;
use wflow_wdk::StepId;
use wflow_wdk::StepKind;
use wflow_wdk::StepMeta;
use wflow_wdk::StepState;
use wflow_wdk::WflowBackend;
use wflow_wdk::WflowCtx;
use wflow_wdk::run_wflow;
use wflow_wdk::types;

use crate::interlude::*;

pub type NativeWflowRunner = Arc<
    dyn Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
        + Send
        + Sync,
>;

use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types::CleanupCause;

#[derive(Debug, Clone)]
enum SessionResume {
    Continue,
    /// Resumes a wait with the value the host resolved it to.
    Deliver(Arc<str>),
    /// Resumes a parked boundary with the durable cause the job entered
    /// cleanup under: a committed `PersistStep` continues, a wait or a
    /// `BeginCleanup` returns the cause to the workflow.
    Interrupt(CleanupCause),
}

#[derive(Debug, Clone)]
struct ActiveStepCtx {
    step_id: u64,
    attempt_id: u64,
    start_at: Timestamp,
    /// The boundary kind/name captured by `next_step`, so every yield that
    /// completes or parks this step reports the identity that was validated
    /// against the replay log.
    meta: StepMeta,
}

struct ActiveJobState {
    replay: Vec<ReplayStep>,
    cur_step: u64,
    active_step: Option<ActiveStepCtx>,
    /// Set once the job entered cleanup; every forward boundary reached after
    /// that returns it as [`BackendError::Interrupted`] unless a committed
    /// replay value exists for the step, which replays as usual so the
    /// workflow re-registers the defers it derived from it.
    cleanup: Option<CleanupCause>,
}

struct ActiveJobShared {
    job_id: Arc<str>,
    session_gen: u64,
    state: Mutex<ActiveJobState>,
    yield_tx: async_channel::Sender<SessionYield>,
    resume_rx: async_channel::Receiver<SessionResume>,
    pause_cancel: CancellationToken,
    finished: AtomicBool,
    #[expect(
        clippy::type_complexity,
        reason = "per-job generation and step metadata map"
    )]
    job_step_metas: Arc<Mutex<HashMap<Arc<str>, (u64, Vec<StepMeta>)>>>,
}

pub struct NativeWflowBackend {
    shared: Arc<ActiveJobShared>,
}

fn wait_id_for(step_id: u64, attempt_id: u64) -> u64 {
    let lo = step_id & 0xFFFF_FFFF;
    let hi = (attempt_id & 0xFFFF_FFFF) << 32;
    hi | lo
}

/// Converts the legacy partition journal into replay steps: a step whose last
/// attempt succeeded replays its value, otherwise the next attempt id is the
/// number of recorded attempts; durable waits pin the step kind.
pub fn replay_from_job_state(journal: &state::JobState) -> Vec<ReplayStep> {
    let mut replay: Vec<ReplayStep> = journal
        .steps
        .iter()
        .map(|step| {
            let state::JobStepState::Effect { attempts } = step;
            match attempts.last().map(|attempt| &attempt.deets) {
                Some(job_events::JobEffectResultDeets::Success { value_json }) => {
                    ReplayStep::completed(value_json.to_string())
                }
                Some(job_events::JobEffectResultDeets::EffectErr(_)) | None => ReplayStep {
                    failed_attempts: attempts.len() as u64,
                    ..default()
                },
            }
        })
        .collect();
    for run in &journal.runs {
        let job_events::JobRunResult::StepWait(wait) = &run.result else {
            continue;
        };
        let idx = wait.step_id as usize;
        if replay.len() <= idx {
            replay.resize_with(idx + 1, default);
        }
        replay[idx].kind = Some(match wait.deets {
            job_events::JobWaitResultDeets::Timer { .. } => StepKind::Sleep,
            job_events::JobWaitResultDeets::Message { .. } => StepKind::Recv,
        });
    }
    replay
}

fn kind_compatible(replayed: StepKind, actual: StepKind) -> bool {
    replayed == actual || matches!((replayed, actual), (StepKind::Effect, StepKind::Defer))
}

impl ActiveJobShared {
    fn validate_and_record_meta(
        &self,
        replay: Option<&ReplayStep>,
        step_id: u64,
        meta: &StepMeta,
    ) -> Result<(), BackendError> {
        // A durable replay pins the boundary identity of every step it
        // recorded. `kind` is always pinned when present; `name` is tri-state,
        // where `None` means the source journals no names and pins nothing.
        if let Some(step) = replay {
            let kind_diverged = step
                .kind
                .is_some_and(|kind| !kind_compatible(kind, meta.kind));
            let name_diverged = step
                .name
                .as_ref()
                .is_some_and(|name| name.as_deref() != meta.name.as_deref());
            if kind_diverged || name_diverged {
                return Err(BackendError::NondeterministicReplay {
                    step_id,
                    // Fields the durable record does not pin are echoed from
                    // the found meta, so the report isolates the divergence.
                    expected: StepMeta {
                        kind: step.kind.unwrap_or(meta.kind),
                        name: step
                            .name
                            .clone()
                            .flatten()
                            .map_or_else(|| meta.name.clone(), |name| Some(name.to_string())),
                    },
                    found: meta.clone(),
                });
            }
        }
        let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
        let Some((latest_gen, metas)) = all_metas.get_mut(&self.job_id) else {
            return Ok(());
        };
        if let Some(expected) = metas.get(step_id as usize) {
            if expected != meta {
                return Err(BackendError::NondeterministicReplay {
                    step_id,
                    expected: expected.clone(),
                    found: meta.clone(),
                });
            }
            return Ok(());
        }
        if *latest_gen == self.session_gen && metas.len() == step_id as usize {
            metas.push(meta.clone());
        }
        Ok(())
    }

    /// Parks the session on `trap` until the host resumes it. A hard stop
    /// (the session was cancelled or its channels closed) is not cooperative
    /// cleanup: the invocation is being torn down, so it surfaces as a
    /// protocol error rather than an interruption that would run defers.
    async fn yield_and_wait_resume(
        &self,
        trap: SessionYield,
    ) -> Result<SessionResume, BackendError> {
        if self.yield_tx.send(trap).await.is_err() {
            if self.pause_cancel.is_cancelled() {
                return Err(BackendError::Protocol("session cancelled".into()));
            }
            return Err(BackendError::Protocol("session parent dropped".into()));
        }
        let cmd = tokio::select! {
            biased;
            _ = self.pause_cancel.cancelled() => {
                return Err(BackendError::Protocol("session cancelled".into()));
            }
            cmd = self.resume_rx.recv() => cmd.ok(),
        };
        cmd.ok_or_else(|| BackendError::Protocol("session stopped".into()))
    }

    /// Abandons the uncommitted active step for cleanup: a wait the host
    /// interrupted, or an effect whose closure failed terminally. It never
    /// committed, so its step slot is the one the first cleanup step takes:
    /// `cur_step` stays put and the boundary identity recorded for the slot is
    /// dropped, matching what a cold replay in cleanup records there.
    fn abandon_active_step(&self, cause: CleanupCause) {
        let mut state = self.state.lock().expect(ERROR_MUTEX);
        state.cleanup = Some(cause);
        let Some(active) = state.active_step.take() else {
            return;
        };
        let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
        if let Some((latest_gen, metas)) = all_metas.get_mut(&self.job_id)
            && *latest_gen == self.session_gen
            && metas.len() == active.step_id as usize + 1
        {
            metas.truncate(active.step_id as usize);
        }
    }

    fn interrupt_wait(&self, cause: CleanupCause) -> BackendError {
        self.abandon_active_step(cause.clone());
        BackendError::Interrupted(cause)
    }
}

impl WflowBackend for NativeWflowBackend {
    fn next_step(&self, meta: StepMeta) -> LocalBoxFuture<'_, Result<StepState, BackendError>> {
        Box::pin(async move {
            if self.shared.pause_cancel.is_cancelled() {
                return Err(BackendError::Protocol("session cancelled".into()));
            }
            let mut state = self.shared.state.lock().expect(ERROR_MUTEX);
            if let Some(stale_active) = state.active_step.as_ref() {
                let stale_step_id = stale_active.step_id;
                let stale_completed = state
                    .replay
                    .get(stale_step_id as usize)
                    .is_some_and(|step| step.value_json.is_some());
                if stale_completed {
                    state.active_step = None;
                    if state.cur_step == stale_step_id {
                        state.cur_step = stale_step_id + 1;
                    }
                } else {
                    return Err(BackendError::Protocol(
                        "concurrent steps not allowed".into(),
                    ));
                }
            }
            let step_id = state.cur_step;
            let replay = state.replay.get(step_id as usize);
            if let Some(cause) = state.cleanup.as_ref()
                && meta.kind != StepKind::Defer
                && replay.is_none_or(|step| step.value_json.is_none())
            {
                return Err(BackendError::Interrupted(cause.clone()));
            }
            self.shared
                .validate_and_record_meta(replay, step_id, &meta)?;

            let attempt_id = match replay {
                Some(ReplayStep {
                    value_json: Some(value_json),
                    ..
                }) => {
                    let value_json = value_json.to_string();
                    state.cur_step = step_id + 1;
                    return Ok(StepState::Completed(CompletedStepState {
                        id: step_id,
                        value_json,
                    }));
                }
                Some(step) => step.failed_attempts,
                None => 0,
            };

            let start_at = Timestamp::now();
            state.active_step = Some(ActiveStepCtx {
                step_id,
                attempt_id,
                start_at,
                meta,
            });
            Ok(StepState::Active(ActiveStepState { id: step_id }))
        })
    }

    fn persist_step<'a>(
        &'a self,
        step_id: StepId,
        value_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), BackendError>> {
        Box::pin(async move {
            let trap = {
                let mut state = self.shared.state.lock().expect(ERROR_MUTEX);
                let Some(active) = state.active_step.take() else {
                    return Err(BackendError::Protocol("step not active".into()));
                };
                if active.step_id != step_id {
                    return Err(BackendError::Protocol("given step_id is not active".into()));
                }
                assert_eq!(state.cur_step, step_id, "step cursor out of sync");
                state.cur_step = step_id + 1;
                SessionYield::PersistStep {
                    step_id,
                    attempt_id: active.attempt_id,
                    start_at: active.start_at,
                    end_at: Timestamp::now(),
                    value_json: Arc::from(value_json),
                    meta: active.meta.clone(),
                }
            };
            match self.shared.yield_and_wait_resume(trap).await? {
                SessionResume::Continue => Ok(()),
                // The host only interrupts a parked persist after its commit
                // landed, so the step is durable and the workflow observes
                // the cause at its next forward boundary.
                SessionResume::Interrupt(cause) => {
                    self.shared.state.lock().expect(ERROR_MUTEX).cleanup = Some(cause);
                    Ok(())
                }
                SessionResume::Deliver(_) => Err(BackendError::Protocol(
                    "persisted step resumed with a delivered value".into(),
                )),
            }
        })
    }

    fn sleep(
        &self,
        step_id: StepId,
        duration_ms: u64,
    ) -> LocalBoxFuture<'_, Result<(), BackendError>> {
        Box::pin(async move {
            let trap = {
                let state = self.shared.state.lock().expect(ERROR_MUTEX);
                let Some(active) = state.active_step.as_ref() else {
                    return Err(BackendError::Protocol("step not active".into()));
                };
                if active.step_id != step_id {
                    return Err(BackendError::Protocol("given step_id is not active".into()));
                }
                let fire_at = active
                    .start_at
                    .checked_add(Duration::from_millis(duration_ms))
                    .expect("ts overflow");
                SessionYield::WaitStep {
                    step_id,
                    attempt_id: active.attempt_id,
                    start_at: active.start_at,
                    deets: WaitYield::Timer {
                        wait_id: wait_id_for(step_id, active.attempt_id),
                        fire_at,
                    },
                    meta: active.meta.clone(),
                }
            };
            // A timer resolves to no value the workflow observes.
            let resumed = self.shared.yield_and_wait_resume(trap).await?;
            if let SessionResume::Interrupt(cause) = resumed {
                return Err(self.shared.interrupt_wait(cause));
            }
            let mut state = self.shared.state.lock().expect(ERROR_MUTEX);
            state.active_step = None;
            state.cur_step = step_id + 1;
            Ok(())
        })
    }

    fn recv_message(&self, step_id: StepId) -> LocalBoxFuture<'_, Result<String, BackendError>> {
        Box::pin(async move {
            let trap = {
                let state = self.shared.state.lock().expect(ERROR_MUTEX);
                let Some(active) = state.active_step.as_ref() else {
                    return Err(BackendError::Protocol("step not active".into()));
                };
                if active.step_id != step_id {
                    return Err(BackendError::Protocol("given step_id is not active".into()));
                }
                SessionYield::WaitStep {
                    step_id,
                    attempt_id: active.attempt_id,
                    start_at: active.start_at,
                    deets: WaitYield::Message {
                        wait_id: wait_id_for(step_id, active.attempt_id),
                    },
                    meta: active.meta.clone(),
                }
            };
            let resumed = self.shared.yield_and_wait_resume(trap).await?;
            let mut state = self.shared.state.lock().expect(ERROR_MUTEX);
            // The legacy partition worker delivers by rewriting the replay
            // before a plain resume.
            let delivered = match resumed {
                SessionResume::Deliver(value_json) => Some(value_json),
                SessionResume::Continue => state
                    .replay
                    .get(step_id as usize)
                    .and_then(|step| step.value_json.clone()),
                SessionResume::Interrupt(cause) => {
                    drop(state);
                    return Err(self.shared.interrupt_wait(cause));
                }
            };
            let Some(value_json) = delivered else {
                return Err(BackendError::Protocol(
                    "resumed message wait has no delivered value".into(),
                ));
            };
            let out = value_json.to_string();
            state.active_step = None;
            state.cur_step = step_id + 1;
            Ok(out)
        })
    }

    fn begin_cleanup<'a>(
        &'a self,
        error_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<CleanupCause, BackendError>> {
        Box::pin(async move {
            let trap = SessionYield::BeginCleanup {
                error_json: Arc::from(error_json),
            };
            match self.shared.yield_and_wait_resume(trap).await? {
                // The effect whose closure failed terminally (if any) is still
                // the active step; it never persisted, so its slot is freed for
                // the first defer instead of reporting a concurrent step.
                SessionResume::Interrupt(cause) => {
                    self.shared.abandon_active_step(cause.clone());
                    Ok(cause)
                }
                SessionResume::Continue | SessionResume::Deliver(_) => Err(BackendError::Protocol(
                    "cleanup began without a durable cause".into(),
                )),
            }
        })
    }
}

struct SpawnSessionCmd {
    runner: NativeWflowRunner,
    run_args: types::RunArgs,
    shared: Arc<ActiveJobShared>,
}

struct LocalExecutor {
    spawn_tx: async_channel::Sender<SpawnSessionCmd>,
    shutdown: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LocalExecutor {
    fn new() -> Self {
        let (spawn_tx, spawn_rx) = async_channel::unbounded::<SpawnSessionCmd>();
        let shutdown = CancellationToken::new();
        let shutdown_thread = shutdown.clone();
        let thread = std::thread::Builder::new()
            .name("wflow-native-localset".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect(ERROR_TOKIO);
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    loop {
                        let cmd = tokio::select! {
                            biased;
                            _ = shutdown_thread.cancelled() => break,
                            cmd = spawn_rx.recv() => match cmd {
                                Ok(cmd) => cmd,
                                Err(_) => break,
                            },
                        };
                        Self::spawn_session_task(cmd);
                    }
                });
            })
            .expect("failed to spawn wflow native localset thread");
        Self {
            spawn_tx,
            shutdown,
            thread: Some(thread),
        }
    }

    fn spawn_session_task(cmd: SpawnSessionCmd) {
        let SpawnSessionCmd {
            runner,
            run_args,
            shared,
        } = cmd;
        tokio::task::spawn_local(async move {
            let backend: Rc<dyn WflowBackend> = Rc::new(NativeWflowBackend {
                shared: Arc::clone(&shared),
            });
            let pause_cancel = shared.pause_cancel.clone();
            let fut = std::panic::AssertUnwindSafe((runner)(run_args, backend)).catch_unwind();
            let outcome = pause_cancel.run_until_cancelled(fut).await;
            shared.finished.store(true, Ordering::Release);
            let Some(outcome) = outcome else {
                return;
            };
            let trap = match outcome {
                Ok(res) => SessionYield::Complete(res),
                Err(panic_payload) => {
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        (*s).to_owned()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic payload".to_owned()
                    };
                    SessionYield::Panicked(msg)
                }
            };
            if shared.yield_tx.send(trap).await.is_err() && !shared.pause_cancel.is_cancelled() {
                panic!("{}", ERROR_CALLER);
            }
        });
    }
}

impl Drop for LocalExecutor {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.spawn_tx.close();
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

/// One workflow invocation running on the driver's `LocalSet` thread. The
/// owner pulls suspensions with [`Self::next_yield`] and decides, per
/// suspension, whether the invocation may continue: a parked session only
/// moves past a `PersistStep`/`WaitStep` after [`Self::resume`]. Dropping the
/// session cancels the invocation.
pub struct NativeSession {
    shared: Arc<ActiveJobShared>,
    resume_tx: async_channel::Sender<SessionResume>,
    yield_rx: async_channel::Receiver<SessionYield>,
}

impl NativeSession {
    /// The next suspension or final outcome; `None` once the session was
    /// cancelled.
    pub async fn next_yield(&self) -> Option<SessionYield> {
        self.yield_rx.recv().await.ok()
    }

    /// Lets the parked invocation continue past its last suspension. `false`
    /// means the session was already cancelled.
    #[must_use]
    pub fn resume(&self) -> bool {
        self.resume_tx.try_send(SessionResume::Continue).is_ok()
    }

    /// Lets the invocation parked on a `WaitStep` continue with the value the
    /// wait resolved to: a received message's payload is returned to the
    /// workflow, a timer's value is ignored. `false` means the session was
    /// already cancelled.
    #[must_use]
    pub fn deliver(&self, value_json: Arc<str>) -> bool {
        self.resume_tx
            .try_send(SessionResume::Deliver(value_json))
            .is_ok()
    }

    /// Resumes the invocation parked on a `PersistStep`, `WaitStep` or
    /// `BeginCleanup` yield with the durable cause the job entered cleanup
    /// under. The host calls this only for a parked session, after the cause
    /// is committed: a persisted step continues (its next forward boundary
    /// returns the cause), a wait returns the cause right away, and a
    /// `BeginCleanup` receives it as the cause its defers run under. A
    /// session that is running between yields is not interrupted here; the
    /// host waits for its next yield. `false` means the session was already
    /// cancelled.
    #[must_use]
    pub fn interrupt(&self, cause: CleanupCause) -> bool {
        self.shared.state.lock().expect(ERROR_MUTEX).cleanup = Some(cause.clone());
        self.resume_tx
            .try_send(SessionResume::Interrupt(cause))
            .is_ok()
    }

    pub fn cancel(&self) {
        self.shared.pause_cancel.cancel();
        self.resume_tx.close();
    }

    pub fn is_finished(&self) -> bool {
        self.shared.finished.load(Ordering::Acquire)
    }

    fn replace_replay(&self, replay: Vec<ReplayStep>) {
        self.shared.state.lock().expect(ERROR_MUTEX).replay = replay;
    }
}

impl Drop for NativeSession {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    WflowNotFound,
    ExecutorStopped,
}

/// Warm-session bookkeeping for the legacy partition worker's `run_job`.
pub struct NativeSessionHandle {
    session: NativeSession,
    job_id: Arc<str>,
    wflow_key: Arc<str>,
    next_run_id: u64,
    last_effect_id: effects::EffectId,
}

impl NativeSessionHandle {
    pub fn request_cancel(&self) {
        self.session.cancel();
    }
}

pub struct NativeHostDriver {
    workflows: RwLock<HashMap<Arc<str>, NativeWflowRunner>>,
    #[expect(
        clippy::type_complexity,
        reason = "per-job generation and step metadata map"
    )]
    job_step_metas: Arc<Mutex<HashMap<Arc<str>, (u64, Vec<StepMeta>)>>>,
    executor: LocalExecutor,
}

impl Default for NativeHostDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeHostDriver {
    pub fn new() -> Self {
        Self {
            workflows: RwLock::new(HashMap::new()),
            job_step_metas: Arc::new(Mutex::new(HashMap::new())),
            executor: LocalExecutor::new(),
        }
    }

    pub fn register<F>(&self, key: impl Into<Arc<str>>, runner: F)
    where
        F: Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
            + Send
            + Sync
            + 'static,
    {
        let runner: NativeWflowRunner = Arc::new(runner);
        self.workflows
            .write()
            .expect(ERROR_MUTEX)
            .insert(key.into(), runner);
    }

    pub fn register_bundle<I, K, F>(&self, keys: I, runner: F)
    where
        I: IntoIterator<Item = K>,
        K: Into<Arc<str>>,
        F: Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
            + Send
            + Sync
            + 'static,
    {
        let runner: NativeWflowRunner = Arc::new(runner);
        let mut map = self.workflows.write().expect(ERROR_MUTEX);
        for key in keys {
            map.insert(key.into(), Arc::clone(&runner));
        }
    }

    pub fn register_typed<A, R, F>(&self, key: impl Into<Arc<str>>, handler: F)
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: for<'a> Fn(&'a mut WflowCtx, A) -> LocalBoxFuture<'a, Result<R, JobErrorX>>
            + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        self.register(key, move |args, backend| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                run_wflow(args, backend, move |cx, key, args_json| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let parsed: A = serde_json::from_str(args_json).map_err(|err| {
                            JobErrorX::Terminal(ferr!(
                                "error parsing json ({args_json}) for {key} args: {err}"
                            ))
                        })?;
                        handler(cx, parsed).await
                    })
                })
                .await
            })
        });
    }

    pub fn unregister(&self, key: &str) -> bool {
        self.workflows
            .write()
            .expect(ERROR_MUTEX)
            .remove(key)
            .is_some()
    }

    pub fn contains_wflow(&self, key: &str) -> bool {
        self.workflows.read().expect(ERROR_MUTEX).contains_key(key)
    }

    /// Starts `wflow_key` as job `job_id`, replaying `replay` before running
    /// any step closure. `cleanup` is the durable cleanup the job is already
    /// in, if any: the invocation then replays its committed forward steps
    /// (re-registering their defers) and is interrupted at the first forward
    /// boundary without a committed value. Step metadata recorded for this
    /// job beyond the replayed prefix is discarded: it came from an
    /// invocation whose later steps never became durable.
    pub fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
        cleanup: Option<CleanupCause>,
    ) -> Result<NativeSession, SpawnError> {
        let runner = self
            .workflows
            .read()
            .expect(ERROR_MUTEX)
            .get(wflow_key)
            .cloned()
            .ok_or(SpawnError::WflowNotFound)?;
        let session_gen = {
            let mut all_metas = self.job_step_metas.lock().expect(ERROR_MUTEX);
            let (generation, metas) = all_metas.entry(Arc::clone(&job_id)).or_default();
            *generation += 1;
            metas.truncate(replay.len());
            *generation
        };
        let run_args = types::RunArgs {
            ctx: types::JobCtx {
                job_id: job_id.to_string(),
            },
            wflow_key: wflow_key.to_owned(),
            args_json,
        };
        let (yield_tx, yield_rx) = async_channel::unbounded();
        let (resume_tx, resume_rx) = async_channel::unbounded();
        let shared = Arc::new(ActiveJobShared {
            job_id,
            session_gen,
            state: Mutex::new(ActiveJobState {
                replay,
                cur_step: 0,
                active_step: None,
                cleanup,
            }),
            yield_tx,
            resume_rx,
            pause_cancel: CancellationToken::new(),
            finished: AtomicBool::new(false),
            job_step_metas: Arc::clone(&self.job_step_metas),
        });
        self.executor
            .spawn_tx
            .try_send(SpawnSessionCmd {
                runner,
                run_args,
                shared: Arc::clone(&shared),
            })
            .map_err(|_| SpawnError::ExecutorStopped)?;
        Ok(NativeSession {
            shared,
            resume_tx,
            yield_rx,
        })
    }

    /// Drops the replay-determinism record of a job that reached a terminal
    /// outcome; no later invocation of it can exist.
    pub fn forget_job(&self, job_id: &str) {
        self.job_step_metas
            .lock()
            .expect(ERROR_MUTEX)
            .remove(job_id);
    }

    fn start_session(
        &self,
        job_id: &Arc<str>,
        journal: &state::JobState,
    ) -> Result<NativeSessionHandle, job_events::JobRunResult> {
        let wflow_key = journal.wflow.key.as_str();
        match self.spawn(
            Arc::clone(job_id),
            wflow_key,
            journal.init_args_json.to_string(),
            replay_from_job_state(journal),
            None,
        ) {
            Ok(session) => Ok(NativeSessionHandle {
                session,
                job_id: Arc::clone(job_id),
                wflow_key: Arc::from(wflow_key),
                next_run_id: 0,
                last_effect_id: effects::EffectId {
                    entry_id: 0,
                    effect_idx: 0,
                },
            }),
            Err(SpawnError::WflowNotFound) => Err(job_events::JobRunResult::WorkerErr(
                job_events::JobRunWorkerError::WflowNotFound,
            )),
            Err(SpawnError::ExecutorStopped) => Err(job_events::JobRunResult::WorkerErr(
                job_events::JobRunWorkerError::Other {
                    msg: "native local executor stopped".into(),
                },
            )),
        }
    }

    /// Awaits the next suspension the legacy partition worker records as a
    /// run result. A `BeginCleanup` is acknowledged here non-durably: this
    /// path has no authority to journal the cause, so the session is
    /// interrupted with the terminal error as-is and the loop awaits the
    /// defers' yields. Defers still run on a terminal error, but a crash
    /// mid-cleanup re-runs forward work on the next invocation. The legacy
    /// worker path is slated for obsolete-path review.
    async fn wait_for_session_yield(
        session: &NativeSession,
        cancel_token: &CancellationToken,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        loop {
            let trap = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("cancel token lit for native workflow session");
                    session.cancel();
                    return Ok(job_events::JobRunResult::Aborted);
                }
                trap = session.next_yield() => trap,
            };
            let Some(trap) = trap else {
                return Err(job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::Other {
                        msg: "native session loop closed without yielding".into(),
                    },
                ));
            };
            return match trap {
                SessionYield::BeginCleanup { error_json } => {
                    let cause = CleanupCause::Failed {
                        error: error_json.to_string(),
                    };
                    if session.interrupt(cause) {
                        continue;
                    }
                    Err(job_events::JobRunResult::WorkerErr(
                        job_events::JobRunWorkerError::Other {
                            msg: "native session stopped before its cleanup began".into(),
                        },
                    ))
                }
                SessionYield::Complete(Err(err)) => match err {
                    types::JobError::Transient(err) => Err(job_events::JobError::Transient {
                        error_json: err.error_json.into(),
                        retry_policy: err.retry_policy,
                    }
                    .into()),
                    types::JobError::Terminal(err_json) => Err(job_events::JobError::Terminal {
                        error_json: err_json.into(),
                    }
                    .into()),
                },
                // The legacy run-result shape carries no step kind or name, so
                // the boundary identity is dropped here; only the durable
                // authority journal pins it. Replay against this log therefore
                // leaves names unpinned (`ReplayStep::name == None`).
                SessionYield::PersistStep {
                    step_id,
                    value_json,
                    start_at,
                    end_at,
                    attempt_id,
                    meta: _,
                } => Ok(job_events::JobRunResult::StepEffect(
                    job_events::JobEffectResult {
                        step_id,
                        attempt_id,
                        start_at,
                        end_at,
                        deets: job_events::JobEffectResultDeets::Success { value_json },
                    },
                )),
                SessionYield::WaitStep {
                    step_id,
                    attempt_id,
                    start_at,
                    deets,
                    meta: _,
                } => Ok(job_events::JobRunResult::StepWait(
                    job_events::JobWaitResult {
                        step_id,
                        attempt_id,
                        start_at,
                        deets: match deets {
                            WaitYield::Timer { wait_id, fire_at } => {
                                job_events::JobWaitResultDeets::Timer { wait_id, fire_at }
                            }
                            WaitYield::Message { wait_id } => {
                                job_events::JobWaitResultDeets::Message { wait_id }
                            }
                        },
                    },
                )),
                SessionYield::Complete(Ok(value_json)) => Ok(job_events::JobRunResult::Success {
                    value_json: value_json.into(),
                }),
                SessionYield::Panicked(msg) => Err(job_events::JobRunResult::WorkerErr(
                    job_events::JobRunWorkerError::Other {
                        msg: format!("native workflow task panicked: {msg}"),
                    },
                )),
            };
        }
    }

    pub async fn run_job(
        &self,
        effect_id: &effects::EffectId,
        run_id: u64,
        job_id: Arc<str>,
        journal: state::JobState,
        session: Option<NativeSessionHandle>,
        cancel_token: CancellationToken,
    ) -> (
        Result<job_events::JobRunResult, job_events::JobRunResult>,
        Option<NativeSessionHandle>,
    ) {
        let warm = session.filter(|existing| {
            existing.next_run_id == run_id
                && existing.job_id == job_id
                && &existing.last_effect_id != effect_id
                && existing.wflow_key.as_ref() == journal.wflow.key.as_str()
                && !existing.session.is_finished()
        });
        let resumed = warm.and_then(|existing| {
            existing
                .session
                .replace_replay(replay_from_job_state(&journal));
            existing.session.resume().then_some(existing)
        });
        let mut active_session = match resumed {
            Some(existing) => existing,
            None => match self.start_session(&job_id, &journal) {
                Ok(started) => started,
                Err(err) => return (Err(err), None),
            },
        };

        let result = Self::wait_for_session_yield(&active_session.session, &cancel_token).await;
        active_session.next_run_id = run_id + 1;
        active_session.last_effect_id = effect_id.clone();
        (result, Some(active_session))
    }
}
