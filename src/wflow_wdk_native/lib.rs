//! Native workflow sessions for the authority runtime.
//!
//! This crate owns only the local invocation executor.  It does not translate
//! sessions to partition-worker journals; the authority consumes `SessionYield`
//! directly through `wflow_tokio::authority_runtime::session::Session`.

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
use wflow_wdk::ActiveStepState;
use wflow_wdk::BackendError;
use wflow_wdk::CompletedStepState;
use wflow_wdk::JobErrorX;
use wflow_wdk::LocalBoxFuture;
use wflow_wdk::StepId;
use wflow_wdk::StepMeta;
use wflow_wdk::StepState;
use wflow_wdk::WflowBackend;
use wflow_wdk::WflowCtx;
use wflow_wdk::run_wflow;
use wflow_wdk::session::ReplayDivergence;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types;

use crate::interlude::*;

pub type NativeWflowRunner = Arc<
    dyn Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
        + Send
        + Sync,
>;

#[derive(Debug, Clone)]
enum SessionResume {
    Continue,
    Deliver(Arc<str>),
}

#[derive(Debug, Clone)]
struct ActiveStep {
    id: u64,
    start_at: Timestamp,
    meta: StepMeta,
}

struct Shared {
    replay: Mutex<Vec<ReplayStep>>,
    cur_step: Mutex<u64>,
    active_step: Mutex<Option<ActiveStep>>,
    /// The first replay divergence this invocation hit, if any.
    ///
    /// The adapter detects a fork inside a backend call, where the guest only
    /// sees a terminal error; recording it here lets the session report the
    /// engine class once the run stops, whichever way it stopped.
    diverged: Mutex<Option<ReplayDivergence>>,
    yield_tx: async_channel::Sender<SessionYield>,
    resume_rx: async_channel::Receiver<SessionResume>,
    cancel: CancellationToken,
    finished: AtomicBool,
}

struct NativeBackend {
    shared: Arc<Shared>,
}

impl Shared {
    fn validate_meta(&self, step_id: u64, meta: &StepMeta) -> Result<(), BackendError> {
        let replay = self
            .replay
            .lock()
            .expect(ERROR_MUTEX)
            .get(step_id as usize)
            .cloned();
        if let Some(replay) = replay
            && (replay.kind.is_some_and(|kind| kind != meta.kind)
                || replay
                    .name
                    .as_ref()
                    .is_some_and(|name| name.as_deref() != meta.name.as_deref()))
        {
            // The first fork is the one the run diverged from; a later call
            // cannot be reached without the guest swallowing this error, and the
            // classification must not depend on that.
            let mut diverged = self.diverged.lock().expect(ERROR_MUTEX);
            diverged.get_or_insert(ReplayDivergence {
                step: step_id,
                replayed: meta.kind,
            });
            return Err(BackendError::NondeterministicReplay {
                step_id,
                expected: StepMeta {
                    kind: replay.kind.unwrap_or(meta.kind),
                    name: replay.name.map_or_else(
                        || meta.name.clone(),
                        |name| name.map(|name| name.to_string()),
                    ),
                },
                found: meta.clone(),
            });
        }
        Ok(())
    }

    /// The divergence this invocation recorded, if any.
    fn take_divergence(&self) -> Option<ReplayDivergence> {
        self.diverged.lock().expect(ERROR_MUTEX).take()
    }

    async fn yield_and_wait(&self, yielded: SessionYield) -> Result<SessionResume, BackendError> {
        self.yield_tx
            .send(yielded)
            .await
            .map_err(|_| BackendError::Protocol("session parent dropped".into()))?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(BackendError::Protocol("session cancelled".into())),
            result = self.resume_rx.recv() => result.map_err(|_| BackendError::Protocol("session stopped".into())),
        }
    }
}

impl WflowBackend for NativeBackend {
    fn next_step(&self, meta: StepMeta) -> LocalBoxFuture<'_, Result<StepState, BackendError>> {
        Box::pin(async move {
            if self.shared.cancel.is_cancelled() {
                return Err(BackendError::Protocol("session cancelled".into()));
            }
            let mut active = self.shared.active_step.lock().expect(ERROR_MUTEX);
            if active.is_some() {
                return Err(BackendError::Protocol(
                    "concurrent steps not allowed".into(),
                ));
            }
            let mut cursor = self.shared.cur_step.lock().expect(ERROR_MUTEX);
            let step_id = *cursor;
            self.shared.validate_meta(step_id, &meta)?;
            if let Some(step) = self
                .shared
                .replay
                .lock()
                .expect(ERROR_MUTEX)
                .get(step_id as usize)
                .filter(|step| step.value_json.is_some())
            {
                *cursor += 1;
                return Ok(StepState::Completed(CompletedStepState {
                    id: step_id,
                    value_json: step.value_json.clone().expect("checked above").to_string(),
                }));
            }
            *active = Some(ActiveStep {
                id: step_id,
                start_at: Timestamp::now(),
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
            let active = self
                .shared
                .active_step
                .lock()
                .expect(ERROR_MUTEX)
                .take()
                .ok_or_else(|| BackendError::Protocol("step not active".into()))?;
            if active.id != step_id {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            {
                let mut cursor = self.shared.cur_step.lock().expect(ERROR_MUTEX);
                assert_eq!(*cursor, step_id, "step cursor out of sync");
                *cursor += 1;
            }
            match self
                .shared
                .yield_and_wait(SessionYield::PersistStep {
                    step_id,
                    start_at: active.start_at,
                    end_at: Timestamp::now(),
                    value_json: Arc::from(value_json),
                    meta: active.meta,
                })
                .await?
            {
                SessionResume::Continue => Ok(()),
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
            let active = self
                .shared
                .active_step
                .lock()
                .expect(ERROR_MUTEX)
                .as_ref()
                .cloned()
                .ok_or_else(|| BackendError::Protocol("step not active".into()))?;
            if active.id != step_id {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            let fire_at = active
                .start_at
                .checked_add(Duration::from_millis(duration_ms))
                .expect("ts overflow");
            self.shared
                .yield_and_wait(SessionYield::WaitStep {
                    step_id,
                    start_at: active.start_at,
                    deets: WaitYield::Timer {
                        wait_id: step_id,
                        fire_at,
                    },
                    meta: active.meta,
                })
                .await?;
            self.shared.active_step.lock().expect(ERROR_MUTEX).take();
            *self.shared.cur_step.lock().expect(ERROR_MUTEX) = step_id + 1;
            Ok(())
        })
    }

    fn recv_message(&self, step_id: StepId) -> LocalBoxFuture<'_, Result<String, BackendError>> {
        Box::pin(async move {
            let active = self
                .shared
                .active_step
                .lock()
                .expect(ERROR_MUTEX)
                .as_ref()
                .cloned()
                .ok_or_else(|| BackendError::Protocol("step not active".into()))?;
            if active.id != step_id {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            let resumed = self
                .shared
                .yield_and_wait(SessionYield::WaitStep {
                    step_id,
                    start_at: active.start_at,
                    deets: WaitYield::Message { wait_id: step_id },
                    meta: active.meta,
                })
                .await?;
            let value = match resumed {
                SessionResume::Deliver(value) => value,
                SessionResume::Continue => self
                    .shared
                    .replay
                    .lock()
                    .expect(ERROR_MUTEX)
                    .get(step_id as usize)
                    .and_then(|step| step.value_json.clone())
                    .ok_or_else(|| BackendError::Protocol("message wait has no value".into()))?,
            };
            self.shared.active_step.lock().expect(ERROR_MUTEX).take();
            *self.shared.cur_step.lock().expect(ERROR_MUTEX) = step_id + 1;
            Ok(value.to_string())
        })
    }
}

struct SpawnCommand {
    runner: NativeWflowRunner,
    args: types::RunArgs,
    shared: Arc<Shared>,
}

struct LocalExecutor {
    tx: async_channel::Sender<SpawnCommand>,
    shutdown: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LocalExecutor {
    fn new() -> Self {
        let (tx, rx) = async_channel::unbounded();
        let shutdown = CancellationToken::new();
        let thread_shutdown = shutdown.clone();
        let thread = std::thread::Builder::new()
            .name("wflow-native-localset".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create native workflow runtime");
                tokio::task::LocalSet::new().block_on(&runtime, async move {
                    loop {
                        let command = tokio::select! {
                            biased;
                            _ = thread_shutdown.cancelled() => break,
                            command = rx.recv() => match command { Ok(command) => command, Err(_) => break },
                        };
                        tokio::task::spawn_local(run_session(command));
                    }
                });
            })
            .expect("failed to spawn native workflow executor");
        Self {
            tx,
            shutdown,
            thread: Some(thread),
        }
    }
}

async fn run_session(command: SpawnCommand) {
    let SpawnCommand {
        runner,
        args,
        shared,
    } = command;
    let backend: Rc<dyn WflowBackend> = Rc::new(NativeBackend {
        shared: Arc::clone(&shared),
    });
    let outcome = shared
        .cancel
        .run_until_cancelled(std::panic::AssertUnwindSafe((runner)(args, backend)).catch_unwind())
        .await;
    shared.finished.store(true, Ordering::Release);
    let Some(outcome) = outcome else { return };
    // A recorded fork outranks whatever the guest returned for it: the guest
    // may have turned the divergence into a terminal error, or swallowed it and
    // returned a value or panicked, and the durable class must not depend on
    // which. Reading the cursor after the run is complete is what makes a short
    // run visible to the completion guard.
    let yielded = match shared.take_divergence() {
        Some(diverged) => SessionYield::ReplayDiverged(diverged),
        None => match outcome {
            Ok(result) => SessionYield::Complete {
                steps: *shared.cur_step.lock().expect(ERROR_MUTEX),
                result,
            },
            Err(payload) => {
                let message = payload.downcast_ref::<&str>().map_or_else(
                    || "unknown panic payload".to_owned(),
                    |message| (*message).to_owned(),
                );
                SessionYield::Panicked(message)
            }
        },
    };
    if shared.yield_tx.send(yielded).await.is_err() && !shared.cancel.is_cancelled() {
        panic!("{}", ERROR_CALLER);
    }
}

impl Drop for LocalExecutor {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.tx.close();
        if let Some(thread) = self.thread.take() {
            thread.join().expect("native workflow executor panicked");
        }
    }
}

/// A canonical authority-runtime session for one native workflow invocation.
pub struct NativeSession {
    shared: Arc<Shared>,
    resume_tx: async_channel::Sender<SessionResume>,
    yield_rx: async_channel::Receiver<SessionYield>,
}

impl NativeSession {
    pub async fn next_yield(&self) -> Option<SessionYield> {
        self.yield_rx.recv().await.ok()
    }

    #[must_use]
    pub fn resume(&self) -> bool {
        self.resume_tx.try_send(SessionResume::Continue).is_ok()
    }

    #[must_use]
    pub fn deliver(&self, value_json: Arc<str>) -> bool {
        self.resume_tx
            .try_send(SessionResume::Deliver(value_json))
            .is_ok()
    }
}

impl Drop for NativeSession {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
        self.resume_tx.close();
    }
}

/// Registry and local executor used by `LocalNativeHost`.
pub struct NativeHostDriver {
    workflows: RwLock<HashMap<Arc<str>, NativeWflowRunner>>,
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
        self.workflows
            .write()
            .expect(ERROR_MUTEX)
            .insert(key.into(), Arc::new(runner));
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
        let mut workflows = self.workflows.write().expect(ERROR_MUTEX);
        for key in keys {
            workflows.insert(key.into(), Arc::clone(&runner));
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
                        let parsed = serde_json::from_str(args_json).map_err(|err| {
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

    /// Authority jobs carry replay state in the authority journal, not in the host.
    pub fn forget_job(&self, _job_id: &str) {}

    pub fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Result<NativeSession, SpawnError> {
        let runner = self
            .workflows
            .read()
            .expect(ERROR_MUTEX)
            .get(wflow_key)
            .cloned()
            .ok_or(SpawnError::WflowNotFound)?;
        let (yield_tx, yield_rx) = async_channel::unbounded();
        let (resume_tx, resume_rx) = async_channel::unbounded();
        let shared = Arc::new(Shared {
            replay: Mutex::new(replay),
            cur_step: Mutex::new(0),
            active_step: Mutex::new(None),
            diverged: Mutex::new(None),
            yield_tx,
            resume_rx,
            cancel: CancellationToken::new(),
            finished: AtomicBool::new(false),
        });
        self.executor
            .tx
            .try_send(SpawnCommand {
                runner,
                args: types::RunArgs {
                    ctx: types::JobCtx {
                        job_id: job_id.to_string(),
                    },
                    wflow_key: wflow_key.to_owned(),
                    args_json,
                },
                shared: Arc::clone(&shared),
            })
            .map_err(|_| SpawnError::ExecutorStopped)?;
        Ok(NativeSession {
            shared,
            resume_tx,
            yield_rx,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    WflowNotFound,
    ExecutorStopped,
}
