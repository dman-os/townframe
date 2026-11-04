mod interlude {
    pub(crate) use serde::de::DeserializeOwned;
    pub(crate) use std::cell::Cell;
    pub(crate) use std::cell::RefCell;
    pub(crate) use std::rc::Rc;
    pub(crate) use utils_rs::prelude::*;
}

#[cfg(test)]
mod tests;

pub mod session;

use std::future::Future;
use std::pin::Pin;

use crate::interlude::*;

pub use wflow_core::ActiveStepState;
pub use wflow_core::CompletedStepState;
pub use wflow_core::StepId;
pub use wflow_core::StepKind;
pub use wflow_core::StepMeta;
pub use wflow_core::StepState;

pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub mod types {
    use crate::interlude::*;

    /// The retry schedule, its bounds and the failure classes are the durable
    /// authority's own types: a workflow, its submitter and the partition all
    /// speak one representation, so nothing has to be converted or kept in
    /// step across a boundary.
    pub use wflow_core::partition::CleanupCause;
    pub use wflow_core::partition::JobFailure;
    pub use wflow_core::partition::RetryBudget;
    pub use wflow_core::partition::RetryPolicy;

    pub type Json = String;
    pub type JobId = String;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct JobCtx {
        pub job_id: JobId,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct TransientJobError {
        /// Overrides the submission's policy for this failure, when set.
        pub retry_policy: Option<RetryPolicy>,
        pub error_json: Json,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum JobError {
        Transient(TransientJobError),
        Terminal(Json),
    }

    pub type JobResult = Result<Json, JobError>;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RunArgs {
        pub ctx: JobCtx,
        pub wflow_key: String,
        pub args_json: Json,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, displaydoc::Display)]
pub enum BackendError {
    /// forward work interrupted for cleanup: {0:?}
    Interrupted(types::CleanupCause),
    /// nondeterministic replay at step {step_id}: expected {expected:?}, found {found:?}
    NondeterministicReplay {
        step_id: StepId,
        expected: StepMeta,
        found: StepMeta,
    },
    /// workflow backend protocol error: {0}
    Protocol(String),
}

/// The host side of one workflow invocation. Every forward boundary
/// (`next_step` for an effect/sleep/receive, the wait it parks on) may return
/// [`BackendError::Interrupted`] once the job entered cleanup: the workflow
/// propagates it out of its body and [`run_wflow`] runs the registered defers
/// under the durable cause it carries. Cleanup (`Defer`) steps are never
/// interrupted.
pub trait WflowBackend {
    fn next_step(&self, meta: StepMeta) -> LocalBoxFuture<'_, Result<StepState, BackendError>>;

    fn persist_step<'a>(
        &'a self,
        step_id: StepId,
        value_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), BackendError>>;

    fn sleep(
        &self,
        step_id: StepId,
        duration_ms: u64,
    ) -> LocalBoxFuture<'_, Result<(), BackendError>>;

    fn recv_message(&self, step_id: StepId) -> LocalBoxFuture<'_, Result<String, BackendError>>;

    /// Durably records that forward work ended in the terminal `error_json`
    /// while compensation was registered, before any cleanup step runs. The
    /// cause returned is the durable one: a cancel that landed first wins and
    /// a replay that reaches the same error again gets the recorded cause.
    fn begin_cleanup<'a>(
        &'a self,
        error_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<types::CleanupCause, BackendError>>;
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum JobErrorX {
    /// terminal error {0:?}
    Terminal(eyre::Report),
    /// transient error {0:?}
    Transient(eyre::Report),
    /// forward work interrupted for cleanup: {0:?}
    Interrupted(types::CleanupCause),
}

impl From<BackendError> for JobErrorX {
    fn from(err: BackendError) -> Self {
        match err {
            BackendError::Interrupted(cause) => Self::Interrupted(cause),
            BackendError::NondeterministicReplay { .. } | BackendError::Protocol(_) => {
                Self::Terminal(eyre::Report::new(err))
            }
        }
    }
}

struct RegisteredDefer {
    name: String,
    action: Box<dyn FnOnce() -> LocalBoxFuture<'static, Result<(), JobErrorX>>>,
}

#[derive(Clone)]
pub struct WflowCtx {
    pub job: types::JobCtx,
    in_step: Rc<Cell<bool>>,
    backend: Rc<dyn WflowBackend>,
    defers: Rc<RefCell<Vec<RegisteredDefer>>>,
}

struct StepGuard {
    in_step: Rc<Cell<bool>>,
}

impl Drop for StepGuard {
    fn drop(&mut self) {
        self.in_step.set(false);
    }
}

pub struct Json<T>(pub T);

pub trait RecvCodec: Sized {
    fn decode(value_json: &str) -> Result<Self, JobErrorX>;
}

impl<T> RecvCodec for Json<T>
where
    T: DeserializeOwned,
{
    fn decode(value_json: &str) -> Result<Self, JobErrorX> {
        let value = serde_json::from_str(value_json).map_err(|err| {
            JobErrorX::Terminal(ferr!(
                "error parsing workflow message json for '{type_name}': {err:?}",
                type_name = std::any::type_name::<T>()
            ))
        })?;
        Ok(Json(value))
    }
}

impl WflowCtx {
    pub fn new(job: types::JobCtx, backend: Rc<dyn WflowBackend>) -> Self {
        Self {
            job,
            in_step: Rc::new(Cell::new(false)),
            backend,
            defers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn enter_step(&self, op: &'static str) -> Result<StepGuard, JobErrorX> {
        if self.in_step.replace(true) {
            return Err(JobErrorX::Terminal(ferr!(
                "nested workflow step entry not allowed while executing '{op}'"
            )));
        }
        Ok(StepGuard {
            in_step: Rc::clone(&self.in_step),
        })
    }

    pub async fn effect<F, O, Fut>(&mut self, func: F) -> Result<O, JobErrorX>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Json<O>, JobErrorX>>,
        O: DeserializeOwned + Serialize,
    {
        self.effect_with_meta(StepMeta::new(StepKind::Effect, None), func)
            .await
    }

    pub async fn named_effect<F, O, Fut>(
        &mut self,
        name: &'static str,
        func: F,
    ) -> Result<O, JobErrorX>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Json<O>, JobErrorX>>,
        O: DeserializeOwned + Serialize,
    {
        self.effect_with_meta(StepMeta::new(StepKind::Effect, Some(name)), func)
            .await
    }

    async fn effect_with_meta<F, O, Fut>(&mut self, meta: StepMeta, func: F) -> Result<O, JobErrorX>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Json<O>, JobErrorX>>,
        O: DeserializeOwned + Serialize,
    {
        let _step_guard = self.enter_step("effect")?;
        let state = self.backend.next_step(meta).await?;
        match state {
            StepState::Completed(completed) => {
                let value: O = serde_json::from_str(&completed.value_json).map_err(|err| {
                    JobErrorX::Terminal(ferr!(
                        "error parsing replay step value as json for '{type_name}': {err:?}",
                        type_name = std::any::type_name::<O>()
                    ))
                })?;
                Ok(value)
            }
            StepState::Active(active_op_state) => {
                let Json(result) = func().await?;
                let json = serde_json::to_string(&result).map_err(|err| {
                    JobErrorX::Terminal(ferr!(
                        "error serializing result as json for '{type_name}': {err:?}",
                        type_name = std::any::type_name::<O>()
                    ))
                })?;
                self.backend.persist_step(active_op_state.id, &json).await?;
                Ok(result)
            }
        }
    }

    pub async fn sleep(&mut self, duration: Duration) -> Result<(), JobErrorX> {
        self.sleep_with_meta(StepMeta::new(StepKind::Sleep, None), duration)
            .await
    }

    pub async fn named_sleep(
        &mut self,
        name: &'static str,
        duration: Duration,
    ) -> Result<(), JobErrorX> {
        self.sleep_with_meta(StepMeta::new(StepKind::Sleep, Some(name)), duration)
            .await
    }

    async fn sleep_with_meta(
        &mut self,
        meta: StepMeta,
        duration: Duration,
    ) -> Result<(), JobErrorX> {
        let _step_guard = self.enter_step("sleep")?;
        let state = self.backend.next_step(meta).await?;
        match state {
            StepState::Completed(_completed) => Ok(()),
            StepState::Active(active_op_state) => {
                let duration_ms: u64 = duration.as_millis().try_into().map_err(|_| {
                    JobErrorX::Terminal(ferr!("duration too large for host sleep API"))
                })?;
                self.backend.sleep(active_op_state.id, duration_ms).await?;
                Ok(())
            }
        }
    }

    pub async fn recv<O>(&mut self) -> Result<O, JobErrorX>
    where
        O: RecvCodec,
    {
        self.recv_with_meta(StepMeta::new(StepKind::Recv, None))
            .await
    }

    pub async fn named_recv<O>(&mut self, name: &'static str) -> Result<O, JobErrorX>
    where
        O: RecvCodec,
    {
        self.recv_with_meta(StepMeta::new(StepKind::Recv, Some(name)))
            .await
    }

    async fn recv_with_meta<O>(&mut self, meta: StepMeta) -> Result<O, JobErrorX>
    where
        O: RecvCodec,
    {
        let _step_guard = self.enter_step("recv")?;
        let state = self.backend.next_step(meta).await?;
        let value_json = match state {
            StepState::Completed(completed) => completed.value_json,
            StepState::Active(active_op_state) => {
                self.backend.recv_message(active_op_state.id).await?
            }
        };
        O::decode(&value_json)
    }

    pub fn defer<F, Fut>(&mut self, name: &'static str, func: F) -> Result<(), JobErrorX>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<(), JobErrorX>> + 'static,
    {
        if self.in_step.get() {
            return Err(JobErrorX::Terminal(ferr!(
                "cannot register workflow defer from inside an active step"
            )));
        }
        self.defers.borrow_mut().push(RegisteredDefer {
            name: name.to_owned(),
            action: Box::new(move || Box::pin(func())),
        });
        Ok(())
    }

    pub fn disarm_defers(&mut self) {
        self.defers.borrow_mut().clear();
    }

    /// Runs the registered defers in reverse order, each as a durable `Defer`
    /// step, and settles the job with the outcome `cause` prescribes. A defer
    /// that fails transiently propagates so the host re-runs the job: replay
    /// skips the committed defers and resumes at the outstanding one. A defer
    /// that fails terminally settles the job as [`JobFailure::CleanupFailed`]
    /// with the remaining compensation outstanding, never archived as done.
    async fn finish_cleanup(&mut self, cause: types::CleanupCause) -> types::JobResult {
        let defers = std::mem::take(&mut *self.defers.borrow_mut());
        for registered in defers.into_iter().rev() {
            let _step_guard = self.enter_step("defer").map_err(job_error_from_x)?;
            let meta = StepMeta::new(StepKind::Defer, Some(&registered.name));
            let state = self
                .backend
                .next_step(meta)
                .await
                .map_err(|err| job_error_from_x(err.into()))?;
            let StepState::Active(active_op_state) = state else {
                continue;
            };
            match (registered.action)().await {
                Ok(()) => {}
                Err(JobErrorX::Transient(err)) => {
                    return Err(job_error_from_x(JobErrorX::Transient(err)));
                }
                Err(err) => {
                    return Err(failure_envelope(&types::JobFailure::CleanupFailed {
                        cause,
                        defer: registered.name,
                        error: format!("{err:?}"),
                    }));
                }
            }
            self.backend
                .persist_step(active_op_state.id, "null")
                .await
                .map_err(|err| job_error_from_x(err.into()))?;
        }
        Err(cleanup_settlement(cause))
    }
}

/// The terminal error a cleanup settles with once its defers ran: a cancel
/// and an exhausted forward budget are classified failures, a terminal error
/// is the workflow's own.
fn cleanup_settlement(cause: types::CleanupCause) -> types::JobError {
    match cause {
        types::CleanupCause::Cancelled { reason } => {
            failure_envelope(&types::JobFailure::Cancelled { reason })
        }
        types::CleanupCause::Failed { error } => types::JobError::Terminal(error),
        types::CleanupCause::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        } => failure_envelope(&types::JobFailure::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        }),
    }
}

/// The terminal error whose JSON is the classified `failure`, the one shape
/// hosts settle budget exhaustion and cleanup outcomes with.
fn failure_envelope(failure: &types::JobFailure) -> types::JobError {
    types::JobError::Terminal(serde_json::to_string(failure).expect(ERROR_JSON))
}

pub fn job_error_from_x(err: JobErrorX) -> types::JobError {
    use types::TransientJobError;
    match err {
        JobErrorX::Terminal(err) => types::JobError::Terminal(format!("{err:?}")),
        JobErrorX::Transient(err) => types::JobError::Transient(TransientJobError {
            retry_policy: None,
            error_json: serde_json::to_string(&json!({
                "msg": format!("{err:?}")
            }))
            .expect(ERROR_JSON),
        }),
        // Only reachable when a workflow swallows the interruption and maps it
        // to another error itself; its defers then never ran, which is what the
        // envelope records.
        JobErrorX::Interrupted(cause) => cleanup_settlement(cause),
    }
}

/// Runs one workflow invocation to its terminal outcome. Success disarms the
/// registered defers. A terminal error with defers registered first records
/// the cause durably ([`WflowBackend::begin_cleanup`]) so a crash mid-cleanup
/// resumes compensation instead of re-running forward work, then runs them;
/// without defers it settles directly. An interruption carries the cause the
/// host already recorded and goes straight to the defers.
pub async fn run_wflow<F, R>(
    args: types::RunArgs,
    backend: Rc<dyn WflowBackend>,
    handler: F,
) -> types::JobResult
where
    F: for<'a> FnOnce(
        &'a mut WflowCtx,
        &'a str,
        &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<R, JobErrorX>> + 'a>>,
    R: Serialize,
{
    let mut cx = WflowCtx::new(args.ctx, backend);
    let args_json = &args.args_json;

    let outcome = handler(&mut cx, &args.wflow_key, args_json).await;
    match outcome {
        Ok(value) => {
            cx.disarm_defers();
            serde_json::to_string(&value).map_err(|err| {
                types::JobError::Terminal(
                    serde_json::to_string(&json!({
                        "msg": format!(
                            "error serializing result to json for {key}: {err}",
                            key = args.wflow_key
                        )
                    }))
                    .expect(ERROR_JSON),
                )
            })
        }
        Err(JobErrorX::Terminal(err)) => {
            let error = format!("{err:?}");
            if cx.defers.borrow().is_empty() {
                return Err(types::JobError::Terminal(error));
            }
            let cause = cx
                .backend
                .begin_cleanup(&error)
                .await
                .map_err(|err| job_error_from_x(err.into()))?;
            cx.finish_cleanup(cause).await
        }
        Err(JobErrorX::Interrupted(cause)) => cx.finish_cleanup(cause).await,
        Err(err @ JobErrorX::Transient(_)) => Err(job_error_from_x(err)),
    }
}

#[doc(hidden)]
pub fn parse_wflow_args<T: serde::de::DeserializeOwned>(
    args_json: &str,
    wflow_key: &str,
) -> Result<T, JobErrorX> {
    serde_json::from_str(args_json).map_err(|err| {
        JobErrorX::Terminal(ferr!(
            "error parsing json ({args_json}) for {wflow_key} args: {err}"
        ))
    })
}

#[doc(hidden)]
pub fn unrecognized_wflow_key(wflow_key: &str) -> types::JobResult {
    Err(job_error_from_x(JobErrorX::Terminal(ferr!(
        "unrecognized wflow_key: {wflow_key}"
    ))))
}

#[macro_export]
macro_rules! route_wflows {
    ($args:expr, $backend:expr, { $($key:literal => |$cx_ident:ident, $args_var:ident: $args_ty:ty| $handler:expr),* $(,)? }) => {
        {
            let __args = $args;
            let __backend = $backend;
            async move {
                match __args.wflow_key.as_str() {
                $(
                    $key => {
                        $crate::run_wflow(__args, __backend, |cx, key, args_json| {
                            Box::pin(async move {
                                let $args_var: $args_ty = $crate::parse_wflow_args(args_json, key)?;
                                let $cx_ident = cx;
                                $handler.await
                            })
                        })
                        .await
                    }
                )*
                    other => $crate::unrecognized_wflow_key(other),
                }
            }
        }
    };
}
