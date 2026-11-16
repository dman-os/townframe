mod interlude {
    pub(crate) use serde::de::DeserializeOwned;
    pub(crate) use std::cell::Cell;
    pub(crate) use std::rc::Rc;
    pub(crate) use utils_rs::prelude::*;
}

#[cfg(test)]
mod tests;

pub mod session;

use std::future::Future;
use std::pin::Pin;

use crate::interlude::*;

pub type StepId = u64;

/// The kind of host boundary that produced or was entered at a workflow step.
/// Replay checks this so an invocation that reaches a committed index with
/// another boundary kind is rejected as nondeterministic instead of reading a
/// foreign value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Effect,
    Sleep,
    Recv,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepMeta {
    pub kind: StepKind,
    pub name: Option<String>,
}

impl StepMeta {
    pub fn new(kind: StepKind, name: Option<&str>) -> Self {
        Self {
            kind,
            name: name.map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveStepState {
    pub id: StepId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedStepState {
    pub id: StepId,
    pub value_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepState {
    Completed(CompletedStepState),
    Active(ActiveStepState),
}

pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub mod types {
    use crate::interlude::*;

    /// How a failed workflow activation is rescheduled by the authority.
    ///
    /// The delay is a pure function of the retry attempt count so every host
    /// computes the same schedule; the scheduler owns the clock and optional
    /// jitter on top of it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum RetryPolicy {
        /// Re-run as soon as a worker is available; no deadline is armed.
        Immediate,
        /// Re-run after a constant delay.
        Fixed { delay_ms: u64 },
        /// Re-run after `base_ms` doubled per attempt, never exceeding `cap_ms`.
        CappedExponential { base_ms: u64, cap_ms: u64 },
    }

    impl RetryPolicy {
        /// The delay before the retry following `attempts` failures. The exponent
        /// is clamped so absurd attempt counts saturate instead of overflowing.
        pub fn delay_ms(&self, attempts: u64) -> u64 {
            match self {
                Self::Immediate => 0,
                Self::Fixed { delay_ms } => *delay_ms,
                Self::CappedExponential { base_ms, cap_ms } => {
                    let exponent = attempts.min(63);
                    (*cap_ms).min((*base_ms).saturating_mul(1u64 << exponent))
                }
            }
        }
    }

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
    /// nondeterministic replay at step {step_id}: expected {expected:?}, found {found:?}
    NondeterministicReplay {
        step_id: StepId,
        expected: StepMeta,
        found: StepMeta,
    },
    /// workflow backend protocol error: {0}
    Protocol(String),
}

/// The host side of one workflow invocation. Each boundary either replays a
/// completed value or yields the active step to be persisted or parked.
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
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum JobErrorX {
    /// terminal error {0:?}
    Terminal(eyre::Report),
    /// transient error {0:?}
    Transient(eyre::Report),
}

/// The guest-visible projection of a backend error.
///
/// A divergence is folded into `Terminal` for the guest, whose error taxonomy
/// has no engine class, and the adapter keeps the typed divergence beside the
/// run (`SessionYield::ReplayDiverged`) so the durable settlement is the
/// engine's `ReplayDiverged` fault rather than a generic terminal string. A
/// guest that swallows this terminal error cannot turn a fork into a success:
/// its own step cursor still lags the durable log, which the completion guard
/// rejects.
impl From<BackendError> for JobErrorX {
    fn from(err: BackendError) -> Self {
        Self::Terminal(eyre::Report::new(err))
    }
}

#[derive(Clone)]
pub struct WflowCtx {
    pub job: types::JobCtx,
    in_step: Rc<Cell<bool>>,
    backend: Rc<dyn WflowBackend>,
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
            StepState::Active(active_step) => {
                let Json(result) = func().await?;
                let json = serde_json::to_string(&result).map_err(|err| {
                    JobErrorX::Terminal(ferr!(
                        "error serializing result as json for '{type_name}': {err:?}",
                        type_name = std::any::type_name::<O>()
                    ))
                })?;
                self.backend.persist_step(active_step.id, &json).await?;
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
            StepState::Completed(_) => Ok(()),
            StepState::Active(active_step) => {
                let duration_ms: u64 = duration.as_millis().try_into().map_err(|_| {
                    JobErrorX::Terminal(ferr!("duration too large for host sleep API"))
                })?;
                self.backend.sleep(active_step.id, duration_ms).await?;
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
            StepState::Active(active_step) => self.backend.recv_message(active_step.id).await?,
        };
        O::decode(&value_json)
    }
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
    }
}

/// Runs one workflow invocation directly to a success, terminal failure, or
/// transient failure outcome.
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

    match handler(&mut cx, &args.wflow_key, args_json).await {
        Ok(value) => serde_json::to_string(&value).map_err(|err| {
            types::JobError::Terminal(
                serde_json::to_string(&json!({
                    "msg": format!(
                        "error serializing result to json for {key}: {err}",
                        key = args.wflow_key
                    )
                }))
                .expect(ERROR_JSON),
            )
        }),
        Err(err) => Err(job_error_from_x(err)),
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
