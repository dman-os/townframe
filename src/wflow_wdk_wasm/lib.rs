mod interlude {
    pub(crate) use std::rc::Rc;
    pub(crate) use utils_rs::prelude::*;
}

use std::future::Future;
use std::pin::Pin;

use crate::interlude::*;

pub use wflow_wdk;
pub use wflow_wdk::ActiveStepState;
pub use wflow_wdk::BackendError;
pub use wflow_wdk::CompletedStepState;
pub use wflow_wdk::JobErrorX;
pub use wflow_wdk::Json;
pub use wflow_wdk::LocalBoxFuture;
pub use wflow_wdk::RecvCodec;
pub use wflow_wdk::StepId;
pub use wflow_wdk::StepKind;
pub use wflow_wdk::StepMeta;
pub use wflow_wdk::StepState;
pub use wflow_wdk::WflowBackend;
pub use wflow_wdk::WflowCtx;
pub use wflow_wdk::types::CleanupCause;

pub mod wit {
    wit_bindgen::generate!({
        world: "wdk",
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:keyvalue/store@0.2.0-draft": wasm_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": wasm_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": wasm_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": wasm_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": wasm_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": wasm_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": wasm_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": wasm_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": wasm_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": wasm_utils_rs::wit::utils,

            "townframe:wflow/types": generate,
            "townframe:wflow/host": generate,
        }
    });
}

pub use crate::wit::townframe::wflow::host;
pub use crate::wit::townframe::wflow::types;

#[derive(Debug, Clone)]
pub struct WasmWflowBackend {
    job_id: String,
}

impl WasmWflowBackend {
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
        }
    }
}

fn to_wit_step_kind(kind: StepKind) -> host::StepKind {
    match kind {
        StepKind::Effect => host::StepKind::Effect,
        StepKind::Sleep => host::StepKind::Sleep,
        StepKind::Recv => host::StepKind::Recv,
        StepKind::Defer => host::StepKind::Defer,
    }
}

fn from_wit_step_kind(kind: host::StepKind) -> StepKind {
    match kind {
        host::StepKind::Effect => StepKind::Effect,
        host::StepKind::Sleep => StepKind::Sleep,
        host::StepKind::Recv => StepKind::Recv,
        host::StepKind::Defer => StepKind::Defer,
    }
}

fn to_wit_step_meta(meta: StepMeta) -> host::StepMeta {
    host::StepMeta {
        kind: to_wit_step_kind(meta.kind),
        name: meta.name,
    }
}

fn from_wit_step_meta(meta: host::StepMeta) -> StepMeta {
    StepMeta {
        kind: from_wit_step_kind(meta.kind),
        name: meta.name,
    }
}

fn from_wit_cleanup_cause(cause: host::CleanupCause) -> CleanupCause {
    match cause {
        host::CleanupCause::Cancelled(reason) => CleanupCause::Cancelled { reason },
        host::CleanupCause::Failed(error) => CleanupCause::Failed { error },
        host::CleanupCause::RetriesExhausted(host::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        }) => CleanupCause::RetriesExhausted {
            retries,
            max_retries,
            last_error,
        },
    }
}

fn from_wit_host_error(err: host::HostError) -> BackendError {
    match err {
        host::HostError::Interrupted(cause) => {
            BackendError::Interrupted(from_wit_cleanup_cause(cause))
        }
        host::HostError::NondeterministicReplay(err) => BackendError::NondeterministicReplay {
            step_id: err.step_id,
            expected: from_wit_step_meta(err.expected),
            found: from_wit_step_meta(err.found),
        },
        host::HostError::Protocol(msg) => BackendError::Protocol(msg),
    }
}

fn from_wit_step_state(state: host::StepState) -> StepState {
    match state {
        host::StepState::Active(active) => StepState::Active(ActiveStepState { id: active.id }),
        host::StepState::Completed(completed) => StepState::Completed(CompletedStepState {
            id: completed.id,
            value_json: completed.value_json,
        }),
    }
}

impl WflowBackend for WasmWflowBackend {
    fn next_step(&self, meta: StepMeta) -> LocalBoxFuture<'_, Result<StepState, BackendError>> {
        Box::pin(async move {
            let state = host::next_step(self.job_id.clone(), to_wit_step_meta(meta))
                .await
                .map_err(from_wit_host_error)?;
            Ok(from_wit_step_state(state))
        })
    }

    fn persist_step<'a>(
        &'a self,
        step_id: StepId,
        value_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), BackendError>> {
        Box::pin(async move {
            host::persist_step(self.job_id.clone(), step_id, value_json.to_owned())
                .await
                .map_err(from_wit_host_error)
        })
    }

    fn sleep(
        &self,
        step_id: StepId,
        duration_ms: u64,
    ) -> LocalBoxFuture<'_, Result<(), BackendError>> {
        Box::pin(async move {
            host::sleep(self.job_id.clone(), step_id, duration_ms)
                .await
                .map_err(from_wit_host_error)
        })
    }

    fn recv_message(&self, step_id: StepId) -> LocalBoxFuture<'_, Result<String, BackendError>> {
        Box::pin(async move {
            host::recv_message(self.job_id.clone(), step_id)
                .await
                .map_err(from_wit_host_error)
        })
    }

    fn begin_cleanup<'a>(
        &'a self,
        error_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<CleanupCause, BackendError>> {
        Box::pin(async move {
            host::begin_cleanup(self.job_id.clone(), error_json.to_owned())
                .await
                .map(from_wit_cleanup_cause)
                .map_err(from_wit_host_error)
        })
    }
}

#[doc(hidden)]
pub fn from_wit_run_args(args: types::RunArgs) -> wflow_wdk::types::RunArgs {
    wflow_wdk::types::RunArgs {
        ctx: wflow_wdk::types::JobCtx {
            job_id: args.ctx.job_id,
        },
        wflow_key: args.wflow_key,
        args_json: args.args_json,
    }
}

#[doc(hidden)]
pub fn to_wit_job_error(err: wflow_wdk::types::JobError) -> types::JobError {
    match err {
        wflow_wdk::types::JobError::Terminal(json) => types::JobError::Terminal(json),
        wflow_wdk::types::JobError::Transient(err) => {
            types::JobError::Transient(types::TransientJobError {
                error_json: err.error_json,
                retry_policy: err.retry_policy.map(|policy| match policy {
                    wflow_wdk::types::RetryPolicy::Immediate => types::RetryPolicy::Immediate,
                    wflow_wdk::types::RetryPolicy::Fixed { delay_ms } => {
                        types::RetryPolicy::Fixed(types::FixedRetryPolicy { delay_ms })
                    }
                    wflow_wdk::types::RetryPolicy::CappedExponential { base_ms, cap_ms } => {
                        types::RetryPolicy::CappedExponential(types::CappedExponentialRetryPolicy {
                            base_ms,
                            cap_ms,
                        })
                    }
                }),
            })
        }
    }
}

pub fn job_error_from_x(err: JobErrorX) -> types::JobError {
    to_wit_job_error(wflow_wdk::job_error_from_x(err))
}

pub async fn run_wflow<F, R>(args: types::RunArgs, handler: F) -> types::JobResult
where
    F: for<'a> FnOnce(
        &'a mut WflowCtx,
        &'a str,
        &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<R, JobErrorX>> + 'a>>,
    R: Serialize,
{
    let wdk_args = from_wit_run_args(args);
    let backend: Rc<dyn WflowBackend> = Rc::new(WasmWflowBackend::new(wdk_args.ctx.job_id.clone()));
    wflow_wdk::run_wflow(wdk_args, backend, handler)
        .await
        .map_err(to_wit_job_error)
}

#[macro_export]
macro_rules! route_wflows {
    ($args:expr, { $($key:literal => |$cx_ident:ident, $args_var:ident: $args_ty:ty| $handler:expr),* $(,)? }) => {
        {
            let __wit_args = $args;
            let __wdk_args = $crate::from_wit_run_args(__wit_args);
            let __backend: ::std::rc::Rc<dyn $crate::WflowBackend> =
                ::std::rc::Rc::new($crate::WasmWflowBackend::new(__wdk_args.ctx.job_id.clone()));
            async move {
                $crate::wflow_wdk::route_wflows!(__wdk_args, __backend, {
                    $($key => |$cx_ident, $args_var: $args_ty| $handler),*
                })
                .await
                .map_err($crate::to_wit_job_error)
            }
        }
    };
}
