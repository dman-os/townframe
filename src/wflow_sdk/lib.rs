mod interlude {
    pub use api_utils_rs::prelude::*;
}

pub mod wit {
    wit_bindgen::generate!({
        world: "sdk",
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:wflow/types": generate,
            "townframe:wflow/host": generate,
        }
    });
}

// Re-export the types and bundle interfaces for convenience
// The structure is: wit::<package>::<interface>
pub use crate::wit::townframe::wflow::host;
pub use crate::wit::townframe::wflow::types;

use crate::interlude::*;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum JobErrorX {
    /// terminal error {0:?}
    Terminal(eyre::Report),
    /// transient error {0:?}
    Transient(#[from] eyre::Report),
}

#[derive(Clone)]
pub struct WflowCtx {
    pub job: types::JobCtx,
}

pub struct Json<T>(pub T);

impl WflowCtx {
    pub fn effect<F, O>(&self, func: F) -> Result<O, JobErrorX>
    where
        F: FnOnce() -> Result<Json<O>, JobErrorX>,
        O: serde::de::DeserializeOwned + Serialize,
    {
        let state = host::next_step(&self.job.job_id)
            .map_err(|err| ferr!("error getting next op: {err}"))?;
        match state {
            host::StepState::Completed(completed) => {
                let value: O = serde_json::from_slice(&completed.value).map_err(|err| {
                    ferr!(
                        "error parsing replay step value as json for '{type_name}': {err:?}",
                        type_name = std::any::type_name::<O>()
                    )
                })?;
                Ok(value)
            }
            host::StepState::Active(active_op_state) => {
                let Json(result) = func()?;
                let json = serde_json::to_vec(&result).map_err(|err| {
                    ferr!(
                        "error serializing result as json for '{type_name}': {err:?}",
                        type_name = std::any::type_name::<O>()
                    )
                })?;
                host::persist_step(&self.job.job_id, active_op_state.id, &json)
                    .map_err(|err| ferr!("error persisting step {err:?}"))?;
                Ok(result)
            }
        }
    }
}

/// Helper function to convert JobErrorX to JobError
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

/// Helper function to handle workflow routing with JSON parsing/serialization
/// 
/// This function takes a `RunArgs` and a handler function that routes `wflow_key` to
/// handler functions. It handles:
/// - JSON parsing of args
/// - JSON serialization of results
/// - Error conversion from `JobErrorX` to `JobError`
/// 
/// # Example
/// 
/// ```ignore
/// impl wit::exports::townframe::wflow::bundle::Guest for Component {
///     fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
///         wflow_sdk::run_wflow(args, |cx, key, args_json| {
///             match key {
///                 "my-workflow" => {
///                     let args: MyArgs = serde_json::from_str(args_json)?;
///                     my_handler(cx, args)
///                 }
///                 _ => Err(JobErrorX::Terminal(ferr!("unknown workflow: {key}")))
///             }
///         })
///     }
/// }
/// ```
pub fn run_wflow<F, R>(
    args: types::RunArgs,
    handler: F,
) -> types::JobResult
where
    F: FnOnce(WflowCtx, &str, &str) -> Result<R, JobErrorX>,
    R: Serialize,
{
    let cx = WflowCtx { job: args.ctx };
    
    // Parse args JSON
    let args_json = &args.args_json;
    
    // Call handler and convert errors
    let result = handler(cx, &args.wflow_key, args_json)
        .map_err(job_error_from_x);
    
    // Serialize result
    match result {
        Ok(value) => {
            serde_json::to_string(&value)
                .map_err(|err| {
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
        Err(err) => types::JobResult::Err(err),
    }
}

/// Macro to simplify workflow routing with automatic JSON parsing
/// 
/// # Example
/// 
/// ```ignore
/// impl wit::exports::townframe::wflow::bundle::Guest for Component {
///     fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
///         wflow_sdk::route_wflows!(args, {
///             "fails_once" => |cx, args: FailsOnceArgs| fails_once(cx, args),
///             "other_workflow" => |cx, args: OtherArgs| other_handler(cx, args),
///         })
///     }
/// }
/// ```
#[macro_export]
macro_rules! route_wflows {
    ($args:expr, { $($key:literal => |$cx_ident:ident, $args_var:ident: $args_ty:ty| $handler:expr),* $(,)? }) => {
        {
            use $crate::{run_wflow, WflowCtx, JobErrorX};
            use serde_json::json;
            use color_eyre::eyre::format_err as ferr;
            
            run_wflow($args, |cx, key, args_json| {
                match key {
                    $(
                        $key => {
                            let $args_var: $args_ty = serde_json::from_str(args_json)
                                .map_err(|err| JobErrorX::Terminal(ferr!(
                                    "error parsing json ({json}) for {key} args: {err}",
                                    json = args_json,
                                    key = key
                                )))?;
                            {
                                let $cx_ident = cx.clone();
                                $handler
                            }
                        }
                    )*
                    _ => Err(JobErrorX::Terminal(ferr!("unrecognized wflow_key: {key}"))),
                }
            })
        }
    };
}
