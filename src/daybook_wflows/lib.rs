#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}

mod gen;

mod wit {
    wit_bindgen::generate!({
        world: "bundle",

        // generate_all,
        // async: true,
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
            "townframe:am-repo/repo": generate,

            // "wasi:io/poll@0.2.6": generate,
            // "wasi:io/error@0.2.6": generate,
            // "wasi:io/streams@0.2.6": generate,
            // "wasi:http/types@0.2.6": generate,
        }
    });
}

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::{
    JobCtx, JobError, JobResult, TransientJobError,
};
use crate::wit::townframe::am_repo::repo;
use crate::wit::townframe::wflow::host;

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        let cx = WflowCtx { job: args.ctx };
        match &args.wflow_key[..] {
            "doc-created" => doc_created(
                cx,
                serde_json::from_str(&args.args_json).map_err(|err| {
                    JobError::Terminal(
                        serde_json::to_string(&json!({
                            "msg": format!(
                                "error parsing json ({json}) for {key} args: {err}",
                                json = args.args_json,
                                key = args.wflow_key
                            )
                        }))
                        .expect(ERROR_JSON),
                    )
                })?,
            )
            .map_err(|err| match err {
                JobErrorX::Terminal(err) => JobError::Terminal(format!("{err:?}")),
                JobErrorX::Transient(err) => JobError::Transient(TransientJobError {
                    retry_policy: None,
                    error_json: serde_json::to_string(&json!({
                        "msg": format!("{err:?}")
                    }))
                    .expect(ERROR_JSON),
                }),
            })
            .and_then(|res| {
                serde_json::to_string(&res).map_err(|err| {
                    JobError::Terminal(
                        serde_json::to_string(&json!({
                            "msg": format!(
                                "error serializing result ({res:?}) to json for {key} args: {err}",
                                key = args.wflow_key
                            )
                        }))
                        .expect(ERROR_JSON),
                    )
                })
            }),
            key => Err(JobError::Terminal(format!("unrecognized wflow_key: {key}"))),
        }
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
enum JobErrorX {
    /// terminal error {0:?}
    Terminal(eyre::Report),
    /// transient error {0:?}
    Transient(#[from] eyre::Report),
}

struct WflowCtx {
    job: JobCtx,
}

trait EffectFunctionResult<R> {
    fn into_job_result(self) -> JobResult;
}

struct Json<T>(T);

impl<T> EffectFunctionResult<T> for Json<T>
where
    T: Serialize,
{
    fn into_job_result(self) -> JobResult {
        JobResult::Ok(serde_json::to_string(&self.0).expect(ERROR_JSON))
    }
}

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

fn doc_created(cx: WflowCtx, args: crate::gen::doc::DocAddedEvent) -> Result<(), JobErrorX> {
    // Call the am-repo plugin to hydrate the document at the root object
    let json_str: String = cx.effect(|| {
        use crate::wit::townframe::am_repo::repo;

        // Convert types to match WIT bindings
        let doc_id = args.id.clone();
        let heads = args.heads.clone();
        let obj_id = repo::ObjId::Root;
        let path: Vec<repo::PathProp> = vec![];

        let result = repo::hydrate_path_at_head(&doc_id, &heads, &obj_id, &path);

        match result {
            Ok(json) => Ok(Json(json)),
            Err(err) => Err(JobErrorX::Terminal(ferr!(
                "error hydrating document: {:?}",
                err
            ))),
        }
    })?;

    // Print the hydrated JSON document
    println!("Document {} hydrated JSON:\n{}", args.id, json_str);

    Ok(())
}
