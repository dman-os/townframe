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

fn doc_created(cx: WflowCtx, args: crate::gen::doc::DocAddedEvent) -> Result<(), JobErrorX> {
    match wit::townframe::wflow::host::next_step(&cx.job.job_id) {
        Err(err) => return Err(ferr!("error getting next op {err}").into()),
        Ok(state) => match state {
            wit::townframe::wflow::host::StepState::Active(active_op_state) => {
                wit::townframe::wflow::host::persist_step(
                    &cx.job.job_id,
                    active_op_state.id,
                    "hi".as_bytes(),
                )
                .map_err(|err| ferr!("error persisting step {err:?}"))?;
            }
            wit::townframe::wflow::host::StepState::Completed(completed_op_state) => {}
        },
    }
    Ok(())
}
