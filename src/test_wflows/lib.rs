#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}

mod wit {
    wit_bindgen::generate!({
        world: "bundle",
        path: "wit",

        with: {
            "wasi:keyvalue/store@0.2.0-draft": generate,
            "wasi:keyvalue/atomics@0.2.0-draft": generate,
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
        }
    });
}

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::{
    JobCtx, JobError, JobResult, TransientJobError,
};
use crate::wit::townframe::wflow::host;

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        let cx = WflowCtx { job: args.ctx };
        match &args.wflow_key[..] {
            "fails_once" => fails_once(
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

struct Json<T>(T);

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

#[derive(Debug, Serialize, Deserialize)]
struct FailsOnceArgs {
    key: String,
}

fn fails_once(cx: WflowCtx, args: FailsOnceArgs) -> Result<(), JobErrorX> {
    use crate::wit::wasi::keyvalue::store;

    // Get the current value from keyvalue store
    cx.effect(|| {
        let bucket = store::open("default")
            .map_err(|err| JobErrorX::Terminal(ferr!("error opening bucket: {:?}", err)))?;
        let key = &args.key;
        const VALUE: u64 = 42;
        match bucket.get(key) {
            Err(err) => Err(JobErrorX::Terminal(ferr!(
                "error getting keyvalue: {:?}",
                err
            ))),
            Ok(None) => match bucket.set(key, &VALUE.to_le_bytes()) {
                Ok(()) => Err(ferr!("first run, woohoo!").into()),
                Err(err) => Err(JobErrorX::Terminal(ferr!(
                    "error setting keyvalue: {:?}",
                    err
                ))),
            },
            Ok(Some(bytes)) => {
                // Parse as u64 (little-endian, 8 bytes)
                if bytes.len() != 8 {
                    return Err(ferr!("not u64").into());
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                let number = u64::from_le_bytes(buf);
                if number != VALUE {
                    Err(JobErrorX::Terminal(ferr!("unexpected value: {number}")))
                } else {
                    Ok(Json(()))
                }
            }
        }
    })?;

    Ok(())
}
