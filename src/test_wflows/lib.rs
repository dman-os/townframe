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

            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,
        }
    });
}

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::JobResult;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        wflow_sdk::route_wflows!(args, {
            "fails_once" => |cx, args: FailsOnceArgs| fails_once(cx, args),
            "fails_until_told" => |cx, args: FailsUntilToldArgs| fails_until_told(cx, args),
            "effect_chain" => |cx, args: EffectChainArgs| effect_chain(cx, args),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FailsOnceArgs {
    key: String,
}

fn fails_once(cx: WflowCtx, args: FailsOnceArgs) -> Result<(), JobErrorX> {
    use api_utils_rs::wit::wasi::keyvalue::store;

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

#[derive(Debug, Serialize, Deserialize)]
struct FailsUntilToldArgs {
    key: String,
}

fn fails_until_told(cx: WflowCtx, args: FailsUntilToldArgs) -> Result<(), JobErrorX> {
    use api_utils_rs::wit::wasi::keyvalue::store;

    // Check if we should succeed by reading from keyvalue store
    cx.effect(|| {
        let bucket = store::open("default")
            .map_err(|err| JobErrorX::Terminal(ferr!("error opening bucket: {:?}", err)))?;
        let key = &args.key;
        match bucket.get(key) {
            Err(err) => Err(JobErrorX::Terminal(ferr!(
                "error getting keyvalue: {:?}",
                err
            ))),
            Ok(None) => {
                // Key doesn't exist, fail transiently
                Err(ferr!("waiting for flag to be set").into())
            }
            Ok(Some(bytes)) => {
                // Check if the value is true (1 byte with value 1)
                if bytes.len() == 1 && bytes[0] == 1 {
                    // Flag is set, succeed
                    Ok(Json(()))
                } else {
                    // Flag is not set or wrong value, fail transiently
                    Err(ferr!("flag not set yet").into())
                }
            }
        }
    })?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct EffectChainArgs {
    steps: u64,
}

fn effect_chain(cx: WflowCtx, args: EffectChainArgs) -> Result<(), JobErrorX> {
    for ii in 0..args.steps {
        cx.effect(|| Ok(Json(ii)))?;
    }
    Ok(())
}
