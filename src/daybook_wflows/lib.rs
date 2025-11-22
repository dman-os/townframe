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
            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,
            "townframe:am-repo/repo": generate,
            "townframe:utils/llm-chat": generate,

            // "wasi:io/poll@0.2.6": generate,
            // "wasi:io/error@0.2.6": generate,
            // "wasi:io/streams@0.2.6": generate,
            // "wasi:http/types@0.2.6": generate,
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
            "doc-created" => |cx, args: crate::gen::doc::DocAddedEvent| doc_created(cx, args),
        })
    }
}

fn doc_created(cx: WflowCtx, args: crate::gen::doc::DocAddedEvent) -> Result<(), JobErrorX> {
    // Call the am-repo plugin to hydrate the document at the root object
    let doc = cx.effect(|| {
        use crate::wit::townframe::am_repo::repo;

        // Convert types to match WIT bindings
        let doc_id = args.id.clone();
        let heads = args.heads.clone();
        let obj_id = repo::ObjId::Root;
        let path: Vec<repo::PathProp> = vec![];

        let json = repo::hydrate_path_at_head(&doc_id, &heads, &obj_id, &path)
            .wrap_err("error hydrating document")
            .map_err(JobErrorX::Terminal)?;
        println!("XXX {json}");
        let doc: crate::gen::doc::Doc = serde_json::from_str(&json)
            .wrap_err("error parsing json doc")
            .map_err(JobErrorX::Terminal)?;

        Ok(Json(doc))
    })?;

    // Call the LLM with the document content
    let _llm_response: String = cx.effect(|| {
        use crate::wit::townframe::utils::llm_chat;

        let message_text = format!("What do you think of {doc:?}");
        let request = llm_chat::Request {
            input: llm_chat::RequestInput::Text(message_text),
        };

        let result = llm_chat::respond(&request);

        match result {
            Ok(response) => Ok(Json(response.text)),
            Err(err) => Err(JobErrorX::Terminal(ferr!("error calling LLM: {err}"))),
        }
    })?;

    Ok(())
}
