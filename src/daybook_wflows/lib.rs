#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}

mod wit {
    wit_bindgen::generate!({
        path: "wit",
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

            "townframe:utils/llm-chat": generate,

            "townframe:daybook-types/doc": daybook_types::wit::doc,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/prop-routine": generate,
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
            "pseudo-label" => |cx, _args: serde_json::Value| pseudo_labeler(cx),
            "test-label" => |cx, _args: serde_json::Value| test_labeler(cx),
        })
    }
}

fn pseudo_labeler(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::drawer;
    use crate::wit::townframe::daybook::prop_routine;

    let args = prop_routine::get_args();

    // Find the working prop token (the one with write access matching prop_key)
    let working_prop_token = args
        .rw_prop_tokens
        .iter()
        .find(|(key, _)| key == &args.prop_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working prop key '{}' not found in rw_prop_tokens",
                args.prop_key
            ))
        })?;

    // Get doc using drawer interface
    let doc = drawer::get_doc_at_heads(&args.doc_id, &args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("error getting doc: {err:?}")))?;

    let doc: daybook_types::doc::Doc = doc
        .try_into()
        .map_err(|err| JobErrorX::Terminal(ferr!("failure on doc parsing: {err:?}")))?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::{DocContent, WellKnownProp, WellKnownPropTag};
    let content_text = match doc
        .props
        .get(&WellKnownPropTag::Content.into())
        .map(|val| WellKnownProp::from_json(val.clone(), WellKnownPropTag::Content))
    {
        Some(Ok(WellKnownProp::Content(DocContent::Text(text)))) => text,
        Some(Ok(_)) => unreachable!(),
        Some(Err(err)) => {
            return Err(JobErrorX::Terminal(
                err.wrap_err("unable to parse prop found on doc"),
            ))
        }
        None => {
            return Err(JobErrorX::Terminal(ferr!(
                "no {tag} found on doc",
                tag = WellKnownPropTag::Content.as_str()
            )))
        }
    };

    // Call the LLM to generate a label
    let llm_response: String = cx.effect(|| {
        use crate::wit::townframe::utils::llm_chat;

        let message_text = format!(
            "Based on the following document content, provide a single short label or category (1-3 words). \
            Just return the label, nothing else.\n\nDocument content:\n{}",
            content_text
        );
        let request = llm_chat::Request {
            input: llm_chat::RequestInput::Text(message_text),
        };

        let result = llm_chat::respond(&request);

        match result {
            Ok(response) => {
                // Clean up the response - remove quotes, trim whitespace
                let label = response.text.trim().trim_matches('"').trim_matches('\'').trim().to_string();
                Ok(Json(label))
            }
            Err(err) => Err(JobErrorX::Terminal(ferr!("error calling LLM: {err}"))),
        }
    })?;

    let new_labels = [llm_response.clone()];

    cx.effect(|| {
        let new_prop: daybook_types::doc::DocProp =
            WellKnownProp::PseudoLabel(new_labels.join(",")).into();
        let new_prop = serde_json::to_string(&new_prop).expect(ERROR_JSON);
        working_prop_token
            .update(&new_prop)
            .wrap_err("error updating prop")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}

fn test_labeler(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::prop_routine;
    let args = prop_routine::get_args();

    // Find the working prop token (the one with write access matching prop_key)
    let working_prop_token = args
        .rw_prop_tokens
        .iter()
        .find(|(key, _)| key == &args.prop_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working prop key '{}' not found in rw_prop_tokens",
                args.prop_key
            ))
        })?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::WellKnownProp;

    cx.effect(|| {
        let new_prop: daybook_types::doc::DocProp =
            WellKnownProp::LabelGeneric("test_label".into()).into();
        let new_prop = serde_json::to_string(&new_prop).expect(ERROR_JSON);
        working_prop_token
            .update(&new_prop)
            .wrap_err("error updating prop")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}
