#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}


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
            "townframe:daybook/drawer": generate,
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
            "pseudo-labeler" => |cx, args: daybook_types::gen::wit::doc::DocAddedEvent| pseudo_labeler(cx, args),
        })
    }
}

fn pseudo_labeler(cx: WflowCtx, args: daybook_types::gen::wit::doc::DocAddedEvent) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::drawer;

    // Call the daybook plugin to get the document at the specified heads
    let doc = cx.effect(|| {
        let doc_id = args.id.clone();
        let heads = args.heads.clone();

        let json = match drawer::get_doc_at_heads(&doc_id, &heads) {
            Ok(Some(json)) => json,
            Ok(None) => {
                return Err(JobErrorX::Terminal(ferr!("document not found: {doc_id}")));
            }
            Err(err) => {
                return Err(JobErrorX::Terminal(ferr!(
                    "error getting document: {err:?}"
                )));
            }
        };
        let doc: daybook_types::Doc = serde_json::from_str(&json)
            .wrap_err("error parsing json doc")
            .map_err(JobErrorX::Terminal)?;

        Ok(Json(doc))
    })?;

    // Extract text content for LLM
    let content_text = match &doc.content {
        daybook_types::DocContent::Text(text) => text.clone(),
        daybook_types::DocContent::Blob(_) => "Binary content".to_string(),
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

    // Find or create the pseudo label tag
    let mut updated_tags = doc.props.clone();
    let pseudo_label_index = updated_tags
        .iter()
        .position(|tag| matches!(tag, daybook_types::DocProp::PseudoLabel(_)));

    let new_labels = vec![llm_response.clone()];

    match pseudo_label_index {
        Some(index) => {
            // Replace existing pseudo label tag at the found index
            updated_tags[index] = daybook_types::DocProp::PseudoLabel(new_labels);
        }
        None => {
            // Add new pseudo label tag
            updated_tags.push(daybook_types::DocProp::PseudoLabel(new_labels));
        }
    }

    // Update the doc with the new tags at the original heads
    cx.effect(|| {
        let doc_id = args.id.clone();
        let heads = args.heads.clone();

        // Create a patch with just the props field
        let patch = serde_json::json!({
            "props": updated_tags
        });
        let patch_str = serde_json::to_string(&patch)
            .wrap_err("error serializing patch")
            .map_err(JobErrorX::Terminal)?;

        drawer::update_doc_at_heads(&doc_id, &heads, &patch_str)
            .map_err(|err| JobErrorX::Terminal(ferr!("error updating document: {err:?}")))?;

        Ok(Json(()))
    })?;

    Ok(())
}
