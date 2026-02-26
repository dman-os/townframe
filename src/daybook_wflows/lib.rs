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

            "townframe:mltools/ocr": generate,
            "townframe:mltools/embed": generate,
            "townframe:sql/types": generate,

            "townframe:daybook-types/doc": generate,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:daybook/sqlite-connection": generate,
            "townframe:daybook/mltools-ocr": generate,
            "townframe:daybook/mltools-embed": generate,
            "townframe:daybook/mltools-image-tools": generate,
            "townframe:daybook/mltools-llm-chat": generate,
        }
    });
}
mod wflows;

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::JobResult;

wit::export!(Component with_types_in wit);

struct Component;

fn tuple_list_get<'a, T>(pairs: &'a [(String, T)], key: &str) -> Option<&'a T> {
    pairs
        .iter()
        .find(|(entry_key, _)| entry_key == key)
        .map(|(_, entry_value)| entry_value)
}

fn tuple_list_take<T>(pairs: &mut Vec<(String, T)>, key: &str) -> Option<T> {
    let ix = pairs.iter().position(|(entry_key, _)| entry_key == key)?;
    Some(pairs.swap_remove(ix).1)
}

pub(crate) fn row_text(
    row: &crate::wit::townframe::sql::types::ResultRow,
    name: &str,
) -> Option<String> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Text(value) if entry.column_name == name => {
            Some(value.clone())
        }
        _ => None,
    })
}

pub(crate) fn row_i64(
    row: &crate::wit::townframe::sql::types::ResultRow,
    name: &str,
) -> Option<i64> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Integer(value)
            if entry.column_name == name =>
        {
            Some(*value)
        }
        _ => None,
    })
}

pub(crate) fn row_blob(
    row: &crate::wit::townframe::sql::types::ResultRow,
    name: &str,
) -> Option<Vec<u8>> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Blob(value) if entry.column_name == name => {
            Some(value.clone())
        }
        _ => None,
    })
}

pub(crate) fn embedding_bytes_to_f32(bytes: &[u8]) -> Res<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        eyre::bail!(
            "embedding bytes length {} is not divisible by 4",
            bytes.len()
        );
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        use wflows::*;
        wflow_sdk::route_wflows!(args, {
            "pseudo-label" => |cx, _args: serde_json::Value| pseudo_labeler::run(cx),
            "test-label" => |cx, _args: serde_json::Value| test_labeler::run(cx),
            "ocr-image" => |cx, _args: serde_json::Value| ocr_image::run(cx),
            "embed-image" => |cx, _args: serde_json::Value| embed_image::run(cx),
            "embed-text" => |cx, _args: serde_json::Value| embed_text::run(cx),
            "index-embedding" => |cx, _args: serde_json::Value| index_embedding::run(cx),
            "classify-image-label" => |cx, _args: serde_json::Value| classify_image_label::run(cx),
            "learn-image-label-proposals" => |cx, _args: serde_json::Value| {
                learn_image_label_proposals::run(cx)
            },
        })
    }
}
