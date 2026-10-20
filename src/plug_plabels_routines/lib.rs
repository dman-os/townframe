#![recursion_limit = "256"]

mod interlude {
    pub use utils_rs::prelude::ERROR_JSON;
    pub use utils_rs::prelude::Url;
    pub use utils_rs::prelude::error;
    pub use utils_rs::prelude::eyre;
    pub use utils_rs::prelude::ferr;
    pub use utils_rs::prelude::warn;
}

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "bundle",
        // generate_all,
        // async: true,
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
            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,

            "townframe:mltools/ocr": generate,
            "townframe:mltools/embed": generate,
            "townframe:sqlite/types": generate,

            "townframe:daybook-types/doc": generate,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:sqlite/sqlite-connection": generate,
            "townframe:daybook/mltools-ocr": generate,
            "townframe:daybook/mltools-embed": generate,
            "townframe:daybook/mltools-image-tools": generate,
            "townframe:daybook/mltools-llm-chat": generate,
        }
    });
}

mod wflows;

mod wasm_runtime {
    use crate::wit;
    use crate::wit::exports::townframe::wflow::bundle::JobResult;

    wit::export!(Component with_types_in wit);

    struct Component;

    pub(crate) fn row_text(
        set: &crate::wit::townframe::sqlite::types::ResultSet,
        row: &[crate::wit::townframe::sqlite::types::SqlValue],
        name: &str,
    ) -> Option<String> {
        row_value(set, row, name).and_then(|value| match value {
            crate::wit::townframe::sqlite::types::SqlValue::Text(value) => Some(value.clone()),
            _ => None,
        })
    }

    pub(crate) fn row_i64(
        set: &crate::wit::townframe::sqlite::types::ResultSet,
        row: &[crate::wit::townframe::sqlite::types::SqlValue],
        name: &str,
    ) -> Option<i64> {
        row_value(set, row, name).and_then(|value| match value {
            crate::wit::townframe::sqlite::types::SqlValue::Integer(value) => Some(*value),
            _ => None,
        })
    }

    pub(crate) fn row_blob(
        set: &crate::wit::townframe::sqlite::types::ResultSet,
        row: &[crate::wit::townframe::sqlite::types::SqlValue],
        name: &str,
    ) -> Option<Vec<u8>> {
        row_value(set, row, name).and_then(|value| match value {
            crate::wit::townframe::sqlite::types::SqlValue::Blob(value) => Some(value.clone()),
            _ => None,
        })
    }

    pub(crate) fn row_value<'a>(
        set: &'a crate::wit::townframe::sqlite::types::ResultSet,
        row: &'a [crate::wit::townframe::sqlite::types::SqlValue],
        name: &str,
    ) -> Option<&'a crate::wit::townframe::sqlite::types::SqlValue> {
        let idx = set.columns.iter().position(|column| column == name)?;
        row.get(idx)
    }

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            use crate::wflows::*;
            wflow_sdk::route_wflows!(args, {
                "label-note" => |cx, _args: serde_json::Value| {
                    label_note::run(cx)
                },
                "label-image" => |cx, _args: serde_json::Value| {
                    label_image::run(cx)
                },
                "learn-image-label-candidates" => |cx, _args: serde_json::Value| {
                    learn_image_label_candidates::run(cx)
                },
                "learn-note-label-candidates" => |cx, _args: serde_json::Value| {
                    learn_note_label_candidates::run(cx)
                },
            })
        }
    }
}
