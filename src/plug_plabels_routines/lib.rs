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

use crate::wit::townframe::sqlite::types as sqlite_types;
sqlite_rs::impl_wit_sqlite_backend!(
    crate::wit::townframe::sqlite::sqlite_connection::Connection,
    sqlite_types,
);
sqlite_rs::impl_wit_sqlite_backend!(
    crate::wit::townframe::sqlite::sqlite_connection::Transaction,
    sqlite_types,
);

mod wflows;

mod wasm_runtime {
    use crate::wit;
    use crate::wit::exports::townframe::wflow::bundle::JobResult;

    wit::export!(Component with_types_in wit);

    struct Component;

    use sqlite_rs::ResultSet;
    use sqlite_rs::Row;
    use sqlite_rs::SqlValue;

    pub(crate) fn row_text(_set: &ResultSet, row: &Row, name: &str) -> Option<String> {
        row.get(name).ok()
    }

    pub(crate) fn row_i64(_set: &ResultSet, row: &Row, name: &str) -> Option<i64> {
        row.get(name).ok()
    }

    pub(crate) fn row_blob(_set: &ResultSet, row: &Row, name: &str) -> Option<Vec<u8>> {
        row.get(name).ok()
    }

    pub(crate) fn row_value<'a>(
        set: &'a ResultSet,
        row: &'a Row,
        name: &str,
    ) -> Option<&'a SqlValue> {
        let idx = set.columns.iter().position(|column| column == name)?;
        row.value_at(idx)
    }

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        async fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            use crate::wflows::*;
            wflow_sdk::route_wflows!(args, {
                "label-note" => |cx, _args: serde_json::Value| async move {
                    label_note::run(cx).await
                },
                "label-image" => |cx, _args: serde_json::Value| async move {
                    label_image::run(cx).await
                },
                "learn-image-label-candidates" => |cx, _args: serde_json::Value| async move {
                    learn_image_label_candidates::run(cx).await
                },
                "learn-note-label-candidates" => |cx, _args: serde_json::Value| async move {
                    learn_note_label_candidates::run(cx).await
                },
            })
            .await
        }
    }
}
