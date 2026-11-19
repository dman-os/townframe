#![recursion_limit = "256"]

mod interlude {
    pub use utils_rs::prelude::ERROR_JSON;
    pub use utils_rs::prelude::HashMap;
    pub use utils_rs::prelude::HashSet;
    pub use utils_rs::prelude::ferr;
}

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "bundle",
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

            "townframe:sqlite/types": generate,

            "townframe:daybook-types/doc": generate,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:sqlite/sqlite-connection": generate,
        }
    });
}

mod wflows;

mod wasm_runtime {
    use crate::wit;
    use crate::wit::exports::townframe::wflow::bundle::JobResult;

    wit::export!(Component with_types_in wit);

    struct Component;

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        async fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            use crate::wflows::*;
            wflow_sdk::route_wflows!(args, {
                "parse-hledger" => |cx, _args: serde_json::Value| async move {
                    parse_hledger::run(cx).await
                },
                // NOTE: route_wflows! matches keys as `:literal` patterns, so the routine key
                // must be inlined here rather than referenced via INIT_NOTE_EDITOR_CONFIG_ROUTINE_KEY.
                "init-note-editor-config" => |cx, _args: serde_json::Value| async move {
                    init_note_editor_config::run(cx).await
                },
            })
            .await
        }
    }

    impl crate::wit::exports::townframe::daybook::stateless_view::Guest for Component {
        async fn render_facet_view(
            args: crate::wit::exports::townframe::daybook::stateless_view::RenderFacetViewArgs,
        ) -> Result<
            crate::wit::exports::townframe::daybook::stateless_view::RenderViewResponse,
            crate::wit::exports::townframe::daybook::stateless_view::RenderViewError,
        > {
            if args.view_key != plug_dayledger_manifest::LEDGER_META_VIEW_KEY {
                return Err(
                    crate::wit::exports::townframe::daybook::stateless_view::RenderViewError::InvalidView(
                        format!("unknown view key '{}'", args.view_key),
                    ),
                );
            }

            let ledger_meta_token = args
                .primary_doc
                .facets
                .iter()
                .find(|token| {
                    token.key() == args.target_facet_key
                        && token
                            .rights()
                            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::READ)
                })
                .ok_or_else(|| {
                    crate::wit::exports::townframe::daybook::stateless_view::RenderViewError::InvalidRequest(
                        format!(
                            "target facet '{}' not found with read rights",
                            args.target_facet_key
                        ),
                    )
                })?;

            let ledger_meta_raw = ledger_meta_token.get().await.map_err(|err| {
                crate::wit::exports::townframe::daybook::stateless_view::RenderViewError::Denied(
                    format!("error reading ledger meta facet: {err:?}"),
                )
            })?;
            let ledger_meta: plug_dayledger_manifest::types::LedgerMeta =
                serde_json::from_str(&ledger_meta_raw).map_err(|err| {
                    crate::wit::exports::townframe::daybook::stateless_view::RenderViewError::InvalidRequest(
                        format!(
                            "target facet '{}' is not valid LedgerMeta json: {err}",
                            args.target_facet_key
                        ),
                    )
                })?;

            Ok(
                crate::wit::exports::townframe::daybook::stateless_view::RenderViewResponse {
                    view_json: plug_dayledger_manifest::ledger_meta_view_json(&ledger_meta),
                    plugin_state_json: None,
                },
            )
        }
    }
}
