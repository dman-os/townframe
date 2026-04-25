mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod hledger;
pub mod types;

#[cfg(target_arch = "wasm32")]
mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "bundle",
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

            "townframe:sql/types": generate,

            "townframe:daybook-types/doc": generate,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:daybook/sqlite-connection": generate,
        }
    });
}

#[cfg(target_arch = "wasm32")]
mod wasm_runtime {
    use crate::interlude::*;
    use crate::wit;
    use crate::wit::exports::townframe::wflow::bundle::JobResult;

    wit::export!(Component with_types_in wit);

    struct Component;

    pub(crate) fn tuple_list_get<'a, T>(pairs: &'a [(String, T)], key: &str) -> Option<&'a T> {
        pairs
            .iter()
            .find(|(entry_key, _)| entry_key == key)
            .map(|(_, entry_value)| entry_value)
    }

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            use crate::wflows::*;
            wflow_sdk::route_wflows!(args, {
                "parse-hledger" => |cx, _args: serde_json::Value| {
                    parse_hledger::run(cx)
                },
            })
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) use wasm_runtime::tuple_list_get;

#[cfg(target_arch = "wasm32")]
pub mod wflows {
    pub mod parse_hledger;
}

use daybook_types::doc::{Note, WellKnownFacetTag};
use daybook_types::manifest::{
    CompareOp, DocPredicateClause, FacetDependencyManifest, FacetManifest, FacetReferenceKind,
    FacetReferenceManifest, PlugDependencyManifest, PlugManifest, ProcessorDeets,
    ProcessorManifest, RoutineFacetAccess, RoutineImpl, RoutineManifest, RoutineManifestDeets,
};
use std::sync::Arc;

pub fn plug_manifest() -> PlugManifest {
    use crate::types::{Account, Claim, DayledgerFacetTag, LedgerMeta, Txn};

    let note_tag: daybook_types::manifest::FacetTag = WellKnownFacetTag::Note.into();
    let claim_tag: daybook_types::manifest::FacetTag = DayledgerFacetTag::Claim.as_str().into();

    PlugManifest {
        namespace: "daybook".into(),
        name: "dayledger".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Day Ledger".into(),
        desc: "Personal accounting facets and ledger data model".into(),
        facets: vec![
            FacetManifest {
                key_tag: claim_tag.clone(),
                value_schema: schemars::schema_for!(Claim),
                display_config: Default::default(),
                references: vec![FacetReferenceManifest {
                    reference_kind: FacetReferenceKind::UrlFacet,
                    json_path: "$.srcRef.ref".into(),
                    at_commit_json_path: Some("$.srcRef.heads".into()),
                }],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Txn.as_str().into(),
                value_schema: schemars::schema_for!(Txn),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Account.as_str().into(),
                value_schema: schemars::schema_for!(Account),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::LedgerMeta.as_str().into(),
                value_schema: schemars::schema_for!(LedgerMeta),
                display_config: Default::default(),
                references: vec![],
            },
        ],
        local_states: Default::default(),
        dependencies: [(
            "@daybook/core@v0.0.1".into(),
            PlugDependencyManifest {
                keys: vec![FacetDependencyManifest {
                    key_tag: note_tag.clone(),
                    value_schema: schemars::schema_for!(Note),
                }],
                local_states: vec![],
            }
            .into(),
        )]
        .into(),
        routines: [(
            "parse-hledger".into(),
            Arc::new(RoutineManifest {
                r#impl: RoutineImpl::Wflow {
                    key: "parse-hledger".into(),
                    bundle: "plug_dayledger".into(),
                },
                deets: RoutineManifestDeets::DocFacet {
                    working_facet_tag: claim_tag.clone(),
                    facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: note_tag.clone(),
                            key_id: None,
                            read: true,
                            write: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: claim_tag,
                            key_id: None,
                            read: true,
                            write: true,
                        },
                    ],
                    config_facet_acl: Default::default(),
                },
                command_invoke_acl: Default::default(),
                local_state_acl: Default::default(),
            }),
        )]
        .into(),
        wflow_bundles: [(
            "plug_dayledger".into(),
            Arc::new(daybook_types::manifest::WflowBundleManifest {
                keys: vec!["parse-hledger".into()],
                component_urls: vec!["build://component/plug_dayledger.wasm".parse().unwrap()],
            }),
        )]
        .into(),
        commands: Default::default(),
        inits: Default::default(),
        processors: [(
            "parse-hledger".into(),
            Arc::new(ProcessorManifest {
                desc: "Parse hledger journal notes into dayledger claims".into(),
                deets: ProcessorDeets::DocProcessor {
                    event_predicate: Default::default(),
                    routine_name: "parse-hledger".into(),
                    predicate: DocPredicateClause::And(vec![
                        DocPredicateClause::HasTag(note_tag.clone()),
                        DocPredicateClause::FacetFieldMatch {
                            tag: note_tag,
                            json_path: "$.mime".into(),
                            operator: CompareOp::Eq,
                            value: serde_json::json!("text/x-hledger-journal"),
                        },
                    ]),
                },
            }),
        )]
        .into(),
    }
}
