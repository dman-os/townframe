#![recursion_limit = "256"]

mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod hledger;
pub mod types;

#[cfg(test)]
mod e2e;

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

    impl crate::wit::exports::townframe::daybook::stateless_view::Guest for Component {
        fn render_facet_view(
            args: crate::wit::exports::townframe::daybook::stateless_view::RenderFacetViewArgs,
        ) -> Result<
            crate::wit::exports::townframe::daybook::stateless_view::RenderViewResponse,
            crate::wit::exports::townframe::daybook::stateless_view::RenderViewError,
        > {
            if args.view_key != super::LEDGER_META_VIEW_KEY {
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

            let ledger_meta_raw = ledger_meta_token.get().map_err(|err| {
                crate::wit::exports::townframe::daybook::stateless_view::RenderViewError::Denied(
                    format!("error reading ledger meta facet: {err:?}"),
                )
            })?;
            let ledger_meta: crate::types::LedgerMeta =
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
                    view_json: super::ledger_meta_view_json(&ledger_meta),
                    plugin_state_json: None,
                },
            )
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub mod wflows {
    pub mod parse_hledger;
}

use daybook_types::doc::{Note, WellKnownFacetTag};
use daybook_types::manifest::{
    CompareOp, DocChangePredicate, DocPredicateClause, FacetDependencyManifest, FacetDisplayDeets,
    FacetDisplayHint, FacetManifest, FacetReferenceManifest, FacetViewMode, PlugDependencyManifest,
    PlugManifest, ProcessorDeets, ProcessorEventPredicate, ProcessorManifest, RoutineDocAcl,
    RoutineFacetAccess, RoutineImpl, RoutineManifest, ViewManifest, ViewProviderManifest, ViewRef,
};
#[cfg(any(test, target_arch = "wasm32"))]
use daybook_types::view::{
    CardNodeV1, ListNodeV1, SectionNodeV1, TextNodeV1, ViewNodeId, ViewNodeKindV1, ViewNodeV1,
    ViewSpec, ViewSpecV1,
};
use std::sync::Arc;

pub fn plug_manifest() -> PlugManifest {
    use crate::types::{Account, Claim, DayledgerFacetTag, LedgerMeta, Txn};

    let note_tag: daybook_types::manifest::FacetTag = WellKnownFacetTag::Note.into();
    let claim_tag: daybook_types::manifest::FacetTag = DayledgerFacetTag::Claim.as_str().into();
    let ledger_meta_view_key: daybook_types::manifest::KeyGeneric = LEDGER_META_VIEW_KEY.into();

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
                references: vec![FacetReferenceManifest::UrlObjectMany {
                    json_path: "$.srcRefs[*]".into(),
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
                display_config: FacetDisplayHint {
                    deets: FacetDisplayDeets::CustomView {
                        view: ViewRef {
                            plug_id: None,
                            view_key: ledger_meta_view_key.clone(),
                        },
                        mode: FacetViewMode::Display,
                        priority: 0,
                    },
                    ..Default::default()
                },
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
        views: [(
            ledger_meta_view_key,
            Arc::new(ViewManifest {
                title: "Ledger Meta".into(),
                desc: "Read-only structural overview of a LedgerMeta facet".into(),
                provider: ViewProviderManifest::StatelessWasm {
                    bundle: "plug_dayledger".into(),
                    export: LEDGER_META_VIEW_EXPORT.into(),
                },
            }),
        )]
        .into(),
        routines: [(
            "parse-hledger".into(),
            Arc::new(RoutineManifest {
                r#impl: RoutineImpl::Wflow {
                    key: "parse-hledger".into(),
                    bundle: "plug_dayledger".into(),
                },
                doc_acls: vec![RoutineDocAcl {
                    doc_predicate: DocPredicateClause::And(vec![
                        DocPredicateClause::HasTag(note_tag.clone()),
                        DocPredicateClause::FacetFieldMatch {
                            tag: note_tag.clone(),
                            json_path: "$.mime".into(),
                            operator: CompareOp::Eq,
                            value: serde_json::json!("text/x-hledger-journal"),
                        },
                    ]),
                    facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::Note.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: DayledgerFacetTag::Claim.as_str().into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: true,
                            delete: false,
                        },
                    ],
                }],
                query_acls: vec![],
                config_facet_acl: Default::default(),
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
                    event_predicate: ProcessorEventPredicate {
                        doc_change_predicate: DocChangePredicate::ChangedFacetTags(vec![
                            note_tag.clone()
                        ]),
                        ..Default::default()
                    },
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

const LEDGER_META_VIEW_KEY: &str = "ledger-meta";
const LEDGER_META_VIEW_EXPORT: &str = "render-facet-view";

#[cfg(any(test, target_arch = "wasm32"))]
fn ledger_meta_view_spec(ledger_meta: &crate::types::LedgerMeta) -> ViewSpec {
    ViewSpec::V1(ViewSpecV1 {
        root: ViewNodeV1 {
            id: ViewNodeId::from("root"),
            kind: ViewNodeKindV1::Card(CardNodeV1 {
                title: Some(ledger_meta.title.clone()),
                children: vec![
                    ViewNodeV1 {
                        id: ViewNodeId::from("summary"),
                        kind: ViewNodeKindV1::Section(SectionNodeV1 {
                            title: Some("Summary".into()),
                            children: vec![
                                text_node(
                                    "ledger-id",
                                    format!("Ledger ID: {}", ledger_meta.ledger_id),
                                ),
                                text_node(
                                    "journal-commodity",
                                    format!("Journal commodity: {}", ledger_meta.journal_commodity),
                                ),
                                text_node(
                                    "account-ref-count",
                                    format!("Account refs: {}", ledger_meta.account_refs.len()),
                                ),
                                text_node(
                                    "transaction-ref-count",
                                    format!(
                                        "Transaction refs: {}",
                                        ledger_meta.transaction_refs.len()
                                    ),
                                ),
                            ],
                        }),
                        events: vec![],
                    },
                    ledger_ref_section(
                        "account-refs",
                        "Account refs",
                        &ledger_meta.account_refs,
                        "No account refs",
                    ),
                    ledger_ref_section(
                        "transaction-refs",
                        "Transaction refs",
                        &ledger_meta.transaction_refs,
                        "No transaction refs",
                    ),
                ],
            }),
            events: vec![],
        },
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn ledger_ref_section(
    section_id: &str,
    title: &str,
    refs: &[url::Url],
    empty_text: &str,
) -> ViewNodeV1 {
    let children = if refs.is_empty() {
        vec![text_node(format!("{section_id}-empty"), empty_text)]
    } else {
        vec![ViewNodeV1 {
            id: ViewNodeId::from(format!("{section_id}-list")),
            kind: ViewNodeKindV1::List(ListNodeV1 {
                items: refs
                    .iter()
                    .enumerate()
                    .map(|(index, reference)| {
                        text_node(format!("{section_id}-item-{index}"), reference.to_string())
                    })
                    .collect(),
            }),
            events: vec![],
        }]
    };

    ViewNodeV1 {
        id: ViewNodeId::from(section_id),
        kind: ViewNodeKindV1::Section(SectionNodeV1 {
            title: Some(title.into()),
            children,
        }),
        events: vec![],
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
fn text_node(id: impl Into<String>, text: impl Into<String>) -> ViewNodeV1 {
    ViewNodeV1 {
        id: ViewNodeId::from(id.into()),
        kind: ViewNodeKindV1::Text(TextNodeV1 { text: text.into() }),
        events: vec![],
    }
}

#[cfg(target_arch = "wasm32")]
fn ledger_meta_view_json(ledger_meta: &crate::types::LedgerMeta) -> String {
    serde_json::to_string(&ledger_meta_view_spec(ledger_meta))
        .expect(utils_rs::expect_tags::ERROR_JSON)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ledger_meta() -> crate::types::LedgerMeta {
        crate::types::LedgerMeta {
            ledger_id: "ledger-1".into(),
            title: "Primary ledger".into(),
            journal_commodity: "USD".into(),
            account_refs: vec![
                url::Url::parse("db+facet:///doc/assets/main").unwrap(),
                url::Url::parse("db+facet:///doc/income/main").unwrap(),
            ],
            transaction_refs: vec![url::Url::parse("db+facet:///doc/txn-1/main").unwrap()],
        }
    }

    #[test]
    fn ledger_meta_view_spec_has_expected_structural_shape() {
        let spec = ledger_meta_view_spec(&sample_ledger_meta());
        spec.validate().expect("valid ledger meta view");

        assert_eq!(
            serde_json::to_value(spec).expect("serialize view"),
            serde_json::json!({
                "schemaVersion": "v1",
                "spec": {
                    "root": {
                        "id": "root",
                        "kind": {
                            "card": {
                                "title": "Primary ledger",
                                "children": [
                                    {
                                        "id": "summary",
                                        "kind": {
                                            "section": {
                                                "title": "Summary",
                                                "children": [
                                                    {
                                                        "id": "ledger-id",
                                                        "kind": {
                                                            "text": {
                                                                "text": "Ledger ID: ledger-1"
                                                            }
                                                        },
                                                        "events": []
                                                    },
                                                    {
                                                        "id": "journal-commodity",
                                                        "kind": {
                                                            "text": {
                                                                "text": "Journal commodity: USD"
                                                            }
                                                        },
                                                        "events": []
                                                    },
                                                    {
                                                        "id": "account-ref-count",
                                                        "kind": {
                                                            "text": {
                                                                "text": "Account refs: 2"
                                                            }
                                                        },
                                                        "events": []
                                                    },
                                                    {
                                                        "id": "transaction-ref-count",
                                                        "kind": {
                                                            "text": {
                                                                "text": "Transaction refs: 1"
                                                            }
                                                        },
                                                        "events": []
                                                    }
                                                ]
                                            }
                                        },
                                        "events": []
                                    },
                                    {
                                        "id": "account-refs",
                                        "kind": {
                                            "section": {
                                                "title": "Account refs",
                                                "children": [
                                                    {
                                                        "id": "account-refs-list",
                                                        "kind": {
                                                            "list": {
                                                                "items": [
                                                                    {
                                                                        "id": "account-refs-item-0",
                                                                        "kind": {
                                                                            "text": {
                                                                                "text": "db+facet:///doc/assets/main"
                                                                            }
                                                                        },
                                                                        "events": []
                                                                    },
                                                                    {
                                                                        "id": "account-refs-item-1",
                                                                        "kind": {
                                                                            "text": {
                                                                                "text": "db+facet:///doc/income/main"
                                                                            }
                                                                        },
                                                                        "events": []
                                                                    }
                                                                ]
                                                            }
                                                        },
                                                        "events": []
                                                    }
                                                ]
                                            }
                                        },
                                        "events": []
                                    },
                                    {
                                        "id": "transaction-refs",
                                        "kind": {
                                            "section": {
                                                "title": "Transaction refs",
                                                "children": [
                                                    {
                                                        "id": "transaction-refs-list",
                                                        "kind": {
                                                            "list": {
                                                                "items": [
                                                                    {
                                                                        "id": "transaction-refs-item-0",
                                                                        "kind": {
                                                                            "text": {
                                                                                "text": "db+facet:///doc/txn-1/main"
                                                                            }
                                                                        },
                                                                        "events": []
                                                                    }
                                                                ]
                                                            }
                                                        },
                                                        "events": []
                                                    }
                                                ]
                                            }
                                        },
                                        "events": []
                                    }
                                ]
                            }
                        },
                        "events": []
                    }
                }
            }),
        );
    }

    #[test]
    fn plug_manifest_declares_ledger_meta_custom_view() {
        let manifest = plug_manifest();

        let ledger_meta_facet = manifest
            .facets
            .iter()
            .find(|facet| facet.key_tag.0 == "org.example.dayledger.meta")
            .expect("ledger meta facet should exist");
        match &ledger_meta_facet.display_config.deets {
            FacetDisplayDeets::CustomView {
                view,
                mode,
                priority,
            } => {
                assert_eq!(view.plug_id, None);
                assert_eq!(view.view_key.0, LEDGER_META_VIEW_KEY);
                assert_eq!(*mode, FacetViewMode::Display);
                assert_eq!(*priority, 0);
            }
            other => panic!("unexpected display config for ledger meta facet: {other:?}"),
        }

        let view = manifest
            .views
            .get(LEDGER_META_VIEW_KEY)
            .expect("ledger meta view should be declared");
        assert_eq!(view.title, "Ledger Meta");
        assert_eq!(
            view.desc,
            "Read-only structural overview of a LedgerMeta facet"
        );
        match &view.provider {
            ViewProviderManifest::StatelessWasm { bundle, export } => {
                assert_eq!(bundle.as_str(), "plug_dayledger");
                assert_eq!(export.as_str(), LEDGER_META_VIEW_EXPORT);
            }
        }
    }
}
