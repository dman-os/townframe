#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}
mod types;

#[cfg(target_arch = "wasm32")]
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
#[cfg(test)]
mod e2e;
#[cfg(target_arch = "wasm32")]
mod wflows;

use daybook_types::manifest::{
    CommandDeets, CommandManifest, DocPredicateClause, FacetDependencyManifest, FacetManifest,
    FacetReferenceKind, FacetReferenceManifest, PlugManifest, ProcessorDeets, ProcessorManifest,
    RoutineDocAcl, RoutineFacetAccess, RoutineImpl, RoutineLocalStateAccess, RoutineManifest,
};
use std::sync::Arc;

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

    pub(crate) fn tuple_list_take<T>(pairs: &mut Vec<(String, T)>, key: &str) -> Option<T> {
        let ix = pairs.iter().position(|(entry_key, _)| entry_key == key)?;
        Some(pairs.swap_remove(ix).1)
    }

    pub(crate) fn row_text(
        row: &crate::wit::townframe::sql::types::ResultRow,
        name: &str,
    ) -> Option<String> {
        row.iter().find_map(|entry| match &entry.value {
            crate::wit::townframe::sql::types::SqlValue::Text(value)
                if entry.column_name == name =>
            {
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
            crate::wit::townframe::sql::types::SqlValue::Blob(value)
                if entry.column_name == name =>
            {
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

#[cfg(target_arch = "wasm32")]
pub(crate) use wasm_runtime::{
    embedding_bytes_to_f32, row_blob, row_i64, row_text, tuple_list_get, tuple_list_take,
};

pub fn plug_manifest() -> PlugManifest {
    use crate::types::{PlabelFacetTag, PseudoLabel, PseudoLabelCandidatesFacet, PseudoLabelError};
    use daybook_types::doc::{Blob, Embedding, Note, WellKnownFacetTag};
    use daybook_types::manifest::{LocalStateManifest, PlugDependencyManifest};

    PlugManifest {
        namespace: "daybook".into(),
        name: "plabels".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Pseudo Labels".into(),
        desc: "Pseudo-labeling routines and facets".into(),
        local_states: [
            (
                "label-classifier".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
            (
                "label-candidates-learner".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
        ]
        .into(),
        dependencies: [(
            "@daybook/core@v0.0.1".into(),
            PlugDependencyManifest {
                keys: vec![
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Note.into(),
                        value_schema: schemars::schema_for!(Note),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Blob.into(),
                        value_schema: schemars::schema_for!(Blob),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Embedding.into(),
                        value_schema: schemars::schema_for!(Embedding),
                    },
                ],
                local_states: vec![],
            }
            .into(),
        )]
        .into(),
        routines: [
            (
                "label-note".into(),
                RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "label-note".into(),
                        bundle: "plug_plabels".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(PlabelFacetTag::PseudoLabel.as_str().into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabel.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                        key_id: Some("label-candidates".into()),
                        read: true,
                        write: true,
                    }],
                    local_state_acl: vec![RoutineLocalStateAccess {
                        plug_id: "@daybook/plabels".into(),
                        local_state_key: "label-classifier".into(),
                    }],
                    command_invoke_acl: vec![],
                }
                .into(),
            ),
            (
                "label-image".into(),
                RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "label-image".into(),
                        bundle: "plug_plabels".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(PlabelFacetTag::PseudoLabel.as_str().into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Blob.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabel.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                        key_id: Some("label-candidates".into()),
                        read: true,
                        write: true,
                    }],
                    local_state_acl: vec![RoutineLocalStateAccess {
                        plug_id: "@daybook/plabels".into(),
                        local_state_key: "label-classifier".into(),
                    }],
                    command_invoke_acl: vec![],
                }
                .into(),
            ),
            (
                "learn-image-label-candidates".into(),
                RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "learn-image-label-candidates".into(),
                        bundle: "plug_plabels".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(PlabelFacetTag::PseudoLabel.as_str().into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Blob.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                        key_id: Some("label-candidates".into()),
                        read: true,
                        write: true,
                    }],
                    local_state_acl: vec![RoutineLocalStateAccess {
                        plug_id: "@daybook/plabels".into(),
                        local_state_key: "label-candidates-learner".into(),
                    }],
                    command_invoke_acl: vec![],
                }
                .into(),
            ),
            (
                "learn-note-label-candidates".into(),
                RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "learn-note-label-candidates".into(),
                        bundle: "plug_plabels".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(PlabelFacetTag::PseudoLabel.as_str().into()),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::Note.into(),
                            key_id: None,
                            read: true,
                            write: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                        key_id: Some("label-candidates".into()),
                        read: true,
                        write: true,
                    }],
                    local_state_acl: vec![RoutineLocalStateAccess {
                        plug_id: "@daybook/plabels".into(),
                        local_state_key: "label-candidates-learner".into(),
                    }],
                    command_invoke_acl: vec![],
                }
                .into(),
            ),
        ]
        .into(),
        wflow_bundles: [(
            "plug_plabels".into(),
            daybook_types::manifest::WflowBundleManifest {
                keys: vec![
                    "label-note".into(),
                    "label-image".into(),
                    "learn-image-label-candidates".into(),
                    "learn-note-label-candidates".into(),
                ],
                component_urls: vec!["build://component/plug_plabels.wasm".parse().unwrap()],
            }
            .into(),
        )]
        .into(),
        commands: [
            (
                "label-note".into(),
                CommandManifest {
                    desc: "Label note content with pseudo labels".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "label-note".into(),
                    },
                }
                .into(),
            ),
            (
                "label-image".into(),
                CommandManifest {
                    desc: "Label image documents with pseudo labels".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "label-image".into(),
                    },
                }
                .into(),
            ),
            (
                "learn-image-label-candidates".into(),
                CommandManifest {
                    desc: "Learn/merge pseudo-label candidates from images".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "learn-image-label-candidates".into(),
                    },
                }
                .into(),
            ),
            (
                "learn-note-label-candidates".into(),
                CommandManifest {
                    desc: "Learn/merge pseudo-label candidates from notes".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "learn-note-label-candidates".into(),
                    },
                }
                .into(),
            ),
        ]
        .into(),
        inits: std::collections::HashMap::new(),
        processors: [
            (
                "label-note".into(),
                ProcessorManifest {
                    desc: "Auto label text notes with pseudo labels".into(),
                    deets: ProcessorDeets::DocProcessor {
                        event_predicate: Default::default(),
                        routine_name: "label-note".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                            DocPredicateClause::HasTag(WellKnownFacetTag::Embedding.into()),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::Blob.into(),
                            ))),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                PlabelFacetTag::PseudoLabel.as_str().into(),
                            ))),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                            ))),
                        ]),
                    },
                }
                .into(),
            ),
            (
                "label-image".into(),
                ProcessorManifest {
                    desc: "Auto label image docs with pseudo labels".into(),
                    deets: ProcessorDeets::DocProcessor {
                        event_predicate: Default::default(),
                        routine_name: "label-image".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                            DocPredicateClause::HasTag(WellKnownFacetTag::Embedding.into()),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                PlabelFacetTag::PseudoLabel.as_str().into(),
                            ))),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                            ))),
                        ]),
                    },
                }
                .into(),
            ),
        ]
        .into(),
        facets: vec![
            FacetManifest {
                key_tag: PlabelFacetTag::PseudoLabel.as_str().into(),
                value_schema: schemars::schema_for!(PseudoLabel),
                display_config: Default::default(),
                references: vec![
                    FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "$.sourceRef".into(),
                        at_commit_json_path: None,
                    },
                    FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "$.candidateSetRef".into(),
                        at_commit_json_path: None,
                    },
                ],
            },
            FacetManifest {
                key_tag: PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                value_schema: schemars::schema_for!(PseudoLabelError),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                value_schema: schemars::schema_for!(PseudoLabelCandidatesFacet),
                display_config: Default::default(),
                references: vec![],
            },
        ],
    }
}
