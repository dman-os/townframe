#![recursion_limit = "256"]

mod interlude {
    pub use utils_rs::prelude::*;
}
pub mod label_algo;
pub mod learn_algo;
pub mod types;

#[cfg(test)]
mod e2e;

use daybook_types::manifest::CommandDeets;
use daybook_types::manifest::CommandManifest;
use daybook_types::manifest::DocPredicateClause;
use daybook_types::manifest::FacetDependencyManifest;
use daybook_types::manifest::FacetManifest;
use daybook_types::manifest::FacetReferenceManifest;
use daybook_types::manifest::PlugManifest;
use daybook_types::manifest::ProcessorDeets;
use daybook_types::manifest::ProcessorManifest;
use daybook_types::manifest::RoutineDocAcl;
use daybook_types::manifest::RoutineFacetAccess;
use daybook_types::manifest::RoutineImpl;
use daybook_types::manifest::RoutineLocalStateAccess;
use daybook_types::manifest::RoutineManifest;
use std::sync::Arc;

const ROUTINES_COMPONENT_URL: &str = "build://component/plug_plabels_routines.wasm";

pub fn plug_manifest() -> PlugManifest {
    use crate::types::PlabelFacetTag;
    use crate::types::PseudoLabel;
    use crate::types::PseudoLabelCandidatesFacet;
    use crate::types::PseudoLabelError;
    use daybook_types::doc::Blob;
    use daybook_types::doc::Embedding;
    use daybook_types::doc::Note;
    use daybook_types::doc::WellKnownFacetTag;
    use daybook_types::manifest::LocalStateManifest;
    use daybook_types::manifest::PlugDependencyManifest;

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
        views: Default::default(),
        routines: [
            (
                "label-note".into(),
                RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "label-note".into(),
                        bundle: "plug_plabels".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabel.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: None,
                            read: false,
                            write: false,
                            create: true,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: Some("label-candidates".into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                    ],
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
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Blob.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabel.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: None,
                            read: false,
                            write: false,
                            create: true,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: Some("label-candidates".into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                    ],
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
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Blob.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: None,
                            read: false,
                            write: false,
                            create: true,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: Some("label-candidates".into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                    ],
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
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::Note.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: None,
                            read: false,
                            write: false,
                            create: true,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
                            key_id: Some("label-candidates".into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                    ],
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
                component_urls: vec![ROUTINES_COMPONENT_URL.parse().unwrap()],
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
                references: vec![FacetReferenceManifest::UrlString {
                    json_path: "$.sourceRef".into(),
                }],
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

pub fn tuple_list_get<'a, T>(pairs: &'a [(String, T)], key: &str) -> Option<&'a T> {
    pairs
        .iter()
        .find(|(entry_key, _)| entry_key == key)
        .map(|(_, entry_value)| entry_value)
}

pub fn embedding_bytes_to_f32(bytes: &[u8]) -> utils_rs::prelude::Res<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        utils_rs::prelude::eyre::bail!(
            "embedding bytes length {} is not divisible by 4",
            bytes.len()
        );
    }
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .copied()
        .map(f32::from_le_bytes)
        .collect())
}

pub fn facet_key_id(facet_key: &str) -> String {
    daybook_types::doc::FacetKey::from(facet_key).id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_keeps_bundle_identity_with_routines_component() {
        let manifest = plug_manifest();
        assert_eq!(manifest.id(), "@daybook/plabels");
        assert_eq!(manifest.wflow_bundles.len(), 1);
        let bundle = manifest.wflow_bundles.get("plug_plabels").unwrap();
        assert_eq!(
            bundle.component_urls,
            vec![
                "build://component/plug_plabels_routines.wasm"
                    .parse::<utils_rs::prelude::Url>()
                    .unwrap()
            ],
        );
    }

    #[test]
    fn facet_key_id_uses_the_key_suffix() {
        assert_eq!(facet_key_id("org.example.note/custom"), "custom");
        assert_eq!(facet_key_id("org.example.note"), "main");
    }
}
