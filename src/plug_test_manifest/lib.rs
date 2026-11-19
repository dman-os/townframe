#![recursion_limit = "256"]

mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use std::sync::Arc;

use daybook_types::manifest::CommandDeets;
use daybook_types::manifest::CommandManifest;
use daybook_types::manifest::DocPredicateClause;
use daybook_types::manifest::FacetDependencyManifest;
use daybook_types::manifest::PlugDependencyManifest;
use daybook_types::manifest::PlugManifest;
use daybook_types::manifest::RoutineDocAcl;
use daybook_types::manifest::RoutineFacetAccess;
use daybook_types::manifest::RoutineImpl;
use daybook_types::manifest::RoutineManifest;
use daybook_types::manifest::ViewManifest;
use daybook_types::manifest::ViewProviderManifest;
use daybook_types::view::EmitViewActionV1;
use daybook_types::view::MarkdownNodeV1;
use daybook_types::view::ViewActionV1;
use daybook_types::view::ViewEventBindingV1;
use daybook_types::view::ViewEventKindV1;
use daybook_types::view::ViewNodeId;
use daybook_types::view::ViewNodeKindV1;
use daybook_types::view::ViewNodeV1;
use daybook_types::view::ViewSpec;
use daybook_types::view::ViewSpecV1;

pub mod facet_helper {
    pub fn facet_key_id(facet_key: &str) -> String {
        daybook_types::doc::FacetKey::from(facet_key).id
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn facet_key_id_uses_the_key_suffix() {
            assert_eq!(facet_key_id("org.example.note/custom"), "custom");
            assert_eq!(facet_key_id("org.example.note"), "main");
        }
    }
}

#[cfg(test)]
mod e2e;

pub const SAMPLE_VIEW_KEY: &str = "sample-summary-card";
const SAMPLE_VIEW_EXPORT: &str = "render-facet-view";

fn sample_stateless_view_spec() -> ViewSpec {
    ViewSpec::V1(ViewSpecV1 {
        root: ViewNodeV1 {
            id: ViewNodeId::from("root"),
            kind: ViewNodeKindV1::Card(daybook_types::view::CardNodeV1 {
                title: Some("Sample summary".into()),
                children: vec![
                    ViewNodeV1 {
                        id: ViewNodeId::from("summary"),
                        kind: ViewNodeKindV1::Markdown(MarkdownNodeV1 {
                            markdown: "A tiny sample stateless view from `plug_test`.".into(),
                        }),
                        events: vec![],
                    },
                    ViewNodeV1 {
                        id: ViewNodeId::from("action"),
                        kind: ViewNodeKindV1::Button(daybook_types::view::ButtonNodeV1 {
                            label: "Emit event".into(),
                        }),
                        events: vec![ViewEventBindingV1 {
                            event: ViewEventKindV1::Click,
                            action: ViewActionV1::Emit(EmitViewActionV1 {
                                name: "plug-test.sample-button-clicked".into(),
                                payload: serde_json::json!({
                                    "source": "plug_test",
                                })
                                .into(),
                            }),
                        }],
                    },
                ],
            }),
            events: vec![],
        },
    })
}

pub fn sample_stateless_view_json() -> String {
    serde_json::to_string(&sample_stateless_view_spec()).expect(ERROR_JSON)
}

#[derive(Debug, serde::Deserialize)]
pub struct ChildArgs {
    pub source: String,
}

pub fn plug_manifest() -> PlugManifest {
    use daybook_types::doc::WellKnownFacetTag;
    use daybook_types::manifest::FacetManifest;

    PlugManifest {
        namespace: "daybook".into(),
        name: "test".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Daybook Test Plug".into(),
        desc: "Internal e2e test plug for command invocation".into(),
        local_states: [
            (
                "capability-report".into(),
                Arc::new(daybook_types::manifest::LocalStateManifest::SqliteFile {}),
            ),
            (
                "doc-embedding-index".into(),
                Arc::new(daybook_types::manifest::LocalStateManifest::SqliteFile {}),
            ),
        ]
        .into(),
        dependencies: [(
            "@daybook/core@v0.0.1".into(),
            PlugDependencyManifest {
                keys: vec![
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::LabelGeneric.into(),
                        value_schema: schemars::schema_for!(String),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Note.into(),
                        value_schema: schemars::schema_for!(daybook_types::doc::Note),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Blob.into(),
                        value_schema: schemars::schema_for!(daybook_types::doc::Blob),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Embedding.into(),
                        value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
                    },
                ],
                local_states: vec![],
            }
            .into(),
        )]
        .into(),
        routines: [
            (
                "invoke-child-success".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "invoke-child-success".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![
                        daybook_pdk::build_command_url("@daybook/test", "child-success").unwrap(),
                    ],
                }),
            ),
            (
                "invoke-child-failure".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "invoke-child-failure".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![
                        daybook_pdk::build_command_url("@daybook/test", "child-failure").unwrap(),
                    ],
                }),
            ),
            (
                "child-success".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "child-success".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "child-failure".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "child-failure".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-full-command".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Note.into(),
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
                            tag: "org.example.test.config".into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config-ro".into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        },
                    ],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![
                        daybook_pdk::build_command_url("@daybook/test", "child-success").unwrap(),
                    ],
                }),
            ),
            (
                "report-full-processor".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Note.into(),
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
                            tag: "org.example.test.config".into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config-ro".into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        },
                    ],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-minimal-command".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-minimal-processor".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-downscope".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-downscope".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-denied-update".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-denied-update".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-acl-aggregate".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-acl-aggregate".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: false,
                                write: true,
                                create: false,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-create-facet".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-create-facet".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: "org.example.test.createable".into(),
                                key_id: None,
                                read: false,
                                write: false,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-get-create-token".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-get-create-token".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: "org.example.test.createable".into(),
                                key_id: None,
                                read: false,
                                write: false,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-key-specific-create-acl".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-key-specific-create-acl".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: "org.example.test.createable".into(),
                                // key-specific: only "authorized-key" may be created
                                key_id: Some("authorized-key".into()),
                                read: false,
                                write: false,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-delete-facet".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-delete-facet".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: true,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-label".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-label".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: true,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "ocr-image".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "ocr-image".into(),
                        bundle: "plug_test".into(),
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
                                tag: WellKnownFacetTag::Note.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "embed-image".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "embed-image".into(),
                        bundle: "plug_test".into(),
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
                                write: true,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "embed-text".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "embed-text".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
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
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "index-embedding".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "index-embedding".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::Embedding.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::Embedding.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "doc-embedding-index".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
        ]
        .into(),
        wflow_bundles: [(
            "plug_test".into(),
            daybook_types::manifest::WflowBundleManifest {
                keys: vec![
                    "invoke-child-success".into(),
                    "invoke-child-failure".into(),
                    "child-success".into(),
                    "child-failure".into(),
                    "report-capabilities".into(),
                    "test-downscope".into(),
                    "test-denied-update".into(),
                    "test-acl-aggregate".into(),
                    "test-create-facet".into(),
                    "test-get-create-token".into(),
                    "test-key-specific-create-acl".into(),
                    "test-delete-facet".into(),
                    "test-label".into(),
                    "ocr-image".into(),
                    "embed-image".into(),
                    "embed-text".into(),
                    "index-embedding".into(),
                ],
                component_urls: vec!["build://component/plug_test_routines.wasm".parse().unwrap()],
            }
            .into(),
        )]
        .into(),
        views: [(
            SAMPLE_VIEW_KEY.into(),
            Arc::new(ViewManifest {
                title: "Sample Summary".into(),
                desc: "Sample stateless view for plug_test".into(),
                provider: ViewProviderManifest::StatelessWasm {
                    bundle: "plug_test".into(),
                    export: SAMPLE_VIEW_EXPORT.into(),
                },
            }),
        )]
        .into(),
        commands: [
            (
                "invoke-child-success".into(),
                Arc::new(CommandManifest {
                    desc: "invoke child-success via command token".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "invoke-child-success".into(),
                    },
                }),
            ),
            (
                "invoke-child-failure".into(),
                Arc::new(CommandManifest {
                    desc: "invoke child-failure via command token".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "invoke-child-failure".into(),
                    },
                }),
            ),
            (
                "child-success".into(),
                Arc::new(CommandManifest {
                    desc: "child success command".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "child-success".into(),
                    },
                }),
            ),
            (
                "child-failure".into(),
                Arc::new(CommandManifest {
                    desc: "child failure command".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "child-failure".into(),
                    },
                }),
            ),
            (
                "report-full-command".into(),
                Arc::new(CommandManifest {
                    desc: "report capabilities (full command)".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "report-full-command".into(),
                    },
                }),
            ),
            (
                "report-minimal-command".into(),
                Arc::new(CommandManifest {
                    desc: "report capabilities (minimal command)".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "report-minimal-command".into(),
                    },
                }),
            ),
            (
                "test-label".into(),
                Arc::new(CommandManifest {
                    desc: "Add a test LabelGeneric for testing".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "test-label".into(),
                    },
                }),
            ),
            (
                "embed-image".into(),
                Arc::new(CommandManifest {
                    desc: "Embed image blob and write embedding facet".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "embed-image".into(),
                    },
                }),
            ),
            (
                "embed-text".into(),
                Arc::new(CommandManifest {
                    desc: "Embed note text and write embedding facet".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "embed-text".into(),
                    },
                }),
            ),
            (
                "index-embedding".into(),
                Arc::new(CommandManifest {
                    desc: "Index embedding facet into local vector store".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "index-embedding".into(),
                    },
                }),
            ),
        ]
        .into(),
        inits: Default::default(),
        processors: [
            (
                "test-label".into(),
                Arc::new(daybook_types::manifest::ProcessorManifest {
                    desc: "Add a test LabelGeneric for testing".into(),
                    deets: daybook_types::manifest::ProcessorDeets::DocProcessor {
                        event_predicate: default(),
                        routine_name: "test-label".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::Blob.into(),
                            ))),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::LabelGeneric.into(),
                            ))),
                        ]),
                    },
                }),
            ),
            (
                "ocr-image".into(),
                Arc::new(daybook_types::manifest::ProcessorManifest {
                    desc: "Extract OCR text from blob image into note".into(),
                    deets: daybook_types::manifest::ProcessorDeets::DocProcessor {
                        event_predicate: default(),
                        routine_name: "ocr-image".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::Note.into(),
                            ))),
                        ]),
                    },
                }),
            ),
            (
                "embed-image".into(),
                Arc::new(daybook_types::manifest::ProcessorManifest {
                    desc: "Compute image embedding facet from image blob".into(),
                    deets: daybook_types::manifest::ProcessorDeets::DocProcessor {
                        event_predicate: default(),
                        routine_name: "embed-image".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                            DocPredicateClause::Not(Box::new(
                                DocPredicateClause::HasReferenceToTag {
                                    source_tag: WellKnownFacetTag::Embedding.into(),
                                    target_tag: WellKnownFacetTag::Blob.into(),
                                },
                            )),
                        ]),
                    },
                }),
            ),
            (
                "embed-text".into(),
                Arc::new(daybook_types::manifest::ProcessorManifest {
                    desc: "Compute embedding facet from note content".into(),
                    deets: daybook_types::manifest::ProcessorDeets::DocProcessor {
                        event_predicate: default(),
                        routine_name: "embed-text".into(),
                        predicate: DocPredicateClause::And(vec![
                            DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::Blob.into(),
                            ))),
                            DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                WellKnownFacetTag::Embedding.into(),
                            ))),
                        ]),
                    },
                }),
            ),
            (
                "index-embedding".into(),
                Arc::new(daybook_types::manifest::ProcessorManifest {
                    desc: "Index embedding facets into local sqlite vec store".into(),
                    deets: daybook_types::manifest::ProcessorDeets::DocProcessor {
                        event_predicate: default(),
                        routine_name: "index-embedding".into(),
                        predicate: DocPredicateClause::HasTag(WellKnownFacetTag::Embedding.into()),
                    },
                }),
            ),
        ]
        .into(),
        facets: vec![
            FacetManifest {
                key_tag: "org.example.test.config".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: "org.example.test.config-ro".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: "org.example.test.createable".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_keeps_bundle_identity_with_routines_component() {
        let manifest = plug_manifest();
        assert_eq!(manifest.id(), "@daybook/test");
        assert_eq!(manifest.wflow_bundles.len(), 1);
        let bundle = manifest.wflow_bundles.get("plug_test").unwrap();
        assert_eq!(
            bundle.component_urls,
            vec![
                "build://component/plug_test_routines.wasm"
                    .parse::<utils_rs::prelude::Url>()
                    .unwrap()
            ],
        );
    }

    #[test]
    fn plug_manifest_uses_flat_routine_doc_acl_shape() {
        let manifest = plug_manifest();
        for (name, routine) in &manifest.routines {
            assert!(
                !routine.doc_acls.is_empty() || !routine.query_acls.is_empty(),
                "routine {name} should have at least one doc_acl or query_acl"
            );
            for acl in &routine.doc_acls {
                assert!(
                    !acl.facet_acl.is_empty(),
                    "routine {name} doc_acl should have non-empty facet_acl"
                );
            }
        }
    }

    #[test]
    fn plug_manifest_has_command_invoke_acl_where_needed() {
        let manifest = plug_manifest();
        let invoke_child_success = manifest.routines.get("invoke-child-success").unwrap();
        assert!(
            !invoke_child_success.command_invoke_acl.is_empty(),
            "invoke-child-success should have command_invoke_acl"
        );

        let invoke_child_failure = manifest.routines.get("invoke-child-failure").unwrap();
        assert!(
            !invoke_child_failure.command_invoke_acl.is_empty(),
            "invoke-child-failure should have command_invoke_acl"
        );

        let child_success = manifest.routines.get("child-success").unwrap();
        assert!(
            child_success.command_invoke_acl.is_empty(),
            "child-success should not have command_invoke_acl"
        );
    }

    #[test]
    fn plug_manifest_routines_have_expected_working_facet_tag() {
        let manifest = plug_manifest();
        for (name, routine) in &manifest.routines {
            if name.starts_with("test-")
                || name.starts_with("invoke-")
                || name.starts_with("child-")
                || name.starts_with("report-")
            {
                let has_label_generic = routine.doc_acls.iter().any(|acl| {
                    acl.facet_acl
                        .iter()
                        .any(|fa| fa.tag.0 == "org.example.daybook.labelGeneric")
                });
                assert!(
                    has_label_generic,
                    "routine {name} should have labelgeneric in its facet_acl"
                );
            }
        }
    }

    #[test]
    fn plug_manifest_declares_sample_stateless_view() {
        let manifest = plug_manifest();
        let view = manifest
            .views
            .get(SAMPLE_VIEW_KEY)
            .expect("sample view should be declared");

        assert_eq!(view.title, "Sample Summary");
        assert_eq!(view.desc, "Sample stateless view for plug_test");

        match &view.provider {
            ViewProviderManifest::StatelessWasm { bundle, export } => {
                assert_eq!(bundle.as_str(), "plug_test");
                assert_eq!(export.as_str(), SAMPLE_VIEW_EXPORT);
            }
        }
    }

    #[test]
    fn sample_stateless_view_json_has_expected_schema_shape() {
        let json: serde_json::Value =
            serde_json::from_str(&sample_stateless_view_json()).expect("parse view JSON");
        assert_eq!(
            json,
            serde_json::json!({
                "schemaVersion": "v1",
                "spec": {
                    "root": {
                        "id": "root",
                        "kind": {
                            "card": {
                                "title": "Sample summary",
                                "children": [
                                    {
                                        "id": "summary",
                                        "kind": {
                                            "markdown": {
                                                "markdown": "A tiny sample stateless view from `plug_test`."
                                            }
                                        },
                                        "events": []
                                    },
                                    {
                                        "id": "action",
                                        "kind": {
                                            "button": {
                                                "label": "Emit event"
                                            }
                                        },
                                        "events": [
                                            {
                                                "event": "click",
                                                "action": {
                                                    "emit": {
                                                        "name": "plug-test.sample-button-clicked",
                                                        "payload": {
                                                            "source": "plug_test"
                                                        }
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "events": []
                    }
                }
            })
        );
    }

    #[test]
    fn plug_manifest_serializes_and_deserializes() {
        let manifest = plug_manifest();
        let json = serde_json::to_value(&manifest).expect("serialize manifest");
        let deserialized: PlugManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(manifest.routines.len(), deserialized.routines.len());
        assert_eq!(manifest.commands.len(), deserialized.commands.len());
    }
}
