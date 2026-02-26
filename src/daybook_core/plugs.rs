use crate::interlude::*;
use tokio_util::sync::CancellationToken;

pub mod manifest;
pub mod reference;

pub fn system_plugs() -> Vec<manifest::PlugManifest> {
    use daybook_types::doc::*;
    use manifest::*;

    vec![
        PlugManifest {
            namespace: "daybook".into(),
            name: "core".into(),
            version: "0.0.1".parse().unwrap(),
            title: "Daybook Core".into(),
            desc: "Core keys and routines".into(),
            local_states: default(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            processors: default(),
            facets: vec![
                FacetManifest {
                    key_tag: WellKnownFacetTag::Dmeta.into(),
                    value_schema: schemars::schema_for!(serde_json::Value),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::RefGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::LabelGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::TitleGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: FacetDisplayHint {
                        display_title: Some("Title".to_string()),
                        deets: FacetKeyDisplayDeets::Title { show_editor: true },
                        ..default()
                    },
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::PathGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: FacetDisplayHint {
                        display_title: Some("Path".to_string()),
                        deets: FacetKeyDisplayDeets::UnixPath,
                        ..default()
                    },
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::ImageMetadata.into(),
                    value_schema: schemars::schema_for!(ImageMetadata),
                    display_config: default(),
                    references: vec![FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "/facetRef".into(),
                        at_commit_json_path: Some("/refHeads".into()),
                    }],
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::Note.into(),
                    value_schema: schemars::schema_for!(Note),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::Blob.into(),
                    value_schema: schemars::schema_for!(Blob),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::Pending.into(),
                    value_schema: schemars::schema_for!(Pending),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::Body.into(),
                    value_schema: schemars::schema_for!(Body),
                    display_config: default(),
                    references: vec![FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "/order".into(),
                        at_commit_json_path: None,
                    }],
                },
            ],
        },
        PlugManifest {
            namespace: "daybook".into(),
            name: "wip".into(),
            version: "0.0.1".parse().unwrap(),
            title: "Daybook WIP".into(),
            desc: "Experiment bed for WIP features".into(),
            local_states: [
                (
                    "doc-embedding-index".into(),
                    Arc::new(LocalStateManifest::SqliteFile {}),
                ),
                (
                    "doc-facet-set-index".into(),
                    Arc::new(LocalStateManifest::SqliteFile {}),
                ),
                (
                    "doc-facet-ref-index".into(),
                    Arc::new(LocalStateManifest::SqliteFile {}),
                ),
                (
                    "image-label-classifier".into(),
                    Arc::new(LocalStateManifest::SqliteFile {}),
                ),
                (
                    "learned-image-label-proposals".into(),
                    Arc::new(LocalStateManifest::SqliteFile {}),
                ),
            ]
            .into(),
            dependencies: [
                //
                (
                    "@daybook/core@v0.0.1".into(),
                    PlugDependencyManifest {
                        keys: vec![
                            FacetDependencyManifest {
                                key_tag: WellKnownFacetTag::Note.into(),
                                value_schema: schemars::schema_for!(Note),
                            },
                            FacetDependencyManifest {
                                key_tag: WellKnownFacetTag::LabelGeneric.into(),
                                value_schema: schemars::schema_for!(String),
                            },
                            FacetDependencyManifest {
                                key_tag: WellKnownFacetTag::Blob.into(),
                                value_schema: schemars::schema_for!(Blob),
                            },
                        ],
                        local_states: vec![],
                    }
                    .into(),
                ),
            ]
            .into(),
            routines: [
                (
                    "pseudo-label".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "pseudo-label".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::PseudoLabel.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Note.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::PseudoLabel.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![],
                        },
                        local_state_acl: vec![],
                    }
                    .into(),
                ),
                (
                    "ocr-image".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "ocr-image".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::Note.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Blob.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Note.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![RoutineFacetAccess {
                                tag: WellKnownFacetTag::PseudoLabelCandidates.into(),
                                key_id: Some("daybook-wip-image-label-set".into()),
                                read: true,
                                write: true,
                            }],
                        },
                        local_state_acl: vec![],
                    }
                    .into(),
                ),
                (
                    "embed-image".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "embed-image".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::Embedding.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Blob.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Embedding.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![],
                        },
                        local_state_acl: vec![],
                    }
                    .into(),
                ),
                (
                    "embed-text".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "embed-text".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::Embedding.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Note.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Embedding.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![],
                        },
                        local_state_acl: vec![],
                    }
                    .into(),
                ),
                (
                    "classify-image-label".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "classify-image-label".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::PseudoLabel.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Blob.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Embedding.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::PseudoLabel.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![RoutineFacetAccess {
                                tag: WellKnownFacetTag::PseudoLabelCandidates.into(),
                                key_id: Some("daybook-wip-image-label-set".into()),
                                read: true,
                                write: true,
                            }],
                        },
                        local_state_acl: vec![RoutineLocalStateAccess {
                            plug_id: "@daybook/wip".into(),
                            local_state_key: "image-label-classifier".into(),
                        }],
                    }
                    .into(),
                ),
                (
                    "index-embedding".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "index-embedding".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::Embedding.into(),
                            facet_acl: vec![RoutineFacetAccess {
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            }],
                            config_prop_acl: vec![],
                        },
                        local_state_acl: vec![RoutineLocalStateAccess {
                            plug_id: "@daybook/wip".into(),
                            local_state_key: "doc-embedding-index".into(),
                        }],
                    }
                    .into(),
                ),
                (
                    "learn-image-label-proposals".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "learn-image-label-proposals".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::PseudoLabel.into(),
                            facet_acl: vec![
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Blob.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::Embedding.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    tag: WellKnownFacetTag::PseudoLabel.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_prop_acl: vec![RoutineFacetAccess {
                                tag: WellKnownFacetTag::PseudoLabelCandidates.into(),
                                key_id: Some("daybook_wip_learned_image_label_proposals".into()),
                                read: true,
                                write: true,
                            }],
                        },
                        local_state_acl: vec![RoutineLocalStateAccess {
                            plug_id: "@daybook/wip".into(),
                            local_state_key: "learned-image-label-proposals".into(),
                        }],
                    }
                    .into(),
                ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "test-label".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocFacet {
                            working_facet_tag: WellKnownFacetTag::LabelGeneric.into(),
                            facet_acl: vec![RoutineFacetAccess {
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                            }],
                            config_prop_acl: vec![],
                        },
                        local_state_acl: vec![],
                    }
                    .into(),
                ),
            ]
            .into(),
            commands: [
                (
                    "pseudo-label".into(),
                    CommandManifest {
                        desc: "Use LLM to label the document".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "pseudo-label".into(),
                        },
                    }
                    .into(),
                ),
                (
                    "embed-image".into(),
                    CommandManifest {
                        desc: "Embed image blob and write embedding facet".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "embed-image".into(),
                        },
                    }
                    .into(),
                ),
                (
                    "embed-text".into(),
                    CommandManifest {
                        desc: "Embed note text and write embedding facet".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "embed-text".into(),
                        },
                    }
                    .into(),
                ),
                (
                    "classify-image-label".into(),
                    CommandManifest {
                        desc: "Classify image embedding into a label using local state KNN".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "classify-image-label".into(),
                        },
                    }
                    .into(),
                ),
                (
                    "index-embedding".into(),
                    CommandManifest {
                        desc: "Index embedding facet into local vector store".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "index-embedding".into(),
                        },
                    }
                    .into(),
                ),
                (
                    "learn-image-label-proposals".into(),
                    CommandManifest {
                        desc: "Learn image label proposals into a global pseudo-label proposal set"
                            .into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "learn-image-label-proposals".into(),
                        },
                    }
                    .into(),
                ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    CommandManifest {
                        desc: "Add a test LabelGeneric for testing".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "test-label".into(),
                        },
                    }
                    .into(),
                ),
            ]
            .into(),
            processors: [
                (
                    "pseudo-label".into(),
                    ProcessorManifest {
                        desc: "Use LLM to label the document content".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "pseudo-label".into(),
                            predicate: DocPredicateClause::And(vec![
                                DocPredicateClause::HasTag(WellKnownFacetTag::Note.into()),
                                DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                    WellKnownFacetTag::Blob.into(),
                                ))),
                            ]),
                        },
                    }
                    .into(),
                ),
                (
                    "ocr-image".into(),
                    ProcessorManifest {
                        desc: "Extract OCR text from blob image into note".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "ocr-image".into(),
                            predicate: DocPredicateClause::And(vec![
                                DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                                DocPredicateClause::Not(Box::new(DocPredicateClause::HasTag(
                                    WellKnownFacetTag::Note.into(),
                                ))),
                            ]),
                        },
                    }
                    .into(),
                ),
                (
                    "embed-image".into(),
                    ProcessorManifest {
                        desc: "Compute image embedding facet from image blob".into(),
                        deets: ProcessorDeets::DocProcessor {
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
                    }
                    .into(),
                ),
                (
                    "embed-text".into(),
                    ProcessorManifest {
                        desc: "Compute embedding facet from note content".into(),
                        deets: ProcessorDeets::DocProcessor {
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
                    }
                    .into(),
                ),
                (
                    "classify-image-label".into(),
                    ProcessorManifest {
                        desc: "Classify image embeddings into local fallback labels".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "classify-image-label".into(),
                            predicate: DocPredicateClause::HasReferenceToTag {
                                source_tag: WellKnownFacetTag::Embedding.into(),
                                target_tag: WellKnownFacetTag::Blob.into(),
                            },
                        },
                    }
                    .into(),
                ),
                (
                    "index-embedding".into(),
                    ProcessorManifest {
                        desc: "Index embedding facets into local sqlite vec store".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "index-embedding".into(),
                            predicate: DocPredicateClause::HasTag(
                                WellKnownFacetTag::Embedding.into(),
                            ),
                        },
                    }
                    .into(),
                ),
                // FIXME: temporary disable label learner
                // (
                //     "learn-image-label-proposals".into(),
                //     ProcessorManifest {
                //         desc:
                //             "Learn image label proposals from image embeddings using multimodal LLM"
                //                 .into(),
                //         deets: ProcessorDeets::DocProcessor {
                //             routine_name: "learn-image-label-proposals".into(),
                //             predicate: DocPredicateClause::HasReferenceToTag {
                //                 source_tag: WellKnownFacetTag::Embedding.into(),
                //                 target_tag: WellKnownFacetTag::Blob.into(),
                //             },
                //         },
                //     }
                //     .into(),
                // ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    ProcessorManifest {
                        desc: "Add a test LabelGeneric for testing".into(),
                        deets: ProcessorDeets::DocProcessor {
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
                    }
                    .into(),
                ),
            ]
            .into(),
            wflow_bundles: [
                //
                (
                    "daybook_wflows".into(),
                    WflowBundleManifest {
                        keys: vec![
                            "pseudo-label".into(),
                            "test-label".into(),
                            "ocr-image".into(),
                            "embed-image".into(),
                            "embed-text".into(),
                            "index-embedding".into(),
                            "classify-image-label".into(),
                            "learn-image-label-proposals".into(),
                        ],
                        // FIXME: make this more generic
                        component_urls: vec![
                            "static:daybook_wflows.wasm.zst".parse().unwrap(),
                            /*{
                                let path = std::path::absolute(
                                    Path::new(env!("CARGO_MANIFEST_DIR"))
                                        .join("../../target/wasm32-wasip2/release/daybook_wflows.wasm"),
                                )
                                .unwrap();

                                format!("file://{path}", path = path.to_string_lossy())
                                    .parse()
                                    .unwrap()
                            }*/
                        ],
                    }
                    .into(),
                ),
            ]
            .into(),
            facets: vec![
                //
                FacetManifest {
                    key_tag: WellKnownFacetTag::PseudoLabel.into(),
                    value_schema: schemars::schema_for!(Vec<String>),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::PseudoLabelCandidates.into(),
                    value_schema: schemars::schema_for!(daybook_types::doc::PseudoLabelCandidatesFacet),
                    display_config: default(),
                    references: default(),
                },
                FacetManifest {
                    key_tag: WellKnownFacetTag::Embedding.into(),
                    value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
                    display_config: default(),
                    references: vec![FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "/facetRef".into(),
                        at_commit_json_path: Some("/refHeads".into()),
                    }],
                },
            ],
        },
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedPlug {
    pub version: Uuid,
    pub payload: Arc<manifest::PlugManifest>,
}

#[derive(Default, Reconcile, Hydrate)]
pub struct PlugsStore {
    pub manifests: HashMap<String, ThroughJson<VersionedPlug>>,

    /// Index: property tag -> plug id (@ns/name)
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub tag_to_plug: HashMap<String, String>,
    /// Index: property tag -> facet manifest
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub facet_manifests: HashMap<String, manifest::FacetManifest>,
}

impl PlugsStore {
    pub fn rebuild_indices(&mut self) {
        self.tag_to_plug.clear();
        self.facet_manifests.clear();

        for (plug_id, versioned) in &self.manifests {
            for facet in &versioned.payload.facets {
                self.tag_to_plug
                    .insert(facet.key_tag.to_string(), plug_id.clone());
                self.facet_manifests
                    .insert(facet.key_tag.to_string(), facet.clone());
            }
        }
    }
}

#[async_trait]
impl crate::stores::Store for PlugsStore {
    fn prop() -> Cow<'static, str> {
        "plugs".into()
    }
}

pub mod version_updates {
    use super::*;
    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        doc.put(ROOT, "$schema", "daybook.plugs")?;
        reconcile_prop(
            &mut doc,
            ROOT,
            super::PlugsStore::prop().as_ref(),
            super::PlugsStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

pub struct PlugsRepo {
    pub acx: AmCtx,
    pub app_doc_id: DocumentId,
    pub app_am_handle: samod::DocHandle,
    store: crate::stores::StoreHandle<PlugsStore>,
    pub blobs: Arc<crate::blobs::BlobsRepo>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub mutation_mutex: tokio::sync::Mutex<()>,
    pub local_actor_id: automerge::ActorId,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PlugsEvent {
    // ListChanged { heads: ChangeHashSet },
    PlugAdded { id: String, heads: ChangeHashSet },
    PlugChanged { id: String, heads: ChangeHashSet },
    PlugDeleted { id: String, heads: ChangeHashSet },
}

impl crate::repos::Repo for PlugsRepo {
    type Event = PlugsEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl PlugsRepo {
    pub async fn load(
        acx: AmCtx,
        blobs: Arc<crate::blobs::BlobsRepo>,
        app_doc_id: DocumentId,
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store_val = PlugsStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(
            store_val,
            acx.clone(),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        store.mutate_sync(|store| store.rebuild_indices()).await?;

        let app_am_handle = acx
            .find_doc(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;

        let (broker, broker_stop) = acx.change_manager().add_doc(app_am_handle.clone()).await?;

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        let ticket = PlugsStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Self {
            acx: acx.clone(),
            app_doc_id: app_doc_id.clone(),
            app_am_handle,
            store,
            blobs,
            local_actor_id,
            registry: Arc::clone(&registry),
            mutation_mutex: tokio::sync::Mutex::new(()),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.handle_notifs(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                broker_stop_tokens: broker_stop.into_iter().collect(),
            },
        ))
    }

    async fn handle_notifs(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    break;
                }
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };

            events.clear();
            for notif in notifs {
                if notif.is_local_only(&self.local_actor_id) {
                    continue;
                }

                // 3. Call events_for_patch (pure-ish).
                self.events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await?;
            }

            for event in &events {
                match event {
                    PlugsEvent::PlugAdded { id, heads } | PlugsEvent::PlugChanged { id, heads } => {
                        let (new_versioned, _) = self
                            .acx
                            .hydrate_path_at_heads::<ThroughJson<VersionedPlug>>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![
                                    "manifests".into(),
                                    autosurgeon::Prop::Key(id.clone().into()),
                                ],
                            )
                            .await?
                            .expect(ERROR_INVALID_PATCH);
                        let new_versioned = new_versioned.0;

                        self.store
                            .mutate_sync(|store| {
                                store
                                    .manifests
                                    .insert(id.clone(), ThroughJson(new_versioned));
                                store.rebuild_indices();
                            })
                            .await?;
                    }
                    PlugsEvent::PlugDeleted { id, .. } => {
                        self.store
                            .mutate_sync(|store| {
                                store.manifests.remove(id);
                                store.rebuild_indices();
                            })
                            .await?;
                    }
                }
            }
            self.registry.notify(events.drain(..));
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<PlugsEvent>> {
        let (patches, heads) = self.app_am_handle.with_document(|am_doc| {
            let heads = if let Some(ref to_set) = to {
                to_set.clone()
            } else {
                ChangeHashSet(am_doc.get_heads().into())
            };
            let patches = am_doc
                .diff_obj(&automerge::ROOT, &from, &heads, true)
                .wrap_err("diff_obj failed")?;
            eyre::Ok((patches, heads))
        })?;
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads, &mut events).await?;
        }
        Ok(events)
    }

    pub fn get_plugs_heads(&self) -> ChangeHashSet {
        self.app_am_handle
            .with_document(|am_doc| ChangeHashSet(am_doc.get_heads().into()))
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<PlugsEvent>,
    ) -> Res<()> {
        let heads = ChangeHashSet(Arc::clone(patch_heads));
        match &patch.action {
            automerge::PatchAction::PutMap {
                key,
                value: (val, _),
                ..
            } if patch.path.len() == 3
                && patch.path[1].1 == automerge::Prop::Map("manifests".into()) =>
            {
                if key == "version" {
                    let Some((_obj, automerge::Prop::Map(plug_id))) = patch.path.get(2) else {
                        return Ok(());
                    };

                    let version_bytes = match val {
                        automerge::Value::Scalar(scalar) => match &**scalar {
                            automerge::ScalarValue::Bytes(bytes) => bytes,
                            _ => return Ok(()),
                        },
                        _ => return Ok(()),
                    };
                    let version = Uuid::from_slice(version_bytes)?;

                    if version.is_nil() {
                        out.push(PlugsEvent::PlugAdded {
                            id: plug_id.clone(),
                            heads,
                        });
                    } else {
                        out.push(PlugsEvent::PlugChanged {
                            id: plug_id.clone(),
                            heads,
                        });
                    }
                }
            }
            automerge::PatchAction::DeleteMap { key }
                if patch.path.len() == 2
                    && patch.path[1].1 == automerge::Prop::Map("manifests".into()) =>
            {
                out.push(PlugsEvent::PlugDeleted {
                    id: key.clone(),
                    heads,
                });
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn ensure_system_plugs(&self) -> Res<()> {
        let is_empty = self
            .store
            .query_sync(|store| store.manifests.is_empty())
            .await;
        if is_empty {
            for plug in system_plugs() {
                self.add(plug).await?;
            }
        }

        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        self.store
            .query_sync(|store| store.manifests.get(id).map(|man| Arc::clone(&man.payload)))
            .await
    }

    pub async fn get_display_hint(&self, prop_tag: &str) -> Option<manifest::FacetDisplayHint> {
        self.store
            .query_sync(|store| {
                store
                    .facet_manifests
                    .get(prop_tag)
                    .map(|facet_manifest| facet_manifest.display_config.clone())
            })
            .await
    }

    pub async fn get_facet_manifest_by_tag(
        &self,
        facet_tag: &str,
    ) -> Option<manifest::FacetManifest> {
        self.store
            .query_sync(|store| store.facet_manifests.get(facet_tag).cloned())
            .await
    }

    pub async fn list_display_hints(&self) -> Vec<(String, manifest::FacetDisplayHint)> {
        self.store
            .query_sync(|store| {
                store
                    .manifests
                    .values()
                    .flat_map(|versioned| {
                        versioned
                            .payload
                            .facets
                            .iter()
                            .map(|facet| (facet.key_tag.to_string(), facet.display_config.clone()))
                    })
                    .collect()
            })
            .await
    }

    pub async fn list_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        self.store
            .query_sync(|store| {
                store
                    .manifests
                    .values()
                    .map(|man| Arc::clone(&man.payload))
                    .collect()
            })
            .await
    }

    /// Add a new plug to the repo after validating it.
    ///
    /// This method follows a literate programming approach to clearly document
    /// the validation and reconciliation steps.
    pub async fn add(&self, mut manifest: manifest::PlugManifest) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        // we use the mutex to make a critical section
        // to avoid race conditions on the validation checks
        let _guard = self.mutation_mutex.lock().await;
        // 1. Validate the incoming plug manifest
        // We do this first to ensure that we don't pollute the store with invalid data.
        // This includes checking internal consistency, external dependencies,
        // and compatibility with existing versions of the same plug.
        self.validate_incoming_plug(&manifest).await?;

        // 1.5 Convert file:// URLs to db+blob:// URLs
        // This ensures that all components are stored in the BlobsRepo for portability.
        if true {
            for bundle in manifest.wflow_bundles.values_mut() {
                let bundle = Arc::make_mut(bundle);
                for url in bundle.component_urls.iter_mut() {
                    match url.scheme() {
                        "file" => {
                            let path = url.to_file_path().map_err(|err| {
                                eyre::eyre!("invalid path in url {url:?} {err:?}")
                            })?;
                            let data = tokio::fs::read(&path).await.wrap_err_with(|| {
                                format!("failed to read component file: {}", path.display())
                            })?;
                            let hash = self.blobs.put(&data).await?;
                            *url = url::Url::parse(&format!(
                                "{}:///{}",
                                crate::blobs::BLOB_SCHEME,
                                hash
                            ))?;
                        }
                        "static" => {
                            let wasm_zst_bytes = match url.path() {
                                "daybook_wflows.wasm.zst" => {
                                    include_bytes!(concat!(
                                        env!("OUT_DIR"),
                                        "/daybook_wflows.wasm.zst"
                                    ))
                                }
                                _ => {
                                    eyre::bail!("unsupported static wasm component_url");
                                }
                            };
                            let data = tokio::task::spawn_blocking(move || {
                                let mut wasm_bytes = vec![];
                                zstd::stream::copy_decode(&wasm_zst_bytes[..], &mut wasm_bytes)
                                    .wrap_err("error decompressing serialized component")?;
                                eyre::Ok(wasm_bytes)
                            })
                            .await??;
                            let hash = self.blobs.put(&data).await?;
                            *url = url::Url::parse(&format!(
                                "{}:///{}",
                                crate::blobs::BLOB_SCHEME,
                                hash
                            ))?;
                        }
                        crate::blobs::BLOB_SCHEME => {}
                        _ => eyre::bail!("unsupported component_url scheme: {url}"),
                    }
                }
            }
        }

        // 2. Perform Automerge reconciliation
        // Once validated, we update the Automerge store.
        // We use the plug's identity (@namespace/name) as the key in the manifests map
        // to simplify lookups and ensure uniqueness.
        let plug_id = manifest.id();

        let ((plug_id, is_update), hash) = self
            .store
            .mutate_sync(move |store| {
                let manifest = manifest;
                let is_update = store.manifests.contains_key(&plug_id);

                let versioned = VersionedPlug {
                    version: if is_update {
                        Uuid::new_v4()
                    } else {
                        Uuid::nil()
                    },
                    payload: Arc::new(manifest),
                };

                // Update the manifest in the store
                store
                    .manifests
                    .insert(plug_id.clone(), ThroughJson(versioned));

                // 3. Rebuild indices
                // Indices are in-memory caches (marked with #[autosurgeon(skip)])
                // used to hyper-accelerate validation and routing logic.
                // We rebuild them here so they're immediately available for subsequent calls.
                store.rebuild_indices();

                (plug_id, is_update)
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        // Notify listeners that the plug list or a specific plug has changed
        self.registry.notify([if is_update {
            PlugsEvent::PlugChanged { id: plug_id, heads }
        } else {
            PlugsEvent::PlugAdded { id: plug_id, heads }
        }]);

        Ok(())
    }

    /// Comprehensive validation for an incoming plug.
    ///
    /// This method checks for:
    /// - Structural validity (via garde).
    /// - Property tag clashes with other plugs.
    /// - Dependency resolution (existence and schema compatibility).
    /// - Internal consistency (commands referencing existing routines).
    /// - ACL scope restrictions.
    /// - Versioning rules (no breaking changes in non-major updates).
    pub async fn validate_incoming_plug(&self, manifest: &manifest::PlugManifest) -> Res<()> {
        use garde::Validate;

        // -- Structural Validation --
        // Use the 'garde' crate to perform basic field-level validations (regex, length, etc.)
        // defined in the manifest structs.
        manifest
            .validate()
            .map_err(|err| eyre::eyre!("validation error: {err}"))?;

        let mut seen_facet_tags = HashSet::new();
        for facet_manifest in &manifest.facets {
            let facet_tag = facet_manifest.key_tag.to_string();
            if !seen_facet_tags.insert(facet_tag.clone()) {
                eyre::bail!("duplicate facet tag '{}' in plug manifest", facet_tag);
            }

            validate_facet_reference_manifests(
                &facet_manifest.key_tag.to_string(),
                &facet_manifest.value_schema,
                &facet_manifest.references,
            )?;
        }

        let plug_id = manifest.id();
        let existing = self.get(&plug_id).await;

        // -- Versioning and Breaking Change Protection --
        // To maintain stability, we don't allow breaking changes (like removing commands
        // or changing their parameters) in minor or patch updates.
        if let Some(old) = &existing {
            if manifest.version <= old.version {
                eyre::bail!(
                    "Version must be greater than existing version (current: {}, incoming: {})",
                    old.version,
                    manifest.version
                );
            }

            let is_major = manifest.version.major > old.version.major
                || (old.version.major == 0 && manifest.version.minor > old.version.minor);

            if !is_major {
                // In non-major updates, we must ensure existing commands are preserved
                // to avoid breaking integrations or automated workflows.
                for (old_cmd_name, old_cmd) in &old.commands {
                    let new_cmd = manifest.commands.get(old_cmd_name);
                    if let Some(new_cmd) = new_cmd {
                        // Deets define the routine and parameters; changing them breaks callers.
                        // FIXME: we need a better comparison for CommandDeets if it's complex
                        if format!("{:?}", new_cmd.deets) != format!("{:?}", old_cmd.deets) {
                            eyre::bail!("Breaking change: command '{}' deets cannot change in non-major version update", old_cmd_name);
                        }
                    } else {
                        eyre::bail!("Breaking change: command '{}' cannot be removed in non-major version update", old_cmd_name);
                    }
                }
            }

            // We also check that property keys aren't removed or their schemas don't become incompatible.
            for old_prop in &old.facets {
                if let Some(new_prop) = manifest
                    .facets
                    .iter()
                    .find(|prop| prop.key_tag == old_prop.key_tag)
                {
                    if !is_schema_compatible(&old_prop.value_schema, &new_prop.value_schema) {
                        eyre::bail!(
                            "Incompatible schema for property tag '{}'",
                            old_prop.key_tag
                        );
                    }
                }
            }
        }

        // -- Property Tag Clash Detection --
        // Many parts of the system rely on property tags being unique identifiers.
        // We use an index to quickly check if any of the tags this plug wants to declare
        // are already owned by another plug.
        self.store
            .query_sync(|store| {
                for prop in &manifest.facets {
                    if let Some(owner) = store.tag_to_plug.get(&prop.key_tag.to_string()) {
                        if owner != &plug_id {
                            return Err(eyre::eyre!(
                                "Tag clash: tag '{}' is already owned by plug '{}'",
                                prop.key_tag,
                                owner
                            ));
                        }
                    }
                }
                Ok(())
            })
            .await?;

        // -- Dependency Verification --
        // Plugs can declare dependencies on other plugs to reuse their property keys.
        // We verify that:
        // 1. The depended-on plug exists.
        // 2. The specific keys being requested are actually defined by that plug.
        // 3. The requested schema is compatible with what the provider offers.
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = if dep_id_full.starts_with('@') {
                // For strings like "@ns/name@1.2.3", we want "@ns/name"
                let parts: Vec<&str> = dep_id_full.strip_prefix('@').unwrap().split('@').collect();
                format!("@{}", parts[0])
            } else {
                dep_id_full
                    .split('@')
                    .next()
                    .ok_or_eyre("invalid dependency id")?
                    .to_string()
            };
            let provider = self
                .get(&dep_base_id)
                .await
                .ok_or_eyre(format!("Dependency not found: '{}'", dep_base_id))?;

            for key_dep in &dep_manifest.keys {
                let provider_prop = provider
                    .facets
                    .iter()
                    .find(|prop| prop.key_tag == key_dep.key_tag)
                    .ok_or_eyre(format!(
                        "Dependency error: plug '{}' does not define tag '{}'",
                        dep_base_id, key_dep.key_tag
                    ))?;

                if !is_schema_compatible(&provider_prop.value_schema, &key_dep.value_schema) {
                    eyre::bail!(
                        "Dependency error: incompatible schema for tag '{}' from plug '{}'",
                        key_dep.key_tag,
                        dep_base_id
                    );
                }
            }

            for local_state_dep in &dep_manifest.local_states {
                let provider_state_kind = provider
                    .local_states
                    .get(&local_state_dep.local_state_key)
                    .ok_or_eyre(format!(
                        "Dependency error: plug '{}' does not define local_state '{}'",
                        dep_base_id, local_state_dep.local_state_key
                    ))?;
                if **provider_state_kind != local_state_dep.state_kind {
                    eyre::bail!(
                        "Dependency error: incompatible local_state kind for '{}' from plug '{}'",
                        local_state_dep.local_state_key,
                        dep_base_id
                    );
                }
            }
        }

        // -- Internal Routine Integrity --
        // Commands act as triggers for routines. If a command points to a non-existent
        // routine, it will fail at runtime. We catch these early.
        for (routine_name, routine) in &manifest.routines {
            let manifest::RoutineImpl::Wflow { bundle, key } = &routine.r#impl;
            let Some(bundle_manifest) = manifest.wflow_bundles.get(bundle) else {
                eyre::bail!(
                    "Invalid routine '{}': wflow bundle '{}' not found in manifest",
                    routine_name,
                    bundle
                );
            };
            if !bundle_manifest.keys.contains(key) {
                eyre::bail!(
                    "Invalid routine '{}': key '{}' not found in wflow bundle '{}'",
                    routine_name,
                    key,
                    bundle
                );
            }
        }

        for (cmd_name, cmd) in &manifest.commands {
            match &cmd.deets {
                manifest::CommandDeets::DocCommand { routine_name } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid command deets: routine '{}' not found in plug (command='{}')",
                            routine_name,
                            cmd_name
                        );
                    }
                }
            }
        }

        for (processor_name, processor_manifest) in &manifest.processors {
            match &processor_manifest.deets {
                manifest::ProcessorDeets::DocProcessor {
                    routine_name,
                    predicate: _,
                } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid processor deets: routine '{}' not found in plug (processor='{}')",
                            routine_name,
                            processor_name
                        );
                    }
                }
            }
        }

        // -- Component URL Validation --
        for (bundle_name, bundle) in &manifest.wflow_bundles {
            for url in &bundle.component_urls {
                match url.scheme() {
                    "file" => {
                        let path = url
                            .to_file_path()
                            .map_err(|_| eyre::eyre!("invalid file path in url: {}", url))?;
                        if !path.exists() {
                            eyre::bail!(
                                "Component file not found for bundle '{}': {}",
                                bundle_name,
                                path.display()
                            );
                        }
                    }
                    "static" => match url.path() {
                        "daybook_wflows.wasm.zst" => {}
                        _ => eyre::bail!("Unrecognized static component_url: {url}",),
                    },
                    scheme if scheme == crate::blobs::BLOB_SCHEME => {
                        let hash = url.path().trim_start_matches('/');
                        if self.blobs.get_path(hash).await.is_err() {
                            eyre::bail!(
                                "Blob not found in BlobsRepo for bundle {bundle_name:?}: {hash:?}",
                            );
                        }
                    }
                    _ => {
                        eyre::bail!(
                            "Unsupported URL scheme for bundle {bundle_name:?}: {}",
                            url.scheme()
                        );
                    }
                }
            }
        }

        // -- ACL Scope Restriction --
        // Routines must explicitly declare which properties they need access to.
        // To prevent security leaks, a routine can only specify tags that
        // the plug itself declares or explicitly depends on.
        let mut available_tags: HashSet<String> = manifest
            .facets
            .iter()
            .map(|prop| prop.key_tag.to_string())
            .collect();
        for dep in manifest.dependencies.values() {
            for key in &dep.keys {
                available_tags.insert(key.key_tag.to_string());
            }
        }
        let mut available_local_states: HashSet<(String, String)> = manifest
            .local_states
            .keys()
            .map(|key| (plug_id.clone(), key.to_string()))
            .collect();
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = if dep_id_full.starts_with('@') {
                let parts: Vec<&str> = dep_id_full.strip_prefix('@').unwrap().split('@').collect();
                format!("@{}", parts[0])
            } else {
                dep_id_full
                    .split('@')
                    .next()
                    .ok_or_eyre("invalid dependency id")?
                    .to_string()
            };
            for local_state in &dep_manifest.local_states {
                available_local_states
                    .insert((dep_base_id.clone(), local_state.local_state_key.to_string()));
            }
        }

        for (routine_name, routine) in &manifest.routines {
            for access in routine.facet_acl() {
                if !available_tags.contains(&access.tag.to_string()) {
                    eyre::bail!("Invalid ACL in routine '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}", routine_name, access.tag);
                }
            }
            for access in &routine.local_state_acl {
                if !available_local_states
                    .contains(&(access.plug_id.clone(), access.local_state_key.to_string()))
                {
                    eyre::bail!(
                        "Invalid local_state ACL in routine '{}': '{}:{}' is neither declared nor depended on by this plug",
                        routine_name,
                        access.plug_id,
                        access.local_state_key
                    );
                }
            }

            // If it's a DocProp routine, the 'working_prop_tag' must also be accessible.
            if let manifest::RoutineManifestDeets::DocFacet {
                working_facet_tag, ..
            } = &routine.deets
            {
                if !available_tags.contains(&working_facet_tag.to_string()) {
                    eyre::bail!(
                        "Invalid routine deets for '{}': working_facet_tag '{}' not in scope",
                        routine_name,
                        working_facet_tag
                    );
                }
            }
        }

        for (processor_name, processor_manifest) in &manifest.processors {
            match &processor_manifest.deets {
                manifest::ProcessorDeets::DocProcessor {
                    predicate,
                    routine_name: _,
                } => {
                    for referenced_tag in predicate.referenced_tags() {
                        if !available_tags.contains(&referenced_tag.to_string()) {
                            eyre::bail!(
                                "Invalid processor predicate in '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Helper to check JSON Schema compatibility.
///
/// In this context, 'compatible' means that the 'new' schema can accept data
/// validated by the 'old' schema without breaking (forward compatibility).
fn is_schema_compatible(old: &schemars::Schema, new: &schemars::Schema) -> bool {
    // If they are exactly the same, they are definitely compatible.
    if old == new {
        return true;
    }

    // Treat them as JSON values for a pragmatic compatibility check.
    // In schemars 1.0, Schema is a wrapper around serde_json::Value.
    let old_json = serde_json::to_value(old).unwrap_or(serde_json::Value::Null);
    let new_json = serde_json::to_value(new).unwrap_or(serde_json::Value::Null);

    is_json_schema_compatible(&old_json, &new_json)
}

fn is_json_schema_compatible(old: &serde_json::Value, new: &serde_json::Value) -> bool {
    if old == new {
        return true;
    }

    match (old, new) {
        (serde_json::Value::Object(old_obj), serde_json::Value::Object(new_obj)) => {
            // Check basic type matching
            if old_obj.get("type") != new_obj.get("type") {
                return false;
            }

            // If it's an object, check properties
            if old_obj.get("type") == Some(&serde_json::json!("object")) {
                let old_props = old_obj
                    .get("properties")
                    .and_then(|value| value.as_object());
                let new_props = new_obj
                    .get("properties")
                    .and_then(|value| value.as_object());

                if let (Some(old_props), Some(new_props)) = (old_props, new_props) {
                    // All properties in old must be present and compatible in new
                    for (name, old_val) in old_props {
                        if let Some(new_val) = new_props.get(name) {
                            if !is_json_schema_compatible(old_val, new_val) {
                                return false;
                            }
                        } else {
                            // Property removed -> breaking change
                            return false;
                        }
                    }
                }

                // Check required fields: new cannot require something that was not required in old
                let old_required = old_obj.get("required").and_then(|value| value.as_array());
                let new_required = new_obj.get("required").and_then(|value| value.as_array());
                if let Some(new_req) = new_required {
                    let old_req_set: HashSet<_> = old_required
                        .map(|array| array.iter().collect())
                        .unwrap_or_default();
                    for req in new_req {
                        if !old_req_set.contains(req) {
                            // New required field -> breaking change
                            // Unless it has a default? But JSON Schema's 'default' doesn't satisfy 'required'.
                            return false;
                        }
                    }
                }
            }

            // FIXME: Add more checks for arrays, enums, etc.
            true
        }
        _ => false,
    }
}

fn validate_facet_reference_manifests(
    facet_tag: &str,
    value_schema: &schemars::Schema,
    references: &[manifest::FacetReferenceManifest],
) -> Res<()> {
    let schema_json = serde_json::to_value(value_schema)?;
    for reference_manifest in references {
        let Some(reference_node) = crate::plugs::reference::schema_node_for_json_path(
            &schema_json,
            &reference_manifest.json_path,
        )?
        else {
            eyre::bail!(
                "invalid reference json_path '{}' for facet tag '{}': path does not exist in schema",
                reference_manifest.json_path,
                facet_tag
            );
        };

        match reference_manifest.reference_kind {
            manifest::FacetReferenceKind::UrlFacet => {
                if !crate::plugs::reference::schema_allows_url_reference(reference_node) {
                    eyre::bail!(
                        "invalid reference json_path '{}' for facet tag '{}': schema node must allow a URL string or an array of URL strings",
                        reference_manifest.json_path,
                        facet_tag
                    );
                }
            }
        }

        if let Some(at_commit_json_path) = &reference_manifest.at_commit_json_path {
            let Some(at_commit_node) = crate::plugs::reference::schema_node_for_json_path(
                &schema_json,
                at_commit_json_path,
            )?
            else {
                eyre::bail!(
                    "invalid at_commit_json_path '{}' for facet tag '{}': path does not exist in schema",
                    at_commit_json_path,
                    facet_tag
                );
            };
            if !crate::plugs::reference::schema_allows_array_of_strings(at_commit_node) {
                eyre::bail!(
                    "invalid at_commit_json_path '{}' for facet tag '{}': schema node must allow an array of commit hashes",
                    at_commit_json_path,
                    facet_tag
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_repo() -> Res<(AmCtx, Arc<PlugsRepo>, DocumentId, tempfile::TempDir)> {
        let local_actor_id = automerge::ActorId::random();
        let (acx, _acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            None::<samod::AlwaysAnnounce>,
        )
        .await?;

        let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        let doc_id = handle.document_id().clone();

        let temp_dir = tempfile::tempdir()?;
        let blobs = crate::blobs::BlobsRepo::new(temp_dir.path().to_path_buf()).await?;

        let (repo, _repo_stop) =
            PlugsRepo::load(acx.clone(), blobs, doc_id.clone(), local_actor_id).await?;
        Ok((acx, repo, doc_id, temp_dir))
    }

    fn mock_plug(name: &str) -> manifest::PlugManifest {
        manifest::PlugManifest {
            namespace: "test".into(),
            name: name.into(),
            version: "0.1.0".parse().unwrap(),
            title: format!("Test Plug {}", name),
            desc: "A test plug".into(),
            facets: vec![],
            local_states: default(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            processors: default(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_add_success() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;
        let plug = mock_plug("plug1");

        repo.add(plug).await?;

        let saved = repo.get("@test/plug1").await.unwrap();
        assert_eq!(saved.name, "plug1");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_tag_clash() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add first plug with a tag
        let mut p1 = mock_plug("plug1");
        p1.facets.push(manifest::FacetManifest {
            key_tag: "org.test.tag".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
            references: default(),
        });
        repo.add(p1).await?;

        // Try to add second plug with same tag
        let mut p2 = mock_plug("plug2");
        p2.facets.push(manifest::FacetManifest {
            key_tag: "org.test.tag".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
            references: default(),
        });

        let res = repo.add(p2).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Tag clash"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_dependency_resolution() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add provider plug
        let mut provider = mock_plug("provider");
        provider.facets.push(manifest::FacetManifest {
            key_tag: "org.test.shared".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
            references: default(),
        });
        repo.add(provider).await?;

        // Add consumer plug that depends on provider
        let mut consumer = mock_plug("consumer");
        consumer.dependencies.insert(
            "@test/provider".into(),
            manifest::PlugDependencyManifest {
                keys: vec![manifest::FacetDependencyManifest {
                    key_tag: "org.test.shared".into(),
                    value_schema: schemars::schema_for!(String),
                }],
                local_states: vec![],
            }
            .into(),
        );

        repo.add(consumer).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_missing_dependency() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let mut consumer = mock_plug("consumer");
        consumer.dependencies.insert(
            "@test/missing".into(),
            manifest::PlugDependencyManifest {
                keys: vec![],
                local_states: vec![],
            }
            .into(),
        );

        let res = repo.add(consumer).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Dependency not found"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_version_breaking_change() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Create a temporary file for the component (keep it alive)
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm content").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Initial version
        let mut p1_v1 = mock_plug("plug1");
        p1_v1.version = "0.1.0".parse().unwrap();
        p1_v1.commands.insert(
            "cmd1".into(),
            manifest::CommandManifest {
                desc: "First command".into(),
                deets: manifest::CommandDeets::DocCommand {
                    routine_name: "routine1".into(),
                },
            }
            .into(),
        );
        p1_v1.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
            }
            .into(),
        );
        p1_v1.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );
        repo.add(p1_v1).await?;

        // Update version (patch) with command removed -> should fail
        let mut p1_v2 = mock_plug("plug1");
        p1_v2.version = "0.1.1".parse().unwrap();
        // cmd1 is missing

        let res = repo.add(p1_v2).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Breaking change"));

        // Update version (major) with command removed -> should succeed
        let mut p1_v3 = mock_plug("plug1");
        p1_v3.version = "1.0.0".parse().unwrap(); // major bump from 0.1 to 1.0 (in standard semver terms)

        repo.add(p1_v3).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_version_must_increase() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add initial version
        let mut p1_v1 = mock_plug("plug1");
        p1_v1.version = "0.1.0".parse().unwrap();
        repo.add(p1_v1).await?;

        // Try to add same version -> should fail
        let mut p1_same = mock_plug("plug1");
        p1_same.version = "0.1.0".parse().unwrap();
        let res = repo.add(p1_same).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Version must be greater"));

        // Try to add lower version -> should fail
        let mut p1_lower = mock_plug("plug1");
        p1_lower.version = "0.0.9".parse().unwrap();
        let res = repo.add(p1_lower).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Version must be greater"));

        // Add higher version -> should succeed
        let mut p1_v2 = mock_plug("plug1");
        p1_v2.version = "0.1.1".parse().unwrap();
        repo.add(p1_v2).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_bundle_key_validation() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Create plug with routine referencing non-existent bundle
        let mut plug = mock_plug("plug1");
        plug.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "missing_bundle".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
            }
            .into(),
        );
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url.clone()],
            }
            .into(),
        );

        let res = repo.add(plug).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("wflow bundle 'missing_bundle' not found"));

        // Create plug with routine referencing non-existent key in bundle
        let mut plug2 = mock_plug("plug2");
        plug2.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "missing_key".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
            }
            .into(),
        );
        plug2.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );

        let res = repo.add(plug2).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("key 'missing_key' not found"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_component_url_validation() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Test with non-existent file URL
        let mut plug = mock_plug("plug1");
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec!["file:///nonexistent/path".parse().unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Component file not found"));

        // Test with non-existent blob URL
        let mut plug2 = mock_plug("plug2");
        plug2.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec![format!("{}:///nonexistent_hash", crate::blobs::BLOB_SCHEME)
                    .parse()
                    .unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug2).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Blob not found"));

        // Test with unsupported scheme
        let mut plug3 = mock_plug("plug3");
        plug3.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec!["http://example.com/wasm.wasm".parse().unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug3).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Unsupported URL scheme"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_reference_json_path_must_exist_in_schema() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let mut plug = mock_plug("ref-path");
        plug.facets.push(manifest::FacetManifest {
            key_tag: "org.test.image".into(),
            value_schema: schemars::schema_for!(daybook_types::doc::ImageMetadata),
            display_config: default(),
            references: vec![manifest::FacetReferenceManifest {
                reference_kind: manifest::FacetReferenceKind::UrlFacet,
                json_path: "/doesNotExist".into(),
                at_commit_json_path: Some("/refHeads".into()),
            }],
        });

        let result = repo.add(plug).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("path does not exist in schema"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_at_commit_json_path_type_must_be_array_of_strings() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let mut plug = mock_plug("bad-at-commit");
        plug.facets.push(manifest::FacetManifest {
            key_tag: "org.test.image".into(),
            value_schema: schemars::schema_for!(daybook_types::doc::ImageMetadata),
            display_config: default(),
            references: vec![manifest::FacetReferenceManifest {
                reference_kind: manifest::FacetReferenceKind::UrlFacet,
                json_path: "/facetRef".into(),
                at_commit_json_path: Some("/mime".into()),
            }],
        });

        let result = repo.add(plug).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must allow an array of commit hashes"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processor_routine_must_exist() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let mut plug = mock_plug("processor-routine");
        plug.processors.insert(
            "proc1".into(),
            manifest::ProcessorManifest {
                desc: "Processor".into(),
                deets: manifest::ProcessorDeets::DocProcessor {
                    predicate: manifest::DocPredicateClause::HasTag("org.test.tag".into()),
                    routine_name: "missing-routine".into(),
                },
            }
            .into(),
        );

        let result = repo.add(plug).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid processor deets"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processor_predicate_tags_must_be_in_scope() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        let mut plug = mock_plug("processor-predicate-scope");
        plug.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
            }
            .into(),
        );
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );
        plug.processors.insert(
            "proc1".into(),
            manifest::ProcessorManifest {
                desc: "Processor".into(),
                deets: manifest::ProcessorDeets::DocProcessor {
                    predicate: manifest::DocPredicateClause::HasTag("org.test.missing".into()),
                    routine_name: "routine1".into(),
                },
            }
            .into(),
        );

        let result = repo.add(plug).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid processor predicate"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_file_to_blob_conversion() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Create a temporary file with wasm content (keep it alive)
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        let wasm_content = b"fake wasm binary content";
        tokio::fs::write(&temp_path, wasm_content).await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Create plug with file:// URL
        let mut plug = mock_plug("plug1");
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec![file_url],
            }
            .into(),
        );

        // Add plug - should convert file:// to db+blob://
        repo.add(plug.clone()).await?;

        // Retrieve the plug and verify URL was converted
        let saved = repo.get("@test/plug1").await.unwrap();
        let bundle = saved.wflow_bundles.get("bundle1").unwrap();
        assert_eq!(bundle.component_urls.len(), 1);
        let converted_url = &bundle.component_urls[0];
        assert_eq!(converted_url.scheme(), crate::blobs::BLOB_SCHEME);

        // Verify the blob exists and contains the correct content
        let hash = converted_url.path().trim_start_matches('/');
        let blob_path = repo.blobs.get_path(hash).await?;
        let blob_content = tokio::fs::read(&blob_path).await?;
        assert_eq!(blob_content, wasm_content);

        Ok(())
    }
}
