use crate::interlude::*;
use tokio_util::sync::CancellationToken;

use daybook_types::manifest;

pub fn system_plugs() -> Vec<manifest::PlugManifest> {
    use daybook_types::doc::*;
    use manifest::*;

    let plugs = vec![
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
            inits: default(),
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
                    key_tag: WellKnownFacetTag::Embedding.into(),
                    value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
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
                            FacetDependencyManifest {
                                key_tag: WellKnownFacetTag::Embedding.into(),
                                value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
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
                                    owner_plug_id: None,
                                    tag: WellKnownFacetTag::Blob.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    owner_plug_id: None,
                                    tag: WellKnownFacetTag::Note.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_facet_acl: vec![],
                        },
                        local_state_acl: vec![],
                        command_invoke_acl: vec![],
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
                                    write: true,
                                },
                            ],
                            config_facet_acl: vec![],
                        },
                        local_state_acl: vec![],
                        command_invoke_acl: vec![],
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
                                    owner_plug_id: None,
                                    tag: WellKnownFacetTag::Note.into(),
                                    key_id: None,
                                    read: true,
                                    write: false,
                                },
                                RoutineFacetAccess {
                                    owner_plug_id: None,
                                    tag: WellKnownFacetTag::Embedding.into(),
                                    key_id: None,
                                    read: true,
                                    write: true,
                                },
                            ],
                            config_facet_acl: vec![],
                        },
                        local_state_acl: vec![],
                        command_invoke_acl: vec![],
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
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Embedding.into(),
                                key_id: None,
                                read: true,
                                write: false,
                            }],
                            config_facet_acl: vec![],
                        },
                        local_state_acl: vec![RoutineLocalStateAccess {
                            plug_id: "@daybook/wip".into(),
                            local_state_key: "doc-embedding-index".into(),
                        }],
                        command_invoke_acl: vec![],
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
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                            }],
                            config_facet_acl: vec![],
                        },
                        local_state_acl: vec![],
                        command_invoke_acl: vec![],
                    }
                    .into(),
                ),
            ]
            .into(),
            commands: [
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
                    "index-embedding".into(),
                    CommandManifest {
                        desc: "Index embedding facet into local vector store".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "index-embedding".into(),
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
            inits: default(),
            processors: [
                (
                    "ocr-image".into(),
                    ProcessorManifest {
                        desc: "Extract OCR text from blob image into note".into(),
                        deets: ProcessorDeets::DocProcessor {
                            event_predicate: default(),
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
                    }
                    .into(),
                ),
                (
                    "embed-text".into(),
                    ProcessorManifest {
                        desc: "Compute embedding facet from note content".into(),
                        deets: ProcessorDeets::DocProcessor {
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
                    }
                    .into(),
                ),
                (
                    "index-embedding".into(),
                    ProcessorManifest {
                        desc: "Index embedding facets into local sqlite vec store".into(),
                        deets: ProcessorDeets::DocProcessor {
                            event_predicate: default(),
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
                            "test-label".into(),
                            "ocr-image".into(),
                            "embed-image".into(),
                            "embed-text".into(),
                            "index-embedding".into(),
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

                                format!("file://{path}", path = path.to_string())
                                    .parse()
                                    .unwrap()
                            }*/
                        ],
                    }
                    .into(),
                ),
            ]
            .into(),
            facets: vec![],
        },
    ];

    plugs
}

#[derive(Reconcile, Hydrate)]
pub struct PlugsStore {
    pub manifests: HashMap<String, Versioned<ThroughJson<Arc<manifest::PlugManifest>>>>,
    pub manifests_deleted: HashMap<String, Vec<VersionTag>>,
    pub plug_config_doc_ids: Versioned<ThroughJson<HashMap<String, String>>>,

    /// Index: property tag -> plug id (@ns/name)
    #[autosurgeon(with = "am_utils_rs::codecs::skip")]
    pub tag_to_plug: HashMap<String, String>,
    /// Index: property tag -> facet manifest
    #[autosurgeon(with = "am_utils_rs::codecs::skip")]
    pub facet_manifests: HashMap<String, manifest::FacetManifest>,
}

impl Default for PlugsStore {
    fn default() -> Self {
        Self {
            manifests: default(),
            manifests_deleted: default(),
            plug_config_doc_ids: Versioned {
                vtag: VersionTag::nil(),
                val: ThroughJson(default()),
            },
            tag_to_plug: default(),
            facet_manifests: default(),
        }
    }
}

impl PlugsStore {
    pub fn rebuild_indices(&mut self) {
        self.tag_to_plug.clear();
        self.facet_manifests.clear();

        for (plug_id, versioned) in &self.manifests {
            for facet in &versioned.facets {
                self.tag_to_plug
                    .insert(facet.key_tag.to_string(), plug_id.clone());
                self.facet_manifests
                    .insert(facet.key_tag.to_string(), facet.clone());
            }
        }
    }
}

#[async_trait]
impl crate::stores::AmStore for PlugsStore {
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
    pub registry: Arc<crate::repos::ListenersRegistry>,
    big_repo: SharedBigRepo,
    app_doc_id: DocumentId,
    app_am_handle: am_utils_rs::repo::BigDocHandle,
    store: crate::stores::AmStoreHandle<PlugsStore>,
    blobs: Arc<crate::blobs::BlobsRepo>,
    mutation_mutex: tokio::sync::Mutex<()>,
    plug_config_doc_init_lock: tokio::sync::Mutex<()>,
    local_actor_id: ActorId,
    local_peer_id: am_utils_rs::repo::PeerId,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<am_utils_rs::repo::BigRepoChangeListenerRegistration>,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PlugsEvent {
    // ListChanged { heads: ChangeHashSet },
    PlugAdded {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    PlugChanged {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    PlugDeleted {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    ConfigDocsChanged {
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
}

pub const OCI_PLUG_ARTIFACT_TYPE: &str = "application/vnd.daybook.plug.v1";
pub const OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE: &str =
    "application/vnd.daybook.plug.manifest.v1+json";

fn parse_dep_base_id(dep_id: &str) -> Res<String> {
    if dep_id.starts_with('@') {
        let without_prefix = dep_id
            .strip_prefix('@')
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        let base = without_prefix
            .split('@')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        Ok(format!("@{base}"))
    } else {
        let base = dep_id
            .split('@')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        Ok(base.to_string())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OciImportOptions {
    pub strict: bool,
}

impl Default for OciImportOptions {
    fn default() -> Self {
        Self { strict: true }
    }
}

#[derive(Debug, Clone)]
pub struct ImportedPlug {
    pub plug_id: String,
    pub version: semver::Version,
    pub imported_blob_hashes: Vec<String>,
    pub source_digest: Option<String>,
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
    fn local_origin(&self) -> crate::event_origin::SwitchEventOrigin {
        crate::event_origin::SwitchEventOrigin::Local {
            actor_id: self.local_actor_id.to_string(),
        }
    }

    pub async fn load(
        big_repo: SharedBigRepo,
        blobs: Arc<crate::blobs::BlobsRepo>,
        app_doc_id: DocumentId,
        local_user_path: daybook_types::doc::UserPath,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(&local_user_path, "plugs-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let registry = crate::repos::ListenersRegistry::new();

        let store_val = PlugsStore::load(&big_repo, &app_doc_id).await?;
        let store = crate::stores::AmStoreHandle::new(
            store_val,
            Arc::clone(&big_repo),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        store.mutate_sync(|store| store.rebuild_indices()).await?;

        let app_am_handle = big_repo
            .find_doc_handle(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;

        let cancel_token = CancellationToken::new();
        let (ticket, notif_rx) =
            PlugsStore::register_change_listener(&big_repo, &app_doc_id, vec![]).await?;

        let repo = Self {
            big_repo: Arc::clone(&big_repo),
            app_doc_id: app_doc_id.clone(),
            app_am_handle,
            store,
            blobs,
            local_actor_id,
            local_peer_id: big_repo.local_peer_id(),
            registry: Arc::clone(&registry),
            mutation_mutex: tokio::sync::Mutex::new(()),
            plug_config_doc_init_lock: tokio::sync::Mutex::new(()),
            cancel_token: cancel_token.clone(),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.child_token();
            async move {
                repo.notifs_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    pub async fn get_plugs_heads(&self) -> ChangeHashSet {
        self.app_am_handle
            .with_document(|am_doc| ChangeHashSet(am_doc.get_heads().into()))
            .await
            .expect("with_document read should not fail")
    }

    async fn latest_manifest_delete_actor(
        &self,
        plug_id: &str,
        heads: &Arc<[automerge::ChangeHash]>,
    ) -> Res<Option<ActorId>> {
        let Some((tags, _)) = self
            .big_repo
            .hydrate_path_at_heads::<Vec<VersionTag>>(
                &self.app_doc_id,
                heads,
                automerge::ROOT,
                vec![
                    PlugsStore::prop().into(),
                    "manifests_deleted".into(),
                    autosurgeon::Prop::Key(plug_id.to_string().into()),
                ],
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(tags.last().map(|tag| tag.actor_id.clone()))
    }

    async fn notifs_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<am_utils_rs::repo::BigRepoChangeNotification>,
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
                let am_utils_rs::repo::BigRepoChangeNotification::DocChanged {
                    patch,
                    heads,
                    origin,
                    ..
                } = notif
                else {
                    continue;
                };
                // 3. Call events_for_patch (pure-ish).
                self.events_for_patch(
                    &patch,
                    &heads,
                    &mut events,
                    Some(&origin),
                    Some(&self.local_peer_id),
                )
                .await?;
            }

            let mut delivered_events = Vec::with_capacity(events.len());
            for event in events.drain(..) {
                let is_added = matches!(event, PlugsEvent::PlugAdded { .. });
                match event {
                    PlugsEvent::PlugAdded { id, heads, origin }
                    | PlugsEvent::PlugChanged { id, heads, origin } => {
                        let Some((new_versioned, _)) = self
                            .big_repo
                            .hydrate_path_at_heads::<Versioned<ThroughJson<Arc<manifest::PlugManifest>>>>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![
                                    "manifests".into(),
                                    autosurgeon::Prop::Key(id.clone().into()),
                                ],
                            )
                            .await?
                        else {
                            warn!(plug_id = id, "ignoring stale plug patch: entry missing at heads");
                            continue;
                        };
                        let prev_hashes = match self
                            .store
                            .query_sync(|store| {
                                store
                                    .manifests
                                    .get(&id)
                                    .map(|versioned| Arc::clone(&versioned.val))
                            })
                            .await
                            .map(|manifest| Self::blob_hashes_for_manifest(manifest.as_ref()))
                            .transpose()
                        {
                            Ok(value) => value.unwrap_or_default(),
                            Err(err) => {
                                warn!(
                                    plug_id = id,
                                    ?err,
                                    "failed reading previous plug blob hashes; skipping event"
                                );
                                continue;
                            }
                        };
                        let next_hashes =
                            match Self::blob_hashes_for_manifest(new_versioned.val.as_ref()) {
                                Ok(value) => value,
                                Err(err) => {
                                    warn!(
                                        plug_id = id,
                                        ?err,
                                        "failed reading next plug blob hashes; skipping event"
                                    );
                                    continue;
                                }
                            };
                        if let Err(err) = self
                            .publish_plug_scope_diff_for_manifest_change(
                                &id,
                                &prev_hashes,
                                &next_hashes,
                            )
                            .await
                        {
                            warn!(
                                plug_id = id,
                                ?err,
                                "failed publishing plug scope hash diff; skipping event"
                            );
                            continue;
                        }

                        self.store
                            .mutate_sync(|store| {
                                store.manifests.insert(id.clone(), new_versioned);
                                store.rebuild_indices();
                            })
                            .await?;
                        delivered_events.push(if is_added {
                            PlugsEvent::PlugAdded { id, heads, origin }
                        } else {
                            PlugsEvent::PlugChanged { id, heads, origin }
                        });
                    }
                    PlugsEvent::PlugDeleted { id, heads, origin } => {
                        let removed_manifest = self
                            .store
                            .query_sync(|store| {
                                store.manifests.get(&id).map(|value| Arc::clone(&value.val))
                            })
                            .await;
                        if let Some(removed) = removed_manifest {
                            let removed_hashes =
                                match Self::blob_hashes_for_manifest(removed.as_ref()) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        warn!(
                                        plug_id = id,
                                        ?err,
                                        "failed reading removed plug blob hashes; skipping event"
                                    );
                                        continue;
                                    }
                                };
                            if let Err(err) = self
                                .publish_plug_scope_diff_for_manifest_change(
                                    &id,
                                    &removed_hashes,
                                    &HashSet::new(),
                                )
                                .await
                            {
                                warn!(plug_id = id, ?err, "failed publishing removed plug scope hash diff; skipping event");
                                continue;
                            }
                            self.store
                                .mutate_sync(|store| {
                                    store.manifests.remove(&id);
                                    store.rebuild_indices();
                                })
                                .await?;
                            delivered_events.push(PlugsEvent::PlugDeleted { id, heads, origin });
                        }
                    }
                    PlugsEvent::ConfigDocsChanged { heads, origin } => {
                        let Some((new_versioned, _)) = self
                            .big_repo
                            .hydrate_path_at_heads::<Versioned<ThroughJson<HashMap<String, String>>>>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![PlugsStore::prop().into(), "plug_config_doc_ids".into()],
                            )
                            .await?
                        else {
                            warn!("ignoring stale config-docs patch: value missing at heads");
                            continue;
                        };
                        self.store
                            .mutate_sync(|store| {
                                store.plug_config_doc_ids = new_versioned;
                            })
                            .await?;
                        delivered_events.push(PlugsEvent::ConfigDocsChanged { heads, origin });
                    }
                }
            }
            self.registry.notify(delivered_events.drain(..));
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<PlugsEvent>> {
        let (patches, heads) = self
            .app_am_handle
            .with_document(|am_doc| {
                let heads = if let Some(ref to_set) = to {
                    to_set.clone()
                } else {
                    ChangeHashSet(am_doc.get_heads().into())
                };
                let patches = am_doc
                    .diff_obj(&automerge::ROOT, &from, &heads, true)
                    .wrap_err("diff_obj failed")?;
                eyre::Ok((patches, heads))
            })
            .await??;
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            // Replay path: do not apply live-origin filtering.
            self.events_for_patch(&patch, &heads, &mut events, None, None)
                .await?;
        }
        Ok(events)
    }

    pub async fn events_for_init(&self) -> Res<Vec<PlugsEvent>> {
        // Init snapshot is synthesized from current local store state.
        let heads = self.get_plugs_heads().await;
        let plug_ids = self
            .store
            .query_sync(|store| store.manifests.keys().cloned().collect::<Vec<_>>())
            .await;
        let mut events = Vec::with_capacity(plug_ids.len());
        for id in plug_ids {
            events.push(PlugsEvent::PlugAdded {
                id,
                heads: heads.clone(),
                origin: self.local_origin(),
            });
        }
        Ok(events)
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<PlugsEvent>,
        live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
        exclude_peer_id: Option<&am_utils_rs::repo::PeerId>,
    ) -> Res<()> {
        let is_config_docs_vtag_patch = matches!(
            &patch.action,
            automerge::PatchAction::PutMap {
                key,
                value: (automerge::Value::Scalar(scalar), _),
                ..
            } if patch.path.len() == 2
                && patch.path[1].1 == automerge::Prop::Map("plug_config_doc_ids".into())
                && key == "vtag"
                && matches!(&**scalar, automerge::ScalarValue::Bytes(_))
        );
        // Live notification path: local writes are emitted by mutators.
        // Replay/diff paths pass `live_origin = None`.
        if crate::repos::should_skip_live_patch(live_origin, exclude_peer_id)
            && !is_config_docs_vtag_patch
        {
            return Ok(());
        }
        let heads = ChangeHashSet(Arc::clone(patch_heads));
        match &patch.action {
            automerge::PatchAction::PutMap {
                key,
                value: (val, _),
                ..
            } if patch.path.len() == 3
                && patch.path[1].1 == automerge::Prop::Map("manifests".into()) =>
            {
                if key == "vtag" {
                    let Some((_obj, automerge::Prop::Map(plug_id))) = patch.path.get(2) else {
                        return Ok(());
                    };

                    let vtag_bytes = match val {
                        automerge::Value::Scalar(scalar) => match &**scalar {
                            automerge::ScalarValue::Bytes(bytes) => bytes,
                            _ => return Ok(()),
                        },
                        _ => return Ok(()),
                    };
                    let vtag = VersionTag::hydrate_bytes(vtag_bytes)?;
                    let event_origin = crate::repos::resolve_origin_from_vtag_actor(
                        &self.local_actor_id,
                        &vtag.actor_id,
                        live_origin,
                    );
                    if vtag.version.is_nil() {
                        out.push(PlugsEvent::PlugAdded {
                            id: plug_id.clone(),
                            heads: heads.clone(),
                            origin: event_origin.clone(),
                        });
                    } else {
                        out.push(PlugsEvent::PlugChanged {
                            id: plug_id.clone(),
                            heads: heads.clone(),
                            origin: event_origin.clone(),
                        });
                    }
                }
            }
            automerge::PatchAction::DeleteMap { key }
                if patch.path.len() == 2
                    && patch.path[1].1 == automerge::Prop::Map("manifests".into()) =>
            {
                // Delete patches have no vtag; use delete tombstones at these heads when replaying.
                let tombstone_actor_id =
                    self.latest_manifest_delete_actor(key, patch_heads).await?;
                let event_origin = crate::repos::resolve_origin_for_delete(
                    &self.local_actor_id,
                    live_origin,
                    tombstone_actor_id.as_ref(),
                );
                out.push(PlugsEvent::PlugDeleted {
                    id: key.clone(),
                    heads,
                    origin: event_origin,
                });
            }
            automerge::PatchAction::PutMap {
                key,
                value: (automerge::Value::Scalar(scalar), _),
                ..
            } if patch.path.len() == 2
                && patch.path[1].1 == automerge::Prop::Map("plug_config_doc_ids".into())
                && key == "vtag"
                && matches!(&**scalar, automerge::ScalarValue::Bytes(_)) =>
            {
                let automerge::ScalarValue::Bytes(vtag_bytes) = &**scalar else {
                    unreachable!("guard above ensures bytes")
                };
                let vtag = VersionTag::hydrate_bytes(vtag_bytes)?;
                let event_origin = crate::repos::resolve_origin_from_vtag_actor(
                    &self.local_actor_id,
                    &vtag.actor_id,
                    live_origin,
                );
                out.push(PlugsEvent::ConfigDocsChanged {
                    heads,
                    origin: event_origin,
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
            .query_sync(|store| store.manifests.get(id).map(|man| Arc::clone(&man.val)))
            .await
    }

    pub async fn get_plug_config_doc_id(&self, plug_id: &str) -> Option<String> {
        let plug_id = plug_id.to_string();
        self.store
            .query_sync(move |store| store.plug_config_doc_ids.val.0.get(&plug_id).cloned())
            .await
    }

    pub async fn set_plug_config_doc_id(&self, plug_id: &str, doc_id: String) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let plug_id = plug_id.to_string();
        self.store
            .mutate_sync(move |store| {
                let mut config_doc_ids = store.plug_config_doc_ids.val.0.clone();
                config_doc_ids.insert(plug_id, doc_id);
                store
                    .plug_config_doc_ids
                    .replace(self.local_actor_id.clone(), ThroughJson(config_doc_ids));
            })
            .await?;
        Ok(())
    }

    pub async fn get_or_init_plug_config_doc_id(
        &self,
        plug_id: &str,
        drawer_repo: &crate::drawer::DrawerRepo,
    ) -> Res<String> {
        if let Some(doc_id) = self.get_plug_config_doc_id(plug_id).await {
            return Ok(doc_id);
        }
        let _guard = self.plug_config_doc_init_lock.lock().await;
        if let Some(doc_id) = self.get_plug_config_doc_id(plug_id).await {
            return Ok(doc_id);
        }
        let doc_id = drawer_repo
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: HashMap::new(),
                user_path: None,
            })
            .await?;
        self.set_plug_config_doc_id(plug_id, doc_id.clone()).await?;
        Ok(doc_id)
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
                            .val
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
                    .map(|man| Arc::clone(&man.val))
                    .collect()
            })
            .await
    }

    pub async fn import_from_oci_layout(
        &self,
        layout_root: &std::path::Path,
        opts: OciImportOptions,
    ) -> Res<ImportedPlug> {
        let _oci_layout = oci_spec::image::OciLayout::from_file(layout_root.join("oci-layout"))?;
        let index = oci_spec::image::ImageIndex::from_file(layout_root.join("index.json"))?;
        let selected_manifest_descriptor = index
            .manifests()
            .first()
            .cloned()
            .ok_or_eyre("oci index has no manifests")?;
        let selected_manifest_sha = selected_manifest_descriptor
            .as_digest_sha256()
            .ok_or_eyre("oci index manifest descriptor must use sha256 digest")?
            .to_string();
        let manifest_bytes = Self::read_oci_layout_blob_by_sha(layout_root, &selected_manifest_sha)
            .await
            .wrap_err("error reading selected OCI manifest blob from layout")?;
        let oci_manifest: oci_client::manifest::OciManifest =
            serde_json::from_slice(&manifest_bytes)?;

        let image_manifest = match oci_manifest {
            oci_client::manifest::OciManifest::Image(manifest) => manifest,
            oci_client::manifest::OciManifest::ImageIndex(index_manifest) => {
                let nested_descriptor = index_manifest
                    .manifests
                    .first()
                    .ok_or_eyre("nested OCI image index has no manifests")?;
                let nested_sha = Self::sha256_hex_from_digest_str(&nested_descriptor.digest)?;
                let nested_bytes = Self::read_oci_layout_blob_by_sha(layout_root, &nested_sha)
                    .await
                    .wrap_err("error reading nested OCI manifest blob from layout")?;
                match serde_json::from_slice::<oci_client::manifest::OciManifest>(&nested_bytes)? {
                    oci_client::manifest::OciManifest::Image(manifest) => manifest,
                    oci_client::manifest::OciManifest::ImageIndex(_) => {
                        eyre::bail!("nested OCI manifest must resolve to an image manifest")
                    }
                }
            }
        };

        self.import_from_oci_image_manifest(
            image_manifest,
            Some(selected_manifest_sha),
            opts,
            |digest| async move {
                let sha = Self::sha256_hex_from_digest_str(&digest)?;
                Self::read_oci_layout_blob_by_sha(layout_root, &sha).await
            },
        )
        .await
    }

    pub async fn import_from_oci_registry(
        &self,
        reference: &str,
        auth: oci_client::secrets::RegistryAuth,
        opts: OciImportOptions,
    ) -> Res<ImportedPlug> {
        let reference: oci_client::Reference = reference.parse()?;
        let client_config = oci_client::client::ClientConfig {
            connect_timeout: Some(std::time::Duration::from_secs(15)),
            read_timeout: Some(std::time::Duration::from_secs(300)),
            ..Default::default()
        };
        let client = oci_client::Client::new(client_config);
        let (manifest, source_digest) = client.pull_manifest(&reference, &auth).await?;
        let (target_manifest, target_ref) = match manifest {
            oci_client::manifest::OciManifest::Image(manifest) => (manifest, reference.clone()),
            oci_client::manifest::OciManifest::ImageIndex(index_manifest) => {
                let desc = index_manifest
                    .manifests
                    .first()
                    .ok_or_eyre("oci image index has no manifests")?;
                let target_ref = reference.clone_with_digest(desc.digest.clone());
                let (nested, _) = client.pull_manifest(&target_ref, &auth).await?;
                let oci_client::manifest::OciManifest::Image(manifest) = nested else {
                    eyre::bail!("nested OCI manifest must resolve to an image manifest");
                };
                (manifest, target_ref)
            }
        };

        self.import_from_oci_image_manifest(target_manifest, Some(source_digest), opts, |digest| {
            let client = &client;
            let target_ref = &target_ref;
            async move {
                let mut out = Vec::new();
                client
                    .pull_blob(target_ref, digest.as_str(), &mut out)
                    .await?;
                eyre::Ok(out)
            }
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
                            let wasm_zst_bytes: &[u8] = match url.path() {
                                "daybook_wflows.wasm.zst" => include_bytes!(concat!(
                                    env!("OUT_DIR"),
                                    "/daybook_wflows.wasm.zst"
                                ))
                                .as_slice(),
                                _ => {
                                    eyre::bail!("unsupported static wasm component_url");
                                }
                            };
                            let data = tokio::task::spawn_blocking(move || {
                                let mut wasm_bytes = vec![];
                                zstd::stream::copy_decode(wasm_zst_bytes, &mut wasm_bytes)
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
        let prev_hashes = self
            .get(&plug_id)
            .await
            .map(|old| Self::blob_hashes_for_manifest(old.as_ref()))
            .transpose()?
            .unwrap_or_default();
        let next_hashes = Self::blob_hashes_for_manifest(&manifest)?;
        self.publish_plug_scope_diff_for_manifest_change(&plug_id, &prev_hashes, &next_hashes)
            .await?;

        let ((plug_id, is_update), hash) = self
            .store
            .mutate_sync(move |store| {
                let manifest = manifest;
                let is_update = store.manifests.contains_key(&plug_id);

                let versioned = Versioned {
                    vtag: VersionTag {
                        actor_id: self.local_actor_id.clone(),
                        version: if is_update {
                            Uuid::new_v4()
                        } else {
                            Uuid::nil()
                        },
                    },
                    val: Arc::new(manifest).into(),
                };

                // Update the manifest in the store
                store.manifests.insert(plug_id.clone(), versioned);

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
            PlugsEvent::PlugChanged {
                id: plug_id,
                heads,
                origin: self.local_origin(),
            }
        } else {
            PlugsEvent::PlugAdded {
                id: plug_id,
                heads,
                origin: self.local_origin(),
            }
        }]);

        Ok(())
    }

    async fn import_from_oci_image_manifest<F, Fut>(
        &self,
        image_manifest: oci_client::manifest::OciImageManifest,
        source_digest: Option<String>,
        opts: OciImportOptions,
        mut pull_blob_by_digest: F,
    ) -> Res<ImportedPlug>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Res<Vec<u8>>>,
    {
        let mut manifest_layer: Option<Vec<u8>> = None;
        let mut oci_digest_to_repo_hash: HashMap<String, String> = HashMap::new();
        let mut imported_blob_hashes = vec![];

        for layer in &image_manifest.layers {
            let layer_bytes = pull_blob_by_digest(layer.digest.clone())
                .await
                .wrap_err_with(|| format!("error pulling OCI layer blob '{}'", layer.digest))?;
            if opts.strict {
                Self::validate_sha256_digest(&layer.digest, &layer_bytes)?;
            }
            let repo_hash = self.blobs.put(&layer_bytes).await?;
            oci_digest_to_repo_hash.insert(layer.digest.clone(), repo_hash.clone());
            imported_blob_hashes.push(repo_hash);
            if layer.media_type == OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE {
                if manifest_layer.is_some() {
                    eyre::bail!(
                        "OCI artifact contains multiple '{}' layers",
                        OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE
                    );
                }
                manifest_layer = Some(layer_bytes);
            }
        }

        let manifest_layer = manifest_layer.ok_or_eyre(format!(
            "missing required '{}' layer",
            OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE
        ))?;

        let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_layer)
            .wrap_err("error parsing plug manifest layer JSON")?;
        let rewritten_manifest_json =
            Self::rewrite_oci_component_urls(manifest_json, &oci_digest_to_repo_hash)?;
        let plug_manifest: manifest::PlugManifest = serde_json::from_value(rewritten_manifest_json)
            .wrap_err("error parsing rewritten plug manifest JSON into PlugManifest")?;
        let plug_id = plug_manifest.id();
        let plug_version = plug_manifest.version.clone();

        self.add(plug_manifest).await?;

        Ok(ImportedPlug {
            plug_id,
            version: plug_version,
            imported_blob_hashes,
            source_digest,
        })
    }

    fn rewrite_oci_component_urls(
        mut manifest_json: serde_json::Value,
        oci_digest_to_repo_hash: &HashMap<String, String>,
    ) -> Res<serde_json::Value> {
        let bundles = manifest_json
            .get_mut("wflowBundles")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_eyre("plug manifest JSON missing object at 'wflowBundles'")?;

        for bundle in bundles.values_mut() {
            let component_urls = bundle
                .get_mut("componentUrls")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or_eyre("plug manifest JSON bundle missing array at 'componentUrls'")?;

            for url_value in component_urls.iter_mut() {
                let Some(url_str) = url_value.as_str() else {
                    eyre::bail!("componentUrls entries must be strings");
                };
                if !url_str.starts_with("oci://sha256:") {
                    eyre::bail!("componentUrls entries must be OCI digests: '{url_str}'");
                }
                let digest_hex = url_str.trim_start_matches("oci://sha256:");
                if digest_hex.is_empty() {
                    eyre::bail!("empty digest in OCI URL '{url_str}'");
                }
                let digest_key = format!("sha256:{digest_hex}");
                let Some(repo_hash) = oci_digest_to_repo_hash.get(&digest_key) else {
                    eyre::bail!(
                        "OCI URL '{url_str}' references missing layer digest '{digest_key}'"
                    );
                };
                *url_value = serde_json::Value::String(format!(
                    "{}:///{repo_hash}",
                    crate::blobs::BLOB_SCHEME
                ));
            }
        }

        Ok(manifest_json)
    }

    fn sha256_hex_from_digest_str(digest: &str) -> Res<String> {
        let Some((algo, hex)) = digest.split_once(':') else {
            eyre::bail!("invalid OCI digest '{digest}'");
        };
        eyre::ensure!(
            algo == "sha256",
            "unsupported OCI digest algorithm '{algo}'"
        );
        eyre::ensure!(!hex.is_empty(), "empty OCI digest hex");
        Ok(hex.to_string())
    }

    fn validate_sha256_digest(digest: &str, bytes: &[u8]) -> Res<()> {
        use sha2::{Digest as _, Sha256};
        let expected_hex = Self::sha256_hex_from_digest_str(digest)?;
        let actual_hex = format!("{:x}", Sha256::digest(bytes));
        eyre::ensure!(
            expected_hex.eq_ignore_ascii_case(&actual_hex),
            "OCI blob digest mismatch for '{digest}'"
        );
        Ok(())
    }

    async fn read_oci_layout_blob_by_sha(
        layout_root: &std::path::Path,
        sha_hex: &str,
    ) -> Res<Vec<u8>> {
        let path = layout_root.join("blobs").join("sha256").join(sha_hex);
        tokio::fs::read(&path)
            .await
            .wrap_err_with(|| format!("error reading OCI layout blob '{}'", path.display()))
    }

    fn blob_hashes_for_manifest(manifest: &manifest::PlugManifest) -> Res<HashSet<String>> {
        let mut hashes = HashSet::new();
        for bundle in manifest.wflow_bundles.values() {
            for component_url in &bundle.component_urls {
                if component_url.scheme() != crate::blobs::BLOB_SCHEME {
                    continue;
                }
                eyre::ensure!(
                    component_url.host_str().is_none(),
                    "invalid blob URL host in plug manifest: {component_url}"
                );
                let hash = component_url.path().trim_start_matches('/');
                eyre::ensure!(!hash.is_empty(), "empty blob hash in plug manifest URL");
                utils_rs::hash::decode_base58_multibase(hash)?;
                hashes.insert(hash.to_string());
            }
        }
        Ok(hashes)
    }

    async fn publish_plug_scope_diff_for_manifest_change(
        &self,
        plug_id: &str,
        prev_hashes: &HashSet<String>,
        next_hashes: &HashSet<String>,
    ) -> Res<()> {
        for hash in next_hashes.difference(prev_hashes) {
            self.blobs
                .add_hash_to_scope(crate::blobs::BlobScope::Plugs, hash)
                .await?;
        }
        for hash in prev_hashes.difference(next_hashes) {
            if !self
                .is_blob_hash_referenced_by_any_plug_excluding(hash, plug_id)
                .await
            {
                self.blobs
                    .remove_hash_from_scope(crate::blobs::BlobScope::Plugs, hash)
                    .await?;
            }
        }
        Ok(())
    }

    async fn is_blob_hash_referenced_by_any_plug_excluding(
        &self,
        hash: &str,
        excluded_plug_id: &str,
    ) -> bool {
        self.store
            .query_sync(|store| {
                store
                    .manifests
                    .iter()
                    .filter(|(plug_id, _)| plug_id.as_str() != excluded_plug_id)
                    .any(|(_, manifest)| {
                        manifest.val.wflow_bundles.values().any(|bundle| {
                            bundle.component_urls.iter().any(|url| {
                                url.scheme() == crate::blobs::BLOB_SCHEME
                                    && url.host_str().is_none()
                                    && url.path().trim_start_matches('/') == hash
                            })
                        })
                    })
            })
            .await
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
            let dep_base_id = parse_dep_base_id(dep_id_full)?;
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
        for (init_name, init_manifest) in &manifest.inits {
            match &init_manifest.deets {
                manifest::InitDeets::InvokeRoutine { routine_name } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid init deets: routine '{}' not found in plug (init='{}')",
                            routine_name,
                            init_name
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
                    event_predicate: _,
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
        let dependency_base_ids: HashSet<String> = manifest
            .dependencies
            .keys()
            .map(|dep_id_full| parse_dep_base_id(dep_id_full))
            .collect::<Res<HashSet<_>>>()?;
        let mut cached_command_target_manifests: HashMap<String, Arc<manifest::PlugManifest>> =
            HashMap::new();
        let mut available_local_states: HashSet<(String, String)> = manifest
            .local_states
            .keys()
            .map(|key| (plug_id.clone(), key.to_string()))
            .collect();
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = parse_dep_base_id(dep_id_full)?;
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
            for access in routine.config_facet_acl() {
                let owner_plug_id = access.owner_plug_id.as_deref().unwrap_or(&plug_id);
                if owner_plug_id != plug_id && !dependency_base_ids.contains(owner_plug_id) {
                    eyre::bail!(
                        "Invalid config_facet_acl in routine '{}': owner plug '{}' is neither this plug nor a declared dependency",
                        routine_name,
                        owner_plug_id
                    );
                }
                if !available_tags.contains(&access.tag.to_string()) {
                    eyre::bail!(
                        "Invalid config_facet_acl in routine '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                        routine_name,
                        access.tag
                    );
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
            for target_command_url in routine.command_invoke_acl() {
                let parsed_target =
                    daybook_pdk::parse_command_url(target_command_url).map_err(|err| {
                        eyre::eyre!(
                            "Invalid command_invoke_acl in routine '{}': url '{}' is invalid: {}",
                            routine_name,
                            target_command_url,
                            err
                        )
                    })?;
                if parsed_target.plug_id != plug_id
                    && !dependency_base_ids.contains(&parsed_target.plug_id)
                {
                    eyre::bail!(
                        "Invalid command_invoke_acl in routine '{}': target plug '{}' is neither this plug nor a declared dependency",
                        routine_name,
                        parsed_target.plug_id
                    );
                }
                let command_exists = if parsed_target.plug_id == plug_id {
                    manifest
                        .commands
                        .contains_key(parsed_target.command_name.as_str())
                } else {
                    let target_manifest = if let Some(cached) =
                        cached_command_target_manifests.get(&parsed_target.plug_id)
                    {
                        Arc::clone(cached)
                    } else {
                        let loaded = self
                            .get(&parsed_target.plug_id)
                            .await
                            .ok_or_else(|| {
                                ferr!(
                                    "Invalid command_invoke_acl in routine '{}': target plug '{}' not found",
                                    routine_name,
                                    parsed_target.plug_id
                                )
                            })?;
                        cached_command_target_manifests
                            .insert(parsed_target.plug_id.clone(), Arc::clone(&loaded));
                        loaded
                    };
                    target_manifest
                        .commands
                        .contains_key(parsed_target.command_name.as_str())
                };
                if !command_exists {
                    eyre::bail!(
                        "Invalid command_invoke_acl in routine '{}': target command '{}/{}' not found",
                        routine_name,
                        parsed_target.plug_id,
                        parsed_target.command_name
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
                    event_predicate,
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
                    let mut read_tags = HashSet::new();
                    let mut read_keys = HashSet::new();
                    event_predicate
                        .doc_change_predicate
                        .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
                    for referenced_tag in read_tags {
                        if !available_tags.contains(&referenced_tag) {
                            eyre::bail!(
                                "Invalid processor event predicate in '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag
                            );
                        }
                    }
                    for referenced_key in read_keys {
                        let referenced_tag = referenced_key.tag.to_string();
                        if !available_tags.contains(&referenced_tag) {
                            eyre::bail!(
                                "Invalid processor event predicate in '{}': tag '{}' (from key '{}') is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag,
                                referenced_key
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
        let Some(reference_node) = daybook_types::reference::schema_node_for_json_path(
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
                if !daybook_types::reference::schema_allows_url_reference(reference_node) {
                    eyre::bail!(
                        "invalid reference json_path '{}' for facet tag '{}': schema node must allow a URL string or an array of URL strings",
                        reference_manifest.json_path,
                        facet_tag
                    );
                }
            }
        }

        if let Some(at_commit_json_path) = &reference_manifest.at_commit_json_path {
            let Some(at_commit_node) = daybook_types::reference::schema_node_for_json_path(
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
            if !daybook_types::reference::schema_allows_array_of_strings(at_commit_node) {
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
    use crate::repos::{Repo, SubscribeOpts, TryRecvError};

    async fn setup_repo() -> Res<(SharedBigRepo, Arc<PlugsRepo>, DocumentId, tempfile::TempDir)> {
        let local_user_path = daybook_types::doc::UserPath::from("/test-user/test-device");
        let (big_repo, _acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
            peer_id: crate::peer_id_from_label("test"),
            storage: am_utils_rs::repo::StorageConfig::Memory,
        })
        .await?;

        let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
        let handle = big_repo.add_doc(doc).await?;
        let doc_id = handle.document_id().clone();

        let temp_dir = tempfile::tempdir()?;
        let blobs = crate::blobs::BlobsRepo::new(
            temp_dir.path().to_path_buf(),
            "/test-user".to_string(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                big_repo.partition_store(),
            )),
        )
        .await?;

        let (repo, _repo_stop) = PlugsRepo::load(
            Arc::clone(&big_repo),
            blobs,
            doc_id.clone(),
            local_user_path,
        )
        .await?;
        Ok((big_repo, repo, doc_id, temp_dir))
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
            inits: default(),
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
    async fn test_plug_add_emits_single_local_event() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;
        let listener = repo.subscribe(SubscribeOpts::new(16));

        repo.add(mock_plug("plug-single-event")).await?;

        let first: Arc<PlugsEvent> = listener
            .recv_async()
            .await
            .map_err(|err| ferr!("listener recv failed: {err:?}"))?;
        assert!(
            matches!(&*first, PlugsEvent::PlugAdded { id, .. } if id == "@test/plug-single-event"),
            "expected PlugAdded event, got: {first:?}"
        );

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            matches!(listener.try_recv(), Err(TryRecvError::Empty)),
            "expected no duplicate local listener event"
        );
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
                command_invoke_acl: vec![],
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
                command_invoke_acl: vec![],
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
                command_invoke_acl: vec![],
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
                    event_predicate: default(),
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
                command_invoke_acl: vec![],
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
                    event_predicate: default(),
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
    async fn test_command_invoke_acl_rejects_target_without_dependency() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        let mut target = mock_plug("target");
        target.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
                command_invoke_acl: vec![],
            }
            .into(),
        );
        target.commands.insert(
            "cmd1".into(),
            manifest::CommandManifest {
                desc: "target command".into(),
                deets: manifest::CommandDeets::DocCommand {
                    routine_name: "routine1".into(),
                },
            }
            .into(),
        );
        target.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url.clone()],
            }
            .into(),
        );
        repo.add(target).await?;

        let mut caller = mock_plug("caller");
        caller.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
                command_invoke_acl: vec!["db+command:///@test/target/cmd1".parse().unwrap()],
            }
            .into(),
        );
        caller.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url.clone()],
            }
            .into(),
        );

        let result = repo.add(caller).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("declared dependency"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_command_invoke_acl_rejects_missing_command() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        let mut provider = mock_plug("provider");
        provider.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
                command_invoke_acl: vec![],
            }
            .into(),
        );
        provider.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url.clone()],
            }
            .into(),
        );
        repo.add(provider).await?;

        let mut caller = mock_plug("caller");
        caller.dependencies.insert(
            "@test/provider".into(),
            manifest::PlugDependencyManifest {
                keys: vec![],
                local_states: vec![],
            }
            .into(),
        );
        caller.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                local_state_acl: vec![],
                command_invoke_acl: vec!["db+command:///@test/provider/nope".parse().unwrap()],
            }
            .into(),
        );
        caller.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );

        let result = repo.add(caller).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("target command"));
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_blob_scope_partition_tracks_add_and_remove() -> Res<()> {
        let (big_repo, repo, _doc_id, _temp_dir) = setup_repo().await?;
        let partition_id = crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.to_string();

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"scope-membership-bytes").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        let mut plug = mock_plug("scope-membership");
        plug.version = "0.1.0".parse().unwrap();
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec![file_url],
            }
            .into(),
        );
        repo.add(plug.clone()).await?;

        let saved = repo
            .get("@test/scope-membership")
            .await
            .ok_or_eyre("expected saved plug")?;
        let hash = saved
            .wflow_bundles
            .get("bundle1")
            .and_then(|bundle| bundle.component_urls.first())
            .map(|url| url.path().trim_start_matches('/').to_string())
            .ok_or_eyre("expected converted blob URL in bundle1")?;
        assert_eq!(big_repo.partition_member_count(&partition_id).await?, 1);
        assert!(
            big_repo
                .is_member_present_in_partition_item_state(&partition_id, &hash)
                .await?
        );

        let mut plug_update = mock_plug("scope-membership");
        plug_update.version = "0.2.0".parse().unwrap();
        repo.add(plug_update).await?;

        assert_eq!(big_repo.partition_member_count(&partition_id).await?, 0);
        assert!(
            !big_repo
                .is_member_present_in_partition_item_state(&partition_id, &hash)
                .await?
        );
        Ok(())
    }
}
