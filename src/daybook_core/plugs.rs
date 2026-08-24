use crate::interlude::*;
use crate::repos::Repo;
use tokio_util::sync::CancellationToken;

use daybook_types::manifest;

use self::cache::PlugsCache;

pub use self::events::PlugsEvent;
pub(crate) use self::events::{PlugsNotif, PlugsSwitchSink};

mod cache;
mod events;
mod mutations;
mod oci;
mod queries;
mod validation;

#[cfg(test)]
mod tests;

pub fn system_plugs() -> Vec<manifest::PlugManifest> {
    use daybook_types::doc::*;
    use manifest::{
        FacetDisplayDeets, FacetDisplayHint, FacetManifest, FacetReferenceManifest,
        LocalStateManifest,
    };

    let plugs = vec![manifest::PlugManifest {
        namespace: "daybook".into(),
        name: "core".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Daybook Core".into(),
        desc: "Core keys and routines".into(),
        local_states: [
            (
                "doc-facet-set-index".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
            (
                "doc-blobs-index".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
            (
                "doc-facet-ref-index".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
            (
                "doc-blob-pins-index".into(),
                Arc::new(LocalStateManifest::SqliteFile {}),
            ),
        ]
        .into(),
        dependencies: default(),
        views: default(),
        routines: default(),
        wflow_bundles: default(),
        commands: default(),
        inits: default(),
        processors: default(),
        facets: vec![
            FacetManifest {
                key_tag: WellKnownFacetTag::PlugManifest.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::PlugManifestFacet),
                display_config: default(),
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::PlugsConfig.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::PlugsConfig),
                display_config: default(),
                references: default(),
            },
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
                    deets: FacetDisplayDeets::Title { show_editor: true },
                    ..default()
                },
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::PathGeneric.into(),
                value_schema: schemars::schema_for!(String),
                display_config: FacetDisplayHint {
                    display_title: Some("Path".to_string()),
                    deets: FacetDisplayDeets::UnixPath,
                    ..default()
                },
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::ImageMetadata.into(),
                value_schema: schemars::schema_for!(ImageMetadata),
                display_config: default(),
                references: vec![FacetReferenceManifest::UrlStringSplit {
                    json_path: "/facetRef".into(),
                    at_commit_json_path: "/refHeads".into(),
                }],
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::Embedding.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
                display_config: default(),
                references: vec![FacetReferenceManifest::UrlStringSplit {
                    json_path: "/facetRef".into(),
                    at_commit_json_path: "/refHeads".into(),
                }],
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::Note.into(),
                value_schema: schemars::schema_for!(Note),
                display_config: default(),
                references: default(),
            },
            FacetManifest {
                key_tag: "org.example.daybook.note-editor-config".into(),
                value_schema: schemars::schema_for!(daybook_types::doc::NoteEditorConfig),
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
                key_tag: WellKnownFacetTag::BlobPin.into(),
                value_schema: schemars::schema_for!(BlobPin),
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
                references: vec![FacetReferenceManifest::UrlStringMany {
                    json_path: "/order".into(),
                }],
            },
        ],
    }];
    plugs
}

use daybook_types::doc::KnownPlug;

/// ADR 007 §1: the plugg config facet value.
pub type PlugsConfig = daybook_types::doc::PlugsConfig;

impl crate::stores::FacetStore for PlugsConfig {
    fn facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig)
    }

    fn seed() -> Self {
        PlugsConfig {
            enabled: HashMap::new(),
            known_plugs: HashMap::new(),
            plug_config_doc_ids: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FacetManifestLookup {
    Found(manifest::FacetManifest),
    PlugDisabled { plug_id: String },
    UnknownTag,
}

pub struct PlugsRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    big_repo: SharedBigRepo,
    blobs: Arc<crate::blobs::BlobsRepo>,
    doc_config_id: daybook_types::doc::DocId,

    drawer: tokio::sync::OnceCell<Arc<crate::drawer::DrawerRepo>>,
    config_store: tokio::sync::OnceCell<crate::stores::FacetStoreHandle<PlugsConfig>>,

    mutation_mutex: tokio::sync::Mutex<()>,
    local_actor_id: ActorId,
    cancel_token: CancellationToken,
    cache: tokio::sync::Mutex<PlugsCache>,
    notif_tx: tokio::sync::mpsc::UnboundedSender<PlugsNotif>,
}

#[derive(Debug, Clone)]
pub struct ImportedPlug {
    pub plug_id: String,
    pub version: semver::Version,
    /// Doc id of the authored manifest doc (authoring imports only).
    pub doc_id: Option<daybook_types::doc::DocId>,
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
        doc_config_id: DocumentId,
        local_user_path: daybook_types::doc::UserPathBuf,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "plugs-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel();

        let repo = Arc::new(Self {
            big_repo: Arc::clone(&big_repo),
            blobs,
            doc_config_id: daybook_types::doc::DocId::from(doc_config_id.to_string()),
            drawer: tokio::sync::OnceCell::new(),
            config_store: tokio::sync::OnceCell::new(),
            mutation_mutex: tokio::sync::Mutex::new(()),
            local_actor_id,
            registry: Arc::clone(&registry),
            cancel_token: cancel_token.clone(),
            cache: tokio::sync::Mutex::new(PlugsCache::default()),
            notif_tx,
        });

        // ADR 007 §7: the notif loop maintains the derived cache and emits
        // enabled-only events; fed by the switch sink (PlugsSwitchSink).
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.child_token();
            async move {
                repo.notif_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling plugs notifs")
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

    /// ADR 007 §2: the drawer is loaded after the plugs repo (the drawer needs
    /// the plugs repo for facet validation). Attach it here so the plugs repo
    /// can read manifest docs and write the plugg config facet through it.
    pub async fn attach_drawer(&self, drawer: Arc<crate::drawer::DrawerRepo>) -> Res<()> {
        if self.drawer.set(drawer).is_err() {
            eyre::bail!("drawer already attached to plugs repo");
        }
        let store = crate::stores::FacetStoreHandle::load(
            Arc::clone(self.drawer.get().expect("just set")),
            self.doc_config_id.clone(),
            daybook_types::doc::BranchPathBuf::from("main"),
        )
        .await?;
        if self.config_store.set(store).is_err() {
            eyre::bail!("plugs config store already attached");
        }
        Ok(())
    }

    fn config_store(&self) -> Res<&crate::stores::FacetStoreHandle<PlugsConfig>> {
        self.config_store
            .get()
            .ok_or_eyre("plugs config store not attached")
    }

    /// The `FacetStoreSink` that keeps the config store's projection live
    /// (registered in the rt switch sinks map).
    pub fn config_store_sink(&self) -> Option<crate::stores::FacetStoreSink<PlugsConfig>> {
        self.config_store.get().map(|store| store.sink())
    }

    fn plug_config_facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig)
    }

    fn plug_manifest_facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugManifest)
    }

    /// Activate a plug from its enabled ref: materialize the manifest,
    /// update the active cache, and emit the config-delta event — `added`
    /// (the ref was newly enabled) → `PlugEnabled`; otherwise the ref
    /// changed → `EnabledPlugUpdated`. The event type comes from the config
    /// delta, not the cache; the cache is only the materialization side
    /// effect (plus the idempotence fast path). A pending (unreadable) ref
    /// clears the active entry and emits nothing — the config still enables
    /// the plug; it emits `PlugEnabled` when it resolves.
    async fn activate_from_ref(
        &self,
        plug_id: &str,
        ref_url: &url::Url,
        added: bool,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Res<Option<PlugsEvent>> {
        let parsed = Self::parse_enabled_ref(ref_url)?;
        if let Some(at) = &parsed.at {
            let pinned = ChangeHashSet(am_utils_rs::parse_commit_heads(at)?);
            if self.cache.lock().await.is_active_at(plug_id, &pinned) {
                return Ok(None);
            }
        }
        let Some((heads, manifest)) = self.materialize_active(plug_id, ref_url).await? else {
            // Pending: enabled but not readable at pinned heads.
            self.cache.lock().await.clear_active(plug_id);
            return Ok(None);
        };
        self.cache
            .lock()
            .await
            .set_active(plug_id, heads.clone(), manifest);
        Ok(Some(if added {
            PlugsEvent::PlugEnabled {
                id: plug_id.to_string(),
                heads,
                origin: origin.clone(),
            }
        } else {
            PlugsEvent::EnabledPlugUpdated {
                id: plug_id.to_string(),
                heads,
                origin: origin.clone(),
            }
        }))
    }

alue_schema: schemars::schema_for!(String),
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
                    deets: FacetDisplayDeets::Title { show_editor: true },
                    ..default()
                },
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::PathGeneric.into(),
                value_schema: schemars::schema_for!(String),
                display_config: FacetDisplayHint {
                    display_title: Some("Path".to_string()),
                    deets: FacetDisplayDeets::UnixPath,
                    ..default()
                },
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::ImageMetadata.into(),
                value_schema: schemars::schema_for!(ImageMetadata),
                display_config: default(),
                references: vec![FacetReferenceManifest::UrlStringSplit {
                    json_path: "/facetRef".into(),
                    at_commit_json_path: "/refHeads".into(),
                }],
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::Embedding.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::Embedding),
                display_config: default(),
                references: vec![FacetReferenceManifest::UrlStringSplit {
                    json_path: "/facetRef".into(),
                    at_commit_json_path: "/refHeads".into(),
                }],
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::Note.into(),
                value_schema: schemars::schema_for!(Note),
                display_config: default(),
                references: default(),
            },
            FacetManifest {
                key_tag: "org.example.daybook.note-editor-config".into(),
                value_schema: schemars::schema_for!(daybook_types::doc::NoteEditorConfig),
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
                key_tag: WellKnownFacetTag::BlobPin.into(),
                value_schema: schemars::schema_for!(BlobPin),
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
                references: vec![FacetReferenceManifest::UrlStringMany {
                    json_path: "/order".into(),
                }],
            },
        ],
    }];
    plugs
}

use daybook_types::doc::KnownPlug;

/// ADR 007 §1: the plugg config facet value.
pub type PlugsConfig = daybook_types::doc::PlugsConfig;

impl crate::stores::FacetStore for PlugsConfig {
    fn facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig)
    }

    fn seed() -> Self {
        PlugsConfig {
            enabled: HashMap::new(),
            known_plugs: HashMap::new(),
            plug_config_doc_ids: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FacetManifestLookup {
    Found(manifest::FacetManifest),
    PlugDisabled { plug_id: String },
    UnknownTag,
}

pub struct PlugsRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    big_repo: SharedBigRepo,
    blobs: Arc<crate::blobs::BlobsRepo>,
    doc_config_id: daybook_types::doc::DocId,

    drawer: tokio::sync::OnceCell<Arc<crate::drawer::DrawerRepo>>,
    config_store: tokio::sync::OnceCell<crate::stores::FacetStoreHandle<PlugsConfig>>,

    mutation_mutex: tokio::sync::Mutex<()>,
    local_actor_id: ActorId,
    cancel_token: CancellationToken,
    cache: tokio::sync::Mutex<PlugsCache>,
    notif_tx: tokio::sync::mpsc::UnboundedSender<PlugsNotif>,
}

#[derive(Debug, Clone)]
pub struct ImportedPlug {
    pub plug_id: String,
    pub version: semver::Version,
    /// Doc id of the authored manifest doc (authoring imports only).
    pub doc_id: Option<daybook_types::doc::DocId>,
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
        doc_config_id: DocumentId,
        local_user_path: daybook_types::doc::UserPathBuf,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "plugs-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel();

        let repo = Arc::new(Self {
            big_repo: Arc::clone(&big_repo),
            blobs,
            doc_config_id: daybook_types::doc::DocId::from(doc_config_id.to_string()),
            drawer: tokio::sync::OnceCell::new(),
            config_store: tokio::sync::OnceCell::new(),
            mutation_mutex: tokio::sync::Mutex::new(()),
            local_actor_id,
            registry: Arc::clone(&registry),
            cancel_token: cancel_token.clone(),
            cache: tokio::sync::Mutex::new(PlugsCache::default()),
            notif_tx,
        });

        // ADR 007 §7: the notif loop maintains the derived cache and emits
        // enabled-only events; fed by the switch sink (PlugsSwitchSink).
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.child_token();
            async move {
                repo.notif_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling plugs notifs")
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

    /// ADR 007 §2: the drawer is loaded after the plugs repo (the drawer needs
    /// the plugs repo for facet validation). Attach it here so the plugs repo
    /// can read manifest docs and write the plugg config facet through it.
    pub async fn attach_drawer(&self, drawer: Arc<crate::drawer::DrawerRepo>) -> Res<()> {
        if self.drawer.set(drawer).is_err() {
            eyre::bail!("drawer already attached to plugs repo");
        }
        let store = crate::stores::FacetStoreHandle::load(
            Arc::clone(self.drawer.get().expect("just set")),
            self.doc_config_id.clone(),
            daybook_types::doc::BranchPathBuf::from("main"),
        )
        .await?;
        if self.config_store.set(store).is_err() {
            eyre::bail!("plugs config store already attached");
        }
        Ok(())
    }

    fn config_store(&self) -> Res<&crate::stores::FacetStoreHandle<PlugsConfig>> {
        self.config_store
            .get()
            .ok_or_eyre("plugs config store not attached")
    }

    /// The `FacetStoreSink` that keeps the config store's projection live
    /// (registered in the rt switch sinks map).
    pub fn config_store_sink(&self) -> Option<crate::stores::FacetStoreSink<PlugsConfig>> {
        self.config_store.get().map(|store| store.sink())
    }

    fn plug_config_facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig)
    }

    fn plug_manifest_facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugManifest)
    }


    /// ADR 007 §7: init snapshot synthesized from the plugg config facet —
    /// `PlugEnabled` for the active set only (pending plugs emit on
    /// resolution, §6 — the notif loop's active-set diff covers it).
    pub async fn events_for_init(&self) -> Res<Vec<PlugsEvent>> {
        self.rebuild_cache().await?;
        let cache = self.cache.lock().await;
        let mut events = Vec::with_capacity(cache.active_manifests.len());
        for (id, (heads, _)) in &cache.active_manifests {
            events.push(PlugsEvent::PlugEnabled {
                id: id.clone(),
                heads: heads.clone(),
                origin: self.local_origin(),
            });
        }
        Ok(events)
    }
}
