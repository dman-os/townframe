use crate::interlude::*;
use crate::repos::Repo;
use tokio_util::sync::CancellationToken;

use daybook_types::manifest;

#[cfg(test)]
mod tests;

pub fn system_plugs() -> Vec<manifest::PlugManifest> {
    use daybook_types::doc::*;
    use manifest::{
        FacetDisplayDeets, FacetDisplayHint, FacetManifest, FacetReferenceManifest, LocalStateManifest,
    };

    let plugs = vec![
        manifest::PlugManifest {
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
        },
    ];

    plugs
}

/// ADR 007 §3: `@daybook/core` cannot be disabled. The guard lives in the
/// plugg config facet mutation, not in the runtime.
pub const CORE_PLUG_ID: &str = "@daybook/core";

/// ADR 007 §1: the plugg config facet value.
pub type PlugsConfig = daybook_types::doc::PlugsConfig;

impl crate::stores::FacetStore for PlugsConfig {
    fn facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig)
    }

    fn seed() -> Self {
        PlugsConfig {
            enabled: HashMap::new(),
            known_manifests: HashMap::new(),
            plug_config_doc_ids: HashMap::new(),
        }
    }
}

/// ADR 007 §5: known plugs are a derived read cache over the config facet's
/// known_manifests (manifest docs), not a replicated AmStore. The app-doc
/// `PlugsStore` is retired; existing data is ignored (no migration).
/// ADR 007 §5: derived read cache. `enabled` / `plug_config_doc_ids` live in
/// the config `FacetStore`; only manifest-derived state is cached here.
#[derive(Default)]
struct PlugsCache {
    /// plug id -> manifest at heads (known plugs).
    manifests: HashMap<String, Arc<manifest::PlugManifest>>,
    /// Index: property tag -> plug id (@ns/name).
    tag_to_plug: HashMap<String, String>,
    /// Index: property tag -> facet manifest.
    facet_manifests: HashMap<String, manifest::FacetManifest>,
    /// Active plugs: enabled + readable at pinned heads (plug id -> heads + manifest).
    active_manifests: HashMap<String, (ChangeHashSet, Arc<manifest::PlugManifest>)>,
}

/// ADR 007 §7: a doc change forwarded from the switch sink to the notif loop.
#[derive(Debug, Clone)]
pub(crate) enum PlugsNotif {
    DocChanged {
        doc_id: daybook_types::doc::DocId,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
        config_changed: bool,
        manifest_changed: bool,
    },
}

/// ADR 007 §6: drawer facet validation consults active plugs only, with a
/// distinct "plug disabled" vs "unknown tag" error.
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
    /// ADR 007 §2: the repo config doc (third core doc).
    doc_config_id: daybook_types::doc::DocId,

    drawer: tokio::sync::OnceCell<Arc<crate::drawer::DrawerRepo>>,
    /// ADR 007 §2: the plugg config facet as a `FacetStore` — the in-memory
    /// projection kept live by `FacetStoreSink` (registered in rt).
    config_store: tokio::sync::OnceCell<crate::stores::FacetStoreHandle<PlugsConfig>>,

    mutation_mutex: tokio::sync::Mutex<()>,
    local_actor_id: ActorId,
    cancel_token: CancellationToken,
    cache: tokio::sync::Mutex<PlugsCache>,

    /// ADR 007 §7: fed by the switch sink; the notif loop keeps the derived
    /// cache fresh and emits enabled-only events (like the old notifs_loop).
    notif_tx: tokio::sync::mpsc::UnboundedSender<PlugsNotif>,
}

// Granular event enum for specific changes (ADR 007 §7: enabled-only).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PlugsEvent {
    /// Config entry added, or pending -> active (ADR 007 §6).
    PlugEnabled {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// Config entry removed.
    PlugDisabled {
        id: String,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// Explicit re-pin only.
    PlugUpdated {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// The plugg config facet moved.
    PlugsConfigChanged {
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
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

    async fn read_manifest_doc(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<Arc<manifest::PlugManifest>>> {
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch_heads(
                doc_id,
                daybook_types::doc::BranchPath::new("main"),
                heads,
                Some(vec![Self::plug_manifest_facet_key()]),
            )
            .await?
        else {
            return Ok(None);
        };
        let Some(raw) = doc.facets.get(&Self::plug_manifest_facet_key()) else {
            return Ok(None);
        };
        let manifest = serde_json::from_value::<manifest::PlugManifest>(raw.clone())?;
        Ok(Some(Arc::new(manifest)))
    }

    fn parse_enabled_ref(url: &url::Url) -> Res<daybook_types::url::FacetRef> {
        let parsed = daybook_types::url::parse_facet_ref(url)?;
        if parsed.facet_key.tag
            != daybook_types::doc::FacetTag::WellKnown(
                daybook_types::doc::WellKnownFacetTag::PlugManifest,
            )
            || parsed.facet_key.id != "main"
        {
            eyre::bail!(
                "enabled ref must point at org.example.daybook.plugManifest/main: {url}"
            );
        }
        Ok(parsed)
    }

    /// Read the manifest pointed at by a full facet ref (doc id + branch +
    /// pinned heads, or current branch heads when unpinned). Returns None when
    /// the doc/heads are not locally readable (ADR 007 §6: pending). Used for
    /// enabled refs and for pending resolution alike.
    async fn read_manifest_at_ref(
        &self,
        ref_url: &url::Url,
    ) -> Res<Option<(ChangeHashSet, Arc<manifest::PlugManifest>)>> {
        let parsed = Self::parse_enabled_ref(ref_url)?;
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let branch_path =
            daybook_types::doc::BranchPath::new(parsed.branch.as_deref().unwrap_or("main"));
        let heads = if let Some(at) = &parsed.at {
            ChangeHashSet(am_utils_rs::parse_commit_heads(at)?)
        } else {
            let Some(heads) = drawer
                .get_branch_heads_for_path(&parsed.doc_id, branch_path)
                .await?
            else {
                return Ok(None);
            };
            heads
        };
        let Some(manifest) = self.read_manifest_doc(&parsed.doc_id, &heads).await? else {
            return Ok(None);
        };
        Ok(Some((heads, manifest)))
    }

    fn build_enabled_ref(doc_id: &str, branch: &str, heads: &ChangeHashSet) -> Res<url::Url> {
        let at = am_utils_rs::serialize_commit_heads(heads.as_ref()).join("|");
        let url = format!(
            "db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch={branch}&at={at}"
        );
        Ok(url.parse()?)
    }

    /// Rebuild the derived cache from the config facet (known + enabled) and the plugg
    /// config facet (enabled + active at pinned heads).
    async fn refresh_cache(&self) -> Res<()> {
        let mut manifests: HashMap<String, Arc<manifest::PlugManifest>> = HashMap::new();
        let mut tag_to_plug: HashMap<String, String> = HashMap::new();
        let mut facet_manifests: HashMap<String, manifest::FacetManifest> = HashMap::new();
        // ADR 007 §5: known plugs come from the config store, not the
        // facet-set index. Each known ref pins the manifest doc + heads.
        let (enabled, known_manifests) = self
            .config_store()?
            .query_sync(|config| (config.enabled.clone(), config.known_manifests.clone()))
            .await;
        for ref_url in known_manifests.values() {
            let parsed = match Self::parse_enabled_ref(ref_url) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            let Some(at) = &parsed.at else {
                continue;
            };
            let heads = ChangeHashSet(am_utils_rs::parse_commit_heads(at)?);
            let Some(manifest) = self.read_manifest_doc(&parsed.doc_id, &heads).await? else {
                continue;
            };
            let plug_id = manifest.id();
            manifests.insert(plug_id.clone(), Arc::clone(&manifest));
            for facet in &manifest.facets {
                tag_to_plug.insert(facet.key_tag.to_string(), plug_id.clone());
                facet_manifests.insert(facet.key_tag.to_string(), facet.clone());
            }
        }
        let mut active_manifests: HashMap<String, (ChangeHashSet, Arc<manifest::PlugManifest>)> =
            HashMap::new();
        for (id, ref_url) in &enabled {
            if let Some((heads, manifest)) = self.read_manifest_at_ref(ref_url).await? {
                if manifest.id() == *id {
                    active_manifests.insert(id.clone(), (heads, manifest));
                } else {
                    warn!(
                        plug_id = %id,
                        manifest_id = %manifest.id(),
                        "enabled ref manifest id mismatch; treating as pending"
                    );
                }
            }
        }
        let mut cache = self.cache.lock().await;
        cache.manifests = manifests;
        cache.tag_to_plug = tag_to_plug;
        cache.facet_manifests = facet_manifests;
        cache.active_manifests = active_manifests;
        Ok(())
    }

    /// ADR 007 §4: idempotent ensure-on-open. Ensures the core manifest doc
    /// exists (creating it via the single unchecked internal doc-create) and
    /// the plugg config facet exists with `@daybook/core` enabled at the core
    /// doc's initial heads.
    pub async fn ensure_core_plug(&self) -> Res<()> {
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        // Register the repo config doc in the drawer (idempotent) so the plugg
        // config facet writes go through the drawer like any facet write.
        drawer
            .register_existing_doc(
                &self.doc_config_id,
                self.doc_config_id
                    .parse()
                    .map_err(|err| ferr!("invalid doc_config id: {err}"))?,
                daybook_types::doc::BranchPath::new("main"),
            )
            .await?;

        if self
            .config_store()?
            .query_sync(|config| config.enabled.contains_key(CORE_PLUG_ID))
            .await
        {
            return Ok(());
        }

        // Create the core manifest doc if missing (unchecked: nothing is
        // registered yet — the single write that skips facet validation).
        let core_manifest = system_plugs()
            .into_iter()
            .find(|manifest| manifest.id() == CORE_PLUG_ID)
            .ok_or_eyre("core plug manifest missing from system_plugs")?;
        let core_doc_id = drawer
            .add_unchecked(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    Self::plug_manifest_facet_key(),
                    daybook_types::doc::WellKnownFacet::PlugManifest(core_manifest).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        let core_doc_id = daybook_types::doc::DocId::from(core_doc_id);

        // Read the core manifest back at its initial heads and seed the derived
        // cache so the config facet write below validates normally.
        let core_heads = drawer
            .get_doc_branches(&core_doc_id)
            .await?
            .and_then(|entry| entry.branches.get("main").cloned())
            .ok_or_eyre("core manifest doc missing main branch")?;
        let core_manifest = self
            .read_manifest_doc(&core_doc_id, &core_heads)
            .await?
            .ok_or_eyre("core manifest doc unreadable at initial heads")?;
        let ref_url = Self::build_enabled_ref(&core_doc_id, "main", &core_heads)?;
        {
            let mut cache = self.cache.lock().await;
            cache
                .manifests
                .insert(CORE_PLUG_ID.to_string(), Arc::clone(&core_manifest));
            // Core is being enabled by the config write below; seed the
            // manifest maps + active so that write validates normally (the
            // plugConfig facet is owned by core itself).
            cache
                .active_manifests
                .insert(CORE_PLUG_ID.to_string(), (core_heads.clone(), Arc::clone(&core_manifest)));
            for facet in &core_manifest.facets {
                cache
                    .tag_to_plug
                    .insert(facet.key_tag.to_string(), CORE_PLUG_ID.to_string());
                cache
                    .facet_manifests
                    .insert(facet.key_tag.to_string(), facet.clone());
            }
        }

        // Write the plugg config facet with core enabled at the core doc's
        // initial heads (validated against core's own plugConfig facet).
        let (_, heads) = self.config_store()?.mutate_sync(|config| {
            config.enabled.insert(CORE_PLUG_ID.to_string(), ref_url.clone());
            config
                .known_manifests
                .insert(CORE_PLUG_ID.to_string(), ref_url);
        }).await?;
        self.process_config_change(&heads, &self.local_origin()).await?;
        Ok(())
    }

    /// Diff two active-set snapshots into enabled-only events (ADR 007 §7).
    fn diff_active_manifests(
        old: &HashMap<String, (ChangeHashSet, Arc<manifest::PlugManifest>)>,
        new: &HashMap<String, (ChangeHashSet, Arc<manifest::PlugManifest>)>,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Vec<PlugsEvent> {
        let mut events = vec![];
        for (id, (heads, _)) in new {
            match old.get(id) {
                None => events.push(PlugsEvent::PlugEnabled {
                    id: id.clone(),
                    heads: heads.clone(),
                    origin: origin.clone(),
                }),
                Some((old_heads, _)) if old_heads != heads => events.push(PlugsEvent::PlugUpdated {
                    id: id.clone(),
                    heads: heads.clone(),
                    origin: origin.clone(),
                }),
                _ => {}
            }
        }
        for id in old.keys() {
            if !new.contains_key(id) {
                events.push(PlugsEvent::PlugDisabled {
                    id: id.clone(),
                    origin: origin.clone(),
                });
            }
        }
        events
    }

    /// ADR 007 §7: a config facet change (local write or remote notif).
    /// Rebuild the derived cache from the store, diff the active set, and
    /// emit enabled-only events.
    async fn process_config_change(
        &self,
        heads: &ChangeHashSet,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Res<()> {
        let old_active = self.cache.lock().await.active_manifests.clone();
        self.refresh_cache().await?;
        let new_active = self.cache.lock().await.active_manifests.clone();
        let mut events = Self::diff_active_manifests(&old_active, &new_active, origin);
        events.push(PlugsEvent::PlugsConfigChanged {
            heads: heads.clone(),
            origin: origin.clone(),
        });
        self.registry.notify(events);
        Ok(())
    }

    /// ADR 007 §7: a manifest doc changed (a pending plug's doc may have
    /// landed). Rebuild the cache and diff the active set; no
    /// `PlugsConfigChanged` (the config facet did not move).
    async fn process_manifest_change(
        &self,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Res<()> {
        let old_active = self.cache.lock().await.active_manifests.clone();
        self.refresh_cache().await?;
        let new_active = self.cache.lock().await.active_manifests.clone();
        let events = Self::diff_active_manifests(&old_active, &new_active, origin);
        self.registry.notify(events);
        Ok(())
    }

    /// ADR 007 §7: the notif loop, fed by the switch sink. Maintains the
    /// derived cache and emits enabled-only events, mirroring the old
    /// notifs_loop. Reloads the config store first so the refresh below
    /// reads fresh (the FacetStoreSink is a separate async path).
    async fn notif_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<PlugsNotif>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        loop {
            let notif = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                msg = notif_rx.recv() => match msg {
                    Some(notif) => notif,
                    None => break,
                },
            };
            let PlugsNotif::DocChanged {
                doc_id,
                heads,
                origin,
                config_changed,
                manifest_changed,
            } = notif;
            self.config_store()?.reload().await?;
            if manifest_changed {
                self.record_known_manifest_doc(&doc_id, &heads).await?;
            }
            if config_changed {
                self.process_config_change(&heads, &origin).await?;
            } else if manifest_changed {
                self.process_manifest_change(&origin).await?;
            }
        }
        Ok(())
    }

    /// ADR 007 §3: enable a plug by pinning a full ref. The ref must point at a
    /// readable `plugManifest/main` facet; the manifest id becomes the key.
    pub async fn enable_plug(&self, ref_url: &url::Url) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let parsed = Self::parse_enabled_ref(ref_url)?;
        let Some((heads, manifest)) = self.read_manifest_at_ref(ref_url).await? else {
            eyre::bail!("manifest not readable at ref heads: {ref_url}");
        };
        let plug_id = manifest.id();
        let ref_url = if parsed.at.is_none() {
            Self::build_enabled_ref(&parsed.doc_id, "main", &heads)?
        } else {
            ref_url.clone()
        };
        let _guard = self.mutation_mutex.lock().await;
        // ADR 007 §2: the plug's config doc is created at enablement and the
        // mapping recorded in the config facet, so an enabled plug always has
        // a config doc. The mapping is retained across disablement (disable
        // only removes the enabled entry). The doc creation is async, so it
        // happens before the store mutate; the closure re-checks under the
        // store lock.
        let needs_config_doc = !self
            .config_store()?
            .query_sync(|config| config.plug_config_doc_ids.contains_key(&plug_id))
            .await;
        let config_doc_id = if needs_config_doc {
            let drawer = self
                .drawer
                .get()
                .ok_or_eyre("plugs repo drawer not attached")?;
            Some(
                drawer
                    .add(daybook_types::doc::AddDocArgs {
                        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        facets: HashMap::new(),
                        user_path: None,
                    })
                    .await?,
            )
        } else {
            None
        };
        let (_, new_heads) = self.config_store()?.mutate_sync(|config| {
            config.enabled.insert(plug_id.clone(), ref_url);
            if let Some(doc_id) = config_doc_id {
                config
                    .plug_config_doc_ids
                    .entry(plug_id.clone())
                    .or_insert(doc_id);
            }
        }).await?;
        self.process_config_change(&new_heads, &self.local_origin()).await?;
        Ok(new_heads)
    }

    /// ADR 007 §3: disable a plug by removing its config entry. `@daybook/core`
    /// cannot be disabled — the guard lives in this config mutation.
    pub async fn disable_plug(&self, plug_id: &str) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        if plug_id == CORE_PLUG_ID {
            eyre::bail!("@daybook/core cannot be disabled");
        }
        let _guard = self.mutation_mutex.lock().await;
        let (_, new_heads) = self.config_store()?.mutate_sync(|config| {
            config.enabled.remove(plug_id);
        }).await?;
        self.process_config_change(&new_heads, &self.local_origin()).await?;
        Ok(new_heads)
    }

    /// ADR 007 §3: explicit re-pin to the latest main-branch heads. Fails if
    /// the manifest is not readable at the new heads.
    pub async fn update_plug(&self, plug_id: &str) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let _guard = self.mutation_mutex.lock().await;
        let Some(old_ref) = self
            .config_store()?
            .query_sync(|config| config.enabled.get(plug_id).cloned())
            .await
        else {
            eyre::bail!("plug not enabled: {plug_id}");
        };
        let parsed = Self::parse_enabled_ref(old_ref)?;
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let heads = drawer
            .get_branch_heads_for_path(&parsed.doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("doc has no main branch heads")?;
        let new_ref = Self::build_enabled_ref(&parsed.doc_id, "main", &heads)?;
        let Some((_, manifest)) = self.read_manifest_at_ref(&new_ref).await? else {
            eyre::bail!("manifest not readable at latest heads for {plug_id}");
        };
        if manifest.id() != plug_id {
            eyre::bail!("manifest id mismatch at latest heads for {plug_id}");
        }
        let (_, new_heads) = self.config_store()?.mutate_sync(|config| {
            config.enabled.insert(plug_id.to_string(), new_ref);
        }).await?;
        self.process_config_change(&new_heads, &self.local_origin()).await?;
        Ok(new_heads)
    }

    /// ADR 007 §7: init snapshot synthesized from the plugg config facet —
    /// `PlugEnabled` for the active set only (pending plugs emit on
    /// resolution, §6 — the notif loop's active-set diff covers it).
    pub async fn events_for_init(&self) -> Res<Vec<PlugsEvent>> {
        self.refresh_cache().await?;
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

    /// Active plug manifest (enabled + readable at pinned heads). Known-but-
    /// disabled plugs return None; commands and inits resolve through active
    /// plugs only (ADR 007 §6).
    pub async fn get(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        let cache = self.cache.lock().await;
        cache
            .active_manifests
            .get(id)
            .map(|(_, manifest)| Arc::clone(manifest))
    }

    /// Known plug manifest (from the facet-set index), regardless of enablement.
    pub async fn get_known(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        let cache = self.cache.lock().await;
        cache.manifests.get(id).cloned()
    }

    pub async fn get_plug_config_doc_id(&self, plug_id: &str) -> Option<String> {
        let store = self.config_store.get()?;
        store
            .query_sync(|config| config.plug_config_doc_ids.get(plug_id).cloned())
            .await
    }

    pub async fn get_display_hint(&self, prop_tag: &str) -> Option<manifest::FacetDisplayHint> {
        let cache = self.cache.lock().await;
        cache
            .facet_manifests
            .get(prop_tag)
            .map(|facet_manifest| facet_manifest.display_config.clone())
    }

    /// ADR 007 §6: drawer facet validation consults active plugs only, with a
    /// distinct "plug disabled" vs "unknown tag" error.
    pub async fn get_facet_manifest_by_tag(
        &self,
        facet_tag: &str,
    ) -> FacetManifestLookup {
        let (plug_id, facet_manifest) = {
            let cache = self.cache.lock().await;
            let Some(plug_id) = cache.tag_to_plug.get(facet_tag) else {
                return FacetManifestLookup::UnknownTag;
            };
            (plug_id.clone(), cache.facet_manifests.get(facet_tag).cloned())
        };
        let enabled = match self.config_store.get() {
            Some(store) => {
                store
                    .query_sync(|config| config.enabled.contains_key(&plug_id))
                    .await
            }
            None => false,
        };
        if !enabled {
            return FacetManifestLookup::PlugDisabled { plug_id };
        }
        match facet_manifest {
            Some(facet_manifest) => FacetManifestLookup::Found(facet_manifest.clone()),
            None => FacetManifestLookup::UnknownTag,
        }
    }

    pub async fn get_owner_plug_id_by_facet_tag(&self, facet_tag: &str) -> Option<String> {
        let plug_id = {
            let cache = self.cache.lock().await;
            cache.tag_to_plug.get(facet_tag)?.clone()
        };
        let enabled = match self.config_store.get() {
            Some(store) => {
                store
                    .query_sync(|config| config.enabled.contains_key(&plug_id))
                    .await
            }
            None => false,
        };
        if !enabled {
            return None;
        }
        Some(plug_id)
    }

    pub async fn list_display_hints(&self) -> Vec<(String, manifest::FacetDisplayHint)> {
        let cache = self.cache.lock().await;
        cache
            .facet_manifests
            .iter()
            .map(|(tag, facet_manifest)| (tag.clone(), facet_manifest.display_config.clone()))
            .collect()
    }

    /// Known plugs (all manifest docs catalogued by the facet-set index).
    pub async fn list_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        let cache = self.cache.lock().await;
        cache.manifests.values().cloned().collect()
    }

    /// Active plugs only (enabled + readable at pinned heads).
    pub async fn list_active_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        let cache = self.cache.lock().await;
        cache
            .active_manifests
            .values()
            .map(|(_, manifest)| Arc::clone(manifest))
            .collect()
    }

    /// ADR 007 §8: import a plug from an existing manifest doc. Reads the
    /// manifest facet at the given heads, validates, grants core-docs-group
    /// access so the doc replicates in the core partition, and enables at
    /// those heads.
    pub async fn import_from_doc_id(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: &ChangeHashSet,
    ) -> Res<ImportedPlug> {
        let manifest = self
            .read_manifest_doc(doc_id, heads)
            .await?
            .ok_or_eyre("no plugManifest facet at given heads")?;
        self.validate_incoming_plug(&manifest).await?;
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let branch_ref = drawer
            .get_branch_ref(doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("manifest doc missing main branch")?;
        let authority = crate::authority::ensure(
            &self.big_repo,
            drawer.meta_store_sql(),
            None,
        )
        .await?;
        crate::authority::grant_docs_admin(
            &self.big_repo,
            &authority.core_docs,
            [branch_ref.branch_doc_id],
        )
        .await?;
        let ref_url = Self::build_enabled_ref(doc_id, "main", heads)?;
        self.record_known_manifest_doc(doc_id, heads).await?;
        self.enable_plug(&ref_url).await?;
        Ok(ImportedPlug {
            plug_id: manifest.id(),
            version: manifest.version.clone(),
            doc_id: Some(doc_id.clone()),
            imported_blob_hashes: vec![],
            source_digest: None,
        })
    }

    /// Add a new plug to the repo after validating it (ADR 007 §8: authoring).
    /// Writes a manifest doc through the drawer; the manifest facet is
    /// validated like any other facet write.
    pub async fn add(&self, mut manifest: manifest::PlugManifest) -> Res<daybook_types::doc::DocId> {
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
                        eyre::bail!("unsupported static wasm component_url: {url}");
                    }
                    crate::blobs::BLOB_SCHEME => {}
                    _ => {
                        eyre::bail!("unsupported component_url scheme: {url}");
                    }
                }
            }
        }

        // 2. Write a manifest doc through the drawer (validated).
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let doc_id = drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    Self::plug_manifest_facet_key(),
                    daybook_types::doc::WellKnownFacet::PlugManifest(manifest).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // 3. Grant core-docs-group access so the manifest doc replicates in the
        // core partition.
        let branch_ref = drawer
            .get_branch_ref(&doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("manifest doc missing main branch")?;
        let authority = crate::authority::ensure(
            &self.big_repo,
            drawer.meta_store_sql(),
            None,
        )
        .await?;
        crate::authority::grant_docs_admin(
            &self.big_repo,
            &authority.core_docs,
            [branch_ref.branch_doc_id],
        )
        .await?;

        // ADR 007 §5: record the new manifest doc in the config facet's
        // known_manifests so the derived cache sees it (authoring validations
        // like tag clashes and version checks run against the cache). The
        // config write takes the mutation mutex itself.
        drop(_guard);
        let heads = drawer
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|entry| entry.branches.get("main").cloned())
            .ok_or_eyre("manifest doc missing main branch")?;
        self.record_known_manifest_doc(&doc_id, &heads).await?;
        self.refresh_cache().await?;

        Ok(doc_id)
    }

    /// ADR 007 §7: the config facet moved (local or remote write). Read the
    /// current config and diff enabled/active, emitting enabled-only events.
    /// Called inline by the switch sink.
    /// ADR 007 §5: ensure a manifest doc is recorded in the config facet's
    /// known_manifests (plug id -> full ref at the given heads). Called by
    /// the switch sink and by the authoring/import paths.
    async fn record_known_manifest_doc(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        let _guard = self.mutation_mutex.lock().await;
        let Some(manifest) = self.read_manifest_doc(doc_id, heads).await? else {
            return Ok(());
        };
        let ref_url = Self::build_enabled_ref(doc_id, "main", heads)?;
        // Known-manifests changes do not affect enabled/active; callers
        // refresh the derived cache themselves. No PlugsEvent is emitted
        // here (ADR 007 §7: events are enabled-only).
        self.config_store()?
            .mutate_sync(|config| {
                if config.known_manifests.get(&manifest.id()) != Some(&ref_url) {
                    config.known_manifests.insert(manifest.id(), ref_url);
                }
            })
            .await?;
        Ok(())
    }

}

/// ADR 007 §7: switch sink that drives the plugs repo inline. The switch's
/// drawer predicate filters Doc events to the plug facets (plugsConfig +
/// plugManifest); on_event calls the repo's private methods directly. The
/// plugs repo no longer subscribes to big_repo and has no worker loop.
pub(crate) struct PlugsSwitchSink {
    repo: Arc<PlugsRepo>,
}

impl PlugsSwitchSink {
    pub(crate) fn new(repo: Arc<PlugsRepo>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl crate::rt::switch::SwitchSink for PlugsSwitchSink {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        use daybook_types::manifest::DocPredicateClause;
        use daybook_types::doc::FacetTag;
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: false,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            // Only docs whose diff touches the plug facets reach on_event.
            drawer_predicate: Some(DocPredicateClause::Or(vec![
                DocPredicateClause::HasTag(FacetTag::WellKnown(
                    daybook_types::doc::WellKnownFacetTag::PlugsConfig,
                )),
                DocPredicateClause::HasTag(FacetTag::WellKnown(
                    daybook_types::doc::WellKnownFacetTag::PlugManifest,
                )),
            ])),
        }
    }

    async fn on_event(
        &mut self,
        event: &crate::rt::switch::SwitchEvent,
        _ctx: &crate::rt::switch::SwitchSinkCtx<'_>,
    ) -> Res<crate::rt::switch::SwitchSinkOutcome> {
        let crate::rt::switch::SwitchEvent::Doc(evt) = event else {
            return Ok(crate::rt::switch::SwitchSinkOutcome::default());
        };
        let Some(diff) = &evt.diff else {
            return Ok(crate::rt::switch::SwitchSinkOutcome::default());
        };
        // Forward to the notif loop (which reloads the store, refreshes the
        // cache, and emits events). Cheap — no reads in the sink.
        let changed: Vec<&daybook_types::doc::FacetKey> = diff
            .changed_facet_keys
            .iter()
            .chain(diff.added_facet_keys.iter())
            .chain(diff.removed_facet_keys.iter())
            .collect();
        let plugs_config_tag = daybook_types::doc::FacetTag::WellKnown(
            daybook_types::doc::WellKnownFacetTag::PlugsConfig,
        );
        let plug_manifest_tag = daybook_types::doc::FacetTag::WellKnown(
            daybook_types::doc::WellKnownFacetTag::PlugManifest,
        );
        self.repo
            .notif_tx
            .send(PlugsNotif::DocChanged {
                doc_id: evt.doc_id.clone(),
                heads: evt.new_heads.clone(),
                origin: evt.origin.clone(),
                config_changed: changed.iter().any(|key| key.tag == plugs_config_tag),
                manifest_changed: changed.iter().any(|key| key.tag == plug_manifest_tag),
            })
            .map_err(|_| ferr!("plugs notif channel closed"))?;
        Ok(crate::rt::switch::SwitchSinkOutcome::default())
    }
}
