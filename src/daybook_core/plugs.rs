use crate::interlude::*;
use tokio_util::sync::CancellationToken;

use daybook_types::manifest;

use self::cache::PlugsCache;
use big_sync::keyed_frontier::SqliteFrontierSelector;
use big_sync::keyed_frontier::{SqliteFrontierCodec, SqliteKeyedFrontier};
use big_sync_core::keyed_frontier::{FrontierRevision, KeyedFrontier, KeyedFrontierTransaction};
use big_sync_core::live_revision_watch::LiveRevisionWatch;
use big_sync_core::revisioned_store::{
    KeyedFrontierRevisionReader, RevisionRead, RevisionReadLimits, RevisionedStore,
    RevisionedStoreReader,
};
use std::collections::BTreeSet;

pub(crate) use self::events::{
    PLUG_MANIFEST_CONSUMER_STATE_ID, PLUGS_CONFIG_CONSUMER_STATE_ID,
    PlugsConfigFacetSetConsumerStopToken, PlugsManifestConsumerStopToken,
    spawn_facet_set_plugs_config_consumer, spawn_facet_set_plugs_manifest_consumer,
};
pub use self::oci::OciImportOptions;

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
                key_tag: WellKnownFacetTag::Branch.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::Branch),
                display_config: default(),
                references: default(),
            },
            FacetManifest {
                key_tag: WellKnownFacetTag::Branches.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::Branches),
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
    big_repo: SharedBigRepo,
    blobs: Arc<crate::blobs::BlobsRepo>,
    doc_config_id: daybook_types::doc::DocId,

    drawer: tokio::sync::OnceCell<Arc<crate::drawer::DrawerRepo>>,
    config_store: tokio::sync::OnceCell<crate::stores::FacetStoreHandle<PlugsConfig>>,

    mutation_mutex: tokio::sync::Mutex<()>,
    cancel_token: CancellationToken,
    cache: surelock::mutex::Mutex<PlugsCache>,
    pub(crate) revision_frontier: SqliteKeyedFrontier<PlugRevisionCodec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlugRevisionState {
    pub manifest_heads: ChangeHashSet,
    pub manifest: manifest::PlugManifest,
}

#[derive(Clone)]
pub(crate) struct PlugRevisionCodec;

#[derive(Debug, Clone)]
pub enum PlugsRevisionSelector {
    All,
    PlugIds(BTreeSet<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PlugsWatchChange {
    pub plug_id: String,
    pub active: bool,
}

impl SqliteFrontierCodec for PlugRevisionCodec {
    type Key = String;
    type Value = PlugRevisionState;
    fn encode_key(&self, key: &String) -> Vec<u8> {
        key.as_bytes().to_vec()
    }
    fn decode_key(&self, bytes: &[u8]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
    fn encode_value(&self, value: &PlugRevisionState) -> Vec<u8> {
        serde_json::to_vec(value).expect(ERROR_JSON)
    }
    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<PlugRevisionState, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[async_trait]
impl RevisionedStore for PlugsRepo {
    type Revision = FrontierRevision;
    type Entry = big_sync_core::keyed_frontier::FrontierEntry<String, PlugRevisionState>;
    type Selector = PlugsRevisionSelector;
    type Error = big_sync_core::keyed_frontier::KeyedFrontierError;
    type Reader<'a>
        = KeyedFrontierRevisionReader<'a, String, PlugRevisionState>
    where
        Self: 'a;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.revision_frontier.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: Self::Revision,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        let selector = match selector {
            PlugsRevisionSelector::All => SqliteFrontierSelector::All { after },
            PlugsRevisionSelector::PlugIds(keys) => {
                SqliteFrontierSelector::Keys(keys.into_iter().map(|key| (key, after)).collect())
            }
        };
        let inner = self.revision_frontier.open(selector).await?;
        Ok(KeyedFrontierRevisionReader::new(inner))
    }
}

pub struct PlugsWatch<'a> {
    inner: LiveRevisionWatch<'a, PlugsRepo>,
}

impl<'a> PlugsWatch<'a> {
    pub async fn open(
        source: &'a PlugsRepo,
        selector: PlugsRevisionSelector,
    ) -> Result<Self, big_sync_core::keyed_frontier::KeyedFrontierError> {
        Ok(Self {
            inner: LiveRevisionWatch::open(source, selector).await?,
        })
    }

    pub async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<
        big_sync_core::revisioned_store::RevisionRead<FrontierRevision, PlugsWatchChange>,
        big_sync_core::keyed_frontier::KeyedFrontierError,
    > {
        match self.inner.next(limits).await? {
            RevisionRead::Entries { revision, entries } => Ok(RevisionRead::Entries {
                revision,
                entries: entries
                    .into_iter()
                    .map(|entry| PlugsWatchChange {
                        plug_id: entry.key,
                        active: entry.value.is_some(),
                    })
                    .collect(),
            }),
            RevisionRead::ReplayComplete { .. } => {
                unreachable!("LiveRevisionWatch hides replay completion")
            }
        }
    }
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

#[allow(
    clippy::clone_on_ref_ptr,
    clippy::disallowed_names,
    clippy::while_let_loop
)]
impl PlugsRepo {
    pub async fn watch<'a>(
        &'a self,
        selector: PlugsRevisionSelector,
    ) -> Result<PlugsWatch<'a>, big_sync_core::keyed_frontier::KeyedFrontierError> {
        PlugsWatch::open(self, selector).await
    }

    pub(crate) async fn reconcile_revision_frontier(&self) -> Res<()> {
        let mut reader = self
            .open(PlugsRevisionSelector::All, 0)
            .await
            .map_err(|e| eyre::eyre!(e.to_string()))?;
        let mut current = HashMap::<String, PlugRevisionState>::new();
        loop {
            match reader
                .next(RevisionReadLimits::default())
                .await
                .map_err(|e| eyre::eyre!(e.to_string()))?
            {
                RevisionRead::Entries { entries, .. } => {
                    for entry in entries {
                        if let Some(value) = entry.value {
                            current.insert(entry.key, value);
                        } else {
                            current.remove(&entry.key);
                        }
                    }
                }
                RevisionRead::ReplayComplete { .. } => break,
            }
        }
        drop(reader);
        let desired = surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache
                .active_manifests
                .iter()
                .map(|(id, (heads, manifest))| {
                    (
                        id.clone(),
                        PlugRevisionState {
                            manifest_heads: heads.clone(),
                            manifest: (**manifest).clone(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>()
        });
        let mut tx = self
            .revision_frontier
            .begin()
            .await
            .map_err(|e| eyre::eyre!(e.to_string()))?;
        let mut changed = false;
        for (id, state) in &desired {
            let unchanged = match current.get(id) {
                Some(existing) => {
                    existing.manifest_heads == state.manifest_heads
                        && serde_json::to_vec(&existing.manifest)?
                            == serde_json::to_vec(&state.manifest)?
                }
                None => false,
            };
            if !unchanged {
                changed = true;
                tx.put(id.clone(), state.clone())
                    .await
                    .map_err(|e| eyre::eyre!(e.to_string()))?;
            }
        }
        for id in current.keys().filter(|id| !desired.contains_key(*id)) {
            changed = true;
            tx.delete(id.clone())
                .await
                .map_err(|e| eyre::eyre!(e.to_string()))?;
        }
        if changed {
            tx.commit().await.map_err(|e| eyre::eyre!(e.to_string()))?;
        } else {
            tx.rollback()
                .await
                .map_err(|e| eyre::eyre!(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn load(
        big_repo: SharedBigRepo,
        blobs: Arc<crate::blobs::BlobsRepo>,
        doc_config_id: DocumentId,
        _local_user_path: daybook_types::doc::UserPathBuf,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let cancel_token = CancellationToken::new();
        let frontier_sql = sqlite_local_state_repo
            .ensure_sqlite_ctx("@daybook/core/plugs-revisions")
            .await?;
        let revision_frontier = SqliteKeyedFrontier::new(
            frontier_sql.read_pool.clone(),
            frontier_sql.write_pool.clone(),
            "plugs",
            PlugRevisionCodec,
            Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .map_err(|e| eyre::eyre!(e.to_string()))?;

        let repo = Arc::new(Self {
            big_repo: Arc::clone(&big_repo),
            blobs,
            doc_config_id: daybook_types::doc::DocId::from(doc_config_id.to_string()),
            drawer: tokio::sync::OnceCell::new(),
            config_store: tokio::sync::OnceCell::new(),
            mutation_mutex: tokio::sync::Mutex::new(()),
            cancel_token: cancel_token.clone(),
            cache: surelock::mutex::Mutex::new(PlugsCache::default()),
            revision_frontier,
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: None,
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
        // Ensure the config doc is registered in this drawer. `ensure_core_plug`
        // only registers it during repo init; on a reopen a fresh DrawerRepo
        // has no entry for it, so the config store would hydrate to its empty
        // seed and the derived cache would stay cold. register_existing_doc is
        // idempotent (no-op when already known) and is required for
        // FacetStoreHandle::load below to read the persisted config facet.
        let drawer_ref = self.drawer.get().expect("just set");
        drawer_ref
            .register_existing_doc(
                &self.doc_config_id,
                self.doc_config_id
                    .parse()
                    .map_err(|err| ferr!("invalid doc_config id: {err}"))?,
                daybook_types::doc::BranchPath::new("main"),
            )
            .await?;
        let store = crate::stores::FacetStoreHandle::load(
            drawer_ref.clone(),
            self.doc_config_id.clone(),
            daybook_types::doc::BranchPathBuf::from("main"),
        )
        .await?;
        if self.config_store.set(store).is_err() {
            eyre::bail!("plugs config store already attached");
        }
        // Warm the derived cache from the durable config so drawer facet
        // validation (and plug-init queueing at rt boot) works immediately on
        // this open — including a reopen where the facet-set consumers have
        // already settled the config/manifest revisions and will not re-apply
        // them. Unreadable manifest refs are treated as pending (deferred)
        // exactly like the live consumer, never a fatal error.
        self.warm_cache().await?;
        self.reconcile_revision_frontier().await?;
        Ok(())
    }

    /// Rebuild the derived cache from the durable config store: every
    /// known-manifest ref (materializes tag -> plug + tag -> facet manifest)
    /// and every enabled ref (pins the active manifest). This is the boot-time
    /// counterpart to the incremental `apply_config_diff`; it is idempotent
    /// against concurrent consumer writes (`upsert_known`/`set_active` are
    /// monotonic under the cache lock).
    async fn warm_cache(&self) -> Res<()> {
        let (known_plugs, enabled) = match self.config_store() {
            Ok(store) => {
                store
                    .query_sync(|config| (config.known_plugs.clone(), config.enabled.clone()))
                    .await
            }
            Err(_) => return Ok(()),
        };
        for (plug_id, track) in &known_plugs {
            let Some((_, manifest)) = self.read_manifest_at_ref(&track.last_valid).await? else {
                continue;
            };
            surelock::key::lock_scope(|key| {
                let (mut cache, _key) = key.lock(&self.cache);
                cache.upsert_known(plug_id, &manifest);
            });
        }
        for (plug_id, ref_url) in &enabled {
            self.activate_from_ref(plug_id, ref_url).await?;
        }
        Ok(())
    }

    fn config_store(&self) -> Res<&crate::stores::FacetStoreHandle<PlugsConfig>> {
        self.config_store
            .get()
            .ok_or_eyre("plugs config store not attached")
    }

    pub(crate) fn config_doc_id(&self) -> daybook_types::doc::DocId {
        self.doc_config_id.clone()
    }

    pub(crate) fn config_facet_route(&self) -> crate::index::FacetRouteKey {
        let document_id = self.config_doc_id();
        crate::index::FacetRouteKey {
            branch_id: daybook_types::doc::BranchId(document_id.clone()),
            document_id,
            facet_key: <PlugsConfig as crate::stores::FacetStore>::facet_key(),
        }
    }

    fn plug_manifest_facet_key() -> daybook_types::doc::FacetKey {
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugManifest)
    }

    /// Activate a plug from its enabled ref and update the active cache.
    async fn activate_from_ref(&self, plug_id: &str, ref_url: &url::Url) -> Res<()> {
        let parsed = Self::parse_enabled_ref(ref_url)?;
        if let Some(at) = &parsed.at {
            let pinned = ChangeHashSet(am_utils_rs::parse_commit_heads(at)?);
            if surelock::key::lock_scope(|key| {
                let (cache, _key) = key.lock(&self.cache);
                cache.is_active_at(plug_id, &pinned)
            }) {
                return Ok(());
            }
        }
        let Some((heads, manifest)) = self.materialize_active(plug_id, ref_url).await? else {
            // Pending: enabled but not readable at pinned heads.
            surelock::key::lock_scope(|key| {
                let (mut cache, _key) = key.lock(&self.cache);
                cache.clear_active(plug_id);
            });
            return Ok(());
        };
        surelock::key::lock_scope(|key| {
            let (mut cache, _key) = key.lock(&self.cache);
            cache.set_active(plug_id, heads.clone(), manifest);
        });
        Ok(())
    }
}
