use crate::interlude::*;
use tokio_util::sync::CancellationToken;

use daybook_types::manifest;

use self::cache::PlugsCache;

pub use self::events::PlugsEvent;
pub(crate) use self::events::{
    PLUG_MANIFEST_CONSUMER_STATE_ID, PLUGS_CONFIG_CONSUMER_STATE_ID, PlugsConfigEventStore,
    PlugsConfigRevision, spawn_facet_set_plugs_manifest_consumer, spawn_plugs_config_consumer,
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
                key_tag: WellKnownFacetTag::Jwk.into(),
                value_schema: schemars::schema_for!(daybook_types::doc::Jwk),
                display_config: default(),
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

/// Exact per-plug installation identity. Global config heads are only a read witness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlugActivationTarget {
    pub plug_id: String,
    pub enabled_ref: url::Url,
    pub enablement_entry_id: String,
    pub config_doc_id: daybook_types::doc::DocId,
    pub config_doc_heads: ChangeHashSet,
}

pub(crate) struct ProcessorActivationSnapshot {
    pub targets: HashMap<String, PlugActivationTarget>,
    pub manifests: HashMap<String, Arc<manifest::PlugManifest>>,
    pub rejected: HashMap<String, String>,
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
    /// Live tap of the config-facet event stream. The durable stream is
    /// [`PlugsConfigEventStore`]; this broadcast carries the same events plus
    /// pending→active transitions, which are live-only (they depend on local
    /// materialization state, so replay cannot reproduce them).
    pub(crate) events_tx: tokio::sync::broadcast::Sender<PlugsEvent>,
}

/// FFI-facing projection of [`PlugsEvent`]: the named `Clone` record
/// cross-language listeners need. Enabled/updated map to `active`, disabled
/// to `!active`; config-only changes carry no plug id and are not surfaced.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PlugsWatchChange {
    pub plug_id: String,
    pub active: bool,
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

/// Whether a config revision describes the state the store already holds.
///
/// Heads equality is the only evidence available: a revision whose heads match
/// the store's current heads is the state we are already on, so its hydrated
/// snapshot is installed as-is. Anything else — stale, coalesced, local, or
/// out-of-order revisions — is discarded in favour of the store's own drawer
/// state, and the revision's snapshot is not installed: the hydrated snapshot
/// is what later local mutations build patches on, which is how those revisions
/// converge at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotDecision {
    /// Revision heads equal the store's: install the revision's snapshot.
    Install,
    /// Heads differ, or the store has no snapshot yet: keep the store's own
    /// state and discard the revision's snapshot.
    Discard,
}

/// Decide a revision's snapshot against the store's current heads.
pub(crate) fn snapshot_decision(
    current_heads: Option<&ChangeHashSet>,
    revision: &PlugsConfigRevision,
) -> SnapshotDecision {
    if current_heads == Some(&revision.heads) {
        SnapshotDecision::Install
    } else {
        SnapshotDecision::Discard
    }
}

/// The derived-cache work one revision event needs before it is announced.
///
/// Every revision event is announced either way; this says what, if anything,
/// must land in the cache first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RevisionAction {
    /// Nothing to project — the event is announced as it stands.
    AnnounceOnly,
    /// Drop the plug's active projection. A plug the config just disabled must
    /// not still be active for a subscriber reacting to its own `PlugDisabled`.
    ClearActive { plug_id: String },
    /// Materialize the plug's active projection from the ref the current config
    /// holds for it.
    Activate { plug_id: String, ref_url: url::Url },
    /// Rebuild the known-manifest projection from the current config.
    RefreshKnown,
}

/// Decide one revision event's cache work against the config it is applied to.
///
/// An enablement event whose plug has no ref in the current config projects
/// nothing — the config the revision was diffed from may already have moved
/// past it — but it is still announced: the announcement is the revision's
/// event, not the action's.
pub(crate) fn revision_action(config: &PlugsConfig, event: &PlugsEvent) -> RevisionAction {
    match event {
        PlugsEvent::PlugEnabled { plug_id, .. } | PlugsEvent::PlugUpdated { plug_id, .. } => {
            match config.enabled.get(plug_id) {
                Some(ref_url) => RevisionAction::Activate {
                    plug_id: plug_id.clone(),
                    ref_url: ref_url.clone(),
                },
                None => RevisionAction::AnnounceOnly,
            }
        }
        PlugsEvent::PlugDisabled { plug_id } => RevisionAction::ClearActive {
            plug_id: plug_id.clone(),
        },
        PlugsEvent::PlugsConfigChanged { .. } => RevisionAction::RefreshKnown,
    }
}

/// A publication of one revision event, observed with the projection it is
/// announced under.
///
/// This is what makes "the projection is applied before the event that
/// announces it" structural rather than a statement order inside a longer
/// method: `announce` cannot be called without the cache in hand, and every
/// call site hands it over after the mutation.
pub(crate) trait EventSink {
    fn announce(&mut self, cache: &PlugsCache, event: &PlugsEvent);
}

/// Clear a plug's active projection, then announce the event that required it.
pub(crate) fn clear_active_and_announce(
    cache: &mut PlugsCache,
    plug_id: &str,
    event: &PlugsEvent,
    sink: &mut impl EventSink,
) {
    cache.clear_active(plug_id);
    sink.announce(cache, event);
}

/// Announce the pending→active edge of an enabled ref that just became
/// readable. Returns whether it announced anything.
///
/// Only the inactive→active edge announces: an already-active plug stays
/// silent. That is what keeps the live tap and the durable stream consistent —
/// pending→active depends on local materialization state, so replay cannot
/// reproduce it, and re-announcing it per boot would make the two disagree.
pub(crate) fn announce_pending_activation(
    cache: &PlugsCache,
    was_active: bool,
    plug_id: &str,
    sink: &mut impl EventSink,
) -> bool {
    if was_active {
        return false;
    }
    let Some((heads, _manifest)) = cache.active_manifests.get(plug_id) else {
        // Still pending: enabled, but not readable at its pinned heads.
        return false;
    };
    sink.announce(
        cache,
        &PlugsEvent::PlugEnabled {
            plug_id: plug_id.to_owned(),
            heads: heads.clone(),
        },
    );
    true
}

/// The live tap as an [`EventSink`]. Publication does not read the cache, so
/// the projection handed in is unused here; it is in the signature so no caller
/// can announce ahead of the cache work.
struct PlugsRepoEventSink<'a>(&'a PlugsRepo);

impl EventSink for PlugsRepoEventSink<'_> {
    fn announce(&mut self, _cache: &PlugsCache, event: &PlugsEvent) {
        self.0.publish_event(event.clone());
    }
}

#[expect(clippy::clone_on_ref_ptr)]
impl PlugsRepo {
    /// Live tap of the config event stream: the same [`PlugsEvent`]s the
    /// revisioned store replays, plus pending→active transitions (live-only,
    /// see [`PlugsRepo::events_tx`]).
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<PlugsEvent> {
        self.events_tx.subscribe()
    }

    fn publish_event(&self, event: PlugsEvent) {
        // Broadcast with zero receivers is a no-op, not an error to surface:
        // subscribers come and go, and the durable stream is the event store.
        drop(self.events_tx.send(event));
    }

    /// Announce one event, handing the sink the projection it is announced
    /// under. Together with [`clear_active_and_announce`] this is the only path
    /// to [`PlugsRepoEventSink`], so no call site can announce ahead of the
    /// cache work it belongs to.
    fn announce_applied(&self, sink: &mut PlugsRepoEventSink<'_>, event: &PlugsEvent) {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            sink.announce(&cache, event);
        });
    }

    /// Apply one config revision's events to the derived cache and publish
    /// them. The revision's hydrated config is installed as the config store's
    /// in-memory snapshot first, so subsequent local mutations build patches
    /// on fresh state (stale, coalesced, local, and out-of-order revisions all
    /// converge through the hydrated snapshot).
    ///
    /// The three rules this method exists to uphold live in their own
    /// functions so they can be exercised without a BigRepo: the snapshot gate
    /// is [`snapshot_decision`], the per-event cache work is [`revision_action`],
    /// and the projection-before-announcement order is [`EventSink`].
    pub(crate) async fn apply_config_revision(&self, revision: &PlugsConfigRevision) -> Res<()> {
        let _guard = self.mutation_mutex.lock().await;
        let store = self.config_store()?;
        let describe_tracks = |config: &PlugsConfig| {
            config
                .known_plugs
                .iter()
                .map(|(id, track)| {
                    (
                        id.clone(),
                        track.latest_version.clone(),
                        track.last_enabled_version.clone(),
                        track.latest_rejection.is_some(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let before_tracks = store.query_sync(describe_tracks).await;
        tracing::debug!(
            revision_heads = ?revision.heads,
            events = ?revision.events,
            ?before_tracks,
            "applying plugs config revision"
        );
        let (_, current_heads) = store.latest_snapshot().await?;
        match snapshot_decision(current_heads.as_ref(), revision) {
            SnapshotDecision::Install => {
                store
                    .apply_external_snapshot(revision.config.clone(), revision.heads.clone())
                    .await?;
            }
            SnapshotDecision::Discard => {
                tracing::debug!(
                    revision_heads = ?revision.heads,
                    ?current_heads,
                    "discarding stale plugs config snapshot in favor of current drawer state"
                );
                store.reload().await?;
            }
        }
        let current_config = store.query_sync(|config| config.clone()).await;
        let after_tracks = store.query_sync(describe_tracks).await;
        tracing::debug!(?after_tracks, "applied plugs config revision snapshot");
        let mut sink = PlugsRepoEventSink(self);
        for event in &revision.events {
            match revision_action(&current_config, event) {
                RevisionAction::ClearActive { plug_id } => {
                    surelock::key::lock_scope(|key| {
                        let (mut cache, _key) = key.lock(&self.cache);
                        clear_active_and_announce(&mut cache, &plug_id, event, &mut sink);
                    });
                }
                RevisionAction::Activate { plug_id, ref_url } => {
                    self.activate_from_ref(&plug_id, &ref_url).await?;
                    self.announce_applied(&mut sink, event);
                }
                RevisionAction::RefreshKnown => {
                    self.refresh_known_cache().await?;
                    self.announce_applied(&mut sink, event);
                }
                RevisionAction::AnnounceOnly => {
                    self.announce_applied(&mut sink, event);
                }
            }
        }
        Ok(())
    }

    /// Re-materialize one enabled-but-unreadable ref; publish `PlugEnabled`
    /// when the manifest becomes readable.
    pub(crate) async fn resolve_pending_enabled_plug(&self, plug_id: &str) -> Res<bool> {
        let _guard = self.mutation_mutex.lock().await;
        let Some(ref_url) = self
            .config_store()?
            .query_sync(|config| config.enabled.get(plug_id).cloned())
            .await
        else {
            return Ok(false);
        };
        self.resolve_enabled_plug_locked(plug_id, &ref_url).await
    }

    async fn resolve_enabled_plug_locked(&self, plug_id: &str, ref_url: &url::Url) -> Res<bool> {
        let was_active = surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache.active_manifests.contains_key(plug_id)
        });
        self.activate_from_ref(plug_id, ref_url).await?;
        let mut sink = PlugsRepoEventSink(self);
        let is_active = surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            let is_active = cache.active_manifests.contains_key(plug_id);
            announce_pending_activation(&cache, was_active, plug_id, &mut sink);
            is_active
        });
        Ok(is_active)
    }

    /// Re-materialize the known-manifest cache from the durable config's
    /// known refs. The config-side rejections/versions stay as recorded.
    pub(crate) async fn refresh_known_cache(&self) -> Res<()> {
        let known = self
            .config_store()?
            .query_sync(|config| config.known_plugs.clone())
            .await;
        for (plug_id, track) in known {
            let Some((_, manifest)) = self.read_manifest_at_ref(&track.last_valid).await? else {
                continue;
            };
            surelock::key::lock_scope(|key| {
                let (mut cache, _key) = key.lock(&self.cache);
                cache.upsert_known(&plug_id, &manifest);
            });
        }
        Ok(())
    }

    pub async fn load(
        big_repo: SharedBigRepo,
        blobs: Arc<crate::blobs::BlobsRepo>,
        doc_config_id: DocumentId,
        _local_user_path: daybook_types::doc::UserPathBuf,
        // Kept for API stability: the retired plugs-revision frontier was the
        // only consumer of a local sqlite ctx here. The event store's walker
        // state lives with its consumer (PLUGS_CONFIG_CONSUMER_STATE_ID).
        _sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let cancel_token = CancellationToken::new();
        let (events_tx, _) = tokio::sync::broadcast::channel(256);

        let repo = Arc::new(Self {
            big_repo: Arc::clone(&big_repo),
            blobs,
            doc_config_id: daybook_types::doc::DocId::from(doc_config_id.to_string()),
            drawer: tokio::sync::OnceCell::new(),
            config_store: tokio::sync::OnceCell::new(),
            mutation_mutex: tokio::sync::Mutex::new(()),
            cancel_token: cancel_token.clone(),
            cache: surelock::mutex::Mutex::new(PlugsCache::default()),
            events_tx,
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
        // Register the config doc before loading its facet store. A reopened
        // DrawerRepo has no local registry entry yet; register_existing_doc is
        // idempotent and is required for FacetStoreHandle::load to read the
        // persisted config facet. The core invariant is established below,
        // after that store is attached.
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
        // Establish the core plug invariant at the plugs repo's boot boundary,
        // before exposing the attached repo to drawer callers. This is also
        // what makes fresh init independent of a later repo-init dance.
        self.ensure_core_plug().await?;
        // Warm the derived cache from the durable config so drawer facet
        // validation (and plug-init queueing at rt boot) works immediately on
        // this open — including a reopen where the facet-set consumers have
        // already settled the config/manifest revisions and will not re-apply
        // them. Unreadable non-core manifest refs are treated as pending
        // (deferred) exactly like the live consumer, never a fatal error.
        self.warm_cache().await?;
        Ok(())
    }

    /// Rebuild the derived cache from the durable config store: every
    /// known-manifest ref (materializes tag -> plug + tag -> facet manifest)
    /// and every enabled ref (pins the active manifest). This is the boot-time
    /// counterpart to the event consumer's incremental cache application; it is idempotent
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
