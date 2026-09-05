//! Plug config events: the two-version diff over the PlugConfig facet's
//! automerge history, the revisioned store exposing it, and the consumers.
//!
//! The event producer is a struct diff, never an automerge patch interpreter
//! (patches surface dead intermediary state; see the tables.rs
//! `diff_events`/`events_for_patch` split for the two-use pattern — here the
//! shared diff path serves both the revisioned store below and the live
//! consumers through the same `PlugsEvent` vocabulary).
//!
//! Durability: the facet-set walker cursor is the only durable state. The
//! revisioned reader folds the config facet's history from its seed on every
//! open (config revisions are rare, so the replay cost is a handful of
//! hydrations) and diffs consecutive versions, which keeps replay
//! deterministic: the same history always produces the same events.

use super::*;

use crate::drawer::{DrawerRepo, MaterializationChange};
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetReader, FacetSetRevisionStore, FacetSetSelector};
use crate::stores::FacetStore;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo as _;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use daybook_types::doc::{FacetKey, WellKnownFacetTag};
use sqlx_utils_rs::SqlCtx;

pub(crate) const PLUGS_CONFIG_CONSUMER_STATE_ID: &str = "@daybook/core/plugs-config-facet-set";

pub(crate) const PLUG_MANIFEST_CONSUMER_STATE_ID: &str = "@daybook/core/plugs-manifest-delta";

#[derive(Default)]
struct PendingManifestWakes {
    watchers: tokio::task::JoinSet<Res<(String, u64, MaterializationChange)>>,
    registrations: std::collections::HashMap<String, (u64, tokio::task::AbortHandle)>,
    next_id: u64,
}

impl PendingManifestWakes {
    fn remove(&mut self, plug_id: &str) {
        if let Some((_id, abort)) = self.registrations.remove(plug_id) {
            abort.abort();
        }
    }

    async fn watch(&mut self, drawer: &DrawerRepo, plug_id: &str, ref_url: &url::Url) -> Res<bool> {
        if self.registrations.contains_key(plug_id) {
            return Ok(false);
        }
        let parsed = crate::plugs::PlugsRepo::parse_enabled_ref(ref_url)?;
        let branch_id = daybook_types::doc::BranchId(parsed.doc_id.to_string());
        let mut wake = drawer
            .subscribe_document_materialization(&branch_id)
            .await?;
        let registration_id = self.next_id;
        self.next_id += 1;
        let plug_id = plug_id.to_owned();
        let task_plug_id = plug_id.clone();
        let abort = self.watchers.spawn(async move {
            wake.ready_changed()
                .await
                .map(|change| (task_plug_id, registration_id, change))
        });
        self.registrations.insert(plug_id, (registration_id, abort));
        Ok(true)
    }

    async fn next(&mut self) -> Res<(String, MaterializationChange)> {
        loop {
            let Some(result) = self.watchers.join_next().await else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            match result {
                Ok(Ok((plug_id, registration_id, change))) => {
                    if self
                        .registrations
                        .get(&plug_id)
                        .is_some_and(|(current_id, _)| *current_id == registration_id)
                    {
                        self.registrations.remove(&plug_id);
                        return Ok((plug_id, change));
                    }
                }
                Ok(Err(error)) => return Err(error),
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(ferr!("pending manifest wake task failed: {error}")),
            }
        }
    }
}

/// ADR 007 §7: the typed plug config events. Emitted by diffing consecutive
/// PlugConfig facet versions; consumed by the live cache maintenance, the FFI
/// listener bridge, and (via the revisioned store) the pin worker's
/// enablement-driven inventory maintenance.
///
/// No `origin` field: per the tables.rs convention, live/replay origin
/// filtering is a consumer-side concern (`should_skip_live_patch`), not event
/// payload — a replayed history cannot know an origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlugsEvent {
    /// The config facet gained an enabled entry for the plug at the pinned
    /// manifest heads. Also the pending→active transition shape (ADR 007 §6):
    /// the entry existed before, but the manifest only just became readable.
    PlugEnabled {
        plug_id: String,
        heads: ChangeHashSet,
    },
    /// The config facet's enabled entry for the plug was removed.
    PlugDisabled { plug_id: String },
    /// The enabled entry was re-pinned to different manifest heads.
    PlugUpdated {
        plug_id: String,
        heads: ChangeHashSet,
    },
    /// The config facet moved without an enabled-set change (known-plug
    /// recording, config doc id updates). Known-but-disabled manifest changes
    /// are otherwise invisible (ADR 007 §7).
    PlugsConfigChanged { heads: ChangeHashSet },
}

/// One config-facet revision's diff result, as delivered by
/// [`PlugsConfigEventStore`]. Revisions that do not touch the config facet
/// carry empty events so consumers can settle the cursor continuously.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlugsConfigRevision {
    pub heads: ChangeHashSet,
    /// The hydrated config facet value at this revision's heads. Consumers
    /// install it as the config store's in-memory snapshot so later local
    /// mutations build patches on fresh state.
    pub config: PlugsConfig,
    pub events: Vec<PlugsEvent>,
}

/// Diff two PlugConfig versions into typed events. Pure — no I/O, no cache,
/// no store writes; both the revisioned reader and live-state consumers share
/// this one path.
///
/// Enabled-set changes produce the specific events. A config that moved
/// without any enabled-set change (known-plug recording, config doc ids)
/// produces [`PlugsEvent::PlugsConfigChanged`] only.
pub(crate) fn diff_plug_config(
    from: Option<&PlugsConfig>,
    to: &PlugsConfig,
    heads: ChangeHashSet,
) -> Vec<PlugsEvent> {
    let mut events = Vec::new();
    let from_enabled = from.map(|config| &config.enabled);
    for (plug_id, ref_url) in &to.enabled {
        let prev_ref = from_enabled.and_then(|enabled| enabled.get(plug_id));
        let pinned_heads = ref_pinned_heads(ref_url);
        match prev_ref {
            None => events.push(PlugsEvent::PlugEnabled {
                plug_id: plug_id.clone(),
                heads: pinned_heads,
            }),
            Some(prev) if prev != ref_url => events.push(PlugsEvent::PlugUpdated {
                plug_id: plug_id.clone(),
                heads: pinned_heads,
            }),
            Some(_) => {}
        }
    }
    if let Some(from) = from {
        for plug_id in from.enabled.keys() {
            if !to.enabled.contains_key(plug_id) {
                events.push(PlugsEvent::PlugDisabled {
                    plug_id: plug_id.clone(),
                });
            }
        }
        // The config moved but the enabled set did not: known-plug recording
        // or config doc id drift. Specific events win; this is the residue.
        if events.is_empty() && from != to {
            events.push(PlugsEvent::PlugsConfigChanged { heads });
        }
    }
    events
}

/// The manifest heads pinned by an enabled ref (empty set when unpinned —
/// the ref then tracks the branch's current heads).
fn ref_pinned_heads(ref_url: &url::Url) -> ChangeHashSet {
    PlugsRepo::parse_enabled_ref(ref_url)
        .ok()
        .and_then(|parsed| parsed.at)
        .and_then(|at| am_utils_rs::parse_commit_heads(at.as_ref()).ok())
        .map(ChangeHashSet)
        .unwrap_or_default()
}

/// The plugs revision store: typed config events over the PlugConfig facet's
/// automerge history. The source is the facet-set store filtered to the
/// config route; each entry is one config revision's hydrated value plus the
/// diff events against the previous revision.
///
/// The reader folds the config history from its seed on every open — the
/// diff is a pure function of the history, so replay is deterministic and the
/// facet-set cursor is the only durability.
pub struct PlugsConfigEventStore {
    facet_set_store: Arc<FacetSetRevisionStore>,
    drawer: Arc<DrawerRepo>,
    doc_config_id: daybook_types::doc::DocId,
    route: crate::index::FacetRouteKey,
}

impl PlugsConfigEventStore {
    pub(crate) fn new(
        facet_set_store: Arc<FacetSetRevisionStore>,
        drawer: Arc<DrawerRepo>,
        plugs_repo: &PlugsRepo,
    ) -> Self {
        Self {
            facet_set_store,
            drawer,
            doc_config_id: plugs_repo.config_doc_id(),
            route: plugs_repo.config_facet_route(),
        }
    }

    fn config_facet_key() -> FacetKey {
        <PlugsConfig as crate::stores::FacetStore>::facet_key()
    }

    /// Hydrate the config facet value at one revision's branch heads.
    async fn hydrate_config_at(&self, heads: &ChangeHashSet) -> Res<PlugsConfig> {
        let Some(doc) = self
            .drawer
            .get_doc_with_facets_at_branch_heads(
                &self.doc_config_id,
                daybook_types::doc::BranchPath::new("main"),
                heads,
                Some(vec![Self::config_facet_key()]),
            )
            .await?
        else {
            // The config doc is a core doc; a revision that touched its
            // config facet must be hydratable at those heads.
            eyre::bail!("config doc not readable at revision heads {heads:?}");
        };
        let Some(raw) = doc.facets.get(&Self::config_facet_key()) else {
            return Ok(PlugsConfig::seed());
        };
        Ok(serde_json::from_value(raw.clone())?)
    }
}

#[async_trait]
impl RevisionedStore for PlugsConfigEventStore {
    type Revision = u64;
    type Entry = PlugsConfigRevision;
    type Selector = ();
    type Error = eyre::Report;
    type Reader<'a>
        = PlugsConfigEventReader<'a>
    where
        Self: 'a;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.facet_set_store
            .latest_revision()
            .await
            .map_err(|error| ferr!("reading facet-set revision: {error}"))
    }

    async fn open<'a>(
        &'a self,
        _selector: Self::Selector,
        after: Self::Revision,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        // The inner reader replays from the facet history's beginning: the
        // diff folds from the seed regardless of `after`, and only entries
        // past `after` are yielded.
        let inner = self
            .facet_set_store
            .open(
                FacetSetSelector::Routes([self.route.clone()].into_iter().collect()),
                0,
            )
            .await
            .map_err(|error| ferr!("opening facet-set reader: {error}"))?;
        Ok(PlugsConfigEventReader {
            inner,
            store: self,
            after,
            last_config: PlugsConfig::seed(),
            last_heads: None,
        })
    }
}

/// Fold the config facet's history into typed diff events. `last_config` is
/// the previous revision's hydrated value; every revision — including ones at
/// or before the consumer's cursor — advances it so replay stays faithful.
pub struct PlugsConfigEventReader<'a> {
    inner: FacetSetReader<'a>,
    store: &'a PlugsConfigEventStore,
    after: u64,
    last_config: PlugsConfig,
    last_heads: Option<ChangeHashSet>,
}

#[async_trait]
impl RevisionedStoreReader<u64, PlugsConfigRevision, eyre::Report> for PlugsConfigEventReader<'_> {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, PlugsConfigRevision>, eyre::Report> {
        loop {
            match self
                .inner
                .next(RevisionReadLimits {
                    max_entries: limits.max_entries,
                })
                .await
                .map_err(|error| ferr!("reading facet-set stream: {error}"))?
            {
                RevisionRead::ReplayComplete { through } => {
                    return Ok(RevisionRead::ReplayComplete { through });
                }
                RevisionRead::Entries { revision, entries } => {
                    let mut events = Vec::new();
                    for delta in &entries {
                        let Some(heads) = delta.current_branch_heads.clone() else {
                            // The config doc was removed; the facet-set store
                            // tracks its route, so treat the value as cleared.
                            let heads = self.last_heads.clone().unwrap_or_default();
                            let config = PlugsConfig::seed();
                            events.extend(diff_plug_config(
                                Some(&self.last_config),
                                &config,
                                heads.clone(),
                            ));
                            self.last_config = config;
                            self.last_heads = Some(heads);
                            continue;
                        };
                        let config = self.store.hydrate_config_at(&heads).await?;
                        events.extend(diff_plug_config(
                            Some(&self.last_config),
                            &config,
                            heads.clone(),
                        ));
                        self.last_config = config;
                        self.last_heads = Some(heads);
                    }
                    let entry = PlugsConfigRevision {
                        heads: self.last_heads.clone().unwrap_or_default(),
                        config: self.last_config.clone(),
                        events,
                    };
                    if revision > self.after {
                        return Ok(RevisionRead::Entries {
                            revision,
                            entries: vec![entry],
                        });
                    }
                    // At or before the consumer's cursor: fold only, yield
                    // nothing. The loop continues so live revisions are not
                    // held back by replay catch-up.
                }
            }
        }
    }
}

/// Consume the config facet's event stream: apply each revision's events to
/// the derived cache (the materialization side effects only — the diff itself
/// is pure) and publish them to live subscribers. A materialization wake
/// re-evaluates enabled-but-unreadable refs, emitting `PlugEnabled` for
/// pending→active transitions (ADR 007 §6: pending resolution is exactly a
/// new manual enablement, only the trigger differs).
pub(crate) async fn spawn_plugs_config_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    drawer: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sql: SqlCtx,
    parent_cancel_token: CancellationToken,
) -> Res<crate::repos::RepoStopToken> {
    let state = SqliteDeltaWalkerStateRepo::new(
        sql.read_pool.clone(),
        sql.write_pool.clone(),
        PLUGS_CONFIG_CONSUMER_STATE_ID,
        "facets",
    )
    .await?;
    let event_store = Arc::new(PlugsConfigEventStore::new(
        facet_set_store,
        Arc::clone(&drawer),
        &plugs_repo,
    ));
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let worker_handle = tokio::spawn(async move {
        let durable = state
            .progress()
            .await
            .expect(ERROR_IMPOSSIBLE)
            .upstream_revision;
        let reader = event_store.open((), durable).await.expect(ERROR_IMPOSSIBLE);
        let mut walker: SerialDeltaWalker<'_, PlugsConfigEventStore, _> =
            SerialDeltaWalker::open(reader, &state)
                .await
                .expect(ERROR_IMPOSSIBLE);
        let mut pending_wakes = PendingManifestWakes::default();
        loop {
            let read: RevisionRead<u64, PlugsConfigRevision> = tokio::select! {
                biased;
                _ = worker_cancel_token.cancelled() => break,
                wake = pending_wakes.next() => {
                    let (plug_id, _change) = wake.expect(ERROR_IMPOSSIBLE);
                    let active = plugs_repo
                        .resolve_pending_enabled_plug(&plug_id)
                        .await
                        .expect(ERROR_IMPOSSIBLE);
                    if active {
                        pending_wakes.remove(&plug_id);
                    } else if let Some((_, ref_url)) = plugs_repo
                        .list_pending()
                        .await
                        .into_iter()
                        .find(|(pending_id, _)| pending_id == &plug_id)
                    {
                        pending_wakes
                            .watch(&drawer, &plug_id, &ref_url)
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                    }
                    continue;
                }
                read = walker.next() => read.expect(ERROR_IMPOSSIBLE),
            };
            match read {
                RevisionRead::ReplayComplete { .. } => {
                    refresh_pending_manifest_wakes(&drawer, &plugs_repo, &mut pending_wakes)
                        .await
                        .expect(ERROR_IMPOSSIBLE);
                }
                RevisionRead::Entries { revision, entries } => {
                    for entry in entries {
                        plugs_repo
                            .apply_config_revision(&entry)
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                    }
                    walker.settle(revision).await.expect(ERROR_IMPOSSIBLE);
                    refresh_pending_manifest_wakes(&drawer, &plugs_repo, &mut pending_wakes)
                        .await
                        .expect(ERROR_IMPOSSIBLE);
                }
            }
        }
    });
    Ok(crate::repos::RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

async fn refresh_pending_manifest_wakes(
    drawer: &DrawerRepo,
    plugs_repo: &PlugsRepo,
    pending_wakes: &mut PendingManifestWakes,
) -> Res<()> {
    let pending = plugs_repo.list_pending().await;
    let pending_ids: std::collections::HashSet<_> = pending
        .iter()
        .map(|(plug_id, _)| plug_id.as_str())
        .collect();
    let stale = pending_wakes
        .registrations
        .keys()
        .filter(|plug_id| !pending_ids.contains(plug_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for plug_id in stale {
        pending_wakes.remove(&plug_id);
    }
    for (plug_id, ref_url) in pending {
        if pending_wakes.watch(drawer, &plug_id, &ref_url).await? {
            if plugs_repo.resolve_pending_enabled_plug(&plug_id).await? {
                pending_wakes.remove(&plug_id);
            }
        }
    }
    Ok(())
}

/// Record remote manifest docs into the config facet's known-plug registry
/// (version/compat gates and rejections live in `record_known_manifest_doc`).
/// This consumer only drives that recording — the derived cache and the
/// pending→active resolution belong to the config event consumer above.
pub(crate) async fn spawn_facet_set_plugs_manifest_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    manifest_sql: SqlCtx,
    parent_cancel_token: CancellationToken,
) -> Res<crate::repos::RepoStopToken> {
    let state = SqliteDeltaWalkerStateRepo::new(
        manifest_sql.read_pool.clone(),
        manifest_sql.write_pool.clone(),
        PLUG_MANIFEST_CONSUMER_STATE_ID,
        "facets",
    )
    .await
    .map_err(|error| ferr!("initializing Plugs manifest walker state: {error}"))?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let worker_handle = tokio::spawn(async move {
        run_facet_set_plugs_manifest_consumer(
            facet_set_store,
            plugs_repo,
            state,
            worker_cancel_token,
        )
        .await
        .unwrap();
    });
    Ok(crate::repos::RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

async fn run_facet_set_plugs_manifest_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    state: SqliteDeltaWalkerStateRepo,
    cancel_token: CancellationToken,
) -> Res<()> {
    let durable = state.progress().await?.upstream_revision;
    let reader = facet_set_store
        .open(
            FacetSetSelector::Tag(WellKnownFacetTag::PlugManifest),
            durable,
        )
        .await
        .map_err(|error| ferr!("opening Plugs manifest FacetSet reader: {error}"))?;
    let mut walker: SerialDeltaWalker<'_, FacetSetRevisionStore, SqliteDeltaWalkerStateRepo> =
        SerialDeltaWalker::open(reader, &state)
            .await
            .map_err(|error| ferr!("opening Plugs manifest FacetSet walker: {error}"))?;
    loop {
        let read: RevisionRead<u64, FacetDelta> = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Ok(()),
            read = walker.next() => read.map_err(|error| ferr!("reading Plugs manifest FacetSet walker: {error}"))?,
        };
        match read {
            RevisionRead::ReplayComplete { .. } => {}
            RevisionRead::Entries { revision, entries } => {
                for delta in entries {
                    if delta.key.facet_key != FacetKey::from(WellKnownFacetTag::PlugManifest)
                        || delta.key.branch_id.0.as_str() != delta.key.document_id.as_str()
                    {
                        continue;
                    }
                    // The record gate is idempotent per (doc, heads); the
                    // config track is the durable registry, not a cache.
                    let Some(snapshot) = &delta.current else {
                        continue;
                    };
                    plugs_repo
                        .record_known_manifest_doc(&delta.key.document_id, &snapshot.branch_heads)
                        .await?;
                }
                walker
                    .settle(revision)
                    .await
                    .map_err(|error| ferr!("settling Plugs manifest FacetSet walker: {error}"))?;
            }
        }
    }
}
