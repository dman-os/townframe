use super::mutations::RecordKnownOutcome;
use super::*;

use crate::drawer::DrawerRepo;
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use daybook_types::doc::{FacetKey, WellKnownFacetTag};
use sqlx_utils_rs::SqlCtx;

pub(crate) const PLUGS_CONFIG_CONSUMER_STATE_ID: &str = "@daybook/core/plugs-config-facet-set";

pub(crate) struct PlugsConfigFacetSetConsumerStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PlugsConfigFacetSetConsumerStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

pub(crate) async fn spawn_facet_set_plugs_config_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    drawer: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sql: SqlCtx,
    parent_cancel_token: CancellationToken,
) -> Res<PlugsConfigFacetSetConsumerStopToken> {
    let state = SqliteDeltaWalkerStateRepo::new(
        sql.read_pool.clone(),
        sql.write_pool.clone(),
        PLUGS_CONFIG_CONSUMER_STATE_ID,
        "facets",
    )
    .await?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let route = plugs_repo.config_facet_route();
    let worker_handle = tokio::spawn(async move {
        let mut walker = SerialDeltaWalker::open(
            facet_set_store.as_ref(),
            &state,
            FacetSetSelector::Routes([route.clone()].into_iter().collect()),
            RevisionReadLimits::default(),
        )
        .await
        .expect(ERROR_IMPOSSIBLE);
        loop {
            let read = tokio::select! {
                biased;
                _ = worker_cancel_token.cancelled() => break,
                read = walker.next() => read.expect(ERROR_IMPOSSIBLE),
            };
            match read {
                RevisionRead::ReplayComplete { .. } => {}
                RevisionRead::Entries { revision, entries } => {
                    for delta in entries {
                        assert_eq!(
                            delta.key, route,
                            "unexpected route in PlugsConfig FacetSet consumer"
                        );
                        plugs_repo
                            .process_config_facet_delta(&drawer, delta)
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                    }
                    walker.settle(revision).await.expect(ERROR_IMPOSSIBLE);
                }
            }
        }
    });
    Ok(PlugsConfigFacetSetConsumerStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

pub(crate) const PLUG_MANIFEST_CONSUMER_STATE_ID: &str = "@daybook/core/plugs-manifest-delta";

pub(crate) struct PlugsManifestConsumerStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PlugsManifestConsumerStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

/// Consume PlugManifest facet membership from the durable FacetSet source.
/// Manifest values are hydrated at the exact membership heads before the
/// existing PlugsRepo reconciliation updates its cache and frontier.
pub(crate) async fn spawn_facet_set_plugs_manifest_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    drawer: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    manifest_sql: SqlCtx,
    parent_cancel_token: CancellationToken,
) -> Res<PlugsManifestConsumerStopToken> {
    let state = SqliteDeltaWalkerStateRepo::new(
        manifest_sql.read_pool.clone(),
        manifest_sql.write_pool.clone(),
        PLUG_MANIFEST_CONSUMER_STATE_ID,
        "facets",
    )
    .await
    .map_err(|error| ferr!("initializing Plugs manifest walker state: {error}"))?;
    let wake = drawer.subscribe_materialization_wake(None).await?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let worker_handle = tokio::spawn(async move {
        run_facet_set_plugs_manifest_consumer(
            facet_set_store,
            drawer,
            plugs_repo,
            state,
            wake,
            worker_cancel_token,
        )
        .await
        .unwrap();
    });
    Ok(PlugsManifestConsumerStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

async fn run_facet_set_plugs_manifest_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    drawer: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    state: SqliteDeltaWalkerStateRepo,
    mut wake: crate::drawer::MaterializationWake,
    cancel_token: CancellationToken,
) -> Res<()> {
    let mut walker = SerialDeltaWalker::open(
        facet_set_store.as_ref(),
        &state,
        FacetSetSelector::Tag(WellKnownFacetTag::PlugManifest),
        RevisionReadLimits::default(),
    )
    .await
    .map_err(|error| ferr!("opening Plugs manifest FacetSet walker: {error}"))?;
    let mut deferred: Option<(u64, Vec<FacetDelta>)> = None;
    loop {
        let read = if let Some((revision, entries)) = deferred.take() {
            RevisionRead::Entries { revision, entries }
        } else {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                read = walker.next() => read.map_err(|error| ferr!("reading Plugs manifest FacetSet walker: {error}"))?,
            }
        };
        match read {
            RevisionRead::ReplayComplete { .. } => {}
            RevisionRead::Entries { revision, entries } => {
                if !apply_manifest_entries(&drawer, &plugs_repo, &entries).await? {
                    deferred = Some((revision, entries));
                } else {
                    walker.settle(revision).await.map_err(|error| {
                        ferr!("settling Plugs manifest FacetSet walker: {error}")
                    })?;
                }
            }
        }
        if deferred.is_some() {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                result = wake.wait() => result?,
            }
        }
    }
}

async fn apply_manifest_entries(
    drawer: &DrawerRepo,
    plugs_repo: &Arc<PlugsRepo>,
    entries: &[FacetDelta],
) -> Res<bool> {
    let manifest_key = FacetKey::from(WellKnownFacetTag::PlugManifest);
    for delta in entries {
        if delta.key.facet_key != manifest_key
            || delta.key.branch_id.0.as_str() != delta.key.document_id.as_str()
        {
            continue;
        }
        let Some(snapshot) = &delta.current else {
            plugs_repo
                .process_manifest_doc_tombstone(&delta.key.document_id)
                .await?;
            continue;
        };
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch_heads(
                &delta.key.document_id,
                daybook_types::doc::BranchPath::new("main"),
                &snapshot.branch_heads,
                Some(vec![manifest_key.clone()]),
            )
            .await?
        else {
            return Ok(false);
        };
        let Some(raw) = doc.facets.get(&manifest_key) else {
            continue;
        };
        drop(serde_json::from_value::<
            daybook_types::manifest::PlugManifest,
        >(raw.clone())?);
        plugs_repo
            .process_manifest_doc_change(&delta.key.document_id, &snapshot.branch_heads)
            .await?;
    }
    Ok(true)
}

impl PlugsRepo {
    /// Reconcile derived cache state from two config versions.
    async fn apply_config_diff(&self, prev: Option<&PlugsConfig>, cur: &PlugsConfig) -> Res<()> {
        // known_plugs: added/changed last_valid refs refresh the manifest
        // cache for that plug only; removed entries drop it. A rejected
        // latest or a last_enabled_version-only change has no cache effect
        // (the cache materializes valid versions only).
        for (id, track) in &cur.known_plugs {
            let changed = prev.as_ref().is_none_or(|plug| {
                plug.known_plugs.get(id).map(|old| &old.last_valid) != Some(&track.last_valid)
            });
            if changed
                && let Some((_, manifest)) = self.read_manifest_at_ref(&track.last_valid).await?
            {
                surelock::key::lock_scope(|key| {
                    let (mut cache, _key) = key.lock(&self.cache);
                    cache.upsert_known(id, &manifest);
                });
            }
        }
        if let Some(prev) = prev {
            for id in prev.known_plugs.keys() {
                if !cur.known_plugs.contains_key(id) {
                    surelock::key::lock_scope(|key| {
                        let (mut cache, _key) = key.lock(&self.cache);
                        cache.drop_known(id);
                    });
                }
            }
        }
        // enabled: the cache is only the materialization side effect.
        for (id, ref_url) in &cur.enabled {
            let prev_ref = prev.as_ref().and_then(|plug| plug.enabled.get(id));
            if prev_ref == Some(ref_url) {
                continue; // unchanged — no event, no read
            }
            self.activate_from_ref(id, ref_url).await?;
        }
        if let Some(prev) = prev {
            for id in prev.enabled.keys() {
                if !cur.enabled.contains_key(id) {
                    surelock::key::lock_scope(|key| {
                        let (mut cache, _key) = key.lock(&self.cache);
                        cache.clear_active(id);
                    });
                }
            }
        }
        Ok(())
    }

    /// ADR 007 §7: a manifest doc moved (remote). Re-record the known ref
    /// (which updates the derived cache incrementally for this plug) and
    /// resolve a pending plug whose pinned heads became readable.
    pub(crate) async fn process_config_facet_delta(
        &self,
        _drawer: &DrawerRepo,
        _delta: crate::index::FacetDelta,
    ) -> Res<()> {
        // FacetSet events are dirty hints, not snapshots to install. Serialize
        // reconciliation with local plug commands, then load the config at one
        // exact current drawer frontier. This makes stale, coalesced, local,
        // and out-of-order events converge through Automerge resolution.
        let _guard = self.mutation_mutex.lock().await;
        let store = self.config_store()?;
        let previous = store.query_sync(Clone::clone).await;
        let (current, current_heads) = store.latest_snapshot().await?;
        self.apply_config_diff(Some(&previous), &current).await?;
        store
            .apply_external_snapshot(current, current_heads.unwrap_or_default())
            .await?;
        self.reconcile_revision_frontier().await?;
        Ok(())
    }

    async fn process_manifest_doc_change(
        &self,
        doc_id: &daybook_types::doc::DocId,
        new_heads: &ChangeHashSet,
    ) -> Res<()> {
        match self.record_known_manifest_doc(doc_id, new_heads).await? {
            RecordKnownOutcome::Recorded { plug_id } => {
                // Pending resolution: if the plug is enabled but not yet
                // materialized, its pinned heads may have just become
                // readable — activate it. An already-active plug was handled
                // synchronously by its own mutator; re-resolving it here from
                // a possibly-stale enabled ref could revert or drop the
                // cache entry, so only touch plugs that are genuinely pending.
                let enabled_ref = self
                    .config_store()?
                    .query_sync(|config| config.enabled.get(&plug_id).cloned())
                    .await;
                let is_pending = surelock::key::lock_scope(|key| {
                    let (cache, _key) = key.lock(&self.cache);
                    !cache.active_manifests.contains_key(&plug_id)
                });
                if let Some(ref_url) = enabled_ref.filter(|_| is_pending) {
                    // Pending -> active: the pinned heads became readable.
                    self.activate_from_ref(&plug_id, &ref_url).await?;
                    self.reconcile_revision_frontier().await?;
                }
            }
            RecordKnownOutcome::Rejected {
                plug_id,
                version,
                reason,
            } => {
                tracing::warn!(
                    plug_id,
                    version = %version,
                    reason,
                    "manifest update rejected by version/compat gate"
                );
            }
            RecordKnownOutcome::Unreadable => {}
        }
        Ok(())
    }

    /// Remove the derived manifest state for a manifest document whose
    /// current facet membership was tombstoned. The config track remains the
    /// durable history of the plug; a later readable manifest revision can
    /// repopulate the cache through the same consumer.
    async fn process_manifest_doc_tombstone(&self, doc_id: &daybook_types::doc::DocId) -> Res<()> {
        let _guard = self.mutation_mutex.lock().await;
        let tracked = self
            .config_store()?
            .query_sync(|config| {
                config
                    .known_plugs
                    .iter()
                    .map(|(plug_id, track)| (plug_id.clone(), track.last_valid.clone()))
                    .collect::<Vec<_>>()
            })
            .await;
        let plug_ids = tracked
            .into_iter()
            .filter_map(|(plug_id, ref_url)| {
                let parsed = Self::parse_enabled_ref(&ref_url)
                    .expect("known plug refs must be valid manifest refs");
                (&parsed.doc_id == doc_id).then_some(plug_id)
            })
            .collect::<Vec<_>>();
        if plug_ids.is_empty() {
            return Ok(());
        }
        let changed = surelock::key::lock_scope(|key| {
            let (mut cache, _key) = key.lock(&self.cache);
            plug_ids.into_iter().fold(false, |changed, plug_id| {
                let active_changed = cache.clear_active(&plug_id);
                let known_changed = cache.drop_known(&plug_id);
                changed || active_changed || known_changed
            })
        });
        if changed {
            self.reconcile_revision_frontier().await?;
        }
        Ok(())
    }
}
