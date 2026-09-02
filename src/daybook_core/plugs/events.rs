use super::mutations::RecordKnownOutcome;
use super::*;

/// a doc change forwarded from the switch sink to the notif
/// loop
#[derive(Debug, Clone)]
pub(crate) enum PlugsNotif {
    ConfigDocChanged {
        prev_heads: Option<ChangeHashSet>,
        new_heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    ManifestDocChanged {
        doc_id: daybook_types::doc::DocId,
        new_heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
}

// Granular event enum for specific changes (ADR 007 §7: enabled-only).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PlugsEvent {
    /// Config entry added, or pending -> active (ADR 007 §6). Emitted for
    /// local enables and for remote config writes that add/change an enabled
    /// ref (via the notif loop's config diff).
    PlugEnabled {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// Config entry removed (local disable or remote drop of an enabled ref).
    PlugDisabled {
        id: String,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// The enabled ref was re-pinned to different heads (explicit update to
    /// a newer version). Same plug, new pinned version.
    EnabledPlugUpdated {
        id: String,
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// The plugs config facet moved to `heads`. A coarse state-version
    /// marker: fires on every config write whether or not a granular
    /// `PlugEnabled`/`PlugDisabled`/`EnabledPlugUpdated` fired (e.g. a
    /// remote known-manifest record has no plug-level event). Consumers that
    /// mirror the whole config (triage's processor refresh, the facet-ref
    /// index) act on any plugs event; the heads let a consumer re-read the
    /// config at a known point.
    PlugsConfigChanged {
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    /// A manifest update was rejected by the version/compat gate (ADR 007
    /// §5): republish without a version bump, downgrade, or a breaking
    /// change in a non-major update. The rejection is also durable in the
    /// config's per-plug track (latest + latest_rejection).
    ManifestRejected {
        id: String,
        version: String,
        reason: String,
        origin: crate::event_origin::SwitchEventOrigin,
    },
}

impl PlugsRepo {
    /// ADR 007 §7: diff two config versions of the write series into
    /// enabled-only events, maintaining the derived cache incrementally.
    /// Unchanged refs no-op via the fast paths in the cache helpers.
    async fn apply_config_diff(
        &self,
        prev: Option<&PlugsConfig>,
        cur: &PlugsConfig,
        out: &mut Vec<PlugsEvent>,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Res<()> {
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
        // enabled: the event type comes from the config delta — a ref that
        // was absent is `PlugEnabled`, a changed ref is
        // `EnabledPlugUpdated`, a removed ref is `PlugDisabled`. The cache
        // is only the materialization side effect.
        for (id, ref_url) in &cur.enabled {
            let prev_ref = prev.as_ref().and_then(|plug| plug.enabled.get(id));
            if prev_ref == Some(ref_url) {
                continue; // unchanged — no event, no read
            }
            if let Some(event) = self
                .activate_from_ref(id, ref_url, prev_ref.is_none(), origin)
                .await?
            {
                out.push(event);
            }
        }
        if let Some(prev) = prev {
            for id in prev.enabled.keys() {
                if !cur.enabled.contains_key(id) {
                    surelock::key::lock_scope(|key| {
                        let (mut cache, _key) = key.lock(&self.cache);
                        cache.clear_active(id);
                    });
                    out.push(PlugsEvent::PlugDisabled {
                        id: id.clone(),
                        origin: origin.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// ADR 007 §7: a manifest doc moved (remote). Re-record the known ref
    /// (which updates the derived cache incrementally for this plug) and
    /// resolve a pending plug whose pinned heads became readable.
    async fn process_manifest_doc_change(
        &self,
        doc_id: &daybook_types::doc::DocId,
        new_heads: &ChangeHashSet,
        origin: &crate::event_origin::SwitchEventOrigin,
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
                    if let Some(event) = self
                        .activate_from_ref(&plug_id, &ref_url, true, origin)
                        .await?
                    {
                        self.registry.notify([event]);
                    }
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
                self.registry.notify([PlugsEvent::ManifestRejected {
                    id: plug_id,
                    version: version.to_string(),
                    reason,
                    origin: origin.clone(),
                }]);
            }
            RecordKnownOutcome::Unreadable => {}
        }
        Ok(())
    }

    /// ADR 007 §7: the notif loop, fed by the switch sink. Config changes
    /// are processed as a series of facet write versions (drawer dmeta
    /// snapshots) — basic diffing between consecutive versions; manifest
    /// doc changes update the derived cache incrementally. No store reload.
    pub(crate) async fn notif_loop(
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
            match notif {
                PlugsNotif::ConfigDocChanged {
                    prev_heads,
                    new_heads,
                    origin,
                } => {
                    self.process_config_doc_change(prev_heads.as_ref(), &new_heads, &origin)
                        .await?;
                }
                PlugsNotif::ManifestDocChanged {
                    doc_id,
                    new_heads,
                    origin,
                } => {
                    self.process_manifest_doc_change(&doc_id, &new_heads, &origin)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// ADR 007 §7: a config facet change between the before/after heads.
    /// Enumerate the write versions (each with its author), skip local
    /// writes (applied synchronously by the mutators), and basic-diff each
    /// remaining version against the previous one.
    async fn process_config_doc_change(
        &self,
        prev_heads: Option<&ChangeHashSet>,
        new_heads: &ChangeHashSet,
        origin: &crate::event_origin::SwitchEventOrigin,
    ) -> Res<()> {
        let store = self.config_store()?;
        let versions = store.versions(prev_heads, new_heads).await?;
        let local_store = store.local_writer_actor().await;
        let mut events = vec![];
        let mut prev_config = match prev_heads {
            Some(prev) => store.at(prev).await?,
            None => None,
        };
        for version in versions {
            let is_local = local_store
                .as_ref()
                .is_some_and(|actor| actor == &version.actor_id);
            if is_local {
                // Applied synchronously by the mutator; advance the baseline.
                prev_config = Some(version.value);
                continue;
            }
            self.apply_config_diff(prev_config.as_ref(), &version.value, &mut events, origin)
                .await?;
            prev_config = Some(version.value);
        }
        events.push(PlugsEvent::PlugsConfigChanged {
            heads: new_heads.clone(),
            origin: origin.clone(),
        });
        self.registry.notify(events);
        Ok(())
    }
}

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
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: false,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            // Only docs whose diff touches the plug facets reach on_event.
            drawer_predicate: Some(DocPredicateClause::Or(vec![
                DocPredicateClause::HasTag(
                    daybook_types::doc::WellKnownFacetTag::PlugsConfig.into(),
                ),
                DocPredicateClause::HasTag(
                    daybook_types::doc::WellKnownFacetTag::PlugManifest.into(),
                ),
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
        // Forward to the notif loop with distinct config vs manifest
        // variants plus before/after heads (the loop diffs value-level
        // patches via the drawer API). Cheap — no reads in the sink.
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
        if changed.iter().any(|key| key.tag == plugs_config_tag) {
            self.repo
                .notif_tx
                .send(PlugsNotif::ConfigDocChanged {
                    prev_heads: evt.prev_heads.clone(),
                    new_heads: evt.new_heads.clone(),
                    origin: evt.origin.clone(),
                })
                .map_err(|_| ferr!("plugs notif channel closed"))?;
        }
        if changed.iter().any(|key| key.tag == plug_manifest_tag) {
            self.repo
                .notif_tx
                .send(PlugsNotif::ManifestDocChanged {
                    doc_id: evt.doc_id.clone(),
                    new_heads: evt.new_heads.clone(),
                    origin: evt.origin.clone(),
                })
                .map_err(|_| ferr!("plugs notif channel closed"))?;
        }
        Ok(crate::rt::switch::SwitchSinkOutcome::default())
    }
}
