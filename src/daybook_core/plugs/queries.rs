use super::*;

impl PlugsRepo {
    pub(crate) async fn processor_activation_snapshot(&self) -> Res<ProcessorActivationSnapshot> {
        let _guard = self.mutation_mutex.lock().await;
        self.processor_activation_snapshot_locked().await
    }

    pub(crate) async fn processor_activation_snapshot_locked(
        &self,
    ) -> Res<ProcessorActivationSnapshot> {
        self.processor_activation_snapshot_with_heads(None).await
    }

    pub(crate) async fn cached_processor_activation_snapshot(
        &self,
        heads: &HashMap<String, ChangeHashSet>,
    ) -> Res<ProcessorActivationSnapshot> {
        let _guard = self.mutation_mutex.lock().await;
        self.processor_activation_snapshot_with_heads(Some(heads))
            .await
    }

    async fn processor_activation_snapshot_with_heads(
        &self,
        cached_heads: Option<&HashMap<String, ChangeHashSet>>,
    ) -> Res<ProcessorActivationSnapshot> {
        use automerge::ReadDoc as _;
        let (config, heads) = self.config_store()?.latest_snapshot().await?;
        let mut snapshot = ProcessorActivationSnapshot {
            targets: HashMap::new(),
            manifests: HashMap::new(),
            rejected: HashMap::new(),
        };
        let Some(heads) = heads else {
            return Ok(snapshot);
        };
        let config_document_id: big_repo::DocumentId = self.doc_config_id.parse()?;
        let handle = self
            .big_repo
            .get_doc(&config_document_id)
            .await?
            .into_ready(config_document_id)?;
        let enabled_ids = handle
            .with_document_read(|doc| -> Res<HashMap<String, String>> {
                let facets = doc
                    .get_at(automerge::ROOT, "facets", heads.as_ref())?
                    .ok_or_eyre("plugs config facets missing")?
                    .1;
                let facet = doc
                    .get_at(
                        &facets,
                        <PlugsConfig as crate::stores::FacetStore>::facet_key().to_string(),
                        heads.as_ref(),
                    )?
                    .ok_or_eyre("plugs config facet missing")?
                    .1;
                let enabled = doc
                    .get_at(&facet, "enabled", heads.as_ref())?
                    .ok_or_eyre("plugs enabled map missing")?
                    .1;
                config
                    .enabled
                    .iter()
                    .map(|(plug_id, expected)| {
                        // JSON URLs reconcile as scalar strings. get_at returns the winning
                        // scalar put's operation identity, matching hydration's winner.
                        let (value, operation) = doc
                            .get_at(&enabled, plug_id.as_str(), heads.as_ref())?
                            .ok_or_eyre("enabled entry missing")?;
                        eyre::ensure!(
                            value.as_str() == Some(expected.as_str()),
                            "enabled winner differs from hydrated config"
                        );
                        Ok((plug_id.clone(), operation.to_string()))
                    })
                    .collect()
            })
            .await?;
        let drawer = self.drawer.get().ok_or_eyre("plugs drawer missing")?;
        for (plug_id, enabled_ref) in config.enabled {
            let config_doc_id = config
                .plug_config_doc_ids
                .get(&plug_id)
                .ok_or_eyre("enabled plug has no configuration document")?
                .clone();
            let config_doc_heads =
                if let Some(heads) = cached_heads.and_then(|heads| heads.get(&config_doc_id)) {
                    Some(heads.clone())
                } else {
                    drawer.ready_main_branch_heads(&config_doc_id).await?
                };
            let config_doc_heads = config_doc_heads.map(|heads| {
                let mut hashes = heads.as_ref().to_vec();
                hashes.sort();
                ChangeHashSet(hashes.into())
            });
            let parsed = Self::parse_enabled_ref(&enabled_ref)?;
            let pinned = parsed
                .at
                .as_ref()
                .map(|at| am_utils_rs::parse_commit_heads(at))
                .transpose()?
                .map(|heads| {
                    let mut hashes = heads.as_ref().to_vec();
                    hashes.sort();
                    ChangeHashSet(hashes.into())
                });
            let exact_ref = if let Some(heads) = &pinned {
                Self::build_enabled_ref(
                    &parsed.doc_id,
                    parsed.branch.as_deref().unwrap_or("main"),
                    heads,
                )?
            } else {
                enabled_ref.clone()
            };
            let target = PlugActivationTarget {
                enablement_entry_id: enabled_ids[&plug_id].clone(),
                plug_id: plug_id.clone(),
                enabled_ref: exact_ref,
                config_doc_id,
                config_doc_heads: config_doc_heads.clone().unwrap_or_default(),
            };
            if let Some(track) = config.known_plugs.get(&plug_id)
                && track.latest == enabled_ref
                && let Some(reason) = &track.latest_rejection
            {
                snapshot
                    .rejected
                    .insert(plug_id.clone(), format!("{reason:?}"));
            }
            if config_doc_heads.is_some() && !snapshot.rejected.contains_key(&plug_id) {
                let manifest = surelock::key::lock_scope(|key| {
                    let (cache, _key) = key.lock(&self.cache);
                    cache
                        .active_manifests
                        .get(&plug_id)
                        .and_then(|(manifest_heads, manifest)| {
                            pinned
                                .as_ref()
                                .is_some_and(|pinned| {
                                    pinned.len() == manifest_heads.len()
                                        && pinned.iter().all(|head| manifest_heads.contains(head))
                                })
                                .then(|| Arc::clone(manifest))
                        })
                });
                if let Some(manifest) = manifest {
                    snapshot.manifests.insert(plug_id.clone(), manifest);
                }
            }
            snapshot.targets.insert(plug_id, target);
        }
        Ok(snapshot)
    }
    pub(crate) async fn read_manifest_doc(
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

    pub(crate) fn configuration_document_id(&self) -> &str {
        &self.doc_config_id
    }

    pub(crate) async fn capture_config_bindings(
        &self,
    ) -> Res<(String, ChangeHashSet, HashMap<String, String>)> {
        let (config, heads) = self.config_store()?.latest_snapshot().await?;
        let heads = heads.ok_or_eyre("plugs configuration has no retained heads")?;
        Ok((
            self.doc_config_id.clone(),
            heads,
            config.plug_config_doc_ids.clone(),
        ))
    }

    pub(crate) async fn config_bindings_at(
        &self,
        document_id: &str,
        heads: &ChangeHashSet,
    ) -> Res<Option<HashMap<String, String>>> {
        eyre::ensure!(
            document_id == self.doc_config_id,
            "configuration association proof is not the native plugs configuration"
        );
        let id: big_repo::DocumentId = document_id.parse()?;
        let big_repo::DocLookup::Ready(handle) = self.big_repo.get_doc(&id).await? else {
            return Ok(None);
        };
        let config = handle
            .hydrate_path_at_heads::<am_utils_rs::codecs::ThroughJson<PlugsConfig>>(
                &heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(
                        <PlugsConfig as crate::stores::FacetStore>::facet_key()
                            .to_string()
                            .into(),
                    ),
                ],
            )
            .await?;
        Ok(config.map(|config| config.0.plug_config_doc_ids))
    }

    /// The plug's current enabled ref from the live config facet, if enabled.
    /// Used by broadcast-driven consumers that carry no config snapshot (the
    /// durable rev-store path uses each revision's own config instead).
    pub(crate) async fn enabled_ref(&self, plug_id: &str) -> Res<Option<url::Url>> {
        let store = self
            .config_store
            .get()
            .ok_or_eyre("plugs config store not attached")?;
        Ok(store
            .query_sync(|config| config.enabled.get(plug_id).cloned())
            .await)
    }

    pub(crate) fn parse_enabled_ref(url: &url::Url) -> Res<daybook_types::url::FacetRef> {
        let parsed = daybook_types::url::parse_facet_ref(url)?;
        if parsed.facet_key.tag
            != daybook_types::doc::FacetTag::WellKnown(
                daybook_types::doc::WellKnownFacetTag::PlugManifest,
            )
            || parsed.facet_key.id != "main"
        {
            eyre::bail!("enabled ref must point at org.example.daybook.plugManifest/main: {url}");
        }
        eyre::ensure!(
            parsed.branch.as_deref().unwrap_or("main") == "main",
            "plug manifest references must use the main branch: {url}"
        );
        Ok(parsed)
    }

    /// Read the manifest pointed at by a full facet ref (doc id + branch +
    /// pinned heads, or current branch heads when unpinned). Returns None when
    /// the doc/heads are not locally readable (ADR 007 §6: pending). Used for
    /// enabled refs and for pending resolution alike.
    pub(crate) async fn read_manifest_at_ref(
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

    /// Materialize a plug's manifest at its enabled ref (pinned heads, or
    /// current branch heads when unpinned). None when the ref is not locally
    /// readable (pending) or the manifest id mismatches the plug. Pure read —
    /// no cache writes, no events.
    pub(crate) async fn materialize_active(
        &self,
        plug_id: &str,
        ref_url: &url::Url,
    ) -> Res<Option<(ChangeHashSet, Arc<manifest::PlugManifest>)>> {
        let Some((heads, manifest)) = self.read_manifest_at_ref(ref_url).await? else {
            return Ok(None);
        };
        if manifest.id() != plug_id {
            warn!(
                plug_id = %plug_id,
                manifest_id = %manifest.id(),
                "enabled ref manifest id mismatch; treating as pending"
            );
            return Ok(None);
        }
        Ok(Some((heads, manifest)))
    }

    pub(crate) fn build_enabled_ref(
        doc_id: &str,
        branch: &str,
        heads: &ChangeHashSet,
    ) -> Res<url::Url> {
        let at = am_utils_rs::serialize_commit_heads(heads.as_ref()).join("|");
        let url = format!(
            "db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch={branch}&at={at}"
        );
        Ok(url.parse()?)
    }

    /// Active plug manifest (enabled + readable at pinned heads). Known-but-
    /// disabled plugs return None; commands and inits resolve through active
    /// plugs only (ADR 007 §6).
    pub async fn get(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache
                .active_manifests
                .get(id)
                .map(|(_, manifest)| Arc::clone(manifest))
        })
    }

    /// Known plug manifest (from the facet-set index), regardless of enablement.
    pub async fn get_known(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache.manifests.get(id).cloned()
        })
    }

    pub async fn get_plug_config_doc_id(&self, plug_id: &str) -> Option<String> {
        let store = self.config_store.get()?;
        store
            .query_sync(|config| config.plug_config_doc_ids.get(plug_id).cloned())
            .await
    }

    pub async fn get_display_hint(&self, prop_tag: &str) -> Option<manifest::FacetDisplayHint> {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache
                .facet_manifests
                .get(prop_tag)
                .map(|facet_manifest| facet_manifest.display_config.clone())
        })
    }

    /// ADR 007 §6: drawer facet validation consults active plugs only, with a
    /// distinct "plug disabled" vs "unknown tag" error.
    pub async fn get_facet_manifest_by_tag(&self, facet_tag: &str) -> FacetManifestLookup {
        let (plug_id, facet_manifest) = surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            let Some(plug_id) = cache.tag_to_plug.get(facet_tag) else {
                return (None, None);
            };
            (
                Some(plug_id.clone()),
                cache.facet_manifests.get(facet_tag).cloned(),
            )
        });
        let Some(plug_id) = plug_id else {
            return FacetManifestLookup::UnknownTag;
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
        let plug_id = surelock::key::lock_scope(|key| -> Option<String> {
            let (cache, _key) = key.lock(&self.cache);
            Some(cache.tag_to_plug.get(facet_tag)?.clone())
        })?;
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
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache
                .facet_manifests
                .iter()
                .map(|(tag, facet_manifest)| (tag.clone(), facet_manifest.display_config.clone()))
                .collect()
        })
    }

    /// Known plugs (all manifest docs catalogued by the facet-set index).
    pub async fn list_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache.manifests.values().cloned().collect()
        })
    }

    /// Active plugs only (enabled + readable at pinned heads).
    pub async fn list_active_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        surelock::key::lock_scope(|key| {
            let (cache, _key) = key.lock(&self.cache);
            cache
                .active_manifests
                .values()
                .map(|(_, manifest)| Arc::clone(manifest))
                .collect()
        })
    }

    /// Read the current plugs config (enabled map + per-plug tracks). None
    /// when the config store is not attached.
    pub async fn get_config(&self) -> Option<PlugsConfig> {
        let store = self.config_store.get()?;
        Some(store.query_sync(|config| config.clone()).await)
    }

    /// Resolve a plug id or a full facet ref to a manifest. A ref is read at
    /// its pinned heads (None when not locally readable); an id resolves
    /// through the known-manifest cache.
    pub async fn resolve_manifest(&self, target: &str) -> Res<Option<Arc<manifest::PlugManifest>>> {
        if target.starts_with("db+facet://") {
            let ref_url = url::Url::parse(target)?;
            let Some((_, manifest)) = self.read_manifest_at_ref(&ref_url).await? else {
                return Ok(None);
            };
            Ok(Some(manifest))
        } else {
            Ok(self.get_known(target).await)
        }
    }

    /// ADR 007 §6: enabled entries whose doc/heads are not locally readable
    /// (config/manifest race). Returns (plug id, pinned ref).
    pub async fn list_pending(&self) -> Vec<(String, url::Url)> {
        let Some(store) = self.config_store.get() else {
            return Vec::new();
        };
        let enabled = store.query_sync(|config| config.enabled.clone()).await;
        let mut pending = Vec::new();
        for (id, ref_url) in enabled {
            match self.read_manifest_at_ref(&ref_url).await {
                Ok(Some(_)) => {}
                _ => pending.push((id, ref_url)),
            }
        }
        pending
    }
}
