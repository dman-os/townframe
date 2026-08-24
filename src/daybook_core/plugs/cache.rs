use super::*;

/// ADR 007 §5: the derived cache — a pure projection of the config facet
/// (`enabled`/`known` refs) + the manifest docs, materialized for hot-path
/// lookups. The config `FacetStore` is the source of truth for the enabled /
/// known / plug-config-doc-id sets; this cache adds the hydrated manifest
/// contents and the lookup indices (tag -> plug, tag -> facet manifest,
/// display hints) on top.
///
/// The maintenance methods below ONLY touch the cache: no store/drawer
/// reads, no event emission. Materialization (reading manifests) and event
/// decisions live in the callers (mutations, the notif loop).
#[derive(Default)]
pub(crate) struct PlugsCache {
    /// plug id -> manifest at heads (known plugs, valid versions only).
    pub(crate) manifests: HashMap<String, Arc<manifest::PlugManifest>>,
    /// Index: property tag -> plug id (@ns/name).
    pub(crate) tag_to_plug: HashMap<String, String>,
    /// Index: facet tag -> facet manifest.
    pub(crate) facet_manifests: HashMap<String, manifest::FacetManifest>,
    /// Active plugs: enabled + readable at pinned heads (plug id -> heads + manifest).
    pub(crate) active_manifests: HashMap<String, (ChangeHashSet, Arc<manifest::PlugManifest>)>,
}

impl PlugsCache {
    /// Whether the plug is active at exactly the given pinned heads — the
    /// idempotence fast path (re-applying an unchanged ref is a no-op).
    pub(crate) fn is_active_at(&self, plug_id: &str, heads: &ChangeHashSet) -> bool {
        self.active_manifests
            .get(plug_id)
            .is_some_and(|(old_heads, _)| old_heads == heads)
    }

    /// Insert/replace the active entry for a plug (pinned heads + manifest).
    pub(crate) fn set_active(
        &mut self,
        plug_id: &str,
        heads: ChangeHashSet,
        manifest: Arc<manifest::PlugManifest>,
    ) {
        self.active_manifests
            .insert(plug_id.to_string(), (heads, manifest));
    }

    /// Remove the active entry; returns whether it was present.
    pub(crate) fn clear_active(&mut self, plug_id: &str) -> bool {
        self.active_manifests.remove(plug_id).is_some()
    }

    /// Upsert the known entry for a plug + its indices (tag -> plug, tag ->
    /// facet manifest). The plug's stale index entries are dropped first.
    pub(crate) fn upsert_known(&mut self, plug_id: &str, manifest: &Arc<manifest::PlugManifest>) {
        let stale_tags: Vec<String> = self
            .tag_to_plug
            .iter()
            .filter(|(_, pid)| *pid == plug_id)
            .map(|(tag, _)| tag.clone())
            .collect();
        for tag in stale_tags {
            self.facet_manifests.remove(&tag);
        }
        self.tag_to_plug.retain(|_, pid| pid != plug_id);
        self.manifests
            .insert(plug_id.to_string(), Arc::clone(manifest));
        for facet in &manifest.facets {
            self.tag_to_plug
                .insert(facet.key_tag.to_string(), plug_id.to_string());
            self.facet_manifests
                .insert(facet.key_tag.to_string(), facet.clone());
        }
    }

    /// Drop the known entry + its indices; returns whether it was present.
    pub(crate) fn drop_known(&mut self, plug_id: &str) -> bool {
        if self.manifests.remove(plug_id).is_none() {
            return false;
        }
        let stale_tags: Vec<String> = self
            .tag_to_plug
            .iter()
            .filter(|(_, pid)| *pid == plug_id)
            .map(|(tag, _)| tag.clone())
            .collect();
        for tag in stale_tags {
            self.facet_manifests.remove(&tag);
        }
        self.tag_to_plug.retain(|_, pid| pid != plug_id);
        true
    }
}

impl PlugsRepo {
    /// Cold-start rebuild of the whole cache from the config facet (valid
    /// known refs + enabled refs) and the manifest docs. Used by
    /// `events_for_init` only — live updates are incremental.
    pub(crate) async fn rebuild_cache(&self) -> Res<()> {
        let mut manifests: HashMap<String, Arc<manifest::PlugManifest>> = HashMap::new();
        let mut tag_to_plug: HashMap<String, String> = HashMap::new();
        let mut facet_manifests: HashMap<String, manifest::FacetManifest> = HashMap::new();
        // ADR 007 §5: known plugs come from the config store, not the
        // facet-set index. Each known track's last_valid ref pins the
        // manifest doc + heads (rejected latests are not materialized).
        let (enabled, known_plugs) = self
            .config_store()?
            .query_sync(|config| (config.enabled.clone(), config.known_plugs.clone()))
            .await;
        for track in known_plugs.values() {
            let parsed = match Self::parse_enabled_ref(&track.last_valid) {
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
}
