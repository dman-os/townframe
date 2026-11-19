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
}
