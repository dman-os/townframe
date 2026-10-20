//! The MDK's typed document-mirror authoring surface (ADR 015 §5, §4). This adds ergonomics —
//! typed decoding, named bindings, observable values, and a receipt vocabulary that does not
//! require callers to navigate wire enums — without adding authority: the underlying
//! `documents::DocumentAccess` resource is still the only capability, and filtering still
//! happens host-side before data reaches this mirror.
use crate::documents;
use std::collections::BTreeMap;

/// The state of one authorized facet route in this mirror.
#[derive(Clone, Debug, PartialEq)]
pub enum FacetState {
    /// Not yet hydrated locally. Distinct from `Absent`: an uncached facet is never mistaken
    /// for a nonexistent one.
    Loading,
    /// Known absent after a successful read.
    Absent,
    /// The current authorized value, as raw JSON text (the wire `facet-raw` form).
    Present(String),
}

/// A decoded snapshot of one watch's authorized selection, keyed by route. Availability and
/// freshness are evidence for rendering (ADR 014 §4); a pending refresh never hides an
/// existing local value.
#[derive(Clone, Debug, PartialEq)]
pub struct MirrorSnapshot {
    pub revision: u64,
    pub facets: BTreeMap<(String, String), FacetState>,
    /// Operation ids still locally applied, not yet durably accepted.
    pub pending: Vec<u64>,
    /// A targeted refresh is in flight for this watch's current selection.
    pub refreshing: bool,
    /// The mirror's authority was revoked; all values have been withdrawn.
    pub revoked: bool,
}

impl MirrorSnapshot {
    /// Decode one facet route as `T`. `Loading`/`Absent`/missing decode as `None`.
    pub fn value<T: serde::de::DeserializeOwned>(&self, tag: &str, key_id: &str) -> Option<T> {
        match self.facets.get(&(tag.to_owned(), key_id.to_owned())) {
            Some(FacetState::Present(raw)) => Some(
                serde_json::from_str(raw)
                    .expect("configuration facet violates its declared value shape"),
            ),
            Some(FacetState::Loading | FacetState::Absent) | None => None,
        }
    }
}

/// One named, already-authorized document binding handed to a micro at activation. The host
/// resolved the owner-config document and granted exactly this facet access; the binding is
/// authority, not a lookup result.
pub struct ConfigDocument {
    binding: documents::DocumentBinding,
}

impl ConfigDocument {
    pub fn new(binding: documents::DocumentBinding) -> Self {
        Self { binding }
    }

    pub fn name(&self) -> &str {
        &self.binding.name
    }

    /// Begin watching one authorized facet key. The initial snapshot and subsequent updates
    /// form one subscription (no read-then-subscribe gap). Returns the watch handle plus the
    /// first snapshot decoded into mirror form.
    pub fn watch(
        &self,
        tag: &str,
        key_id: &str,
    ) -> Result<(MirrorWatch, MirrorSnapshot), documents::DocumentError> {
        let watch = self.binding.access.watch(&documents::WatchSelection {
            documents: vec![self.binding.document.clone()],
            branch: self.binding.branch.clone(),
            facets: documents::FacetSelection::Keys(vec![documents::FacetKey {
                tag: tag.into(),
                key_id: key_id.into(),
            }]),
        })?;
        let snapshot = mirror_snapshot(watch.current()?);
        Ok((MirrorWatch { watch }, snapshot))
    }

    /// Submit a mutation intent against one authorized facet route. The operation's overlay is
    /// applied to authorized mirrors immediately; durable acceptance or rollback arrives on the
    /// returned receipt. Closing the mirror never cancels the submission (ADR 014 §5).
    pub fn write<T: serde::Serialize>(
        &self,
        tag: &str,
        key_id: &str,
        value: &T,
    ) -> Result<documents::Operation, documents::DocumentError> {
        self.binding.access.mutate(
            &documents::FacetRoute {
                document: self.binding.document.clone(),
                branch: self.binding.branch.clone(),
                key: documents::FacetKey {
                    tag: tag.into(),
                    key_id: key_id.into(),
                },
            },
            &documents::FacetMutation::Update(
                serde_json::to_string(value).expect("facet value must serialize to JSON"),
            ),
        )
    }
}

/// A live watch over a mirror selection. Dropping it releases retention and ends delivery
/// (the watch is a resource, transcript:328 §7).
pub struct MirrorWatch {
    watch: documents::DocumentWatch,
}

impl MirrorWatch {
    pub fn current(&self) -> Result<MirrorSnapshot, documents::DocumentError> {
        self.watch.current().map(mirror_snapshot)
    }

    /// Request a targeted refresh of this watch's retained selection.
    pub fn refresh(&self) -> Result<(), documents::DocumentError> {
        self.watch.refresh()
    }
}

fn mirror_snapshot(snapshot: documents::DocumentSnapshot) -> MirrorSnapshot {
    MirrorSnapshot {
        revision: snapshot.revision,
        facets: snapshot
            .facets
            .into_iter()
            .map(|entry| {
                let state = match entry.value {
                    documents::FacetValue::Loading => FacetState::Loading,
                    documents::FacetValue::Absent => FacetState::Absent,
                    documents::FacetValue::Present(raw) => FacetState::Present(raw),
                };
                ((entry.route.key.tag, entry.route.key.key_id), state)
            })
            .collect(),
        pending: snapshot.pending,
        refreshing: snapshot.refreshing,
        revoked: snapshot.revoked,
    }
}
