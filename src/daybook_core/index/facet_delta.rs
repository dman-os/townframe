//! Dmeta-derived facet transition types.
//!
//! The durable revision source lives with the facet-set projection. This
//! module contains only the value contract shared by that source and walkers.

use crate::index::doc_delta::DocDelta;
use crate::interlude::*;
use daybook_types::doc::{BranchId, ChangeHashSet, DocId, FacetKey};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Stable identity of a facet on one logical branch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct FacetRouteKey {
    pub document_id: DocId,
    pub branch_id: BranchId,
    pub facet_key: FacetKey,
}

impl Ord for FacetRouteKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.document_id
            .cmp(&other.document_id)
            .then_with(|| self.branch_id.0.cmp(&other.branch_id.0))
            .then_with(|| self.facet_key.cmp(&other.facet_key))
    }
}

impl PartialOrd for FacetRouteKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Current dmeta-derived state for one active facet. User values are not
/// persisted here; consumers hydrate values from these exact heads when they
/// need them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FacetSnapshot {
    pub branch_heads: ChangeHashSet,
    pub facet_heads: ChangeHashSet,
    pub actor_id: automerge::ActorId,
}

/// One collapsed current-state transition. `current = None` is a typed
/// facet tombstone; `current_branch_heads = None` additionally identifies a
/// removed source document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FacetDelta {
    pub key: FacetRouteKey,
    pub current: Option<FacetSnapshot>,
    #[serde(default)]
    pub current_branch_heads: Option<ChangeHashSet>,
    /// Whether a live facet removal was authored by a local actor. This is
    /// carried by the typed removal value because a low-level frontier
    /// deletion has no provenance of its own.
    #[serde(default)]
    pub removed_local: bool,
}

/// Exact-head dmeta lookup for one route. `Present` carries metadata only and
/// never a selected user facet value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FacetHydration {
    Deferred,
    Absent,
    Present(FacetSnapshot),
}

/// Build a deterministic current-state transition. Previous/current are
/// supplied by the projection or walker state, but only current is published.
pub(crate) fn transition(
    key: FacetRouteKey,
    previous: Option<FacetSnapshot>,
    previous_branch_heads: Option<ChangeHashSet>,
    current_branch_heads: Option<ChangeHashSet>,
    current: Option<FacetSnapshot>,
) -> Option<FacetDelta> {
    (previous != current || previous_branch_heads != current_branch_heads).then_some(FacetDelta {
        key,
        current,
        current_branch_heads,
        removed_local: false,
    })
}

/// Return the current-state transition represented by one accepted document
/// delta. Source removal is always a typed document tombstone.
pub(crate) fn transition_for_doc_delta(
    delta: &DocDelta,
    key: FacetRouteKey,
    previous: Option<FacetSnapshot>,
    previous_branch_heads: Option<ChangeHashSet>,
    current: Option<FacetSnapshot>,
) -> Option<FacetDelta> {
    debug_assert_eq!(key.document_id, delta.document_id);
    debug_assert_eq!(key.branch_id, delta.branch_id);
    let current_branch_heads = delta.current_heads.clone();
    if current_branch_heads.is_none() {
        return Some(FacetDelta {
            key,
            current: None,
            current_branch_heads: None,
            removed_local: false,
        });
    }
    transition(
        key,
        previous,
        previous_branch_heads,
        current_branch_heads.clone(),
        current_branch_heads.is_some().then_some(current).flatten(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_types::doc::WellKnownFacetTag;

    fn key() -> FacetRouteKey {
        FacetRouteKey {
            document_id: DocId::from("doc"),
            branch_id: BranchId::from("branch"),
            facet_key: FacetKey::from(WellKnownFacetTag::Blob),
        }
    }

    #[test]
    fn transition_suppresses_identical_state() {
        let heads = ChangeHashSet(Vec::new().into());
        assert_eq!(
            transition(key(), None, Some(heads.clone()), Some(heads), None),
            None
        );
    }

    #[test]
    fn transition_emits_absent_facet_tombstone_with_live_heads() {
        let heads = ChangeHashSet(Vec::new().into());
        let delta = transition(key(), None, None, Some(heads.clone()), None).expect("change");
        assert_eq!(delta.current, None);
        assert_eq!(delta.current_branch_heads, Some(heads));
    }

    #[test]
    fn source_removal_forces_tombstone_and_clears_heads() {
        let source = DocDelta {
            document_id: DocId::from("doc"),
            branch_id: BranchId::from("branch"),
            previous_heads: None,
            current_heads: None,
        };
        let delta = transition_for_doc_delta(&source, key(), None, None, None).expect("change");
        assert_eq!(delta.current, None);
        assert_eq!(delta.current_branch_heads, None);
    }
}
