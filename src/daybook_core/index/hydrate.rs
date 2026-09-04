use crate::drawer::DrawerRepo;
use crate::interlude::*;
use daybook_types::doc::{ChangeHashSet, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag};

/// The exact facet transition between two revisions of a logical branch.
///
/// Revision consumers use this to give predicate evaluation the same
/// dmeta-derived change set and local actor attribution as the document state.
pub(crate) struct DocFacetDiff {
    pub changed_facet_keys: HashSet<FacetKey>,
    pub added_facet_keys: HashSet<FacetKey>,
    pub removed_facet_keys: HashSet<FacetKey>,
    pub local_changed_facet_keys: HashSet<FacetKey>,
}

async fn dmeta_facets_at_heads(
    drawer: &DrawerRepo,
    doc_id: &DocId,
    branch_path: &daybook_types::doc::BranchPath,
    heads: Option<&ChangeHashSet>,
) -> Res<(
    HashSet<FacetKey>,
    HashMap<FacetKey, Vec<daybook_types::doc::Timestamp>>,
)> {
    let Some(heads) = heads else {
        return Ok((HashSet::new(), HashMap::new()));
    };
    let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
    let Some(doc) = drawer
        .get_doc_with_facets_at_branch_heads(
            doc_id,
            branch_path,
            heads,
            Some(vec![dmeta_key.clone()]),
        )
        .await?
    else {
        return Ok((HashSet::new(), HashMap::new()));
    };
    let Some(dmeta_raw) = doc.facets.get(&dmeta_key) else {
        return Ok((HashSet::new(), HashMap::new()));
    };
    let dmeta = match WellKnownFacet::from_json(dmeta_raw.clone(), WellKnownFacetTag::Dmeta)? {
        WellKnownFacet::Dmeta(dmeta) => dmeta,
        other => eyre::bail!("expected dmeta facet, got {:?}", other.tag()),
    };
    let mut keys = HashSet::new();
    let mut updated_at = HashMap::new();
    for (key, meta) in dmeta.facets {
        if key == dmeta_key || !meta.deleted_at.is_empty() {
            continue;
        }
        keys.insert(key.clone());
        updated_at.insert(key, meta.updated_at);
    }
    Ok((keys, updated_at))
}

pub(crate) async fn compute_doc_facet_diff(
    drawer: &DrawerRepo,
    doc_id: &DocId,
    branch_path: &daybook_types::doc::BranchPath,
    previous_heads: Option<&ChangeHashSet>,
    current_heads: Option<&ChangeHashSet>,
) -> Res<DocFacetDiff> {
    let (old_keys, old_updated_at) =
        dmeta_facets_at_heads(drawer, doc_id, branch_path, previous_heads).await?;
    let (new_keys, new_updated_at) =
        dmeta_facets_at_heads(drawer, doc_id, branch_path, current_heads).await?;
    let added: HashSet<FacetKey> = new_keys.difference(&old_keys).cloned().collect();
    let removed: HashSet<FacetKey> = old_keys.difference(&new_keys).cloned().collect();
    let mut changed = HashSet::new();
    if let (Some(previous_heads), Some(current_heads)) = (previous_heads, current_heads) {
        for key in old_keys.intersection(&new_keys) {
            if old_updated_at.get(key) != new_updated_at.get(key)
                || drawer
                    .get_facet_heads_at_branch_heads(doc_id, branch_path, previous_heads, key)
                    .await?
                    != drawer
                        .get_facet_heads_at_branch_heads(doc_id, branch_path, current_heads, key)
                        .await?
            {
                changed.insert(key.clone());
            }
        }
    }
    let mut local_candidates: Vec<_> = changed
        .iter()
        .chain(added.iter())
        .chain(removed.iter())
        .filter(|key| **key != FacetKey::from(WellKnownFacetTag::Dmeta))
        .cloned()
        .collect();
    local_candidates.sort();
    local_candidates.dedup();
    let local_changed_facet_keys = if let Some(current_heads) = current_heads {
        drawer
            .facet_keys_touched_by_local_actor(
                doc_id,
                branch_path,
                current_heads,
                &local_candidates,
            )
            .await?
    } else {
        HashSet::new()
    };
    Ok(DocFacetDiff {
        changed_facet_keys: changed,
        added_facet_keys: added,
        removed_facet_keys: removed,
        local_changed_facet_keys,
    })
}

}
