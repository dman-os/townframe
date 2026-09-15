use crate::interlude::*;

use automerge::{Automerge, ChangeHash, ObjType, ReadDoc, Value};
use daybook_types::doc::{FacetKey, WellKnownFacetTag};

pub fn recover_facet_heads(doc: &Automerge, facet_key: &FacetKey) -> Res<Vec<ChangeHash>> {
    recover_facet_heads_inner(doc, facet_key, None)
}

pub fn recover_facet_heads_at(
    doc: &Automerge,
    facet_key: &FacetKey,
    heads: &[ChangeHash],
) -> Res<Vec<ChangeHash>> {
    recover_facet_heads_inner(doc, facet_key, Some(heads))
}

fn recover_facet_heads_inner(
    doc: &Automerge,
    facet_key: &FacetKey,
    read_heads: Option<&[ChangeHash]>,
) -> Res<Vec<ChangeHash>> {
    let Some(updated_at_list) = facet_updated_at_list(doc, facet_key, read_heads)? else {
        return Ok(Vec::new());
    };

    let mut recovered = Vec::new();
    let length = match read_heads {
        Some(read_heads) => doc.length_at(&updated_at_list, read_heads),
        None => doc.length(&updated_at_list),
    };
    for ii in 0..length {
        if let Some((_, exid)) = get(doc, &updated_at_list, ii, read_heads)? {
            let Some(hash) = doc.hash_for_opid(&exid) else {
                eyre::bail!(
                    "failed recovering facet heads: missing hash for updatedAt entry index={} opid={}",
                    ii,
                    exid
                );
            };
            recovered.push(hash);
        }
    }

    Ok(recovered)
}

/// The facet's dmeta `updatedAt` list object at the given heads (None when
/// the dmeta walk yields no list). The list object id is stable across
/// writes — it is created once at facet-meta creation and reused.
pub fn facet_updated_at_list(
    doc: &Automerge,
    facet_key: &FacetKey,
    read_heads: Option<&[ChangeHash]>,
) -> Res<Option<automerge::ObjId>> {
    // Path: facets -> org.example.daybook.dmeta/main -> facets -> <facet_key> -> updatedAt
    let facets_obj = match get(doc, automerge::ROOT, "facets", read_heads)? {
        Some((Value::Object(ObjType::Map), id)) => id,
        None => return Ok(None),
        Some((other, _)) => {
            eyre::bail!("unexpected value for 'facets' property: expected Map, got {other:?}");
        }
    };

    let dmeta_key = format!("{}/main", WellKnownFacetTag::Dmeta.as_str());
    let dmeta_obj = match get(doc, &facets_obj, &dmeta_key, read_heads)? {
        Some((Value::Object(ObjType::Map), id)) => id,
        None => return Ok(None),
        Some((other, _)) => {
            eyre::bail!("unexpected value for dmeta facet property: expected Map, got {other:?}");
        }
    };

    let dmeta_facets_obj = match get(doc, &dmeta_obj, "facets", read_heads)? {
        Some((Value::Object(ObjType::Map), id)) => id,
        None => return Ok(None),
        Some((other, _)) => {
            eyre::bail!("unexpected value for dmeta.facets property: expected Map, got {other:?}");
        }
    };

    let facet_meta_obj = match get(doc, &dmeta_facets_obj, facet_key.to_string(), read_heads)? {
        Some((Value::Object(ObjType::Map), id)) => id,
        None => return Ok(None),
        Some((other, _)) => {
            eyre::bail!(
                "unexpected value for facet metadata property: expected Map, got {other:?}"
            );
        }
    };

    match get(doc, &facet_meta_obj, "updatedAt", read_heads)? {
        Some((Value::Object(ObjType::List), id)) => Ok(Some(id)),
        None => Ok(None),
        Some((other, _)) => {
            eyre::bail!("unexpected value for updatedAt property: expected List, got {other:?}");
        }
    }
}

/// The write points (heads + author) of a facet between two head sets,
/// oldest first, derived from the facet's dmeta `updatedAt` markers. Each
/// marker is written in the same change as the facet content it snapshots,
/// so hydrating the facet at a write point's heads yields a consistent,
/// non-partially-updated version.
pub fn facet_write_points(
    doc: &Automerge,
    facet_key: &FacetKey,
    from: &[ChangeHash],
    to: &[ChangeHash],
) -> Res<Vec<(ChangeHashSet, ActorId)>> {
    let Some(updated_at_list) = facet_updated_at_list(doc, facet_key, Some(to))? else {
        return Ok(Vec::new());
    };
    // A facet write rewrites its updatedAt marker list; the diff window
    // therefore shows an Insert patch on the (stable) list per write.
    let patches = doc.diff_obj(&automerge::ROOT, from, to, true)?;
    let mut write_hashes: HashSet<ChangeHash> = HashSet::new();
    for patch in &patches {
        let automerge::PatchAction::Insert { values, .. } = &patch.action else {
            continue;
        };
        if patch.obj != updated_at_list {
            continue;
        }
        for (_, exid, _) in values.iter() {
            if let Some(hash) = doc.hash_for_opid(exid) {
                write_hashes.insert(hash);
            }
        }
    }
    // Causal order + heads + author from the change graph.
    let mut points = Vec::new();
    for change in doc.get_changes(from) {
        if write_hashes.contains(&change.hash()) {
            let mut heads: Vec<ChangeHash> = change.deps().to_vec();
            heads.push(change.hash());
            points.push((ChangeHashSet(heads.into()), change.actor_id().clone()));
        }
    }
    Ok(points)
}

fn get<'a, P: Into<automerge::Prop>>(
    doc: &'a Automerge,
    obj: impl AsRef<automerge::ObjId>,
    prop: P,
    heads: Option<&'a [ChangeHash]>,
) -> Result<Option<(Value<'a>, automerge::ObjId)>, automerge::AutomergeError> {
    match heads {
        Some(read_heads) => doc.get_at(obj, prop, read_heads),
        None => doc.get(obj, prop),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::Automerge;
    use automerge::transaction::Transactable;
    use daybook_types::doc::{FacetKey, WellKnownFacetTag};

    #[test]
    fn test_recover_facet_heads_single() -> Res<()> {
        let mut doc = Automerge::new();
        let facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let facet_key_str = facet_key.to_string();
        let dmeta_key = format!("{}/main", WellKnownFacetTag::Dmeta.as_str());

        // Setup structure manually using raw APIs
        let mut tx = doc.transaction();
        let facets_id = tx.put_object(automerge::ROOT, "facets", ObjType::Map)?;
        let dmeta_id = tx.put_object(&facets_id, &dmeta_key, ObjType::Map)?;
        let dmeta_facets_id = tx.put_object(&dmeta_id, "facets", ObjType::Map)?;
        let facet_meta_id = tx.put_object(&dmeta_facets_id, &facet_key_str, ObjType::Map)?;
        let updated_at_id = tx.put_object(&facet_meta_id, "updatedAt", ObjType::List)?;

        // Write updated_at
        tx.insert(&updated_at_id, 0, Timestamp::now().as_second())?;

        let commit_hash = tx.commit().0.expect("should commit");
        let heads = recover_facet_heads(&doc, &facet_key)?;

        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0], commit_hash);

        Ok(())
    }

    #[test]
    fn test_recover_facet_heads_merge() -> Res<()> {
        let mut doc1 = Automerge::new();
        let facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let facet_key_str = facet_key.to_string();
        let dmeta_key = format!("{}/main", WellKnownFacetTag::Dmeta.as_str());

        // Helper to setup dmeta structure
        let mut tx = doc1.transaction();
        let facets_id = tx.put_object(automerge::ROOT, "facets", ObjType::Map)?;
        let dmeta_id = tx.put_object(&facets_id, &dmeta_key, ObjType::Map)?;
        let dmeta_facets_id = tx.put_object(&dmeta_id, "facets", ObjType::Map)?;
        let facet_meta_id = tx.put_object(&dmeta_facets_id, &facet_key_str, ObjType::Map)?;
        let updated_at_id = tx.put_object(&facet_meta_id, "updatedAt", ObjType::List)?;
        tx.insert(&updated_at_id, 0, 1000i64)?;
        tx.commit().0.unwrap();

        let mut doc2 = doc1.fork();

        // Concurrent update on doc1
        let mut tx1 = doc1.transaction();
        let facets_id1 = tx1.get(automerge::ROOT, "facets")?.unwrap().1;
        let dmeta_id1 = tx1.get(&facets_id1, &dmeta_key)?.unwrap().1;
        let dmeta_facets_id1 = tx1.get(&dmeta_id1, "facets")?.unwrap().1;
        let facet_meta_id1 = tx1.get(&dmeta_facets_id1, &facet_key_str)?.unwrap().1;
        let updated_at1 = tx1.get(&facet_meta_id1, "updatedAt")?.unwrap().1;
        tx1.delete(&updated_at1, 0)?;
        tx1.insert(&updated_at1, 0, 1001i64)?;
        let hash1_new = tx1.commit().0.unwrap();

        // Concurrent update on doc2
        let mut tx2 = doc2.transaction();
        let facets_id2 = tx2.get(automerge::ROOT, "facets")?.unwrap().1;
        let dmeta_id2 = tx2.get(&facets_id2, &dmeta_key)?.unwrap().1;
        let dmeta_facets_id2 = tx2.get(&dmeta_id2, "facets")?.unwrap().1;
        let facet_meta_id2 = tx2.get(&dmeta_facets_id2, &facet_key_str)?.unwrap().1;
        let updated_at2 = tx2.get(&facet_meta_id2, "updatedAt")?.unwrap().1;
        tx2.delete(&updated_at2, 0)?;
        tx2.insert(&updated_at2, 0, 1002i64)?;
        let hash2_new = tx2.commit().0.unwrap();

        // Merge
        doc1.merge(&mut doc2)?;

        let heads = recover_facet_heads(&doc1, &facet_key)?;

        assert_eq!(heads.len(), 2);
        assert!(heads.contains(&hash1_new));
        assert!(heads.contains(&hash2_new));

        Ok(())
    }

    #[test]
    fn test_recover_facet_heads_missing_returns_empty() -> Res<()> {
        let doc = Automerge::new();
        let facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let heads = recover_facet_heads(&doc, &facet_key)?;
        assert!(heads.is_empty());
        Ok(())
    }

    #[test]
    fn test_recover_facet_heads_malformed_facets_type_fails() -> Res<()> {
        let mut doc = Automerge::new();
        let facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "facets", "not_a_map")?;
        tx.commit().0.unwrap();

        let result = recover_facet_heads(&doc, &facet_key);
        assert!(result.is_err(), "expected error on scalar facets property");
        Ok(())
    }

    #[test]
    fn test_recover_facet_heads_malformed_updated_at_type_fails() -> Res<()> {
        let mut doc = Automerge::new();
        let facet_key = FacetKey::from(WellKnownFacetTag::Note);
        let facet_key_str = facet_key.to_string();
        let dmeta_key = format!("{}/main", WellKnownFacetTag::Dmeta.as_str());

        let mut tx = doc.transaction();
        let facets_id = tx.put_object(automerge::ROOT, "facets", ObjType::Map)?;
        let dmeta_id = tx.put_object(&facets_id, &dmeta_key, ObjType::Map)?;
        let dmeta_facets_id = tx.put_object(&dmeta_id, "facets", ObjType::Map)?;
        let facet_meta_id = tx.put_object(&dmeta_facets_id, &facet_key_str, ObjType::Map)?;
        tx.put(&facet_meta_id, "updatedAt", "not_a_list")?;
        tx.commit().0.unwrap();

        let result = recover_facet_heads(&doc, &facet_key);
        assert!(
            result.is_err(),
            "expected error on scalar updatedAt property"
        );
        Ok(())
    }
}
