use crate::interlude::*;
use automerge::{Automerge, ChangeHash, ObjType, ReadDoc, Value};
use daybook_types::doc::{FacetKey, WellKnownFacetTag};

pub fn recover_facet_heads(doc: &Automerge, facet_key: &FacetKey) -> Res<Vec<ChangeHash>> {
    // Path: facets -> org.example.daybook.dmeta/main -> facets -> <facet_key> -> updatedAt
    let facets_obj = match doc.get(automerge::ROOT, "facets")? {
        Some((Value::Object(ObjType::Map), id)) => id,
        _ => eyre::bail!("facets object not found"),
    };

    let dmeta_key = format!("{}/main", WellKnownFacetTag::Dmeta.as_str());
    let dmeta_obj = match doc.get(&facets_obj, &dmeta_key)? {
        Some((Value::Object(ObjType::Map), id)) => id,
        _ => eyre::bail!("dmeta facet not found"),
    };

    let dmeta_facets_obj = match doc.get(&dmeta_obj, "facets")? {
        Some((Value::Object(ObjType::Map), id)) => id,
        _ => eyre::bail!("dmeta.facets map not found"),
    };

    let facet_meta_obj = match doc.get(&dmeta_facets_obj, facet_key.to_string())? {
        Some((Value::Object(ObjType::Map), id)) => id,
        _ => eyre::bail!("facet meta not found for key: {}", facet_key),
    };

    let updated_at_list = match doc.get(&facet_meta_obj, "updatedAt")? {
        Some((Value::Object(ObjType::List), id)) => id,
        _ => eyre::bail!("updatedAt list not found for facet: {}", facet_key),
    };

    let mut heads = Vec::new();
    let length = doc.length(&updated_at_list);
    for ii in 0..length {
        if let Some((_, exid)) = doc.get(&updated_at_list, ii)? {
            if let Some(hash) = doc.hash_for_opid(&exid) {
                heads.push(hash);
            }
        }
    }

    Ok(heads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use automerge::Automerge;
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
}
