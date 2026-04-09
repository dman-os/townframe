use crate::interlude::*;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use daybook_types::doc::{
    ChangeHashSet, FacetKey, FacetMeta, UserMeta, UserPath, WellKnownFacet, WellKnownFacetTag,
};

fn dmeta_key() -> String {
    [WellKnownFacetTag::Dmeta.as_str(), "/main"].concat()
}

fn timestamp_scalar(now: Timestamp) -> automerge::ScalarValue {
    automerge::ScalarValue::Timestamp(now.as_second())
}

pub fn facet_meta_obj<D: ReadDoc>(doc: &D, facet_key: &FacetKey) -> Res<Option<automerge::ObjId>> {
    let facets_obj = match doc.get(automerge::ROOT, "facets")? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let key = dmeta_key();
    let dmeta_obj = match doc.get(&facets_obj, &key)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let dmeta_facets_obj = match doc.get(&dmeta_obj, "facets")? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let facet_key_str = facet_key.to_string();
    match doc.get(&dmeta_facets_obj, facet_key_str)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => Ok(Some(id)),
        _ => Ok(None),
    }
}

pub fn facet_uuid_for_key<D: ReadDoc + autosurgeon::ReadDoc>(
    doc: &D,
    facet_key: &FacetKey,
) -> Res<Option<Uuid>> {
    let Some(facet_meta_obj) = facet_meta_obj(doc, facet_key)? else {
        return Ok(None);
    };
    match automerge::ReadDoc::get(doc, &facet_meta_obj, "uuid")? {
        Some((automerge::Value::Scalar(scalar), _)) => {
            Ok(Some(parse_uuid_scalar(scalar.as_ref())?))
        }
        Some((automerge::Value::Object(automerge::ObjType::List), list_obj)) => {
            if automerge::ReadDoc::length(doc, &list_obj) == 0 {
                return Ok(None);
            }
            let Some((value, _)) = automerge::ReadDoc::get(doc, &list_obj, 0)? else {
                return Ok(None);
            };
            let automerge::Value::Scalar(scalar) = value else {
                eyre::bail!("facet uuid list contains non-scalar value for key {facet_key}");
            };
            Ok(Some(parse_uuid_scalar(scalar.as_ref())?))
        }
        Some((other, _)) => {
            eyre::bail!("facet uuid has invalid shape for key {facet_key}: {other:?}");
        }
        None => Ok(None),
    }
}

pub fn facet_uuid_for_key_at(
    doc: &automerge::Automerge,
    facet_key: &FacetKey,
    heads: &[automerge::ChangeHash],
) -> Res<Option<Uuid>> {
    let Some(facet_meta_obj) = facet_meta_obj_at(doc, facet_key, heads)? else {
        return Ok(None);
    };
    let is_deleted = match doc.get_at(&facet_meta_obj, "deletedAt", heads)? {
        Some((automerge::Value::Object(automerge::ObjType::List), deleted_at_list)) => {
            doc.length_at(&deleted_at_list, heads) > 0
        }
        Some((other, _)) => {
            eyre::bail!("facet meta deletedAt has invalid shape for key {facet_key}: {other:?}");
        }
        None => false,
    };
    if is_deleted {
        return Ok(None);
    }
    match doc.get_at(&facet_meta_obj, "uuid", heads)? {
        Some((automerge::Value::Scalar(scalar), _)) => {
            Ok(Some(parse_uuid_scalar(scalar.as_ref())?))
        }
        Some((automerge::Value::Object(automerge::ObjType::List), list_obj)) => {
            if doc.length_at(&list_obj, heads) == 0 {
                return Ok(None);
            }
            let Some((value, _)) = doc.get_at(&list_obj, 0, heads)? else {
                return Ok(None);
            };
            let automerge::Value::Scalar(scalar) = value else {
                eyre::bail!("facet uuid list contains non-scalar value for key {facet_key}");
            };
            Ok(Some(parse_uuid_scalar(scalar.as_ref())?))
        }
        Some((other, _)) => {
            eyre::bail!(
                "facet uuid has invalid shape for key {facet_key} at heads {:?}: {other:?}",
                am_utils_rs::serialize_commit_heads(heads)
            );
        }
        None => Ok(None),
    }
}

fn parse_uuid_scalar(scalar: &automerge::ScalarValue) -> Res<Uuid> {
    match scalar {
        automerge::ScalarValue::Str(text) => Ok(Uuid::parse_str(text)?),
        automerge::ScalarValue::Bytes(bytes) => Ok(Uuid::from_slice(bytes)?),
        other => eyre::bail!("facet uuid has invalid scalar type: {other:?}"),
    }
}

pub fn facet_heads_for_key(doc: &automerge::Automerge, facet_key: &FacetKey) -> Res<ChangeHashSet> {
    let heads = super::facet_recovery::recover_facet_heads(doc, facet_key)?;
    Ok(ChangeHashSet(Arc::from(heads)))
}

pub fn facet_heads_for_key_at(
    doc: &automerge::Automerge,
    facet_key: &FacetKey,
    heads: &[automerge::ChangeHash],
) -> Res<ChangeHashSet> {
    let heads = super::facet_recovery::recover_facet_heads_at(doc, facet_key, heads)?;
    Ok(ChangeHashSet(Arc::from(heads)))
}

fn facet_meta_obj_at(
    doc: &automerge::Automerge,
    facet_key: &FacetKey,
    heads: &[automerge::ChangeHash],
) -> Res<Option<automerge::ObjId>> {
    let facets_obj = match doc.get_at(automerge::ROOT, "facets", heads)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let key = dmeta_key();
    let dmeta_obj = match doc.get_at(&facets_obj, &key, heads)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let dmeta_facets_obj = match doc.get_at(&dmeta_obj, "facets", heads)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => return Ok(None),
    };

    let facet_key_str = facet_key.to_string();
    match doc.get_at(&dmeta_facets_obj, facet_key_str, heads)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => Ok(Some(id)),
        _ => Ok(None),
    }
}

fn load_dmeta(
    tx: &mut automerge::transaction::Transaction,
    facets_obj: &automerge::ObjId,
) -> Res<(automerge::ObjId, automerge::ObjId, automerge::ObjId)> {
    let key = dmeta_key();
    let dmeta_obj = match tx.get(facets_obj, &key)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => eyre::bail!("dmeta facet map not found"),
    };
    let dmeta_facets_obj = match tx.get(&dmeta_obj, "facets")? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => eyre::bail!("dmeta.facets map not found"),
    };
    let dmeta_facet_uuids_obj = match tx.get(&dmeta_obj, "facetUuids")? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
        _ => eyre::bail!("dmeta.facetUuids map not found"),
    };
    Ok((dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj))
}

fn set_updated_at_list(
    tx: &mut automerge::transaction::Transaction,
    obj: &automerge::ObjId,
    prop: &str,
    now: Timestamp,
) -> Res<()> {
    let updated_at_list = match tx.get(obj, prop)? {
        Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
        _ => eyre::bail!("missing or invalid {prop} list"),
    };

    let len = tx.length(&updated_at_list);
    for _ in 0..len {
        tx.delete(&updated_at_list, 0)?;
    }
    tx.insert(&updated_at_list, 0, timestamp_scalar(now))?;
    Ok(())
}

fn set_user(
    tx: &mut automerge::transaction::Transaction,
    dmeta_obj: &automerge::ObjId,
    actor_id: &ActorId,
    user_path: Option<&UserPath>,
) -> Res<()> {
    let Some(user_path) = user_path else {
        return Ok(());
    };

    let mut actors =
        match autosurgeon::hydrate_prop::<_, Option<ThroughJson<HashMap<String, UserMeta>>>, _, _>(
            tx, dmeta_obj, "actors",
        )? {
            Some(ThroughJson(map)) => map,
            None => HashMap::new(),
        };
    actors.insert(
        actor_id.to_string(),
        UserMeta {
            user_path: user_path.clone(),
        },
    );
    autosurgeon::reconcile_prop(tx, dmeta_obj, "actors", ThroughJson(actors))?;
    Ok(())
}

pub fn ensure_for_add(
    tx: &mut automerge::transaction::Transaction,
    facets_obj: &automerge::ObjId,
    facet_keys: &[FacetKey],
    now: Timestamp,
    user_path: Option<&UserPath>,
    actor_id: &ActorId,
) -> Res<()> {
    let key = dmeta_key();
    let doc_id = match tx.get(automerge::ROOT, "id")? {
        Some((automerge::Value::Scalar(doc_id_scalar), _)) => {
            if let automerge::ScalarValue::Str(doc_id_str) = doc_id_scalar.as_ref() {
                doc_id_str.to_string()
            } else {
                eyre::bail!("content doc id is not a string");
            }
        }
        _ => eyre::bail!("content doc id not found"),
    };
    let mut facet_uuids = HashMap::new();
    let mut facets = HashMap::new();
    let mut actors = HashMap::new();
    if let Some(user_path) = user_path {
        actors.insert(
            actor_id.to_string(),
            UserMeta {
                user_path: user_path.clone(),
            },
        );
    }
    for facet_key in facet_keys {
        let facet_uuid = Uuid::new_v4();
        facet_uuids.insert(facet_uuid, facet_key.clone());
        facets.insert(
            facet_key.clone(),
            FacetMeta {
                created_at: now,
                uuid: vec![facet_uuid],
                updated_at: vec![now],
                deleted_at: Vec::new(),
            },
        );
    }
    autosurgeon::reconcile_prop(
        tx,
        facets_obj,
        &*key,
        ThroughJson(WellKnownFacet::Dmeta(daybook_types::doc::Dmeta {
            id: doc_id,
            created_at: now,
            updated_at: vec![now],
            actors,
            facet_uuids,
            facets,
        })),
    )?;

    Ok(())
}

fn tombstone_facet_meta(
    tx: &mut automerge::transaction::Transaction,
    dmeta_facets_obj: &automerge::ObjId,
    dmeta_facet_uuids_obj: &automerge::ObjId,
    key_str: &str,
    now: Timestamp,
) -> Res<Vec<Uuid>> {
    let mut invalidated_uuids = Vec::new();
    if let Some((automerge::Value::Object(automerge::ObjType::Map), facet_meta_obj)) =
        tx.get(dmeta_facets_obj, key_str)?
    {
        if let Some((automerge::Value::Object(automerge::ObjType::List), uuid_list)) =
            tx.get(&facet_meta_obj, "uuid")?
        {
            let len = tx.length(&uuid_list);
            for ii in 0..len {
                if let Some((automerge::Value::Scalar(uuid_scalar), _)) = tx.get(&uuid_list, ii)? {
                    if let automerge::ScalarValue::Str(uuid_str) = uuid_scalar.as_ref() {
                        if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                            invalidated_uuids.push(uuid);
                            tx.delete(dmeta_facet_uuids_obj, uuid.to_string())?;
                        }
                    }
                }
            }
        }
        let deleted_at_list = match tx.get(&facet_meta_obj, "deletedAt")? {
            Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
            Some((other, _)) => {
                eyre::bail!(
                    "facet meta deletedAt has invalid shape while tombstoning key {key_str}: {other:?}"
                )
            }
            None => tx.put_object(&facet_meta_obj, "deletedAt", automerge::ObjType::List)?,
        };
        tx.insert(
            &deleted_at_list,
            tx.length(&deleted_at_list),
            timestamp_scalar(now),
        )?;
    }
    Ok(invalidated_uuids)
}

fn touch_facet_meta(
    tx: &mut automerge::transaction::Transaction,
    dmeta_facets_obj: &automerge::ObjId,
    dmeta_facet_uuids_obj: &automerge::ObjId,
    key_str: &str,
    now: Timestamp,
) -> Res<Uuid> {
    let (facet_meta_obj, is_new_meta) = match tx.get(dmeta_facets_obj, key_str)? {
        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => (id, false),
        _ => (
            tx.put_object(dmeta_facets_obj, key_str, automerge::ObjType::Map)?,
            true,
        ),
    };

    if is_new_meta {
        tx.put(&facet_meta_obj, "createdAt", timestamp_scalar(now))?;
    } else if tx.get(&facet_meta_obj, "createdAt")?.is_none() {
        eyre::bail!("facet meta missing createdAt for key {key_str}");
    }

    let updated_at_list = match tx.get(&facet_meta_obj, "updatedAt")? {
        Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
        _ if is_new_meta => {
            tx.put_object(&facet_meta_obj, "updatedAt", automerge::ObjType::List)?
        }
        _ => eyre::bail!("facet meta missing updatedAt list for key {key_str}"),
    };
    let deleted_at_list = match tx.get(&facet_meta_obj, "deletedAt")? {
        Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
        None if is_new_meta => {
            tx.put_object(&facet_meta_obj, "deletedAt", automerge::ObjType::List)?
        }
        Some((other, _)) if is_new_meta => {
            eyre::bail!("facet meta deletedAt has invalid shape for key {key_str}: {other:?}")
        }
        _ => eyre::bail!("facet meta missing deletedAt list for key {key_str}"),
    };

    let uuid_list = match tx.get(&facet_meta_obj, "uuid")? {
        Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
        _ if is_new_meta => tx.put_object(&facet_meta_obj, "uuid", automerge::ObjType::List)?,
        _ => eyre::bail!("facet meta missing uuid list for key {key_str}"),
    };
    let facet_uuid = if tx.length(&uuid_list) > 0 {
        match tx.get(&uuid_list, 0)? {
            Some((automerge::Value::Scalar(uuid_scalar), _)) => {
                if let automerge::ScalarValue::Str(uuid_str) = uuid_scalar.as_ref() {
                    Uuid::parse_str(uuid_str)?
                } else {
                    eyre::bail!("facet meta uuid is not a string for key {key_str}")
                }
            }
            _ => eyre::bail!("facet meta uuid entry missing for key {key_str}"),
        }
    } else if is_new_meta {
        let uuid = Uuid::new_v4();
        tx.insert(&uuid_list, 0, uuid.to_string())?;
        uuid
    } else {
        eyre::bail!("facet meta uuid list empty for key {key_str}");
    };
    tx.put(dmeta_facet_uuids_obj, facet_uuid.to_string(), key_str)?;

    let len = tx.length(&updated_at_list);
    for _ in 0..len {
        tx.delete(&updated_at_list, 0)?;
    }
    tx.insert(&updated_at_list, 0, timestamp_scalar(now))?;
    let deleted_len = tx.length(&deleted_at_list);
    for _ in 0..deleted_len {
        tx.delete(&deleted_at_list, 0)?;
    }

    Ok(facet_uuid)
}

pub fn apply_update(
    tx: &mut automerge::transaction::Transaction,
    facets_obj: &automerge::ObjId,
    facet_keys_set: &[FacetKey],
    facet_keys_remove: &[FacetKey],
    now: Timestamp,
    user_path: Option<&UserPath>,
    actor_id: &ActorId,
) -> Res<Vec<Uuid>> {
    let (dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj) = load_dmeta(tx, facets_obj)?;
    set_updated_at_list(tx, &dmeta_obj, "updatedAt", now)?;
    set_user(tx, &dmeta_obj, actor_id, user_path)?;
    let mut invalidated_uuids = Vec::new();

    for key in facet_keys_remove {
        let key_str = key.to_string();
        invalidated_uuids.extend(tombstone_facet_meta(
            tx,
            &dmeta_facets_obj,
            &dmeta_facet_uuids_obj,
            &key_str,
            now,
        )?);
    }

    for key in facet_keys_set {
        let key_str = key.to_string();
        let facet_uuid =
            touch_facet_meta(tx, &dmeta_facets_obj, &dmeta_facet_uuids_obj, &key_str, now)?;
        invalidated_uuids.push(facet_uuid);
    }

    Ok(invalidated_uuids)
}

pub fn apply_merge(
    tx: &mut automerge::transaction::Transaction,
    facets_obj: &automerge::ObjId,
    modified_facet_key_strs: &std::collections::HashSet<String>,
    now: Timestamp,
    user_path: Option<&UserPath>,
    actor_id: &ActorId,
) -> Res<Vec<Uuid>> {
    if modified_facet_key_strs.is_empty() {
        return Ok(Vec::new());
    }
    let (dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj) = load_dmeta(tx, facets_obj)?;
    set_updated_at_list(tx, &dmeta_obj, "updatedAt", now)?;
    set_user(tx, &dmeta_obj, actor_id, user_path)?;
    let mut invalidated_uuids = Vec::new();
    for key_str in modified_facet_key_strs {
        let facet_uuid =
            touch_facet_meta(tx, &dmeta_facets_obj, &dmeta_facet_uuids_obj, key_str, now)?;
        invalidated_uuids.push(facet_uuid);
    }
    Ok(invalidated_uuids)
}
