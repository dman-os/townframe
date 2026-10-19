use super::{Bytes, Dmeta};
use crate::facet::codec::{
    CodecError, CompiledSchema, Stored, hydrate_value, reconcile_json_atomic,
};
use automerge::transaction::{Transactable, Transaction};
use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Hydrate(#[from] autosurgeon::HydrateError),
    #[error(transparent)]
    Automerge(#[from] automerge::AutomergeError),
    #[error("immutable {index} binding {uuid} was changed or removed")]
    ReboundBinding { index: &'static str, uuid: String },
    #[error("expected {expected:?} at {object}/{prop:?}")]
    UnexpectedStorage {
        object: ObjId,
        prop: automerge::Prop,
        expected: ObjType,
    },
}

impl Dmeta {
    pub fn reconcile_atomic<'a>(
        &self,
        mut tx: Transaction<'a>,
        schema: &CompiledSchema,
        object: &ObjId,
        prop: autosurgeon::Prop<'_>,
    ) -> Result<Transaction<'a>, StorageError> {
        let prepared = (|| -> Result<Value, StorageError> {
            let mut submitted = Stored::check(
                schema,
                serde_json::to_value(self).expect("dmeta serializes to JSON"),
            )?
            .into_value();
            let native_prop = match &prop {
                autosurgeon::Prop::Key(key) => automerge::Prop::Map(key.to_string()),
                autosurgeon::Prop::Index(index) => automerge::Prop::Seq(*index as usize),
            };
            for (value, id) in tx.get_all(object, native_prop.clone())? {
                if value != automerge::Value::Object(ObjType::Map) {
                    return Err(StorageError::UnexpectedStorage {
                        object: object.clone(),
                        prop: native_prop,
                        expected: ObjType::Map,
                    });
                }
                check_bindings(&tx, &id, &submitted)?;
            }
            let base = hydrate_value(&tx, object, prop.clone(), schema)?;
            let map = ensure_object(&mut tx, object, native_prop, ObjType::Map)?;
            reconcile_set(
                &mut tx,
                &map,
                "updatedAt",
                &mut submitted["updatedAt"],
                base.get("updatedAt"),
            )?;
            let facets = ensure_object(&mut tx, &map, "facets".into(), ObjType::Map)?;
            for (key, metadata) in submitted["facets"]
                .as_object_mut()
                .expect("dmeta facets are a map")
            {
                let facet = ensure_object(&mut tx, &facets, key.as_str().into(), ObjType::Map)?;
                for field in ["updatedAt", "deletedAt", "signatures"] {
                    reconcile_set(
                        &mut tx,
                        &facet,
                        field,
                        &mut metadata[field],
                        base.get("facets")
                            .and_then(|facets| facets.get(key))
                            .and_then(|metadata| metadata.get(field)),
                    )?;
                }
            }
            Ok(submitted)
        })();
        match prepared {
            Ok(submitted) => reconcile_json_atomic(tx, schema, object, prop, submitted)
                .map_err(StorageError::from),
            Err(error) => {
                tx.rollback();
                Err(error)
            }
        }
    }
}

fn ensure_object(
    tx: &mut Transaction<'_>,
    object: &ObjId,
    prop: automerge::Prop,
    expected: ObjType,
) -> Result<ObjId, StorageError> {
    match tx.get(object, prop.clone())? {
        Some((automerge::Value::Object(kind), id)) if kind == expected => Ok(id),
        Some(_) => Err(StorageError::UnexpectedStorage {
            object: object.clone(),
            prop,
            expected,
        }),
        None => Ok(tx.put_object(object, prop, expected)?),
    }
}

fn check_bindings(
    tx: &Transaction<'_>,
    map: &ObjId,
    submitted: &Value,
) -> Result<(), StorageError> {
    for index in ["actorUuids", "facetUuids"] {
        for (kind, bindings) in tx.get_all(map, index)? {
            if kind != automerge::Value::Object(ObjType::Map) {
                return Err(StorageError::UnexpectedStorage {
                    object: map.clone(),
                    prop: index.into(),
                    expected: ObjType::Map,
                });
            }
            for binding in tx.map_range(&bindings, ..) {
                let rebound = || StorageError::ReboundBinding {
                    index,
                    uuid: binding.key.to_string(),
                };
                let wanted = submitted[index]
                    .get(binding.key.as_ref())
                    .ok_or_else(rebound)?;
                for (value, id) in tx.get_all(&bindings, binding.key.as_ref())? {
                    if index == "facetUuids" {
                        if value.to_str() != wanted.as_str() {
                            return Err(rebound());
                        }
                        continue;
                    }
                    let fields = wanted.as_object().expect("an actor binding is an object");
                    if value != automerge::Value::Object(ObjType::Map)
                        || tx.length(&id) != fields.len()
                    {
                        return Err(rebound());
                    }
                    for (key, expected) in fields {
                        let expected = match expected {
                            Value::String(text) => ScalarValue::Str(text.as_str().into()),
                            _ => {
                                let Bytes(bytes) = serde_json::from_value(expected.clone())
                                    .expect("an actor's non-string field is bytes");
                                ScalarValue::Bytes(bytes)
                            }
                        };
                        let held = tx.get_all(&id, key.as_str())?;
                        if held.is_empty()
                            || held
                                .iter()
                                .any(|(value, _)| value.to_scalar() != Some(&expected))
                        {
                            return Err(rebound());
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn reconcile_set(
    tx: &mut Transaction<'_>,
    object: &ObjId,
    field: &'static str,
    submitted: &mut Value,
    base: Option<&Value>,
) -> Result<(), StorageError> {
    let list = ensure_object(tx, object, field.into(), ObjType::List)?;
    let mut wanted = BTreeMap::new();
    let Value::Array(values) = std::mem::take(submitted) else {
        unreachable!("dmeta sets are arrays");
    };
    for mut value in values {
        if field != "signatures" {
            let instant: jiff::Timestamp = value
                .as_str()
                .expect("a timestamp is a string")
                .parse()
                .expect("a typed timestamp parses");
            value = Value::String(
                jiff::Timestamp::from_millisecond(instant.as_millisecond())
                    .expect("a timestamp's milliseconds are in range")
                    .to_string(),
            );
        }
        wanted
            .entry(serde_json::to_string(&value).expect("a set value serializes"))
            .or_insert(value);
    }
    let observed = match base {
        Some(Value::Array(values)) => values.as_slice(),
        None => &[],
        Some(_) => {
            return Err(StorageError::UnexpectedStorage {
                object: object.clone(),
                prop: field.into(),
                expected: ObjType::List,
            });
        }
    };
    assert_eq!(tx.length(&list), observed.len());
    let mut retained = Vec::with_capacity(wanted.len());
    let mut removed = Vec::new();
    for (index, value) in observed.iter().enumerate() {
        let identity = serde_json::to_string(value).expect("an observed set value serializes");
        if wanted.remove(&identity).is_some() {
            retained.push(value.clone());
        } else {
            removed.push(index);
        }
    }
    for index in removed.into_iter().rev() {
        tx.delete(&list, index)?;
    }
    for value in wanted.into_values() {
        if field == "signatures" {
            tx.insert_object(&list, retained.len(), ObjType::Map)?;
        } else {
            let instant: jiff::Timestamp = value
                .as_str()
                .expect("a timestamp is a string")
                .parse()
                .expect("a normalized timestamp parses");
            tx.insert(
                &list,
                retained.len(),
                ScalarValue::Timestamp(instant.as_millisecond()),
            )?;
        }
        retained.push(value);
    }
    *submitted = Value::Array(retained);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dmeta::{
        ActorMetadata, FacetMetadata, FacetSignature, InheritedWitness, MultibaseKey,
        SignatureBytes, Uuid7,
    };
    use crate::facet::{Bundle, Declaration, Universe};
    use automerge::{ActorId, Automerge, ROOT};
    use serde_json::json;

    fn schema() -> CompiledSchema {
        let universe = Universe::collect(vec![Declaration::of::<Dmeta>().unwrap()]);
        let bundle = Bundle::new(&universe, Vec::new());
        CompiledSchema::compile(serde_json::to_value(bundle.document("Dmeta").unwrap()).unwrap())
            .unwrap()
    }

    fn fixture() -> Dmeta {
        let mut dmeta = Dmeta::new(jiff::Timestamp::from_millisecond(0).unwrap());
        let actor = Uuid7::new();
        dmeta.actor_uuids.insert(
            actor,
            ActorMetadata {
                pubkey: MultibaseKey::try_from([1; 32].as_slice()).unwrap(),
                actor_url: "actor:alice".to_owned(),
                delegation: Some(Bytes::from(vec![2, 3])),
            },
        );
        let key = "org.example.note/main".parse().unwrap();
        dmeta.facet_uuids.insert(Uuid7::new(), key);
        let key = "org.example.note/main".parse().unwrap();
        dmeta.facets.insert(
            key,
            FacetMetadata {
                updated_at: Vec::new(),
                deleted_at: Vec::new(),
                signatures: Vec::new(),
            },
        );
        dmeta.extra.insert(
            "nested".to_owned(),
            json!({
                "stable": [{"blob": {"$bytes": "AQI="}, "label": "unchanged"}],
                "changed": "before"
            }),
        );
        dmeta
    }

    fn signature(dmeta: &Dmeta, byte: u8) -> FacetSignature {
        FacetSignature {
            actor_uuid: *dmeta.actor_uuids.keys().next().unwrap(),
            signature: SignatureBytes::try_from([byte; 64].as_slice()).unwrap(),
            message: Some(Bytes::from(vec![byte, 0])),
            inherited: Some(InheritedWitness {
                origin_doc: crate::dmeta::DocumentId::new(vec![byte]),
                message: Bytes::from(vec![byte, 1]),
                value: Some(Bytes::from(vec![byte, 2])),
            }),
        }
    }

    fn write(doc: &mut Automerge, schema: &CompiledSchema, dmeta: &Dmeta) -> usize {
        let tx = dmeta
            .reconcile_atomic(doc.transaction(), schema, &ROOT, "dmeta".into())
            .unwrap();
        let count = tx.pending_ops();
        tx.commit();
        count
    }

    fn read(doc: &Automerge, schema: &CompiledSchema) -> Dmeta {
        serde_json::from_value(hydrate_value(doc, &ROOT, "dmeta".into(), schema).unwrap()).unwrap()
    }

    fn objects(doc: &Automerge) -> (ObjId, ObjId, [ObjId; 4]) {
        let root = doc.get(ROOT, "dmeta").unwrap().unwrap().1;
        let facets = doc.get(&root, "facets").unwrap().unwrap().1;
        let facet = doc
            .get(&facets, "org.example.note/main")
            .unwrap()
            .unwrap()
            .1;
        let lists = [
            doc.get(&root, "updatedAt").unwrap().unwrap().1,
            doc.get(&facet, "updatedAt").unwrap().unwrap().1,
            doc.get(&facet, "deletedAt").unwrap().unwrap().1,
            doc.get(&facet, "signatures").unwrap().unwrap().1,
        ];
        (root, facet, lists)
    }

    #[test]
    fn initializes_lists_and_preserves_nested_identity_on_change_and_resubmission() {
        let schema = schema();
        let mut value = fixture();
        let mut doc = Automerge::new();
        assert!(write(&mut doc, &schema, &value) > 0);
        let before = objects(&doc);
        for list in &before.2 {
            assert_eq!(doc.object_type(list).unwrap(), ObjType::List);
            assert_eq!(doc.length(list), 0);
        }
        let nested = doc.get(&before.0, "nested").unwrap().unwrap().1;
        let stable = doc.get(&nested, "stable").unwrap().unwrap().1;
        let item = doc.get(&stable, 0).unwrap().unwrap().1;
        let entries = doc
            .get_all(&item, "blob")
            .unwrap()
            .into_iter()
            .map(|(value, id)| (value.into_owned(), id))
            .collect::<Vec<_>>();
        value.extra.get_mut("nested").unwrap()["changed"] = json!("after");
        assert_eq!(write(&mut doc, &schema, &value), 1);
        assert_eq!(objects(&doc), before);
        assert_eq!(doc.get(&before.0, "nested").unwrap().unwrap().1, nested);
        assert_eq!(doc.get(&nested, "stable").unwrap().unwrap().1, stable);
        assert_eq!(doc.get(&stable, 0).unwrap().unwrap().1, item);
        assert_eq!(doc.get_all(&item, "blob").unwrap(), entries);
        assert_eq!(read(&doc, &schema), value);
        let heads = doc.get_heads();
        assert_eq!(write(&mut doc, &schema, &value), 0);
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(objects(&doc), before);
    }

    #[test]
    fn timestamps_replace_instead_of_growing_and_resurrection_clears_observed_deletions() {
        let schema = schema();
        let mut value = fixture();
        let mut doc = Automerge::new();
        write(&mut doc, &schema, &value);
        let before = objects(&doc);
        let first = jiff::Timestamp::from_millisecond(1000).unwrap();
        let second = jiff::Timestamp::from_millisecond(2000).unwrap();
        value.updated_at = vec![first];
        value.facets.values_mut().next().unwrap().updated_at = vec![first];
        assert_eq!(write(&mut doc, &schema, &value), 2);
        value.updated_at = vec![second];
        value.facets.values_mut().next().unwrap().updated_at = vec![second];
        assert_eq!(write(&mut doc, &schema, &value), 4);
        assert_eq!(read(&doc, &schema), value);
        value.facets.values_mut().next().unwrap().deleted_at = vec![second];
        assert_eq!(write(&mut doc, &schema, &value), 1);
        value.facets.values_mut().next().unwrap().deleted_at.clear();
        assert_eq!(write(&mut doc, &schema, &value), 1);
        assert_eq!(read(&doc, &schema), value);
        assert_eq!(objects(&doc), before);
        assert_eq!(doc.length(&before.2[2]), 0);
        assert_eq!(write(&mut doc, &schema, &value), 0);
    }

    #[test]
    fn concurrent_replacements_keep_both_signatures_intact_at_supplied_heads() {
        let schema = schema();
        let mut initial = fixture();
        let first = signature(&initial, 1);
        initial.updated_at = vec![initial.created_at];
        let metadata = initial.facets.values_mut().next().unwrap();
        metadata.signatures = vec![first];
        metadata.updated_at = vec![initial.created_at];
        metadata.deleted_at = vec![initial.created_at];
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from([1u8; 32]));
        write(&mut doc, &schema, &initial);
        let base = doc.get_heads();
        let before = objects(&doc);
        let old_entry = doc.get(&before.2[3], 0).unwrap().unwrap().1;
        let mut peer = doc.fork();
        peer.set_actor(ActorId::from([2u8; 32]));
        let mut left = initial.clone();
        let mut right = initial.clone();
        left.updated_at = vec![jiff::Timestamp::from_millisecond(1000).unwrap()];
        right.updated_at = vec![jiff::Timestamp::from_millisecond(2000).unwrap()];
        left.facets.values_mut().next().unwrap().signatures = vec![signature(&initial, 2)];
        right.facets.values_mut().next().unwrap().signatures = vec![signature(&initial, 3)];
        left.facets.values_mut().next().unwrap().updated_at = left.updated_at.clone();
        right.facets.values_mut().next().unwrap().updated_at = right.updated_at.clone();
        left.facets.values_mut().next().unwrap().deleted_at.clear();
        right.facets.values_mut().next().unwrap().deleted_at = right.updated_at.clone();
        right.actor_uuids.insert(
            Uuid7::new(),
            ActorMetadata {
                pubkey: MultibaseKey::try_from([4; 32].as_slice()).unwrap(),
                actor_url: "actor:bob".to_owned(),
                delegation: None,
            },
        );
        right
            .facet_uuids
            .insert(Uuid7::new(), "org.example.note/main".parse().unwrap());
        write(&mut peer, &schema, &right);
        doc.merge(&mut peer).unwrap();
        let tx = doc
            .transaction_at(automerge::PatchLog::null(), &base)
            .unwrap();
        left.reconcile_atomic(tx, &schema, &ROOT, "dmeta".into())
            .unwrap()
            .commit();
        peer.merge(&mut doc).unwrap();
        for merged in [&doc, &peer] {
            assert_eq!(objects(merged), before);
            let observed = read(merged, &schema);
            let signatures = &observed.facets.values().next().unwrap().signatures;
            assert_eq!(signatures.len(), 2);
            assert!(signatures.contains(&signature(&initial, 2)));
            assert!(signatures.contains(&signature(&initial, 3)));
            assert_eq!(observed.updated_at.len(), 2);
            assert!(observed.updated_at.contains(&left.updated_at[0]));
            assert!(observed.updated_at.contains(&right.updated_at[0]));
            let metadata = observed.facets.values().next().unwrap();
            assert_eq!(metadata.updated_at.len(), 2);
            assert!(metadata.updated_at.contains(&left.updated_at[0]));
            assert!(metadata.updated_at.contains(&right.updated_at[0]));
            assert_eq!(metadata.deleted_at, right.updated_at);
            assert_eq!(observed.actor_uuids, right.actor_uuids);
            assert_eq!(observed.facet_uuids, right.facet_uuids);
            for index in 0..2 {
                let entry = merged.get(&before.2[3], index).unwrap().unwrap().1;
                assert_ne!(entry, old_entry);
                assert_eq!(merged.get_all(&before.2[3], index).unwrap().len(), 1);
                assert_eq!(merged.get_all(&entry, "signature").unwrap().len(), 1);
                assert!(
                    matches!(merged.get(&entry, "signature").unwrap().unwrap().0.to_scalar(),
                    Some(ScalarValue::Bytes(bytes)) if bytes.len() == 64)
                );
                assert!(matches!(
                    merged
                        .get(&entry, "message")
                        .unwrap()
                        .unwrap()
                        .0
                        .to_scalar(),
                    Some(ScalarValue::Bytes(_))
                ));
            }
        }
        let merged = read(&doc, &schema);
        assert_eq!(write(&mut doc, &schema, &merged), 0);
    }

    #[test]
    fn set_order_duplicates_and_timestamp_precision_do_not_rewrite_equal_entries() {
        let schema = schema();
        let mut value = fixture();
        let first = signature(&value, 1);
        let second = signature(&value, 2);
        value.facets.values_mut().next().unwrap().signatures =
            vec![first.clone(), second.clone(), first.clone()];
        let fractional: jiff::Timestamp = "2024-01-01T00:00:00.123456Z".parse().unwrap();
        value.updated_at = vec![fractional, fractional];
        let mut doc = Automerge::new();
        write(&mut doc, &schema, &value);
        let lists = objects(&doc).2;
        assert_eq!(doc.length(&lists[0]), 1);
        assert_eq!(doc.length(&lists[3]), 2);
        let entries = (0..2)
            .map(|index| doc.get(&lists[3], index).unwrap().unwrap().1)
            .collect::<Vec<_>>();
        value.facets.values_mut().next().unwrap().signatures = vec![second, first];
        assert_eq!(write(&mut doc, &schema, &value), 0);
        assert_eq!(
            (0..2)
                .map(|index| doc.get(&lists[3], index).unwrap().unwrap().1)
                .collect::<Vec<_>>(),
            entries
        );
        let mut tx = doc.transaction();
        tx.insert(
            &lists[0],
            1,
            ScalarValue::Timestamp(fractional.as_millisecond()),
        )
        .unwrap();
        tx.commit();
        assert_eq!(write(&mut doc, &schema, &value), 1);
        assert_eq!(doc.length(&lists[0]), 1);
        assert_eq!(write(&mut doc, &schema, &value), 0);
    }

    #[test]
    fn a_hidden_conflicting_binding_is_not_silently_rebound_to_the_winner() {
        let schema = schema();
        for index in ["actorUuids", "facetUuids"] {
            let initial = fixture();
            let mut doc = Automerge::new();
            doc.set_actor(ActorId::from([1u8; 32]));
            write(&mut doc, &schema, &initial);
            let root = objects(&doc).0;
            let bindings = doc.get(&root, index).unwrap().unwrap().1;
            let uuid = if index == "actorUuids" {
                initial.actor_uuids.keys().next().unwrap().to_string()
            } else {
                initial.facet_uuids.keys().next().unwrap().to_string()
            };
            let (object, prop) = if index == "actorUuids" {
                (
                    doc.get(&bindings, uuid.as_str()).unwrap().unwrap().1,
                    "pubkey".to_owned(),
                )
            } else {
                (bindings, uuid)
            };
            let mut peer = doc.fork();
            peer.set_actor(ActorId::from([2u8; 32]));
            for (branch, byte) in [(&mut doc, 2), (&mut peer, 3)] {
                let replacement = if index == "actorUuids" {
                    MultibaseKey::try_from([byte; 32].as_slice())
                        .unwrap()
                        .to_string()
                } else {
                    format!("org.example.other{byte}/main")
                };
                let mut tx = branch.transaction();
                tx.put(&object, prop.as_str(), replacement).unwrap();
                tx.commit();
            }
            doc.merge(&mut peer).unwrap();
            let observed = read(&doc, &schema);
            let heads = doc.get_heads();
            assert_eq!(doc.get_all(&object, prop.as_str()).unwrap().len(), 2);
            let error = observed
                .reconcile_atomic(doc.transaction(), &schema, &ROOT, "dmeta".into())
                .unwrap_err();
            assert!(
                matches!(error, StorageError::ReboundBinding { index: actual, .. } if actual == index)
            );
            assert_eq!(doc.get_heads(), heads);
            assert_eq!(doc.get_all(&object, prop.as_str()).unwrap().len(), 2);
        }
    }

    #[test]
    fn rejected_bindings_and_late_codec_errors_roll_back_the_owned_transaction() {
        let schema = schema();
        let initial = fixture();
        let mut doc = Automerge::new();
        write(&mut doc, &schema, &initial);
        let heads = doc.get_heads();
        for index in ["actorUuids", "facetUuids"] {
            for remove in [false, true] {
                let mut submitted = initial.clone();
                if index == "actorUuids" {
                    if remove {
                        submitted.actor_uuids.clear();
                    } else {
                        submitted.actor_uuids.values_mut().next().unwrap().pubkey =
                            MultibaseKey::try_from([9; 32].as_slice()).unwrap();
                    }
                } else if remove {
                    submitted.facet_uuids.clear();
                } else {
                    *submitted.facet_uuids.values_mut().next().unwrap() =
                        "org.example.other/main".parse().unwrap();
                }
                let mut tx = doc.transaction();
                tx.put(ROOT, "pending", "must roll back").unwrap();
                let error = submitted
                    .reconcile_atomic(tx, &schema, &ROOT, "dmeta".into())
                    .unwrap_err();
                assert!(matches!(error, StorageError::ReboundBinding { .. }));
                assert!(doc.get(ROOT, "pending").unwrap().is_none());
                assert_eq!(doc.get_heads(), heads);
                assert_eq!(read(&doc, &schema), initial);
            }
        }
        let mut invalid = initial.clone();
        invalid.extra.insert("$invalid".to_owned(), json!(true));
        let mut tx = doc.transaction();
        tx.put(ROOT, "pending", true).unwrap();
        assert!(matches!(
            invalid.reconcile_atomic(tx, &schema, &ROOT, "dmeta".into()),
            Err(StorageError::Codec(_))
        ));
        assert!(doc.get(ROOT, "pending").unwrap().is_none());
        assert_eq!(doc.get_heads(), heads);
        let root = objects(&doc).0;
        let mut tx = doc.transaction();
        tx.put(&root, "opaque", ScalarValue::Bytes(vec![1]))
            .unwrap();
        tx.commit();
        let heads = doc.get_heads();
        let mut submitted = initial.clone();
        submitted.updated_at = vec![jiff::Timestamp::from_millisecond(1000).unwrap()];
        submitted
            .extra
            .insert("opaque".to_owned(), json!("not bytes"));
        let mut tx = doc.transaction();
        tx.put(ROOT, "pending", true).unwrap();
        let error = submitted
            .reconcile_atomic(tx, &schema, &ROOT, "dmeta".into())
            .unwrap_err();
        assert!(matches!(error, StorageError::Codec(_)));
        assert!(doc.get(ROOT, "pending").unwrap().is_none());
        assert_eq!(doc.get_heads(), heads);
        assert_eq!(doc.length(&objects(&doc).2[0]), 0);
    }
}
