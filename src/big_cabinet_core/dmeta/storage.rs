use super::Bytes;
use super::Dmeta;
use crate::DatumKey;
use crate::datum::CodecError;
use crate::datum::CompiledSchema;
use crate::datum::Stored;
use crate::datum::hydrate_value;
use crate::datum::reconcile_json_atomic;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::transaction::Transactable;
use automerge::transaction::Transaction;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Codec(Box<CodecError>),
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

impl From<CodecError> for StorageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(Box::new(error))
    }
}

impl Dmeta {
    /// Write this dmeta over the one stored at `object`/`prop`, preserving the identity of every
    /// signature entry it retains.
    ///
    /// `replaced` names the datums whose competing signature set this write replaces outright. The
    /// typed value only sees the winning copy of a conflicted container, but a concurrent first
    /// write of a slot's metadata leaves two metadata maps (or two signature lists), and the losing
    /// copy's entries are still current signatures. A replacing write retires those too; every
    /// other datum's losing copies are left exactly as they are, since this write never saw them.
    pub fn reconcile_atomic<'a>(
        &self,
        mut tx: Transaction<'a>,
        schema: &CompiledSchema,
        object: &ObjId,
        prop: autosurgeon::Prop<'_>,
        replaced: &BTreeSet<DatumKey>,
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
            let datums = ensure_object(&mut tx, &map, "datums".into(), ObjType::Map)?;
            for (key, metadata) in submitted["datums"]
                .as_object_mut()
                .expect("dmeta datums are a map")
            {
                let datum = ensure_object(&mut tx, &datums, key.as_str().into(), ObjType::Map)?;
                if replaced.iter().any(|replaced| replaced.to_string() == *key) {
                    retire_losing_signatures(&mut tx, &datums, key, &datum)?;
                }
                for field in ["updatedAt", "deletedAt", "signatures"] {
                    reconcile_set(
                        &mut tx,
                        &datum,
                        field,
                        &mut metadata[field],
                        base.get("datums")
                            .and_then(|datums| datums.get(key))
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

/// Empty every signature list under `key` other than the winning list of the winning metadata map.
fn retire_losing_signatures(
    tx: &mut Transaction<'_>,
    datums: &ObjId,
    key: &str,
    winner: &ObjId,
) -> Result<(), StorageError> {
    let winning_list = match tx.get(winner, "signatures")? {
        Some((automerge::Value::Object(ObjType::List), list)) => Some(list),
        _ => None,
    };
    let mut losing = Vec::new();
    for (_, metadata) in tx.get_all(datums, key)? {
        for (value, list) in tx.get_all(&metadata, "signatures")? {
            if value == automerge::Value::Object(ObjType::List)
                && Some(&list) != winning_list.as_ref()
            {
                losing.push(list);
            }
        }
    }
    for list in losing {
        for index in (0..tx.length(&list)).rev() {
            tx.delete(&list, index)?;
        }
    }
    Ok(())
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
    for index in ["actors", "branchUuids", "datumUuids"] {
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
                    if index != "actors" {
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
    let mut wanted: BTreeMap<String, (usize, Value)> = BTreeMap::new();
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
        let key = serde_json::to_string(&value).expect("a set value serializes");
        let entry = wanted.entry(key).or_insert((0, value));
        if field == "signatures" || entry.0 == 0 {
            entry.0 += 1;
        }
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
    let mut matched = BTreeSet::new();
    for (index, value) in observed.iter().enumerate() {
        let identity = serde_json::to_string(value).expect("an observed set value serializes");
        if let Some((remaining, _)) = wanted.get_mut(&identity)
            && *remaining > 0
        {
            *remaining -= 1;
            matched.insert(identity);
            retained.push(value.clone());
        } else {
            removed.push(index);
        }
    }
    for index in removed.into_iter().rev() {
        tx.delete(&list, index)?;
    }
    for (identity, (remaining, value)) in wanted {
        if remaining == 0 || matched.contains(&identity) {
            continue;
        }
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
mod tests;
