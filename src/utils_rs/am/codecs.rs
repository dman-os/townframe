use super::*;
use automerge::ObjId;

/// A newtype wrapper around `serde_json::Value` that implements `Hydrate` and `Reconcile`
/// to allow hydrating JSON values directly from Automerge documents.
#[derive(Debug, Clone, PartialEq)]
pub struct AutosurgeonJson(pub serde_json::Value);

impl autosurgeon::Hydrate for AutosurgeonJson {
    fn hydrate_map<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        use automerge::Value;
        let mut map = serde_json::Map::new();

        for item in doc.map_range(obj.clone(), ..) {
            let key = item.key.to_string();
            let prop = automerge::Prop::Map(key.clone());
            match doc.get(obj, prop)? {
                Some((Value::Object(inner_type), id)) => {
                    let json_value = hydrate_value(doc, &id, inner_type)?;
                    map.insert(key, json_value);
                }
                Some((Value::Scalar(s), _)) => {
                    let json_value = scalar_to_json(s.as_ref());
                    map.insert(key, json_value);
                }
                None => {}
            }
        }

        Ok(AutosurgeonJson(serde_json::Value::Object(map)))
    }

    fn hydrate_seq<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        use automerge::Value;
        let mut arr = Vec::new();

        for i in 0..doc.length(obj) {
            match doc.get(obj, i)? {
                Some((Value::Object(inner_type), id)) => {
                    arr.push(hydrate_value(doc, &id, inner_type)?);
                }
                Some((Value::Scalar(s), _)) => {
                    arr.push(scalar_to_json(s.as_ref()));
                }
                None => {
                    arr.push(serde_json::Value::Null);
                }
            }
        }

        Ok(AutosurgeonJson(serde_json::Value::Array(arr)))
    }

    fn hydrate_text<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        let text = doc.text(obj)?;
        Ok(AutosurgeonJson(serde_json::Value::String(text)))
    }

    fn hydrate_scalar(
        s: std::borrow::Cow<'_, automerge::ScalarValue>,
    ) -> Result<Self, autosurgeon::HydrateError> {
        use automerge::ScalarValue;
        let json_value = match s.as_ref() {
            ScalarValue::Null => serde_json::Value::Null,
            ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
            ScalarValue::Bytes(b) => {
                // Encode bytes as base32 string
                serde_json::Value::String(data_encoding::BASE32_NOPAD.encode(b))
            }
            ScalarValue::Counter(c) => {
                let counter_val: i64 = c.clone().into();
                serde_json::Value::Number(counter_val.into())
            }
            ScalarValue::F64(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
            ),
            ScalarValue::Int(i) => serde_json::Value::Number((*i).into()),
            ScalarValue::Uint(u) => serde_json::Value::Number((*u).into()),
            ScalarValue::Str(s) => serde_json::Value::String(s.to_string()),
            ScalarValue::Timestamp(t) => serde_json::Value::Number((*t).into()),
            ScalarValue::Unknown { .. } => serde_json::Value::Null,
        };
        Ok(AutosurgeonJson(json_value))
    }

    fn hydrate_none() -> Result<Self, autosurgeon::HydrateError> {
        Ok(AutosurgeonJson(serde_json::Value::Null))
    }
}

/// Helper function to hydrate a value from an Automerge object
fn hydrate_value<D: autosurgeon::ReadDoc>(
    doc: &D,
    obj: &automerge::ObjId,
    obj_type: automerge::ObjType,
) -> Result<serde_json::Value, autosurgeon::HydrateError> {
    use automerge::{ObjType, Value};
    match obj_type {
        ObjType::Map | ObjType::Table => {
            let mut map = serde_json::Map::new();
            for item in doc.map_range(obj.clone(), ..) {
                let key = item.key.to_string();
                let prop = automerge::Prop::Map(key.clone());
                match doc.get(obj, prop)? {
                    Some((Value::Object(inner_type), id)) => {
                        let json_value = hydrate_value(doc, &id, inner_type)?;
                        map.insert(key, json_value);
                    }
                    Some((Value::Scalar(s), _)) => {
                        let json_value = scalar_to_json(s.as_ref());
                        map.insert(key, json_value);
                    }
                    None => {}
                }
            }
            Ok(serde_json::Value::Object(map))
        }
        ObjType::List => {
            let mut arr = Vec::new();
            for i in 0..doc.length(obj) {
                match doc.get(obj, i)? {
                    Some((Value::Object(inner_type), id)) => {
                        arr.push(hydrate_value(doc, &id, inner_type)?);
                    }
                    Some((Value::Scalar(s), _)) => {
                        arr.push(scalar_to_json(s.as_ref()));
                    }
                    None => {
                        arr.push(serde_json::Value::Null);
                    }
                }
            }
            Ok(serde_json::Value::Array(arr))
        }
        ObjType::Text => {
            let text = doc.text(obj)?;
            Ok(serde_json::Value::String(text))
        }
    }
}

/// Helper function to convert a scalar value to JSON
fn scalar_to_json(s: &automerge::ScalarValue) -> serde_json::Value {
    use automerge::ScalarValue;
    match s {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
        ScalarValue::Bytes(b) => {
            // Encode bytes as base32 string
            serde_json::Value::String(data_encoding::BASE32_NOPAD.encode(b))
        }
        ScalarValue::Counter(c) => {
            let counter_val: i64 = c.clone().into();
            serde_json::Value::Number(counter_val.into())
        }
        ScalarValue::F64(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
        ),
        ScalarValue::Int(i) => serde_json::Value::Number((*i).into()),
        ScalarValue::Uint(u) => serde_json::Value::Number((*u).into()),
        ScalarValue::Str(s) => serde_json::Value::String(s.to_string()),
        ScalarValue::Timestamp(t) => serde_json::Value::Number((*t).into()),
        ScalarValue::Unknown { .. } => serde_json::Value::Null,
    }
}

impl autosurgeon::Reconcile for AutosurgeonJson {
    type Key<'a> = ();

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        reconcile_json_value(&self.0, reconciler)
    }
}

fn reconcile_json_value<R: autosurgeon::Reconciler>(
    value: &serde_json::Value,
    mut reconciler: R,
) -> Result<(), R::Error> {
    use autosurgeon::reconcile::{MapReconciler, SeqReconciler};
    match value {
        serde_json::Value::Null => reconciler.none(),
        serde_json::Value::Bool(b) => reconciler.boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                reconciler.i64(i)
            } else if let Some(u) = n.as_u64() {
                reconciler.u64(u)
            } else if let Some(f) = n.as_f64() {
                reconciler.f64(f)
            } else {
                reconciler.none()
            }
        }
        serde_json::Value::String(s) => reconciler.str(s),
        serde_json::Value::Array(arr) => {
            let mut seq = reconciler.seq()?;
            // Delete any extra items
            let old_len = seq.len()?;
            if old_len > arr.len() {
                for i in (arr.len()..old_len).rev() {
                    seq.delete(i)?;
                }
            }
            // Set or insert items
            for (idx, item) in arr.iter().enumerate() {
                if idx < old_len {
                    seq.set(idx, &AutosurgeonJson(item.clone()))?;
                } else {
                    seq.insert(idx, &AutosurgeonJson(item.clone()))?;
                }
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            let mut map_reconciler = reconciler.map()?;
            // Get existing keys and delete ones not in the new map
            let old_keys: std::collections::HashSet<String> = map_reconciler
                .entries()
                .map(|(k, _)| k.to_string())
                .collect();
            let new_keys: std::collections::HashSet<String> = map.keys().cloned().collect();
            for key in old_keys.difference(&new_keys) {
                map_reconciler.delete(key)?;
            }
            // Put or update entries
            for (key, value) in map {
                map_reconciler.put(key, &AutosurgeonJson(value.clone()))?;
            }
            Ok(())
        }
    }
}

pub mod date {
    use super::*;

    pub fn reconcile<R: autosurgeon::Reconciler>(
        ts: &OffsetDateTime,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.timestamp(ts.unix_timestamp())
    }

    struct Wrapper(i64);
    impl autosurgeon::Hydrate for Wrapper {
        fn hydrate_timestamp(ts: i64) -> Result<Self, autosurgeon::HydrateError> {
            Ok(Self(ts))
        }
    }

    pub fn hydrate<'a, D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<OffsetDateTime, autosurgeon::HydrateError> {
        let Wrapper(inner) = Wrapper::hydrate(doc, obj, prop)?;
        OffsetDateTime::from_unix_timestamp(inner).map_err(|err| {
            autosurgeon::HydrateError::unexpected(
                "an valid unix timestamp",
                format!("error parsing timestamp int {err}"),
            )
        })
    }
}

pub mod skip {
    use super::*;
    use autosurgeon::{HydrateError, ReadDoc, Reconciler};

    pub fn reconcile<T: Default, R: Reconciler>(
        _value: &T,
        _reconciler: R,
    ) -> Result<(), R::Error> {
        Ok(())
    }

    pub fn hydrate<'a, D: ReadDoc, T: Default>(
        _doc: &D,
        _obj: &ObjId,
        _prop: autosurgeon::Prop<'a>,
    ) -> Result<T, HydrateError> {
        Ok(T::default())
    }
}

pub mod json {
    use super::*;
    use autosurgeon::{HydrateError, ReadDoc};

    /// Hydrate a serde_json::Value from an automerge object
    /// This creates a temporary document with the object at ROOT and uses AutoSerde
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &ObjId,
        _prop: autosurgeon::Prop<'a>,
    ) -> Result<serde_json::Value, HydrateError> {
        // Get the object type to determine how to handle it
        let Some(obj_type) = doc.object_type(obj) else {
            return Ok(serde_json::Value::Null);
        };

        // Create a temporary document with just this object at ROOT
        let mut temp_doc = automerge::Automerge::new();
        temp_doc
            .transact(|tx| {
                copy_object_to_doc(doc, obj, tx, automerge::ROOT, obj_type)?;
                Ok(())
            })
            .map_err(|e| HydrateError::Automerge(e.error))?;

        // Use AutoSerde to convert to JSON
        let autoserde = automerge::AutoSerde::from(&temp_doc);
        serde_json::to_value(&autoserde).map_err(|e| {
            HydrateError::unexpected("valid JSON", format!("error serializing to json: {e}"))
        })
    }

    /// Copy an object from source doc to target doc
    fn copy_object_to_doc(
        source: &impl ReadDoc,
        source_obj: &ObjId,
        target_tx: &mut impl automerge::transaction::Transactable,
        target_obj: ObjId,
        obj_type: automerge::ObjType,
    ) -> Result<(), automerge::AutomergeError> {
        match obj_type {
            automerge::ObjType::Map | automerge::ObjType::Table => {
                for item in source.map_range(source_obj.clone(), ..) {
                    let key = item.key.to_string();
                    let prop = automerge::Prop::Map(key.clone());
                    match source.get(source_obj, prop)? {
                        Some((automerge::Value::Object(inner_type), id)) => {
                            let new_id = target_tx.put_object(
                                &target_obj,
                                automerge::Prop::Map(key.clone()),
                                inner_type,
                            )?;
                            copy_object_to_doc(source, &id, target_tx, new_id, inner_type)?;
                        }
                        Some((automerge::Value::Scalar(s), _)) => {
                            target_tx.put(
                                &target_obj,
                                automerge::Prop::Map(key.clone()),
                                s.as_ref().clone(),
                            )?;
                        }
                        None => {}
                    }
                }
            }
            automerge::ObjType::List => {
                for i in 0..source.length(source_obj) {
                    match source.get(source_obj, i)? {
                        Some((automerge::Value::Object(inner_type), id)) => {
                            let new_id = target_tx.insert_object(&target_obj, i, inner_type)?;
                            copy_object_to_doc(source, &id, target_tx, new_id, inner_type)?;
                        }
                        Some((automerge::Value::Scalar(s), _)) => {
                            target_tx.insert(&target_obj, i, s.as_ref().clone())?;
                        }
                        None => {}
                    }
                }
            }
            automerge::ObjType::Text => {
                let text = source.text(source_obj)?;
                let text_obj = target_tx.put_object(
                    &target_obj,
                    automerge::Prop::Map("_text".to_string()),
                    automerge::ObjType::Text,
                )?;
                target_tx.splice_text(&text_obj, 0, 0, &text)?;
            }
        }
        Ok(())
    }
}
