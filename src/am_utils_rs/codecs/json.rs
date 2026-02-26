use crate::interlude::*;

use automerge::ObjId;

use automerge::*;
use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
use std::borrow::Cow;
use std::collections::HashSet;

const BASE64_FIELD_SUFFIX: &str = "Base64";

fn decode_base64_field(value: &str) -> Option<Vec<u8>> {
    data_encoding::BASE64
        .decode(value.as_bytes())
        .ok()
        .or_else(|| data_encoding::BASE64_NOPAD.decode(value.as_bytes()).ok())
}

fn encode_base64_field(value: &[u8]) -> String {
    data_encoding::BASE64.encode(value)
}

fn is_base64_field(key: &str) -> bool {
    key.ends_with(BASE64_FIELD_SUFFIX)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JsonHeuristicKey<'a> {
    Str(Cow<'a, str>),
    U64(u64),
    I64(i64),
}

fn heuristic_json_key_from_value<'a>(value: &'a serde_json::Value) -> Option<JsonHeuristicKey<'a>> {
    match value {
        serde_json::Value::String(text) => Some(JsonHeuristicKey::Str(Cow::Borrowed(text))),
        serde_json::Value::Number(number) => {
            if let Some(uint_value) = number.as_u64() {
                Some(JsonHeuristicKey::U64(uint_value))
            } else {
                number.as_i64().map(JsonHeuristicKey::I64)
            }
        }
        _ => None,
    }
}

fn heuristic_json_object_key<'a>(value: &'a serde_json::Value) -> Option<JsonHeuristicKey<'a>> {
    let obj = match value {
        serde_json::Value::Object(obj) => obj,
        _ => return None,
    };
    if let Some(key) = obj.get("id").and_then(heuristic_json_key_from_value) {
        return Some(key);
    }
    obj.get("key").and_then(heuristic_json_key_from_value)
}

fn can_use_heuristic_keyed_array(values: &[serde_json::Value]) -> bool {
    if values.is_empty() {
        return false;
    }
    let mut seen = HashSet::with_capacity(values.len());
    for value in values {
        let Some(key) = heuristic_json_object_key(value) else {
            return false;
        };
        if !seen.insert(key) {
            return false;
        }
    }
    true
}

fn hydrate_heuristic_key<'a, D: ReadDoc>(
    doc: &D,
    obj: &automerge::ObjId,
    prop: Prop<'_>,
) -> Result<autosurgeon::reconcile::LoadKey<JsonHeuristicKey<'a>>, autosurgeon::ReconcileError> {
    let Some((automerge::Value::Object(_), item_obj)) = doc.get(obj, &prop)? else {
        return Ok(autosurgeon::reconcile::LoadKey::KeyNotFound);
    };
    for field in ["id", "key"] {
        let Some((automerge::Value::Scalar(scalar), _)) = doc.get(&item_obj, field)? else {
            continue;
        };
        match scalar.as_ref() {
            automerge::ScalarValue::Str(text) => {
                return Ok(autosurgeon::reconcile::LoadKey::Found(
                    JsonHeuristicKey::Str(Cow::Owned(text.to_string())),
                ));
            }
            automerge::ScalarValue::Uint(uint_value) => {
                return Ok(autosurgeon::reconcile::LoadKey::Found(
                    JsonHeuristicKey::U64(*uint_value),
                ));
            }
            automerge::ScalarValue::Int(int_value) => {
                if let Ok(uint_value) = u64::try_from(*int_value) {
                    return Ok(autosurgeon::reconcile::LoadKey::Found(
                        JsonHeuristicKey::U64(uint_value),
                    ));
                }
                return Ok(autosurgeon::reconcile::LoadKey::Found(
                    JsonHeuristicKey::I64(*int_value),
                ));
            }
            _ => {}
        }
    }
    Ok(autosurgeon::reconcile::LoadKey::KeyNotFound)
}

struct HeuristicallyKeyedJsonValue<'a>(&'a serde_json::Value);

impl autosurgeon::Reconcile for HeuristicallyKeyedJsonValue<'_> {
    type Key<'a> = JsonHeuristicKey<'a>;

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        reconcile_value(self.0, reconciler)
    }

    fn hydrate_key<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<autosurgeon::reconcile::LoadKey<Self::Key<'a>>, autosurgeon::ReconcileError> {
        hydrate_heuristic_key(doc, obj, prop)
    }

    fn key(&self) -> autosurgeon::reconcile::LoadKey<Self::Key<'_>> {
        heuristic_json_object_key(self.0)
            .map(autosurgeon::reconcile::LoadKey::Found)
            .unwrap_or(autosurgeon::reconcile::LoadKey::KeyNotFound)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct ThroughJson<T>(pub T);

impl<T> std::ops::Deref for ThroughJson<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for ThroughJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for ThroughJson<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

pub fn reconcile<T: serde::Serialize, R: Reconciler>(
    value: &T,
    reconciler: R,
) -> Result<(), R::Error> {
    let value = serde_json::to_value(value).expect(ERROR_JSON);
    reconcile_value(&value, reconciler)
}

pub fn reconcile_value<R: Reconciler>(
    value: &serde_json::Value,
    mut reconciler: R,
) -> Result<(), R::Error> {
    match value {
        serde_json::Value::Null => reconciler.none(),
        serde_json::Value::Bool(val) => reconciler.boolean(*val),
        serde_json::Value::Number(val) => {
            if let Some(int) = val.as_u64() {
                reconciler.u64(int)
            } else if let Some(uint) = val.as_i64() {
                reconciler.i64(uint)
            } else if let Some(float) = val.as_f64() {
                reconciler.f64(float)
            } else {
                panic!("attempt to reconcile unsupported json number: {val}")
            }
        }
        serde_json::Value::String(val) => reconciler.str(val),
        serde_json::Value::Array(val) => {
            if can_use_heuristic_keyed_array(val) {
                let keyed = val
                    .iter()
                    .map(HeuristicallyKeyedJsonValue)
                    .collect::<Vec<_>>();
                return autosurgeon::Reconcile::reconcile(&keyed, reconciler);
            }

            use autosurgeon::reconcile::SeqReconciler;

            let mut seq = reconciler.seq()?;
            // Delete any extra items
            let old_len = seq.len()?;
            if old_len > val.len() {
                for idx in (val.len()..old_len).rev() {
                    seq.delete(idx)?;
                }
            }
            // Set or insert items
            for (idx, item) in val.iter().enumerate() {
                if idx < old_len {
                    seq.set(idx, ThroughJson(item))?;
                } else {
                    seq.insert(idx, ThroughJson(item))?;
                }
            }
            Ok(())
        }
        serde_json::Value::Object(val) => {
            use autosurgeon::reconcile::MapReconciler;

            let mut map_reconciler = reconciler.map()?;
            let mut keys_to_del = vec![];
            for (key, _) in map_reconciler.entries() {
                if !val.contains_key(&key[..]) {
                    keys_to_del.push(key.to_string());
                }
            }
            for key in keys_to_del {
                map_reconciler.delete(key)?;
            }
            // Put or update entries
            for (key, value) in val {
                if is_base64_field(key) {
                    if let serde_json::Value::String(encoded) = value {
                        if let Some(bytes) = decode_base64_field(encoded) {
                            map_reconciler.put(key, autosurgeon::bytes::ByteVec::from(bytes))?;
                            continue;
                        }
                        warn!(key, "invalid base64 payload, storing as string");
                    }
                }
                map_reconciler.put(key, ThroughJson(value))?;
            }
            Ok(())
        }
    }
}

pub fn hydrate<'a, D: ReadDoc, T: serde::de::DeserializeOwned>(
    doc: &D,
    obj: &ObjId,
    prop: autosurgeon::Prop<'a>,
) -> Result<T, HydrateError> {
    let value = ThroughJson::hydrate(doc, obj, prop)?;
    Ok(value.0)
}

pub fn hydrate_value<'a, D: ReadDoc>(
    doc: &D,
    obj: &ObjId,
    prop: autosurgeon::Prop<'a>,
) -> Result<serde_json::Value, HydrateError> {
    let value = match doc.get(obj, &prop)? {
        Some((automerge::Value::Scalar(scalar), _)) => ThroughJson::hydrate_scalar(scalar),
        Some((automerge::Value::Object(obj_type), obj)) => match obj_type {
            ObjType::Map | ObjType::Table => ThroughJson::hydrate_map(doc, &obj),
            ObjType::List => ThroughJson::hydrate_seq(doc, &obj),
            ObjType::Text => ThroughJson::hydrate_text(doc, &obj),
        },
        None => ThroughJson::hydrate_none(),
    }?;
    Ok(value.0)
}

pub fn from_value<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, HydrateError> {
    match serde_json::from_value(value) {
        Ok(value) => Ok(value),
        Err(err) => Err(autosurgeon::HydrateError::unexpected(
            format!("a json repr of {}", std::any::type_name::<T>()),
            format!("failure parsing json: {err:?}"),
        )),
    }
}

impl<T> autosurgeon::Reconcile for ThroughJson<T>
where
    T: serde::Serialize,
{
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        reconcile(&self.0, reconciler)
    }
}

impl<T> autosurgeon::Hydrate for ThroughJson<T>
where
    T: serde::de::DeserializeOwned,
{
    fn hydrate<D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<Self, HydrateError> {
        let value = hydrate_value(doc, obj, prop)?;
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_bool(value: bool) -> Result<Self, HydrateError> {
        let value = serde_json::Value::Bool(value);
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_bytes(value: &[u8]) -> Result<Self, HydrateError> {
        let value = serde_json::Value::Array(
            value
                .iter()
                .map(|byte| serde_json::Value::Number(serde_json::Number::from(*byte)))
                .collect(),
        );
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_f64(value: f64) -> Result<Self, HydrateError> {
        let value =
            serde_json::Value::Number(serde_json::Number::from_f64(value).ok_or_else(|| {
                autosurgeon::HydrateError::unexpected(
                    "a real float",
                    format!("NaN and Infinite unsupported by json, found: {value}"),
                )
            })?);
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_counter(value: i64) -> Result<Self, HydrateError> {
        let value = serde_json::Value::Number(value.into());
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_int(value: i64) -> Result<Self, HydrateError> {
        let value = serde_json::Value::Number(serde_json::Number::from(value));
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_uint(value: u64) -> Result<Self, HydrateError> {
        let value = serde_json::Value::Number(serde_json::Number::from(value));
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_string(value: &'_ str) -> Result<Self, HydrateError> {
        let value = serde_json::Value::String(String::from(value));
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_timestamp(value: i64) -> Result<Self, HydrateError> {
        let value = match Timestamp::from_second(value) {
            Ok(ts) => serde_json::Value::String(ts.to_string()),
            Err(err) => {
                return Err(autosurgeon::HydrateError::unexpected(
                    "a valid timestamp",
                    format!("error converting timestamp: {err}"),
                ));
            }
        };
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_unknown(_type_code: u8, _bytes: &[u8]) -> Result<Self, HydrateError> {
        Err(HydrateError::Unexpected(
            autosurgeon::hydrate::Unexpected::Unknown,
        ))
    }

    fn hydrate_map<D: ReadDoc>(doc: &D, obj: &automerge::ObjId) -> Result<Self, HydrateError> {
        let value = {
            let mut map = serde_json::Map::new();
            for item in doc.map_range(obj, ..) {
                let value = match doc.get(obj, item.key.clone())? {
                    Some((automerge::Value::Scalar(scalar), _)) if is_base64_field(&item.key) => {
                        match scalar.as_ref() {
                            automerge::ScalarValue::Bytes(bytes) => {
                                serde_json::Value::String(encode_base64_field(bytes))
                            }
                            _ => hydrate_value(doc, obj, autosurgeon::Prop::Key(item.key.clone()))?,
                        }
                    }
                    _ => hydrate_value(doc, obj, autosurgeon::Prop::Key(item.key.clone()))?,
                };
                map.insert(item.key.to_string(), value);
            }
            serde_json::Value::Object(map)
        };
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_seq<D: ReadDoc>(doc: &D, obj: &automerge::ObjId) -> Result<Self, HydrateError> {
        let value = {
            let mut arr = Vec::new();
            for ii in 0..doc.length(obj) {
                let value = hydrate_value(doc, obj, autosurgeon::Prop::Index(ii as u32))?;
                arr.push(value);
            }
            serde_json::Value::Array(arr)
        };
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_text<D: ReadDoc>(doc: &D, obj: &automerge::ObjId) -> Result<Self, HydrateError> {
        let value = {
            let text = doc.text(obj)?;
            serde_json::Value::String(text)
        };
        let value = from_value(value)?;
        Ok(Self(value))
    }

    fn hydrate_none() -> Result<Self, HydrateError> {
        let value = serde_json::Value::Null;
        let value = from_value(value)?;
        Ok(Self(value))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        PartialEq,
        autosurgeon::Hydrate,
        autosurgeon::Reconcile,
    )]
    struct Scalars {
        null: Option<bool>,
        bool: bool,
        int_1: i64,
        int_2: i32,
        uint_1: u64,
        uint_2: u32,
        float_1: f64,
        float_2: f32,
        string: String,
    }
    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        PartialEq,
        autosurgeon::Hydrate,
        autosurgeon::Reconcile,
    )]
    struct Objects {
        map: HashMap<String, Foo>,
        arr: Vec<Foo>,
    }
    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        PartialEq,
        autosurgeon::Hydrate,
        autosurgeon::Reconcile,
    )]
    struct Foo {
        scalars: Scalars,
        obj: Objects,
    }

    fn test_val() -> Foo {
        let scalars = Scalars {
            null: None,
            bool: true,
            int_1: -123,
            int_2: -456,
            uint_1: 789,
            uint_2: 145,
            float_1: 256.,
            float_2: 357.,
            string: "Magic Alive!".into(),
        };
        Foo {
            scalars: scalars.clone(),
            obj: Objects {
                map: [
                    (
                        "first".into(),
                        Foo {
                            scalars: scalars.clone(),
                            obj: Objects {
                                arr: vec![Foo {
                                    scalars: scalars.clone(),
                                    obj: Objects {
                                        map: default(),
                                        arr: default(),
                                    },
                                }],
                                map: [(
                                    "nested".into(),
                                    Foo {
                                        scalars: scalars.clone(),
                                        obj: Objects {
                                            map: default(),
                                            arr: default(),
                                        },
                                    },
                                )]
                                .into(),
                            },
                        },
                    ),
                    (
                        "second".into(),
                        Foo {
                            scalars: scalars.clone(),
                            obj: Objects {
                                arr: vec![Foo {
                                    scalars: scalars.clone(),
                                    obj: Objects {
                                        map: default(),
                                        arr: default(),
                                    },
                                }],
                                map: [(
                                    "nested".into(),
                                    Foo {
                                        scalars: scalars.clone(),
                                        obj: Objects {
                                            map: default(),
                                            arr: default(),
                                        },
                                    },
                                )]
                                .into(),
                            },
                        },
                    ),
                ]
                .into(),
                arr: vec![
                    Foo {
                        scalars: scalars.clone(),
                        obj: Objects {
                            arr: vec![Foo {
                                scalars: scalars.clone(),
                                obj: Objects {
                                    map: default(),
                                    arr: default(),
                                },
                            }],
                            map: [(
                                "nested".into(),
                                Foo {
                                    scalars: scalars.clone(),
                                    obj: Objects {
                                        map: default(),
                                        arr: default(),
                                    },
                                },
                            )]
                            .into(),
                        },
                    },
                    Foo {
                        scalars: scalars.clone(),
                        obj: Objects {
                            arr: vec![Foo {
                                scalars: scalars.clone(),
                                obj: Objects {
                                    map: default(),
                                    arr: default(),
                                },
                            }],
                            map: [(
                                "nested".into(),
                                Foo {
                                    scalars: scalars.clone(),
                                    obj: Objects {
                                        map: default(),
                                        arr: default(),
                                    },
                                },
                            )]
                            .into(),
                        },
                    },
                ],
            },
        }
    }

    #[test]
    fn smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let mut doc = automerge::AutoCommit::new();

        let val = test_val();

        // NOTE: if this comes third, it removes the other keys
        // since our hydrate_map impl eliminates keys
        autosurgeon::reconcile(&mut doc, ThroughJson(val.clone()))?;
        autosurgeon::reconcile_prop(&mut doc, automerge::ROOT, "serde", ThroughJson(val.clone()))?;
        autosurgeon::reconcile_prop(&mut doc, automerge::ROOT, "normal", val.clone())?;
        doc.commit();

        let _raw: ThroughJson<serde_json::Value> = autosurgeon::hydrate(&doc)?;

        let commited1: ThroughJson<Foo> = autosurgeon::hydrate(&doc)?;
        let commited2: ThroughJson<Foo> =
            autosurgeon::hydrate_prop(&doc, automerge::ROOT, "serde")?;
        let commited3: ThroughJson<Foo> =
            autosurgeon::hydrate_prop(&doc, automerge::ROOT, "normal")?;

        assert_eq!(commited1.0, val);
        assert_eq!(commited2.0, val);
        assert_eq!(commited3.0, val);

        Ok(())
    }

    #[test]
    fn on_a_map() -> Res<()> {
        #[derive(Debug, autosurgeon::Hydrate, autosurgeon::Reconcile)]
        struct OnAMap {
            pub map: HashMap<String, ThroughJson<Arc<Foo>>>,
        }

        utils_rs::testing::setup_tracing_once();
        let mut doc = automerge::AutoCommit::new();

        let mut val = {
            let val = OnAMap { map: default() };
            autosurgeon::reconcile(&mut doc, val)?;
            doc.commit();
            let commited: OnAMap = autosurgeon::hydrate(&doc)?;
            assert_eq!(commited.map.len(), 0);
            commited
        };
        let mut val = {
            val.map.insert("one".into(), ThroughJson(test_val().into()));
            autosurgeon::reconcile(&mut doc, val)?;
            doc.commit();
            let commited: OnAMap = autosurgeon::hydrate(&doc)?;
            assert_eq!(commited.map.len(), 1);
            commited
        };
        let _val = {
            val.map.insert("two".into(), ThroughJson(test_val().into()));
            autosurgeon::reconcile(&mut doc, val)?;
            doc.commit();
            let commited: OnAMap = autosurgeon::hydrate(&doc)?;
            assert_eq!(commited.map.len(), 2);
            commited
        };

        Ok(())
    }

    #[test]
    fn base64_suffix_round_trips_as_automerge_bytes() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let payload = serde_json::json!({
            "vectorBase64": "AQIDBA=="
        });

        autosurgeon::reconcile_prop(
            &mut doc,
            automerge::ROOT,
            "facet",
            ThroughJson(payload.clone()),
        )?;

        let facet_obj = doc
            .get(&automerge::ROOT, "facet")?
            .ok_or_else(|| ferr!("facet not found"))?
            .1;
        let stored = doc
            .get(&facet_obj, "vectorBase64")?
            .ok_or_else(|| ferr!("vectorBase64 not found"))?
            .0;
        match stored {
            automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                automerge::ScalarValue::Bytes(bytes) => assert_eq!(bytes.as_slice(), &[1, 2, 3, 4]),
                other => panic!("expected bytes scalar, found {other:?}"),
            },
            other => panic!("expected scalar value, found {other:?}"),
        }

        let hydrated: ThroughJson<serde_json::Value> =
            autosurgeon::hydrate_prop(&doc, automerge::ROOT, "facet")?;
        assert_eq!(hydrated.0, payload);
        Ok(())
    }

    fn reconcile_json_prop_with_delta(
        doc: &mut automerge::AutoCommit,
        prop: &str,
        value: serde_json::Value,
    ) -> Res<(
        Option<automerge::ChangeHash>,
        Vec<automerge::ChangeHash>,
        Vec<automerge::Patch>,
    )> {
        let old_heads = doc.get_heads();
        autosurgeon::reconcile_prop(doc, automerge::ROOT, prop, ThroughJson(value))?;
        let committed = doc.commit();
        let new_heads = doc.get_heads();
        let patches = doc.diff(&old_heads, &new_heads);
        Ok((committed, new_heads, patches))
    }

    fn object_ids_by_scalar_field_in_array_prop(
        doc: &automerge::AutoCommit,
        parent_prop: &str,
        array_prop: &str,
        item_key_field: &str,
    ) -> Res<HashMap<String, automerge::ObjId>> {
        let parent_obj = doc
            .get(&automerge::ROOT, parent_prop)?
            .ok_or_else(|| ferr!("{parent_prop} not found"))?
            .1;
        let array_obj = doc
            .get(&parent_obj, array_prop)?
            .ok_or_else(|| ferr!("{array_prop} not found"))?
            .1;

        let mut by_id = HashMap::new();
        for ii in 0..doc.length(&array_obj) {
            let (item_value, item_obj) = doc
                .get(&array_obj, ii)?
                .ok_or_else(|| ferr!("array item {ii} missing"))?;
            match item_value {
                automerge::Value::Object(_) => {}
                other => panic!("expected object item, found {other:?}"),
            }
            let id_value = doc
                .get(&item_obj, item_key_field)?
                .ok_or_else(|| ferr!("item {item_key_field} missing"))?
                .0;
            let id = match id_value {
                automerge::Value::Scalar(s) => match s.as_ref() {
                    automerge::ScalarValue::Str(s) => s.to_string(),
                    automerge::ScalarValue::Uint(v) => v.to_string(),
                    automerge::ScalarValue::Int(v) => v.to_string(),
                    other => panic!("expected string id, found {other:?}"),
                },
                other => panic!("expected scalar id, found {other:?}"),
            };
            by_id.insert(id, item_obj);
        }

        Ok(by_id)
    }

    #[test]
    fn reconcile_identical_json_emits_no_change_or_patches() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let payload = serde_json::json!({
            "keep": "same",
            "nested": {
                "count": 2,
                "flag": true,
                "arr": [1, {"x": 2}, 3],
            },
            "bytesBase64": "AQIDBA=="
        });

        let (first_commit, first_heads, first_patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", payload.clone())?;
        assert!(first_commit.is_some(), "initial reconcile should commit");
        assert!(
            !first_patches.is_empty(),
            "initial reconcile should emit patches for inserted state"
        );

        let (second_commit, second_heads, second_patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", payload)?;
        assert!(
            second_commit.is_none(),
            "identical reconcile should not create a new automerge change"
        );
        assert_eq!(
            second_heads, first_heads,
            "heads should be unchanged after idempotent reconcile"
        );
        assert!(
            second_patches.is_empty(),
            "identical reconcile should produce no diff patches"
        );

        Ok(())
    }

    #[test]
    fn reconcile_json_modifications_emit_targeted_map_and_list_patches() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "stable": "same",
            "obj": {
                "same": 1,
                "change": 1,
                "drop": true
            },
            "arr": [10, 20, 30]
        });
        let modified = serde_json::json!({
            "stable": "same",
            "obj": {
                "same": 1,
                "change": 2,
                "add": "new"
            },
            "arr": [10, 99]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let (commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;

        assert!(commit.is_some(), "modified reconcile should commit");
        assert!(
            !patches.is_empty(),
            "modified reconcile should emit patches"
        );

        let put_map_keys = patches
            .iter()
            .filter_map(|patch| match &patch.action {
                automerge::PatchAction::PutMap { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let delete_map_keys = patches
            .iter()
            .filter_map(|patch| match &patch.action {
                automerge::PatchAction::DeleteMap { key } => Some(key.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let put_seq_indices = patches
            .iter()
            .filter_map(|patch| match patch.action {
                automerge::PatchAction::PutSeq { index, .. } => Some(index),
                _ => None,
            })
            .collect::<Vec<_>>();

        let delete_seq_spans = patches
            .iter()
            .filter_map(|patch| match patch.action {
                automerge::PatchAction::DeleteSeq { index, length } => Some((index, length)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(
            put_map_keys.contains(&"change"),
            "expected nested map value update patch for obj.change, saw: {put_map_keys:?}"
        );
        assert!(
            put_map_keys.contains(&"add"),
            "expected nested map insert patch for obj.add, saw: {put_map_keys:?}"
        );
        assert!(
            delete_map_keys.contains(&"drop"),
            "expected nested map delete patch for obj.drop, saw: {delete_map_keys:?}"
        );
        assert!(
            put_seq_indices.contains(&1),
            "expected list index update patch for arr[1], saw: {put_seq_indices:?}"
        );
        assert!(
            delete_seq_spans.contains(&(2, 1)),
            "expected list tail deletion patch for arr[2], saw: {delete_seq_spans:?}"
        );

        assert!(
            !put_map_keys.contains(&"stable"),
            "unchanged key should not emit a put patch"
        );
        assert!(
            !put_map_keys.contains(&"same"),
            "unchanged nested key should not emit a put patch"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_preserves_object_identity_on_front_insert() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "items": [
                {"id": "a", "name": "A"},
                {"id": "b", "name": "B"}
            ]
        });
        let modified = serde_json::json!({
            "items": [
                {"id": "x", "name": "X"},
                {"id": "a", "name": "A"},
                {"id": "b", "name": "B"}
            ]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let ids_before = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;
        let ids_after = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        assert_eq!(ids_before.get("a"), ids_after.get("a"));
        assert_eq!(ids_before.get("b"), ids_after.get("b"));
        assert_ne!(ids_after.get("x"), ids_after.get("a"));
        assert_ne!(ids_after.get("x"), ids_after.get("b"));

        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "expected front insert patch, saw: {patches:?}"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_supports_key_field_identity_preservation() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "items": [
                {"key": "a", "name": "A"},
                {"key": "b", "name": "B"}
            ]
        });
        let modified = serde_json::json!({
            "items": [
                {"key": "x", "name": "X"},
                {"key": "a", "name": "A"},
                {"key": "b", "name": "B"}
            ]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let ids_before = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "key")?;

        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;
        let ids_after = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "key")?;

        assert_eq!(ids_before.get("a"), ids_after.get("a"));
        assert_eq!(ids_before.get("b"), ids_after.get("b"));
        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "expected front insert patch, saw: {patches:?}"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_supports_integer_id_identity_preservation() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "items": [
                {"id": 1, "name": "A"},
                {"id": 2, "name": "B"}
            ]
        });
        let modified = serde_json::json!({
            "items": [
                {"id": 9, "name": "X"},
                {"id": 1, "name": "A"},
                {"id": 2, "name": "B"}
            ]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let ids_before = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;
        let ids_after = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        assert_eq!(ids_before.get("1"), ids_after.get("1"));
        assert_eq!(ids_before.get("2"), ids_after.get("2"));
        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "expected front insert patch, saw: {patches:?}"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_falls_back_when_duplicate_keys_present() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "items": [
                {"id": "a", "name": "A1"},
                {"id": "a", "name": "A2"}
            ]
        });
        let modified = serde_json::json!({
            "items": [
                {"id": "x", "name": "X"},
                {"id": "a", "name": "A1"},
                {"id": "a", "name": "A2"}
            ]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;

        assert!(
            !patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "duplicate keys should disable keyed front insert behavior, saw: {patches:?}"
        );
        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 2, .. })),
            "expected positional fallback to append the shifted tail item at index 2, saw: {patches:?}"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_falls_back_when_key_type_is_bool() -> Res<()> {
        let mut doc = automerge::AutoCommit::new();
        let initial = serde_json::json!({
            "items": [
                {"id": true, "name": "A"},
                {"id": "b", "name": "B"}
            ]
        });
        let modified = serde_json::json!({
            "items": [
                {"id": "x", "name": "X"},
                {"id": true, "name": "A"},
                {"id": "b", "name": "B"}
            ]
        });

        let _ = reconcile_json_prop_with_delta(&mut doc, "facet", initial)?;
        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;

        assert!(
            !patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "bool ids should disable keyed front insert behavior, saw: {patches:?}"
        );
        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 2, .. })),
            "expected positional fallback to append the shifted tail item at index 2, saw: {patches:?}"
        );

        Ok(())
    }

    #[test]
    fn heuristic_keyed_array_matches_existing_positive_int_id_with_json_uint_id() -> Res<()> {
        use automerge::transaction::Transactable;

        let mut doc = automerge::AutoCommit::new();
        let facet_obj = doc.put_object(automerge::ROOT, "facet", automerge::ObjType::Map)?;
        let items_obj = doc.put_object(&facet_obj, "items", automerge::ObjType::List)?;
        let item_obj = doc.insert_object(&items_obj, 0, automerge::ObjType::Map)?;
        doc.put(&item_obj, "id", 1_i64)?;
        doc.put(&item_obj, "name", "A")?;
        doc.commit();

        let ids_before = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        let modified = serde_json::json!({
            "items": [
                {"id": "x", "name": "X"},
                {"id": 1, "name": "A"}
            ]
        });
        let (_commit, _heads, patches) =
            reconcile_json_prop_with_delta(&mut doc, "facet", modified)?;
        let ids_after = object_ids_by_scalar_field_in_array_prop(&doc, "facet", "items", "id")?;

        assert_eq!(
            ids_before.get("1"),
            ids_after.get("1"),
            "positive automerge Int(1) id should match JSON number 1 and preserve object identity"
        );
        assert!(
            patches
                .iter()
                .any(|p| matches!(p.action, automerge::PatchAction::Insert { index: 0, .. })),
            "expected keyed front insert when matching existing int id=1, saw: {patches:?}"
        );

        Ok(())
    }
}
