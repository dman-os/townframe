use crate::interlude::*;

use automerge::ObjId;

use automerge::*;
use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

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
                let value = hydrate_value(doc, obj, autosurgeon::Prop::Key(item.key.clone()))?;
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
        crate::testing::setup_tracing_once();
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

        crate::testing::setup_tracing_once();
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
}
