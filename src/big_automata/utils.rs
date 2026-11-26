use crate::interlude::*;

use serde::{Deserializer, Serializer};

pub fn value_as_cbor<S>(val: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(val, &mut bytes).map_err(serde::ser::Error::custom)?;
    serializer.serialize_bytes(&bytes)
}

pub fn value_from_cbor<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: Deserializer<'de>,
{
    struct MyVisitor;
    impl<'de> serde::de::Visitor<'de> for MyVisitor {
        type Value = serde_json::Value;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            fmt.write_str("a cbor byte string")
        }

        fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let val = ciborium::from_reader(val).map_err(serde::de::Error::custom)?;
            Ok(val)
        }
    }
    deserializer.deserialize_bytes(MyVisitor)
}
