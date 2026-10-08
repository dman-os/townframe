//! The wire form of the declared shapes: the JSON and the schema a peer reads.
//!
//! Open unions, members this build has never heard of, and vocabularies of strings are the three
//! shapes `serde` cannot express, so this is what the declaration macro's impls call. The storage
//! codec — how a value sits in a document — is [`super::codec`].
//!
//! A closed union needs none of this — `#[serde(tag = …, content = …)]` derives the wire form and
//! `schemars` derives the schema. An open union has a fallback that keeps a member this build does
//! not know about, and no serde attribute expresses that: `#[serde(other)]` has to be a unit
//! variant, so it catches the discriminant and then cannot read the payload. The declaration macro
//! writes the three impls, and everything below is what they call.

use crate::facet::{Extra, FacetMember};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::borrow::Cow;

/// Writes one declared member: `{"<tag>": <P as FacetMember>::ID, "<content>": <payload>}`.
pub fn serialize_member<S, P>(
    serializer: S,
    tag: &'static str,
    content: &'static str,
    payload: &P,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    P: Serialize + FacetMember,
{
    let mut member = serializer.serialize_map(Some(2))?;
    member.serialize_entry(tag, P::ID)?;
    member.serialize_entry(content, payload)?;
    member.end()
}

/// One declared member: the id it is tagged with, and how to read its payload.
pub struct Member<T> {
    pub id: &'static str,
    pub read: fn(Value) -> Result<T, serde_json::Error>,
}

/// Reads a member object, dispatching on its discriminant.
///
/// An unrecognized discriminant goes to `fallback`, which keeps the member whole. A recognized
/// discriminant whose payload is malformed is an error and never falls back, so a member cannot
/// silently become an unknown one.
pub fn deserialize_union<'de, D, T>(
    deserializer: D,
    tag: &'static str,
    content: &'static str,
    members: &[Member<T>],
    fallback: fn(Value) -> Result<T, serde_json::Error>,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let member = Value::deserialize(deserializer)?;
    let object = member.as_object().ok_or_else(|| {
        D::Error::custom(format!(
            "a union member is an object with a {tag} discriminant"
        ))
    })?;
    let id = object.get(tag).and_then(Value::as_str).ok_or_else(|| {
        D::Error::custom(format!("a union member needs a string {tag} discriminant"))
    })?;
    let Some(declared) = members.iter().find(|declared| declared.id == id) else {
        return fallback(member).map_err(D::Error::custom);
    };
    let payload = object
        .get(content)
        .cloned()
        .ok_or_else(|| D::Error::custom(format!("union member {id} needs a {content} payload")))?;
    (declared.read)(payload).map_err(D::Error::custom)
}

/// A union member this build has no declaration for, kept whole so a round trip is lossless.
///
/// Holding the member object rather than its payload is what lets a reader that does not know the
/// type write back exactly what it received, discriminant included.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct UnknownMember(Extra);

impl UnknownMember {
    /// The discriminant the member arrived with.
    pub fn id(&self, tag: &str) -> Option<&str> {
        self.0.get(tag).and_then(Value::as_str)
    }

    /// The member object, exactly as it arrived.
    pub fn as_map(&self) -> &Extra {
        &self.0
    }

    /// The member object, exactly as it arrived.
    pub fn into_map(self) -> Extra {
        self.0
    }
}

impl Serialize for UnknownMember {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UnknownMember {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self(Extra::deserialize(deserializer)?))
    }
}

impl JsonSchema for UnknownMember {
    fn schema_name() -> Cow<'static, str> {
        "UnknownMember".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({ "type": "object", "additionalProperties": true })
    }
}

/// One declared member: the id its branch is tagged with, and the schema of its payload.
pub struct MemberSchema {
    pub id: &'static str,
    pub schema: Schema,
}

/// Builds the `oneOf` a union emits: one branch per declared member, plus, for an open union, the
/// branch whose discriminant is unconstrained — which is what `check_shape` reads as the fallback.
pub fn union_schema(
    tag: &'static str,
    content: &'static str,
    open: bool,
    members: Vec<MemberSchema>,
) -> Schema {
    let mut branches: Vec<Value> = Vec::with_capacity(members.len() + 1);
    for member in members {
        branches.push(adjacent_branch(
            tag,
            content,
            serde_json::json!({ "const": member.id }),
            Value::from(member.schema),
        ));
    }
    if open {
        // Every member carries a payload — a marker member carries an empty object — so the branch
        // requires one and leaves it unconstrained.
        branches.push(adjacent_branch(
            tag,
            content,
            serde_json::json!({ "type": "string" }),
            Value::Bool(true),
        ));
    }
    let mut root = serde_json::Map::new();
    root.insert("oneOf".to_owned(), Value::Array(branches));
    Schema::try_from(Value::Object(root)).expect("a union schema is a valid JSON Schema")
}

fn adjacent_branch(tag: &str, content: &str, discriminant: Value, payload: Value) -> Value {
    let mut properties = serde_json::Map::new();
    properties.insert(tag.to_owned(), discriminant);
    properties.insert(content.to_owned(), payload);
    let mut branch = serde_json::Map::new();
    branch.insert("properties".to_owned(), Value::Object(properties));
    branch.insert("required".to_owned(), serde_json::json!([tag, content]));
    Value::Object(branch)
}

/// A known spelling of a string vocabulary, and how to build the variant it selects.
pub type KnownString<T> = (&'static str, fn() -> T);

/// Reads a string vocabulary: a known spelling picks its variant, and any other string is handed to
/// `unknown` as it was written, so nothing a peer invents is lost or an error.
pub fn deserialize_known_str<'de, D, T>(
    deserializer: D,
    known: &[KnownString<T>],
    unknown: fn(String) -> T,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Ok(
        match known.iter().find(|(spelling, _)| *spelling == value) {
            Some((_, make)) => make(),
            None => unknown(value),
        },
    )
}

/// The schema of an open string vocabulary: the known spellings are listed, and no other string is
/// constrained.
pub fn open_string_schema(known: &'static [&'static str]) -> Schema {
    let mut root = serde_json::Map::new();
    root.insert("type".to_owned(), Value::from("string"));
    root.insert("knownValues".to_owned(), serde_json::json!(known));
    Schema::try_from(Value::Object(root)).expect("a string vocabulary is a valid JSON Schema")
}
