//! The wire form of the declared shapes: the JSON and the schema a peer reads.
//!
//! Open unions, members this build has never heard of, and vocabularies of strings are the three
//! shapes `serde` cannot express, so this is what the declaration macro's impls call. The storage
//! codec — how a value sits in a document — is [`super::codec`].
//!
//! Internal tagging is the authored default: the member object carries `$type`, and the payload
//! fields sit beside it. Adjacent tagging is the explicit wrapper `{"$type", "value"}`. A closed
//! union's serde derive writes the wire form; the macro still emits the schema, because `schemars`
//! enum encoding is not the contract. An open union has a fallback that keeps a member this build
//! does not know about, and no serde attribute expresses that.

use crate::datum::DatumMember;
use crate::datum::Extra;
#[cfg(feature = "schemars")]
use crate::datum::Openness;
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
#[cfg(feature = "schemars")]
use schemars::Schema;
#[cfg(feature = "schemars")]
use schemars::SchemaGenerator;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::Error as _;
use serde::ser::Error as _;
use serde::ser::SerializeMap;
#[cfg(feature = "schemars")]
use serde_json::Map;
use serde_json::Value;
#[cfg(feature = "schemars")]
use std::borrow::Cow;

/// Where a union member's discriminant sits, as the macro writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnionLayout {
    /// `{"$type": "<member id>", ...payload fields}`
    Internal { tag: &'static str },
    /// `{"$type": "<member id>", "value": <payload>}`
    Adjacent {
        tag: &'static str,
        content: &'static str,
    },
    /// `{"<member id>": <payload>}`
    External,
}

/// Writes one declared member in the layout the union stated.
pub fn serialize_member<S, P>(
    serializer: S,
    layout: &UnionLayout,
    payload: &P,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    P: Serialize + DatumMember,
{
    match *layout {
        UnionLayout::Adjacent { tag, content } => {
            let mut member = serializer.serialize_map(Some(2))?;
            member.serialize_entry(tag, P::ID)?;
            member.serialize_entry(content, payload)?;
            member.end()
        }
        UnionLayout::Internal { tag } => {
            let mut value = serde_json::to_value(payload).map_err(S::Error::custom)?;
            let object = value.as_object_mut().ok_or_else(|| {
                S::Error::custom("an internally tagged member serializes as an object")
            })?;
            object.insert(tag.to_owned(), Value::from(P::ID));
            value.serialize(serializer)
        }
        UnionLayout::External => {
            let mut member = serializer.serialize_map(Some(1))?;
            member.serialize_entry(P::ID, payload)?;
            member.end()
        }
    }
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
/// silently become an unknown one. Internal tagging strips the discriminant before the payload is
/// read, so an overflow field does not capture `$type` and `deny_unknown_fields` still holds.
pub fn deserialize_union<'de, D, T>(
    deserializer: D,
    layout: &UnionLayout,
    members: &[Member<T>],
    fallback: fn(Value) -> Result<T, serde_json::Error>,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let member = Value::deserialize(deserializer)?;
    match *layout {
        UnionLayout::External => {
            let object = member.as_object().ok_or_else(|| {
                D::Error::custom("an externally tagged member is an object of one key")
            })?;
            let Some((id, payload)) = object.iter().next() else {
                return Err(D::Error::custom(
                    "an externally tagged member names its payload",
                ));
            };
            if object.len() != 1 {
                return Err(D::Error::custom(
                    "an externally tagged member is an object of one key",
                ));
            }
            let Some(declared) = members.iter().find(|declared| declared.id == id) else {
                return fallback(member).map_err(D::Error::custom);
            };
            (declared.read)(payload.clone()).map_err(D::Error::custom)
        }
        UnionLayout::Internal { tag } | UnionLayout::Adjacent { tag, .. } => {
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
            let payload = match *layout {
                UnionLayout::Adjacent { content, .. } => {
                    object.get(content).cloned().ok_or_else(|| {
                        D::Error::custom(format!("union member {id} needs a {content} payload"))
                    })?
                }
                UnionLayout::Internal { tag } => {
                    let mut payload = object.clone();
                    payload.remove(tag);
                    Value::Object(payload)
                }
                UnionLayout::External => unreachable!("matched above"),
            };
            (declared.read)(payload).map_err(D::Error::custom)
        }
    }
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

#[cfg(feature = "schemars")]
impl JsonSchema for UnknownMember {
    fn schema_name() -> Cow<'static, str> {
        "UnknownMember".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({ "type": "object", "additionalProperties": true })
    }
}

/// One declared member: the id its branch is tagged with, and the schema of its payload.
#[cfg(feature = "schemars")]
pub struct MemberSchema {
    pub id: &'static str,
    pub schema: Schema,
}

/// Builds the `oneOf` a union emits: one branch per declared member, plus, for an open union, a
/// fallback that excludes known discriminants so each member matches exactly one branch.
///
/// An internal branch is the payload's own schema with the discriminant added — its properties
/// inlined beside `$type`, and a member `$ref` resolved first so that inlining does not hide the
/// payload behind an `allOf` the codec refuses. Adjacent tagging keeps the payload under the pinned
/// content key instead, which is the wrapper a payload that is not an object needs.
#[cfg(feature = "schemars")]
pub fn union_schema(
    layout: &UnionLayout,
    open: bool,
    members: Vec<MemberSchema>,
    definitions: &Map<String, Value>,
) -> Schema {
    let mut branches: Vec<Value> = Vec::with_capacity(members.len() + 1);
    let known: Vec<_> = members.iter().map(|member| member.id).collect();
    for member in members {
        let payload = Value::from(member.schema);
        branches.push(match *layout {
            UnionLayout::Internal { tag } => internal_branch(tag, member.id, &payload, definitions),
            UnionLayout::Adjacent { tag, content } => adjacent_branch(
                tag,
                content,
                serde_json::json!({ "const": member.id }),
                payload,
            ),
            UnionLayout::External => external_branch(member.id, payload),
        });
    }
    if open {
        branches.push(match *layout {
            UnionLayout::Internal { tag } => internal_fallback(tag, &known),
            UnionLayout::Adjacent { tag, content } => adjacent_branch(
                tag,
                content,
                serde_json::json!({ "type": "string", "not": { "enum": known } }),
                Value::Bool(true),
            ),
            UnionLayout::External => {
                let mut branch = Map::new();
                branch.insert("type".to_owned(), Value::from("object"));
                branch.insert("additionalProperties".to_owned(), Value::Bool(true));
                Value::Object(branch)
            }
        });
    }
    let mut root = Map::new();
    root.insert("oneOf".to_owned(), Value::Array(branches));
    Schema::try_from(Value::Object(root)).expect("a union schema is a valid JSON Schema")
}

#[cfg(feature = "schemars")]
fn internal_branch(
    tag: &str,
    id: &str,
    payload: &Value,
    definitions: &Map<String, Value>,
) -> Value {
    let payload = resolve_def(payload, definitions);
    // The payload's own schema is the branch, with the discriminant added: copying only the keys we
    // thought of would silently drop every other constraint the member states — its property count,
    // its description — and the branch would then admit a value the member never did.
    let mut branch = payload
        .as_object()
        .unwrap_or_else(|| {
            panic!(
                "an internally tagged member's payload is an object, and {id} projects {payload}"
            )
        })
        .clone();
    // The definitions belong to the document this union is inlined into, not to one branch; a
    // nested reference inside the payload resolves against the document's own.
    for keyword in ["$defs", "$schema", "$id"] {
        branch.remove(keyword);
    }
    branch.insert("type".to_owned(), Value::from("object"));
    let mut properties = branch
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if properties
        .insert(tag.to_owned(), serde_json::json!({ "const": id }))
        .is_some()
    {
        panic!(
            "the member {id} declares a `{tag}` field of its own, which is the discriminant the \
             union writes: rename it, or the two would be one key with two meanings"
        );
    }
    branch.insert("properties".to_owned(), Value::Object(properties));
    let mut required = branch
        .get("required")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !required.iter().any(|name| name == tag) {
        required.insert(0, Value::from(tag));
    }
    branch.insert("required".to_owned(), Value::Array(required));
    Value::Object(branch)
}

#[cfg(feature = "schemars")]
fn internal_fallback(tag: &str, known: &[&str]) -> Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            tag: {"type": "string", "not": {"enum": known}}
        },
        "required": [tag],
        "additionalProperties": true
    })
}

#[cfg(feature = "schemars")]
fn resolve_def(schema: &Value, definitions: &Map<String, Value>) -> Value {
    let mut current = schema.clone();
    for _ in 0..8 {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return current;
        };
        let Some(name) = reference.strip_prefix("#/$defs/") else {
            return current;
        };
        let Some(resolved) = definitions.get(name) else {
            return current;
        };
        current = resolved.clone();
    }
    current
}

#[cfg(feature = "schemars")]
fn adjacent_branch(tag: &str, content: &str, discriminant: Value, payload: Value) -> Value {
    let mut properties = serde_json::Map::new();
    properties.insert(tag.to_owned(), discriminant);
    properties.insert(content.to_owned(), payload);
    let mut branch = serde_json::Map::new();
    branch.insert("type".to_owned(), Value::from("object"));
    branch.insert("properties".to_owned(), Value::Object(properties));
    branch.insert("required".to_owned(), serde_json::json!([tag, content]));
    Value::Object(branch)
}

#[cfg(feature = "schemars")]
fn external_branch(id: &str, payload: Value) -> Value {
    let mut properties = Map::new();
    properties.insert(id.to_owned(), payload);
    let mut branch = Map::new();
    branch.insert("type".to_owned(), Value::from("object"));
    branch.insert("properties".to_owned(), Value::Object(properties));
    branch.insert("required".to_owned(), serde_json::json!([id]));
    branch.insert("additionalProperties".to_owned(), Value::Bool(false));
    Value::Object(branch)
}

/// A known spelling of a string vocabulary, and how to build the variant it selects.
pub type KnownString<T> = (&'static str, fn() -> T);

/// Reads an open string vocabulary: a known spelling picks its variant, and any other string is
/// handed to `unknown` as it was written, so nothing a peer invents is lost or an error.
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

/// Reads a closed string vocabulary: a known spelling picks its variant, and any other string is an
/// error, because the declaration states that the list is the whole vocabulary.
pub fn deserialize_closed_str<'de, D, T>(
    deserializer: D,
    known: &[KnownString<T>],
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match known.iter().find(|(spelling, _)| *spelling == value) {
        Some((_, make)) => Ok(make()),
        None => {
            let expected = known
                .iter()
                .map(|(spelling, _)| format!("`{spelling}`"))
                .collect::<Vec<_>>()
                .join(", ");
            Err(serde::de::Error::custom(format!(
                "unknown variant `{value}`, expected one of {expected}"
            )))
        }
    }
}

/// The schema of a string vocabulary: an open one lists the known spellings in `knownValues` and
/// constrains nothing else; a closed one spells the same list as `enum`.
#[cfg(feature = "schemars")]
pub fn string_schema(known: &'static [&'static str], openness: Openness) -> Schema {
    let mut root = serde_json::Map::new();
    root.insert("type".to_owned(), Value::from("string"));
    let key = match openness {
        Openness::Open => "knownValues",
        Openness::Closed => "enum",
    };
    root.insert(key.to_owned(), serde_json::json!(known));
    Schema::try_from(Value::Object(root)).expect("a string vocabulary is a valid JSON Schema")
}

#[cfg(all(test, feature = "schemars"))]
mod tests {
    use super::*;
    use serde_json::json;

    /// The branch is the payload's own schema with the discriminant added: copying only the keys
    /// the builder knows would drop every other constraint the member states, and the union would
    /// then admit a value the member never did.
    #[test]
    fn an_internal_branch_is_the_payload_schema_with_the_discriminant_added() {
        let payload = json!({
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "minProperties": 2,
            "description": "a member",
            "$defs": {"Unused": {"type": "null"}},
            "$schema": "https://json-schema.org/draft/2020-12/schema"
        });
        let branch = internal_branch("$type", "org.example.member", &payload, &Map::new());
        assert_eq!(
            branch,
            json!({
                "type": "object",
                "properties": {
                    "$type": {"const": "org.example.member"},
                    "text": {"type": "string"}
                },
                "required": ["$type", "text"],
                "minProperties": 2,
                "description": "a member"
            }),
            "the payload's own constraints travel and its document keys do not"
        );
    }

    /// A member that resolves through a `$ref` is inlined before the discriminant is added, so the
    /// payload never hides behind an `allOf` the codec refuses.
    #[test]
    fn an_internal_branch_resolves_a_referenced_payload_first() {
        let mut definitions = Map::new();
        definitions.insert(
            "Member".to_owned(),
            json!({"type": "object", "properties": {"text": {"type": "string"}}}),
        );
        let branch = internal_branch(
            "$type",
            "org.example.member",
            &json!({"$ref": "#/$defs/Member"}),
            &definitions,
        );
        assert_eq!(branch["properties"]["text"], json!({"type": "string"}));
        assert_eq!(
            branch["properties"]["$type"],
            json!({"const": "org.example.member"})
        );
    }
}
