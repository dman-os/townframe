//! How a value is stored in a document, declared by its schema rather than guessed from the name of
//! the field that holds it.
//!
//! serde says what a value looks like as JSON. It cannot say what that value has to be *in a
//! document*, where the storage has shapes JSON does not: bytes rather than an array of numbers, an
//! instant rather than a string, and a counter or a text, which are not values but objects that
//! merge. So the storage form is declared beside the JSON form, and reconciliation reads the
//! declaration.
//!
//! One of the five storage forms is a standard JSON Schema keyword: `jiff::Timestamp` emits
//! `format: "date-time"`. Bytes are not that. `contentEncoding` annotates a string; it does not
//! select storage. Canonical bytes are the `{ "$bytes": "…" }` object, padded or not, and the
//! sidecar marks that object with `x-automerge`. The other three forms are `x-automerge` as well,
//! because JSON Schema cannot describe a CRDT object, cannot say that a number is a *float*, and
//! cannot state the non-finite values at all — so a float's declaration is its value's own type
//! (`facet::Float64`/`Float32`, spelled as the decimal string it is), and a bare `f64` derives a
//! plain number, which is the opt-out the Rust type itself states. Nothing opts in: a type declares
//! its storage by what its `JsonSchema` says, so a foreign type whose schema states a storage form
//! — `jiff::Timestamp` today, `chrono`'s tomorrow — is stored correctly as soon as it does.
//!
//! A declaration is asserted rather than sniffed, so a value is checked against the declarations it
//! will be stored under before anything is written ([`Stored::check`]); from then on it cannot fail
//! on its own account, and a field named `createdAt` is an instant because its schema says so, not
//! because of the way it is spelled.

use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use autosurgeon::reconcile::{CounterReconciler, TextReconciler};
use autosurgeon::{HydrateError, Reconciler};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::ser::{Error as SerError, SerializeMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};
use std::borrow::Cow;
use std::num::FpCategory;

mod references;
mod target;
mod validation;
mod walk;

#[cfg(test)]
mod e2e;

pub use references::{DeclaredReference, declared_references};
#[cfg(test)]
pub(crate) use target::reconcile_json;
pub use target::reconcile_json_atomic;
pub use validation::CompiledSchema;
pub use walk::{Stored, hydrate_value};

/// The keyword for the storage forms JSON Schema cannot describe.
pub const KEYWORD: &str = "x-automerge";

/// Where JSON Schema does state a storage form, and the value we read there.
const FORMAT: &str = "format";
const DATE_TIME: &str = "date-time";
const JS_SAFE_INTEGER_MAX: i64 = 9_007_199_254_740_991;

/// The schema an object member is declared by: the property it is named as, the typed catch-all
/// `additionalProperties` states for the members a record does not name, or the value schema of a
/// [`keyed_map`].
///
/// A position whose `patternProperties` is not a keyed map has no declaration for its pattern
/// members, which is why the codec refuses to compile one rather than storing them as plain JSON.
pub(super) fn declared_member<'a>(schema: &'a Value, key: &str) -> Option<&'a Value> {
    schema
        .get("properties")
        .and_then(|properties| properties.get(key))
        .or_else(|| {
            schema
                .get("additionalProperties")
                .filter(|stated| stated.is_object())
        })
        .or_else(|| {
            schema.as_object().and_then(|object| {
                keyed_map(object).expect("a compiled schema's `patternProperties` is a keyed map")
            })
        })
}

/// A dynamic map whose keys one pattern states, and the schema every one of its values is declared
/// by. `BTreeMap<Uuid7, V>` derives one: the key type's `pattern` becomes the map's only
/// `patternProperties` entry, closed by `additionalProperties: false`.
///
/// JSON Schema dispatches a `patternProperties` member by matching its key against each pattern.
/// The storage walk and the Lexicon projection read a member's declaration without running a
/// pattern, and they can only do that where matching decides nothing: exactly one pattern, anchored
/// at both ends so it states the whole key rather than a part of it, `additionalProperties: false`
/// so that no key falls outside it, and no named properties beside it so that a member is a map
/// entry and never also a record field. Every member then has the one value schema, and the
/// validator — which does run the pattern — refuses a key outside it before the walk reads a value.
///
/// `Ok(None)` is a position without `patternProperties`. Any other use of it is an error, because
/// reading past it would store or publish its members under a declaration they do not have.
pub(super) fn keyed_map(object: &Map<String, Value>) -> Result<Option<&Value>, &'static str> {
    let Some(patterns) = object.get("patternProperties") else {
        return Ok(None);
    };
    let Some((pattern, values)) = patterns
        .as_object()
        .filter(|patterns| patterns.len() == 1)
        .and_then(|patterns| patterns.iter().next())
    else {
        return Err(
            "`patternProperties` states other than exactly one key pattern, so a member's \
             declaration would depend on which pattern its key matches",
        );
    };
    if !pattern.starts_with('^') || !pattern.ends_with('$') || pattern.ends_with("\\$") {
        return Err(
            "the `patternProperties` key pattern is not anchored at both ends, so it constrains \
             a part of each key rather than the key",
        );
    }
    if object.get("additionalProperties") != Some(&Value::Bool(false))
        || object.contains_key("unevaluatedProperties")
    {
        return Err(
            "`patternProperties` is not closed by `additionalProperties: false` alone, so a key \
             outside its pattern would be governed by another declaration",
        );
    }
    if object
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| !properties.is_empty())
    {
        return Err(
            "`patternProperties` sits beside named properties, so one member could be both a \
             record field and a map entry",
        );
    }
    if !values.is_object() {
        return Err("the `patternProperties` value schema is not a schema object");
    }
    Ok(Some(values))
}

/// A shape whose storage in a document is not its JSON form — with one exception: a reference is
/// a string on both sides, and its marker declares what the string *is* (a `db+facet:` address)
/// so that a malformed one is refused on write and the reference index can find every declared
/// position from the schema alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// Bytes in the document, the protocol `{ "$bytes": "…" }` object in JSON.
    Bytes,
    /// A timestamp in milliseconds in the document, an RFC 3339 string in JSON.
    Timestamp,
    /// A counter — a number that merges by addition — in the document, a number in JSON.
    Counter,
    /// A float in the document, a decimal string in JSON: JSON's number type cannot say that a
    /// value is a float, and `1` is not the `1.0` it came from.
    Float,
    /// A collaborative text in the document, a string in JSON.
    Text,
    Uint,
    Datetime,
    /// A facet reference: a string in the document and in JSON, which must parse as a
    /// [`crate::FacetReference`]. An optional `refHeads` sibling of the marked member pins the
    /// referenced facet's frontier (ADR 016).
    Reference,
}

impl Codec {
    /// Every shape a declaration can state.
    pub const ALL: [Codec; 8] = [
        Codec::Bytes,
        Codec::Timestamp,
        Codec::Counter,
        Codec::Text,
        Codec::Float,
        Codec::Uint,
        Codec::Datetime,
        Codec::Reference,
    ];

    /// How this shape is spelled in our keyword, and how it is named in a message.
    pub const fn name(self) -> &'static str {
        match self {
            Codec::Bytes => "bytes",
            Codec::Timestamp => "timestamp",
            Codec::Counter => "counter",
            Codec::Float => "float",
            Codec::Text => "text",
            Codec::Uint => "uint",
            Codec::Datetime => "datetime",
            Codec::Reference => "reference",
        }
    }

    /// The shape a schema states at this position, if any.
    ///
    /// Our keyword is read first and is closed: a spelling this build does not implement is refused,
    /// because storing the value as anything else gives a reader that trusts the keyword a different
    /// type than the writer wrote. The standard keywords are open vocabularies, so a `format` we do
    /// not know is simply not a storage form. `"number"` and `"list"` under it are not storage
    /// forms either: they name a plain position a field's own marker states on purpose, and the
    /// codec has nothing to do at those — the walk already holds what they say.
    pub fn of(schema: &Value) -> Result<Option<Self>, CodecError> {
        if let Some(stated) = schema.get(KEYWORD) {
            let Some(stated) = stated.as_str() else {
                return Err(CodecError::NotASpelling {
                    stated: stated.clone(),
                });
            };
            if stated == "number" || stated == "list" {
                // A plain position's marker, stated by its field on purpose and not a storage form;
                // the walk already holds exactly what these say.
                return Ok(None);
            }
            return Self::ALL
                .iter()
                .find(|codec| codec.name() == stated)
                .copied()
                .map(Some)
                .ok_or_else(|| CodecError::Unknown {
                    stated: stated.to_owned(),
                });
        }
        if schema.get(FORMAT).and_then(Value::as_str) == Some(DATE_TIME) {
            return Ok(Some(Codec::Timestamp));
        }
        Ok(None)
    }

    /// Write the value this shape's JSON form states.
    ///
    /// The value was checked, so every conversion here is total: nothing that reaches this point can
    /// fail to be what the declaration says.
    fn write<R: Reconciler>(self, value: &Value, reconciler: &mut R) -> Result<(), R::Error> {
        match self {
            Codec::Bytes => {
                let stated = bytes_value(value).expect("checked");
                reconciler.bytes(stated)
            }
            Codec::Timestamp => {
                let stated = value.as_str().expect("checked");
                reconciler.timestamp(instant_millis(stated).expect("checked"))
            }
            Codec::Datetime => {
                let stated = value.as_str().expect("checked");
                match instant_millis(stated).filter(|millis| instant_string(*millis) == stated) {
                    Some(millis) => reconciler.timestamp(millis),
                    None => reconciler.str(stated),
                }
            }
            Codec::Uint => {
                reconciler.u64(value.as_str().expect("checked").parse().expect("checked"))
            }
            Codec::Reference => reconciler.str(value.as_str().expect("checked")),
            Codec::Counter => reconciler.counter()?.set(value.as_i64().expect("checked")),
            Codec::Text => reconciler.text()?.update(value.as_str().expect("checked")),
            Codec::Float => {
                // A number is what a typed writer hands over and a decimal string is what the
                // convention carries: both are the value the document holds.
                let stated = match value {
                    Value::String(stated) => stated.parse::<f64>().expect("checked"),
                    _ => value.as_f64().expect("checked"),
                };
                reconciler.f64(stated)
            }
        }
    }

    /// What this shape reads back as. The document holds the storage form, and the reader gets the
    /// JSON form the declaration spells: for a float that is the convention's decimal string, and
    /// for the rest it is the form serde wrote.
    fn read<D: ReadDoc>(
        self,
        doc: &D,
        stored: &automerge::Value,
        id: &ObjId,
        schema: &Value,
    ) -> Result<Value, HydrateError> {
        let unexpected = |found: &str| HydrateError::unexpected(self.name(), found.to_owned());
        let at_f32 = schema.get(FORMAT).and_then(Value::as_str) == Some("float");
        match (self, stored) {
            (Codec::Bytes, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Bytes(bytes) => {
                    Ok(json!({"$bytes": data_encoding::BASE64.encode(bytes)}))
                }
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Uint, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Uint(value) => Ok(Value::String(value.to_string())),
                ScalarValue::Int(value) if *value >= 0 => Ok(Value::String(value.to_string())),
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Datetime, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Timestamp(millis) => Ok(Value::String(instant_string(*millis))),
                ScalarValue::Str(value) => Ok(Value::String(value.to_string())),
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Reference, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Str(value) => Ok(Value::String(value.to_string())),
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Timestamp, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Timestamp(millis) => Ok(Value::String(instant_string(*millis))),
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Float, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Int(value) => Ok(Value::String(float_string(*value as f64, at_f32))),
                ScalarValue::Uint(value) => Ok(Value::String(float_string(*value as f64, at_f32))),
                ScalarValue::F64(value) => Ok(Value::String(float_string(*value, at_f32))),
                // A peer that spelled the convention itself is kept as it spelled it, but only if
                // what it spelled is a number.
                ScalarValue::Str(spelled) => match spelled.parse::<f64>() {
                    Ok(_) => Ok(Value::String(spelled.to_string())),
                    Err(_) => Err(unexpected(stored_scalar(stated))),
                },
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Counter, automerge::Value::Scalar(stated)) => match stated.as_ref() {
                ScalarValue::Counter(value)
                    if schema.get(crate::facet::input::KEYWORD).is_some()
                        || i64::from(value).unsigned_abs() <= JS_SAFE_INTEGER_MAX as u64 =>
                {
                    Ok(Value::Number(i64::from(value).into()))
                }
                ScalarValue::Counter(value) => Err(unexpected(&format!(
                    "counter {} is outside the JavaScript-safe integer range",
                    i64::from(value)
                ))),
                other => Err(unexpected(stored_scalar(other))),
            },
            (Codec::Text, automerge::Value::Object(ObjType::Text)) => {
                Ok(Value::String(doc.text(id)?))
            }
            (_shape, stored) => Err(unexpected(stored_kind(stored))),
        }
    }

    /// Whether a value is one this storage form can hold.
    fn holds(self, value: &Value) -> Result<(), CodecError> {
        let found = kind(value);
        match self {
            Codec::Bytes => match bytes_value(value) {
                Some(_) => Ok(()),
                None => Err(CodecError::NotBytes { found }),
            },
            Codec::Timestamp => match value.as_str() {
                Some(stated) => match instant_millis(stated) {
                    Some(_) => Ok(()),
                    None => Err(CodecError::NotAnInstant {
                        stated: stated.to_owned(),
                    }),
                },
                None => Err(CodecError::NotAnInstant {
                    stated: found.to_owned(),
                }),
            },
            Codec::Datetime => value
                .as_str()
                .filter(|value| crate::facet::input::datetime(value))
                .map(|_| ())
                .ok_or_else(|| CodecError::NotAnInstant {
                    stated: found.to_owned(),
                }),
            Codec::Uint => value
                .as_str()
                .filter(|value| {
                    !value.is_empty()
                        && value.bytes().all(|byte| byte.is_ascii_digit())
                        && (value.len() == 1 || !value.starts_with('0'))
                })
                .and_then(|value| value.parse::<u64>().ok())
                .map(|_| ())
                .ok_or_else(|| CodecError::SchemaViolation {
                    path: String::new(),
                    message: "expected a stringified uint64".to_owned(),
                }),
            Codec::Reference => match value.as_str() {
                Some(stated) => {
                    crate::FacetReference::parse(stated)
                        .map(|_| ())
                        .map_err(|reason| CodecError::NotAFacetReference {
                            stated: stated.to_owned(),
                            reason: reason.to_string(),
                        })
                }
                None => Err(CodecError::NotAFacetReference {
                    stated: kind(value).to_owned(),
                    reason: "a facet reference is a `db+facet:` address string".to_owned(),
                }),
            },
            Codec::Counter => value
                .as_i64()
                .map(|_value| ())
                .ok_or(CodecError::NotACounter { found }),
            Codec::Text => value
                .as_str()
                .map(|_value| ())
                .ok_or(CodecError::NotAText { found }),
            Codec::Float => match value {
                Value::Number(_) => Ok(()),
                Value::String(stated) => {
                    if stated.parse::<f64>().is_ok() {
                        Ok(())
                    } else {
                        Err(CodecError::NotAFloat {
                            stated: stated.clone(),
                        })
                    }
                }
                _ => Err(CodecError::NotAFloat {
                    stated: found.to_owned(),
                }),
            },
        }
    }
}

/// The JSON kind of a value, for a refusal to say what it found instead.
fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(number) if number.is_f64() => "a fractional number",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Base64, padded or not on the way in: both spell the same bytes, and a writer that leaves the
/// padding off is unambiguous.
pub(super) fn decode_base64(stated: &str) -> Option<Vec<u8>> {
    data_encoding::BASE64
        .decode(stated.as_bytes())
        .or_else(|_| data_encoding::BASE64_NOPAD.decode(stated.as_bytes()))
        .ok()
}

/// Read the one canonical bytes object. Plain base64 strings belong to explicit foreign adapters,
/// never to the Cabinet canonical path.
pub(super) fn bytes_value(value: &Value) -> Option<Vec<u8>> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.get("$bytes")?.as_str().and_then(decode_base64)
}

/// The milliseconds of an RFC 3339 instant.
///
/// A document holds a timestamp as whole milliseconds since the unix epoch — the format says so, and
/// encodes the integer unchanged — so that is the precision a value fits in. An instant finer than a
/// millisecond, which is every `Timestamp::now()`, keeps its millisecond and loses the rest: the
/// storage has the precision it has, and a value that needs more of it is a string field.
fn instant_millis(stated: &str) -> Option<i64> {
    stated
        .parse::<jiff::Timestamp>()
        .ok()
        .map(|instant| instant.as_millisecond())
}

/// The RFC 3339 spelling of a millisecond, which is what a document reads back as.
fn instant_string(millis: i64) -> String {
    jiff::Timestamp::from_millisecond(millis)
        .expect("a timestamp that a document holds is in range")
        .to_string()
}

/// The spelling of a float in cabinet JSON: the shortest round-tripping decimal at the width the
/// declaration's type states — an `f32` position says `"0.1"`, not the `f64` that widened it on the
/// way through a JSON value — with the decimal point kept (`"1.0"`, which is how a reader knows
/// this was never a whole number) and the three values JSON cannot state at all by the names every
/// parser reads.
pub(super) fn float_string(stated: f64, at_f32: bool) -> String {
    let stated = if at_f32 {
        (stated as f32) as f64
    } else {
        stated
    };
    match stated.classify() {
        FpCategory::Nan => return "NaN".to_owned(),
        FpCategory::Infinite if stated.is_sign_negative() => return "-Infinity".to_owned(),
        FpCategory::Infinite => return "Infinity".to_owned(),
        _ => {}
    }
    let mut spelled = if at_f32 {
        (stated as f32).to_string()
    } else {
        serde_json::Number::from_f64(stated)
            .expect("a float is finite here")
            .to_string()
    };
    if !spelled.contains('.') && !spelled.contains(['e', 'E']) {
        spelled.push_str(".0")
    }
    spelled
}
/// A number stated, or spelled.
fn parse_float(stated: &Value) -> Option<f64> {
    stated
        .as_f64()
        .or_else(|| stated.as_str().and_then(|it| it.parse().ok()))
}

/// The schema of a value's JSON form, with its storage declaration added.
///
/// A type that writes its own `JsonSchema` — a newtype over bytes, say — calls this, so that a field
/// of that type is stored by the codec wherever it sits. That is what makes `Vec<Timestamp>` and
/// `Option<Bytes>` expressible: the declaration is the element's, not the field's.
pub fn codec_schema(codec: Codec, schema: Schema) -> Schema {
    let mut schema = schema;
    match codec {
        Codec::Bytes => {
            // The canonical bytes object carries the standard encoding annotation on its
            // `$bytes` member; the sidecar marks the enclosing object as the storage form.
            schema.insert(KEYWORD.to_owned(), codec.name().into());
        }
        Codec::Timestamp => {
            schema.insert(FORMAT.to_owned(), DATE_TIME.into());
        }
        Codec::Counter
        | Codec::Text
        | Codec::Float
        | Codec::Uint
        | Codec::Datetime
        | Codec::Reference => {
            schema.insert(KEYWORD.to_owned(), codec.name().into());
        }
    };
    schema
}

/// A position whose value the storage declaration cannot hold.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// our keyword names a shape this build does not implement
    #[error(
        "`{KEYWORD}` is `{stated}`, which is not a storage form this build implements: the peer that \
         wrote it knows a shape we would silently store as something else"
    )]
    Unknown { stated: String },

    /// our keyword is not a spelling
    #[error("`{KEYWORD}` names a storage form, but this position states {stated}")]
    NotASpelling { stated: Value },

    /// the position is declared as bytes, and the value is not the protocol bytes object
    #[error("this position is stored as bytes, so it is a {{$bytes}} object, not {found}")]
    NotBytes { found: &'static str },

    /// an object holds the protocol bytes key and not bytes it can read
    #[error(
        "`$bytes` is the canonical bytes object, and {stated} is not the base64 string it holds"
    )]
    NotBase64 { stated: Value },

    /// the position is declared as an instant, and the value is not an RFC 3339 string
    #[error("this position is stored as an instant, so it is an RFC 3339 string, not {stated}")]
    NotAnInstant { stated: String },

    /// the position is declared as a counter, and the value is not a whole number
    #[error("this position is stored as a counter, so it is a whole number, not {found}")]
    NotACounter { found: &'static str },

    /// the position is declared as a text, and the value is not a string
    #[error("this position is stored as text, so it is a string, not {found}")]
    NotAText { found: &'static str },

    /// the position is declared a facet reference, and the value is not a `db+facet:` address
    #[error("this position is declared a facet reference, and {stated} is not one: {reason}")]
    NotAFacetReference { stated: String, reason: String },

    /// the position is declared as a float, and the value does not spell a number
    #[error(
        "this position is carried as the decimal string of a float, and {stated} does not spell \
         one"
    )]
    NotAFloat { stated: String },

    /// the position is declared a number, and the value is not one
    #[error("this position is declared a number, and {found} is not one")]
    KindMismatch { found: &'static str },

    /// the document holds a number JSON has no way to write
    #[error("this position holds {stated}, which JSON cannot state: it has no NaN and no infinity")]
    NotAJsonNumber { stated: f64 },

    /// the carried JSON Schema itself is invalid or cannot resolve a non-carried reference
    #[error("the carried JSON Schema is invalid: {message}")]
    InvalidSchema { message: String },

    /// the submitted value violates the carried JSON Schema
    #[error("the submitted value violates JSON Schema at `{path}`: {message}")]
    SchemaViolation { path: String, message: String },

    /// a canonical integer is outside JavaScript's exact-integer range
    #[error("the canonical integer {stated} is outside the JavaScript-safe range")]
    NotAJsonInteger { stated: String },

    /// an integer declaration received a JSON number without an integral representation
    #[error(
        "the integer declaration cannot store JSON number {stated} without losing its authored value"
    )]
    NonIntegralInteger { stated: String },

    /// authored normalization exceeded its deterministic resource budget
    #[error("authored normalization exceeded its deterministic resource budget of {limit} visits")]
    NormalizationLimit { limit: usize },

    /// authoring normalization has more than one carried branch with no deterministic selection
    #[error("authored normalization has ambiguous carried branches")]
    NormalizationAmbiguous,

    /// an application tried to use a protocol-reserved object key
    #[error("{key} is reserved by the canonical protocol and cannot be an application key")]
    ReservedKey { key: String },

    /// a reference names a definition the document does not carry
    #[error("the schema refers to `{name}`, which this document does not define")]
    Undefined { name: String },

    /// a reference is not a reference into the definitions
    #[error("`$ref` is `{stated}`, which is not a reference into this document's definitions")]
    NotAReference { stated: String },

    /// no branch of a `anyOf`/`oneOf` fits the value
    #[error("no branch here accepts {kind}")]
    NoBranch { kind: &'static str },

    /// the schema composes a position in a way the codec does not walk
    #[error(
        "this position is composed with `{keyword}`, which the codec does not walk: a declaration \
         hidden in there would be stored as a plain value without a word"
    )]
    Composed { keyword: &'static str },

    /// a reference or a branch chain that does not settle
    #[error("this position's schema does not resolve: {chain}")]
    Unresolved { chain: String },

    /// a position the declarations say nothing about holds a value only a declaration can read
    #[error(
        "this position declares nothing about what it holds, and the document holds {held} there: \
         guessing it back would hide a declaration the writer made"
    )]
    Undeclared { held: &'static str },

    /// the checked delta a counter would need is outside the counter's range
    #[error(
        "this counter holds {base}, and writing {submitted} would need the delta the counter \
         cannot arrive at"
    )]
    CounterDelta { base: i64, submitted: i64 },

    /// a number the native kind at this position cannot hold
    #[error(
        "the document holds {held} native storage at this position, and {found} cannot be stored \
         in it: it would lose the value or the storage's kind, so it is refused"
    )]
    Unrepresentable { held: &'static str, found: String },

    /// a write a checked plan asked for was refused by the document
    #[error(transparent)]
    Reconcile(#[from] autosurgeon::reconcile::ReconcileError),

    /// an Automerge read a check needed could not be made
    #[error("the base view could not be read: {source}")]
    Automerge { source: automerge::AutomergeError },

    /// what a refusal says about where it happened
    #[error("at `{path}`: {source}")]
    At {
        path: String,
        #[source]
        source: Box<CodecError>,
    },
}

/// Bytes in the document, the protocol `{ "$bytes": "…" }` object in JSON.
///
/// A byte field needs a type of its own: `autosurgeon` reconciles `Vec<T>` as a list, so `Vec<u8>` is
/// an array of numbers to it. This type carries the whole declaration — the JSON form, the schema and
/// the storage — where the alternative is a `serialize_with`, a `deserialize_with` and a
/// `schema_with` on every field that holds bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    /// The bytes, borrowed.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Self {
        bytes.0
    }
}

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(1))?;
        object.serialize_entry("$bytes", &data_encoding::BASE64.encode(&self.0))?;
        object.end()
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let Some(stated) = value.get("$bytes").and_then(Value::as_str) else {
            return Err(serde::de::Error::custom(
                "bytes need a $bytes base64 string",
            ));
        };
        if value.as_object().is_none_or(|object| object.len() != 1) {
            return Err(serde::de::Error::custom(
                "bytes only contain the $bytes member",
            ));
        }
        decode_base64(stated)
            .map(Bytes)
            .ok_or_else(|| serde::de::Error::custom(format!("`{stated}` is not base64")))
    }
}

impl JsonSchema for Bytes {
    /// A storage form is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Bytes".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Bytes").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        codec_schema(
            Codec::Bytes,
            json_schema!({
                "type": "object",
                "properties": {
                    "$bytes": {"type": "string", "contentEncoding": "base64"}
                },
                "required": ["$bytes"],
                "additionalProperties": false
            }),
        )
    }
}

impl autosurgeon::Reconcile for Bytes {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(&self.0)
    }
}

impl autosurgeon::Hydrate for Bytes {
    fn hydrate_bytes(stated: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Bytes(stated.to_vec()))
    }
}

/// A counter in the document, a number in JSON.
///
/// A counter merges by addition rather than by last writer winning, so it is an object in the
/// document and not a number. A JSON document states a value, so reconciling one *sets* the counter;
/// an editor that means "add five" increments the counter it hydrated instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Counter(i64);

impl Counter {
    /// Construct a canonical counter. An unsafe integer is a programming error because it cannot
    /// be represented by the facet JSON contract or read back by a canonical consumer.
    pub fn new(value: i64) -> Self {
        assert!(
            value.unsigned_abs() <= JS_SAFE_INTEGER_MAX as u64,
            "a counter is outside the JavaScript-safe integer range"
        );
        Self(value)
    }

    /// The counter's canonical integer value.
    pub fn value(self) -> i64 {
        self.0
    }
}

impl From<i64> for Counter {
    fn from(value: i64) -> Self {
        Self::new(value)
    }
}

impl Serialize for Counter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.unsigned_abs() > JS_SAFE_INTEGER_MAX as u64 {
            return Err(S::Error::custom(
                "a counter is outside the JavaScript-safe integer range",
            ));
        }
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for Counter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let stated = i64::deserialize(deserializer)?;
        if stated.unsigned_abs() > JS_SAFE_INTEGER_MAX as u64 {
            return Err(serde::de::Error::custom(
                "a counter is outside the JavaScript-safe integer range",
            ));
        }
        Ok(Counter(stated))
    }
}

impl JsonSchema for Counter {
    /// A storage form is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Counter".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Counter").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        codec_schema(Codec::Counter, json_schema!({"type": "integer"}))
    }
}

impl autosurgeon::Reconcile for Counter {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        assert!(
            self.0.unsigned_abs() <= JS_SAFE_INTEGER_MAX as u64,
            "a counter is outside the JavaScript-safe integer range"
        );
        reconciler.counter()?.set(self.0)
    }
}

impl autosurgeon::Hydrate for Counter {
    fn hydrate_counter(stated: i64) -> Result<Self, autosurgeon::HydrateError> {
        if stated.unsigned_abs() > JS_SAFE_INTEGER_MAX as u64 {
            return Err(autosurgeon::HydrateError::unexpected(
                "a JavaScript-safe counter",
                stated.to_string(),
            ));
        }
        Ok(Counter(stated))
    }
}
/// A float in the document, a decimal string in JSON.
///
/// The widths are types of their own, like `Bytes` is: a JSON value cannot say that a number is a
/// float, cannot state the non-finite values at all, and a carrier that knows nothing can lower a
/// number's precision on the way through — so the declared width is spelled in the JSON, and a
/// whole number keeps the point that says it is not an integer. A bare `f64` field derives a plain
/// number instead, which is the opt-out stated by the type itself.
#[derive(Debug, Clone, PartialEq)]
pub struct Float64(pub f64);
impl From<f64> for Float64 {
    fn from(stated: f64) -> Self {
        Self(stated)
    }
}

impl Serialize for Float64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&float_string(self.0, false))
    }
}

impl<'de> Deserialize<'de> for Float64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let stated = Value::deserialize(deserializer)?;
        parse_float(&stated)
            .map(Float64)
            .ok_or_else(|| serde::de::Error::custom(format!("{stated} does not spell a number")))
    }
}

impl JsonSchema for Float64 {
    /// A storage form is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Float64".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Float64").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        codec_schema(
            Codec::Float,
            json_schema!({"type": "string", "format": "double"}),
        )
    }
}

impl autosurgeon::Reconcile for Float64 {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.f64(self.0)
    }
}

impl autosurgeon::Hydrate for Float64 {
    fn hydrate_f64(stated: f64) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Float64(stated))
    }

    // A document written before the widths were declared stores the token's kind; recover a float.
    fn hydrate_int(stated: i64) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Float64(stated as f64))
    }

    fn hydrate_uint(stated: u64) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Float64(stated as f64))
    }
}

/// The same float at `f32`, whose shortest spelling is the width an `f32` field recovers exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct Float32(pub f32);

impl From<f32> for Float32 {
    fn from(stated: f32) -> Self {
        Self(stated)
    }
}

impl Serialize for Float32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&float_string(self.0 as f64, true))
    }
}

impl<'de> Deserialize<'de> for Float32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let stated = Value::deserialize(deserializer)?;
        parse_float(&stated)
            .map(|stated| Float32(stated as f32))
            .ok_or_else(|| serde::de::Error::custom(format!("{stated} does not spell a number")))
    }
}

impl JsonSchema for Float32 {
    /// A storage form is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Float32".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Float32").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        codec_schema(
            Codec::Float,
            json_schema!({"type": "string", "format": "float"}),
        )
    }
}

impl autosurgeon::Reconcile for Float32 {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.f64(self.0 as f64)
    }
}

impl autosurgeon::Hydrate for Float32 {
    fn hydrate_f64(stated: f64) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Float32(stated as f32))
    }
}

/// A collaborative text in the document, a string in JSON.
///
/// The Rust side is `autosurgeon`'s `Text`, so an editor hydrates it, splices, and reconciles the
/// whole value — which is how a text merges edit by edit, and why reconciling one against heads that
/// have moved since it was hydrated is an error rather than a silent overwrite. A JSON document
/// states a whole text instead, so a string reconciled by [`Stored`] writes the difference between
/// what the document holds and what the string says.
#[derive(Debug)]
pub struct Text(pub autosurgeon::Text);

impl Text {
    /// The text as it was hydrated.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<autosurgeon::Text> for Text {
    fn from(text: autosurgeon::Text) -> Self {
        Self(text)
    }
}

impl From<String> for Text {
    fn from(stated: String) -> Self {
        Self(autosurgeon::Text::with_value(stated))
    }
}

impl From<&str> for Text {
    fn from(stated: &str) -> Self {
        Self(autosurgeon::Text::with_value(stated))
    }
}

impl Serialize for Text {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Text(autosurgeon::Text::with_value(String::deserialize(
            deserializer,
        )?)))
    }
}

impl JsonSchema for Text {
    /// A storage form is a position, never a name a peer points at, so it is inlined.
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Text".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Text").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        codec_schema(Codec::Text, json_schema!({"type": "string"}))
    }
}

impl autosurgeon::Reconcile for Text {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        self.0.reconcile(reconciler)
    }
}

impl autosurgeon::Hydrate for Text {
    fn hydrate<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: autosurgeon::Prop<'_>,
    ) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Text(autosurgeon::Text::hydrate(doc, obj, prop)?))
    }
}

/// How a message names a stored scalar.
fn stored_scalar(scalar: &ScalarValue) -> &'static str {
    match scalar {
        ScalarValue::Null => "null",
        ScalarValue::Boolean(_) => "a boolean",
        ScalarValue::Int(_) | ScalarValue::Uint(_) => "an integer",
        ScalarValue::F64(_) => "a fractional number",
        ScalarValue::Counter(_) => "a counter",
        ScalarValue::Bytes(_) => "bytes",
        ScalarValue::Str(_) => "a string",
        ScalarValue::Timestamp(_) => "a timestamp",
        ScalarValue::Unknown { .. } => "a scalar this build does not know",
    }
}

/// How a message names a stored value.
fn stored_kind(stored: &automerge::Value) -> &'static str {
    match stored {
        automerge::Value::Scalar(scalar) => stored_scalar(scalar),
        automerge::Value::Object(ObjType::Map | ObjType::Table) => "an object",
        automerge::Value::Object(ObjType::List) => "a list",
        automerge::Value::Object(ObjType::Text) => "a text",
    }
}
