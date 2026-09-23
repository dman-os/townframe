//! How a value is stored in a document, declared by its schema rather than guessed from the name of
//! the field that holds it.
//!
//! serde says what a value looks like as JSON. It cannot say what that value has to be *in a
//! document*, where the storage has shapes JSON does not: bytes rather than an array of numbers, an
//! instant rather than a string, and a counter or a text, which are not values but objects that
//! merge. So the storage form is declared beside the JSON form, and reconciliation reads the
//! declaration.
//!
//! Two of the five storage forms are standard JSON Schema keywords, and schemas already carry them
//! without our help: `jiff::Timestamp` emits `format: "date-time"`, and a byte field emits
//! `contentEncoding: "base64"`. The other three are `x-automerge`, because JSON Schema cannot
//! describe a CRDT object, cannot say that a number is a *float*, and cannot state the non-finite
//! values at all — so a float's declaration is its value's own type (`facet::Float64`/`Float32`,
//! spelled as the decimal string it is), and a bare `f64` derives a plain number, which is the
//! opt-out the Rust type itself states. Nothing opts in: a type declares its storage by what its
//! `JsonSchema` says, so a foreign type whose schema states a storage form — `jiff::Timestamp`
//! today, `chrono`'s or `bytes::Bytes` tomorrow — is stored correctly as soon as it does.
//!
//! A declaration is asserted rather than sniffed, so a value is checked against the declarations it
//! will be stored under before anything is written ([`Stored::check`]); from then on it cannot fail
//! on its own account, and a field named `createdAt` is an instant because its schema says so, not
//! because of the way it is spelled.

use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use autosurgeon::reconcile::{CounterReconciler, TextReconciler};
use autosurgeon::{HydrateError, Reconciler};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::borrow::Cow;
use std::num::FpCategory;

mod walk;

#[cfg(test)]
mod e2e;

pub use walk::{Stored, hydrate_value};

/// The keyword for the storage forms JSON Schema cannot describe.
pub const KEYWORD: &str = "x-automerge";

/// Where JSON Schema does state a storage form, and the value we read there.
const CONTENT_ENCODING: &str = "contentEncoding";
const BASE64: &str = "base64";
const FORMAT: &str = "format";
const DATE_TIME: &str = "date-time";

/// A shape whose storage in a document is not its JSON form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// Bytes in the document, a base64 string in JSON.
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
}

impl Codec {
    /// Every shape a declaration can state.
    pub const ALL: [Codec; 5] = [
        Codec::Bytes,
        Codec::Timestamp,
        Codec::Counter,
        Codec::Text,
        Codec::Float,
    ];

    /// How this shape is spelled in our keyword, and how it is named in a message.
    pub const fn name(self) -> &'static str {
        match self {
            Codec::Bytes => "bytes",
            Codec::Timestamp => "timestamp",
            Codec::Counter => "counter",
            Codec::Float => "float",
            Codec::Text => "text",
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
        if schema
            .get(CONTENT_ENCODING)
            .and_then(Value::as_str)
            .is_some_and(|stated| stated.eq_ignore_ascii_case(BASE64))
        {
            return Ok(Some(Codec::Bytes));
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
                let stated = value.as_str().expect("checked");
                reconciler.bytes(decode_base64(stated).expect("checked"))
            }
            Codec::Timestamp => {
                let stated = value.as_str().expect("checked");
                reconciler.timestamp(instant_millis(stated).expect("checked"))
            }
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
                ScalarValue::Bytes(bytes) => Ok(Value::String(data_encoding::BASE64.encode(bytes))),
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
                ScalarValue::Counter(value) => Ok(Value::Number(i64::from(value).into())),
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
            Codec::Bytes => match value.as_str().and_then(decode_base64) {
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
fn decode_base64(stated: &str) -> Option<Vec<u8>> {
    data_encoding::BASE64
        .decode(stated.as_bytes())
        .or_else(|_| data_encoding::BASE64_NOPAD.decode(stated.as_bytes()))
        .ok()
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
fn float_string(stated: f64, at_f32: bool) -> String {
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
            schema.insert(CONTENT_ENCODING.to_owned(), BASE64.into());
        }
        Codec::Timestamp => {
            schema.insert(FORMAT.to_owned(), DATE_TIME.into());
        }
        Codec::Counter | Codec::Text | Codec::Float => {
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

    /// the position is declared as bytes, and the value is not base64
    #[error("this position is stored as bytes, so it is a base64 string, not {found}")]
    NotBytes { found: &'static str },

    /// the position is declared as an instant, and the value is not an RFC 3339 string
    #[error("this position is stored as an instant, so it is an RFC 3339 string, not {stated}")]
    NotAnInstant { stated: String },

    /// the position is declared as a counter, and the value is not a whole number
    #[error("this position is stored as a counter, so it is a whole number, not {found}")]
    NotACounter { found: &'static str },

    /// the position is declared as a text, and the value is not a string
    #[error("this position is stored as text, so it is a string, not {found}")]
    NotAText { found: &'static str },

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

    /// what a refusal says about where it happened
    #[error("at `{path}`: {source}")]
    At {
        path: String,
        #[source]
        source: Box<CodecError>,
    },
}

/// Bytes in the document, a base64 string in JSON.
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
        serializer.serialize_str(&data_encoding::BASE64.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let stated = String::deserialize(deserializer)?;
        decode_base64(&stated)
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
        codec_schema(Codec::Bytes, json_schema!({"type": "string"}))
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
pub struct Counter(pub i64);

impl From<i64> for Counter {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl Serialize for Counter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for Counter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Counter(i64::deserialize(deserializer)?))
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
        reconciler.counter()?.set(self.0)
    }
}

impl autosurgeon::Hydrate for Counter {
    fn hydrate_counter(stated: i64) -> Result<Self, autosurgeon::HydrateError> {
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
