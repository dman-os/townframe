//! Both directions of the codec: a JSON value into a document, and a document back into JSON.
//!
//! The declaration is read at the position it was written at, so both directions walk the value and
//! the schema together: a `$ref` resolves in the document's definitions, a union narrows to the
//! branch the content fits, and a member the schema does not name is plain JSON — which is what
//! makes an open object open.

use super::{Codec, CodecError, kind};
use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use autosurgeon::reconcile::NoKey;
use autosurgeon::reconcile::{MapReconciler, SeqReconciler};
use autosurgeon::{HydrateError, Prop, Reconcile, Reconciler};
use serde_json::Value;
use std::borrow::Cow;
use std::fmt::{self, Display};

/// How many reference and branch hops a position may take before its schema is a loop rather than a
/// declaration.
const HOPS: usize = 16;

/// A value checked against the declarations of the document it is going into.
///
/// A storage declaration is an assertion — a position declared as bytes holds base64, not a string
/// that failed to decode — so the value is checked once, at the edge where a bad one can still be
/// reported. Reconciliation afterwards reports only what the reconciler itself reports.
#[derive(Debug)]
pub struct Stored<'a> {
    document: &'a Value,
    value: Value,
}

impl<'a> Stored<'a> {
    /// Check a value against the declarations of the document it will be stored in.
    pub fn check(document: &'a Value, value: Value) -> Result<Self, CodecError> {
        let path = Path::root();
        Position::new(document, Some(document)).check(&value, &path)?;
        Ok(Self { document, value })
    }

    /// The value, as it was checked.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The value, checked.
    pub fn into_value(self) -> Value {
        self.value
    }

    /// Write the value into a document.
    pub fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        let path = Path::root();
        Position::new(self.document, Some(self.document)).write(&self.value, &path, reconciler)
    }
}

impl Reconcile for Stored<'_> {
    type Key<'a> = NoKey;

    fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        Stored::reconcile(self, reconciler)
    }
}

/// Read a value out of a document as the JSON form its schema declares.
///
/// A document holds what the declarations say, so a storage form at a position nothing declares is
/// refused: reading a counter as a number, or bytes as a base64 string, hands back a value whose type
/// the writer did not intend.
pub fn hydrate_value<D: ReadDoc>(
    doc: &D,
    obj: &ObjId,
    prop: Prop<'_>,
    document: &Value,
) -> Result<Value, HydrateError> {
    let path = Path::root();
    Position::new(document, Some(document)).read(doc, obj, prop, &path)
}

/// Where a position sits, as a chain that is rendered only when something is refused: reading or
/// writing a value does not allocate for the sake of an error that does not happen.
struct Path<'p> {
    parent: Option<&'p Path<'p>>,
    step: Step<'p>,
}

enum Step<'p> {
    Root,
    Key(&'p str),
    Index(usize),
}

impl Path<'_> {
    fn root() -> Path<'static> {
        Path {
            parent: None,
            step: Step::Root,
        }
    }
}

impl<'p> Path<'p> {
    fn key(parent: &'p Path<'p>, key: &'p str) -> Self {
        Self {
            parent: Some(parent),
            step: Step::Key(key),
        }
    }

    fn index(parent: &'p Path<'p>, index: usize) -> Self {
        Self {
            parent: Some(parent),
            step: Step::Index(index),
        }
    }
}

impl Display for Path<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut steps = vec![&self.step];
        let mut parent = self.parent;
        while let Some(path) = parent {
            steps.push(&path.step);
            parent = path.parent;
        }
        for step in steps.into_iter().rev() {
            match step {
                Step::Root => write!(out, "$")?,
                Step::Key(key) => write!(out, ".{key}")?,
                Step::Index(index) => write!(out, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// The kind of what sits at a position, which is what a union is decided by: the kind of the JSON
/// value going in, and the kind of what the document holds coming out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl Kind {
    /// What a message calls this kind of content.
    fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Boolean => "a boolean",
            Kind::Number => "a number",
            Kind::String => "a string",
            Kind::Array => "an array",
            Kind::Object => "an object",
        }
    }

    /// Whether a schema's `type` states this kind. `integer` is the whole numbers of `number`, and a
    /// kind does not tell whole from fractional here: a union is decided by shape, and a value that
    /// has to be whole is refused by the storage form that says so.
    fn matches(self, stated: &str) -> bool {
        match self {
            Kind::Null => stated == "null",
            Kind::Boolean => stated == "boolean",
            Kind::Number => stated == "number" || stated == "integer",
            Kind::String => stated == "string",
            Kind::Array => stated == "array",
            Kind::Object => stated == "object",
        }
    }

    fn of_json(value: &Value) -> Self {
        match value {
            Value::Null => Kind::Null,
            Value::Bool(_) => Kind::Boolean,
            Value::Number(_) => Kind::Number,
            Value::String(_) => Kind::String,
            Value::Array(_) => Kind::Array,
            Value::Object(_) => Kind::Object,
        }
    }

    /// The kind a stored value has *as JSON*, which is what a reader gets back: bytes and a timestamp
    /// read as strings, a counter as a number, a text as a string.
    fn of_stored(stored: &automerge::Value) -> Self {
        match stored {
            automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                ScalarValue::Null => Kind::Null,
                ScalarValue::Boolean(_) => Kind::Boolean,
                ScalarValue::Int(_) | ScalarValue::Uint(_) | ScalarValue::F64(_) => Kind::Number,
                ScalarValue::Counter(_) => Kind::Number,
                ScalarValue::Bytes(_) | ScalarValue::Str(_) | ScalarValue::Timestamp(_) => {
                    Kind::String
                }
                ScalarValue::Unknown { .. } => Kind::Null,
            },
            automerge::Value::Object(ObjType::Map | ObjType::Table) => Kind::Object,
            automerge::Value::Object(ObjType::List) => Kind::Array,
            automerge::Value::Object(ObjType::Text) => Kind::String,
        }
    }
}

/// A position in a schema: what is declared here, and the document whose definitions those
/// declarations resolve in.
#[derive(Debug, Clone, Copy)]
struct Position<'a> {
    document: &'a Value,
    /// Nothing when this position is not declared at all, which is what an unnamed member of an open
    /// object is: it holds plain JSON.
    schema: Option<&'a Value>,
}

impl<'a> Position<'a> {
    fn new(document: &'a Value, schema: Option<&'a Value>) -> Self {
        Self { document, schema }
    }

    /// The schema at this position, with a reference followed and a union narrowed to the branch the
    /// content's kind fits.
    ///
    /// A position that composes its schemas in a way this does not walk is refused rather than walked
    /// in part: a declaration hidden inside one would be stored as a plain value without a word.
    fn resolved(&self, kind: Kind) -> Result<Option<&'a Value>, CodecError> {
        let Some(mut schema) = self.schema else {
            return Ok(None);
        };
        for _hop in 0..HOPS {
            if let Some(composed) = composed(schema) {
                return Err(CodecError::Composed { keyword: composed });
            }
            if let Some(reference) = schema.get("$ref") {
                let Some(stated) = reference.as_str() else {
                    return Err(CodecError::NotAReference {
                        stated: reference.to_string(),
                    });
                };
                let Some(name) = stated.strip_prefix("#/$defs/") else {
                    return Err(CodecError::NotAReference {
                        stated: stated.to_owned(),
                    });
                };
                schema = self.definition(name)?;
                continue;
            }
            if let Some(branches) = any_of(schema) {
                let mut fit = None;
                for branch in branches {
                    if self.fits(branch, kind)? {
                        fit = Some(branch);
                        break;
                    }
                }
                schema = fit.ok_or(CodecError::NoBranch { kind: kind.name() })?;
                continue;
            }
            return Ok(Some(schema));
        }
        Err(CodecError::Unresolved {
            chain: "it refers back to itself".to_owned(),
        })
    }

    /// One definition of this document. Every definition is carried in the same flat place, which is
    /// what lets two facets reach one definition once.
    fn definition(&self, name: &str) -> Result<&'a Value, CodecError> {
        self.document
            .get("$defs")
            .and_then(|definitions| definitions.get(name))
            .ok_or_else(|| CodecError::Undefined {
                name: name.to_owned(),
            })
    }

    /// Whether a branch of a union is the one this kind of content belongs in. A branch that states
    /// no type accepts anything, which is what a reference looks like before it is followed.
    fn fits(&self, branch: &'a Value, kind: Kind) -> Result<bool, CodecError> {
        let mut branch = branch;
        if let Some(name) = branch
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|stated| stated.strip_prefix("#/$defs/"))
            && let Ok(definition) = self.definition(name)
        {
            branch = definition;
        }
        match Codec::of(branch)? {
            // A position our keyword is on is spelled by the codec, and a float is the one whose
            // storage and JSON are of different kinds: the document holds the number and the
            // convention carries the string, so both spellings are the float branch.
            Some(Codec::Float) => Ok(matches!(kind, Kind::Number | Kind::String)),
            _ => match branch.get("type") {
                None => Ok(true),
                Some(Value::String(one)) => Ok(kind.matches(one)),
                Some(Value::Array(ones)) => Ok(ones
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|one| kind.matches(one))),
                Some(_) => Ok(true),
            },
        }
    }

    /// The position of one member of an object. A member the schema does not name is plain JSON, or
    /// the schema `additionalProperties` states for the members it does not name.
    fn member(&self, schema: Option<&'a Value>, key: &str) -> Position<'a> {
        Position {
            document: self.document,
            schema: schema.and_then(|schema| declared_member(schema, key)),
        }
    }

    /// The position of one element of an array.
    fn element(&self, schema: Option<&'a Value>, index: usize) -> Position<'a> {
        Position {
            document: self.document,
            schema: schema.and_then(|schema| declared_element(schema, index)),
        }
    }

    /// Refuse a value the declarations here cannot hold. Nothing is written afterwards, so this is
    /// where a declaration turns into an assertion.
    fn check<'p>(&self, value: &'p Value, path: &'p Path<'p>) -> Result<(), CodecError> {
        let Some(schema) = self
            .resolved(Kind::of_json(value))
            .map_err(|err| at(path, err))?
        else {
            return Ok(());
        };
        if let Some(codec) = Codec::of(schema).map_err(|err| at(path, err))? {
            // A `null` a declaration claims — the `None` of an option, whose `type` carries it —
            // belongs to the declaration and not to the codec, whose holds is schema-blind.
            if !(value.is_null() && type_mentions(schema, "null")) {
                codec.holds(value).map_err(|err| at(path, err))?;
            }
        }
        // A scalar that is not a number, at a position declared one, would store a value the Rust
        // type cannot read — the storage's only fidelity rule; the rest of a kind mismatch is the
        // validator's business. Reaching an object or array at such a position is the same drift.
        if (type_mentions(schema, "number") || type_mentions(schema, "integer"))
            && !matches!(value, Value::Number(_))
        {
            return Err(at(path, CodecError::KindMismatch { found: kind(value) }));
        }
        match value {
            Value::Object(members) => {
                for (key, member) in members {
                    let child = Path::key(path, key);
                    self.member(Some(schema), key).check(member, &child)?;
                }
                Ok(())
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    let child = Path::index(path, index);
                    self.element(Some(schema), index).check(item, &child)?;
                }
                Ok(())
            }
            _value => Ok(()),
        }
    }

    /// Write a value into a document, reading the declaration at this position.
    fn write<'p, R: Reconciler>(
        &self,
        value: &'p Value,
        path: &'p Path<'p>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let schema = self
            .resolved(Kind::of_json(value))
            .expect("a checked value resolves at every position");
        if let Some(codec) = schema.and_then(|schema| {
            Codec::of(schema).expect("a checked value declares nothing this build does not have")
        }) {
            // A `null` a declaration claims — the `None` of an option — is written as the
            // document's null, and the codec is never asked about a value it does not have.
            let claimed = schema.is_some_and(|stated| type_mentions(stated, "null"));
            if !(value.is_null() && claimed) {
                return codec.write(value, &mut reconciler);
            }
        }
        match value {
            Value::Null => reconciler.none(),
            Value::Bool(stated) => reconciler.boolean(*stated),
            // What a number is *in the document* is what the schema says it is. JSON has one number
            // type, so a peer writing `1` for a float — or for an `i64` — would otherwise leave a
            // `Uint` where nothing can read a float or a signed integer back out
            // (`autosurgeon` reads an `i64` from `Int` only and a `u64` from `Uint` only).
            Value::Number(stated) => match number_kind(schema) {
                Number::Float => {
                    reconciler.f64(stated.as_f64().expect("a json number is a number"))
                }
                Number::Signed => match stated.as_i64() {
                    Some(whole) => reconciler.i64(whole),
                    // A value too large for the declared sign keeps its value: the schema states no
                    // maximum, so refusing or losing it would both be worse than the type drifting.
                    None => {
                        reconciler.u64(stated.as_u64().expect("a number is signed or unsigned"))
                    }
                },
                Number::Unsigned => match stated.as_u64() {
                    Some(whole) => reconciler.u64(whole),
                    // Likewise for a negative value at a position declared unsigned, which a schema
                    // with `minimum: 0` would have refused before this ran.
                    None => reconciler.i64(stated.as_i64().expect("a number is one of the two")),
                },
                // Nothing is declared at this position, so the token is all there is to go on.
                Number::Token => {
                    if let Some(whole) = stated.as_u64() {
                        reconciler.u64(whole)
                    } else if let Some(whole) = stated.as_i64() {
                        reconciler.i64(whole)
                    } else {
                        reconciler.f64(stated.as_f64().expect("a json number is a number"))
                    }
                }
            },
            Value::String(stated) => reconciler.str(stated),
            Value::Array(items) => {
                let mut sequence = reconciler.seq()?;
                // What the document has and the value does not is deleted, what both have is set, and
                // what only the value has is inserted: setting past the end of a list is not a write.
                let existing = sequence.len()?;
                if existing > items.len() {
                    for index in (items.len()..existing).rev() {
                        sequence.delete(index)?;
                    }
                }
                for (index, item) in items.iter().enumerate() {
                    let child = Path::index(path, index);
                    let position = self.element(schema, index);
                    if index < existing {
                        sequence.set(index, Documented(position, item, &child))?;
                    } else {
                        sequence.insert(index, Documented(position, item, &child))?;
                    }
                }
                Ok(())
            }
            Value::Object(members) => {
                let mut map = reconciler.map()?;
                let mut unknown = Vec::new();
                for (key, _member) in map.entries() {
                    if !members.contains_key(&key[..]) {
                        unknown.push(key.to_string());
                    }
                }
                for key in unknown {
                    map.delete(key)?;
                }
                for (key, member) in members {
                    let child = Path::key(path, key);
                    let position = self.member(schema, key);
                    map.put(key, Documented(position, member, &child))?;
                }
                Ok(())
            }
        }
    }

    /// Read what a document holds at a position as the JSON form its declaration states.
    fn read<'p, D: ReadDoc>(
        &self,
        doc: &D,
        obj: &ObjId,
        prop: Prop<'_>,
        path: &'p Path<'p>,
    ) -> Result<Value, HydrateError> {
        let Some((stored, id)) = doc.get(obj, &prop)? else {
            return Ok(Value::Null);
        };
        let schema = self
            .resolved(Kind::of_stored(&stored))
            .map_err(|err| refusal(path, err))?;
        // A `None` of an option read back: the stored null is the declaration's null, and the
        // codec is never asked about a value it does not have.
        if matches!(
            stored,
            automerge::Value::Scalar(ref stated) if matches!(stated.as_ref(), ScalarValue::Null)
        ) && schema.is_some_and(|stated| type_mentions(stated, "null"))
        {
            return Ok(Value::Null);
        }
        if let Some(codec) = schema
            .map(Codec::of)
            .transpose()
            .map_err(|err| refusal(path, err))?
            .flatten()
        {
            return codec.read(
                doc,
                &stored,
                &id,
                schema.expect("the codec read from a schema"),
            );
        }
        match stored {
            automerge::Value::Scalar(scalar) => scalar_json(&scalar, path),
            automerge::Value::Object(ObjType::Map | ObjType::Table) => {
                let mut members = serde_json::Map::new();
                for item in doc.map_range(&id, ..) {
                    let key = item.key.to_string();
                    let child = Path::key(path, &key);
                    let member = self.member(schema, &key).read(
                        doc,
                        &id,
                        Prop::Key(Cow::Borrowed(&key)),
                        &child,
                    )?;
                    members.insert(key, member);
                }
                Ok(Value::Object(members))
            }
            automerge::Value::Object(ObjType::List) => {
                let mut items = Vec::new();
                for index in 0..doc.length(&id) {
                    let child = Path::index(path, index);
                    items.push(self.element(schema, index).read(
                        doc,
                        &id,
                        Prop::Index(index as u32),
                        &child,
                    )?);
                }
                Ok(Value::Array(items))
            }
            automerge::Value::Object(ObjType::Text) => Ok(Value::String(doc.text(&id)?)),
        }
    }
}
/// A position on its way into a document: what a reconciler's own sub-reconcilers take.
struct Documented<'p>(Position<'p>, &'p Value, &'p Path<'p>);

impl Reconcile for Documented<'_> {
    type Key<'a> = NoKey;

    fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        self.0.write(self.1, self.2, reconciler)
    }
}

/// What a schema says a number is, which is what a document holds at that position.
enum Number {
    /// `type: number`: a float, which is an `F64` whatever the token looks like.
    Float,
    /// `type: integer` and signed: an `Int`, because an `i64` is read from `Int` only.
    Signed,
    /// `type: integer` and unsigned: a `Uint`, likewise.
    Unsigned,
    /// Nothing is declared here, so the token decides.
    Token,
}

/// The kind of number a schema declares.
///
/// JSON Schema already draws the distinction Automerge needs — `integer` and `number` are different
/// types, and `format` states the width and the sign — so this needs no vocabulary of ours. The
/// default for a bare `type: integer` is signed, because that type includes the negatives; a schema
/// that means unsigned says `format: uint64`.
fn number_kind(schema: Option<&Value>) -> Number {
    let Some(schema) = schema else {
        return Number::Token;
    };
    if type_mentions(schema, "number") {
        return Number::Float;
    }
    if !type_mentions(schema, "integer") {
        return Number::Token;
    }
    match schema.get("format").and_then(Value::as_str) {
        Some(format) if format.starts_with("uint") => Number::Unsigned,
        _value => Number::Signed,
    }
}

/// Whether a schema's `type` states this, written as one type or as a list of them: an
/// `Option<i64>` is `{"type": ["integer", "null"]}`.
fn type_mentions(schema: &Value, stated: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(one)) => one == stated,
        Some(Value::Array(ones)) => ones.iter().any(|one| one.as_str() == Some(stated)),
        _value => false,
    }
}

/// A composition this walk does not read, refused rather than walked in part.
fn composed(schema: &Value) -> Option<&'static str> {
    ["allOf", "prefixItems"]
        .into_iter()
        .find(|keyword| schema.get(*keyword).is_some())
}

fn any_of(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Value::as_array)
}

/// The schema a member is declared by: the one it is named in, or the one `additionalProperties`
/// states for the members the schema does not name.
fn declared_member<'a>(schema: &'a Value, key: &str) -> Option<&'a Value> {
    schema
        .get("properties")
        .and_then(|properties| properties.get(key))
        .or_else(|| {
            schema
                .get("additionalProperties")
                .filter(|stated| stated.is_object())
        })
}

/// The schema an element is declared by: `items` says the same thing about every element, and the
/// array form of it says one thing per position.
fn declared_element(schema: &Value, index: usize) -> Option<&Value> {
    match schema.get("items")? {
        Value::Array(by_position) => by_position.get(index),
        items => Some(items),
    }
}

/// What a refusal says about where it happened. It is attached where the refusal is raised, so the
/// path in it is the deepest one and nothing re-wraps it on the way up.
fn at(path: &Path<'_>, source: CodecError) -> CodecError {
    CodecError::At {
        path: path.to_string(),
        source: Box::new(source),
    }
}

/// The same refusal, said to a reader.
fn refusal(path: &Path<'_>, source: CodecError) -> HydrateError {
    HydrateError::unexpected(
        "a value its declarations hold",
        format!("at `{path}`: {source}"),
    )
}

/// A stored scalar as the JSON form a reader gets back. A storage form nothing declares as one is
/// refused: a counter read as a number, or bytes read as a string, is a value whose type the writer
/// did not intend, and guessing it back would hide a declaration that did not travel.
fn scalar_json(scalar: &ScalarValue, path: &Path<'_>) -> Result<Value, HydrateError> {
    let undeclared = |found: &str| {
        refusal(
            path,
            CodecError::Unknown {
                stated: found.to_owned(),
            },
        )
    };
    match scalar {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(stated) => Ok(Value::Bool(*stated)),
        ScalarValue::Int(stated) => Ok(Value::Number((*stated).into())),
        ScalarValue::Uint(stated) => Ok(Value::Number((*stated).into())),
        // JSON has no NaN and no infinity, so a document that holds one holds a number this
        // direction cannot state: it is refused rather than rounded or dropped.
        ScalarValue::F64(stated) => match serde_json::Number::from_f64(*stated) {
            Some(number) => Ok(Value::Number(number)),
            None => Err(refusal(
                path,
                CodecError::NotAJsonNumber { stated: *stated },
            )),
        },
        ScalarValue::Str(stated) => Ok(Value::String(stated.to_string())),
        ScalarValue::Counter(_) => Err(undeclared("counter")),
        ScalarValue::Timestamp(_) => Err(undeclared("timestamp")),
        ScalarValue::Bytes(_) => Err(undeclared("bytes")),
        ScalarValue::Unknown { .. } => Err(undeclared("an unknown scalar")),
    }
}
