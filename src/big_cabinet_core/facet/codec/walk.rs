//! Structural storage traversal and hydration for a compiled facet schema.

use super::target::Plan;
use super::validation::{CompiledSchema, ValidationProof};
use super::{
    Codec, CodecError, JS_SAFE_INTEGER_MAX, KEYWORD, bytes_value, declared_member, instant_string,
    kind,
};
use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use autosurgeon::reconcile::{MapReconciler, NoKey, SeqReconciler};
use autosurgeon::{HydrateError, Prop, Reconcile, Reconciler};
use serde_json::Value;
use std::borrow::Cow;
use std::fmt::{self, Display};

const HOPS: usize = 16;

fn js_safe_integer(value: i128) -> bool {
    (-i128::from(JS_SAFE_INTEGER_MAX)..=i128::from(JS_SAFE_INTEGER_MAX)).contains(&value)
}

fn protocol_key(key: &str) -> bool {
    matches!(key, "$type" | "$bytes" | "$link")
}

/// What the canonical data model requires of content no schema describes, at every depth.
///
/// An undeclared position is opaque to the *storage* walk — there is no declaration to follow, so
/// what a peer put there is kept as it is — but it is not opaque to the data model. The rules that
/// keep a document readable without a schema reach inside it too: the reserved `$` keys, the integer
/// range a JSON consumer can hold, and a `$bytes` object that has to hold bytes rather than claim
/// to.
///
/// What is deliberately not refused here is a fractional number. Accepting one is the liberal half
/// of the convention: a float arriving from a foreign author is stored as the `F64` it is, and only
/// a position *declared* a float emits the decimal string the convention spells. Refusing an
/// undeclared one would make a field nobody described stricter than a field somebody did.
fn canonical(value: &Value, path: &Path<'_>, lexicon: bool) -> Result<(), CodecError> {
    match value {
        Value::Number(number) => {
            let outside = !lexicon
                && (number
                    .as_i64()
                    .is_some_and(|integer| !js_safe_integer(i128::from(integer)))
                    || number
                        .as_u64()
                        .is_some_and(|integer| integer > JS_SAFE_INTEGER_MAX as u64));
            if outside {
                return Err(at(
                    path,
                    CodecError::NotAJsonInteger {
                        stated: number.to_string(),
                    },
                ));
            }
            Ok(())
        }
        Value::Object(members) => {
            // The canonical bytes object is the one `$` shape an undeclared position may hold, and
            // holding it is what makes it that rather than an application's use of a reserved key.
            if members.len() == 1 && members.contains_key("$bytes") {
                if bytes_value(value).is_none() {
                    return Err(at(
                        path,
                        CodecError::NotBase64 {
                            stated: members["$bytes"].clone(),
                        },
                    ));
                }
                return Ok(());
            }
            for (key, member) in members {
                if !lexicon && key.starts_with("$") && !protocol_key(key) {
                    return Err(at(
                        &Path::key(path, key),
                        CodecError::ReservedKey { key: key.clone() },
                    ));
                }
                canonical(member, &Path::key(path, key), lexicon)?;
            }
            Ok(())
        }
        Value::Array(items) => items
            .iter()
            .enumerate()
            .try_for_each(|(index, item)| canonical(item, &Path::index(path, index), lexicon)),
        _ => Ok(()),
    }
}

/// A value checked against the declarations — and, when it was checked against a transaction's
/// base view, against the native storage — of the document it is going into.
///
/// A storage declaration is an assertion — a position declared as bytes holds base64, not a string
/// that failed to decode — so the value is checked once, at the edge where a bad one can still be
/// reported. Reconciliation afterwards reports only what the reconciler itself reports: the
/// conversions a declaration asked for are total, and the decisions a target-aware check recorded
/// are already made.
#[derive(Debug)]
pub struct Stored {
    schema: CompiledSchema,
    proof: ValidationProof,
    value: Value,
    /// The decisions the walk may consult while writing: none when this value was checked without a
    /// document, so reconciliation keeps the behavior of a fresh target throughout.
    plan: Option<Plan>,
}

impl Stored {
    /// Check a value against the declarations of the document it will be stored in.
    pub fn check(schema: &CompiledSchema, value: Value) -> Result<Self, CodecError> {
        let value = schema.validate(&value)?;
        let proof = schema.proof();
        let path = Path::root();
        Position::new(schema.document(), Some(schema.document())).check(&value, &path)?;
        Ok(Self {
            schema: schema.clone(),
            proof,
            value,
            plan: None,
        })
    }

    /// The value, as it was checked.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The value, checked.
    pub fn into_value(self) -> Value {
        self.value
    }

    /// The value a target-aware check produced. The plan tells the write what the base held at
    /// its positions, and it is not a public spelling on purpose: a caller that wants it checked
    /// against a base reconciles through `reconcile_json`.
    pub(super) fn with_plan(schema: CompiledSchema, value: Value, plan: Plan) -> Self {
        Self {
            proof: schema.proof(),
            schema,
            value,
            plan: Some(plan),
        }
    }

    pub(super) fn into_target_parts(self) -> (CompiledSchema, Value) {
        let Self {
            schema,
            proof: _proof,
            value,
            plan: None,
        } = self
        else {
            unreachable!("a newly checked value cannot already have a target plan");
        };
        (schema, value)
    }

    /// Write a target-aware checked value. This is private to the target-aware boundary; a plain
    /// `Stored::check` is validation evidence only and cannot be used as a no-plan write.
    pub(super) fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        assert!(
            self.plan.is_some(),
            "a target-aware Stored value needs a plan"
        );
        let path = Path::root();
        Position::new(self.schema.document(), Some(self.schema.document())).write(
            &self.value,
            &path,
            reconciler,
            self.plan.as_ref(),
        )
    }
}

pub(super) struct Planned<'a>(&'a Stored);

impl Stored {
    pub(super) fn planned(&self) -> Planned<'_> {
        Planned(self)
    }
}

impl Reconcile for Planned<'_> {
    type Key<'a> = NoKey;

    fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        self.0.reconcile(reconciler)
    }
}
/// Read a value out of a document as the JSON form its schema declares.
///
/// A document holds what the declarations say, so a storage form at a position nothing declares is
/// refused: reading a counter as a number, or bytes as a plain base64 string, hands back a value whose type
/// the writer did not intend.
pub fn hydrate_value<D: ReadDoc>(
    doc: &D,
    obj: &ObjId,
    prop: Prop<'_>,
    schema: &CompiledSchema,
) -> Result<Value, HydrateError> {
    let path = Path::root();
    Position::new(schema.document(), Some(schema.document())).read(doc, obj, prop, &path)
}

/// Where a position sits, as a chain that is rendered only when something is refused: reading or
/// writing a value does not allocate for the sake of an error that does not happen.
///
/// The same rendering is what a target-aware plan keys its decisions by, so both passes of a
/// reconciliation build the chain the same way and a plan's entries are found where they were
/// checked.
pub(super) struct Path<'p> {
    parent: Option<&'p Path<'p>>,
    step: Step<'p>,
}

enum Step<'p> {
    Root,
    Key(&'p str),
    Index(usize),
}

impl Path<'_> {
    pub(super) fn root() -> Path<'static> {
        Path {
            parent: None,
            step: Step::Root,
        }
    }
}

impl<'p> Path<'p> {
    /// The position's structural key, for a plan keyed by something a member's own name cannot
    /// collide with.
    pub(super) fn structural(&self) -> PathKey {
        let mut steps = Vec::new();
        for path in self.chain() {
            steps.push(match path.step {
                Step::Key(key) => PathStep::Key(key.to_owned()),
                Step::Index(index) => PathStep::Index(index),
                Step::Root => continue,
            });
        }
        steps.reverse();
        PathKey(steps)
    }

    /// The position and its ancestors, root first.
    fn chain(&self) -> Vec<&Path<'p>> {
        let mut chain = Vec::new();
        let mut next: Option<&Path<'p>> = Some(self);
        while let Some(path) = next {
            chain.push(path);
            next = path.parent;
        }
        chain.reverse();
        chain
    }

    pub(super) fn key(parent: &'p Path<'p>, key: &'p str) -> Self {
        Self {
            parent: Some(parent),
            step: Step::Key(key),
        }
    }

    pub(super) fn index(parent: &'p Path<'p>, index: usize) -> Self {
        Self {
            parent: Some(parent),
            step: Step::Index(index),
        }
    }
}

/// One step of a position's structural key.
#[derive(Debug, PartialEq, Eq, Hash)]
enum PathStep {
    Key(String),
    Index(usize),
}

/// A plan's key for a position: structural, and injective where a rendered chain is not — a
/// member may be named like one of its neighbors' positions (`a.b`, `a[0]`), so the entries are
/// keyed by the steps themselves rather than by how they render, and a decision checked at one
/// position is found only there. Rendering stays in `Path`'s `Display`, which only refusals pay
/// for.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct PathKey(Vec<PathStep>);

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

/// The kind of what sits at a position. Untagged alternatives such as `Option` narrow by kind;
/// tagged object alternatives also require their discriminant to match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
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

    pub(super) fn of_json(value: &Value) -> Self {
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
    pub(super) fn of_stored(stored: &automerge::Value) -> Self {
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
///
/// One interpreter serves every direction a value travels: `Stored::check` and `Stored::reconcile`
/// here, and the target-aware checks in `super::target` — resolution, tag dispatch and the codec's
/// conversions are shared, so the target-aware walk is a traversal and not a second decoder.
#[derive(Debug, Clone, Copy)]
pub(super) struct Position<'a> {
    document: &'a Value,
    /// Nothing when this position is not declared at all, which is what an unnamed member of an open
    /// object is: it holds plain JSON.
    schema: Option<&'a Value>,
}

impl<'a> Position<'a> {
    pub(super) fn new(document: &'a Value, schema: Option<&'a Value>) -> Self {
        Self { document, schema }
    }

    /// Follow references and narrow alternatives by kind and, for tagged members, discriminant.
    /// `member_matches` checks discriminants in JSON on writes and native map entries on reads.
    pub(super) fn resolved<E>(
        &self,
        kind: Kind,
        member_matches: impl Fn(&str, &Value) -> Result<bool, E>,
        error: impl Fn(CodecError) -> E,
    ) -> Result<Option<&'a Value>, E> {
        let Some(mut schema) = self.schema else {
            return Ok(None);
        };
        for _hop in 0..HOPS {
            schema = self.referenced(schema).map_err(&error)?;
            if let Some(branches) = any_of(schema) {
                let mut fit = None;
                for branch in branches {
                    let branch = self.referenced(branch).map_err(&error)?;
                    if !self.fits(branch, kind, 0).map_err(&error)? {
                        continue;
                    }
                    let mut matches = true;
                    if let Some(properties) = branch.get("properties").and_then(Value::as_object) {
                        for (key, declaration) in properties {
                            // These are the string tag constraints emitted by serde/schemars and
                            // our open fallback. Payload validation still belongs to the validator.
                            if (declaration.get("const").is_some_and(Value::is_string)
                                || declaration.pointer("/not/enum").is_some())
                                && !member_matches(key, declaration)?
                            {
                                matches = false;
                                break;
                            }
                        }
                    }
                    if matches {
                        fit = Some(branch);
                        break;
                    }
                }
                schema = fit.ok_or_else(|| error(CodecError::NoBranch { kind: kind.name() }))?;
                continue;
            }
            return Ok(Some(schema));
        }
        Err(error(CodecError::Unresolved {
            chain: "it refers back to itself".to_owned(),
        }))
    }

    /// A reference or unsupported composition must not hide a branch's discriminant or codec.
    fn referenced(&self, mut schema: &'a Value) -> Result<&'a Value, CodecError> {
        for _hop in 0..HOPS {
            if let Some(composed) = composed(schema) {
                return Err(CodecError::Composed { keyword: composed });
            }
            let Some(reference) = schema.get("$ref") else {
                return Ok(schema);
            };
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

    /// Whether an alternative accepts this kind, including nested wrappers such as `Option<Union>`.
    /// A leaf that states no type accepts any kind; references have already been followed.
    fn fits(&self, branch: &'a Value, kind: Kind, hops: usize) -> Result<bool, CodecError> {
        if hops >= HOPS {
            return Err(CodecError::Unresolved {
                chain: "it refers back to itself".to_owned(),
            });
        }
        // `Option<Union>` wraps the union in anyOf before its null branch. The wrapper has no
        // type of its own, but must not greedily accept null on behalf of its object members.
        if let Some(branches) = any_of(branch) {
            for branch in branches {
                if self.fits(self.referenced(branch)?, kind, hops + 1)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        match Codec::of(branch)? {
            // A position our keyword is on is spelled by the codec, and a float is the one whose
            // storage and JSON are of different kinds: the document holds the number and the
            // convention carries the string, so both spellings are the float branch.
            Some(Codec::Float | Codec::Uint | Codec::Datetime) => {
                Ok(matches!(kind, Kind::Number | Kind::String))
            }
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
    /// the schema `additionalProperties` or a keyed map states for the members it does not name.
    pub(super) fn member(&self, schema: Option<&'a Value>, key: &str) -> Position<'a> {
        Position {
            document: self.document,
            schema: schema.and_then(|schema| declared_member(schema, key)),
        }
    }

    /// The position of one element of an array.
    pub(super) fn element(&self, schema: Option<&'a Value>, index: usize) -> Position<'a> {
        Position {
            document: self.document,
            schema: schema.and_then(|schema| declared_element(schema, index)),
        }
    }

    /// Refuse a value the declarations here cannot hold. Nothing is written afterwards, so this is
    /// where a declaration turns into an assertion.
    pub(super) fn check<'p>(&self, value: &'p Value, path: &'p Path<'p>) -> Result<(), CodecError> {
        let lexicon = self.document.get(crate::facet::input::DATA) == Some(&Value::Bool(true));
        if !lexicon
            && let Value::Number(number) = value
            && (number
                .as_i64()
                .is_some_and(|integer| !js_safe_integer(i128::from(integer)))
                || number
                    .as_u64()
                    .is_some_and(|integer| integer > JS_SAFE_INTEGER_MAX as u64))
        {
            return Err(at(
                path,
                CodecError::NotAJsonInteger {
                    stated: number.to_string(),
                },
            ));
        }
        if let Value::Object(members) = value {
            for key in members.keys().filter(|key| key.starts_with("$")) {
                if self.document.get(crate::facet::input::DATA) != Some(&Value::Bool(true))
                    && !protocol_key(key)
                {
                    return Err(at(
                        &Path::key(path, key),
                        CodecError::ReservedKey { key: key.clone() },
                    ));
                }
            }
        }
        let Some(schema) = self
            .resolved(
                Kind::of_json(value),
                |key, declaration| {
                    Ok(discriminant_matches(
                        value.get(key).and_then(Value::as_str),
                        declaration,
                    ))
                },
                |err| err,
            )
            .map_err(|err| at(path, err))?
        else {
            // Nothing declares this position, which is not the same as nothing checking it: an
            // unknown member a peer wrote is still a value in this document, and the data model
            // reaches every depth of it.
            return canonical(value, path, lexicon);
        };
        if type_mentions(schema, "integer")
            && let Value::Number(number) = value
            && number.as_i64().is_none()
            && number.as_u64().is_none()
        {
            return Err(at(
                path,
                CodecError::NonIntegralInteger {
                    stated: number.to_string(),
                },
            ));
        }
        if let Some(codec) = Codec::of(schema).map_err(|err| at(path, err))? {
            // A `null` a declaration claims — the `None` of an option, whose `type` carries it —
            // belongs to the declaration and not to the codec, whose holds is schema-blind.
            if !(value.is_null() && type_mentions(schema, "null")) {
                codec.holds(value).map_err(|err| at(path, err))?;
            }
            return Ok(());
        }
        // A scalar that is not a number, at a position declared one, would store a value the Rust
        // type cannot read — the storage's only fidelity rule; the rest of a kind mismatch is the
        // validator's business. Reaching an object or array at such a position is the same drift.
        if (type_mentions(schema, "number") || type_mentions(schema, "integer"))
            && !(value.is_null() && type_mentions(schema, "null"))
            && !matches!(value, Value::Number(_))
        {
            return Err(at(path, CodecError::KindMismatch { found: kind(value) }));
        }
        match value {
            Value::Object(members) => {
                let properties = schema.get("properties").and_then(Value::as_object);
                for (key, member) in members {
                    let child = Path::key(path, key);
                    let declared = properties.and_then(|properties| properties.get(key));
                    if self.document.get(crate::facet::input::DATA) != Some(&Value::Bool(true))
                        && declared.is_none()
                        && key.starts_with("$")
                    {
                        return Err(at(
                            &child,
                            CodecError::ReservedKey {
                                key: key.to_owned(),
                            },
                        ));
                    }
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
    ///
    /// `plan` carries the decisions a target-aware check made against the base view the caller
    /// reconciles inside: a position it says something about neither creates nor changes anything,
    /// which is what keeps an unchanged native value's identity and its concurrent conflicts. The
    /// plan is consulted here — every position of the value passes through this once — rather than
    /// at each container, so a plan entry cannot drift out of sync with where it was checked.
    pub(super) fn write<'p, R: Reconciler>(
        &self,
        value: &'p Value,
        path: &'p Path<'p>,
        mut reconciler: R,
        plan: Option<&'p Plan>,
    ) -> Result<(), R::Error> {
        // A plan entry was made against the base and is written as is: a `Skip` writes nothing,
        // which is what "unchanged" says, so a text keeps its object and a counter keeps the
        // entries a concurrent writer added after the base.
        if let Some(op) = plan.and_then(|plan| plan.get(path)) {
            return op.write(reconciler);
        }
        let schema = self
            .resolved(
                Kind::of_json(value),
                |key, declaration| {
                    Ok(discriminant_matches(
                        value.get(key).and_then(Value::as_str),
                        declaration,
                    ))
                },
                |err| err,
            )
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
        if schema.is_none()
            && let Some(stated) = bytes_value(value)
        {
            return reconciler.bytes(stated);
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
                    let documented = Documented(position, item, &child, plan);
                    if index < existing {
                        sequence.set(index, documented)?;
                    } else {
                        sequence.insert(index, documented)?;
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
                    map.put(key, Documented(position, member, &child, plan))?;
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
        let schema = self.resolved(
            Kind::of_stored(&stored),
            |key, declaration| {
                let entry = doc.get(&id, key)?;
                Ok(discriminant_matches(
                    entry.as_ref().and_then(|(value, _id)| value.to_str()),
                    declaration,
                ))
            },
            |err| refusal(path, err),
        )?;
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
            automerge::Value::Scalar(scalar) => {
                let raw = raw_fallback(schema).map_err(|err| refusal(path, err))?;
                scalar_json(
                    &scalar,
                    path,
                    raw,
                    self.document.get(crate::facet::input::DATA) == Some(&Value::Bool(true)),
                )
            }
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
/// A position on its way into a document: what a reconciler's own sub-reconcilers take. The plan a
/// value was checked with rides along, because a sub-reconciler only receives a `Reconcile`.
struct Documented<'p>(Position<'p>, &'p Value, &'p Path<'p>, Option<&'p Plan>);

impl Reconcile for Documented<'_> {
    type Key<'a> = NoKey;

    fn reconcile<R: Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        let Self(position, value, path, plan) = self;
        position.write(value, path, reconciler, *plan)
    }
}

/// What a schema says a number is, which is what a document holds at that position.
pub(super) enum Number {
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
pub(super) fn number_kind(schema: Option<&Value>) -> Number {
    let Some(schema) = schema else {
        return Number::Token;
    };
    // The keyword's own marker names a plain number on purpose, `type` or not.
    if schema.get(KEYWORD).and_then(Value::as_str) == Some("number") {
        return Number::Float;
    }
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

/// What a position declares, one spelling for one concept: the storage form's codec, an ordinary
/// kind stated on purpose, or genuinely nothing at all. Every check consults this, so a
/// deliberately plain position is never mistaken for an undeclared one, and the other way around:
/// a marker the keyword carries without a `type` is a statement, not an absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Declared {
    Codec(Codec),
    /// A kind this position states itself: plain markers (`number`, `list`) included, with or
    /// without a `type`.
    Plain,
    Undeclared,
}

/// What the schema at this position declares.
pub(super) fn declared(schema: Option<&Value>) -> Result<Declared, CodecError> {
    let Some(schema) = schema else {
        return Ok(Declared::Undeclared);
    };
    match Codec::of(schema)? {
        Some(codec) => Ok(Declared::Codec(codec)),
        None => {
            let stated = (schema.get(KEYWORD).is_some_and(Value::is_string))
                || [
                    "string", "number", "integer", "boolean", "array", "object", "null",
                ]
                .iter()
                .any(|kind| type_mentions(schema, kind));
            Ok(if stated {
                Declared::Plain
            } else {
                Declared::Undeclared
            })
        }
    }
}

/// Whether a schema's `type` states this, written as one type or as a list of them: an
/// `Option<i64>` is `{"type": ["integer", "null"]}`.
pub(super) fn type_mentions(schema: &Value, stated: &str) -> bool {
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

/// String discriminants select known members by const; an open fallback excludes those constants.
pub(super) fn discriminant_matches(stated: Option<&str>, declaration: &Value) -> bool {
    let Some(stated) = stated else {
        return false;
    };
    if let Some(known) = declaration.get("const").and_then(Value::as_str)
        && stated != known
    {
        return false;
    }
    !declaration
        .pointer("/not/enum")
        .and_then(Value::as_array)
        .is_some_and(|known| known.iter().any(|value| value.as_str() == Some(stated)))
}

fn any_of(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Value::as_array)
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
pub(super) fn at(path: &Path<'_>, source: CodecError) -> CodecError {
    CodecError::At {
        path: path.to_string(),
        source: Box::new(source),
    }
}

/// The same refusal, said to a reader.
pub(super) fn refusal(path: &Path<'_>, source: CodecError) -> HydrateError {
    HydrateError::unexpected(
        "a value its declarations hold",
        format!("at `{path}`: {source}"),
    )
}

/// Whether a position declares nothing about what sits in it. There, and only there, the storage
/// forms a peer may have written without a schema travel to the read the best-effort raw spellings
/// the fallback spells them in — bytes as the canonical `$bytes` object they are, an instant as its RFC 3339, a
/// counter as the number it summed to. The spellings are a fallback, and not the signed fidelity a
/// declaration gives: where a schema declares, everything it says is authoritative, and a mismatch
/// is a refusal rather than a projection.
pub(super) fn raw_fallback(schema: Option<&Value>) -> Result<bool, CodecError> {
    Ok(declared(schema)? == Declared::Undeclared)
}

/// A stored scalar as the JSON form a reader gets back. Where a declaration says what sits here, a
/// storage form other than that is refused: a counter read as a plain number, or bytes read as a
/// string, is a value whose type the writer intended to be read differently. Where nothing is
/// declared (`raw_fallback`), the scalars Automerge holds are what the raw spellings carry.
fn scalar_json(
    scalar: &ScalarValue,
    path: &Path<'_>,
    raw: bool,
    lexicon: bool,
) -> Result<Value, HydrateError> {
    let undeclared = |held: &'static str| refusal(path, CodecError::Undeclared { held });
    match scalar {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(stated) => Ok(Value::Bool(*stated)),
        ScalarValue::Int(stated) if lexicon || js_safe_integer(i128::from(*stated)) => {
            Ok(Value::Number((*stated).into()))
        }
        ScalarValue::Int(stated) => Err(refusal(
            path,
            CodecError::NotAJsonInteger {
                stated: stated.to_string(),
            },
        )),
        ScalarValue::Uint(stated)
            if (lexicon && *stated <= i64::MAX as u64) || *stated <= JS_SAFE_INTEGER_MAX as u64 =>
        {
            Ok(Value::Number((*stated).into()))
        }
        ScalarValue::Uint(stated) => Err(refusal(
            path,
            CodecError::NotAJsonInteger {
                stated: stated.to_string(),
            },
        )),
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
        // The raw fallback, and nothing else: a counter is read as the number it summed to, its
        // bytes as the canonical `$bytes` object they spell, an instant as its RFC 3339. A scalar from a
        // future Automerge has no spelling at all and is refused wherever it sits.
        ScalarValue::Counter(stated)
            if raw && i64::from(stated).unsigned_abs() <= JS_SAFE_INTEGER_MAX as u64 =>
        {
            Ok(Value::Number(i64::from(stated).into()))
        }
        ScalarValue::Counter(stated) if raw => Err(refusal(
            path,
            CodecError::NotAJsonInteger {
                stated: i64::from(stated).to_string(),
            },
        )),
        ScalarValue::Timestamp(stated) if raw => Ok(Value::String(instant_string(*stated))),
        ScalarValue::Bytes(stated) if raw => {
            Ok(serde_json::json!({"$bytes": data_encoding::BASE64.encode(stated)}))
        }
        ScalarValue::Counter(_) => Err(undeclared("a counter")),
        ScalarValue::Timestamp(_) => Err(undeclared("an instant")),
        ScalarValue::Bytes(_) => Err(undeclared("bytes")),
        ScalarValue::Unknown { .. } => Err(undeclared("a scalar this build does not know")),
    }
}
