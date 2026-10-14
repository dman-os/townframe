//! Both directions of the codec: a JSON value into a document, and a document back into JSON.
//!
//! The declaration is read at the position it was written at, so both directions walk the value and
//! the schema together: a `$ref` resolves in the document's definitions, a union narrows to the
//! branch the content fits, and a member the schema does not name is plain JSON — which is what
//! makes an open object open.

use super::{Codec, CodecError, KEYWORD, bytes_value, instant_string, kind};
use crate::facet::codec::target::Plan;
use automerge::{ObjId, ObjType, ReadDoc, ScalarValue};
use autosurgeon::reconcile::{MapReconciler, NoKey, SeqReconciler};
use autosurgeon::{HydrateError, Prop, Reconcile, Reconciler};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::Arc;

/// How many reference and branch hops a position may take before its schema is a loop rather than a
/// declaration.
const HOPS: usize = 16;
/// Maximum heap work units for one submitted value. The iterative normalizer never recurses through
/// sibling entries; wide ordinary values therefore consume linear work without consuming call stack.
const NORMALIZATION_STEPS: usize = 1_000_000;
/// Maximum authored schema/value nesting depth; exceeding it is a structured refusal, not a stack overflow.
const NORMALIZATION_DEPTH: usize = 1024;

/// Canonical JSON integers remain exactly representable in JavaScript consumers.
const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

type BranchValidators = HashMap<usize, Arc<jsonschema::Validator>>;

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    document: Arc<Value>,
    validator: Arc<jsonschema::Validator>,
    branch_validators: Arc<BranchValidators>,
}

#[derive(Debug, Clone, Copy)]
struct CarriedSchemaRetriever;

// This proof is private so callers can only obtain it from an immutable compiled handle after
// validation; it is carried into Stored without validating the submitted value a second time.
#[derive(Debug, Clone)]
struct ValidationProof {
    validator: Arc<jsonschema::Validator>,
}

impl jsonschema::Retrieve for CarriedSchemaRetriever {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("external schema reference is not carried: {uri}").into())
    }
}

impl CompiledSchema {
    /// Compile one carried Bundle schema for reuse by checks and target writes.
    pub fn compile(document: Value) -> Result<Self, CodecError> {
        reject_unsupported_schema_keywords(&document)?;
        let validator = jsonschema::options()
            .with_retriever(CarriedSchemaRetriever)
            .build(&document)
            .map_err(|error| CodecError::InvalidSchema {
                message: error.to_string(),
            })?;
        let branch_validators = compile_branch_validators(&document)?;
        Ok(Self {
            document: Arc::new(document),
            validator: Arc::new(validator),
            branch_validators: Arc::new(branch_validators),
        })
    }

    /// The immutable carried schema document.
    pub fn document(&self) -> &Value {
        &self.document
    }

    fn proof(&self) -> ValidationProof {
        ValidationProof {
            validator: Arc::clone(&self.validator),
        }
    }

    fn validate(&self, value: &Value) -> Result<Value, CodecError> {
        let proof = self.proof();
        enforce_value_resource_bound(value)?;
        // Canonical values need no authoring walk at all. The resource preflight above applies the
        // same depth/work policy before this wide-map/list fast path and before cloning the value.
        if proof.validator.is_valid(value) {
            return Ok(value.clone());
        }
        let mut budget = NormalizationBudget { work: 0 };
        let Some(normalized) = normalize_authoring(
            &self.document,
            &self.branch_validators,
            &self.document,
            value,
            &mut budget,
        )?
        else {
            return Err(CodecError::NormalizationAmbiguous);
        };
        if let Some(error) = proof.validator.iter_errors(&normalized).next() {
            return Err(CodecError::SchemaViolation {
                path: error.instance_path().to_string(),
                message: error.to_string(),
            });
        }
        Ok(normalized)
    }
}

fn enforce_value_resource_bound(value: &Value) -> Result<(), CodecError> {
    let mut pending = vec![(value, 0usize)];
    let mut work = 0usize;
    while let Some((value, depth)) = pending.pop() {
        work += 1;
        if work > NORMALIZATION_STEPS {
            return Err(CodecError::NormalizationLimit {
                limit: NORMALIZATION_STEPS,
            });
        }
        if depth > NORMALIZATION_DEPTH {
            return Err(CodecError::NormalizationLimit {
                limit: NORMALIZATION_DEPTH,
            });
        }
        match value {
            Value::Object(members) => {
                pending.extend(members.values().map(|value| (value, depth + 1)))
            }
            Value::Array(items) => pending.extend(items.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

fn compile_branch_validators(document: &Value) -> Result<BranchValidators, CodecError> {
    let definitions = document.get("$defs").cloned();
    let mut pending = vec![document];
    let mut validators = HashMap::new();
    while let Some(schema) = pending.pop() {
        let Some(object) = schema.as_object() else {
            if let Some(items) = schema.as_array() {
                pending.extend(items);
            }
            continue;
        };
        for (keyword, value) in object {
            if matches!(keyword.as_str(), "anyOf" | "oneOf") {
                if let Some(branches) = value.as_array() {
                    for branch in branches {
                        let mut branch_document = branch.clone();
                        if let (
                            Some(Value::Object(root_definitions)),
                            Value::Object(branch_object),
                        ) = (definitions.as_ref(), &mut branch_document)
                        {
                            let branch_definitions = branch_object
                                .entry("$defs")
                                .or_insert_with(|| Value::Object(serde_json::Map::new()));
                            if let Value::Object(branch_definitions) = branch_definitions {
                                for (name, definition) in root_definitions {
                                    branch_definitions
                                        .entry(name.clone())
                                        .or_insert_with(|| definition.clone());
                                }
                            }
                        }
                        let validator = jsonschema::options()
                            .with_retriever(CarriedSchemaRetriever)
                            .build(&branch_document)
                            .map_err(|error| CodecError::InvalidSchema {
                                message: error.to_string(),
                            })?;
                        validators.insert(branch as *const Value as usize, Arc::new(validator));
                        pending.push(branch);
                    }
                }
            } else {
                pending.push(value);
            }
        }
    }
    Ok(validators)
}

fn reject_unsupported_schema_keywords(schema: &Value) -> Result<(), CodecError> {
    match schema {
        Value::Object(object) => {
            if object.contains_key("prefixItems") || object.contains_key("allOf") {
                return Err(CodecError::InvalidSchema {
                    message: "the carried JSON Schema uses an unsupported composition keyword (`allOf` or `prefixItems`)".to_owned(),
                });
            }
            if object.contains_key("anyOf") && object.contains_key("oneOf") {
                return Err(CodecError::InvalidSchema {
                    message: "the carried JSON Schema uses both `anyOf` and `oneOf` at one position, which this codec cannot dispatch without composing branch candidates".to_owned(),
                });
            }
            for child in object.values() {
                reject_unsupported_schema_keywords(child)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                reject_unsupported_schema_keywords(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

enum NormalizationTask<'a> {
    Visit {
        schema: &'a Value,
        value: Value,
        hops: usize,
        depth: usize,
    },
    Object {
        schema: &'a Value,
        entries: serde_json::map::IntoIter,
        out: serde_json::Map<String, Value>,
        pending: Option<String>,
        hops: usize,
        depth: usize,
    },
    Array {
        schema: &'a Value,
        items: std::vec::IntoIter<Value>,
        out: Vec<Value>,
        pending: bool,
        hops: usize,
        depth: usize,
    },
    Branch {
        branches: &'a [Value],
        index: usize,
        value: Value,
        selected: Option<Value>,
        ambiguous: bool,
        pending: Option<usize>,
        hops: usize,
        depth: usize,
    },
}

struct NormalizationBudget {
    work: usize,
}

impl NormalizationBudget {
    fn consume(&mut self, limit: usize) -> Result<(), CodecError> {
        self.work += 1;
        if self.work > limit {
            return Err(CodecError::NormalizationLimit { limit });
        }
        Ok(())
    }
}

fn normalize_authoring(
    document: &Value,
    branch_validators: &BranchValidators,
    schema: &Value,
    value: &Value,
    budget: &mut NormalizationBudget,
) -> Result<Option<Value>, CodecError> {
    let mut tasks = vec![NormalizationTask::Visit {
        schema,
        value: value.clone(),
        hops: HOPS,
        depth: 0,
    }];
    let mut results: Vec<Option<Value>> = Vec::new();

    while let Some(task) = tasks.pop() {
        budget.consume(NORMALIZATION_STEPS)?;
        match task {
            NormalizationTask::Visit {
                schema,
                value,
                hops,
                depth,
            } => {
                if depth > NORMALIZATION_DEPTH || hops == 0 {
                    return Err(CodecError::NormalizationLimit {
                        limit: NORMALIZATION_DEPTH,
                    });
                }
                let schema = resolve_schema(document, schema, hops)?;
                if let Some(branches) = schema
                    .get("anyOf")
                    .or_else(|| schema.get("oneOf"))
                    .and_then(Value::as_array)
                {
                    tasks.push(NormalizationTask::Branch {
                        branches,
                        index: 0,
                        value,
                        selected: None,
                        ambiguous: false,
                        pending: None,
                        hops,
                        depth,
                    });
                    continue;
                }
                if schema.get(KEYWORD).and_then(Value::as_str) == Some("float")
                    && let Some(number) = value.as_f64()
                {
                    let at_f32 = schema.get("format").and_then(Value::as_str) == Some("float");
                    results.push(Some(Value::String(super::float_string(number, at_f32))));
                    continue;
                }
                match value {
                    Value::Object(members) => {
                        tasks.push(NormalizationTask::Object {
                            schema,
                            entries: members.into_iter(),
                            out: serde_json::Map::new(),
                            pending: None,
                            hops,
                            depth: depth + 1,
                        });
                    }
                    Value::Array(items) => {
                        tasks.push(NormalizationTask::Array {
                            schema,
                            items: items.into_iter(),
                            out: Vec::new(),
                            pending: false,
                            hops,
                            depth: depth + 1,
                        });
                    }
                    scalar => results.push(Some(scalar)),
                }
            }
            NormalizationTask::Object {
                schema,
                mut entries,
                mut out,
                mut pending,
                hops,
                depth,
            } => {
                if let Some(key) = pending.take() {
                    let child = results.pop().expect("child normalization result");
                    let Some(child) = child else {
                        results.push(None);
                        continue;
                    };
                    out.insert(key, child);
                }
                if let Some((key, value)) = entries.next() {
                    let member_schema = schema
                        .get("properties")
                        .and_then(Value::as_object)
                        .and_then(|properties| properties.get(&key))
                        .or_else(|| {
                            schema
                                .get("additionalProperties")
                                .filter(|schema| schema.is_object())
                        });
                    if let Some(member_schema) = member_schema {
                        pending = Some(key);
                        tasks.push(NormalizationTask::Object {
                            schema,
                            entries,
                            out,
                            pending,
                            hops,
                            depth,
                        });
                        tasks.push(NormalizationTask::Visit {
                            schema: member_schema,
                            value,
                            hops: hops - 1,
                            depth,
                        });
                    } else {
                        out.insert(key, value);
                        tasks.push(NormalizationTask::Object {
                            schema,
                            entries,
                            out,
                            pending,
                            hops,
                            depth,
                        });
                    }
                } else {
                    results.push(Some(Value::Object(out)));
                }
            }
            NormalizationTask::Array {
                schema,
                mut items,
                mut out,
                pending,
                hops,
                depth,
            } => {
                if pending {
                    let child = results.pop().expect("child normalization result");
                    let Some(child) = child else {
                        results.push(None);
                        continue;
                    };
                    out.push(child);
                }
                if let Some(value) = items.next() {
                    let item_schema = schema.get("items");
                    if let Some(item_schema) = item_schema {
                        tasks.push(NormalizationTask::Array {
                            schema,
                            items,
                            out,
                            pending: true,
                            hops,
                            depth,
                        });
                        tasks.push(NormalizationTask::Visit {
                            schema: item_schema,
                            value,
                            hops: hops - 1,
                            depth,
                        });
                    } else {
                        out.push(value);
                        tasks.push(NormalizationTask::Array {
                            schema,
                            items,
                            out,
                            pending: false,
                            hops,
                            depth,
                        });
                    }
                } else {
                    results.push(Some(Value::Array(out)));
                }
            }
            NormalizationTask::Branch {
                branches,
                mut index,
                value,
                mut selected,
                mut ambiguous,
                mut pending,
                hops,
                depth,
            } => {
                if let Some(branch_index) = pending.take() {
                    let candidate = results.pop().expect("branch normalization result");
                    let Some(candidate) = candidate else {
                        ambiguous = true;
                        tasks.push(NormalizationTask::Branch {
                            branches,
                            index,
                            value,
                            selected,
                            ambiguous,
                            pending,
                            hops,
                            depth,
                        });
                        continue;
                    };
                    let branch = &branches[branch_index];
                    let validator = branch_validators
                        .get(&(branch as *const Value as usize))
                        .expect("compiled branch validator");
                    if validator.is_valid(&candidate) {
                        if selected.is_some() {
                            ambiguous = true;
                        } else {
                            selected = Some(candidate);
                        }
                    }
                }
                if index < branches.len() {
                    let branch_index = index;
                    index += 1;
                    let branch = &branches[branch_index];
                    let resolved = resolve_schema(document, branch, hops)?;
                    if !authoring_matches(resolved, &value) {
                        tasks.push(NormalizationTask::Branch {
                            branches,
                            index,
                            value,
                            selected,
                            ambiguous,
                            pending,
                            hops,
                            depth,
                        });
                        continue;
                    }
                    budget.consume(NORMALIZATION_STEPS)?;
                    tasks.push(NormalizationTask::Branch {
                        branches,
                        index,
                        value: value.clone(),
                        selected,
                        ambiguous,
                        pending: Some(branch_index),
                        hops,
                        depth,
                    });
                    tasks.push(NormalizationTask::Visit {
                        schema: resolved,
                        value: value.clone(),
                        hops: hops - 1,
                        depth,
                    });
                } else {
                    results.push(if ambiguous { None } else { selected });
                }
            }
        }
        if tasks.is_empty() {
            return Ok(results.pop().flatten());
        }
    }
    Ok(results.pop().flatten())
}

fn resolve_schema<'a>(
    document: &'a Value,
    mut schema: &'a Value,
    mut hops: usize,
) -> Result<&'a Value, CodecError> {
    while let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if hops == 0 {
            return Err(CodecError::Unresolved {
                chain: "schema reference depth exceeded".to_owned(),
            });
        }
        let Some(name) = reference.strip_prefix("#/$defs/") else {
            return Ok(schema);
        };
        let Some(definition) = document.get("$defs").and_then(|defs| defs.get(name)) else {
            return Ok(schema);
        };
        schema = definition;
        hops -= 1;
    }
    Ok(schema)
}

fn authoring_matches(schema: &Value, value: &Value) -> bool {
    if schema.get(KEYWORD).and_then(Value::as_str) == Some("float") && value.is_number() {
        return true;
    }
    let Some(kind) = schema.get("type") else {
        return schema.get("properties").is_some_and(|_| value.is_object())
            || schema
                .get("additionalProperties")
                .is_some_and(|_| value.is_object())
            || schema.get("items").is_some_and(|_| value.is_array())
            || schema.get("const").is_some()
            || schema.get("enum").is_some()
            || schema.get("anyOf").is_some()
            || schema.get("oneOf").is_some();
    };
    let matches = |kind: &str| match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        _ => false,
    };
    match kind {
        Value::String(kind) => matches(kind),
        Value::Array(kinds) => kinds.iter().filter_map(Value::as_str).any(matches),
        _ => false,
    }
}

fn js_safe_integer(value: i128) -> bool {
    (-i128::from(JS_SAFE_INTEGER_MAX)..=i128::from(JS_SAFE_INTEGER_MAX)).contains(&value)
}

fn protocol_key(key: &str) -> bool {
    matches!(key, "$type" | "$bytes" | "$link")
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
        if let Value::Number(number) = value
            && (number
                .as_i64()
                .is_some_and(|integer| !js_safe_integer(i128::from(integer)))
                || number
                    .as_u64()
                    .is_some_and(|integer| integer > JS_SAFE_INTEGER_MAX))
        {
            return Err(at(
                path,
                CodecError::NotAJsonInteger {
                    stated: number.to_string(),
                },
            ));
        }
        if let Value::Object(members) = value {
            for key in members.keys().filter(|key| key.starts_with('$')) {
                if !protocol_key(key) {
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
            if let Value::Object(members) = value {
                if members.len() == 1
                    && members.contains_key("$bytes")
                    && bytes_value(value).is_some()
                {
                    return Ok(());
                }
                if let Some(key) = members.keys().find(|key| key.starts_with('$')) {
                    return Err(at(
                        &Path::key(path, key),
                        CodecError::ReservedKey {
                            key: key.to_owned(),
                        },
                    ));
                }
            }
            return Ok(());
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
                    if declared.is_none() && key.starts_with('$') {
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
                scalar_json(&scalar, path, raw)
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
fn scalar_json(scalar: &ScalarValue, path: &Path<'_>, raw: bool) -> Result<Value, HydrateError> {
    let undeclared = |held: &'static str| refusal(path, CodecError::Undeclared { held });
    match scalar {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(stated) => Ok(Value::Bool(*stated)),
        ScalarValue::Int(stated) if js_safe_integer(i128::from(*stated)) => {
            Ok(Value::Number((*stated).into()))
        }
        ScalarValue::Int(stated) => Err(refusal(
            path,
            CodecError::NotAJsonInteger {
                stated: stated.to_string(),
            },
        )),
        ScalarValue::Uint(stated) if *stated <= JS_SAFE_INTEGER_MAX => {
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
            if raw && i64::from(stated).unsigned_abs() <= JS_SAFE_INTEGER_MAX =>
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
