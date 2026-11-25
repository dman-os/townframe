//! Schema compilation, validation, and bounded authoring normalization.
//!
//! The declaration is read at the position it was written at, so both directions walk the value and
//! the schema together: a `$ref` resolves in the document's definitions, a union narrows to the
//! branch the content fits, and a member the schema does not name is plain JSON — which is what
//! makes an open object open.

use super::Codec;
use super::CodecError;
use super::KEYWORD;
use super::declared_member;
use super::keyed_map;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// How many reference and branch hops a position may take before its schema is a loop rather than a
/// declaration.
const HOPS: usize = 16;
/// Maximum heap work units for one submitted value. The iterative normalizer never recurses through
/// sibling entries; wide ordinary values therefore consume linear work without consuming call stack.
const NORMALIZATION_STEPS: usize = 1_000_000;
/// Maximum authored schema/value nesting depth; exceeding it is a structured refusal, not a stack overflow.
const NORMALIZATION_DEPTH: usize = 1024;

type BranchValidators = HashMap<String, Arc<jsonschema::Validator>>;

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    document: Arc<Value>,
    validator: Arc<jsonschema::Validator>,
    branch_validators: Arc<BranchValidators>,
    /// Whether the value can hold a reference; see [`Self::declares_references`].
    declares_references: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::datum) struct CarriedSchemaRetriever;

// This proof is private so callers can only obtain it from an immutable compiled handle after
// validation; it is carried into Stored without validating the submitted value a second time.
#[derive(Debug, Clone)]
pub(super) struct ValidationProof {
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
        Self::check_storage_declarations(&document)?;
        let validator = crate::datum::input::options()
            .with_retriever(CarriedSchemaRetriever)
            .build(&document)
            .map_err(|error| CodecError::InvalidSchema {
                message: error.to_string(),
            })?;
        let branch_validators = compile_branch_validators(&document)?;
        let declares_references = reaches_reference(&document);
        Ok(Self {
            document: Arc::new(document),
            validator: Arc::new(validator),
            branch_validators: Arc::new(branch_validators),
            declares_references,
        })
    }

    /// The immutable carried schema document.
    pub fn document(&self) -> &Value {
        &self.document
    }

    /// The schema one member of this document is declared by, compiled on its own, so a value
    /// written into that member alone is checked and reconciled exactly as it would be in place.
    ///
    /// `path` names object members from the root; every member of a keyed map shares one
    /// declaration, so any key names it. The member keeps this document's definitions and Lexicon
    /// data marker, so references inside it resolve as before and its values check under the same
    /// data model.
    pub fn member(&self, path: &[&str]) -> Result<Self, CodecError> {
        let mut schema = resolve_schema(&self.document, &self.document, HOPS)?;
        for key in path {
            let declared = declared_member(schema, key).ok_or_else(|| CodecError::Undefined {
                name: format!("member `{key}`"),
            })?;
            schema = resolve_schema(&self.document, declared, HOPS)?;
        }
        let mut document = schema
            .as_object()
            .expect("a resolved member declaration is an object")
            .clone();
        for keyword in ["$schema", "$defs", crate::datum::input::DATA] {
            if let Some(carried) = self.document.get(keyword) {
                document.insert(keyword.to_owned(), carried.clone());
            }
        }
        Self::compile(Value::Object(document))
    }

    /// Whether any position this document's value can reach refers to one of the reference defs
    /// (`#datumRef`, `#datumLocation`, `#datumPin`). A consumer walks a value for its references
    /// only when this says there are any to find.
    ///
    /// Reachability is followed from the root through `$ref`: every carried Lexicon compiles into
    /// `$defs`, the reference defs among them, so a def merely being present says nothing about
    /// what this datum holds.
    pub fn declares_references(&self) -> bool {
        self.declares_references
    }

    pub(super) fn proof(&self) -> ValidationProof {
        ValidationProof {
            validator: Arc::clone(&self.validator),
        }
    }

    pub(super) fn validate(&self, value: &Value) -> Result<Value, CodecError> {
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

/// Whether a position reachable from `document`'s root through `$ref` refers to a reference def,
/// computed once at compile time: an indexer asks for every datum it visits.
fn reaches_reference(document: &Value) -> bool {
    let defs = document.get("$defs").and_then(Value::as_object);
    let mut reached = std::collections::HashSet::new();
    let mut pending = vec![document];
    while let Some(entry) = pending.pop() {
        for (_, schema) in schema_positions(entry, false) {
            if super::references::ReferenceForm::of(schema).is_some() {
                return true;
            }
            if let Some(key) = schema
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
                && reached.insert(key)
                && let Some(definition) = defs.and_then(|defs| defs.get(key))
            {
                pending.push(definition);
            }
        }
    }
    false
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

/// One validator per `anyOf`/`oneOf` branch, so the codec can ask which branch a candidate fits.
///
/// The root document is registered once, in a registry every branch validator shares, and each
/// branch is compiled as a `$ref` to its JSON Pointer in it. A branch therefore resolves its
/// `#/$defs/..` references against the registered root, and the root's definitions are neither
/// copied into every branch nor re-processed by every build, which is what made compiling a large
/// Lexicon's branches cost a full registry build per branch.
fn compile_branch_validators(document: &Value) -> Result<BranchValidators, CodecError> {
    /// The registered root's URI; the document carries no `$id` of its own to take its place.
    const ROOT: &str = "urn:carried-schema:root";
    let invalid = |error: &dyn std::fmt::Display| CodecError::InvalidSchema {
        message: error.to_string(),
    };
    let mut validators = HashMap::new();
    let mut registry = None;
    for (pointer, schema) in schema_positions(document, true) {
        let Some(object) = schema.as_object() else {
            continue;
        };
        for keyword in ["anyOf", "oneOf"] {
            let Some(branches) = object.get(keyword).and_then(Value::as_array) else {
                continue;
            };
            for (index, branch) in branches.iter().enumerate() {
                let key = serde_json::to_string(branch).expect("a schema branch serializes");
                // A branch repeated verbatim resolves the same wherever it sits.
                if validators.contains_key(&key) {
                    continue;
                }
                if registry.is_none() {
                    registry = Some(
                        jsonschema::Registry::try_new(
                            ROOT,
                            jsonschema::Resource::from_contents(document.clone()),
                        )
                        .map_err(|error| invalid(&error))?,
                    );
                }
                let reference = serde_json::json!({
                    "$ref": format!("{ROOT}#{}", fragment(&format!("{pointer}/{keyword}/{index}"))),
                });
                let validator = crate::datum::input::options()
                    .with_retriever(CarriedSchemaRetriever)
                    .with_registry(registry.clone().expect("registered above"))
                    .build(&reference)
                    .map_err(|error| invalid(&error))?;
                validators.insert(key, Arc::new(validator));
            }
        }
    }
    Ok(validators)
}

/// A JSON Pointer as a URI fragment: every byte a fragment does not allow is percent-encoded.
fn fragment(pointer: &str) -> String {
    let mut encoded = String::with_capacity(pointer.len());
    for byte in pointer.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

impl CompiledSchema {
    pub(in crate::datum) fn check_storage_declarations(document: &Value) -> Result<(), CodecError> {
        for (_, schema) in schema_positions(document, true) {
            let Some(object) = schema.as_object() else {
                continue;
            };
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
            keyed_map(object).map_err(|reason| CodecError::InvalidSchema {
                message: format!(
                    "the carried JSON Schema is not a keyed map this codec can store: {reason}"
                ),
            })?;
            let Some(codec) = Codec::of(schema)? else {
                continue;
            };
            if ["$ref", "anyOf", "oneOf"]
                .iter()
                .any(|keyword| object.contains_key(*keyword))
            {
                return Err(CodecError::InvalidSchema {
                    message: format!(
                        "the {} storage declaration sits beside a reference or branches that the codec resolves before reading the declaration",
                        codec.name()
                    ),
                });
            }
            let expected = match codec {
                Codec::Bytes => "object",
                Codec::Timestamp | Codec::Float | Codec::Text | Codec::Uint | Codec::Datetime => {
                    "string"
                }
                Codec::Counter => "integer",
            };
            if let Some(stated) = object.get("type") {
                let agrees = match stated {
                    Value::String(kind) => kind == expected,
                    Value::Array(kinds) => {
                        kinds.iter().any(|kind| kind.as_str() == Some(expected))
                            && kinds.iter().all(|kind| {
                                kind.as_str() == Some(expected) || kind.as_str() == Some("null")
                            })
                    }
                    _ => false,
                };
                if !agrees {
                    return Err(CodecError::InvalidSchema {
                        message: format!(
                            "the {} storage declaration requires JSON type `{expected}`, not {stated}",
                            codec.name()
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Every schema in a document, reached through schema positions only.
///
/// A keyword's value is either a schema or instance data, and the two must not be walked alike:
/// `const`, `enum`, `default` and `examples` hold *values*, so a default that happens to contain a
/// key named `allOf` states an author's data and is not a composition this codec refuses to walk.
/// Reading data as schema rejects a legitimate literal, and reading it the other way round would
/// miss a composition nested under one of these keys' siblings.
///
/// `follow_defs` says whether the schemas in `$defs` are positions of this document, or only
/// definitions it may refer to.
fn schema_positions(document: &Value, follow_defs: bool) -> Vec<(String, &Value)> {
    /// One schema, whatever it applies to.
    const SINGLE: [&str; 10] = [
        "items",
        "contains",
        "not",
        "if",
        "then",
        "else",
        "propertyNames",
        "unevaluatedItems",
        "additionalProperties",
        "unevaluatedProperties",
    ];
    /// A list of schemas.
    const LIST: [&str; 4] = ["allOf", "anyOf", "oneOf", "prefixItems"];
    /// A map of names to schemas.
    const MAP: [&str; 5] = [
        "$defs",
        "definitions",
        "properties",
        "patternProperties",
        "dependentSchemas",
    ];

    /// Appends one JSON Pointer segment, escaped per RFC 6901.
    fn child(pointer: &str, segment: &str) -> String {
        format!(
            "{pointer}/{}",
            segment.replace('~', "~0").replace('/', "~1")
        )
    }

    let mut found = Vec::new();
    let mut pending = vec![(String::new(), document)];
    while let Some((pointer, schema)) = pending.pop() {
        found.push((pointer.clone(), schema));
        let Some(object) = schema.as_object() else {
            // A boolean schema has no positions inside it, and any other value got here as one.
            continue;
        };
        for (keyword, value) in object {
            let keyword = keyword.as_str();
            if SINGLE.contains(&keyword) {
                pending.push((child(&pointer, keyword), value));
            } else if LIST.contains(&keyword) {
                if let Some(schemas) = value.as_array() {
                    let list = child(&pointer, keyword);
                    pending.extend(
                        schemas
                            .iter()
                            .enumerate()
                            .map(|(index, schema)| (format!("{list}/{index}"), schema)),
                    );
                }
            } else if MAP.contains(&keyword)
                && (follow_defs || keyword != "$defs")
                && let Some(schemas) = value.as_object()
            {
                let map = child(&pointer, keyword);
                pending.extend(
                    schemas
                        .iter()
                        .map(|(name, schema)| (child(&map, name), schema)),
                );
            }
        }
    }
    found
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
    let mut normalization = Normalization {
        document,
        branch_validators,
        budget,
        tasks: vec![NormalizationTask::Visit {
            schema,
            value: value.clone(),
            hops: HOPS,
            depth: 0,
        }],
        results: Vec::new(),
    };
    while let Some(task) = normalization.tasks.pop() {
        normalization.budget.consume(NORMALIZATION_STEPS)?;
        normalization.step(task)?;
    }
    Ok(normalization.results.pop().flatten())
}

/// The explicit stack an authoring normalization runs on. A task either settles its result onto
/// `results`, or pushes itself back (with the progress it made) beneath the child it needs
/// normalized first, and picks that child's result up when it is popped again. A `None` result is
/// an ambiguity the parent propagates rather than writes.
struct Normalization<'a, 'r> {
    document: &'a Value,
    branch_validators: &'r BranchValidators,
    budget: &'r mut NormalizationBudget,
    tasks: Vec<NormalizationTask<'a>>,
    results: Vec<Option<Value>>,
}

impl<'a> Normalization<'a, '_> {
    fn step(&mut self, task: NormalizationTask<'a>) -> Result<(), CodecError> {
        match task {
            NormalizationTask::Visit {
                schema,
                value,
                hops,
                depth,
            } => self.visit(schema, value, hops, depth)?,
            NormalizationTask::Object {
                schema,
                entries,
                out,
                pending,
                hops,
                depth,
            } => self.object(schema, entries, out, pending, hops, depth),
            NormalizationTask::Array {
                schema,
                items,
                out,
                pending,
                hops,
                depth,
            } => self.array(schema, items, out, pending, hops, depth),
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
                    let candidate = self.results.pop().expect("branch normalization result");
                    let Some(candidate) = candidate else {
                        ambiguous = true;
                        self.tasks.push(NormalizationTask::Branch {
                            branches,
                            index,
                            value,
                            selected,
                            ambiguous,
                            pending,
                            hops,
                            depth,
                        });
                        return Ok(());
                    };
                    if self.accepts(&branches[branch_index], &candidate) {
                        if selected.is_some() {
                            ambiguous = true;
                        } else {
                            selected = Some(candidate);
                        }
                    }
                }
                if index >= branches.len() {
                    self.results.push(if ambiguous { None } else { selected });
                    return Ok(());
                }
                let branch_index = index;
                index += 1;
                let resolved = resolve_schema(self.document, &branches[branch_index], hops)?;
                if !authoring_matches(resolved, &value) {
                    self.tasks.push(NormalizationTask::Branch {
                        branches,
                        index,
                        value,
                        selected,
                        ambiguous,
                        pending,
                        hops,
                        depth,
                    });
                    return Ok(());
                }
                self.budget.consume(NORMALIZATION_STEPS)?;
                self.tasks.push(NormalizationTask::Branch {
                    branches,
                    index,
                    value: value.clone(),
                    selected,
                    ambiguous,
                    pending: Some(branch_index),
                    hops,
                    depth,
                });
                self.tasks.push(NormalizationTask::Visit {
                    schema: resolved,
                    value,
                    hops: hops - 1,
                    depth,
                });
            }
        }
        Ok(())
    }

    /// Resolve the position, then either settle a scalar spelling the declaration states, or
    /// schedule the branches, members, or items the value is normalized through.
    fn visit(
        &mut self,
        schema: &'a Value,
        value: Value,
        hops: usize,
        depth: usize,
    ) -> Result<(), CodecError> {
        if depth > NORMALIZATION_DEPTH || hops == 0 {
            return Err(CodecError::NormalizationLimit {
                limit: NORMALIZATION_DEPTH,
            });
        }
        let schema = resolve_schema(self.document, schema, hops)?;
        if let Some(branches) = schema
            .get("anyOf")
            .or_else(|| schema.get("oneOf"))
            .and_then(Value::as_array)
        {
            self.tasks.push(NormalizationTask::Branch {
                branches,
                index: 0,
                value,
                selected: None,
                ambiguous: false,
                pending: None,
                hops,
                depth,
            });
            return Ok(());
        }
        if schema.get(KEYWORD).and_then(Value::as_str) == Some("float")
            && let Some(number) = super::parse_float(&value)
        {
            self.results
                .push(Some(Value::String(super::float_string(number))));
            return Ok(());
        }
        if schema.get(KEYWORD).and_then(Value::as_str) == Some("uint")
            && let Some(number) = value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
        {
            self.results.push(Some(Value::String(number.to_string())));
            return Ok(());
        }
        match value {
            Value::Object(members) => {
                self.tasks.push(NormalizationTask::Object {
                    schema,
                    entries: members.into_iter(),
                    out: serde_json::Map::new(),
                    pending: None,
                    hops,
                    depth: depth + 1,
                });
            }
            Value::Array(items) => {
                self.tasks.push(NormalizationTask::Array {
                    schema,
                    items: items.into_iter(),
                    out: Vec::new(),
                    pending: false,
                    hops,
                    depth: depth + 1,
                });
            }
            scalar => self.results.push(Some(scalar)),
        }
        Ok(())
    }

    /// Take the member normalized last, then schedule the next declared member; an undeclared
    /// member is carried as it was submitted.
    fn object(
        &mut self,
        schema: &'a Value,
        mut entries: serde_json::map::IntoIter,
        mut out: serde_json::Map<String, Value>,
        mut pending: Option<String>,
        hops: usize,
        depth: usize,
    ) {
        if let Some(key) = pending.take() {
            let child = self.results.pop().expect("child normalization result");
            let Some(child) = child else {
                self.results.push(None);
                return;
            };
            out.insert(key, child);
        }
        let Some((key, value)) = entries.next() else {
            self.results.push(Some(Value::Object(out)));
            return;
        };
        let child = match declared_member(schema, &key) {
            Some(member_schema) => {
                pending = Some(key);
                Some(NormalizationTask::Visit {
                    schema: member_schema,
                    value,
                    hops: HOPS,
                    depth,
                })
            }
            None => {
                out.insert(key, value);
                None
            }
        };
        self.tasks.push(NormalizationTask::Object {
            schema,
            entries,
            out,
            pending,
            hops,
            depth,
        });
        self.tasks.extend(child);
    }

    /// Take the item normalized last, then schedule the next one; items of an array that declares
    /// no `items` are carried as they were submitted.
    fn array(
        &mut self,
        schema: &'a Value,
        mut items: std::vec::IntoIter<Value>,
        mut out: Vec<Value>,
        pending: bool,
        hops: usize,
        depth: usize,
    ) {
        if pending {
            let child = self.results.pop().expect("child normalization result");
            let Some(child) = child else {
                self.results.push(None);
                return;
            };
            out.push(child);
        }
        let Some(value) = items.next() else {
            self.results.push(Some(Value::Array(out)));
            return;
        };
        let child = match schema.get("items") {
            Some(item_schema) => Some(NormalizationTask::Visit {
                schema: item_schema,
                value,
                hops: HOPS,
                depth,
            }),
            None => {
                out.push(value);
                None
            }
        };
        self.tasks.push(NormalizationTask::Array {
            schema,
            items,
            out,
            pending: child.is_some(),
            hops,
            depth,
        });
        self.tasks.extend(child);
    }

    /// Whether a branch's normalized candidate is one its compiled validator accepts.
    fn accepts(&self, branch: &Value, candidate: &Value) -> bool {
        let key = serde_json::to_string(branch).expect("a schema branch serializes");
        self.branch_validators
            .get(&key)
            .expect("compiled branch validator")
            .is_valid(candidate)
    }
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
    // A union branch pinned to a discriminant does not carry a value spelling a different one:
    // without this the authoring walk visits every branch, and a member whose schema rejects the
    // candidate (another member's required fields absent) reads as ambiguity rather than as the
    // discriminant having chosen its branch. The pin is on the branch's `$type` property, the
    // same position the compiled union adds it at.
    let pinned = schema
        .get("properties")
        .and_then(|properties| properties.get("$type"))
        .and_then(|tagged| tagged.get("const"))
        .and_then(Value::as_str);
    if let Some(pinned) = pinned {
        return value.get("$type").and_then(Value::as_str) == Some(pinned);
    }
    if matches!(
        schema.get(KEYWORD).and_then(Value::as_str),
        Some("float" | "uint")
    ) && value.is_number()
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn branch_validators_resolve_definitions_from_the_shared_root_at_any_pointer() {
        // Member names that need JSON Pointer escaping and URI percent-encoding both.
        let name = "a b/c~%#";
        let document = json!({
            "type": "object",
            "properties": {
                name: {
                    "anyOf": [
                        {"$ref": "#/$defs/Text"},
                        {"type": "object", "properties": {"n": {"$ref": "#/$defs/Count"}}, "required": ["n"]}
                    ]
                }
            },
            "$defs": {"Text": {"type": "string"}, "Count": {"type": "integer"}}
        });
        let compiled = CompiledSchema::compile(document.clone()).expect("compiles");
        let branches = document["properties"][name]["anyOf"].as_array().unwrap();
        let accepts = |branch: &Value, candidate: Value| {
            let key = serde_json::to_string(branch).unwrap();
            compiled.branch_validators[&key].is_valid(&candidate)
        };
        assert!(accepts(&branches[0], json!("text")));
        assert!(!accepts(&branches[0], json!(1)));
        assert!(accepts(&branches[1], json!({"n": 1})));
        assert!(!accepts(&branches[1], json!({"n": "x"})));
    }

    #[test]
    fn compilation_rejects_contradictory_storage_declarations_at_nested_positions() {
        for (codec, kind) in [
            ("bytes", "string"),
            ("timestamp", "integer"),
            ("float", "number"),
            ("text", "integer"),
            ("counter", "boolean"),
        ] {
            let declaration = json!({"type": kind, "x-automerge": codec});
            for document in [
                declaration.clone(),
                json!({"type": "array", "items": declaration}),
                json!({"$ref": "#/$defs/Carried", "$defs": {"Carried": declaration}}),
            ] {
                let error = CompiledSchema::compile(document).expect_err("contradictory codec");
                assert!(matches!(
                    error,
                    CodecError::InvalidSchema { message }
                        if message.contains(codec) && message.contains("requires JSON type")
                ));
            }
        }
        assert!(matches!(
            CompiledSchema::compile(json!({"type": "integer", "format": "date-time"})),
            Err(CodecError::InvalidSchema { message }) if message.contains("timestamp")
        ));
    }

    #[test]
    fn compilation_refuses_unknown_or_bypassed_storage_declarations() {
        assert!(matches!(
            CompiledSchema::compile(json!({"type": "string", "x-automerge": "future"})),
            Err(CodecError::Unknown { stated }) if stated == "future"
        ));
        assert!(matches!(
            CompiledSchema::compile(json!({"type": "string", "x-automerge": 1})),
            Err(CodecError::NotASpelling { stated }) if stated == json!(1)
        ));
        for document in [
            json!({
                "$ref": "#/$defs/Carried",
                "$defs": {"Carried": {"type": "string"}},
                "x-automerge": "text"
            }),
            json!({"anyOf": [{"type": "string"}], "x-automerge": "text"}),
            json!({"oneOf": [{"type": "string"}], "format": "date-time"}),
        ] {
            assert!(matches!(
                CompiledSchema::compile(document),
                Err(CodecError::InvalidSchema { message }) if message.contains("resolves before")
            ));
        }
    }

    #[test]
    fn compilation_preserves_supported_nullable_and_referenced_codec_declarations() {
        for (codec, kind) in [
            ("bytes", "object"),
            ("timestamp", "string"),
            ("float", "string"),
            ("text", "string"),
            ("counter", "integer"),
        ] {
            let declaration = json!({"type": [kind, "null"], "x-automerge": codec});
            let document = json!({
                "$ref": "#/$defs/Carried",
                "$defs": {"Carried": declaration},
                "const": {"x-automerge": "future", "type": "not-a-type"}
            });
            assert_eq!(
                CompiledSchema::compile(document.clone())
                    .expect("codec declarations are schema positions, not literal data")
                    .document(),
                &document
            );
            CompiledSchema::compile(json!({"x-automerge": codec}))
                .expect("a codec without a redundant JSON type still declares its storage");
        }
    }
}
