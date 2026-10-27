//! Conservative schema evolution checks for the core Cabinet representation.
//!
//! This module deliberately does not decide whether a schema may be published.  It compares two
//! already-produced schema bundles and reports three independent questions: can the JSON reader and
//! writer understand one another, can the existing native representation be retained, and does the
//! canonical/Lexicon-facing shape evolve safely?  A higher-level `SchemaRepo` can apply its own
//! admission policy to the report.
//!
//! The checker understands the JSON Schema subset emitted by Cabinet, and nothing else. A union is
//! read as the internal `$type` layout a published declaration has: the discriminant is the `$type`
//! const on the branch and the payload is the branch itself, because an adjacent member is a
//! declared wrapper type whose single property happens to be `value` — not a carrier the checker
//! unwraps. Bytes are the `{ "$bytes": ... }` object the codec emits; a `contentEncoding`
//! annotation is an ordinary string annotation and is not sniffed. A storage form is `x-automerge`
//! or `format: date-time` and nothing further, so a unit annotation is not a storage kind.
//!
//! Lexicon evolution is bidirectional. An optional property may be added or removed, and an open
//! union may gain or lose a known member. A required property, a type, a nullable name, a constraint
//! that rejects a previously valid value, or a closed union's member set is incompatible.
//!
//! Unsupported applicators are reported as `not established`; they are never treated as compatible
//! merely because two arbitrary JSON documents happen to compare equal. The Lexicon dimension
//! compares the files [`crate::facet::Bundle::lexicons`] actually publishes, so a bundle that fails
//! that gate has no Lexicon to evolve.

mod accessors;
#[cfg(feature = "schemars")]
mod lexicon;
mod native;
mod union;

use accessors::Additional;
use accessors::additional;
use accessors::child;
use accessors::has_type;
use accessors::required;
use accessors::resolve;
use accessors::types;
use native::compare_native;
use serde_json::Value;
use std::collections::BTreeMap;
use union::compare_open_union_refinement;
use union::compare_unions;
use union::is_open_fallback_tag;
use union::union_branches;
use union::union_operator;

/// A schema document together with the definitions available while resolving local references.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaBundle {
    /// The root schema to compare.
    pub root: Value,
    /// Definitions carried by the bundle.  Local `#/$defs/name` references resolve here first.
    pub definitions: BTreeMap<String, Value>,
}

impl SchemaBundle {
    /// Builds a bundle from a root document, copying its `$defs` into the local definition table.
    pub fn new(root: Value) -> Self {
        let definitions = root
            .get("$defs")
            .and_then(Value::as_object)
            .map(|defs| {
                defs.iter()
                    .map(|(name, schema)| (name.clone(), schema.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Self { root, definitions }
    }

    /// Builds a bundle whose definitions are supplied separately from its root.
    pub fn with_definitions(
        root: Value,
        definitions: impl IntoIterator<Item = (String, Value)>,
    ) -> Self {
        let mut collected = BTreeMap::new();
        for (name, schema) in definitions {
            if let Some(previous) = collected.insert(name.clone(), schema.clone()) {
                assert_eq!(
                    previous, schema,
                    "conflicting schemas supplied for definition `{name}`"
                );
            }
        }
        Self {
            root,
            definitions: collected,
        }
    }
}

/// The result of one compatibility dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compatibility {
    /// The supported rules found no incompatibility.
    Compatible,
    /// A supported rule proves that the evolution breaks a reader or native representation.
    Incompatible,
    /// The schemas use a construct this checker cannot prove safe.
    NotEstablished,
}

/// A path-rich explanation for a compatibility result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// A JSON-pointer-like path.  The direction is included in `reason` so paths remain valid JSON pointers.
    pub path: String,
    /// A stable, human-readable reason suitable for logs and snapshots.
    pub reason: String,
}

/// One compatibility answer and its explanations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityOutcome {
    pub result: Compatibility,
    pub diagnostics: Vec<Diagnostic>,
}

/// Independent answers for JSON, native storage, and Lexicon evolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub json: CompatibilityOutcome,
    pub native_storage: CompatibilityOutcome,
    pub lexicon: CompatibilityOutcome,
}

/// Compare two roots that carry their own `$defs` tables.
pub fn compare_schemas(old: &Value, new: &Value) -> CompatibilityReport {
    compare(
        &SchemaBundle::new(old.clone()),
        &SchemaBundle::new(new.clone()),
    )
}

/// Compare an old accepted bundle with a proposed new bundle.
pub fn compare(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityReport {
    let json = compare_json(old, new);
    let native_storage = compare_native(old, new);
    let lexicon = compare_lexicon(old, new);
    CompatibilityReport {
        json,
        native_storage,
        lexicon,
    }
}

/// Compare named facet documents from two Cabinet bundles without requiring callers to flatten
/// `$defs` themselves.  Definition resolution remains limited to each bundle's carried context.
#[cfg(feature = "schemars")]
pub fn compare_facet_bundles(
    old: &crate::facet::Bundle<'_>,
    old_facet: &str,
    new: &crate::facet::Bundle<'_>,
    new_facet: &str,
) -> Option<CompatibilityReport> {
    // The document a bundle exposes is a `$ref` into its definitions, which is not itself a
    // position. The facet's own definition is, and that is what evolves.
    let old_schema = serde_json::to_value(old.resolve(old_facet)?).expect("a schema is JSON");
    let new_schema = serde_json::to_value(new.resolve(new_facet)?).expect("a schema is JSON");
    let mut report = compare_schemas(&old_schema, &new_schema);
    // The Lexicon dimension compares published files, not a projection of the sidecar. A bundle
    // that fails the publish gate is a codec document: it has no Lexicon, so the dimension is not
    // established rather than guessed.
    let old_id = old
        .plug
        .facets
        .iter()
        .find(|facet| facet.name == old_facet)
        .map(|facet| facet.id.as_str());
    let new_id = new
        .plug
        .facets
        .iter()
        .find(|facet| facet.name == new_facet)
        .map(|facet| facet.id.as_str());
    report.lexicon = match (old_id, new_id, old.lexicons(), new.lexicons()) {
        (Some(old_id), Some(new_id), Ok(old_lexicons), Ok(new_lexicons)) => {
            let old_file = old_lexicons
                .into_iter()
                .find(|lexicon| lexicon.id == old_id);
            let new_file = new_lexicons
                .into_iter()
                .find(|lexicon| lexicon.id == new_id);
            match (old_file, new_file) {
                (Some(old_file), Some(new_file)) if old_id == new_id => {
                    lexicon::evolve_files(&old_file.file, &new_file.file)
                }
                (Some(_), Some(_)) => incompatible_outcome("/", "lexicon id changed"),
                _ => not_established_outcome(
                    "/",
                    "facet has no published Lexicon: it is a codec document".to_owned(),
                ),
            }
        }
        (_, _, Err(error), _) | (_, _, _, Err(error)) => {
            not_established_outcome("/", format!("facet bundle is not lexicon-aligned: {error}"))
        }
        _ => not_established_outcome("/", "facet is not declared by the bundle".to_owned()),
    };
    Some(report)
}

#[derive(Default)]
struct Findings {
    diagnostics: Vec<Diagnostic>,
    incompatible: bool,
    not_established: bool,
}

impl Findings {
    fn incompatible(&mut self, path: impl Into<String>, reason: impl Into<String>) {
        self.incompatible = true;
        self.diagnostics.push(Diagnostic {
            path: path.into(),
            reason: reason.into(),
        });
    }

    fn not_established(&mut self, path: impl Into<String>, reason: impl Into<String>) {
        self.not_established = true;
        self.diagnostics.push(Diagnostic {
            path: path.into(),
            reason: reason.into(),
        });
    }

    /// Record where the writer's `const`/`enum` vocabulary reaches values the reader's does not
    /// admit: a changed or unenforced `const`, a writer `const` outside the reader `enum`, an
    /// `enum` value removed, or a closed reader `enum` the writer does not close.
    fn compare_vocabulary(&mut self, reader: &Value, writer: &Value, path: &str, direction: &str) {
        if let Some(reader_const) = reader.get("const") {
            match writer.get("const") {
                Some(writer_const) if writer_const != reader_const => {
                    self.incompatible(path, format!("{direction}: const value changed"))
                }
                None => self.incompatible(
                    path,
                    format!("{direction}: reader const is not enforced by the writer"),
                ),
                _ => {}
            }
        } else if let Some(writer_const) = writer.get("const")
            && let Some(reader_enum) = reader.get("enum").and_then(Value::as_array)
            && !reader_enum.contains(writer_const)
        {
            self.incompatible(
                path,
                format!("{direction}: writer const is outside the reader enum"),
            );
        }

        if let Some(reader_enum) = reader.get("enum").and_then(Value::as_array) {
            match writer.get("enum").and_then(Value::as_array) {
                Some(writer_enum)
                    if !writer_enum.iter().all(|value| reader_enum.contains(value)) =>
                {
                    self.incompatible(path, format!("{direction}: enum value was removed"));
                }
                Some(_) => {}
                None => self.incompatible(
                    path,
                    format!("{direction}: reader closed enum is broader in the writer"),
                ),
            }
        }
    }

    fn outcome(self) -> CompatibilityOutcome {
        let result = if self.incompatible {
            Compatibility::Incompatible
        } else if self.not_established {
            Compatibility::NotEstablished
        } else {
            Compatibility::Compatible
        };
        CompatibilityOutcome {
            result,
            diagnostics: self.diagnostics,
        }
    }
}

fn not_established_outcome(path: &str, reason: String) -> CompatibilityOutcome {
    outcome(Compatibility::NotEstablished, path, reason)
}

#[cfg(feature = "schemars")]
fn incompatible_outcome(path: &str, reason: &str) -> CompatibilityOutcome {
    outcome(Compatibility::Incompatible, path, reason.to_owned())
}

fn outcome(result: Compatibility, path: &str, reason: String) -> CompatibilityOutcome {
    CompatibilityOutcome {
        result,
        diagnostics: vec![Diagnostic {
            path: path.to_owned(),
            reason,
        }],
    }
}

fn scan_unsupported_bundle(bundle: &SchemaBundle, side: &str, findings: &mut Findings) {
    scan_unsupported_schema(&bundle.root, "/", side, findings);
    for (name, schema) in &bundle.definitions {
        scan_unsupported_schema(schema, &child("/$defs", name), side, findings);
    }
}

fn scan_unsupported_schema(schema: &Value, path: &str, side: &str, findings: &mut Findings) {
    if is_open_fallback_tag(schema) {
        return;
    }
    if let Some(keyword) = unsupported_keyword(schema) {
        findings.not_established(
            path,
            format!("{side}: unsupported schema keyword or type context `{keyword}`"),
        );
        return;
    }
    let Some(object) = schema.as_object() else {
        return;
    };
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            scan_unsupported_schema(property, &child(path, name), side, findings);
        }
    }
    if let Some(definitions) = object.get("$defs").and_then(Value::as_object) {
        for (name, definition) in definitions {
            scan_unsupported_schema(
                definition,
                &child(&format!("{path}/$defs"), name),
                side,
                findings,
            );
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(value) = object.get(key)
            && value.is_object()
        {
            scan_unsupported_schema(value, &child(path, &format!("<{key}>")), side, findings);
        }
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(branches)) = object.get(key) {
            for (index, branch) in branches.iter().enumerate() {
                scan_unsupported_schema(branch, &format!("{path}<union:{index}>"), side, findings);
            }
        }
    }
}

fn compare_json(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    let mut findings = Findings::default();
    scan_unsupported_bundle(old, "old", &mut findings);
    scan_unsupported_bundle(new, "new", &mut findings);
    if findings.not_established {
        return findings.outcome();
    }
    accepts(
        old,
        new,
        &old.root,
        &new.root,
        "/",
        "old<-new",
        &mut findings,
    );
    accepts(
        new,
        old,
        &new.root,
        &old.root,
        "/",
        "new<-old",
        &mut findings,
    );
    findings.outcome()
}

/// Project the supported canonical representation through the same projector a bundle publishes
/// with, so this dimension cannot disagree with [`crate::facet::lexicon::project_facet`] about a
/// schema the crate itself authored. A schema that projector refuses is not a Lexicon, and the
/// answer is `not established` rather than a guess. Two projections that differ are judged by the
/// bidirectional evolution rules, not by byte equality.
#[cfg(feature = "schemars")]
fn compare_lexicon(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    match (
        crate::facet::lexicon::project_position(&old.root, &defs_of(&old.definitions)),
        crate::facet::lexicon::project_position(&new.root, &defs_of(&new.definitions)),
    ) {
        (Ok(old_position), Ok(new_position)) => {
            lexicon::evolve_projected(&old_position, &new_position)
        }
        (Err(reason), _) | (_, Err(reason)) => {
            not_established_outcome("/", format!("not a publishable Lexicon: {reason}"))
        }
    }
}

#[cfg(not(feature = "schemars"))]
fn compare_lexicon(_: &SchemaBundle, _: &SchemaBundle) -> CompatibilityOutcome {
    not_established_outcome("/", "this build has no lexicon projector".to_owned())
}

#[cfg(feature = "schemars")]
fn defs_of(definitions: &BTreeMap<String, Value>) -> serde_json::Map<String, Value> {
    definitions
        .iter()
        .map(|(name, schema)| (name.clone(), schema.clone()))
        .collect()
}

/// A storage marker that contradicts the JSON shape it annotates, or names a form this build does
/// not store. The shapes are the ones the codec emits: canonical bytes are an object, a counter is a
/// whole number, and a float is a string.
fn contradictory_annotation(schema: &Value) -> Option<String> {
    let marker = schema.get("x-automerge")?;
    let Some(marker) = marker.as_str() else {
        return Some("x-automerge marker is not a string".to_owned());
    };
    let allowed = match marker {
        "bytes" => "object",
        "float" | "timestamp" | "text" => "string",
        "counter" => "integer",
        "number" => "number",
        "list" => "array",
        _ => return Some(format!("unknown x-automerge storage `{marker}`")),
    };
    let consistent = schema
        .get("type")
        .is_none_or(|type_value| match type_value {
            Value::String(kind) => kind == allowed,
            Value::Array(kinds) => {
                kinds.iter().any(|kind| kind.as_str() == Some(allowed))
                    && kinds.iter().all(|kind| {
                        kind.as_str()
                            .is_some_and(|kind| kind == allowed || kind == "null")
                    })
            }
            _ => false,
        });
    if consistent {
        None
    } else {
        Some("x-automerge marker contradicts the declared JSON type".to_owned())
    }
}

/// Check that values accepted by `writer` can be consumed by `reader`.
fn accepts(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    let Some(reader) = resolve(reader_bundle, reader, path, direction, findings) else {
        return;
    };
    let Some(writer) = resolve(writer_bundle, writer, path, direction, findings) else {
        return;
    };

    if reader == &Value::Bool(true) || writer == &Value::Bool(false) {
        return;
    }

    if is_open_fallback_tag(reader) || is_open_fallback_tag(writer) {
        compare_open_union_refinement(reader, writer, path, direction, findings);
        return;
    }
    if let Some(keyword) = unsupported_keyword(reader).or_else(|| unsupported_keyword(writer)) {
        findings.not_established(
            path,
            format!("{direction}: unsupported schema keyword or type context `{keyword}`"),
        );
        return;
    }
    if let Some(reason) =
        contradictory_annotation(reader).or_else(|| contradictory_annotation(writer))
    {
        findings.not_established(path, format!("{direction}: {reason}"));
        return;
    }
    if reader == &Value::Bool(false) {
        findings.incompatible(path, format!("{direction}: reader rejects every value"));
        return;
    }
    if writer == &Value::Bool(true) {
        if reader != &Value::Bool(true) {
            findings.incompatible(
                path,
                format!("{direction}: unrestricted writer reaches a constrained reader"),
            );
        }
        return;
    }

    let reader_unconstrained = unconstrained(reader);
    let writer_unconstrained = unconstrained(writer);
    if reader_unconstrained || writer_unconstrained {
        if writer_unconstrained && !reader_unconstrained {
            findings.incompatible(
                path,
                format!("{direction}: unconstrained writer is broader than the reader"),
            );
        }
        return;
    }

    if compare_open_union_refinement(reader, writer, path, direction, findings) == Some(true) {
        return;
    }

    if let (Some(reader_types), Some(writer_types)) = (types(reader), types(writer))
        && !writer_types.iter().all(|kind| reader_types.contains(kind))
    {
        findings.incompatible(
                path,
                format!(
                    "{direction}: writer types {writer_types:?} are not accepted by reader types {reader_types:?}"
                ),
            );
        return;
    }

    findings.compare_vocabulary(reader, writer, path, direction);

    let reader_operator = union_operator(reader);
    let writer_operator = union_operator(writer);
    if reader_operator == Some("invalid") || writer_operator == Some("invalid") {
        findings.not_established(
            path,
            format!("{direction}: schema declares both oneOf and anyOf"),
        );
        return;
    }
    if let (Some(reader_operator), Some(writer_operator)) = (reader_operator, writer_operator)
        && reader_operator != writer_operator
    {
        findings.not_established(path, format!("{direction}: union operator changed"));
        return;
    }
    let reader_union = union_branches(reader);
    let writer_union = union_branches(writer);
    if reader_union.is_some() || writer_union.is_some() {
        compare_unions(
            reader_bundle,
            writer_bundle,
            reader_union,
            writer_union,
            path,
            direction,
            findings,
        );
        return;
    }

    let reader_is_object = has_type(reader, "object") || reader.get("properties").is_some();
    let writer_is_object = has_type(writer, "object") || writer.get("properties").is_some();
    if reader_is_object || writer_is_object {
        compare_objects(
            reader_bundle,
            writer_bundle,
            reader,
            writer,
            path,
            direction,
            findings,
        );
        return;
    }

    if has_type(reader, "array") || has_type(writer, "array") {
        compare_arrays(
            reader_bundle,
            writer_bundle,
            reader,
            writer,
            path,
            direction,
            findings,
        );
    }

    compare_constraints(reader, writer, path, direction, findings);
}

fn compare_objects(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    let reader_properties = reader.get("properties").and_then(Value::as_object);
    let writer_properties = writer.get("properties").and_then(Value::as_object);
    let reader_required = required(reader);
    let writer_required = required(writer);

    for name in &reader_required {
        if !writer_required.contains(name) {
            findings.incompatible(
                child(path, name),
                format!("{direction}: reader requires a property the writer may omit"),
            );
        }
    }

    if let Some(writer_properties) = writer_properties {
        for (name, writer_property) in writer_properties {
            let property_path = child(path, name);
            if let Some(reader_property) =
                reader_properties.and_then(|properties| properties.get(name))
            {
                accepts(
                    reader_bundle,
                    writer_bundle,
                    reader_property,
                    writer_property,
                    &property_path,
                    direction,
                    findings,
                );
            } else {
                match additional(reader) {
                    Additional::Allowed => {}
                    Additional::Schema(reader_schema) => accepts(
                        reader_bundle,
                        writer_bundle,
                        reader_schema,
                        writer_property,
                        &property_path,
                        direction,
                        findings,
                    ),
                    Additional::Closed => findings.incompatible(
                        property_path,
                        format!("{direction}: writer added a property to a closed object"),
                    ),
                    Additional::Unknown => findings.not_established(
                        property_path,
                        format!("{direction}: object openness is not stated"),
                    ),
                }
            }
        }
    }

    // An open writer may carry arbitrary extension members.  A closed reader cannot promise to
    // consume those members, while a typed map is checked at its extension position.
    match (additional(writer), additional(reader)) {
        (Additional::Allowed | Additional::Schema(_), Additional::Closed) => findings.incompatible(
            path,
            format!("{direction}: writer permits extension members but reader is closed"),
        ),
        (Additional::Unknown, _) | (_, Additional::Unknown) => {
            findings.not_established(path, format!("{direction}: object openness is not stated"))
        }
        (Additional::Schema(writer_schema), Additional::Schema(reader_schema)) => accepts(
            reader_bundle,
            writer_bundle,
            reader_schema,
            writer_schema,
            &child(path, "<additionalProperties>"),
            direction,
            findings,
        ),
        _ => {}
    }
}

fn compare_arrays(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    for key in ["prefixItems", "contains", "unevaluatedItems"] {
        if reader.get(key) != writer.get(key)
            && (reader.get(key).is_some() || writer.get(key).is_some())
        {
            findings.not_established(
                path,
                format!("{direction}: array keyword `{key}` changed and is unsupported"),
            );
        }
    }
    match (reader.get("items"), writer.get("items")) {
        (Some(reader_items), Some(writer_items)) => accepts(
            reader_bundle,
            writer_bundle,
            reader_items,
            writer_items,
            &child(path, "<items>"),
            direction,
            findings,
        ),
        (Some(_), None) => findings.not_established(
            path,
            format!("{direction}: reader constrains items but writer does not declare them"),
        ),
        _ => {}
    }
    compare_constraints(reader, writer, path, direction, findings);
}

fn compare_constraints(
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    // Storage annotations intentionally do not participate here.  They are compared by
    // `compare_native`; changing Text to Bytes does not change the JSON string shape.
    for key in [
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        "format",
    ] {
        if reader.get(key) != writer.get(key)
            && (reader.get(key).is_some() || writer.get(key).is_some())
        {
            findings.incompatible(path, format!("{direction}: constraint `{key}` changed"));
        }
    }
}

fn unconstrained(schema: &Value) -> bool {
    const CONSTRAINTS: &[&str] = &[
        "type",
        "enum",
        "const",
        "oneOf",
        "anyOf",
        "allOf",
        "$ref",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "prefixItems",
        "contains",
        "not",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        "knownValues",
        "format",
        "$defs",
        "required",
    ];
    !CONSTRAINTS.iter().any(|key| schema.get(*key).is_some())
}

const VALID_TYPES: &[&str] = &[
    "null", "boolean", "object", "array", "number", "integer", "string",
];

fn unsupported_keyword(schema: &Value) -> Option<String> {
    let Value::Object(object) = schema else {
        return None;
    };
    if let Some(type_value) = object.get("type") {
        let valid = match type_value {
            Value::String(kind) => VALID_TYPES.contains(&kind.as_str()),
            Value::Array(kinds) => {
                !kinds.is_empty()
                    && kinds.iter().all(|kind| {
                        kind.as_str()
                            .is_some_and(|kind| VALID_TYPES.contains(&kind))
                    })
            }
            _ => false,
        };
        if !valid {
            return Some("type".to_owned());
        }
    }
    const SUPPORTED: &[&str] = &[
        "$schema",
        "$id",
        "$defs",
        "$ref",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "oneOf",
        "anyOf",
        "const",
        "enum",
        "knownValues",
        "format",
        "title",
        "description",
        "default",
        "examples",
        "x-automerge",
        "contentEncoding",
        "contentMediaType",
        "unit",
        "x-unit",
        "x-automerge-unit",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
    ];
    object
        .keys()
        .find(|key| !SUPPORTED.contains(&key.as_str()))
        .cloned()
}

#[cfg(all(test, feature = "schemars"))]
mod tests;
