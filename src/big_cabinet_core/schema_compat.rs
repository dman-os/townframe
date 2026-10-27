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

#[cfg(feature = "schemars")]
mod lexicon;
mod native;

use native::compare_native;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

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

    if let Some(reader_const) = reader.get("const") {
        match writer.get("const") {
            Some(writer_const) if writer_const != reader_const => {
                findings.incompatible(path, format!("{direction}: const value changed"))
            }
            None => findings.incompatible(
                path,
                format!("{direction}: reader const is not enforced by the writer"),
            ),
            _ => {}
        }
    } else if let Some(writer_const) = writer.get("const")
        && let Some(reader_enum) = reader.get("enum").and_then(Value::as_array)
        && !reader_enum.contains(writer_const)
    {
        findings.incompatible(
            path,
            format!("{direction}: writer const is outside the reader enum"),
        );
    }

    if let Some(reader_enum) = reader.get("enum").and_then(Value::as_array) {
        match writer.get("enum").and_then(Value::as_array) {
            Some(writer_enum) if !writer_enum.iter().all(|value| reader_enum.contains(value)) => {
                findings.incompatible(path, format!("{direction}: enum value was removed"));
            }
            Some(_) => {}
            None => findings.incompatible(
                path,
                format!("{direction}: reader closed enum is broader in the writer"),
            ),
        }
    }

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

fn compare_unions(
    reader_bundle: &SchemaBundle,
    writer_bundle: &SchemaBundle,
    reader: Option<&Vec<Value>>,
    writer: Option<&Vec<Value>>,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) {
    let Some(writer) = writer else {
        findings.not_established(
            path,
            format!("{direction}: union was replaced by a non-union"),
        );
        return;
    };
    let Some(reader) = reader else {
        findings.incompatible(
            path,
            format!("{direction}: union member cannot be read by a non-union"),
        );
        return;
    };
    if !validate_union(reader_bundle, reader, path, direction, findings)
        || !validate_union(writer_bundle, writer, path, direction, findings)
    {
        return;
    }

    let reader_fallback = reader.iter().find(|branch| {
        branch_tag_resolved(reader_bundle, branch, path, direction, findings).is_none()
    });
    let mut reader_tagged = BTreeMap::new();
    for branch in reader {
        if let Some(tag) = branch_tag_resolved(reader_bundle, branch, path, direction, findings)
            && reader_tagged.insert(tag.clone(), branch).is_some()
        {
            findings.not_established(
                path,
                format!("{direction}: union contains duplicate member tag `{tag}`"),
            );
        }
    }

    for (index, writer_branch) in writer.iter().enumerate() {
        let branch_path = format!("{path}<union:{index}>");
        if let Some(tag) = branch_tag_resolved(
            writer_bundle,
            writer_branch,
            &branch_path,
            direction,
            findings,
        ) {
            if let Some(reader_branch) = reader_tagged.get(&tag) {
                compare_union_branch_pair(
                    reader_bundle,
                    writer_bundle,
                    reader_branch,
                    writer_branch,
                    &branch_path,
                    direction,
                    findings,
                );
            } else if let Some(fallback) = reader_fallback {
                compare_union_branch_pair(
                    reader_bundle,
                    writer_bundle,
                    fallback,
                    writer_branch,
                    &branch_path,
                    direction,
                    findings,
                );
            } else {
                findings.incompatible(
                    branch_path,
                    format!("{direction}: union member `{tag}` is not known and union is closed"),
                );
            }
        } else if let Some(fallback) = reader_fallback {
            compare_union_branch_pair(
                reader_bundle,
                writer_bundle,
                fallback,
                writer_branch,
                &branch_path,
                direction,
                findings,
            );
        } else {
            findings.not_established(
                branch_path,
                format!("{direction}: untagged union member cannot be matched conservatively"),
            );
        }
    }
}

fn validate_union(
    bundle: &SchemaBundle,
    branches: &[Value],
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> bool {
    if branches.is_empty() {
        findings.not_established(path, format!("{direction}: empty union is unsupported"));
        return false;
    }
    let mut fallback_count = 0;
    let mut tags = BTreeSet::new();
    let mut valid = true;
    for (index, branch) in branches.iter().enumerate() {
        let branch_path = format!("{path}<union:{index}>");
        let Some(branch) = resolve(bundle, branch, &branch_path, direction, findings) else {
            valid = false;
            continue;
        };
        if unsupported_keyword(branch).is_some() {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch uses an unsupported keyword"),
            );
            valid = false;
        }
        let Some(tag_schema) = branch_discriminant(branch) else {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch has no `$type` discriminant"),
            );
            valid = false;
            continue;
        };
        let tag = if tag_schema.get("const").and_then(Value::as_str).is_some() {
            branch_tag(branch)
        } else if is_open_fallback_tag(tag_schema) {
            fallback_count += 1;
            None
        } else {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch has neither a stable tag nor a valid fallback"),
            );
            valid = false;
            None
        };
        if let Some(tag) = tag
            && !tags.insert(tag.clone())
        {
            findings.not_established(
                &branch_path,
                format!("{direction}: union contains duplicate member tag `{tag}`"),
            );
            valid = false;
        }
    }
    if fallback_count > 1 {
        findings.not_established(
            path,
            format!("{direction}: union contains more than one fallback branch"),
        );
        valid = false;
    }
    valid
}

/// The `$type` schema of a union branch.
///
/// A published union is internally tagged, so the discriminant is a property of the branch rather
/// than a field of a carrier. An adjacent member is the same shape: it is a declared wrapper type
/// whose payload sits under `value`, and `value` is an ordinary property of that wrapper.
fn branch_discriminant(schema: &Value) -> Option<&Value> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("$type"))
}

fn branch_tag_resolved(
    bundle: &SchemaBundle,
    branch: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<String> {
    resolve(bundle, branch, path, direction, findings).and_then(branch_tag)
}

fn compare_union_branch_pair(
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
    let Some(reader_tag) = branch_discriminant(reader) else {
        findings.not_established(
            path,
            format!("{direction}: reader union branch has no `$type` discriminant"),
        );
        return;
    };
    let Some(writer_tag) = branch_discriminant(writer) else {
        findings.not_established(
            path,
            format!("{direction}: writer union branch has no `$type` discriminant"),
        );
        return;
    };

    // The discriminant is compared on its own. The payload is everything else the branch declares,
    // and it is compared property by property: feeding the branch back through `accepts` would
    // re-enter the union and demand an openness the carrier never stated.
    if compare_open_union_refinement(reader_tag, writer_tag, path, direction, findings).is_none() {
        accepts(
            reader_bundle,
            writer_bundle,
            reader_tag,
            writer_tag,
            &child(path, "$type"),
            direction,
            findings,
        );
    }
    let reader_properties = reader.get("properties").and_then(Value::as_object);
    let writer_properties = writer.get("properties").and_then(Value::as_object);
    if let Some(writer_properties) = writer_properties {
        for (name, writer_property) in writer_properties {
            if name == "$type" {
                continue;
            }
            let Some(reader_property) =
                reader_properties.and_then(|properties| properties.get(name))
            else {
                findings.incompatible(
                    child(path, name),
                    format!("{direction}: union member gained a property the reader does not have"),
                );
                continue;
            };
            accepts(
                reader_bundle,
                writer_bundle,
                reader_property,
                writer_property,
                &child(path, name),
                direction,
                findings,
            );
        }
    }
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

fn excluded_values(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("not")
        .and_then(|not| not.get("enum"))
        .and_then(Value::as_array)
}

fn compare_open_union_refinement(
    reader: &Value,
    writer: &Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<bool> {
    let reader_excluded = excluded_values(reader);
    let writer_excluded = excluded_values(writer);
    match (
        reader_excluded,
        writer_excluded,
        reader.get("const"),
        writer.get("const"),
    ) {
        (Some(reader_excluded), Some(writer_excluded), _, _) => {
            let reader_subset = reader_excluded
                .iter()
                .all(|value| writer_excluded.contains(value));
            let writer_subset = writer_excluded
                .iter()
                .all(|value| reader_excluded.contains(value));
            if !(reader_subset || writer_subset) {
                findings.not_established(
                    path,
                    format!("{direction}: open-union fallback exclusion changed incompatibly"),
                );
            }
            Some(true)
        }
        (Some(reader_excluded), None, _, Some(writer_const)) => {
            if reader_excluded.contains(writer_const) {
                findings.incompatible(
                    path,
                    format!("{direction}: union tag is excluded by the reader fallback"),
                );
            }
            Some(true)
        }
        (None, Some(writer_excluded), Some(reader_const), _) => {
            if writer_excluded.contains(reader_const) {
                findings.incompatible(
                    path,
                    format!("{direction}: reader union tag is excluded by the writer fallback"),
                );
            }
            Some(true)
        }
        _ => None,
    }
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

fn is_open_fallback_tag(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    matches!(object.get("type"), Some(Value::String(kind)) if kind == "string")
        && matches!(
            object.get("not"),
            Some(Value::Object(not))
                if not.len() == 1 && not.get("enum").is_some_and(Value::is_array)
        )
        && object.keys().all(|key| key == "type" || key == "not")
}

fn resolve<'a>(
    bundle: &'a SchemaBundle,
    schema: &'a Value,
    path: &str,
    direction: &str,
    findings: &mut Findings,
) -> Option<&'a Value> {
    let mut current = schema;
    let mut seen = BTreeSet::new();
    loop {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return Some(current);
        };
        if !seen.insert(reference.to_owned()) {
            findings.not_established(
                path,
                format!("{direction}: cyclic schema reference `{reference}`"),
            );
            return None;
        }
        let name = reference
            .strip_prefix("#/$defs/")
            .or_else(|| reference.strip_prefix("#/definitions/"));
        let Some(name) = name.map(unescape_pointer_segment) else {
            findings.not_established(
                path,
                format!("{direction}: external schema reference `{reference}` is unavailable"),
            );
            return None;
        };
        let Some(target) = bundle.definitions.get(&name) else {
            findings.not_established(
                path,
                format!("{direction}: schema reference `{reference}` is undeclared"),
            );
            return None;
        };
        current = target;
    }
}

fn types(schema: &Value) -> Option<Vec<String>> {
    match schema.get("type") {
        Some(Value::String(kind)) => Some(vec![kind.clone()]),
        Some(Value::Array(kinds)) => Some(
            kinds
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        ),
        _ => None,
    }
}

fn has_type(schema: &Value, kind: &str) -> bool {
    types(schema).is_some_and(|kinds| kinds.iter().any(|item| item == kind))
}

fn required(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

enum Additional<'a> {
    Allowed,
    Closed,
    Schema(&'a Value),
    Unknown,
}

fn additional(schema: &Value) -> Additional<'_> {
    match schema.get("additionalProperties") {
        Some(Value::Bool(true)) => Additional::Allowed,
        Some(Value::Bool(false)) => Additional::Closed,
        Some(value) => Additional::Schema(value),
        None => Additional::Unknown,
    }
}

fn union_operator(schema: &Value) -> Option<&str> {
    match (schema.get("oneOf"), schema.get("anyOf")) {
        (Some(_), Some(_)) => Some("invalid"),
        (Some(_), None) => Some("oneOf"),
        (None, Some(_)) => Some("anyOf"),
        (None, None) => None,
    }
}

fn union_branches(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
}

fn branch_tag(schema: &Value) -> Option<String> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("$type"))
        .and_then(|tag| tag.get("const"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            schema
                .get("const")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn unescape_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn child(path: &str, name: &str) -> String {
    let escaped = name.replace('~', "~0").replace('/', "~1");
    if path == "/" {
        format!("/{escaped}")
    } else {
        format!("{path}/{escaped}")
    }
}

#[cfg(all(test, feature = "schemars"))]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn optional_property_on_open_object_is_compatible() {
        let old = json!({
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "additionalProperties": true
        });
        let new = json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "summary": {"type": "string"}
            },
            "additionalProperties": true
        });
        let report = compare_schemas(&old, &new);
        assert_eq!(report.json.result, Compatibility::Compatible);
        // Lexicon objects are open, so an optional property is an evolution in either direction.
        assert_eq!(report.lexicon.result, Compatibility::Compatible);
    }

    #[test]
    fn required_addition_and_closed_extension_are_incompatible() {
        let old = json!({"type":"object", "properties":{"title":{"type":"string"}}, "additionalProperties":false});
        let new = json!({"type":"object", "properties":{"title":{"type":"string"},"summary":{"type":"string"}}, "required":["title","summary"], "additionalProperties":false});
        let report = compare_schemas(&old, &new);
        assert_eq!(report.json.result, Compatibility::Incompatible);
        assert!(
            report
                .json
                .diagnostics
                .iter()
                .any(|item| item.path == "/summary")
        );
        assert_eq!(report.lexicon.result, Compatibility::Incompatible);
    }

    #[test]
    fn lexicon_nullable_change_is_incompatible_in_either_direction() {
        let present = json!({"type":"object","properties":{"value":{"type":"string"}}});
        let nullable = json!({"type":"object","properties":{"value":{"type":["string","null"]}}});
        assert_eq!(
            compare_schemas(&present, &nullable).lexicon.result,
            Compatibility::Incompatible
        );
        assert_eq!(
            compare_schemas(&nullable, &present).lexicon.result,
            Compatibility::Incompatible
        );
    }

    #[test]
    fn open_union_and_vocabulary_allow_additions_but_closed_union_does_not() {
        let branch = |tag: Value| {
            json!({
                "type":"object",
                "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
                "required":["$type","value"]
            })
        };
        let fallback = |known: &[&str]| branch(json!({"type":"string", "not":{"enum":known}}));
        let old_union = json!({"anyOf":[branch(json!({"const":"known"})), fallback(&["known"])]});
        let new_union = json!({"anyOf":[
            branch(json!({"const":"known"})),
            branch(json!({"const":"future"})),
            fallback(&["known","future"])
        ]});
        let report = compare_schemas(&old_union, &new_union);
        assert_eq!(
            report.json.result,
            Compatibility::Compatible,
            "{:?}",
            report.json.diagnostics
        );

        let old_closed = json!({"oneOf":[branch(json!({"const":"known"}))]});
        let new_closed = json!({"oneOf":[
            branch(json!({"const":"known"})),
            branch(json!({"const":"future"}))
        ]});
        assert_eq!(
            compare_schemas(&old_closed, &new_closed).json.result,
            Compatibility::Incompatible
        );
        assert_eq!(
            compare_schemas(&old_closed, &new_union).json.result,
            Compatibility::NotEstablished
        );

        let old_vocab = json!({"type":"string", "knownValues":["known"]});
        let new_vocab = json!({"type":"string", "knownValues":["known","future"]});
        assert_eq!(
            compare_schemas(&old_vocab, &new_vocab).json.result,
            Compatibility::Compatible
        );
    }

    #[test]
    fn native_codec_change_is_separate_from_json_shape() {
        let old = json!({"type":"string", "x-automerge":"text"});
        let new = json!({
            "type":"object",
            "x-automerge":"bytes",
            "properties":{"$bytes":{"type":"string"}},
            "required":["$bytes"],
            "additionalProperties":false
        });
        let report = compare_schemas(&old, &new);
        assert_eq!(
            report.native_storage.result,
            Compatibility::Incompatible,
            "{:?}",
            report.native_storage.diagnostics
        );
        assert_eq!(report.json.result, Compatibility::Incompatible);
    }

    #[test]
    fn unresolved_reference_is_not_established() {
        let old = json!({"$ref":"#/$defs/Item"});
        let new = json!({"$ref":"#/$defs/Item"});
        let report = compare_schemas(&old, &new);
        assert_eq!(report.json.result, Compatibility::NotEstablished);
        assert!(
            report
                .json
                .diagnostics
                .iter()
                .any(|item| item.reason.contains("undeclared"))
        );
    }

    #[test]
    fn bundle_references_are_resolved() {
        let old = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/Item"}),
            [(
                "Item".to_owned(),
                json!({"type":"string", "knownValues":["old"]}),
            )],
        );
        let new = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/Item"}),
            [(
                "Item".to_owned(),
                json!({"type":"string", "knownValues":["old","new"]}),
            )],
        );
        assert_eq!(compare(&old, &new).json.result, Compatibility::Compatible);
    }

    #[test]
    fn unconstrained_enum_and_const_changes_are_not_silently_compatible() {
        let unconstrained = json!({});
        let string = json!({"type":"string"});
        assert_eq!(
            compare_schemas(&unconstrained, &string).json.result,
            Compatibility::Incompatible
        );

        let old_enum = json!({"type":"string", "enum":["a"]});
        let open_string = json!({"type":"string"});
        assert_eq!(
            compare_schemas(&old_enum, &open_string).json.result,
            Compatibility::Incompatible
        );

        let old_const = json!({"const":"a"});
        let new_const = json!({"const":"b"});
        assert_eq!(
            compare_schemas(&old_const, &new_const).json.result,
            Compatibility::Incompatible
        );
    }

    #[test]
    fn typed_map_codec_changes_are_native_incompatible() {
        let old = json!({
            "type":"object",
            "additionalProperties":{"type":"string", "x-automerge":"text"}
        });
        let new = json!({
            "type":"object",
            "additionalProperties":{
                "type":"object",
                "x-automerge":"bytes",
                "properties":{"$bytes":{"type":"string"}},
                "required":["$bytes"],
                "additionalProperties":false
            }
        });
        assert_eq!(
            compare_schemas(&old, &new).native_storage.result,
            Compatibility::Incompatible
        );
    }

    #[test]
    fn generated_open_union_fallback_accepts_new_member() {
        let branch = |tag: Value| {
            json!({
                "type":"object",
                "properties":{"$type":tag,"value":true},
                "required":["$type","value"]
            })
        };
        let fallback = |known: &[&str]| branch(json!({"type":"string", "not":{"enum":known}}));
        let old = json!({"oneOf":[branch(json!({"const":"known"})), fallback(&["known"])]});
        let new = json!({"oneOf":[
            branch(json!({"const":"known"})),
            branch(json!({"const":"future"})),
            fallback(&["known","future"])
        ]});
        assert_eq!(
            compare_schemas(&old, &new).json.result,
            Compatibility::Compatible
        );
    }

    #[test]
    fn arrays_nullable_and_sidecar_constraints_have_separate_answers() {
        let old = json!({
            "type":"object",
            "properties":{"values":{"type":"array","items":{"type":["integer","null"]},"minLength":1}},
            "additionalProperties":false
        });
        let new = json!({
            "type":"object",
            "properties":{"values":{"type":"array","items":{"type":["integer","null"]},"minLength":2}},
            "additionalProperties":false
        });
        let report = compare_schemas(&old, &new);
        assert_eq!(report.json.result, Compatibility::Incompatible);
        // `minLength` is a sidecar refinement with no Lexicon spelling, so the published files agree
        // even though the sidecars do not. The native storage is untouched either way.
        assert_eq!(report.native_storage.result, Compatibility::Compatible);
    }

    #[test]
    fn local_reference_renames_are_structural_but_changed_targets_are_reported() {
        let old = SchemaBundle::with_definitions(
            json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/One"}},"additionalProperties":false}),
            [("One".to_owned(), json!({"type":"string"}))],
        );
        let renamed = SchemaBundle::with_definitions(
            json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/Two"}},"additionalProperties":false}),
            [("Two".to_owned(), json!({"type":"string"}))],
        );
        assert_eq!(
            compare(&old, &renamed).json.result,
            Compatibility::Compatible
        );

        let changed = SchemaBundle::with_definitions(
            json!({"type":"object","properties":{"a/b":{"$ref":"#/$defs/Two"}},"additionalProperties":false}),
            [("Two".to_owned(), json!({"type":"integer"}))],
        );
        let report = compare(&old, &changed);
        assert_eq!(report.json.result, Compatibility::Incompatible);
        assert!(
            report
                .json
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.path == "/a~1b")
        );
        assert_eq!(report, compare(&old, &changed));
    }

    #[test]
    fn supported_shapes_are_reflexive() {
        let fixtures = [
            json!({"type":"string"}),
            json!({"type":"array","items":{"type":"integer"}}),
            json!({"type":"object","properties":{"name":{"type":"string"}},"additionalProperties":true}),
            json!({"oneOf":[
                {"type":"object","properties":{"$type":{"const":"a"},"value":{"type":"object","additionalProperties":true}},"required":["$type","value"]},
                {"type":"object","properties":{"$type":{"const":"b"},"value":{"type":"object","additionalProperties":true}},"required":["$type","value"]}
            ]}),
        ];
        for schema in fixtures {
            let report = compare_schemas(&schema, &schema);
            assert_eq!(report.json.result, Compatibility::Compatible);
            assert_eq!(report.native_storage.result, Compatibility::Compatible);
            assert_eq!(
                report.lexicon.result,
                Compatibility::Compatible,
                "{:?}",
                report.lexicon.diagnostics
            );
            assert!(report.json.diagnostics.is_empty());
        }
    }

    #[test]
    fn actual_facet_union_schema_is_compatible_when_a_member_is_added() {
        fn member(id: &'static str) -> crate::facet::MemberSchema {
            crate::facet::MemberSchema {
                id,
                schema: schemars::Schema::try_from(
                    json!({"type":"object","additionalProperties":true}),
                )
                .unwrap(),
            }
        }
        let old = serde_json::to_value(crate::facet::union_schema(
            &crate::facet::UnionLayout::Adjacent {
                tag: "$type",
                content: "value",
            },
            true,
            vec![member("known")],
            &serde_json::Map::new(),
        ))
        .unwrap();
        let new = serde_json::to_value(crate::facet::union_schema(
            &crate::facet::UnionLayout::Adjacent {
                tag: "$type",
                content: "value",
            },
            true,
            vec![member("known"), member("future")],
            &serde_json::Map::new(),
        ))
        .unwrap();
        assert_eq!(
            compare_schemas(&old, &new).json.result,
            Compatibility::Compatible
        );
    }

    #[test]
    fn unchanged_and_changed_unsupported_applicators_are_not_established() {
        let schemas = [
            json!({"type":"object","patternProperties":{"^x":{"type":"string"}}}),
            json!({"type":"array","prefixItems":[{"type":"string"}]}),
            json!({"type":"array","contains":{"type":"string"}}),
            json!({"type":"object","unevaluatedProperties":false}),
            json!({"type":"object","propertyNames":{"pattern":"x"}}),
            json!({"type":"bogus"}),
        ];
        for schema in schemas {
            let report = compare_schemas(&schema, &schema);
            assert_eq!(report.json.result, Compatibility::NotEstablished);
            assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
        }
        let old = json!({"type":"array","minContains":1,"contains":{"type":"string"}});
        let new = json!({"type":"array","minContains":2,"contains":{"type":"string"}});
        assert_eq!(
            compare_schemas(&old, &new).json.result,
            Compatibility::NotEstablished
        );
    }

    #[test]
    fn union_operator_and_malformed_carriers_are_not_silently_matched() {
        let branch = |tag: &str| {
            json!({
                "type":"object",
                "properties":{"$type":{"const":tag},"value":{"type":"string"}},
                "required":["$type","value"]
            })
        };
        let duplicate = json!({"oneOf":[branch("same"), branch("same")]});
        assert_eq!(
            compare_schemas(&duplicate, &duplicate).json.result,
            Compatibility::NotEstablished
        );

        let fallback = json!({
            "type":"object",
            "properties":{"$type":{"type":"string","not":{"enum":["known"]}},"value":{"type":"string"}},
            "required":["$type","value"]
        });
        let multiple_fallbacks = json!({"oneOf":[branch("known"), fallback.clone(), fallback]});
        assert_eq!(
            compare_schemas(&multiple_fallbacks, &multiple_fallbacks)
                .json
                .result,
            Compatibility::NotEstablished
        );

        let one = json!({"oneOf":[branch("known")]});
        let any = json!({"anyOf":[branch("known")]});
        assert_eq!(
            compare_schemas(&one, &any).json.result,
            Compatibility::NotEstablished
        );
    }

    #[test]
    fn fallback_exclusion_changes_are_checked() {
        let branch = |tag: Value| {
            json!({
                "type":"object",
                "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
                "required":["$type","value"]
            })
        };
        let old = json!({"oneOf":[branch(json!({"const":"known"})), branch(json!({"type":"string","not":{"enum":["known"]}}))]});
        let changed = json!({"oneOf":[branch(json!({"const":"known"})), branch(json!({"type":"string","not":{"enum":["other"]}}))]});
        assert_eq!(
            compare_schemas(&old, &changed).json.result,
            Compatibility::NotEstablished
        );
    }

    #[test]
    fn nullable_storage_markers_preserve_canonical_nullability() {
        // Nullability is a property of the object holding the position, not of the position, so the
        // marker is tested on an object's property. A position that is itself nullable has no
        // Lexicon spelling.
        let property = |kind: &str, marker: &str, nullable: bool| {
            let type_value = if nullable {
                json!([kind, "null"])
            } else {
                json!(kind)
            };
            json!({
                "type":"object",
                "properties":{"value":{"type":type_value,"x-automerge":marker}},
                "additionalProperties":false
            })
        };
        for (marker, kind, lexicon) in [
            ("float", "string", Compatibility::Incompatible),
            ("text", "string", Compatibility::Incompatible),
            ("counter", "integer", Compatibility::Incompatible),
            ("timestamp", "string", Compatibility::Incompatible),
            // This list fixture is an array that states no items, so it is not a publishable Lexicon.
            ("list", "array", Compatibility::NotEstablished),
        ] {
            let nullable = property(kind, marker, true);
            let present = property(kind, marker, false);
            assert_eq!(
                compare_schemas(&nullable, &nullable).json.result,
                Compatibility::Compatible,
                "{marker}"
            );
            assert_eq!(
                compare_schemas(&nullable, &present).json.result,
                Compatibility::Incompatible,
                "{marker}"
            );
            // A storage form that projects keeps nullability as a name on the holding object. That
            // name appearing or disappearing is the same incompatible change as a plain property.
            assert_eq!(
                compare_schemas(&nullable, &present).lexicon.result,
                lexicon,
                "{marker}"
            );
        }
    }

    #[test]
    fn nullable_storage_marker_changes_are_checked_through_references_and_unions() {
        // The nullable position is an object's property, which is the only place a Lexicon can say
        // that a value admits null.
        let holding = |nullable: bool| {
            json!({
                "type":"object",
                "properties":{"text":{"type": if nullable {json!(["string","null"])} else {json!("string")}, "x-automerge":"text"}},
                "additionalProperties":false
            })
        };
        let old_value = holding(true);
        let new_value = holding(false);
        let old = SchemaBundle::with_definitions(old_value.clone(), []);
        let new = SchemaBundle::with_definitions(new_value.clone(), []);
        let reference_report = compare(&old, &new);
        assert_eq!(reference_report.json.result, Compatibility::Incompatible);
        assert_eq!(
            reference_report.native_storage.result,
            Compatibility::Compatible
        );
        assert_eq!(reference_report.lexicon.result, Compatibility::Incompatible);

        let branch = |value: Value| {
            json!({
                "type":"object",
                "properties":{
                    "$type":{"const":"value"},
                    "value":value
                },
                "required":["$type", "value"],
                "additionalProperties":false
            })
        };
        let old_union = json!({"oneOf":[branch(old_value)]});
        let new_union = json!({"oneOf":[branch(new_value)]});
        let union_report = compare_schemas(&old_union, &new_union);
        assert_eq!(union_report.json.result, Compatibility::Incompatible);
        assert_eq!(
            union_report.native_storage.result,
            Compatibility::Compatible
        );
        // A union file names the member id and drops the payload, so the payload's nullability is
        // that member's own lexicon, not a difference this union file can show.
        assert_eq!(union_report.lexicon.result, Compatibility::Compatible);
    }

    #[test]
    fn canonical_bytes_are_the_protocol_object_and_an_encoding_annotation_is_not_bytes() {
        // `contentEncoding` annotates a string. It does not select storage, so a string carrying it
        // is a string in every dimension — sniffing it as bytes would report a string field
        // storage-compatible with a bytes field.
        let annotated = json!({"type":"string","contentEncoding":"base64"});
        assert_eq!(
            compare_schemas(&annotated, &json!({"type":"string"}))
                .json
                .result,
            Compatibility::Compatible
        );
        assert_eq!(
            compare_schemas(&annotated, &annotated)
                .native_storage
                .result,
            Compatibility::Compatible
        );

        let bytes = json!({
            "type":"object",
            "x-automerge":"bytes",
            "properties":{"$bytes":{"type":"string"}},
            "required":["$bytes"],
            "additionalProperties":false
        });
        assert_eq!(
            compare_schemas(&bytes, &bytes).lexicon.result,
            Compatibility::Compatible,
            "{:?}",
            compare_schemas(&bytes, &bytes).lexicon.diagnostics
        );
        // A plain number is the float opt-out, and it is not a Lexicon type.
        let plain_number = json!({"type":"number","x-automerge":"number"});
        assert_eq!(
            compare_schemas(&plain_number, &plain_number).lexicon.result,
            Compatibility::NotEstablished
        );
    }

    #[test]
    fn lexicon_formats_nullable_unions_and_reference_identity_are_bounded() {
        let timestamp = json!({"type":"string","format":"date-time"});
        let uri = json!({"type":"string","format":"uri"});
        let unknown = json!({"type":"string","format":"made-up"});
        assert_eq!(
            compare_schemas(&timestamp, &uri).lexicon.result,
            Compatibility::Incompatible
        );
        // A format Lexicon does not know stays in the sidecar, so the published files still agree.
        assert_eq!(
            compare_schemas(&unknown, &unknown).lexicon.result,
            Compatibility::Compatible
        );

        // A nullable position is an object's `nullable` list, not a type array, so a type array is
        // not a publishable Lexicon.
        let nullable = json!({"type":["string","null"]});
        assert_eq!(
            compare_schemas(&nullable, &nullable).lexicon.result,
            Compatibility::NotEstablished
        );

        let scalar_union = json!({"oneOf":[{"type":"string"},{"type":"integer"}]});
        assert_eq!(
            compare_schemas(&scalar_union, &scalar_union).lexicon.result,
            Compatibility::NotEstablished
        );

        let old = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/oldName"}),
            [("oldName".to_owned(), json!({"type":"string"}))],
        );
        let renamed = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/newName"}),
            [("newName".to_owned(), json!({"type":"string"}))],
        );
        let renamed_report = compare(&old, &renamed);
        assert_eq!(renamed_report.json.result, Compatibility::Compatible);
        assert_eq!(
            renamed_report.native_storage.result,
            Compatibility::Compatible
        );
        assert_eq!(renamed_report.lexicon.result, Compatibility::Incompatible);
    }

    #[test]
    fn a_timestamp_is_a_timestamp_and_a_unit_annotation_is_not_a_storage_kind() {
        // Nothing the crate emits states a unit. Two timestamps are the same storage kind; the unit
        // annotation is sidecar metadata and does not manufacture a native incompatibility.
        let timestamp_ms = json!({"type":"string","format":"date-time","unit":"milliseconds"});
        let timestamp_seconds = json!({"type":"string","format":"date-time","unit":"seconds"});
        assert_eq!(
            compare_schemas(&timestamp_ms, &timestamp_seconds)
                .native_storage
                .result,
            Compatibility::Compatible
        );

        let signed = json!({"type":"integer","format":"int64","minimum":-9007199254740991i64});
        let unsigned = json!({"type":"integer","format":"uint64","minimum":0});
        assert_eq!(
            compare_schemas(&signed, &unsigned).native_storage.result,
            Compatibility::Incompatible
        );
        assert_eq!(
            compare_schemas(&signed, &unsigned).json.result,
            Compatibility::Incompatible
        );

        let old = json!({"type":"array","items":{"type":"string"}});
        let new = json!({"type":"array","items":{"type":"integer"}});
        assert_eq!(
            compare_schemas(&old, &new).json.result,
            Compatibility::Incompatible
        );
    }

    #[test]
    fn map_openness_and_null_missing_are_reported() {
        let open = json!({"type":"object","additionalProperties":true});
        let closed = json!({"type":"object","additionalProperties":false});
        assert_eq!(
            compare_schemas(&open, &closed).json.result,
            Compatibility::Incompatible
        );

        let optional = json!({"type":"object","properties":{"value":{"type":["string","null"]}},"additionalProperties":false});
        let required = json!({"type":"object","properties":{"value":{"type":["string","null"]}},"required":["value"],"additionalProperties":false});
        assert_eq!(
            compare_schemas(&optional, &required).json.result,
            Compatibility::Incompatible
        );
    }

    #[test]
    #[should_panic(expected = "conflicting schemas supplied")]
    fn conflicting_definition_constructor_does_not_last_win() {
        let _unused = SchemaBundle::with_definitions(
            json!({"type":"string"}),
            [
                ("Thing".to_owned(), json!({"type":"string"})),
                ("Thing".to_owned(), json!({"type":"integer"})),
            ],
        );
    }

    #[test]
    fn facet_bundle_adapter_reports_all_three_dimensions_for_a_real_change() {
        let schema = |value| schemars::Schema::try_from(value).unwrap();
        let old_universe = crate::facet::Universe {
            facets: vec![crate::facet::FacetDecl {
                id: "org.example.root".to_owned(),
                name: "Root".to_owned(),
                schema: schema(
                    json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
                ),
                flags: Vec::new(),
            }],
            types: Vec::new(),
        };
        let new_universe = crate::facet::Universe {
            facets: vec![crate::facet::FacetDecl {
                id: "org.example.root".to_owned(),
                name: "Root".to_owned(),
                schema: schema(
                    json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}),
                ),
                flags: Vec::new(),
            }],
            types: Vec::new(),
        };
        let old_bundle = crate::facet::Bundle::new(&old_universe, Vec::new());
        let new_bundle = crate::facet::Bundle::new(&new_universe, Vec::new());
        let report = compare_facet_bundles(&old_bundle, "Root", &new_bundle, "Root").unwrap();
        assert_eq!(report.json.result, Compatibility::Incompatible);
        assert_eq!(report.native_storage.result, Compatibility::Incompatible);
        assert_eq!(report.lexicon.result, Compatibility::Incompatible);
    }

    #[test]
    fn reference_backed_union_has_independent_json_native_and_lexicon_results() {
        let branch = |tag: Value| {
            json!({
                "type":"object",
                "properties":{"$type":tag,"value":{"type":"object","additionalProperties":true}},
                "required":["$type","value"]
            })
        };
        let old = SchemaBundle::with_definitions(
            json!({"oneOf":[{"$ref":"#/$defs/Known"},{"$ref":"#/$defs/Fallback"}]}),
            [
                ("Known".to_owned(), branch(json!({"const":"known"}))),
                (
                    "Fallback".to_owned(),
                    branch(json!({"type":"string","not":{"enum":["known"]}})),
                ),
            ],
        );
        let new = SchemaBundle::with_definitions(
            json!({"oneOf":[{"$ref":"#/$defs/Known"},{"$ref":"#/$defs/Future"},{"$ref":"#/$defs/Fallback"}]}),
            [
                ("Known".to_owned(), branch(json!({"const":"known"}))),
                ("Future".to_owned(), branch(json!({"const":"future"}))),
                (
                    "Fallback".to_owned(),
                    branch(json!({"type":"string","not":{"enum":["known","future"]}})),
                ),
            ],
        );
        let report = compare(&old, &new);
        assert_eq!(report.json.result, Compatibility::Compatible);
        assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
        assert_eq!(report.lexicon.result, Compatibility::Compatible);
    }

    #[test]
    fn ordinary_enum_const_and_stable_union_tags_have_distinct_identity() {
        let old_scalar = json!({"type":"string","enum":["old","other"],"const":"old"});
        let new_scalar = json!({"type":"string","enum":["new","other"],"const":"new"});
        let report = compare_schemas(&old_scalar, &new_scalar);
        assert_eq!(report.json.result, Compatibility::Incompatible);
        assert_eq!(report.native_storage.result, Compatibility::Compatible);
        assert_eq!(report.lexicon.result, Compatibility::Incompatible);

        let branch = |tag: &str| {
            json!({
                "type":"object",
                "properties":{"$type":{"const":tag},"value":{"type":"string"}},
                "required":["$type","value"]
            })
        };
        let old_union = json!({"oneOf":[branch("org.example.old")]});
        let new_union = json!({"oneOf":[branch("org.example.new")]});
        let report = compare_schemas(&old_union, &new_union);
        assert_eq!(report.json.result, Compatibility::Incompatible);
        assert_eq!(report.native_storage.result, Compatibility::Incompatible);
        assert_eq!(report.lexicon.result, Compatibility::Incompatible);
    }

    #[test]
    fn malformed_markers_and_union_carriers_refuse_all_dimensions() {
        let malformed_marker = json!({"type":"integer","x-automerge":"bytes"});
        let report = compare_schemas(&malformed_marker, &malformed_marker);
        assert_eq!(report.json.result, Compatibility::NotEstablished);
        assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
        assert_eq!(report.lexicon.result, Compatibility::NotEstablished);

        let dollar_property = json!({
            "type":"object",
            "properties":{"$private":{"type":"string"}},
            "additionalProperties":false
        });
        let report = compare_schemas(&dollar_property, &dollar_property);
        assert_eq!(report.json.result, Compatibility::Compatible);
        assert_eq!(report.native_storage.result, Compatibility::Compatible);
        assert_eq!(report.lexicon.result, Compatibility::NotEstablished);

        let branch = json!({
            "type":"object",
            "properties":{"$type":{"const":"same"},"value":{"type":"string"}},
            "required":["$type","value"]
        });
        let malformed = json!({"oneOf":[branch.clone(), branch]});
        let report = compare_schemas(&malformed, &malformed);
        assert_eq!(report.json.result, Compatibility::NotEstablished);
        assert_eq!(report.native_storage.result, Compatibility::NotEstablished);
        assert_eq!(report.lexicon.result, Compatibility::Compatible);
    }
}
