//! Conservative schema evolution checks for the core Cabinet representation.
//!
//! This module deliberately does not decide whether a schema may be published.  It compares two
//! already-produced schema bundles and reports three independent questions: can the JSON reader and
//! writer understand one another, can the existing native representation be retained, and does the
//! canonical/Lexicon-facing shape evolve safely?  A higher-level `SchemaRepo` can apply its own
//! admission policy to the report.
//!
//! The checker understands the JSON Schema subset emitted by Cabinet.  Unsupported applicators are
//! reported as `not established`; they are never treated as compatible merely because two arbitrary
//! JSON documents happen to compare equal.

use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

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
    if let Err(error) = old.validate() {
        return Some(not_established_report(
            "/$defs",
            format!("old facet bundle is invalid: {error}"),
        ));
    }
    if let Err(error) = new.validate() {
        return Some(not_established_report(
            "/$defs",
            format!("new facet bundle is invalid: {error}"),
        ));
    }
    let old_document = serde_json::to_value(old.document(old_facet)?).ok()?;
    let new_document = serde_json::to_value(new.document(new_facet)?).ok()?;
    Some(compare_schemas(&old_document, &new_document))
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

fn not_established_report(path: &str, reason: String) -> CompatibilityReport {
    let outcome = CompatibilityOutcome {
        result: Compatibility::NotEstablished,
        diagnostics: vec![Diagnostic {
            path: path.to_owned(),
            reason,
        }],
    };
    CompatibilityReport {
        json: outcome.clone(),
        native_storage: outcome.clone(),
        lexicon: outcome,
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

/// Project the supported canonical representation.  Validation refinements and storage annotations
/// are dropped only after their canonical wire meaning has been accounted for.
fn compare_lexicon(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    let mut validation = Findings::default();
    let old_valid =
        validate_lexicon_schema(old, &old.root, "/", &mut validation, &mut BTreeSet::new());
    let new_valid =
        validate_lexicon_schema(new, &new.root, "/", &mut validation, &mut BTreeSet::new());
    for (name, schema) in &old.definitions {
        validate_lexicon_schema(
            old,
            schema,
            &child("/$defs", name),
            &mut validation,
            &mut BTreeSet::new(),
        );
    }
    for (name, schema) in &new.definitions {
        validate_lexicon_schema(
            new,
            schema,
            &child("/$defs", name),
            &mut validation,
            &mut BTreeSet::new(),
        );
    }
    if !old_valid || !new_valid || validation.not_established {
        return validation.outcome();
    }
    let (Ok(old_root), Ok(new_root)) =
        (lexicon_projection(&old.root), lexicon_projection(&new.root))
    else {
        return CompatibilityOutcome {
            result: Compatibility::NotEstablished,
            diagnostics: vec![Diagnostic {
                path: "/".to_owned(),
                reason: "Lexicon projection is not established for an unsupported schema construct"
                    .to_owned(),
            }],
        };
    };
    let old_definitions: Result<BTreeMap<_, _>, ()> = old
        .definitions
        .iter()
        .map(|(name, schema)| Ok((name.clone(), lexicon_projection(schema)?)))
        .collect();
    let new_definitions: Result<BTreeMap<_, _>, ()> = new
        .definitions
        .iter()
        .map(|(name, schema)| Ok((name.clone(), lexicon_projection(schema)?)))
        .collect();
    let (Ok(old_definitions), Ok(new_definitions)) = (old_definitions, new_definitions) else {
        return CompatibilityOutcome {
            result: Compatibility::NotEstablished,
            diagnostics: vec![Diagnostic {
                path: "/$defs".to_owned(),
                reason: "Lexicon projection is not established for an unsupported definition"
                    .to_owned(),
            }],
        };
    };
    compare_json(
        &SchemaBundle::with_definitions(old_root, old_definitions),
        &SchemaBundle::with_definitions(new_root, new_definitions),
    )
}

fn validate_lexicon_schema(
    bundle: &SchemaBundle,
    schema: &Value,
    path: &str,
    findings: &mut Findings,
    seen: &mut BTreeSet<String>,
) -> bool {
    let Some(schema) = resolve(bundle, schema, path, "lexicon", findings) else {
        return false;
    };
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if !seen.insert(reference.to_owned()) {
            findings.not_established(path, "Lexicon reference cycle is not established");
            return false;
        }
        return validate_lexicon_schema(bundle, schema, path, findings, seen);
    }
    if is_open_fallback_tag(schema) {
        return true;
    }
    if unsupported_keyword(schema).is_some() {
        findings.not_established(
            path,
            "Lexicon projection uses an unsupported schema construct",
        );
        return false;
    }
    if let Some(marker) = schema.get("x-automerge").and_then(Value::as_str) {
        if !marker_is_consistent(schema, marker) {
            findings.not_established(
                path,
                "Lexicon storage marker contradicts the declared JSON type",
            );
            return false;
        }
        return true;
    }
    if schema.get("contentEncoding").is_some()
        && !schema
            .get("type")
            .is_none_or(|type_value| type_value_accepts(type_value, "string"))
    {
        findings.not_established(path, "bytes encoding contradicts the declared JSON type");
        return false;
    }
    if let Some(format) = schema.get("format").and_then(Value::as_str)
        && !matches!(format, "date-time" | "uri" | "uuid" | "cid" | "datetime")
    {
        findings.not_established(
            path,
            "Lexicon format is not in the supported canonical allowlist",
        );
        return false;
    }
    let mut valid = true;
    if let Some(branches) = union_branches(schema) {
        for (index, branch) in branches.iter().enumerate() {
            let branch_path = format!("{path}<union:{index}>");
            let Some(resolved) = resolve(bundle, branch, &branch_path, "lexicon", findings) else {
                valid = false;
                continue;
            };
            let Some((tag, value)) = carrier_parts(resolved) else {
                findings.not_established(
                    &branch_path,
                    "Lexicon scalar/list unions require adjacent object wrappers",
                );
                valid = false;
                continue;
            };
            if tag.get("const").and_then(Value::as_str).is_none() && !is_open_fallback_tag(tag) {
                findings.not_established(&branch_path, "Lexicon union tag is not canonical");
                valid = false;
            }
            valid &=
                validate_lexicon_schema(bundle, tag, &child(&branch_path, "$type"), findings, seen);
            valid &= validate_lexicon_schema(
                bundle,
                value,
                &child(&branch_path, "value"),
                findings,
                seen,
            );
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        let carrier = carrier_parts(schema).is_some();
        for (name, property) in properties {
            if name.starts_with('$') && !matches!(name.as_str(), "$type" | "$bytes" | "$link") {
                findings.not_established(
                    child(path, name),
                    "application-defined `$` property is not canonical",
                );
                valid = false;
            }
            if carrier
                && name == "$type"
                && !property.get("const").is_some_and(Value::is_string)
                && !is_open_fallback_tag(property)
            {
                findings.not_established(child(path, name), "union carrier tag is not canonical");
                valid = false;
            }
            valid &= validate_lexicon_schema(bundle, property, &child(path, name), findings, seen);
        }
    }
    if let Some(items) = schema.get("items") {
        valid &= validate_lexicon_schema(bundle, items, &child(path, "<items>"), findings, seen);
    }
    if let Some(additional) = schema.get("additionalProperties")
        && additional.is_object()
    {
        valid &= validate_lexicon_schema(
            bundle,
            additional,
            &child(path, "<additionalProperties>"),
            findings,
            seen,
        );
    }
    valid
}

fn lexicon_projection(schema: &Value) -> Result<Value, ()> {
    match schema {
        Value::Bool(true) => Ok(Value::Bool(true)),
        Value::Bool(false) => Ok(Value::Bool(false)),
        Value::Array(values) => values
            .iter()
            .map(lexicon_projection)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(object) => {
            if is_open_fallback_tag(schema) {
                return Ok(schema.clone());
            }
            if unsupported_keyword(schema).is_some() {
                return Err(());
            }
            if let Some(reference) = object.get("$ref") {
                let reference = reference.as_str().ok_or(())?;
                if !(reference.starts_with("#/$defs/") || reference.starts_with("#/definitions/")) {
                    return Err(());
                }
                return Ok(json!({"$ref": reference}));
            }
            if let Some(marker) = object.get("x-automerge").and_then(Value::as_str) {
                return lexicon_codec_projection(object, marker);
            }
            if let Some(encoding) = object.get("contentEncoding") {
                if encoding.as_str() != Some("base64") {
                    return Err(());
                }
                return Ok(canonical_bytes_schema(object));
            }
            if let Some(format) = object.get("format").and_then(Value::as_str)
                && !matches!(format, "date-time" | "uri" | "uuid" | "cid" | "datetime")
            {
                return Err(());
            }
            let mut projected = Map::new();
            for (key, value) in object {
                if matches!(
                    key.as_str(),
                    "$schema"
                        | "$id"
                        | "title"
                        | "description"
                        | "default"
                        | "examples"
                        | "x-automerge"
                        | "contentEncoding"
                        | "contentMediaType"
                        | "unit"
                        | "x-unit"
                        | "x-automerge-unit"
                        | "minimum"
                        | "maximum"
                        | "exclusiveMinimum"
                        | "exclusiveMaximum"
                        | "multipleOf"
                        | "minLength"
                        | "maxLength"
                        | "pattern"
                        | "minItems"
                        | "maxItems"
                        | "uniqueItems"
                        | "minProperties"
                        | "maxProperties"
                        | "enum"
                        | "const"
                ) {
                    continue;
                }
                if key == "not" {
                    return Err(());
                }
                if key.starts_with('$') && !matches!(key.as_str(), "$ref" | "$defs") {
                    return Err(());
                }
                if key == "properties" || key == "$defs" {
                    let entries = value.as_object().ok_or(())?;
                    let mut projected_entries = Map::new();
                    let carrier = carrier_parts(schema).is_some();
                    for (name, entry) in entries {
                        if key == "properties"
                            && name.starts_with('$')
                            && !matches!(name.as_str(), "$type" | "$bytes" | "$link")
                        {
                            return Err(());
                        }
                        let projected_entry = if carrier && name == "$type" {
                            lexicon_tag_projection(entry)?
                        } else {
                            lexicon_projection(entry)?
                        };
                        projected_entries.insert(name.clone(), projected_entry);
                    }
                    projected.insert(key.clone(), Value::Object(projected_entries));
                } else if key == "additionalProperties" && value.is_object() {
                    projected.insert(key.clone(), Value::Bool(true));
                } else if key == "type" {
                    project_type(value, &mut projected)?;
                } else {
                    projected.insert(key.clone(), lexicon_projection(value)?);
                }
            }
            Ok(Value::Object(projected))
        }
        _ => Ok(schema.clone()),
    }
}

fn lexicon_tag_projection(schema: &Value) -> Result<Value, ()> {
    if is_open_fallback_tag(schema) {
        return Ok(schema.clone());
    }
    let Some(tag) = schema.get("const").and_then(Value::as_str) else {
        return Err(());
    };
    Ok(json!({"const": tag}))
}

fn marker_is_consistent(schema: &Value, marker: &str) -> bool {
    let Some(type_value) = schema.get("type") else {
        return true;
    };
    let allowed = match marker {
        "bytes" | "float" | "timestamp" | "text" => "string",
        "counter" | "number" => "number",
        "list" => "array",
        _ => return false,
    };
    type_value_accepts(type_value, allowed)
}

fn type_value_accepts(type_value: &Value, allowed: &str) -> bool {
    match type_value {
        Value::String(kind) => kind == allowed,
        Value::Array(kinds) => {
            kinds.iter().all(|kind| {
                kind.as_str()
                    .is_some_and(|kind| kind == allowed || kind == "null")
            }) && kinds.iter().any(|kind| kind.as_str() == Some(allowed))
        }
        _ => false,
    }
}

fn project_type(value: &Value, projected: &mut Map<String, Value>) -> Result<(), ()> {
    let Some(types) = value.as_array() else {
        let kind = value.as_str().ok_or(())?;
        if !VALID_TYPES.contains(&kind) {
            return Err(());
        }
        projected.insert("type".to_owned(), Value::String(kind.to_owned()));
        return Ok(());
    };
    if types.is_empty()
        || types.iter().any(|kind| {
            kind.as_str()
                .is_none_or(|kind| !VALID_TYPES.contains(&kind))
        })
    {
        return Err(());
    }
    projected.insert("type".to_owned(), Value::Array(types.clone()));
    Ok(())
}

fn lexicon_codec_projection(object: &Map<String, Value>, marker: &str) -> Result<Value, ()> {
    match marker {
        "bytes" => Ok(canonical_bytes_schema(object)),
        "float" => Ok(canonical_decimal_schema(object)),
        "timestamp" => Ok(canonical_nullable_schema(
            object,
            json!({"type":"string", "format":"date-time"}),
        )),
        "text" => Ok(canonical_nullable_schema(object, json!({"type":"string"}))),
        "counter" | "number" => Ok(canonical_nullable_schema(object, json!({"type":"number"}))),
        "list" => Ok(canonical_nullable_schema(object, json!({"type":"array"}))),
        _ => Err(()),
    }
}

fn canonical_nullable_schema(object: &Map<String, Value>, mut schema: Value) -> Value {
    let nullable = object
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|types| types.iter().any(|kind| kind.as_str() == Some("null")));
    if nullable {
        let kind = schema
            .get("type")
            .and_then(Value::as_str)
            .expect("canonical marker projection always has a scalar type");
        schema["type"] = json!([kind, "null"]);
    }
    schema
}

fn canonical_bytes_schema(object: &Map<String, Value>) -> Value {
    let nullable = object
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|types| types.iter().any(|kind| kind.as_str() == Some("null")));
    let byte_schema = json!({
        "type":"object",
        "properties":{"$bytes":{"type":"string"}},
        "required":["$bytes"],
        "additionalProperties":false
    });
    if nullable {
        let mut byte_schema = byte_schema;
        byte_schema["type"] = json!(["object", "null"]);
        byte_schema
    } else {
        byte_schema
    }
}

fn canonical_decimal_schema(object: &Map<String, Value>) -> Value {
    let nullable = object
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|types| types.iter().any(|kind| kind.as_str() == Some("null")));
    let mut schema = json!({"type":"string", "format":"decimal"});
    if nullable {
        schema["type"] = json!(["string", "null"]);
    }
    schema
}

fn compare_native(old: &SchemaBundle, new: &SchemaBundle) -> CompatibilityOutcome {
    let mut findings = Findings::default();
    scan_unsupported_bundle(old, "old", &mut findings);
    scan_unsupported_bundle(new, "new", &mut findings);
    if findings.not_established {
        return findings.outcome();
    }
    native_at(
        old,
        new,
        &old.root,
        &new.root,
        "/",
        &mut findings,
        &mut BTreeSet::new(),
    );
    findings.outcome()
}

fn contradictory_annotation(schema: &Value) -> Option<String> {
    if let Some(marker) = schema.get("x-automerge") {
        let Some(marker) = marker.as_str() else {
            return Some("x-automerge marker is not a string".to_owned());
        };
        if !matches!(
            marker,
            "bytes" | "float" | "timestamp" | "text" | "counter" | "number" | "list"
        ) {
            return Some(format!("unknown x-automerge storage `{marker}`"));
        }
        if !marker_is_consistent(schema, marker) {
            return Some("x-automerge marker contradicts the declared JSON type".to_owned());
        }
    }
    if schema.get("contentEncoding").is_some()
        && schema.get("contentEncoding").and_then(Value::as_str) != Some("base64")
    {
        return Some("unsupported content encoding annotation".to_owned());
    }
    if schema.get("contentEncoding").is_some()
        && !schema
            .get("type")
            .is_none_or(|type_value| type_value_accepts(type_value, "string"))
    {
        return Some("bytes encoding contradicts the declared JSON type".to_owned());
    }
    None
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
        let Some((tag_schema, _value_schema)) = carrier_parts(branch) else {
            findings.not_established(
                &branch_path,
                format!("{direction}: union branch is not an adjacent carrier"),
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

fn carrier_parts(schema: &Value) -> Option<(&Value, &Value)> {
    if !has_type(schema, "object") {
        return None;
    }
    let properties = schema.get("properties")?.as_object()?;
    let tag = properties.get("$type")?;
    let value = properties.get("value")?;
    let required = required(schema);
    if required.len() != 2 || !required.contains("$type") || !required.contains("value") {
        return None;
    }
    if properties.len() != 2 {
        return None;
    }
    Some((tag, value))
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
    let Some((reader_tag, reader_value)) = carrier_parts(reader) else {
        findings.not_established(
            path,
            format!("{direction}: reader union carrier is malformed"),
        );
        return;
    };
    let Some((writer_tag, writer_value)) = carrier_parts(writer) else {
        findings.not_established(
            path,
            format!("{direction}: writer union carrier is malformed"),
        );
        return;
    };

    if required(reader) != required(writer) || additional_kind(reader) != additional_kind(writer) {
        findings.not_established(
            path,
            format!("{direction}: union carrier requiredness or openness changed"),
        );
        return;
    }
    if let (Additional::Schema(reader_schema), Additional::Schema(writer_schema)) =
        (additional(reader), additional(writer))
    {
        accepts(
            reader_bundle,
            writer_bundle,
            reader_schema,
            writer_schema,
            &child(path, "<additionalProperties>"),
            direction,
            findings,
        );
    }
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
    accepts(
        reader_bundle,
        writer_bundle,
        reader_value,
        writer_value,
        &child(path, "value"),
        direction,
        findings,
    );
}

fn additional_kind(schema: &Value) -> &'static str {
    match additional(schema) {
        Additional::Allowed => "allowed",
        Additional::Closed => "closed",
        Additional::Schema(_) => "schema",
        Additional::Unknown => "unknown",
    }
}

fn native_at(
    old_bundle: &SchemaBundle,
    new_bundle: &SchemaBundle,
    old: &Value,
    new: &Value,
    path: &str,
    findings: &mut Findings,
    seen: &mut BTreeSet<(String, String)>,
) {
    let old_ref = old.get("$ref").and_then(Value::as_str).unwrap_or_default();
    let new_ref = new.get("$ref").and_then(Value::as_str).unwrap_or_default();
    if (!old_ref.is_empty() || !new_ref.is_empty())
        && !seen.insert((old_ref.to_owned(), new_ref.to_owned()))
    {
        return;
    }
    let Some(old) = resolve(old_bundle, old, path, "native", findings) else {
        return;
    };
    let Some(new) = resolve(new_bundle, new, path, "native", findings) else {
        return;
    };
    if is_open_fallback_tag(old) || is_open_fallback_tag(new) {
        return;
    }
    if let Some(keyword) = unsupported_keyword(old).or_else(|| unsupported_keyword(new)) {
        findings.not_established(
            path,
            format!("native: unsupported schema keyword or type context `{keyword}`"),
        );
        return;
    }
    if let Some(reason) = contradictory_annotation(old).or_else(|| contradictory_annotation(new)) {
        findings.not_established(path, format!("native: {reason}"));
        return;
    }
    let old_operator = union_operator(old);
    let new_operator = union_operator(new);
    if (old_operator.is_some() || new_operator.is_some())
        && (old_operator == Some("invalid")
            || new_operator == Some("invalid")
            || old_operator != new_operator)
    {
        findings.not_established(path, "native: union operator or carrier shape changed");
        return;
    }
    if let Some(old_branches) = union_branches(old)
        && !validate_union(old_bundle, old_branches, path, "native", findings)
    {
        return;
    }
    if let Some(new_branches) = union_branches(new)
        && !validate_union(new_bundle, new_branches, path, "native", findings)
    {
        return;
    }
    if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new)) {
        let old_tags: BTreeSet<_> = old_branches
            .iter()
            .filter_map(|branch| branch_tag_resolved(old_bundle, branch, path, "native", findings))
            .collect();
        let new_tags: BTreeSet<_> = new_branches
            .iter()
            .filter_map(|branch| branch_tag_resolved(new_bundle, branch, path, "native", findings))
            .collect();
        if old_tags != new_tags {
            let has_fallback = old_branches.iter().any(|branch| {
                branch_tag_resolved(old_bundle, branch, path, "native", findings).is_none()
            }) && new_branches.iter().any(|branch| {
                branch_tag_resolved(new_bundle, branch, path, "native", findings).is_none()
            });
            if has_fallback && old_tags.is_subset(&new_tags) {
                findings.not_established(
                    path,
                    "native: open union member addition cannot be proven in storage",
                );
            } else {
                findings.incompatible(path, "native: stable union member tags changed");
            }
            return;
        }
    }
    let old_kind = native_kind(old, path, findings);
    let new_kind = native_kind(new, path, findings);
    if old_kind.is_none()
        && new_kind.is_none()
        && union_branches(old).is_none()
        && union_branches(new).is_none()
    {
        findings.not_established(path, "native storage kind is not established");
        return;
    }
    if old_kind != new_kind {
        match (old_kind, new_kind) {
            (Some(old_kind), Some(new_kind)) => findings.incompatible(
                path,
                format!("native storage changed from `{old_kind}` to `{new_kind}`"),
            ),
            _ => findings
                .not_established(path, "native storage declaration is incomplete or unknown"),
        }
        return;
    }

    if let (Some(old_properties), Some(new_properties)) = (
        old.get("properties").and_then(Value::as_object),
        new.get("properties").and_then(Value::as_object),
    ) {
        for (name, old_property) in old_properties {
            if let Some(new_property) = new_properties.get(name) {
                native_at(
                    old_bundle,
                    new_bundle,
                    old_property,
                    new_property,
                    &child(path, name),
                    findings,
                    seen,
                );
            }
        }
    }
    if let (Some(old_items), Some(new_items)) = (old.get("items"), new.get("items")) {
        native_at(
            old_bundle,
            new_bundle,
            old_items,
            new_items,
            &child(path, "<items>"),
            findings,
            seen,
        );
    }
    match (additional(old), additional(new)) {
        (Additional::Schema(old_schema), Additional::Schema(new_schema)) => native_at(
            old_bundle,
            new_bundle,
            old_schema,
            new_schema,
            &child(path, "<additionalProperties>"),
            findings,
            seen,
        ),
        (Additional::Schema(_), Additional::Allowed)
        | (Additional::Allowed, Additional::Schema(_))
        | (Additional::Schema(_), Additional::Unknown)
        | (Additional::Unknown, Additional::Schema(_)) => findings.not_established(
            child(path, "<additionalProperties>"),
            "typed-map storage cannot be proven across an undeclared or unrestricted map",
        ),
        _ => {}
    }
    if let (Some(old_branches), Some(new_branches)) = (union_branches(old), union_branches(new)) {
        for old_branch in old_branches {
            let old_tag = branch_tag_resolved(old_bundle, old_branch, path, "native", findings);
            if let Some(tag) = old_tag {
                if let Some(new_branch) = new_branches.iter().find(|branch| {
                    branch_tag_resolved(new_bundle, branch, path, "native", findings)
                        == Some(tag.clone())
                }) {
                    native_at(
                        old_bundle, new_bundle, old_branch, new_branch, path, findings, seen,
                    );
                }
            } else if let Some(new_branch) = new_branches.iter().find(|branch| {
                branch_tag_resolved(new_bundle, branch, path, "native", findings).is_none()
            }) {
                native_at(
                    old_bundle, new_bundle, old_branch, new_branch, path, findings, seen,
                );
            }
        }
    }
}

fn native_kind(schema: &Value, path: &str, findings: &mut Findings) -> Option<String> {
    if let Some(marker) = schema.get("x-automerge") {
        let Some(marker) = marker.as_str() else {
            findings.not_established(path, "x-automerge is not a string declaration");
            return None;
        };
        if !matches!(
            marker,
            "bytes" | "timestamp" | "counter" | "float" | "text" | "number" | "list"
        ) {
            findings.not_established(path, format!("unknown x-automerge storage `{marker}`"));
        }
        return Some(format!("x-automerge:{marker}"));
    }
    if schema
        .get("contentEncoding")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("base64"))
    {
        return Some("bytes".to_owned());
    }
    for key in ["unit", "x-unit", "x-automerge-unit"] {
        if schema.get(key).is_some_and(|unit| !unit.is_string()) {
            findings.not_established(path, format!("native unit `{key}` is not a string"));
            return None;
        }
    }
    if let Some(unit) = ["unit", "x-unit", "x-automerge-unit"]
        .into_iter()
        .find_map(|key| schema.get(key).and_then(Value::as_str))
    {
        if schema.get("format").and_then(Value::as_str) == Some("date-time") {
            return Some(format!("timestamp:{unit}"));
        }
        return Some(format!("unit:{unit}"));
    }
    if schema.get("format").and_then(Value::as_str) == Some("date-time") {
        return Some("timestamp".to_owned());
    }
    if let Some(constant) = schema.get("const") {
        let kind = match constant {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };
        return Some(format!("const:{kind}"));
    }
    let (kind, nullable) = match schema.get("type") {
        Some(Value::String(kind)) => (kind.as_str(), false),
        Some(Value::Array(kinds)) => {
            let non_null: Vec<_> = kinds
                .iter()
                .filter_map(Value::as_str)
                .filter(|kind| *kind != "null")
                .collect();
            if non_null.len() != 1 {
                return None;
            }
            (non_null[0], kinds.len() != non_null.len())
        }
        _ => return None,
    };
    let prefix = if nullable { "nullable:" } else { "" };
    if matches!(kind, "integer" | "number")
        && let Some(format) = schema.get("format").and_then(Value::as_str)
    {
        return Some(format!("{prefix}{kind}:{format}"));
    }
    Some(format!("{prefix}{kind}"))
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

#[cfg(test)]
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
        assert_eq!(
            compare_schemas(&old_union, &new_union).json.result,
            Compatibility::Compatible
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
        let new = json!({"type":"string", "x-automerge":"bytes", "contentEncoding":"base64"});
        let report = compare_schemas(&old, &new);
        assert_eq!(report.native_storage.result, Compatibility::Incompatible);
        assert_eq!(report.json.result, Compatibility::Compatible);
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
            "additionalProperties":{"type":"string", "x-automerge":"bytes"}
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
        assert_eq!(report.lexicon.result, Compatibility::Compatible);
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
            json!({"type":"array","items":{"type":["integer","null"]}}),
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
            assert_eq!(report.lexicon.result, Compatibility::Compatible);
            assert!(report.json.diagnostics.is_empty());
        }
    }

    #[cfg(feature = "schemars")]
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
            "$type",
            "value",
            true,
            vec![member("known")],
        ))
        .unwrap();
        let new = serde_json::to_value(crate::facet::union_schema(
            "$type",
            "value",
            true,
            vec![member("known"), member("future")],
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
            assert_eq!(report.lexicon.result, Compatibility::NotEstablished);
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
        let markers = [
            ("bytes", "string"),
            ("float", "string"),
            ("timestamp", "string"),
            ("text", "string"),
            ("counter", "number"),
            ("number", "number"),
            ("list", "array"),
        ];

        for (marker, kind) in markers {
            let nullable = json!({"type":[kind, "null"], "x-automerge":marker});
            let nonnullable = json!({"type":kind, "x-automerge":marker});

            for equal_schema in [&nullable, &nonnullable] {
                let equal = compare_schemas(equal_schema, equal_schema);
                assert_eq!(
                    equal.json.result,
                    Compatibility::Compatible,
                    "{marker} JSON"
                );
                assert_eq!(
                    equal.native_storage.result,
                    Compatibility::Compatible,
                    "{marker} native"
                );
                assert_eq!(
                    equal.lexicon.result,
                    Compatibility::Compatible,
                    "{marker} Lexicon"
                );
            }

            for (old, new) in [(&nullable, &nonnullable), (&nonnullable, &nullable)] {
                let report = compare_schemas(old, new);
                assert_eq!(
                    report.json.result,
                    Compatibility::Incompatible,
                    "{marker} JSON"
                );
                assert_eq!(
                    report.native_storage.result,
                    Compatibility::Compatible,
                    "{marker} native"
                );
                assert_eq!(
                    report.lexicon.result,
                    Compatibility::Incompatible,
                    "{marker} Lexicon"
                );
            }
        }
    }

    #[test]
    fn nullable_storage_marker_changes_are_checked_through_references_and_unions() {
        let old_value = json!({"type":["string", "null"], "x-automerge":"text"});
        let new_value = json!({"type":"string", "x-automerge":"text"});
        let old = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/value"}),
            [("value".to_owned(), old_value.clone())],
        );
        let new = SchemaBundle::with_definitions(
            json!({"$ref":"#/$defs/value"}),
            [("value".to_owned(), new_value.clone())],
        );
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
        assert_eq!(union_report.lexicon.result, Compatibility::Incompatible);
    }

    #[test]
    fn canonical_bytes_and_decimal_float_projection_is_not_sidecar_erasure() {
        let bytes = json!({"type":"string","contentEncoding":"base64"});
        let bytes_again = json!({"type":"string","x-automerge":"bytes"});
        assert_eq!(
            compare_schemas(&bytes, &bytes).lexicon.result,
            Compatibility::Compatible
        );
        assert_eq!(
            compare_schemas(&bytes, &bytes_again).lexicon.result,
            Compatibility::Compatible
        );
        assert_eq!(
            compare_schemas(&bytes, &json!({"type":"string"}))
                .lexicon
                .result,
            Compatibility::Incompatible
        );

        let float = json!({"type":"string","x-automerge":"float","format":"float"});
        let float_again = json!({"type":"string","x-automerge":"float","format":"double"});
        assert_eq!(
            compare_schemas(&float, &float_again).lexicon.result,
            Compatibility::Compatible
        );
        assert_eq!(
            compare_schemas(&float, &json!({"type":"number"}))
                .lexicon
                .result,
            Compatibility::Incompatible
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
        assert_eq!(
            compare_schemas(&unknown, &unknown).lexicon.result,
            Compatibility::NotEstablished
        );

        let nullable = json!({"type":["string","null"]});
        assert_eq!(
            compare_schemas(&nullable, &nullable).lexicon.result,
            Compatibility::Compatible
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
        assert_eq!(renamed_report.lexicon.result, Compatibility::Compatible);
    }

    #[test]
    fn native_units_sign_ranges_timestamps_and_array_items_are_distinct() {
        let timestamp_ms = json!({"type":"string","format":"date-time","unit":"milliseconds"});
        let timestamp_seconds = json!({"type":"string","format":"date-time","unit":"seconds"});
        assert_eq!(
            compare_schemas(&timestamp_ms, &timestamp_seconds)
                .native_storage
                .result,
            Compatibility::Incompatible
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

    #[cfg(feature = "schemars")]
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
        assert_eq!(report.lexicon.result, Compatibility::Compatible);

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
        assert_eq!(report.lexicon.result, Compatibility::NotEstablished);
    }
}
