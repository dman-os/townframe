//! Known facet-schema configuration for one Cabinet embedding.
//!
//! A schema repository owns the embedder-provided Lexicon document and the
//! validated carried sidecar bundle together. It does not discover, publish,
//! fetch, or compare schemas. Unknown facets remain readable as best-effort
//! raw data, but writes require an explicitly configured known facet.
//!
//! Lexicon validation is deliberately a bounded V1 structural check, not a
//! general Lexicon engine: it supports record/object/params/array/scalar/ref,
//! and union definitions plus their nested property forms. Endpoint definitions
//! are outside this facet-data subset and are refused. Unsupported definition kinds and malformed supported forms
//! are refused; valid external Lexicon refs use the bounded ATProto NSID plus
//! the optional fragment syntax supported by this V1 availability seam. Fragment
//! contents remain opaque here; this boundary is not a claim about universal
//! Lexicon fragment grammar or online resolution. Core sidecar refs have a
//! stricter local-only boundary.

use big_cabinet_core::facet::{Bundle, Universe, UniverseError};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use thiserror::Error;

/// One facet Lexicon supplied by an embedding during repository construction.
///
/// The corresponding sidecar bundle is supplied by a
/// [`SchemaRegistrationGroup`] so independent Core bundles can coexist in one
/// repository without merging definitions that happen to share a name.
pub struct SchemaRegistration {
    pub facet_tag: String,
    pub lexicon: Value,
}

impl SchemaRegistration {
    pub fn new(facet_tag: impl Into<String>, lexicon: Value) -> Self {
        Self {
            facet_tag: facet_tag.into(),
            lexicon,
        }
    }
}

/// A batch of facet Lexicons that share one supplied Core sidecar bundle.
pub struct SchemaRegistrationGroup<'a> {
    pub sidecar: Bundle<'a>,
    pub registrations: Vec<SchemaRegistration>,
}

impl<'a> SchemaRegistrationGroup<'a> {
    pub fn new(
        sidecar: Bundle<'a>,
        registrations: impl IntoIterator<Item = SchemaRegistration>,
    ) -> Self {
        Self {
            sidecar,
            registrations: registrations.into_iter().collect(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SchemaRepoError {
    #[error("known facet schema tag is empty")]
    EmptyFacetTag,
    #[error("known facet schema tag `{0}` is configured more than once")]
    DuplicateFacetTag(String),
    #[error("Lexicon for facet `{tag}` is not a JSON object")]
    LexiconNotObject { tag: String },
    #[error("Lexicon for facet `{tag}` must declare `lexicon: 1`")]
    LexiconVersion { tag: String },
    #[error("Lexicon for facet `{tag}` is missing string field `{field}`")]
    LexiconMissingString { tag: String, field: &'static str },
    #[error("Lexicon for facet `{tag}` is missing object field `{field}`")]
    LexiconMissingObject { tag: String, field: &'static str },
    #[error("Lexicon id `{lexicon_id}` does not identify configured facet `{tag}`")]
    LexiconIdentityMismatch { tag: String, lexicon_id: String },
    #[error("Lexicon for facet `{tag}` is invalid at `{path}`: {reason}")]
    LexiconInvalid {
        tag: String,
        path: String,
        reason: String,
    },
    #[error("sidecar bundle is invalid: {source}")]
    SidecarInvalid {
        #[source]
        source: UniverseError,
    },
    #[error(
        "sidecar reference `{reference}` from `{referenced_by}` is not a carried local definition"
    )]
    SidecarReferenceInvalid {
        referenced_by: String,
        reference: String,
    },
    #[error("sidecar bundle for facet `{tag}` does not carry that facet tag")]
    SidecarFacetMissing { tag: String },
    #[error("sidecar document for facet `{tag}` is missing definition `{definition}`")]
    SidecarDocumentMissing { tag: String, definition: String },
    #[error("sidecar document for facet `{tag}` has identity `{actual}` instead of `{expected}`")]
    SidecarDocumentIdentityMismatch {
        tag: String,
        expected: String,
        actual: String,
    },
    #[error("facet `{tag}` is not known to this SchemaRepo")]
    UnknownFacet { tag: String },
}

#[derive(Debug, Clone)]
struct OwnedSidecar {
    plug: Universe,
    dependencies: Vec<Universe>,
}

impl OwnedSidecar {
    fn from_bundle(bundle: &Bundle<'_>) -> Result<Self, SchemaRepoError> {
        bundle
            .validate()
            .map_err(|source| SchemaRepoError::SidecarInvalid { source })?;
        validate_sidecar_references(bundle)?;
        Ok(Self {
            plug: bundle.plug.clone(),
            dependencies: bundle
                .dependencies
                .iter()
                .map(|dependency| (*dependency).clone())
                .collect(),
        })
    }

    fn borrowed(&self) -> Bundle<'_> {
        Bundle::new(&self.plug, self.dependencies.iter().collect::<Vec<_>>())
    }
}

#[derive(Debug)]
struct KnownSchema {
    facet_tag: String,
    lexicon: Value,
    sidecar: Arc<OwnedSidecar>,
}

/// An owned, reusable handle to one known facet schema.
///
/// Cloning this handle clones only the `Arc`; the Lexicon and carried sidecar
/// remain in the repository's owned entries and are not rebuilt per lookup.
#[derive(Debug, Clone)]
pub struct SchemaHandle {
    inner: Arc<KnownSchema>,
}

impl SchemaHandle {
    pub fn facet_tag(&self) -> &str {
        &self.inner.facet_tag
    }

    pub fn lexicon(&self) -> &Value {
        &self.inner.lexicon
    }

    /// Borrow the owned carried sidecar as the core Bundle context.
    pub fn sidecar(&self) -> Bundle<'_> {
        self.inner.sidecar.borrowed()
    }
}

/// The known schemas for one Cabinet embedding.
///
/// Construction is atomic: the sidecar is validated once, every registration
/// is validated into a local map, and no repository is returned if one
/// registration conflicts or is malformed. There is deliberately no mutation
/// or online lookup API.
#[derive(Debug)]
pub struct SchemaRepo {
    known: BTreeMap<String, SchemaHandle>,
}

impl SchemaRepo {
    pub fn open<'a>(
        groups: impl IntoIterator<Item = SchemaRegistrationGroup<'a>>,
    ) -> Result<Self, SchemaRepoError> {
        let mut known = BTreeMap::new();
        for group in groups {
            let sidecar = Arc::new(OwnedSidecar::from_bundle(&group.sidecar)?);
            let borrowed = sidecar.borrowed();
            for registration in group.registrations {
                let tag = registration.facet_tag;
                if known.contains_key(&tag) {
                    return Err(SchemaRepoError::DuplicateFacetTag(tag));
                }
                validate_tag(&tag)?;
                validate_lexicon(&tag, &registration.lexicon)?;
                validate_sidecar_association(&tag, &borrowed)?;
                let handle = SchemaHandle {
                    inner: Arc::new(KnownSchema {
                        facet_tag: tag.clone(),
                        lexicon: registration.lexicon,
                        sidecar: Arc::clone(&sidecar),
                    }),
                };
                known.insert(tag, handle);
            }
        }
        Ok(Self { known })
    }

    /// Best-effort read lookup. Unknown facets remain available to raw readers.
    pub fn resolve_for_read(&self, facet_tag: &str) -> Option<SchemaHandle> {
        self.known.get(facet_tag).cloned()
    }

    /// Require a known schema before accepting a facet write.
    pub fn require_for_write(&self, facet_tag: &str) -> Result<SchemaHandle, SchemaRepoError> {
        self.resolve_for_read(facet_tag)
            .ok_or_else(|| SchemaRepoError::UnknownFacet {
                tag: facet_tag.to_owned(),
            })
    }
}

fn validate_tag(tag: &str) -> Result<(), SchemaRepoError> {
    if tag.is_empty() {
        return Err(SchemaRepoError::EmptyFacetTag);
    }
    Ok(())
}

fn validate_lexicon(tag: &str, lexicon: &Value) -> Result<(), SchemaRepoError> {
    let Some(document) = lexicon.as_object() else {
        return Err(SchemaRepoError::LexiconNotObject {
            tag: tag.to_owned(),
        });
    };
    if document.get("lexicon").and_then(Value::as_i64) != Some(1) {
        return Err(SchemaRepoError::LexiconVersion {
            tag: tag.to_owned(),
        });
    }
    let Some(id) = document.get("id").and_then(Value::as_str) else {
        return Err(SchemaRepoError::LexiconMissingString {
            tag: tag.to_owned(),
            field: "id",
        });
    };
    if id != tag {
        return Err(SchemaRepoError::LexiconIdentityMismatch {
            tag: tag.to_owned(),
            lexicon_id: id.to_owned(),
        });
    }
    let Some(definitions) = document.get("defs").and_then(Value::as_object) else {
        return Err(SchemaRepoError::LexiconMissingObject {
            tag: tag.to_owned(),
            field: "defs",
        });
    };
    if !definitions.contains_key("main") {
        return invalid_lexicon(tag, "defs.main", "the main definition is required");
    }
    for (name, definition) in definitions {
        validate_lexicon_definition(tag, &format!("defs.{name}"), definition, definitions)?;
    }
    Ok(())
}

fn validate_lexicon_definition(
    tag: &str,
    path: &str,
    definition: &Value,
    definitions: &Map<String, Value>,
) -> Result<(), SchemaRepoError> {
    let Some(definition) = definition.as_object() else {
        return invalid_lexicon(tag, path, "a definition must be an object");
    };
    let Some(kind) = definition.get("type").and_then(Value::as_str) else {
        return invalid_lexicon(tag, path, "a definition requires a string `type`");
    };
    match kind {
        "record" => {
            if let Some(key) = definition.get("key")
                && !key.is_string()
            {
                return invalid_lexicon(tag, &format!("{path}.key"), "record key must be a string");
            }
            let record = required_object(tag, path, definition, "record")?;
            validate_object_schema(
                tag,
                &format!("{path}.record"),
                record,
                definitions,
                "object",
            )?;
        }
        "object" | "params" => {
            validate_object_schema(tag, path, definition, definitions, kind)?;
        }
        "array" => {
            let items = required_object(tag, path, definition, "items")?;
            validate_lexicon_field(tag, &format!("{path}.items"), items, definitions)?;
        }
        "string" | "integer" | "boolean" | "bytes" | "cid-link" | "blob" | "token" | "unknown" => {
            validate_scalar_options(tag, path, definition, kind)?;
        }
        "ref" => {
            let reference = required_string(tag, path, definition, "ref")?;
            validate_lexicon_reference(tag, &format!("{path}.ref"), reference, definitions)?;
        }
        "union" => {
            let Some(refs) = definition.get("refs").and_then(Value::as_array) else {
                return invalid_lexicon(
                    tag,
                    path,
                    "a union requires a non-empty string `refs` array",
                );
            };
            if refs.is_empty() {
                return invalid_lexicon(
                    tag,
                    path,
                    "a union requires a non-empty string `refs` array",
                );
            }
            for (index, reference) in refs.iter().enumerate() {
                let Some(reference) = reference.as_str() else {
                    return invalid_lexicon(
                        tag,
                        &format!("{path}.refs[{index}]"),
                        "a union reference must be a string",
                    );
                };
                validate_lexicon_reference(
                    tag,
                    &format!("{path}.refs[{index}]"),
                    reference,
                    definitions,
                )?;
            }
        }
        other => {
            return invalid_lexicon(
                tag,
                &format!("{path}.type"),
                &format!("unsupported Lexicon definition type `{other}`"),
            );
        }
    }
    Ok(())
}

fn validate_object_schema(
    tag: &str,
    path: &str,
    schema: &Map<String, Value>,
    definitions: &Map<String, Value>,
    expected_type: &str,
) -> Result<(), SchemaRepoError> {
    if schema.get("type").and_then(Value::as_str) != Some(expected_type) {
        return invalid_lexicon(
            tag,
            &format!("{path}.type"),
            &format!("an object schema must have type `{expected_type}`"),
        );
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return invalid_lexicon(
            tag,
            &format!("{path}.properties"),
            "an object schema requires an object `properties` field",
        );
    };
    for (name, property) in properties {
        let property = property.as_object().ok_or_else(|| {
            invalid_lexicon_error(
                tag,
                &format!("{path}.properties.{name}"),
                "a property schema must be an object",
            )
        })?;
        validate_lexicon_field(
            tag,
            &format!("{path}.properties.{name}"),
            property,
            definitions,
        )?;
    }
    if let Some(required) = schema.get("required") {
        let Some(required) = required.as_array() else {
            return invalid_lexicon(
                tag,
                &format!("{path}.required"),
                "required must be an array",
            );
        };
        for (index, name) in required.iter().enumerate() {
            let Some(name) = name.as_str() else {
                return invalid_lexicon(
                    tag,
                    &format!("{path}.required[{index}]"),
                    "required property names must be strings",
                );
            };
            if !properties.contains_key(name) {
                return invalid_lexicon(
                    tag,
                    &format!("{path}.required[{index}]"),
                    "required property is absent from properties",
                );
            }
        }
    }
    Ok(())
}

fn validate_lexicon_field(
    tag: &str,
    path: &str,
    field: &Map<String, Value>,
    definitions: &Map<String, Value>,
) -> Result<(), SchemaRepoError> {
    let Some(kind) = field.get("type").and_then(Value::as_str) else {
        return invalid_lexicon(tag, path, "a field requires a string `type`");
    };
    match kind {
        "object" | "params" => validate_object_schema(tag, path, field, definitions, kind),
        "array" => {
            let items = required_object(tag, path, field, "items")?;
            validate_lexicon_field(tag, &format!("{path}.items"), items, definitions)
        }
        "ref" => {
            let reference = required_string(tag, path, field, "ref")?;
            validate_lexicon_reference(tag, &format!("{path}.ref"), reference, definitions)
        }
        "union" => {
            let Some(refs) = field.get("refs").and_then(Value::as_array) else {
                return invalid_lexicon(
                    tag,
                    path,
                    "a union requires a non-empty string `refs` array",
                );
            };
            if refs.is_empty() {
                return invalid_lexicon(
                    tag,
                    path,
                    "a union requires a non-empty string `refs` array",
                );
            }
            for (index, reference) in refs.iter().enumerate() {
                let Some(reference) = reference.as_str() else {
                    return invalid_lexicon(
                        tag,
                        &format!("{path}.refs[{index}]"),
                        "a union reference must be a string",
                    );
                };
                validate_lexicon_reference(
                    tag,
                    &format!("{path}.refs[{index}]"),
                    reference,
                    definitions,
                )?;
            }
            Ok(())
        }
        "string" | "integer" | "boolean" | "bytes" | "cid-link" | "blob" | "token" | "unknown" => {
            validate_scalar_options(tag, path, field, kind)
        }
        other => invalid_lexicon(
            tag,
            &format!("{path}.type"),
            &format!("unsupported Lexicon field type `{other}`"),
        ),
    }
}

fn validate_scalar_options(
    tag: &str,
    path: &str,
    value: &Map<String, Value>,
    kind: &str,
) -> Result<(), SchemaRepoError> {
    if let Some(enum_values) = value.get("enum") {
        let Some(enum_values) = enum_values.as_array() else {
            return invalid_lexicon(tag, &format!("{path}.enum"), "enum must be an array");
        };
        for (index, item) in enum_values.iter().enumerate() {
            let valid = match kind {
                "integer" => item.as_i64().is_some(),
                "boolean" => item.is_boolean(),
                _ => item.is_string(),
            };
            if !valid {
                return invalid_lexicon(
                    tag,
                    &format!("{path}.enum[{index}]"),
                    "enum value has the wrong type",
                );
            }
        }
    }
    for field in ["minLength", "maxLength", "minGraphemes", "maxGraphemes"] {
        if let Some(value) = value.get(field)
            && value.as_u64().is_none()
        {
            return invalid_lexicon(
                tag,
                &format!("{path}.{field}"),
                "length bounds must be non-negative integers",
            );
        }
    }
    for field in ["minimum", "maximum"] {
        if let Some(value) = value.get(field)
            && value.as_i64().is_none()
        {
            return invalid_lexicon(
                tag,
                &format!("{path}.{field}"),
                "integer bounds must be integers",
            );
        }
    }
    if let Some(format) = value.get("format")
        && !format.is_string()
    {
        return invalid_lexicon(tag, &format!("{path}.format"), "format must be a string");
    }
    Ok(())
}

fn validate_lexicon_reference(
    tag: &str,
    path: &str,
    reference: &str,
    definitions: &Map<String, Value>,
) -> Result<(), SchemaRepoError> {
    if reference.is_empty() {
        return invalid_lexicon(tag, path, "reference must not be empty");
    }
    if let Some(name) = reference.strip_prefix("#") {
        let name = name.strip_prefix("/$defs/").unwrap_or(name);
        if !valid_lexicon_fragment(name) {
            return invalid_lexicon(
                tag,
                path,
                "local reference has an invalid definition fragment",
            );
        }
        if !definitions.contains_key(name) {
            return invalid_lexicon(
                tag,
                path,
                "local reference does not name a carried definition",
            );
        }
        return Ok(());
    }

    let mut parts = reference.split('#');
    let nsid = parts.next().unwrap_or_default();
    let fragment = parts.next();
    if parts.next().is_some() || !valid_atproto_nsid(nsid) {
        return invalid_lexicon(
            tag,
            path,
            "external reference must use an ATProto NSID with an optional definition fragment",
        );
    }
    if let Some(fragment) = fragment
        && !valid_lexicon_fragment(fragment)
    {
        return invalid_lexicon(
            tag,
            path,
            "external reference has an invalid definition fragment",
        );
    }
    Ok(())
}

/// The bounded ATProto NSID grammar used by Lexicon references. The final
/// name is separated from the reverse-DNS authority because their start rules
/// differ: the authority has at least two labels, only its first (TLD) label
/// must start with a letter, and the final name starts with a letter and has no
/// hyphens. The limits are the normative 253-character authority and
/// 317-character NSID maxima.
fn valid_atproto_nsid(value: &str) -> bool {
    if value.is_empty() || value.len() > 317 {
        return false;
    }
    let Some((authority, name)) = value.rsplit_once('.') else {
        return false;
    };
    if authority.is_empty() || authority.len() > 253 || !valid_nsid_name(name) {
        return false;
    }

    let mut labels = authority.split('.');
    let Some(tld) = labels.next() else {
        return false;
    };
    let Some(second) = labels.next() else {
        return false;
    };
    valid_nsid_authority_label(tld, true)
        && valid_nsid_authority_label(second, false)
        && labels.all(|label| valid_nsid_authority_label(label, false))
}

fn valid_nsid_authority_label(value: &str, is_tld: bool) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && (!is_tld || value.starts_with(|character: char| character.is_ascii_alphabetic()))
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn valid_nsid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.starts_with(|character: char| character.is_ascii_alphabetic())
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}

/// This is the deliberately small fragment boundary supported by the V1
/// availability seam. It admits slash-separated opaque path components, such
/// as the specification's `someView/main` example. Fragments are not resolved
/// or assigned a universal Lexicon grammar here.
fn valid_lexicon_fragment(value: &str) -> bool {
    !value.is_empty()
        && value.split('/').all(|component| {
            !component.is_empty()
                && component.starts_with(|character: char| character.is_ascii_alphanumeric())
                && component.chars().all(|character| {
                    character.is_ascii_alphanumeric() || character == '_' || character == '-'
                })
        })
}

fn required_string<'a>(
    tag: &str,
    path: &str,
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, SchemaRepoError> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        invalid_lexicon_error(tag, &format!("{path}.{field}"), "field must be a string")
    })
}

fn required_object<'a>(
    tag: &str,
    path: &str,
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a Map<String, Value>, SchemaRepoError> {
    object.get(field).and_then(Value::as_object).ok_or_else(|| {
        invalid_lexicon_error(tag, &format!("{path}.{field}"), "field must be an object")
    })
}

fn invalid_lexicon<T>(tag: &str, path: &str, reason: &str) -> Result<T, SchemaRepoError> {
    Err(invalid_lexicon_error(tag, path, reason))
}

fn invalid_lexicon_error(tag: &str, path: &str, reason: &str) -> SchemaRepoError {
    SchemaRepoError::LexiconInvalid {
        tag: tag.to_owned(),
        path: path.to_owned(),
        reason: reason.to_owned(),
    }
}

fn validate_sidecar_association(tag: &str, bundle: &Bundle<'_>) -> Result<(), SchemaRepoError> {
    let Some(facet) = bundle.plug.facets.iter().find(|facet| facet.id == tag) else {
        return Err(SchemaRepoError::SidecarFacetMissing {
            tag: tag.to_owned(),
        });
    };
    let Some(document) = bundle.document(&facet.name) else {
        return Err(SchemaRepoError::SidecarDocumentMissing {
            tag: tag.to_owned(),
            definition: facet.name.clone(),
        });
    };
    let document = serde_json::to_value(document).expect("a core Schema serializes to JSON");
    let expected = format!("#/$defs/{}", facet.name);
    let actual = document
        .get("$ref")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if actual != expected {
        return Err(SchemaRepoError::SidecarDocumentIdentityMismatch {
            tag: tag.to_owned(),
            expected,
            actual: actual.to_owned(),
        });
    }
    Ok(())
}

fn validate_sidecar_references(bundle: &Bundle<'_>) -> Result<(), SchemaRepoError> {
    let definitions = bundle.definitions();
    let names: BTreeSet<&str> = definitions.iter().map(|(name, ..)| *name).collect();
    for (name, schema, _) in definitions {
        let value = serde_json::to_value(schema).expect("a core Schema serializes to JSON");
        scan_schema_value(&names, name, &value)?;
    }
    Ok(())
}

fn scan_schema_value(
    names: &BTreeSet<&str>,
    owner: &str,
    value: &Value,
) -> Result<(), SchemaRepoError> {
    let Value::Object(object) = value else {
        return Ok(());
    };
    if let Some(reference) = object.get("$ref") {
        let Some(reference) = reference.as_str() else {
            return Err(SchemaRepoError::SidecarReferenceInvalid {
                referenced_by: owner.to_owned(),
                reference: reference.to_string(),
            });
        };
        let Some(name) = reference.strip_prefix("#/$defs/") else {
            return Err(SchemaRepoError::SidecarReferenceInvalid {
                referenced_by: owner.to_owned(),
                reference: reference.to_owned(),
            });
        };
        if name.is_empty() || !names.contains(name) {
            return Err(SchemaRepoError::SidecarReferenceInvalid {
                referenced_by: owner.to_owned(),
                reference: reference.to_owned(),
            });
        }
    }
    for (key, nested) in object {
        match key.as_str() {
            "$defs" | "definitions" | "properties" | "patternProperties" | "dependentSchemas" => {
                let Some(properties) = nested.as_object() else {
                    continue;
                };
                for schema in properties.values() {
                    scan_schema_value(names, owner, schema)?;
                }
            }
            "additionalProperties"
            | "additionalItems"
            | "propertyNames"
            | "contains"
            | "contentSchema"
            | "not"
            | "if"
            | "then"
            | "else"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "items" => scan_schema_value(names, owner, nested)?,
            "prefixItems" | "allOf" | "anyOf" | "oneOf" => {
                let Some(items) = nested.as_array() else {
                    continue;
                };
                for schema in items {
                    scan_schema_value(names, owner, schema)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::facet::{Declaration, FacetDecl, TypeDecl};
    use serde_json::json;

    fn record_lexicon(tag: &str) -> Value {
        json!({
            "lexicon": 1,
            "id": tag,
            "defs": {
                "main": {
                    "type": "record",
                    "key": "tid",
                    "record": {
                        "type": "object",
                        "required": ["value"],
                        "properties": {"value": {"type": "string"}}
                    }
                }
            }
        })
    }

    fn manual_declaration(
        tag: &str,
        name: &str,
        schema: Value,
        types: Vec<TypeDecl>,
    ) -> Declaration {
        Declaration {
            facet: FacetDecl {
                id: tag.to_owned(),
                name: name.to_owned(),
                schema: schemars::Schema::try_from(schema).expect("sidecar schema"),
                flags: Vec::new(),
            },
            types,
        }
    }

    fn manual_universe(tag: &str) -> Universe {
        Universe::collect([manual_declaration(
            tag,
            "Main",
            json!({
                "type": "object",
                "properties": {"carried": {"$ref": "#/$defs/Carried"}}
            }),
            vec![TypeDecl {
                name: "Carried".to_owned(),
                schema: schemars::Schema::try_from(json!({"type": "string"}))
                    .expect("carried schema"),
            }],
        )])
    }

    fn group<'a>(
        sidecar: Bundle<'a>,
        registrations: impl IntoIterator<Item = SchemaRegistration>,
    ) -> SchemaRegistrationGroup<'a> {
        SchemaRegistrationGroup::new(sidecar, registrations)
    }

    #[test]
    fn real_declaration_bundle_and_lexicon_are_owned_for_repeated_lookup() {
        let tag = "org.example.schema.note";
        let universe = manual_universe(tag);
        let repo = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [SchemaRegistration::new(tag, record_lexicon(tag))],
        )])
        .expect("valid known schema");
        let first = repo.resolve_for_read(tag).expect("known read schema");
        let second = repo.resolve_for_read(tag).expect("repeated read schema");
        assert!(Arc::ptr_eq(&first.inner, &second.inner));
        assert_eq!(first.facet_tag(), tag);
        assert_eq!(first.lexicon()["id"], tag);
        assert!(first.sidecar().document("Main").is_some());
    }

    #[test]
    fn multifacet_registration_shares_one_owned_sidecar() {
        let note = "org.example.schema.note";
        let photo = "org.example.schema.photo";
        let shared = Universe::collect([
            manual_declaration(
                note,
                "Note",
                json!({"type": "object", "properties": {"value": {"type": "string"}}}),
                Vec::new(),
            ),
            manual_declaration(
                photo,
                "Photo",
                json!({"type": "object", "properties": {"value": {"type": "string"}}}),
                Vec::new(),
            ),
        ]);
        let repo = SchemaRepo::open([group(
            Bundle::new(&shared, Vec::new()),
            [
                SchemaRegistration::new(note, record_lexicon(note)),
                SchemaRegistration::new(photo, record_lexicon(photo)),
            ],
        )])
        .expect("valid multifacet configuration");
        let note_handle = repo.resolve_for_read(note).expect("known note");
        let photo_handle = repo.resolve_for_read(photo).expect("known photo");
        assert!(Arc::ptr_eq(
            &note_handle.inner.sidecar,
            &photo_handle.inner.sidecar
        ));
        assert!(note_handle.sidecar().document("Note").is_some());
        assert!(photo_handle.sidecar().document("Photo").is_some());
    }

    #[test]
    fn dependency_definitions_are_owned_and_resolved() {
        let tag = "org.example.schema.dependency";
        let plug = Universe::collect([manual_declaration(
            tag,
            "Main",
            json!({"$ref": "#/$defs/Carried"}),
            Vec::new(),
        )]);
        let dependency = Universe {
            facets: Vec::new(),
            types: vec![TypeDecl {
                name: "Carried".to_owned(),
                schema: schemars::Schema::try_from(json!({"type": "string"}))
                    .expect("dependency schema"),
            }],
        };
        let repo = SchemaRepo::open([group(
            Bundle::new(&plug, vec![&dependency]),
            [SchemaRegistration::new(tag, record_lexicon(tag))],
        )])
        .expect("dependency definition is carried");
        assert!(
            repo.resolve_for_read(tag)
                .expect("known dependency schema")
                .sidecar()
                .resolve("Carried")
                .is_some()
        );
    }

    #[test]
    fn independent_groups_keep_same_named_definitions_and_share_within_group() {
        let first_tag = "org.example.schema.first";
        let second_tag = "org.example.schema.second";
        let first = Universe::collect([manual_declaration(
            first_tag,
            "Main",
            json!({
                "type": "object",
                "properties": {"$ref": {"$ref": "#/$defs/Shared"}},
                "const": {"$ref": "#/$defs/NotCarried"},
                "default": {"$ref": "#/$defs/NotCarried"},
                "enum": [{"$ref": "#/$defs/NotCarried"}],
                "data": {"$ref": "#/$defs/NotCarried"}
            }),
            vec![TypeDecl {
                name: "Shared".to_owned(),
                schema: schemars::Schema::try_from(json!({"type": "string"}))
                    .expect("first carried schema"),
            }],
        )]);
        let second = Universe::collect([manual_declaration(
            second_tag,
            "Main",
            json!({
                "type": "object",
                "properties": {"$ref": {"$ref": "#/$defs/Shared"}},
                "const": {"$ref": "#/$defs/NotCarried"},
                "default": {"$ref": "#/$defs/NotCarried"},
                "enum": [{"$ref": "#/$defs/NotCarried"}],
                "data": {"$ref": "#/$defs/NotCarried"}
            }),
            vec![TypeDecl {
                name: "Shared".to_owned(),
                schema: schemars::Schema::try_from(json!({"type": "integer"}))
                    .expect("second carried schema"),
            }],
        )]);
        let repo = SchemaRepo::open([
            group(
                Bundle::new(&first, Vec::new()),
                [SchemaRegistration::new(
                    first_tag,
                    record_lexicon(first_tag),
                )],
            ),
            group(
                Bundle::new(&second, Vec::new()),
                [SchemaRegistration::new(
                    second_tag,
                    record_lexicon(second_tag),
                )],
            ),
        ])
        .expect("independent groups are valid");
        let first_handle = repo.resolve_for_read(first_tag).expect("first handle");
        let second_handle = repo.resolve_for_read(second_tag).expect("second handle");
        assert!(!Arc::ptr_eq(
            &first_handle.inner.sidecar,
            &second_handle.inner.sidecar
        ));
        assert_ne!(
            serde_json::to_value(
                first_handle
                    .sidecar()
                    .resolve("Shared")
                    .expect("first Shared")
            )
            .expect("first schema JSON"),
            serde_json::to_value(
                second_handle
                    .sidecar()
                    .resolve("Shared")
                    .expect("second Shared")
            )
            .expect("second schema JSON")
        );

        let duplicate = SchemaRepo::open([
            group(
                Bundle::new(&first, Vec::new()),
                [SchemaRegistration::new(
                    first_tag,
                    record_lexicon(first_tag),
                )],
            ),
            group(
                Bundle::new(&second, Vec::new()),
                [SchemaRegistration::new(
                    first_tag,
                    record_lexicon(first_tag),
                )],
            ),
        ])
        .expect_err("cross-group duplicate tag");
        assert!(matches!(duplicate, SchemaRepoError::DuplicateFacetTag(tag) if tag == first_tag));
    }

    #[test]
    fn unsupported_endpoint_definition_is_refused() {
        let tag = "org.example.schema.endpoint";
        let universe = manual_universe(tag);
        let endpoint = json!({
            "lexicon": 1,
            "id": tag,
            "defs": {"main": {"type": "query"}}
        });
        let error = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [SchemaRegistration::new(tag, endpoint)],
        )])
        .expect_err("endpoint is outside the facet-data subset");
        assert!(
            matches!(error, SchemaRepoError::LexiconInvalid { path, reason, .. }
            if path == "defs.main.type" && reason.contains("unsupported"))
        );
    }

    #[test]
    fn unknown_reads_are_best_effort_but_unknown_writes_are_refused() {
        let universe = Universe::default();
        let repo = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            std::iter::empty::<SchemaRegistration>(),
        )])
        .expect("empty configuration is ready");
        assert!(repo.resolve_for_read("org.example.unknown").is_none());
        assert!(matches!(
            repo.require_for_write("org.example.unknown"),
            Err(SchemaRepoError::UnknownFacet { tag }) if tag == "org.example.unknown"
        ));
    }

    #[test]
    fn duplicate_configuration_is_rejected_atomically() {
        let tag = "org.example.schema.note";
        let universe = manual_universe(tag);
        let error = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [
                SchemaRegistration::new(tag, record_lexicon(tag)),
                SchemaRegistration::new(tag, record_lexicon(tag)),
            ],
        )])
        .expect_err("duplicate configuration");
        assert!(matches!(error, SchemaRepoError::DuplicateFacetTag(found) if found == tag));
    }

    #[test]
    fn malformed_main_and_nested_shapes_are_rejected() {
        let tag = "org.example.schema.invalid";
        let universe = manual_universe(tag);
        let malformed = |main: Value| json!({"lexicon": 1, "id": tag, "defs": {"main": main}});
        for document in [json!({}), json!({"type": "not-a-lexicon-definition"})] {
            let error = SchemaRepo::open([group(
                Bundle::new(&universe, Vec::new()),
                [SchemaRegistration::new(tag, malformed(document))],
            )])
            .expect_err("malformed main");
            assert!(matches!(error, SchemaRepoError::LexiconInvalid { .. }));
        }
        let nested = json!({
            "type": "record",
            "key": "tid",
            "record": {"type": "object", "properties": {"value": {}}}
        });
        let error = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [SchemaRegistration::new(tag, malformed(nested))],
        )])
        .expect_err("malformed nested field");
        assert!(
            matches!(error, SchemaRepoError::LexiconInvalid { path, .. } if path.contains("properties.value"))
        );
        let bad_reference = json!({
            "type": "record",
            "key": "tid",
            "record": {"type": "object", "properties": {
                "value": {"type": "ref", "ref": "#Missing"}
            }}
        });
        let error = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [SchemaRegistration::new(tag, malformed(bad_reference))],
        )])
        .expect_err("missing Lexicon definition reference");
        assert!(matches!(error, SchemaRepoError::LexiconInvalid { path, .. }
            if path.contains("value.ref")));
    }

    #[test]
    fn data_type_main_definition_is_supported() {
        let tag = "org.example.schema.value";
        let universe = manual_universe(tag);
        let document = json!({
            "lexicon": 1,
            "id": tag,
            "defs": {"main": {"type": "string", "format": "datetime"}}
        });
        assert!(
            SchemaRepo::open([group(
                Bundle::new(&universe, Vec::new()),
                [SchemaRegistration::new(tag, document)],
            )])
            .is_ok()
        );
    }

    #[test]
    fn nested_lexicon_references_are_validated_and_accepted() {
        let tag = "org.example.schema.nested";
        let universe = manual_universe(tag);
        let document = json!({
            "lexicon": 1,
            "id": tag,
            "defs": {
                "main": {
                    "type": "record",
                    "key": "tid",
                    "record": {
                        "type": "object",
                        "properties": {"child": {"type": "ref", "ref": "#Child"}}
                    }
                },
                "Child": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}}
                }
            }
        });
        assert!(
            SchemaRepo::open([group(
                Bundle::new(&universe, Vec::new()),
                [SchemaRegistration::new(tag, document)],
            )])
            .is_ok()
        );
    }

    #[test]
    fn external_lexicon_references_use_bounded_nsid_grammar() {
        let tag = "org.example.schema.external-refs";
        let universe = manual_universe(tag);
        let document = |reference: &str| {
            json!({
                "lexicon": 1,
                "id": tag,
                "defs": {"main": {
                    "type": "record",
                    "key": "tid",
                    "record": {"type": "object", "properties": {
                        "value": {"type": "ref", "ref": reference}
                    }}
                }}
            })
        };
        let assert_valid = |reference: &str| {
            assert!(
                SchemaRepo::open([group(
                    Bundle::new(&universe, Vec::new()),
                    [SchemaRegistration::new(tag, document(reference))],
                )])
                .is_ok(),
                "expected valid external Lexicon reference: {reference}"
            );
        };
        let assert_invalid = |reference: &str| {
            let error = SchemaRepo::open([group(
                Bundle::new(&universe, Vec::new()),
                [SchemaRegistration::new(tag, document(reference))],
            )])
            .expect_err("invalid external Lexicon reference");
            assert!(
                matches!(error, SchemaRepoError::LexiconInvalid { path, reason, .. }
                if path.ends_with("value.ref") && reason.contains("reference")),
                "expected invalid external Lexicon reference: {reference}"
            );
        };

        for reference in [
            "com.atproto.repo.strongRef",
            "com.atproto.repo.strongRef#main",
            "com.atproto.repo.strongRef#someView/main",
            "cn.8.l",
            "a-0.b-1.c",
        ] {
            assert_valid(reference);
        }
        let long_authority = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61),
        );
        assert_eq!(long_authority.len(), 253);
        let long_reference = format!("{long_authority}.{}", "e".repeat(63));
        assert_eq!(long_reference.len(), 317);
        assert_valid(&long_reference);

        for reference in [
            "not-a-ref",
            "com.example.bad ref",
            "https://example.com/schema",
            "com.example.foo#bad#extra",
            "com.example.foo#bad//name",
            "com.example.foo#bad/name/",
            "com.example.foo#-bad/name",
            "1.com.example.foo",
            "com.example.foo#",
            "com.example.3",
            "com.example.foo-bar",
            "com.-example.foo",
            "com.example-.foo",
            "com.exämple.foo",
            "com.example.foo ",
        ] {
            assert_invalid(reference);
        }
        let authority_254 = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(62),
        );
        assert_eq!(authority_254.len(), 254);
        assert_invalid(&format!("{authority_254}.e"));
        assert_invalid(&format!("com.example.{}", "f".repeat(64)));
        assert_invalid(&format!("{}.{}", long_authority, "e".repeat(64)));
    }

    #[test]
    fn missing_carried_and_external_sidecar_references_are_precise() {
        let missing_tag = "org.example.schema.missing";
        let missing = Universe::collect([manual_declaration(
            missing_tag,
            "Main",
            json!({"$ref": "#/$defs/NotCarried"}),
            Vec::new(),
        )]);
        let missing_error = SchemaRepo::open([group(
            Bundle::new(&missing, Vec::new()),
            [SchemaRegistration::new(
                missing_tag,
                record_lexicon(missing_tag),
            )],
        )])
        .expect_err("missing carried definition");
        assert!(matches!(
            missing_error,
            SchemaRepoError::SidecarInvalid {
                source: UniverseError::Undeclared { name, .. },
            } if name == "NotCarried"
        ));

        let external_tag = "org.example.schema.external";
        let external = Universe::collect([manual_declaration(
            external_tag,
            "Main",
            json!({"$ref": "https://example.com/schema.json"}),
            Vec::new(),
        )]);
        let external_error = SchemaRepo::open([group(
            Bundle::new(&external, Vec::new()),
            [SchemaRegistration::new(
                external_tag,
                record_lexicon(external_tag),
            )],
        )])
        .expect_err("external sidecar reference");
        assert!(matches!(
            external_error,
            SchemaRepoError::SidecarReferenceInvalid { reference, .. }
                if reference == "https://example.com/schema.json"
        ));
    }

    #[test]
    fn lexicon_and_sidecar_identity_mismatches_are_rejected() {
        let tag = "org.example.schema.note";
        let universe = manual_universe(tag);
        let lexicon_error = SchemaRepo::open([group(
            Bundle::new(&universe, Vec::new()),
            [SchemaRegistration::new(
                tag,
                record_lexicon("org.example.schema.other"),
            )],
        )])
        .expect_err("lexicon identity mismatch");
        assert!(matches!(
            lexicon_error,
            SchemaRepoError::LexiconIdentityMismatch { .. }
        ));
        let other = manual_universe("org.example.schema.other");
        let sidecar_error = SchemaRepo::open([group(
            Bundle::new(&other, Vec::new()),
            [SchemaRegistration::new(tag, record_lexicon(tag))],
        )])
        .expect_err("sidecar identity mismatch");
        assert!(matches!(
            sidecar_error,
            SchemaRepoError::SidecarFacetMissing { .. }
        ));
    }
}
