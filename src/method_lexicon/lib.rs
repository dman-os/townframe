//! The atproto Lexicon file a method carries, assembled from the schemas `schemars` derived for
//! its request, output and error types.
//!
//! The file is `{lexicon: 1, id: <nsid>, defs: {main, <carried definitions>}}`. `main` is the
//! method's `query`, `procedure` or `subscription` definition:
//!
//! * a procedure's request is the `input` body, a query's or subscription's request is flat
//!   `parameters` (every field a primitive or an array of one, since a query string carries
//!   nothing else);
//! * the output (a subscription's event is its `message`) is a JSON object body, and a method
//!   returning `()` states no `output`;
//! * `errors` lists the domain error enum's variants by their outer name. The typed body an error
//!   carries (`operationError`) is a serde externally tagged enum, which a Lexicon cannot state,
//!   so it is not published.
//!
//! Types are projected with the datum Lexicon projection, so a method type is held to the same
//! canonical data model a datum is (no plain `number`, no `allOf`), and the finished file is
//! admitted by `datum::operation::validate`, the frame check a nodule's own operation Lexicons
//! pass. Nothing datum-specific (declarations, `Extra`, Automerge storage) applies here.

use big_cabinet_core::datum::lexicon::project_position;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::datum::operation::validate;
use big_cabinet_core::datum::rust_number_formats_to_bounds;
use method_core::MethodKind;
use method_core::schema::MethodSchema;
use method_core::schema::MethodSchemas;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum MethodLexiconError {
    #[error("{nsid}: {part} does not project to a Lexicon: {reason}")]
    Projection {
        nsid: &'static str,
        part: &'static str,
        reason: String,
    },
    #[error("{nsid}: {part} is not an object, so it cannot be a {what}")]
    NotAnObject {
        nsid: &'static str,
        part: &'static str,
        what: &'static str,
    },
    #[error("{nsid}: parameter `{name}` is not a primitive or an array of one")]
    NotFlat { nsid: &'static str, name: String },
    #[error("{nsid}: the error type is not an enum of named variants")]
    ErrorNotAnEnum { nsid: &'static str },
    #[error("{nsid}: the Lexicon file is refused: {reason}")]
    Frame { nsid: &'static str, reason: String },
}

/// The Lexicon file of method `M`.
pub fn lexicon<M: MethodSchema>() -> Result<Value, MethodLexiconError> {
    let nsid = M::XRPC.nsid;
    let MethodSchemas {
        request,
        output,
        error,
        definitions,
    } = M::schemas();
    let mut request = serde_json::to_value(request).expect("a schema serializes");
    let mut output = serde_json::to_value(output).expect("a schema serializes");
    let mut error = serde_json::to_value(error).expect("a schema serializes");
    let mut definitions_value = Value::Object(definitions);
    for value in [
        &mut request,
        &mut output,
        &mut error,
        &mut definitions_value,
    ] {
        rust_number_formats_to_bounds(value).map_err(|reason| MethodLexiconError::Projection {
            nsid,
            part: "numbers",
            reason,
        })?;
    }
    let Value::Object(mut definitions) = definitions_value else {
        unreachable!("the definitions were an object")
    };
    // Read before `normalize`, which rewrites an enum into the object or string its wire form is.
    let errors = error_names(nsid, resolve(&error, &definitions))?;
    let primitives = primitive_definitions(&definitions);
    for value in definitions.values_mut() {
        normalize(value, &primitives);
    }
    for value in [&mut request, &mut output, &mut error] {
        normalize(value, &primitives);
    }

    let mut carried = BTreeMap::new();
    let request_object = resolve(&request, &definitions);
    let mut main = Map::new();
    let kind = match M::XRPC.kind {
        MethodKind::Query => OperationKind::Query,
        MethodKind::Procedure => OperationKind::Procedure,
        MethodKind::Subscription => OperationKind::Subscription,
    };
    main.insert("type".to_owned(), json!(kind.as_str()));
    if let Some(description) = request_object.get("description") {
        main.insert("description".to_owned(), description.clone());
    }
    match kind {
        OperationKind::Procedure => {
            let schema = project(nsid, "request", &request, &definitions, &mut carried)?;
            main.insert(
                "input".to_owned(),
                json!({ "encoding": "application/json", "schema": schema }),
            );
        }
        OperationKind::Query | OperationKind::Subscription => {
            let parameters = parameters(nsid, request_object, &definitions)?;
            main.insert("parameters".to_owned(), parameters);
        }
    }
    let body = resolve(&output, &definitions);
    if body.get("type") != Some(&json!("null")) {
        if body.get("type") != Some(&json!("object")) && body.get("oneOf").is_none() {
            return Err(MethodLexiconError::NotAnObject {
                nsid,
                part: "output",
                what: "JSON body",
            });
        }
        let schema = project(nsid, "output", &output, &definitions, &mut carried)?;
        if kind == OperationKind::Subscription {
            main.insert("message".to_owned(), json!({ "schema": schema }));
        } else {
            main.insert(
                "output".to_owned(),
                json!({ "encoding": "application/json", "schema": schema }),
            );
        }
    }
    if !errors.is_empty() {
        main.insert("errors".to_owned(), Value::Array(errors));
    }

    let mut defs = Map::new();
    defs.insert("main".to_owned(), Value::Object(main));
    defs.extend(carried);
    let file = json!({ "lexicon": 1, "id": nsid, "defs": Value::Object(defs) });
    admit(nsid, kind, &file)?;
    Ok(file)
}

/// The named definitions that are a bare primitive (a newtype over a string, say). A Lexicon
/// states those in place, so a reference to one is replaced by its definition.
fn primitive_definitions(definitions: &Map<String, Value>) -> BTreeMap<String, Value> {
    definitions
        .iter()
        .filter(|(_, definition)| {
            definition.as_object().is_some_and(|object| {
                matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("string" | "integer" | "boolean")
                ) && !object.contains_key("oneOf")
            })
        })
        .map(|(name, definition)| {
            let mut definition = definition.clone();
            if let Some(object) = definition.as_object_mut() {
                object.remove("description");
            }
            (name.clone(), definition)
        })
        .collect()
}

/// Rewrites what the datum projection refuses but a method type legitimately states, without
/// changing the wire form:
///
/// * a `$ref` with siblings (a field's rustdoc beside the reference) is the reference alone;
/// * a reference to a primitive definition is that primitive;
/// * a serde tagged enum (`#[serde(tag = "kind")]`, internal or adjacent) has no Lexicon spelling,
///   since a Lexicon union is `$type`-discriminated by NSID. It is stated as the object it is on
///   the wire: the tag as a closed string and every variant's fields as optional properties. A
///   Lexicon-only reader therefore accepts more than the type does, and the Rust type still
///   refuses the rest; the variant list stays in the tag's `enum`;
/// * a `oneOf` of string constants (an enum with documented unit variants) is one string `enum`;
/// * `true` (a field holding any JSON value) is `unknown`, the nearest Lexicon type: it is an
///   object, so a bare scalar there is outside what the Lexicon states.
fn normalize(value: &mut Value, primitives: &BTreeMap<String, Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                normalize(item, primitives);
            }
        }
        Value::Object(object) => {
            for child in object.values_mut() {
                normalize(child, primitives);
            }
            for key in ["properties", "items"] {
                match object.get_mut(key) {
                    Some(Value::Object(properties)) if key == "properties" => {
                        for property in properties.values_mut() {
                            if *property == Value::Bool(true) {
                                *property = unknown();
                            }
                        }
                    }
                    Some(items) if *items == Value::Bool(true) => *items = unknown(),
                    _ => {}
                }
            }
            if let Some(names) = string_constants(object) {
                *value = json!({ "type": "string", "enum": names });
                return;
            }
            if let Some(Value::String(reference)) = object.get("$ref") {
                if let Some(primitive) = reference
                    .strip_prefix("#/$defs/")
                    .and_then(|name| primitives.get(name))
                {
                    *value = primitive.clone();
                } else if object.len() > 1 {
                    let reference = Value::String(reference.clone());
                    object.clear();
                    object.insert("$ref".to_owned(), reference);
                }
            } else if let Some(flat) = flatten_tagged(object).or_else(|| flatten_external(object)) {
                *value = flat;
            }
        }
        _ => {}
    }
}

/// Any data object: a dynamic map with no stated value type projects to Lexicon `unknown`.
fn unknown() -> Value {
    json!({ "type": "object", "additionalProperties": true })
}

/// The spellings of a `oneOf` whose branches are all string constants or runs of them (schemars
/// groups the undocumented unit variants of an enum into one `enum` branch).
fn string_constants(object: &Map<String, Value>) -> Option<Vec<Value>> {
    let mut names = Vec::new();
    for branch in object.get("oneOf")?.as_array()? {
        if let Some(name) = branch.get("const").filter(|name| name.is_string()) {
            names.push(name.clone());
        } else {
            names.extend(branch.get("enum")?.as_array()?.iter().cloned());
        }
    }
    Some(names)
}

/// A serde externally tagged enum (`{"Variant": payload}` or a bare `"Unit"` string): with only
/// payload variants, an object whose optional properties are the variants; with any unit variant
/// the value is a string or an object, which no Lexicon type is, so it is `unknown`.
fn flatten_external(object: &Map<String, Value>) -> Option<Value> {
    let mut properties = Map::new();
    let mut unit = false;
    for branch in object.get("oneOf")?.as_array()? {
        if branch.get("const").is_some() || branch.get("enum").is_some() {
            unit = true;
            continue;
        }
        let variant = branch.get("properties")?.as_object()?;
        if variant.len() != 1 {
            return None;
        }
        let (name, payload) = variant.iter().next()?;
        properties.insert(name.clone(), payload.clone());
    }
    if unit || properties.is_empty() {
        return Some(unknown());
    }
    Some(json!({ "type": "object", "properties": properties }))
}

/// The flat object for a `oneOf` whose branches are objects sharing one constant-valued tag
/// property, or `None` for any other position.
fn flatten_tagged(object: &Map<String, Value>) -> Option<Value> {
    let branches = object.get("oneOf")?.as_array()?;
    let constant = |branch: &Value, tag: &str| {
        branch["properties"][tag]
            .get("const")
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let tag = branches
        .first()?
        .get("properties")?
        .as_object()?
        .iter()
        .find(|(_, property)| property.get("const").is_some_and(Value::is_string))
        .map(|(name, _)| name.clone())?;
    let mut variants = Vec::new();
    let mut properties = Map::new();
    for branch in branches {
        let variant = constant(branch, &tag)?;
        variants.push(Value::String(variant));
        for (name, property) in branch["properties"].as_object().into_iter().flatten() {
            if *name == tag {
                continue;
            }
            match properties.get(name) {
                // Variants that disagree about a field's type leave it any object.
                Some(existing) if existing != property => {
                    properties.insert(name.clone(), unknown());
                }
                _ => {
                    properties.insert(name.clone(), property.clone());
                }
            }
        }
    }
    properties.insert(tag.clone(), json!({ "type": "string", "enum": variants }));
    let mut flat = Map::new();
    flat.insert("type".to_owned(), json!("object"));
    flat.insert("properties".to_owned(), Value::Object(properties));
    flat.insert("required".to_owned(), json!([tag]));
    Some(Value::Object(flat))
}

/// The definition a `$ref` position names, else the position itself.
fn resolve<'a>(schema: &'a Value, definitions: &'a Map<String, Value>) -> &'a Value {
    schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.strip_prefix("#/$defs/"))
        .and_then(|name| definitions.get(name))
        .unwrap_or(schema)
}

/// One position through the datum projection, its reached definitions merged into `carried`.
fn project(
    nsid: &'static str,
    part: &'static str,
    schema: &Value,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
) -> Result<Value, MethodLexiconError> {
    let projected = project_position(schema, definitions)
        .map_err(|reason| MethodLexiconError::Projection { nsid, part, reason })?;
    Ok(split(projected, carried))
}

/// `project_position` returns the bare position, or `{position, $defs}` when it reached named
/// definitions; this hands back the position and merges the definitions.
fn split(projected: Value, carried: &mut BTreeMap<String, Value>) -> Value {
    match projected {
        Value::Object(mut object)
            if object.contains_key("position") && object.contains_key("$defs") =>
        {
            let Some(Value::Object(defs)) = object.remove("$defs") else {
                unreachable!("`$defs` is an object")
            };
            carried.extend(defs);
            object.remove("position").expect("checked above")
        }
        other => other,
    }
}

/// A query's or subscription's request as Lexicon `params`: the request object's properties, each
/// a primitive or an array of one; a field the object does not require is optional.
fn parameters(
    nsid: &'static str,
    request: &Value,
    definitions: &Map<String, Value>,
) -> Result<Value, MethodLexiconError> {
    if request.get("type") != Some(&json!("object")) {
        return Err(MethodLexiconError::NotAnObject {
            nsid,
            part: "request",
            what: "parameters list",
        });
    }
    let mut carried = BTreeMap::new();
    let projected = project(nsid, "request", request, definitions, &mut carried)?;
    let mut properties = projected
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if !carried.is_empty() {
        let name = properties
            .iter()
            .find(|(_, property)| property.get("type") == Some(&json!("ref")))
            .map_or_else(String::new, |(name, _)| name.clone());
        return Err(MethodLexiconError::NotFlat { nsid, name });
    }
    for (name, property) in &mut properties {
        let item = match property.get("type").and_then(Value::as_str) {
            Some("array") => &property["items"],
            _ => &*property,
        };
        if !matches!(
            item.get("type").and_then(Value::as_str),
            Some("boolean" | "integer" | "string" | "unknown")
        ) {
            return Err(MethodLexiconError::NotFlat {
                nsid,
                name: name.clone(),
            });
        }
    }
    let mut params = Map::new();
    params.insert("type".to_owned(), json!("params"));
    if let Some(required) = projected.get("required") {
        params.insert("required".to_owned(), required.clone());
    }
    params.insert("properties".to_owned(), Value::Object(properties));
    Ok(Value::Object(params))
}

/// The variants of a serde externally tagged enum, by outer name: a unit variant is a string
/// constant, any other a one-key object. The variant's rustdoc is its description.
fn error_names(nsid: &'static str, error: &Value) -> Result<Vec<Value>, MethodLexiconError> {
    let not_an_enum = || MethodLexiconError::ErrorNotAnEnum { nsid };
    let branches = error
        .get("oneOf")
        .and_then(Value::as_array)
        .ok_or_else(not_an_enum)?;
    let mut errors = Vec::new();
    for branch in branches {
        // A run of undocumented unit variants is one `enum` branch.
        if let Some(names) = branch.get("enum").and_then(Value::as_array) {
            for name in names {
                errors.push(json!({ "name": name }));
            }
            continue;
        }
        let name = match branch.get("const") {
            Some(name) => name.clone(),
            None => {
                let properties = branch
                    .get("properties")
                    .and_then(Value::as_object)
                    .filter(|properties| properties.len() == 1)
                    .ok_or_else(not_an_enum)?;
                // The variant that carries `ContextError` is how the common errors reach the
                // enum; they are the same for every method and are not listed per method.
                if properties
                    .values()
                    .any(|payload| payload.get("$ref") == Some(&json!("#/$defs/ContextError")))
                {
                    continue;
                }
                json!(properties.keys().next().expect("one property"))
            }
        };
        let mut entry = Map::new();
        entry.insert("name".to_owned(), name);
        if let Some(description) = branch.get("description") {
            entry.insert("description".to_owned(), description.clone());
        }
        errors.push(Value::Object(entry));
    }
    Ok(errors)
}

/// The frame check a nodule's own operation Lexicons pass, plus that every file-local `ref`
/// resolves. A `ref` to another NSID (a datum union's member) is reported by the frame check and
/// left to the bundle that binds the providers, as for a nodule's own Lexicons.
fn admit(nsid: &'static str, kind: OperationKind, file: &Value) -> Result<(), MethodLexiconError> {
    let frame = |reason: String| MethodLexiconError::Frame { nsid, reason };
    let defs = file["defs"].as_object().expect("assembled with defs");
    let refs = validate(kind, defs, "$").map_err(|error| frame(error.to_string()))?;
    for reference in refs {
        let local = reference.target.strip_prefix('#');
        if local.is_some_and(|name| !defs.contains_key(name)) {
            return Err(frame(format!(
                "{} refers to `{}`, which the file does not carry",
                reference.path, reference.target
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
