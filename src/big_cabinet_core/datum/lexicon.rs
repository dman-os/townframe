//! The Lexicon a datum publishes, projected from its sidecar JSON Schema.
//!
//! Lexicon is the primary artifact: it states the canonical JSON a peer reads. The sidecar is what
//! Lexicon cannot say — the `x-automerge` storage annotations, and refinements that *narrow* a
//! Lexicon type. Narrowing is the whole arrangement: a dynamic map is a Lexicon `unknown` whose
//! value type and key pattern the sidecar states, and a Lexicon `string` keeps its `pattern` and its byte lengths
//! there, because Lexicon counts UTF-8 bytes where JSON Schema counts code points and copying one
//! into the other would silently change the constraint.
//!
//! What the sidecar may never do is admit a value outside the canonical data model. So a plain
//! `number` is refused rather than projected: the convention carries a float as a decimal string,
//! and a datum that opts out of it is a codec document, not a publishable declaration.
//!
//! The projection errs rather than widening. A position whose sidecar cannot be said in Lexicon is
//! an error naming the position; it is never quietly turned into an `unknown` that admits more than
//! the author declared.

use super::codec::KEYWORD;
use super::codec::keyed_map;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The object keys the canonical data model reserves. An application `$` key is refused everywhere,
/// including inside content no schema describes, so that relaxing this later stays possible.
const PROTOCOL_KEYS: [&str; 3] = ["$type", "$bytes", "$link"];
/// The union discriminant. Internal tagging is the only layout a Lexicon union has.
const TAG: &str = "$type";

/// Project one datum's sidecar document — its definition plus the `$defs` it carries — into the
/// Lexicon file it publishes.
///
/// Named definitions the datum reaches are carried into this file and referenced locally, which is
/// what lets a plug be read on its own: there is no registry, and a definition two datums both
/// reach is carried by both. A union member is referenced by the id its `$type` const states,
/// because that id is data a peer reads and not a structure this file owns.
pub fn project_datum(id: &str, document: &Value) -> Result<Value, String> {
    let definitions = document
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut carried: BTreeMap<String, Value> = BTreeMap::new();
    let main = position(document, &definitions, &mut carried, "$")?;
    if main.get("type").and_then(Value::as_str) != Some("object") {
        return Err(
            "$: a datum's Lexicon `main` is an object, and a union, an array or a scalar is a \
             position inside one"
                .to_owned(),
        );
    }
    let mut defs = Map::new();
    defs.insert("main".to_owned(), main);
    for (name, definition) in carried {
        defs.insert(name, definition);
    }
    let file = json!({ "lexicon": 1, "id": id, "defs": Value::Object(defs) });
    validate(&file).map_err(|reason| format!("the projection is not a Lexicon file: {reason}"))?;
    Ok(file)
}

/// Project one schema position, carrying the definitions it reaches.
///
/// [`project_datum`] refuses anything but an object, because a datum's `main` is one. A compatibility
/// check is asked about arbitrary positions — a string, an array, a union — so it projects the
/// position on its own and compares the results. The rules are the same; only the object requirement
/// is absent.
pub fn project_position(schema: &Value, definitions: &Map<String, Value>) -> Result<Value, String> {
    let mut carried = BTreeMap::new();
    let projected = position(schema, definitions, &mut carried, "$")?;
    if carried.is_empty() {
        return Ok(projected);
    }
    let mut out = Map::new();
    out.insert("position".to_owned(), projected);
    out.insert(
        "$defs".to_owned(),
        Value::Object(carried.into_iter().collect()),
    );
    Ok(Value::Object(out))
}

/// One schema position, projected. `carried` accumulates the local definitions the position
/// reached, so a `$ref` is emitted as a reference and its target travels beside it.
fn position(
    schema: &Value,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<Value, String> {
    let Some(object) = schema.as_object() else {
        return Err(here(
            path,
            "a position is a schema object: `true` admits any JSON value, which no Lexicon type does",
        ));
    };
    for keyword in [
        "allOf",
        "not",
        "prefixItems",
        "propertyNames",
        "dependentSchemas",
        "contains",
        "unevaluatedItems",
        "if",
        "then",
        "else",
    ] {
        if object.contains_key(keyword) {
            return Err(here(
                path,
                format!("`{keyword}` composes a position Lexicon cannot state"),
            ));
        }
    }
    // A keyed map is the one `patternProperties` whose members share a declaration, and it projects
    // as the dynamic map it is; any other use is refused here, wherever the position sits.
    keyed_map(object).map_err(|reason| here(path, reason))?;
    if let Some(reference) = object.get("$ref") {
        return reference_position(reference, object.len(), definitions, carried, path);
    }
    if object.contains_key("oneOf") || object.contains_key("anyOf") {
        return union(object, definitions, path);
    }
    if let Some(marker) = object.get(KEYWORD)
        && let Some(projected) = marked(marker, object, path)?
    {
        return Ok(projected);
    }
    match object.get("type") {
        Some(Value::String(kind)) => typed(kind, object, definitions, carried, path),
        Some(Value::Array(kinds)) => Err(here(
            path,
            format!(
                "{kinds:?} states more than one type: `null` belongs to an object's `nullable` \
                 list, and nothing else has two Lexicon types"
            ),
        )),
        Some(other) => Err(here(
            path,
            format!("`type` is {other}, which is not a type name"),
        )),
        // No `type` at all: an object is still an object when it says what it holds, and anything
        // else is a position that states nothing and would project as a wider type than it is.
        None if object.contains_key("properties")
            || object.contains_key("additionalProperties")
            || object.contains_key("unevaluatedProperties") =>
        {
            object_position(object, definitions, carried, path)
        }
        None => Err(here(
            path,
            "a position states no type and holds no properties, so there is nothing to project",
        )),
    }
}

/// A `$ref` becomes a local reference, and the definition it names is carried into this file.
fn reference_position(
    reference: &Value,
    siblings: usize,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<Value, String> {
    if siblings != 1 {
        return Err(here(
            path,
            "`$ref` has siblings, which compose the reference with constraints Lexicon cannot state",
        ));
    }
    let Some(reference) = reference.as_str() else {
        return Err(here(path, "`$ref` is not a string"));
    };
    let Some(name) = reference.strip_prefix("#/$defs/") else {
        return Err(here(
            path,
            format!("`{reference}` is not a reference into this document's definitions"),
        ));
    };
    if carried.contains_key(name) {
        return Ok(json!({ "type": "ref", "ref": format!("#{name}") }));
    }
    let Some(definition) = definitions.get(name) else {
        return Err(here(
            path,
            format!("`{reference}` names a definition this document does not carry"),
        ));
    };
    // Placed before the recursion so a definition that reaches itself is reported rather than
    // exhausting the stack; the sentinel is always overwritten on the way out.
    carried.insert(name.to_owned(), Value::Null);
    let projected = position(definition, definitions, carried, &format!("{path}#/{name}"));
    match projected {
        Ok(projected) => {
            carried.insert(name.to_owned(), projected);
            Ok(json!({ "type": "ref", "ref": format!("#{name}") }))
        }
        Err(reason) => {
            carried.remove(name);
            Err(reason)
        }
    }
}

/// The storage forms, each of which states a JSON shape Lexicon has a type for.
///
/// `Ok(None)` is the one marker that is not a storage form — the deliberate list opt-out — and
/// falls through to the ordinary projection of the array it annotates.
fn marked(
    marker: &Value,
    object: &Map<String, Value>,
    path: &str,
) -> Result<Option<Value>, String> {
    let Some(marker) = marker.as_str() else {
        return Err(here(
            path,
            format!("`{KEYWORD}` is {marker}, which is not a storage form"),
        ));
    };
    let stated = |wanted: &str| {
        object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == wanted)
    };
    match marker {
        "bytes" => {
            if !bytes_object(object) {
                return Err(here(
                    path,
                    "canonical bytes are the `{\"$bytes\": \"…\"}` object, and this position is not \
                     one: the marker and the JSON shape disagree",
                ));
            }
            Ok(Some(json!({ "type": "bytes" })))
        }
        "float" | "text" | "timestamp" | "reference" => {
            if !stated("string") {
                return Err(here(
                    path,
                    format!("a `{marker}` is carried as a string, and this position is not one"),
                ));
            }
            Ok(Some(json!({ "type": "string" })))
        }
        "counter" => {
            if !stated("integer") {
                return Err(here(
                    path,
                    "a counter is carried as a whole number, and this position is not one",
                ));
            }
            Ok(Some(json!({ "type": "integer" })))
        }
        // The opt-out that says a `Vec<u8>` is deliberately an array of numbers: an ordinary array,
        // which the ordinary projection states.
        "list" => {
            if !stated("array") {
                return Err(here(
                    path,
                    "the list marker annotates an array, and this position is not one",
                ));
            }
            Ok(None)
        }
        // A plain number is the float opt-out. It is a legitimate codec position and it is not
        // canonical JSON, so the datum that holds one is not publishable.
        "number" => Err(here(
            path,
            "a plain `number` is the float opt-out: Lexicon has no float, and a datum that carries \
             one is a codec document rather than a publishable declaration",
        )),
        _ => Err(here(
            path,
            format!("`{KEYWORD}` is `{marker}`, which is not a storage form this build knows"),
        )),
    }
}

/// Whether a position is the canonical bytes object: one `$bytes` member, a base64 string, and
/// nothing else admitted.
fn bytes_object(object: &Map<String, Value>) -> bool {
    object.get("type").and_then(Value::as_str) == Some("object")
        && object
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| {
                properties.len() == 1
                    && properties
                        .get("$bytes")
                        .and_then(|member| member.get("type"))
                        .and_then(Value::as_str)
                        == Some("string")
            })
        && object.get("additionalProperties") == Some(&Value::Bool(false))
}

fn typed(
    kind: &str,
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<Value, String> {
    match kind {
        "string" => Ok(string(object, path)?),
        "integer" => Ok(integer(object, path)?),
        "boolean" => Ok(boolean(object)),
        "array" => array(object, definitions, carried, path),
        "object" => object_position(object, definitions, carried, path),
        "number" => Err(here(
            path,
            "a plain `number` has no Lexicon type: a float is carried as the decimal string \
             `datum::Float64` declares, and a whole number is an `integer`",
        )),
        "null" => Err(here(
            path,
            "`null` is not a position's type: it belongs to the `nullable` list of the object that \
             holds the position",
        )),
        _ => Err(here(path, format!("`{kind}` is not a JSON Schema type"))),
    }
}

/// A string, with the constraints Lexicon states and none of the ones it counts differently.
///
/// `minLength`/`maxLength` stay in the sidecar on purpose: Lexicon counts UTF-8 bytes and JSON
/// Schema counts code points, so copying the number would state a different constraint in each.
/// `pattern` has no Lexicon spelling at all. Both keep narrowing the sidecar, which is the
/// arrangement; the grapheme counts and the value sets are Lexicon's own and travel.
fn string(object: &Map<String, Value>, path: &str) -> Result<Value, String> {
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("string"));
    if let Some(format) = object.get("format").and_then(Value::as_str)
        && let Some(spelled) = lexicon_format(format)
    {
        out.insert("format".to_owned(), Value::from(spelled));
    }
    for key in ["knownValues", "enum", "minGraphemes", "maxGraphemes"] {
        if let Some(value) = object.get(key) {
            strings(value)
                .ok_or_else(|| here(path, format!("`{key}` is not a list of strings")))?;
            out.insert(key.to_owned(), value.clone());
        }
    }
    for key in ["const", "default"] {
        if let Some(value) = object.get(key) {
            if !value.is_string() {
                return Err(here(path, format!("a string's `{key}` is a string")));
            }
            out.insert(key.to_owned(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

/// The string formats Lexicon names. One JSON Schema spelling differs — `date-time` is
/// `datetime` — and a format Lexicon does not know stays in the sidecar, where it narrows.
fn lexicon_format(format: &str) -> Option<&'static str> {
    match format {
        "date-time" => Some("datetime"),
        "datetime" => Some("datetime"),
        "at-identifier" => Some("at-identifier"),
        "at-uri" => Some("at-uri"),
        "cid" => Some("cid"),
        "did" => Some("did"),
        "handle" => Some("handle"),
        "nsid" => Some("nsid"),
        "tid" => Some("tid"),
        "record-key" => Some("record-key"),
        "uri" => Some("uri"),
        "language" => Some("language"),
        _ => None,
    }
}

fn integer(object: &Map<String, Value>, path: &str) -> Result<Value, String> {
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("integer"));
    for key in ["minimum", "maximum"] {
        if let Some(value) = object.get(key) {
            if !value.is_i64() && !value.is_u64() {
                return Err(here(
                    path,
                    format!("an integer's `{key}` is a whole number"),
                ));
            }
            out.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(values) = object.get("enum") {
        if !values
            .as_array()
            .is_some_and(|values| !values.is_empty() && values.iter().all(Value::is_i64))
        {
            return Err(here(path, "an integer's `enum` is a list of whole numbers"));
        }
        out.insert("enum".to_owned(), values.clone());
    }
    for key in ["const", "default"] {
        if let Some(value) = object.get(key) {
            if !value.is_i64() && !value.is_u64() {
                return Err(here(
                    path,
                    format!("an integer's `{key}` is a whole number"),
                ));
            }
            out.insert(key.to_owned(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

fn boolean(object: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("boolean"));
    for key in ["const", "default"] {
        if let Some(value @ Value::Bool(_)) = object.get(key) {
            out.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(out)
}

fn array(
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<Value, String> {
    let Some(items) = object.get("items") else {
        return Err(here(path, "an array states its `items`"));
    };
    if items.is_array() {
        return Err(here(
            path,
            "positional `items` is a tuple, and Lexicon arrays are homogeneous",
        ));
    }
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("array"));
    out.insert(
        "items".to_owned(),
        position(items, definitions, carried, &format!("{path}[]"))?,
    );
    // Lexicon counts array elements with the length keywords, where JSON Schema counts them with
    // the item keywords: the same bound, in the spelling its type uses.
    for (from, to) in [("minItems", "minLength"), ("maxItems", "maxLength")] {
        if let Some(value) = object.get(from) {
            out.insert(to.to_owned(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

/// An object: its named properties, or the dynamic map that has none.
///
/// A dynamic map is the one place the two layers deliberately disagree. Lexicon has no map type, so
/// the map projects as `unknown` — any data object — and the sidecar states the value type it
/// narrows to. A Lexicon-only peer therefore accepts more than this datum does, which is what the
/// sidecar is for; what it may not do is admit a value outside the canonical data model, so the
/// value type is projected for its alignment and then dropped rather than trusted unchecked.
///
/// A keyed map is the same dynamic map with its keys narrowed too: the sidecar's one anchored
/// pattern is a constraint Lexicon cannot state, exactly like a string's `pattern`, so it stays in
/// the sidecar and the position is the same `unknown`.
fn object_position(
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<Value, String> {
    let properties = object.get("properties").and_then(Value::as_object);
    let keyed = keyed_map(object)
        .expect("`position` refused a `patternProperties` that is not a keyed map");
    if let Some(values) = keyed.or_else(|| typed_catch_all(object)) {
        if properties.is_some_and(|properties| !properties.is_empty()) {
            return Err(here(
                path,
                "an object names properties and also states a typed catch-all: Lexicon has one \
                 shape for a record and another for a dynamic map, and this position is both",
            ));
        }
        position(
            values,
            definitions,
            &mut BTreeMap::new(),
            &format!("{path}.<values>"),
        )?;
        return Ok(json!({ "type": "unknown" }));
    }
    let Some(properties) = properties.filter(|properties| !properties.is_empty()) else {
        // Nothing named and nothing caught: Lexicon's `object` requires its properties, and an
        // object with none is the `unknown` whose sidecar states the rest.
        return Ok(json!({ "type": "unknown" }));
    };
    let mut projected = Map::new();
    let mut nullable = Vec::new();
    for (name, schema) in properties {
        let child = format!("{path}.{name}");
        if name.starts_with('$') && !PROTOCOL_KEYS.contains(&name.as_str()) {
            return Err(here(
                &child,
                "an application key cannot start with `$`: the canonical data model reserves it, \
                 and refusing it here is what leaves the spelling free to be relaxed later",
            ));
        }
        let (member, admits_null) = property(schema, definitions, carried, &child)?;
        projected.insert(name.clone(), member);
        if admits_null {
            nullable.push(Value::from(name.clone()));
        }
    }
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("object"));
    out.insert("properties".to_owned(), Value::Object(projected));
    if let Some(required) = object.get("required") {
        for name in
            strings(required).ok_or_else(|| here(path, "`required` is a list of property names"))?
        {
            if !properties.contains_key(name) {
                return Err(here(
                    path,
                    format!("`required` names `{name}`, which is not one of its properties"),
                ));
            }
        }
        out.insert("required".to_owned(), required.clone());
    }
    if !nullable.is_empty() {
        out.insert("nullable".to_owned(), Value::Array(nullable));
    }
    Ok(Value::Object(out))
}

/// One object property, and whether it admits `null`.
///
/// Lexicon states nullability as a list of names on the object holding the position, so this is the
/// only place a `null` is expressible. An optional array element, map value or union member has
/// nowhere to put one and is refused rather than projected as the non-nullable type it is not.
fn property(
    schema: &Value,
    definitions: &Map<String, Value>,
    carried: &mut BTreeMap<String, Value>,
    path: &str,
) -> Result<(Value, bool), String> {
    if let Some(object) = schema.as_object() {
        if let Some(kinds) = object.get("type").and_then(Value::as_array) {
            let (nulls, rest): (Vec<_>, Vec<_>) =
                kinds.iter().partition(|kind| kind.as_str() == Some("null"));
            if !nulls.is_empty() {
                let [only] = rest.as_slice() else {
                    return Err(here(
                        path,
                        format!("{kinds:?} is nullable and states more than one other type"),
                    ));
                };
                let mut inner = object.clone();
                inner.insert("type".to_owned(), (*only).clone());
                return Ok((
                    position(&Value::Object(inner), definitions, carried, path)?,
                    true,
                ));
            }
        }
        // What `schemars` emits for an `Option` around a named type: the type and `null` as branches.
        for keyword in ["anyOf", "oneOf"] {
            if let Some(branches) = object.get(keyword).and_then(Value::as_array) {
                let (nulls, rest): (Vec<_>, Vec<_>) =
                    branches.iter().partition(|branch| is_null(branch));
                if !nulls.is_empty() {
                    let [only] = rest.as_slice() else {
                        return Err(here(
                            path,
                            format!(
                                "`{keyword}` is nullable and states more than one other branch"
                            ),
                        ));
                    };
                    return Ok((position(only, definitions, carried, path)?, true));
                }
            }
        }
    }
    Ok((position(schema, definitions, carried, path)?, false))
}

/// A union, which in Lexicon is a list of the ids its members carry.
///
/// Internal `$type` tagging is the only layout that has one: the discriminant is a member of the
/// payload object, so the branch's `$type` const *is* the reference. An adjacent or external branch
/// has no such const and is refused, which is what makes the tagging a published-declaration
/// decision rather than a wire detail.
fn union(
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
    path: &str,
) -> Result<Value, String> {
    let branches = match (object.get("oneOf"), object.get("anyOf")) {
        (Some(branches), None) | (None, Some(branches)) => branches,
        (Some(_), Some(_)) => {
            return Err(here(
                path,
                "a position states both `oneOf` and `anyOf`, which is two unions at once",
            ));
        }
        (None, None) => return Err(here(path, "a union states its branches")),
    };
    let branches = branches
        .as_array()
        .ok_or_else(|| here(path, "a union's branches are a list"))?;
    let mut refs: Vec<String> = Vec::new();
    let mut fallback: Option<BTreeSet<String>> = None;
    for (index, branch) in branches.iter().enumerate() {
        let branch_path = format!("{path}<{index}>");
        let branch = resolve(branch, definitions, &branch_path)?;
        let Some(properties) = branch.get("properties").and_then(Value::as_object) else {
            return Err(here(
                &branch_path,
                "a union branch is an object that carries its `$type` discriminant",
            ));
        };
        let Some(discriminant) = properties.get(TAG) else {
            return Err(here(
                &branch_path,
                "a union branch states no `$type` discriminant, so it is not an internally tagged \
                 Lexicon union: adjacent and external tagging are representable in the sidecar and \
                 are not publishable",
            ));
        };
        if let Some(id) = discriminant.get("const").and_then(Value::as_str) {
            check_member(branch, definitions, &branch_path)?;
            refs.push(id.to_owned());
        } else if let Some(excluded) = open_fallback(discriminant) {
            if fallback.is_some() {
                return Err(here(
                    &branch_path,
                    "a union has exactly one fallback branch",
                ));
            }
            fallback = Some(excluded);
        } else {
            return Err(here(
                &branch_path,
                "a branch's `$type` is neither one member's id nor the fallback's exclusion",
            ));
        }
    }
    if let Some(excluded) = &fallback {
        let known: BTreeSet<String> = refs.iter().cloned().collect();
        if *excluded != known {
            return Err(here(
                path,
                format!(
                    "the fallback excludes {excluded:?} while the union declares {known:?}: a \
                     member would match two branches or none"
                ),
            ));
        }
    }
    let closed = fallback.is_none();
    if closed && refs.is_empty() {
        return Err(here(path, "a closed union names at least one member"));
    }
    let mut out = Map::new();
    out.insert("type".to_owned(), Value::from("union"));
    out.insert(
        "refs".to_owned(),
        Value::Array(refs.into_iter().map(Value::from).collect()),
    );
    if closed {
        out.insert("closed".to_owned(), Value::Bool(true));
    }
    Ok(Value::Object(out))
}

/// Validate one member's payload, which the union references by id rather than carries.
///
/// The payload is projected into a scratch carry and the result dropped: the definitions it reaches
/// travel with the Lexicon its id names, so carrying them here would publish one member twice under
/// two identities. What matters is that the payload *is* projectable, which is the alignment the
/// discriminant alone cannot show.
fn check_member(
    branch: &Value,
    definitions: &Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    let mut payload = branch
        .as_object()
        .ok_or_else(|| here(path, "a union branch is an object"))?
        .clone();
    // The discriminant is the union's, not the payload's: stripping it is what the wire form does
    // on the way in, and projecting it here would ask the member to declare its own `$type`.
    if let Some(properties) = payload.get_mut("properties").and_then(Value::as_object_mut) {
        properties.remove(TAG);
    }
    if let Some(required) = payload.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|name| name.as_str() != Some(TAG));
    }
    position(
        &Value::Object(payload),
        definitions,
        &mut BTreeMap::new(),
        &format!("{path}.<payload>"),
    )?;
    Ok(())
}

/// The exclusion list of an open union's fallback, if this discriminant is one.
fn open_fallback(discriminant: &Value) -> Option<BTreeSet<String>> {
    if discriminant.get("type").and_then(Value::as_str) != Some("string") {
        return None;
    }
    discriminant
        .get("not")
        .and_then(|not| not.get("enum"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
}

/// The schema a typed catch-all states for the members an object does not name.
fn typed_catch_all(object: &Map<String, Value>) -> Option<&Value> {
    ["additionalProperties", "unevaluatedProperties"]
        .iter()
        .find_map(|key| object.get(*key).filter(|value| !value.is_boolean()))
}

/// Follow a branch's `$ref` chain to the schema it names.
fn resolve<'a>(
    schema: &'a Value,
    definitions: &'a Map<String, Value>,
    path: &str,
) -> Result<&'a Value, String> {
    let mut current = schema;
    for _ in 0..16 {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return Ok(current);
        };
        let Some(name) = reference.strip_prefix("#/$defs/") else {
            return Err(here(
                path,
                format!("`{reference}` is not a reference into this document's definitions"),
            ));
        };
        current = definitions.get(name).ok_or_else(|| {
            here(
                path,
                format!("`{reference}` names a definition this document does not carry"),
            )
        })?;
    }
    Err(here(path, "a reference chain that does not settle"))
}

fn is_null(schema: &Value) -> bool {
    schema
        .as_object()
        .is_some_and(|object| object.get("type") == Some(&json!("null")))
}

fn strings(value: &Value) -> Option<Vec<&str>> {
    value
        .as_array()
        .map(|values| values.iter().filter_map(Value::as_str).collect())
}

fn here(path: &str, reason: impl std::fmt::Display) -> String {
    format!("{path}: {reason}")
}

/// The Lexicon grammar, checked against what the projector emitted.
///
/// Nothing outside this crate validates these files, and the projector is the only thing that
/// writes them — so this checks the projector, not an author. A type it invented (`map`, which
/// Lexicon does not have), a reference it left dangling, a keyword it copied into a type that has
/// no such field, or a definition named with a type that cannot be one is caught here instead of
/// being published as an artifact no peer can read.
fn validate(file: &Value) -> Result<(), String> {
    let file = file.as_object().ok_or("a Lexicon file is an object")?;
    if file.get("lexicon") != Some(&json!(1)) {
        return Err("`lexicon` is the language version, 1".to_owned());
    }
    let id = file
        .get("id")
        .and_then(Value::as_str)
        .ok_or("`id` is the NSID this file is published under")?;
    if id.is_empty() || id.contains('#') {
        return Err(format!("`{id}` is not an NSID"));
    }
    let defs = file
        .get("defs")
        .and_then(Value::as_object)
        .ok_or("`defs` is required, and a file with none is invalid")?;
    if !defs.contains_key("main") {
        return Err("`defs` has no `main`".to_owned());
    }
    for (name, definition) in defs {
        let path = format!("#{name}");
        let kind = definition
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{path}: a definition states its type"))?;
        // A sub-type, an `unknown`, a `ref` and a `union` are positions rather than names: nothing
        // can point at them, so none of them can be a definition.
        if matches!(kind, "unknown" | "ref" | "union" | "params" | "permission") {
            return Err(format!("{path}: `{kind}` cannot be a named definition"));
        }
        schema(definition, defs, &path)?;
    }
    Ok(())
}

fn schema(value: &Value, defs: &Map<String, Value>, path: &str) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{path}: a schema is an object"))?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path}: a schema states its type"))?;
    if allowed(kind).is_empty() {
        return Err(format!(
            "{path}: `{kind}` is not a Lexicon type, so the projection invented one"
        ));
    }
    for key in object.keys() {
        if !allowed(kind).contains(&key.as_str()) {
            return Err(format!(
                "{path}: `{key}` is not a field of a Lexicon `{kind}`"
            ));
        }
    }
    match kind {
        "object" => {
            let properties = object
                .get("properties")
                .and_then(Value::as_object)
                .filter(|properties| !properties.is_empty())
                .ok_or_else(|| {
                    format!("{path}: an object has properties; a position with none is `unknown`")
                })?;
            for (name, member) in properties {
                schema(member, defs, &format!("{path}.{name}"))?;
            }
            for key in ["required", "nullable"] {
                let Some(names) = object.get(key) else {
                    continue;
                };
                for name in strings(names)
                    .ok_or_else(|| format!("{path}: `{key}` is a list of property names"))?
                {
                    if !properties.contains_key(name) {
                        return Err(format!(
                            "{path}: `{key}` names `{name}`, which is not one of its properties"
                        ));
                    }
                }
            }
            Ok(())
        }
        "array" => {
            let items = object
                .get("items")
                .ok_or_else(|| format!("{path}: an array states its `items`"))?;
            schema(items, defs, &format!("{path}[]"))
        }
        "string" => match object.get("format").and_then(Value::as_str) {
            Some(format) if !FORMATS.contains(&format) => {
                Err(format!("{path}: `{format}` is not a Lexicon string format"))
            }
            _ => Ok(()),
        },
        "ref" => target(object.get("ref"), defs, path),
        "union" => {
            let refs = object
                .get("refs")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("{path}: a union states its `refs`"))?;
            for reference in refs {
                target(Some(reference), defs, path)?;
            }
            if object.get("closed") == Some(&json!(true)) && refs.is_empty() {
                return Err(format!("{path}: a closed union with no members is invalid"));
            }
            Ok(())
        }
        "integer" | "boolean" | "bytes" | "cid-link" | "blob" | "unknown" | "token" => Ok(()),
        _ => Err(format!(
            "{path}: `{kind}` is a primary type, which a datum value is not"
        )),
    }
}

/// One reference target: a local definition this file carries, or the NSID of another file.
fn target(reference: Option<&Value>, defs: &Map<String, Value>, path: &str) -> Result<(), String> {
    let reference = reference
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path}: a reference states its target"))?;
    match reference.strip_prefix('#') {
        Some(name) => {
            if name == "main" {
                return Err(format!(
                    "{path}: `main` is referenced by its NSID alone, never as `#main`"
                ));
            }
            if !defs.contains_key(name) {
                return Err(format!(
                    "{path}: `#{name}` is not a definition this file carries"
                ));
            }
        }
        None if !reference.contains('.') => {
            return Err(format!("{path}: `{reference}` is not an NSID"));
        }
        None => {}
    }
    Ok(())
}

/// The fields each Lexicon type has, `description` included. A key outside the list is a projection
/// bug rather than a refinement: Lexicon has no extension mechanism, so the sidecar is where a
/// narrowing lives.
fn allowed(kind: &str) -> &'static [&'static str] {
    match kind {
        "object" => &["type", "description", "properties", "required", "nullable"],
        "array" => &["type", "description", "items", "minLength", "maxLength"],
        "string" => &[
            "type",
            "description",
            "format",
            "minLength",
            "maxLength",
            "minGraphemes",
            "maxGraphemes",
            "knownValues",
            "enum",
            "default",
            "const",
        ],
        "integer" => &[
            "type",
            "description",
            "minimum",
            "maximum",
            "enum",
            "default",
            "const",
        ],
        "boolean" => &["type", "description", "default", "const"],
        "bytes" => &["type", "description", "minLength", "maxLength"],
        "blob" => &["type", "description", "accept", "maxSize"],
        "ref" => &["type", "description", "ref"],
        "union" => &["type", "description", "refs", "closed"],
        "cid-link" | "unknown" | "token" => &["type", "description"],
        _ => &[],
    }
}

/// The string formats Lexicon names. A format it does not know stays in the sidecar, where it
/// narrows a Lexicon `string` rather than pretending to be one.
const FORMATS: [&str; 11] = [
    "at-identifier",
    "at-uri",
    "cid",
    "datetime",
    "did",
    "handle",
    "nsid",
    "tid",
    "record-key",
    "uri",
    "language",
];

#[cfg(test)]
mod tests;
