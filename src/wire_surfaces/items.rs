//! The items of a published surface that declare a stability (ADR 021, "Stability is declared per
//! item"), keyed by their path in the sidecar.
//!
//! An item is a field, a definition or a union member of a datum, or the whole file (a method, or a
//! datum that is itself unstable). Every one that exists is listed, annotated or not, because
//! "a stable item is never removed" needs the baseline items that carry no annotation as well as
//! the ones that do. Paths are sidecar JSON pointers with the union members spelled
//! `<union>/$union/<member id>`; `$` cannot begin an application key, so the spelling is free.

use crate::evolution::Surface;
use big_cabinet_core::datum::lexicon::stability::Declared;
use serde_json::Value;
use std::collections::BTreeMap;

/// The sidecar with the datum's own definition as its root. The sidecar file renders a datum as a
/// `$ref` to its definition in `$defs`; the Lexicon calls that definition `main`, and a path in
/// either is easier to relate when the root *is* the datum. The definition is dropped from `$defs`
/// unless something else refers to it (a recursive type), so a change is reported once.
pub fn main_form(sidecar: &Value) -> Value {
    let Some(name) = sidecar
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.strip_prefix("#/$defs/"))
    else {
        return sidecar.clone();
    };
    let mut defs = sidecar["$defs"].as_object().cloned().unwrap_or_default();
    let Some(Value::Object(mut root)) = defs.get(name).cloned() else {
        return sidecar.clone();
    };
    if !defs
        .iter()
        .filter(|(other, _)| other.as_str() != name)
        .any(|(_, definition)| refers_to(definition, name))
        && !refers_to(&Value::Object(root.clone()), name)
    {
        defs.remove(name);
    }
    if !defs.is_empty() {
        root.insert("$defs".to_owned(), Value::Object(defs));
    }
    Value::Object(root)
}

fn refers_to(value: &Value, name: &str) -> bool {
    match value {
        Value::String(reference) => reference.strip_prefix("#/$defs/") == Some(name),
        Value::Array(items) => items.iter().any(|item| refers_to(item, name)),
        Value::Object(object) => object.values().any(|member| refers_to(member, name)),
        _ => false,
    }
}

/// The union member a branch states: the `$type` constant of the object it is, or the one its
/// `$ref` names. A branch without one is the open union's fallback.
pub fn member_id<'a>(branch: &'a Value, defs: Option<&'a Value>) -> Option<&'a str> {
    let branch = match branch.get("$ref").and_then(Value::as_str) {
        Some(reference) => defs?.get(reference.strip_prefix("#/$defs/")?)?,
        None => branch,
    };
    branch["properties"]["$type"]["const"].as_str()
}

/// The non-`null` branches of a union-looking position and whether it is a union of members: an
/// `Option` around a type is `anyOf` the type and `null`, which is a nullable position and not a
/// union.
pub fn branches(position: &Value) -> Option<(Vec<&Value>, bool)> {
    let list = ["oneOf", "anyOf"]
        .iter()
        .find_map(|keyword| position.get(*keyword)?.as_array())?;
    let rest: Vec<&Value> = list
        .iter()
        .filter(|branch| branch.get("type") != Some(&Value::from("null")))
        .collect();
    let nullable = rest.len() == 1 && rest[0].pointer("/properties/$type").is_none();
    Some((rest, !nullable))
}

/// Every item `surface` declares, by path; `""` is the whole file.
pub fn items(surface: &Surface) -> BTreeMap<String, Declared> {
    let declared = |value: &Value| {
        Declared::read(value.as_object().expect("a schema position is an object"))
            .expect("the projection validated every stability annotation")
    };
    let mut out = BTreeMap::new();
    // A method has no sidecar: its stability sits beside its `revision`, and a file that names
    // a revision is the stable one.
    let mut file = declared(surface.lexicon);
    if surface.sidecar.is_none() {
        file.since = surface.lexicon["revision"].as_u64();
    }
    if let Some(sidecar) = surface.sidecar {
        let root = declared(sidecar);
        file.deprecated = file.deprecated.or(root.deprecated);
        file.unstable = file.unstable.or(root.unstable);
        file.since = file.since.or(root.since);
        let main = main_form(sidecar);
        let defs = main.get("$defs");
        collect(&main, "", defs, &mut out);
    }
    out.insert(String::new(), file);
    out
}

fn collect(schema: &Value, path: &str, defs: Option<&Value>, out: &mut BTreeMap<String, Declared>) {
    let declared = |value: &Value| {
        Declared::read(value.as_object().expect("a schema position is an object"))
            .expect("the projection validated every stability annotation")
    };
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            let key = format!("{path}/properties/{name}");
            out.insert(key.clone(), declared(property));
            collect(property, &key, defs, out);
        }
    }
    if let Some(items) = schema.get("items") {
        collect(items, &format!("{path}/items"), defs, out);
    }
    if let Some((rest, is_union)) = branches(schema) {
        if is_union {
            for branch in rest {
                if let Some(id) = member_id(branch, defs) {
                    out.insert(format!("{path}/$union/{id}"), declared(branch));
                }
            }
        } else {
            collect(rest[0], path, defs, out);
        }
    }
    // Only the root carries definitions, and a definition is an item of its own.
    if path.is_empty()
        && let Some(definitions) = schema.get("$defs").and_then(Value::as_object)
    {
        for (name, definition) in definitions {
            let key = format!("/$defs/{name}");
            out.insert(key.clone(), declared(definition));
            collect(definition, &key, defs, out);
        }
    }
}

/// The path in the Lexicon file of the item at sidecar path `item`, which is where a difference
/// the Lexicon classifier reports at or under it sits.
pub fn lexicon_path(item: &str) -> String {
    if item.is_empty() {
        String::new()
    } else if let Some(definition) = item.strip_prefix("/$defs/") {
        format!("/defs/{definition}")
    } else {
        format!("/defs/main{item}")
    }
}

/// How the index and the review lines spell an item's stability.
pub fn describe(declared: &Declared) -> String {
    match (declared.since, &declared.deprecated) {
        (_, Some((since, _))) => format!("deprecated since {since}"),
        (Some(since), None) => format!("since {since}"),
        (None, None) => "unstable".to_owned(),
    }
}
