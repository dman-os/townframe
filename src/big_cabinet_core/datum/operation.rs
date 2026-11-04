//! Operation Lexicons: `query`, `procedure` and `subscription` mains, the three XRPC kinds taken
//! from AT Protocol unchanged (ADR 023 §3).
//!
//! The datum compiler models record and object Lexicons and refuses an operation main, so the
//! operation contracts a nodule carries are admitted here: the frame of the main definition
//! (which keys each kind has, the `params` vocabulary, body encodings, the message schema, the
//! error list) and the schema positions it reaches, checked with the same grammar the datum
//! compiler compiles against. A `ref` is not resolved here; it is reported back so the nodule can
//! resolve the ones under its own authority and leave the foreign ones to the bundle that binds
//! providers.

use super::grammar::GrammarError;
use super::grammar::check_position;
use super::grammar::invalid;
use super::grammar::parse_ref;
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OperationKind {
    Query,
    Procedure,
    Subscription,
}

impl OperationKind {
    pub fn parse(kind: &str) -> Option<Self> {
        match kind {
            "query" => Some(Self::Query),
            "procedure" => Some(Self::Procedure),
            "subscription" => Some(Self::Subscription),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Procedure => "procedure",
            Self::Subscription => "subscription",
        }
    }
}

/// A `ref` an operation's schemas make: where in the file, and the text as written (`nsid`,
/// `nsid#def` or the file-local `#def`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRef<'a> {
    pub path: String,
    pub target: &'a str,
}

type Result<T> = std::result::Result<T, GrammarError>;

/// Admits the definitions of one operation Lexicon file whose `main` is `kind`. Every other
/// definition in the file is a schema position the main may refer to, so it is checked with the
/// same vocabulary; a second operation or a record in the same file is refused, as in AT Protocol.
pub fn validate<'a>(
    kind: OperationKind,
    defs: &'a Map<String, Value>,
    path: &str,
) -> Result<Vec<SchemaRef<'a>>> {
    let mut refs = Vec::new();
    for (name, definition) in defs {
        let path = format!("{path}.{name}");
        if name == "main" {
            main(kind, definition, &path, &mut refs)?;
            continue;
        }
        let object = definition
            .as_object()
            .ok_or_else(|| invalid(&path, "a definition is an object"))?;
        match object.get("type").and_then(Value::as_str) {
            Some("token") => {
                allowed(object, &["type", "description"], &path)?;
                description(object, &path)?;
            }
            Some("record" | "query" | "procedure" | "subscription") => {
                return Err(invalid(
                    &path,
                    "an operation file has one main; a record or another operation is its own file",
                ));
            }
            _ => schema(definition, &path, &mut refs)?,
        }
    }
    Ok(refs)
}

fn main<'a>(
    kind: OperationKind,
    definition: &'a Value,
    path: &str,
    refs: &mut Vec<SchemaRef<'a>>,
) -> Result<()> {
    let object = definition
        .as_object()
        .ok_or_else(|| invalid(path, "a definition is an object"))?;
    let keys: &[&str] = match kind {
        OperationKind::Query => &["type", "description", "parameters", "output", "errors"],
        OperationKind::Procedure => &[
            "type",
            "description",
            "parameters",
            "input",
            "output",
            "errors",
        ],
        OperationKind::Subscription => &["type", "description", "parameters", "message", "errors"],
    };
    allowed(object, keys, path)?;
    description(object, path)?;
    if let Some(parameters) = object.get("parameters") {
        params(parameters, &format!("{path}.parameters"))?;
    }
    for key in ["input", "output"] {
        if let Some(value) = object.get(key) {
            body(value, &format!("{path}.{key}"), refs)?;
        }
    }
    if let Some(message) = object.get("message") {
        let path = format!("{path}.message");
        let object = message
            .as_object()
            .ok_or_else(|| invalid(&path, "a message is an object"))?;
        allowed(object, &["description", "schema"], &path)?;
        description(object, &path)?;
        let schema_value = object
            .get("schema")
            .ok_or_else(|| invalid(&path, "a message states its schema"))?;
        top_level(schema_value, &format!("{path}.schema"), refs)?;
    } else if kind == OperationKind::Subscription {
        return Err(invalid(path, "a subscription states the message it emits"));
    }
    if let Some(errors) = object.get("errors") {
        let path = format!("{path}.errors");
        let errors = errors
            .as_array()
            .ok_or_else(|| invalid(&path, "errors are a list"))?;
        let mut names = BTreeSet::new();
        for (index, error) in errors.iter().enumerate() {
            let path = format!("{path}[{index}]");
            let object = error
                .as_object()
                .ok_or_else(|| invalid(&path, "an error is an object"))?;
            allowed(object, &["name", "description"], &path)?;
            description(object, &path)?;
            let name = string(object, "name", &path)?;
            if name.is_empty() {
                return Err(invalid(&path, "an error name is not empty"));
            }
            if !names.insert(name) {
                return Err(invalid(&path, format!("error `{name}` is declared twice")));
            }
        }
    }
    Ok(())
}

/// `params`: the query-string vocabulary, primitives and arrays of primitives only, each position
/// grammar-checked so a malformed constraint is refused here and not at call time.
fn params(value: &Value, path: &str) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(path, "parameters are an object"))?;
    allowed(
        object,
        &["type", "description", "required", "properties"],
        path,
    )?;
    description(object, path)?;
    if string(object, "type", path)? != "params" {
        return Err(invalid(
            format!("{path}.type"),
            "parameters have type `params`",
        ));
    }
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(path, "parameters state their properties"))?;
    for (name, property) in properties {
        let path = format!("{path}.properties.{name}");
        let kind = check_position(property, &path)?;
        let item = if kind == "array" {
            check_position(&property["items"], &format!("{path}.items"))?
        } else {
            kind
        };
        if !matches!(item, "boolean" | "integer" | "string" | "unknown") {
            return Err(invalid(
                &path,
                format!(
                    "a parameter is a boolean, integer, string, unknown or an array of one; `{item}` is not"
                ),
            ));
        }
    }
    required(object, properties, path)
}

/// `input` / `output`: an encoding, and a schema when the encoding is JSON.
fn body<'a>(value: &'a Value, path: &str, refs: &mut Vec<SchemaRef<'a>>) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(path, "a body is an object"))?;
    allowed(object, &["description", "encoding", "schema"], path)?;
    description(object, path)?;
    let encoding = string(object, "encoding", path)?;
    if encoding.is_empty() {
        return Err(invalid(
            format!("{path}.encoding"),
            "a body states its MIME encoding",
        ));
    }
    if let Some(schema_value) = object.get("schema") {
        if encoding != "application/json" {
            return Err(invalid(
                format!("{path}.schema"),
                format!("a schema describes a JSON body, not `{encoding}`"),
            ));
        }
        top_level(schema_value, &format!("{path}.schema"), refs)?;
    }
    Ok(())
}

/// A body or message schema is an object, a ref or a union; a bare primitive is not a body.
fn top_level<'a>(value: &'a Value, path: &str, refs: &mut Vec<SchemaRef<'a>>) -> Result<()> {
    let kind = value
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str);
    if !matches!(kind, Some("object" | "ref" | "union")) {
        return Err(invalid(
            path,
            "a body schema is an object, a ref or a union",
        ));
    }
    schema(value, path, refs)
}

/// One schema position, grammar-checked, then descended into; every `ref` it spells is collected.
/// A record is refused here: an operation file has no named main record, and a nested record is
/// another file.
fn schema<'a>(value: &'a Value, path: &str, refs: &mut Vec<SchemaRef<'a>>) -> Result<()> {
    match check_position(value, path)? {
        "object" => {
            let properties = value["properties"]
                .as_object()
                .expect("checked object properties");
            for (name, property) in properties {
                schema(property, &format!("{path}.properties.{name}"), refs)?;
            }
        }
        "array" => schema(&value["items"], &format!("{path}.items"), refs)?,
        "ref" => push_ref(
            value["ref"].as_str().expect("checked ref"),
            format!("{path}.ref"),
            refs,
        )?,
        "union" => {
            let targets = value["refs"].as_array().expect("checked union refs");
            for (index, target) in targets.iter().enumerate() {
                push_ref(
                    target.as_str().expect("checked union ref"),
                    format!("{path}.refs[{index}]"),
                    refs,
                )?;
            }
        }
        "record" => return Err(invalid(path, "a record must be a named main definition")),
        _ => {}
    }
    Ok(())
}

fn push_ref<'a>(target: &'a str, path: String, refs: &mut Vec<SchemaRef<'a>>) -> Result<()> {
    if parse_ref(target).is_none() {
        return Err(invalid(
            &path,
            format!("`{target}` is not a Lexicon reference: `nsid`, `nsid#def` or `#def`"),
        ));
    }
    refs.push(SchemaRef { path, target });
    Ok(())
}

fn required(
    object: &Map<String, Value>,
    properties: &Map<String, Value>,
    path: &str,
) -> Result<()> {
    match object.get("required") {
        Some(required) => names_in(required, properties, &format!("{path}.required")),
        None => Ok(()),
    }
}

fn names_in(names: &Value, properties: &Map<String, Value>, path: &str) -> Result<()> {
    let names = names
        .as_array()
        .ok_or_else(|| invalid(path, "a list of property names"))?;
    for name in names {
        let name = name
            .as_str()
            .ok_or_else(|| invalid(path, "a property name is a string"))?;
        if !properties.contains_key(name) {
            return Err(invalid(
                path,
                format!("`{name}` is not a declared property"),
            ));
        }
    }
    Ok(())
}

fn allowed(object: &Map<String, Value>, keys: &[&str], path: &str) -> Result<()> {
    for key in object.keys() {
        if !keys.contains(&key.as_str()) {
            return Err(invalid(path, format!("`{key}` is not a field here")));
        }
    }
    Ok(())
}

fn description(object: &Map<String, Value>, path: &str) -> Result<()> {
    if object
        .get("description")
        .is_some_and(|value| !value.is_string())
    {
        return Err(invalid(
            format!("{path}.description"),
            "description must be a string",
        ));
    }
    Ok(())
}

fn string<'a>(object: &'a Map<String, Value>, key: &str, path: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{path}.{key}"), format!("`{key}` is a string")))
}

#[cfg(test)]
mod tests;
