use super::Bundle;
use super::CodecError;
use super::CompiledSchema;
use super::is_nsid;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use unicode_segmentation::UnicodeSegmentation;

pub(super) const KEYWORD: &str = "x-cabinet-lexicon";
pub(super) const DATA: &str = "x-cabinet-lexicon-data";

#[derive(Debug, thiserror::Error)]
pub enum LexiconAdmissionError {
    #[error("Lexicon admission failed at `{path}`: {reason}")]
    Invalid { path: String, reason: String },
    #[error("invalid supplement bundle: {0}")]
    Bundle(#[from] super::UniverseError),
    #[error("compiled Lexicon is not operable: {0}")]
    Compilation(#[source] Box<CodecError>),
}

type Result<T> = std::result::Result<T, LexiconAdmissionError>;

fn invalid(path: &str, reason: impl Into<String>) -> LexiconAdmissionError {
    LexiconAdmissionError::Invalid {
        path: path.to_owned(),
        reason: reason.into(),
    }
}

impl Bundle<'_> {
    pub fn validate_lexicon(
        &self,
        tag: &str,
        primary: &Value,
        carried: &[&Value],
    ) -> Result<CompiledSchema> {
        self.validate()?;
        if !self.definitions().is_empty() && !self.plug.datums.iter().any(|datum| datum.id == tag) {
            return Err(invalid(
                "supplement",
                format!("bundle does not declare datum `{tag}`"),
            ));
        }
        let mut compiler = Compiler {
            files: BTreeMap::new(),
            names: BTreeMap::new(),
            definitions: Map::new(),
        };
        for file in std::iter::once(primary).chain(carried.iter().copied()) {
            bound(file)?;
            let object = file
                .as_object()
                .ok_or_else(|| invalid("$", "a Lexicon file is an object"))?;
            check_keys(
                object,
                &["lexicon", "id", "description", "defs", "$type"],
                "$",
            )?;
            if object.get("lexicon") != Some(&json!(1)) {
                return Err(invalid("lexicon", "the language version must be 1"));
            }
            if object
                .get("description")
                .is_some_and(|value| !value.is_string())
            {
                return Err(invalid("description", "description must be a string"));
            }
            if object
                .get("$type")
                .is_some_and(|value| value != "com.atproto.lexicon.schema")
            {
                return Err(invalid(
                    "$type",
                    "a published Lexicon has type `com.atproto.lexicon.schema`",
                ));
            }
            let id = string(object, "id", "$")?;
            if !is_nsid(id) {
                return Err(invalid("id", "invalid NSID"));
            }
            if compiler.files.insert(id.to_owned(), file).is_some() {
                return Err(invalid("id", format!("duplicate carried Lexicon `{id}`")));
            }
            let defs = object
                .get("defs")
                .and_then(Value::as_object)
                .filter(|defs| !defs.is_empty())
                .ok_or_else(|| invalid("defs", "non-empty definitions are required"))?;
            for (name, definition) in defs {
                if name.is_empty() || name.contains('#') {
                    return Err(invalid("defs", "unsupported definition name"));
                }
                let kind = definition
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid(name, "a definition states its type"))?;
                if matches!(kind, "unknown" | "ref" | "union" | "params" | "permission") {
                    return Err(invalid(
                        name,
                        format!("`{kind}` cannot be a named definition"),
                    ));
                }
                if kind == "record" && name != "main" {
                    return Err(invalid(name, "a record must be the main definition"));
                }
                let index = compiler.names.len();
                compiler
                    .names
                    .insert((id.to_owned(), name.clone()), format!("L{index}"));
            }
        }
        if primary.get("id").and_then(Value::as_str) != Some(tag) {
            return Err(invalid(
                "id",
                format!("primary Lexicon must identify `{tag}`"),
            ));
        }
        let names = compiler.names.clone();
        for ((id, name), key) in names {
            let definition = &compiler.files[&id]["defs"][&name];
            if definition["type"] == "token" {
                let object = definition
                    .as_object()
                    .ok_or_else(|| invalid(&name, "a definition is an object"))?;
                check_keys(object, &["type", "description"], &name)?;
                if object
                    .get("description")
                    .is_some_and(|value| !value.is_string())
                {
                    return Err(invalid(&name, "description must be a string"));
                }
                continue;
            }
            let compiled = compiler.position(&id, definition, &format!("{id}#{name}"), 0)?;
            compiler.definitions.insert(key.clone(), compiled.clone());
            if definition["type"] == "object" {
                let mut tagged = compiled;
                add_tag(&mut tagged, &identity(&id, &name));
                compiler.definitions.insert(format!("T{key}"), tagged);
            }
        }
        let root = compiler
            .names
            .get(&(tag.to_owned(), "main".to_owned()))
            .ok_or_else(|| invalid("defs.main", "the datum requires a main definition"))?
            .clone();
        let main = &primary["defs"]["main"];
        if !matches!(main["type"].as_str(), Some("record" | "object")) {
            return Err(invalid(
                "defs.main.type",
                "a datum must have record or object main",
            ));
        }
        let mut document = json!({"$ref": format!("#/$defs/{root}"), DATA: true});
        if let Some(datum) = self.plug.datums.iter().find(|datum| datum.id == tag) {
            let supplement = serde_json::to_value(
                self.document(&datum.name)
                    .expect("validated datum definition"),
            )
            .expect("schema serializes");
            bound(&supplement)?;
            let source = supplement["$defs"].as_object().expect("bundle definitions");
            check_supplement(&supplement, source, "supplement")?;
            CompiledSchema::check_storage_declarations(&supplement)
                .map_err(|error| LexiconAdmissionError::Compilation(Box::new(error)))?;
            let mut overlay = Overlay {
                source,
                base: compiler.definitions.clone(),
                definitions: &mut compiler.definitions,
                paired: HashMap::new(),
            };
            overlay.apply(&mut document, &supplement, "supplement", 0)?;
        }
        document
            .as_object_mut()
            .expect("compiled root")
            .insert("$defs".to_owned(), Value::Object(compiler.definitions));
        CompiledSchema::compile(document)
            .map_err(|error| LexiconAdmissionError::Compilation(Box::new(error)))
    }
}

fn bound(value: &Value) -> Result<()> {
    let mut pending = vec![(value, 0)];
    let mut visits = 0;
    while let Some((value, depth)) = pending.pop() {
        visits += 1;
        if depth > 128 || visits > 100_000 {
            return Err(invalid("$", "schema resource limit exceeded"));
        }
        match value {
            Value::Object(object) => {
                pending.extend(object.values().map(|value| (value, depth + 1)))
            }
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

struct Compiler<'a> {
    files: BTreeMap<String, &'a Value>,
    names: BTreeMap<(String, String), String>,
    definitions: Map<String, Value>,
}

impl Compiler<'_> {
    fn target(&self, id: &str, reference: &str, path: &str) -> Result<(String, String, String)> {
        let (file, name) = if let Some(name) = reference.strip_prefix('#') {
            (id, name)
        } else {
            reference.split_once('#').unwrap_or((reference, "main"))
        };
        let key = self
            .names
            .get(&(file.to_owned(), name.to_owned()))
            .ok_or_else(|| {
                invalid(
                    path,
                    format!("reference `{reference}` has no carried definition"),
                )
            })?;
        let kind = self.files[file]["defs"][name]["type"]
            .as_str()
            .expect("indexed type");
        if kind == "token" {
            return Err(invalid(path, "a token has no data representation"));
        }
        Ok((key.clone(), identity(file, name), kind.to_owned()))
    }

    fn position(&self, id: &str, value: &Value, path: &str, depth: usize) -> Result<Value> {
        if depth > 128 {
            return Err(invalid(path, "schema depth exceeded"));
        }
        let object = value
            .as_object()
            .ok_or_else(|| invalid(path, "a schema position is an object"))?;
        let kind = string(object, "type", path)?;
        let fields: &[&str] = match kind {
            "record" => &["key", "record"],
            "object" => &["properties", "required", "nullable"],
            "array" => &["items", "minLength", "maxLength"],
            "string" => &[
                "format",
                "minLength",
                "maxLength",
                "minGraphemes",
                "maxGraphemes",
                "knownValues",
                "enum",
                "const",
                "default",
            ],
            "integer" => &["minimum", "maximum", "enum", "const", "default"],
            "boolean" => &["const", "default"],
            "bytes" => &["minLength", "maxLength"],
            "cid-link" | "unknown" => &[],
            "ref" => &["ref"],
            "union" => &["refs", "closed"],
            "blob" => {
                return Err(invalid(
                    path,
                    "blob schemas are deliberately unsupported in this compiler",
                ));
            }
            _ => return Err(invalid(path, format!("unsupported Lexicon type `{kind}`"))),
        };
        for key in object.keys() {
            if !matches!(key.as_str(), "type" | "description") && !fields.contains(&key.as_str()) {
                return Err(invalid(path, format!("`{key}` is not a field of `{kind}`")));
            }
        }
        if object
            .get("description")
            .is_some_and(|value| !value.is_string())
        {
            return Err(invalid(path, "description must be a string"));
        }
        for (min, max) in [("minLength", "maxLength"), ("minGraphemes", "maxGraphemes")] {
            for key in [min, max] {
                if object
                    .get(key)
                    .is_some_and(|value| value.as_u64().is_none())
                {
                    return Err(invalid(
                        path,
                        format!("`{key}` must be a non-negative integer"),
                    ));
                }
            }
            if let (Some(minimum), Some(maximum)) = (
                object.get(min).and_then(Value::as_u64),
                object.get(max).and_then(Value::as_u64),
            ) && minimum > maximum
            {
                return Err(invalid(path, "inverted length bounds"));
            }
        }
        match kind {
            "record" => {
                if depth != 0 {
                    return Err(invalid(path, "a record must be a named main definition"));
                }
                let key = string(object, "key", path)?;
                if !matches!(key, "tid" | "nsid" | "any")
                    && !key.strip_prefix("literal:").is_some_and(record_key)
                {
                    return Err(invalid(path, "invalid record key declaration"));
                }
                let record = object
                    .get("record")
                    .filter(|record| record["type"] == "object")
                    .ok_or_else(|| invalid(path, "record requires an object schema"))?;
                let mut compiled =
                    self.position(id, record, &format!("{path}.record"), depth + 1)?;
                add_tag(&mut compiled, id);
                Ok(compiled)
            }
            "object" => self.object_position(id, object, path, depth),
            "array" => {
                let items = object
                    .get("items")
                    .ok_or_else(|| invalid(path, "array items are required"))?;
                let mut out = json!({"type": "array", "items": self.position(id, items, &format!("{path}.items"), depth + 1)?});
                for (from, to) in [("minLength", "minItems"), ("maxLength", "maxItems")] {
                    if let Some(value) = object.get(from) {
                        out[to] = value.clone();
                    }
                }
                Ok(out)
            }
            "ref" => {
                let (key, _, _) = self.target(id, string(object, "ref", path)?, path)?;
                Ok(json!({"$ref": format!("#/$defs/{key}")}))
            }
            "union" => self.union_position(id, object, path),
            "bytes" => Ok(
                json!({"type": "object", "properties": {"$bytes": {"type": "string", KEYWORD: {"type": "base64"}}}, "required": ["$bytes"], "additionalProperties": false, "x-automerge": "bytes", KEYWORD: value}),
            ),
            "cid-link" => Ok(
                json!({"type": "object", "properties": {"$link": {"type": "string", KEYWORD: {"type": "string", "format": "cid"}}}, "required": ["$link"], "additionalProperties": false}),
            ),
            "unknown" => Ok(json!({"type": "object", KEYWORD: {"type": "unknown"}})),
            "string" | "integer" | "boolean" => Self::primitive_position(kind, object, value, path),
            _ => unreachable!(),
        }
    }

    /// An object position: its properties compiled in order, a nullable one as an `anyOf` with
    /// its null, its `required` names restated.
    fn object_position(
        &self,
        id: &str,
        object: &Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<Value> {
        let properties = object
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid(path, "object properties are required"))?;
        let required = names(object, "required", path)?;
        let nullable = names(object, "nullable", path)?;
        if required
            .iter()
            .chain(&nullable)
            .any(|name| !properties.contains_key(*name))
        {
            return Err(invalid(
                path,
                "required/nullable names must identify properties",
            ));
        }
        let mut compiled = Map::new();
        for (name, property) in properties {
            if name.starts_with('$') {
                return Err(invalid(
                    path,
                    "application properties cannot use reserved dollar keys",
                ));
            }
            let mut property = self.position(
                id,
                property,
                &format!("{path}.properties.{name}"),
                depth + 1,
            )?;
            if nullable.contains(name.as_str()) {
                property = json!({"anyOf": [property, {"type": "null"}]});
            }
            compiled.insert(name.clone(), property);
        }
        Ok(json!({"type": "object", "properties": compiled, "required": required}))
    }

    /// A union position: one `$ref` branch per member, each pinned by its tagged definition, and
    /// — unless the union is closed — the fallback branch any other discriminant is carried by.
    fn union_position(&self, id: &str, object: &Map<String, Value>, path: &str) -> Result<Value> {
        let refs = object
            .get("refs")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid(path, "union refs must be an array"))?;
        let closed = match object.get("closed") {
            None => false,
            Some(Value::Bool(closed)) => *closed,
            _ => return Err(invalid(path, "closed must be boolean")),
        };
        if closed && refs.is_empty() {
            return Err(invalid(path, "a closed union cannot be empty"));
        }
        let mut branches = Vec::new();
        let mut tags = BTreeSet::new();
        for reference in refs {
            let reference = reference
                .as_str()
                .ok_or_else(|| invalid(path, "union references must be strings"))?;
            let (key, tag, kind) = self.target(id, reference, path)?;
            if !matches!(kind.as_str(), "object" | "record") {
                return Err(invalid(path, "union variants must be objects or records"));
            }
            if !tags.insert(tag) {
                return Err(invalid(path, "duplicate union variant"));
            }
            let key = if kind == "object" {
                format!("T{key}")
            } else {
                key
            };
            branches.push(json!({"$ref": format!("#/$defs/{key}")}));
        }
        if !closed {
            branches.push(json!({"type": "object", "properties": {"$type": {"type": "string", "not": {"enum": tags}, KEYWORD: {"type": "tag"}}}, "required": ["$type"]}));
        }
        Ok(json!({"anyOf": branches}))
    }

    /// A string, integer or boolean position: its constraint values checked against the kind,
    /// its format checked against the supported vocabulary, and the Lexicon itself carried under
    /// [`KEYWORD`] for the constraint keyword to read.
    fn primitive_position(
        kind: &str,
        object: &Map<String, Value>,
        value: &Value,
        path: &str,
    ) -> Result<Value> {
        if object.contains_key("const") && object.contains_key("default") {
            return Err(invalid(path, "const and default are mutually exclusive"));
        }
        let fits = |value: &Value| match kind {
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some(),
            "boolean" => value.is_boolean(),
            _ => unreachable!(),
        };
        for key in ["const", "default", "minimum", "maximum"] {
            if object.get(key).is_some_and(|value| !fits(value)) {
                return Err(invalid(path, format!("invalid `{key}` value")));
            }
        }
        if let (Some(min), Some(max)) = (
            object.get("minimum").and_then(Value::as_i64),
            object.get("maximum").and_then(Value::as_i64),
        ) && min > max
        {
            return Err(invalid(path, "inverted integer bounds"));
        }
        for key in ["enum", "knownValues"] {
            if let Some(values) = object.get(key)
                && !values
                    .as_array()
                    .is_some_and(|values| values.iter().all(fits))
            {
                return Err(invalid(path, format!("invalid `{key}` list")));
            }
        }
        if let Some(format) = object.get("format") {
            let format = format
                .as_str()
                .ok_or_else(|| invalid(path, "format must be a string"))?;
            if !matches!(
                format,
                "at-identifier"
                    | "at-uri"
                    | "cid"
                    | "datetime"
                    | "did"
                    | "handle"
                    | "nsid"
                    | "tid"
                    | "record-key"
                    | "uri"
            ) {
                return Err(invalid(
                    path,
                    format!("unsupported Lexicon string format `{format}`"),
                ));
            }
        }
        let mut out = json!({"type": kind, KEYWORD: value});
        for key in ["enum", "const", "minimum", "maximum"] {
            if let Some(value) = object.get(key) {
                out[key] = value.clone();
            }
        }
        if kind == "integer" {
            out["minimum"] = json!(
                object
                    .get("minimum")
                    .and_then(Value::as_i64)
                    .unwrap_or(i64::MIN)
            );
            out["maximum"] = json!(
                object
                    .get("maximum")
                    .and_then(Value::as_i64)
                    .unwrap_or(i64::MAX)
            );
        }
        if kind == "string" {
            out[KEYWORD] = value.clone();
            if object.get("format").and_then(Value::as_str) == Some("datetime") {
                out["x-automerge"] = json!("datetime");
            }
        }
        Ok(out)
    }
}

fn identity(id: &str, name: &str) -> String {
    if name == "main" {
        id.to_owned()
    } else {
        format!("{id}#{name}")
    }
}

fn add_tag(schema: &mut Value, tag: &str) {
    schema["properties"]["$type"] = json!({"type": "string", "const": tag});
    schema["required"]
        .as_array_mut()
        .expect("compiled object required")
        .push(json!("$type"));
}

fn check_keys(object: &Map<String, Value>, allowed: &[&str], path: &str) -> Result<()> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid(path, format!("unsupported keyword `{key}`")));
    }
    Ok(())
}

fn string<'a>(object: &'a Map<String, Value>, key: &str, path: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(path, format!("`{key}` must be a string")))
}

fn names<'a>(object: &'a Map<String, Value>, key: &str, path: &str) -> Result<BTreeSet<&'a str>> {
    let Some(values) = object.get(key) else {
        return Ok(BTreeSet::new());
    };
    let values = values
        .as_array()
        .ok_or_else(|| invalid(path, format!("`{key}` must be a string array")))?;
    let mut out = BTreeSet::new();
    for name in values {
        let name = name
            .as_str()
            .ok_or_else(|| invalid(path, format!("`{key}` names must be strings")))?;
        if !out.insert(name) {
            return Err(invalid(path, format!("duplicate `{key}` name")));
        }
    }
    Ok(out)
}

/// The keywords a sidecar position may state. Beyond the JSON Schema structure the overlay reads,
/// this is exactly what `schemars` emits for the datum macros' declarations: `default` is the
/// `#[serde(default)]` spelling (an annotation, never validation), `x-lexicon-id` is the datum
/// macro's record of the published id on its own definition, and `pattern` is the `Uuid7` key
/// spelling — admitted only on the `format: "uuid"` string that carries it, never as a general
/// constraint keyword.
const SUPPLEMENT_KEYS: &[&str] = &[
    "$schema",
    "$defs",
    "$ref",
    "title",
    "description",
    "type",
    "properties",
    "required",
    "items",
    "anyOf",
    "oneOf",
    "additionalProperties",
    "patternProperties",
    "x-automerge",
    "x-lexicon-id",
    "format",
    "contentEncoding",
    "default",
    "pattern",
    "enum",
    "knownValues",
    "const",
    "not",
];

fn check_supplement(schema: &Value, definitions: &Map<String, Value>, path: &str) -> Result<()> {
    let object = schema
        .as_object()
        .ok_or_else(|| invalid(path, "supplement positions must be schema objects"))?;
    check_keys(object, SUPPLEMENT_KEYS, path)?;
    for key in ["$schema", "title", "description", "x-lexicon-id"] {
        if object.get(key).is_some_and(|value| !value.is_string()) {
            return Err(invalid(path, format!("`{key}` must be a string")));
        }
    }
    if let Some(reference) = object.get("$ref") {
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "$ref" | "$defs" | "$schema" | "title" | "description"
            )
        }) {
            return Err(invalid(path, "supplement reference has semantic siblings"));
        }
        if reference
            .as_str()
            .and_then(|reference| reference.strip_prefix("#/$defs/"))
            .is_none_or(|name| !definitions.contains_key(name))
        {
            return Err(invalid(
                path,
                "supplement reference has no carried local definition",
            ));
        }
    }
    check_supplement_keywords(object, path)?;
    super::codec::keyed_map(object).map_err(|reason| invalid(path, reason))?;
    for key in ["$defs", "properties", "patternProperties"] {
        if let Some(values) = object.get(key) {
            let values = values
                .as_object()
                .ok_or_else(|| invalid(path, format!("`{key}` must be an object")))?;
            for (name, value) in values {
                check_supplement(value, definitions, &format!("{path}.{key}.{name}"))?;
            }
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(value) = object.get(key)
            && !value.is_boolean()
        {
            check_supplement(value, definitions, &format!("{path}.{key}"))?;
        }
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(values) = object.get(key) {
            let values = values
                .as_array()
                .ok_or_else(|| invalid(path, "branches must be arrays"))?;
            for value in values {
                check_supplement(value, definitions, path)?;
            }
        }
    }
    Ok(())
}

/// The values a supplement position's own keywords may take: the storage markers, formats and
/// encodings a native spelling uses, the `not`/`const`/vocabulary spellings a union or string
/// vocabulary states, and the JSON types a position may declare. Its subschemas are the walk's
/// concern in [`check_supplement`].
fn check_supplement_keywords(object: &Map<String, Value>, path: &str) -> Result<()> {
    if object.get("x-automerge").is_some_and(|marker| {
        !matches!(
            marker.as_str(),
            Some(
                "bytes"
                    | "float"
                    | "uint"
                    | "text"
                    | "timestamp"
                    | "datetime"
                    | "counter"
                    | "list"
                    | "reference"
            )
        )
    }) {
        return Err(invalid(path, "unsupported storage marker"));
    }
    if object.get("format").is_some_and(|format| {
        !matches!(
            format.as_str(),
            Some("float" | "double" | "uint64" | "date-time" | "datetime" | "uuid")
        )
    }) {
        return Err(invalid(path, "unsupported supplement format"));
    }
    // `pattern` is not a generally admissible constraint: the one spelling the declarations carry
    // is the `Uuid7` definition's, a `uuid`-formatted string, and anything else is refused.
    if let Some(pattern) = object.get("pattern")
        && (!pattern.is_string()
            || object.get("type").and_then(Value::as_str) != Some("string")
            || object.get("format").and_then(Value::as_str) != Some("uuid"))
    {
        return Err(invalid(
            path,
            "`pattern` is admitted only on the macro-emitted `uuid` string spelling",
        ));
    }
    if object
        .get("contentEncoding")
        .is_some_and(|encoding| encoding != "base64")
    {
        return Err(invalid(path, "unsupported supplement encoding"));
    }
    if object.contains_key("required") {
        names(object, "required", path)?;
    }
    // A union's fallback branch excludes the known discriminants with `not`: an enum of tags, or
    // a member-branch anyOf. Anything else under `not` is not a discriminant spelling.
    if let Some(not) = object.get("not") {
        let enum_tags = not
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|tags| tags.iter().all(Value::is_string));
        let member_branches = not
            .get("anyOf")
            .and_then(Value::as_array)
            .is_some_and(|branches| {
                branches.iter().all(|branch| {
                    branch
                        .get("properties")
                        .and_then(|properties| properties.get("$type"))
                        .and_then(|tagged| tagged.get("const"))
                        .is_some_and(Value::is_string)
                })
            });
        if !enum_tags && !member_branches {
            return Err(invalid(
                path,
                "`not` is admitted only as a union fallback's discriminant exclusion",
            ));
        }
    }
    // A union member branch pins its discriminant with `const`, the same value the compiled
    // branch's `$type` was given; the overlay holds the two to agreement below.
    if let Some(constant) = object.get("const") {
        let typed_string = object.get("type").and_then(Value::as_str) == Some("string");
        let untyped_string = !object.contains_key("type") && constant.is_string();
        if !typed_string && !untyped_string {
            return Err(invalid(
                path,
                "`const` is admitted only as a string discriminant",
            ));
        }
    }
    // A string vocabulary spells its values as `enum` (closed) or `knownValues` (open), the
    // same list its Lexicon projection carries; the overlay holds the two to agreement.
    for key in ["enum", "knownValues"] {
        if let Some(values) = object.get(key)
            && (object.get("type").and_then(Value::as_str) != Some("string")
                || !values.as_array().is_some_and(|values| {
                    !values.is_empty() && values.iter().all(Value::is_string)
                }))
        {
            return Err(invalid(
                path,
                format!("`{key}` is admitted only as a string vocabulary on a string position"),
            ));
        }
    }
    if let Some(kind) = object.get("type") {
        let supported = |kind: &Value| {
            matches!(
                kind.as_str(),
                Some("object" | "array" | "string" | "integer" | "boolean" | "null")
            )
        };
        if !supported(kind)
            && !kind
                .as_array()
                .is_some_and(|kinds| !kinds.is_empty() && kinds.iter().all(supported))
        {
            return Err(invalid(path, "unsupported supplement type"));
        }
        if let Some(kinds) = kind.as_array()
            && kinds
                .iter()
                .filter_map(Value::as_str)
                .collect::<BTreeSet<_>>()
                .len()
                != kinds.len()
        {
            return Err(invalid(path, "duplicate supplement type"));
        }
    }
    Ok(())
}

struct Overlay<'a> {
    source: &'a Map<String, Value>,
    base: Map<String, Value>,
    definitions: &'a mut Map<String, Value>,
    paired: HashMap<(String, String), String>,
}

impl Overlay<'_> {
    fn apply(
        &mut self,
        primary: &mut Value,
        supplement: &Value,
        path: &str,
        depth: usize,
    ) -> Result<()> {
        if depth > 128 {
            return Err(invalid(path, "supplement reference/depth limit exceeded"));
        }
        let object = supplement
            .as_object()
            .ok_or_else(|| invalid(path, "supplement positions must be schema objects"))?;
        check_keys(object, SUPPLEMENT_KEYS, path)?;
        if let Some(reference) = object.get("$ref") {
            if object.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "$ref" | "$defs" | "$schema" | "title" | "description"
                )
            }) {
                return Err(invalid(path, "supplement reference has semantic siblings"));
            }
            let name = reference
                .as_str()
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
                .ok_or_else(|| {
                    invalid(
                        path,
                        "supplement references must be carried local definitions",
                    )
                })?;
            let target = self
                .source
                .get(name)
                .ok_or_else(|| invalid(path, format!("missing supplement definition `{name}`")))?
                .clone();
            return self.apply(primary, &target, path, depth + 1);
        }
        if let Some(key) = primary
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|reference| reference.strip_prefix("#/$defs/"))
        {
            let key = key.to_owned();
            return self.apply_reference(primary, key, supplement, path, depth);
        }
        if let Some(branches) = primary.get_mut("anyOf").and_then(Value::as_array_mut) {
            if branches.len() == 2 && branches[1] == json!({"type": "null"}) {
                return self.apply_nullable(&mut branches[0], supplement, object, path, depth);
            }
            return self.apply_union(branches, object, path, depth);
        }
        let kind = primary
            .get("type")
            .and_then(Value::as_str)
            .expect("compiled typed position")
            .to_owned();
        Self::check_vocabulary(primary, object, &kind, path)?;
        Self::apply_storage(primary, object, &kind, path)?;
        self.apply_structure(primary, object, path, depth)
    }

    /// Overlay a supplement onto the compiled definition a primary `$ref` names. The overlaid
    /// copy is a fresh `S…` definition the reference is pointed at, shared by every position
    /// that pairs the same target with the same supplement, so the base definition other
    /// positions reference stays as it was compiled.
    fn apply_reference(
        &mut self,
        primary: &mut Value,
        key: String,
        supplement: &Value,
        path: &str,
        depth: usize,
    ) -> Result<()> {
        let pair = (
            key.clone(),
            serde_json::to_string(supplement).expect("schema serializes"),
        );
        if let Some(paired) = self.paired.get(&pair) {
            primary["$ref"] = json!(format!("#/$defs/{paired}"));
            return Ok(());
        }
        let name = format!("S{}", self.paired.len());
        self.paired.insert(pair, name.clone());
        let mut target = self
            .base
            .get(&key)
            .expect("compiled reference target")
            .clone();
        self.apply(&mut target, supplement, path, depth + 1)?;
        self.definitions.insert(name.clone(), target);
        primary["$ref"] = json!(format!("#/$defs/{name}"));
        Ok(())
    }

    /// Overlay a nullable position's supplement onto the primary's non-null branch: the
    /// supplement spells the null as a `type` alongside the stated one, or as an `anyOf` with a
    /// bare null branch, and either way only the non-null schema is overlaid.
    fn apply_nullable(
        &mut self,
        primary: &mut Value,
        supplement: &Value,
        object: &Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<()> {
        let mut nonnull = supplement.clone();
        if let Some(kinds) = nonnull.get_mut("type").and_then(Value::as_array_mut) {
            kinds.retain(|kind| kind != "null");
            if kinds.len() != 1 {
                return Err(invalid(path, "nullable supplement has contradictory types"));
            }
            nonnull["type"] = kinds[0].clone();
        } else if let Some(alternatives) = nonnull.get("anyOf").and_then(Value::as_array) {
            check_keys(
                object,
                &["anyOf", "$defs", "$schema", "title", "description"],
                path,
            )?;
            let nonnulls: Vec<_> = alternatives
                .iter()
                .filter(|schema| schema.get("type") != Some(&json!("null")))
                .collect();
            if alternatives.len() != 2
                || nonnulls.len() != 1
                || alternatives
                    .iter()
                    .filter(|schema| schema.get("type") == Some(&json!("null")))
                    .any(|schema| {
                        schema
                            .as_object()
                            .expect("checked supplement object")
                            .keys()
                            .any(|key| !matches!(key.as_str(), "type" | "title" | "description"))
                    })
            {
                return Err(invalid(path, "unsupported nullable supplement branches"));
            }
            nonnull = nonnulls[0].clone();
        }
        self.apply(primary, &nonnull, path, depth + 1)
    }

    /// A union's supplement is its member branches: one `oneOf` arm per member, matched to the
    /// primary's branch by its discriminant `const`, with each member overlaid in place so the
    /// compiled union keeps its tag order and openness. This is how a declared union member's
    /// datum-level constraints (its storage markers, its closed object) reach the compiled
    /// document the codec validates against.
    fn apply_union(
        &mut self,
        primary_branches: &mut [Value],
        object: &Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<()> {
        let Some(supplement_branches) = object.get("oneOf").and_then(Value::as_array) else {
            return Err(invalid(
                path,
                "a union's supplement is a `oneOf` of its member branches",
            ));
        };
        // The primary's branches are compiled `$ref`s to the members' tagged definitions; the
        // supplement names a member by its discriminant, so resolve each `$ref` and overlay the
        // member's declared definition (not the tagged copy — two members would pair to the same
        // one), with the discriminant the compiler added re-stated on it.
        for branch in supplement_branches {
            let branch_object = branch
                .as_object()
                .ok_or_else(|| invalid(path, "a union branch is an object"))?;
            let Some(tag) = branch_object
                .get("properties")
                .and_then(|properties| properties.get("$type"))
                .and_then(|tagged| tagged.get("const"))
                .and_then(Value::as_str)
            else {
                // The fallback branch: it states only that the discriminant is none of the known
                // ones. There is no member to overlay.
                continue;
            };
            let base = &self.base;
            let resolve = |branch: &Value| -> Option<String> {
                let tag_of = |schema: &Value| {
                    schema
                        .get("properties")
                        .and_then(|properties| properties.get("$type"))
                        .and_then(|tagged| tagged.get("const"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                };
                if let Some(tag) = tag_of(branch) {
                    return Some(tag);
                }
                let reference = branch.get("$ref")?.as_str()?.strip_prefix("#/$defs/")?;
                base.get(reference).and_then(tag_of)
            };
            let target = primary_branches
                .iter_mut()
                .find(|primary_branch| resolve(primary_branch).as_deref() == Some(tag));
            let Some(target) = target else {
                return Err(invalid(
                    path,
                    format!("union member `{tag}` is absent from the primary union"),
                ));
            };
            let mut member = self.union_member(target, tag, path)?;
            let mut payload = branch.clone();
            if let Some(properties) = payload.get_mut("properties").and_then(Value::as_object_mut) {
                properties.remove("$type");
            }
            if let Some(required) = payload.get_mut("required").and_then(Value::as_array_mut) {
                required.retain(|name| name != "$type");
            }
            self.apply(&mut member, &payload, path, depth + 1)?;
            let paired_name = format!("S{}", self.paired.len());
            self.paired.insert(
                (
                    format!("union-member:{tag}"),
                    serde_json::to_string(&payload).expect("schema serializes"),
                ),
                paired_name.clone(),
            );
            self.definitions.insert(paired_name.clone(), member);
            *target = json!({"$ref": format!("#/$defs/{paired_name}")});
        }
        Ok(())
    }

    /// The definition a union member's supplement is overlaid onto, with its discriminant
    /// restated and required: the compiled copy the primary branch `target` references with the
    /// member's declared properties restated on it, or — when the supplement declares no member
    /// with that discriminant — the compiled definition that pins `tag`.
    fn union_member(&self, target: &Value, tag: &str, path: &str) -> Result<Value> {
        // The member's declared definition lives in the supplement's `$defs` under its type name;
        // its discriminant (`$type` const) names which one `tag` is.
        let member_name = self
            .source
            .iter()
            .find(|(_, definition)| {
                definition
                    .get("properties")
                    .and_then(|properties| properties.get("$type"))
                    .and_then(|tagged| tagged.get("const"))
                    .and_then(Value::as_str)
                    == Some(tag)
            })
            .map(|(name, _)| name.clone());
        let mut member = match member_name {
            Some(name) => {
                // The compiled copy the branch `$ref`s carries the discriminant and the member's
                // primary constraints; the declared definition adds nothing the copy does not
                // state except what the overlay is about to restate.
                let reference = target
                    .get("$ref")
                    .and_then(Value::as_str)
                    .and_then(|reference| reference.strip_prefix("#/$defs/"))
                    .expect("a compiled union branch is a member `$ref`")
                    .to_owned();
                let mut compiled = self
                    .base
                    .get(&reference)
                    .cloned()
                    .unwrap_or_else(|| self.source.get(&name).cloned().expect("declared"));
                let declared = self.source.get(&name).cloned().expect("declared");
                // The compiled `T…` copy pins the tag; restate only what the overlay is for: the
                // member's own properties, from its declaration.
                for (key, property) in declared
                    .get("properties")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flatten()
                {
                    if key != "$type" {
                        compiled["properties"][key] = property.clone();
                    }
                }
                compiled
            }
            None => self
                .base
                .iter()
                .find(|(_, definition)| {
                    definition
                        .get("properties")
                        .and_then(|properties| properties.get("$type"))
                        .and_then(|tagged| tagged.get("const"))
                        .and_then(Value::as_str)
                        == Some(tag)
                })
                .map(|(_, definition)| definition.clone())
                .ok_or_else(|| {
                    invalid(
                        path,
                        format!("union member `{tag}` has no carried declaration"),
                    )
                })?,
        };
        member["properties"]["$type"] = json!({"type": "string", "const": tag});
        let required = member
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if !required.iter().any(|name| name.as_str() == Some("$type")) {
            let mut names = required;
            names.push(json!("$type"));
            member["required"] = Value::Array(names);
        }
        Ok(member)
    }

    /// Hold a typed position's supplement to the vocabulary the primary states — its type, its
    /// `enum`/`knownValues`, its discriminant `const` — and carry the `default` annotation.
    fn check_vocabulary(
        primary: &mut Value,
        object: &Map<String, Value>,
        kind: &str,
        path: &str,
    ) -> Result<()> {
        if let Some(stated) = object.get("type")
            && stated.as_str() != Some(kind)
        {
            return Err(invalid(
                path,
                format!("supplement type {stated} contradicts primary `{kind}`"),
            ));
        }
        if let Some(values) = object.get("enum")
            && primary.get("enum") != Some(values)
        {
            return Err(invalid(
                path,
                "supplement enum contradicts the primary vocabulary",
            ));
        }
        if let Some(values) = object.get("knownValues")
            && primary.get(KEYWORD).and_then(|lex| lex.get("knownValues")) != Some(values)
        {
            return Err(invalid(
                path,
                "supplement knownValues contradicts the primary vocabulary",
            ));
        }
        if let Some(constant) = object.get("const")
            && primary.get("const") != Some(constant)
        {
            return Err(invalid(
                path,
                "supplement const contradicts the primary discriminant",
            ));
        }
        // `default` is the `#[serde(default)]` spelling: an annotation with no validation or
        // storage meaning, carried so the compiled document states what the declaration stated.
        if let Some(default) = object.get("default") {
            primary["default"] = default.clone();
        }
        Ok(())
    }

    /// Carry a typed position's native storage spelling — its `x-automerge` marker, the formats
    /// and `pattern` that spelling admits, its byte encoding — onto the primary, refusing one the
    /// primary's kind cannot be stored as or whose numeric spelling contradicts a primary
    /// constant.
    fn apply_storage(
        primary: &mut Value,
        object: &Map<String, Value>,
        kind: &str,
        path: &str,
    ) -> Result<()> {
        let marker = object
            .get("x-automerge")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| invalid(path, "storage marker must be a string"))
            })
            .transpose()?;
        if let Some(marker) = marker {
            let expected = match marker {
                "bytes" => primary.get("x-automerge").and_then(Value::as_str) == Some("bytes"),
                "float" | "uint" | "text" | "reference" => kind == "string",
                "timestamp" | "datetime" => {
                    kind == "string"
                        && primary
                            .pointer(&format!("/{KEYWORD}/format"))
                            .and_then(Value::as_str)
                            == Some("datetime")
                }
                "counter" => kind == "integer",
                "list" => kind == "array",
                _ => false,
            };
            if !expected {
                return Err(invalid(
                    path,
                    format!(
                        "storage declaration `{marker}` contradicts primary kind or is unsupported"
                    ),
                ));
            }
            primary["x-automerge"] = json!(if marker == "timestamp" {
                "datetime"
            } else {
                marker
            });
            if matches!(marker, "float" | "uint") {
                if primary.pointer(&format!("/{KEYWORD}/format")).is_some() {
                    return Err(invalid(
                        path,
                        "native numeric spelling cannot replace a primary string format",
                    ));
                }
                primary[KEYWORD]["native"] = json!(marker);
            }
        }
        if let Some(format) = object.get("format") {
            let allowed = match marker {
                Some("float") => matches!(format.as_str(), Some("float" | "double")),
                Some("uint") => format.as_str() == Some("uint64"),
                Some("timestamp" | "datetime") | None if primary["x-automerge"] == "datetime" => {
                    matches!(format.as_str(), Some("date-time" | "datetime"))
                }
                None if kind == "string" && format.as_str() == Some("uuid") => true,
                _ => false,
            };
            if !allowed {
                return Err(invalid(path, "format is not an operable native supplement"));
            }
            if marker == Some("float") {
                primary["format"] = format.clone();
                primary[KEYWORD]["width"] = format.clone();
            }
            // `uuid` is not a storage form and not a `Constraint` format; it is the declaration's
            // own annotation on the one string spelling that carries a `pattern`.
            if format.as_str() == Some("uuid") {
                primary["format"] = format.clone();
            }
        }
        if let Some(pattern) = object.get("pattern") {
            let spelling = kind == "string"
                && pattern.is_string()
                && object.get("format").and_then(Value::as_str) == Some("uuid");
            if !spelling {
                return Err(invalid(
                    path,
                    "`pattern` is admitted only on the macro-emitted `uuid` string spelling",
                ));
            }
            primary["pattern"] = pattern.clone();
        }
        if matches!(marker, Some("float" | "uint")) {
            let constraint = Constraint(primary[KEYWORD].clone());
            if primary
                .get("const")
                .into_iter()
                .chain(
                    primary
                        .get("enum")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten(),
                )
                .any(|value| !jsonschema::Keyword::is_valid(&constraint, value))
            {
                return Err(invalid(
                    path,
                    "native spelling contradicts a primary constant or enum",
                ));
            }
        }
        if let Some(encoding) = object.get("contentEncoding")
            && (encoding != "base64" || primary[KEYWORD]["type"] != "base64")
        {
            return Err(invalid(
                path,
                "contentEncoding is only a canonical bytes representation annotation",
            ));
        }
        Ok(())
    }

    /// Overlay a typed position's structure: the typed map an unknown position becomes, then the
    /// properties and items the supplement descends into, holding its `required` set to the
    /// primary's and refusing branches that would be a second validation authority.
    fn apply_structure(
        &mut self,
        primary: &mut Value,
        object: &Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<()> {
        let unknown = primary
            .pointer(&format!("/{KEYWORD}/type"))
            .and_then(Value::as_str)
            == Some("unknown");
        if let Some(additional) = object.get("additionalProperties") {
            if unknown && additional.is_object() {
                primary["additionalProperties"] = self.leaf(additional, path, depth + 1)?;
            } else if additional != &Value::Bool(true)
                && !(unknown
                    && object.contains_key("patternProperties")
                    && additional == &Value::Bool(false))
                && !(primary["x-automerge"] == "bytes" && additional == &Value::Bool(false))
            {
                return Err(invalid(
                    path,
                    "closing or narrowing ordinary Lexicon objects is forbidden",
                ));
            }
        }
        if let Some(patterns) = object.get("patternProperties") {
            if !unknown {
                return Err(invalid(
                    path,
                    "typed maps require a primary unknown position",
                ));
            }
            super::codec::keyed_map(object).map_err(|reason| invalid(path, reason))?;
            let patterns = patterns.as_object().expect("validated keyed map");
            let (pattern, leaf) = patterns.iter().next().expect("one pattern");
            primary["patternProperties"] = json!({pattern: self.leaf(leaf, path, depth + 1)?});
            primary["additionalProperties"] = json!(false);
        }
        if let Some(properties) = object.get("properties") {
            let properties = properties
                .as_object()
                .ok_or_else(|| invalid(path, "properties must be an object"))?;
            for (name, property) in properties {
                let target = primary
                    .get_mut("properties")
                    .and_then(|properties| properties.get_mut(name))
                    .ok_or_else(|| {
                        invalid(
                            path,
                            format!(
                                "supplement property `{name}` is absent from the primary Lexicon"
                            ),
                        )
                    })?;
                self.apply(
                    target,
                    property,
                    &format!("{path}.properties.{name}"),
                    depth + 1,
                )?;
            }
        }
        if let Some(items) = object.get("items") {
            let target = primary
                .get_mut("items")
                .ok_or_else(|| invalid(path, "items supplement is not on an array"))?;
            self.apply(target, items, &format!("{path}.items"), depth + 1)?;
        }
        if object.contains_key("required") {
            // The discriminant the compile added to a union member is restated in the member's
            // own supplement; it is the same `$type` the filter below removes from the primary's
            // set before the two are compared.
            let required: BTreeSet<String> = names(object, "required", path)?
                .into_iter()
                .filter(|name| *name != "$type")
                .map(str::to_owned)
                .collect();
            let actual: BTreeSet<_> = primary
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|name| *name != "$type")
                .map(str::to_owned)
                .collect();
            if required != actual {
                return Err(invalid(
                    path,
                    "supplement required fields contradict the primary Lexicon",
                ));
            }
        }
        if object.contains_key("anyOf") || object.contains_key("oneOf") {
            return Err(invalid(
                path,
                "supplement branches are not an independent validation authority",
            ));
        }
        Ok(())
    }

    fn leaf(&mut self, schema: &Value, path: &str, depth: usize) -> Result<Value> {
        if depth > 128 {
            return Err(invalid(path, "typed-map schema depth exceeded"));
        }
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            if schema.as_object().is_none_or(|object| object.len() != 1) {
                return Err(invalid(path, "typed-map reference has siblings"));
            }
            let target = reference
                .strip_prefix("#/$defs/")
                .and_then(|name| self.source.get(name))
                .ok_or_else(|| invalid(path, "missing typed-map definition"))?
                .clone();
            return self.leaf(&target, path, depth + 1);
        }
        // An `Option` in a map value: schemars spells it `type: [T, "null"]` or
        // `anyOf: [T, {"type": "null"}]`, and the leaf is the non-null schema made nullable.
        if let Some(kinds) = schema.get("type").and_then(Value::as_array) {
            let nonnull: Vec<&Value> = kinds
                .iter()
                .filter(|kind| kind.as_str() != Some("null"))
                .collect();
            if nonnull.len() == 1 && nonnull.len() < kinds.len() {
                let mut inner = schema.clone();
                inner
                    .as_object_mut()
                    .expect("typed-map schema object")
                    .insert("type".to_owned(), nonnull[0].clone());
                return self.nullable_leaf(&inner, path, depth + 1);
            }
        }
        if let Some(branches) = schema.get("anyOf").and_then(Value::as_array) {
            let nonnull: Vec<&Value> = branches
                .iter()
                .filter(|branch| branch.get("type") != Some(&json!("null")))
                .collect();
            if nonnull.len() == 1 && nonnull.len() < branches.len() {
                return self.nullable_leaf(nonnull[0], path, depth + 1);
            }
        }
        let kind = schema
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(path, "typed-map leaves must declare a supported type"))?;
        let mut primary = match kind {
            "string"
                if matches!(
                    schema.get("x-automerge").and_then(Value::as_str),
                    Some("timestamp" | "datetime")
                ) || matches!(
                    schema.get("format").and_then(Value::as_str),
                    Some("date-time" | "datetime")
                ) =>
            {
                json!({"type": "string", "x-automerge": "datetime", KEYWORD: {"type": "string", "format": "datetime"}})
            }
            "string" => json!({"type": "string", KEYWORD: {"type": "string"}}),
            "integer" => {
                json!({"type": "integer", "minimum": i64::MIN, "maximum": i64::MAX, KEYWORD: {"type": "integer"}})
            }
            "boolean" => json!({"type": "boolean"}),
            "object" if schema.get("x-automerge").and_then(Value::as_str) == Some("bytes") => {
                json!({"type": "object", "properties": {"$bytes": {"type": "string", KEYWORD: {"type": "base64"}}}, "required": ["$bytes"], "additionalProperties": false, "x-automerge": "bytes"})
            }
            // A record-shaped leaf — a struct used as a map value: the object is built from its
            // declared members rather than read as the untyped open object.
            "object"
                if schema
                    .get("properties")
                    .and_then(Value::as_object)
                    .is_some_and(|properties| !properties.is_empty()) =>
            {
                let properties = schema["properties"]
                    .as_object()
                    .expect("checked properties");
                let mut compiled = Map::new();
                for (name, property) in properties {
                    if name.starts_with('$') {
                        return Err(invalid(
                            path,
                            "application properties cannot use reserved dollar keys",
                        ));
                    }
                    compiled.insert(name.clone(), self.leaf(property, path, depth + 1)?);
                }
                let mut primary = json!({"type": "object", "properties": Value::Object(compiled)});
                let mut annotation = schema.clone();
                {
                    let annotation = annotation.as_object_mut().expect("typed-map schema object");
                    if let Some(required) = annotation.remove("required") {
                        let required = required.as_array().expect("checked required list");
                        if required
                            .iter()
                            .filter_map(Value::as_str)
                            .any(|name| !properties.contains_key(name))
                        {
                            return Err(invalid(path, "required names must identify properties"));
                        }
                        primary["required"] = Value::Array(required.clone());
                    }
                    annotation.remove("properties");
                    // A boolean `additionalProperties` beside named properties is the struct's
                    // open/closed marker and is stated on the built object directly; left in the
                    // annotation it would read as the map narrowing `apply` forbids.
                    match annotation.remove("additionalProperties") {
                        None => {}
                        Some(additional) if additional.is_boolean() => {
                            primary["additionalProperties"] = additional;
                        }
                        Some(_) => {
                            return Err(invalid(
                                path,
                                "an object leaf names properties and also states a typed catch-all",
                            ));
                        }
                    }
                }
                self.apply(&mut primary, &annotation, path, depth + 1)?;
                return Ok(primary);
            }
            "object" => json!({"type": "object", KEYWORD: {"type": "unknown"}}),
            "array" => {
                let mut annotation = schema.clone();
                let items = annotation
                    .as_object_mut()
                    .expect("typed-map schema object")
                    .remove("items")
                    .ok_or_else(|| invalid(path, "typed array requires items"))?;
                let mut primary =
                    json!({"type": "array", "items": self.leaf(&items, path, depth + 1)?});
                self.apply(&mut primary, &annotation, path, depth + 1)?;
                return Ok(primary);
            }
            _ => return Err(invalid(path, "unsupported typed-map leaf type")),
        };
        self.apply(&mut primary, schema, path, depth + 1)?;
        Ok(primary)
    }

    /// One map-value leaf for an `Option` position: the non-null leaf plus its null.
    ///
    /// The null is an `anyOf` branch, the primary compiler's nullable spelling, everywhere but on
    /// a bytes leaf: bytes is a JSON object but a scalar in the document, so branch dispatch by
    /// stored kind would read a present value away from its declaration. There the leaf keeps the
    /// sidecar's own spelling — `null` beside the type on the one position — which the walk's
    /// null reading understands directly.
    fn nullable_leaf(&mut self, schema: &Value, path: &str, depth: usize) -> Result<Value> {
        let leaf = self.leaf(schema, path, depth)?;
        if leaf.get("x-automerge").and_then(Value::as_str) == Some("bytes") {
            let mut leaf = leaf;
            let kind = leaf
                .get("type")
                .and_then(Value::as_str)
                .expect("a bytes leaf states its type")
                .to_owned();
            leaf["type"] = json!([kind, "null"]);
            return Ok(leaf);
        }
        Ok(json!({"anyOf": [leaf, {"type": "null"}]}))
    }
}

pub(super) fn options() -> jsonschema::ValidationOptions {
    jsonschema::options()
        .with_keyword(KEYWORD, |_, value, _| {
            Ok(Box::new(Constraint(value.clone())))
        })
        .with_keyword(DATA, |_, _, _| Ok(Box::new(Data)))
}

struct Constraint(Value);

impl jsonschema::Keyword for Constraint {
    fn validate<'i>(
        &self,
        instance: &'i Value,
    ) -> std::result::Result<(), jsonschema::ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(jsonschema::ValidationError::custom(
                "value violates primary Lexicon semantics",
            ))
        }
    }

    fn is_valid(&self, instance: &Value) -> bool {
        match self.0["type"].as_str() {
            Some("string") => {
                let Some(value) = instance.as_str() else {
                    return false;
                };
                if !length(&self.0, "minLength", "maxLength", value.len()) {
                    return false;
                }
                if (self.0.get("minGraphemes").is_some() || self.0.get("maxGraphemes").is_some())
                    && !length(
                        &self.0,
                        "minGraphemes",
                        "maxGraphemes",
                        value.graphemes(true).count(),
                    )
                {
                    return false;
                }
                if self.0["native"] == "uint"
                    && (value.is_empty()
                        || !value.bytes().all(|byte| byte.is_ascii_digit())
                        || value.parse::<u64>().is_err()
                        || (value.len() > 1 && value.starts_with('0')))
                {
                    return false;
                }
                if self.0["native"] == "float"
                    && !value.parse::<f64>().is_ok_and(|number| {
                        super::codec::float_string(number, self.0["width"] == "float") == value
                    })
                {
                    return false;
                }
                self.0
                    .get("format")
                    .and_then(Value::as_str)
                    .is_none_or(|format| valid_format(format, value))
            }
            Some("integer") => instance.as_i64().is_some(),
            Some("boolean") => instance.is_boolean(),
            Some("base64") => instance
                .as_str()
                .is_some_and(|value| super::codec::decode_base64(value).is_some()),
            Some("bytes") => super::codec::bytes_value(instance)
                .is_some_and(|bytes| length(&self.0, "minLength", "maxLength", bytes.len())),
            Some("unknown") => instance.as_object().is_some_and(|object| {
                !object.contains_key("$bytes")
                    && !object.contains_key("$link")
                    && object.get("$type").and_then(Value::as_str) != Some("blob")
            }),
            Some("tag") => instance.as_str().is_some_and(|value| {
                let (id, name) = value.split_once('#').unwrap_or((value, ""));
                is_nsid(id)
                    && name != "main"
                    && !name.contains('#')
                    && (!value.contains('#') || !name.is_empty())
            }),
            _ => false,
        }
    }
}

struct Data;

impl jsonschema::Keyword for Data {
    fn validate<'i>(
        &self,
        instance: &'i Value,
    ) -> std::result::Result<(), jsonschema::ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(jsonschema::ValidationError::custom(
                "value violates the ATProto data model",
            ))
        }
    }

    fn is_valid(&self, instance: &Value) -> bool {
        let mut pending = vec![(instance, 0)];
        let mut visits = 0;
        while let Some((value, depth)) = pending.pop() {
            visits += 1;
            if visits > 1_000_000 || depth > 1024 {
                return false;
            }
            match value {
                Value::Number(number) if number.as_i64().is_none() => return false,
                Value::Object(object) => {
                    if object.contains_key("$bytes") {
                        if super::codec::bytes_value(value).is_none() {
                            return false;
                        }
                    } else if object.contains_key("$link") {
                        if object.len() != 1 || !object["$link"].as_str().is_some_and(cid) {
                            return false;
                        }
                    } else {
                        if object.get("$type").is_some_and(|value| !value.is_string()) {
                            return false;
                        }
                        if object.get("$type").and_then(Value::as_str) == Some("blob") {
                            return false;
                        }
                        pending.extend(object.values().map(|value| (value, depth + 1)));
                    }
                }
                Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
                _ => {}
            }
        }
        true
    }
}

fn length(schema: &Value, min: &str, max: &str, length: usize) -> bool {
    schema
        .get(min)
        .and_then(Value::as_u64)
        .is_none_or(|min| length as u64 >= min)
        && schema
            .get(max)
            .and_then(Value::as_u64)
            .is_none_or(|max| length as u64 <= max)
}

fn cid(value: &str) -> bool {
    let Some(value) = value.strip_prefix('b') else {
        return false;
    };
    if value.len() != 58
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || (b'2'..=b'7').contains(&byte))
    {
        return false;
    }
    data_encoding::BASE32_NOPAD
        .decode(value.to_ascii_uppercase().as_bytes())
        .is_ok_and(|bytes| {
            bytes.len() == 36
                && bytes[0] == 1
                && matches!(bytes[1], 0x55 | 0x71)
                && bytes[2..4] == [0x12, 0x20]
        })
}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn handle(value: &str) -> bool {
    value.len() <= 253
        && value.contains('.')
        && value.split('.').all(label)
        && value
            .rsplit('.')
            .next()
            .is_some_and(|last| last.starts_with(|character: char| character.is_ascii_alphabetic()))
}

fn did(value: &str) -> bool {
    let Some((method, identifier)) = value
        .strip_prefix("did:")
        .and_then(|value| value.split_once(':'))
    else {
        return false;
    };
    value.len() <= 2048
        && !method.is_empty()
        && method.bytes().all(|byte| byte.is_ascii_lowercase())
        && !identifier.is_empty()
        && !identifier.ends_with([':', '%'])
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:%-".contains(&byte))
}

fn record_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_:~".contains(&byte))
}

fn valid_format(format: &str, value: &str) -> bool {
    match format {
        "cid" => cid(value),
        "nsid" => is_nsid(value),
        "did" => did(value),
        "handle" => handle(value),
        "at-identifier" => did(value) || handle(value),
        "record-key" => record_key(value),
        "tid" => {
            value.len() == 13
                && value
                    .bytes()
                    .next()
                    .is_some_and(|byte| b"234567abcdefghij".contains(&byte))
                && value
                    .bytes()
                    .all(|byte| b"234567abcdefghijklmnopqrstuvwxyz".contains(&byte))
        }
        "at-uri" => {
            let Some(rest) = value.strip_prefix("at://") else {
                return false;
            };
            if value.len() > 8192 {
                return false;
            }
            let mut parts = rest.split('/');
            let authority = parts.next().expect("split has first entry");
            (did(authority) || handle(authority))
                && parts.next().is_none_or(is_nsid)
                && parts.next().is_none_or(record_key)
                && parts.next().is_none()
        }
        "datetime" => datetime(value),
        "uri" => {
            static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> =
                std::sync::OnceLock::new();
            value.len() <= 8192
                && VALIDATOR
                    .get_or_init(|| {
                        jsonschema::options()
                            .should_validate_formats(true)
                            .build(&json!({"type": "string", "format": "uri"}))
                            .expect("URI format schema")
                    })
                    .is_valid(&json!(value))
        }
        _ => false,
    }
}

pub(super) fn datetime(value: &str) -> bool {
    if !value.is_ascii()
        || value.len() < 20
        || value.as_bytes()[10] != b'T'
        || value.ends_with("-00:00")
    {
        return false;
    }
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().take(19).enumerate() {
        if !match index {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            _ => byte.is_ascii_digit(),
        } {
            return false;
        }
    }
    let tail = &value[19..];
    let (fraction, zone) = if let Some(fraction) = tail.strip_prefix('.') {
        let end = fraction
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(fraction.len());
        if end == 0 {
            return false;
        }
        (&fraction[..end], &fraction[end..])
    } else {
        ("", tail)
    };
    if zone != "Z"
        && !(zone.len() == 6
            && matches!(zone.as_bytes()[0], b'+' | b'-')
            && zone.as_bytes()[3] == b':'
            && zone[1..3].bytes().all(|byte| byte.is_ascii_digit())
            && zone[4..].bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    let normalized = if fraction.is_empty() {
        value.to_owned()
    } else {
        format!(
            "{}.{}{zone}",
            &value[..19],
            &fraction[..fraction.len().min(9)]
        )
    };
    normalized.parse::<jiff::Timestamp>().is_ok_and(|instant| {
        instant
            >= "0000-01-01T00:00:00Z"
                .parse::<jiff::Timestamp>()
                .expect("minimum Lexicon datetime")
    })
}

#[cfg(test)]
mod tests;
