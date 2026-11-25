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

use big_cabinet_core::datum::LexiconAdmissionError;
use big_cabinet_core::datum::PositionValidator;
use big_cabinet_core::datum::compile_position;
use big_cabinet_core::datum::lexicon::project_position;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::datum::operation::validate;
use big_cabinet_core::datum::rust_number_formats_to_bounds;
use method_core::Method;
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

/// Where in a method's Lexicon file a body is stated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// A procedure's `input.schema`.
    Input,
    /// A query's or procedure's `output.schema`.
    Output,
    /// A subscription's `message.schema`.
    Message,
    /// A query's or subscription's `parameters`, as the object a query string coerces to.
    Parameters,
}

/// The validator of one position of a method's Lexicon `file`, compiled with the datum Lexicon
/// compiler (strict atproto semantics: `unknown` admits objects only, refs resolve in the file
/// and the `carried` files). `None` when the method states nothing at that position (a procedure
/// has no parameters, a method returning `()` no output).
pub fn position_validator(
    file: &Value,
    carried: &[&Value],
    position: Position,
) -> Result<Option<PositionValidator>, LexiconAdmissionError> {
    let main = &file["defs"]["main"];
    let (schema, path) = match position {
        Position::Input => (main["input"].get("schema").cloned(), "input.schema"),
        Position::Output => (main["output"].get("schema").cloned(), "output.schema"),
        Position::Message => (main["message"].get("schema").cloned(), "message.schema"),
        Position::Parameters => (
            main.get("parameters").map(|parameters| {
                json!({
                    "type": "object",
                    "properties": parameters.get("properties").cloned().unwrap_or_else(|| json!({})),
                    "required": parameters.get("required").cloned().unwrap_or_else(|| json!([])),
                })
            }),
            "parameters",
        ),
    };
    schema
        .map(|schema| compile_position(file, carried, &schema, path))
        .transpose()
}

/// A query parameter that is not the type the Lexicon declares.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("parameter `{name}`: {reason}")]
pub struct ParameterError {
    pub name: String,
    pub reason: String,
}

/// The typed JSON object a query string carries, read through a method's Lexicon `parameters`
/// (`file["defs"]["main"]["parameters"]`): `boolean` is `true` or `false`, `integer` a base-10
/// integer, `string` itself, and an `array` is every occurrence of the key, in order. This is the
/// whole of the coercion: bounds, formats, enums and required names are the position validator's
/// (compile [`Position::Parameters`] and validate the result). A name the Lexicon does not
/// declare is ignored (atproto objects are open for extension, and clients add cache-busters:
/// `@atproto/xrpc-server`'s `decodeQueryParams` only keeps keys with a declared property), and a
/// declared parameter the query omits takes the Lexicon's `default`.
pub fn coerce_parameters(
    parameters: &Value,
    query: &[(&str, &str)],
) -> Result<Map<String, Value>, Vec<ParameterError>> {
    let empty = Map::new();
    let declared = parameters
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let mut errors = Vec::new();
    let mut occurrences: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (name, value) in query {
        occurrences.entry(name).or_default().push(value);
    }
    let mut coerced = Map::new();
    for (name, values) in occurrences {
        let mut fail = |reason: String| {
            errors.push(ParameterError {
                name: name.to_owned(),
                reason,
            })
        };
        let Some(declaration) = declared.get(name) else {
            continue;
        };
        let scalar = |kind: &str, text: &str| -> Result<Value, String> {
            match kind {
                "string" => Ok(json!(text)),
                "boolean" => match text {
                    "true" => Ok(json!(true)),
                    "false" => Ok(json!(false)),
                    _ => Err(format!("`{text}` is not `true` or `false`")),
                },
                "integer" => text
                    .parse::<i64>()
                    .map(|number| json!(number))
                    .map_err(|_| format!("`{text}` is not an integer")),
                other => Err(format!("a `{other}` cannot come from a query string")),
            }
        };
        let kind = declaration["type"].as_str().unwrap_or_default();
        if kind == "array" {
            let item = declaration["items"]["type"].as_str().unwrap_or_default();
            let items: Result<Vec<Value>, String> =
                values.iter().map(|text| scalar(item, text)).collect();
            match items {
                Ok(items) => {
                    coerced.insert(name.to_owned(), Value::Array(items));
                }
                Err(reason) => fail(reason),
            }
        } else if let [text] = values.as_slice() {
            match scalar(kind, text) {
                Ok(value) => {
                    coerced.insert(name.to_owned(), value);
                }
                Err(reason) => fail(reason),
            }
        } else {
            fail(format!(
                "is given {} times but is not an array",
                values.len()
            ));
        }
    }
    for (name, declaration) in declared {
        if let Some(default) = declaration.get("default")
            && !coerced.contains_key(name)
        {
            coerced.insert(name.clone(), default.clone());
        }
    }
    if errors.is_empty() {
        Ok(coerced)
    } else {
        Err(errors)
    }
}

type Assemble = fn() -> Result<Value, MethodLexiconError>;

/// Every built-in method with the function that assembles its Lexicon. One list, so a method
/// added to a service is one line here; a router that registers a built-in operation missing from
/// it refuses to start (`big_dispatch::ServiceProvider::register`), so a new method cannot go
/// unlisted unnoticed.
pub fn builtin_methods() -> impl Iterator<Item = (&'static str, Assemble)> {
    builtin_list()
        .iter()
        .map(|(nsid, assemble, _)| (*nsid, *assemble))
}

type Builtin = (&'static str, Assemble, fn() -> MethodSchemas);

/// [`builtin_methods`] with each method's schemas too, which the tests read the error enums from.
fn builtin_list() -> &'static [Builtin] {
    static LIST: std::sync::LazyLock<Vec<Builtin>> = std::sync::LazyLock::new(|| {
        macro_rules! group {
                ($($method:path),* $(,)?) => { vec![$((
                    <$method as Method>::XRPC.nsid,
                    lexicon::<$method> as Assemble,
                    <$method as MethodSchema>::schemas as fn() -> MethodSchemas,
                )),*] };
            }
        use big_blobs_core::protocol as blobs;
        use big_cabinet_core::authority;
        use big_cabinet_core::doc as docs;
        use big_cabinet_core::drawer::protocol as drawers;
        use big_cartridges_core::protocol as cartridges;
        use big_dispatch_core as dispatch;
        use big_nodule_core::protocol as nodules;
        group![
            authority::OpenActor,
            authority::ResolveActor,
            authority::ResolveAuthority,
            authority::MintCapability,
            authority::AttenuateCapability,
            authority::InspectCapability,
            authority::RetractCapability,
            authority::SignDatumClaim,
            authority::ObserveCapability,
            docs::MintDocCapability,
            docs::CreateDoc,
            docs::WriteDatums,
            docs::BranchDoc,
            docs::MergeDoc,
            docs::ReadDatums,
            docs::ListDatums,
            docs::PollDatums,
            docs::QueryDatumRefs,
            docs::ResolveKeyRef,
            docs::UnsafeInspectDatum,
            docs::InspectDatumClaims,
            docs::GetDocAvailability,
            docs::ObserveDocAvailability,
            drawers::MintDrawerCapability,
            drawers::CreateDrawer,
            drawers::AddDoc,
            drawers::CreateMetaDoc,
            drawers::RemoveDoc,
            drawers::AddChild,
            drawers::RemoveChild,
            drawers::GetDrawer,
            drawers::ListDocs,
            drawers::RememberDrawer,
            drawers::ForgetDrawer,
            drawers::ListKnownDrawers,
            drawers::SetDrawerPolicy,
            drawers::SetDrawerSuccessor,
            drawers::ResolveDrawerSuccessor,
            drawers::ListDrawers,
            drawers::ListDocDrawers,
            drawers::GrantDrawer,
            drawers::RevokeDrawer,
            drawers::InvitePrincipal,
            drawers::RevokePrincipal,
            dispatch::Invoke,
            dispatch::MintDispatchCapability,
            nodules::MintNoduleCapability,
            nodules::ConfirmNoduleResolution,
            nodules::SetNoduleDevTakeover,
            nodules::SetMethodRoute,
            nodules::GetNodule,
            nodules::ListNodules,
            blobs::MintBlobsCapability,
            blobs::PutBlob,
            blobs::ImportFile,
            blobs::ReadBlob,
            blobs::StatBlob,
            cartridges::MintCartridgesCapability,
            cartridges::PublishCartridgeVersion,
            cartridges::StageCartridgeVersion,
            cartridges::RemoveCartridgeVersion,
            cartridges::RemoveCartridge,
            cartridges::ImportCartridgeDocument,
            cartridges::GetPublisher,
            cartridges::ListCartridges,
            cartridges::GetCartridge,
            cartridges::GetCartridgeVersion,
            cartridges::ResolveCartridgeVersion,
            cartridges::WatchCartridges,
            cartridges::InstallCartridge,
            cartridges::SetCartridgeEnabled,
            cartridges::UninstallCartridge,
            cartridges::SetCartridgeGrant,
            cartridges::ClearCartridgeConfig,
            cartridges::ListCartridgeInstalls,
            cartridges::ObserveCartridgeInstalls,
        ]
    });
    &LIST
}

/// The compiled form of one method's Lexicon: what dispatch checks a call against, built once
/// when the method's provider registers. `request` is a procedure's input body or a query's
/// parameters object, `output` the success body; a position the Lexicon does not state (a method
/// returning `()` has no output) is not checked.
pub struct MethodContract {
    kind: OperationKind,
    request: Option<PositionValidator>,
    output: Option<PositionValidator>,
    parameters: Value,
}

fn check(validator: Option<&PositionValidator>, value: &Value) -> Result<(), Vec<String>> {
    match validator {
        Some(validator) if !validator.is_valid(value) => Err(validator.violations(value)),
        _ => Ok(()),
    }
}

impl MethodContract {
    /// Compiles method `file` (a query, procedure or subscription Lexicon) with the files it may
    /// reference in `carried`.
    pub fn compile(file: &Value, carried: &[&Value]) -> Result<Self, LexiconAdmissionError> {
        let main = &file["defs"]["main"];
        let kind = match main["type"].as_str() {
            Some("query") => OperationKind::Query,
            Some("procedure") => OperationKind::Procedure,
            Some("subscription") => OperationKind::Subscription,
            other => unreachable!("a method Lexicon's main is an operation, not {other:?}"),
        };
        // A procedure whose body is bytes states only the body's encoding, so what dispatch checks
        // of its request is the parameters, as for a query.
        let json_body = main["input"]["encoding"].as_str() == Some("application/json");
        let request = match kind {
            OperationKind::Procedure if json_body => Position::Input,
            OperationKind::Procedure | OperationKind::Query | OperationKind::Subscription => {
                Position::Parameters
            }
        };
        Ok(Self {
            kind,
            request: position_validator(file, carried, request)?,
            output: position_validator(file, carried, Position::Output)?,
            parameters: main.get("parameters").cloned().unwrap_or_else(|| json!({})),
        })
    }

    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    /// The typed object a query string carries, read through the method's `parameters`
    /// ([`coerce_parameters`]).
    pub fn coerce_query(
        &self,
        query: &[(&str, &str)],
    ) -> Result<Map<String, Value>, Vec<ParameterError>> {
        coerce_parameters(&self.parameters, query)
    }

    /// Whether `input` conforms to the method's request body or parameters; otherwise every way
    /// it departs. The pass is the common case and allocates nothing.
    pub fn check_request(&self, input: &Value) -> Result<(), Vec<String>> {
        check(self.request.as_ref(), input)
    }

    /// Whether `output` conforms to the method's success body; otherwise every way it departs.
    pub fn check_output(&self, output: &Value) -> Result<(), Vec<String>> {
        check(self.output.as_ref(), output)
    }
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
    // A byte body (ADR 042 §1) is stated by its encoding alone, `*/*` for any media type, and
    // its method keeps JSON-shaped parameters, which are the request type's fields.
    let encoding = |encoding: method_core::Encoding| match encoding {
        method_core::Encoding::Json => None,
        method_core::Encoding::Bytes { mime } => Some(json!({ "encoding": mime.unwrap_or("*/*") })),
        method_core::Encoding::Descriptors => {
            Some(json!({ "encoding": "application/x-descriptors" }))
        }
    };
    match kind {
        OperationKind::Procedure => match encoding(M::XRPC.input_encoding) {
            None => {
                let schema = project(nsid, "request", &request, &definitions, &mut carried)?;
                main.insert(
                    "input".to_owned(),
                    json!({ "encoding": "application/json", "schema": schema }),
                );
            }
            Some(input) => {
                let parameters = parameters(nsid, request_object, &definitions)?;
                main.insert("parameters".to_owned(), parameters);
                main.insert("input".to_owned(), input);
            }
        },
        OperationKind::Query | OperationKind::Subscription => {
            let parameters = parameters(nsid, request_object, &definitions)?;
            main.insert("parameters".to_owned(), parameters);
        }
    }
    let body = resolve(&output, &definitions);
    if let Some(output) = encoding(M::XRPC.output_encoding) {
        main.insert("output".to_owned(), output);
    } else if body.get("type") != Some(&json!("null")) {
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
            } else if let Some(mut flat) =
                flatten_tagged(object).or_else(|| flatten_external(object))
            {
                // A struct that `#[serde(flatten)]`s an enum states its own fields beside the
                // enum's `oneOf`; they are fields of the same wire object.
                let own = Value::Object(std::mem::take(object));
                let flat_object = flat.as_object_mut().expect("a flattened enum is an object");
                if let Some(Value::Object(own_properties)) = own.get("properties") {
                    let properties = flat_object
                        .entry("properties")
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                        .expect("properties are an object");
                    properties.extend(own_properties.clone());
                }
                if let Some(Value::Array(own_required)) = own.get("required") {
                    let required = flat_object
                        .entry("required")
                        .or_insert_with(|| json!([]))
                        .as_array_mut()
                        .expect("required is an array");
                    for name in own_required {
                        if !required.contains(name) {
                            required.push(name.clone());
                        }
                    }
                }
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
