use super::lexicon;
use method_core::schema::MethodSchema;
use serde_json::Value;
use serde_json::json;

/// Every built-in method: its NSID with the result of assembling its Lexicon.
fn every_builtin() -> Vec<(&'static str, Result<Value, super::MethodLexiconError>)> {
    let mut all: Vec<_> = super::builtin_methods()
        .map(|(nsid, assemble)| (nsid, assemble()))
        .collect();
    all.sort_by_key(|(nsid, _)| *nsid);
    all
}

#[test]
fn every_builtin_method_publishes_a_lexicon_that_passes_the_frame_check() {
    let all = every_builtin();
    let mut nsids: Vec<_> = all.iter().map(|(nsid, _)| *nsid).collect();
    nsids.dedup();
    assert_eq!(nsids.len(), all.len(), "an NSID is listed twice");

    let failures: Vec<_> = all
        .iter()
        .filter_map(|(nsid, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{nsid}: {error}"))
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn lexicon_of<M: MethodSchema>() -> Value {
    lexicon::<M>().expect("the method publishes a Lexicon")
}

#[test]
fn a_procedure_states_its_request_as_the_input_body() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::drawer::protocol::AddChild>());
}

#[test]
fn a_query_states_its_request_as_parameters() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::doc::GetDocAvailability>());
}

#[test]
fn a_subscription_states_its_event_as_the_message() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::doc::ObserveDocAvailability>());
}

#[test]
fn a_method_with_no_result_states_no_output_and_lists_its_errors() {
    let file = lexicon_of::<big_cabinet_core::drawer::protocol::ForgetDrawer>();
    let main = &file["defs"]["main"];
    assert!(main.get("output").is_none());
    let names: Vec<&str> = main["errors"]
        .as_array()
        .expect("the error enum's variants are listed")
        .iter()
        .map(|error| error["name"].as_str().expect("a name"))
        .collect();
    assert!(names.contains(&"Denied"), "{names:?}");
    assert!(
        !names.contains(&"Context"),
        "ContextError is a common error, not a per-method entry: {names:?}"
    );
}

/// The name a method's error envelope carries, checked against the `errors` its Lexicon declares.
fn envelope_error_is_declared<M: MethodSchema>(error: M::Error) {
    let envelope = serde_json::to_value(service_core_rs::xrpc::domain_error_envelope(&error))
        .expect("an envelope serializes");
    let name = envelope["error"]
        .as_str()
        .expect("the envelope names its error");
    let file = lexicon_of::<M>();
    let declared: Vec<&str> = file["defs"]["main"]["errors"]
        .as_array()
        .expect("the method lists its errors")
        .iter()
        .map(|error| error["name"].as_str().expect("a name"))
        .collect();
    assert!(
        declared.contains(&name),
        "{}: the envelope names `{name}`, the Lexicon declares {declared:?}",
        M::XRPC.nsid
    );
    assert_eq!(envelope["message"], error.to_string());
}

#[test]
fn an_error_envelope_names_a_variant_its_methods_lexicon_declares() {
    use big_blobs_core::protocol as blobs;
    use big_cabinet_core::doc as docs;
    use big_cabinet_core::drawer::protocol as drawers;
    use big_nodule_core::protocol as nodules;
    envelope_error_is_declared::<docs::ReadDatums>(docs::DocsError::Denied);
    envelope_error_is_declared::<docs::WriteDatums>(docs::DocsError::Access("no".to_owned()));
    envelope_error_is_declared::<drawers::ListKnownDrawers>(drawers::DrawersError::Denied);
    envelope_error_is_declared::<drawers::AddChild>(drawers::DrawersError::Storage(
        "disk".to_owned(),
    ));
    envelope_error_is_declared::<blobs::ReadBlob>(blobs::BlobsError::NotFound);
    envelope_error_is_declared::<nodules::ListNodules>(nodules::NodulesError::Denied);
}

#[test]
fn a_common_error_is_named_as_dispatch_names_its_refusals() {
    use big_cabinet_core::doc::DocsError;
    use method_core::ContextError;
    for (error, name) in [
        (ContextError::DeadlineExceeded, "DeadlineExceeded"),
        (ContextError::Unauthenticated, "Unauthenticated"),
        (
            ContextError::CancellationCapacityExceeded { limit: 3 },
            "CancellationCapacityExceeded",
        ),
    ] {
        let envelope = service_core_rs::xrpc::domain_error_envelope(&DocsError::Context(error));
        assert_eq!(serde_json::to_value(envelope).unwrap()["error"], name);
    }
}

const POSITIONS: [super::Position; 4] = [
    super::Position::Input,
    super::Position::Output,
    super::Position::Message,
    super::Position::Parameters,
];

#[test]
fn every_builtin_methods_positions_compile_to_validators() {
    let mut failures = Vec::new();
    for (nsid, file) in every_builtin() {
        let file = file.expect("the method publishes a Lexicon");
        for position in POSITIONS {
            if let Err(error) = super::position_validator(&file, &[], position) {
                failures.push(format!("{nsid} {position:?}: {error}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The body a method takes: a procedure's input, else a query's parameters.
fn request_position(file: &Value) -> super::Position {
    if file["defs"]["main"].get("input").is_some() {
        super::Position::Input
    } else {
        super::Position::Parameters
    }
}

/// Every built-in: a non-object request is refused, and a query refuses an empty object exactly
/// when it declares a required parameter.
#[test]
fn every_builtin_methods_request_refuses_what_is_not_its_shape() {
    for (nsid, file) in every_builtin() {
        let file = file.unwrap();
        if file["defs"]["main"]["type"] == "subscription"
            && file["defs"]["main"].get("parameters").is_none()
        {
            continue;
        }
        let position = request_position(&file);
        let validator = super::position_validator(&file, &[], position)
            .unwrap()
            .unwrap_or_else(|| panic!("{nsid} states a request"));
        assert!(
            !validator.violations(&json!(7)).is_empty(),
            "{nsid}: a number is not a request"
        );
        if position == super::Position::Parameters {
            let required = file["defs"]["main"]["parameters"]["required"]
                .as_array()
                .is_some_and(|names| !names.is_empty());
            assert_eq!(
                validator.violations(&json!({})).is_empty(),
                !required,
                "{nsid}: `{{}}` is a query exactly when nothing is required"
            );
        }
    }
}

/// A well-formed request per method, built the way a client writes it. Each is accepted as
/// written, refused with `capability` the wrong type, and refused without it.
fn valid_requests() -> Vec<(&'static str, Value)> {
    let cap =
        serde_json::to_value(big_cabinet_core::authority::CapId::from_bytes([1; 16])).unwrap();
    let doc = json!("z11111111111111111111111111111111");
    let cmd = json!("c1");
    let at = serde_json::to_value(big_cabinet_core::doc::At::Current).unwrap();
    let access = json!("Read");
    vec![
        (
            "org.example.daybook.docs.getDocAvailability",
            json!({"capability": cap, "docId": doc}),
        ),
        (
            "org.example.daybook.docs.observeDocAvailability",
            json!({"capability": cap, "docId": doc}),
        ),
        (
            "org.example.daybook.docs.listDatums",
            json!({"capability": cap, "docId": doc, "at": at}),
        ),
        (
            "org.example.daybook.docs.readDatums",
            json!({"capability": cap, "docId": doc, "at": at}),
        ),
        (
            "org.example.daybook.docs.mergeDoc",
            json!({"capability": cap, "sourceId": doc, "targetId": doc, "at": at}),
        ),
        (
            "org.example.daybook.docs.branchDoc",
            json!({"capability": cap, "command": cmd, "docId": doc, "at": at, "path": "p", "placement": "x"}),
        ),
        (
            "org.example.daybook.docs.createDoc",
            json!({"capability": cap, "command": cmd, "placement": "x", "datums": {}}),
        ),
        (
            "org.example.daybook.drawers.createDrawer",
            json!({"capability": cap, "command": cmd, "layout": "singleGroup"}),
        ),
        (
            "org.example.daybook.drawers.addDoc",
            json!({"capability": cap, "reach": cap, "command": cmd, "drawerId": doc, "docId": doc, "access": access}),
        ),
        (
            "org.example.daybook.drawers.removeDoc",
            json!({"capability": cap, "command": cmd, "drawerId": doc, "docId": doc}),
        ),
        (
            "org.example.daybook.drawers.addChild",
            json!({"capability": cap, "command": cmd, "parentId": doc, "childId": doc, "access": access}),
        ),
        (
            "org.example.daybook.drawers.removeChild",
            json!({"capability": cap, "command": cmd, "parentId": doc, "childId": doc}),
        ),
        (
            "org.example.daybook.drawers.getDrawer",
            json!({"capability": cap, "drawerId": doc}),
        ),
        (
            "org.example.daybook.drawers.listDocs",
            json!({"capability": cap, "drawerId": doc, "offset": 0}),
        ),
        (
            "org.example.daybook.drawers.rememberDrawer",
            json!({"capability": cap, "drawerId": doc}),
        ),
        (
            "org.example.daybook.drawers.forgetDrawer",
            json!({"capability": cap, "drawerId": doc}),
        ),
        (
            "org.example.daybook.drawers.listKnownDrawers",
            json!({"capability": cap}),
        ),
        (
            "org.example.daybook.drawers.listDrawers",
            json!({"capability": cap}),
        ),
        (
            "org.example.daybook.drawers.listDocDrawers",
            json!({"docId": doc}),
        ),
        (
            "org.example.daybook.drawers.setDrawerSuccessor",
            json!({"capability": cap, "drawerId": doc, "successor": doc}),
        ),
        (
            "org.example.daybook.drawers.resolveDrawerSuccessor",
            json!({"capability": cap, "drawerId": doc}),
        ),
        (
            "org.example.daybook.drawers.invitePrincipal",
            json!({"capability": cap, "drawerId": doc, "principal": {"kind": "peer", "peerId": doc}, "access": access}),
        ),
        (
            "org.example.daybook.drawers.mintDrawerCapability",
            json!({"scope": {"drawers": {}, "create": true}, "rights": ["use"]}),
        ),
    ]
}

#[test]
fn a_well_formed_request_passes_and_a_broken_one_does_not() {
    let files: std::collections::BTreeMap<_, _> = every_builtin()
        .into_iter()
        .map(|(nsid, file)| (nsid, file.unwrap()))
        .collect();
    for (nsid, valid) in valid_requests() {
        let file = &files[nsid];
        let validator = super::position_validator(file, &[], request_position(file))
            .unwrap()
            .unwrap();
        assert_eq!(validator.violations(&valid), Vec::<String>::new(), "{nsid}");
        let first = valid.as_object().unwrap().keys().next().unwrap().clone();
        let mut wrong_type = valid.clone();
        wrong_type[&first] = json!(7);
        assert!(
            !validator.violations(&wrong_type).is_empty(),
            "{nsid}: `{first}` as a number"
        );
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(&first);
        assert!(
            !validator.violations(&missing).is_empty(),
            "{nsid}: without `{first}`"
        );
    }
}

#[test]
fn a_nested_wrong_type_and_an_unknown_variant_are_refused() {
    let file = lexicon_of::<big_cabinet_core::drawer::protocol::MintDrawerCapability>();
    let validator = super::position_validator(&file, &[], super::Position::Input)
        .unwrap()
        .unwrap();
    let broken = json!({"scope": {"drawers": {}, "create": "yes"}, "rights": ["use"]});
    assert!(!validator.violations(&broken).is_empty());
}

fn coercion(nsid_params: &Value, query: &[(&str, &str)]) -> Result<Value, Vec<String>> {
    super::coerce_parameters(nsid_params, query)
        .map(Value::Object)
        .map_err(|errors| errors.iter().map(ToString::to_string).collect())
}

fn params() -> Value {
    json!({
        "type": "params",
        "required": ["name"],
        "properties": {
            "name": {"type": "string", "format": "datetime"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            "flag": {"type": "boolean"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "ns": {"type": "array", "items": {"type": "integer"}},
        }
    })
}

#[test]
fn a_query_string_coerces_to_the_types_the_parameters_declare() {
    assert_eq!(
        coercion(
            &params(),
            &[
                ("name", "x"),
                ("limit", "5"),
                ("flag", "false"),
                ("tags", "a"),
                ("tags", "b"),
                ("ns", "1"),
                ("ns", "-2")
            ]
        ),
        Ok(json!({"name": "x", "limit": 5, "flag": false, "tags": ["a", "b"], "ns": [1, -2]}))
    );
    // One occurrence of an array parameter is still an array.
    assert_eq!(
        coercion(&params(), &[("tags", "a")]),
        Ok(json!({"tags": ["a"]}))
    );
}

/// atproto precedent: a name the Lexicon does not declare is dropped, not refused.
#[test]
fn an_undeclared_parameter_is_ignored() {
    assert_eq!(
        coercion(
            &params(),
            &[("name", "x"), ("cacheBuster", "9f3"), ("limit", "7")]
        ),
        Ok(json!({"name": "x", "limit": 7}))
    );
}

/// A declared parameter the query omits takes the Lexicon's `default`; one it gives wins.
#[test]
fn an_omitted_parameter_takes_its_default() {
    let declared = json!({
        "type": "params",
        "properties": {
            "limit": {"type": "integer", "default": 50},
            "flag": {"type": "boolean", "default": false},
            "tags": {"type": "array", "items": {"type": "string"}, "default": ["a"]},
            "plain": {"type": "string"},
        }
    });
    assert_eq!(
        coercion(&declared, &[]),
        Ok(json!({"limit": 50, "flag": false, "tags": ["a"]}))
    );
    assert_eq!(
        coercion(&declared, &[("limit", "3"), ("tags", "z")]),
        Ok(json!({"limit": 3, "flag": false, "tags": ["z"]}))
    );
}

#[test]
fn each_bad_parameter_is_named_with_its_own_reason() {
    assert_eq!(
        coercion(
            &params(),
            &[
                ("limit", "five"),
                ("flag", "yes"),
                ("name", "a"),
                ("name", "b"),
                ("ns", "1"),
                ("ns", "x")
            ]
        ),
        Err(vec![
            "parameter `flag`: `yes` is not `true` or `false`".to_owned(),
            "parameter `limit`: `five` is not an integer".to_owned(),
            "parameter `name`: is given 2 times but is not an array".to_owned(),
            "parameter `ns`: `x` is not an integer".to_owned(),
        ])
    );
}

/// Coercion types the string; the position validator then holds it to bounds, formats and
/// required names.
#[test]
fn bounds_formats_and_required_names_are_the_validators() {
    let file = json!({
        "lexicon": 1,
        "id": "org.example.test.get",
        "defs": {"main": {"type": "query", "parameters": params(), "output": {"encoding": "application/json", "schema": {"type": "object", "properties": {}}}}}
    });
    let validator = super::position_validator(&file, &[], super::Position::Parameters)
        .unwrap()
        .unwrap();
    let check = |query: &[(&str, &str)]| {
        validator.violations(&Value::Object(
            super::coerce_parameters(&params(), query).unwrap(),
        ))
    };
    assert!(check(&[("name", "2026-01-01T00:00:00Z"), ("limit", "100")]).is_empty());
    assert!(!check(&[("name", "2026-01-01T00:00:00Z"), ("limit", "0")]).is_empty());
    assert!(!check(&[("name", "2026-01-01T00:00:00Z"), ("limit", "101")]).is_empty());
    assert!(!check(&[("name", "not a time")]).is_empty());
    assert!(!check(&[("limit", "5")]).is_empty());
}

/// An `unknown` admits any object and nothing else, and a union admits a variant it does not
/// carry unless it is closed.
#[test]
fn unknown_admits_objects_only_and_an_open_union_admits_other_variants() {
    let file = json!({
        "lexicon": 1,
        "id": "org.example.test.put",
        "defs": {
            "main": {"type": "procedure", "input": {"encoding": "application/json", "schema": {
                "type": "object", "required": ["any", "pick"],
                "properties": {"any": {"type": "unknown"}, "pick": {"type": "union", "refs": ["#a"]}}
            }}},
            "a": {"type": "object", "required": ["n"], "properties": {"n": {"type": "integer"}}}
        }
    });
    let validator = super::position_validator(&file, &[], super::Position::Input)
        .unwrap()
        .unwrap();
    let a = json!({"$type": "org.example.test.put#a", "n": 1});
    assert!(
        validator
            .violations(&json!({"any": {"x": 1}, "pick": a}))
            .is_empty()
    );
    assert!(
        !validator
            .violations(&json!({"any": 3, "pick": a}))
            .is_empty()
    );
    assert!(
        !validator
            .violations(&json!({"any": [], "pick": a}))
            .is_empty()
    );
    assert!(
        validator
            .violations(&json!({"any": {}, "pick": {"$type": "org.example.other#b"}}))
            .is_empty()
    );
    assert!(
        !validator
            .violations(&json!({"any": {}, "pick": {"$type": "org.example.test.put#a", "n": "x"}}))
            .is_empty()
    );
}
/// A struct that flattens a tagged enum states its own fields and the enum's, both required where
/// the type requires them (`CreateDrawer`'s `capability` and `command` beside the layout).
#[test]
fn a_flattened_enum_keeps_the_structs_own_fields() {
    let file = lexicon_of::<big_cabinet_core::drawer::protocol::CreateDrawer>();
    let schema = &file["defs"]["CreateDrawer"];
    for field in ["capability", "command", "docId", "layout"] {
        assert!(
            schema["properties"].get(field).is_some(),
            "{field}: {schema}"
        );
    }
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    assert!(
        required.contains(&"capability") && required.contains(&"command"),
        "{required:?}"
    );
}
