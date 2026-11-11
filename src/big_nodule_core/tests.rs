use super::*;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::request::Necessity;
use serde_json::json;

/// The NSID authority the photo browser's envelope holds.
const PHOTOS: &str = "org.example.photos";

fn file(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

/// A datum whose `main` is a record or an object under the authority is held; a Lexicon with no
/// `main` (a type library) and a datum outside the authority are carried.
#[test]
fn registrable_datums_hold_record_and_object_mains_under_the_authority() {
    let mut nodule = Nodule::default();
    for (id, defs) in [
        (
            "org.example.photos.record",
            json!({"main": {"type": "record", "key": "any", "record": {"type": "object", "properties": {}}}}),
        ),
        (
            "org.example.photos.object",
            json!({"main": {"type": "object", "properties": {}}}),
        ),
        (
            "org.example.photos.library",
            json!({"shared": {"type": "object", "properties": {}}}),
        ),
        (
            "org.example.other.object",
            json!({"main": {"type": "object", "properties": {}}}),
        ),
    ] {
        nodule
            .lexicons
            .push(file(json!({"lexicon": 1, "id": id, "defs": defs})));
    }
    let split = nodule.registrable_datums(Some(PHOTOS));
    let held: Vec<&str> = split.held.iter().map(|(id, ..)| *id).collect();
    assert_eq!(
        held,
        ["org.example.photos.record", "org.example.photos.object"]
    );
    let carried: Vec<&str> = split
        .carried
        .iter()
        .map(|file| file["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        carried,
        ["org.example.photos.library", "org.example.other.object"]
    );
}

/// The photo browser of ADR 027 §3: declares its own datum, query and subscription, requires
/// the search vocabulary, and carries a command, view and processor binding it never interprets.
fn photos() -> Nodule {
    let mut nodule = Nodule::default();
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.photo",
        "defs": {
            "main": {
                "type": "record",
                "key": "tid",
                "record": { "type": "object", "properties": { "title": { "type": "string" } } },
            },
            "still": { "type": "object", "properties": {} },
        },
    })));
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.list",
        "defs": {
            "main": {
                "type": "query",
                "parameters": { "type": "params", "properties": { "after": { "type": "integer" } } },
                "output": {
                    "encoding": "application/json",
                    "schema": { "type": "ref", "ref": "#page" },
                },
            },
            "page": {
                "type": "object",
                "properties": {
                    "photos": { "type": "array", "items": { "type": "ref", "ref": "org.example.photos.photo" } },
                },
            },
        },
    })));
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.import",
        "defs": {
            "main": {
                "type": "procedure",
                "input": { "encoding": "image/*" },
                "output": { "encoding": "application/json", "schema": { "type": "ref", "ref": "org.example.photos.photo" } },
            },
        },
    })));
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.changes",
        "defs": {
            "main": {
                "type": "subscription",
                "message": { "schema": { "type": "union", "refs": ["org.example.photos.photo"] } },
            },
        },
    })));
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.present",
        "defs": {
            "main": {
                "type": "query",
                "output": { "encoding": "application/json", "schema": { "type": "ref", "ref": "org.example.search.query#hits" } },
            },
        },
    })));
    nodule.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.settings",
        "defs": { "main": { "type": "object", "properties": { "thumbnails": { "type": "boolean" } } } },
    })));
    nodule.private_state.push(PrivateState {
        key: "index".into(),
        kind: StateKind::Sqlite,
        extra: Default::default(),
    });
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = [
        "org.example.photos.list",
        "org.example.photos.import",
        "org.example.photos.present",
        "org.example.photos.changes",
        "org.example.search.query",
    ]
    .map(String::from)
    .to_vec();
    nodule.revisioned_stores.push(RevisionedStore {
        key: "photos".into(),
        selector: "org.example.photos.photo#still".into(),
        entry: "org.example.photos.photo".into(),
        mode: StoreMode::Frontier,
        source: Some("org.example.photos.import".into()),
        extra: Default::default(),
    });
    nodule.initialization.push(Initializer {
        key: "import".into(),
        target: "org.example.photos.import".into(),
        mode: RunMode::Once,
        extra: Default::default(),
    });
    nodule.bindings.insert(
        "org.example.daybook.nodule.commands".into(),
        json!([{ "key": "findPhotos", "target": "org.example.search.query", "view": "hits" }]),
    );
    nodule.bindings.insert(
        "org.example.daybook.nodule.views".into(),
        json!([{ "key": "hits", "input": "#nowhere", "presenter": "not-a-routine" }]),
    );
    nodule.bindings.insert(
        "org.example.daybook.nodule.processors".into(),
        json!([{ "key": "thumbnails", "target": "org.example.tags.apply", "trigger": { "on": "documentWritten" } }]),
    );
    nodule.config = Some(Config {
        schema: "org.example.photos.settings".into(),
        defaults: Some(
            [("thumbnails".to_owned(), json!(true))]
                .into_iter()
                .collect(),
        ),
        extra: Default::default(),
    });
    nodule
}

fn refuse(nodule: Nodule, needle: &str) {
    refuse_under(nodule, Some(PHOTOS), needle);
}

fn refuse_under(nodule: Nodule, authority: Option<&str>, needle: &str) {
    let error = nodule.admit(authority).unwrap_err().to_string();
    assert!(error.contains(needle), "{error}");
}

/// A request to read the data contract `contract`.
fn data_request(binding: &str, contract: &str) -> Request {
    let mut request = Request::new(binding, contract);
    request.access = Some("read".into());
    request
}

#[test]
fn the_photo_browser_admits_and_round_trips() {
    let nodule = photos();
    let admitted = nodule.admit(Some(PHOTOS)).unwrap();
    assert_eq!(
        admitted.operations,
        [
            (
                "org.example.photos.changes",
                Some(OperationKind::Subscription)
            ),
            ("org.example.photos.import", Some(OperationKind::Procedure)),
            ("org.example.photos.list", Some(OperationKind::Query)),
            ("org.example.photos.present", Some(OperationKind::Query)),
            ("org.example.search.query", None),
        ]
        .into_iter()
        .map(|(id, kind)| (id.to_owned(), kind))
        .collect()
    );
    assert_eq!(admitted.artifacts, BTreeSet::from(["handler".to_owned()]));
    assert_eq!(nodule.datum_lexicons().count(), 2);

    let value = serde_json::to_value(&nodule).unwrap();
    assert_eq!(value["handler"]["artifact"], "handler");
    assert_eq!(value["implements"][0], "org.example.photos.list");
    assert_eq!(value["revisionedStores"][0]["mode"], "frontier");
    assert_eq!(value["initialization"][0]["mode"], "once");
    assert_eq!(value["privateState"][0]["kind"], "sqlite");
    assert_eq!(serde_json::from_value::<Nodule>(value).unwrap(), nodule);
}

#[test]
fn unknown_vocabulary_is_carried_on_the_wire_and_refused_at_admission() {
    let value = json!({
        "privateState": [{ "key": "s", "kind": "graph" }],
        "futureField": 1,
    });
    let nodule: Nodule = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        nodule.private_state[0].kind,
        StateKind::Unknown("graph".into())
    );
    assert_eq!(serde_json::to_value(&nodule).unwrap(), value);
    refuse(nodule, "no `graph` state");
}

#[test]
fn bindings_are_carried_verbatim_and_never_validated() {
    // Known sections with content the old per-binding checks refused, and a section no build
    // knows: all admit, and the description re-encodes byte-identically.
    let mut value = serde_json::to_value(photos()).unwrap();
    value["bindings"]["org.example.future.section"] =
        json!({ "anything": [1, { "goes": null }], "$type": "org.example.future.section" });
    value["bindings"]["org.example.daybook.nodule.commands"] =
        json!([{ "key": "", "target": "not an nsid", "unknownField": true }]);
    let nodule: Nodule = serde_json::from_value(value.clone()).unwrap();
    nodule.admit(Some(PHOTOS)).unwrap();
    assert_eq!(nodule.bindings.len(), 4);
    assert_eq!(serde_json::to_value(&nodule).unwrap(), value);

    // Pre-reshape top-level binding fields are no longer typed: they are carried as unknown
    // members, not interpreted.
    let legacy = json!({ "commands": [{ "key": "x" }] });
    let nodule: Nodule = serde_json::from_value(legacy.clone()).unwrap();
    nodule.admit(Some(PHOTOS)).unwrap();
    assert_eq!(serde_json::to_value(&nodule).unwrap(), legacy);
}

#[test]
fn authority_classifies_held_and_foreign_lexicon_files() {
    let mut duplicate = photos();
    duplicate.lexicons.push(duplicate.lexicons[0].clone());
    refuse(duplicate, "declared twice");

    let mut not_nsid = photos();
    not_nsid.lexicons[0].insert("id".into(), json!("photo"));
    refuse(not_nsid, "lexicons[0].id");

    let admitted = photos().admit(Some(PHOTOS)).unwrap();
    assert_eq!(admitted.authority.as_deref(), Some(PHOTOS));

    // A file outside the authority is a vendored copy: refused with its path unless it is the
    // contract of a request or an operation the nodule implements.
    let mut vendored = photos();
    vendored.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.tags.label",
        "defs": { "main": { "type": "object", "properties": {} } },
    })));
    refuse(
        vendored.clone(),
        "lexicons[6].id`: `org.example.tags.label` is outside this cartridge's authority `org.example.photos` and is neither requested nor implemented",
    );
    vendored
        .requires
        .push(data_request("labels", "org.example.tags.label"));
    vendored.admit(Some(PHOTOS)).unwrap();

    // Nothing is held without an authority: every file must be a vendored copy.
    refuse_under(photos(), None, "lexicons[0].id");
    // Whole segments only: `org.example.photosynthesis` is not under `org.example.photos`.
    refuse_under(photos(), Some("org.example.phot"), "lexicons[0].id");

    // A request cannot name a contract under the nodule's own authority.
    let mut own = photos();
    own.requires
        .push(Request::new("list", "org.example.photos.list"));
    refuse(
        own,
        "requires[0].contract`: `org.example.photos.list` is under this cartridge's authority",
    );
}

#[test]
fn own_references_resolve_and_foreign_ones_pass() {
    let mut missing_file = photos();
    missing_file.revisioned_stores[0].entry = "org.example.photos.album".into();
    refuse(
        missing_file,
        "`revisionedStores[0].entry`: `org.example.photos.album` is under this cartridge's authority but not declared",
    );

    let mut missing_def = photos();
    missing_def.revisioned_stores[0].selector = "org.example.photos.photo#moving".into();
    refuse(missing_def, "declares no definition `moving`");

    let mut operation_as_schema = photos();
    operation_as_schema.config.as_mut().unwrap().schema = "org.example.photos.list".into();
    refuse(operation_as_schema, "is an operation, not a schema");

    let mut local_ref_in_contribution = photos();
    local_ref_in_contribution.revisioned_stores[0].selector = "#still".into();
    refuse(local_ref_in_contribution, "a contribution names its file");

    let mut bad_operation_ref = photos();
    bad_operation_ref.lexicons[1]["defs"]["page"]["properties"]["photos"]["items"]["ref"] =
        json!("org.example.photos.photo#missing");
    refuse(
        bad_operation_ref,
        "lexicons[1].defs.page.properties.photos.items.ref",
    );

    // A foreign Lexicon is left to its provider, but only once the nodule has asked for it.
    let mut foreign = photos();
    foreign.revisioned_stores[0].entry = "org.example.elsewhere.defs#anything".into();
    refuse(
        foreign.clone(),
        "revisionedStores[0].entry`: `org.example.elsewhere.defs` is outside this cartridge's authority and is neither implemented nor requested",
    );
    foreign
        .requires
        .push(data_request("defs", "org.example.elsewhere.defs"));
    foreign.admit(Some(PHOTOS)).unwrap();
}

/// The node's reference defs resolve for any cartridge without a request or a vendored copy; a
/// vendored copy is accepted only when it is the node's own, and a def the library lacks is
/// refused.
#[test]
fn the_nodes_reference_defs_resolve_without_a_request_or_a_vendored_copy() {
    let tagging = |reference: &str| {
        let mut nodule = Nodule::default();
        nodule.lexicons.push(file(json!({
            "lexicon": 1,
            "id": "org.example.photos.tag",
            "defs": { "main": {
                "type": "procedure",
                "input": {
                    "encoding": "application/json",
                    "schema": {
                        "type": "object",
                        "properties": { "part": { "type": "ref", "ref": reference } },
                    },
                },
            }},
        })));
        nodule.handler = Some(Handler {
            artifact: "handler".into(),
            extra: Default::default(),
        });
        nodule.implements = ["org.example.photos.tag".to_owned()].into();
        nodule
    };
    for reference in [
        "org.example.daybook.defs#datumRef",
        "org.example.daybook.defs#datumLocation",
        "org.example.daybook.defs#datumPin",
    ] {
        tagging(reference).admit(Some(PHOTOS)).unwrap();
    }
    refuse(
        tagging("org.example.daybook.defs#datumNope"),
        "declares no definition `datumNope`",
    );

    let mut vendored = tagging("org.example.daybook.defs#datumRef");
    let copy = big_cabinet_core::reference::defs_lexicon();
    vendored.lexicons.push(file(copy.clone()));
    vendored.admit(Some(PHOTOS)).unwrap();

    let mut conflicting = tagging("org.example.daybook.defs#datumRef");
    let mut altered = copy;
    altered["defs"]["datumRef"]["description"] = json!("a different datumRef");
    conflicting.lexicons.push(file(altered));
    refuse(
        conflicting,
        "`org.example.daybook.defs` is a type library the node provides; a vendored copy must equal the node's",
    );
}

#[test]
fn the_handler_implements_each_declared_operation_once() {
    let mut twice = photos();
    twice.implements.push("org.example.photos.list".into());
    refuse(twice, "`org.example.photos.list` is implemented twice");

    let mut undeclared = photos();
    undeclared.implements[0] = "org.example.photos.missing".into();
    refuse(
        undeclared,
        "implements[0]`: `org.example.photos.missing` is under this cartridge's authority but is not a declared operation",
    );

    let mut datum_not_operation = photos();
    datum_not_operation.implements[0] = "org.example.photos.photo".into();
    refuse(
        datum_not_operation,
        "implements[0]`: `org.example.photos.photo` is declared here but is not an operation",
    );

    let mut self_edge = photos();
    self_edge
        .requires
        .push(Request::new("search", "org.example.search.query"));
    refuse(self_edge, "both requested and implemented");

    // A foreign operation is admitted on syntax alone (its contract is resolved at bind time);
    // a malformed NSID and a duplicate foreign entry are not.
    let mut malformed = photos();
    malformed.implements.push("not an nsid".into());
    refuse(malformed, "`not an nsid` is not an NSID");
    let mut foreign_twice = photos();
    foreign_twice
        .implements
        .push("org.example.search.query".into());
    refuse(
        foreign_twice,
        "`org.example.search.query` is implemented twice",
    );
    let mut foreign = photos();
    foreign.implements.push("org.example.tags.apply".into());
    assert_eq!(
        foreign.admit(Some(PHOTOS)).unwrap().operations["org.example.tags.apply"],
        None
    );

    let mut no_handler = photos();
    no_handler.handler = None;
    refuse(no_handler, "need a `handler`");

    let mut no_artifact = photos();
    no_artifact.handler.as_mut().unwrap().artifact = String::new();
    refuse(no_artifact, "names the artifact");

    // A handler with nothing to serve and a nodule with neither are both fine.
    let mut idle_handler = photos();
    idle_handler.implements.clear();
    idle_handler
        .requires
        .push(Request::new("search", "org.example.search.query"));
    idle_handler.revisioned_stores[0].source = None;
    idle_handler.initialization.clear();
    idle_handler.admit(Some(PHOTOS)).unwrap();
    Nodule::default().admit(Some(PHOTOS)).unwrap();
}

#[test]
fn initializers_and_store_sources_name_implemented_or_required_operations() {
    let mut subscription = photos();
    subscription.initialization[0].target = "org.example.photos.changes".into();
    refuse(subscription, "is a subscription");

    let mut unimplemented = photos();
    unimplemented
        .implements
        .retain(|nsid| nsid != "org.example.photos.import");
    unimplemented.revisioned_stores[0].source = None;
    refuse(
        unimplemented,
        "initialization[0].target`: `org.example.photos.import` is declared here but is not implemented",
    );

    let mut unrequired = photos();
    unrequired.initialization[0].target = "org.example.tags.apply".into();
    refuse(
        unrequired,
        "initialization[0].target`: `org.example.tags.apply` is neither implemented nor requested",
    );

    // A foreign operation this nodule implements is as good a target or source as a local one.
    let mut foreign_implemented = photos();
    foreign_implemented.initialization[0].target = "org.example.search.query".into();
    foreign_implemented.revisioned_stores[0].source = Some("org.example.search.query".into());
    foreign_implemented.admit(Some(PHOTOS)).unwrap();

    let mut required = photos();
    required
        .requires
        .push(Request::new("tags", "org.example.tags.apply"));
    required.initialization[0].target = "org.example.tags.apply".into();
    required.admit(Some(PHOTOS)).unwrap();

    // A data contract is read or written, not called.
    let mut data_target = photos();
    data_target
        .requires
        .push(data_request("tags", "org.example.tags.apply"));
    data_target.initialization[0].target = "org.example.tags.apply".into();
    refuse(
        data_target,
        "requested with `read` access; an initializer calls an operation",
    );

    let mut missing_source = photos();
    missing_source.revisioned_stores[0].source = Some("ingest".into());
    refuse(missing_source, "`ingest` is not an implemented operation");

    let mut unknown_mode = photos();
    unknown_mode.initialization[0].mode = RunMode::Unknown("hourly".into());
    refuse(unknown_mode, "no `hourly` run mode");
}

#[test]
fn initializer_keys_are_present_and_unique() {
    let mut empty_key = photos();
    empty_key.initialization[0].key = String::new();
    refuse(empty_key, "a key is not empty");

    let mut repeated = photos();
    let mut again = repeated.initialization[0].clone();
    again.mode = RunMode::EveryStart;
    repeated.initialization.push(again);
    refuse(repeated, "`import` is declared twice");

    // The key is a required field of the description, so a datum without one never parses.
    let mut value = serde_json::to_value(photos()).unwrap();
    value["initialization"][0]
        .as_object_mut()
        .unwrap()
        .remove("key");
    let error = serde_json::from_value::<Nodule>(value).unwrap_err();
    assert!(error.to_string().contains("key"), "{error}");
}

#[test]
fn config_schema_must_be_local_object_and_defaults_must_conform() {
    let mut foreign_schema = photos();
    foreign_schema.config.as_mut().unwrap().schema = "org.example.other.settings".into();
    refuse(
        foreign_schema.clone(),
        "config.schema`: `org.example.other.settings` is outside this cartridge's authority and is neither implemented nor requested",
    );
    // Requesting it makes it another nodule's schema, which this nodule's settings cannot use.
    foreign_schema
        .requires
        .push(data_request("other", "org.example.other.settings"));
    refuse(
        foreign_schema,
        "is not a Lexicon under this cartridge's authority",
    );

    let mut record_schema = photos();
    record_schema.config.as_mut().unwrap().schema = "org.example.photos.photo".into();
    refuse(record_schema, "must be an object schema");

    let mut bad_property_schema = photos();
    bad_property_schema.lexicons[5] = file(json!({
        "lexicon": 1,
        "id": "org.example.photos.settings",
        "defs": {
            "main": {
                "type": "object",
                "properties": { "thumbnails": { "type": "ref", "ref": "#missing" } }
            }
        }
    }));
    refuse(bad_property_schema, "declares no definition `missing`");

    let mut wrong_default_type = photos();
    wrong_default_type.config.as_mut().unwrap().defaults = Some(
        [("thumbnails".to_owned(), json!("yes"))]
            .into_iter()
            .collect(),
    );
    refuse(wrong_default_type, "config.defaults");

    let mut unknown_default_prop = photos();
    unknown_default_prop.config.as_mut().unwrap().defaults = Some(
        [
            ("thumbnails".to_owned(), json!(true)),
            ("undeclared".to_owned(), json!(1)),
        ]
        .into_iter()
        .collect(),
    );
    refuse(unknown_default_prop, "unknown property `undeclared`");

    let mut missing_required_default = photos();
    missing_required_default.lexicons[5] = file(json!({
        "lexicon": 1,
        "id": "org.example.photos.settings",
        "defs": {
            "main": {
                "type": "object",
                "required": ["thumbnails", "maxCount"],
                "properties": {
                    "thumbnails": { "type": "boolean" },
                    "maxCount": { "type": "integer", "minimum": 1 }
                }
            }
        }
    }));
    refuse(
        missing_required_default,
        "missing required property `maxCount`",
    );
}

#[test]
fn requests_are_validated_and_an_unknown_necessity_is_required() {
    let mut duplicate = photos();
    duplicate.requires = vec![
        Request::new("tags", "org.example.tags.apply"),
        Request::new("tags", "org.example.tags.list"),
    ];
    refuse(
        duplicate,
        "requires[1].binding`: binding `tags` is declared twice",
    );

    let mut bad_nsid = photos();
    bad_nsid.requires = vec![Request::new("tags", "tags")];
    refuse(bad_nsid, "requires[0].contract`: `tags` is not an NSID");

    // An unknown necessity is carried through the wire and counts as required.
    let value = json!({
        "requires": [
            { "binding": "tags", "contract": "org.example.tags.apply", "necessity": "someday" },
            { "binding": "log", "contract": "org.example.log.write", "necessity": "optional", "resolution": "atRuntime", "minRevision": 3 },
        ],
    });
    let nodule: Nodule = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        nodule.requires[0].necessity,
        Necessity::Unknown("someday".into())
    );
    assert_eq!(serde_json::to_value(&nodule).unwrap(), value);
    nodule.admit(Some(PHOTOS)).unwrap();
    let none = DispatchScope::default();
    let unmet: Vec<_> = nodule
        .unmet_requests(&none)
        .map(|request| request.binding.as_str())
        .collect();
    assert_eq!(unmet, ["tags"], "the optional request needs no grant");
}

#[test]
fn a_lexicon_revision_is_read_from_the_file_defining_the_contract_and_must_start_at_one() {
    let mut nodule = photos();
    assert_eq!(nodule.lexicon_revision("org.example.photos.list"), None);
    nodule.lexicons[1].insert("revision".into(), json!(3));
    nodule.admit(Some(PHOTOS)).unwrap();
    assert_eq!(nodule.lexicon_revision("org.example.photos.list"), Some(3));
    assert_eq!(nodule.lexicon_revision("org.example.photos.photo"), None);
    assert_eq!(nodule.lexicon_revision("org.example.absent"), None);
    for bad in [
        json!(0),
        json!("2"),
        json!(1.5),
        json!(u64::from(u32::MAX) + 1),
    ] {
        let mut nodule = photos();
        nodule.lexicons[1].insert("revision".into(), bad);
        refuse(
            nodule,
            "lexicons[1].revision`: a Lexicon revision is an integer",
        );
    }
}

#[test]
fn grants_never_exceed_requests() {
    let nodule = Nodule {
        requires: vec![
            Request::new("apply", "org.example.tags.apply"),
            Request::new("list", "org.example.tags.list"),
            data_request("docs", "org.example.docs.note"),
        ],
        ..Default::default()
    };
    let grants = DispatchScope {
        operations: ["org.example.tags.apply", "org.example.mail.send"]
            .map(String::from)
            .into(),
        authorities: ["org.example.tags", "org.example.search"]
            .map(String::from)
            .into(),
    };
    let (applied, unrequested) = nodule.bind_operations(&grants);
    // The `org.example.tags` prefix is applied as the two requested operations it covers, not as
    // the prefix; a data request is never an operation.
    assert_eq!(
        applied.operations,
        ["org.example.tags.apply", "org.example.tags.list"]
            .map(String::from)
            .into()
    );
    assert!(applied.authorities.is_empty());
    assert_eq!(
        unrequested.operations,
        ["org.example.mail.send".to_owned()].into()
    );
    assert_eq!(
        unrequested.authorities,
        ["org.example.search".to_owned()].into()
    );
    assert_eq!(nodule.unmet_requests(&applied).count(), 0);
    let only_apply = DispatchScope {
        operations: ["org.example.tags.apply".to_owned()].into(),
        authorities: Default::default(),
    };
    let unmet: Vec<_> = nodule
        .unmet_requests(&only_apply)
        .map(|request| request.contract.as_str())
        .collect();
    assert_eq!(unmet, ["org.example.tags.list"]);
}

#[test]
fn strictness_refuses_every_unrequested_foreign_nsid_with_its_path() {
    // A reference from an operation's own schema.
    let mut schema_ref = photos();
    schema_ref.lexicons[1]["defs"]["page"]["properties"]["photos"]["items"]["ref"] =
        json!("org.example.albums.album");
    refuse(
        schema_ref.clone(),
        "lexicons[1].defs.page.properties.photos.items.ref`: `org.example.albums.album` is outside this cartridge's authority and is neither implemented nor requested",
    );
    schema_ref
        .requires
        .push(data_request("albums", "org.example.albums.album"));
    schema_ref.admit(Some(PHOTOS)).unwrap();

    // Implementing a foreign operation satisfies a reference to it; dropping that provider
    // leaves the `present` query's output ref unsatisfied, at its exact path.
    let mut unimplemented = photos();
    unimplemented
        .implements
        .retain(|nsid| nsid != "org.example.search.query");
    refuse(
        unimplemented,
        "lexicons[4].defs.main.output.schema.ref`: `org.example.search.query` is outside this cartridge's authority and is neither implemented nor requested",
    );
}

mod admission_tests {
    use super::*;
    use crate::config::Admission;
    use crate::config::AdmissionCache;
    use crate::config::NoduleConfig;
    use crate::config::NoduleInstall;
    use crate::config::NoduleResolution;
    use crate::config::install_key;
    use crate::inventory::ArtifactState;
    use crate::inventory::InstalledNodule;
    use crate::inventory::InventoryError;
    use big_cabinet_core::DocId;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DatumStore;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::DatumsRead;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cartridges_core::Artifact;
    use big_cartridges_core::Cartridge;
    use big_cartridges_core::CartridgeId;
    use big_cartridges_core::Contribution;
    use big_cartridges_core::Digest;
    use big_cartridges_core::NoduleContribution;
    use big_cartridges_core::example_cartridge;
    use big_dispatch_core::Readiness;
    use std::sync::Arc;

    /// An envelope holding the photo browser's authority; a test that holds another overwrites
    /// `authority`.
    fn cartridge_with(id: &str, version: &str, nodule: Nodule) -> Cartridge {
        let mut cartridge = Cartridge::new(id.parse().unwrap(), version.parse().unwrap());
        cartridge.authority = Some(PHOTOS.into());
        cartridge.artifacts.push(Artifact {
            key: "handler".into(),
            digest: Digest::of(b"wasm-bytes"),
            size: Some(10),
            media_type: Some("application/wasm".into()),
            extra: Default::default(),
        });
        let description: IndexMap<String, Value> =
            serde_json::from_value(serde_json::to_value(nodule).unwrap()).unwrap();
        cartridge
            .contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: Nodule::ID.into(),
                description,
                extra: Default::default(),
            }));
        cartridge
    }

    fn settings(thumbnails: Value) -> IndexMap<String, Value> {
        [("thumbnails".to_owned(), thumbnails)]
            .into_iter()
            .collect()
    }

    /// The installed nodule `config` currently resolves for `id`, as the runtime would see it.
    fn resolved(
        config: &NoduleConfig,
        cache: &AdmissionCache,
        id: &CartridgeId,
    ) -> InstalledNodule {
        match config.resolve(cache).remove(id) {
            Some(NoduleResolution::Admitted { installed, .. }) => installed,
            other => panic!("expected `{id}` to resolve admitted, got {other:?}"),
        }
    }

    #[test]
    fn admits_cartridge_tracks_readiness_and_configures_settings() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let cartridge = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();

        let installed = config.admit(&cache, &cartridge, None).unwrap();
        assert_eq!(installed.cartridge_id, id);
        assert_eq!(installed.version, "1.0.0".parse().unwrap());
        assert_eq!(installed.revision, cartridge.revision());
        assert!(!installed.enabled);
        assert_eq!(installed.readiness(), Readiness::Disabled);
        assert!(!installed.readiness().is_ready());
        assert_eq!(
            installed.artifacts.get("handler").map(|(_, s)| *s),
            Some(ArtifactState::Unavailable)
        );
        assert_eq!(installed.config, Some(settings(json!(true))));
        assert_eq!(resolved(&config, &cache, &id), installed);

        let enabled = config.set_enabled(&cache, &id, true).unwrap();
        assert!(enabled.enabled);
        assert_eq!(enabled.readiness(), Readiness::ArtifactsUnavailable);
        assert!(!enabled.readiness().is_ready());
        assert!(Readiness::Ready.is_ready());

        let updated = config.configure(&id, settings(json!(false))).unwrap();
        assert_eq!(updated.config, Some(settings(json!(false))));
        assert!(updated.is_configured());

        let bad = config.configure(&id, settings(json!("invalid")));
        assert!(matches!(bad, Err(InventoryError::Nodule(_))));
        assert_eq!(
            resolved(&config, &cache, &id).config,
            Some(settings(json!(false))),
            "a refused configure leaves the accepted settings in place"
        );
        assert!(!Readiness::Refused.is_ready());
        assert!(!Readiness::ExecutorUnavailable.is_ready());

        config.uninstall(&id).unwrap();
        assert!(config.resolve(&cache).is_empty());
    }

    #[test]
    fn a_handlerless_nodule_is_ready_when_enabled() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let mut nodule = Nodule::default();
        nodule.lexicons.push(file(json!({
            "lexicon": 1,
            "id": "org.example.schema.item",
            "defs": {
                "main": {
                    "type": "record",
                    "key": "tid",
                    "record": { "type": "object", "properties": {} }
                }
            }
        })));
        let mut cartridge = cartridge_with(example_cartridge!("schema"), "0.1.0", nodule);
        cartridge.authority = Some("org.example.schema".into());
        let id: CartridgeId = example_cartridge!("schema").parse().unwrap();

        let installed = config.admit(&cache, &cartridge, None).unwrap();
        assert!(installed.artifacts.is_empty());
        assert_eq!(installed.readiness(), Readiness::Disabled);

        let enabled = config.set_enabled(&cache, &id, true).unwrap();
        assert_eq!(enabled.readiness(), Readiness::Ready);
        assert!(matches!(
            config.configure(&id, IndexMap::new()),
            Err(InventoryError::NoConfigSchema(_))
        ));
    }

    #[test]
    fn upgrades_preserve_state_and_validate_configured_settings() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let v1 = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        config.admit(&cache, &v1, None).unwrap();
        config.set_enabled(&cache, &id, true).unwrap();

        // Unconfigured upgrade adopts the new version's defaults and keeps `enabled`.
        let mut nodule_v2 = photos();
        nodule_v2.config.as_mut().unwrap().defaults = Some(settings(json!(false)));
        let v2 = cartridge_with(example_cartridge!("photos"), "1.1.0", nodule_v2);
        let upgraded = config.admit(&cache, &v2, None).unwrap();
        assert!(upgraded.enabled);
        assert_eq!(upgraded.version, "1.1.0".parse().unwrap());
        assert_eq!(upgraded.config, Some(settings(json!(false))));

        // Explicit configuration is preserved across a compatible upgrade and rejected if the
        // new version's schema breaks it.
        config.configure(&id, settings(json!(true))).unwrap();
        let v3 = cartridge_with(example_cartridge!("photos"), "1.2.0", photos());
        let upgraded = config.admit(&cache, &v3, None).unwrap();
        assert_eq!(upgraded.config, Some(settings(json!(true))));

        let mut incompatible = photos();
        incompatible.lexicons[5] = file(json!({
            "lexicon": 1,
            "id": "org.example.photos.settings",
            "defs": {
                "main": {
                    "type": "object",
                    "required": ["thumbnails", "endpoint"],
                    "properties": {
                        "thumbnails": { "type": "boolean" },
                        "endpoint": { "type": "string" }
                    }
                }
            }
        }));
        incompatible.config.as_mut().unwrap().defaults = Some(
            [
                ("thumbnails".to_owned(), json!(true)),
                ("endpoint".to_owned(), json!("https://example.org")),
            ]
            .into_iter()
            .collect(),
        );
        let v4_bad = cartridge_with(example_cartridge!("photos"), "2.0.0", incompatible);
        // The upgrade is admitted even though it invalidates the explicit settings; resolution
        // refuses the nodule until the settings are rewritten (see the wedge test below).
        config.admit(&cache, &v4_bad, None).unwrap();
        assert!(matches!(
            config.resolve(&cache).get(&id),
            Some(NoduleResolution::Refused { version: Some(v), .. }) if *v == "2.0.0".parse().unwrap()
        ));
    }

    #[test]
    fn method_grants_resolve_beside_drawer_grants_and_revoke_to_absence() {
        use big_dispatch_core::DispatchScope;
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let drawer = DocId::random();
        config
            .admit(
                &cache,
                &cartridge_with(example_cartridge!("photos"), "1.0.0", photos()),
                Some(BTreeMap::from([(drawer.clone(), DrawerAccess::Read)])),
            )
            .unwrap();
        let scope = DispatchScope {
            operations: ["org.example.search.query".to_owned()].into(),
            authorities: ["org.example.callee".to_owned()].into(),
        };
        config.set_method_grants(&id, scope.clone()).unwrap();
        let resolved = |config: &NoduleConfig| {
            let Some(NoduleResolution::Admitted {
                desired_grants,
                desired_operations,
                ..
            }) = config.resolve(&cache).remove(&id)
            else {
                panic!("the nodule is admitted");
            };
            (desired_grants, desired_operations)
        };
        assert_eq!(
            resolved(&config),
            (
                BTreeMap::from([(drawer.clone(), DrawerAccess::Read)]),
                scope
            )
        );

        // Rewriting the drawer grants keeps the methods, and emptying the methods keeps the
        // drawer grants; with nothing left the datum is absent.
        config.set_drawer_grants(&id, BTreeMap::new()).unwrap();
        assert_eq!(resolved(&config).1.authorities.len(), 1);
        config
            .set_drawer_grants(&id, BTreeMap::from([(drawer.clone(), DrawerAccess::Write)]))
            .unwrap();
        config
            .set_method_grants(&id, DispatchScope::default())
            .unwrap();
        assert_eq!(
            resolved(&config),
            (
                BTreeMap::from([(drawer, DrawerAccess::Write)]),
                DispatchScope::default()
            )
        );
        config.set_drawer_grants(&id, BTreeMap::new()).unwrap();
        assert!(config.entry(&id).unwrap().grants.is_absent());
    }

    #[test]
    fn an_upgrade_that_invalidates_settings_is_admitted_refused_and_repaired_by_configure() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let drawer = DocId::random();
        config
            .admit(
                &cache,
                &cartridge_with(example_cartridge!("photos"), "1.0.0", photos()),
                Some(BTreeMap::from([(drawer.clone(), DrawerAccess::Read)])),
            )
            .unwrap();
        config.set_enabled(&cache, &id, true).unwrap();
        config.configure(&id, settings(json!(true))).unwrap();

        // v2 requires a string `endpoint` the stored settings lack.
        let mut v2_nodule = photos();
        v2_nodule.lexicons[5] = file(json!({
            "lexicon": 1,
            "id": "org.example.photos.settings",
            "defs": {
                "main": {
                    "type": "object",
                    "required": ["thumbnails", "endpoint"],
                    "properties": {
                        "thumbnails": { "type": "boolean" },
                        "endpoint": { "type": "string" }
                    }
                }
            }
        }));
        v2_nodule.config.as_mut().unwrap().defaults = Some(
            [
                ("thumbnails".to_owned(), json!(true)),
                ("endpoint".to_owned(), json!("https://example.org")),
            ]
            .into_iter()
            .collect(),
        );
        let before = config.clone();
        config
            .admit(
                &cache,
                &cartridge_with(example_cartridge!("photos"), "2.0.0", v2_nodule.clone()),
                None,
            )
            .unwrap();

        let Some(NoduleResolution::Refused {
            version,
            enabled,
            config: kept_settings,
            reason,
            ..
        }) = config.resolve(&cache).remove(&id)
        else {
            panic!("settings that no longer conform refuse the nodule");
        };
        assert_eq!(version, Some("2.0.0".parse().unwrap()));
        assert!(enabled);
        assert_eq!(kept_settings, Some(settings(json!(true))));
        assert!(reason.contains("endpoint"), "{reason}");

        // The stored settings and grants datums are not rewritten by the upgrade.
        let patch = NoduleConfig::diff(&before, &config).unwrap();
        assert_eq!(patch.set.keys().collect::<Vec<_>>(), [&install_key(&id)]);
        assert!(patch.remove.is_empty());

        // Enabling and grants still work on the refused nodule, and `configure` validates against
        // the newly installed revision: a bad rewrite is refused, a conforming one repairs it.
        config.set_enabled(&cache, &id, false).unwrap();
        config.set_enabled(&cache, &id, true).unwrap();
        assert!(matches!(
            config.configure(&id, settings(json!(true))),
            Err(InventoryError::Nodule(_))
        ));
        let mut repaired = settings(json!(true));
        repaired.insert("endpoint".into(), json!("https://example.org"));
        config.configure(&id, repaired.clone()).unwrap();
        let Some(NoduleResolution::Admitted {
            installed,
            desired_grants,
            ..
        }) = config.resolve(&cache).remove(&id)
        else {
            panic!("rewritten settings repair the nodule");
        };
        assert_eq!(installed.config, Some(repaired));
        assert_eq!(
            desired_grants,
            BTreeMap::from([(drawer, DrawerAccess::Read)]),
            "desired grants survive the refusal"
        );

        // An upgrade to a revision with no config schema at all is refused the same way. Settings
        // can then no longer be rewritten, which `configure` reports as `NoConfigSchema`.
        let schemaless = cartridge_with(example_cartridge!("photos"), "3.0.0", {
            let mut nodule = photos();
            nodule.config = None;
            nodule.lexicons.truncate(5);
            nodule
        });
        config.admit(&cache, &schemaless, None).unwrap();
        let Some(NoduleResolution::Refused { reason, .. }) = config.resolve(&cache).remove(&id)
        else {
            panic!("settings without a schema refuse the nodule");
        };
        assert!(reason.contains("config schema"), "{reason}");
        assert!(matches!(
            config.configure(&id, settings(json!(true))),
            Err(InventoryError::NoConfigSchema(_))
        ));
    }

    #[test]
    fn unchanged_envelopes_are_admitted_once_and_changed_ones_again() {
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let v1 = NoduleInstall::from_cartridge(
            &cartridge_with(example_cartridge!("photos"), "1.0.0", photos()),
            false,
        );
        let first = cache.admission(&id, &v1);
        assert!(matches!(&*first, Admission::Admitted { .. }));
        assert!(
            Arc::ptr_eq(&first, &cache.admission(&id, &v1)),
            "the same envelope reuses the stored outcome"
        );

        // Toggling `enabled` leaves the envelope alone; a new revision does not.
        let mut enabled = v1.clone();
        enabled.enabled = true;
        assert!(Arc::ptr_eq(&first, &cache.admission(&id, &enabled)));
        let v2 = NoduleInstall::from_cartridge(
            &cartridge_with(example_cartridge!("photos"), "1.1.0", photos()),
            false,
        );
        assert!(!Arc::ptr_eq(&first, &cache.admission(&id, &v2)));

        // A tampered envelope under an unchanged revision pin is not served from the cache and
        // fails the revision check instead.
        let mut tampered = v2.clone();
        tampered.envelope.insert("version".into(), json!("9.9.9"));
        let Admission::Refused { reason, .. } = &*cache.admission(&id, &tampered) else {
            panic!("a tampered envelope is refused");
        };
        assert!(reason.contains("revision mismatch"), "{reason}");
    }

    #[test]
    fn authority_conflicts_cover_equal_ancestor_and_descendant_prefixes() {
        let cache = AdmissionCache::new();
        // Enabling conflicting nodules is refused by `set_enabled`, so seed the config with
        // enabled install datums the way an import would, then let `resolve` find the conflicts.
        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        for (cartridge_id, authority) in [
            (example_cartridge!("a"), "org.example.a"),
            (example_cartridge!("b"), "org.example.a.b"),
            (example_cartridge!("c"), "org.example.ab"),
            (example_cartridge!("d"), "org.example.d"),
            (example_cartridge!("e"), "org.example.d"),
        ] {
            let mut cartridge = cartridge_with(cartridge_id, "1.0.0", Nodule::default());
            cartridge.authority = Some(authority.into());
            read.datums.insert(
                install_key(&cartridge.id),
                DatumRead::Accepted {
                    value: serde_json::to_value(NoduleInstall::from_cartridge(&cartridge, true))
                        .unwrap(),
                    version: DatumVersion::default(),
                    authors: Vec::new(),
                },
            );
        }
        let config = NoduleConfig::hydrate(&read);
        let resolved = config.resolve(&cache);
        let refused = |cartridge_id: &str| {
            matches!(
                resolved.get(&cartridge_id.parse::<CartridgeId>().unwrap()),
                Some(NoduleResolution::Refused { .. })
            )
        };
        // `org.example.a` holds `org.example.a.b` but not the sibling `org.example.ab`.
        assert!(
            refused(example_cartridge!("a"))
                && refused(example_cartridge!("b"))
                && !refused(example_cartridge!("c"))
        );
        assert!(refused(example_cartridge!("d")) && refused(example_cartridge!("e")));
    }

    /// Resolves the enabled `carriers` (cartridge name, the authority it holds, the copy of the shared
    /// Lexicon it carries) and returns the names that were refused.
    fn lexicon_refusals(carriers: &[(&str, Option<&str>, Value)]) -> Vec<String> {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        for (name, authority, copy) in carriers {
            let mut nodule = Nodule::default();
            nodule.lexicons.push(file(copy.clone()));
            if authority.is_none() {
                nodule
                    .requires
                    .push(data_request("thing", "org.example.shared.thing"));
            }
            let mut cartridge =
                cartridge_with(&format!("did:key:z6MkTest/{name}"), "1.0.0", nodule);
            cartridge.authority = authority.map(str::to_owned);
            config.admit(&cache, &cartridge, None).unwrap();
            config.set_enabled(&cache, &cartridge.id, true).unwrap();
        }
        config
            .resolve(&cache)
            .into_iter()
            .filter(|(_, resolution)| matches!(resolution, NoduleResolution::Refused { .. }))
            .map(|(cartridge_id, _)| cartridge_id.name.to_string())
            .collect()
    }

    fn shared_thing(revision: Option<u32>, field: &str) -> Value {
        let mut lexicon = json!({
            "lexicon": 1,
            "id": "org.example.shared.thing",
            "defs": { "main": {
                "type": "record",
                "key": "tid",
                "record": { "type": "object", "properties": { field: { "type": "string" } } },
            } },
        });
        if let Some(revision) = revision {
            lexicon["revision"] = revision.into();
        }
        lexicon
    }

    #[test]
    fn lexicon_copies_agree_when_identical_or_ordered_and_a_vendor_never_out_revises_the_holder() {
        const HOLDER: Option<&str> = Some("org.example.shared");
        // Identical copies and an older revision coexist with the holder's.
        assert!(
            lexicon_refusals(&[
                ("holder", HOLDER, shared_thing(Some(2), "a")),
                ("same", None, shared_thing(Some(2), "a")),
                ("older", None, shared_thing(Some(1), "b")),
            ])
            .is_empty()
        );
        // A different body at the holder's revision, a body with no revision to order it by and a
        // higher revision than the holder's are each refused; the holder never is.
        assert_eq!(
            lexicon_refusals(&[
                ("holder", HOLDER, shared_thing(Some(2), "a")),
                ("forked", None, shared_thing(Some(2), "b")),
                ("unordered", None, shared_thing(None, "c")),
                ("ahead", None, shared_thing(Some(3), "d")),
            ]),
            ["ahead", "forked", "unordered"]
        );
        // Without a holder, copies that cannot be ordered are all refused, and one at a distinct
        // revision is not caught up in it.
        assert_eq!(
            lexicon_refusals(&[
                ("p", None, shared_thing(Some(2), "a")),
                ("q", None, shared_thing(Some(2), "b")),
                ("r", None, shared_thing(Some(4), "c")),
            ]),
            ["p", "q"]
        );
    }

    #[test]
    fn refuses_missing_contribution_bad_schema_missing_artifact_and_authority_conflicts() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let empty = Cartridge::new(
            example_cartridge!("empty").parse().unwrap(),
            "0.1.0".parse().unwrap(),
        );
        assert!(matches!(
            config.admit(&cache, &empty, None),
            Err(InventoryError::MissingContribution(_))
        ));

        let mut bad_schema = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_schema.contributions[0] {
            n.schema = "org.example.other.nodule".into();
        }
        assert!(matches!(
            config.admit(&cache, &bad_schema, None),
            Err(InventoryError::UnsupportedSchema { .. })
        ));

        let mut bad_payload = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_payload.contributions[0] {
            n.description.insert("lexicons".into(), json!(123));
        }
        assert!(matches!(
            config.admit(&cache, &bad_payload, None),
            Err(InventoryError::InvalidContribution { .. })
        ));

        let mut no_artifact = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        no_artifact.artifacts.clear();
        assert!(matches!(
            config.admit(&cache, &no_artifact, None),
            Err(InventoryError::MissingArtifact { artifact, .. }) if artifact == "handler"
        ));
        let mut no_size = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        no_size.artifacts[0].size = None;
        assert!(matches!(
            config.admit(&cache, &no_size, None),
            Err(InventoryError::ArtifactSizeRequired { artifact, .. }) if artifact == "handler"
        ));
        assert!(
            config.resolve(&cache).is_empty(),
            "refused admissions install nothing"
        );

        let valid = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        config.admit(&cache, &valid, None).unwrap();
        config.set_enabled(&cache, &valid.id, true).unwrap();

        // Installing a rival holds nothing; enabling it is refused while `photos` holds the prefix.
        let duplicate_authority =
            cartridge_with(example_cartridge!("other-photos"), "1.0.0", photos());
        config.admit(&cache, &duplicate_authority, None).unwrap();
        assert!(matches!(
            config.set_enabled(&cache, &duplicate_authority.id, true),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let mut nested =
            cartridge_with(example_cartridge!("sub-photos"), "1.0.0", Nodule::default());
        nested.authority = Some("org.example.photos.sub".into());
        config.admit(&cache, &nested, None).unwrap();
        assert!(matches!(
            config.set_enabled(&cache, &nested.id, true),
            Err(InventoryError::AuthorityConflict { authority, .. }) if authority == "org.example.photos.sub"
        ));
        let mut sibling = cartridge_with(
            example_cartridge!("photosynthesis"),
            "1.0.0",
            Nodule::default(),
        );
        sibling.authority = Some("org.example.photosynthesis".into());
        config.admit(&cache, &sibling, None).unwrap();
        config.set_enabled(&cache, &sibling.id, true).unwrap();
    }
}

#[cfg(feature = "schemars")]
mod declaration {
    use super::*;
    use big_cabinet_core::datum::Bundle;
    use big_cabinet_core::datum::Declaration;
    use big_cabinet_core::datum::Universe;

    #[test]
    fn the_description_datums_declare_publish_and_admit() {
        let universe = Universe::collect([Declaration::of::<Nodule>().unwrap()]);
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(ids, ["org.example.daybook.nodule"]);
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 1);
        let nodule = &lexicons[0].file;
        let main = &nodule["defs"]["main"];
        assert_eq!(
            main["properties"]["lexicons"]["items"]["type"], "unknown",
            "{nodule}"
        );
        let handler = &nodule["defs"]["Handler"]["properties"];
        assert_eq!(handler["artifact"]["type"], "string", "{nodule}");
        assert_eq!(
            main["properties"]["bindings"]["type"], "unknown",
            "{nodule}"
        );
        let mode = &nodule["defs"]["RevisionedStore"]["properties"]["mode"];
        assert_eq!(mode["knownValues"], json!(["frontier", "log"]), "{nodule}");
        for (index, lexicon) in lexicons.iter().enumerate() {
            let carried: Vec<&Value> = lexicons
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, other)| &other.file)
                .collect();
            bundle
                .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
        }
    }

    #[test]
    fn the_config_datums_declare_publish_and_admit() {
        use crate::config::NoduleGrants;
        use crate::config::NoduleInstall;
        use crate::config::NoduleSettings;
        use crate::config::NoduleSubstitution;
        use crate::config::config_universe;

        let universe = config_universe();
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(
            ids,
            [
                NoduleInstall::ID,
                NoduleSettings::ID,
                NoduleGrants::ID,
                NoduleSubstitution::ID
            ]
        );
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 4);

        let grants_lex = lexicons
            .iter()
            .find(|lex| lex.id == NoduleGrants::ID)
            .unwrap();
        let drawers_prop = &grants_lex.file["defs"]["main"]["properties"]["drawers"];
        assert_eq!(drawers_prop["type"], "unknown", "{}", grants_lex.file);

        for (index, lexicon) in lexicons.iter().enumerate() {
            let carried: Vec<&Value> = lexicons
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, other)| &other.file)
                .collect();
            bundle
                .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
        }
    }
}

mod config_tests {
    use super::*;
    use crate::config::AdmissionCache;
    use crate::config::DatumSlot;
    use crate::config::NoduleConfig;
    use crate::config::NoduleGrants;
    use crate::config::NoduleInstall;
    use crate::config::NoduleResolution;
    use crate::config::NoduleSettings;
    use crate::config::artifact_key;
    use crate::config::grants_key;
    use crate::config::install_key;
    use crate::config::settings_key;
    use big_cabinet_core::DatumKey;
    use big_cabinet_core::DocId;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DatumStore;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::DatumsRead;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::EntryAccess;
    use big_cartridges_core::Artifact;
    use big_cartridges_core::Cartridge;
    use big_cartridges_core::CartridgeId;
    use big_cartridges_core::Contribution;
    use big_cartridges_core::Digest;
    use big_cartridges_core::NoduleContribution;
    use big_cartridges_core::example_cartridge;

    fn sample_cartridge(id: &str, version: &str) -> Cartridge {
        let mut cartridge = Cartridge::new(id.parse().unwrap(), version.parse().unwrap());
        cartridge.authority = Some(PHOTOS.into());
        cartridge.artifacts.push(Artifact {
            key: "handler".into(),
            digest: Digest::of(b"wasm-bytes"),
            size: Some(10),
            media_type: Some("application/wasm".into()),
            extra: Default::default(),
        });
        let description: IndexMap<String, Value> =
            serde_json::from_value(serde_json::to_value(photos()).unwrap()).unwrap();
        cartridge
            .contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: Nodule::ID.into(),
                description,
                extra: Default::default(),
            }));
        cartridge
    }

    #[test]
    fn config_datums_preserve_unknown_fields_and_vocabulary_on_round_trip() {
        let cartridge = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let cartridge_id = cartridge.id.clone();

        let install = NoduleInstall::from_cartridge(&cartridge, true);
        let mut install_json = serde_json::to_value(&install).unwrap();
        install_json["futureInstallField"] = json!("kept");
        let decoded_install: NoduleInstall = serde_json::from_value(install_json.clone()).unwrap();
        assert_eq!(decoded_install.decode_cartridge().unwrap(), cartridge);
        assert!(decoded_install.enabled);
        assert_eq!(
            serde_json::to_value(&decoded_install).unwrap(),
            install_json
        );

        let settings_json = json!({
            "values": { "thumbnails": false },
            "futureSettingsField": 42
        });
        let decoded_settings: NoduleSettings =
            serde_json::from_value(settings_json.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&decoded_settings).unwrap(),
            settings_json
        );

        let drawer_a = DocId::random();
        let drawer_b = DocId::random();
        let grants_json = json!({
            "drawers": {
                drawer_a.to_string(): "Write",
                drawer_b.to_string(): "SuperAdmin"
            },
            "futureGrantsField": true
        });
        let decoded_grants: NoduleGrants = serde_json::from_value(grants_json.clone()).unwrap();
        assert_eq!(
            decoded_grants.drawers.get(&drawer_a),
            Some(&EntryAccess::Write)
        );
        assert_eq!(
            decoded_grants.drawers.get(&drawer_b),
            Some(&EntryAccess::Unknown("SuperAdmin".into()))
        );
        assert_eq!(serde_json::to_value(&decoded_grants).unwrap(), grants_json);

        let ik = install_key(&cartridge_id);
        let sk = settings_key(&cartridge_id);
        let gk = grants_key(&cartridge_id);
        assert!(NoduleConfig::owns(&ik));
        assert!(NoduleConfig::owns(&sk));
        assert!(NoduleConfig::owns(&gk));
    }

    #[test]
    fn datum_store_hydrates_diffs_and_isolates_invalid_or_orphan_entries() {
        let photos_cartridge = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let photos_id = photos_cartridge.id.clone();

        let broken_id: CartridgeId = example_cartridge!("broken").parse().unwrap();
        let orphan_id: CartridgeId = example_cartridge!("orphan").parse().unwrap();
        let drawer = DocId::random();

        let unowned_key = DatumKey::from_text("org.example.daybook.drawer", "main").unwrap();
        let malformed_key =
            DatumKey::from_text(NoduleInstall::ID, "invalid-cartridge-slot").unwrap();

        let mut install_val =
            serde_json::to_value(NoduleInstall::from_cartridge(&photos_cartridge, false)).unwrap();
        install_val["extraInstall"] = json!(1);

        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        read.datums.insert(
            unowned_key.clone(),
            DatumRead::Accepted {
                value: json!({"layout": {"$type": "org.example.daybook.drawer.selfDoc", "empty": true}}),
                version: DatumVersion::default(),
                authors: Vec::new(),
            },
        );
        read.datums.insert(
            malformed_key.clone(),
            DatumRead::Accepted {
                value: json!({"unexpected": true}),
                version: DatumVersion::default(),
                authors: Vec::new(),
            },
        );
        read.datums.insert(
            install_key(&photos_id),
            DatumRead::Accepted {
                value: install_val,
                version: DatumVersion::default(),
                authors: Vec::new(),
            },
        );
        read.datums.insert(
            install_key(&broken_id),
            DatumRead::Accepted {
                value: json!({"envelope": {}, "revision": "bad", "enabled": true}),
                version: DatumVersion::default(),
                authors: Vec::new(),
            },
        );
        read.datums.insert(
            settings_key(&broken_id),
            DatumRead::Pending {
                reason: "awaiting schema".into(),
                version: DatumVersion::default(),
                value: Some(json!({"values": {}})),
            },
        );
        read.datums.insert(
            grants_key(&broken_id),
            DatumRead::Rejected {
                reason: "unauthorized writer".into(),
            },
        );
        read.datums.insert(
            settings_key(&orphan_id),
            DatumRead::Accepted {
                value: json!({"values": {"thumbnails": false}}),
                version: DatumVersion::default(),
                authors: Vec::new(),
            },
        );

        let before = NoduleConfig::hydrate(&read);
        assert!(before.carried_keys().contains_key(&malformed_key));
        assert!(matches!(
            before.entry(&broken_id).unwrap().install,
            DatumSlot::Invalid { .. }
        ));
        assert!(matches!(
            before.entry(&broken_id).unwrap().settings,
            DatumSlot::Invalid { .. }
        ));
        assert!(matches!(
            before.entry(&broken_id).unwrap().grants,
            DatumSlot::Invalid { .. }
        ));
        assert!(before.entry(&orphan_id).unwrap().install.is_absent());

        // Resolve admits `example/photos`, refuses `example/broken`, and ignores `example/orphan`.
        let resolved = before.resolve(&Default::default());
        assert!(matches!(
            resolved.get(&photos_id),
            Some(NoduleResolution::Admitted { .. })
        ));
        assert!(matches!(
            resolved.get(&broken_id),
            Some(NoduleResolution::Refused { .. })
        ));
        assert!(!resolved.contains_key(&orphan_id));

        // Mutating `example/photos` leaves broken, orphan, malformed, and unowned keys untouched.
        let mut after = before.clone();
        after
            .set_enabled(&AdmissionCache::new(), &photos_id, true)
            .unwrap();
        after
            .configure(
                &photos_id,
                [("thumbnails".to_owned(), json!(false))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        after
            .set_drawer_grants(
                &photos_id,
                BTreeMap::from([(drawer.clone(), DrawerAccess::Read)]),
            )
            .unwrap();

        let patch = NoduleConfig::diff(&before, &after).unwrap();
        assert!(patch.remove.is_empty());
        assert_eq!(patch.set.len(), 3);
        assert_eq!(patch.set[&install_key(&photos_id)]["enabled"], json!(true));
        assert_eq!(
            patch.set[&install_key(&photos_id)]["extraInstall"],
            json!(1)
        );
        assert_eq!(
            patch.set[&settings_key(&photos_id)]["values"]["thumbnails"],
            json!(false)
        );
        assert_eq!(
            patch.set[&grants_key(&photos_id)]["drawers"][&drawer.to_string()],
            json!("Read")
        );

        // Uninstalling `example/photos` removes all three datums in one patch.
        let mut uninstalled = after.clone();
        uninstalled.uninstall(&photos_id).unwrap();
        let uninstall_patch = NoduleConfig::diff(&after, &uninstalled).unwrap();
        assert!(uninstall_patch.set.is_empty());
        assert_eq!(
            uninstall_patch.remove,
            BTreeSet::from([
                install_key(&photos_id),
                settings_key(&photos_id),
                grants_key(&photos_id),
            ])
        );

        // Removing a carried malformed key emits it in `patch.remove`.
        let mut cleaned = before.clone();
        assert!(cleaned.remove_carried_key(&malformed_key));
        let clean_patch = NoduleConfig::diff(&before, &cleaned).unwrap();
        assert_eq!(clean_patch.remove, BTreeSet::from([malformed_key]));
    }

    /// The `Blob` datum stored for `cartridge`'s handler artifact, as the document holds it.
    fn stored_artifact(cartridge: &Cartridge) -> Value {
        let artifact = cartridge.artifact("handler").unwrap();
        json!({
            "mime": "application/wasm",
            "lengthOctets": artifact.size.unwrap(),
            "digest": artifact.digest.to_string(),
        })
    }

    fn accepted(value: Value) -> DatumRead {
        DatumRead::Accepted {
            value,
            version: DatumVersion::default(),
            authors: Vec::new(),
        }
    }

    #[test]
    fn handler_artifacts_are_referenced_by_blob_datums_that_follow_the_install() {
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let slot = artifact_key(&id, "handler");
        assert_eq!(
            slot.to_string(),
            concat!(
                "org.example.daybook.blob/nodule.artifact/",
                example_cartridge!("photos"),
                "/handler"
            )
        );
        let foreign = DatumKey::from_text(big_blobs_core::Blob::ID, "avatar").unwrap();
        let malformed =
            DatumKey::from_text(big_blobs_core::Blob::ID, "nodule.artifact/garbage").unwrap();
        assert!(!NoduleConfig::owns(&foreign));
        assert!(NoduleConfig::owns(&malformed));
        assert!(NoduleConfig::owns(&slot));

        // Admitting writes the install and the reference in the same patch, and the reference
        // is the datum wire form big_blobs reads (no `null` for the unset options).
        let v1 = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let cache = AdmissionCache::new();
        let mut config = NoduleConfig::new();
        config.admit(&cache, &v1, None).unwrap();
        let patch = NoduleConfig::diff(&NoduleConfig::new(), &config).unwrap();
        assert_eq!(
            patch.set.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([install_key(&id), slot.clone()])
        );
        assert_eq!(patch.set[&slot], stored_artifact(&v1));

        // The document as stored holds the patch, another service's `Blob`, a malformed
        // reference of ours, and a reference to an artifact no revision names any more.
        let stale = artifact_key(&id, "old");
        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: patch
                .set
                .iter()
                .map(|(key, value)| (key.clone(), accepted(value.clone())))
                .collect(),
        };
        let other_blob = json!({"mime": "image/png", "lengthOctets": 1, "digest": "zQm"});
        read.datums
            .insert(foreign.clone(), accepted(other_blob.clone()));
        read.datums
            .insert(malformed.clone(), accepted(other_blob.clone()));
        read.datums.insert(stale.clone(), accepted(other_blob));
        let stored = NoduleConfig::hydrate(&read);
        assert!(!stored.carried_keys().contains_key(&foreign));
        assert!(stored.carried_keys().contains_key(&malformed));
        assert!(matches!(
            stored.entry(&id).unwrap().artifacts["handler"],
            DatumSlot::Accepted { .. }
        ));

        // Re-admitting the same revision rewrites the install datum's slot only if it changed,
        // and drops the reference no artifact of the revision names.
        let mut again = stored.clone();
        again.admit(&cache, &v1, None).unwrap();
        let patch = NoduleConfig::diff(&stored, &again).unwrap();
        assert!(patch.set.is_empty(), "{:?}", patch.set);
        assert_eq!(patch.remove, BTreeSet::from([stale.clone()]));

        // Upgrading to a revision with another handler swaps the reference in place.
        let mut v2 = sample_cartridge(example_cartridge!("photos"), "1.1.0");
        v2.artifacts[0].digest = Digest::of(b"other-wasm-bytes");
        v2.artifacts[0].size = Some(16);
        let mut upgraded = stored.clone();
        upgraded.admit(&cache, &v2, None).unwrap();
        let patch = NoduleConfig::diff(&stored, &upgraded).unwrap();
        assert_eq!(
            patch.set.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([install_key(&id), slot.clone()])
        );
        assert_eq!(patch.set[&slot], stored_artifact(&v2));
        assert_eq!(patch.remove, BTreeSet::from([stale.clone()]));

        // A reference that no longer decodes is replaced by the admission, which repairs it.
        let mut damaged = read.clone();
        damaged
            .datums
            .insert(slot.clone(), accepted(json!({"mime": 7})));
        let damaged = NoduleConfig::hydrate(&damaged);
        assert!(matches!(
            damaged.entry(&id).unwrap().artifacts["handler"],
            DatumSlot::Invalid { .. }
        ));
        let mut repaired = damaged.clone();
        repaired.admit(&cache, &v1, None).unwrap();
        let patch = NoduleConfig::diff(&damaged, &repaired).unwrap();
        assert_eq!(patch.set[&slot], stored_artifact(&v1));

        // Uninstalling removes the references with the install, and only ours.
        let mut gone = stored.clone();
        gone.uninstall(&id).unwrap();
        let patch = NoduleConfig::diff(&stored, &gone).unwrap();
        assert_eq!(
            patch.remove,
            BTreeSet::from([install_key(&id), slot, stale])
        );
        assert!(!patch.remove.contains(&foreign));
    }
}
