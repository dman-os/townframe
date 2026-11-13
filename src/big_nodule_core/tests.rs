use super::*;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Cartridge;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::request::Necessity;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
struct Nodule {
    lexicons: Vec<IndexMap<String, Value>>,
    sidecar: IndexMap<String, Value>,
    requires: Vec<Request>,
    handler: Option<Handler>,
    implements: Vec<String>,
    revisioned_stores: Vec<RevisionedStore>,
    initialization: Vec<Initializer>,
    private_state: Vec<PrivateState>,
    settings: Vec<String>,
    bindings: IndexMap<String, Value>,
    #[serde(flatten)]
    extra: big_cabinet_core::datum::Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Handler {
    artifact: String,
    #[serde(flatten)]
    extra: big_cabinet_core::datum::Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RevisionedStore {
    key: String,
    selector: String,
    entry: String,
    mode: StoreMode,
    source: Option<String>,
    #[serde(flatten)]
    extra: big_cabinet_core::datum::Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Initializer {
    key: String,
    target: String,
    mode: RunMode,
    #[serde(flatten)]
    extra: big_cabinet_core::datum::Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrivateState {
    key: String,
    kind: StateKind,
    #[serde(flatten)]
    extra: big_cabinet_core::datum::Extra,
}

impl Nodule {
    fn resolved(&self, authority: Option<&str>) -> ResolvedCartridge {
        use big_cabinet_core::reference::DatumPin;
        use big_cabinet_core::reference::Location;
        use big_cartridges_core::Component;
        use big_cartridges_core::ComponentExport;
        use big_cartridges_core::LocalState;
        use big_cartridges_core::PermbindDatum;
        use big_cartridges_core::PermbindMethod;
        use big_cartridges_core::RevisionedStore as ManifestStore;
        use big_cartridges_core::Routine;
        use big_cartridges_core::RoutineImpl;
        use big_cartridges_core::RoutineUse;
        let mut cartridge = Cartridge::new(
            "did:plc:example/photos".parse().unwrap(),
            "1.0.0".parse().unwrap(),
        );
        cartridge.authority = authority.map(str::to_owned);
        let mut lexicons = BTreeMap::new();
        for file in &self.lexicons {
            let id = file["id"].as_str().unwrap().parse().unwrap();
            lexicons.insert(id, file.clone());
        }
        let mut components = BTreeMap::new();
        if let Some(handler) = &self.handler
            && !handler.artifact.is_empty()
        {
            components.insert(
                handler.artifact.clone(),
                Component::Wasm(big_cartridges_core::WasmComponent {
                    bytes: DatumPin::new(
                        Location::parse("dtxt:datum:self/org.example.daybook/test").unwrap(),
                    ),
                    digest: big_cartridges_core::Digest::of(b"wasm"),
                    size: 4,
                    media_type: "application/wasm".into(),
                    extra: Default::default(),
                }),
            );
        }
        for request in &self.requires {
            let nsid = legacy_nsid(&request.contract);
            let permbind = if request.access.is_some() {
                Permbind::Datum(PermbindDatum {
                    nsid,
                    description: request.binding.clone(),
                    read: request
                        .access
                        .as_deref()
                        .is_some_and(|access| access == "read")
                        .then_some(request.necessity.clone()),
                    write: request
                        .access
                        .as_deref()
                        .is_some_and(|access| access == "write")
                        .then_some(request.necessity.clone()),
                    create: request
                        .access
                        .as_deref()
                        .is_some_and(|access| access == "create")
                        .then_some(request.necessity.clone()),
                    delete: request
                        .access
                        .as_deref()
                        .is_some_and(|access| access == "delete")
                        .then_some(request.necessity.clone()),
                    extra: Default::default(),
                })
            } else {
                Permbind::Method(PermbindMethod {
                    nsid,
                    description: request.binding.clone(),
                    necessity: request.necessity.clone(),
                    min_revision: request.min_revision,
                    extra: Default::default(),
                })
            };
            cartridge
                .permbinds
                .insert(request.binding.clone(), permbind);
        }
        let uses = cartridge
            .permbinds
            .iter()
            .map(|(key, permbind)| {
                let use_value = match permbind {
                    Permbind::Datum(value) => RoutineUse {
                        read: value.read.is_some(),
                        write: value.write.is_some(),
                        create: value.create.is_some(),
                        delete: value.delete.is_some(),
                        extra: Default::default(),
                    },
                    Permbind::Drawers(value) => RoutineUse {
                        read: value.read.is_some(),
                        write: value.write.is_some(),
                        create: value.create.is_some(),
                        delete: value.delete.is_some(),
                        extra: Default::default(),
                    },
                    _ => RoutineUse::default(),
                };
                (key.clone(), use_value)
            })
            .collect::<BTreeMap<_, _>>();
        let mut routines = BTreeMap::new();
        for operation in &self.implements {
            routines.insert(
                operation.clone(),
                Routine {
                    r#impl: RoutineImpl::ComponentExport(ComponentExport {
                        component: self
                            .handler
                            .as_ref()
                            .map_or_else(|| "handler".into(), |handler| handler.artifact.clone()),
                        export: operation.rsplit('.').next().unwrap_or(operation).into(),
                        extra: Default::default(),
                    }),
                    implements: legacy_nsid(operation),
                    uses: uses.clone(),
                    extra: Default::default(),
                },
            );
        }
        let manifest = big_cartridges_core::Nodule {
            routines,
            revisioned_stores: self
                .revisioned_stores
                .iter()
                .map(|store| {
                    (
                        store.key.clone(),
                        ManifestStore {
                            selector: legacy_nsid(&store.selector),
                            entry: store.entry.parse().unwrap(),
                            mode: store.mode.clone(),
                            source: store.source.as_ref().map(|source| source.parse().unwrap()),
                            extra: Default::default(),
                        },
                    )
                })
                .collect(),
            local_state: self
                .private_state
                .iter()
                .map(|state| {
                    (
                        state.key.clone(),
                        LocalState {
                            kind: state.kind.clone(),
                            extra: Default::default(),
                        },
                    )
                })
                .collect(),
            initialization: self
                .initialization
                .iter()
                .map(|init| {
                    (
                        init.key.clone(),
                        big_cartridges_core::Initializer {
                            target: init.target.parse().unwrap(),
                            mode: init.mode.clone(),
                            extra: Default::default(),
                        },
                    )
                })
                .collect(),
            bindings: self.bindings.clone(),
            extra: Default::default(),
        };
        cartridge.settings = self
            .settings
            .iter()
            .map(|setting| setting.parse().unwrap())
            .collect();
        cartridge.nodule = Some(manifest);
        ResolvedCartridge::new(cartridge, lexicons, BTreeMap::new(), components)
    }

    fn bind_operations(&self, grants: &DispatchScope) -> (DispatchScope, DispatchScope) {
        self.resolved(None).bind_operations(grants)
    }
    fn admit(&self, authority: Option<&str>) -> Result<Admitted> {
        for (index, operation) in self.implements.iter().enumerate() {
            if operation.parse::<big_cabinet_core::datum::Nsid>().is_err() {
                return Err(NoduleError::invalid(
                    format!("implements[{index}].nsid"),
                    format!("`{operation}` is not an NSID"),
                ));
            }
        }
        for (index, file) in self.lexicons.iter().enumerate() {
            let id = file["id"].as_str().unwrap_or_default();
            if id.parse::<big_cabinet_core::datum::Nsid>().is_err() {
                return Err(NoduleError::invalid(
                    format!("lexicons[{index}].id"),
                    format!("`{id}` is not an NSID"),
                ));
            }
        }
        for (index, request) in self.requires.iter().enumerate() {
            if request
                .contract
                .parse::<big_cabinet_core::datum::Nsid>()
                .is_err()
            {
                return Err(NoduleError::invalid(
                    format!("requires[{index}].contract"),
                    format!("`{}` is not an NSID", request.contract),
                ));
            }
        }
        self.resolved(authority).admit(authority)
    }

    fn registrable_datums(&self, authority: Option<&str>) -> RegistrableDatums<'static> {
        let resolved: &'static ResolvedCartridge = Box::leak(Box::new(self.resolved(authority)));
        resolved.registrable_datums(authority)
    }

    fn datum_lexicons(&self) -> impl Iterator<Item = &IndexMap<String, Value>> {
        self.lexicons
            .iter()
            .filter(|file| operation_kind(file).is_none())
    }

    fn lexicon_revision(&self, nsid: &str) -> Option<u32> {
        self.lexicons
            .iter()
            .find(|file| file["id"] == nsid)
            .and_then(|file| file.get("revision"))
            .and_then(Value::as_u64)
            .map(|revision| revision as u32)
    }

    fn unmet_requests(&self, granted: &DispatchScope) -> std::vec::IntoIter<Request> {
        self.requires
            .iter()
            .filter(|request| {
                request.is_operation_call()
                    && request.necessity.is_required()
                    && !granted.permits(&request.contract)
            })
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }
}
/// The NSID authority the photo browser's envelope holds.
const PHOTOS: &str = "org.example.photos";

fn legacy_nsid(value: &str) -> big_cabinet_core::datum::Nsid {
    value.split('#').next().unwrap().parse().unwrap()
}
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
        ["org.example.photos.object", "org.example.photos.record"]
    );
    let carried: Vec<&str> = split
        .carried
        .iter()
        .map(|file| file["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        carried,
        ["org.example.other.object", "org.example.photos.library"]
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
    nodule.settings = vec!["org.example.photos.settings".into()];
    nodule
}

fn refuse(nodule: Nodule, needle: &str) {
    refuse_under(nodule, Some(PHOTOS), needle);
}

fn refuse_under(nodule: Nodule, authority: Option<&str>, needle: &str) {
    let error = nodule.admit(authority).unwrap_err().to_string();
    let alternate = needle.replace("`:", ":");
    let reason = needle.rsplit("`: ").next().unwrap_or(needle);
    assert!(
        error.contains(needle) || error.contains(&alternate) || error.contains(reason),
        "{error}; expected {needle}"
    );
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
    let encoded = serde_json::to_value(&nodule).unwrap();
    assert_eq!(encoded["futureField"], value["futureField"]);
    assert_eq!(encoded["privateState"], value["privateState"]);
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
    let encoded = serde_json::to_value(&nodule).unwrap();
    assert_eq!(encoded["bindings"], value["bindings"]);
    assert_eq!(encoded["commands"], value["commands"]);

    // Pre-reshape top-level binding fields are no longer typed: they are carried as unknown
    // members, not interpreted.
    let legacy = json!({ "commands": [{ "key": "x" }] });
    let nodule: Nodule = serde_json::from_value(legacy.clone()).unwrap();
    nodule.admit(Some(PHOTOS)).unwrap();
    let encoded = serde_json::to_value(&nodule).unwrap();
    assert_eq!(encoded["commands"], legacy["commands"]);
}

#[test]
fn authority_classifies_held_and_foreign_lexicon_files() {
    let mut duplicate = photos();
    duplicate.lexicons.push(duplicate.lexicons[0].clone());
    duplicate.admit(Some(PHOTOS)).unwrap();

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
    refuse_under(photos(), None, "outside this cartridge's authority");
    // Whole segments only: `org.example.photosynthesis` is not under `org.example.photos`.
    refuse_under(
        photos(),
        Some("org.example.phot"),
        "outside this cartridge's authority",
    );

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
    missing_def.revisioned_stores[0].selector = "org.example.photos.album".into();
    refuse(
        missing_def,
        "is under this cartridge's authority but not declared",
    );

    let mut operation_as_schema = photos();
    operation_as_schema.settings = vec!["org.example.photos.list".into()];
    refuse(operation_as_schema, "is not a datum");

    // Revisioned store selectors are typed NSIDs in the resolved manifest; fragment selectors are no longer accepted.
    let mut local_ref_in_contribution = photos();
    local_ref_in_contribution.revisioned_stores[0].selector = "org.example.photos.photo".into();
    local_ref_in_contribution.admit(Some(PHOTOS)).unwrap();
    let mut bad_operation_ref = photos();
    bad_operation_ref.lexicons[1]["defs"]["page"]["properties"]["photos"]["items"]["ref"] =
        json!("org.example.photos.photo#missing");
    refuse(
        bad_operation_ref,
        "lexicons[2].defs.page.properties.photos.items.ref",
    );

    // A foreign Lexicon is left to its provider, but only once the nodule has asked for it.
    let mut foreign = photos();
    foreign.revisioned_stores[0].entry = "org.example.elsewhere.defs".into();
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
    twice.admit(Some(PHOTOS)).unwrap();

    let mut undeclared = photos();
    undeclared.implements[0] = "org.example.photos.missing".into();
    refuse(
        undeclared,
        "`org.example.photos.missing` is under this cartridge's authority but is not declared",
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
    foreign_twice.admit(Some(PHOTOS)).unwrap();
    let mut foreign = photos();
    foreign.implements.push("org.example.tags.apply".into());
    assert_eq!(
        foreign.admit(Some(PHOTOS)).unwrap().operations["org.example.tags.apply"],
        None
    );

    let mut no_handler = photos();
    no_handler.handler = None;
    refuse(no_handler, "not a resolved component");

    let mut no_artifact = photos();
    no_artifact.handler.as_mut().unwrap().artifact = String::new();
    refuse(no_artifact, "not a resolved component");

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
    refuse(data_target, "neither implemented nor requested");

    let mut missing_source = photos();
    missing_source.revisioned_stores[0].source = Some("org.example.photos.ingest".into());
    refuse(
        missing_source,
        "`org.example.photos.ingest` is not an implemented operation",
    );

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
    repeated.admit(Some(PHOTOS)).unwrap();

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
fn settings_name_datums_this_nodule_holds_once_each() {
    // A settings NSID under another authority is that cartridge's datum, not this nodule's.
    let mut foreign = photos();
    foreign.settings = vec!["org.example.other.settings".into()];
    refuse(
        foreign,
        "settings[0]`: `org.example.other.settings` is not a Lexicon under this cartridge's authority",
    );

    // Declared in this authority is not enough: it must be declared.
    let mut undeclared = photos();
    undeclared.settings = vec!["org.example.photos.nowhere".into()];
    refuse(
        undeclared,
        "is not a Lexicon under this cartridge's authority",
    );

    let mut twice = photos();
    twice.settings = vec![
        "org.example.photos.settings".into(),
        "org.example.photos.settings".into(),
    ];
    twice.admit(Some(PHOTOS)).unwrap();

    // A type library has no `main`, so there is no datum to write.
    let mut library = photos();
    library.lexicons.push(file(json!({
        "lexicon": 1,
        "id": "org.example.photos.shapes",
        "defs": { "point": { "type": "object", "properties": {} } }
    })));
    library.settings = vec!["org.example.photos.shapes".into()];
    refuse(library, "is not a datum");

    // A record is a datum too, and a nodule may declare several.
    let mut several = photos();
    several.settings = vec![
        "org.example.photos.settings".into(),
        "org.example.photos.photo".into(),
    ];
    several.admit(Some(PHOTOS)).unwrap();
}

#[test]
fn requests_are_validated_and_an_unknown_necessity_is_required() {
    let mut duplicate = photos();
    duplicate.requires = vec![
        Request::new("tags", "org.example.tags.apply"),
        Request::new("tags", "org.example.tags.list"),
    ];
    duplicate.admit(Some(PHOTOS)).unwrap();

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
    let encoded = serde_json::to_value(&nodule).unwrap();
    assert_eq!(encoded["requires"][0]["necessity"], "someday");
    assert_eq!(encoded["requires"][1]["resolution"], "atRuntime");
    nodule.admit(Some(PHOTOS)).unwrap();
    let none = DispatchScope::default();
    let unmet: Vec<_> = nodule
        .unmet_requests(&none)
        .map(|request| request.binding)
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
            "lexicons[2].revision`: a Lexicon revision is an integer",
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
        privileged: false,
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
        privileged: false,
    };
    let unmet: Vec<_> = nodule
        .unmet_requests(&only_apply)
        .map(|request| request.contract)
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
        "lexicons[2].defs.page.properties.photos.items.ref`: `org.example.albums.album` is outside this cartridge's authority and is neither implemented nor requested",
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
    use crate::config::CartridgeConfig;
    use crate::config::NoduleConfig;
    use crate::config::NoduleResolution;
    use crate::inventory::ArtifactState;
    use crate::inventory::InstalledNodule;
    use crate::inventory::InventoryError;
    use big_cabinet_core::DocId;

    use big_cabinet_core::doc::ChangeHash;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DatumStore;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::DatumsRead;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::EntryAccess;
    use big_cartridges_core::Cartridge;
    use big_cartridges_core::CartridgeId;

    use big_cartridges_core::MicroContribution;

    use big_cartridges_core::config::CartridgeInstall;
    use big_cartridges_core::config::Component;
    use big_cartridges_core::config::ComponentInstall;
    use big_cartridges_core::config::GrantBody;
    use big_cartridges_core::config::GrantKey;
    use big_cartridges_core::config::install_key;
    use big_cartridges_core::example_cartridge;
    use big_cartridges_core::registry::version_pin;
    use big_dispatch_core::DispatchScope;
    use big_dispatch_core::Readiness;
    use std::sync::Arc;

    /// An envelope holding the photo browser's authority; a test that holds another overwrites
    /// `authority`.
    fn cartridge_with(id: &str, version: &str, nodule: Nodule) -> Cartridge {
        let resolved = nodule.resolved(Some(PHOTOS));
        let mut cartridge = resolved.cartridge;
        cartridge.id = id.parse().unwrap();
        cartridge.version = version.parse().unwrap();
        cartridge.authority = Some(PHOTOS.into());
        cartridge.extra.insert(
            "_resolvedLexicons".into(),
            serde_json::to_value(resolved.lexicons).unwrap(),
        );
        cartridge.extra.insert(
            "_resolvedComponents".into(),
            serde_json::to_value(resolved.components).unwrap(),
        );
        cartridge
    }

    /// The pin of `cartridge` in a registry document of its own, at heads derived from its
    /// revision, so a new revision is a new pin.
    fn pin_of(cartridge: &Cartridge) -> big_cabinet_core::DatumPin {
        version_pin(
            DocId::random(),
            &cartridge.version,
            [ChangeHash(cartridge.revision().0)],
        )
    }

    /// What the state actor does for an install: the node's config document records the pin of
    /// the version it read, and enters its admission in the cache.
    pub(super) fn install(
        config: &mut NoduleConfig,
        cache: &AdmissionCache,
        cartridge: &Cartridge,
    ) -> std::result::Result<(), InventoryError> {
        config.install(cache, cartridge, pin_of(cartridge), DocId::random())
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
    fn installs_track_readiness_and_enablement() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let cartridge = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();

        install(&mut config, &cache, &cartridge).unwrap();
        let installed = resolved(&config, &cache, &id);
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

        config
            .set_enabled(&cache, &id, Component::Nodule, true)
            .unwrap();
        let enabled = resolved(&config, &cache, &id);
        assert!(enabled.enabled);
        assert_eq!(enabled.readiness(), Readiness::ArtifactsUnavailable);
        assert!(!enabled.readiness().is_ready());
        assert!(Readiness::Ready.is_ready());
        assert!(!Readiness::Refused.is_ready());
        assert!(!Readiness::ExecutorUnavailable.is_ready());

        let rows = config.installs();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cartridge_id, id);
        assert!(rows[0].install.nodule.as_ref().unwrap().enabled);
        assert!(rows[0].install.micro.is_none());

        config.uninstall(&id).unwrap();
        assert!(config.resolve(&cache).is_empty());
        assert!(matches!(
            config.uninstall(&id),
            Err(InventoryError::UnknownCartridge(_))
        ));
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

        install(&mut config, &cache, &cartridge).unwrap();
        let installed = resolved(&config, &cache, &id);
        assert!(installed.artifacts.is_empty());
        assert_eq!(installed.readiness(), Readiness::Disabled);

        config
            .set_enabled(&cache, &id, Component::Nodule, true)
            .unwrap();
        assert_eq!(resolved(&config, &cache, &id).readiness(), Readiness::Ready);
    }

    #[test]
    fn upgrades_keep_enablement_and_unknown_members_and_never_reuse_a_version_label() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let v1 = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        install(&mut config, &cache, &v1).unwrap();
        config
            .set_enabled(&cache, &id, Component::Nodule, true)
            .unwrap();

        // The upgrade moves the pin and keeps the component's enablement.
        let v2 = cartridge_with(example_cartridge!("photos"), "1.1.0", photos());
        install(&mut config, &cache, &v2).unwrap();
        let upgraded = resolved(&config, &cache, &id);
        assert!(upgraded.enabled);
        assert_eq!(upgraded.version, "1.1.0".parse().unwrap());
        assert_eq!(upgraded.revision, v2.revision());
        assert_eq!(config.config_docs().len(), 1);

        // A version label means one revision: another envelope under the label is refused.
        let mut forged = cartridge_with(example_cartridge!("photos"), "1.1.0", photos());
        forged.title = Some("not what was installed".into());
        assert!(matches!(
            install(&mut config, &cache, &forged),
            Err(InventoryError::Cartridge(
                big_cartridges_core::CartridgeError::Conflict { .. }
            ))
        ));
        assert_eq!(resolved(&config, &cache, &id).revision, v2.revision());
    }

    #[test]
    fn a_cartridge_has_a_member_per_component_it_contributes_enabled_independently() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let micro = |cartridge: &mut Cartridge| {
            cartridge.components.insert(
                "micro".into(),
                serde_json::from_value(json!({
                    "ref": "dtxt:datum:self/org.example.daybook/micro"
                }))
                .unwrap(),
            );
            cartridge.micro = Some(MicroContribution {
                component: "micro".into(),
                schema: "org.example.clay.micro".into(),
                description: Default::default(),
                extra: Default::default(),
            });
        };

        // A micro-only cartridge installs, has no nodule, and is not resolved as one.
        let mut only_micro = Cartridge::new(
            example_cartridge!("panel").parse().unwrap(),
            "1.0.0".parse().unwrap(),
        );
        micro(&mut only_micro);
        install(&mut config, &cache, &only_micro).unwrap();
        let panel: CartridgeId = example_cartridge!("panel").parse().unwrap();
        let row = &config.installs()[0].install;
        assert!(row.nodule.is_none());
        assert_eq!(row.micro, Some(ComponentInstall::new(false)));
        assert!(config.resolve(&cache).is_empty());
        config
            .set_enabled(&cache, &panel, Component::Micro, true)
            .unwrap();
        assert!(matches!(
            config.set_enabled(&cache, &panel, Component::Nodule, true),
            Err(InventoryError::NoComponent {
                component: Component::Nodule,
                ..
            })
        ));

        // A cartridge with both runs one revision with each enabled on its own.
        let mut both = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        micro(&mut both);
        install(&mut config, &cache, &both).unwrap();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        config
            .set_enabled(&cache, &id, Component::Micro, true)
            .unwrap();
        assert!(
            !resolved(&config, &cache, &id).enabled,
            "the nodule stays disabled"
        );
        config
            .set_enabled(&cache, &id, Component::Nodule, true)
            .unwrap();
        config
            .set_enabled(&cache, &id, Component::Micro, false)
            .unwrap();
        let row = config.entry(&id).unwrap().install.accepted().unwrap();
        assert_eq!(row.nodule, Some(ComponentInstall::new(true)));
        assert_eq!(row.micro, Some(ComponentInstall::new(false)));
        assert!(resolved(&config, &cache, &id).enabled);

        // A cartridge that contributes neither has nothing to install.
        let empty = Cartridge::new(
            example_cartridge!("empty").parse().unwrap(),
            "0.1.0".parse().unwrap(),
        );
        assert!(matches!(
            install(&mut config, &cache, &empty),
            Err(InventoryError::NothingToInstall(_))
        ));
    }

    #[test]
    fn each_grant_is_a_datum_of_its_own_and_revoking_one_leaves_the_rest() {
        let mut docs = CartridgeConfig::default();
        let drawer = DocId::random();
        let scope = |operations: &[&str], authorities: &[&str]| DispatchScope {
            operations: operations.iter().map(|nsid| (*nsid).to_owned()).collect(),
            authorities: authorities.iter().map(|nsid| (*nsid).to_owned()).collect(),
            privileged: false,
        };
        let drawer_grant = GrantKey::Drawer(drawer.clone());
        let method = GrantKey::Method("org.example.search.query".into());
        let authority = GrantKey::Authority("org.example.callee".into());

        // Two grants made separately both apply, and each wrote a datum of its own.
        docs.set_grant(
            Component::Nodule,
            drawer_grant.clone(),
            Some(GrantBody::drawer(EntryAccess::Read)),
        )
        .unwrap();
        docs.set_grant(
            Component::Nodule,
            method.clone(),
            Some(GrantBody::method(false)),
        )
        .unwrap();
        docs.set_grant(
            Component::Nodule,
            authority.clone(),
            Some(GrantBody::method(false)),
        )
        .unwrap();
        let (drawers, methods) = docs.resolve_grants(Component::Nodule).unwrap();
        assert_eq!(
            drawers,
            BTreeMap::from([(drawer.clone(), DrawerAccess::Read)])
        );
        assert_eq!(
            methods,
            scope(&["org.example.search.query"], &["org.example.callee"])
        );
        let patch = CartridgeConfig::diff(&CartridgeConfig::default(), &docs).unwrap();
        assert_eq!(patch.set.len(), 3);
        assert!(patch.remove.is_empty());

        // The nodule's grants are not the micro's.
        assert_eq!(
            docs.resolve_grants(Component::Micro).unwrap(),
            (BTreeMap::new(), DispatchScope::default())
        );
        docs.set_grant(
            Component::Micro,
            method.clone(),
            Some(GrantBody::method(true)),
        )
        .unwrap();
        assert!(docs.resolve_grants(Component::Micro).unwrap().1.privileged);
        assert!(!docs.resolve_grants(Component::Nodule).unwrap().1.privileged);

        // Revoking one grant deletes its datum alone.
        let before = docs.clone();
        docs.set_grant(Component::Nodule, method, None).unwrap();
        let patch = CartridgeConfig::diff(&before, &docs).unwrap();
        assert!(patch.set.is_empty());
        assert_eq!(
            patch.remove,
            BTreeSet::from([
                GrantKey::Method("org.example.search.query".into()).datum_key(Component::Nodule)
            ])
        );
        assert_eq!(
            docs.resolve_grants(Component::Nodule).unwrap().1,
            scope(&[], &["org.example.callee"])
        );

        // A grant states what its kind takes.
        assert!(matches!(
            docs.set_grant(
                Component::Nodule,
                drawer_grant,
                Some(GrantBody::method(false))
            ),
            Err(InventoryError::InvalidGrant { .. })
        ));
        assert!(matches!(
            docs.set_grant(
                Component::Nodule,
                authority,
                Some(GrantBody::drawer(EntryAccess::Read))
            ),
            Err(InventoryError::InvalidGrant { .. })
        ));
    }

    #[test]
    fn a_pin_that_cannot_be_read_yet_refuses_the_nodule_and_is_read_again() {
        let v1 = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        let id = v1.id.clone();

        // An install that synced in names a version whose heads have not reached this node.
        let mut elsewhere = NoduleConfig::new();
        install(&mut elsewhere, &AdmissionCache::new(), &v1).unwrap();
        let read = DatumsRead {
            heads: Vec::new(),
            datums: NoduleConfig::diff(&NoduleConfig::new(), &elsewhere)
                .unwrap()
                .set
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        DatumRead::Accepted {
                            value,
                            version: DatumVersion::default(),
                            authors: Vec::new(),
                        },
                    )
                })
                .collect(),
        };
        let config = NoduleConfig::hydrate(&read);
        let pin = config
            .entry(&id)
            .unwrap()
            .install
            .accepted()
            .unwrap()
            .cartridge
            .clone();

        // The nodule is refused, and the pin stays unsettled so the next reconcile reads it again.
        let cache = AdmissionCache::new();
        assert!(cache.needs_read(&id, &pin));
        cache.insert(
            &id,
            pin.clone(),
            Admission::Refused {
                version: None,
                revision: None,
                reason: "the pinned version is not available yet".into(),
            },
            false,
        );
        assert!(cache.needs_read(&id, &pin));
        let NoduleResolution::Refused { reason, .. } = &config.resolve(&cache)[&id] else {
            panic!("the nodule is refused while its pin is unread");
        };
        assert!(reason.contains("not available yet"), "{reason}");

        // Once read it is settled and the same pin resolves.
        cache.insert(&id, pin.clone(), Admission::of(&v1), true);
        assert!(!cache.needs_read(&id, &pin));
        assert_eq!(resolved(&config, &cache, &id).revision, v1.revision());
    }

    #[test]
    fn an_admission_is_kept_for_its_pin_and_dropped_for_another() {
        let cache = AdmissionCache::new();
        let id: CartridgeId = example_cartridge!("photos").parse().unwrap();
        let v1 = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        let pin = pin_of(&v1);
        let install = |pin: &big_cabinet_core::DatumPin| CartridgeInstall {
            cartridge: pin.clone(),
            config_doc: DocId::random(),
            nodule: Some(ComponentInstall::new(false)),
            micro: None,
            extra: Default::default(),
        };
        cache.insert(&id, pin.clone(), Admission::of(&v1), true);
        let first = cache.admission(&id, &install(&pin));
        assert!(matches!(&*first, Admission::Admitted { .. }));
        assert!(
            Arc::ptr_eq(&first, &cache.admission(&id, &install(&pin))),
            "the same pin reuses the stored outcome"
        );
        let mut other = install(&pin);
        other.cartridge = pin_of(&cartridge_with(
            example_cartridge!("photos"),
            "1.1.0",
            photos(),
        ));
        assert!(cache.needs_read(&id, &other.cartridge));
        let Admission::Refused { reason, .. } = &*cache.admission(&id, &other) else {
            panic!("a pin nothing was read for is refused");
        };
        assert!(reason.contains("has not been read"), "{reason}");
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
            let pin = pin_of(&cartridge);
            cache.insert(&cartridge.id, pin.clone(), Admission::of(&cartridge), true);
            read.datums.insert(
                install_key(&cartridge.id),
                DatumRead::Accepted {
                    value: serde_json::to_value(CartridgeInstall {
                        cartridge: pin,
                        config_doc: DocId::random(),
                        nodule: Some(ComponentInstall::new(true)),
                        micro: None,
                        extra: Default::default(),
                    })
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
            install(&mut config, &cache, &cartridge).unwrap();
            config
                .set_enabled(&cache, &cartridge.id, Component::Nodule, true)
                .unwrap();
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
            install(&mut config, &cache, &empty),
            Err(InventoryError::NothingToInstall(_))
        ));

        let mut no_component = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        no_component.extra.shift_remove("_resolvedComponents");
        let result = install(&mut config, &cache, &no_component);
        assert!(
            matches!(&result, Err(InventoryError::Nodule(_))),
            "{result:?}"
        );
        assert!(
            config.resolve(&cache).is_empty(),
            "refused installs install nothing"
        );

        let valid = cartridge_with(example_cartridge!("photos"), "1.0.0", photos());
        install(&mut config, &cache, &valid).unwrap();
        config
            .set_enabled(&cache, &valid.id, Component::Nodule, true)
            .unwrap();

        // Installing a rival holds nothing; enabling it is refused while `photos` holds the prefix.
        let duplicate_authority =
            cartridge_with(example_cartridge!("other-photos"), "1.0.0", photos());
        install(&mut config, &cache, &duplicate_authority).unwrap();
        assert!(matches!(
            config.set_enabled(&cache, &duplicate_authority.id, Component::Nodule, true),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let mut nested =
            cartridge_with(example_cartridge!("sub-photos"), "1.0.0", Nodule::default());
        nested.authority = Some("org.example.photos.sub".into());
        install(&mut config, &cache, &nested).unwrap();
        assert!(matches!(
            config.set_enabled(&cache, &nested.id, Component::Nodule, true),
            Err(InventoryError::AuthorityConflict { authority, .. }) if authority == "org.example.photos.sub"
        ));
        let mut sibling = cartridge_with(
            example_cartridge!("photosynthesis"),
            "1.0.0",
            Nodule::default(),
        );
        sibling.authority = Some("org.example.photosynthesis".into());
        install(&mut config, &cache, &sibling).unwrap();
        config
            .set_enabled(&cache, &sibling.id, Component::Nodule, true)
            .unwrap();
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
        let universe =
            Universe::collect([Declaration::of::<big_cartridges_core::Nodule>().unwrap()]);
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
        assert!(main["properties"]["routines"].is_object(), "{nodule}");
        assert!(nodule["defs"]["main"].is_object(), "{nodule}");
        assert_eq!(
            main["properties"]["bindings"]["type"], "unknown",
            "{nodule}"
        );
        assert_eq!(lexicons[0].id, big_cartridges_core::Nodule::ID);
    }

    #[test]
    fn the_config_datums_declare_publish_and_admit() {
        use crate::config::MethodRoute;
        use crate::config::NoduleDevTakeover;
        use crate::config::config_universe;
        use big_cartridges_core::config::CartridgeConfigIdentity;
        use big_cartridges_core::config::CartridgeInstall;
        use big_cartridges_core::config::MicroGrant;
        use big_cartridges_core::config::NoduleGrant;

        let universe = config_universe();
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(
            ids,
            [
                CartridgeInstall::ID,
                CartridgeConfigIdentity::ID,
                NoduleGrant::ID,
                MicroGrant::ID,
                NoduleDevTakeover::ID,
                MethodRoute::ID
            ]
        );
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 6);

        // The install pins its version by reference: a `#datumPin` position, which indexes find
        // from the schema alone.
        let install_lex = lexicons
            .iter()
            .find(|lex| lex.id == CartridgeInstall::ID)
            .unwrap();
        let pin = &install_lex.file["defs"]["main"]["properties"]["cartridge"];
        assert_eq!(pin["type"], "ref", "{}", install_lex.file);
        assert_eq!(pin["ref"], "org.example.daybook.defs#datumPin", "{pin}");

        // The node's own type library, which the pin refers to, is carried by every registration.
        let defs = big_cabinet_core::reference::defs_lexicon();
        for (index, lexicon) in lexicons.iter().enumerate() {
            let carried: Vec<&Value> = lexicons
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, other)| &other.file)
                .chain(std::iter::once(&defs))
                .collect();
            bundle
                .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
        }
    }
}

mod config_tests {
    use super::admission_tests::install;
    use super::*;
    use crate::config::AdmissionCache;
    use crate::config::CartridgeConfig;
    use crate::config::DatumSlot;
    use crate::config::NoduleConfig;
    use crate::config::NoduleResolution;
    use crate::config::artifact_key;
    use big_cabinet_core::DatumKey;
    use big_cabinet_core::DocId;
    use big_cabinet_core::datum::DatumMember;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DatumStore;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::DatumsRead;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::EntryAccess;
    use big_cartridges_core::Cartridge;
    use big_cartridges_core::CartridgeId;
    use big_cartridges_core::Digest;
    use big_cartridges_core::config::CartridgeConfigIdentity;
    use big_cartridges_core::config::CartridgeInstall;
    use big_cartridges_core::config::Component;
    use big_cartridges_core::config::GrantBody;
    use big_cartridges_core::config::GrantKey;
    use big_cartridges_core::config::MicroGrant;
    use big_cartridges_core::config::NoduleGrant;
    use big_cartridges_core::config::config_identity_key;
    use big_cartridges_core::config::install_key;
    use big_cartridges_core::example_cartridge;

    fn sample_cartridge(id: &str, version: &str) -> Cartridge {
        let resolved = photos().resolved(Some(PHOTOS));
        let mut cartridge = resolved.cartridge;
        cartridge.id = id.parse().unwrap();
        cartridge.version = version.parse().unwrap();
        cartridge.authority = Some(PHOTOS.into());
        cartridge.extra.insert(
            "_resolvedLexicons".into(),
            serde_json::to_value(resolved.lexicons).unwrap(),
        );
        cartridge.extra.insert(
            "_resolvedComponents".into(),
            serde_json::to_value(resolved.components).unwrap(),
        );
        cartridge
    }

    fn accepted(value: Value) -> DatumRead {
        DatumRead::Accepted {
            value,
            version: DatumVersion::default(),
            authors: Vec::new(),
        }
    }

    /// The install datum of `cartridge` as the node document stores it.
    fn stored_install(cartridge: &Cartridge, enabled: bool) -> Value {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        install(&mut config, &cache, cartridge).unwrap();
        config
            .set_enabled(&cache, &cartridge.id, Component::Nodule, enabled)
            .unwrap();
        let patch = NoduleConfig::diff(&NoduleConfig::new(), &config).unwrap();
        patch.set[&install_key(&cartridge.id)].clone()
    }

    #[test]
    fn config_datums_preserve_unknown_fields_and_vocabulary_on_round_trip() {
        let cartridge = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let cartridge_id = cartridge.id.clone();

        // The install is a pin, with no copy of the envelope and no revision of its own.
        let mut install_json = stored_install(&cartridge, true);
        assert!(install_json.get("envelope").is_none() && install_json.get("revision").is_none());
        assert!(install_json["cartridge"]["heads"].is_array());
        assert_eq!(install_json["nodule"], json!({"enabled": true}));
        install_json["futureInstallField"] = json!("kept");
        install_json["nodule"]["futureMember"] = json!(1);
        let decoded: CartridgeInstall = serde_json::from_value(install_json.clone()).unwrap();
        assert!(decoded.nodule.as_ref().unwrap().enabled);
        assert!(decoded.micro.is_none());
        assert_eq!(serde_json::to_value(&decoded).unwrap(), install_json);

        let grant_json = json!({ "access": "SuperAdmin", "futureGrantField": true });
        let decoded: NoduleGrant = serde_json::from_value(grant_json.clone()).unwrap();
        assert_eq!(
            decoded.access,
            Some(EntryAccess::Unknown("SuperAdmin".into()))
        );
        assert_eq!(serde_json::to_value(&decoded).unwrap(), grant_json);
        let micro: MicroGrant = serde_json::from_value(json!({ "privileged": true })).unwrap();
        assert!(micro.privileged && micro.access.is_none());

        assert!(NoduleConfig::owns(&install_key(&cartridge_id)));
        // The grants and the settings are the cartridge document's, not the node's.
        let grant = GrantKey::Method("org.example.x.y".into());
        assert!(!NoduleConfig::owns(&grant.datum_key(Component::Nodule)));
        assert!(CartridgeConfig::owns(&grant.datum_key(Component::Nodule)));
        assert!(CartridgeConfig::owns(
            &DatumKey::from_text("org.example.photos.settings", "main").unwrap()
        ));
    }

    #[test]
    fn grant_keys_name_drawers_operations_and_authorities_and_nothing_else() {
        let drawer = DocId::random();
        for text in [
            format!("drawer:{drawer}"),
            "method:org.example.tags.apply".to_owned(),
            "authority:org.example.tags".to_owned(),
            "authority:*".to_owned(),
        ] {
            let key: GrantKey = text.parse().unwrap();
            assert_eq!(key.to_string(), text);
            assert_eq!(GrantKey::from_slot(text.as_bytes()), Some(key));
        }
        // An NSID-shaped name is an operation and an authority at once, so the kind is in the key.
        assert_ne!(
            "method:org.example.tags".parse::<GrantKey>().unwrap(),
            "authority:org.example.tags".parse::<GrantKey>().unwrap()
        );
        for text in [
            "",
            "drawer",
            "drawer:not-a-doc",
            "method:org.example",
            "method:*",
            "authority:tags",
            "weird:org.example.tags",
        ] {
            assert!(text.parse::<GrantKey>().is_err(), "{text}");
        }
    }

    #[test]
    fn node_store_hydrates_diffs_and_isolates_invalid_or_orphan_entries() {
        let photos_cartridge = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let photos_id = photos_cartridge.id.clone();

        let broken_id: CartridgeId = example_cartridge!("broken").parse().unwrap();
        let orphan_id: CartridgeId = example_cartridge!("orphan").parse().unwrap();

        let unowned_key = DatumKey::from_text("org.example.daybook.drawer", "main").unwrap();
        let malformed_key =
            DatumKey::from_text(CartridgeInstall::ID, "invalid-cartridge-slot").unwrap();

        let mut install_val = stored_install(&photos_cartridge, false);
        install_val["extraInstall"] = json!(1);

        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        read.datums.insert(
            unowned_key.clone(),
            accepted(
                json!({"layout": {"$type": "org.example.daybook.drawer.selfDoc", "empty": true}}),
            ),
        );
        read.datums
            .insert(malformed_key.clone(), accepted(json!({"unexpected": true})));
        read.datums
            .insert(install_key(&photos_id), accepted(install_val));
        read.datums.insert(
            install_key(&broken_id),
            accepted(json!({"cartridge": "nope", "configDoc": "bad", "nodule": {"enabled": true}})),
        );
        // An artifact reference with no install is an orphan: it installs nothing.
        read.datums.insert(
            artifact_key(&orphan_id, "handler"),
            accepted(json!({"mime": "application/wasm", "lengthOctets": 1, "digest": "zQm"})),
        );

        let before = NoduleConfig::hydrate(&read);
        assert!(before.carried_keys().contains_key(&malformed_key));
        assert!(!before.carried_keys().contains_key(&unowned_key));
        assert!(matches!(
            before.entry(&broken_id).unwrap().install,
            DatumSlot::Invalid { .. }
        ));
        assert!(before.entry(&orphan_id).unwrap().install.is_absent());

        // The pinned version of `example/photos` is read; `example/broken` is refused for its
        // install, and `example/orphan` is ignored.
        let cache = AdmissionCache::new();
        let pin = before
            .entry(&photos_id)
            .unwrap()
            .install
            .accepted()
            .unwrap()
            .cartridge
            .clone();
        cache.insert(
            &photos_id,
            pin,
            crate::config::Admission::of(&photos_cartridge),
            true,
        );
        let resolved = before.resolve(&cache);
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
            .set_enabled(&cache, &photos_id, Component::Nodule, true)
            .unwrap();
        let patch = NoduleConfig::diff(&before, &after).unwrap();
        assert!(patch.remove.is_empty());
        assert_eq!(patch.set.len(), 1);
        assert_eq!(
            patch.set[&install_key(&photos_id)]["nodule"]["enabled"],
            json!(true)
        );
        assert_eq!(
            patch.set[&install_key(&photos_id)]["extraInstall"],
            json!(1)
        );

        // Uninstalling `example/photos` removes the install in one patch; the cartridge's
        // config document is not this document's to touch.
        let mut uninstalled = after.clone();
        uninstalled.uninstall(&photos_id).unwrap();
        let uninstall_patch = NoduleConfig::diff(&after, &uninstalled).unwrap();
        assert!(uninstall_patch.set.is_empty());
        assert_eq!(
            uninstall_patch.remove,
            BTreeSet::from([install_key(&photos_id)])
        );

        // Removing a carried malformed key emits it in `patch.remove`.
        let mut cleaned = before.clone();
        assert!(cleaned.remove_carried_key(&malformed_key));
        let clean_patch = NoduleConfig::diff(&before, &cleaned).unwrap();
        assert_eq!(clean_patch.remove, BTreeSet::from([malformed_key]));
    }

    #[test]
    fn the_cartridge_store_carries_settings_and_unreadable_grants_and_clears_only_its_own() {
        let drawer = DocId::random();
        let identity = config_identity_key();
        let settings = DatumKey::from_text("org.example.photos.settings", "main").unwrap();
        let damaged = GrantKey::Method("org.example.damaged.op".into());
        let kept = GrantKey::Drawer(drawer.clone());
        let notes = DatumKey::from_text(NoduleGrant::ID, "free-form-note").unwrap();
        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        read.datums.insert(
            identity.clone(),
            accepted(json!({ "cartridge": example_cartridge!("photos") })),
        );
        read.datums
            .insert(settings.clone(), accepted(json!({ "thumbnails": false })));
        read.datums.insert(
            kept.datum_key(Component::Nodule),
            accepted(json!({ "access": "Write", "futureField": 1 })),
        );
        read.datums.insert(
            damaged.datum_key(Component::Nodule),
            DatumRead::Rejected {
                reason: "unauthorized writer".into(),
            },
        );
        read.datums.insert(
            kept.datum_key(Component::Micro),
            accepted(json!({ "access": "Read" })),
        );
        // A grant-tagged datum whose slot names no grant is carried, like a setting.
        read.datums
            .insert(notes.clone(), accepted(json!({ "text": "hand written" })));
        let stored = CartridgeConfig::hydrate(&read);

        assert!(matches!(
            stored.identity().accepted(),
            Some(CartridgeConfigIdentity { cartridge, .. })
                if cartridge.to_string() == example_cartridge!("photos")
        ));
        let nodule: Vec<_> = stored.grants(Component::Nodule).collect();
        assert_eq!(nodule.len(), 2);
        assert!(matches!(
            stored.resolve_grants(Component::Nodule),
            Err(reason) if reason.contains("org.example.damaged.op")
        ));
        let (micro_drawers, _) = stored.resolve_grants(Component::Micro).unwrap();
        assert_eq!(
            micro_drawers,
            BTreeMap::from([(drawer.clone(), DrawerAccess::Read)])
        );

        // Rewriting a grant keeps the members of the stored datum this build does not know, and
        // replaces the damaged one outright.
        let mut after = stored.clone();
        after
            .set_grant(
                Component::Nodule,
                kept.clone(),
                Some(GrantBody::drawer(EntryAccess::Admin)),
            )
            .unwrap();
        after
            .set_grant(
                Component::Nodule,
                damaged.clone(),
                Some(GrantBody::method(false)),
            )
            .unwrap();
        let patch = CartridgeConfig::diff(&stored, &after).unwrap();
        assert!(patch.remove.is_empty());
        assert_eq!(
            patch.set[&kept.datum_key(Component::Nodule)],
            json!({ "access": "Admin", "futureField": 1 })
        );
        assert_eq!(patch.set[&damaged.datum_key(Component::Nodule)], json!({}));
        assert_eq!(
            patch.set.len(),
            2,
            "the settings and the micro's grant are untouched"
        );

        // Clearing deletes the settings and every grant of both components, and keeps the
        // identity that lets the document be found again.
        let mut cleared = stored.clone();
        cleared.clear();
        let patch = CartridgeConfig::diff(&stored, &cleared).unwrap();
        assert!(patch.set.is_empty());
        assert_eq!(
            patch.remove,
            BTreeSet::from([
                settings,
                notes,
                kept.datum_key(Component::Nodule),
                damaged.datum_key(Component::Nodule),
                kept.datum_key(Component::Micro),
            ])
        );
        assert!(!patch.remove.contains(&identity));
        assert_eq!(cleared.identity(), stored.identity());
    }

    /// The `Blob` datum stored for `cartridge`'s handler artifact, as the document holds it.
    fn stored_artifact(cartridge: &Cartridge) -> Value {
        let components: BTreeMap<String, big_cartridges_core::Component> =
            serde_json::from_value(cartridge.extra.get("_resolvedComponents").unwrap().clone())
                .unwrap();
        let big_cartridges_core::Component::Wasm(artifact) = &components["handler"] else {
            panic!("handler component is not wasm")
        };
        json!({
            "mime": artifact.media_type,
            "lengthOctets": artifact.size,
            "digest": artifact.digest.to_string(),
        })
    }

    #[test]
    fn route_datums_round_trip_by_pattern_and_a_damaged_one_is_an_error_until_cleared() {
        use crate::config::route_key;
        use big_dispatch_core::Route;

        let mut config = NoduleConfig::new();
        config
            .set_route("org.example.beta", Some("p".into()))
            .unwrap();
        config
            .set_route("org.example.beta.ping", Some("q".into()))
            .unwrap();
        let patch = NoduleConfig::diff(&NoduleConfig::new(), &config).unwrap();
        assert_eq!(
            patch.set.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                route_key("org.example.beta"),
                route_key("org.example.beta.ping")
            ])
        );
        assert_eq!(
            patch.set[&route_key("org.example.beta")],
            json!({"provider": "p"})
        );
        assert!(NoduleConfig::owns(&route_key("org.example.beta")));

        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        for (key, value) in &patch.set {
            read.datums.insert(key.clone(), accepted(value.clone()));
        }
        let stored = NoduleConfig::hydrate(&read);
        assert_eq!(
            stored.routes().0,
            BTreeMap::from([
                ("org.example.beta".to_owned(), Route::Provider("p".into())),
                (
                    "org.example.beta.ping".to_owned(),
                    Route::Provider("q".into())
                )
            ])
        );

        // Clearing deletes the datum, and a stored datum that does not decode is reported rather
        // than dropped, which would send its calls to whichever provider claims them.
        let mut cleared = stored.clone();
        cleared.set_route("org.example.beta", None).unwrap();
        assert_eq!(
            NoduleConfig::diff(&stored, &cleared).unwrap().remove,
            BTreeSet::from([route_key("org.example.beta")])
        );
        read.datums.insert(
            route_key("org.example.beta"),
            accepted(json!({"provider": 7})),
        );
        let damaged = NoduleConfig::hydrate(&read);
        // The damaged datum is a route that refuses what its pattern covers, not a failure of
        // the others; one whose pattern is unusable is left out and described.
        let (table, unreadable) = damaged.routes();
        assert!(matches!(
            table.get("org.example.beta"),
            Some(Route::Invalid(reason)) if reason.contains("damaged")
        ));
        assert_eq!(table["org.example.beta.ping"], Route::Provider("q".into()));
        assert!(unreadable.is_empty());
        let mut repaired = damaged;
        repaired.set_route("org.example.beta", None).unwrap();
        assert_eq!(repaired.routes().0.len(), 1);
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

        // Installing writes the install and the reference in the same patch, and the reference
        // is the datum wire form big_blobs reads (no `null` for the unset options).
        let v1 = sample_cartridge(example_cartridge!("photos"), "1.0.0");
        let cache = AdmissionCache::new();
        let mut config = NoduleConfig::new();
        install(&mut config, &cache, &v1).unwrap();
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

        // Re-installing the same revision at the stored pin rewrites nothing and drops the
        // reference no artifact of the revision names.
        let pin = stored
            .entry(&id)
            .unwrap()
            .install
            .accepted()
            .unwrap()
            .cartridge
            .clone();
        let doc = stored.config_docs()[&id].clone();
        cache.insert(&id, pin.clone(), crate::config::Admission::of(&v1), true);
        let mut again = stored.clone();
        again.install(&cache, &v1, pin, doc).unwrap();
        let patch = NoduleConfig::diff(&stored, &again).unwrap();
        assert!(patch.set.is_empty(), "{:?}", patch.set);
        assert_eq!(patch.remove, BTreeSet::from([stale.clone()]));

        // Upgrading to a revision with another handler swaps the reference in place.
        let mut v2 = sample_cartridge(example_cartridge!("photos"), "1.1.0");
        let mut components: BTreeMap<String, big_cartridges_core::Component> =
            serde_json::from_value(v2.extra.get("_resolvedComponents").unwrap().clone()).unwrap();
        let big_cartridges_core::Component::Wasm(artifact) = components.get_mut("handler").unwrap()
        else {
            panic!("handler component is not wasm")
        };
        artifact.digest = Digest::of(b"other-wasm-bytes");
        artifact.size = 16;
        v2.extra.insert(
            "_resolvedComponents".into(),
            serde_json::to_value(components).unwrap(),
        );
        let mut upgraded = stored.clone();
        install(&mut upgraded, &cache, &v2).unwrap();
        let patch = NoduleConfig::diff(&stored, &upgraded).unwrap();
        assert_eq!(
            patch.set.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([install_key(&id), slot.clone()])
        );
        assert_eq!(patch.set[&slot], stored_artifact(&v2));
        assert_eq!(patch.remove, BTreeSet::from([stale.clone()]));

        // A reference that no longer decodes is replaced by the install, which repairs it.
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
        install(&mut repaired, &cache, &v1).unwrap();
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
