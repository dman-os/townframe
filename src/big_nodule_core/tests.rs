use super::*;
use big_cabinet_core::datum::DatumMember;
use serde_json::json;

fn file(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

fn call(artifact: &str, export: &str) -> Execution {
    Execution::Call(Call {
        artifact: artifact.into(),
        export: export.into(),
        extra: Default::default(),
    })
}

/// The photo browser of ADR 023 §3: declares its own datum, query and subscription, requires
/// the search vocabulary, and presents search hits through a stateless view.
fn photos() -> Nodule {
    let mut nodule = Nodule::new("org.example.photos");
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
    nodule.requires.push("org.example.search.query".into());
    nodule.private_state.push(PrivateState {
        key: "index".into(),
        kind: StateKind::Sqlite,
        extra: Default::default(),
    });
    nodule.routines.push(Routine {
        key: "list".into(),
        implements: "org.example.photos.list".into(),
        execution: call("routines", "list"),
        state: vec!["index".into()],
        extra: Default::default(),
    });
    nodule.routines.push(Routine {
        key: "import".into(),
        implements: "org.example.photos.import".into(),
        execution: Execution::Workflow(Workflow {
            artifact: "routines".into(),
            export: "import".into(),
            extra: Default::default(),
        }),
        state: vec!["index".into()],
        extra: Default::default(),
    });
    nodule.routines.push(Routine {
        key: "present".into(),
        implements: "org.example.photos.present".into(),
        execution: call("routines", "present"),
        state: Vec::new(),
        extra: Default::default(),
    });
    nodule.routines.push(Routine {
        key: "search".into(),
        implements: "org.example.search.query".into(),
        execution: call("routines", "search"),
        state: vec!["index".into()],
        extra: Default::default(),
    });
    nodule.revisioned_stores.push(RevisionedStore {
        key: "photos".into(),
        selector: "org.example.photos.photo#still".into(),
        entry: "org.example.photos.photo".into(),
        mode: StoreMode::Frontier,
        source: Some("import".into()),
        extra: Default::default(),
    });
    nodule.views.push(View {
        key: "hits".into(),
        input: "org.example.search.query#hits".into(),
        presenter: "present".into(),
        extra: Default::default(),
    });
    nodule.commands.push(Command {
        key: "findPhotos".into(),
        target: "org.example.search.query".into(),
        view: Some("hits".into()),
        extra: Default::default(),
    });
    nodule.initialization.push(Initializer {
        target: "org.example.photos.import".into(),
        mode: RunMode::Once,
        extra: Default::default(),
    });
    nodule.processors.push(Processor {
        key: "thumbnails".into(),
        target: "org.example.photos.import".into(),
        trigger: [("on".to_owned(), json!("documentWritten"))]
            .into_iter()
            .collect(),
        extra: Default::default(),
    });
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
    let error = nodule.admit().unwrap_err().to_string();
    assert!(error.contains(needle), "{error}");
}

#[test]
fn the_photo_browser_admits_and_round_trips() {
    let nodule = photos();
    let admitted = nodule.admit().unwrap();
    assert_eq!(
        admitted.operations,
        [
            ("org.example.photos.changes", OperationKind::Subscription),
            ("org.example.photos.import", OperationKind::Procedure),
            ("org.example.photos.list", OperationKind::Query),
            ("org.example.photos.present", OperationKind::Query),
        ]
        .into_iter()
        .map(|(id, kind)| (id.to_owned(), kind))
        .collect()
    );
    assert_eq!(admitted.artifacts, BTreeSet::from(["routines".to_owned()]));
    assert_eq!(nodule.datum_lexicons().count(), 2);

    let value = serde_json::to_value(&nodule).unwrap();
    assert_eq!(value["routines"][0]["execution"]["$type"], Call::ID);
    assert_eq!(value["routines"][1]["execution"]["$type"], Workflow::ID);
    assert_eq!(value["revisionedStores"][0]["mode"], "frontier");
    assert_eq!(value["initialization"][0]["mode"], "once");
    assert_eq!(value["privateState"][0]["kind"], "sqlite");
    assert_eq!(serde_json::from_value::<Nodule>(value).unwrap(), nodule);
}

#[test]
fn unknown_kinds_are_carried_on_the_wire_and_refused_at_admission() {
    let value = json!({
        "id": "org.example.photos",
        "routines": [{
            "key": "r",
            "implements": "org.example.other.op",
            "execution": { "$type": "org.example.daybook.nodule.native", "binary": "x" },
        }],
        "privateState": [{ "key": "s", "kind": "graph" }],
        "futureField": 1,
    });
    let nodule: Nodule = serde_json::from_value(value.clone()).unwrap();
    assert!(matches!(
        nodule.routines[0].execution,
        Execution::Unknown(_)
    ));
    assert_eq!(
        nodule.private_state[0].kind,
        StateKind::Unknown("graph".into())
    );
    assert_eq!(serde_json::to_value(&nodule).unwrap(), value);
    refuse(nodule.clone(), "no `graph` state");
    let mut known_state = nodule;
    known_state.private_state[0].kind = StateKind::Kv;
    refuse(
        known_state,
        "no `org.example.daybook.nodule.native` execution",
    );
}

#[test]
fn lexicons_live_under_the_nodule_authority() {
    let mut foreign = photos();
    foreign.lexicons[0].insert("id".into(), json!("org.example.other.photo"));
    refuse(foreign, "not under this nodule's authority");

    let mut duplicate = photos();
    duplicate.lexicons.push(duplicate.lexicons[0].clone());
    refuse(duplicate, "declared twice");

    let mut not_nsid = photos();
    not_nsid.id = "photos".into();
    refuse(not_nsid, "`photos` is not an NSID");

    let nodule = Nodule::new("org.example.photos");
    assert!(nodule.owns("org.example.photos"));
    assert!(nodule.owns("org.example.photos.photo"));
    assert!(!nodule.owns("org.example.photosx.photo"));
    assert!(!nodule.owns("org.example.search.query"));
}

#[test]
fn own_references_resolve_and_foreign_ones_pass() {
    let mut missing_file = photos();
    missing_file.revisioned_stores[0].entry = "org.example.photos.album".into();
    refuse(
        missing_file,
        "`org.example.photos.album` is under this nodule's authority but not declared",
    );

    let mut missing_def = photos();
    missing_def.revisioned_stores[0].selector = "org.example.photos.photo#moving".into();
    refuse(missing_def, "declares no definition `moving`");

    let mut operation_as_schema = photos();
    operation_as_schema.config.as_mut().unwrap().schema = "org.example.photos.list".into();
    refuse(operation_as_schema, "is an operation, not a schema");

    let mut local_ref_in_contribution = photos();
    local_ref_in_contribution.views[0].input = "#hits".into();
    refuse(local_ref_in_contribution, "a contribution names its file");

    let mut bad_operation_ref = photos();
    bad_operation_ref.lexicons[1]["defs"]["page"]["properties"]["photos"]["items"]["ref"] =
        json!("org.example.photos.photo#missing");
    refuse(
        bad_operation_ref,
        "lexicons[1].defs.page.properties.photos.items.ref",
    );

    let mut foreign = photos();
    foreign.views[0].input = "org.example.elsewhere.defs#anything".into();
    foreign.admit().unwrap();
}

#[test]
fn routines_implement_one_operation_each_with_a_fitting_execution() {
    let mut twice = photos();
    twice.routines[2].implements = "org.example.photos.list".into();
    refuse(twice, "already implemented by routine `list`");

    let mut undeclared = photos();
    undeclared.routines[0].implements = "org.example.photos.missing".into();
    refuse(undeclared, "not a declared operation");

    let mut workflow_query = photos();
    workflow_query.routines[0].execution = Execution::Workflow(Workflow {
        artifact: "routines".into(),
        export: "list".into(),
        extra: Default::default(),
    });
    refuse(workflow_query, "only a `procedure` is one");

    let mut no_artifact = photos();
    no_artifact.routines[0].execution = call("", "list");
    refuse(no_artifact, "names the artifact");

    let mut unknown_state = photos();
    unknown_state.routines[0].state.push("cache".into());
    refuse(unknown_state, "`cache` is not a declared private state");

    let mut duplicate_key = photos();
    duplicate_key.routines[1].key = "list".into();
    refuse(duplicate_key, "routines[1].key");

    // A foreign operation's kind is another nodule's contract: a workflow for it passes here.
    let mut foreign_workflow = photos();
    foreign_workflow.routines[3].execution = Execution::Workflow(Workflow {
        artifact: "routines".into(),
        export: "search".into(),
        extra: Default::default(),
    });
    foreign_workflow.admit().unwrap();
}

#[test]
fn invocations_target_queries_or_procedures_that_are_declared_or_required() {
    let mut subscription = photos();
    subscription.commands[0].target = "org.example.photos.changes".into();
    refuse(subscription, "is a subscription");

    let mut unrequired = photos();
    unrequired.processors[0].target = "org.example.tags.apply".into();
    refuse(unrequired, "foreign and not in `requires`");

    let mut own_in_requires = photos();
    own_in_requires
        .requires
        .push("org.example.photos.list".into());
    refuse(own_in_requires, "`requires` lists foreign operations");

    let mut missing_view = photos();
    missing_view.commands[0].view = Some("table".into());
    refuse(missing_view, "`table` is not a declared view");

    let mut workflow_presenter = photos();
    workflow_presenter.views[0].presenter = "import".into();
    refuse(workflow_presenter, "a presenter is stateless");

    let mut missing_source = photos();
    missing_source.revisioned_stores[0].source = Some("ingest".into());
    refuse(missing_source, "`ingest` is not a declared routine");

    let mut unknown_mode = photos();
    unknown_mode.initialization[0].mode = RunMode::Unknown("hourly".into());
    refuse(unknown_mode, "no `hourly` run mode");
}

#[test]
fn config_schema_must_be_local_object_and_defaults_must_conform() {
    let mut foreign_schema = photos();
    foreign_schema.config.as_mut().unwrap().schema = "org.example.other.settings".into();
    refuse(foreign_schema, "not under this nodule's authority");

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

mod inventory_tests {
    use super::*;
    use crate::inventory::ArtifactState;
    use crate::inventory::Inventory;
    use crate::inventory::InventoryError;
    use crate::inventory::Readiness;
    use big_plugs_core::Artifact;
    use big_plugs_core::Contribution;
    use big_plugs_core::Digest;
    use big_plugs_core::NoduleContribution;
    use big_plugs_core::Plug;
    use big_plugs_core::PlugId;

    fn plug_with(id: &str, version: &str, nodule: Nodule) -> Plug {
        let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
        plug.artifacts.push(Artifact {
            key: "routines".into(),
            digest: Digest::of(b"wasm-bytes"),
            size: Some(10),
            media_type: Some("application/wasm".into()),
            extra: Default::default(),
        });
        let description: IndexMap<String, Value> =
            serde_json::from_value(serde_json::to_value(nodule).unwrap()).unwrap();
        plug.contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: Nodule::ID.into(),
                description,
                extra: Default::default(),
            }));
        plug
    }

    #[test]
    fn admits_plug_tracks_readiness_and_configures_settings() {
        let mut inventory = Inventory::new();
        let plug = plug_with("@example/photos", "1.0.0", photos());
        let id: PlugId = "@example/photos".parse().unwrap();

        let installed = inventory.admit(&plug).unwrap();
        assert_eq!(installed.plug_id, id);
        assert_eq!(installed.version, "1.0.0".parse().unwrap());
        assert_eq!(installed.revision, plug.revision());
        assert!(!installed.enabled);
        assert_eq!(installed.readiness(), Readiness::Disabled);
        assert!(!installed.readiness().is_ready());
        assert_eq!(
            installed.artifacts.get("routines").map(|(_, s)| *s),
            Some(ArtifactState::Unavailable)
        );
        assert_eq!(
            installed.config,
            Some(
                [("thumbnails".to_owned(), json!(true))]
                    .into_iter()
                    .collect()
            )
        );

        let enabled = inventory.set_enabled(&id, true).unwrap();
        assert!(enabled.enabled);
        assert_eq!(enabled.readiness(), Readiness::ArtifactsUnavailable);
        assert!(!enabled.readiness().is_ready());

        let updated = inventory
            .configure(
                &id,
                [("thumbnails".to_owned(), json!(false))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        assert_eq!(
            updated.config,
            Some(
                [("thumbnails".to_owned(), json!(false))]
                    .into_iter()
                    .collect()
            )
        );

        let bad = inventory.configure(
            &id,
            [("thumbnails".to_owned(), json!("invalid"))]
                .into_iter()
                .collect(),
        );
        assert!(matches!(bad, Err(InventoryError::Nodule(_))));
        assert_eq!(inventory.iter().count(), 1);
        assert!(inventory.get(&id).is_some());
        assert!(!Readiness::Refused.is_ready());

        let removed = inventory.remove(&id).unwrap();
        assert_eq!(removed.plug_id, id);
        assert!(inventory.get(&id).is_none());
    }

    #[test]
    fn lexicon_only_nodule_reaches_executor_unavailable_when_enabled() {
        let mut inventory = Inventory::new();
        let mut nodule = Nodule::new("org.example.schema");
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
        let plug = plug_with("@example/schema", "0.1.0", nodule);
        let id: PlugId = "@example/schema".parse().unwrap();

        let installed = inventory.admit(&plug).unwrap();
        assert!(installed.artifacts.is_empty());
        assert_eq!(installed.readiness(), Readiness::Disabled);

        let enabled = inventory.set_enabled(&id, true).unwrap();
        assert_eq!(enabled.readiness(), Readiness::ExecutorUnavailable);
        assert!(!enabled.readiness().is_ready());
        assert!(matches!(
            inventory.configure(&id, IndexMap::new()),
            Err(InventoryError::NoConfigSchema(_))
        ));
    }

    #[test]
    fn upgrades_preserve_state_and_validate_configured_settings() {
        let mut inventory = Inventory::new();
        let id: PlugId = "@example/photos".parse().unwrap();
        let v1 = plug_with("@example/photos", "1.0.0", photos());
        inventory.admit(&v1).unwrap();
        inventory.set_enabled(&id, true).unwrap();

        // Unconfigured upgrade adopts the new version's defaults and keeps `enabled`.
        let mut nodule_v2 = photos();
        nodule_v2.config.as_mut().unwrap().defaults = Some(
            [("thumbnails".to_owned(), json!(false))]
                .into_iter()
                .collect(),
        );
        let v2 = plug_with("@example/photos", "1.1.0", nodule_v2);
        let upgraded = inventory.admit(&v2).unwrap();
        assert!(upgraded.enabled);
        assert_eq!(upgraded.version, "1.1.0".parse().unwrap());
        assert_eq!(
            upgraded.config,
            Some(
                [("thumbnails".to_owned(), json!(false))]
                    .into_iter()
                    .collect()
            )
        );

        // Explicit configuration is preserved across a compatible upgrade and rejected if the
        // new version's schema breaks it.
        inventory
            .configure(
                &id,
                [("thumbnails".to_owned(), json!(true))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let v3 = plug_with("@example/photos", "1.2.0", photos());
        let upgraded = inventory.admit(&v3).unwrap();
        assert_eq!(
            upgraded.config,
            Some(
                [("thumbnails".to_owned(), json!(true))]
                    .into_iter()
                    .collect()
            )
        );

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
        let v4_bad = plug_with("@example/photos", "2.0.0", incompatible);
        assert!(matches!(
            inventory.admit(&v4_bad),
            Err(InventoryError::Nodule(_))
        ));
        assert_eq!(
            inventory.get(&id).unwrap().version,
            "1.2.0".parse().unwrap()
        );
    }

    #[test]
    fn refuses_missing_contribution_bad_schema_missing_artifact_and_authority_conflicts() {
        let mut inventory = Inventory::new();
        let empty = Plug::new("@example/empty".parse().unwrap(), "0.1.0".parse().unwrap());
        assert!(matches!(
            inventory.admit(&empty),
            Err(InventoryError::MissingContribution(_))
        ));

        let mut bad_schema = plug_with("@example/photos", "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_schema.contributions[0] {
            n.schema = "org.example.other.nodule".into();
        }
        assert!(matches!(
            inventory.admit(&bad_schema),
            Err(InventoryError::UnsupportedSchema { .. })
        ));

        let mut bad_payload = plug_with("@example/photos", "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_payload.contributions[0] {
            n.description.insert("id".into(), json!(123));
        }
        assert!(matches!(
            inventory.admit(&bad_payload),
            Err(InventoryError::InvalidContribution { .. })
        ));

        let mut no_artifact = plug_with("@example/photos", "1.0.0", photos());
        no_artifact.artifacts.clear();
        assert!(matches!(
            inventory.admit(&no_artifact),
            Err(InventoryError::MissingArtifact { artifact, .. }) if artifact == "routines"
        ));

        let valid = plug_with("@example/photos", "1.0.0", photos());
        inventory.admit(&valid).unwrap();

        let duplicate_authority = plug_with("@example/other-photos", "1.0.0", photos());
        assert!(matches!(
            inventory.admit(&duplicate_authority),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let sub_authority = Nodule::new("org.example.photos.sub");
        let sub_plug = plug_with("@example/sub-photos", "1.0.0", sub_authority);
        assert!(matches!(
            inventory.admit(&sub_plug),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let changed_plug = plug_with(
            "@example/photos",
            "2.0.0",
            Nodule::new("org.example.gallery"),
        );
        assert!(matches!(
            inventory.admit(&changed_plug),
            Err(InventoryError::AuthorityChanged { .. })
        ));
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
        let universe = Universe::collect([
            Declaration::of::<Nodule>().unwrap(),
            Declaration::of::<Call>().unwrap(),
            Declaration::of::<Workflow>().unwrap(),
        ]);
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(
            ids,
            [
                "org.example.daybook.nodule",
                "org.example.daybook.nodule.call",
                "org.example.daybook.nodule.workflow",
            ]
        );
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 3);
        let nodule = &lexicons[0].file;
        let main = &nodule["defs"]["main"];
        assert_eq!(
            main["properties"]["lexicons"]["items"]["type"], "unknown",
            "{nodule}"
        );
        let execution = &nodule["defs"]["Routine"]["properties"]["execution"];
        assert_eq!(execution["type"], "union", "{nodule}");
        assert_eq!(execution.get("closed"), None, "{nodule}");
        assert_eq!(
            execution["refs"],
            json!([Call::ID, Workflow::ID]),
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
        use crate::config::config_universe;

        let universe = config_universe();
        let ids: Vec<&str> = universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        assert_eq!(
            ids,
            [NoduleInstall::ID, NoduleSettings::ID, NoduleGrants::ID]
        );
        let bundle = Bundle::new(&universe, Vec::new());
        let lexicons = bundle.lexicons().unwrap();
        assert_eq!(lexicons.len(), 3);

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
    use crate::config::DatumSlot;
    use crate::config::NoduleConfig;
    use crate::config::NoduleGrants;
    use crate::config::NoduleInstall;
    use crate::config::NoduleResolution;
    use crate::config::NoduleSettings;
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
    use big_plugs_core::Artifact;
    use big_plugs_core::Contribution;
    use big_plugs_core::Digest;
    use big_plugs_core::NoduleContribution;
    use big_plugs_core::Plug;
    use big_plugs_core::PlugId;

    fn sample_plug(id: &str, version: &str) -> Plug {
        let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
        plug.artifacts.push(Artifact {
            key: "routines".into(),
            digest: Digest::of(b"wasm-bytes"),
            size: Some(10),
            media_type: Some("application/wasm".into()),
            extra: Default::default(),
        });
        let description: IndexMap<String, Value> =
            serde_json::from_value(serde_json::to_value(photos()).unwrap()).unwrap();
        plug.contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: Nodule::ID.into(),
                description,
                extra: Default::default(),
            }));
        plug
    }

    #[test]
    fn config_datums_preserve_unknown_fields_and_vocabulary_on_round_trip() {
        let plug = sample_plug("@example/photos", "1.0.0");
        let plug_id = plug.id.clone();

        let install = NoduleInstall::from_plug(&plug, true);
        let mut install_json = serde_json::to_value(&install).unwrap();
        install_json["futureInstallField"] = json!("kept");
        let decoded_install: NoduleInstall = serde_json::from_value(install_json.clone()).unwrap();
        assert_eq!(decoded_install.decode_plug().unwrap(), plug);
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

        let ik = install_key(&plug_id);
        let sk = settings_key(&plug_id);
        let gk = grants_key(&plug_id);
        assert!(NoduleConfig::owns(&ik));
        assert!(NoduleConfig::owns(&sk));
        assert!(NoduleConfig::owns(&gk));
    }

    #[test]
    fn datum_store_hydrates_diffs_and_isolates_invalid_or_orphan_entries() {
        let photos_plug = sample_plug("@example/photos", "1.0.0");
        let photos_id = photos_plug.id.clone();

        let broken_id: PlugId = "@example/broken".parse().unwrap();
        let orphan_id: PlugId = "@example/orphan".parse().unwrap();
        let drawer = DocId::random();

        let unowned_key = DatumKey::from_text("org.example.daybook.drawer", "main").unwrap();
        let malformed_key = DatumKey::from_text(NoduleInstall::ID, "invalid-plug-slot").unwrap();

        let mut install_val =
            serde_json::to_value(NoduleInstall::from_plug(&photos_plug, false)).unwrap();
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
            },
        );
        read.datums.insert(
            malformed_key.clone(),
            DatumRead::Accepted {
                value: json!({"unexpected": true}),
                version: DatumVersion::default(),
            },
        );
        read.datums.insert(
            install_key(&photos_id),
            DatumRead::Accepted {
                value: install_val,
                version: DatumVersion::default(),
            },
        );
        read.datums.insert(
            install_key(&broken_id),
            DatumRead::Accepted {
                value: json!({"envelope": {}, "revision": "bad", "enabled": true}),
                version: DatumVersion::default(),
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

        // Resolve admits `@example/photos`, refuses `@example/broken`, and ignores `@example/orphan`.
        let resolved = before.resolve();
        assert!(matches!(
            resolved.get(&photos_id),
            Some(NoduleResolution::Admitted { .. })
        ));
        assert!(matches!(
            resolved.get(&broken_id),
            Some(NoduleResolution::Refused { .. })
        ));
        assert!(!resolved.contains_key(&orphan_id));

        // Mutating `@example/photos` leaves broken, orphan, malformed, and unowned keys untouched.
        let mut after = before.clone();
        after.set_enabled(&photos_id, true).unwrap();
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

        // Uninstalling `@example/photos` removes all three datums in one patch.
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
}
