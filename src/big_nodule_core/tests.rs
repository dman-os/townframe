use super::*;
use big_cabinet_core::datum::DatumMember;
use serde_json::json;

fn file(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

/// The photo browser of ADR 027 §3: declares its own datum, query and subscription, requires
/// the search vocabulary, and carries a command, view and processor binding it never interprets.
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
        "id": "org.example.photos",
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
    nodule.admit().unwrap();
    assert_eq!(nodule.bindings.len(), 4);
    assert_eq!(serde_json::to_value(&nodule).unwrap(), value);

    // Pre-reshape top-level binding fields are no longer typed: they are carried as unknown
    // members, not interpreted.
    let legacy = json!({ "id": "org.example.photos", "commands": [{ "key": "x" }] });
    let nodule: Nodule = serde_json::from_value(legacy.clone()).unwrap();
    nodule.admit().unwrap();
    assert_eq!(serde_json::to_value(&nodule).unwrap(), legacy);
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
    local_ref_in_contribution.revisioned_stores[0].selector = "#still".into();
    refuse(local_ref_in_contribution, "a contribution names its file");

    let mut bad_operation_ref = photos();
    bad_operation_ref.lexicons[1]["defs"]["page"]["properties"]["photos"]["items"]["ref"] =
        json!("org.example.photos.photo#missing");
    refuse(
        bad_operation_ref,
        "lexicons[1].defs.page.properties.photos.items.ref",
    );

    let mut foreign = photos();
    foreign.revisioned_stores[0].entry = "org.example.elsewhere.defs#anything".into();
    foreign.admit().unwrap();
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
        "under this nodule's authority but is not a declared operation",
    );

    let mut datum_not_operation = photos();
    datum_not_operation.implements[0] = "org.example.photos.photo".into();
    refuse(
        datum_not_operation,
        "under this nodule's authority but is not a declared operation",
    );

    let mut self_edge = photos();
    self_edge.requires.push("org.example.search.query".into());
    refuse(self_edge, "both required and implemented");

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
        foreign.admit().unwrap().operations["org.example.tags.apply"],
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
    idle_handler.revisioned_stores[0].source = None;
    idle_handler.initialization.clear();
    idle_handler.admit().unwrap();
    Nodule::new("org.example.photos").admit().unwrap();
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
        "under this nodule's authority but is not implemented",
    );

    let mut unrequired = photos();
    unrequired.initialization[0].target = "org.example.tags.apply".into();
    refuse(unrequired, "foreign and not in `requires`");

    // A foreign operation this nodule implements is as good a target or source as a local one.
    let mut foreign_implemented = photos();
    foreign_implemented.initialization[0].target = "org.example.search.query".into();
    foreign_implemented.revisioned_stores[0].source = Some("org.example.search.query".into());
    foreign_implemented.admit().unwrap();

    let mut required = photos();
    required.requires.push("org.example.tags.apply".into());
    required.initialization[0].target = "org.example.tags.apply".into();
    required.admit().unwrap();

    let mut own_in_requires = photos();
    own_in_requires
        .requires
        .push("org.example.photos.list".into());
    refuse(own_in_requires, "`requires` lists foreign operations");

    let mut missing_source = photos();
    missing_source.revisioned_stores[0].source = Some("ingest".into());
    refuse(missing_source, "`ingest` is not an implemented operation");

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
    use crate::inventory::Readiness;
    use big_cabinet_core::DocId;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DatumStore;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::DatumsRead;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_plugs_core::Artifact;
    use big_plugs_core::Contribution;
    use big_plugs_core::Digest;
    use big_plugs_core::NoduleContribution;
    use big_plugs_core::Plug;
    use big_plugs_core::PlugId;
    use std::sync::Arc;

    fn plug_with(id: &str, version: &str, nodule: Nodule) -> Plug {
        let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
        plug.artifacts.push(Artifact {
            key: "handler".into(),
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

    fn settings(thumbnails: Value) -> IndexMap<String, Value> {
        [("thumbnails".to_owned(), thumbnails)]
            .into_iter()
            .collect()
    }

    /// The installed nodule `config` currently resolves for `id`, as the runtime would see it.
    fn resolved(config: &NoduleConfig, cache: &AdmissionCache, id: &PlugId) -> InstalledNodule {
        match config.resolve(cache).remove(id) {
            Some(NoduleResolution::Admitted { installed, .. }) => installed,
            other => panic!("expected `{id}` to resolve admitted, got {other:?}"),
        }
    }

    #[test]
    fn admits_plug_tracks_readiness_and_configures_settings() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let plug = plug_with("@example/photos", "1.0.0", photos());
        let id: PlugId = "@example/photos".parse().unwrap();

        let installed = config.admit(&cache, &plug, None).unwrap();
        assert_eq!(installed.plug_id, id);
        assert_eq!(installed.version, "1.0.0".parse().unwrap());
        assert_eq!(installed.revision, plug.revision());
        assert!(!installed.enabled);
        assert_eq!(installed.readiness(), Readiness::Disabled);
        assert!(!installed.readiness().is_ready());
        assert_eq!(
            installed.artifacts.get("handler").map(|(_, s)| *s),
            Some(ArtifactState::Unavailable)
        );
        assert_eq!(installed.config, Some(settings(json!(true))));
        assert_eq!(resolved(&config, &cache, &id), installed);

        let enabled = config.set_enabled(&id, true).unwrap();
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
    fn lexicon_only_nodule_reaches_executor_unavailable_when_enabled() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
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

        let installed = config.admit(&cache, &plug, None).unwrap();
        assert!(installed.artifacts.is_empty());
        assert_eq!(installed.readiness(), Readiness::Disabled);

        let enabled = config.set_enabled(&id, true).unwrap();
        assert_eq!(enabled.readiness(), Readiness::ExecutorUnavailable);
        assert!(!enabled.readiness().is_ready());
        assert!(matches!(
            config.configure(&id, IndexMap::new()),
            Err(InventoryError::NoConfigSchema(_))
        ));
    }

    #[test]
    fn upgrades_preserve_state_and_validate_configured_settings() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: PlugId = "@example/photos".parse().unwrap();
        let v1 = plug_with("@example/photos", "1.0.0", photos());
        config.admit(&cache, &v1, None).unwrap();
        config.set_enabled(&id, true).unwrap();

        // Unconfigured upgrade adopts the new version's defaults and keeps `enabled`.
        let mut nodule_v2 = photos();
        nodule_v2.config.as_mut().unwrap().defaults = Some(settings(json!(false)));
        let v2 = plug_with("@example/photos", "1.1.0", nodule_v2);
        let upgraded = config.admit(&cache, &v2, None).unwrap();
        assert!(upgraded.enabled);
        assert_eq!(upgraded.version, "1.1.0".parse().unwrap());
        assert_eq!(upgraded.config, Some(settings(json!(false))));

        // Explicit configuration is preserved across a compatible upgrade and rejected if the
        // new version's schema breaks it.
        config.configure(&id, settings(json!(true))).unwrap();
        let v3 = plug_with("@example/photos", "1.2.0", photos());
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
        let v4_bad = plug_with("@example/photos", "2.0.0", incompatible);
        // The upgrade is admitted even though it invalidates the explicit settings; resolution
        // refuses the nodule until the settings are rewritten (see the wedge test below).
        config.admit(&cache, &v4_bad, None).unwrap();
        assert!(matches!(
            config.resolve(&cache).get(&id),
            Some(NoduleResolution::Refused { version: Some(v), .. }) if *v == "2.0.0".parse().unwrap()
        ));
    }

    #[test]
    fn an_upgrade_that_invalidates_settings_is_admitted_refused_and_repaired_by_configure() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let id: PlugId = "@example/photos".parse().unwrap();
        let drawer = DocId::random();
        config
            .admit(
                &cache,
                &plug_with("@example/photos", "1.0.0", photos()),
                Some(BTreeMap::from([(drawer.clone(), DrawerAccess::Read)])),
            )
            .unwrap();
        config.set_enabled(&id, true).unwrap();
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
                &plug_with("@example/photos", "2.0.0", v2_nodule.clone()),
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
        config.set_enabled(&id, false).unwrap();
        config.set_enabled(&id, true).unwrap();
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
        let schemaless = plug_with("@example/photos", "3.0.0", {
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
        let id: PlugId = "@example/photos".parse().unwrap();
        let v1 = NoduleInstall::from_plug(&plug_with("@example/photos", "1.0.0", photos()), false);
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
        let v2 = NoduleInstall::from_plug(&plug_with("@example/photos", "1.1.0", photos()), false);
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
    fn authority_conflicts_cover_equal_ancestor_and_descendant_authorities() {
        let cache = AdmissionCache::new();
        // Admitting pairwise-conflicting nodules is refused by `admit`, so seed the config with
        // install datums the way an import would, then let `resolve` find the conflicts.
        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        for (plug_id, authority) in [
            ("@example/a", "org.example.a"),
            ("@example/b", "org.example.a.b"),
            ("@example/c", "org.example.ab"),
            ("@example/d", "org.example.d"),
            ("@example/e", "org.example.d"),
        ] {
            let plug = plug_with(plug_id, "1.0.0", Nodule::new(authority));
            read.datums.insert(
                install_key(&plug.id),
                DatumRead::Accepted {
                    value: serde_json::to_value(NoduleInstall::from_plug(&plug, false)).unwrap(),
                    version: DatumVersion::default(),
                },
            );
        }
        let config = NoduleConfig::hydrate(&read);
        let resolved = config.resolve(&cache);
        let refused = |plug_id: &str| {
            matches!(
                resolved.get(&plug_id.parse::<PlugId>().unwrap()),
                Some(NoduleResolution::Refused { .. })
            )
        };
        // `org.example.a` owns `org.example.a.b` but not the sibling `org.example.ab`.
        assert!(refused("@example/a") && refused("@example/b") && !refused("@example/c"));
        assert!(refused("@example/d") && refused("@example/e"));
    }

    #[test]
    fn refuses_missing_contribution_bad_schema_missing_artifact_and_authority_conflicts() {
        let mut config = NoduleConfig::new();
        let cache = AdmissionCache::new();
        let empty = Plug::new("@example/empty".parse().unwrap(), "0.1.0".parse().unwrap());
        assert!(matches!(
            config.admit(&cache, &empty, None),
            Err(InventoryError::MissingContribution(_))
        ));

        let mut bad_schema = plug_with("@example/photos", "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_schema.contributions[0] {
            n.schema = "org.example.other.nodule".into();
        }
        assert!(matches!(
            config.admit(&cache, &bad_schema, None),
            Err(InventoryError::UnsupportedSchema { .. })
        ));

        let mut bad_payload = plug_with("@example/photos", "1.0.0", photos());
        if let Contribution::Nodule(n) = &mut bad_payload.contributions[0] {
            n.description.insert("id".into(), json!(123));
        }
        assert!(matches!(
            config.admit(&cache, &bad_payload, None),
            Err(InventoryError::InvalidContribution { .. })
        ));

        let mut no_artifact = plug_with("@example/photos", "1.0.0", photos());
        no_artifact.artifacts.clear();
        assert!(matches!(
            config.admit(&cache, &no_artifact, None),
            Err(InventoryError::MissingArtifact { artifact, .. }) if artifact == "handler"
        ));
        let mut no_size = plug_with("@example/photos", "1.0.0", photos());
        no_size.artifacts[0].size = None;
        assert!(matches!(
            config.admit(&cache, &no_size, None),
            Err(InventoryError::ArtifactSizeRequired { artifact, .. }) if artifact == "handler"
        ));
        assert!(
            config.resolve(&cache).is_empty(),
            "refused admissions install nothing"
        );

        let valid = plug_with("@example/photos", "1.0.0", photos());
        config.admit(&cache, &valid, None).unwrap();

        let duplicate_authority = plug_with("@example/other-photos", "1.0.0", photos());
        assert!(matches!(
            config.admit(&cache, &duplicate_authority, None),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let sub_authority = Nodule::new("org.example.photos.sub");
        let sub_plug = plug_with("@example/sub-photos", "1.0.0", sub_authority);
        assert!(matches!(
            config.admit(&cache, &sub_plug, None),
            Err(InventoryError::AuthorityConflict { .. })
        ));

        let changed_plug = plug_with(
            "@example/photos",
            "2.0.0",
            Nodule::new("org.example.gallery"),
        );
        assert!(matches!(
            config.admit(&cache, &changed_plug, None),
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
    use big_plugs_core::Artifact;
    use big_plugs_core::Contribution;
    use big_plugs_core::Digest;
    use big_plugs_core::NoduleContribution;
    use big_plugs_core::Plug;
    use big_plugs_core::PlugId;

    fn sample_plug(id: &str, version: &str) -> Plug {
        let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
        plug.artifacts.push(Artifact {
            key: "handler".into(),
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
    fn refused_entries_take_the_nodule_id_from_the_nodule_contribution_only() {
        // A micro precedes the nodule and carries a string `description.id` of its own; the
        // nodule's schema is wrong so admission refuses and the id must come from the raw envelope.
        let mut plug = sample_plug("@example/photos", "1.0.0");
        let mut micro_description = IndexMap::new();
        micro_description.insert("id".to_owned(), json!("ui-button"));
        plug.contributions.insert(
            0,
            Contribution::Micro(big_plugs_core::MicroContribution {
                key: "button".into(),
                schema: "org.example.micro.button".into(),
                description: micro_description,
                extra: Default::default(),
            }),
        );
        let Some(Contribution::Nodule(nodule)) = plug.contributions.get_mut(1) else {
            panic!("sample plug carries its nodule after the micro");
        };
        nodule.schema = "org.example.other.nodule".into();

        let mut read = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        read.datums.insert(
            install_key(&plug.id),
            DatumRead::Accepted {
                value: serde_json::to_value(NoduleInstall::from_plug(&plug, false)).unwrap(),
                version: DatumVersion::default(),
            },
        );
        let config = NoduleConfig::hydrate(&read);
        let Some(NoduleResolution::Refused { nodule_id, .. }) =
            config.resolve(&Default::default()).remove(&plug.id)
        else {
            panic!("a nodule with an unsupported schema is refused");
        };
        assert_eq!(nodule_id.as_deref(), Some("org.example.photos"));
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

    /// The `Blob` datum stored for `plug`'s handler artifact, as the document holds it.
    fn stored_artifact(plug: &Plug) -> Value {
        let artifact = plug.artifact("handler").unwrap();
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
        }
    }

    #[test]
    fn handler_artifacts_are_referenced_by_blob_datums_that_follow_the_install() {
        let id: PlugId = "@example/photos".parse().unwrap();
        let slot = artifact_key(&id, "handler");
        assert_eq!(
            slot.to_string(),
            "org.example.daybook.blob/nodule.artifact/@example/photos/handler"
        );
        let foreign = DatumKey::from_text(big_blobs_core::Blob::ID, "avatar").unwrap();
        let malformed =
            DatumKey::from_text(big_blobs_core::Blob::ID, "nodule.artifact/garbage").unwrap();
        assert!(!NoduleConfig::owns(&foreign));
        assert!(NoduleConfig::owns(&malformed));
        assert!(NoduleConfig::owns(&slot));

        // Admitting writes the install and the reference in the same patch, and the reference
        // is the datum wire form big_blobs reads (no `null` for the unset options).
        let v1 = sample_plug("@example/photos", "1.0.0");
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
        let mut v2 = sample_plug("@example/photos", "1.1.0");
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
