use super::*;
use big_cabinet_core::datum::DatumDecl;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Stored;
use big_cabinet_core::datum::TypeDecl;
use serde_json::json;

fn record(tag: &str, value: Value) -> Value {
    json!({"lexicon": 1, "id": tag, "defs": {"main": {"type": "record", "key": "tid", "record": {"type": "object", "required": ["value"], "properties": {"value": value}}}}})
}

fn registration(tag: &str, value: Value) -> SchemaRegistration {
    SchemaRegistration::new(tag, record(tag, value))
}

fn universe(tag: &str, value: Value) -> Universe {
    Universe {
        datums: vec![DatumDecl {
            id: tag.to_owned(),
            name: "Main".to_owned(),
            schema: schemars::Schema::try_from(
                json!({"type": "object", "properties": {"value": value}}),
            )
            .unwrap(),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    }
}

#[test]
fn independently_authored_records_are_authoritative_and_handles_are_owned() {
    let tag = "org.example.schema.note";
    let repo = SchemaRepo::open([SchemaRegistrationGroup::new(
        None,
        [registration(tag, json!({"type": "string", "maxLength": 2}))],
        Vec::new(),
    )])
    .unwrap();
    let first = repo.resolve_for_read(tag).unwrap();
    let second = repo.require_for_write(tag).unwrap();
    drop(repo);
    assert!(Arc::ptr_eq(&first.inner, &second.inner));
    assert!(std::ptr::eq(first.compiled(), second.compiled()));
    assert!(std::ptr::eq(
        first.compiled().document(),
        first.compiled().clone().document()
    ));
    assert!(first.sidecar().is_none());
    let value = json!({"$type": tag, "value": "é", "future": true});
    assert_eq!(
        Stored::check(first.compiled(), value.clone())
            .unwrap()
            .into_value(),
        value
    );
    for value in [
        json!({"value": "a"}),
        json!({"$type": tag}),
        json!({"$type": "org.example.wrong", "value": "a"}),
        json!({"$type": tag, "value": "abc"}),
    ] {
        assert!(Stored::check(first.compiled(), value).is_err());
    }
}

#[test]
fn dependency_storage_annotations_are_owned_without_becoming_validation_authority() {
    let tag = "org.example.schema.text";
    let repo = {
        let cartridge = universe(tag, json!({"$ref": "#/$defs/Carried"}));
        let dependency = Universe {
            datums: Vec::new(),
            types: vec![TypeDecl {
                name: "Carried".to_owned(),
                schema: schemars::Schema::try_from(
                    json!({"type": "string", "x-automerge": "text"}),
                )
                .unwrap(),
            }],
        };
        SchemaRepo::open([SchemaRegistrationGroup::new(
            Some(Bundle::new(&cartridge, vec![&dependency])),
            [registration(tag, json!({"type": "string", "minLength": 2}))],
            Vec::new(),
        )])
        .unwrap()
    };
    let handle = repo.require_for_write(tag).unwrap();
    assert!(handle.sidecar().unwrap().document("Main").is_some());
    assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "ab"})).is_ok());
    assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "a"})).is_err());
    assert!(
        handle
            .compiled()
            .document()
            .to_string()
            .contains("x-automerge")
    );
}

#[test]
fn semantic_contradictions_and_unapproved_validation_constraints_fail_open() {
    let tag = "org.example.schema.invalid";
    for supplement in [
        json!({"type": "integer"}),
        json!({"type": "string", "pattern": "a"}),
        json!({"type": "string", "maxLength": 2}),
        json!({"type": "string", "enum": ["a"]}),
        json!({"type": "string", "x-automerge": "counter"}),
        json!({"type": "string", "format": "email"}),
    ] {
        let universe = universe(tag, supplement.clone());
        assert!(matches!(
            SchemaRepo::open([SchemaRegistrationGroup::new(
                Some(Bundle::new(&universe, Vec::new())),
                [registration(tag, json!({"type": "string"}))],
                Vec::new()
            )]),
            Err(SchemaRepoError::Admission { .. })
        ));
    }
}

#[test]
fn external_lexicons_must_be_carried_and_are_owned_in_the_compiled_context() {
    let tag = "org.example.schema.ref";
    let external = json!({"lexicon": 1, "id": "org.example.types", "defs": {"label": {"type": "string", "enum": ["carried"]}}});
    let make = || {
        registration(
            tag,
            json!({"type": "ref", "ref": "org.example.types#label"}),
        )
    };
    assert!(SchemaRepo::open([SchemaRegistrationGroup::new(None, [make()], Vec::new())]).is_err());
    let repo =
        SchemaRepo::open([SchemaRegistrationGroup::new(None, [make()], vec![external])]).unwrap();
    let handle = repo.require_for_write(tag).unwrap();
    drop(repo);
    assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "carried"})).is_ok());
    assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "other"})).is_err());
}

#[test]
fn peer_registrations_are_carried_without_cross_group_discovery() {
    let first = "org.example.schema.first";
    let second = "org.example.schema.second";
    let make = || registration(first, json!({"type": "ref", "ref": second}));
    let peer = || registration(second, json!({"type": "string", "maxLength": 2}));
    let repo = SchemaRepo::open([SchemaRegistrationGroup::new(
        None,
        [make(), peer()],
        Vec::new(),
    )])
    .unwrap();
    let handle = repo.require_for_write(first).unwrap();
    assert!(
        Stored::check(
            handle.compiled(),
            json!({"$type": first, "value": {"$type": second, "value": "ok"}})
        )
        .is_ok()
    );
    assert!(
        Stored::check(
            handle.compiled(),
            json!({"$type": first, "value": {"$type": second, "value": "long"}})
        )
        .is_err()
    );
    assert!(
        SchemaRepo::open([
            SchemaRegistrationGroup::new(None, [make()], Vec::new()),
            SchemaRegistrationGroup::new(None, [peer()], Vec::new()),
        ])
        .is_err()
    );
}

#[test]
fn independent_groups_keep_same_named_supplements_isolated() {
    let first_tag = "org.example.schema.first";
    let second_tag = "org.example.schema.second";
    let first = universe(first_tag, json!({"type": "string", "x-automerge": "text"}));
    let second = universe(
        second_tag,
        json!({"type": "integer", "x-automerge": "counter"}),
    );
    let repo = SchemaRepo::open([
        SchemaRegistrationGroup::new(
            Some(Bundle::new(&first, Vec::new())),
            [registration(first_tag, json!({"type": "string"}))],
            Vec::new(),
        ),
        SchemaRegistrationGroup::new(
            Some(Bundle::new(&second, Vec::new())),
            [registration(second_tag, json!({"type": "integer"}))],
            Vec::new(),
        ),
    ])
    .unwrap();
    let first = repo.require_for_write(first_tag).unwrap();
    let second = repo.require_for_write(second_tag).unwrap();
    assert!(!Arc::ptr_eq(
        first.inner.sidecar.as_ref().unwrap(),
        second.inner.sidecar.as_ref().unwrap()
    ));
    assert!(
        Stored::check(
            first.compiled(),
            json!({"$type": first_tag, "value": "text"})
        )
        .is_ok()
    );
    assert!(Stored::check(second.compiled(), json!({"$type": second_tag, "value": 2})).is_ok());
    assert!(
        Stored::check(
            second.compiled(),
            json!({"$type": second_tag, "value": "text"})
        )
        .is_err()
    );
}

#[test]
fn duplicates_identity_and_malformed_documents_fail_atomically() {
    let tag = "org.example.schema.note";
    let make = || registration(tag, json!({"type": "string"}));
    assert!(matches!(
        SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [make(), make()],
            Vec::new()
        )]),
        Err(SchemaRepoError::DuplicateDatumTag(_))
    ));
    for document in [
        Value::Null,
        json!({"lexicon": 2, "id": tag}),
        record("org.example.other", json!({"type": "string"})),
        record(tag, json!({"type": "string", "maxLength": "two"})),
        record(tag, json!({"type": "ref", "ref": "#missing"})),
    ] {
        assert!(matches!(
            SchemaRepo::open([SchemaRegistrationGroup::new(
                None,
                [SchemaRegistration::new(tag, document)],
                Vec::new()
            )]),
            Err(SchemaRepoError::Admission { .. })
        ));
    }
}

#[test]
fn core_datums_are_admitted_like_registered_ones_and_keep_their_narrowing() {
    let tag = "org.example.daybook.dmeta";
    let repo = SchemaRepo::open(Vec::new()).unwrap();
    let handle = repo.require_for_write(tag).unwrap();
    assert!(handle.sidecar().is_some());
    let valid = json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z"});
    assert!(Stored::check(handle.compiled(), valid).is_ok());
    for invalid in [
        json!({"$type": tag, "createdAt": "not an instant"}),
        json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "actors": {"not-a-uuid": {"pubkey": "z", "actorUrl": "https://example.test"}}}),
        json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "datums": {"org.example.note/main": {"deletedAt": [], "signatures": [{"signature": {"$bytes": "AQ=="}}]}}}),
        json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "actors": {"01a15688-2519-7ea0-a1ea-421237f5c0cc": {"pubkey": "z", "actorUrl": "https://example.test", "delegation": "not bytes"}}}),
    ] {
        assert!(
            Stored::check(handle.compiled(), invalid.clone()).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn reference_tags_select_the_datums_that_declare_references() {
    let referencing = "org.example.schema.referencing";
    let plain = "org.example.schema.plain";
    let repo = SchemaRepo::open([
        SchemaRegistrationGroup::new(
            None,
            [registration(
                referencing,
                json!({"type": "ref", "ref": "org.example.daybook.defs#datumRef"}),
            )],
            Vec::new(),
        ),
        SchemaRegistrationGroup::new(
            None,
            [registration(plain, json!({"type": "string"}))],
            Vec::new(),
        ),
    ])
    .unwrap();
    let tags = repo.reference_tags();
    // The core branch datum carries a `DatumPin`, so it is selected alongside the
    // registered referencing datum; the plain one and dmeta are not.
    assert_eq!(
        tags,
        vec![
            "org.example.daybook.branch".to_owned(),
            referencing.to_owned()
        ]
    );
}

#[test]
fn unknown_reads_are_best_effort_but_unknown_writes_are_refused() {
    let repo = SchemaRepo::open(Vec::new()).unwrap();
    assert!(repo.resolve_for_read("org.example.unknown").is_none());
    assert!(matches!(
        repo.require_for_write("org.example.unknown"),
        Err(SchemaRepoError::UnknownDatum { .. })
    ));
}

fn cartridge_group(tags: &[&str]) -> SchemaRegistrationGroup<'static> {
    SchemaRegistrationGroup::new(
        None,
        tags.iter()
            .map(|tag| registration(tag, json!({"type": "string"}))),
        Vec::new(),
    )
}

#[test]
fn an_owners_schemas_register_replace_and_retire_live() {
    let repo = SchemaRepo::open([]).unwrap();
    repo.register(
        "cartridge-a",
        cartridge_group(&["org.example.a.one", "org.example.a.two"]),
    )
    .unwrap();
    assert!(repo.resolve_for_read("org.example.a.one").is_some());
    assert!(!repo.is_boot_schema("org.example.a.one"));
    assert!(repo.is_boot_schema(Dmeta::ID));
    // A handle outlives the retirement of its schema; only new readers stop resolving it.
    let held = repo.resolve_for_read("org.example.a.one").unwrap();

    // Registering again replaces the owner's set, so `two` goes and `three` arrives.
    repo.register(
        "cartridge-a",
        cartridge_group(&["org.example.a.one", "org.example.a.three"]),
    )
    .unwrap();
    assert!(repo.resolve_for_read("org.example.a.two").is_none());
    assert!(repo.resolve_for_read("org.example.a.three").is_some());

    repo.retire("cartridge-a");
    assert!(repo.resolve_for_read("org.example.a.one").is_none());
    assert_eq!(held.datum_tag(), "org.example.a.one");
    assert!(
        repo.resolve_for_read(Dmeta::ID).is_some(),
        "retiring never touches the boot set"
    );
    repo.retire("cartridge-a");
}

#[test]
fn a_registration_never_shadows_and_a_refusal_leaves_the_owner_untouched() {
    let repo = SchemaRepo::open([]).unwrap();
    repo.register("cartridge-a", cartridge_group(&["org.example.a.one"]))
        .unwrap();
    assert!(matches!(
        repo.register("cartridge-b", cartridge_group(&["org.example.a.one"])),
        Err(SchemaRepoError::DuplicateDatumTag(_))
    ));
    assert!(matches!(
        repo.register("cartridge-b", cartridge_group(&[Dmeta::ID])),
        Err(SchemaRepoError::DuplicateDatumTag(_))
    ));
    // A group that fails to compile does not retire what the owner already had.
    let broken = SchemaRegistrationGroup::new(
        None,
        [SchemaRegistration::new(
            "org.example.a.two",
            json!({"lexicon": 2}),
        )],
        Vec::new(),
    );
    assert!(matches!(
        repo.register("cartridge-a", broken),
        Err(SchemaRepoError::Admission { .. })
    ));
    assert!(repo.resolve_for_read("org.example.a.one").is_some());
    assert!(repo.resolve_for_read("org.example.a.two").is_none());
}

#[test]
fn a_carried_copy_of_the_reference_defs_must_be_the_nodes() {
    let tag = "org.example.schema.note";
    let mut vendored = big_cabinet_core::reference::defs_lexicon();
    let make = |defs: Value| {
        SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [registration(
                tag,
                json!({"type": "ref", "ref": "org.example.daybook.defs#datumRef"}),
            )],
            vec![defs],
        )])
    };
    make(vendored.clone()).expect("the node's own copy is accepted");
    vendored["defs"]["datumRef"]["format"] = json!("at-uri");
    assert!(matches!(
        make(vendored),
        Err(SchemaRepoError::ConflictingDefs(_))
    ));
}
