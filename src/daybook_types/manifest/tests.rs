use super::*;

#[test]
fn processor_event_predicate_defaults_to_local_any() {
    let predicate = ProcessorEventPredicate::default();
    assert!(matches!(
        predicate.node_predicate,
        NodePredicate::ChangeOrigin(ChangeOriginDeets::Local)
    ));
    assert!(matches!(
        predicate.doc_change_predicate,
        DocChangePredicate::Any
    ));
}

#[test]
fn doc_processor_event_predicate_is_optional_in_serde() {
    let json = serde_json::json!({
        "desc": "x",
        "deets": {
            "docProcessor": {
                "predicate": { "hasTag": "org.example.tag" },
                "routineName": "routine1"
            }
        }
    });
    let manifest: ProcessorManifest = serde_json::from_value(json).expect("valid manifest");
    let ProcessorDeets::DocProcessor {
        event_predicate, ..
    } = manifest.deets;
    assert!(matches!(
        event_predicate.node_predicate,
        NodePredicate::ChangeOrigin(ChangeOriginDeets::Local)
    ));
    assert!(matches!(
        event_predicate.doc_change_predicate,
        DocChangePredicate::Any
    ));
}

#[test]
fn doc_change_predicate_changed_facet_tags_evaluate_change() {
    use std::collections::HashSet;

    let changed: HashSet<crate::doc::FacetKey> = vec![
        crate::doc::FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        },
        crate::doc::FacetKey {
            tag: "org.example.todo".into(),
            id: "1".into(),
        },
    ]
    .into_iter()
    .collect();

    let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()]);
    assert!(pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));

    let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.unknown".into()]);
    assert!(!pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
}

#[test]
fn doc_change_predicate_changed_facet_keys_evaluate_change() {
    use std::collections::HashSet;

    let target_key = crate::doc::FacetKey {
        tag: "org.example.todo".into(),
        id: "1".into(),
    };
    let changed: HashSet<crate::doc::FacetKey> = vec![target_key.clone()].into_iter().collect();

    let pred = DocChangePredicate::ChangedFacetKeys(vec![target_key]);
    assert!(pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
    assert!(!pred.evaluate_change(DocChangeKind::Updated, None, None, None));

    let pred = DocChangePredicate::ChangedFacetKeys(vec![crate::doc::FacetKey {
        tag: "org.example.todo".into(),
        id: "nope".into(),
    }]);
    assert!(!pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
}

#[test]
fn doc_change_predicate_append_referenced_facet_scope() {
    use std::collections::HashSet;

    let mut read_tags = HashSet::new();
    let mut read_keys = HashSet::new();
    DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()])
        .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
    assert!(read_tags.contains("org.example.todo"));
    assert!(read_keys.is_empty());

    let key = crate::doc::FacetKey {
        tag: "org.example.todo".into(),
        id: "1".into(),
    };
    DocChangePredicate::ChangedFacetKeys(vec![key.clone()])
        .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
    assert!(read_keys.contains(&key));
}

#[test]
fn facet_field_match_eq_string() {
    let tag: FacetTag = "org.example.note".into();
    let note_json = serde_json::json!({
        "mime": "text/x-hledger-journal",
        "content": "2024-01-01 test\n  assets:cash  $10\n  expenses:food"
    });
    let facets = vec![(
        crate::doc::FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        },
        note_json,
    )];

    let predicate = DocPredicateClause::FacetFieldMatch {
        tag: tag.clone(),
        json_path: "$.mime".into(),
        operator: CompareOp::Eq,
        value: serde_json::Value::String("text/x-hledger-journal".into()),
    };

    let mut requirements = HashSet::new();
    predicate.append_requirements(&mut requirements);
    assert!(requirements.contains(&DocPredicateEvalRequirement::FacetsOfTag(tag.clone())));

    let mut resolved = HashMap::new();
    resolved.insert(
        DocPredicateEvalRequirement::FacetsOfTag(tag),
        DocPredicateEvalResolved::FacetsOfTag(facets),
    );

    let doc = crate::doc::Doc {
        id: "doc1".into(),
        facets: HashMap::new(),
    };

    assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
}

#[test]
fn has_tag_requests_full_facet_resolution() {
    let tag: FacetTag = "org.example.embedding".into();
    let predicate = DocPredicateClause::HasTag(tag.clone());
    let mut requirements = HashSet::new();

    predicate.append_requirements(&mut requirements);

    assert!(requirements.contains(&DocPredicateEvalRequirement::FacetsOfTag(tag)));
}

#[test]
fn facet_field_match_ne_string() {
    let tag: FacetTag = "org.example.note".into();
    let note_json = serde_json::json!({
        "mime": "text/plain",
        "content": "hello"
    });
    let facets = vec![(
        crate::doc::FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        },
        note_json,
    )];

    let predicate = DocPredicateClause::FacetFieldMatch {
        tag: tag.clone(),
        json_path: "$.mime".into(),
        operator: CompareOp::Ne,
        value: serde_json::Value::String("text/x-hledger-journal".into()),
    };

    let mut resolved = HashMap::new();
    resolved.insert(
        DocPredicateEvalRequirement::FacetsOfTag(tag),
        DocPredicateEvalResolved::FacetsOfTag(facets),
    );

    let doc = crate::doc::Doc {
        id: "doc1".into(),
        facets: HashMap::new(),
    };

    assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
}

#[test]
fn facet_field_match_gt_numeric() {
    let tag: FacetTag = "org.example.note".into();
    let note_json = serde_json::json!({
        "confidence": 0.95
    });
    let facets = vec![(
        crate::doc::FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        },
        note_json,
    )];

    let predicate = DocPredicateClause::FacetFieldMatch {
        tag: tag.clone(),
        json_path: "$.confidence".into(),
        operator: CompareOp::Gt,
        value: serde_json::json!(0.5),
    };

    let mut resolved = HashMap::new();
    resolved.insert(
        DocPredicateEvalRequirement::FacetsOfTag(tag),
        DocPredicateEvalResolved::FacetsOfTag(facets),
    );

    let doc = crate::doc::Doc {
        id: "doc1".into(),
        facets: HashMap::new(),
    };

    assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
}

#[test]
fn facet_field_match_mismatch() {
    let tag: FacetTag = "org.example.note".into();
    let note_json = serde_json::json!({
        "mime": "text/plain"
    });
    let facets = vec![(
        crate::doc::FacetKey {
            tag: "org.example.note".into(),
            id: "main".into(),
        },
        note_json,
    )];

    let predicate = DocPredicateClause::FacetFieldMatch {
        tag: tag.clone(),
        json_path: "$.mime".into(),
        operator: CompareOp::Eq,
        value: serde_json::Value::String("text/x-hledger-journal".into()),
    };

    let mut resolved = HashMap::new();
    resolved.insert(
        DocPredicateEvalRequirement::FacetsOfTag(tag),
        DocPredicateEvalResolved::FacetsOfTag(facets),
    );

    let doc = crate::doc::Doc {
        id: "doc1".into(),
        facets: HashMap::new(),
    };

    assert!(!predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
}

#[test]
fn facet_field_match_deserialization() {
    let json = serde_json::json!({
        "facetFieldMatch": {
            "tag": "org.example.note",
            "jsonPath": "$.mime",
            "operator": "eq",
            "value": "text/x-hledger-journal"
        }
    });

    let clause: DocPredicateClause = serde_json::from_value(json).expect("valid clause");
    assert!(matches!(clause, DocPredicateClause::FacetFieldMatch {
        json_path, ..
    } if json_path == "$.mime"));
}

#[test]
fn compare_json_values_eq_neq() {
    assert!(compare_json_values(
        &serde_json::json!("hello"),
        CompareOp::Eq,
        &serde_json::json!("hello"),
    ));
    assert!(!compare_json_values(
        &serde_json::json!("hello"),
        CompareOp::Eq,
        &serde_json::json!("world"),
    ));
    assert!(compare_json_values(
        &serde_json::json!("hello"),
        CompareOp::Ne,
        &serde_json::json!("world"),
    ));
    assert!(compare_json_values(
        &serde_json::json!(1),
        CompareOp::Eq,
        &serde_json::json!(1.0),
    ));
    assert!(!compare_json_values(
        &serde_json::json!(1),
        CompareOp::Ne,
        &serde_json::json!(1.0),
    ));
}

#[test]
fn compare_json_values_numeric() {
    assert!(compare_json_values(
        &serde_json::json!(10.0),
        CompareOp::Gt,
        &serde_json::json!(5.0),
    ));
    assert!(compare_json_values(
        &serde_json::json!(18446744073709551615u64),
        CompareOp::Gt,
        &serde_json::json!(18446744073709551614u64),
    ));
    assert!(compare_json_values(
        &serde_json::json!(18446744073709551615u64),
        CompareOp::Eq,
        &serde_json::json!(18446744073709551615u64),
    ));
    assert!(!compare_json_values(
        &serde_json::json!(3.0),
        CompareOp::Gt,
        &serde_json::json!(5.0),
    ));
    assert!(compare_json_values(
        &serde_json::json!(5.0),
        CompareOp::Gte,
        &serde_json::json!(5.0),
    ));
    assert!(compare_json_values(
        &serde_json::json!(3.0),
        CompareOp::Lt,
        &serde_json::json!(5.0),
    ));
}

#[test]
fn routine_manifest_read_facet_set_from_doc_acls() {
    let manifest = RoutineManifest {
        r#impl: RoutineImpl::Wflow {
            bundle: "test".into(),
            key: "test".into(),
        },
        doc_acls: vec![
            RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                facet_acl: vec![
                    RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: "org.example.note".into(),
                        key_id: None,
                        read: true,
                        write: false,
                        create: false,
                        delete: false,
                    },
                    RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: "org.example.blob".into(),
                        key_id: Some("main".into()),
                        read: true,
                        write: true,
                        create: false,
                        delete: false,
                    },
                ],
            },
            RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.todo".into()),
                facet_acl: vec![RoutineFacetAccess {
                    owner_plug_id: None,
                    tag: "org.example.todo".into(),
                    key_id: None,
                    read: false,
                    write: true,
                    create: false,
                    delete: false,
                }],
            },
        ],
        query_acls: vec![],
        config_facet_acl: vec![],
        command_invoke_acl: vec![],
        local_state_acl: vec![],
    };

    let (read_tags, read_keys) = manifest.read_facet_set();
    assert!(read_tags.contains("org.example.note"));
    assert!(!read_tags.contains("org.example.blob"));
    assert!(!read_tags.contains("org.example.todo"));
    assert_eq!(read_keys.len(), 1);
    assert!(read_keys.contains(&crate::doc::FacetKey {
        tag: "org.example.blob".into(),
        id: "main".into(),
    }));
}

#[test]
fn routine_manifest_facet_acl_unions_all_doc_acls() {
    let manifest = RoutineManifest {
        r#impl: RoutineImpl::Wflow {
            bundle: "test".into(),
            key: "test".into(),
        },
        doc_acls: vec![
            RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                facet_acl: vec![RoutineFacetAccess {
                    owner_plug_id: None,
                    tag: "org.example.note".into(),
                    key_id: None,
                    read: true,
                    write: false,
                    create: false,
                    delete: false,
                }],
            },
            RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.todo".into()),
                facet_acl: vec![RoutineFacetAccess {
                    owner_plug_id: None,
                    tag: "org.example.todo".into(),
                    key_id: None,
                    read: false,
                    write: true,
                    create: false,
                    delete: false,
                }],
            },
        ],
        query_acls: vec![],
        config_facet_acl: vec![],
        command_invoke_acl: vec![],
        local_state_acl: vec![],
    };

    let acl = manifest.facet_acl();
    assert_eq!(acl.len(), 2);
    assert!(acl.iter().any(|a| a.tag.0 == "org.example.note" && a.read));
    assert!(acl.iter().any(|a| a.tag.0 == "org.example.todo" && a.write));
}

#[test]
fn routine_manifest_referenced_tags_includes_doc_predicate_and_query_acls() {
    let manifest = RoutineManifest {
        r#impl: RoutineImpl::Wflow {
            bundle: "test".into(),
            key: "test".into(),
        },
        doc_acls: vec![RoutineDocAcl {
            doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
            facet_acl: vec![RoutineFacetAccess {
                owner_plug_id: None,
                tag: "org.example.blob".into(),
                key_id: None,
                read: true,
                write: false,
                create: false,
                delete: false,
            }],
        }],
        query_acls: vec![DocPredicateClause::HasTag("org.example.todo".into())],
        config_facet_acl: vec![],
        command_invoke_acl: vec![],
        local_state_acl: vec![],
    };

    let tags = manifest.referenced_tags();
    assert!(tags.contains(&FacetTag::from("org.example.note")));
    assert!(tags.contains(&FacetTag::from("org.example.blob")));
    assert!(tags.contains(&FacetTag::from("org.example.todo")));
}

#[test]
fn routine_manifest_serializes_with_flat_acl_fields() {
    let manifest = RoutineManifest {
        r#impl: RoutineImpl::Wflow {
            bundle: "test".into(),
            key: "test".into(),
        },
        doc_acls: vec![RoutineDocAcl {
            doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
            facet_acl: vec![RoutineFacetAccess {
                owner_plug_id: None,
                tag: "org.example.note".into(),
                key_id: None,
                read: true,
                write: false,
                create: false,
                delete: false,
            }],
        }],
        query_acls: vec![DocPredicateClause::HasTag("org.example.todo".into())],
        config_facet_acl: vec![RoutineFacetAccess {
            owner_plug_id: Some("@daybook/test".into()),
            tag: "org.example.config".into(),
            key_id: None,
            read: true,
            write: true,
            create: false,
            delete: false,
        }],
        command_invoke_acl: vec![],
        local_state_acl: vec![],
    };

    let json = serde_json::to_value(&manifest).expect("serialize");
    assert!(json.get("docAcls").is_some(), "docAcls field should exist");
    assert!(
        json.get("queryAcls").is_some(),
        "queryAcls field should exist"
    );
    assert!(
        json.get("configFacetAcl").is_some(),
        "configFacetAcl field should exist"
    );
    assert!(
        json.get("deets").is_none(),
        "old RoutineManifestDeets enum should not exist"
    );
}

#[test]
fn routine_manifest_deserializes_flat_acl_fields() {
    let json = serde_json::json!({
        "impl": {
            "wflow": {
                "bundle": "test",
                "key": "test"
            }
        },
        "docAcls": [
            {
                "docPredicate": { "hasTag": "org.example.note" },
                "facetAcl": [
                    {
                        "tag": "org.example.note",
                        "read": true,
                        "write": false
                    }
                ]
            }
        ],
        "queryAcls": [
            { "hasTag": "org.example.todo" }
        ],
        "configFacetAcl": [
            {
                "tag": "org.example.config",
                "read": true,
                "write": true
            }
        ]
    });

    let manifest: RoutineManifest = serde_json::from_value(json).expect("deserialize");
    assert_eq!(manifest.doc_acls.len(), 1);
    assert_eq!(manifest.query_acls.len(), 1);
    assert_eq!(manifest.config_facet_acl.len(), 1);
    assert_eq!(manifest.local_state_acl.len(), 0);
}

#[test]
fn plug_manifest_json_schema_generates() {
    // ADR 007 §1: the plugManifest facet's value schema is schema_for!(PlugManifest).
    let root = schemars::schema_for!(PlugManifest);
    let json = serde_json::to_value(&root).expect("schema serializes");
    assert!(json.is_object());
    assert!(
        serde_json::to_string(&root)
            .expect("schema to string")
            .len()
            > 100,
        "generated schema should be non-trivial"
    );
}
