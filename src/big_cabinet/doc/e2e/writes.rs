//! The ordinary write surface: whole-datum values, explicit bases, and the four write modes.

use super::*;

#[tokio::test]
async fn a_created_document_reads_back_its_datums_at_the_creation_heads() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let receipt = node
        .create(&alice, "create", [(key("main"), note("hello"))])
        .await
        .unwrap();
    let read = node
        .read(&alice, &receipt.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, receipt.heads);
    let (value, datum_heads) = accepted(&read.datums[&key("main")]);
    assert_eq!(value, &note("hello"));
    assert_eq!(datum_heads, receipt.heads.as_slice());
    assert_eq!(read.datums.len(), 1, "dmeta is not a readable datum");
    node.stop().await;
}

#[tokio::test]
async fn unmentioned_datums_keep_their_value_and_datum_heads() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(
            &alice,
            "create",
            [(key("a"), note("a0")), (key("b"), note("b0"))],
        )
        .await
        .unwrap();
    let document = &created.doc_id;
    let written = node
        .write(
            &alice,
            document,
            WriteMode::AtHeads {
                heads: created.heads.clone(),
            },
            [(key("a"), note("a1"))],
            [],
        )
        .await
        .unwrap();
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, written.heads);
    assert_eq!(
        accepted(&read.datums[&key("a")]),
        (&note("a1"), written.heads.as_slice())
    );
    assert_eq!(
        accepted(&read.datums[&key("b")]),
        (&note("b0"), created.heads.as_slice())
    );
    let historical = node
        .read(&alice, document, At::Heads(created.heads.clone()), false)
        .await
        .unwrap();
    assert_eq!(accepted(&historical.datums[&key("a")]).0, &note("a0"));
    node.stop().await;
}

#[tokio::test]
async fn removal_is_a_verified_absence_and_a_later_set_resurrects_the_slot() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let removed = node
        .write(&alice, document, WriteMode::AtCurrent, [], [key("main")])
        .await
        .unwrap();
    assert!(
        matches!(
            node.read_one(&alice, document, At::Current, &key("main")).await,
            DatumRead::Absent { version } if version.heads == removed.heads && version.clock > 0
        ),
        "a removal is an absence at the removal's version"
    );
    let listing = op(node
        .client()
        .call(
            RequestContext::root(),
            ListDatums {
                capability: node
                    .capability(
                        &alice,
                        DocScope {
                            read: BTreeSet::from([document.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await
                    .unwrap(),
                doc_id: document.clone(),
                at: At::Current,
            },
        )
        .await)
    .unwrap();
    assert!(listing.datums.is_empty(), "a removed slot is not listed");
    let back = node
        .write(
            &alice,
            document,
            WriteMode::IfDatumsUnchanged {
                datums: BTreeMap::from([(
                    key("main"),
                    DatumVersion {
                        heads: removed.heads.clone(),
                        clock: 0,
                    },
                )]),
            },
            [(key("main"), note("y"))],
            [],
        )
        .await
        .unwrap();
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, document, At::Current, &key("main"))
                .await
        ),
        (&note("y"), back.heads.as_slice())
    );
    node.stop().await;
}

#[tokio::test]
async fn refused_writes_commit_nothing() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let unknown: DatumKey = "org.example.unknown/main".parse().unwrap();
    let dmeta: DatumKey = "org.example.daybook.dmeta/main".parse().unwrap();
    let branch: DatumKey = "org.example.daybook.branch/main".parse().unwrap();
    let cases = [
        (
            vec![(unknown.clone(), json!({}))],
            vec![],
            DocsError::UnknownSchema {
                datum_tag: "org.example.unknown".into(),
            },
        ),
        (
            vec![(dmeta.clone(), json!({}))],
            vec![],
            DocsError::SystemDatum {
                datum_key: dmeta.clone(),
            },
        ),
        (
            vec![(branch.clone(), json!({"path": "/x"}))],
            vec![],
            DocsError::SystemDatum {
                datum_key: branch.clone(),
            },
        ),
        (
            vec![],
            vec![branch.clone()],
            DocsError::SystemDatum {
                datum_key: branch.clone(),
            },
        ),
        (
            vec![(key("main"), note("y"))],
            vec![key("main")],
            DocsError::SetAndRemoved {
                datum_key: key("main"),
            },
        ),
    ];
    for (set, remove, expected) in cases {
        assert_eq!(
            node.write(&alice, document, WriteMode::AtCurrent, set, remove)
                .await
                .unwrap_err(),
            expected
        );
    }
    let invalid = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("main"), json!({"$type": NOTE, "value": 7}))],
            [],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(invalid, DocsError::InvalidValue { .. }),
        "{invalid:?}"
    );
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, created.heads);
    node.stop().await;
}

#[tokio::test]
async fn conditional_modes_compare_under_the_commit_and_report_what_changed() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(
            &alice,
            "create",
            [(key("a"), note("a")), (key("b"), note("b"))],
        )
        .await
        .unwrap();
    let document = &created.doc_id;
    let moved = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("b"), note("b1"))],
            [],
        )
        .await
        .unwrap();
    assert_eq!(
        node.write(
            &alice,
            document,
            WriteMode::IfDocUnchanged {
                heads: created.heads.clone()
            },
            [(key("a"), note("a1"))],
            [],
        )
        .await
        .unwrap_err(),
        DocsError::Conflict {
            heads: moved.heads.clone(),
            datums: BTreeMap::new(),
        }
    );
    // Another datum moving does not break a datum-scoped condition.
    node.write(
        &alice,
        document,
        WriteMode::IfDatumsUnchanged {
            datums: BTreeMap::from([(
                key("a"),
                DatumVersion {
                    heads: created.heads.clone(),
                    clock: 0,
                },
            )]),
        },
        [(key("a"), note("a1"))],
        [],
    )
    .await
    .unwrap();
    let stale = node
        .write(
            &alice,
            document,
            WriteMode::IfDatumsUnchanged {
                datums: BTreeMap::from([(
                    key("b"),
                    DatumVersion {
                        heads: created.heads.clone(),
                        clock: 0,
                    },
                )]),
            },
            [(key("b"), note("b2"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&stale, DocsError::Conflict { datums, .. } if datums[&key("b")].heads == moved.heads),
        "{stale:?}"
    );
    node.stop().await;
}

#[tokio::test]
async fn writes_at_the_same_historical_heads_leave_a_set_of_datum_heads() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("base"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let mut introductions = Vec::new();
    for text in ["left", "right"] {
        let receipt = node
            .write(
                &alice,
                document,
                WriteMode::AtHeads {
                    heads: created.heads.clone(),
                },
                [(key("main"), note(text))],
                [],
            )
            .await
            .unwrap();
        let fresh: Vec<_> = receipt
            .heads
            .into_iter()
            .filter(|head| !introductions.contains(head) && !created.heads.contains(head))
            .collect();
        introductions.extend(fresh);
    }
    introductions.sort();
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    let (value, datum_heads) = accepted(&read);
    assert_eq!(datum_heads, introductions.as_slice());
    assert!(value == &note("left") || value == &note("right"));
    // The merged view is readable, and a writer that saw it can sign over it.
    node.write(
        &alice,
        document,
        WriteMode::IfDatumsUnchanged {
            datums: BTreeMap::from([(
                key("main"),
                DatumVersion {
                    heads: introductions.clone(),
                    clock: 0,
                },
            )]),
        },
        [(key("main"), note("merged"))],
        [],
    )
    .await
    .unwrap();
    node.stop().await;
}

#[tokio::test]
async fn unknown_heads_and_unauthorized_actors_are_refused() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let mallory = node.writer("mallory").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let unknown = ChangeHash([7; 32]);
    assert_eq!(
        node.read(&alice, document, At::Heads(vec![unknown]), false)
            .await
            .unwrap_err(),
        DocsError::UnknownHeads {
            heads: vec![unknown]
        }
    );
    assert_eq!(
        node.write(
            &alice,
            document,
            WriteMode::AtHeads {
                heads: vec![unknown]
            },
            [(key("main"), note("y"))],
            [],
        )
        .await
        .unwrap_err(),
        DocsError::UnknownHeads {
            heads: vec![unknown]
        }
    );
    assert_eq!(
        node.read(&mallory, document, At::Current, false)
            .await
            .unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.write(
            &mallory,
            document,
            WriteMode::AtCurrent,
            [(key("main"), note("y"))],
            []
        )
        .await
        .unwrap_err(),
        DocsError::Denied
    );
    node.grant(
        &mallory,
        LedgerAccess::object(AccessRequest::Read(document)),
    )
    .await;
    node.read(&mallory, document, At::Current, false)
        .await
        .unwrap();
    node.stop().await;
}
