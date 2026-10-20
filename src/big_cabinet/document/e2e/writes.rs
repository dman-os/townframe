//! The ordinary write surface: whole-facet values, explicit bases, and the four write modes.

use super::*;

#[tokio::test]
async fn a_created_document_reads_back_its_facets_at_the_creation_heads() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let receipt = node
        .create(&alice, "create", [(key("main"), note("hello"))])
        .await
        .unwrap();
    let read = node
        .read(&alice, &receipt.document_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, receipt.heads);
    let (value, facet_heads) = accepted(&read.facets[&key("main")]);
    assert_eq!(value, &note("hello"));
    assert_eq!(facet_heads, receipt.heads.as_slice());
    assert_eq!(read.facets.len(), 1, "dmeta is not a readable facet");
    node.stop().await;
}

#[tokio::test]
async fn unmentioned_facets_keep_their_value_and_facet_heads() {
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
    let document = &created.document_id;
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
        accepted(&read.facets[&key("a")]),
        (&note("a1"), written.heads.as_slice())
    );
    assert_eq!(
        accepted(&read.facets[&key("b")]),
        (&note("b0"), created.heads.as_slice())
    );
    let historical = node
        .read(&alice, document, At::Heads(created.heads.clone()), false)
        .await
        .unwrap();
    assert_eq!(accepted(&historical.facets[&key("a")]).0, &note("a0"));
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
    let document = &created.document_id;
    let removed = node
        .write(&alice, document, WriteMode::AtCurrent, [], [key("main")])
        .await
        .unwrap();
    assert_eq!(
        node.read_one(&alice, document, At::Current, &key("main"))
            .await,
        FacetRead::Absent {
            facet_heads: removed.heads.clone()
        }
    );
    let listing = op(node
        .client()
        .call(
            RequestContext::root(),
            ListFacets {
                capability: alice.capability,
                document_id: document.clone(),
                at: At::Current,
            },
        )
        .await)
    .unwrap();
    assert!(listing.facets.is_empty(), "a removed slot is not listed");
    let back = node
        .write(
            &alice,
            document,
            WriteMode::IfFacetsUnchanged {
                facets: BTreeMap::from([(key("main"), removed.heads.clone())]),
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
    let document = &created.document_id;
    let unknown: FacetKey = "org.example.unknown/main".parse().unwrap();
    let dmeta: FacetKey = "org.example.daybook.dmeta/main".parse().unwrap();
    let branch: FacetKey = "org.example.daybook.branch/main".parse().unwrap();
    let branches: FacetKey = "org.example.daybook.branches/main".parse().unwrap();
    let cases = [
        (
            vec![(unknown.clone(), json!({}))],
            vec![],
            DocumentsError::UnknownSchema {
                facet_tag: "org.example.unknown".into(),
            },
        ),
        (
            vec![(dmeta.clone(), json!({}))],
            vec![],
            DocumentsError::SystemFacet {
                facet_key: dmeta.clone(),
            },
        ),
        (
            vec![(branch.clone(), json!({"path": "/x"}))],
            vec![],
            DocumentsError::SystemFacet {
                facet_key: branch.clone(),
            },
        ),
        (
            vec![],
            vec![branches.clone()],
            DocumentsError::SystemFacet {
                facet_key: branches.clone(),
            },
        ),
        (
            vec![(key("main"), note("y"))],
            vec![key("main")],
            DocumentsError::SetAndRemoved {
                facet_key: key("main"),
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
        matches!(invalid, DocumentsError::InvalidValue { .. }),
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
    let document = &created.document_id;
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
            WriteMode::IfDocumentUnchanged {
                heads: created.heads.clone()
            },
            [(key("a"), note("a1"))],
            [],
        )
        .await
        .unwrap_err(),
        DocumentsError::Conflict {
            heads: moved.heads.clone(),
            facets: BTreeMap::new(),
        }
    );
    // Another facet moving does not break a facet-scoped condition.
    node.write(
        &alice,
        document,
        WriteMode::IfFacetsUnchanged {
            facets: BTreeMap::from([(key("a"), created.heads.clone())]),
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
            WriteMode::IfFacetsUnchanged {
                facets: BTreeMap::from([(key("b"), created.heads.clone())]),
            },
            [(key("b"), note("b2"))],
            [],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&stale, DocumentsError::Conflict { facets, .. } if facets[&key("b")] == moved.heads),
        "{stale:?}"
    );
    node.stop().await;
}

#[tokio::test]
async fn writes_at_the_same_historical_heads_leave_a_set_of_facet_heads() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("base"))])
        .await
        .unwrap();
    let document = &created.document_id;
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
    let (value, facet_heads) = accepted(&read);
    assert_eq!(facet_heads, introductions.as_slice());
    assert!(value == &note("left") || value == &note("right"));
    // The merged view is readable, and a writer that saw it can sign over it.
    node.write(
        &alice,
        document,
        WriteMode::IfFacetsUnchanged {
            facets: BTreeMap::from([(key("main"), introductions.clone())]),
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
    let document = &created.document_id;
    let unknown = ChangeHash([7; 32]);
    assert_eq!(
        node.read(&alice, document, At::Heads(vec![unknown]), false)
            .await
            .unwrap_err(),
        DocumentsError::UnknownHeads {
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
        DocumentsError::UnknownHeads {
            heads: vec![unknown]
        }
    );
    assert_eq!(
        node.read(&mallory, document, At::Current, false)
            .await
            .unwrap_err(),
        DocumentsError::Denied
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
        DocumentsError::Denied
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
