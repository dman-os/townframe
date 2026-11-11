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
                with_authors: false,
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
async fn a_value_over_the_read_budget_names_its_datum_and_limit() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let maximum = ResourceLimit::Bytes.default_maximum();
    // The read charged the map keys (`$type`, `value`) and the `$type` string before it reached the value
    // that did not fit, and `observed` counts them.
    let spent_before_value = "$type".len() + NOTE.len() + "value".len();
    let error = node
        .write(
            &alice,
            &created.doc_id,
            WriteMode::AtCurrent,
            [(key("main"), note(&"x".repeat(maximum + 1)))],
            [],
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        DocsError::ValueTooLarge {
            datum_key: key("main"),
            limit: ResourceLimit::Bytes,
            observed: (maximum + 1 + spent_before_value) as u64,
            maximum: maximum as u64,
        }
    );
    node.stop().await;
}

/// A text over the budget is refused from the incoming JSON, before it is reconciled: a text is
/// reconciled character by character, so reaching the post-reconcile read took minutes and
/// gigabytes. The error is the one that read raises.
#[tokio::test]
async fn a_text_over_the_read_budget_is_refused_before_it_is_reconciled() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let text_key: DatumKey = format!("{TEXT_NOTE}/main").parse().unwrap();
    let created = node
        .create(
            &alice,
            "create",
            [(text_key.clone(), json!({"$type": TEXT_NOTE, "text": "x"}))],
        )
        .await
        .unwrap();
    let maximum = ResourceLimit::Bytes.default_maximum();
    let spent_before_value = "$type".len() + TEXT_NOTE.len() + "text".len();
    let error = node
        .write(
            &alice,
            &created.doc_id,
            WriteMode::AtCurrent,
            [(
                text_key.clone(),
                json!({"$type": TEXT_NOTE, "text": "x".repeat(maximum + 1)}),
            )],
            [],
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        DocsError::ValueTooLarge {
            datum_key: text_key,
            limit: ResourceLimit::Bytes,
            observed: (maximum + 1 + spent_before_value) as u64,
            maximum: maximum as u64,
        }
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

/// Two writers that each first write `b` from a base without it create two metadata entries for it,
/// each carrying the alias its creator minted. Automerge shows one entry; both aliases must stay
/// readable, and the next write must choose the smaller of the two.
#[tokio::test]
async fn concurrent_creators_of_a_datum_keep_both_aliases_and_the_next_write_chooses_the_smallest()
{
    use crate::doc::claim_history::ClaimHistoryResult;
    use crate::doc::claim_history::HistoryBudget;
    use crate::doc::claim_history::reconstruct_current_claims;

    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("a"), note("a0"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    for text in ["left", "right"] {
        node.write(
            &alice,
            document,
            WriteMode::AtHeads {
                heads: created.heads.clone(),
            },
            [(key("b"), note(text))],
            [],
        )
        .await
        .unwrap();
    }
    let merged = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(key("b"), note("merged"))],
            [],
        )
        .await
        .unwrap();
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert_eq!(accepted(&read.datums[&key("b")]).0, &note("merged"));
    node.sabotage(document, |doc| {
        let datums = doc.get(automerge::ROOT, "datums").unwrap().unwrap().1;
        let dmeta = doc
            .get(&datums, "org.example.daybook.dmeta/main")
            .unwrap()
            .unwrap()
            .1;
        let slots = doc.get(&dmeta, "datums").unwrap().unwrap().1;
        let entries = doc.get_all(&slots, key("b").to_string()).unwrap();
        assert_eq!(entries.len(), 2, "each creator's entry survives the merge");
        let mut union = BTreeSet::new();
        for (_, entry) in &entries {
            let held: Vec<String> = doc
                .get_all(entry, "uuids")
                .unwrap()
                .into_iter()
                .flat_map(|(_, index)| doc.keys(&index).collect::<Vec<_>>())
                .collect();
            assert_eq!(held.len(), 1, "an entry holds only its creator's alias");
            union.extend(held);
        }
        assert_eq!(union.len(), 2);
        let claims =
            reconstruct_current_claims(doc, &key("b"), &doc.get_heads(), HistoryBudget::default())
                .unwrap();
        let [claim] = claims.as_slice() else {
            panic!("the merging write replaced every competing signature: {claims:?}")
        };
        let ClaimHistoryResult::Accepted(claim) = &claim.result else {
            panic!("{claim:?}")
        };
        assert_eq!(
            claim.claim.chosen_uuid.to_string(),
            *union.iter().next().unwrap()
        );
        assert_eq!(claim.introduction.resulting_heads, merged.heads);
    })
    .await;
    node.stop().await;
}

/// Finding a datum's aliases reads its own entry rather than scanning an index with a binding per
/// datum, so a verification does not exhaust the per-frontier budget (1,024 entries) once a
/// document holds a few hundred datums, as it did when the scan was charged two entries a datum
/// (so 600 datums is past the old ceiling of about 500; 1,200 took over a minute in a debug build).
#[tokio::test]
async fn a_document_of_many_datums_accepts_an_update_to_one() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(
            &alice,
            "create",
            (0..600).map(|i| (key(&format!("n{i:05}")), note("first"))),
        )
        .await
        .unwrap();
    let document = &created.doc_id;
    let target = key("n00300");
    let written = node
        .write(
            &alice,
            document,
            WriteMode::AtCurrent,
            [(target.clone(), note("second"))],
            [],
        )
        .await
        .unwrap();
    let read = node.read_one(&alice, document, At::Current, &target).await;
    assert_eq!(accepted(&read), (&note("second"), written.heads.as_slice()));
    let untouched = node
        .read_one(&alice, document, At::Current, &key("n00001"))
        .await;
    assert_eq!(accepted(&untouched).0, &note("first"));
    node.stop().await;
}
