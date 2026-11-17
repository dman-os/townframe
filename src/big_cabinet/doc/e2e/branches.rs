//! Branch and merge: a branch is its origin's changes under a new document id, and a merge is
//! Automerge's merge of one history-sharing document into another. Nothing is copied by value, so
//! every signature verifies wherever its change travels.

use super::super::commands::Commands;
use super::super::commands::Detail;
use super::super::commands::Kind;
use super::super::commands::State;
use super::*;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_cabinet_core::DatumReference;
use big_cabinet_core::ReferenceDocId;
use big_cabinet_core::dmeta::BranchPath;
use big_cabinet_core::doc::DatumKinds;
use big_cabinet_core::reference::head_text;

fn command(text: &str) -> CommandId {
    CommandId::try_from(text.to_owned()).unwrap()
}

fn branch_key(branch: &DocId) -> DatumKey {
    format!("{}/{branch}", BranchPath::ID).parse().unwrap()
}

fn origin_reference(origin: &DocId) -> String {
    DatumReference::new(ReferenceDocId::Concrete(origin.clone()), branch_key(origin)).to_string()
}

fn branch_value(path: &str, origin: &WriteReceipt) -> Value {
    json!({
        "path": path,
        "from": {
            "ref": origin_reference(&origin.doc_id),
            "heads": origin.heads.iter().map(head_text).collect::<Vec<_>>(),
        },
    })
}

impl Node {
    async fn branch(
        &self,
        actor: &OpenedActor,
        command: &str,
        origin: &DocId,
        path: &str,
        stealth: bool,
    ) -> Result<WriteReceipt, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                BranchDoc {
                    capability: self
                        .capability(
                            actor,
                            DocScope {
                                read: DocScope::read_all([origin.clone()]),
                                write: if stealth {
                                    std::collections::BTreeMap::new()
                                } else {
                                    std::collections::BTreeMap::from([(
                                        origin.clone(),
                                        big_cabinet_core::doc::DatumKinds::All,
                                    )])
                                },
                                create: BTreeSet::from([PLACEMENT.into()]),
                                ..DocScope::default()
                            },
                        )
                        .await?,
                    command: CommandId::try_from(command.to_owned()).unwrap(),
                    doc_id: origin.clone(),
                    at: At::Current,
                    path: path.into(),
                    stealth,
                    placement: PLACEMENT.into(),
                },
            )
            .await)
    }

    async fn merge(
        &self,
        actor: &OpenedActor,
        source: &DocId,
        target: &DocId,
    ) -> Result<WriteReceipt, DocsError> {
        self.merge_as(actor, source, target, DatumKinds::All, DatumKinds::All)
            .await
    }

    /// A merge by an actor holding only `read` kinds on the source and `write` kinds on the target.
    async fn merge_as(
        &self,
        actor: &OpenedActor,
        source: &DocId,
        target: &DocId,
        read: DatumKinds,
        write: DatumKinds,
    ) -> Result<WriteReceipt, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                MergeDoc {
                    capability: self
                        .capability(
                            actor,
                            DocScope {
                                read: BTreeMap::from([(source.clone(), read)]),
                                write: BTreeMap::from([(target.clone(), write)]),
                                ..DocScope::default()
                            },
                        )
                        .await?,
                    command: None,
                    source_id: source.clone(),
                    at: At::Current,
                    target_id: target.clone(),
                },
            )
            .await)
    }

    pub(super) async fn claims(
        &self,
        document: &DocId,
        datum: &DatumKey,
    ) -> Vec<DatumClaimEvidence> {
        let docs = self.docs.as_ref().unwrap();
        let capability = docs
            .mint_inspection_handle(document.clone(), self.docs_actor.actor.actor_id)
            .await
            .unwrap();
        let handle = match self.repo.get_doc(document).await.unwrap() {
            big_repo::DocLookup::Ready(handle) => handle,
            _ => panic!("document is local"),
        };
        let mut heads = handle.with_document_read(|doc| doc.get_heads()).await;
        heads.sort();
        op(self
            .client()
            .call(
                RequestContext::root(),
                InspectDatumClaims {
                    capability: capability.capability_id,
                    doc_id: document.clone(),
                    datum_key: datum.clone(),
                    heads,
                },
            )
            .await)
        .unwrap()
        .evidence
    }
}

fn origins(evidence: &[DatumClaimEvidence]) -> BTreeSet<DocId> {
    evidence
        .iter()
        .map(|entry| match &entry.result {
            SignatureEvidence::VerifiedSignature { evidence } => {
                evidence.claim.origin_document.clone()
            }
            other => panic!("expected a verified signature, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn a_branch_carries_its_origin_history_and_both_sides_name_it() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let before = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap();
    let branch = node
        .branch(&alice, "branch", &origin.doc_id, "/draft", false)
        .await
        .unwrap();
    assert_ne!(branch.doc_id, origin.doc_id);
    let read = node
        .read(&alice, &branch.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        read.datums[&key("main")],
        before.datums[&key("main")],
        "the branch holds the origin's own change, so its datum heads are the origin's"
    );
    assert_eq!(
        accepted(&read.datums[&branch_key(&branch.doc_id)]).0,
        &branch_value("/draft", &origin),
    );
    assert_eq!(
        origins(&node.claims(&branch.doc_id, &key("main")).await),
        BTreeSet::from([origin.doc_id.clone()]),
        "a signature names the document it was made on, wherever it travels"
    );
    assert_eq!(
        accepted(
            &node
                .read_one(
                    &alice,
                    &origin.doc_id,
                    At::Current,
                    &branch_key(&branch.doc_id)
                )
                .await
        )
        .0,
        &branch_value("/draft", &origin),
    );
    assert_eq!(
        node.branch(&alice, "branch", &origin.doc_id, "/draft", false)
            .await
            .unwrap(),
        branch,
        "a replayed branch returns its receipt without branching again"
    );
    let second_base = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap();
    let second = node
        .branch(&alice, "branch-2", &origin.doc_id, "/draft", false)
        .await
        .unwrap();
    let read = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        accepted(&read.datums[&branch_key(&branch.doc_id)]).0,
        &branch_value("/draft", &origin)
    );
    assert_eq!(
        accepted(&read.datums[&branch_key(&second.doc_id)]).0,
        &branch_value(
            "/draft",
            &WriteReceipt {
                doc_id: origin.doc_id.clone(),
                heads: second_base.heads,
            }
        )
    );
    node.stop().await;
}

#[tokio::test]
async fn a_historical_branch_pins_its_concrete_parent_and_preserves_inherited_branch_metadata() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("old"))])
        .await
        .unwrap();
    node.write(
        &alice,
        &origin.doc_id,
        WriteMode::AtCurrent,
        [(key("main"), note("new"))],
        [],
    )
    .await
    .unwrap();
    let branch = op(node
        .client()
        .call(
            RequestContext::root(),
            BranchDoc {
                capability: node
                    .capability(
                        &alice,
                        DocScope {
                            write: std::collections::BTreeMap::from([(
                                origin.doc_id.clone(),
                                big_cabinet_core::doc::DatumKinds::All,
                            )]),
                            create: BTreeSet::from([PLACEMENT.into()]),
                            ..DocScope::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: command("historical"),
                doc_id: origin.doc_id.clone(),
                at: At::Heads(origin.heads.clone()),
                path: "/historical".into(),
                stealth: false,
                placement: PLACEMENT.into(),
            },
        )
        .await)
    .unwrap();
    let expected = branch_value("/historical", &origin);
    for document in [&origin.doc_id, &branch.doc_id] {
        assert_eq!(
            accepted(
                &node
                    .read_one(&alice, document, At::Current, &branch_key(&branch.doc_id))
                    .await
            )
            .0,
            &expected
        );
    }
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, &branch.doc_id, At::Current, &key("main"))
                .await
        )
        .0,
        &note("old")
    );
    let nested = node
        .branch(&alice, "nested", &branch.doc_id, "/nested", true)
        .await
        .unwrap();
    let read = node
        .read(&alice, &nested.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        accepted(&read.datums[&branch_key(&branch.doc_id)]).0,
        &expected
    );
    assert_eq!(
        accepted(&read.datums[&branch_key(&nested.doc_id)]).0,
        &branch_value("/nested", &branch)
    );
    let typed: big_cabinet_core::dmeta::BranchPath = serde_json::from_value(expected).unwrap();
    assert_eq!(
        typed.from.unwrap().location().reference(),
        &DatumReference::parse(&origin_reference(&origin.doc_id))
            .unwrap()
            .pinned(origin.heads.clone())
    );
    node.stop().await;
}

#[tokio::test]
async fn branching_from_a_datum_kind_limited_source_is_refused() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("origin"))])
        .await
        .unwrap();
    let capability = node
        .capability(
            &alice,
            DocScope {
                read: DocScope::read_all([origin.doc_id.clone()]),
                write: BTreeMap::from([(
                    origin.doc_id.clone(),
                    DatumKinds::Only(BTreeSet::from([key("main").tag().clone()])),
                )]),
                create: BTreeSet::from([PLACEMENT.into()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .client()
            .call(
                RequestContext::root(),
                BranchDoc {
                    capability,
                    command: "limited-source".to_owned().try_into().unwrap(),
                    doc_id: origin.doc_id,
                    at: At::Current,
                    path: "/limited".into(),
                    stealth: false,
                    placement: PLACEMENT.into(),
                },
            )
            .await),
        Err(DocsError::Denied)
    );
    node.stop().await;
}

#[tokio::test]
async fn announcing_a_branch_needs_write_on_the_origin_and_stealth_needs_only_read() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let bob = node.writer("bob").await;
    node.grant(
        &bob,
        LedgerAccess::object(AccessRequest::Read(&origin.doc_id)),
    )
    .await;
    assert_eq!(
        node.branch(&bob, "announced", &origin.doc_id, "/bob", false)
            .await,
        Err(DocsError::Denied),
    );
    let branch = node
        .branch(&bob, "stealth", &origin.doc_id, "/bob", true)
        .await
        .unwrap();
    let origin_now = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        origin_now.heads, origin.heads,
        "a stealth branch leaves the origin untouched"
    );
    assert!(!origin_now.datums.contains_key(&branch_key(&branch.doc_id)));
    assert_eq!(
        accepted(
            &node
                .read_one(
                    &bob,
                    &branch.doc_id,
                    At::Current,
                    &branch_key(&branch.doc_id)
                )
                .await
        )
        .0,
        &branch_value("/bob", &origin),
    );
    node.stop().await;
}

#[tokio::test]
async fn a_branch_merges_back_with_the_provenance_of_each_side() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(
            &alice,
            "create",
            [(key("main"), note("x")), (key("other"), note("y"))],
        )
        .await
        .unwrap();
    let branch = node
        .branch(&alice, "branch", &origin.doc_id, "/draft", false)
        .await
        .unwrap();
    let nested = node
        .branch(&alice, "nested", &branch.doc_id, "/nested", true)
        .await
        .unwrap();
    let on_nested = node
        .write(
            &alice,
            &nested.doc_id,
            WriteMode::AtCurrent,
            [(key("third"), note("from the nested branch"))],
            [],
        )
        .await
        .unwrap();
    let on_branch = node
        .write(
            &alice,
            &branch.doc_id,
            WriteMode::AtCurrent,
            [(key("main"), note("from the branch"))],
            [],
        )
        .await
        .unwrap();
    node.merge(&alice, &nested.doc_id, &branch.doc_id)
        .await
        .unwrap();
    assert_eq!(
        accepted(
            &node
                .read_one(
                    &alice,
                    &branch.doc_id,
                    At::Current,
                    &branch_key(&branch.doc_id),
                )
                .await
        )
        .0,
        &branch_value("/draft", &origin),
        "merging a nested branch does not overwrite the branch's own path datum"
    );
    let on_origin = node
        .write(
            &alice,
            &origin.doc_id,
            WriteMode::AtCurrent,
            [(key("other"), note("from the origin"))],
            [],
        )
        .await
        .unwrap();
    let merged = node
        .merge(&alice, &branch.doc_id, &origin.doc_id)
        .await
        .unwrap();
    assert_eq!(merged.doc_id, origin.doc_id);
    let read = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, merged.heads);
    assert_eq!(
        accepted(&read.datums[&key("main")]),
        (&note("from the branch"), on_branch.heads.as_slice()),
    );
    assert_eq!(
        accepted(&read.datums[&key("other")]),
        (&note("from the origin"), on_origin.heads.as_slice()),
    );
    assert_eq!(
        accepted(&read.datums[&key("third")]),
        (&note("from the nested branch"), on_nested.heads.as_slice()),
    );
    assert_eq!(
        origins(&node.claims(&origin.doc_id, &key("main")).await),
        BTreeSet::from([branch.doc_id.clone()]),
    );
    assert_eq!(
        origins(&node.claims(&origin.doc_id, &key("other")).await),
        BTreeSet::from([origin.doc_id.clone()]),
    );
    assert_eq!(
        origins(&node.claims(&origin.doc_id, &key("third")).await),
        BTreeSet::from([nested.doc_id.clone()]),
    );
    accepted(&read.datums[&branch_key(&branch.doc_id)]);
    accepted(&read.datums[&branch_key(&nested.doc_id)]);
    // Writing on the origin after the merge reuses the origin's own document UUID.
    node.write(
        &alice,
        &origin.doc_id,
        WriteMode::AtCurrent,
        [(key("main"), note("after"))],
        [],
    )
    .await
    .unwrap();
    assert_eq!(
        origins(&node.claims(&origin.doc_id, &key("main")).await),
        BTreeSet::from([origin.doc_id.clone()]),
    );
    node.stop().await;
}

#[tokio::test]
async fn merges_refuse_unrelated_documents_unaccepted_sources_and_missing_access() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let first = node
        .create(&alice, "first", [(key("main"), note("x"))])
        .await
        .unwrap();
    let second = node
        .create(&alice, "second", [(key("main"), note("y"))])
        .await
        .unwrap();
    assert_eq!(
        node.merge(&alice, &first.doc_id, &second.doc_id).await,
        Err(DocsError::UnrelatedHistory),
    );
    let branch = node
        .branch(&alice, "branch", &first.doc_id, "/draft", true)
        .await
        .unwrap();
    let bob = node.writer("bob").await;
    node.grant(
        &bob,
        LedgerAccess::object(AccessRequest::Read(&branch.doc_id)),
    )
    .await;
    node.grant(
        &bob,
        LedgerAccess::object(AccessRequest::Read(&first.doc_id)),
    )
    .await;
    assert_eq!(
        node.merge(&bob, &branch.doc_id, &first.doc_id).await,
        Err(DocsError::Denied),
        "a merge writes its target"
    );
    node.sabotage(&branch.doc_id, |doc| {
        let datums = doc.get(automerge::ROOT, "datums").unwrap().unwrap().1;
        let mut tx = doc.transaction();
        tx.delete(&datums, key("main").to_string()).unwrap();
        tx.commit();
    })
    .await;
    assert!(matches!(
        node.merge(&alice, &branch.doc_id, &first.doc_id)
            .await,
        Err(DocsError::BaseNotAccepted { datum_key, .. }) if datum_key == key("main")
    ));
    for (document, heads) in [(&first, &first.heads), (&second, &second.heads)] {
        assert_eq!(
            &node
                .read(&alice, &document.doc_id, At::Current, false)
                .await
                .unwrap()
                .heads,
            heads,
            "a refused merge changes nothing"
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn the_boot_pass_announces_and_places_a_committed_branch_and_abandons_an_uncommitted_one() {
    let mut node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    let committed = node
        .branch(&alice, "seed-committed", &origin.doc_id, "/committed", true)
        .await
        .unwrap()
        .doc_id;
    let uncommitted = node.repo.allocate_id().await.unwrap();
    for (name, branch) in [("committed", &committed), ("uncommitted", &uncommitted)] {
        ledger
            .begin(
                &command(name),
                Kind::Branch,
                [0; 32],
                alice.actor.actor_id,
                None,
                Some(PLACEMENT),
                Some(&Detail::Branch {
                    origin: origin.doc_id.clone(),
                    path: format!("/{name}"),
                    stealth: false,
                    owner_id: Some(alice.actor.actor_id),
                }),
            )
            .await
            .unwrap();
        ledger.set_document(&command(name), branch).await.unwrap();
    }
    node.restart().await.unwrap();
    let settled = ledger.get(&command("committed")).await.unwrap().unwrap();
    assert_eq!(settled.state, State::Done);
    assert!(ledger.get(&command("uncommitted")).await.unwrap().is_none());
    assert!(
        !node
            .repo
            .reserved_doc_ids()
            .await
            .unwrap()
            .contains(&uncommitted)
    );
    assert_eq!(
        node.read(&alice, &committed, At::Current, false)
            .await
            .unwrap()
            .heads,
        settled.heads,
        "placement granted the brancher access"
    );
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, &origin.doc_id, At::Current, &branch_key(&committed))
                .await
        )
        .0,
        &branch_value("/committed", &origin),
    );
    // A second boot finds the backlink by its commit message and does not write it again.
    let heads = node
        .read(&alice, &origin.doc_id, At::Current, false)
        .await
        .unwrap()
        .heads;
    node.restart().await.unwrap();
    assert_eq!(
        node.read(&alice, &origin.doc_id, At::Current, false)
            .await
            .unwrap()
            .heads,
        heads
    );
    node.stop().await;
}

#[tokio::test]
async fn the_boot_pass_settles_a_merge_by_whether_the_target_holds_the_source_heads() {
    let mut node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let branch = node
        .branch(&alice, "branch", &origin.doc_id, "/draft", true)
        .await
        .unwrap();
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    for (name, heads) in [("merged", &origin.heads), ("lost", &branch.heads)] {
        ledger
            .begin(
                &command(name),
                Kind::Merge,
                [0; 32],
                alice.actor.actor_id,
                Some(&origin.doc_id),
                None,
                Some(&Detail::Merge {
                    source: branch.doc_id.clone(),
                    heads: heads.clone(),
                }),
            )
            .await
            .unwrap();
    }
    node.restart().await.unwrap();
    let settled = ledger.get(&command("merged")).await.unwrap().unwrap();
    assert_eq!((settled.state, settled.heads), (State::Done, origin.heads));
    assert!(ledger.get(&command("lost")).await.unwrap().is_none());
    node.stop().await;
}

fn kinds(tags: Option<&[&str]>) -> DatumKinds {
    tags.map_or(DatumKinds::All, |tags| {
        DatumKinds::Only(
            tags.iter()
                .map(|tag| {
                    format!("{tag}/x")
                        .parse::<DatumKey>()
                        .unwrap()
                        .tag()
                        .clone()
                })
                .collect(),
        )
    })
}

utils_rs::table_tests! {
    merges_are_held_to_what_they_change tokio,
    (read, write, branch_changes, expect),
    {
        let node = Node::boot().await;
        let alice = node.writer("alice").await;
        let text_key: DatumKey = format!("{TEXT_NOTE}/main").parse().unwrap();
        let text = json!({"$type": TEXT_NOTE, "text": "x"});
        let origin = node
            .create(
                &alice,
                "create",
                [(key("main"), note("x")), (text_key.clone(), text.clone())],
            )
            .await
            .unwrap();
        let branch = node
            .branch(&alice, "branch", &origin.doc_id, "/draft", true)
            .await
            .unwrap();
        for &tag in branch_changes {
            let (datum, value) = match tag {
                NOTE => (key("main"), note("changed")),
                TEXT_NOTE => (text_key.clone(), json!({"$type": TEXT_NOTE, "text": "changed"})),
                other => panic!("unknown tag {other}"),
            };
            node.write(&alice, &branch.doc_id, WriteMode::AtCurrent, [(datum, value)], [])
                .await
                .unwrap();
        }
        let merged = node
            .merge_as(&alice, &branch.doc_id, &origin.doc_id, kinds(read), kinds(write))
            .await;
        match expect {
            Ok(()) => {
                let merged = merged.unwrap();
                let read = node.read(&alice, &origin.doc_id, At::Current, false).await.unwrap();
                assert_eq!(read.heads, merged.heads);
                for &tag in branch_changes {
                    let datum = if tag == NOTE { key("main") } else { text_key.clone() };
                    assert!(accepted(&read.datums[&datum]).0.to_string().contains("changed"));
                }
            }
            Err(error) => {
                assert_eq!(merged, Err(error));
                assert_eq!(
                    node.read(&alice, &origin.doc_id, At::Current, false).await.unwrap().heads,
                    origin.heads,
                    "a refused merge changes nothing"
                );
            }
        }
        node.stop().await;
    },
}

// A merge is held to the datums it changes; the branch datum it brings is the repository's own.
merges_are_held_to_what_they_change! {
    a_target_writable_at_the_changed_kinds_takes_the_merge: (None, Some(&[NOTE][..]), &[NOTE][..], Ok(())),
    a_target_not_writable_at_a_changed_kind_refuses_the_merge: (None, Some(&[NOTE][..]), &[NOTE, TEXT_NOTE][..], Err(DocsError::Denied)),
    // The branch's own `branch/<id>` datum comes along by design (ADR 034); it is the
    // repository's record, so it needs no kind of the merging actor's.
    the_branch_datum_a_merge_brings_needs_no_kind: (Some(&[NOTE][..]), Some(&[NOTE][..]), &[NOTE][..], Ok(())),
    a_source_readable_at_the_changed_kinds_gives_the_merge: (Some(&[NOTE][..]), None, &[NOTE][..], Ok(())),
    a_source_not_readable_at_a_changed_kind_refuses_the_merge: (Some(&[NOTE][..]), None, &[NOTE, TEXT_NOTE][..], Err(DocsError::Denied)),
}

#[tokio::test]
async fn a_merge_that_brings_nothing_needs_no_kind() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let origin = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let branch = node
        .branch(&alice, "branch", &origin.doc_id, "/draft", true)
        .await
        .unwrap();
    // The branch holds all of the origin's history, so merging the origin into it changes nothing.
    let merged = node
        .merge_as(
            &alice,
            &origin.doc_id,
            &branch.doc_id,
            kinds(Some(&[TEXT_NOTE])),
            kinds(Some(&[TEXT_NOTE])),
        )
        .await
        .unwrap();
    assert_eq!(merged.heads, branch.heads);
    node.stop().await;
}
