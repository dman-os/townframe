//! Branch and merge: a branch is its origin's changes under a new document id, and a merge is
//! Automerge's merge of one history-sharing document into another. Nothing is copied by value, so
//! every signature verifies wherever its change travels.

use super::super::commands::{Commands, Detail, Kind, State};
use super::*;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_cabinet_core::dmeta::BRANCH_FACET_TAG;

fn command(text: &str) -> CommandId {
    CommandId::try_from(text.to_owned()).unwrap()
}

fn branch_key(branch: &DocId) -> FacetKey {
    format!("{BRANCH_FACET_TAG}/{branch}").parse().unwrap()
}

fn branch_value(path: &str, origin: &WriteReceipt) -> Value {
    json!({
        "path": path,
        "from": {
            "facetRef": format!("db+facet:/{}/{}", origin.doc_id, branch_key(&origin.doc_id)),
            "refHeads": origin.heads,
        },
    })
}

impl Node {
    async fn branch(
        &self,
        actor: &Actor,
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
                    authorization: Authorization::identity(actor.capability),
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
        actor: &Actor,
        source: &DocId,
        target: &DocId,
    ) -> Result<WriteReceipt, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                MergeDoc {
                    authorization: Authorization::identity(actor.capability),
                    command: None,
                    source_id: source.clone(),
                    at: At::Current,
                    target_id: target.clone(),
                },
            )
            .await)
    }

    async fn claims(&self, document: &DocId, facet: &FacetKey) -> Vec<FacetClaimEvidence> {
        let docs = self.docs.as_ref().unwrap();
        let capability = docs.mint_inspection_handle(document.clone()).await.unwrap();
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
                InspectFacetClaims {
                    authorization: Authorization {
                        authority: capability.capability_id,
                        signer: self.docs_actor.capability,
                    },
                    doc_id: document.clone(),
                    facet_key: facet.clone(),
                    heads,
                },
            )
            .await)
        .unwrap()
        .evidence
    }
}

fn origins(evidence: &[FacetClaimEvidence]) -> BTreeSet<DocId> {
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
        read.facets[&key("main")],
        before.facets[&key("main")],
        "the branch holds the origin's own change, so its facet heads are the origin's"
    );
    assert_eq!(
        accepted(&read.facets[&branch_key(&branch.doc_id)]).0,
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
        accepted(&read.facets[&branch_key(&branch.doc_id)]).0,
        &branch_value("/draft", &origin)
    );
    assert_eq!(
        accepted(&read.facets[&branch_key(&second.doc_id)]).0,
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
                authorization: Authorization::identity(alice.capability),
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
        accepted(&read.facets[&branch_key(&branch.doc_id)]).0,
        &expected
    );
    assert_eq!(
        accepted(&read.facets[&branch_key(&nested.doc_id)]).0,
        &branch_value("/nested", &branch)
    );
    let typed: big_cabinet_core::dmeta::BranchPath = serde_json::from_value(expected).unwrap();
    assert_eq!(
        typed.from.unwrap().facet_ref.to_uri_string().unwrap(),
        format!("db+facet:/{}/{}", origin.doc_id, branch_key(&origin.doc_id))
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
    assert!(!origin_now.facets.contains_key(&branch_key(&branch.doc_id)));
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
        "merging a nested branch does not overwrite the branch's own path facet"
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
        accepted(&read.facets[&key("main")]),
        (&note("from the branch"), on_branch.heads.as_slice()),
    );
    assert_eq!(
        accepted(&read.facets[&key("other")]),
        (&note("from the origin"), on_origin.heads.as_slice()),
    );
    assert_eq!(
        accepted(&read.facets[&key("third")]),
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
    accepted(&read.facets[&branch_key(&branch.doc_id)]);
    accepted(&read.facets[&branch_key(&nested.doc_id)]);
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
        let facets = doc.get(automerge::ROOT, "facets").unwrap().unwrap().1;
        let mut tx = doc.transaction();
        tx.delete(&facets, key("main").to_string()).unwrap();
        tx.commit();
    })
    .await;
    assert!(matches!(
        node.merge(&alice, &branch.doc_id, &first.doc_id)
            .await,
        Err(DocsError::BaseNotAccepted { facet_key, .. }) if facet_key == key("main")
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
                alice.actor_id,
                None,
                Some(PLACEMENT),
                Some(&Detail::Branch {
                    origin: origin.doc_id.clone(),
                    path: format!("/{name}"),
                    stealth: false,
                    owner_id: Some(alice.actor_id),
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
                alice.actor_id,
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
