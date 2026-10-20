//! Branch and merge: a branch is its origin's changes under a new document id, and a merge is
//! Automerge's merge of one history-sharing document into another. Nothing is copied by value, so
//! every signature verifies wherever its change travels.

use super::super::commands::{Commands, Detail, Kind, State};
use super::*;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_cabinet_core::dmeta::{BRANCH_FACET_TAG, BRANCHES_FACET_TAG};

fn command(text: &str) -> CommandId {
    CommandId::try_from(text.to_owned()).unwrap()
}

fn branch_key(branch: &DocumentId) -> FacetKey {
    format!("{BRANCH_FACET_TAG}/{branch}").parse().unwrap()
}

fn branches_key() -> FacetKey {
    format!("{BRANCHES_FACET_TAG}/main").parse().unwrap()
}

impl Node {
    async fn branch(
        &self,
        actor: &Actor,
        command: &str,
        origin: &DocumentId,
        path: &str,
        stealth: bool,
    ) -> Result<WriteReceipt, DocumentsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                BranchDocument {
                    capability: actor.capability,
                    command: CommandId::try_from(command.to_owned()).unwrap(),
                    document_id: origin.clone(),
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
        source: &DocumentId,
        target: &DocumentId,
    ) -> Result<WriteReceipt, DocumentsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                MergeDocument {
                    capability: actor.capability,
                    command: None,
                    source: source.clone(),
                    at: At::Current,
                    target: target.clone(),
                },
            )
            .await)
    }

    async fn claims(&self, document: &DocumentId, facet: &FacetKey) -> Vec<FacetClaimEvidence> {
        let documents = self.documents.as_ref().unwrap();
        let capability = documents
            .mint_inspection_handle(document.clone())
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
                InspectFacetClaims {
                    capability: capability.id,
                    document_id: document.clone(),
                    facet_key: facet.clone(),
                    heads,
                },
            )
            .await)
        .unwrap()
        .evidence
    }
}

fn origins(evidence: &[FacetClaimEvidence]) -> BTreeSet<DocumentId> {
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
        .read(&alice, &origin.document_id, At::Current, false)
        .await
        .unwrap();
    let branch = node
        .branch(&alice, "branch", &origin.document_id, "/draft", false)
        .await
        .unwrap();
    assert_ne!(branch.document_id, origin.document_id);
    let read = node
        .read(&alice, &branch.document_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        read.facets[&key("main")],
        before.facets[&key("main")],
        "the branch holds the origin's own change, so its facet heads are the origin's"
    );
    assert_eq!(
        accepted(&read.facets[&branch_key(&branch.document_id)]).0,
        &json!({"path": "/draft"}),
    );
    assert_eq!(
        origins(&node.claims(&branch.document_id, &key("main")).await),
        BTreeSet::from([origin.document_id.clone()]),
        "a signature names the document it was made on, wherever it travels"
    );
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, &origin.document_id, At::Current, &branches_key())
                .await
        )
        .0,
        &json!({
            "byId": {branch.document_id.to_string(): {"path": "/draft"}},
            "byPath": {"/draft": [branch.document_id.to_string()]},
        }),
    );
    assert_eq!(
        node.branch(&alice, "branch", &origin.document_id, "/draft", false)
            .await
            .unwrap(),
        branch,
        "a replayed branch returns its receipt without branching again"
    );
    let second = node
        .branch(&alice, "branch-2", &origin.document_id, "/draft", false)
        .await
        .unwrap();
    let mut ids = [branch.document_id.clone(), second.document_id.clone()];
    ids.sort();
    assert_eq!(
        accepted(
            &node
                .read_one(&alice, &origin.document_id, At::Current, &branches_key())
                .await
        )
        .0,
        &json!({
            "byId": {
                branch.document_id.to_string(): {"path": "/draft"},
                second.document_id.to_string(): {"path": "/draft"},
            },
            "byPath": {"/draft": [ids[0].to_string(), ids[1].to_string()]},
        }),
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
        LedgerAccess::object(AccessRequest::Read(&origin.document_id)),
    )
    .await;
    assert_eq!(
        node.branch(&bob, "announced", &origin.document_id, "/bob", false)
            .await,
        Err(DocumentsError::Denied),
    );
    let branch = node
        .branch(&bob, "stealth", &origin.document_id, "/bob", true)
        .await
        .unwrap();
    let origin_now = node
        .read(&alice, &origin.document_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(
        origin_now.heads, origin.heads,
        "a stealth branch leaves the origin untouched"
    );
    assert!(!origin_now.facets.contains_key(&branches_key()));
    assert_eq!(
        accepted(
            &node
                .read_one(
                    &bob,
                    &branch.document_id,
                    At::Current,
                    &branch_key(&branch.document_id)
                )
                .await
        )
        .0,
        &json!({"path": "/bob"}),
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
        .branch(&alice, "branch", &origin.document_id, "/draft", false)
        .await
        .unwrap();
    let nested = node
        .branch(&alice, "nested", &branch.document_id, "/nested", true)
        .await
        .unwrap();
    let on_nested = node
        .write(
            &alice,
            &nested.document_id,
            WriteMode::AtCurrent,
            [(key("third"), note("from the nested branch"))],
            [],
        )
        .await
        .unwrap();
    let on_branch = node
        .write(
            &alice,
            &branch.document_id,
            WriteMode::AtCurrent,
            [(key("main"), note("from the branch"))],
            [],
        )
        .await
        .unwrap();
    node.merge(&alice, &nested.document_id, &branch.document_id)
        .await
        .unwrap();
    assert_eq!(
        accepted(
            &node
                .read_one(
                    &alice,
                    &branch.document_id,
                    At::Current,
                    &branch_key(&branch.document_id),
                )
                .await
        )
        .0,
        &json!({"path": "/draft"}),
        "merging a nested branch does not overwrite the branch's own path facet"
    );
    let on_origin = node
        .write(
            &alice,
            &origin.document_id,
            WriteMode::AtCurrent,
            [(key("other"), note("from the origin"))],
            [],
        )
        .await
        .unwrap();
    let merged = node
        .merge(&alice, &branch.document_id, &origin.document_id)
        .await
        .unwrap();
    assert_eq!(merged.document_id, origin.document_id);
    let read = node
        .read(&alice, &origin.document_id, At::Current, false)
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
        origins(&node.claims(&origin.document_id, &key("main")).await),
        BTreeSet::from([branch.document_id.clone()]),
    );
    assert_eq!(
        origins(&node.claims(&origin.document_id, &key("other")).await),
        BTreeSet::from([origin.document_id.clone()]),
    );
    assert_eq!(
        origins(&node.claims(&origin.document_id, &key("third")).await),
        BTreeSet::from([nested.document_id.clone()]),
    );
    accepted(&read.facets[&branch_key(&branch.document_id)]);
    accepted(&read.facets[&branch_key(&nested.document_id)]);
    accepted(&read.facets[&branches_key()]);
    // Writing on the origin after the merge reuses the origin's own document UUID.
    node.write(
        &alice,
        &origin.document_id,
        WriteMode::AtCurrent,
        [(key("main"), note("after"))],
        [],
    )
    .await
    .unwrap();
    assert_eq!(
        origins(&node.claims(&origin.document_id, &key("main")).await),
        BTreeSet::from([origin.document_id.clone()]),
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
        node.merge(&alice, &first.document_id, &second.document_id)
            .await,
        Err(DocumentsError::UnrelatedHistory),
    );
    let branch = node
        .branch(&alice, "branch", &first.document_id, "/draft", true)
        .await
        .unwrap();
    let bob = node.writer("bob").await;
    node.grant(
        &bob,
        LedgerAccess::object(AccessRequest::Read(&branch.document_id)),
    )
    .await;
    node.grant(
        &bob,
        LedgerAccess::object(AccessRequest::Read(&first.document_id)),
    )
    .await;
    assert_eq!(
        node.merge(&bob, &branch.document_id, &first.document_id)
            .await,
        Err(DocumentsError::Denied),
        "a merge writes its target"
    );
    node.sabotage(&branch.document_id, |doc| {
        let facets = doc.get(automerge::ROOT, "facets").unwrap().unwrap().1;
        let mut tx = doc.transaction();
        tx.delete(&facets, key("main").to_string()).unwrap();
        tx.commit();
    })
    .await;
    assert!(matches!(
        node.merge(&alice, &branch.document_id, &first.document_id)
            .await,
        Err(DocumentsError::BaseNotAccepted { facet_key, .. }) if facet_key == key("main")
    ));
    for (document, heads) in [(&first, &first.heads), (&second, &second.heads)] {
        assert_eq!(
            &node
                .read(&alice, &document.document_id, At::Current, false)
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
    let committed = node.repo.allocate_id().await.unwrap();
    let uncommitted = node.repo.allocate_id().await.unwrap();
    for (name, branch) in [("committed", &committed), ("uncommitted", &uncommitted)] {
        ledger
            .begin(
                &command(name),
                Kind::Branch,
                [0; 32],
                alice.uuid,
                None,
                Some(PLACEMENT),
                Some(&Detail::Branch {
                    origin: origin.document_id.clone(),
                    path: format!("/{name}"),
                    stealth: false,
                }),
            )
            .await
            .unwrap();
        ledger.set_document(&command(name), branch).await.unwrap();
    }
    let handle = match node.repo.get_doc(&origin.document_id).await.unwrap() {
        big_repo::DocLookup::Ready(handle) => handle,
        _ => panic!("document is local"),
    };
    let fork = handle
        .with_document_read(|doc| doc.fork_at(&origin.heads).unwrap())
        .await;
    node.repo
        .commit_id(committed.clone(), fork, Vec::new(), &[])
        .await
        .unwrap();
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
                .read_one(&alice, &origin.document_id, At::Current, &branches_key())
                .await
        )
        .0,
        &json!({
            "byId": {committed.to_string(): {"path": "/committed"}},
            "byPath": {"/committed": [committed.to_string()]},
        }),
    );
    // A second boot finds the backlink by its commit message and does not write it again.
    let heads = node
        .read(&alice, &origin.document_id, At::Current, false)
        .await
        .unwrap()
        .heads;
    node.restart().await.unwrap();
    assert_eq!(
        node.read(&alice, &origin.document_id, At::Current, false)
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
        .branch(&alice, "branch", &origin.document_id, "/draft", true)
        .await
        .unwrap();
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    for (name, heads) in [("merged", &origin.heads), ("lost", &branch.heads)] {
        ledger
            .begin(
                &command(name),
                Kind::Merge,
                [0; 32],
                alice.uuid,
                Some(&origin.document_id),
                None,
                Some(&Detail::Merge {
                    source: branch.document_id.clone(),
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
