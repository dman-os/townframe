//! Attribution on reads and listings: who wrote the accepted version of a datum.

use super::*;
use big_cabinet_core::doc::DatumAuthor;

/// The author a read reports for `actor`: its id and the URL it was opened with.
pub(super) fn author(actor: &OpenedActor) -> DatumAuthor {
    DatumAuthor {
        actor: actor.actor.actor_id,
        url: actor.actor.url.clone(),
    }
}

pub(super) fn authors_of(read: &DatumRead) -> &[DatumAuthor] {
    match read {
        DatumRead::Accepted { authors, .. } => authors,
        other => panic!("{other:?} is not an accepted read"),
    }
}

async fn writer_of(node: &Node, document: &DocId, path: &str) -> OpenedActor {
    let actor = node.writer(path).await;
    node.grant(&actor, LedgerAccess::object(AccessRequest::Read(document)))
        .await;
    node.grant(&actor, LedgerAccess::object(AccessRequest::Write(document)))
        .await;
    actor
}

async fn listed(
    node: &Node,
    actor: &OpenedActor,
    document: &DocId,
    with_authors: bool,
) -> DatumListing {
    op(node
        .client()
        .call(
            RequestContext::root(),
            ListDatums {
                capability: node
                    .capability(
                        actor,
                        DocScope {
                            read: BTreeSet::from([document.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await
                    .unwrap(),
                doc_id: document.clone(),
                at: At::Current,
                with_authors,
            },
        )
        .await)
    .unwrap()
}

#[tokio::test]
async fn a_read_names_the_actor_that_wrote_the_version_and_follows_overwrites() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("by alice"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let bob = writer_of(&node, document, "bob").await;

    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert_eq!(authors_of(&read), [author(&alice)]);
    assert!(
        author(&alice).url.contains("alice"),
        "the URL is the actor's own: {}",
        author(&alice).url
    );

    node.write(
        &bob,
        document,
        WriteMode::AtCurrent,
        [(key("main"), note("by bob"))],
        [],
    )
    .await
    .unwrap();
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert_eq!(authors_of(&read), [author(&bob)]);
    // An earlier version keeps its own author.
    let earlier = node
        .read_one(
            &alice,
            document,
            At::Heads(created.heads.clone()),
            &key("main"),
        )
        .await;
    assert_eq!(authors_of(&earlier), [author(&alice)]);
    node.stop().await;
}

#[tokio::test]
async fn concurrent_writes_by_two_actors_list_both_authors_sorted_by_actor_id() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("base"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let bob = writer_of(&node, document, "bob").await;
    for (actor, text) in [(&alice, "left"), (&bob, "right")] {
        node.write(
            actor,
            document,
            WriteMode::AtHeads {
                heads: created.heads.clone(),
            },
            [(key("main"), note(text))],
            [],
        )
        .await
        .unwrap();
    }
    let mut expected = vec![author(&alice), author(&bob)];
    expected.sort_by_key(|author| author.actor);
    let read = node
        .read_one(&alice, document, At::Current, &key("main"))
        .await;
    assert_eq!(authors_of(&read), expected.as_slice());
    node.stop().await;
}

/// A descendant is the owner's to name; the datum it signs carries the descendant's id and URL,
/// not the owner's.
#[tokio::test]
async fn a_datum_written_by_a_descendant_carries_the_descendants_id_and_url() {
    let node = Node::boot().await;
    let owner = node
        .authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: node.authority.root,
                path: "owner".into(),
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE | Rights::MINT_CHILD,
            },
        )
        .await
        .unwrap();
    node.grant(
        &owner,
        LedgerAccess::object(AccessRequest::Create {
            placement: PLACEMENT,
        }),
    )
    .await;
    let agent = node
        .authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: owner.capability,
                path: "owner/agent".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let created = node
        .create(&owner, "create", [(key("main"), note("before"))])
        .await
        .unwrap();
    let scope = DocScope {
        write: std::collections::BTreeMap::from([(
            created.doc_id.clone(),
            big_cabinet_core::doc::DatumKinds::All,
        )]),
        ..DocScope::default()
    };
    let delegate = node
        .client()
        .call_with(
            RequestContext::root(),
            Authorization::identity(owner.capability).metadata(),
            MintDocCapability {
                scope: scope.clone(),
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                subject: None,
            },
        )
        .await
        .unwrap();
    let introduced = op(node
        .client()
        .call_with(
            RequestContext::root(),
            Authorization::identity(delegate.capability_id).metadata(),
            MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: Some(agent.actor.actor_id),
            },
        )
        .await)
    .unwrap();
    node.client()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: introduced.capability_id,
                command: None,
                doc_id: created.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key("main"), note("by the agent"))]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let read = node
        .read_one(&owner, &created.doc_id, At::Current, &key("main"))
        .await;
    assert_eq!(authors_of(&read), [author(&agent)]);
    assert_ne!(author(&agent).url, author(&owner).url);
    node.stop().await;
}

#[tokio::test]
async fn listing_reports_authors_only_when_asked_and_they_match_reads() {
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
    let bob = writer_of(&node, document, "bob").await;
    node.write(
        &bob,
        document,
        WriteMode::AtCurrent,
        [(key("b"), note("b by bob"))],
        [],
    )
    .await
    .unwrap();

    let plain = listed(&node, &alice, document, false).await;
    assert!(plain.authors.is_empty());
    let asked = listed(&node, &alice, document, true).await;
    assert_eq!(asked.datums, plain.datums);
    let read = node
        .read(&alice, document, At::Current, false)
        .await
        .unwrap();
    assert_eq!(asked.authors.len(), 2);
    for (datum, row) in &read.datums {
        assert_eq!(asked.authors[datum], authors_of(row), "{datum}");
    }
    assert_eq!(asked.authors[&key("a")], [author(&alice)]);
    assert_eq!(asked.authors[&key("b")], [author(&bob)]);
    node.stop().await;
}
