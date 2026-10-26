//! What a drawer holds: entries, children, self-doc drawers, listings, and foreign roster values.

use super::*;

#[tokio::test]
async fn a_doc_created_in_a_drawer_belongs_to_it_and_to_the_local_drawers() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let drawer = node.collection(&alice, "work").await;

    // Creating needs Write on the placement drawer.
    assert_eq!(
        node.create_doc(&bob, &drawer, "bobs").await.unwrap_err(),
        DocsError::Denied
    );
    let doc = node.create_doc(&alice, &drawer, "plan").await.unwrap();

    let view = node.view(&alice, &drawer).await.unwrap();
    assert_eq!(
        view.access,
        Access::Admin,
        "the creator administers the drawer"
    );
    assert_eq!(view.docs, BTreeMap::from([(doc.clone(), Access::Admin)]));
    let local = node.local();
    for drawer in local.both() {
        let view = node.view(&node.drawers_actor, drawer).await.unwrap();
        assert_eq!(
            view.docs.get(&doc),
            Some(&Access::Admin),
            "{drawer}: {view:?}"
        );
    }
    // The roster doc agrees with the projection, and the docs group holds the grant.
    let roster = node.roster_facets(&drawer).await;
    assert_eq!(roster[&doc_entry_key(&doc)], json!({"access": "Admin"}));
    let (_, docs_group) = groups_of(&view);
    assert_eq!(
        node.keyhive_level(docs_group, doc.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Admin)
    );

    // Containment: access to the doc is access through the drawer.
    assert_eq!(
        node.read_note(&alice, &doc).await.unwrap(),
        json!({"$type": NOTE, "value": "plan"})
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.view(&bob, &drawer).await.unwrap_err(),
        DrawersError::Denied
    );

    node.grant(&alice, &drawer, &bob, Access::Read).await;
    assert!(node.read_note(&bob, &doc).await.is_ok());
    assert_eq!(
        node.write_note(&bob, &doc, "edited").await.unwrap_err(),
        DocsError::Denied,
        "Read on the drawer is not Write on its docs"
    );
    assert_eq!(node.view(&bob, &drawer).await.unwrap().access, Access::Read);

    node.grant(&alice, &drawer, &bob, Access::Write).await;
    node.write_note(&bob, &doc, "edited").await.unwrap();

    node.drawers()
        .call(
            RequestContext::root(),
            RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );

    // Only an admin hands out grants.
    let carol = node.actor("carol").await;
    node.grant(&alice, &drawer, &carol, Access::Write).await;
    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: node.drawer_capability(&carol, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
                access: Access::Read,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::Denied);
    node.stop().await;
}

#[tokio::test]
async fn an_entry_is_added_grant_first_and_removed_entry_first() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "recipe").await.unwrap();
    let shared_docs = groups_of(&node.view(&alice, &shared).await.unwrap()).1;
    let doc_bytes = doc.to_bytes32().unwrap();

    let receipt = node
        .add_doc(&alice, "share recipe", &shared, &doc, Access::Read)
        .await
        .unwrap();
    assert_eq!(receipt.doc_id, shared);
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Read)
    );
    assert_eq!(
        node.view(&alice, &shared).await.unwrap().docs,
        BTreeMap::from([(doc.clone(), Access::Read)])
    );
    // The same command replays its receipt; a different request under it is refused.
    assert_eq!(
        node.add_doc(&alice, "share recipe", &shared, &doc, Access::Read)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        node.add_doc(&alice, "share recipe", &shared, &doc, Access::Write)
            .await
            .unwrap_err(),
        DrawersError::CommandReused {
            command: command("share recipe")
        }
    );
    // The doc stays in its first drawer: an entry is per roster.
    assert!(
        node.view(&alice, &home)
            .await
            .unwrap()
            .docs
            .contains_key(&doc)
    );

    // Widen, then narrow: both end with the roster and the grant agreeing.
    node.add_doc(&alice, "widen", &shared, &doc, Access::Write)
        .await
        .unwrap();
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Edit)
    );
    node.add_doc(&alice, "narrow", &shared, &doc, Access::Relay)
        .await
        .unwrap();
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Relay)
    );
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Relay"})
    );

    // Only Write on the target drawer may add; Admin entries need Admin; and conferring a
    // level on the doc takes holding that level on the doc through some path of one's own.
    let bob = node.actor("bob").await;
    node.grant(&alice, &shared, &bob, Access::Write).await;
    assert_eq!(
        node.add_doc(&bob, "bob admin", &shared, &doc, Access::Admin)
            .await
            .unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.add_doc(&bob, "bob read", &shared, &doc, Access::Read)
            .await
            .unwrap_err(),
        DrawersError::Denied,
        "bob holds Write on the drawer but nothing on the doc yet"
    );
    node.grant(&alice, &home, &bob, Access::Read).await;
    node.add_doc(&bob, "bob read", &shared, &doc, Access::Read)
        .await
        .unwrap();

    node.remove_doc(&alice, "unshare", &shared, &doc)
        .await
        .unwrap();
    assert!(
        !node
            .roster_facets(&shared)
            .await
            .contains_key(&doc_entry_key(&doc))
    );
    assert_eq!(node.keyhive_level(shared_docs, doc_bytes).await, None);
    assert!(node.view(&alice, &shared).await.unwrap().docs.is_empty());
    assert_eq!(
        node.remove_doc(&alice, "unshare again", &shared, &doc)
            .await
            .unwrap_err(),
        DrawersError::NotAnEntry(doc.clone())
    );
    // Removal from one drawer is not deletion: the doc is still readable through home.
    assert!(node.read_note(&alice, &doc).await.is_ok());
    node.stop().await;
}

#[tokio::test]
async fn a_child_drawer_confers_access_through_its_parent() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let parent = node.collection(&alice, "team").await;
    let child = node.collection(&alice, "project").await;
    let doc = node.create_doc(&alice, &child, "spec").await.unwrap();
    node.grant(&alice, &parent, &bob, Access::Write).await;
    let parent_cap = node
        .mint_drawer(
            &bob,
            DrawerScope {
                drawers: [(parent.clone(), Access::Write)].into(),
                create: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );

    let (parent_meta, parent_docs) = groups_of(&node.view(&alice, &parent).await.unwrap());
    let (child_meta, child_docs) = groups_of(&node.view(&alice, &child).await.unwrap());

    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("self"),
                parent_id: parent.clone(),
                child_id: parent.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::SelfChild);

    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child])
                    .await
                    .unwrap(),
                command: command("adopt"),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.roster_facets(&parent).await[&child_entry_key(&child)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.view(&alice, &parent).await.unwrap().children,
        BTreeMap::from([(child.clone(), Access::Read)])
    );
    assert_eq!(
        node.keyhive_level(parent_docs, child_docs.0).await,
        Some(KeyhiveAccess::Read)
    );
    assert_eq!(
        node.keyhive_level(parent_meta, child_meta.0).await,
        Some(KeyhiveAccess::Read)
    );
    // Bob's Write on the parent is Read on the child: the entry is the weakest link.
    assert!(node.read_note(&bob, &doc).await.is_ok());
    assert_eq!(
        node.write_note(&bob, &doc, "edit").await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(node.view(&bob, &child).await.unwrap().access, Access::Read);
    assert_eq!(
        node.drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap()
            .access,
        Access::Read
    );

    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("disown"),
                parent_id: parent.clone(),
                child_id: child.clone(),
            },
        )
        .await
        .unwrap();
    assert!(
        !node
            .roster_facets(&parent)
            .await
            .contains_key(&child_entry_key(&child))
    );
    assert_eq!(node.keyhive_level(parent_docs, child_docs.0).await, None);
    assert_eq!(node.keyhive_level(parent_meta, child_meta.0).await, None);
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    node.stop().await;
}

#[tokio::test]
async fn a_self_doc_drawer_is_the_doc_itself() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let doc = node.create_doc(&alice, &home, "letter").await.unwrap();

    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node
                    .mint_drawer(&bob, DrawerScope::default())
                    .await
                    .unwrap(),
                command: command("bob declares"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::Denied);

    let created = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(created.drawer_id, doc);
    assert_eq!(
        node.roster_facets(&doc).await[&drawer_key()],
        serde_json::to_value(Drawer::self_doc()).unwrap()
    );
    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare twice"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::AlreadyDrawer(doc.clone()));

    // Sharing the self-doc drawer is sharing the doc.
    node.grant(&alice, &doc, &bob, Access::Read).await;
    assert!(node.read_note(&bob, &doc).await.is_ok());
    let view = node.view(&bob, &doc).await.unwrap();
    assert_eq!(view.layout.known(), Some(LayoutKind::SelfDoc));
    assert_eq!(view.descriptor.groups(), None);
    assert!(view.docs.is_empty());

    // It holds only itself.
    let other = node.create_doc(&alice, &home, "other").await.unwrap();
    assert_eq!(
        node.add_doc(&alice, "grow", &doc, &other, Access::Read)
            .await
            .unwrap_err(),
        DrawersError::SelfDoc(doc.clone())
    );

    let capability = node.drawer_capability(&bob, &[&doc]).await.unwrap();
    let cached_descriptor = node
        .core()
        .state
        .drawer(&doc)
        .await
        .unwrap()
        .unwrap()
        .descriptor;
    assert_eq!(cached_descriptor, Drawer::self_doc());
    let listed = node
        .drawers()
        .call(RequestContext::root(), ListDrawers { capability })
        .await
        .unwrap();
    assert_eq!(
        listed,
        vec![ListedDrawer {
            drawer_id: doc.clone(),
            layout: cached_descriptor.layout.clone(),
            descriptor: cached_descriptor.clone(),
            access: Access::Read,
        }]
    );

    let mut value = serde_json::to_value(&cached_descriptor).unwrap();
    value["layout"]["revision"] = json!(2);
    let fresh_descriptor: Drawer = serde_json::from_value(value.clone()).unwrap();
    assert_ne!(cached_descriptor.layout, fresh_descriptor.layout);
    node.docs()
        .call(
            RequestContext::root(),
            WriteFacets {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: [doc.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: None,
                doc_id: doc.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(drawer_key(), value.clone())]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(node.roster_facets(&doc).await[&drawer_key()], value);
    node.core()
        .state
        .put_drawer(&doc, &cached_descriptor)
        .await
        .unwrap();
    assert_eq!(
        node.core()
            .state
            .drawer(&doc)
            .await
            .unwrap()
            .unwrap()
            .descriptor,
        cached_descriptor
    );
    assert_eq!(
        node.drawers()
            .call(RequestContext::root(), ListDrawers { capability })
            .await
            .unwrap(),
        vec![ListedDrawer {
            drawer_id: doc.clone(),
            layout: fresh_descriptor.layout.clone(),
            descriptor: fresh_descriptor,
            access: Access::Read,
        }]
    );
    assert_eq!(
        node.core()
            .state
            .drawer(&doc)
            .await
            .unwrap()
            .unwrap()
            .descriptor,
        cached_descriptor
    );
    node.stop().await;
}

/// Every roster read or write the service makes on a caller's behalf narrows its own doc
/// authority to a handle minted for that call. Those handles are retracted when the call
/// returns, so serving requests leaves the authority's live set where it was.
#[tokio::test]
async fn serving_requests_leaves_no_service_handles_in_the_authority() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "letter").await.unwrap();
    node.grant(&alice, &shared, &bob, Access::Read).await;
    let bob_lists = node.drawer_capability(&bob, &[&shared]).await.unwrap();
    let alice_edits = node
        .drawer_capability(&alice, &[&home, &shared])
        .await
        .unwrap();
    let live = node.authority.service.live_capabilities().await;
    for round in 0..3 {
        node.drawers()
            .call(
                RequestContext::root(),
                ListDrawers {
                    capability: bob_lists,
                },
            )
            .await
            .unwrap();
        for capability in [alice_edits, bob_lists] {
            node.drawers()
                .call(
                    RequestContext::root(),
                    GetDrawer {
                        capability,
                        drawer_id: shared.clone(),
                    },
                )
                .await
                .unwrap();
        }
        for name in ["add", "again"] {
            node.drawers()
                .call(
                    RequestContext::root(),
                    AddDoc {
                        capability: alice_edits,
                        command: command(&format!("{name} {round}")),
                        drawer_id: shared.clone(),
                        doc_id: doc.clone(),
                        access: Access::Read,
                    },
                )
                .await
                .unwrap();
        }
        node.drawers()
            .call(
                RequestContext::root(),
                RemoveDoc {
                    capability: alice_edits,
                    command: command(&format!("remove {round}")),
                    drawer_id: shared.clone(),
                    doc_id: doc.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            node.authority.service.live_capabilities().await,
            live,
            "round {round} left service handles behind"
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn list_docs_is_one_check_then_the_scoped_roster() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let sub = node.collection(&alice, "sub").await;
    let first = node.create_doc(&alice, &home, "first").await.unwrap();
    let second = node.create_doc(&alice, &home, "second").await.unwrap();
    let inner = node.create_doc(&alice, &sub, "inner").await.unwrap();

    // The drawer is read by nobody else yet: bob cannot list it at all, even though a
    // doc in it might be shared with him later through another path.
    assert_eq!(
        node.list_docs(&bob, &home, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );

    // A relay enumerates: relay entry-capped docs appear as Relay, and relay docs are
    // filtered out — Relay exists to enumerate, not to read content through.
    node.grant(&alice, &home, &bob, Access::Relay).await;
    let listed = node.list_docs(&bob, &home, 0, None).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|listed| (listed.doc_id.clone(), listed.access))
            .collect::<std::collections::BTreeMap<_, _>>(),
        [
            (first.clone(), Access::Relay),
            (second.clone(), Access::Relay),
        ]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>(),
        "bob holds Relay on the drawer, so entries cap at Relay"
    );

    // A child drawer's docs list through the parent at the weaker of the link and the
    // child's own entry; `via` names the drawer that lists it.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&home, &sub])
                    .await
                    .unwrap(),
                command: command("adopt"),
                parent_id: home.clone(),
                child_id: sub.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    let listed = node.list_docs(&bob, &home, 0, None).await.unwrap();
    let inner_listing = listed
        .iter()
        .find(|listed| listed.doc_id == inner)
        .expect("bob enumerates the child doc through the parent's link");
    assert_eq!(
        inner_listing.access,
        Access::Relay,
        "bob's Relay on the parent confers only enumeration on the child"
    );
    let listed = node.list_docs(&alice, &home, 0, None).await.unwrap();
    let inner_listing = listed
        .iter()
        .find(|listed| listed.doc_id == inner)
        .expect("alice's Write grant reads the child link");
    assert_eq!(inner_listing.access, Access::Read);
    assert_eq!(inner_listing.via, Some(sub.clone()));

    // Paging skips and caps the ordered result.
    let page = node.list_docs(&alice, &home, 1, Some(1)).await.unwrap();
    assert_eq!(page.len(), 1);
    let all = node.list_docs(&alice, &home, 0, None).await.unwrap();
    assert_eq!(page[0].doc_id, all[1].doc_id, "pages slice the same order");

    // Revoking the drawer grant ends the listing, whatever the projection holds.
    node.drawers()
        .call(
            RequestContext::root(),
            crate::drawer::RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                drawer_id: home.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.list_docs(&bob, &home, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );
    node.stop().await;
}

#[tokio::test]
async fn an_entry_with_an_unknown_access_level_is_carried_without_granting_or_breaking_the_roster()
{
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let child = node.collection(&alice, "child").await;
    let known_doc = node.create_doc(&alice, &home, "known").await.unwrap();
    let foreign_doc = node.create_doc(&alice, &home, "foreign").await.unwrap();
    node.grant(&alice, &shared, &bob, Access::Read).await;
    node.drawers()
        .call(
            RequestContext::root(),
            AddDoc {
                capability: node
                    .drawer_capability(&alice, &[&home, &shared])
                    .await
                    .unwrap(),
                command: command("enter known"),
                drawer_id: shared.clone(),
                doc_id: known_doc.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    // Write a docEntry and a childEntry carrying a future access string directly onto the drawer
    // doc: the schema admits them (`knownValues`), rescan skips them for the projection, and they
    // remain on the doc untouched without granting Bob access to `foreign_doc` or `child`.
    node.docs()
        .call(
            RequestContext::root(),
            WriteFacets {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: [shared.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: None,
                doc_id: shared.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([
                    (
                        doc_entry_key(&foreign_doc),
                        json!({"access": "Pull", "future": 1}),
                    ),
                    (child_entry_key(&child), json!({"access": "Preview"})),
                ]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let view = node.view(&bob, &shared).await.unwrap();
    assert_eq!(
        view.docs,
        BTreeMap::from([(known_doc.clone(), Access::Read)])
    );
    assert!(view.children.is_empty());
    assert_eq!(
        node.list_docs(&bob, &shared, 0, None).await.unwrap(),
        vec![ListedDoc {
            doc_id: known_doc.clone(),
            access: Access::Read,
            via: None,
        }]
    );
    assert!(node.read_note(&bob, &known_doc).await.is_ok());
    assert_eq!(
        node.read_note(&bob, &foreign_doc).await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.view(&bob, &child).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&foreign_doc)],
        json!({"access": "Pull", "future": 1})
    );
    node.stop().await;
}
