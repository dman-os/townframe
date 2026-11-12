//! What a drawer holds: entries, children, self-doc drawers, listings, and foreign roster values.

use super::*;
use big_cabinet_core::drawer::MetaRoot;

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
    let roster = node.roster_datums(&drawer).await;
    assert_eq!(roster[&doc_entry_key(&doc)], json!({"access": "Admin"}));
    let (_, docs_group, _) = groups_of(&view);
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
    let drawer_capability = node
        .mint_doc(
            &bob,
            DocScope {
                drawers: BTreeMap::from([(drawer.clone(), Access::Write)]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (key, value) = note("drawer capability");
    node.docs()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: drawer_capability,
                command: None,
                doc_id: doc.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key, value)]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();

    node.drawers()
        .call(
            RequestContext::root(),
            RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor.actor_id,
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
                subject_id: bob.actor.actor_id,
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
        node.roster_datums(&shared).await[&doc_entry_key(&doc)],
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
        node.roster_datums(&shared).await[&doc_entry_key(&doc)],
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
            .roster_datums(&shared)
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

    let (parent_meta, parent_docs, parent_sensitive) =
        groups_of(&node.view(&alice, &parent).await.unwrap());
    let (child_meta, child_docs, child_sensitive) =
        groups_of(&node.view(&alice, &child).await.unwrap());

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
        node.roster_datums(&parent).await[&child_entry_key(&child)],
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
    // The Read entry also reaches the child's sensitive group: Read-and-up is that group's
    // audience, and the parent's sensitive group is the route.
    assert_eq!(
        node.keyhive_level(parent_sensitive, child_sensitive.0)
            .await,
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
            .roster_datums(&parent)
            .await
            .contains_key(&child_entry_key(&child))
    );
    assert_eq!(node.keyhive_level(parent_docs, child_docs.0).await, None);
    assert_eq!(node.keyhive_level(parent_meta, child_meta.0).await, None);
    assert_eq!(
        node.keyhive_level(parent_sensitive, child_sensitive.0)
            .await,
        None
    );
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
async fn a_self_doc_child_takes_only_a_doc_grant_at_the_entry_level() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let parent = node.collection(&alice, "team").await;
    let home = node.collection(&alice, "home").await;
    let doc = node.create_doc(&alice, &home, "letter").await.unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare letter"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap();

    // A self-doc parent has no docs group to delegate, so it cannot adopt children.
    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&doc, &parent])
                    .await
                    .unwrap(),
                command: command("self-doc adopts"),
                parent_id: doc.clone(),
                child_id: parent.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::SelfDoc(doc.clone()));

    node.grant(&alice, &parent, &bob, Access::Read).await;
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );

    let (_, parent_docs, _) = groups_of(&node.view(&alice, &parent).await.unwrap());
    let doc_bytes = doc.to_bytes32().unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &doc])
                    .await
                    .unwrap(),
                command: command("adopt letter"),
                parent_id: parent.clone(),
                child_id: doc.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.roster_datums(&parent).await[&child_entry_key(&doc)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.view(&alice, &parent).await.unwrap().children,
        BTreeMap::from([(doc.clone(), Access::Read)])
    );
    // Exactly `Read` on the doc itself: no meta widening and no group delegation exists.
    assert_eq!(
        node.keyhive_level(parent_docs, doc_bytes).await,
        Some(KeyhiveAccess::Read)
    );
    assert!(node.read_note(&bob, &doc).await.is_ok());
    assert_eq!(
        node.write_note(&bob, &doc, "edit").await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.list_docs(&bob, &parent, 0, None)
            .await
            .unwrap()
            .iter()
            .map(|listed| listed.doc_id.clone())
            .collect::<Vec<_>>(),
        vec![doc.clone()]
    );

    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("disown letter"),
                parent_id: parent.clone(),
                child_id: doc.clone(),
            },
        )
        .await
        .unwrap();
    assert!(
        !node
            .roster_datums(&parent)
            .await
            .contains_key(&child_entry_key(&doc))
    );
    assert_eq!(node.keyhive_level(parent_docs, doc_bytes).await, None);
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
        node.roster_datums(&doc).await[&drawer_key()],
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
        ListedDrawers {
            drawers: vec![ListedDrawer {
                drawer_id: doc.clone(),
                layout: cached_descriptor.layout.clone(),
                descriptor: cached_descriptor.clone(),
                access: Access::Read,
            }]
        }
    );

    let mut value = serde_json::to_value(&cached_descriptor).unwrap();
    value["layout"]["revision"] = json!(2);
    let fresh_descriptor: Drawer = serde_json::from_value(value.clone()).unwrap();
    assert_ne!(cached_descriptor.layout, fresh_descriptor.layout);
    node.docs()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: std::collections::BTreeMap::from([(
                                doc.clone(),
                                big_cabinet_core::doc::DatumKinds::All,
                            )]),
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
    assert_eq!(node.roster_datums(&doc).await[&drawer_key()], value);
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
        ListedDrawers {
            drawers: vec![ListedDrawer {
                drawer_id: doc.clone(),
                layout: fresh_descriptor.layout.clone(),
                descriptor: fresh_descriptor,
                access: Access::Read,
            }]
        }
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
                        reach: alice_edits,
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
async fn list_doc_drawers_names_the_drawers_the_presented_authority_reaches_the_doc_through() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let other = node.collection(&alice, "other").await;
    let doc = node.create_doc(&alice, &home, "recipe").await.unwrap();
    node.add_doc(&alice, "share recipe", &shared, &doc, Access::Read)
        .await
        .unwrap();
    let home_access = node.view(&alice, &home).await.unwrap().docs[&doc];

    // The operator reaches every drawer: the doc's two collections and the local drawers every
    // doc is entered into, in drawer id order, each at the access its entry confers.
    let operator = Authorization::identity(node.authority.root);
    let listed = node.list_doc_drawers(operator, &doc).await.unwrap();
    assert!(
        listed.is_sorted_by_key(|entry| entry.drawer_id.clone()),
        "listing is ordered by drawer id: {listed:?}"
    );
    let by_drawer: BTreeMap<DrawerId, Access> = listed
        .into_iter()
        .map(|entry| (entry.drawer_id, entry.access))
        .collect();
    assert_eq!(by_drawer[&home], home_access);
    assert_eq!(by_drawer[&shared], Access::Read);
    assert!(!by_drawer.contains_key(&other));
    for local in node.local().both() {
        assert!(by_drawer.contains_key(local), "the local drawer {local}");
    }

    // An ordinary actor reaches the doc through the drawers it holds grants on, bounded by the
    // entry: bob holds Write on `shared`, so he gets the entry's Read and nothing through `home`.
    let bob = node.actor("bob").await;
    node.grant(&alice, &shared, &bob, Access::Write).await;
    assert_eq!(
        node.list_doc_drawers(Authorization::identity(bob.capability), &doc)
            .await
            .unwrap(),
        vec![DocDrawer {
            drawer_id: shared.clone(),
            access: Access::Read
        }]
    );

    // A scoped handle lists only the drawers it names, at the scope's access bounded by the entry.
    let via_shared = node.drawer_capability(&alice, &[&shared]).await.unwrap();
    assert_eq!(
        node.list_doc_drawers(
            Authorization {
                authority: via_shared,
                signer: alice.capability
            },
            &doc
        )
        .await
        .unwrap(),
        vec![DocDrawer {
            drawer_id: shared.clone(),
            access: Access::Read
        }]
    );
    let via_other = node.drawer_capability(&alice, &[&other]).await.unwrap();
    assert!(
        node.list_doc_drawers(
            Authorization {
                authority: via_other,
                signer: alice.capability
            },
            &doc
        )
        .await
        .unwrap()
        .is_empty(),
        "a handle naming only a drawer the doc is not in reaches it through nothing"
    );

    // A doc in no drawer answers an empty list, not an error.
    assert!(
        node.list_doc_drawers(operator, &DocId::new([9; 32]))
            .await
            .unwrap()
            .is_empty()
    );
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
            big_cabinet_core::drawer::protocol::RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                drawer_id: home.clone(),
                subject_id: bob.actor.actor_id,
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
    let capability = node
        .drawer_capability(&alice, &[&home, &shared])
        .await
        .unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddDoc {
                capability,
                reach: capability,
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
            WriteDatums {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: std::collections::BTreeMap::from([(
                                shared.clone(),
                                big_cabinet_core::doc::DatumKinds::All,
                            )]),
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
        node.roster_datums(&shared).await[&doc_entry_key(&foreign_doc)],
        json!({"access": "Pull", "future": 1})
    );
    node.stop().await;
}

#[tokio::test]
async fn a_meta_doc_is_created_granted_and_projected() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let capability = node.drawer_capability(&alice, &[&home]).await.unwrap();
    let request = CreateMetaDoc {
        capability,
        command: command("meta-1"),
        drawer_id: home.clone(),
        kind: "notes".to_owned(),
        name: "main".to_owned(),
        root: MetaRoot::Public,
    };

    let created = node
        .drawers()
        .call(RequestContext::root(), request.clone())
        .await
        .unwrap();
    assert_ne!(created.doc_id, home, "a meta doc has its own id");
    assert_eq!(created.kind, request.kind);
    assert_eq!(created.name, request.name);
    assert_eq!(created.root, request.root);
    assert_eq!(created.drawer_id, home);
    let view = node.view(&alice, &home).await.unwrap();
    let mut expected = BTreeMap::from([(
        MetaRoot::Public,
        BTreeMap::from([("notes/main".to_owned(), created.doc_id.clone())]),
    )]);
    assert_eq!(view.meta_docs, expected);

    let replayed = node
        .drawers()
        .call(RequestContext::root(), request.clone())
        .await
        .unwrap();
    assert_eq!(replayed, created);
    assert_eq!(node.view(&alice, &home).await.unwrap().meta_docs, expected);

    for (kind, name) in [("..", "main"), ("notes", "..")] {
        let error = node
            .drawers()
            .call(
                RequestContext::root(),
                CreateMetaDoc {
                    command: command(&format!("invalid {kind}/{name}")),
                    kind: kind.to_owned(),
                    name: name.to_owned(),
                    ..request.clone()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            op(error),
            DrawersError::InvalidMetaDocSlot(format!("{kind}/{name}"))
        );
        assert_eq!(node.view(&alice, &home).await.unwrap().meta_docs, expected);
    }

    for (root, kind, name) in [
        (MetaRoot::Sensitive, "notes", "main"),
        (MetaRoot::Public, "notes", "archive"),
        (MetaRoot::Sensitive, "notes", "archive"),
        (MetaRoot::Public, "audit", "main"),
        (MetaRoot::Sensitive, "audit", "main"),
    ] {
        let created = node
            .drawers()
            .call(
                RequestContext::root(),
                CreateMetaDoc {
                    command: command(&format!("{root:?} {kind}/{name}")),
                    kind: kind.to_owned(),
                    name: name.to_owned(),
                    root,
                    ..request.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(created.drawer_id, home);
        assert_eq!(created.root, root);
        assert_eq!(created.kind, kind);
        assert_eq!(created.name, name);
        assert!(
            expected
                .values()
                .all(|slots| !slots.values().any(|doc| *doc == created.doc_id))
        );
        assert!(
            expected
                .entry(root)
                .or_default()
                .insert(format!("{kind}/{name}"), created.doc_id)
                .is_none()
        );
        assert_eq!(node.view(&alice, &home).await.unwrap().meta_docs, expected);
    }

    let view = node.view(&alice, &home).await.unwrap();
    assert!(view.docs.is_empty());
    assert!(view.children.is_empty());
    assert!(
        node.list_docs(&alice, &home, 0, None)
            .await
            .unwrap()
            .is_empty()
    );
    let (public_group, docs_group, sensitive_group) = groups_of(&view);
    for (root, slots) in &expected {
        let (root_doc, backing, other) = match root {
            MetaRoot::Public => (home.clone(), public_group, sensitive_group),
            MetaRoot::Sensitive => (
                view.descriptor.sensitive_root().unwrap(),
                sensitive_group,
                public_group,
            ),
        };
        let references = node
            .roster_datums(&root_doc)
            .await
            .into_iter()
            .filter(|(key, _)| key.tag().as_str() == DrawerMetaDoc::ID)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            references,
            slots
                .iter()
                .map(|(slot, doc)| (
                    format!("{}/{slot}", DrawerMetaDoc::ID).parse().unwrap(),
                    json!({"docId": doc.to_string()}),
                ))
                .collect()
        );
        for doc in slots.values() {
            let target = doc.to_bytes32().unwrap();
            assert_eq!(
                node.keyhive_level(backing, target).await,
                Some(KeyhiveAccess::Admin)
            );
            assert_eq!(node.keyhive_level(other, target).await, None);
            assert_eq!(node.keyhive_level(docs_group, target).await, None);
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn meta_doc_projection_requires_admin_delegation_from_the_referencing_root() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let source = node.collection(&alice, "source").await;
    node.grant(&alice, &home, &bob, Access::Read).await;
    let view = node.view(&alice, &home).await.unwrap();
    let (public_group, _, sensitive_group) = groups_of(&view);
    let context = RequestContext::root();
    let mut expected = BTreeMap::new();
    for root in [MetaRoot::Public, MetaRoot::Sensitive] {
        let doc = node
            .create_doc(&alice, &source, &format!("{root:?} note"))
            .await
            .unwrap();
        let (root_doc, backing, other) = match root {
            MetaRoot::Public => (home.clone(), public_group, sensitive_group),
            MetaRoot::Sensitive => (
                view.descriptor.sensitive_root().unwrap(),
                sensitive_group,
                public_group,
            ),
        };
        node.core()
            .write_roster(
                &context,
                &root_doc,
                None,
                BTreeMap::from([(
                    meta_doc_key("notes", "main").unwrap(),
                    json!({"docId": doc.to_string(), "future": {"revision": 2}}),
                )]),
                BTreeSet::new(),
            )
            .await
            .unwrap();
        for delegation in [
            None,
            Some((other, KeyhiveAccess::Admin)),
            Some((backing, KeyhiveAccess::Relay)),
            Some((backing, KeyhiveAccess::Read)),
            Some((backing, KeyhiveAccess::Edit)),
        ] {
            if let Some((group, access)) = delegation {
                node.repo
                    .grant_doc_access(doc.clone(), node.core().group(group).await.unwrap(), access)
                    .await
                    .unwrap();
            }
            assert_eq!(node.view(&alice, &home).await.unwrap().meta_docs, expected);
            assert!(
                !node
                    .core()
                    .state
                    .ancestors(state::ScopeTarget::Doc(&doc))
                    .await
                    .unwrap()
                    .contains_key(&home)
            );
            assert_eq!(
                node.read_note(&bob, &doc).await.unwrap_err(),
                DocsError::Denied
            );
            assert_eq!(
                node.roster_datums(&root_doc).await[&meta_doc_key("notes", "main").unwrap()],
                json!({"docId": doc.to_string(), "future": {"revision": 2}})
            );
        }
        node.repo
            .grant_doc_access(
                doc.clone(),
                node.core().group(backing).await.unwrap(),
                KeyhiveAccess::Admin,
            )
            .await
            .unwrap();
        assert!(
            expected
                .insert(
                    root,
                    BTreeMap::from([("notes/main".to_owned(), doc.clone())])
                )
                .is_none()
        );
        assert_eq!(node.view(&bob, &home).await.unwrap().meta_docs, expected);
        assert_eq!(
            node.core()
                .state
                .ancestors(state::ScopeTarget::Doc(&doc))
                .await
                .unwrap()
                .get(&home)
                .and_then(|reach| reach.apply(Access::Read)),
            Some(Access::Read)
        );
        assert_eq!(
            node.read_note(&bob, &doc).await.unwrap(),
            json!({"$type": NOTE, "value": format!("{root:?} note")})
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn metadata_protocol_respects_root_and_child_audiences() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let mut drawers = Vec::new();
    for name in ["root", "child", "grandchild", "relay leaf"] {
        drawers.push(node.collection(&alice, name).await);
    }
    let ceilings = [Access::Admin, Access::Write, Access::Read, Access::Relay];
    for (pair, access) in drawers.windows(2).zip(ceilings[1..].iter().copied()) {
        node.drawers()
            .call(
                RequestContext::root(),
                AddChild {
                    capability: node
                        .drawer_capability(&alice, &[&pair[0], &pair[1]])
                        .await
                        .unwrap(),
                    command: command(&format!("link {}", pair[1])),
                    parent_id: pair[0].clone(),
                    child_id: pair[1].clone(),
                    access,
                },
            )
            .await
            .unwrap();
    }
    let mut documents = Vec::new();
    for (drawer, ceiling) in drawers.iter().zip(ceilings) {
        let capability = node.drawer_capability(&alice, &[drawer]).await.unwrap();
        for root in [MetaRoot::Public, MetaRoot::Sensitive] {
            let created = node
                .drawers()
                .call(
                    RequestContext::root(),
                    CreateMetaDoc {
                        capability,
                        command: command(&format!("{drawer} {root:?}")),
                        drawer_id: drawer.clone(),
                        kind: "notes".into(),
                        name: "main".into(),
                        root,
                    },
                )
                .await
                .unwrap();
            documents.push((created.doc_id.clone(), root, ceiling));
            let view = node.view(&alice, drawer).await.unwrap();
            let root_doc = match root {
                MetaRoot::Public => drawer.clone(),
                MetaRoot::Sensitive => view.descriptor.sensitive_root().unwrap(),
            };
            node.wait_indexed(DrawerMetaDoc::ID, |route, _| route.doc_id == root_doc)
                .await;
            documents.push((root_doc, root, ceiling));
            node.write_note(&alice, &created.doc_id, "metadata target")
                .await
                .unwrap();
            node.wait_indexed(NOTE, |route, _| route.doc_id == created.doc_id)
                .await;
        }
    }
    for held in Access::ALL {
        let actor = node.actor(&format!("holder-{held:?}")).await;
        node.grant(&alice, &drawers[0], &actor, held).await;
        let bounded = node
            .mint_doc(
                &alice,
                DocScope {
                    drawers: BTreeMap::from([(drawers[0].clone(), held)]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let scoped = node
            .mint_drawer(
                &alice,
                DrawerScope {
                    drawers: BTreeMap::from([(drawers[0].clone(), held)]),
                    create: false,
                },
            )
            .await
            .unwrap();
        let narrowed = node
            .core()
            .invocation(&RequestContext::root(), scoped)
            .await
            .unwrap()
            .authority;
        for (doc, root, ceiling) in &documents {
            let attenuated = held.min(*ceiling);
            let expected = match root {
                MetaRoot::Public => Some(attenuated.max(Access::Read)),
                MetaRoot::Sensitive => (attenuated >= Access::Read).then_some(attenuated),
            };
            assert_eq!(
                node.core()
                    .doc_level(&RequestContext::root(), &narrowed, doc)
                    .await
                    .unwrap(),
                expected,
            );
            assert_eq!(
                node.core()
                    .reach(&RequestContext::root(), actor.actor.actor_id, doc)
                    .await
                    .unwrap()
                    .map(|(access, _)| access),
                expected,
            );
            let minted = node
                .mint_doc(
                    &actor,
                    DocScope {
                        read: BTreeSet::from([doc.clone()]),
                        ..Default::default()
                    },
                )
                .await;
            let capability = if expected.is_some() {
                minted.unwrap()
            } else {
                assert_eq!(minted.unwrap_err(), DocsError::Denied);
                bounded
            };
            let read = node
                .docs()
                .call(
                    RequestContext::root(),
                    ReadDatums {
                        capability,
                        doc_id: doc.clone(),
                        at: At::Current,
                        datums: None,
                        optimistic: false,
                    },
                )
                .await;
            let polled = crate::test_poll::query(
                node.docs(),
                capability,
                Predicate::IsDoc {
                    doc_id: doc.clone(),
                },
                DatumSelection::All,
            )
            .await;
            if expected.is_some() {
                let read = read.unwrap();
                assert!(!read.datums.is_empty());
                assert!(
                    read.datums
                        .values()
                        .all(|datum| matches!(datum, DatumRead::Accepted { .. }))
                );
                let mut polled = polled.unwrap();
                assert_eq!(polled.len(), 1);
                let datums = polled.remove(doc).unwrap();
                assert_eq!(
                    datums.keys().collect::<Vec<_>>(),
                    read.datums.keys().collect::<Vec<_>>()
                );
                assert!(
                    datums
                        .values()
                        .all(|datum| matches!(datum, ObservedDatum::Present { .. }))
                );
            } else {
                assert_eq!(docs_op(read.unwrap_err()), DocsError::Denied);
                assert_eq!(docs_op(polled.unwrap_err()), DocsError::Denied);
            }
            let writable = node
                .mint_doc(
                    &actor,
                    DocScope {
                        write: std::collections::BTreeMap::from([(
                            doc.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
                        ..Default::default()
                    },
                )
                .await;
            if expected.is_some_and(|access| access >= Access::Write) {
                writable.unwrap();
            } else {
                assert_eq!(writable.unwrap_err(), DocsError::Denied);
            }
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn metadata_only_capability_does_not_grant_drawer_or_content() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let holder = node
        .sub_actor(alice.capability, "alice/metadata-only")
        .await;
    let home = node.collection(&alice, "home").await;
    let content = node.create_doc(&alice, &home, "content").await.unwrap();
    for root in [MetaRoot::Public, MetaRoot::Sensitive] {
        let created = node
            .drawers()
            .call(
                RequestContext::root(),
                CreateMetaDoc {
                    capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                    command: command(&format!("metadata {root:?}")),
                    drawer_id: home.clone(),
                    kind: "notes".into(),
                    name: "main".into(),
                    root,
                },
            )
            .await
            .unwrap();
        node.view(&alice, &home).await.unwrap();
        node.write_note(&alice, &created.doc_id, "shared metadata")
            .await
            .unwrap();
        node.wait_indexed(NOTE, |route, _| route.doc_id == created.doc_id)
            .await;
        let cap = node
            .docs()
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: alice.capability,
                    signer: holder.capability,
                }
                .metadata(),
                MintDocCapability {
                    scope: DocScope {
                        read: BTreeSet::from([created.doc_id.clone()]),
                        ..Default::default()
                    },
                    rights: Rights::USE | Rights::NARROW,
                    subject: Some(holder.actor.actor_id),
                },
            )
            .await
            .unwrap()
            .capability_id;
        let read = node
            .docs()
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability: cap,
                    doc_id: created.doc_id.clone(),
                    at: At::Current,
                    datums: None,
                    optimistic: false,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            read.datums[&note("").0],
            DatumRead::Accepted { .. }
        ));
        let polled = crate::test_poll::query(
            node.docs(),
            cap,
            Predicate::IsDoc {
                doc_id: created.doc_id.clone(),
            },
            DatumSelection::All,
        )
        .await
        .unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(
            polled[&created.doc_id].keys().collect::<Vec<_>>(),
            read.datums.keys().collect::<Vec<_>>(),
        );
        for doc in [&home, &content] {
            assert_eq!(
                docs_op(
                    node.docs()
                        .call(
                            RequestContext::root(),
                            ReadDatums {
                                capability: cap,
                                doc_id: doc.clone(),
                                at: At::Current,
                                datums: None,
                                optimistic: false,
                            },
                        )
                        .await
                        .unwrap_err()
                ),
                DocsError::Denied,
            );
            assert_eq!(
                docs_op(
                    crate::test_poll::query(
                        node.docs(),
                        cap,
                        Predicate::IsDoc {
                            doc_id: doc.clone(),
                        },
                        DatumSelection::All,
                    )
                    .await
                    .unwrap_err()
                ),
                DocsError::Denied,
            );
        }
        for scope in [
            DocScope {
                drawers: BTreeMap::from([(home.clone(), Access::Read)]),
                ..Default::default()
            },
            DocScope {
                read: BTreeSet::from([home.clone()]),
                ..Default::default()
            },
            DocScope {
                read: BTreeSet::from([content.clone()]),
                ..Default::default()
            },
        ] {
            assert_eq!(
                docs_op(
                    node.docs()
                        .call_with(
                            RequestContext::root(),
                            Authorization {
                                authority: cap,
                                signer: holder.capability
                            }
                            .metadata(),
                            MintDocCapability {
                                scope,
                                rights: Rights::USE,
                                subject: Some(holder.actor.actor_id),
                            },
                        )
                        .await
                        .unwrap_err()
                ),
                DocsError::Denied
            );
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn get_drawer_withholds_sensitive_meta_docs_from_relay_scopes() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let parent = node.collection(&alice, "parent").await;
    let child = node.collection(&alice, "child").await;
    let grandchild = node.collection(&alice, "grandchild").await;
    for (parent_id, child_id) in [(&parent, &child), (&child, &grandchild)] {
        node.drawers()
            .call(
                RequestContext::root(),
                AddChild {
                    capability: node
                        .drawer_capability(&alice, &[parent_id, child_id])
                        .await
                        .unwrap(),
                    command: command(&format!("link {parent_id} {child_id}")),
                    parent_id: parent_id.clone(),
                    child_id: child_id.clone(),
                    access: Access::Admin,
                },
            )
            .await
            .unwrap();
    }
    let mut expected = BTreeMap::new();
    for drawer in [&parent, &child, &grandchild] {
        let capability = node.drawer_capability(&alice, &[drawer]).await.unwrap();
        let mut meta_docs = BTreeMap::new();
        for (root, name) in [
            (MetaRoot::Public, "public"),
            (MetaRoot::Sensitive, "private"),
        ] {
            let created = node
                .drawers()
                .call(
                    RequestContext::root(),
                    CreateMetaDoc {
                        capability,
                        command: command(&format!("{drawer} {name}")),
                        drawer_id: drawer.clone(),
                        kind: "notes".to_owned(),
                        name: name.to_owned(),
                        root,
                    },
                )
                .await
                .unwrap();
            assert!(
                meta_docs
                    .insert(
                        root,
                        BTreeMap::from([(format!("notes/{name}"), created.doc_id)])
                    )
                    .is_none()
            );
        }
        assert!(expected.insert(drawer.clone(), meta_docs).is_none());
    }
    for access in [Access::Relay, Access::Read, Access::Write, Access::Admin] {
        let capability = node
            .mint_drawer(
                &alice,
                DrawerScope {
                    drawers: BTreeMap::from([(parent.clone(), access)]),
                    create: false,
                },
            )
            .await
            .unwrap();
        for (drawer, meta_docs) in &expected {
            let view = node
                .drawers()
                .call(
                    RequestContext::root(),
                    GetDrawer {
                        capability,
                        drawer_id: drawer.clone(),
                    },
                )
                .await
                .unwrap();
            assert_eq!(view.access, access);
            assert_eq!(
                node.core().state.meta_docs_of(drawer).await.unwrap(),
                *meta_docs
            );
            let mut visible = meta_docs.clone();
            if access == Access::Relay {
                visible.remove(&MetaRoot::Sensitive);
                let wire = serde_json::to_string(&view).unwrap();
                assert!(!wire.contains("notes/private"));
                for doc in meta_docs[&MetaRoot::Sensitive].values() {
                    assert!(!wire.contains(&doc.to_string()));
                }
            }
            assert_eq!(view.meta_docs, visible);
            let authority = DomainAuthority::Scoped(DrawerScope {
                drawers: BTreeMap::from([(parent.clone(), access)]),
                create: false,
            });
            let sensitive = view.descriptor.sensitive_root().unwrap();
            assert_eq!(
                node.core()
                    .doc_level(&RequestContext::root(), &authority, &sensitive)
                    .await
                    .unwrap(),
                (access >= Access::Read).then_some(access),
            );
            assert_eq!(
                node.core()
                    .doc_level(&RequestContext::root(), &authority, drawer)
                    .await
                    .unwrap(),
                Some(access.max(Access::Read)),
            );
            for (root, slots) in meta_docs {
                for doc in slots.values() {
                    assert_eq!(
                        node.core()
                            .doc_level(&RequestContext::root(), &authority, doc)
                            .await
                            .unwrap(),
                        match root {
                            MetaRoot::Public => Some(access.max(Access::Read)),
                            MetaRoot::Sensitive => (access >= Access::Read).then_some(access),
                        },
                    );
                    assert_eq!(
                        node.core()
                            .level(&RequestContext::root(), &authority, doc)
                            .await
                            .unwrap(),
                        None,
                    );
                }
            }
        }
    }
    node.stop().await;
}
