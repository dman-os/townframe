//! Drawer-scoped facet queries, reference queries, observation, and discovery of synced-in drawers.

use super::*;

/// The document's current heads, read as `actor` without verifying any facet.
async fn heads(node: &Node, actor: &Actor, doc_id: &DocId) -> Vec<ChangeHash> {
    let capability = node
        .mint_doc(
            actor,
            DocScope {
                read: [doc_id.clone()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    node.docs()
        .call(
            RequestContext::root(),
            ReadFacets {
                capability,
                doc_id: doc_id.clone(),
                at: At::Current,
                facets: Some(BTreeSet::new()),
                optimistic: false,
            },
        )
        .await
        .unwrap()
        .heads
}

/// Wait until the `facet_set` index carries the note of `doc_id` at the document's current
/// heads: not merely some row for the document, but the row for the state the test wrote.
async fn wait_for_facet_set(node: &Node, actor: &Actor, doc_id: &DocId) {
    let heads = heads(node, actor, doc_id).await;
    let note_key = note("").0;
    node.wait_indexed(NOTE, |route, membership| {
        route.doc_id == *doc_id
            && route.facet_key == note_key
            && matches!(membership, FacetMembership::Present { doc_heads, .. } if *doc_heads == heads)
    })
    .await;
}

/// Wait until the `facet_ref` index records exactly `targets` out of the note of `doc_id`,
/// each at the `/link` position: a stale edge set, or one still being rewritten, does not
/// match. Every settle the index commits wakes the check.
async fn wait_for_facet_ref(node: &Node, doc_id: &DocId, targets: &[&DocId]) {
    let note_key = note("").0;
    let expected: BTreeSet<(String, FacetRoute)> = targets
        .iter()
        .map(|target| {
            (
                "/link".to_owned(),
                FacetRoute {
                    doc_id: (*target).clone(),
                    facet_key: note_key.clone(),
                },
            )
        })
        .collect();
    let mut settled = node.facet_ref.subscribe_settled();
    loop {
        let edges: BTreeSet<(String, FacetRoute)> = node
            .facet_ref
            .query(FacetRefQuery::OriginDoc(doc_id.clone()))
            .await
            .unwrap()
            .into_iter()
            .map(|edge| (edge.path, edge.target))
            .collect();
        if edges == expected {
            return;
        }
        settled.changed().await.unwrap();
    }
}

/// Alice's containment layout the scoped-query tests read through. `parent` holds `doc_direct`
/// and `doc_relay_entry` (entered at `Relay`), and links `child_read` (holding
/// `doc_in_child_read`) at `Read` and `child_relay` (holding `doc_in_child_relay`) at `Relay`;
/// `other` holds `doc_relay_entry` and `doc_outside`. Bob holds `Read` on `parent` and on
/// `other`, so he reads `doc_outside` and `doc_relay_entry` through `other` but NOT through
/// `parent`; Carol holds only `Relay` on `parent`.
struct Containment {
    node: Node,
    alice: Actor,
    bob: Actor,
    carol: Actor,
    parent: DrawerId,
    other: DrawerId,
    doc_direct: DocId,
    doc_relay_entry: DocId,
    doc_in_child_read: DocId,
    doc_outside: DocId,
    note_key: FacetKey,
    note_tag: FacetTag,
}

impl Containment {
    /// Build the layout and wait until both indexes carry every doc's note at its current heads.
    async fn boot() -> Self {
        let node = Node::boot().await;
        let alice = node.creator("alice").await;
        let bob = node.actor("bob").await;
        let carol = node.actor("carol").await;

        let parent = node.collection(&alice, "parent").await;
        let child_read = node.collection(&alice, "child-read").await;
        let child_relay = node.collection(&alice, "child-relay").await;
        let other = node.collection(&alice, "other").await;

        let doc_direct = node.create_doc(&alice, &parent, "direct").await.unwrap();
        let doc_relay_entry = node
            .create_doc(&alice, &other, "relay-entry")
            .await
            .unwrap();
        let doc_in_child_read = node
            .create_doc(&alice, &child_read, "in-child-read")
            .await
            .unwrap();
        let doc_in_child_relay = node
            .create_doc(&alice, &child_relay, "in-child-relay")
            .await
            .unwrap();
        let doc_outside = node.create_doc(&alice, &other, "outside").await.unwrap();

        // Link `doc_direct -> doc_in_child_read` and `doc_outside -> doc_direct` so `facet_ref`
        // records both an in-drawer edge and an incoming edge from outside `parent`.
        let link = async |origin: &DocId, text: &str, target: &DocId| {
            let (note_key, linking) = note_linking(text, target);
            node.docs()
                .call(
                    RequestContext::root(),
                    WriteFacets {
                        capability: node
                            .mint_doc(
                                &alice,
                                DocScope {
                                    write: [origin.clone()].into(),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap(),
                        command: None,
                        doc_id: origin.clone(),
                        mode: WriteMode::AtCurrent,
                        set: BTreeMap::from([(note_key, linking)]),
                        remove: BTreeSet::new(),
                    },
                )
                .await
                .unwrap();
        };
        link(&doc_direct, "points to child", &doc_in_child_read).await;
        link(&doc_outside, "outside points to direct", &doc_direct).await;

        for doc in [
            &doc_direct,
            &doc_relay_entry,
            &doc_in_child_read,
            &doc_in_child_relay,
            &doc_outside,
        ] {
            wait_for_facet_set(&node, &alice, doc).await;
        }
        wait_for_facet_ref(&node, &doc_direct, &[&doc_in_child_read]).await;
        wait_for_facet_ref(&node, &doc_outside, &[&doc_direct]).await;

        node.add_doc(
            &alice,
            "add-relay-doc",
            &parent,
            &doc_relay_entry,
            Access::Relay,
        )
        .await
        .unwrap();
        for (child, access, name) in [
            (&child_read, Access::Read, "link-child-read"),
            (&child_relay, Access::Relay, "link-child-relay"),
        ] {
            node.drawers()
                .call(
                    RequestContext::root(),
                    AddChild {
                        capability: node
                            .drawer_capability(&alice, &[&parent, child])
                            .await
                            .unwrap(),
                        command: command(name),
                        parent_id: parent.clone(),
                        child_id: child.clone(),
                        access,
                    },
                )
                .await
                .unwrap();
        }

        node.grant(&alice, &parent, &bob, Access::Read).await;
        node.grant(&alice, &other, &bob, Access::Read).await;
        node.grant(&alice, &parent, &carol, Access::Relay).await;

        Self {
            node,
            alice,
            bob,
            carol,
            parent,
            other,
            doc_direct,
            doc_relay_entry,
            doc_in_child_read,
            doc_outside,
            note_key: note("").0,
            note_tag: FacetTag::new(NOTE).unwrap(),
        }
    }

    async fn capability(&self, actor: &Actor, scope: DocScope) -> CapId {
        self.node.mint_doc(actor, scope).await.unwrap()
    }

    /// A doc-scope capability over `parent` alone at `access`.
    async fn parent_capability(&self, actor: &Actor, access: Access) -> CapId {
        self.capability(
            actor,
            DocScope {
                drawers: [(self.parent.clone(), access)].into(),
                ..Default::default()
            },
        )
        .await
    }

    /// Bob's doc-scope capability over both drawers he was granted.
    async fn bob_both_drawers(&self) -> CapId {
        self.capability(
            &self.bob,
            DocScope {
                drawers: [
                    (self.parent.clone(), Access::Read),
                    (self.other.clone(), Access::Read),
                ]
                .into(),
                ..Default::default()
            },
        )
        .await
    }

    /// The `/link` edge from the note of `origin` to the note of `target`.
    fn note_edge(&self, origin: &DocId, target: &DocId) -> QueriedFacetRef {
        QueriedFacetRef {
            origin_doc_id: origin.clone(),
            origin_facet_key: self.note_key.clone(),
            path: "/link".to_owned(),
            target_doc_id: target.clone(),
            target_facet_key: self.note_key.clone(),
            ref_heads: Vec::new(),
        }
    }

    fn backlinks_to(&self, doc_id: &DocId) -> FacetRefSelector {
        FacetRefSelector::Target {
            doc_id: doc_id.clone(),
            facet_key: None,
        }
    }
}

#[tokio::test]
async fn relay_holder_lists_drawer_docs_but_is_denied_drawer_scoped_facet_queries() {
    let fx = Containment::boot().await;

    // Carol holds only `Relay` on `parent`: she can `ListDocs`, but `QueryFacets` and
    // `QueryFacetRefs` on `parent` are refused (`Denied`).
    assert_eq!(
        fx.node
            .list_docs(&fx.carol, &fx.parent, 0, None)
            .await
            .unwrap()
            .len(),
        4
    );
    let carol_query = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: fx.parent_capability(&fx.carol, Access::Relay).await,
                drawer_id: Some(fx.parent.clone()),
                selector: FacetSelector::Tag {
                    tag: fx.note_tag.clone(),
                },
            },
        )
        .await;
    assert_eq!(
        carol_query.unwrap_err(),
        ClientError::Operation(DocsError::Denied)
    );

    fx.node.stop().await;
}

#[tokio::test]
async fn drawer_scoped_facet_queries_obey_containment_and_attenuation() {
    let fx = Containment::boot().await;

    // Bob's drawer-scoped `QueryFacets` on `parent` returns ONLY `doc_direct` and
    // `doc_in_child_read`: `doc_relay_entry` and `doc_in_child_relay` are attenuated to `Relay` in
    // `parent`, and `doc_outside` is not in `parent` even though Bob can read it via `other`.
    let bob_in_parent: BTreeSet<DocId> = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: fx.parent_capability(&fx.bob, Access::Read).await,
                drawer_id: Some(fx.parent.clone()),
                selector: FacetSelector::Tag {
                    tag: fx.note_tag.clone(),
                },
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.doc_id)
        .collect();
    assert_eq!(
        bob_in_parent,
        BTreeSet::from([fx.doc_direct.clone(), fx.doc_in_child_read.clone()])
    );

    // Unscoped `QueryFacets` by tag for Bob includes `doc_outside` and `doc_relay_entry` too,
    // because Bob reaches them via `other`, while still hiding `doc_in_child_relay`.
    let bob_unscoped: BTreeSet<DocId> = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: fx.bob_both_drawers().await,
                drawer_id: None,
                selector: FacetSelector::Tag {
                    tag: fx.note_tag.clone(),
                },
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.doc_id)
        .collect();
    assert_eq!(
        bob_unscoped,
        BTreeSet::from([
            fx.doc_direct.clone(),
            fx.doc_in_child_read.clone(),
            fx.doc_relay_entry.clone(),
            fx.doc_outside.clone(),
        ])
    );

    // Observing the tag under the same reach replays exactly what the unscoped query returned.
    let observation_capability = fx
        .capability(
            &fx.bob,
            DocScope {
                read: [fx.doc_outside.clone()].into(),
                drawers: [
                    (fx.parent.clone(), Access::Read),
                    (fx.other.clone(), Access::Read),
                ]
                .into(),
                ..Default::default()
            },
        )
        .await;
    let mut events = fx
        .node
        .docs()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability: observation_capability,
                scope: ObserveScope::Tag {
                    tag: fx.note_tag.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    let mut observed = BTreeSet::new();
    while let FacetObserved::Entry { doc_id, facet, .. } = events.next().await.unwrap().unwrap() {
        assert!(matches!(facet, ObservedFacet::Present { .. }));
        observed.insert(doc_id);
    }
    assert_eq!(observed, bob_unscoped);
    drop(events);

    fx.node.stop().await;
}

#[tokio::test]
async fn unscoped_queries_are_narrowed_to_the_capability_scope() {
    let fx = Containment::boot().await;

    // A capability over `parent` alone narrows an unscoped key query to the docs `parent`
    // reaches at `Read`, and hides the backlink from `doc_outside`.
    let parent_only = fx.parent_capability(&fx.bob, Access::Read).await;
    let narrowed_records = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: parent_only,
                drawer_id: None,
                selector: FacetSelector::Key {
                    facet_key: fx.note_key.clone(),
                },
            },
        )
        .await
        .unwrap();
    let mut expected_docs = vec![fx.doc_direct.clone(), fx.doc_in_child_read.clone()];
    expected_docs.sort();
    assert_eq!(
        narrowed_records
            .into_iter()
            .map(|record| record.doc_id)
            .collect::<Vec<_>>(),
        expected_docs,
    );
    assert!(
        fx.node
            .docs()
            .call(
                RequestContext::root(),
                QueryFacetRefs {
                    capability: parent_only,
                    drawer_id: None,
                    selector: fx.backlinks_to(&fx.doc_direct),
                },
            )
            .await
            .unwrap()
            .is_empty()
    );

    // A capability reading only `doc_outside` sees that doc's outgoing edge, but may not ask
    // for the backlinks of `doc_direct`, a doc it cannot read.
    let origin_only = fx
        .capability(
            &fx.bob,
            DocScope {
                read: [fx.doc_outside.clone()].into(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        fx.node
            .docs()
            .call(
                RequestContext::root(),
                QueryFacetRefs {
                    capability: origin_only,
                    drawer_id: None,
                    selector: FacetRefSelector::Origin {
                        doc_id: fx.doc_outside.clone(),
                        facet_key: None,
                    },
                },
            )
            .await
            .unwrap(),
        vec![fx.note_edge(&fx.doc_outside, &fx.doc_direct)],
    );
    assert_eq!(
        fx.node
            .docs()
            .call(
                RequestContext::root(),
                QueryFacetRefs {
                    capability: origin_only,
                    drawer_id: None,
                    selector: fx.backlinks_to(&fx.doc_direct),
                },
            )
            .await
            .unwrap_err(),
        ClientError::Operation(DocsError::Denied),
    );

    fx.node.stop().await;
}

#[tokio::test]
async fn reference_queries_do_not_leak_backlinks_from_unreachable_origins() {
    let fx = Containment::boot().await;

    // Drawer-scoped `QueryFacetRefs` on `parent` for incoming edges to `doc_direct` omits the edge
    // from `doc_outside` (because `doc_outside` is not in `parent`), while unscoped
    // `QueryFacetRefs` for Bob includes it (because Bob can read `doc_outside`), and unscoped for
    // an actor who cannot read `other` omits it (no backlink leak from unreadable origin docs).
    let dave = fx.node.actor("dave").await;
    fx.node
        .grant(&fx.alice, &fx.parent, &dave, Access::Read)
        .await;

    let scoped_backlinks = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: fx.parent_capability(&fx.bob, Access::Read).await,
                drawer_id: Some(fx.parent.clone()),
                selector: fx.backlinks_to(&fx.doc_direct),
            },
        )
        .await
        .unwrap();
    assert!(scoped_backlinks.is_empty());

    let dave_unscoped_backlinks = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: fx.parent_capability(&dave, Access::Read).await,
                drawer_id: None,
                selector: fx.backlinks_to(&fx.doc_direct),
            },
        )
        .await
        .unwrap();
    assert!(dave_unscoped_backlinks.is_empty());

    let bob_unscoped_backlinks = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: fx.bob_both_drawers().await,
                drawer_id: None,
                selector: fx.backlinks_to(&fx.doc_direct),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        bob_unscoped_backlinks,
        vec![fx.note_edge(&fx.doc_outside, &fx.doc_direct)]
    );

    // Outgoing edges stay visible inside the drawer: `doc_direct -> doc_in_child_read` is
    // `parent`-internal.
    let scoped_outgoing = fx
        .node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: fx.parent_capability(&dave, Access::Read).await,
                drawer_id: Some(fx.parent.clone()),
                selector: FacetRefSelector::Origin {
                    doc_id: fx.doc_direct.clone(),
                    facet_key: Some(fx.note_key.clone()),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        scoped_outgoing,
        vec![fx.note_edge(&fx.doc_direct, &fx.doc_in_child_read)]
    );

    fx.node.stop().await;
}

#[tokio::test]
async fn drawer_scoped_observation_replays_and_emits_live_membership_and_grant_changes() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;

    let drawer = node.collection(&alice, "observed-drawer").await;
    let child = node.collection(&alice, "observed-child").await;
    let other = node.collection(&alice, "other-drawer").await;

    let doc_initial = node.create_doc(&alice, &drawer, "initial").await.unwrap();
    let doc_added_later = node
        .create_doc(&alice, &other, "added-later")
        .await
        .unwrap();
    let doc_in_child = node.create_doc(&alice, &child, "in-child").await.unwrap();
    for doc in [&doc_initial, &doc_added_later, &doc_in_child] {
        wait_for_facet_set(&node, &alice, doc).await;
    }

    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let doc_scope = DocScope {
        drawers: [(drawer.clone(), Access::Read)].into(),
        ..Default::default()
    };
    let observation = ObserveFacets {
        capability: node.mint_doc(&bob, doc_scope.clone()).await.unwrap(),
        scope: ObserveScope::Drawer {
            drawer_id: drawer.clone(),
            tag: Some(big_cabinet_core::FacetTag::new(NOTE).unwrap()),
        },
        after: 0,
    };
    let mut events = node
        .docs()
        .subscribe(RequestContext::root(), observation.clone())
        .await
        .unwrap();

    // 1. Opening replay delivers `doc_initial` and then `ReplayComplete`.
    let first = events.next().await.unwrap().unwrap();
    let FacetObserved::Entry {
        doc_id,
        facet: ObservedFacet::Present { .. },
        ..
    } = first
    else {
        panic!("expected initial Present entry, got {first:?}");
    };
    assert_eq!(doc_id, doc_initial);
    let replay_done = events.next().await.unwrap().unwrap();
    assert!(
        matches!(replay_done, FacetObserved::ReplayComplete { .. }),
        "expected ReplayComplete, got {replay_done:?}"
    );

    // 2. Adding an already-settled doc (`doc_added_later`) to `drawer` at `Read` emits `Present`
    //    even though `doc_added_later` itself was not edited.
    node.add_doc(&alice, "add-later", &drawer, &doc_added_later, Access::Read)
        .await
        .unwrap();
    let added = events.next().await.unwrap().unwrap();
    let FacetObserved::Entry {
        doc_id,
        facet: ObservedFacet::Present { .. },
        ..
    } = added
    else {
        panic!("expected Present for newly added doc, got {added:?}");
    };
    assert_eq!(doc_id, doc_added_later);

    // 3. Narrowing `doc_added_later` to `Relay` in `drawer` emits `AccessLost`.
    node.add_doc(
        &alice,
        "narrow-later-to-relay",
        &drawer,
        &doc_added_later,
        Access::Relay,
    )
    .await
    .unwrap();
    let narrowed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            narrowed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_added_later
        ),
        "expected AccessLost on narrowing to Relay, got {narrowed:?}"
    );

    // 4. Widening `doc_added_later` back to `Read` resumes observation (`Present`).
    node.add_doc(
        &alice,
        "widen-later-to-read",
        &drawer,
        &doc_added_later,
        Access::Read,
    )
    .await
    .unwrap();
    let widened = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            widened,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_added_later
        ),
        "expected Present on widening back to Read, got {widened:?}"
    );

    // 5. Removing `doc_added_later` from `drawer` emits `AccessLost`.
    node.remove_doc(&alice, "remove-later", &drawer, &doc_added_later)
        .await
        .unwrap();
    let removed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            removed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_added_later
        ),
        "expected AccessLost on RemoveDoc, got {removed:?}"
    );

    // 6. Linking `child` at `Read` emits `Present` for `doc_in_child`; unlinking `child` emits
    //    `AccessLost` for `doc_in_child`.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&drawer, &child])
                    .await
                    .unwrap(),
                command: command("link-child"),
                parent_id: drawer.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    let child_added = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            child_added,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_in_child
        ),
        "expected Present for child doc on AddChild, got {child_added:?}"
    );
    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                command: command("unlink-child"),
                parent_id: drawer.clone(),
                child_id: child.clone(),
            },
        )
        .await
        .unwrap();
    let child_removed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            child_removed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_in_child
        ),
        "expected AccessLost for child doc on RemoveChild, got {child_removed:?}"
    );

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
        events.next().await.unwrap(),
        Err(ClientError::Operation(DocsError::Denied))
    );
    assert!(events.next().await.is_none());
    drop(events);
    assert_eq!(
        node.mint_doc(&bob, doc_scope.clone()).await.unwrap_err(),
        DocsError::Denied
    );
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let refused = node
        .docs()
        .subscribe(RequestContext::root(), observation.clone())
        .await;
    assert!(
        matches!(refused, Err(ClientError::Operation(DocsError::Denied))),
        "regrant must not revive the withdrawn observation capability"
    );
    let mut events = node
        .docs()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability: node.mint_doc(&bob, doc_scope).await.unwrap(),
                ..observation
            },
        )
        .await
        .unwrap();
    let regranted = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            regranted,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_initial
        ),
        "expected Present on re-granting drawer Read, got {regranted:?}"
    );

    drop(events);
    node.stop().await;
}

#[tokio::test]
async fn synced_in_drawer_docs_are_discovered_and_projected_without_polluting_known_registry() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;

    // Create a parent drawer, a child drawer, and a doc inside the child drawer, then wipe the
    // local SQLite drawer projection rows for `parent` and `child` to simulate those drawer docs
    // and their member doc having arrived via BigRepo sync without local `CreateDrawer` calls.
    let parent = node.collection(&alice, "synced-parent").await;
    let child = node.collection(&alice, "synced-child").await;
    let doc = node
        .create_doc(&alice, &child, "synced-payload")
        .await
        .unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child])
                    .await
                    .unwrap(),
                command: command("link-synced-child"),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();

    let transaction = node.sql.begin_write().await.unwrap();
    for id in [&parent, &child] {
        sqlite::clear_docs(&transaction, id.to_string())
            .await
            .unwrap();
        transaction
            .execute(
                "DELETE FROM big_cabinet_drawer_children WHERE parent_id = :id OR child_id = :id",
                (id.to_string(),),
            )
            .await
            .unwrap();
        transaction
            .execute(
                "DELETE FROM big_cabinet_drawers WHERE drawer_id = :id",
                (id.to_string(),),
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    // Grant Bob Read on `parent`. Neither `parent` nor `child` is in `big_cabinet_drawers`.
    node.grant(&alice, &parent, &bob, Access::Read).await;

    // 1. Reading `doc` as Bob triggers discovery of `parent` and its child `child` from the
    //    synced-in drawer docs and authorizes the read through `parent -> child -> doc`.
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap(),
        json!({"$type": "org.example.note", "value": "synced-payload"})
    );

    // 2. `ListDrawers` and `ListDocs` see the newly projected drawers and their transitive docs.
    let listed = node
        .drawers()
        .call(
            RequestContext::root(),
            ListDrawers {
                capability: node.drawer_capability(&bob, &[&parent]).await.unwrap(),
            },
        )
        .await
        .unwrap();
    let listed_ids: BTreeSet<DocId> = listed.iter().map(|d| d.drawer_id.clone()).collect();
    assert!(listed_ids.contains(&parent));
    assert!(listed_ids.contains(&child));

    let docs = node
        .drawers()
        .call(
            RequestContext::root(),
            ListDocs {
                capability: node.drawer_capability(&bob, &[&parent]).await.unwrap(),
                drawer_id: parent.clone(),
                offset: 0,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, doc);
    assert_eq!(docs[0].access, Access::Read);

    // 3. Discovering and projecting synced-in drawers never auto-populates `registry_id`; only an
    //    explicit `RememberDrawer` adds a drawer to `ListKnownDrawers`.
    let known_before = node
        .drawers()
        .call(
            RequestContext::root(),
            ListKnownDrawers {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
            },
        )
        .await
        .unwrap();
    assert!(known_before.is_empty());

    node.drawers()
        .call(
            RequestContext::root(),
            RememberDrawer {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
                drawer_id: parent.clone(),
                mirror: true,
            },
        )
        .await
        .unwrap();
    let known_after = node
        .drawers()
        .call(
            RequestContext::root(),
            ListKnownDrawers {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(known_after.len(), 1);
    assert_eq!(known_after[0].drawer_id, parent);

    node.stop().await;
}
