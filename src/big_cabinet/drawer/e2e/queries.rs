//! Drawer-scoped datum queries, reference queries, observation, and discovery of synced-in drawers.

use super::*;

/// The document's current heads, read as `actor` without verifying any datum.
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
            ReadDatums {
                capability,
                doc_id: doc_id.clone(),
                at: At::Current,
                datums: Some(BTreeSet::new()),
                optimistic: false,
            },
        )
        .await
        .unwrap()
        .heads
}

/// Wait until the `datum_set` index carries the note of `doc_id` at the document's current
/// heads: not merely some row for the document, but the row for the state the test wrote.
async fn wait_for_datum_set(node: &Node, actor: &Actor, doc_id: &DocId) {
    let heads = heads(node, actor, doc_id).await;
    let note_key = note("").0;
    node.wait_indexed(NOTE, |route, membership| {
        route.doc_id == *doc_id
            && route.datum_key == note_key
            && matches!(membership, DatumMembership::Present { doc_heads, .. } if *doc_heads == heads)
    })
    .await;
}

/// Wait until the reference index records exactly `targets` out of the note of `doc_id`, each at
/// the `/link` position: a stale edge set, or one still being rewritten, does not match. The edges
/// settle in the same datum-set transaction as the entries, so its settle counter wakes the check.
async fn wait_for_datum_ref(node: &Node, doc_id: &DocId, targets: &[&DocId]) {
    let note_key = note("").0;
    let expected: BTreeSet<(String, DatumRoute)> = targets
        .iter()
        .map(|target| {
            (
                "/link".to_owned(),
                DatumRoute {
                    doc_id: (*target).clone(),
                    datum_key: note_key.clone(),
                },
            )
        })
        .collect();
    let mut settled = node.datum_set.subscribe_settled();
    loop {
        let edges: BTreeSet<(String, DatumRoute)> = node
            .datum_ref
            .query(DatumRefQuery::OriginDoc(doc_id.clone()))
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

/// Consume a poll until `expected` enters the predicate, asserting the enter carries a present
/// datum. Events about other documents (the opening replay, or a concurrent change) are skipped.
async fn next_enter(poll: &mut crate::test_poll::Poller, expected: &DocId) {
    loop {
        match poll
            .next()
            .await
            .unwrap()
            .expect("the poll ended before the expected enter")
        {
            DatumEvent::Enter { doc_id, datums, .. } if doc_id == *expected => {
                assert!(
                    datums
                        .values()
                        .any(|datum| matches!(datum, ObservedDatum::Present { .. })),
                    "the enter for {expected:?} carried no present datum"
                );
                return;
            }
            _ => {}
        }
    }
}

/// Consume a poll until `expected` leaves the predicate.
async fn next_leave(poll: &mut crate::test_poll::Poller, expected: &DocId) {
    loop {
        match poll
            .next()
            .await
            .unwrap()
            .expect("the poll ended before the expected leave")
        {
            DatumEvent::Leave { doc_id, .. } if doc_id == *expected => return,
            _ => {}
        }
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
    note_key: DatumKey,
    note_tag: DatumTag,
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

        // Link `doc_direct -> doc_in_child_read` and `doc_outside -> doc_direct` so `datum_ref`
        // records both an in-drawer edge and an incoming edge from outside `parent`.
        let link = async |origin: &DocId, text: &str, target: &DocId| {
            let (note_key, linking) = note_linking(text, target);
            node.docs()
                .call(
                    RequestContext::root(),
                    WriteDatums {
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
            wait_for_datum_set(&node, &alice, doc).await;
        }
        wait_for_datum_ref(&node, &doc_direct, &[&doc_in_child_read]).await;
        wait_for_datum_ref(&node, &doc_outside, &[&doc_direct]).await;

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
            note_tag: DatumTag::new(NOTE).unwrap(),
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
    fn note_edge(&self, origin: &DocId, target: &DocId) -> QueriedDatumRef {
        QueriedDatumRef {
            origin_doc_id: origin.clone(),
            origin_datum_key: self.note_key.clone(),
            path: "/link".to_owned(),
            target_doc_id: target.clone(),
            target_datum_key: self.note_key.clone(),
            ref_heads: Vec::new(),
        }
    }

    fn backlinks_to(&self, doc_id: &DocId) -> DatumRefSelector {
        DatumRefSelector::Target {
            doc_id: doc_id.clone(),
            datum_key: None,
        }
    }
}

#[tokio::test]
async fn relay_holder_lists_drawer_docs_but_is_denied_drawer_scoped_datum_queries() {
    let fx = Containment::boot().await;

    // Carol holds only `Relay` on `parent`: she can `ListDocs`, but `PollDatums` and
    // `QueryDatumRefs` on `parent` are refused (`Denied`). The `InDrawer` anchor itself is
    // below the `Read` a datum delivery needs, so the predicate cannot even be admitted.
    assert_eq!(
        fx.node
            .list_docs(&fx.carol, &fx.parent, 0, None)
            .await
            .unwrap()
            .len(),
        4
    );
    let carol_query = crate::test_poll::query(
        fx.node.docs(),
        fx.parent_capability(&fx.carol, Access::Relay).await,
        Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: fx.parent.clone(),
                    transitive: true,
                    at_least: Access::Read,
                },
                Predicate::HasTag {
                    tag: fx.note_tag.clone(),
                },
            ],
        },
        DatumSelection::Of {
            tags: [fx.note_tag.clone()].into(),
            keys: BTreeSet::new(),
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
async fn drawer_scoped_datum_queries_obey_containment_and_attenuation() {
    let fx = Containment::boot().await;

    // Bob's drawer-scoped query on `parent` returns ONLY `doc_direct` and `doc_in_child_read`:
    // `doc_relay_entry` and `doc_in_child_relay` are attenuated to `Relay` in `parent`, and
    // `doc_outside` is not in `parent` even though Bob can read it via `other`.
    let note_selection = DatumSelection::Of {
        tags: [fx.note_tag.clone()].into(),
        keys: BTreeSet::new(),
    };
    let bob_in_parent: BTreeSet<DocId> = crate::test_poll::query(
        fx.node.docs(),
        fx.parent_capability(&fx.bob, Access::Read).await,
        Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: fx.parent.clone(),
                    transitive: true,
                    at_least: Access::Read,
                },
                Predicate::HasTag {
                    tag: fx.note_tag.clone(),
                },
            ],
        },
        note_selection.clone(),
    )
    .await
    .unwrap()
    .into_keys()
    .collect();
    assert_eq!(
        bob_in_parent,
        BTreeSet::from([fx.doc_direct.clone(), fx.doc_in_child_read.clone()])
    );

    // An unanchored tag predicate has no resource to authorize it, so a scoped caller may not make
    // one: it would enumerate every document carrying the tag on the node.
    let bob_unscoped = crate::test_poll::query(
        fx.node.docs(),
        fx.bob_both_drawers().await,
        Predicate::HasTag {
            tag: fx.note_tag.clone(),
        },
        note_selection.clone(),
    )
    .await;
    assert_eq!(
        bob_unscoped.unwrap_err(),
        ClientError::Operation(DocsError::Denied)
    );

    // Through `other`, Bob reaches `doc_outside` and `doc_relay_entry`: the attenuation of
    // `doc_relay_entry` in `parent` does not weaken its separate route through `other`.
    let bob_in_other: BTreeSet<DocId> = crate::test_poll::query(
        fx.node.docs(),
        fx.bob_both_drawers().await,
        Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: fx.other.clone(),
                    transitive: true,
                    at_least: Access::Read,
                },
                Predicate::HasTag {
                    tag: fx.note_tag.clone(),
                },
            ],
        },
        note_selection.clone(),
    )
    .await
    .unwrap()
    .into_keys()
    .collect();
    assert_eq!(
        bob_in_other,
        BTreeSet::from([fx.doc_outside.clone(), fx.doc_relay_entry.clone()])
    );

    // Opening replay is a poll from zero, so it delivers the same documents the current-state
    // query folded out for the same anchored predicate.
    let mut poll = crate::test_poll::Poller::new(
        fx.node.docs(),
        PollDatums {
            capability: fx.parent_capability(&fx.bob, Access::Read).await,
            predicate: Predicate::And {
                clauses: vec![
                    Predicate::InDrawer {
                        drawer_id: fx.parent.clone(),
                        transitive: true,
                        at_least: Access::Read,
                    },
                    Predicate::HasTag {
                        tag: fx.note_tag.clone(),
                    },
                ],
            },
            datums: note_selection,
            after: 0,
            limit: 0,
            hold_ms: 0,
        },
    );
    let mut observed = BTreeSet::new();
    loop {
        let page = poll.page(0).await.unwrap();
        for event in page.events {
            if let DatumEvent::Enter { doc_id, datums, .. } = event {
                assert!(
                    datums
                        .values()
                        .all(|datum| matches!(datum, ObservedDatum::Present { .. }))
                );
                observed.insert(doc_id);
            }
        }
        if page.caught_up {
            break;
        }
    }
    assert_eq!(observed, bob_in_parent);

    fx.node.stop().await;
}

#[tokio::test]
async fn anchored_queries_are_narrowed_to_the_capability_scope() {
    let fx = Containment::boot().await;

    // A capability over `parent` alone narrows a key query to the docs `parent` reaches at
    // `Read`, and hides the backlink from `doc_outside`.
    let parent_only = fx.parent_capability(&fx.bob, Access::Read).await;
    let narrowed_records = crate::test_poll::query(
        fx.node.docs(),
        parent_only,
        Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: fx.parent.clone(),
                    transitive: true,
                    at_least: Access::Read,
                },
                Predicate::HasKey {
                    key: fx.note_key.clone(),
                },
            ],
        },
        DatumSelection::Of {
            tags: BTreeSet::new(),
            keys: [fx.note_key.clone()].into(),
        },
    )
    .await
    .unwrap();
    // The query returns the matching set; result order is not part of the contract.
    let expected_docs = BTreeSet::from([fx.doc_direct.clone(), fx.doc_in_child_read.clone()]);
    assert_eq!(
        narrowed_records.into_keys().collect::<BTreeSet<_>>(),
        expected_docs
    );
    assert!(
        fx.node
            .docs()
            .call(
                RequestContext::root(),
                QueryDatumRefs {
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
                QueryDatumRefs {
                    capability: origin_only,
                    drawer_id: None,
                    selector: DatumRefSelector::Origin {
                        doc_id: fx.doc_outside.clone(),
                        datum_key: None,
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
                QueryDatumRefs {
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

    // Drawer-scoped `QueryDatumRefs` on `parent` for incoming edges to `doc_direct` omits the edge
    // from `doc_outside` (because `doc_outside` is not in `parent`), while unscoped
    // `QueryDatumRefs` for Bob includes it (because Bob can read `doc_outside`), and unscoped for
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
            QueryDatumRefs {
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
            QueryDatumRefs {
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
            QueryDatumRefs {
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
            QueryDatumRefs {
                capability: fx.parent_capability(&dave, Access::Read).await,
                drawer_id: Some(fx.parent.clone()),
                selector: DatumRefSelector::Origin {
                    doc_id: fx.doc_direct.clone(),
                    datum_key: Some(fx.note_key.clone()),
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
async fn non_transitive_poll_includes_self_doc_children_but_not_child_drawer_docs() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let parent = node.collection(&alice, "poll-parent").await;
    let child = node.collection(&alice, "poll-child").await;
    let home = node.collection(&alice, "poll-home").await;
    let direct = node.create_doc(&alice, &parent, "direct").await.unwrap();
    let in_child = node.create_doc(&alice, &child, "in-child").await.unwrap();
    let self_doc = node.create_doc(&alice, &home, "self-doc").await.unwrap();
    for doc in [&direct, &in_child, &self_doc] {
        wait_for_datum_set(&node, &alice, doc).await;
    }
    node.drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare self-doc"),
                layout: CreateLayout::SelfDoc {
                    doc_id: self_doc.clone(),
                },
            },
        )
        .await
        .unwrap();
    node.grant(&alice, &parent, &bob, Access::Read).await;
    for (doc_id, key) in [
        (&parent, doc_entry_key(&direct)),
        (&child, doc_entry_key(&in_child)),
        (&home, doc_entry_key(&self_doc)),
        (&self_doc, drawer_key()),
    ] {
        let expected_heads = heads(&node, &alice, doc_id).await;
        node.wait_indexed(key.tag().as_str(), |route, membership| {
            route.doc_id == *doc_id
                && route.datum_key == key
                && matches!(membership, DatumMembership::Present { doc_heads, .. } if *doc_heads == expected_heads)
        })
        .await;
    }

    // The ADR 026 §9 worker shape, with the harness note tag standing for its datum tags.
    let note_tag = DatumTag::new(NOTE).unwrap();
    let mut poll = crate::test_poll::Poller::new(
        node.docs(),
        PollDatums {
            capability: node
                .mint_doc(
                    &bob,
                    DocScope {
                        drawers: [(parent.clone(), Access::Read)].into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap(),
            predicate: Predicate::And {
                clauses: vec![
                    Predicate::InDrawer {
                        drawer_id: parent.clone(),
                        transitive: false,
                        at_least: Access::Read,
                    },
                    Predicate::HasTag {
                        tag: note_tag.clone(),
                    },
                ],
            },
            datums: DatumSelection::Of {
                tags: [note_tag].into(),
                keys: BTreeSet::new(),
            },
            after: 0,
            limit: 0,
            hold_ms: 0,
        },
    );
    let mut entered = BTreeSet::new();
    loop {
        let page = poll.page(0).await.unwrap();
        for event in page.events {
            if let DatumEvent::Enter { doc_id, .. } = event {
                entered.insert(doc_id);
            }
        }
        if page.caught_up {
            break;
        }
    }
    assert_eq!(entered, BTreeSet::from([direct.clone()]));

    // Linking a single-group child does not move its docs into a non-transitive poll:
    // neither an enter on link nor a spurious leave on unlink.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child])
                    .await
                    .unwrap(),
                command: command("link child"),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        poll.next().await.unwrap(),
        None,
        "grouped child link: parent {parent}, child {child}, direct {direct}, in-child {in_child}, self-doc {self_doc}",
    );
    // Linking a self-doc child moves exactly its doc in, and unlinking moves it out.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &self_doc])
                    .await
                    .unwrap(),
                command: command("link self-doc"),
                parent_id: parent.clone(),
                child_id: self_doc.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    next_enter(&mut poll, &self_doc).await;
    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("unlink child"),
                parent_id: parent.clone(),
                child_id: child.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        poll.next().await.unwrap(),
        None,
        "grouped child unlink: parent {parent}, child {child}, direct {direct}, in-child {in_child}, self-doc {self_doc}",
    );
    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("unlink self-doc"),
                parent_id: parent.clone(),
                child_id: self_doc.clone(),
            },
        )
        .await
        .unwrap();
    next_leave(&mut poll, &self_doc).await;
    node.stop().await;
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
        wait_for_datum_set(&node, &alice, doc).await;
    }

    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let doc_scope = DocScope {
        drawers: [(drawer.clone(), Access::Read)].into(),
        ..Default::default()
    };
    let note_tag = big_cabinet_core::DatumTag::new(NOTE).unwrap();
    let observation = PollDatums {
        capability: node.mint_doc(&bob, doc_scope.clone()).await.unwrap(),
        predicate: Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: drawer.clone(),
                    transitive: true,
                    at_least: Access::Read,
                },
                Predicate::HasTag {
                    tag: note_tag.clone(),
                },
            ],
        },
        datums: DatumSelection::Of {
            tags: [note_tag].into(),
            keys: BTreeSet::new(),
        },
        after: 0,
        limit: 0,
        hold_ms: 0,
    };
    let mut poll = crate::test_poll::Poller::new(node.docs(), observation.clone());

    // 1. Opening replay delivers `doc_initial` and reaches `caughtUp`.
    let first_page = poll.page(0).await.unwrap();
    assert!(first_page.caught_up);
    let mut entered = BTreeSet::new();
    for event in first_page.events {
        if let DatumEvent::Enter { doc_id, datums, .. } = event {
            assert!(
                datums
                    .values()
                    .any(|datum| matches!(datum, ObservedDatum::Present { .. })),
                "the enter for {doc_id:?} carried no present datum"
            );
            entered.insert(doc_id);
        }
    }
    assert_eq!(entered, BTreeSet::from([doc_initial.clone()]));

    // 2. Adding an already-settled doc (`doc_added_later`) to `drawer` at `Read` enters it even
    //    though `doc_added_later` itself was not edited.
    node.add_doc(&alice, "add-later", &drawer, &doc_added_later, Access::Read)
        .await
        .unwrap();
    next_enter(&mut poll, &doc_added_later).await;

    // 3. Narrowing `doc_added_later` to `Relay` in `drawer` makes the `Read` predicate stop
    //    matching it, so the poll reports a `Leave`.
    node.add_doc(
        &alice,
        "narrow-later-to-relay",
        &drawer,
        &doc_added_later,
        Access::Relay,
    )
    .await
    .unwrap();
    next_leave(&mut poll, &doc_added_later).await;

    // 4. Widening `doc_added_later` back to `Read` re-enters it.
    node.add_doc(
        &alice,
        "widen-later-to-read",
        &drawer,
        &doc_added_later,
        Access::Read,
    )
    .await
    .unwrap();
    next_enter(&mut poll, &doc_added_later).await;

    // 5. Removing `doc_added_later` from `drawer` leaves it.
    node.remove_doc(&alice, "remove-later", &drawer, &doc_added_later)
        .await
        .unwrap();
    next_leave(&mut poll, &doc_added_later).await;

    // 6. Linking `child` at `Read` enters `doc_in_child`; unlinking `child` leaves it.
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
    next_enter(&mut poll, &doc_in_child).await;
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
    next_leave(&mut poll, &doc_in_child).await;

    // 7. Revoking `bob`'s drawer grant makes every later poll answer `Denied` without moving the
    //    cursor, and re-granting does not revive the withdrawn capability.
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
    let cursor = poll.cursor();
    assert_eq!(
        poll.next().await,
        Err(ClientError::Operation(DocsError::Denied))
    );
    assert_eq!(
        poll.cursor(),
        cursor,
        "a denied poll must not move the cursor"
    );
    assert_eq!(
        node.mint_doc(&bob, doc_scope.clone()).await.unwrap_err(),
        DocsError::Denied
    );
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let refused = crate::test_poll::Poller::new(node.docs(), observation.clone())
        .page(0)
        .await;
    assert!(
        matches!(refused, Err(ClientError::Operation(DocsError::Denied))),
        "regrant must not revive the withdrawn capability"
    );
    let mut poll = crate::test_poll::Poller::new(
        node.docs(),
        PollDatums {
            capability: node.mint_doc(&bob, doc_scope).await.unwrap(),
            ..observation
        },
    );
    next_enter(&mut poll, &doc_initial).await;
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
