//! Placement, boot, shutdown, and the ledger rows that finish interrupted work at the next boot.

use super::*;

#[tokio::test]
async fn placement_reply_waits_for_committed_rosters() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "workspace").await;
    let placement = drawer_placement(&drawer);
    let capability = node
        .mint_doc(
            &alice,
            DocScope {
                create: [placement.clone()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (paused, resume) = node.drawer_worker.as_ref().unwrap().pause_placement();
    let docs = node.docs().clone();
    let creation = tokio::spawn(async move {
        docs.call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: command("placed-note"),
                placement,
                datums: BTreeMap::from([note("placed-note")]),
            },
        )
        .await
    });
    let document = paused.await.unwrap();
    assert!(!creation.is_finished());
    let local = node.local();
    let targets = [&drawer, &local.present_id, &local.originated_id];
    for target in targets {
        assert_eq!(
            node.core()
                .state
                .doc_entry(target, &document)
                .await
                .unwrap(),
            None
        );
        assert!(
            !node
                .roster_datums(target)
                .await
                .contains_key(&doc_entry_key(&document))
        );
    }
    resume.send(()).unwrap();
    let receipt = creation.await.unwrap().unwrap();
    assert_eq!(receipt.doc_id, document);
    for target in targets {
        assert_eq!(
            node.core()
                .state
                .doc_entry(target, &document)
                .await
                .unwrap(),
            Some(Access::Admin)
        );
        assert!(
            node.roster_datums(target)
                .await
                .contains_key(&doc_entry_key(&document))
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn dropped_placement_caller_releases_the_command_worker() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "workspace").await;
    let (paused, resume) = node.drawer_worker.as_ref().unwrap().pause_placement();
    let mut creation = Box::pin(node.create_doc(&alice, &drawer, "abandoned-note"));
    let document = tokio::select! {
        document = paused => document.unwrap(),
        result = &mut creation => panic!("placement returned before its gate was released: {result:?}"),
    };
    drop(creation);
    let next = node.create_doc(&alice, &drawer, "next-note").await.unwrap();
    assert_ne!(document, next);
    assert!(
        resume.is_closed(),
        "abandoned placement still owns its gate"
    );
    let local = node.local();
    for target in [&drawer, &local.present_id, &local.originated_id] {
        assert_eq!(
            node.core()
                .state
                .doc_entry(target, &document)
                .await
                .unwrap(),
            None,
        );
        assert_eq!(
            node.core().state.doc_entry(target, &next).await.unwrap(),
            Some(Access::Admin),
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn drawer_drop_and_stop_reclaim_state_and_recover_interrupted_placement() {
    enum Shutdown {
        Stop,
        Drop,
    }

    for shutdown in [Shutdown::Stop, Shutdown::Drop] {
        let mut node = Node::boot().await;
        let alice = node.creator("alice").await;
        let drawer = node.collection(&alice, "workspace").await;
        let local = node.local();
        let placement = drawer_placement(&drawer);
        let capability = node
            .mint_doc(
                &alice,
                DocScope {
                    create: [placement.clone()].into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let core = Arc::downgrade(node.core());
        let projection = Arc::downgrade(&node.core().projection);
        let docs_host = Arc::downgrade(&node.docs.as_ref().unwrap().host);
        let drawers_host = Arc::downgrade(&node.drawers.as_ref().unwrap().host);
        let worker_tasks = node.drawer_worker.as_ref().unwrap().tasks();
        let (paused, resume) = node.drawer_worker.as_ref().unwrap().pause_placement();
        let docs = node.docs().clone();
        let creation = tokio::spawn(async move {
            docs.call(
                RequestContext::root(),
                CreateDoc {
                    capability,
                    command: command("interrupted-note"),
                    placement,
                    datums: BTreeMap::from([note("interrupted-note")]),
                },
            )
            .await
        });
        let document = paused.await.unwrap();
        assert!(!creation.is_finished());
        assert_eq!(worker_tasks.live_tasks(), 1);
        match shutdown {
            Shutdown::Stop => {
                node.drawers_stop.take().unwrap().stop().await;
                node.drawers.take();
                node.drawer_worker_stop.take().unwrap().stop().await;
                node.drawer_worker.take();
            }
            Shutdown::Drop => {
                node.drawers.take();
                node.drawer_worker.take();
                node.docs.take();
            }
        }
        assert!(matches!(
            creation.await.unwrap(),
            Err(ClientError::Method(DocsError::Access(_)))
        ));
        node.stop_services().await;
        drop(resume);
        assert_eq!(worker_tasks.live_tasks(), 0);
        assert!(core.upgrade().is_none(), "Docs-owning core was retained");
        assert!(
            projection.upgrade().is_none(),
            "drawer projection was retained"
        );
        assert!(docs_host.upgrade().is_none(), "Docs host was retained");
        assert!(
            drawers_host.upgrade().is_none(),
            "Drawers host was retained"
        );
        let state: String = node
            .sql
            .query(
                "SELECT state FROM big_cabinet_commands WHERE command_id = :command_id",
                ("interrupted-note",),
            )
            .await
            .unwrap()
            .one()
            .unwrap();
        assert_eq!(state, "committed");
        node.restart().await.unwrap();
        assert_eq!(node.local(), local);
        for target in [&drawer, &local.present_id, &local.originated_id] {
            assert_eq!(
                node.core()
                    .state
                    .doc_entry(target, &document)
                    .await
                    .unwrap(),
                Some(Access::Admin)
            );
            assert!(
                node.roster_datums(target)
                    .await
                    .contains_key(&doc_entry_key(&document))
            );
        }
        assert_eq!(
            node.read_note(&alice, &document).await.unwrap(),
            note("interrupted-note").1
        );
        let state: String = node
            .sql
            .query(
                "SELECT state FROM big_cabinet_commands WHERE command_id = :command_id",
                ("interrupted-note",),
            )
            .await
            .unwrap()
            .one()
            .unwrap();
        assert_eq!(state, "done");
        node.stop().await;
    }
}

#[tokio::test]
async fn boot_creates_the_local_drawers_once() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let local = node.local();
    assert_eq!(
        BTreeSet::from([
            &local.present_id,
            &local.originated_id,
            &local.known_drawers_id,
            &local.config_id,
        ])
        .len(),
        4
    );
    // The service itself administers them: it created them.
    let view = node
        .view(&node.drawers_actor, &local.present_id)
        .await
        .unwrap();
    assert_eq!(view.access, Access::Admin);
    assert_eq!(view.layout.known(), Some(LayoutKind::SingleGroup));
    assert!(view.docs.is_empty(), "{view:?}");
    let roster = node.roster_datums(&local.present_id).await;
    assert!(roster.contains_key(&drawer_key()), "{roster:?}");
    assert!(roster.contains_key(&roster_key()), "{roster:?}");

    let config_view = node
        .view(&node.drawers_actor, &local.config_id)
        .await
        .unwrap();
    assert_eq!(config_view.access, Access::Admin);
    assert_eq!(config_view.layout.known(), Some(LayoutKind::SelfDoc));
    assert!(config_view.docs.is_empty(), "{config_view:?}");
    assert_eq!(
        node.view(&alice, &local.config_id).await.unwrap_err(),
        DrawersError::Denied
    );

    node.restart().await.unwrap();
    assert_eq!(node.local(), local, "a second boot finds the same drawers");
    node.stop().await;
}

#[tokio::test]
async fn an_add_interrupted_at_either_boundary_is_finished_by_a_retry_or_at_boot() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "draft").await.unwrap();
    let shared_docs = groups_of(&node.view(&alice, &shared).await.unwrap()).1;

    // The crash: the row is begun and the grant landed, the entry was never published.
    node.arm_crash(CrashPoint::DocGranted);
    let crashed = node
        .add_doc(&alice, "interrupted", &shared, &doc, Access::Write)
        .await
        .unwrap_err();
    assert!(
        matches!(&crashed, DrawersError::Storage(message) if message.contains("injected crash")),
        "{crashed:?}"
    );
    assert_eq!(
        node.keyhive_level(shared_docs, doc.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Edit)
    );
    assert!(
        !node
            .roster_datums(&shared)
            .await
            .contains_key(&doc_entry_key(&doc))
    );
    assert_eq!(node.core().ledger.unfinished().await.unwrap().len(), 1);

    // A retry of the same command resumes the row rather than being refused, and finishes it.
    let receipt = node
        .add_doc(&alice, "interrupted", &shared, &doc, Access::Write)
        .await
        .unwrap();
    assert_eq!(receipt.doc_id, shared);
    assert_eq!(
        node.roster_datums(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Write"})
    );
    assert_eq!(
        node.view(&alice, &shared).await.unwrap().docs,
        BTreeMap::from([(doc.clone(), Access::Write)])
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    assert_eq!(
        node.add_doc(&alice, "interrupted", &shared, &doc, Access::Write)
            .await
            .unwrap(),
        receipt,
        "and from then on the command answers with its receipt"
    );

    // The other boundary with nobody retrying: the roster says Read, the projection never
    // heard, and boot finishes it.
    let other = node.create_doc(&alice, &home, "other").await.unwrap();
    node.arm_crash(CrashPoint::RosterWritten);
    node.add_doc(&alice, "interrupted at boot", &shared, &other, Access::Read)
        .await
        .unwrap_err();
    assert_eq!(
        node.roster_datums(&shared).await[&doc_entry_key(&other)],
        json!({"access": "Read"})
    );
    // ListDocs answers from the projection alone (GetDrawer rescans the roster first).
    async fn listed(
        node: &Node,
        alice: &OpenedActor,
        drawer: &DrawerId,
    ) -> BTreeMap<DocId, Access> {
        node.list_docs(alice, drawer, 0, None)
            .await
            .unwrap()
            .into_iter()
            .map(|listed| (listed.doc_id, listed.access))
            .collect()
    }
    assert_eq!(
        listed(&node, &alice, &shared).await,
        BTreeMap::from([(doc.clone(), Access::Write)]),
        "the projection never saw the write"
    );
    node.restart().await.unwrap();
    assert_eq!(
        listed(&node, &alice, &shared).await,
        BTreeMap::from([(doc.clone(), Access::Write), (other.clone(), Access::Read)])
    );
    assert_eq!(
        node.keyhive_level(shared_docs, other.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Read)
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    let replayed = node
        .add_doc(&alice, "interrupted at boot", &shared, &other, Access::Read)
        .await
        .unwrap();
    assert_eq!(
        replayed.doc_id, shared,
        "boot's receipt answers the command"
    );
    node.stop().await;
}

#[tokio::test]
async fn a_removal_interrupted_after_its_roster_write_is_finished_at_boot() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "draft").await.unwrap();
    let shared_docs = groups_of(&node.view(&alice, &shared).await.unwrap()).1;
    node.add_doc(&alice, "add", &shared, &doc, Access::Write)
        .await
        .unwrap();

    // The crash: the roster and the projection dropped the entry, the grant was never revoked.
    node.arm_crash(CrashPoint::RosterRemoved);
    node.remove_doc(&alice, "remove", &shared, &doc)
        .await
        .unwrap_err();
    assert!(
        !node
            .roster_datums(&shared)
            .await
            .contains_key(&doc_entry_key(&doc))
    );
    assert_eq!(
        node.keyhive_level(shared_docs, doc.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Edit),
        "the entry is gone but the authority behind it lingers"
    );
    assert_eq!(node.core().ledger.unfinished().await.unwrap().len(), 1);

    node.restart().await.unwrap();
    assert_eq!(
        node.keyhive_level(shared_docs, doc.to_bytes32().unwrap())
            .await,
        None,
        "boot revokes what the removal left behind"
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    let receipt = node
        .remove_doc(&alice, "remove", &shared, &doc)
        .await
        .unwrap();
    assert_eq!(receipt.doc_id, shared, "boot's receipt answers the command");
    node.stop().await;
}

/// ADR 019 Boundaries: the mirror query is the node's own policy, keyed on the `mirror` intent
/// alone. A mirrored grouped drawer yields its three groups and its descriptor doc; a remembered
/// drawer that is not mirrored, and a drawer nobody remembered, yield nothing; a mirrored drawer
/// whose descriptor this node does not carry yields only the doc to fetch it by.
#[tokio::test]
async fn the_mirror_query_names_what_the_mirror_intent_wants() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let mirrored = node.collection(&alice, "mirrored").await;
    let remembered = node.collection(&alice, "remembered").await;
    let _unknown = node.collection(&alice, "unknown").await;
    let absent = DrawerId::new([7; 32]);
    let context = RequestContext::root();
    let drawers = node.drawers.as_ref().unwrap();
    assert_eq!(
        drawers.mirrored(&context).await.unwrap(),
        Mirrored::default()
    );

    node.remember(&node.drawers_actor, &mirrored, true).await;
    node.remember(&node.drawers_actor, &remembered, false).await;
    node.remember(&node.drawers_actor, &absent, true).await;
    let (meta, docs, sensitive) = groups_of(&node.view(&alice, &mirrored).await.unwrap());
    assert_eq!(
        drawers.mirrored(&context).await.unwrap(),
        Mirrored {
            groups: [meta, docs, sensitive].into(),
            docs: [mirrored.clone(), absent].into(),
        }
    );

    node.forget(&node.drawers_actor, &mirrored).await;
    assert_eq!(
        drawers.mirrored(&context).await.unwrap(),
        Mirrored {
            groups: Default::default(),
            docs: [DrawerId::new([7; 32])].into(),
        }
    );
    node.stop().await;
}

#[tokio::test]
async fn the_known_drawers_list_is_explicit_and_survives_restart() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let known_drawers = node.local().known_drawers_id;

    // The known-drawers list is a self-doc drawer, readable only through a grant — here the service
    // actor's, as the embedder's own handle — and remembering grants nobody anything.
    let known_drawers_descriptor = node.roster_datums(&known_drawers).await[&drawer_key()].clone();
    let known_drawers_layout = serde_json::from_value::<Drawer>(known_drawers_descriptor).unwrap();
    assert_eq!(
        known_drawers_layout.layout.known(),
        Some(LayoutKind::SelfDoc)
    );
    assert_eq!(
        node.view(&alice, &known_drawers).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.list_docs(&bob, &known_drawers, 0, None)
            .await
            .unwrap_err(),
        DrawersError::Denied
    );
    let config = node.local().config_id;
    assert_eq!(
        node.view(&alice, &config).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.list_docs(&bob, &config, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );
    node.core()
        .grants
        .grant(
            &RequestContext::root(),
            alice.actor.actor_id,
            drawer_object(Access::Admin, &known_drawers),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    assert!(node.known(&alice).await.is_empty());

    node.remember(&alice, &home, true).await;
    node.remember(&alice, &shared, false).await;
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(home.clone(), true), (shared.clone(), false)])
    );
    assert_eq!(
        node.roster_datums(&known_drawers).await[&known_entry_key(&home)],
        json!({"mirror": true})
    );

    // A policy change restates the same entry; forget removes it and nothing else —
    // the drawer, its access, and its bytes are untouched.
    node.drawers()
        .call(
            RequestContext::root(),
            SetDrawerPolicy {
                capability: node
                    .drawer_capability(&alice, &[&known_drawers])
                    .await
                    .unwrap(),
                drawer_id: shared.clone(),
                mirror: true,
            },
        )
        .await
        .unwrap();
    assert!(node.known_map(&alice).await[&shared]);
    node.forget(&alice, &home).await;
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(shared.clone(), true)])
    );
    assert!(
        node.view(&alice, &home).await.is_ok(),
        "the drawer itself is unaffected"
    );

    // The list lives on the known-drawers doc: a restart reads it back from the doc, not from
    // a separate database row.
    node.restart().await.unwrap();
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(shared.clone(), true)])
    );

    // Observing a drawer never writes the known-drawers list: creating and listing drawers does
    // not grow the known list.
    let _other = node.collection(&alice, "other").await;
    assert_eq!(node.known(&alice).await.len(), 1);
    node.stop().await;
}

#[tokio::test]
async fn sensitive_root_creation_replays_every_boundary_without_replacing_groups_or_docs() {
    for point in [
        CrashPoint::MetadataDocCreated,
        CrashPoint::MetadataDocGranted,
        CrashPoint::MetadataDocRecorded,
        CrashPoint::MetadataDocPublished,
    ] {
        for reboot in [false, true] {
            let mut node = Node::boot().await;
            let alice = node.creator("alice").await;
            let mut request = CreateDrawer {
                capability: node
                    .mint_drawer(
                        &alice,
                        DrawerScope {
                            create: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: command("interrupted metadata roots"),
                layout: CreateLayout::SingleGroup,
            };
            node.arm_crash(point);
            let error = node
                .drawers()
                .call(RequestContext::root(), request.clone())
                .await
                .unwrap_err();
            assert!(
                matches!(op(error), DrawersError::Storage(message) if message.contains("injected crash")),
                "{point:?}"
            );
            let rows = node.core().ledger.unfinished().await.unwrap();
            assert_eq!(rows.len(), 1);
            let ledger::Op::Create {
                groups: Some(groups),
                ..
            } = rows[0].op.clone()
            else {
                panic!(
                    "groups must be recorded before the sensitive root: {:?}",
                    rows[0]
                );
            };
            let root_command = create::sensitive_root_command(&request.command);
            let root_id: String = node
                .sql
                .query(
                    "SELECT doc_id FROM big_cabinet_commands WHERE command_id = ?",
                    (root_command.as_str(),),
                )
                .await
                .unwrap()
                .one()
                .unwrap();
            let root_id: DocId = root_id.parse().unwrap();
            assert_eq!(
                node.keyhive_level(groups.sensitive_group, root_id.to_bytes32().unwrap())
                    .await,
                if point == CrashPoint::MetadataDocCreated {
                    None
                } else {
                    Some(KeyhiveAccess::Admin)
                }
            );
            if reboot {
                node.restart().await.unwrap();
                assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
                request.capability = node
                    .mint_drawer(
                        &alice,
                        DrawerScope {
                            create: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            let receipt = node
                .drawers()
                .call(RequestContext::root(), request.clone())
                .await
                .unwrap();
            let view = node.view(&alice, &receipt.drawer_id).await.unwrap();
            assert_eq!(
                groups_of(&view),
                (groups.meta_group, groups.docs_group, groups.sensitive_group)
            );
            let DrawerLayout::SingleGroup(layout) = &view.layout else {
                panic!("expected a grouped drawer")
            };
            assert_eq!(layout.sensitive_root, root_id);
            assert!(node.roster_datums(&root_id).await.is_empty());
            assert_eq!(
                node.keyhive_level(groups.sensitive_group, root_id.to_bytes32().unwrap())
                    .await,
                Some(KeyhiveAccess::Admin)
            );
            assert_eq!(
                node.keyhive_level(groups.meta_group, root_id.to_bytes32().unwrap())
                    .await,
                None
            );
            assert_eq!(
                node.keyhive_level(groups.meta_group, receipt.drawer_id.to_bytes32().unwrap())
                    .await,
                Some(KeyhiveAccess::Admin)
            );
            assert_eq!(
                node.drawers()
                    .call(RequestContext::root(), request)
                    .await
                    .unwrap(),
                receipt
            );
            assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
            node.stop().await;
        }
    }
}

#[tokio::test]
async fn named_metadata_creation_replays_every_boundary_on_both_roots() {
    for root in MetaRoot::ALL {
        for point in [
            CrashPoint::MetadataDocCreated,
            CrashPoint::MetadataDocGranted,
            CrashPoint::MetadataDocRecorded,
            CrashPoint::MetadataDocPublished,
        ] {
            for reboot in [false, true] {
                let mut node = Node::boot().await;
                let alice = node.creator("alice").await;
                let drawer = node.collection(&alice, "workspace").await;
                let view = node.view(&alice, &drawer).await.unwrap();
                let (public, _, sensitive) = groups_of(&view);
                let DrawerLayout::SingleGroup(layout) = &view.layout else {
                    panic!("expected a grouped drawer")
                };
                let root_doc = match root {
                    MetaRoot::Public => drawer.clone(),
                    MetaRoot::Sensitive => layout.sensitive_root.clone(),
                };
                let (backing, other) = match root {
                    MetaRoot::Public => (public, sensitive),
                    MetaRoot::Sensitive => (sensitive, public),
                };
                let mut request = CreateMetaDoc {
                    capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                    command: command("interrupted named metadata"),
                    drawer_id: drawer.clone(),
                    root,
                    kind: "notes".into(),
                    name: "archive".into(),
                };
                node.arm_crash(point);
                let error = node
                    .drawers()
                    .call(RequestContext::root(), request.clone())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(op(error), DrawersError::Storage(message) if message.contains("injected crash")),
                    "{root:?} {point:?}"
                );
                let doc_id: String = node
                    .sql
                    .query(
                        "SELECT doc_id FROM big_cabinet_commands WHERE command_id = ?",
                        (request.command.as_str(),),
                    )
                    .await
                    .unwrap()
                    .one()
                    .unwrap();
                let doc_id: DocId = doc_id.parse().unwrap();
                let slot = meta_doc_key("notes", "archive").unwrap();
                let published = node.roster_datums(&root_doc).await.contains_key(&slot);
                assert_eq!(published, point == CrashPoint::MetadataDocPublished);
                assert_eq!(
                    node.keyhive_level(backing, doc_id.to_bytes32().unwrap())
                        .await,
                    if point == CrashPoint::MetadataDocCreated {
                        None
                    } else {
                        Some(KeyhiveAccess::Admin)
                    }
                );
                assert_eq!(node.core().ledger.unfinished().await.unwrap().len(), 1);
                if reboot {
                    node.restart().await.unwrap();
                    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
                    request.capability = node.drawer_capability(&alice, &[&drawer]).await.unwrap();
                }
                let receipt = node
                    .drawers()
                    .call(RequestContext::root(), request.clone())
                    .await
                    .unwrap();
                assert_eq!(receipt.doc_id, doc_id);
                assert_eq!(receipt.root, root);
                assert_eq!(
                    node.roster_datums(&root_doc).await[&slot],
                    json!({"docId": doc_id})
                );
                assert_eq!(
                    node.keyhive_level(backing, doc_id.to_bytes32().unwrap())
                        .await,
                    Some(KeyhiveAccess::Admin)
                );
                assert_eq!(
                    node.keyhive_level(other, doc_id.to_bytes32().unwrap())
                        .await,
                    None
                );
                assert_eq!(
                    node.view(&alice, &drawer).await.unwrap().meta_docs[&root]["notes/archive"],
                    doc_id
                );
                assert_eq!(
                    node.drawers()
                        .call(RequestContext::root(), request)
                        .await
                        .unwrap(),
                    receipt
                );
                assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
                node.stop().await;
            }
        }
    }
}

#[tokio::test]
async fn an_invite_interrupted_between_its_memberships_is_finished_at_boot() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    // The invitee is a group: a peer principal would need its contact card registered with
    // the node's Keyhive first, and the membership machinery is the same either way.
    let invitee = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let invitee_key = GroupKey(invitee.id().to_bytes());
    let principal = Principal::Group {
        group_id: invitee_key,
    };
    let (meta, docs, sensitive) = groups_of(&node.view(&alice, &home).await.unwrap());

    // The crash: the docs membership landed, the meta membership never did, and the row
    // was never finished.
    node.arm_crash(CrashPoint::DocsMembershipAdded);
    node.invite(&alice, &home, principal.clone(), Access::Read)
        .await
        .unwrap_err();
    assert_eq!(
        node.group_membership_level(docs, invitee_key).await,
        Some(KeyhiveAccess::Read)
    );
    assert_eq!(
        node.group_membership_level(meta, invitee_key).await,
        None,
        "the meta membership never landed"
    );
    assert_eq!(
        node.group_membership_level(sensitive, invitee_key).await,
        None,
        "the sensitive membership never landed"
    );
    assert_eq!(node.core().ledger.unfinished().await.unwrap().len(), 1);

    node.restart().await.unwrap();
    let rows = node.core().ledger.unfinished().await.unwrap();
    assert_eq!(
        node.group_membership_level(meta, invitee_key).await,
        Some(KeyhiveAccess::Read),
        "boot replay adds the missing membership (unfinished rows after boot: {rows:?})"
    );
    assert_eq!(
        node.group_membership_level(sensitive, invitee_key).await,
        Some(KeyhiveAccess::Read),
        "boot replay adds the missing sensitive membership (unfinished rows after boot: {rows:?})"
    );
    assert!(rows.is_empty());
    node.invite(&alice, &home, principal, Access::Read)
        .await
        .expect("a finished invite answers its retry");
    node.stop().await;
}

/// §16.6: a successor pointer resolves to the terminal id and creates nothing.
#[tokio::test]
async fn a_successor_chain_resolves_to_its_terminal_drawer_without_copying_anything() {
    async fn authority_snapshot(
        node: &Node,
        principals: &[GroupKey],
        targets: &BTreeSet<[u8; 32]>,
    ) -> BTreeMap<(GroupKey, [u8; 32]), Option<KeyhiveAccess>> {
        let mut snapshot = BTreeMap::new();
        for principal in principals {
            for target in targets {
                snapshot.insert(
                    (*principal, *target),
                    node.keyhive_level(*principal, *target).await,
                );
            }
        }
        snapshot
    }

    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let carol = node.creator("carol").await;
    let bob = node.actor("bob").await;
    let first = node.collection(&alice, "first").await;
    let second = node.collection(&carol, "second").await;
    let third = node.collection(&carol, "third").await;
    let drawers = [(&alice, &first), (&carol, &second), (&carol, &third)];
    let mut before = Vec::new();
    let mut targets = BTreeSet::new();
    for (owner, drawer) in drawers {
        let doc = node
            .create_doc(owner, drawer, &format!("content/{drawer}"))
            .await
            .unwrap();
        let child = node.collection(owner, &format!("child/{drawer}")).await;
        node.drawers()
            .call(
                RequestContext::root(),
                AddChild {
                    capability: node
                        .drawer_capability(owner, &[drawer, &child])
                        .await
                        .unwrap(),
                    command: command(&format!("link/{drawer}")),
                    parent_id: drawer.clone(),
                    child_id: child.clone(),
                    access: Access::Read,
                },
            )
            .await
            .unwrap();
        let view = node.view(owner, drawer).await.unwrap();
        assert_eq!(view.docs, BTreeMap::from([(doc.clone(), Access::Admin)]));
        assert_eq!(
            view.children,
            BTreeMap::from([(child.clone(), Access::Read)])
        );
        let (meta, docs, sensitive) = groups_of(&view);
        targets.extend([meta.0, docs.0, sensitive.0]);
        let sensitive_root = view.descriptor.sensitive_root().unwrap();
        for target in [drawer, &doc, &child, &sensitive_root] {
            targets.insert(target.to_bytes32().unwrap());
        }
        before.push((
            view,
            node.roster_datums(drawer).await,
            sensitive_root.clone(),
            node.roster_datums(&sensitive_root).await,
        ));
    }
    node.grant(&alice, &first, &bob, Access::Read).await;
    node.grant(&alice, &first, &carol, Access::Read).await;
    let relay = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let relay = group_key(&relay);
    node.invite(
        &alice,
        &first,
        Principal::Group { group_id: relay },
        Access::Relay,
    )
    .await
    .unwrap();
    let mut principals = vec![relay];
    for (view, _, _, _) in &before {
        let (meta, docs, sensitive) = groups_of(view);
        principals.extend([meta, docs, sensitive]);
    }
    let authority = authority_snapshot(&node, &principals, &targets).await;
    node.remember(&node.drawers_actor, &first, true).await;
    node.remember(&node.drawers_actor, &second, false).await;
    let known = node.known_map(&node.drawers_actor).await;
    let known_drawers = node.roster_datums(&node.local().known_drawers_id).await;
    let capability = node.drawer_capability(&bob, &[&first]).await.unwrap();
    let all = node
        .drawer_capability(&carol, &[&first, &second, &third])
        .await
        .unwrap();

    assert_eq!(
        node.resolve_successor(capability, &first).await.unwrap(),
        SuccessorResolution::NoSuccessor
    );
    assert_eq!(
        node.view(&alice, &second).await.unwrap_err(),
        DrawersError::Denied
    );
    for target in [&second, &third] {
        assert_eq!(
            node.view(&bob, target).await.unwrap_err(),
            DrawersError::Denied
        );
    }
    node.set_successor(&alice, &first, &second).await.unwrap();
    node.set_successor(&carol, &second, &third).await.unwrap();
    for (start, terminal) in [
        (
            &first,
            SuccessorResolution::Resolved {
                drawer_id: third.clone(),
            },
        ),
        (
            &second,
            SuccessorResolution::Resolved {
                drawer_id: third.clone(),
            },
        ),
        (&third, SuccessorResolution::NoSuccessor),
    ] {
        assert_eq!(node.resolve_successor(all, start).await.unwrap(), terminal);
    }
    assert_eq!(
        node.drawers()
            .call(
                RequestContext::root(),
                ResolveDrawerSuccessor {
                    capability,
                    drawer_id: first.clone(),
                }
            )
            .await
            .unwrap(),
        SuccessorResolution::AccessRequired {
            drawer_id: second.clone()
        },
    );
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                SetDrawerSuccessor {
                    capability,
                    drawer_id: first.clone(),
                    successor: third.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied,
    );
    for target in [&second, &third] {
        assert_eq!(
            op(node
                .drawers()
                .call(
                    RequestContext::root(),
                    ResolveDrawerSuccessor {
                        capability,
                        drawer_id: target.clone(),
                    }
                )
                .await
                .unwrap_err()),
            DrawersError::Denied,
        );
        assert_eq!(
            op(node
                .drawers()
                .call(
                    RequestContext::root(),
                    GetDrawer {
                        capability,
                        drawer_id: target.clone(),
                    }
                )
                .await
                .unwrap_err()),
            DrawersError::Denied,
        );
        assert_eq!(
            node.view(&bob, target).await.unwrap_err(),
            DrawersError::Denied
        );
    }
    assert_eq!(
        node.view(&alice, &second).await.unwrap_err(),
        DrawersError::Denied
    );
    for (view, _, _, _) in &before[1..] {
        for doc in view.docs.keys() {
            assert_eq!(
                node.read_note(&bob, doc).await.unwrap_err(),
                DocsError::Denied
            );
        }
    }
    assert_eq!(
        authority_snapshot(&node, &principals, &targets).await,
        authority
    );
    assert_eq!(node.known_map(&node.drawers_actor).await, known);
    assert_eq!(
        node.roster_datums(&node.local().known_drawers_id).await,
        known_drawers
    );
    for (index, (owner, drawer)) in drawers.into_iter().enumerate() {
        let (view, mut roster, sensitive_root, sensitive_roster) = before[index].clone();
        if let Some(successor) = [&second, &third].get(index) {
            roster.insert(successor_key(), json!({"drawerId": successor}));
        }
        assert_eq!(node.roster_datums(drawer).await, roster);
        assert_eq!(node.roster_datums(&sensitive_root).await, sensitive_roster);
        assert_eq!(node.view(owner, drawer).await.unwrap(), view);
    }
    node.stop().await;
}

#[tokio::test]
async fn self_doc_successors_resolve_in_both_directions_without_moving_content() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let first = node.self_doc(&alice, &home, "first note").await;
    let middle = node.collection(&alice, "middle").await;
    let terminal = node.self_doc(&alice, &home, "terminal note").await;
    let first_roster = node.roster_datums(&first).await;
    let terminal_roster = node.roster_datums(&terminal).await;
    let bob = node.actor("bob").await;
    node.grant(&alice, &first, &bob, Access::Relay).await;
    let capability = node
        .drawer_capability(&alice, &[&first, &middle, &terminal])
        .await
        .unwrap();
    let relay = node.drawer_capability(&bob, &[&first]).await.unwrap();
    assert_eq!(
        node.resolve_successor(capability, &first).await.unwrap(),
        SuccessorResolution::NoSuccessor
    );
    assert_eq!(
        node.resolve_successor(capability, &terminal).await.unwrap(),
        SuccessorResolution::NoSuccessor
    );
    node.set_successor(&alice, &first, &middle).await.unwrap();
    node.set_successor(&alice, &middle, &terminal)
        .await
        .unwrap();
    for start in [&first, &middle] {
        assert_eq!(
            node.resolve_successor(capability, start).await.unwrap(),
            SuccessorResolution::Resolved {
                drawer_id: terminal.clone()
            }
        );
    }
    assert_eq!(
        node.read_note(&bob, &first).await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.resolve_successor(relay, &first).await.unwrap_err(),
        DrawersError::Denied
    );
    for target in [&middle, &terminal] {
        assert_eq!(
            node.view(&bob, target).await.unwrap_err(),
            DrawersError::Denied
        );
    }
    assert_eq!(
        node.read_note(&bob, &terminal).await.unwrap_err(),
        DocsError::Denied
    );
    let mut expected = first_roster;
    expected.insert(successor_key(), json!({"drawerId": middle}));
    assert_eq!(node.roster_datums(&first).await, expected);
    assert_eq!(node.roster_datums(&terminal).await, terminal_roster);
    assert_eq!(
        node.read_note(&alice, &first).await.unwrap(),
        note("first note").1
    );
    assert_eq!(
        node.read_note(&alice, &terminal).await.unwrap(),
        note("terminal note").1
    );
    node.restart().await.unwrap();
    let capability = node
        .drawer_capability(&alice, &[&first, &middle, &terminal])
        .await
        .unwrap();
    assert_eq!(
        node.resolve_successor(capability, &first).await.unwrap(),
        SuccessorResolution::Resolved {
            drawer_id: terminal
        }
    );
    assert_eq!(node.roster_datums(&first).await, expected);
    node.stop().await;
}

/// §16.6: a cycle is an error, never a silent walk.
#[tokio::test]
async fn a_successor_cycle_is_refused_rather_than_followed() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let a = node.collection(&alice, "a").await;
    let b = node.self_doc(&alice, &a, "b").await;
    let c = node.collection(&alice, "c").await;
    let capability = node.drawer_capability(&alice, &[&a, &b, &c]).await.unwrap();
    node.set_successor(&alice, &a, &b).await.unwrap();
    node.set_successor(&alice, &b, &c).await.unwrap();
    node.set_successor(&alice, &c, &b).await.unwrap();
    assert_eq!(
        node.resolve_successor(capability, &a).await.unwrap_err(),
        DrawersError::SuccessorCycle {
            start: a.clone(),
            revisited: b.clone()
        },
    );
    node.set_successor(&alice, &c, &a).await.unwrap();
    assert_eq!(
        node.resolve_successor(capability, &a).await.unwrap_err(),
        DrawersError::SuccessorCycle {
            start: a.clone(),
            revisited: a
        },
    );
    node.stop().await;
}

#[tokio::test]
async fn successor_self_pointers_are_refused_at_write_and_read() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let single = node.self_doc(&alice, &home, "single").await;
    let terminal = node.collection(&alice, "terminal").await;
    let capability = node
        .drawer_capability(&alice, &[&home, &single, &terminal])
        .await
        .unwrap();
    for drawer in [&home, &single] {
        node.set_successor(&alice, drawer, &terminal).await.unwrap();
        let before = node.roster_datums(drawer).await;
        assert_eq!(
            node.set_successor(&alice, drawer, drawer)
                .await
                .unwrap_err(),
            DrawersError::SuccessorSelf {
                drawer_id: drawer.clone()
            },
        );
        assert_eq!(node.roster_datums(drawer).await, before);
        node.core()
            .write_roster(
                &RequestContext::root(),
                drawer,
                None,
                BTreeMap::from([(successor_key(), json!({"drawerId": drawer}))]),
                BTreeSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            node.resolve_successor(capability, drawer)
                .await
                .unwrap_err(),
            DrawersError::SuccessorSelf {
                drawer_id: drawer.clone()
            },
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn successor_absence_after_removal_is_not_an_unavailable_target() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let old = node.collection(&alice, "old").await;
    let missing = DocId::new([213; 32]);
    let capability = node.drawer_capability(&alice, &[&old]).await.unwrap();
    node.set_successor(&alice, &old, &missing).await.unwrap();
    assert_eq!(
        node.roster_datums(&old).await[&successor_key()],
        json!({"drawerId": missing})
    );
    assert_eq!(
        node.resolve_successor(capability, &old).await.unwrap(),
        SuccessorResolution::AccessRequired {
            drawer_id: missing.clone()
        },
    );
    node.core()
        .grants
        .grant(
            &RequestContext::root(),
            alice.actor.actor_id,
            drawer_object(Access::Read, &missing),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let authorized = node
        .drawer_capability(&alice, &[&old, &missing])
        .await
        .unwrap();
    assert_eq!(
        node.resolve_successor(authorized, &old).await.unwrap_err(),
        DrawersError::Docs(DocsError::DocNotFound),
    );
    node.core()
        .write_roster(
            &RequestContext::root(),
            &old,
            None,
            BTreeMap::new(),
            BTreeSet::from([successor_key()]),
        )
        .await
        .unwrap();
    assert_eq!(
        node.resolve_successor(capability, &old).await.unwrap(),
        SuccessorResolution::NoSuccessor
    );
    node.stop().await;
}

#[tokio::test]
async fn a_pending_successor_is_not_absence_or_a_terminal_answer() {
    use automerge::ReadDoc;
    use automerge::transaction::Transactable;

    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let first = node.collection(&alice, "first").await;
    let middle = node.self_doc(&alice, &first, "middle").await;
    let terminal = node.collection(&alice, "terminal").await;
    let capability = node
        .drawer_capability(&alice, &[&first, &middle, &terminal])
        .await
        .unwrap();
    let first_only = node.drawer_capability(&alice, &[&first]).await.unwrap();
    node.set_successor(&alice, &first, &middle).await.unwrap();
    node.set_successor(&alice, &middle, &terminal)
        .await
        .unwrap();
    let big_repo::DocLookup::Ready(handle) = node.repo.get_doc(&middle).await.unwrap() else {
        panic!("the successor document is local")
    };
    handle
        .with_document(|doc| {
            let object =
                |doc: &automerge::transaction::Transaction<'_>,
                 parent: &automerge::ObjId,
                 prop: &str| { doc.get(parent, prop).unwrap().unwrap().1 };
            let mut tx = doc.transaction();
            let datums = object(&tx, &automerge::ROOT, "datums");
            let value = object(&tx, &datums, &successor_key().to_string());
            tx.put(
                &value,
                "drawerId",
                automerge::ScalarValue::Unknown {
                    type_code: 15,
                    bytes: vec![3],
                },
            )
            .unwrap();
            let dmeta = object(&tx, &datums, "org.example.daybook.dmeta/main");
            let metadata = object(&tx, &dmeta, "datums");
            let metadata = object(&tx, &metadata, &successor_key().to_string());
            let signatures = object(&tx, &metadata, "signatures");
            let previous = tx.get(&signatures, 0).unwrap().unwrap().1;
            let fields: Vec<_> = ["actorUuid", "docUuid", "signature"]
                .into_iter()
                .map(|field| {
                    let automerge::Value::Scalar(value) =
                        tx.get(&previous, field).unwrap().unwrap().0
                    else {
                        panic!("signature fields are scalars")
                    };
                    (field, value.into_owned())
                })
                .collect();
            tx.delete(&signatures, 0).unwrap();
            let entry = tx
                .insert_object(&signatures, 0, automerge::ObjType::Map)
                .unwrap();
            for (field, value) in fields {
                tx.put(&entry, field, value).unwrap();
            }
            tx.commit();
        })
        .await
        .unwrap();
    let read = node
        .docs()
        .call(
            RequestContext::root(),
            ReadDatums {
                capability: node
                    .mint_doc(
                        &node.drawers_actor,
                        DocScope {
                            read: [middle.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                doc_id: middle.clone(),
                at: At::Current,
                datums: Some(BTreeSet::from([successor_key()])),
                optimistic: false,
            },
        )
        .await
        .unwrap();
    let pending = &read.datums[&successor_key()];
    assert!(
        matches!(pending, DatumRead::Pending { value: None, reason, .. } if reason.contains("unknown scalar kind")),
        "{pending:?}"
    );
    assert_eq!(
        node.resolve_successor(first_only, &first).await.unwrap(),
        SuccessorResolution::AccessRequired {
            drawer_id: middle.clone()
        },
    );
    let expected = DrawersError::RosterUnreadable(format!("successor on {middle}: {pending:?}"));
    for start in [&first, &middle] {
        assert_eq!(
            node.resolve_successor(capability, start).await.unwrap_err(),
            expected
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn successor_resolution_requires_read_at_each_hop_without_disclosing_hidden_cycles() {
    for self_doc in [false, true] {
        let node = Node::boot().await;
        let alice = node.creator("alice").await;
        let bob = node.actor("bob").await;
        let home = node.collection(&alice, "home").await;
        let (a, b, c) = if self_doc {
            (
                node.self_doc(&alice, &home, "a").await,
                node.self_doc(&alice, &home, "b").await,
                node.self_doc(&alice, &home, "c").await,
            )
        } else {
            (
                home,
                node.collection(&alice, "b").await,
                node.collection(&alice, "c").await,
            )
        };
        let owner = node.drawer_capability(&alice, &[&a, &b, &c]).await.unwrap();
        node.grant(&alice, &a, &bob, Access::Relay).await;
        let relay = node.drawer_capability(&bob, &[&a]).await.unwrap();
        assert_eq!(
            node.resolve_successor(relay, &a).await.unwrap_err(),
            DrawersError::Denied
        );
        node.set_successor(&alice, &a, &b).await.unwrap();
        assert_eq!(
            node.resolve_successor(relay, &a).await.unwrap_err(),
            DrawersError::Denied
        );

        node.grant(&alice, &a, &bob, Access::Read).await;
        let first_only = node.drawer_capability(&bob, &[&a]).await.unwrap();
        let required = SuccessorResolution::AccessRequired {
            drawer_id: b.clone(),
        };
        assert_eq!(
            node.resolve_successor(first_only, &a).await.unwrap(),
            required
        );
        assert_eq!(
            node.resolve_successor(owner, &a).await.unwrap(),
            SuccessorResolution::Resolved {
                drawer_id: b.clone()
            },
        );
        node.set_successor(&alice, &b, &c).await.unwrap();
        assert_eq!(
            node.resolve_successor(first_only, &a).await.unwrap(),
            required
        );
        assert_eq!(
            node.resolve_successor(owner, &a).await.unwrap(),
            SuccessorResolution::Resolved {
                drawer_id: c.clone()
            },
        );
        node.set_successor(&alice, &b, &a).await.unwrap();
        assert_eq!(
            node.resolve_successor(first_only, &a).await.unwrap(),
            required
        );
        assert_eq!(
            node.resolve_successor(owner, &a).await.unwrap_err(),
            DrawersError::SuccessorCycle {
                start: a.clone(),
                revisited: a.clone()
            },
        );
        node.core()
            .write_roster(
                &RequestContext::root(),
                &b,
                None,
                BTreeMap::from([(successor_key(), json!({"drawerId": b}))]),
                BTreeSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            node.resolve_successor(first_only, &a).await.unwrap(),
            required
        );
        assert_eq!(
            node.resolve_successor(owner, &a).await.unwrap_err(),
            DrawersError::SuccessorSelf {
                drawer_id: b.clone()
            },
        );

        node.set_successor(&alice, &b, &c).await.unwrap();
        node.grant(&alice, &b, &bob, Access::Relay).await;
        let second_relay = node.drawer_capability(&bob, &[&a, &b]).await.unwrap();
        assert_eq!(
            node.resolve_successor(second_relay, &a).await.unwrap(),
            required
        );
        node.grant(&alice, &b, &bob, Access::Read).await;
        let first_two = node.drawer_capability(&bob, &[&a, &b]).await.unwrap();
        let third_required = SuccessorResolution::AccessRequired {
            drawer_id: c.clone(),
        };
        assert_eq!(
            node.resolve_successor(first_two, &a).await.unwrap(),
            third_required
        );
        assert_eq!(
            node.resolve_successor(first_only, &a).await.unwrap(),
            required
        );
        node.grant(&alice, &c, &bob, Access::Read).await;
        let all = node.drawer_capability(&bob, &[&a, &b, &c]).await.unwrap();
        assert_eq!(
            node.resolve_successor(all, &a).await.unwrap(),
            SuccessorResolution::Resolved { drawer_id: c },
        );
        assert_eq!(
            node.resolve_successor(first_two, &a).await.unwrap(),
            third_required
        );
        node.stop().await;
    }
}
