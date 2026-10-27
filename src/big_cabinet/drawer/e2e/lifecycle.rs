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
                facets: BTreeMap::from([note("placed-note")]),
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
                .roster_facets(target)
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
            node.roster_facets(target)
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
                    facets: BTreeMap::from([note("interrupted-note")]),
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
            Err(ClientError::Operation(DocsError::Access(_)))
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
                node.roster_facets(target)
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
    let local = node.local();
    assert_ne!(local.present_id, local.originated_id);
    // The service itself administers them: it created them.
    let view = node
        .view(&node.drawers_actor, &local.present_id)
        .await
        .unwrap();
    assert_eq!(view.access, Access::Admin);
    assert_eq!(view.layout.known(), Some(LayoutKind::SingleGroup));
    assert!(view.docs.is_empty(), "{view:?}");
    let roster = node.roster_facets(&local.present_id).await;
    assert!(roster.contains_key(&drawer_key()), "{roster:?}");
    assert!(roster.contains_key(&roster_key()), "{roster:?}");

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
            .roster_facets(&shared)
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
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
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
        node.roster_facets(&shared).await[&doc_entry_key(&other)],
        json!({"access": "Read"})
    );
    // ListDocs answers from the projection alone (GetDrawer rescans the roster first).
    async fn listed(node: &Node, alice: &Actor, drawer: &DrawerId) -> BTreeMap<DocId, Access> {
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
            .roster_facets(&shared)
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

#[tokio::test]
async fn the_known_drawers_registry_is_explicit_and_survives_restart() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let registry = node.local().registry_id;

    // The registry is a self-doc drawer, readable only through a grant — here the service
    // actor's, as the embedder's own handle — and remembering grants nobody anything.
    let registry_descriptor = node.roster_facets(&registry).await[&drawer_key()].clone();
    let registry_layout = serde_json::from_value::<Drawer>(registry_descriptor).unwrap();
    assert_eq!(registry_layout.layout.known(), Some(LayoutKind::SelfDoc));
    assert_eq!(
        node.view(&alice, &registry).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.list_docs(&bob, &registry, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );
    node.core()
        .grants
        .grant(
            &RequestContext::root(),
            alice.actor_id,
            drawer_object(Access::Admin, &registry),
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
        node.roster_facets(&registry).await[&known_entry_key(&home)],
        json!({"mirror": true})
    );

    // A policy change restates the same entry; forget removes it and nothing else —
    // the drawer, its access, and its bytes are untouched.
    node.drawers()
        .call(
            RequestContext::root(),
            SetDrawerPolicy {
                capability: node.drawer_capability(&alice, &[&registry]).await.unwrap(),
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

    // The list lives on the registry doc: a restart reads it back from the doc, not from
    // a separate database row.
    node.restart().await.unwrap();
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(shared.clone(), true)])
    );

    // Observing a drawer never writes the registry: creating and listing drawers does
    // not grow the known list.
    let _other = node.collection(&alice, "other").await;
    assert_eq!(node.known(&alice).await.len(), 1);
    node.stop().await;
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
    let (meta, docs) = groups_of(&node.view(&alice, &home).await.unwrap());

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
    assert_eq!(node.core().ledger.unfinished().await.unwrap().len(), 1);

    node.restart().await.unwrap();
    let rows = node.core().ledger.unfinished().await.unwrap();
    assert_eq!(
        node.group_membership_level(meta, invitee_key).await,
        Some(KeyhiveAccess::Read),
        "boot replay adds the missing membership (unfinished rows after boot: {rows:?})"
    );
    assert!(rows.is_empty());
    node.invite(&alice, &home, principal, Access::Read)
        .await
        .expect("a finished invite answers its retry");
    node.stop().await;
}
