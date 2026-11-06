//! Two nodes over a real transport: a drawer created and shared on one node becomes the
//! authorization boundary for its documents on the other, once the drawer doc has synced.

use super::*;
use crate::peer_net::Network;
use crate::peer_net::link;
use big_cabinet_core::drawer::protocol::Principal;
use std::collections::HashSet;

impl Node {
    /// Provision a standing grant the way an embedder does for its own actors, without going
    /// through `GrantDrawer` (which would need an Admin on this node).
    async fn provision(&self, actor: &Actor, drawer: &DrawerId, access: Access) {
        self.core()
            .grants
            .grant(
                &RequestContext::root(),
                actor.actor_id,
                DrawerAccess::object(access, drawer),
                Rights::USE,
                None,
            )
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drawer_shared_by_another_node_authorizes_reads_on_this_node() {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot_with_seed(91).await;
    let reader = Node::boot_with_seed(92).await;
    let owner_net = Network::boot(&owner.repo, 91).await;
    let mut reader_net = Network::boot(&reader.repo, 92).await;
    let (connection, accepted) =
        link((&owner.repo, &owner_net), (&reader.repo, &mut reader_net)).await;

    let alice = owner.creator("alice").await;
    let drawer = owner.collection(&alice, "shared").await;
    let doc = owner
        .create_doc(&alice, &drawer, "shared-note")
        .await
        .unwrap();
    let reader_peer = reader.repo.local_peer_id();
    let denied = |target: &DocId| {
        let store = owner.repo.shared_part_store();
        let target = target.clone();
        let peer = reader_peer.clone();
        async move {
            store
                .read_denied(big_sync::ReadTarget::Object(target), peer)
                .await
                .unwrap()
        }
    };
    assert!(denied(&drawer).await);
    assert!(denied(&doc).await);

    owner
        .repo
        .receive_keyhive_contact_card(&reader.repo.local_keyhive_contact_card())
        .await
        .unwrap();
    owner
        .invite(
            &alice,
            &drawer,
            Principal::Peer {
                peer_id: GroupKey(reader_peer.to_bytes32().unwrap()),
            },
            Access::Read,
        )
        .await
        .unwrap();
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    reader.repo.wait_for_keyhive_reconciliation().await.unwrap();
    assert!(!denied(&drawer).await);
    assert!(!denied(&doc).await);

    let (peers, peer_rx) = tokio::sync::watch::channel(HashSet::from([owner.repo.local_peer_id()]));
    let demand = tokio::spawn(reader.repo.drive_sync_demand(&reader_net.worker, peer_rx));
    let mirror = reader.repo.interest(drawer.clone()).await.unwrap();
    mirror.wait_until_ready().await.unwrap();
    reader
        .wait_indexed(big_cabinet_core::drawer::Drawer::ID, |route, membership| {
            route.doc_id == drawer && matches!(membership, DatumMembership::Present { .. })
        })
        .await;

    let bob = reader.actor("bob").await;
    let carol = reader.actor("carol").await;
    reader.provision(&bob, &drawer, Access::Read).await;
    let view = reader.view(&bob, &drawer).await.unwrap();
    assert_eq!(view.drawer_id, drawer);
    assert_eq!(view.access, Access::Read);
    assert_eq!(view.docs, BTreeMap::from([(doc.clone(), Access::Admin)]));
    assert!(matches!(
        reader.view(&carol, &drawer).await,
        Err(DrawersError::Denied)
    ));
    assert!(matches!(
        reader
            .mint_doc(
                &carol,
                DocScope {
                    read: [doc.clone()].into(),
                    ..Default::default()
                },
            )
            .await,
        Err(DocsError::Denied)
    ));

    let capability = reader
        .mint_doc(
            &bob,
            DocScope {
                read: [doc.clone()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (key, _) = note("");
    let mut observations = crate::test_poll::Poller::doc(reader.docs(), capability, doc.clone());
    loop {
        match observations
            .next()
            .await
            .unwrap()
            .expect("the note never replayed")
        {
            DatumEvent::Datum {
                doc_id,
                datum_key,
                datum: ObservedDatum::Present { .. },
                ..
            } if doc_id == doc && datum_key == key => break,
            _ => {}
        }
    }
    let expected = json!({"$type": NOTE, "value": "shared-note"});
    assert_eq!(owner.read_note(&alice, &doc).await.unwrap(), expected);
    assert_eq!(reader.read_note(&bob, &doc).await.unwrap(), expected);

    // A cabinet grant can exceed this node's keyhive standing: the reader holds only keyhive
    // Read on the shared drawer, so keyhive refuses the commit a Write grant authorizes. That
    // refusal is the caller's write denied, not a repository fault. The second attempt runs
    // on the handle re-acquired after the first rejection broke the bundle.
    let dave = reader.actor("dave").await;
    reader.provision(&dave, &drawer, Access::Write).await;
    for _ in 0..2 {
        assert_eq!(
            reader.write_note(&dave, &doc, "beyond keyhive Read").await,
            Err(DocsError::Denied)
        );
    }
    assert_eq!(reader.read_note(&bob, &doc).await.unwrap(), expected);

    drop(observations);
    drop(mirror);
    drop(peers);
    demand.await.unwrap().unwrap();
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    reader_net.stop().await;
    owner_net.stop().await;
    reader.stop().await;
    owner.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn native_metadata_observation_on_a_remote_relay_needs_no_local_grants_or_child_walk() {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot_with_seed(93).await;
    let relay = Node::boot_with_seed(94).await;
    let owner_net = Network::boot(&owner.repo, 93).await;
    let mut relay_net = Network::boot(&relay.repo, 94).await;
    let (connection, accepted) =
        link((&owner.repo, &owner_net), (&relay.repo, &mut relay_net)).await;
    let context = RequestContext::root();
    let alice = owner.creator("alice").await;
    let drawer = owner.collection(&alice, "metadata parent").await;
    let child = owner.collection(&alice, "metadata child").await;
    let ordinary = owner.create_doc(&alice, &drawer, "ordinary").await.unwrap();
    owner
        .drawers()
        .call(
            context.child(),
            AddChild {
                capability: owner
                    .drawer_capability(&alice, &[&drawer, &child])
                    .await
                    .unwrap(),
                command: command("metadata child link"),
                parent_id: drawer.clone(),
                child_id: child.clone(),
                access: Access::Relay,
            },
        )
        .await
        .unwrap();
    let capability = owner.drawer_capability(&alice, &[&drawer]).await.unwrap();
    let mut metadata = BTreeMap::new();
    for root in [MetaRoot::Public, MetaRoot::Sensitive] {
        let created = owner
            .drawers()
            .call(
                context.child(),
                CreateMetaDoc {
                    capability,
                    command: command(&format!("metadata {root:?}")),
                    drawer_id: drawer.clone(),
                    kind: "notes".into(),
                    name: "main".into(),
                    root,
                },
            )
            .await
            .unwrap();
        assert!(
            metadata
                .insert(
                    root,
                    BTreeMap::from([("notes/main".into(), created.doc_id)])
                )
                .is_none()
        );
    }
    owner
        .core()
        .write_roster(
            &context,
            &drawer,
            None,
            BTreeMap::from([(
                meta_doc_key("notes", "forged").unwrap(),
                json!({"docId": ordinary.to_string()}),
            )]),
            BTreeSet::new(),
        )
        .await
        .unwrap();
    let descriptor = owner
        .core()
        .state
        .drawer(&drawer)
        .await
        .unwrap()
        .unwrap()
        .descriptor;
    owner
        .repo
        .receive_keyhive_contact_card(&relay.repo.local_keyhive_contact_card())
        .await
        .unwrap();
    owner
        .invite(
            &alice,
            &drawer,
            Principal::Peer {
                peer_id: GroupKey(relay.repo.local_peer_id().to_bytes32().unwrap()),
            },
            Access::Relay,
        )
        .await
        .unwrap();
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    relay.repo.wait_for_keyhive_reconciliation().await.unwrap();
    let (peers, peer_rx) = tokio::sync::watch::channel(HashSet::from([owner.repo.local_peer_id()]));
    let demand = tokio::spawn(relay.repo.drive_sync_demand(&relay_net.worker, peer_rx));
    let mirror = relay.repo.interest(drawer.clone()).await.unwrap();
    mirror.wait_until_ready().await.unwrap();
    assert!(relay.core().state.drawer(&drawer).await.unwrap().is_none());
    let prior = BTreeMap::from([(MetaRoot::Sensitive, metadata[&MetaRoot::Sensitive].clone())]);
    relay
        .core()
        .state
        .replace_roster(&drawer, &BTreeMap::new(), &BTreeMap::new(), &prior)
        .await
        .unwrap();
    let observer: DrawerService = relay.drawers.as_ref().unwrap().metadata_observer();
    let observation = observer.observe_metadata(&context, &drawer).await.unwrap();
    assert_eq!(
        observation,
        DrawerMetadataObservation {
            drawer_id: drawer.clone(),
            descriptor: descriptor.clone(),
            children: BTreeMap::from([(child.clone(), Access::Relay)]),
            meta_docs: BTreeMap::from([(MetaRoot::Public, metadata[&MetaRoot::Public].clone())]),
            roots: BTreeMap::from([
                (
                    MetaRoot::Public,
                    MetadataRootStatus::Pending {
                        reason: "unadmitted metadata slot notes/forged on Public".into(),
                    },
                ),
                (MetaRoot::Sensitive, MetadataRootStatus::Denied),
            ]),
        }
    );
    assert_eq!(
        relay
            .core()
            .state
            .drawer(&drawer)
            .await
            .unwrap()
            .unwrap()
            .descriptor,
        descriptor
    );
    assert_eq!(
        relay.core().state.meta_docs_of(&drawer).await.unwrap(),
        metadata
    );
    assert_eq!(
        relay.core().state.children_of(&drawer).await.unwrap(),
        observation.children
    );
    let moved = observer.clone();
    let selected = drawer.clone();
    let repeated = tokio::spawn(async move {
        moved
            .observe_metadata(&RequestContext::root(), &selected)
            .await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(repeated, observation);
    assert!(relay.core().state.drawer(&child).await.unwrap().is_none());
    assert_eq!(
        relay.core().state.docs_of(&child).await.unwrap(),
        BTreeMap::new()
    );
    drop(mirror);
    drop(peers);
    demand.await.unwrap().unwrap();
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    relay_net.stop().await;
    owner_net.stop().await;
    relay.stop().await;
    owner.stop().await;
}

#[tokio::test]
async fn native_metadata_observation_projects_authoritative_empty_and_ignores_self_doc_pointers() {
    let node = Node::boot().await;
    let context = RequestContext::root();
    let alice = node.creator("alice").await;
    let drawer = node.collection(&alice, "observed drawer").await;
    let capability = node.drawer_capability(&alice, &[&drawer]).await.unwrap();
    for root in [MetaRoot::Public, MetaRoot::Sensitive] {
        node.drawers()
            .call(
                context.child(),
                CreateMetaDoc {
                    capability,
                    command: command(&format!("observed {root:?}")),
                    drawer_id: drawer.clone(),
                    kind: "notes".into(),
                    name: "main".into(),
                    root,
                },
            )
            .await
            .unwrap();
    }
    let service = DrawerService {
        core: Arc::clone(node.core()),
    };
    let before = service.observe_metadata(&context, &drawer).await.unwrap();
    assert_eq!(
        before.roots,
        BTreeMap::from([
            (MetaRoot::Public, MetadataRootStatus::Authoritative),
            (MetaRoot::Sensitive, MetadataRootStatus::Authoritative),
        ])
    );
    assert_eq!(before.meta_docs.len(), 2);
    let self_doc = node.self_doc(&alice, &drawer, "self doc").await;
    node.core()
        .write_roster(
            &context,
            &self_doc,
            None,
            BTreeMap::from([(
                meta_doc_key("notes", "main").unwrap(),
                json!({"docId": before.meta_docs[&MetaRoot::Public]["notes/main"].to_string()}),
            )]),
            BTreeSet::new(),
        )
        .await
        .unwrap();
    node.core()
        .state
        .replace_roster(
            &self_doc,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &before.meta_docs,
        )
        .await
        .unwrap();
    assert_eq!(
        service.observe_metadata(&context, &self_doc).await.unwrap(),
        DrawerMetadataObservation {
            drawer_id: self_doc.clone(),
            descriptor: Drawer::self_doc(),
            children: BTreeMap::new(),
            meta_docs: BTreeMap::new(),
            roots: BTreeMap::new(),
        }
    );
    assert_eq!(
        node.core().state.meta_docs_of(&self_doc).await.unwrap(),
        BTreeMap::new()
    );
    assert_eq!(
        node.core().state.meta_docs_of(&drawer).await.unwrap(),
        before.meta_docs
    );
    for root in [&drawer, &before.descriptor.sensitive_root().unwrap()] {
        node.core()
            .write_roster(
                &context,
                root,
                None,
                BTreeMap::new(),
                BTreeSet::from([meta_doc_key("notes", "main").unwrap()]),
            )
            .await
            .unwrap();
    }
    let after = service.observe_metadata(&context, &drawer).await.unwrap();
    assert_eq!(
        after,
        DrawerMetadataObservation {
            meta_docs: BTreeMap::new(),
            ..before
        }
    );
    assert_eq!(
        node.core().state.meta_docs_of(&drawer).await.unwrap(),
        BTreeMap::new()
    );
    node.stop().await;
}

#[tokio::test]
async fn native_metadata_observation_preserves_slots_until_backing_delegation_arrives() {
    let node = Node::boot().await;
    let context = RequestContext::root();
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let drawer = node.collection(&alice, "delayed metadata").await;
    let source = node.collection(&alice, "metadata source").await;
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let capability = node.drawer_capability(&alice, &[&drawer]).await.unwrap();
    let observer: DrawerService = node.drawers.as_ref().unwrap().metadata_observer();
    for root in [MetaRoot::Public, MetaRoot::Sensitive] {
        for name in ["main", "removed"] {
            node.drawers()
                .call(
                    context.child(),
                    CreateMetaDoc {
                        capability,
                        command: command(&format!("prior metadata {root:?} {name}")),
                        drawer_id: drawer.clone(),
                        kind: "notes".into(),
                        name: name.into(),
                        root,
                    },
                )
                .await
                .unwrap();
        }
        let before = observer.observe_metadata(&context, &drawer).await.unwrap();
        assert_eq!(before.roots[&root], MetadataRootStatus::Authoritative);
        let prior = node.core().state.meta_docs_of(&drawer).await.unwrap();
        let target = node
            .create_doc(&alice, &source, &format!("delayed {root:?}"))
            .await
            .unwrap();
        let groups = before.descriptor.groups().unwrap();
        let (root_doc, group) = match root {
            MetaRoot::Public => (drawer.clone(), groups.meta_group),
            MetaRoot::Sensitive => (
                before.descriptor.sensitive_root().unwrap(),
                groups.sensitive_group,
            ),
        };
        let admitted_target = node
            .create_doc(&alice, &source, &format!("admitted {root:?}"))
            .await
            .unwrap();
        node.repo
            .grant_doc_access(
                admitted_target.clone(),
                node.core().group(group).await.unwrap(),
                KeyhiveAccess::Admin,
            )
            .await
            .unwrap();
        let admitted = BTreeMap::from([("notes/added".to_owned(), admitted_target.clone())]);
        let mut partial = prior.clone();
        partial.get_mut(&root).unwrap().extend(admitted.clone());
        node.core()
            .write_roster(
                &context,
                &root_doc,
                None,
                BTreeMap::from([
                    (
                        meta_doc_key("notes", "main").unwrap(),
                        json!({"docId": target.to_string()}),
                    ),
                    (
                        meta_doc_key("notes", "added").unwrap(),
                        json!({"docId": admitted_target.to_string()}),
                    ),
                ]),
                BTreeSet::from([meta_doc_key("notes", "removed").unwrap()]),
            )
            .await
            .unwrap();
        for delegation in [None, Some(KeyhiveAccess::Read)] {
            if let Some(access) = delegation {
                node.repo
                    .grant_doc_access(
                        target.clone(),
                        node.core().group(group).await.unwrap(),
                        access,
                    )
                    .await
                    .unwrap();
            }
            let pending = observer.observe_metadata(&context, &drawer).await.unwrap();
            assert_eq!(
                pending.roots[&root],
                MetadataRootStatus::Pending {
                    reason: format!("unadmitted metadata slot notes/main on {root:?}"),
                }
            );
            assert_eq!(pending.meta_docs[&root], admitted);
            assert_eq!(
                node.core().state.meta_docs_of(&drawer).await.unwrap(),
                partial
            );
            assert_eq!(
                node.read_note(&bob, &target).await.unwrap_err(),
                DocsError::Denied
            );
            assert_eq!(
                node.read_note(&bob, &admitted_target).await.unwrap(),
                json!({"$type": NOTE, "value": format!("admitted {root:?}")})
            );
        }
        node.repo
            .grant_doc_access(
                target.clone(),
                node.core().group(group).await.unwrap(),
                KeyhiveAccess::Admin,
            )
            .await
            .unwrap();
        let resolved = observer.observe_metadata(&context, &drawer).await.unwrap();
        assert_eq!(resolved.roots[&root], MetadataRootStatus::Authoritative);
        let mut current = admitted;
        current.insert("notes/main".to_owned(), target.clone());
        assert_eq!(resolved.meta_docs[&root], current);
        let mut expected = prior;
        expected.insert(root, current);
        assert_eq!(
            node.core().state.meta_docs_of(&drawer).await.unwrap(),
            expected
        );
        assert_eq!(
            node.read_note(&bob, &target).await.unwrap(),
            json!({"$type": NOTE, "value": format!("delayed {root:?}")})
        );
    }
    node.stop().await;
}
