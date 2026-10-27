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
        .wait_indexed(
            big_cabinet_core::drawer::DRAWER_DATUM_TAG,
            |route, membership| {
                route.doc_id == drawer && matches!(membership, DatumMembership::Present { .. })
            },
        )
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
    let mut observations = reader
        .docs()
        .subscribe(
            RequestContext::root(),
            ObserveDatums {
                capability,
                scope: ObserveScope::Doc {
                    doc_id: doc.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    let (key, _) = note("");
    loop {
        match observations.next().await.unwrap().unwrap() {
            DatumObserved::Entry {
                doc_id,
                datum_key,
                datum: ObservedDatum::Present { .. },
                ..
            } if doc_id == doc && datum_key == key => break,
            DatumObserved::Entry { .. } | DatumObserved::ReplayComplete { .. } => {}
        }
    }
    let expected = json!({"$type": NOTE, "value": "shared-note"});
    assert_eq!(owner.read_note(&alice, &doc).await.unwrap(), expected);
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
