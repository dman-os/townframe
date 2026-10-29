use super::*;
use crate::peer_net::Network;
use crate::peer_net::link;
use futures::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;

#[tokio::test(flavor = "multi_thread")]
async fn observation_fetches_remote_document_without_manual_document_sync() {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot_with_seed(81).await;
    let reader = Node::boot_with_seed(82).await;
    let owner_net = Network::boot(&owner.repo, 81).await;
    let mut reader_net = Network::boot(&reader.repo, 82).await;
    let (connection, accepted) =
        link((&owner.repo, &owner_net), (&reader.repo, &mut reader_net)).await;
    let writer = owner.writer("owner").await;
    let created = owner
        .create(
            &writer,
            "remote",
            [(key("main"), note("fetched by observation"))],
        )
        .await
        .unwrap();
    let remote_agent = owner
        .repo
        .receive_keyhive_contact_card(&reader.repo.local_keyhive_contact_card())
        .await
        .unwrap();
    let group = owner
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    owner
        .repo
        .add_admin_member_to_group(remote_agent, &group)
        .await
        .unwrap();
    owner
        .repo
        .add_admin_member_to_doc(created.doc_id.clone(), group)
        .await
        .unwrap();
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    reader.repo.wait_for_keyhive_reconciliation().await.unwrap();
    assert!(
        !owner
            .repo
            .shared_part_store()
            .read_denied(
                big_sync::ReadTarget::Object(created.doc_id.clone()),
                reader.repo.local_peer_id(),
            )
            .await
            .unwrap()
    );
    assert!(
        !reader
            .repo
            .contains_sedimentree_id(created.doc_id.clone())
            .await
            .unwrap()
    );
    let principal = reader.writer("reader").await;
    reader
        .grant(
            &principal,
            LedgerAccess::object(AccessRequest::Read(&created.doc_id)),
        )
        .await;
    let capability = reader
        .capability(
            &principal,
            DocScope {
                read: BTreeSet::from([created.doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    let (peers, peer_rx) = tokio::sync::watch::channel(HashSet::from([owner.repo.local_peer_id()]));
    let mut subscription_events = reader_net.worker.subscribe_subscription_events();
    let mut demand_task = tokio::spawn(
        reader
            .repo
            .sync_demand()
            .drive(reader_net.worker.subscription_source(), peer_rx),
    );
    let mut observations = reader
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveDatums {
                capability,
                scope: ObserveScope::Doc {
                    doc_id: created.doc_id.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    loop {
        match observations.next().await.unwrap().unwrap() {
            DatumObserved::Entry {
                doc_id,
                datum_key,
                datum: ObservedDatum::Present { .. },
                ..
            } if doc_id == created.doc_id && datum_key == key("main") => break,
            DatumObserved::Entry { .. } | DatumObserved::ReplayComplete { .. } => {}
        }
    }
    let expected = owner
        .read_one(&writer, &created.doc_id, At::Current, &key("main"))
        .await;
    assert_eq!(
        reader
            .read_one(&principal, &created.doc_id, At::Current, &key("main"))
            .await,
        expected,
    );
    assert_eq!(
        reader_net.worker.snapshot().await.unwrap().peer_parts,
        HashMap::from([(owner.repo.local_peer_id(), HashMap::new())])
    );
    let mut demand = reader.repo.sync_demand();
    assert!(demand.documents().contains(&created.doc_id));
    drop(observations);
    assert!(demand.documents().contains(&created.doc_id));
    while demand.documents().contains(&created.doc_id) {
        demand.changed().await.unwrap();
    }
    let withdrawal = big_sync::SubscriptionEvent {
        peer_id: owner.repo.local_peer_id(),
        update: big_sync::SubscriptionUpdate {
            removed_objects: HashSet::from([created.doc_id.clone()]),
            ..Default::default()
        },
    };
    loop {
        let event = tokio::select! {
            event = subscription_events.recv() => event.unwrap(),
            result = &mut demand_task => panic!("demand driver stopped before withdrawal applied: {result:?}"),
        };
        if event == withdrawal {
            break;
        }
    }
    assert!(!demand_task.is_finished());
    assert!(!peers.is_closed());
    assert_eq!(
        reader_net.worker.snapshot().await.unwrap().peer_objects,
        HashMap::from([(owner.repo.local_peer_id(), HashMap::new())])
    );
    drop(peers);
    demand_task.await.unwrap().unwrap();
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    reader_net.stop().await;
    owner_net.stop().await;
    reader.stop().await;
    owner.stop().await;
}
