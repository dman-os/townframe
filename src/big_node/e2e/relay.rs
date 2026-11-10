//! A relay-access member of a drawer (ADR 038): it stores what it is sent and cannot read it.
//! These tests pin what the node and keyhive do for a member that holds `Access::Relay`, under [`RelayPolicy`].

use super::harness::Cluster;
use super::harness::Topology;
use super::*;
use crate::peers::PolicyChoice;
use crate::peers::RelayPolicy;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::Principal;
use service_utils_rs::ClientError;

/// The node nicknamed `relay` runs the relay policy; every other node the default one.
fn relay_policy(nickname: &'static str) -> PolicyChoice {
    match nickname {
        "relay" => PolicyChoice::Custom(std::sync::Arc::new(RelayPolicy::new(Vec::new()))),
        _ => PolicyChoice::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_access_member_holds_a_drawer_it_cannot_read() {
    let mut cluster = Cluster::boot_with(&["dev", "relay"], relay_policy).await;
    // dev dials the relay, and a keyhive round runs across the link. The test hands over no
    // contact card: the invite below resolves the relay's agent from what the connection
    // carried.
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[]).await;
    let note = cluster
        .create_note(0, &drawer, "for the relay's eyes only")
        .await;

    let dev = cluster.node(0);
    let relay = cluster.node(1);
    dev.drawers
        .client
        .call(
            RequestContext::root(),
            InvitePrincipal {
                capability: super::blobs::drawer_cap(dev, &drawer.id).await,
                drawer_id: drawer.id.clone(),
                principal: Principal::Peer {
                    peer_id: GroupKey(relay.repo.local_peer_id().to_bytes32().unwrap()),
                },
                access: Access::Relay,
            },
        )
        .await
        .unwrap();
    cluster.sync_keyhive().await;

    cluster
        .until(async |cluster| {
            cluster
                .node(1)
                .repo
                .contains_sedimentree_id(note.clone())
                .await
                .unwrap()
        })
        .await;

    let capability = capability(
        cluster.node(1),
        DocScope {
            read: BTreeSet::from([note.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let read = cluster
        .node(1)
        .docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id: note.clone(),
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        )
        .await;
    // The relay is not refused: it holds the document's ciphertext but has no key for its
    // entries, so materializing never completes and a read stays pending.
    assert!(
        matches!(read, Err(ClientError::Method(DocsError::Pending { .. }))),
        "{read:?}"
    );
    cluster.stop_all().await;
}

/// The phone is linked only to the relay, and dev is stopped before the phone reads: the note and
/// the phone's own membership both reach it through the relay.
#[tokio::test(flavor = "multi_thread")]
async fn a_phone_reads_a_drawer_through_a_relay_while_the_dev_node_is_offline() {
    let mut cluster = Cluster::boot_with(&["dev", "relay", "phone"], relay_policy).await;
    cluster.connect(Topology::Line).await;
    let drawer = cluster.share_drawer(0, &[]).await;
    let note = cluster
        .create_note(0, &drawer, "written on the dev machine")
        .await;

    let dev = cluster.node(0);
    dev.drawers
        .client
        .call(
            RequestContext::root(),
            InvitePrincipal {
                capability: super::blobs::drawer_cap(dev, &drawer.id).await,
                drawer_id: drawer.id.clone(),
                principal: Principal::Peer {
                    peer_id: GroupKey(cluster.node(1).repo.local_peer_id().to_bytes32().unwrap()),
                },
                access: Access::Relay,
            },
        )
        .await
        .unwrap();
    cluster.admit(0, 2, &drawer.id, Access::Read).await;

    // The relay holds the note's bytes before dev goes away.
    cluster
        .until(async |cluster| {
            cluster
                .node(1)
                .repo
                .contains_sedimentree_id(note.clone())
                .await
                .unwrap()
        })
        .await;
    tracing::info!("INSTR relay-test pre-stop extra keyhive round"); // INSTR-TMP
    cluster.sync_keyhive().await; // INSTR-TMP
    tracing::info!("INSTR relay-test extra round done"); // INSTR-TMP
    cluster.stop(0).await;

    cluster
        .until(async |cluster| cluster.read_note(2, &note).await.is_some())
        .await;
    assert_eq!(
        cluster
            .read_note(2, &note)
            .await
            .unwrap()
            .text("main")
            .as_deref(),
        Some("written on the dev machine")
    );
    cluster.stop_all().await;
}
