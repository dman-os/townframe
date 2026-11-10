//! A relay-access member of a drawer (ADR 038): it stores what it is sent and cannot read it.
//! The relay's peer policy is not written yet, so these tests pin what the node and keyhive do
//! today for a member that holds `Access::Relay`, with the one thing a relay policy would
//! supply (wanting the groups the node is a member of) stood in by [`Membership`].

use super::harness::Cluster;
use super::harness::Topology;
use super::*;
use crate::peers::PeerPolicy;
use crate::peers::PolicyChoice;
use crate::peers::PolicyContext;
use big_cabinet::drawer::Mirrored;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::Principal;
use big_sync_core::PeerKey;
use service_utils_rs::ClientError;
use std::sync::Arc;
use utils_rs::prelude::Res;

/// Accepts everyone and dials nobody, and wants every group and document its own keyhive says
/// this node is a member of at any level. The keyhive half still bounds it by what the peer
/// may have.
struct Membership {
    changes: tokio::sync::watch::Sender<u64>,
}

#[async_trait::async_trait]
impl PeerPolicy for Membership {
    async fn dial(&self, _node: &PolicyContext) -> Vec<iroh::EndpointAddr> {
        Vec::new()
    }

    async fn accept(&self, _node: &PolicyContext, _peer: &PeerKey) -> bool {
        true
    }

    async fn wanted(&self, node: &PolicyContext, _peer: &PeerKey) -> Res<Mirrored> {
        let local = node.repo.local_keyhive_agent().await?;
        let held = node.repo.keyhive().membered_for_agent(&local.id()).await;
        Ok(Mirrored {
            groups: held.keys().map(|id| GroupKey(*id)).collect(),
            docs: held.keys().map(|id| DocId::new(*id)).collect(),
        })
    }

    fn changes(&self, _node: &PolicyContext) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_access_member_holds_a_drawer_it_cannot_read() {
    let membership = Arc::new(Membership {
        changes: tokio::sync::watch::channel(0).0,
    });
    let mut cluster = Cluster::boot_with(&["dev", "relay"], |nickname| match nickname {
        "relay" => PolicyChoice::Custom(Arc::clone(&membership) as Arc<dyn PeerPolicy>),
        _ => PolicyChoice::default(),
    })
    .await;
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
    // The relay's wanted groups follow its own membership, which has just changed.
    membership.changes.send_modify(|revision| *revision += 1);

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
