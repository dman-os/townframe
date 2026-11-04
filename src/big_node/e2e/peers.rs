//! The peers attachment end to end: two nodes that know each other only through their peers
//! services and the default policy (ADR 019 mirror intent, bounded by keyhive access).

use super::blob_discovery::collection;
use super::blob_discovery::invite;
use super::*;
use crate::peers::PeersConfig;
use crate::peers::PeersEvent;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cabinet_core::drawer::protocol::SetDrawerPolicy;
use big_sync_core::PeerKey;
use tokio::sync::broadcast::Receiver;

async fn node(
    seed: u8,
    secrets: Arc<SecretStore>,
    runtime: &Arc<SqliteRuntime>,
    peers: PeersConfig,
) -> (BigNode, StopToken) {
    let mut cfg = config(
        secrets,
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(runtime).unwrap(),
    );
    cfg.repo.node_identity_seed = [seed; 32];
    cfg.peers = Some(peers);
    BigNode::boot(cfg).await.unwrap()
}

async fn create_in(node: &BigNode, drawer: &DocId, command: &str, text: &str) -> DocId {
    let placement = format!("drawer {drawer}");
    let capability = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    node.docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from(command.to_owned()).unwrap(),
                placement,
                datums: BTreeMap::from([(key(), json!({ "$type": NOTE, "value": text }))]),
            },
        )
        .await
        .unwrap()
        .doc_id
}

async fn registry_capability(node: &BigNode) -> CapId {
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::from([(node.drawers.local().registry_id, Access::Admin)]),
                    create: false,
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

async fn remember(node: &BigNode, drawer: &DocId, mirror: bool) {
    node.drawers
        .client
        .call(
            RequestContext::root(),
            RememberDrawer {
                capability: registry_capability(node).await,
                drawer_id: drawer.clone(),
                mirror,
            },
        )
        .await
        .unwrap();
}

async fn doc_synced(events: &mut Receiver<PeersEvent>, peer: &PeerKey, doc: &DocId) {
    loop {
        match events.recv().await {
            Ok(PeersEvent::DocSynced {
                peer: found,
                doc: synced,
            }) if &found == peer && &synced == doc => {
                return;
            }
            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                panic!("the peers service stopped")
            }
        }
    }
}

/// One key is the keyhive identity, the Subduction signer and the Iroh endpoint's secret key.
#[tokio::test(flavor = "multi_thread")]
async fn the_endpoint_key_is_the_keyhive_identity() {
    utils_rs::testing::setup_tracing_once();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (node, stop) = node(
        160,
        secrets().await,
        &runtime,
        PeersConfig {
            loopback_only: true,
            ..Default::default()
        },
    )
    .await;
    let peers = node.peers.as_ref().expect("peers were configured");
    assert_eq!(
        peers.addr().id.as_bytes(),
        &node.repo.local_peer_id().to_bytes32().unwrap()
    );
    shutdown(node, stop).await;
}

/// A node with no network config has no peers attachment.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_network_config_has_no_peers() {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let cfg = config(
        secrets().await,
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    );
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    assert!(node.peers.is_none());
    shutdown(node, stop).await;
}

/// Both nodes hold read access to both drawers; only the drawer the reader mirrors replicates,
/// so what the reader lacks is the policy's doing, not access's. Flipping the intent replicates
/// the other.
#[tokio::test(flavor = "multi_thread")]
async fn default_policy_replicates_mirrored_drawers_only() {
    utils_rs::testing::setup_tracing_once();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let peers = || PeersConfig {
        loopback_only: true,
        sweep_interval: std::time::Duration::from_millis(200),
        ..Default::default()
    };
    let (owner, owner_stop) = node(161, secrets().await, &runtime, peers()).await;
    let (reader, reader_stop) = node(162, secrets().await, &runtime, peers()).await;

    let mirrored = collection(&owner, "mirrored").await;
    let unmirrored = collection(&owner, "unmirrored").await;
    let mirrored_doc = create_in(&owner, &mirrored, "doc-in-mirrored", "mirrored").await;
    let unmirrored_doc = create_in(&owner, &unmirrored, "doc-in-unmirrored", "unmirrored").await;
    invite(&owner, &reader, &mirrored, Access::Read).await;
    invite(&owner, &reader, &unmirrored, Access::Read).await;
    remember(&reader, &mirrored, true).await;
    remember(&reader, &unmirrored, false).await;

    let owner_key = owner.repo.local_peer_id();
    let mut events = reader.peers.as_ref().unwrap().subscribe();
    let link = network::link(&owner, &reader).await;
    link.sync_keyhive(&owner, &reader).await;

    doc_synced(&mut events, &owner_key, &mirrored_doc).await;
    assert!(
        reader
            .repo
            .contains_sedimentree_id(mirrored_doc)
            .await
            .unwrap()
    );
    assert!(
        !reader
            .repo
            .contains_sedimentree_id(unmirrored_doc.clone())
            .await
            .unwrap(),
        "a drawer the reader does not mirror must not replicate, access or not"
    );

    reader
        .drawers
        .client
        .call(
            RequestContext::root(),
            SetDrawerPolicy {
                capability: registry_capability(&reader).await,
                drawer_id: unmirrored.clone(),
                mirror: true,
            },
        )
        .await
        .unwrap();
    doc_synced(&mut events, &owner_key, &unmirrored_doc).await;
    assert!(
        reader
            .repo
            .contains_sedimentree_id(unmirrored_doc)
            .await
            .unwrap()
    );

    shutdown(reader, reader_stop).await;
    shutdown(owner, owner_stop).await;
}
