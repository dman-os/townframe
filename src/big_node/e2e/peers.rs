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
                    drawers: BTreeMap::from([(
                        node.drawers.local().known_drawers_id,
                        Access::Admin,
                    )]),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
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

/// The node info operation answers through dispatch with the node's id, and, with a peers
/// attachment, its dialable address and the peers it is connected to: the facts a client of a
/// daemon cannot read in process. A node that dials the other's ticket shows it as connected.
#[tokio::test(flavor = "multi_thread")]
async fn node_info_reports_the_id_the_address_and_the_connected_peers() {
    utils_rs::testing::setup_tracing_once();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let peers = || PeersConfig {
        loopback_only: true,
        ..Default::default()
    };
    let (dialed, dialed_stop) = node(163, secrets().await, &runtime, peers()).await;
    let (dialer, dialer_stop) = node(164, secrets().await, &runtime, peers()).await;
    let bare = {
        let cfg = config(
            secrets().await,
            runtime.ephemeral(&[]).unwrap(),
            big_repo::Storage::memory(&runtime).unwrap(),
        );
        BigNode::boot(cfg).await.unwrap()
    };

    async fn info(node: &BigNode) -> crate::NodeInfo {
        let output = node
            .dispatch
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(node.authority.root).metadata(),
                big_dispatch_core::Invoke::json(
                    crate::GET_NODE_INFO,
                    json!({}),
                    Default::default(),
                ),
            )
            .await
            .unwrap();
        serde_json::from_value(output.body.into_json()).unwrap()
    }

    let before = info(&dialer).await;
    assert_eq!(before.node_id, crate::node_id(&[164; 32]));
    assert_eq!(before.node_id, dialer.node_id);
    assert_eq!(
        before.peers.as_ref().unwrap().connected,
        Vec::<String>::new()
    );

    let dialed_info = info(&dialed).await;
    let ticket = dialed_info.peers.as_ref().unwrap().address.clone();
    assert_eq!(
        crate::peers::parse_ticket(&ticket).unwrap(),
        dialed.peers.as_ref().unwrap().addr()
    );
    let connected = dialer
        .peers
        .as_ref()
        .unwrap()
        .connect(crate::peers::parse_ticket(&ticket).unwrap())
        .await
        .unwrap();
    assert_eq!(connected, dialed.repo.local_peer_id());
    assert_eq!(
        info(&dialer).await.peers.unwrap().connected,
        vec![dialed_info.node_id.clone()]
    );

    // No peers attachment: the node still says who it is.
    let (bare_node, bare_stop) = bare;
    let bare_info = info(&bare_node).await;
    assert_eq!(bare_info.node_id, crate::node_id(&[47; 32]));
    assert!(bare_info.peers.is_none());

    shutdown(bare_node, bare_stop).await;
    shutdown(dialer, dialer_stop).await;
    shutdown(dialed, dialed_stop).await;
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

/// Wants whatever the test last set, from every peer, and dials nobody. Wanting the groups
/// directly (rather than through the mirror intent) means they are known before keyhive has
/// admitted anyone into them, which is the window the access stream closes.
struct Scripted {
    wanted: std::sync::Mutex<big_cabinet::drawer::Mirrored>,
    changes: tokio::sync::watch::Sender<u64>,
}

#[async_trait::async_trait]
impl crate::peers::PeerPolicy for Scripted {
    async fn dial(&self, _node: &crate::peers::PolicyContext) -> Vec<iroh::EndpointAddr> {
        Vec::new()
    }

    async fn accept(&self, _node: &crate::peers::PolicyContext, _peer: &PeerKey) -> bool {
        true
    }

    async fn wanted(
        &self,
        _node: &crate::peers::PolicyContext,
        _peer: &PeerKey,
    ) -> utils_rs::prelude::Res<big_cabinet::drawer::Mirrored> {
        Ok(self.wanted.lock().unwrap().clone())
    }

    fn changes(&self, _node: &crate::peers::PolicyContext) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }
}

/// A grant that lands after the connection is acted on when keyhive admits it. The reader already
/// wants the drawer's groups but the owner is not yet a member of them in the reader's keyhive;
/// the sweep is set to an hour, so the only thing that can make the reader subscribe to the
/// groups once the grant is admitted is the access stream. Before it was consumed this waited
/// for the sweep tick.
#[tokio::test(flavor = "multi_thread")]
async fn a_grant_after_connecting_replicates_without_waiting_for_a_sweep() {
    utils_rs::testing::setup_tracing_once();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let scripted = Arc::new(Scripted {
        wanted: Default::default(),
        changes: tokio::sync::watch::channel(0).0,
    });
    let (owner, owner_stop) = node(
        163,
        secrets().await,
        &runtime,
        PeersConfig {
            loopback_only: true,
            ..Default::default()
        },
    )
    .await;
    let (reader, reader_stop) = node(
        164,
        secrets().await,
        &runtime,
        PeersConfig {
            loopback_only: true,
            policy: crate::peers::PolicyChoice::Custom(Arc::clone(&scripted) as _),
            sweep_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
    )
    .await;

    let owner_key = owner.repo.local_peer_id();
    let mut events = reader.peers.as_ref().unwrap().subscribe();
    let link = network::link(&owner, &reader).await;
    link.sync_keyhive(&owner, &reader).await;

    let drawer = collection(&owner, "granted-late").await;
    let doc = create_in(&owner, &drawer, "doc-in-granted-late", "granted late").await;
    remember(&owner, &drawer, true).await;
    let mut wanted = owner
        .drawers
        .mirrored(&RequestContext::root())
        .await
        .unwrap();
    assert!(!wanted.groups.is_empty());
    // Only the groups: a wanted document arriving also re-applies the parts, which would stand
    // in for the access stream.
    wanted.docs.clear();
    *scripted.wanted.lock().unwrap() = wanted;
    scripted.changes.send_modify(|revision| *revision += 1);

    invite(&owner, &reader, &drawer, Access::Read).await;
    link.sync_keyhive(&owner, &reader).await;

    doc_synced(&mut events, &owner_key, &doc).await;
    assert!(reader.repo.contains_sedimentree_id(doc).await.unwrap());

    shutdown(reader, reader_stop).await;
    shutdown(owner, owner_stop).await;
}
