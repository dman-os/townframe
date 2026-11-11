//! Joins two e2e nodes through their peers attachments (`big_node::peers`), the way an embedder
//! would, instead of wiring endpoints by hand.

use super::*;
use crate::peers::PeersConfig;
use big_sync_core::PeerKey;

/// The network attachment of an e2e node: loopback only, the default mirroring policy and no
/// dial list, so nodes meet only when a test links them.
pub(crate) fn peers() -> Option<PeersConfig> {
    Some(PeersConfig {
        loopback_only: true,
        ..Default::default()
    })
}

/// A connection `source` opened to `puller`. Both nodes register the other; the puller's workers
/// pull from the source, as the hand-wired helper this replaces arranged.
pub(crate) struct Link {
    pub source: PeerKey,
    pub puller: PeerKey,
}

pub(crate) async fn link(source: &BigNode, puller: &BigNode) -> Link {
    let source_peers = source
        .peers
        .as_ref()
        .expect("the source has a peers attachment");
    let puller_peers = puller
        .peers
        .as_ref()
        .expect("the puller has a peers attachment");
    let source_key = source.repo.local_peer_id();
    let puller_key = puller.repo.local_peer_id();
    // A connection the source still holds for a puller that went away is replaced only once its
    // end is seen; dialing before that would be answered by the stale one.
    source_peers.wait_disconnected(&puller_key).await;
    let mut events = puller_peers.subscribe();
    let connected = source_peers.connect(puller_peers.addr()).await.unwrap();
    assert_eq!(connected, puller_key);
    if !puller_peers
        .connected()
        .await
        .unwrap()
        .contains(&source_key)
    {
        loop {
            if let crate::peers::PeersEvent::Connected { peer, .. } = events.recv().await.unwrap()
                && peer == source_key
            {
                break;
            }
        }
    }
    Link {
        source: source_key,
        puller: puller_key,
    }
}

impl Link {
    /// One keyhive round each way, so both sides hold the other's membership changes.
    pub(crate) async fn sync_keyhive(&self, source: &BigNode, puller: &BigNode) {
        source
            .repo
            .sync_keyhive_with_peer(self.puller.clone())
            .await
            .unwrap();
        puller
            .repo
            .sync_keyhive_with_peer(self.source.clone())
            .await
            .unwrap();
    }
}
