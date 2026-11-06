//! The peer policy hook (ADR 032 section 6): the decisions `big_node` orchestrates but does not own.
//!
//! The hook answers three questions, and nothing else: who to dial, whether to accept a peer,
//! and what this node *wants* from a peer. The last is the mirroring half of "which parts to sync
//! with it". `big_node` always bounds it with the access half: it asks BigRepo which of the wanted
//! groups the peer is a member of (what the peer may have) and subscribes to those only. A
//! policy therefore cannot widen sync beyond keyhive access, and a relay swaps the mirroring half
//! for sponsorship by implementing [`PeerPolicy::wanted`] without touching access.

use async_trait::async_trait;
use big_cabinet::Drawers;
use big_cabinet::drawer::Mirrored;
use big_repo::SharedBigRepo;
use big_sync_core::PeerKey;
use service_utils_rs::RequestContext;
use utils_rs::prelude::Res;

/// What a policy may consult. Handed to every call so a policy built before the node boots
/// (it is part of the config) can still read the node's own state.
#[derive(Clone)]
pub struct PolicyContext {
    pub drawers: Drawers,
    pub repo: SharedBigRepo,
}

impl PolicyContext {
    /// This node's own mirror intent (ADR 019): the known-drawers list's `mirror` entries.
    pub async fn mirrored(&self) -> Res<Mirrored> {
        Ok(self.drawers.mirrored(&RequestContext::root()).await?)
    }
}

#[async_trait]
pub trait PeerPolicy: Send + Sync + 'static {
    /// The peers to keep a connection to. Asked at boot, whenever [`PeerPolicy::changes`] fires,
    /// after a connection ends and on the retry schedule; a target that is already connected or
    /// backing off is skipped, so this answers "who", not "when".
    async fn dial(&self, node: &PolicyContext) -> Vec<iroh::EndpointAddr>;

    /// Whether to take an incoming connection from `peer`. Asked before the connection is handed
    /// to the repo, with the identity the transport proved (the peer's node key).
    async fn accept(&self, node: &PolicyContext, peer: &PeerKey) -> bool;

    /// What this node wants from `peer`. Bounded by keyhive access afterwards, never widened.
    async fn wanted(&self, node: &PolicyContext, peer: &PeerKey) -> Res<Mirrored>;

    /// Fires when any answer above may have changed. `big_node` re-asks and re-applies the
    /// parts of every connected peer.
    fn changes(&self, node: &PolicyContext) -> tokio::sync::watch::Receiver<u64>;
}

/// The default policy (ADR 019 and ADR 032 section 6): dial a fixed list, accept every peer
/// the transport authenticates (access decides what they get, not whether they may connect), and
/// want what this node's drawer mirror intent names.
#[derive(Debug, Clone, Default)]
pub struct MirrorPolicy {
    pub dial: Vec<iroh::EndpointAddr>,
}

#[async_trait]
impl PeerPolicy for MirrorPolicy {
    async fn dial(&self, _node: &PolicyContext) -> Vec<iroh::EndpointAddr> {
        self.dial.clone()
    }

    async fn accept(&self, _node: &PolicyContext, _peer: &PeerKey) -> bool {
        true
    }

    async fn wanted(&self, node: &PolicyContext, _peer: &PeerKey) -> Res<Mirrored> {
        node.mirrored().await
    }

    fn changes(&self, node: &PolicyContext) -> tokio::sync::watch::Receiver<u64> {
        node.drawers.subscribe_changes()
    }
}
