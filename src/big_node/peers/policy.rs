//! The peer policy hook (ADR 033 section 6): the decisions `big_node` orchestrates but does not own.
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
use big_cabinet_core::DocId;
use big_cabinet_core::drawer::GroupKey;
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

/// The default policy (ADR 019 and ADR 033 section 6): dial a fixed list, accept every peer
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

/// The relay policy (ADR 038 section 3): accept every peer the transport authenticates, dial a
/// fixed list (a relay may have none) and want everything this node is a member of.
///
/// It is membership-driven, not descriptor-driven, because a relay mirrors what keyhive says it
/// is a member of and never learns a drawer's descriptor (it cannot read it), so the known-drawers
/// intent [`MirrorPolicy`] consults has nothing to name.
///
/// Every keyhive access level counts, `Relay` (the lowest) included: membership at any level is
/// what [`BigKeyhiveHandle::membered_for_agent`](big_repo::BigKeyhiveHandle::membered_for_agent)
/// reports, and it is the level that decides what a member may read, not whether it syncs. The
/// access half in `big_node` still bounds the result by what each peer may have.
///
/// A membership change reaches the node as a keyhive access delta, which `big_node` already
/// re-applies every connected peer's parts for, so the policy has no change signal of its own
/// and [`PeerPolicy::changes`] never fires.
#[derive(Debug)]
pub struct RelayPolicy {
    dial: Vec<iroh::EndpointAddr>,
    never: tokio::sync::watch::Sender<u64>,
}

impl RelayPolicy {
    pub fn new(dial: Vec<iroh::EndpointAddr>) -> Self {
        Self {
            dial,
            never: tokio::sync::watch::channel(0).0,
        }
    }
}

#[async_trait]
impl PeerPolicy for RelayPolicy {
    async fn dial(&self, _node: &PolicyContext) -> Vec<iroh::EndpointAddr> {
        self.dial.clone()
    }

    async fn accept(&self, _node: &PolicyContext, _peer: &PeerKey) -> bool {
        true
    }

    async fn wanted(&self, node: &PolicyContext, _peer: &PeerKey) -> Res<Mirrored> {
        let local = node.repo.local_keyhive_agent().await?;
        let held = node.repo.keyhive().membered_for_agent(&local.id()).await;
        // The answer mixes groups and documents by id; asking for each id as both is harmless,
        // since only the kind the id really is has a part or an object behind it.
        Ok(Mirrored {
            groups: held.keys().map(|id| GroupKey(*id)).collect(),
            docs: held.keys().map(|id| DocId::new(*id)).collect(),
        })
    }

    fn changes(&self, _node: &PolicyContext) -> tokio::sync::watch::Receiver<u64> {
        self.never.subscribe()
    }
}
