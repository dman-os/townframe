//! The node's own facts as a built-in operation: who it is and who it is connected to.
//!
//! There is no service behind it (no state to host, no client to hold), so it is a [`Provider`]
//! registered directly in dispatch's router, next to the providers that front the hosted
//! services. It is what lets `day node status` and any other client learn the same facts over a
//! socket that an embedder reads in process through [`crate::BigNode::node_id`] and
//! [`crate::BigNode::peers`].

use crate::peers::Peers;
use async_trait::async_trait;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_dispatch::Caller;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch::Router;
use big_dispatch_core::Body;
use big_dispatch_core::InvocationContext;

use method_core::ContextError;
use method_core::Method;
use method_core::XrpcMethod;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::sync::Arc;

/// The NSID of the node info query. It takes no input.
pub const GET_NODE_INFO: &str = "org.example.daybook.node.getNodeInfo";

/// The node info query ([`GET_NODE_INFO`]). It takes no input. The node's facts publish no
/// Lexicon (there is no service behind them), so unlike the hosted services' methods there is no
/// schema projection here; the type exists so a client calls it as any other method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNodeInfo {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum NodeInfoError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    /// The peers service answered that it is stopped: the node is coming down under this call.
    #[error("the node is unavailable: {0}")]
    Unavailable(String),
}

impl Method for GetNodeInfo {
    type Output = NodeInfo;
    type Error = NodeInfoError;
    const XRPC: XrpcMethod = XrpcMethod::query(GET_NODE_INFO);
}

/// What the node says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    /// [`crate::node_id`]: lowercase hex.
    pub node_id: String,
    /// The drawers the node holds or uses itself. A drawer id is a name, not a capability: it
    /// grants nothing, but it is what a client needs to mint a handle on the drawer (the
    /// known-drawers one lists the drawers the node knows, `listKnownDrawers`, which is how a
    /// client finds the rest).
    pub drawers: NodeDrawers,
    /// `None` when the node has no peers attachment, so it is neither dialable nor dialing.
    pub peers: Option<PeersInfo>,
}

/// The node's own drawers, as the drawers service and the boot record them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDrawers {
    /// The drawer of every doc this node holds locally.
    pub present: DrawerId,
    /// The drawer of every doc this node created.
    pub originated: DrawerId,
    /// The drawers the node itself uses, in no meaningful order; each names its role.
    pub config: Vec<ConfigDrawer>,
}

impl NodeDrawers {
    /// The config drawer in `role`. Every role is reported on every node, so it is always there.
    pub fn config(&self, role: ConfigDrawerRole) -> &DrawerId {
        &self
            .config
            .iter()
            .find(|drawer| drawer.role == role)
            .expect("every config role is reported")
            .drawer_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigDrawer {
    pub role: ConfigDrawerRole,
    pub drawer_id: DrawerId,
}

/// What the node uses a config drawer for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfigDrawerRole {
    /// The list of drawers the node knows (`listKnownDrawers`).
    KnownDrawers,
    /// The nodule service's own config documents.
    NodeConfig,
    /// The cartridge registry's cartridges.
    CartridgeRegistry,
    /// Each cartridge's config document (ADR 044).
    CartridgeConfig,
}

impl ConfigDrawerRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KnownDrawers => "known drawers",
            Self::NodeConfig => "node config",
            Self::CartridgeRegistry => "cartridge registry",
            Self::CartridgeConfig => "cartridge config",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeersInfo {
    /// This node's dialable address as an `iroh_tickets` endpoint ticket: what another node
    /// puts in its bootstrap list to dial this one.
    pub address: String,
    /// The ids (lowercase hex) of the peers with a registered connection now, sorted.
    pub connected: Vec<String>,
}

struct NodeInfoProvider {
    node_id: String,
    drawers: NodeDrawers,
    peers: Option<Peers>,
}

/// Registers the provider as `provider_id`, claiming [`GET_NODE_INFO`].
pub(crate) fn register(
    router: &Router,
    provider_id: &str,
    node_id: String,
    drawers: NodeDrawers,
    peers: Option<Peers>,
) {
    // The node's facts publish no Lexicon (`GetNodeInfo` is a `Method` only for typed clients), so
    // calls are unchecked.
    router
        .register(
            provider_id,
            [(
                GET_NODE_INFO.to_owned(),
                Operation::unchecked(Some(OperationKind::Query)),
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(NodeInfoProvider {
                node_id,
                drawers,
                peers,
            }))),
        )
        .expect("a provider without a Lexicon compiles nothing");
}

#[async_trait]
impl Provider for NodeInfoProvider {
    async fn call(
        &self,
        _caller: &Caller,
        operation: &str,
        _request: &RequestContext,
        _context: &InvocationContext,
        _input: Value,
        _body: Option<Body>,
    ) -> Result<Body, ProviderError> {
        assert_eq!(
            operation, GET_NODE_INFO,
            "the router only resolves NSIDs this provider registered"
        );
        let peers = match &self.peers {
            None => None,
            Some(peers) => {
                // The service answering "stopped" means the node is coming down under this
                // call; that is the caller's to retry, not an invariant break.
                let connected = peers
                    .connected()
                    .await
                    .map_err(|error| ProviderError::Failed {
                        status: 503,
                        body: json!({ "error": "Unavailable", "message": format!("{error:#}") }),
                    })?;
                let mut connected: Vec<String> = connected
                    .iter()
                    .map(|peer| data_encoding::HEXLOWER.encode(peer.as_bytes()))
                    .collect();
                connected.sort();
                Some(PeersInfo {
                    address: peers.ticket(),
                    connected,
                })
            }
        };
        Ok(Body::Json(
            serde_json::to_value(NodeInfo {
                node_id: self.node_id.clone(),
                drawers: self.drawers.clone(),
                peers,
            })
            .expect("node info serializes"),
        ))
    }
}
