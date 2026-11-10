//! The node's own facts as a built-in operation: who it is and who it is connected to.
//!
//! There is no service behind it (no state to host, no client to hold), so it is a [`Provider`]
//! registered directly in dispatch's router, next to the providers that front the hosted
//! services. It is what lets `day status` and any other client learn the same facts over a
//! socket that an embedder reads in process through [`crate::BigNode::node_id`] and
//! [`crate::BigNode::peers`].

use crate::peers::Peers;
use async_trait::async_trait;
use big_cabinet_core::datum::operation::OperationKind;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch::Router;
use big_dispatch_core::InvocationContext;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::sync::Arc;

/// The NSID of the node info query. It takes no input.
pub const GET_NODE_INFO: &str = "org.example.daybook.node.getNodeInfo";

/// What the node says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    /// [`crate::node_id`]: lowercase hex.
    pub node_id: String,
    /// `None` when the node has no peers attachment, so it is neither dialable nor dialing.
    pub peers: Option<PeersInfo>,
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
    peers: Option<Peers>,
}

/// Registers the provider as `provider_id`, claiming [`GET_NODE_INFO`].
pub(crate) fn register(router: &Router, provider_id: &str, node_id: String, peers: Option<Peers>) {
    router.register(
        provider_id,
        [(GET_NODE_INFO.to_owned(), Some(OperationKind::Query))],
        ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(NodeInfoProvider {
            node_id,
            peers,
        }))),
    );
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
    ) -> Result<Value, ProviderError> {
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
        Ok(serde_json::to_value(NodeInfo {
            node_id: self.node_id.clone(),
            peers,
        })
        .expect("node info serializes"))
    }
}
