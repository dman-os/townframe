use crate::context::HttpMetadata;
use crate::context::RequestContext;
use axum::http::StatusCode;
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

pub use service_core_rs::xrpc::AUTHORIZATION_HEADER;
pub use service_core_rs::xrpc::DOMAIN_ERROR_FIELD;
pub use service_core_rs::xrpc::DOMAIN_ERROR_STATUS;
pub use service_core_rs::xrpc::IDEMPOTENCY_KEY_HEADER;
pub use service_core_rs::xrpc::REQUEST_ID_HEADER;
pub use service_core_rs::xrpc::TIMEOUT_HEADER;
pub use service_core_rs::xrpc::TRACEPARENT_HEADER;
pub use service_core_rs::xrpc::XrpcDomainDetailsError;
pub use service_core_rs::xrpc::XrpcErrorEnvelope;
pub use service_core_rs::xrpc::domain_error_envelope;
pub use service_core_rs::xrpc::error_name;
#[cfg(feature = "iroh-http")]
mod iroh;
mod router;
#[cfg(feature = "net")]
mod serve;
#[cfg(test)]
mod tests;
#[cfg(all(feature = "net", unix))]
mod unix;

#[cfg(feature = "iroh-http")]
pub use iroh::IrohXrpcProtocolHandler;
#[cfg(feature = "iroh-http")]
pub use iroh::iroh_protocol_handler;
pub use router::XrpcMethodHandler;
pub use router::XrpcRegistrationError;
pub use router::XrpcRouter;
#[cfg(feature = "net")]
pub use serve::AcceptFuture;
#[cfg(feature = "net")]
pub use serve::ConnectionAcceptor;
#[cfg(feature = "net")]
pub use serve::serve;
#[cfg(feature = "net")]
pub use serve::serve_with_acceptor;
#[cfg(feature = "net")]
pub use serve::serve_with_connection_limit;
#[cfg(all(feature = "net", unix))]
pub use unix::PeerCredentials;
#[cfg(all(feature = "net", unix))]
pub use unix::UnixConnectionAcceptor;

/// Whether a connection error is the peer going away (a reset, a close mid-message) rather than
/// a fault of ours.
#[cfg(any(feature = "net", feature = "iroh-http"))]
fn expected_peer_error(error: &hyper::Error) -> bool {
    error.is_closed()
        || error.is_canceled()
        || error.is_incomplete_message()
        || error.is_body_write_aborted()
        || error.is_shutdown()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcRequest {
    /// The NSID the request addressed. A fallback handler serves many NSIDs and learns which
    /// one here; a registered handler already knows its own.
    pub nsid: String,
    /// The HTTP verb the request used. A fallback handler serves both: it is a GET's query
    /// string (`raw_query`) or a POST's JSON body (`body`).
    pub verb: method_core::HttpMethod,
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    /// The request body as received: a procedure's JSON, which the router has only checked is
    /// present and declared `application/json`; empty for a query. Handlers decode it straight
    /// into their input type.
    pub body: Vec<u8>,
    pub raw_query: Option<String>,
    pub transport: crate::TransportContext,
}

#[derive(Debug, Error)]
pub enum XrpcHandlerError {
    #[error("invalid XRPC input: {0}")]
    InvalidInput(String),
    #[error("operation failed with HTTP status {status}: {body:?}")]
    Domain {
        status: StatusCode,
        body: XrpcErrorEnvelope,
    },
    #[error("XRPC operation is unavailable: {0}")]
    Unavailable(String),
    #[error("XRPC handler failed: {0}")]
    Internal(String),
}

impl XrpcHandlerError {
    pub fn domain(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Domain {
            status,
            body: XrpcErrorEnvelope::new(error.into(), Some(message.into()), BTreeMap::new())
                .expect("empty details contain no reserved fields"),
        }
    }

    pub fn domain_with_details(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
        details: BTreeMap<String, Value>,
    ) -> Result<Self, XrpcDomainDetailsError> {
        Ok(Self::Domain {
            status,
            body: XrpcErrorEnvelope::new(error.into(), Some(message.into()), details)?,
        })
    }
}
