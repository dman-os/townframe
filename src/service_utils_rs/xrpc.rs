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
mod serve;
#[cfg(test)]
mod tests;
#[cfg(unix)]
mod unix;

#[cfg(feature = "iroh-http")]
pub use iroh::IrohXrpcProtocolHandler;
#[cfg(feature = "iroh-http")]
pub use iroh::iroh_protocol_handler;
pub use router::XrpcFallbackHandler;
pub use router::XrpcMethodHandler;
pub use router::XrpcRegistrationError;
pub use router::XrpcRouter;
pub use router::XrpcSubscriptionHandler;
pub use serve::AcceptFuture;
pub use serve::ConnectionAcceptor;
pub use serve::serve;
pub use serve::serve_with_acceptor;
pub use serve::serve_with_connection_limit;
#[cfg(unix)]
pub use unix::UnixConnectionAcceptor;

/// The credentials the kernel holds for the process on the other end of a Unix socket:
/// `SO_PEERCRED` on Linux, `getpeereid`/`LOCAL_PEERCRED` on macOS and the BSDs (tokio picks the
/// mechanism per platform). The peer cannot forge them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub uid: u32,
    pub gid: u32,
    /// Not every platform reports a pid.
    pub pid: Option<i32>,
}

/// What a fallback handler is asked (ADR 042 §2): the request exactly as the transport saw it,
/// with its body unread. The handler knows the NSID's contract and the transport does not, so
/// whether the body is JSON or raw bytes is the handler's to say.
#[derive(Debug)]
pub struct XrpcFallbackRequest {
    /// The NSID the request addressed.
    pub nsid: String,
    pub verb: method_core::HttpMethod,
    pub context: RequestContext,
    /// The credential and idempotency key the request carries, and its `Range` when it has a
    /// single one.
    pub metadata: HttpMetadata,
    /// The query string, still percent-encoded: the handler reads it itself, as it alone knows
    /// which names repeat.
    pub raw_query: Option<String>,
    /// A POST's body, unread: its `Content-Type` is the stream's mime and its `Content-Length`
    /// its length. `None` for a GET.
    pub body: Option<method_core::ByteBody>,
    pub transport: crate::TransportContext,
    /// The peer's credentials when the connection is a Unix socket, which the listener's acceptor
    /// reported.
    pub peer: Option<PeerCredentials>,
}

/// A fallback handler's answer: a JSON value, or a byte stream the transport sends as the raw
/// response body (`Content-Type` from its mime, `Content-Length` from its length, and `206
/// Partial Content` with `Content-Range` when it has a span).
#[derive(Debug)]
pub enum XrpcFallbackResponse {
    Json(crate::ResponseEnvelope<Value>),
    Bytes {
        metadata: crate::ResponseMetadata,
        body: method_core::ByteBody,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcRequest {
    /// The NSID the request addressed. A fallback handler serves many NSIDs and learns which
    /// one here; a registered handler already knows its own.
    pub nsid: String,
    /// The HTTP verb the request used. A fallback handler serves both: it is a GET's query
    /// string (`raw_query`) or a POST's JSON body (`params`).
    pub verb: method_core::HttpMethod,
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: Value,
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
