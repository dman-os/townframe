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
mod client;
#[cfg(feature = "iroh-http")]
mod iroh;
mod router;
mod serve;
#[cfg(test)]
mod tests;

pub use client::XrpcClient;
pub use client::XrpcClientError;
#[cfg(feature = "iroh-http")]
pub use iroh::IROH_XRPC_HTTP_ALPN;
#[cfg(feature = "iroh-http")]
pub use iroh::IrohXrpcClient;
#[cfg(feature = "iroh-http")]
pub use iroh::IrohXrpcProtocolHandler;
#[cfg(feature = "iroh-http")]
pub use iroh::iroh_protocol_handler;
pub use router::XrpcOperationHandler;
pub use router::XrpcRegistrationError;
pub use router::XrpcRouter;
pub use router::XrpcSubscriptionHandler;
pub use serve::serve;
pub use serve::serve_with_connection_limit;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcRequest {
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
