//! Canonical XRPC over HTTP.
//!
//! This is the interface every other transport is an optimization of. The
//! module is split by role:
//!
//! * `router` — `XrpcRouter` decodes `/xrpc/{nsid}` requests, builds the
//!   [`RequestContext`] from headers, and runs a mounted operation.
//! * `client` — `XrpcClient` encodes an operation to HTTP and decodes the
//!   response, keeping procedure ambiguity explicit.
//! * `serve` — bounded HTTP/1 serving over TCP with connection limits and
//!   shutdown that joins.
//! * `iroh` (feature `iroh-http`) — the same HTTP/1 wire over Iroh streams.
//!
//! Typed errors cross HTTP inside the standard XRPC error envelope: the body is
//! `{"error", "message", ..}` and the operation's own error value is carried in
//! the [`DOMAIN_ERROR_FIELD`] detail, so a client decodes exactly the
//! `Operation::Error` the service returned.

use crate::context::{HttpMetadata, RequestContext};
use axum::http::StatusCode;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

mod client;
#[cfg(feature = "iroh-http")]
mod iroh;
mod router;
mod serve;
#[cfg(test)]
mod tests;

pub use client::{XrpcClient, XrpcClientError};
#[cfg(feature = "iroh-http")]
pub use iroh::{
    IROH_XRPC_HTTP_ALPN, IrohXrpcClient, IrohXrpcProtocolHandler, iroh_protocol_handler,
};
pub use router::{XrpcOperationHandler, XrpcRegistrationError, XrpcRouter};
pub use serve::{serve, serve_with_connection_limit};

const REQUEST_ID_HEADER: &str = "x-request-id";
const TRACE_ID_HEADER: &str = "x-trace-id";
const SPAN_ID_HEADER: &str = "x-span-id";
const PARENT_SPAN_ID_HEADER: &str = "x-parent-span-id";
const TRACEPARENT_HEADER: &str = "traceparent";
const TRACESTATE_HEADER: &str = "tracestate";
const AUTHORIZATION_HEADER: &str = "authorization";
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const TIMEOUT_HEADER: &str = "x-timeout-ms";

/// The error-envelope detail that carries a typed operation error. The
/// envelope's `error` is the operation's error schema name and `message` is
/// the error's `Debug` rendering.
pub const DOMAIN_ERROR_FIELD: &str = "operationError";

/// HTTP status used for a typed operation error. Transport ambiguity for
/// procedures is tracked separately by [`XrpcClientError`].
pub const DOMAIN_ERROR_STATUS: StatusCode = StatusCode::BAD_REQUEST;

/// A decoded XRPC request handed to a registered operation handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcRequest {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: Value,
    /// The original query string is retained so typed handlers can apply
    /// serde's query decoding rules for numbers, booleans, and repeated keys.
    pub raw_query: Option<String>,
}

/// Standard XRPC error body. Server handlers cannot accidentally emit a
/// non-XRPC error body through the domain-error path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XrpcErrorEnvelope {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    /// Domain-specific fields remain in the standard envelope instead of
    /// replacing it with an arbitrary scalar, array, or unrelated object.
    #[serde(flatten)]
    details: BTreeMap<String, Value>,
}

impl XrpcErrorEnvelope {
    pub fn error(&self) -> &str {
        &self.error
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub fn details(&self) -> &BTreeMap<String, Value> {
        &self.details
    }
}

impl<'de> serde::Deserialize<'de> for XrpcErrorEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("XRPC error envelope must be an object"))?;
        let error = object.get("error").and_then(Value::as_str).ok_or_else(|| {
            serde::de::Error::custom("XRPC error envelope error must be a string")
        })?;
        let message = object
            .get("message")
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    serde::de::Error::custom("XRPC error envelope message must be a string")
                })
            })
            .transpose()?;
        let details = object
            .iter()
            .filter(|(key, _)| key.as_str() != "error" && key.as_str() != "message")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            error: error.to_owned(),
            message,
            details,
        })
    }
}

/// Domain or protocol result returned by a server-side operation handler.
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum XrpcDomainDetailsError {
    #[error("XRPC domain details cannot redefine reserved field {0}")]
    ReservedField(&'static str),
}

impl XrpcHandlerError {
    pub fn domain(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Domain {
            status,
            body: XrpcErrorEnvelope {
                error: error.into(),
                message: Some(message.into()),
                details: BTreeMap::new(),
            },
        }
    }

    pub fn domain_with_details(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
        details: BTreeMap<String, Value>,
    ) -> Result<Self, XrpcDomainDetailsError> {
        for reserved in ["error", "message"] {
            if details.contains_key(reserved) {
                return Err(XrpcDomainDetailsError::ReservedField(reserved));
            }
        }
        Ok(Self::Domain {
            status,
            body: XrpcErrorEnvelope {
                error: error.into(),
                message: Some(message.into()),
                details,
            },
        })
    }
}
