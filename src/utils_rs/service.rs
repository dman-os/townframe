//! Shared runtime vocabulary for typed services whose canonical interface is
//! XRPC over HTTP, with direct and IRPC transports as local optimizations.
//!
//! Concrete services declare their operations and own their domain types. This
//! module contains only transport-neutral request context and XRPC metadata.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod sample;

/// Propagated state for a native request. This is the small native equivalent
/// of the HTTP metadata currently shared by every transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub trace_id: Uuid,
    pub span_id: Uuid,
    pub parent_span_id: Option<Uuid>,
}

impl RequestContext {
    pub fn root() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            trace_id: Uuid::now_v7(),
            span_id: Uuid::now_v7(),
            parent_span_id: None,
        }
    }

    pub fn child(&self) -> Self {
        Self {
            request_id: self.request_id,
            trace_id: self.trace_id,
            span_id: Uuid::now_v7(),
            parent_span_id: Some(self.span_id),
        }
    }
}

/// Native request envelope used by direct and IRPC transports. An HTTP adapter
/// maps request metadata into `context` and the Lexicon input into `params`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request<P> {
    pub context: RequestContext,
    pub params: P,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XrpcKind {
    Query,
    Procedure,
    Subscription,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpInput {
    Query,
    JsonBody,
}

/// Canonical external identity and HTTP/schema mapping for an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct XrpcOperation {
    pub nsid: &'static str,
    pub kind: XrpcKind,
    pub method: HttpMethod,
    pub input: HttpInput,
    pub output_schema: &'static str,
    pub error_schema: &'static str,
}

/// Transport-independent HTTP request produced from a typed XRPC call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub context: RequestContext,
    pub operation: XrpcOperation,
    pub query: Option<serde_json::Value>,
    pub body: Option<serde_json::Value>,
}
