use super::RequestContext;
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

/// Errors at the typed client boundary keep transport failure distinct from a
/// semantic operation error. A procedure transport failure is retry-unsafe
/// because the server may have committed before its response was lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError<E> {
    #[error("transport failed: {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
    #[error("operation failed: {0}")]
    Operation(E),
}

impl<E> ClientError<E> {
    pub fn transport(error: impl fmt::Display, outcome_unknown: bool) -> Self {
        Self::Transport {
            message: error.to_string(),
            outcome_unknown,
        }
    }
}

/// Transport metadata carried beside domain parameters. HTTP adapters map these
/// fields to headers; native callers carry the same typed values without putting
/// them into domain JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMetadata {
    pub authorization: Option<String>,
    pub idempotency_key: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub timeout: Option<Duration>,
}

/// Native request envelope used by direct and IRPC transports. An HTTP adapter
/// maps request metadata into `context` and the Lexicon input into `params`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request<P> {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: P,
}

impl<P> Request<P> {
    pub fn new(context: RequestContext, params: P) -> Self {
        Self {
            context,
            metadata: HttpMetadata::default(),
            params,
        }
    }

    pub fn with_metadata(context: RequestContext, metadata: HttpMetadata, params: P) -> Self {
        Self {
            context,
            metadata,
            params,
        }
    }

    /// Rebuilds receiver-local deadline state after a transport boundary.
    pub fn for_receiver(mut self) -> Self {
        self.context = self.context.for_receiver();
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XrpcKind {
    Query,
    Procedure,
    Subscription,
}

impl XrpcKind {
    pub const fn transport_outcome_unknown(self) -> bool {
        matches!(self, Self::Procedure | Self::Subscription)
    }
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
    pub metadata: HttpMetadata,
    pub operation: XrpcOperation,
    pub query: Option<serde_json::Value>,
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
#[error("XRPC transport failed: {message}")]
pub struct XrpcTransportError {
    pub message: String,
}

/// Shared typed dispatch seam implemented by each concrete service.
pub trait UnaryServiceDispatch<Operation, Params, Output, Error>: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<Params>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Output, Error>> + Send + 'a>>;
}

/// Shared typed dispatch seam for subscription implementations. The generated
/// transport layer owns transport-specific receivers; services only provide the
/// validated domain subscription returned by this hook.
pub trait StreamServiceDispatch<Operation, Params, Subscription, Error>: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<Params>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Subscription, Error>> + Send + 'a>>;
}
