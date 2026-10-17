//! The operation vocabulary: what a request type says about itself.
//!
//! The request value selects the method. Everything a transport needs to
//! carry a request — its XRPC identity, output, error, and reply channel —
//! is derived from [`Operation`] on the params type, so a protocol never
//! re-spells an operation for direct, IRPC, or HTTP use.

use crate::context::{ContextError, HttpMetadata, RequestContext};
use futures::Stream;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt, pin::Pin};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XrpcKind {
    Query,
    Procedure,
    Subscription,
}

impl XrpcKind {
    /// A lost transport response leaves a procedure's effect unknown; a query
    /// or a subscription can be retried without reasoning about a commit.
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

/// Canonical external identity and HTTP mapping for one operation.
///
/// The constructors fix the conventional XRPC mapping: queries and
/// subscriptions are `GET` with query-string input, procedures are `POST` with
/// a JSON body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct XrpcOperation {
    pub nsid: &'static str,
    pub kind: XrpcKind,
    pub method: HttpMethod,
    pub input: HttpInput,
    pub output_schema: &'static str,
    pub error_schema: &'static str,
}

impl XrpcOperation {
    pub const fn query(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: XrpcKind::Query,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            error_schema: "",
        }
    }

    pub const fn procedure(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: XrpcKind::Procedure,
            method: HttpMethod::Post,
            input: HttpInput::JsonBody,
            output_schema: "",
            error_schema: "",
        }
    }

    pub const fn subscription(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: XrpcKind::Subscription,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            error_schema: "",
        }
    }

    /// Names the Lexicon output and error schemas this operation is bound to.
    pub const fn with_schemas(
        mut self,
        output_schema: &'static str,
        error_schema: &'static str,
    ) -> Self {
        self.output_schema = output_schema;
        self.error_schema = error_schema;
        self
    }
}

/// What a value must be to cross any of the three transports: JSON for HTTP,
/// and irpc's own message bound (`Debug + Unpin` included) for IRPC.
pub trait Wire: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}

impl<T> Wire for T where T: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static
{}

/// A semantic error vocabulary for a service.
///
/// Every service error must absorb [`ContextError`] because the host, not the
/// handler, rejects invalid, expired, or cancelled requests, and it reports
/// that rejection through the operation's own error type.
pub trait ServiceError: From<ContextError> + Wire {}

impl<E> ServiceError for E where E: From<ContextError> + Wire {}

/// A request type that names its own operation.
///
/// ```ignore
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct GetDoc { pub id: DocumentId }
///
/// impl Operation for GetDoc {
///     type Output = DocSnapshot;
///     type Error = DocsError;
///     const XRPC: XrpcOperation = XrpcOperation::query("org.example.docs.get");
/// }
/// ```
pub trait Operation: Serialize + DeserializeOwned + Send + Sync + 'static {
    type Output: Send + 'static;
    type Error: ServiceError;
    const XRPC: XrpcOperation;
}

/// A query or procedure: one request, one serializable response.
pub trait Unary: Operation<Output: Wire> {}

impl<P> Unary for P where P: Operation<Output: Wire> {}

/// A subscription: one request, a stream of serializable events.
///
/// Its `Output` is fixed to [`EventStream`] so a handler returns the same
/// stream shape every transport can carry.
pub trait Subscription:
    Operation<Output = EventStream<<Self as Subscription>::Event, <Self as Operation>::Error>>
{
    type Event: Wire;
}

/// The handler-side shape of a subscription. Each item is either an event or
/// the terminal error that ends the stream.
pub type EventStream<E, Err> = Pin<Box<dyn Stream<Item = Result<E, Err>> + Send + 'static>>;

/// The native request envelope carried by direct and IRPC transports. The HTTP
/// adapter maps request headers into `context` and `metadata`, and the
/// Lexicon input into `params`; nothing in `params` is transport metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: P,
}

impl<P> Envelope<P> {
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

/// The IRPC reply channel for a unary operation. A protocol variant for `P`
/// is declared as `#[rpc(tx = Reply<P>)] P(Envelope<P>)`.
pub type Reply<P> =
    irpc::channel::oneshot::Sender<Result<<P as Operation>::Output, <P as Operation>::Error>>;

/// The IRPC reply channel for a subscription. A protocol variant for `P` is
/// declared as `#[rpc(tx = EventReply<P>)] P(Envelope<P>)`.
pub type EventReply<P> =
    irpc::channel::mpsc::Sender<Result<<P as Subscription>::Event, <P as Operation>::Error>>;

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

impl<E: From<ContextError>> From<ContextError> for ClientError<E> {
    fn from(error: ContextError) -> Self {
        Self::Operation(E::from(error))
    }
}
