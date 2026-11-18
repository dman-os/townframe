use crate::context::HttpMetadata;
use crate::context::RequestContext;
use method_core::ContextError;
use method_core::Method;
use method_core::Subscription;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

/// An iroh endpoint's 32-byte public key, as the iroh server boundary reads it off the
/// authenticated connection. It names the endpoint that carried a request and nothing more: which
/// principal or principals that endpoint may act as is for the embedding layer to decide, since
/// one connection can carry several. The service contract names no iroh type and builds for wasm.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId([u8; 32]);

impl EndpointId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for EndpointId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "EndpointId({self})")
    }
}

/// Lowercase hex, the text form iroh gives an endpoint key.
impl fmt::Display for EndpointId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

/// Where an in-process call says it comes from. A direct or IRPC client states it when it is
/// built ([`TransportContext::Direct`], [`TransportContext::Irpc`]) and nothing defaults it, so
/// code that relays a remote endpoint's request in process has to say so. It is the caller's
/// claim about its origin, the in-process counterpart of [`TransportContext::Iroh`]'s endpoint,
/// not a principal: mapping an origin to the principals it may act as is the embedding layer's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallOrigin {
    /// The node itself.
    LocalNode,
    /// A request from a remote endpoint that the caller relays.
    Endpoint(EndpointId),
}

/// How a request reached its handler, established by the transport and never by the request:
/// an envelope carries it out of band (it is not serialized), the XRPC router derives it from
/// the connection, and no header can set it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TransportContext {
    /// Not yet stamped by a transport. A host refuses to dispatch such a request.
    #[default]
    Unknown,
    /// Routed inline from an in-process client, with the origin that client states.
    Direct { origin: CallOrigin },
    /// Admitted from a same-node IRPC queue, with the origin its client states.
    Irpc { origin: CallOrigin },
    /// XRPC over HTTP (TCP or a Unix socket): the transport vouches for no one.
    Http,
    /// XRPC over an iroh connection from `endpoint`, which the connection authenticated.
    Iroh { endpoint: EndpointId },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub etag: Option<String>,
    pub location: Option<String>,
    pub retry_after: Option<String>,
    pub cache_control: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseEnvelope<T> {
    pub output: T,
    pub metadata: ResponseMetadata,
}

impl<T> From<T> for ResponseEnvelope<T> {
    fn from(output: T) -> Self {
        Self {
            output,
            metadata: ResponseMetadata::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: P,
    #[serde(skip)]
    transport: TransportContext,
}

impl<P> Envelope<P> {
    pub fn transport(&self) -> &TransportContext {
        &self.transport
    }

    pub fn set_transport(&mut self, transport: TransportContext) {
        self.transport = transport;
    }

    pub fn new(context: RequestContext, params: P) -> Self {
        Self {
            context,
            metadata: HttpMetadata::default(),
            params,
            transport: TransportContext::Unknown,
        }
    }

    pub fn with_metadata(context: RequestContext, metadata: HttpMetadata, params: P) -> Self {
        Self {
            context,
            metadata,
            params,
            transport: TransportContext::Unknown,
        }
    }

    pub fn for_receiver(mut self) -> Self {
        self.context = self.context.for_receiver();
        self
    }
}

pub type Reply<P> = irpc::channel::oneshot::Sender<
    Result<ResponseEnvelope<<P as Method>::Output>, <P as Method>::Error>,
>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventFrame<E> {
    Open(ResponseMetadata),
    Event(E),
    End,
}

pub type EventReply<P> = irpc::channel::mpsc::Sender<
    Result<EventFrame<<P as Subscription>::Event>, <P as Method>::Error>,
>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError<E> {
    #[error("transport failed: {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
    #[error("method failed: {0}")]
    Method(E),
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
        Self::Method(E::from(error))
    }
}
