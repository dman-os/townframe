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

pub trait Wire: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}
impl<T> Wire for T where T: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static
{}

pub trait ServiceError: From<ContextError> + Wire {}
impl<E> ServiceError for E where E: From<ContextError> + Wire {}

pub trait Operation: Serialize + DeserializeOwned + Send + Sync + 'static {
    type Output: Send + 'static;
    type Error: ServiceError;
    const XRPC: XrpcOperation;
}

pub trait Unary: Operation<Output: Wire> {}
impl<P> Unary for P where P: Operation<Output: Wire> {}

pub trait Subscription:
    Operation<Output = EventStream<<Self as Subscription>::Event, <Self as Operation>::Error>>
{
    type Event: Wire;
}

pub type EventStream<E, Err> = Pin<Box<dyn Stream<Item = Result<E, Err>> + Send + 'static>>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TransportContext {
    #[default]
    Unknown,
    Direct,
    Irpc,
    Http,
    Iroh {
        peer: String,
    },
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
    Result<ResponseEnvelope<<P as Operation>::Output>, <P as Operation>::Error>,
>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventFrame<E> {
    Open(ResponseMetadata),
    Event(E),
    End,
}

pub type EventReply<P> = irpc::channel::mpsc::Sender<
    Result<EventFrame<<P as Subscription>::Event>, <P as Operation>::Error>,
>;

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
