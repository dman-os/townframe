use crate::context::HttpMetadata;
use crate::context::RequestContext;
use method_core::ByteBody;
use method_core::ContextError;
use method_core::Method;
use method_core::Subscription;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

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

/// The body carried by an in-process invocation. Descriptor bodies have no serialized form and
/// can only be supplied by the native descriptor transport.
#[derive(Debug)]
pub enum Body {
    Json(serde_json::Value),
    Bytes(ByteBody),
    #[cfg(unix)]
    Descriptors(crate::descriptor::DescriptorBody),
}

impl Body {
    pub fn case(&self) -> &'static str {
        match self {
            Self::Json(_) => "JSON",
            Self::Bytes(_) => "bytes",
            #[cfg(unix)]
            Self::Descriptors(_) => "descriptors",
        }
    }

    /// The JSON value, for a caller whose method declares a JSON body.
    pub fn into_json(self) -> serde_json::Value {
        match self {
            Self::Json(value) => value,
            Self::Bytes(_) => panic!("expected a JSON body, found bytes"),
            #[cfg(unix)]
            Self::Descriptors(_) => panic!("expected a JSON body, found descriptors"),
        }
    }
}

impl Serialize for Body {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Json(value) => value.serialize(serializer),
            Self::Bytes(body) => body.serialize(serializer),
            #[cfg(unix)]
            Self::Descriptors(_) => Err(serde::ser::Error::custom(
                "a descriptor body has no serialized form",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Body {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_json::Value::deserialize(deserializer).map(Self::Json)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Body {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Body".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!(true)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: P,
    #[serde(skip)]
    transport: TransportContext,
    /// The request body of a method whose input encoding is bytes (ADR 042): a stream moved
    /// with the envelope, so it exists only in process. The wire forms carry it as the raw HTTP
    /// request body instead, which is why a transport that cannot stream refuses such a method.
    #[serde(skip)]
    body: Option<Body>,
}

impl<P> Envelope<P> {
    /// Attaches the byte request body of a method that takes one.
    pub fn with_body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self
    }

    /// Takes the byte request body: a handler of a bytes-input method calls this once.
    pub fn take_body(&mut self) -> Option<Body> {
        self.body.take()
    }

    pub fn has_body(&self) -> bool {
        self.body.is_some()
    }

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
            body: None,
        }
    }

    pub fn with_metadata(context: RequestContext, metadata: HttpMetadata, params: P) -> Self {
        Self {
            context,
            metadata,
            params,
            transport: TransportContext::Unknown,
            body: None,
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
