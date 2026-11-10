use futures::Stream;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ContextError {
    #[error("request context is missing a request id")]
    MissingRequestId,
    #[error("request was cancelled")]
    Cancelled,
    #[error("request cancellation key is already admitted")]
    CancellationAlreadyAdmitted,
    #[error("request cancellation scope capacity exhausted (limit {limit})")]
    CancellationCapacityExceeded { limit: usize },
    #[error("subscription capacity exhausted (limit {limit})")]
    StreamCapacityExceeded { limit: usize },
    #[error("request exceeded its time budget")]
    DeadlineExceeded,
    #[error("request carries no valid Authorization credential")]
    Unauthenticated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MethodKind {
    Query,
    Procedure,
    Subscription,
}

impl MethodKind {
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
pub struct XrpcMethod {
    pub nsid: &'static str,
    pub kind: MethodKind,
    pub method: HttpMethod,
    pub input: HttpInput,
    pub output_schema: &'static str,
    pub error_schema: &'static str,
    pub long_poll: bool,
}

impl XrpcMethod {
    pub const fn query(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Query,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            error_schema: "",
            long_poll: false,
        }
    }

    pub const fn procedure(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Procedure,
            method: HttpMethod::Post,
            input: HttpInput::JsonBody,
            output_schema: "",
            error_schema: "",
            long_poll: false,
        }
    }

    pub const fn subscription(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Subscription,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            error_schema: "",
            long_poll: false,
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

    /// Marks the method as a long poll. A held poll keeps its request permit
    /// for up to the hold, so the XRPC router admits it through the long-poll
    /// pool instead of consuming an ordinary unary slot.
    pub const fn long_poll(mut self) -> Self {
        self.long_poll = true;
        self
    }
}

pub trait Wire: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}
impl<T> Wire for T where T: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static
{}

pub trait ServiceError: From<ContextError> + Wire {}
impl<E> ServiceError for E where E: From<ContextError> + Wire {}

pub trait Method: Serialize + DeserializeOwned + Send + Sync + 'static {
    type Output: Send + 'static;
    type Error: ServiceError;
    const XRPC: XrpcMethod;
}

pub trait Unary: Method<Output: Wire> {}
impl<P> Unary for P where P: Method<Output: Wire> {}

pub trait Subscription:
    Method<Output = EventStream<<Self as Subscription>::Event, <Self as Method>::Error>>
{
    type Event: Wire;
}

pub type EventStream<E, Err> = Pin<Box<dyn Stream<Item = Result<E, Err>> + Send + 'static>>;


/// Declares the [`Method`] impls of one service: every entry names its request type, output,
/// XRPC kind (`query`/`procedure`) and the NSID suffix appended to `nsid_prefix`. The error type
/// and NSID prefix are per-invocation because a service shares them across its methods.
#[macro_export]
macro_rules! methods {
    (
        error = $error:ty, nsid_prefix = $prefix:literal;
        $($request:ident => $output:ty, $kind:ident, $nsid:literal;)*
    ) => {$(
        impl $crate::Method for $request {
            type Output = $output;
            type Error = $error;
            const XRPC: $crate::XrpcMethod =
                $crate::XrpcMethod::$kind(concat!($prefix, $nsid))
                    .with_schemas(stringify!($output), stringify!($error));
        }
    )*};
}
