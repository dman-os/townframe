use futures::Stream;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    /// Whether the method confers authority its caller does not already hold, or manages the
    /// nodules that run on the node. A dispatch scope reaches a privileged method only when it
    /// says so (`DispatchScope::privileged`), so a scope that names every authority still cannot
    /// reach it. Methods that only narrow the presented handle are not privileged.
    pub privileged: bool,
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
            privileged: false,
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
            privileged: false,
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
            privileged: false,
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

    /// Marks the method as privileged: see [`Self::privileged`].
    pub const fn privileged(mut self) -> Self {
        self.privileged = true;
        self
    }
}

pub trait Wire: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}
impl<T> Wire for T where T: fmt::Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static
{}

/// A method's error: wire-serializable, built from the common [`ContextError`], and with a
/// `Display` text, which is the `message` of its XRPC error envelope.
pub trait ServiceError: From<ContextError> + Wire + fmt::Display {}
impl<E> ServiceError for E where E: From<ContextError> + Wire + fmt::Display {}

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

/// The schemas a method's Lexicon is projected from, as `schemars` derived them from its Rust
/// types. Host-only: `method_lexicon` assembles the Lexicon file from this.
#[cfg(feature = "schemars")]
pub mod schema {
    pub use schemars;
    use schemars::Schema;
    use schemars::SchemaGenerator;
    use schemars::generate::SchemaSettings;

    /// One method's request, success body and error as JSON Schema positions, with the named
    /// definitions they reach (`$ref: #/$defs/<name>`) beside them.
    #[derive(Debug, Clone)]
    pub struct MethodSchemas {
        pub request: Schema,
        /// The success body, or the subscription's message. A method returning `()` states
        /// `{"type":"null"}` here, which the Lexicon spells as no `output`.
        pub output: Schema,
        pub error: Schema,
        pub definitions: serde_json::Map<String, serde_json::Value>,
    }

    /// Implemented by `methods!` / `service_method!` under the caller's `schemars` feature.
    pub trait MethodSchema: crate::Method {
        fn schemas() -> MethodSchemas;
    }

    #[doc(hidden)]
    pub fn collect<Request, Output, Error>() -> MethodSchemas
    where
        Request: schemars::JsonSchema,
        Output: schemars::JsonSchema,
        Error: schemars::JsonSchema,
    {
        let mut generator: SchemaGenerator = SchemaSettings::draft2020_12().into_generator();
        let request = generator.subschema_for::<Request>();
        let output = generator.subschema_for::<Output>();
        let error = generator.subschema_for::<Error>();
        MethodSchemas {
            request,
            output,
            error,
            definitions: generator.take_definitions(true),
        }
    }
}

/// Emits the host-only [`schema::MethodSchema`] impl beside a `Method` impl. Expands in the
/// caller, so the `schemars` feature it tests is the caller's own, as the `datum` macro does.
#[doc(hidden)]
#[macro_export]
macro_rules! __method_schema {
    ($request:ty, $message:ty, $error:ty) => {
        #[cfg(feature = "schemars")]
        impl $crate::schema::MethodSchema for $request {
            fn schemas() -> $crate::schema::MethodSchemas {
                $crate::schema::collect::<$request, $message, $error>()
            }
        }
    };
}

/// Declares the [`Method`] impls of one service: every entry names its request type, output,
/// XRPC kind (`query`/`procedure`) and the NSID suffix appended to `nsid_prefix`. The error type
/// and NSID prefix are per-invocation because a service shares them across its methods.
#[macro_export]
macro_rules! methods {
    (
        error = $error:ty, nsid_prefix = $prefix:literal;
        $($request:ident => $output:ty, $kind:ident, $nsid:literal $(, $attribute:ident)*;)*
    ) => {$(
        impl $crate::Method for $request {
            type Output = $output;
            type Error = $error;
            const XRPC: $crate::XrpcMethod =
                $crate::XrpcMethod::$kind(concat!($prefix, $nsid))
                    .with_schemas(stringify!($output), stringify!($error))
                    $(.$attribute())*;
        }
        $crate::__method_schema!($request, $output, $error);
    )*};
}
