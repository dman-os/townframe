use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;

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

/// How a method's request or response body is encoded: the Lexicon's `input.encoding` and
/// `output.encoding` (ADR 042 §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Encoding {
    /// `application/json`: the body is one JSON value, the only case before ADR 042.
    Json,
    /// Raw octets, never a value that lands whole in memory. `mime` is the media type the
    /// method declares, `None` for `*/*` (any).
    Bytes { mime: Option<&'static str> },
}

impl Encoding {
    pub const fn is_bytes(self) -> bool {
        matches!(self, Self::Bytes { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct XrpcMethod {
    pub nsid: &'static str,
    pub kind: MethodKind,
    pub method: HttpMethod,
    pub input: HttpInput,
    pub output_schema: &'static str,
    pub long_poll: bool,
    /// Whether the method confers authority its caller does not already hold, or manages the
    /// nodules that run on the node. A dispatch scope reaches a privileged method only when it
    /// says so (`DispatchScope::privileged`), so a scope that names every authority still cannot
    /// reach it. Methods that only narrow the presented handle are not privileged.
    pub privileged: bool,
    /// The encoding of the request body. A method with a byte body keeps JSON-shaped
    /// parameters, carried as the query string, so `input` is [`HttpInput::Query`] for it.
    pub input_encoding: Encoding,
    pub output_encoding: Encoding,
    /// Whether dispatch refuses the method unless the call reached an edge transport directly
    /// from the node's own machine (ADR 042 §3). An exposure rule, not authority: a local caller
    /// still needs a capability.
    pub local_only: bool,
}

impl XrpcMethod {
    pub const fn query(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Query,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            long_poll: false,
            privileged: false,
            input_encoding: Encoding::Json,
            output_encoding: Encoding::Json,
            local_only: false,
        }
    }

    pub const fn procedure(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Procedure,
            method: HttpMethod::Post,
            input: HttpInput::JsonBody,
            output_schema: "",
            long_poll: false,
            privileged: false,
            input_encoding: Encoding::Json,
            output_encoding: Encoding::Json,
            local_only: false,
        }
    }

    pub const fn subscription(nsid: &'static str) -> Self {
        Self {
            nsid,
            kind: MethodKind::Subscription,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "",
            long_poll: false,
            privileged: false,
            input_encoding: Encoding::Json,
            output_encoding: Encoding::Json,
            local_only: false,
        }
    }

    pub const fn with_output_schema(mut self, output_schema: &'static str) -> Self {
        self.output_schema = output_schema;
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

    /// Marks the method as local-only: see [`Self::local_only`].
    pub const fn local_only(mut self) -> Self {
        self.local_only = true;
        self
    }

    /// The request body is raw octets of any media type (`*/*`). Only a procedure has a request
    /// body; its parameters move to the query string.
    pub const fn bytes_input(mut self) -> Self {
        assert!(
            matches!(self.kind, MethodKind::Procedure),
            "only a procedure has a request body"
        );
        self.input_encoding = Encoding::Bytes { mime: None };
        self.input = HttpInput::Query;
        self
    }

    /// The response body is raw octets of any media type (`*/*`).
    pub const fn bytes_output(mut self) -> Self {
        assert!(
            !matches!(self.kind, MethodKind::Subscription),
            "a subscription's output is an event stream"
        );
        self.output_encoding = Encoding::Bytes { mime: None };
        self
    }
}

/// Which part of a byte body a reader asks for: the single range of an HTTP `Range: bytes=...`
/// header, resolved against the body's length by whoever serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ByteRange {
    /// `bytes=<start>-`: from `start` to the end.
    From(u64),
    /// `bytes=<start>-<end>`: `end` is inclusive, as HTTP spells it.
    Between { start: u64, end: u64 },
    /// `bytes=-<len>`: the last `len` octets.
    Suffix(u64),
}

impl ByteRange {
    /// The range a `Range` header asks for. `None` for a header that is not a single satisfiable
    /// syntax (another unit, several ranges, garbage): RFC 9110 §14.2 has a server ignore a
    /// `Range` it cannot honour and answer the whole body.
    pub fn from_header(value: &str) -> Option<Self> {
        let spec = value.trim().strip_prefix("bytes=")?;
        if spec.contains(',') {
            return None;
        }
        let (start, end) = spec.split_once('-')?;
        match (start.trim(), end.trim()) {
            ("", "") => None,
            ("", len) => Some(Self::Suffix(len.parse().ok()?)),
            (start, "") => Some(Self::From(start.parse().ok()?)),
            (start, end) => {
                let (start, end): (u64, u64) = (start.parse().ok()?, end.parse().ok()?);
                (start <= end).then_some(Self::Between { start, end })
            }
        }
    }

    /// The `(start, length)` this range selects of a body of `total` octets, or `None` when it
    /// selects nothing (HTTP's 416 Range Not Satisfiable). A range reaching past the end is cut
    /// at it.
    pub fn resolve(self, total: u64) -> Option<(u64, u64)> {
        let (start, end_exclusive) = match self {
            Self::From(start) => (start, total),
            Self::Between { start, end } => (start, end.saturating_add(1).min(total)),
            Self::Suffix(0) => return None,
            Self::Suffix(len) => (total.saturating_sub(len), total),
        };
        (start < end_exclusive).then_some((start, end_exclusive - start))
    }
}

impl fmt::Display for ByteRange {
    /// The `Range` header value that asks for this range.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::From(start) => write!(f, "bytes={start}-"),
            Self::Between { start, end } => write!(f, "bytes={start}-{end}"),
            Self::Suffix(len) => write!(f, "bytes=-{len}"),
        }
    }
}

/// Where a ranged byte body sits in the whole it was cut from, so a transport can answer `206
/// Partial Content` with its `Content-Range`. The body's own length says how far it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSpan {
    pub start: u64,
    pub total: u64,
}

/// Why a [`ByteBody`] could not be gathered into one buffer.
#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("the body is longer than the {limit} octet limit")]
    TooLarge { limit: u64 },
    #[error("the body failed while it was read: {0}")]
    Io(#[from] io::Error),
}

/// A byte body (ADR 042 §2): a pull-based stream of chunks, so the receiver reads as fast as it
/// consumes and nothing lands whole on its stack or heap. It is the same value whichever
/// transport carried the call: an HTTP request or response body is read from the socket as the
/// stream is polled, and in process it is moved, never serialized. It therefore has no wire
/// form: serializing or deserializing one is an error, which is what keeps a byte body from
/// silently crossing a transport that cannot stream it.
pub struct ByteBody {
    /// The media type, when the sender states one.
    pub mime: Option<String>,
    /// How many octets the stream will yield, when the sender knows.
    pub length: Option<u64>,
    /// For a ranged response, the part of the whole this body is.
    pub span: Option<ByteSpan>,
    // `get_mut` reaches the stream without locking, so the mutex only makes the body `Sync`,
    // which the typed service channels require of a message.
    stream: Mutex<BoxStream<'static, io::Result<Bytes>>>,
}

impl ByteBody {
    pub fn new(stream: impl Stream<Item = io::Result<Bytes>> + Send + 'static) -> Self {
        Self {
            mime: None,
            length: None,
            span: None,
            stream: Mutex::new(stream.boxed()),
        }
    }

    /// A body that is already in memory.
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        let length = bytes.len() as u64;
        Self::new(futures::stream::once(std::future::ready(Ok(bytes)))).with_length(length)
    }

    pub fn with_mime(mut self, mime: impl Into<String>) -> Self {
        self.mime = Some(mime.into());
        self
    }

    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    pub fn with_span(mut self, span: ByteSpan) -> Self {
        self.span = Some(span);
        self
    }

    /// Reads the whole body into one buffer, refusing one longer than `limit` octets as soon as
    /// it is seen to be (by its stated length, else while reading).
    pub async fn collect(mut self, limit: u64) -> Result<Bytes, CollectError> {
        if self.length.is_some_and(|length| length > limit) {
            return Err(CollectError::TooLarge { limit });
        }
        let mut buffer = Vec::with_capacity(self.length.unwrap_or(0) as usize);
        while let Some(chunk) = self.next().await {
            let chunk = chunk?;
            if (buffer.len() + chunk.len()) as u64 > limit {
                return Err(CollectError::TooLarge { limit });
            }
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.into())
    }
}

impl Stream for ByteBody {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .stream
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .poll_next_unpin(cx)
    }
}

impl fmt::Debug for ByteBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteBody")
            .field("mime", &self.mime)
            .field("length", &self.length)
            .field("span", &self.span)
            .finish_non_exhaustive()
    }
}

impl Serialize for ByteBody {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "a byte body is a stream with no serialized form; it travels in process or as an HTTP body",
        ))
    }
}

impl<'de> Deserialize<'de> for ByteBody {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "a byte body is a stream with no serialized form; it travels in process or as an HTTP body",
        ))
    }
}

/// A byte body is a Lexicon `bytes` output, whose projection never reads this schema; it exists
/// so a method whose `Output` is a byte body still derives its schemas.
#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ByteBody {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ByteBody".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string", "format": "byte" })
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
                    .with_output_schema(stringify!($output))
                    $(.$attribute())*;
        }
        $crate::__method_schema!($request, $output, $error);
    )*};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_header_is_one_satisfiable_syntax_or_ignored() {
        let parse = ByteRange::from_header;
        assert_eq!(parse("bytes=5-"), Some(ByteRange::From(5)));
        assert_eq!(
            parse("bytes=5-9"),
            Some(ByteRange::Between { start: 5, end: 9 })
        );
        assert_eq!(parse("bytes=-3"), Some(ByteRange::Suffix(3)));
        // Other units, several ranges, an inverted range and garbage are ignored (RFC 9110 §14.2).
        for ignored in [
            "items=1-2",
            "bytes=1-2,5-6",
            "bytes=9-5",
            "bytes=-",
            "bytes=a-b",
            "5-9",
        ] {
            assert_eq!(parse(ignored), None, "{ignored}");
        }
        for range in [
            ByteRange::From(5),
            ByteRange::Between { start: 5, end: 9 },
            ByteRange::Suffix(3),
        ] {
            assert_eq!(parse(&range.to_string()), Some(range));
        }
    }

    #[test]
    fn a_range_resolves_against_the_length_and_is_cut_at_the_end() {
        assert_eq!(ByteRange::From(95).resolve(100), Some((95, 5)));
        assert_eq!(
            ByteRange::Between { start: 10, end: 19 }.resolve(100),
            Some((10, 10))
        );
        assert_eq!(
            ByteRange::Between {
                start: 90,
                end: 500
            }
            .resolve(100),
            Some((90, 10))
        );
        assert_eq!(ByteRange::Suffix(5).resolve(100), Some((95, 5)));
        assert_eq!(ByteRange::Suffix(500).resolve(100), Some((0, 100)));
        // Nothing selected: HTTP's 416.
        assert_eq!(ByteRange::From(100).resolve(100), None);
        assert_eq!(ByteRange::Suffix(0).resolve(100), None);
        assert_eq!(ByteRange::From(0).resolve(0), None);
    }

    #[test]
    fn a_byte_body_is_pulled_and_has_no_serialized_form() {
        let body = ByteBody::new(futures::stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cd")),
        ]));
        assert!(serde_json::to_string(&body).is_err());
        let collected = futures::executor::block_on(body.collect(4)).unwrap();
        assert_eq!(collected.as_ref(), b"abcd");
        let too_long = ByteBody::from_bytes(Bytes::from_static(b"abcde"));
        assert!(matches!(
            futures::executor::block_on(too_long.collect(4)),
            Err(CollectError::TooLarge { limit: 4 })
        ));
    }

    #[test]
    fn the_declaration_builders_set_the_encodings() {
        let put = XrpcMethod::procedure("a.b.put").bytes_input().local_only();
        assert_eq!(put.input_encoding, Encoding::Bytes { mime: None });
        assert_eq!(put.output_encoding, Encoding::Json);
        assert_eq!(
            put.input,
            HttpInput::Query,
            "the parameters move to the query string"
        );
        assert!(put.local_only);
        let read = XrpcMethod::query("a.b.read").bytes_output();
        assert!(read.output_encoding.is_bytes() && !read.input_encoding.is_bytes());
    }
}
