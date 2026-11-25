//! Server side: route `/xrpc/{nsid}` to a mounted operation.

use super::DOMAIN_ERROR_STATUS;
use super::REQUEST_ID_HEADER;
use super::XrpcErrorEnvelope;
use super::XrpcHandlerError;
use super::XrpcRequest;
use crate::context::HttpMetadata;
use crate::context::RequestContext;
use crate::host::Host;
use crate::lifecycle::Spawner;
use crate::middleware::Handler;
use crate::operation::Envelope;
use async_lock::Semaphore;
use async_lock::SemaphoreGuardArc;
use axum::Router;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::any;
use hyper::upgrade::OnUpgrade;
use method_core::HttpInput;
use method_core::HttpMethod;
use method_core::MethodKind;
use method_core::Subscription;
use method_core::Unary;
use method_core::XrpcMethod;
use mime::Mime;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use service_client_native_rs::xrpc::ws::WsIo;
use service_client_native_rs::xrpc::ws::WsMessage;
use service_client_native_rs::xrpc::ws::accept_key;
use service_core_rs::EventFrame;
use service_core_rs::xrpc::domain_error_envelope;
use service_core_rs::xrpc::request_context;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tracing::Instrument;

/// A registered operation handler. [`XrpcRouter::mount`] builds one from a
/// hosted [`Handler`]; [`XrpcMethodHandler::typed`] is the escape hatch for
/// an operation that is not hosted through this crate.
#[derive(Clone)]
pub struct XrpcMethodHandler {
    operation: XrpcMethod,
    handler: Arc<dyn Fn(XrpcRequest) -> HandlerFuture + Send + Sync>,
}

/// Resolves to the response body, already encoded as JSON, and its metadata.
type HandlerFuture = Pin<
    Box<dyn Future<Output = Result<crate::ResponseEnvelope<Vec<u8>>, XrpcHandlerError>> + Send>,
>;

impl std::fmt::Debug for XrpcMethodHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcMethodHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl XrpcMethodHandler {
    /// A handler over the raw request whose output is any JSON value: the shape of a proxy or
    /// fallback, which does not know the operation's types.
    pub fn new<F, Fut, O>(operation: XrpcMethod, handler: F) -> Self
    where
        F: Fn(XrpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, XrpcHandlerError>> + Send + 'static,
        O: Into<crate::ResponseEnvelope<Value>> + 'static,
    {
        Self {
            operation,
            handler: Arc::new(move |request| {
                let future = handler(request);
                Box::pin(async move { future.await.map(|output| encode_output(output.into())) })
            }),
        }
    }

    /// A handler over typed input and output, decoded from and encoded to the wire directly.
    pub fn typed<P, O, F, Fut>(operation: XrpcMethod, handler: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(RequestContext, HttpMetadata, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, XrpcHandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Self {
            operation,
            handler: Arc::new(move |request| {
                let handler = Arc::clone(&handler);
                let params = decode_params::<P>(operation.input, &request);
                let context = request.context;
                let metadata = request.metadata;
                Box::pin(async move {
                    let output = handler(context, metadata, params?).await?;
                    Ok(encode_output(crate::ResponseEnvelope::from(output)))
                })
            }),
        }
    }

    pub fn operation(&self) -> XrpcMethod {
        self.operation
    }

    async fn call(
        &self,
        request: XrpcRequest,
    ) -> Result<crate::ResponseEnvelope<Vec<u8>>, XrpcHandlerError> {
        (self.handler)(request).await
    }
}

/// Decodes an operation's typed input straight from the wire: the query string, or the JSON body.
fn decode_params<P: DeserializeOwned>(
    input: HttpInput,
    request: &XrpcRequest,
) -> Result<P, XrpcHandlerError> {
    match input {
        HttpInput::Query => serde_urlencoded::from_str(
            request.raw_query.as_deref().unwrap_or_default(),
        )
        .map_err(|error| {
            XrpcHandlerError::InvalidInput(format!("invalid query parameters: {error}"))
        }),
        HttpInput::JsonBody => serde_json::from_slice(&request.body)
            .map_err(|error| XrpcHandlerError::InvalidInput(format!("invalid JSON body: {error}"))),
    }
}

/// Encodes an operation's output as the JSON response body. An output that does not serialize is
/// a programming error in its type.
fn encode_output<O: Serialize>(
    response: crate::ResponseEnvelope<O>,
) -> crate::ResponseEnvelope<Vec<u8>> {
    crate::ResponseEnvelope {
        output: serde_json::to_vec(&response.output).expect("operation output must serialize"),
        metadata: response.metadata,
    }
}

/// A mounted subscription, carried over a WebSocket upgrade of the same `/xrpc/{nsid}` route
/// rather than the unary HTTP exchange. Opening it runs the hosted handler; what it returns is the
/// pump that [`dispatch_subscription`] starts once the upgrade response is on its way.
#[derive(Clone)]
struct XrpcSubscriptionHandler {
    operation: XrpcMethod,
    handler: Arc<dyn Fn(XrpcRequest) -> SubscriptionFuture + Send + Sync>,
}

impl std::fmt::Debug for XrpcSubscriptionHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcSubscriptionHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

type SubscriptionFuture =
    Pin<Box<dyn Future<Output = Result<SocketPump, XrpcHandlerError>> + Send>>;

/// Starts an opened subscription's pump on the upgraded socket, holding the admission permit for
/// as long as it runs.
type SocketPump = Box<dyn FnOnce(OnUpgrade, SemaphoreGuardArc) + Send>;

/// The text frames of a subscription socket. `End` has no frame: the server closes the socket.
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum SocketFrame<'a, E, R> {
    Open {
        metadata: &'a crate::ResponseMetadata,
    },
    Event(&'a E),
    Error(&'a R),
}

/// An upgraded HTTP connection as tokio I/O, for [`WsIo`]. It is `hyper_util::rt::TokioIo`
/// without that crate's `tokio` feature, which enables tokio's `net` and so has no browser build.
struct UpgradedIo(hyper::upgrade::Upgraded);

impl tokio::io::AsyncRead for UpgradedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut read = hyper::rt::ReadBuf::new(buf.initialize_unfilled());
        match hyper::rt::Read::poll_read(Pin::new(&mut self.0), cx, read.unfilled()) {
            std::task::Poll::Ready(Ok(())) => {
                let filled = read.filled().len();
                buf.advance(filled);
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl tokio::io::AsyncWrite for UpgradedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        hyper::rt::Write::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_shutdown(Pin::new(&mut self.0), cx)
    }
}

/// Writes the frames the host's pump delivers to `frames` onto the WebSocket that `on_upgrade`
/// yields, until they end, the client closes the socket, or `owner` stops. Returning drops
/// `frames`, which the pump observes as a closed receiver and ends the stream on.
async fn write_socket<P: Subscription>(
    on_upgrade: OnUpgrade,
    mut frames: irpc::channel::mpsc::Receiver<Result<EventFrame<P::Event>, P::Error>>,
    owner: &Spawner,
) {
    let upgraded = tokio::select! {
        upgraded = on_upgrade => match upgraded {
            Ok(upgraded) => upgraded,
            // The connection ended before the upgrade completed: no one to deliver to.
            Err(_) => return,
        },
        _ = owner.cancelled() => return,
    };
    let (read, write) = tokio::io::split(UpgradedIo(upgraded));
    let mut socket = WsIo::new(write, false);
    // The socket is server-to-client: the client sends nothing but its close, so the read half
    // is only watched for that, or for the connection ending. A ping's pong would have to share
    // the write half with the events; this protocol's clients do not ping, so pongs are dropped.
    let closed = async {
        let mut reader = WsIo::new(tokio::io::join(read, tokio::io::sink()), false);
        while let Ok(Some(WsMessage::Text(_))) = reader.read_message().await {}
    };
    let delivery = async {
        while let Ok(Some(frame)) = frames.recv().await {
            let (text, terminal) = match &frame {
                Ok(EventFrame::Open(metadata)) => {
                    (to_frame::<P>(SocketFrame::Open { metadata }), false)
                }
                Ok(EventFrame::Event(event)) => (to_frame::<P>(SocketFrame::Event(event)), false),
                Ok(EventFrame::End) => break,
                Err(error) => (to_frame::<P>(SocketFrame::Error(error)), true),
            };
            if socket.write_text(&text).await.is_err() {
                return;
            }
            if terminal {
                break;
            }
        }
        // The client may already be gone; the socket is dropped either way.
        drop(socket.write_close().await);
    };
    // Delivery is polled first so that the pump's terminal error, queued as the owner stops, is
    // still written when the socket takes it without waiting.
    tokio::select! {
        biased;
        () = delivery => {}
        () = closed => {}
        _ = owner.cancelled() => {}
    }
}

fn to_frame<P: Subscription>(frame: SocketFrame<'_, P::Event, P::Error>) -> String {
    serde_json::to_string(&frame).expect("subscription frames must serialize")
}

/// A bounded HTTP router for registered unary XRPC operations.
#[derive(Clone, Debug)]
pub struct XrpcRouter {
    operations: Arc<BTreeMap<String, XrpcMethodHandler>>,
    fallback: Option<XrpcMethodHandler>,
    subscriptions: Arc<BTreeMap<String, XrpcSubscriptionHandler>>,
    max_body_bytes: usize,
    admission: Arc<Semaphore>,
    long_poll_admission: Arc<Semaphore>,
    #[cfg(test)]
    body_drop_observer: Option<Arc<std::sync::atomic::AtomicBool>>,
    #[cfg(test)]
    body_drop_notifier: Option<Arc<tokio::sync::Notify>>,
}

const DEFAULT_MAX_CONCURRENT_HANDLERS: usize = 64;
const DEFAULT_MAX_CONCURRENT_LONG_POLLS: usize = 32;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum XrpcRegistrationError {
    #[error("duplicate XRPC operation {0}")]
    DuplicateOperation(String),
    #[error("XRPC subscriptions are not supported by the unary HTTP adapter")]
    SubscriptionUnsupported,
    #[error("the XRPC fallback handler must be a procedure")]
    FallbackNotProcedure,
    #[error("an XRPC fallback handler is already registered")]
    DuplicateFallback,
}

impl XrpcRouter {
    pub fn new(max_body_bytes: usize) -> Self {
        Self::with_limits(max_body_bytes, DEFAULT_MAX_CONCURRENT_HANDLERS)
    }

    pub fn with_limits(max_body_bytes: usize, max_concurrent_handlers: usize) -> Self {
        Self::with_admission_limits(
            max_body_bytes,
            max_concurrent_handlers,
            DEFAULT_MAX_CONCURRENT_LONG_POLLS,
        )
    }

    pub fn with_admission_limits(
        max_body_bytes: usize,
        max_concurrent_handlers: usize,
        max_concurrent_long_polls: usize,
    ) -> Self {
        assert!(max_body_bytes > 0, "XRPC body limit must be positive");
        assert!(
            max_concurrent_handlers > 0,
            "XRPC handler limit must be positive"
        );
        assert!(
            max_concurrent_long_polls > 0,
            "XRPC long-poll handler limit must be positive"
        );
        Self {
            operations: Arc::new(BTreeMap::new()),
            fallback: None,
            subscriptions: Arc::new(BTreeMap::new()),
            max_body_bytes,
            admission: Arc::new(Semaphore::new(max_concurrent_handlers)),
            long_poll_admission: Arc::new(Semaphore::new(max_concurrent_long_polls)),
            #[cfg(test)]
            body_drop_observer: None,
            #[cfg(test)]
            body_drop_notifier: None,
        }
    }

    /// Selects the admission pool for an operation. A long poll may occupy its
    /// permit for the whole hold, so it must not compete with ordinary unary
    /// calls for the same slots.
    fn admission_pool(&self, long_poll: bool) -> Arc<Semaphore> {
        if long_poll {
            Arc::clone(&self.long_poll_admission)
        } else {
            Arc::clone(&self.admission)
        }
    }

    #[cfg(test)]
    pub(super) fn observe_body_drop(&mut self, observer: Arc<std::sync::atomic::AtomicBool>) {
        self.body_drop_observer = Some(observer);
    }

    #[cfg(test)]
    pub(super) fn observe_body_drop_notify(&mut self, notifier: Arc<tokio::sync::Notify>) {
        self.body_drop_notifier = Some(notifier);
    }

    /// Mounts a hosted unary operation. The request is decoded from the query
    /// string or JSON body as `P::XRPC` declares, run through the host (so the
    /// same validation, cancellation, and layers apply as on every other
    /// transport), and its typed error is carried in the standard envelope.
    pub fn mount<P, H>(&mut self, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError>
    where
        P: Unary,
        H: Handler<P>,
    {
        self.register(XrpcMethodHandler {
            operation: P::XRPC,
            handler: Arc::new(move |request| {
                let host = Arc::clone(&host);
                Box::pin(async move {
                    let params = decode_params::<P>(P::XRPC.input, &request)?;
                    let mut envelope =
                        Envelope::with_metadata(request.context, request.metadata, params);
                    envelope.set_transport(request.transport);
                    let response = host
                        .invoke(envelope)
                        .await
                        .map_err(|error| domain_error(&error))?;
                    Ok(encode_output(response))
                })
            }),
        })
    }

    /// Mounts a hosted subscription operation. It is served as a WebSocket upgrade of the same
    /// route. The hosted handler's event stream runs through the host's pump, as on every other
    /// transport, so it has the same cancellation and deadline and is tracked by the host's
    /// [`StopToken`](crate::StopToken); its frames are written to the socket as JSON text, so
    /// direct, IRPC and XRPC consumers see one event vocabulary.
    pub fn mount_subscription<P, H>(
        &mut self,
        host: Arc<Host<H>>,
    ) -> Result<(), XrpcRegistrationError>
    where
        P: Subscription,
        H: Handler<P>,
    {
        let operation = P::XRPC;
        let handler = XrpcSubscriptionHandler {
            operation,
            handler: Arc::new(move |request| {
                let host = Arc::clone(&host);
                Box::pin(async move {
                    let params = decode_params::<P>(P::XRPC.input, &request)?;
                    let mut envelope =
                        Envelope::with_metadata(request.context, request.metadata, params);
                    envelope.set_transport(request.transport);
                    let context = envelope.context.clone();
                    let span = Host::<H>::request_span(&envelope);
                    let response = host
                        .invoke(envelope)
                        .await
                        .map_err(|error| domain_error(&error))?;
                    let pump: SocketPump = Box::new(move |on_upgrade, permit| {
                        let owner = host.spawner().clone();
                        let (tx, rx) = irpc::channel::mpsc::channel(1);
                        // A host that is stopping refuses the task; dropping it drops the stream,
                        // the permit and the upgrade, and the client sees the socket close.
                        let _task = host.spawner().spawn(
                            async move {
                                let _permit = permit;
                                futures::join!(
                                    crate::host::pump::<P>(response, tx, context, owner.clone()),
                                    write_socket::<P>(on_upgrade, rx, &owner),
                                );
                            }
                            .instrument(span),
                        );
                    });
                    Ok(pump)
                })
            }),
        };
        if self.subscriptions.contains_key(operation.nsid)
            || self.operations.contains_key(operation.nsid)
        {
            return Err(XrpcRegistrationError::DuplicateOperation(
                operation.nsid.to_owned(),
            ));
        }
        Arc::make_mut(&mut self.subscriptions).insert(operation.nsid.to_owned(), handler);
        Ok(())
    }

    pub fn register(&mut self, handler: XrpcMethodHandler) -> Result<(), XrpcRegistrationError> {
        if handler.operation().kind == MethodKind::Subscription {
            return Err(XrpcRegistrationError::SubscriptionUnsupported);
        }
        let operations = Arc::make_mut(&mut self.operations);
        let nsid = handler.operation().nsid.to_owned();
        if operations.contains_key(&nsid) {
            return Err(XrpcRegistrationError::DuplicateOperation(nsid));
        }
        operations.insert(nsid, handler);
        Ok(())
    }

    /// Installs the handler that serves every NSID no operation is registered for, for a router
    /// whose operations are not known when it is built (a reverse proxy in front of a routing
    /// table that changes at runtime). The request's `nsid` names the operation and the handler
    /// learns the verb ([`XrpcRequest::verb`]): a POST's JSON body is the input, a GET carries
    /// only a query string (`raw_query`, which the handler reads itself, as it alone knows which
    /// names repeat). The method's kind is the handler's to hold the verb to, since only it knows
    /// the NSID's contract. It is registered as a procedure; registered operations take
    /// precedence.
    pub fn register_fallback(
        &mut self,
        handler: XrpcMethodHandler,
    ) -> Result<(), XrpcRegistrationError> {
        let operation = handler.operation();
        if operation.kind != MethodKind::Procedure {
            return Err(XrpcRegistrationError::FallbackNotProcedure);
        }
        if self.fallback.is_some() {
            return Err(XrpcRegistrationError::DuplicateFallback);
        }
        self.fallback = Some(handler);
        Ok(())
    }

    /// Builds an Axum router.  The caller owns the listener and task, which
    /// keeps shutdown and a future Iroh listener outside this transport seam.
    pub fn into_router(self) -> Router {
        Router::new()
            .route("/xrpc/{nsid}", any(dispatch))
            .layer(axum::extract::DefaultBodyLimit::max(self.max_body_bytes))
            .with_state(Arc::new(self))
    }
}

/// Wraps a typed operation error in the shared XRPC error envelope.
fn domain_error<E>(error: &E) -> XrpcHandlerError
where
    E: Serialize + std::fmt::Display,
{
    XrpcHandlerError::Domain {
        status: DOMAIN_ERROR_STATUS,
        body: domain_error_envelope(error),
    }
}

#[cfg(test)]
struct BodyDropGuard {
    observer: Arc<std::sync::atomic::AtomicBool>,
    notifier: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
impl Drop for BodyDropGuard {
    fn drop(&mut self) {
        self.observer
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(notifier) = &self.notifier {
            notifier.notify_waiters();
        }
    }
}

async fn dispatch(
    State(router): State<Arc<XrpcRouter>>,
    Path(nsid): Path<String>,
    RawQuery(raw_query): RawQuery,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    #[cfg(test)]
    let _body_drop_guard = router
        .body_drop_observer
        .as_ref()
        .map(|observer| BodyDropGuard {
            observer: Arc::clone(observer),
            notifier: router.body_drop_notifier.clone(),
        });
    let request_id = request_id_from_headers(&parts.headers);
    if let Some(subscription) = router.subscriptions.get(&nsid) {
        return dispatch_subscription(
            subscription.clone(),
            router.admission_pool(subscription.operation.long_poll),
            nsid,
            raw_query,
            parts,
            request_id,
        )
        .await;
    }
    let registered = router.operations.get(&nsid);
    let Some(handler) = registered.or(router.fallback.as_ref()) else {
        return error_response(
            StatusCode::NOT_FOUND,
            request_id,
            "UnknownMethod",
            "unknown XRPC operation",
        );
    };
    let mut operation = handler.operation();
    // The fallback serves a GET as a query: the verb is the handler's to judge, not the
    // procedure it was registered as.
    if registered.is_none() && parts.method == Method::GET {
        operation = XrpcMethod::query(operation.nsid);
    }
    let (context, metadata) = match request_context(&parts.headers) {
        Ok(value) => value,
        Err(message) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                request_id,
                "InvalidRequest",
                &message,
            );
        }
    };
    let request_id = Some(context.request_id);
    if let Err(status) = validate_method(&parts.method, operation) {
        return error_response(
            status,
            request_id,
            "InvalidRequest",
            "HTTP method or XRPC input kind does not match the operation",
        );
    }
    if operation.input == HttpInput::JsonBody
        && let Err(message) = validate_json_content_type(&parts.headers)
    {
        return error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            request_id,
            "InvalidRequest",
            &message,
        );
    }
    let Some(_admission) = router.admission_pool(operation.long_poll).try_acquire_arc() else {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            request_id,
            "Unavailable",
            "XRPC handler admission is currently full",
        );
    };
    let body = tokio::select! {
        biased;
        _ = context.expired() => return error_response(
            StatusCode::GATEWAY_TIMEOUT, request_id, "DeadlineExceeded", "XRPC request body exceeded its time budget",
        ),
        body = axum::body::to_bytes(body, router.max_body_bytes) => body,
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                request_id,
                "InvalidRequest",
                &error.to_string(),
            );
        }
    };
    // The body is only checked for presence here; the handler decodes it straight into its
    // input type (or, for the fallback, into whatever it relays).
    let body_error = match operation.input {
        HttpInput::Query if !body.is_empty() => {
            Some("query operations must not carry a request body")
        }
        HttpInput::JsonBody if body.is_empty() => Some("procedure requires a JSON body"),
        HttpInput::Query | HttpInput::JsonBody => None,
    };
    if let Some(error) = body_error {
        return error_response(StatusCode::BAD_REQUEST, request_id, "InvalidRequest", error);
    }
    let call = handler.call(XrpcRequest {
        nsid,
        verb: operation.method,
        context: context.clone(),
        metadata: metadata.clone(),
        body: body.into(),
        raw_query,
        transport: parts
            .extensions
            .get::<crate::TransportContext>()
            .cloned()
            .unwrap_or(crate::TransportContext::Http),
    });
    let call = match context.remaining_budget() {
        Some(remaining) => match n0_future::time::timeout(remaining, call).await {
            Ok(result) => result,
            Err(_) => {
                return error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    request_id,
                    "DeadlineExceeded",
                    "XRPC operation exceeded its time budget",
                );
            }
        },
        None => call.await,
    };
    match call {
        Ok(output) => {
            let mut response = json_response(StatusCode::OK, request_id, output.output);
            output.metadata.write_headers(response.headers_mut());
            response
        }
        Err(error) => handler_error_response(error, request_id),
    }
}

async fn dispatch_subscription(
    subscription: XrpcSubscriptionHandler,
    admission: Arc<Semaphore>,
    nsid: String,
    raw_query: Option<String>,
    parts: axum::http::request::Parts,
    request_id: Option<uuid::Uuid>,
) -> Response {
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
    };
    let is_upgrade = header("upgrade").is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
        && header("connection").is_some_and(|value| value.to_ascii_lowercase().contains("upgrade"));
    if !is_upgrade || parts.method != Method::GET {
        return error_response(
            StatusCode::UPGRADE_REQUIRED,
            request_id,
            "InvalidRequest",
            "subscription operations require a WebSocket upgrade",
        );
    }
    let Some(key) = header("sec-websocket-key").map(str::to_owned) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            "InvalidRequest",
            "missing Sec-WebSocket-Key",
        );
    };
    let (context, metadata) = match request_context(&parts.headers) {
        Ok(value) => value,
        Err(message) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                request_id,
                "InvalidRequest",
                &message,
            );
        }
    };
    let request_id = Some(context.request_id);
    let Some(permit) = admission.try_acquire_arc() else {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            request_id,
            "Unavailable",
            "XRPC handler admission is currently full",
        );
    };
    let Some(on_upgrade) = parts.extensions.get::<OnUpgrade>().cloned() else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            "InternalError",
            "websocket upgrade is not available on this listener",
        );
    };
    let transport = parts
        .extensions
        .get::<crate::TransportContext>()
        .cloned()
        .unwrap_or(crate::TransportContext::Http);
    let pump = match (subscription.handler)(XrpcRequest {
        nsid,
        verb: HttpMethod::Get,
        context,
        metadata,
        body: Vec::new(),
        raw_query,
        transport,
    })
    .await
    {
        Ok(pump) => pump,
        Err(error) => return handler_error_response(error, request_id),
    };
    let accept = accept_key(&key);
    pump(on_upgrade, permit);
    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    response
        .headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    response.headers_mut().insert(
        "sec-websocket-accept",
        HeaderValue::from_str(&accept).expect("accept key is header-safe"),
    );
    if let Some(request_id) = request_id
        && let Ok(value) = HeaderValue::from_str(&request_id.to_string())
    {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

fn validate_method(method: &Method, operation: XrpcMethod) -> Result<(), StatusCode> {
    let expected_method = match operation.method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
    };
    if method != expected_method {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    if operation.kind == MethodKind::Subscription {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }
    Ok(())
}

fn validate_json_content_type(headers: &HeaderMap) -> Result<(), String> {
    let values: Vec<_> = headers.get_all(header::CONTENT_TYPE).iter().collect();
    if values.len() != 1 {
        return Err(
            "procedures require exactly one Content-Type: application/json header".to_owned(),
        );
    }
    let value = values[0]
        .to_str()
        .map_err(|_| "Content-Type must be valid ASCII media-type text".to_owned())?;
    let media_type: Mime = value
        .parse()
        .map_err(|error| format!("invalid Content-Type media type: {error}"))?;
    if !media_type
        .type_()
        .as_str()
        .eq_ignore_ascii_case("application")
        || !media_type.subtype().as_str().eq_ignore_ascii_case("json")
    {
        return Err("procedures require Content-Type: application/json".to_owned());
    }
    let mut seen_charset = false;
    for (name, _) in media_type.params() {
        if !name.as_str().eq_ignore_ascii_case("charset") {
            return Err(format!("unsupported application/json parameter: {name}"));
        }
        if seen_charset {
            return Err("Content-Type must not repeat the charset parameter".to_owned());
        }
        seen_charset = true;
    }
    Ok(())
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<uuid::Uuid> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn handler_error_response(error: XrpcHandlerError, request_id: Option<uuid::Uuid>) -> Response {
    match error {
        XrpcHandlerError::InvalidInput(message) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            "InvalidRequest",
            &message,
        ),
        XrpcHandlerError::Domain { status, body } => {
            let (error, message, details) = body.into_parts();
            error_response_with_details(status, request_id, &error, message.as_deref(), details)
        }
        XrpcHandlerError::Unavailable(message) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            "Unavailable",
            &message,
        ),
        XrpcHandlerError::Internal(message) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            "InternalError",
            &message,
        ),
    }
}

fn error_response(
    status: StatusCode,
    request_id: Option<uuid::Uuid>,
    error: &str,
    message: &str,
) -> Response {
    error_response_with_details(status, request_id, error, Some(message), BTreeMap::new())
}

fn error_response_with_details(
    status: StatusCode,
    request_id: Option<uuid::Uuid>,
    error: &str,
    message: Option<&str>,
    details: BTreeMap<String, Value>,
) -> Response {
    let envelope = XrpcErrorEnvelope::new(error.to_owned(), message.map(str::to_owned), details)
        .expect("XRPC details must not contain reserved fields");
    json_response(
        status,
        request_id,
        serde_json::to_vec(&envelope).expect("an XRPC error envelope serializes"),
    )
}

/// A response carrying `body`, which is already encoded JSON.
fn json_response(status: StatusCode, request_id: Option<uuid::Uuid>, body: Vec<u8>) -> Response {
    let content_length = body.len();
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("XRPC content length must fit an HTTP header"),
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(request_id) = request_id
        && let Ok(value) = HeaderValue::from_str(&request_id.to_string())
    {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}
