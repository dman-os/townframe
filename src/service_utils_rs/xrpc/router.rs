//! Server side: route `/xrpc/{nsid}` to a mounted operation.

use super::DOMAIN_ERROR_STATUS;
use super::PeerCredentials;
use super::REQUEST_ID_HEADER;
use super::XrpcErrorEnvelope;
use super::XrpcFallbackRequest;
use super::XrpcFallbackResponse;
use super::XrpcHandlerError;
use super::XrpcRequest;
use crate::context::HttpMetadata;
use crate::context::RequestContext;
use crate::host::Host;
use crate::middleware::Handler;
use crate::operation::Envelope;
use axum::Json;
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
use futures::StreamExt as _;
use method_core::ByteBody;
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
use serde_json::json;
use service_client_native_rs::xrpc::ws::WsIo;
use service_client_native_rs::xrpc::ws::accept_key;
use service_core_rs::xrpc::domain_error_envelope;
use service_core_rs::xrpc::request_context;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;

/// A registered operation handler. [`XrpcRouter::mount`] builds one from a
/// hosted [`Handler`]; [`XrpcMethodHandler::typed`] is the escape hatch for
/// an operation that is not hosted through this crate.
#[derive(Clone)]
pub struct XrpcMethodHandler {
    operation: XrpcMethod,
    handler: Arc<dyn Fn(XrpcRequest) -> HandlerFuture + Send + Sync>,
}

type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<crate::ResponseEnvelope<Value>, XrpcHandlerError>> + Send>>;

impl std::fmt::Debug for XrpcMethodHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcMethodHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl XrpcMethodHandler {
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
                Box::pin(async move { future.await.map(Into::into) })
            }),
        }
    }

    pub fn typed<P, O, F, Fut>(operation: XrpcMethod, handler: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(RequestContext, HttpMetadata, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, XrpcHandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Self::new(operation, move |request| {
            let handler = Arc::clone(&handler);
            let result = match operation.input {
                HttpInput::Query => serde_urlencoded::from_str::<P>(
                    request.raw_query.as_deref().unwrap_or_default(),
                )
                .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string())),
                HttpInput::JsonBody => serde_json::from_value::<P>(request.params.clone())
                    .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string())),
            };
            let context = request.context;
            let metadata = request.metadata;
            async move {
                let params = result?;
                let output = handler(context, metadata, params).await?;
                serde_json::to_value(output)
                    .map_err(|error| XrpcHandlerError::Internal(error.to_string()))
            }
        })
    }

    pub fn operation(&self) -> XrpcMethod {
        self.operation
    }

    async fn call(
        &self,
        request: XrpcRequest,
    ) -> Result<crate::ResponseEnvelope<Value>, XrpcHandlerError> {
        (self.handler)(request).await
    }
}

/// The handler that serves every NSID no operation is registered for, for a router whose
/// operations are not known when it is built (a reverse proxy in front of a routing table that
/// changes at runtime). It sees the request with its body unread and answers JSON or a byte
/// stream ([`XrpcFallbackRequest`], [`XrpcFallbackResponse`]).
#[derive(Clone)]
pub struct XrpcFallbackHandler {
    handler: Arc<dyn Fn(XrpcFallbackRequest) -> FallbackFuture + Send + Sync>,
}

type FallbackFuture =
    Pin<Box<dyn Future<Output = Result<XrpcFallbackResponse, XrpcHandlerError>> + Send>>;

impl std::fmt::Debug for XrpcFallbackHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcFallbackHandler")
            .finish_non_exhaustive()
    }
}

impl XrpcFallbackHandler {
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(XrpcFallbackRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<XrpcFallbackResponse, XrpcHandlerError>> + Send + 'static,
    {
        Self {
            handler: Arc::new(move |request| Box::pin(handler(request))),
        }
    }
}

/// A registered subscription handler, carried over a WebSocket upgrade of
/// the same `/xrpc/{nsid}` route rather than the unary HTTP exchange.
#[derive(Clone)]
pub struct XrpcSubscriptionHandler {
    operation: XrpcMethod,
    handler: Arc<dyn Fn(XrpcRequest) -> SubscriptionFuture + Send + Sync>,
}

type SubscriptionFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (
                        crate::ResponseMetadata,
                        Pin<Box<dyn futures::Stream<Item = Result<Value, Value>> + Send>>,
                    ),
                    XrpcHandlerError,
                >,
            > + Send,
    >,
>;

impl std::fmt::Debug for XrpcSubscriptionHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcSubscriptionHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl XrpcSubscriptionHandler {
    pub fn typed<P, E, F, Fut>(operation: XrpcMethod, handler: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        E: Serialize + Send + 'static,
        F: Fn(RequestContext, HttpMetadata, P) -> Fut + Send + Sync + 'static,
        Fut: Future<
                Output = Result<
                    Pin<Box<dyn futures::Stream<Item = Result<Value, E>> + Send>>,
                    XrpcHandlerError,
                >,
            > + Send
            + 'static,
    {
        let handler = Arc::new(handler);
        Self {
            operation,
            handler: Arc::new(move |request| {
                let handler = Arc::clone(&handler);
                let result = match operation.input {
                    HttpInput::Query => serde_urlencoded::from_str::<P>(
                        request.raw_query.as_deref().unwrap_or_default(),
                    )
                    .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string())),
                    HttpInput::JsonBody => serde_json::from_value::<P>(request.params.clone())
                        .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string())),
                };
                let context = request.context;
                let metadata = request.metadata;
                Box::pin(async move {
                    let params = result?;
                    let stream = handler(context, metadata, params).await?;
                    let stream = futures::StreamExt::map(stream, |item| match item {
                        Ok(value) => Ok(value),
                        Err(error) => Err(serde_json::to_value(error)
                            .expect("subscription errors must serialize")),
                    });
                    Ok((
                        crate::ResponseMetadata::default(),
                        Box::pin(stream)
                            as Pin<Box<dyn futures::Stream<Item = Result<Value, Value>> + Send>>,
                    ))
                })
            }),
        }
    }

    pub fn operation(&self) -> XrpcMethod {
        self.operation
    }
}

/// A bounded HTTP router for registered unary XRPC operations.
#[derive(Clone, Debug)]
pub struct XrpcRouter {
    operations: Arc<BTreeMap<String, XrpcMethodHandler>>,
    fallback: Option<XrpcFallbackHandler>,
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
    #[error("an XRPC fallback handler is already registered")]
    DuplicateFallback,
    #[error(
        "{0} has a byte body, which a mounted operation cannot carry; serve it from a fallback"
    )]
    ByteBodyUnsupported(String),
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
        // A byte body is a stream this adapter would neither read nor send: such a method is
        // reached through a fallback handler that does (dispatch's).
        if P::XRPC.input_encoding.is_bytes() || P::XRPC.output_encoding.is_bytes() {
            return Err(XrpcRegistrationError::ByteBodyUnsupported(
                P::XRPC.nsid.to_owned(),
            ));
        }
        self.register(XrpcMethodHandler::new(P::XRPC, move |request| {
            let host = Arc::clone(&host);
            async move {
                let params: P = match P::XRPC.input {
                    HttpInput::Query => {
                        serde_urlencoded::from_str(request.raw_query.as_deref().unwrap_or_default())
                            .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string()))?
                    }
                    HttpInput::JsonBody => serde_json::from_value(request.params)
                        .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string()))?,
                };
                let mut envelope =
                    Envelope::with_metadata(request.context, request.metadata, params);
                envelope.set_transport(request.transport);
                let response = host
                    .invoke(envelope)
                    .await
                    .map_err(|error| domain_error(&error))?;
                Ok(crate::ResponseEnvelope {
                    output: serde_json::to_value(response.output)
                        .expect("operation output must serialize"),
                    metadata: response.metadata,
                })
            }
        }))
    }

    /// Mounts a hosted subscription operation. It is served as a WebSocket
    /// upgrade of the same route; the hosted handler's event stream is pumped
    /// as JSON text frames, so direct, IRPC and XRPC consumers see one event
    /// vocabulary.
    pub fn mount_subscription<P, H>(
        &mut self,
        host: Arc<Host<H>>,
    ) -> Result<(), XrpcRegistrationError>
    where
        P: Subscription,
        H: Handler<P>,
    {
        self.register_subscription(XrpcSubscriptionHandler {
            operation: P::XRPC,
            handler: Arc::new(move |request| {
                let host = Arc::clone(&host);
                Box::pin(async move {
                    let params: P = match P::XRPC.input {
                        HttpInput::Query => serde_urlencoded::from_str(
                            request.raw_query.as_deref().unwrap_or_default(),
                        )
                        .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string()))?,
                        HttpInput::JsonBody => serde_json::from_value(request.params)
                            .map_err(|error| XrpcHandlerError::InvalidInput(error.to_string()))?,
                    };
                    let mut envelope =
                        Envelope::with_metadata(request.context, request.metadata, params);
                    envelope.set_transport(request.transport);
                    let response = host
                        .invoke(envelope)
                        .await
                        .map_err(|error| domain_error(&error))?;
                    let stream = futures::StreamExt::map(response.output, |item| match item {
                        Ok(event) => Ok(serde_json::to_value(event)
                            .expect("subscription events must serialize")),
                        Err(error) => Err(serde_json::to_value(error)
                            .expect("subscription errors must serialize")),
                    });
                    Ok((
                        response.metadata,
                        Box::pin(stream)
                            as Pin<Box<dyn futures::Stream<Item = Result<Value, Value>> + Send>>,
                    ))
                })
            }),
        })
    }

    pub fn register_subscription(
        &mut self,
        handler: XrpcSubscriptionHandler,
    ) -> Result<(), XrpcRegistrationError> {
        if handler.operation().kind != MethodKind::Subscription {
            return Err(XrpcRegistrationError::SubscriptionUnsupported);
        }
        let subscriptions = Arc::make_mut(&mut self.subscriptions);
        let nsid = handler.operation().nsid.to_owned();
        if subscriptions.contains_key(&nsid) || self.operations.contains_key(&nsid) {
            return Err(XrpcRegistrationError::DuplicateOperation(nsid));
        }
        subscriptions.insert(nsid, handler);
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

    /// Installs the handler that serves every NSID no operation is registered for; registered
    /// operations take precedence. The method's kind and body encoding are the handler's to hold
    /// the request to, since only it knows the NSID's contract: the request arrives with the verb
    /// it used and its body unread, and the answer is JSON or a byte stream.
    pub fn register_fallback(
        &mut self,
        handler: XrpcFallbackHandler,
    ) -> Result<(), XrpcRegistrationError> {
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
            router.admission_pool(subscription.operation().long_poll),
            nsid,
            raw_query,
            parts,
            request_id,
        )
        .await;
    }
    let Some(handler) = router.operations.get(&nsid) else {
        let Some(fallback) = router.fallback.clone() else {
            return error_response(
                StatusCode::NOT_FOUND,
                request_id,
                "UnknownMethod",
                "unknown XRPC operation",
            );
        };
        return dispatch_fallback(
            &fallback,
            router.admission_pool(false),
            nsid,
            raw_query,
            parts,
            body,
            request_id,
        )
        .await;
    };
    let operation = handler.operation();
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
    let _admission = match router
        .admission_pool(operation.long_poll)
        .try_acquire_owned()
    {
        Ok(permit) => permit,
        Err(_) => {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                request_id,
                "Unavailable",
                "XRPC handler admission is currently full",
            );
        }
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
    let params = match decode_input(operation.input, raw_query.as_deref(), &body) {
        Ok(params) => params,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                request_id,
                "InvalidRequest",
                &error,
            );
        }
    };
    let call = handler.call(XrpcRequest {
        nsid,
        verb: operation.method,
        context: context.clone(),
        metadata: metadata.clone(),
        params,
        raw_query,
        transport: parts
            .extensions
            .get::<crate::TransportContext>()
            .cloned()
            .unwrap_or(crate::TransportContext::Http),
    });
    let call = match context.remaining_budget() {
        Some(remaining) => match tokio::time::timeout(remaining, call).await {
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

/// Serves an NSID no operation is registered for through the fallback: the request body is not
/// read here but handed over as a stream, and the answer is JSON or a byte stream (ADR 042 §2).
async fn dispatch_fallback(
    fallback: &XrpcFallbackHandler,
    admission: Arc<Semaphore>,
    nsid: String,
    raw_query: Option<String>,
    parts: axum::http::request::Parts,
    body: axum::body::Body,
    request_id: Option<uuid::Uuid>,
) -> Response {
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
    let verb = match parts.method {
        Method::GET => HttpMethod::Get,
        Method::POST => HttpMethod::Post,
        _ => {
            return error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                request_id,
                "InvalidRequest",
                "XRPC serves GET and POST only",
            );
        }
    };
    let _admission = match admission.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                request_id,
                "Unavailable",
                "XRPC handler admission is currently full",
            );
        }
    };
    let body = match verb {
        HttpMethod::Get if !axum::body::HttpBody::is_end_stream(&body) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                request_id,
                "InvalidRequest",
                "query operations must not carry a request body",
            );
        }
        HttpMethod::Get => None,
        HttpMethod::Post => {
            let mut stream = ByteBody::new(
                body.into_data_stream()
                    .map(|chunk| chunk.map_err(std::io::Error::other)),
            );
            if let Some(mime) = parts
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
            {
                stream = stream.with_mime(mime);
            }
            if let Some(length) = parts
                .headers
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
            {
                stream = stream.with_length(length);
            }
            Some(stream)
        }
    };
    let call = (fallback.handler)(XrpcFallbackRequest {
        nsid,
        verb,
        context: context.clone(),
        metadata,
        raw_query,
        body,
        transport: parts
            .extensions
            .get::<crate::TransportContext>()
            .cloned()
            .unwrap_or(crate::TransportContext::Http),
        peer: parts.extensions.get::<PeerCredentials>().copied(),
    });
    let call = match context.remaining_budget() {
        Some(remaining) => match tokio::time::timeout(remaining, call).await {
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
        Ok(XrpcFallbackResponse::Json(output)) => {
            let mut response = json_response(StatusCode::OK, request_id, output.output);
            output.metadata.write_headers(response.headers_mut());
            response
        }
        Ok(XrpcFallbackResponse::Bytes { metadata, body }) => {
            bytes_response(request_id, &metadata, body)
        }
        Err(error) => handler_error_response(error, request_id),
    }
}

/// A byte body as the raw response body. A body with a span is a ranged answer: `206 Partial
/// Content` with the `Content-Range` that places it in the whole.
fn bytes_response(
    request_id: Option<uuid::Uuid>,
    metadata: &crate::ResponseMetadata,
    body: ByteBody,
) -> Response {
    let (mime, length, span) = (body.mime.clone(), body.length, body.span);
    let mut response = Response::new(axum::body::Body::from_stream(body));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime.as_deref().unwrap_or("application/octet-stream"))
            .expect("a mime type is a valid header value"),
    );
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(length) = length {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    }
    if let Some(span) = span {
        let length = length.expect("a ranged byte body states its length");
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                span.start,
                span.start + length - 1,
                span.total
            ))
            .expect("a content range is a valid header value"),
        );
    }
    if let Some(request_id) = request_id
        && let Ok(value) = HeaderValue::from_str(&request_id.to_string())
    {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    metadata.write_headers(response.headers_mut());
    response
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
    let permit = match admission.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                request_id,
                "Unavailable",
                "XRPC handler admission is currently full",
            );
        }
    };
    let Some(on_upgrade) = parts.extensions.get::<hyper::upgrade::OnUpgrade>().cloned() else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            "InternalError",
            "websocket upgrade is not available on this listener",
        );
    };
    let params = match decode_input(HttpInput::Query, raw_query.as_deref(), &[]) {
        Ok(params) => params,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                request_id,
                "InvalidRequest",
                &error,
            );
        }
    };
    let transport = parts
        .extensions
        .get::<crate::TransportContext>()
        .cloned()
        .unwrap_or(crate::TransportContext::Http);
    let (metadata_out, mut stream) = match (subscription.handler)(XrpcRequest {
        nsid,
        verb: HttpMethod::Get,
        context,
        metadata,
        params,
        raw_query,
        transport,
    })
    .await
    {
        Ok(value) => value,
        Err(error) => return handler_error_response(error, request_id),
    };
    let accept = accept_key(&key);
    tokio::spawn(async move {
        let _permit = permit;
        let Ok(upgraded) = on_upgrade.await else {
            return;
        };
        let mut socket = WsIo::new(hyper_util::rt::TokioIo::new(upgraded), false);
        let open = json!({ "open": { "metadata": metadata_out } });
        if socket.write_text(&open.to_string()).await.is_err() {
            return;
        }
        while let Some(item) = stream.next().await {
            let frame = match item {
                Ok(event) => json!({ "event": event }),
                Err(error) => json!({ "error": error }),
            };
            if socket.write_text(&frame.to_string()).await.is_err() {
                return;
            }
            if frame.get("error").is_some() {
                break;
            }
        }
        drop(socket.write_close().await);
    });
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

fn decode_input(input: HttpInput, raw_query: Option<&str>, body: &[u8]) -> Result<Value, String> {
    match input {
        HttpInput::Query => {
            if !body.is_empty() {
                return Err("query operations must not carry a request body".to_owned());
            }
            let query = raw_query.unwrap_or_default();
            serde_urlencoded::from_str::<BTreeMap<String, String>>(query)
                .map(|query| {
                    Value::Object(
                        query
                            .into_iter()
                            .map(|(key, value)| (key, Value::String(value)))
                            .collect(),
                    )
                })
                .map_err(|error| format!("invalid query parameters: {error}"))
        }
        HttpInput::JsonBody => {
            if body.is_empty() {
                return Err("procedure requires a JSON body".to_owned());
            }
            serde_json::from_slice(body).map_err(|error| format!("invalid JSON body: {error}"))
        }
    }
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
    json_response(
        status,
        request_id,
        json!(
            XrpcErrorEnvelope::new(error.to_owned(), message.map(str::to_owned), details,)
                .expect("XRPC details must not contain reserved fields")
        ),
    )
}

fn json_response(status: StatusCode, request_id: Option<uuid::Uuid>, body: Value) -> Response {
    let content_length = serde_json::to_vec(&body)
        .expect("XRPC JSON value must be serializable")
        .len();
    let mut response = (status, Json(body)).into_response();
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
