//! Canonical unary XRPC-over-HTTP transport for typed service operations.
//!
//! The operation declaration remains the source of truth.  This module only
//! maps that declaration to HTTP and leaves domain dispatch to a handler.

use super::{HttpInput, HttpMetadata, HttpMethod, RequestContext, XrpcKind, XrpcOperation};
use axum::{
    Json, Router,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use futures::StreamExt;
use http_body_util::BodyExt;
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use mime::Mime;
use reqwest::Url;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    task::JoinSet,
};

const REQUEST_ID_HEADER: &str = "x-request-id";
const TRACE_ID_HEADER: &str = "x-trace-id";
const SPAN_ID_HEADER: &str = "x-span-id";
const PARENT_SPAN_ID_HEADER: &str = "x-parent-span-id";
const TRACEPARENT_HEADER: &str = "traceparent";
const TRACESTATE_HEADER: &str = "tracestate";
const AUTHORIZATION_HEADER: &str = "authorization";
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const TIMEOUT_HEADER: &str = "x-timeout-ms";

/// A decoded XRPC request handed to a registered operation handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcRequest {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: Value,
    /// The original query string is retained so typed handlers can apply
    /// serde's query decoding rules for numbers, booleans, and repeated keys.
    pub raw_query: Option<String>,
}

/// Standard XRPC error body. Server handlers cannot accidentally emit a
/// non-XRPC error body through the domain-error path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XrpcErrorEnvelope {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    /// Domain-specific fields remain in the standard envelope instead of
    /// replacing it with an arbitrary scalar, array, or unrelated object.
    #[serde(flatten)]
    details: BTreeMap<String, Value>,
}

impl XrpcErrorEnvelope {
    pub fn error(&self) -> &str {
        &self.error
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub fn details(&self) -> &BTreeMap<String, Value> {
        &self.details
    }
}

impl<'de> serde::Deserialize<'de> for XrpcErrorEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("XRPC error envelope must be an object"))?;
        let error = object.get("error").and_then(Value::as_str).ok_or_else(|| {
            serde::de::Error::custom("XRPC error envelope error must be a string")
        })?;
        let message = object
            .get("message")
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    serde::de::Error::custom("XRPC error envelope message must be a string")
                })
            })
            .transpose()?;
        let details = object
            .iter()
            .filter(|(key, _)| key.as_str() != "error" && key.as_str() != "message")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            error: error.to_owned(),
            message,
            details,
        })
    }
}

/// Domain or protocol result returned by a server-side operation handler.
#[derive(Debug, Error)]
pub enum XrpcHandlerError {
    #[error("invalid XRPC input: {0}")]
    InvalidInput(String),
    #[error("operation failed with HTTP status {status}: {body:?}")]
    Domain {
        status: StatusCode,
        body: XrpcErrorEnvelope,
    },
    #[error("XRPC operation is unavailable: {0}")]
    Unavailable(String),
    #[error("XRPC handler failed: {0}")]
    Internal(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum XrpcDomainDetailsError {
    #[error("XRPC domain details cannot redefine reserved field {0}")]
    ReservedField(&'static str),
}

impl XrpcHandlerError {
    pub fn domain(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Domain {
            status,
            body: XrpcErrorEnvelope {
                error: error.into(),
                message: Some(message.into()),
                details: BTreeMap::new(),
            },
        }
    }

    pub fn domain_with_details(
        status: StatusCode,
        error: impl Into<String>,
        message: impl Into<String>,
        details: BTreeMap<String, Value>,
    ) -> Result<Self, XrpcDomainDetailsError> {
        for reserved in ["error", "message"] {
            if details.contains_key(reserved) {
                return Err(XrpcDomainDetailsError::ReservedField(reserved));
            }
        }
        Ok(Self::Domain {
            status,
            body: XrpcErrorEnvelope {
                error: error.into(),
                message: Some(message.into()),
                details,
            },
        })
    }
}

/// A registered operation handler.  Typed service generation can use
/// [`XrpcOperationHandler::typed`] to deserialize parameters and serialize the
/// typed result without putting transport metadata into domain JSON.
#[derive(Clone)]
pub struct XrpcOperationHandler {
    operation: XrpcOperation,
    handler: Arc<dyn Fn(XrpcRequest) -> HandlerFuture + Send + Sync>,
}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, XrpcHandlerError>> + Send>>;

impl std::fmt::Debug for XrpcOperationHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcOperationHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl XrpcOperationHandler {
    pub fn new<F, Fut>(operation: XrpcOperation, handler: F) -> Self
    where
        F: Fn(XrpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, XrpcHandlerError>> + Send + 'static,
    {
        Self {
            operation,
            handler: Arc::new(move |request| Box::pin(handler(request))),
        }
    }

    pub fn typed<P, O, F, Fut>(operation: XrpcOperation, handler: F) -> Self
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

    pub fn operation(&self) -> XrpcOperation {
        self.operation
    }

    async fn call(&self, request: XrpcRequest) -> Result<Value, XrpcHandlerError> {
        (self.handler)(request).await
    }
}

/// A bounded HTTP router for registered unary XRPC operations.
#[derive(Clone, Debug)]
pub struct XrpcRouter {
    operations: Arc<BTreeMap<String, XrpcOperationHandler>>,
    max_body_bytes: usize,
    admission: Arc<Semaphore>,
    #[cfg(test)]
    body_drop_observer: Option<Arc<std::sync::atomic::AtomicBool>>,
    #[cfg(test)]
    body_drop_notifier: Option<Arc<tokio::sync::Notify>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum XrpcRegistrationError {
    #[error("duplicate XRPC operation {0}")]
    DuplicateOperation(String),
    #[error("XRPC subscriptions are not supported by the unary HTTP adapter")]
    SubscriptionUnsupported,
}

impl XrpcRouter {
    pub fn new(max_body_bytes: usize) -> Self {
        Self::with_limits(max_body_bytes, 64)
    }

    pub fn with_limits(max_body_bytes: usize, max_concurrent_handlers: usize) -> Self {
        assert!(max_body_bytes > 0, "XRPC body limit must be positive");
        assert!(
            max_concurrent_handlers > 0,
            "XRPC handler limit must be positive"
        );
        Self {
            operations: Arc::new(BTreeMap::new()),
            max_body_bytes,
            admission: Arc::new(Semaphore::new(max_concurrent_handlers)),
            #[cfg(test)]
            body_drop_observer: None,
            #[cfg(test)]
            body_drop_notifier: None,
        }
    }

    #[cfg(test)]
    fn observe_body_drop(&mut self, observer: Arc<std::sync::atomic::AtomicBool>) {
        self.body_drop_observer = Some(observer);
    }

    #[cfg(test)]
    fn observe_body_drop_notify(&mut self, notifier: Arc<tokio::sync::Notify>) {
        self.body_drop_notifier = Some(notifier);
    }

    pub fn register(&mut self, handler: XrpcOperationHandler) -> Result<(), XrpcRegistrationError> {
        if handler.operation().kind == XrpcKind::Subscription {
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

    /// Builds an Axum router.  The caller owns the listener and task, which
    /// keeps shutdown and a future Iroh listener outside this transport seam.
    pub fn into_router(self) -> Router {
        Router::new()
            .route("/xrpc/{nsid}", any(dispatch))
            .layer(axum::extract::DefaultBodyLimit::max(self.max_body_bytes))
            .with_state(Arc::new(self))
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
    let Some(handler) = router.operations.get(&nsid) else {
        return error_response(
            StatusCode::NOT_FOUND,
            request_id,
            "UnknownMethod",
            "unknown XRPC operation",
        );
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
    let _admission = match Arc::clone(&router.admission).try_acquire_owned() {
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
    let body = match axum::body::to_bytes(body, router.max_body_bytes).await {
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
        context,
        metadata: metadata.clone(),
        params,
        raw_query,
    });
    let call = match metadata.timeout {
        Some(timeout) => match tokio::time::timeout(timeout, call).await {
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
        Ok(output) => json_response(StatusCode::OK, request_id, output),
        Err(XrpcHandlerError::InvalidInput(message)) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            "InvalidRequest",
            &message,
        ),
        Err(XrpcHandlerError::Domain { status, body }) => error_response_with_details(
            status,
            request_id,
            &body.error,
            body.message.as_deref(),
            body.details,
        ),
        Err(XrpcHandlerError::Unavailable(message)) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            "Unavailable",
            &message,
        ),
        Err(XrpcHandlerError::Internal(message)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            "InternalError",
            &message,
        ),
    }
}

fn validate_method(method: &Method, operation: XrpcOperation) -> Result<(), StatusCode> {
    let expected_method = match operation.method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
    };
    if method != expected_method {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    if operation.kind == XrpcKind::Subscription {
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
    header_string(headers, REQUEST_ID_HEADER)
        .ok()
        .flatten()
        .and_then(|value| value.parse().ok())
}

fn request_context(headers: &HeaderMap) -> Result<(RequestContext, HttpMetadata), String> {
    let mut context = RequestContext::root();
    if let Some(value) = header_string(headers, REQUEST_ID_HEADER)? {
        context.request_id = value
            .parse()
            .map_err(|_| "x-request-id must be a UUID".to_owned())?;
    }
    if let Some(value) = header_string(headers, TRACE_ID_HEADER)? {
        context.trace_id = value
            .parse()
            .map_err(|_| "x-trace-id must be a UUID".to_owned())?;
    }
    if let Some(value) = header_string(headers, SPAN_ID_HEADER)? {
        context.span_id = value
            .parse()
            .map_err(|_| "x-span-id must be a UUID".to_owned())?;
    }
    context.parent_span_id = header_string(headers, PARENT_SPAN_ID_HEADER)?
        .map(|value| value.parse().map_err(|_| "x-parent-span-id must be a UUID"))
        .transpose()?;
    let timeout = header_string(headers, TIMEOUT_HEADER)?
        .map(|value| {
            value
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| "x-timeout-ms must be an unsigned integer".to_owned())
        })
        .transpose()?;
    Ok((
        context,
        HttpMetadata {
            authorization: header_string(headers, AUTHORIZATION_HEADER)?,
            idempotency_key: header_string(headers, IDEMPOTENCY_KEY_HEADER)?,
            traceparent: header_string(headers, TRACEPARENT_HEADER)?,
            tracestate: header_string(headers, TRACESTATE_HEADER)?,
            timeout,
        },
    ))
}

fn header_string(headers: &HeaderMap, name: &str) -> Result<Option<String>, String> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| format!("{name} must be valid UTF-8 HTTP text"))
        })
        .transpose()
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
        json!(XrpcErrorEnvelope {
            error: error.to_owned(),
            message: message.map(str::to_owned),
            details,
        }),
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

/// HTTP client errors preserve whether a procedure result is ambiguous.
#[derive(Debug, Error)]
pub enum XrpcClientError<E> {
    #[error("failed to encode XRPC request: {0}")]
    Encode(String),
    #[error("XRPC transport failed: {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
    #[error("XRPC protocol response failed: {message}")]
    Protocol {
        message: String,
        outcome_unknown: bool,
    },
    #[error("XRPC domain error ({status}): {error:?}")]
    Domain {
        status: StatusCode,
        error: E,
        outcome_unknown: bool,
    },
    #[error("XRPC HTTP error ({status}): {body}")]
    Http {
        status: StatusCode,
        body: Value,
        outcome_unknown: bool,
    },
}

struct EncodedXrpcRequest {
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

fn encode_xrpc_request<P: Serialize>(
    operation: XrpcOperation,
    context: &RequestContext,
    metadata: &HttpMetadata,
    params: &P,
) -> Result<EncodedXrpcRequest, String> {
    if operation.kind == XrpcKind::Subscription {
        return Err("the unary HTTP adapter does not support subscriptions".to_owned());
    }
    let mut path = format!("xrpc/{}", operation.nsid);
    let body = match operation.input {
        HttpInput::Query => {
            let query = serde_urlencoded::to_string(params).map_err(|error| error.to_string())?;
            if !query.is_empty() {
                path.push('?');
                path.push_str(&query);
            }
            Vec::new()
        }
        HttpInput::JsonBody => serde_json::to_vec(params).map_err(|error| error.to_string())?,
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&context.request_id.to_string())
            .map_err(|error| error.to_string())?,
    );
    headers.insert(
        TRACE_ID_HEADER,
        HeaderValue::from_str(&context.trace_id.to_string()).map_err(|error| error.to_string())?,
    );
    headers.insert(
        SPAN_ID_HEADER,
        HeaderValue::from_str(&context.span_id.to_string()).map_err(|error| error.to_string())?,
    );
    if let Some(value) = context.parent_span_id {
        headers.insert(
            PARENT_SPAN_ID_HEADER,
            HeaderValue::from_str(&value.to_string()).map_err(|error| error.to_string())?,
        );
    }
    if let Some(value) = &metadata.authorization {
        headers.insert(
            AUTHORIZATION_HEADER,
            HeaderValue::from_str(value).map_err(|error| error.to_string())?,
        );
    }
    if let Some(value) = &metadata.idempotency_key {
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(value).map_err(|error| error.to_string())?,
        );
    }
    if let Some(value) = &metadata.traceparent {
        headers.insert(
            TRACEPARENT_HEADER,
            HeaderValue::from_str(value).map_err(|error| error.to_string())?,
        );
    }
    if let Some(value) = &metadata.tracestate {
        headers.insert(
            TRACESTATE_HEADER,
            HeaderValue::from_str(value).map_err(|error| error.to_string())?,
        );
    }
    if let Some(timeout) = metadata.timeout {
        headers.insert(
            TIMEOUT_HEADER,
            HeaderValue::from_str(&timeout.as_millis().to_string())
                .map_err(|error| error.to_string())?,
        );
    }
    if operation.input == HttpInput::JsonBody {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    Ok(EncodedXrpcRequest {
        path,
        headers,
        body,
    })
}

/// An HTTP XRPC client.  Its base URL can later be backed by an Iroh HTTP
/// connector without changing operation declarations or handler contracts.
#[derive(Clone, Debug)]
pub struct XrpcClient {
    client: reqwest::Client,
    base_url: Url,
    max_response_bytes: usize,
}

impl XrpcClient {
    pub fn new(base_url: Url, max_response_bytes: usize) -> Result<Self, XrpcClientError<Value>> {
        if max_response_bytes == 0 {
            return Err(XrpcClientError::Encode(
                "response body limit must be positive".to_owned(),
            ));
        }
        Ok(Self {
            client: reqwest::Client::builder().build().map_err(|error| {
                XrpcClientError::Transport {
                    message: error.to_string(),
                    outcome_unknown: false,
                }
            })?,
            base_url,
            max_response_bytes,
        })
    }

    pub fn with_client(
        client: reqwest::Client,
        base_url: Url,
        max_response_bytes: usize,
    ) -> Result<Self, XrpcClientError<Value>> {
        if max_response_bytes == 0 {
            return Err(XrpcClientError::Encode(
                "response body limit must be positive".to_owned(),
            ));
        }
        Ok(Self {
            client,
            base_url,
            max_response_bytes,
        })
    }

    pub async fn execute<P, O, E>(
        &self,
        operation: XrpcOperation,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<O, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        if operation.kind == XrpcKind::Subscription {
            return Err(XrpcClientError::Protocol {
                message: "the unary HTTP adapter does not support subscriptions".to_owned(),
                outcome_unknown: false,
            });
        }
        let encoded = encode_xrpc_request(operation, &context, metadata, params)
            .map_err(XrpcClientError::Encode)?;
        let url = self
            .base_url
            .join(&encoded.path)
            .map_err(|error| XrpcClientError::Encode(error.to_string()))?;
        let mut request = match operation.method {
            HttpMethod::Get => self.client.get(url),
            HttpMethod::Post => self.client.post(url),
        }
        .headers(encoded.headers)
        .body(encoded.body);
        if let Some(timeout) = metadata.timeout {
            request = request.timeout(timeout);
        }
        let response = request
            .send()
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            })?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let body = read_response_body(response, self.max_response_bytes)
            .await
            .map_err(|(message, outcome_unknown)| XrpcClientError::Protocol {
                message,
                outcome_unknown: outcome_unknown || operation.kind == XrpcKind::Procedure,
            })?;
        decode_xrpc_response(
            operation,
            context.request_id,
            status,
            &response_headers,
            &body,
        )
    }
}

fn decode_xrpc_response<O, E>(
    operation: XrpcOperation,
    request_id: uuid::Uuid,
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<O, XrpcClientError<E>>
where
    O: DeserializeOwned,
    E: DeserializeOwned,
{
    if let Err(message) = response_request_id(headers, request_id) {
        return Err(XrpcClientError::Protocol {
            message,
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        });
    }
    let value =
        serde_json::from_slice::<Value>(body).map_err(|error| XrpcClientError::Protocol {
            message: format!("invalid JSON XRPC response: {error}"),
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        })?;
    if status.is_success() {
        return serde_json::from_value(value).map_err(|error| XrpcClientError::Protocol {
            message: format!("invalid XRPC success payload: {error}"),
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        });
    }
    if let Err(error) = serde_json::from_value::<XrpcErrorEnvelope>(value.clone()) {
        return Err(XrpcClientError::Protocol {
            message: format!("invalid XRPC error envelope: {error}"),
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        });
    }
    match serde_json::from_value::<E>(value.clone()) {
        Ok(error) => Err(XrpcClientError::Domain {
            status,
            error,
            outcome_unknown: operation.kind == XrpcKind::Procedure && status.is_server_error(),
        }),
        Err(_) => Err(XrpcClientError::Http {
            status,
            body: value,
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        }),
    }
}

fn response_request_id(headers: &HeaderMap, expected: uuid::Uuid) -> Result<(), String> {
    let Some(value) = headers.get(REQUEST_ID_HEADER) else {
        // x-request-id is an optional interoperability aid. External XRPC
        // servers that do not negotiate this extension remain valid peers.
        return Ok(());
    };
    let value = value
        .to_str()
        .map_err(|_| "XRPC response x-request-id was not valid ASCII".to_owned())?;
    let actual = value
        .parse::<uuid::Uuid>()
        .map_err(|_| "XRPC response x-request-id was not a UUID".to_owned())?;
    if actual != expected {
        return Err("XRPC response x-request-id did not match the request".to_owned());
    }
    Ok(())
}

async fn read_response_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, (String, bool)> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err((
            "XRPC response body exceeds configured limit".to_owned(),
            false,
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| (error.to_string(), true))?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err((
                "XRPC response body exceeds configured limit".to_owned(),
                true,
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

type AcceptFuture<'a> = Pin<
    Box<
        dyn Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>
            + Send
            + 'a,
    >,
>;

/// A production accept driver. Tests can provide a deterministic driver to
/// exercise listener failures without replacing the connection lifecycle.
trait ConnectionAcceptor: Send {
    fn accept(&mut self) -> AcceptFuture<'_>;
}

struct TcpConnectionAcceptor {
    listener: tokio::net::TcpListener,
}

impl ConnectionAcceptor for TcpConnectionAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(self.listener.accept())
    }
}

/// Starts a loopback-friendly HTTP server and returns its owned task and
/// shutdown sender. Production callers can instead attach `into_router()` to
/// an Iroh HTTP listener.
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    serve_with_connection_limit(listener, router, 1024).await
}

/// Starts the unary server with an independent bound on accepted/idle
/// connections. Handler admission and connection admission are separate: an
/// idle peer cannot consume a handler permit, and excess peers wait at the
/// listener boundary instead of being accepted and immediately closed.
pub async fn serve_with_connection_limit(
    listener: tokio::net::TcpListener,
    router: Router,
    max_connections: usize,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    assert!(
        max_connections > 0,
        "XRPC connection limit must be positive"
    );
    serve_with_acceptor(TcpConnectionAcceptor { listener }, router, max_connections).await
}

async fn serve_with_acceptor<A: ConnectionAcceptor + 'static>(
    mut acceptor: A,
    router: Router,
    max_connections: usize,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let connection_admission = Arc::new(Semaphore::new(max_connections));
        let mut connections = JoinSet::new();
        let mut shutdown_receiver = shutdown_receiver;
        loop {
            let accept = accept_with_permit(&mut acceptor, Arc::clone(&connection_admission));
            tokio::pin!(accept);
            if connections.is_empty() {
                tokio::select! {
                    accepted = &mut accept => {
                        match accepted {
                            (permit, Ok((stream, _))) => spawn_connection(&mut connections, router.clone(), permit, stream),
                            (permit, Err(error)) => {
                                drop(permit);
                                return abort_and_reap(&mut connections, error).await;
                            }
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        return reap_shutdown_connections(&mut connections).await;
                    }
                }
            } else {
                tokio::select! {
                    accepted = &mut accept => {
                        match accepted {
                            (permit, Ok((stream, _))) => spawn_connection(&mut connections, router.clone(), permit, stream),
                            (permit, Err(error)) => {
                                drop(permit);
                                return abort_and_reap(&mut connections, error).await;
                            }
                        }
                    }
                    joined = connections.join_next() => {
                        if let Some(joined) = joined
                            && let Some(error) = observe_connection(joined)
                        {
                            connections.abort_all();
                            if let Err(reap_error) = reap_shutdown_connections(&mut connections).await {
                                panic!("XRPC connection reaping failed after task error: {reap_error}");
                            }
                            return Err(error);
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        return reap_shutdown_connections(&mut connections).await;
                    }
                }
            }
        }
    });
    Ok((task, shutdown_sender))
}

async fn accept_with_permit<A: ConnectionAcceptor>(
    acceptor: &mut A,
    connection_admission: Arc<Semaphore>,
) -> (
    OwnedSemaphorePermit,
    std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
) {
    let permit = connection_admission
        .acquire_owned()
        .await
        .expect("connection admission semaphore cannot close");
    (permit, acceptor.accept().await)
}

fn spawn_connection(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
    router: Router,
    connection_permit: OwnedSemaphorePermit,
    stream: tokio::net::TcpStream,
) {
    connections.spawn(async move {
        let _connection_permit = connection_permit;
        let service = TowerToHyperService::new(router);
        http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
    });
}

async fn abort_and_reap(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
    listener_error: std::io::Error,
) -> Result<(), std::io::Error> {
    if let Err(reap_error) = reap_shutdown_connections(connections).await {
        panic!("XRPC connection reaping failed after listener error: {reap_error}");
    }
    Err(listener_error)
}

fn observe_connection(
    joined: Result<Result<(), hyper::Error>, tokio::task::JoinError>,
) -> Option<std::io::Error> {
    match joined {
        Ok(Ok(())) => None,
        Ok(Err(error)) if expected_peer_error(&error) => {
            tracing::debug!(%error, "XRPC peer connection closed");
            None
        }
        Ok(Err(error)) => Some(std::io::Error::other(error.to_string())),
        Err(error) if error.is_cancelled() => None,
        Err(error) => panic!("XRPC connection task failed: {error}"),
    }
}

fn expected_peer_error(error: &hyper::Error) -> bool {
    error.is_closed()
        || error.is_canceled()
        || error.is_incomplete_message()
        || error.is_body_write_aborted()
        || error.is_shutdown()
}

async fn reap_shutdown_connections(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
) -> Result<(), std::io::Error> {
    connections.abort_all();
    let mut first_error = None;
    while let Some(joined) = connections.join_next().await {
        if let Some(error) = observe_connection(joined) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(feature = "iroh-http")]
pub const IROH_XRPC_HTTP_ALPN: &[u8] = b"townframe/xrpc-http/1";

#[cfg(feature = "iroh-http")]
struct IrohHttpStream {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

#[cfg(feature = "iroh-http")]
impl IrohHttpStream {
    fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
        Self { send, recv }
    }
}

#[cfg(feature = "iroh-http")]
impl tokio::io::AsyncRead for IrohHttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

#[cfg(feature = "iroh-http")]
impl tokio::io::AsyncWrite for IrohHttpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_flush(cx)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_shutdown(cx)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[cfg(feature = "iroh-http")]
#[derive(Clone)]
pub struct IrohXrpcProtocolHandler {
    router: Router,
    connection_admission: Arc<Semaphore>,
    stream_admission: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
    shutdown_state: watch::Receiver<bool>,
}

#[cfg(feature = "iroh-http")]
impl std::fmt::Debug for IrohXrpcProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohXrpcProtocolHandler")
            .field(
                "connection_admission",
                &self.connection_admission.available_permits(),
            )
            .field(
                "stream_admission",
                &self.stream_admission.available_permits(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "iroh-http")]
async fn serve_iroh_stream(
    router: Router,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
) -> Result<(), hyper::Error> {
    // Hyper may be waiting on an entered handler and therefore not poll the
    // transport for a peer reset.  Keep the send-side stop notification beside
    // the owned Hyper future so a client stream reset cancels that handler.
    let stopped = send.stopped();
    let io = hyper_util::rt::TokioIo::new(IrohHttpStream::new(send, recv));
    tokio::select! {
        result = hyper::server::conn::http1::Builder::new()
            // HTTP/1 is deliberately one exchange per QUIC bi-stream.  The
            // QUIC connection itself remains reusable and is owned by the client.
            .keep_alive(false)
            .serve_connection(io, hyper_util::service::TowerToHyperService::new(router)) => result,
        _ = stopped => Ok(()),
    }
}

#[cfg(feature = "iroh-http")]
impl iroh::protocol::ProtocolHandler for IrohXrpcProtocolHandler {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let _connection_permit = Arc::clone(&self.connection_admission)
            .try_acquire_owned()
            .map_err(|_| {
                iroh::protocol::AcceptError::from_err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Iroh connection admission is full",
                ))
            })?;
        let mut shutdown = self.shutdown_state.clone();
        let mut streams = JoinSet::new();
        let mut first_error = None;

        'accept: loop {
            tokio::select! {
                biased;
                joined = streams.join_next(), if !streams.is_empty() => {
                    match joined {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) if expected_peer_error(&error) => {}
                        Some(Ok(Err(error))) => {
                            first_error.get_or_insert(error.to_string());
                            streams.abort_all();
                            break;
                        }
                        Some(Err(error)) if !error.is_cancelled() => {
                            first_error.get_or_insert(error.to_string());
                            streams.abort_all();
                            break;
                        }
                        Some(Err(_)) => {}
                        None => unreachable!("guarded JoinSet branch cannot be empty"),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        streams.abort_all();
                        break;
                    }
                }
                closed = connection.closed() => {
                    drop(closed);
                    // The peer closed the connection.  Reset any handler
                    // futures still owned by this connection and join them
                    // before releasing its permit.
                    streams.abort_all();
                    break;
                }
                accepted = connection.accept_bi() => {
                    let (send, recv) = match accepted {
                        Ok(stream) => stream,
                        Err(_) => {
                            // The peer closed the connection.  Reset any
                            // handler futures still owned by this connection
                            // and join them before releasing its permit.
                            streams.abort_all();
                            break;
                        }
                    };
                    let permit = loop {
                        tokio::select! {
                            biased;
                            joined = streams.join_next(), if !streams.is_empty() => {
                                match joined {
                                    Some(Ok(Ok(()))) => {}
                                    Some(Ok(Err(error))) if expected_peer_error(&error) => {}
                                    Some(Ok(Err(error))) => {
                                        first_error.get_or_insert(error.to_string());
                                        streams.abort_all();
                                        break 'accept;
                                    }
                                    Some(Err(error)) if !error.is_cancelled() => {
                                        first_error.get_or_insert(error.to_string());
                                        streams.abort_all();
                                        break 'accept;
                                    }
                                    Some(Err(_)) => {}
                                    None => unreachable!("guarded JoinSet branch cannot be empty"),
                                }
                            }
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    streams.abort_all();
                                    break 'accept;
                                }
                            }
                            closed = connection.closed() => {
                                drop(closed);
                                streams.abort_all();
                                break 'accept;
                            }
                            permit = Arc::clone(&self.stream_admission).acquire_owned() => {
                                break permit.map_err(|_| iroh::protocol::AcceptError::from_err(
                                    std::io::Error::other("Iroh stream admission closed"),
                                ))?;
                            }
                        }
                    };
                    let router = self.router.clone();
                    streams.spawn(async move {
                        let result = serve_iroh_stream(router, send, recv).await;
                        drop(permit);
                        result
                    });
                }
            }
        }

        while let Some(joined) = streams.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) if expected_peer_error(&error) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error.to_string());
                }
                Err(error) if !error.is_cancelled() => {
                    first_error.get_or_insert(error.to_string());
                }
                Err(_) => {}
            }
        }
        first_error.map_or(Ok(()), |error| {
            Err(iroh::protocol::AcceptError::from_err(
                std::io::Error::other(error),
            ))
        })
    }

    async fn shutdown(&self) {
        self.shutdown
            .send(true)
            .expect("Iroh protocol shutdown receiver must remain open");
    }
}

/// Creates the HTTP/1 protocol handler for registration on an embedder-owned
/// Iroh [`iroh::protocol::RouterBuilder`].  The caller must register this
/// handler on the same builder as its other ALPNs and own the resulting
/// [`iroh::protocol::Router`].  This function never changes endpoint ALPNs or
/// starts a competing accept loop.
#[cfg(feature = "iroh-http")]
pub fn iroh_protocol_handler(
    router: Router,
    max_connections: usize,
    max_streams: usize,
) -> IrohXrpcProtocolHandler {
    assert!(
        max_connections > 0,
        "Iroh connection limit must be positive"
    );
    assert!(max_streams > 0, "Iroh stream limit must be positive");
    let (shutdown, shutdown_state) = watch::channel(false);
    IrohXrpcProtocolHandler {
        router,
        connection_admission: Arc::new(Semaphore::new(max_connections)),
        stream_admission: Arc::new(Semaphore::new(max_streams)),
        shutdown,
        shutdown_state,
    }
}

#[cfg(feature = "iroh-http")]
#[derive(Clone)]
pub struct IrohXrpcClient {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    max_response_bytes: usize,
    connection: Arc<tokio::sync::Mutex<IrohClientConnection>>,
}

#[cfg(feature = "iroh-http")]
struct IrohClientConnection {
    connection: Option<iroh::endpoint::Connection>,
    generation: u64,
}

#[cfg(feature = "iroh-http")]
impl std::fmt::Debug for IrohXrpcClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohXrpcClient")
            .field("endpoint_addr", &self.endpoint_addr)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "iroh-http")]
struct IrohConnectionTaskGuard(Option<tokio::task::JoinHandle<Result<(), hyper::Error>>>);

#[cfg(feature = "iroh-http")]
impl Drop for IrohConnectionTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            // A cancelled request cannot await its stream task. Abort only this
            // stream's Hyper future; the shared QUIC connection remains alive.
            task.abort();
        }
    }
}

#[cfg(feature = "iroh-http")]
impl IrohConnectionTaskGuard {
    async fn join(mut self) -> Result<(), String> {
        let task = self
            .0
            .take()
            .expect("Iroh connection task guard must own its task");
        let result = task.await.map_err(|error| error.to_string())?;
        match result {
            Ok(()) => Ok(()),
            Err(error)
                if error.is_closed()
                    || error.is_canceled()
                    || error.is_incomplete_message()
                    || error.is_shutdown() =>
            {
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(feature = "iroh-http")]
async fn collect_iroh_response_body(
    mut body: hyper::body::Incoming,
    max_response_bytes: usize,
) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| error.to_string())?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > max_response_bytes {
            return Err("XRPC response body exceeds configured limit".to_owned());
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}

#[cfg(feature = "iroh-http")]
impl IrohXrpcClient {
    pub fn new(
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        max_response_bytes: usize,
    ) -> Result<Self, XrpcClientError<Value>> {
        if max_response_bytes == 0 {
            return Err(XrpcClientError::Encode(
                "response body limit must be positive".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            endpoint_addr,
            max_response_bytes,
            connection: Arc::new(tokio::sync::Mutex::new(IrohClientConnection {
                connection: None,
                generation: 0,
            })),
        })
    }

    async fn connection(&self) -> Result<(iroh::endpoint::Connection, u64), String> {
        let mut state = self.connection.lock().await;
        if let Some(connection) = &state.connection {
            if connection.close_reason().is_none() {
                return Ok((connection.clone(), state.generation));
            }
            state.connection = None;
        }
        let connection = self
            .endpoint
            .connect(self.endpoint_addr.clone(), IROH_XRPC_HTTP_ALPN)
            .await
            .map_err(|error| error.to_string())?;
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        state.connection = Some(connection.clone());
        Ok((connection, generation))
    }

    async fn discard_connection(&self, generation: u64) {
        let mut state = self.connection.lock().await;
        if state.generation == generation {
            state.connection = None;
        }
    }

    pub async fn execute<P, O, E>(
        &self,
        operation: XrpcOperation,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<O, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        let outcome_unknown = operation.kind == XrpcKind::Procedure;
        if let Some(timeout) = metadata.timeout {
            match tokio::time::timeout(
                timeout,
                self.execute_inner(operation, context, metadata, params),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(XrpcClientError::Transport {
                    message: "Iroh XRPC request exceeded its time budget".to_owned(),
                    outcome_unknown,
                }),
            }
        } else {
            self.execute_inner(operation, context, metadata, params)
                .await
        }
    }

    async fn execute_inner<P, O, E>(
        &self,
        operation: XrpcOperation,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<O, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        let encoded = encode_xrpc_request(operation, &context, metadata, params)
            .map_err(XrpcClientError::Encode)?;
        let (connection, generation) =
            self.connection()
                .await
                .map_err(|message| XrpcClientError::Transport {
                    message,
                    outcome_unknown: operation.kind == XrpcKind::Procedure,
                })?;
        let (send, recv) = match connection.open_bi().await {
            Ok(stream) => stream,
            Err(error) => {
                // Do not retry this request: a procedure may already have
                // crossed its ambiguity boundary.  A later request can make
                // one fresh connection attempt instead.
                self.discard_connection(generation).await;
                return Err(XrpcClientError::Transport {
                    message: error.to_string(),
                    outcome_unknown: operation.kind == XrpcKind::Procedure,
                });
            }
        };
        let io = hyper_util::rt::TokioIo::new(IrohHttpStream::new(send, recv));
        let (mut sender, connection_task) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            })?;
        let connection_task = IrohConnectionTaskGuard(Some(tokio::spawn(connection_task)));
        let method = match operation.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
        };
        let mut request = hyper::Request::builder()
            .method(method)
            .uri(format!("/{}", encoded.path))
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                encoded.body,
            )))
            .map_err(|error| XrpcClientError::Encode(error.to_string()))?;
        *request.headers_mut() = encoded.headers;
        // One HTTP exchange per QUIC bi-stream; this ends the stream task while
        // leaving the shared QUIC connection available for another bi-stream.
        request
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        let response =
            sender
                .send_request(request)
                .await
                .map_err(|error| XrpcClientError::Transport {
                    message: error.to_string(),
                    outcome_unknown: operation.kind == XrpcKind::Procedure,
                })?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = collect_iroh_response_body(response.into_body(), self.max_response_bytes)
            .await
            .map_err(|message| XrpcClientError::Protocol {
                message,
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            })?;
        connection_task
            .join()
            .await
            .map_err(|message| XrpcClientError::Transport {
                message: format!("connection task: {message}"),
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            })?;
        if connection.close_reason().is_some() {
            self.discard_connection(generation).await;
        }
        decode_xrpc_response(operation, context.request_id, status, &headers, &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::net::{TcpListener, TcpStream};

    const QUERY_OPERATION: XrpcOperation = XrpcOperation {
        nsid: "org.example.test.echo",
        kind: XrpcKind::Query,
        method: HttpMethod::Get,
        input: HttpInput::Query,
        output_schema: "EchoOutput",
        error_schema: "TestError",
    };
    const PROCEDURE_OPERATION: XrpcOperation = XrpcOperation {
        nsid: "org.example.test.write",
        kind: XrpcKind::Procedure,
        method: HttpMethod::Post,
        input: HttpInput::JsonBody,
        output_schema: "WriteOutput",
        error_schema: "TestError",
    };
    const NUMERIC_QUERY_OPERATION: XrpcOperation = XrpcOperation {
        nsid: "org.example.test.numeric",
        kind: XrpcKind::Query,
        method: HttpMethod::Get,
        input: HttpInput::Query,
        output_schema: "NumericOutput",
        error_schema: "TestError",
    };

    #[derive(Debug, Deserialize, Serialize)]
    struct EchoParams {
        value: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct NumericParams {
        count: u64,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct NumericOutput {
        doubled: u64,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct Output {
        value: String,
        request_id: uuid::Uuid,
        authorization: Option<String>,
        idempotency_key: Option<String>,
        timeout_millis: Option<u64>,
        traceparent: Option<String>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct TestError {
        error: String,
        message: String,
    }

    #[derive(Debug, Deserialize)]
    struct OptionalTestError {
        error: String,
        message: Option<String>,
        field: String,
    }

    struct ScriptedAcceptor {
        listener: TcpListener,
        calls: Arc<AtomicUsize>,
        first_accepted: Option<oneshot::Sender<()>>,
        failure: Option<oneshot::Receiver<()>>,
    }

    impl ConnectionAcceptor for ScriptedAcceptor {
        fn accept(&mut self) -> AcceptFuture<'_> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            if call == 0 {
                let listener = &mut self.listener;
                let first_accepted = self.first_accepted.take();
                Box::pin(async move {
                    let accepted = listener.accept().await?;
                    if let Some(sender) = first_accepted {
                        sender.send(()).expect("first accept receiver");
                    }
                    Ok(accepted)
                })
            } else if let Some(failure) = self.failure.take() {
                Box::pin(async move {
                    failure.await.map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "accept script cancelled",
                        )
                    })?;
                    Err(std::io::Error::other("scripted listener failure"))
                })
            } else {
                Box::pin(async { Err(std::io::Error::other("unexpected scripted accept")) })
            }
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    struct StreamDrop(Option<oneshot::Sender<()>>);

    impl Drop for StreamDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                sender.send(()).expect("stream drop receiver");
            }
        }
    }

    async fn test_server() -> (
        XrpcClient,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ) {
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::typed(
                QUERY_OPERATION,
                |context, metadata, params: EchoParams| async move {
                    Ok(Output {
                        value: params.value,
                        request_id: context.request_id,
                        authorization: metadata.authorization,
                        idempotency_key: metadata.idempotency_key,
                        timeout_millis: metadata.timeout.map(|timeout| timeout.as_millis() as u64),
                        traceparent: metadata.traceparent,
                    })
                },
            ))
            .expect("query registration");
        router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                |context, metadata, params: EchoParams| async move {
                    if params.value == "pending" {
                        return std::future::pending::<Result<Output, XrpcHandlerError>>().await;
                    } else if params.value == "bad" {
                        return Err(XrpcHandlerError::domain(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "InvalidValue",
                            "bad value",
                        ));
                    }
                    Ok(Output {
                        value: params.value,
                        request_id: context.request_id,
                        authorization: metadata.authorization,
                        idempotency_key: metadata.idempotency_key,
                        timeout_millis: metadata.timeout.map(|timeout| timeout.as_millis() as u64),
                        traceparent: metadata.traceparent,
                    })
                },
            ))
            .expect("procedure registration");
        router
            .register(XrpcOperationHandler::typed(
                NUMERIC_QUERY_OPERATION,
                |_context, _metadata, params: NumericParams| async move {
                    Ok(NumericOutput {
                        doubled: params.count * 2,
                    })
                },
            ))
            .expect("numeric query registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve(listener, router.into_router())
            .await
            .expect("start HTTP server");
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        (client, task, shutdown)
    }

    #[cfg(feature = "iroh-http")]
    const COEXISTING_ALPN: &[u8] = b"townframe/test-coexisting/1";

    #[cfg(feature = "iroh-http")]
    #[derive(Debug)]
    struct CoexistingProtocolHandler(Arc<AtomicUsize>);

    #[cfg(feature = "iroh-http")]
    impl iroh::protocol::ProtocolHandler for CoexistingProtocolHandler {
        async fn accept(
            &self,
            connection: iroh::endpoint::Connection,
        ) -> Result<(), iroh::protocol::AcceptError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            let (mut send, mut recv) = connection.accept_bi().await?;
            tokio::io::copy(&mut recv, &mut send).await?;
            send.finish()?;
            connection.closed().await;
            Ok(())
        }
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_registration_coexists_on_embedder_router() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let xrpc_router = XrpcRouter::new(1024).into_router();
        let other_accepts = Arc::new(AtomicUsize::new(0));
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                COEXISTING_ALPN,
                CoexistingProtocolHandler(Arc::clone(&other_accepts)),
            )
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(xrpc_router, 1, 2),
            )
            .spawn();

        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let connection = client_endpoint
            .connect(server_endpoint.addr(), COEXISTING_ALPN)
            .await
            .expect("coexisting protocol connection");
        let (mut send, mut recv) = connection.open_bi().await.expect("coexisting stream");
        send.write_all(b"coexisting")
            .await
            .expect("coexisting write");
        send.finish().expect("coexisting finish");
        let response = recv.read_to_end(1024).await.expect("coexisting response");
        assert_eq!(response, b"coexisting");
        assert_eq!(other_accepts.load(Ordering::Acquire), 1);

        server_router.shutdown().await.expect("router shutdown");
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[cfg(feature = "iroh-http")]
    async fn iroh_test_endpoint() -> Result<iroh::Endpoint, Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?)
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_query_procedure_and_domain_error_use_shared_wire() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::typed(
                QUERY_OPERATION,
                |_context, metadata, params: EchoParams| async move {
                    Ok(Output {
                        value: params.value,
                        request_id: RequestContext::root().request_id,
                        authorization: metadata.authorization,
                        idempotency_key: metadata.idempotency_key,
                        timeout_millis: metadata.timeout.map(|value| value.as_millis() as u64),
                        traceparent: metadata.traceparent,
                    })
                },
            ))
            .expect("query registration");
        let deadline_started = Arc::new(tokio::sync::Notify::new());
        let handler_deadline_started = Arc::clone(&deadline_started);
        router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                move |_context, _metadata, params: EchoParams| {
                    let handler_deadline_started = Arc::clone(&handler_deadline_started);
                    async move {
                        if params.value == "bad" {
                            return Err(XrpcHandlerError::domain(
                                StatusCode::UNPROCESSABLE_ENTITY,
                                "InvalidValue",
                                "bad value",
                            ));
                        }
                        if params.value == "pending" {
                            handler_deadline_started.notify_one();
                            drop(std::future::pending::<Result<Output, XrpcHandlerError>>().await);
                        }
                        Ok(Output {
                            value: if params.value == "large" {
                                "x".repeat(8192)
                            } else {
                                params.value
                            },
                            request_id: RequestContext::root().request_id,
                            authorization: None,
                            idempotency_key: None,
                            timeout_millis: None,
                            traceparent: None,
                        })
                    }
                },
            ))
            .expect("procedure registration");
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(router.into_router(), 1, 8),
            )
            .spawn();
        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
            .expect("Iroh client");
        let context = RequestContext::root();
        // Both exchanges must share the single admitted QUIC connection.  The
        // server allows only one connection, so a per-RPC connect path would
        // reject one of these concurrent streams.
        let concurrent_metadata = HttpMetadata::default();
        let concurrent_query_params = EchoParams {
            value: "same-connection-query".to_owned(),
        };
        let concurrent_procedure_params = EchoParams {
            value: "same-connection-procedure".to_owned(),
        };
        let concurrent_query_client = client.clone();
        let concurrent_procedure_client = client.clone();
        let concurrent_query = concurrent_query_client.execute::<EchoParams, Output, TestError>(
            QUERY_OPERATION,
            context.clone(),
            &concurrent_metadata,
            &concurrent_query_params,
        );
        let concurrent_procedure = concurrent_procedure_client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                context.clone(),
                &concurrent_metadata,
                &concurrent_procedure_params,
            );
        let (concurrent_query, concurrent_procedure) =
            tokio::join!(concurrent_query, concurrent_procedure);
        assert_eq!(
            concurrent_query.expect("concurrent query").value,
            "same-connection-query"
        );
        assert_eq!(
            concurrent_procedure.expect("concurrent procedure").value,
            "same-connection-procedure"
        );
        let query: Output = client
            .execute::<EchoParams, Output, TestError>(
                QUERY_OPERATION,
                context.clone(),
                &HttpMetadata {
                    authorization: Some("Capability iroh".to_owned()),
                    timeout: Some(Duration::from_millis(500)),
                    ..HttpMetadata::default()
                },
                &EchoParams {
                    value: "iroh-query".to_owned(),
                },
            )
            .await
            .expect("Iroh query");
        assert_eq!(query.value, "iroh-query");
        assert_eq!(query.authorization.as_deref(), Some("Capability iroh"));
        assert_eq!(query.timeout_millis, Some(500));
        // Enter the handler before waiting for the server's declared
        // deadline.  Calling execute_inner bypasses the client's outer timer,
        // so this proves the server timeout boundary and its exact 504 mapping
        // rather than whichever side won a nanosecond race.
        let deadline_context = context.clone();
        let deadline_client = client.clone();
        let deadline_request = tokio::spawn(async move {
            deadline_client
                .execute_inner::<EchoParams, Output, TestError>(
                    PROCEDURE_OPERATION,
                    deadline_context,
                    &HttpMetadata {
                        timeout: Some(Duration::from_millis(25)),
                        ..HttpMetadata::default()
                    },
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        deadline_started.notified().await;
        let deadline = deadline_request.await.expect("deadline request join");
        assert!(matches!(
            deadline,
            Err(XrpcClientError::Domain {
                status: StatusCode::GATEWAY_TIMEOUT,
                outcome_unknown: true,
                ..
            })
        ));
        let limited: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                context.clone(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "large".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            limited,
            Err(XrpcClientError::Protocol {
                outcome_unknown: true,
                ..
            })
        ));
        let error: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                context,
                &HttpMetadata::default(),
                &EchoParams {
                    value: "bad".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            error,
            Err(XrpcClientError::Domain {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                outcome_unknown: false,
                error: TestError { message, .. },
            }) if message == "bad value"
        ));
        server_router
            .shutdown()
            .await
            .expect("Iroh router shutdown");
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_chunked_request_limit_is_cumulative_across_wire_frames() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let mut xrpc_router = XrpcRouter::new(8);
        xrpc_router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                |_context, _metadata, params: EchoParams| async move {
                    Ok(Output {
                        value: params.value,
                        request_id: RequestContext::root().request_id,
                        authorization: None,
                        idempotency_key: None,
                        timeout_millis: None,
                        traceparent: None,
                    })
                },
            ))
            .expect("procedure registration");
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(xrpc_router.into_router(), 1, 2),
            )
            .spawn();
        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let connection = client_endpoint
            .connect(server_endpoint.addr(), IROH_XRPC_HTTP_ALPN)
            .await
            .expect("Iroh connection");
        let (mut send, mut recv) = connection.open_bi().await.expect("Iroh stream");
        let request = format!(
            "POST /xrpc/{} HTTP/1.1\r\nHost: iroh\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nX-Request-ID: {}\r\n\r\n6\r\n{{\"valu\r\n7\r\ne\":\"x\"}}\r\n0\r\n\r\n",
            PROCEDURE_OPERATION.nsid,
            uuid::Uuid::new_v4(),
        );
        send.write_all(request.as_bytes())
            .await
            .expect("chunked request write");
        send.finish().expect("chunked request finish");
        let response = recv.read_to_end(4096).await.expect("chunked response");
        let response = String::from_utf8(response).expect("HTTP response");
        assert!(response.starts_with("HTTP/1.1 413"), "response: {response}");
        server_router.shutdown().await.expect("router shutdown");
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_client_disconnect_drops_entered_handler() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped_sender = Arc::new(Mutex::new(None));
        let (dropped_tx, dropped_rx) = oneshot::channel();
        *dropped_sender.lock().expect("drop sender lock") = Some(dropped_tx);
        let handler_started = Arc::clone(&started);
        let handler_dropped_sender = Arc::clone(&dropped_sender);
        let mut xrpc_router = XrpcRouter::new(1024);
        xrpc_router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                move |_context, _metadata, params: EchoParams| {
                    let handler_started = Arc::clone(&handler_started);
                    let handler_dropped_sender = Arc::clone(&handler_dropped_sender);
                    async move {
                        if params.value == "pending" {
                            handler_started.notify_one();
                            let _drop_guard = StreamDrop(
                                handler_dropped_sender
                                    .lock()
                                    .expect("drop sender lock")
                                    .take(),
                            );
                            std::future::pending::<Result<Output, XrpcHandlerError>>().await
                        } else {
                            Ok(Output {
                                value: params.value,
                                request_id: RequestContext::root().request_id,
                                authorization: None,
                                idempotency_key: None,
                                timeout_millis: None,
                                traceparent: None,
                            })
                        }
                    }
                },
            ))
            .expect("procedure registration");
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(xrpc_router.into_router(), 1, 2),
            )
            .spawn();
        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
            .expect("Iroh client");
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .execute::<EchoParams, Output, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        started.notified().await;
        let connection = client
            .connection
            .lock()
            .await
            .connection
            .clone()
            .expect("retained client connection");
        connection.close(0u32.into(), b"client disconnect");
        let result = request.await.expect("disconnected request join");
        assert!(matches!(
            result,
            Err(XrpcClientError::Transport {
                outcome_unknown: true,
                ..
            }) | Err(XrpcClientError::Protocol {
                outcome_unknown: true,
                ..
            })
        ));
        dropped_rx
            .await
            .expect("handler drop after client disconnect");
        server_router.shutdown().await.expect("router shutdown");
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_client_stream_cancel_drops_only_entered_handler() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let started = Arc::new(tokio::sync::Notify::new());
        let survivor_started = Arc::new(tokio::sync::Notify::new());
        let survivor_release = Arc::new(tokio::sync::Notify::new());
        let dropped_sender = Arc::new(Mutex::new(None));
        let (dropped_tx, dropped_rx) = oneshot::channel();
        *dropped_sender.lock().expect("drop sender lock") = Some(dropped_tx);
        let handler_started = Arc::clone(&started);
        let handler_survivor_started = Arc::clone(&survivor_started);
        let handler_survivor_release = Arc::clone(&survivor_release);
        let handler_dropped_sender = Arc::clone(&dropped_sender);
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                move |_context, _metadata, params: EchoParams| {
                    let handler_started = Arc::clone(&handler_started);
                    let handler_survivor_started = Arc::clone(&handler_survivor_started);
                    let handler_survivor_release = Arc::clone(&handler_survivor_release);
                    let handler_dropped_sender = Arc::clone(&handler_dropped_sender);
                    async move {
                        if params.value == "pending" {
                            handler_started.notify_one();
                            let _drop_guard = StreamDrop(
                                handler_dropped_sender
                                    .lock()
                                    .expect("drop sender lock")
                                    .take(),
                            );
                            std::future::pending::<Result<Output, XrpcHandlerError>>().await
                        } else if params.value == "survivor" {
                            handler_survivor_started.notify_one();
                            handler_survivor_release.notified().await;
                            Ok(Output {
                                value: params.value,
                                request_id: RequestContext::root().request_id,
                                authorization: None,
                                idempotency_key: None,
                                timeout_millis: None,
                                traceparent: None,
                            })
                        } else {
                            Ok(Output {
                                value: params.value,
                                request_id: RequestContext::root().request_id,
                                authorization: None,
                                idempotency_key: None,
                                timeout_millis: None,
                                traceparent: None,
                            })
                        }
                    }
                },
            ))
            .expect("procedure registration");
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(router.into_router(), 1, 2),
            )
            .spawn();
        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
            .expect("Iroh client");

        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .execute::<EchoParams, Output, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        started.notified().await;
        let retained_connection = client
            .connection
            .lock()
            .await
            .connection
            .clone()
            .expect("retained client connection");

        let survivor_started_wait = survivor_started.notified();
        let survivor_client = client.clone();
        let survivor_request = tokio::spawn(async move {
            survivor_client
                .execute::<EchoParams, Output, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "survivor".to_owned(),
                    },
                )
                .await
        });
        survivor_started_wait.await;

        // Cancel only the first HTTP exchange while the unrelated second
        // exchange is already inside its handler on the retained connection.
        request.abort();
        assert!(
            request
                .await
                .expect_err("cancelled request must not complete")
                .is_cancelled()
        );
        dropped_rx
            .await
            .expect("entered handler drop after stream cancellation");
        assert!(retained_connection.close_reason().is_none());

        survivor_release.notify_one();
        let completed: Output = survivor_request
            .await
            .expect("surviving request join")
            .expect("unrelated same-connection request");
        assert_eq!(completed.value, "survivor");

        server_router.shutdown().await.expect("router shutdown");
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[cfg(feature = "iroh-http")]
    #[tokio::test]
    async fn iroh_http1_shutdown_reaps_pending_exchange() {
        let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = Arc::clone(&started);
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::typed(
                PROCEDURE_OPERATION,
                move |_context, _metadata, params: EchoParams| {
                    let handler_started = Arc::clone(&handler_started);
                    async move {
                        if params.value == "pending" {
                            handler_started.notify_one();
                            std::future::pending::<Result<Output, XrpcHandlerError>>().await
                        } else {
                            Ok(Output {
                                value: params.value,
                                request_id: RequestContext::root().request_id,
                                authorization: None,
                                idempotency_key: None,
                                timeout_millis: None,
                                traceparent: None,
                            })
                        }
                    }
                },
            ))
            .expect("procedure registration");
        let server_router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                iroh_protocol_handler(router.into_router(), 1, 8),
            )
            .spawn();
        let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
        let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
            .expect("Iroh client");
        let started_wait = started.notified();
        let request = tokio::spawn(async move {
            client
                .execute::<EchoParams, Output, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        started_wait.await;
        server_router
            .shutdown()
            .await
            .expect("Iroh router shutdown");
        let result = request.await.expect("pending request join");
        assert!(matches!(
            result,
            Err(XrpcClientError::Transport {
                outcome_unknown: true,
                ..
            }) | Err(XrpcClientError::Protocol {
                outcome_unknown: true,
                ..
            })
        ));
        client_endpoint.close().await;
        server_endpoint.close().await;
    }

    #[tokio::test]
    async fn loopback_query_and_procedure_map_http_and_context() {
        let (client, task, shutdown) = test_server().await;
        assert_eq!(QUERY_OPERATION.kind, XrpcKind::Query);
        assert_eq!(QUERY_OPERATION.input, HttpInput::Query);
        assert_eq!(PROCEDURE_OPERATION.method, HttpMethod::Post);
        let context = RequestContext::root();
        let metadata = HttpMetadata {
            authorization: Some("Capability test".to_owned()),
            idempotency_key: Some("command-1".to_owned()),
            timeout: Some(Duration::from_millis(25)),
            traceparent: Some("00-trace-span-01".to_owned()),
            ..HttpMetadata::default()
        };
        let query: Output = client
            .execute::<EchoParams, Output, TestError>(
                QUERY_OPERATION,
                context.clone(),
                &metadata,
                &EchoParams {
                    value: "hello world".to_owned(),
                },
            )
            .await
            .expect("query response");
        assert_eq!(query.value, "hello world");
        assert_eq!(query.request_id, context.request_id);
        assert_eq!(query.authorization, metadata.authorization);
        assert_eq!(query.idempotency_key, metadata.idempotency_key);
        assert_eq!(query.timeout_millis, Some(25));
        assert_eq!(query.traceparent, metadata.traceparent);
        let numeric: NumericOutput = client
            .execute::<NumericParams, NumericOutput, TestError>(
                NUMERIC_QUERY_OPERATION,
                context.clone(),
                &HttpMetadata::default(),
                &NumericParams { count: 17 },
            )
            .await
            .expect("numeric query response");
        assert_eq!(numeric, NumericOutput { doubled: 34 });
        let procedure: Output = client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                context.clone(),
                &metadata,
                &EchoParams {
                    value: "written".to_owned(),
                },
            )
            .await
            .expect("procedure response");
        assert_eq!(procedure.value, "written");
        assert_eq!(procedure.request_id, context.request_id);
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn optional_error_message_decodes_on_public_client_path() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (body, expected_message, malformed) in [
            (r#"{"error":"NotReady","field":"queued"}"#, None, false),
            (
                r#"{"error":"NotReady","message":"try later","field":"queued"}"#,
                Some("try later"),
                false,
            ),
            (
                r#"{"error":"NotReady","message":7,"field":"queued"}"#,
                None,
                true,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let address = listener.local_addr().expect("listener address");
            let body = body.to_owned();
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("accept client");
                let mut request = [0_u8; 4096];
                let _bytes_read = stream.read(&mut request).await.expect("read request");
                let response = format!(
                    "HTTP/1.1 409 Conflict\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
                Ok::<(), std::io::Error>(())
            });
            let client = XrpcClient::new(
                Url::parse(&format!("http://{address}/")).expect("base URL"),
                1024,
            )
            .expect("client");
            let result: Result<Value, XrpcClientError<OptionalTestError>> = client
                .execute(
                    QUERY_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "query".to_owned(),
                    },
                )
                .await;
            if malformed {
                assert!(matches!(
                    result,
                    Err(XrpcClientError::Protocol {
                        outcome_unknown: false,
                        ..
                    })
                ));
            } else {
                match result {
                    Err(XrpcClientError::Domain {
                        error,
                        outcome_unknown: false,
                        ..
                    }) => {
                        assert_eq!(error.message.as_deref(), expected_message);
                        assert_eq!(error.error, "NotReady");
                        assert_eq!(error.field, "queued");
                    }
                    other => panic!("unexpected optional-message result: {other:?}"),
                }
            }
            task.await
                .expect("connection task join")
                .expect("connection task");
        }
    }

    #[tokio::test]
    async fn domain_errors_are_typed_and_malformed_query_is_rejected() {
        let (client, task, shutdown) = test_server().await;
        let error: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "bad".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            error,
            Err(XrpcClientError::Domain {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                error: TestError { message, .. },
                ..
            }) if message == "bad value"
        ));
        let malformed = client
            .client
            .get(
                client
                    .base_url
                    .join("xrpc/org.example.test.echo")
                    .expect("URL"),
            )
            .send()
            .await
            .expect("malformed query response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn response_body_limit_is_enforced() {
        let (client, task, shutdown) = test_server().await;
        let limited = XrpcClient::with_client(client.client.clone(), client.base_url.clone(), 2)
            .expect("limited client");
        let result: Result<Output, XrpcClientError<TestError>> = limited
            .execute(
                QUERY_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "large".to_owned(),
                },
            )
            .await;
        assert!(matches!(result, Err(XrpcClientError::Protocol { .. })));
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn loopback_connection_loss_marks_only_procedures_ambiguous() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            stream.shutdown().await.expect("close response connection");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let query: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                QUERY_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "query".to_owned(),
                },
            )
            .await;
        let procedure: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "procedure".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            query,
            Err(XrpcClientError::Transport {
                outcome_unknown: false,
                ..
            })
        ));
        assert!(matches!(
            procedure,
            Err(XrpcClientError::Transport {
                outcome_unknown: true,
                ..
            })
        ));
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn malformed_procedure_error_body_is_ambiguous_protocol_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            stream
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 9\r\nContent-Type: application/json\r\n\r\nnot-json",
                )
                .await
                .expect("write malformed response");
            stream.shutdown().await.expect("close response connection");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "procedure".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(XrpcClientError::Protocol {
                outcome_unknown: true,
                ..
            })
        ));
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn method_body_and_request_body_limits_are_explicit() {
        let (client, task, shutdown) = test_server().await;
        let wrong_method = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.echo")
                    .expect("URL"),
            )
            .body("{}")
            .send()
            .await
            .expect("method response");
        assert_eq!(wrong_method.status(), StatusCode::METHOD_NOT_ALLOWED);
        let missing_media_type = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .body("{}")
            .send()
            .await
            .expect("media type response");
        assert_eq!(
            missing_media_type.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        let parameterized_body = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "Application/JSON; charset=utf-8")
            .body(r#"{"value":"parameterized"}"#)
            .send()
            .await
            .expect("parameterized media type response");
        assert_eq!(parameterized_body.status(), StatusCode::OK);
        let suffix_body = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "application/jsonp")
            .body("{}")
            .send()
            .await
            .expect("suffix media type response");
        assert_eq!(suffix_body.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let mut duplicate_headers = HeaderMap::new();
        duplicate_headers.append(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        duplicate_headers.append(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let duplicate_parameter = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(
                header::CONTENT_TYPE,
                "application/json; charset=utf-8; charset=utf-8",
            )
            .body("{}")
            .send()
            .await
            .expect("duplicate parameter response");
        assert_eq!(
            duplicate_parameter.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        let duplicate_body = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .headers(duplicate_headers)
            .body("{}")
            .send()
            .await
            .expect("duplicate media type response");
        assert_eq!(duplicate_body.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let malformed_media_type = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "application/json; charset")
            .body("{}")
            .send()
            .await
            .expect("malformed media type response");
        assert_eq!(
            malformed_media_type.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        let malformed_body = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body("not json")
            .send()
            .await
            .expect("body response");
        assert_eq!(malformed_body.status(), StatusCode::BAD_REQUEST);
        let oversized_body = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body("x".repeat(2048))
            .send()
            .await
            .expect("oversized body response");
        assert_eq!(oversized_body.status(), StatusCode::PAYLOAD_TOO_LARGE);
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn server_timeout_stops_handler_before_it_can_complete() {
        let (client, task, shutdown) = test_server().await;
        let response = client
            .client
            .post(
                client
                    .base_url
                    .join("xrpc/org.example.test.write")
                    .expect("URL"),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .header(TIMEOUT_HEADER, "0")
            .json(&EchoParams {
                value: "pending".to_owned(),
            })
            .send()
            .await
            .expect("deadline response");
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let envelope: XrpcErrorEnvelope = response.json().await.expect("error envelope");
        assert_eq!(
            envelope,
            XrpcErrorEnvelope {
                error: "DeadlineExceeded".to_owned(),
                message: Some("XRPC operation exceeded its time budget".to_owned()),
                details: BTreeMap::new(),
            }
        );
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn chunked_request_and_response_limits_are_exact() {
        use futures::stream;
        let (client, task, shutdown) = test_server().await;
        let endpoint = client
            .base_url
            .join("xrpc/org.example.test.write")
            .expect("URL");
        let exact_body = "x".repeat(1024);
        let exact_response = client
            .client
            .post(endpoint.clone())
            .header(header::CONTENT_TYPE, "application/json")
            .body(reqwest::Body::wrap_stream(stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                exact_body
            )])))
            .send()
            .await
            .expect("exact chunked request response");
        assert_ne!(exact_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let oversized_body = "x".repeat(1025);
        let oversized_response = client
            .client
            .post(endpoint)
            .header(header::CONTENT_TYPE, "application/json")
            .body(reqwest::Body::wrap_stream(stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                oversized_body,
            )])))
            .send()
            .await
            .expect("oversized chunked request response");
        assert_eq!(oversized_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");

        for (chunks, expected) in [
            (vec!["{".to_owned(), "}".to_owned()], Ok(())),
            (vec!["{".to_owned(), "} ".to_owned()], Err(())),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let address = listener.local_addr().expect("listener address");
            let context = RequestContext::root();
            let request_id = context.request_id;
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("accept client");
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut request = [0_u8; 4096];
                let _bytes_read = stream.read(&mut request).await.expect("read request");
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n"
                );
                stream
                    .write_all(headers.as_bytes())
                    .await
                    .expect("write headers");
                for chunk in chunks {
                    let frame = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                    stream
                        .write_all(frame.as_bytes())
                        .await
                        .expect("write chunk");
                }
                stream.write_all(b"0\r\n\r\n").await.expect("write end");
                Ok::<(), std::io::Error>(())
            });
            let client = XrpcClient::new(
                Url::parse(&format!("http://{address}/")).expect("base URL"),
                2,
            )
            .expect("client");
            let result: Result<Value, XrpcClientError<TestError>> = client
                .execute(
                    QUERY_OPERATION,
                    context,
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "chunked".to_owned(),
                    },
                )
                .await;
            match expected {
                Ok(()) => assert_eq!(result.expect("exact response"), json!({})),
                Err(()) => assert!(matches!(result, Err(XrpcClientError::Protocol { .. }))),
            }
            task.await
                .expect("connection task join")
                .expect("connection task");
        }
    }

    #[tokio::test]
    async fn fragmented_request_exact_boundary_and_overflow_are_bounded() {
        use futures::stream;
        use tokio::sync::mpsc;
        let (client, task, shutdown) = test_server().await;
        let endpoint = client
            .base_url
            .join("xrpc/org.example.test.write")
            .expect("URL");
        for (chunks, oversized) in [
            (
                vec!["x".repeat(300), "x".repeat(300), "x".repeat(424)],
                false,
            ),
            (
                vec!["x".repeat(300), "x".repeat(300), "x".repeat(425)],
                true,
            ),
        ] {
            let (chunk_sender, chunk_receiver) = mpsc::channel::<Result<String, std::io::Error>>(1);
            let (first_polled_sender, first_polled_receiver) = oneshot::channel();
            let (resume_sender, resume_receiver) = oneshot::channel();
            let body_stream = stream::unfold(
                (
                    chunk_receiver,
                    Some(first_polled_sender),
                    Some(resume_receiver),
                ),
                |(mut chunk_receiver, first_polled, mut resume)| async move {
                    if first_polled.is_none()
                        && let Some(resume_receiver) = resume.take()
                    {
                        resume_receiver.await.expect("body resume sender");
                    }
                    chunk_receiver.recv().await.map(|chunk| {
                        let next_resume = if first_polled.is_some() { resume } else { None };
                        if let Some(sender) = first_polled {
                            sender.send(()).expect("first body chunk receiver");
                        }
                        (chunk, (chunk_receiver, None, next_resume))
                    })
                },
            );
            let request = client
                .client
                .post(endpoint.clone())
                .header(header::CONTENT_TYPE, "application/json")
                .body(reqwest::Body::wrap_stream(body_stream));
            let request_task = tokio::spawn(request.send());
            let mut chunks = chunks.into_iter();
            chunk_sender
                .send(Ok(chunks.next().expect("first body chunk")))
                .await
                .expect("body consumer");
            first_polled_receiver
                .await
                .expect("first body chunk polled");
            resume_sender.send(()).expect("body resume receiver");
            for chunk in chunks {
                chunk_sender.send(Ok(chunk)).await.expect("body consumer");
            }
            drop(chunk_sender);
            let response = request_task
                .await
                .expect("request join")
                .expect("request response");
            if oversized {
                assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            } else {
                assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            }
        }
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn fragmented_response_pauses_between_wire_chunks() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let context = RequestContext::root();
        let request_id = context.request_id;
        let (first_sender, first_receiver) = oneshot::channel();
        let (continue_sender, continue_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n"
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("write headers");
            stream
                .write_all(b"1\r\n{\r\n")
                .await
                .expect("write first chunk");
            first_sender.send(()).expect("first chunk receiver");
            continue_receiver.await.expect("continue receiver");
            stream
                .write_all(b"1\r\n}\r\n0\r\n\r\n")
                .await
                .expect("write final chunk");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            2,
        )
        .expect("client");
        let request = tokio::spawn(async move {
            client
                .execute::<EchoParams, Value, TestError>(
                    QUERY_OPERATION,
                    context,
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "fragmented".to_owned(),
                    },
                )
                .await
        });
        first_receiver.await.expect("first chunk sent");
        continue_sender.send(()).expect("continue sender");
        assert_eq!(
            request.await.expect("request join").expect("response"),
            json!({})
        );
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn blocked_fragmented_request_drops_on_shutdown_before_handler() {
        use futures::stream;
        use tokio::sync::mpsc;
        let invoked = Arc::new(AtomicBool::new(false));
        let body_dropped = Arc::new(AtomicBool::new(false));
        let mut router = XrpcRouter::new(1024);
        router.observe_body_drop(Arc::clone(&body_dropped));
        let handler_invoked = Arc::clone(&invoked);
        router
            .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
                handler_invoked.store(true, Ordering::Release);
                async { Ok(json!({"value":"late"})) }
            }))
            .expect("procedure registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve(listener, router.into_router())
            .await
            .expect("start server");
        let (chunk_sender, chunk_receiver) = mpsc::channel::<Result<String, std::io::Error>>(1);
        let (polled_sender, polled_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = oneshot::channel();
        let body_stream = stream::unfold(
            (
                chunk_receiver,
                Some(polled_sender),
                StreamDrop(Some(dropped_sender)),
            ),
            move |(mut chunk_receiver, polled, drop_guard)| async move {
                chunk_receiver.recv().await.map(|chunk| {
                    if let Some(sender) = polled {
                        sender.send(()).expect("body polled receiver");
                    }
                    (chunk, (chunk_receiver, None, drop_guard))
                })
            },
        );
        let request = tokio::spawn(
            reqwest::Client::new()
                .post(format!(
                    "http://{address}/xrpc/{}",
                    PROCEDURE_OPERATION.nsid
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .body(reqwest::Body::wrap_stream(body_stream))
                .send(),
        );
        chunk_sender
            .send(Ok(r#"{"value":"#.to_owned()))
            .await
            .expect("body consumer");
        polled_receiver.await.expect("body was polled");
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
        assert!(body_dropped.load(Ordering::Acquire));
        drop(chunk_sender);
        dropped_receiver.await.expect("body stream dropped");
        assert!(!invoked.load(Ordering::Acquire));
        assert!(request.await.expect("request join").is_err());
    }

    #[tokio::test]
    async fn peer_disconnect_drops_blocked_body_and_recovers_capacity() {
        use tokio::io::AsyncWriteExt;
        let invoked = Arc::new(AtomicBool::new(false));
        let body_dropped = Arc::new(tokio::sync::Notify::new());
        let mut router = XrpcRouter::with_limits(1024, 1);
        router.observe_body_drop(Arc::new(AtomicBool::new(false)));
        router.observe_body_drop_notify(Arc::clone(&body_dropped));
        let handler_invoked = Arc::clone(&invoked);
        router
            .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
                handler_invoked.store(true, Ordering::Release);
                async { Ok(json!({"value":"complete"})) }
            }))
            .expect("procedure registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve(listener, router.into_router())
            .await
            .expect("start server");

        let mut peer = TcpStream::connect(address).await.expect("connect peer");
        let server_drop = body_dropped.notified();
        peer.write_all(
            format!(
                "POST /xrpc/{} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nA\r\n{{\"value\":\"\r\n",
                PROCEDURE_OPERATION.nsid
            )
            .as_bytes(),
        )
        .await
        .expect("write incomplete chunk");
        peer.shutdown().await.expect("disconnect peer");
        server_drop.await;
        assert!(!invoked.load(Ordering::Acquire));
        let response = reqwest::Client::new()
            .post(format!(
                "http://{address}/xrpc/{}",
                PROCEDURE_OPERATION.nsid
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .body(r#"{"value":"recovery"}"#)
            .send()
            .await
            .expect("recovery response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(invoked.load(Ordering::Acquire));
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn handler_timeout_drops_guard_before_receipt() {
        let completed = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(None));
        let (started_sender, started_receiver) = oneshot::channel();
        *started.lock().expect("started lock") = Some(started_sender);
        let handler_completed = Arc::clone(&completed);
        let handler_started = Arc::clone(&started);
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
                let completed = Arc::clone(&handler_completed);
                let started = Arc::clone(&handler_started);
                async move {
                    if let Some(sender) = started.lock().expect("started lock").take() {
                        sender.send(()).expect("started receiver");
                    }
                    let _drop_flag = DropFlag(completed);
                    std::future::pending::<Result<Value, XrpcHandlerError>>().await
                }
            }))
            .expect("procedure registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve(listener, router.into_router())
            .await
            .expect("start server");
        let response = reqwest::Client::new()
            .post(format!(
                "http://{address}/xrpc/{}",
                PROCEDURE_OPERATION.nsid
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .header(TIMEOUT_HEADER, "0")
            .body(r#"{"value":"pending"}"#)
            .send()
            .await
            .expect("timeout response");
        started_receiver.await.expect("handler started");
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(!completed.load(Ordering::Acquire));
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn procedure_timeout_domain_error_remains_ambiguous() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let context = RequestContext::root();
        let response_request_id = context.request_id;
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 50\r\nContent-Type: application/json\r\nx-request-id: {response_request_id}\r\n\r\n{{\"error\":\"DeadlineExceeded\",\"message\":\"timed out\"}}"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                context,
                &HttpMetadata::default(),
                &EchoParams {
                    value: "procedure".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(XrpcClientError::Domain {
                status: StatusCode::GATEWAY_TIMEOUT,
                outcome_unknown: true,
                error: TestError { error, .. },
            }) if error == "DeadlineExceeded"
        ));
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn procedure_server_error_after_effect_remains_ambiguous() {
        use std::sync::atomic::AtomicBool;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let effect = Arc::new(AtomicBool::new(false));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let context = RequestContext::root();
        let request_id = context.request_id;
        let handler_effect = Arc::clone(&effect);
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            handler_effect.store(true, Ordering::Release);
            let response = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 52\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n{{\"error\":\"InternalError\",\"message\":\"effect applied\"}}"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Output, XrpcClientError<TestError>> = client
            .execute(
                PROCEDURE_OPERATION,
                context,
                &HttpMetadata::default(),
                &EchoParams {
                    value: "effect".to_owned(),
                },
            )
            .await;
        assert!(effect.load(Ordering::Acquire));
        assert!(matches!(
            result,
            Err(XrpcClientError::Domain {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                outcome_unknown: true,
                error: TestError { message, .. },
            }) if message == "effect applied"
        ));
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_pending_connection() {
        let started = Arc::new(Mutex::new(None));
        let (started_sender, started_receiver) = oneshot::channel();
        *started.lock().expect("started lock") = Some(started_sender);
        let completed = Arc::new(AtomicBool::new(false));
        let mut router = XrpcRouter::with_limits(1024, 1);
        let handler_completed = Arc::clone(&completed);
        let handler_started = Arc::clone(&started);
        router
            .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
                if let Some(sender) = handler_started.lock().expect("started lock").take() {
                    sender.send(()).expect("started receiver");
                }
                let completed = Arc::clone(&handler_completed);
                async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    completed.store(true, Ordering::Release);
                    Ok(json!({"value":"completed"}))
                }
            }))
            .expect("procedure registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve(listener, router.into_router())
            .await
            .expect("start server");
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let request = tokio::spawn(async move {
            client
                .execute::<EchoParams, Value, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        started_receiver.await.expect("handler started");
        let rejected = reqwest::Client::new()
            .post(format!(
                "http://{address}/xrpc/{}",
                PROCEDURE_OPERATION.nsid
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .header(
                REQUEST_ID_HEADER,
                RequestContext::root().request_id.to_string(),
            )
            .body(r#"{"value":"rejected"}"#)
            .send()
            .await
            .expect("admission response");
        assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
        shutdown.send(()).expect("server shutdown receiver");
        task.await.expect("server task join").expect("server task");
        assert!(!completed.load(Ordering::Acquire));
        assert!(request.await.expect("request join").is_err());
    }

    #[tokio::test]
    async fn response_request_id_must_correlate() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\nx-request-id: 00000000-0000-0000-0000-000000000000\r\n\r\n{}")
                .await
                .expect("write response");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Value, XrpcClientError<TestError>> = client
            .execute(
                QUERY_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "query".to_owned(),
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(XrpcClientError::Protocol {
                outcome_unknown: false,
                ..
            })
        ));
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn response_without_request_id_remains_compatible() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{}")
                .await
                .expect("write response");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Value, XrpcClientError<TestError>> = client
            .execute(
                QUERY_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "query".to_owned(),
                },
            )
            .await;
        assert_eq!(
            result.expect("response without optional request ID"),
            json!({})
        );
        task.await
            .expect("connection task join")
            .expect("connection task");
    }

    #[tokio::test]
    async fn domain_error_details_remain_in_standard_envelope() {
        let mut details = BTreeMap::new();
        details.insert("retry_after_ms".to_owned(), json!(250));
        let error = XrpcHandlerError::domain_with_details(
            StatusCode::TOO_MANY_REQUESTS,
            "RateLimited",
            "try again later",
            details.clone(),
        )
        .expect("non-reserved domain details");
        let XrpcHandlerError::Domain { body, .. } = error else {
            panic!("expected domain error")
        };
        assert_eq!(body.error, "RateLimited");
        assert_eq!(body.details, details);
        assert_eq!(
            serde_json::to_value(body).expect("serialize envelope")["message"],
            "try again later"
        );
        let mut reserved = BTreeMap::new();
        reserved.insert("message".to_owned(), json!("collision"));
        assert!(matches!(
            XrpcHandlerError::domain_with_details(
                StatusCode::BAD_REQUEST,
                "Invalid",
                "message",
                reserved,
            ),
            Err(XrpcDomainDetailsError::ReservedField("message")),
        ));
        assert!(serde_json::from_str::<XrpcErrorEnvelope>("null").is_err());
        assert!(
            serde_json::from_str::<XrpcErrorEnvelope>(r#"{"error":12,"message":"invalid"}"#)
                .is_err()
        );
    }

    #[tokio::test]
    async fn listener_failure_aborts_and_joins_active_connection() {
        let accepted_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = accepted_listener.local_addr().expect("listener address");
        let calls = Arc::new(AtomicUsize::new(0));
        let (accepted_sender, accepted_receiver) = oneshot::channel();
        let (failure_sender, failure_receiver) = oneshot::channel();
        let acceptor = ScriptedAcceptor {
            listener: accepted_listener,
            calls: Arc::clone(&calls),
            first_accepted: Some(accepted_sender),
            failure: Some(failure_receiver),
        };
        let started = Arc::new(Mutex::new(None));
        let (started_sender, started_receiver) = oneshot::channel();
        *started.lock().expect("started lock") = Some(started_sender);
        let active = Arc::new(AtomicBool::new(false));
        let handler_active = Arc::clone(&active);
        let handler_started = Arc::clone(&started);
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
                if let Some(sender) = handler_started.lock().expect("started lock").take() {
                    sender.send(()).expect("started receiver");
                }
                handler_active.store(true, Ordering::Release);
                let active = Arc::clone(&handler_active);
                async move {
                    let _drop_flag = DropFlag(active);
                    std::future::pending::<Result<Value, XrpcHandlerError>>().await
                }
            }))
            .expect("procedure registration");
        let router = router.into_router();
        let (task, _shutdown) = serve_with_acceptor(acceptor, router, 2)
            .await
            .expect("start server");
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let request = tokio::spawn(async move {
            client
                .execute::<EchoParams, Value, TestError>(
                    PROCEDURE_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &EchoParams {
                        value: "pending".to_owned(),
                    },
                )
                .await
        });
        accepted_receiver.await.expect("first connection accepted");
        started_receiver.await.expect("handler started");
        failure_sender.send(()).expect("failure receiver");
        let result = task.await.expect("server task join");
        assert!(result.is_err());
        assert!(!active.load(Ordering::Acquire));
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert!(request.await.expect("request join").is_err());
    }

    #[tokio::test]
    async fn connection_capacity_waits_before_accepting_more_peers() {
        let accepted_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = accepted_listener.local_addr().expect("listener address");
        let calls = Arc::new(AtomicUsize::new(0));
        let (accepted_sender, accepted_receiver) = oneshot::channel();
        let acceptor = ScriptedAcceptor {
            listener: accepted_listener,
            calls: Arc::clone(&calls),
            first_accepted: Some(accepted_sender),
            failure: None,
        };
        let router = XrpcRouter::new(1024).into_router();
        let (task, shutdown) = serve_with_acceptor(acceptor, router, 1)
            .await
            .expect("start server");
        let first = tokio::net::TcpStream::connect(address)
            .await
            .expect("first peer");
        accepted_receiver.await.expect("first connection accepted");
        let second = tokio::net::TcpStream::connect(address)
            .await
            .expect("second peer queued by listener");
        assert_eq!(calls.load(Ordering::Acquire), 1);
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
        drop(second);
        drop(first);
    }

    #[tokio::test]
    async fn repeated_normal_connections_complete_under_connection_bound() {
        let mut router = XrpcRouter::new(1024);
        router
            .register(XrpcOperationHandler::typed(
                QUERY_OPERATION,
                |_context, _metadata, params: EchoParams| async move {
                    Ok(json!({"value": params.value}))
                },
            ))
            .expect("query registration");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let (task, shutdown) = serve_with_connection_limit(listener, router.into_router(), 2)
            .await
            .expect("start server");
        let client = reqwest::Client::new();
        for value in ["one", "two", "three", "four", "five"] {
            let response = client
                .get(format!(
                    "http://{address}/xrpc/{}?value={value}",
                    QUERY_OPERATION.nsid
                ))
                .header("connection", "close")
                .send()
                .await
                .expect("normal response");
            assert_eq!(response.status(), StatusCode::OK);
        }
        shutdown.send(()).expect("shutdown receiver");
        task.await.expect("server task join").expect("server task");
    }

    #[tokio::test]
    async fn unsupported_subscription_is_explicit() {
        let subscription = XrpcOperation {
            nsid: "org.example.test.events",
            kind: XrpcKind::Subscription,
            method: HttpMethod::Get,
            input: HttpInput::Query,
            output_schema: "Event",
            error_schema: "TestError",
        };
        let mut router = XrpcRouter::new(1024);
        assert_eq!(
            router
                .register(XrpcOperationHandler::new(subscription, |_| async {
                    Ok(json!({}))
                }))
                .expect_err("subscription must be rejected"),
            XrpcRegistrationError::SubscriptionUnsupported
        );
    }
}
