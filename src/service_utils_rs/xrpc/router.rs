//! Server side: route `/xrpc/{nsid}` to a mounted operation.

use super::{
    AUTHORIZATION_HEADER, DOMAIN_ERROR_FIELD, DOMAIN_ERROR_STATUS, IDEMPOTENCY_KEY_HEADER,
    REQUEST_ID_HEADER, TIMEOUT_HEADER, TRACEPARENT_HEADER, XrpcErrorEnvelope, XrpcHandlerError,
    XrpcRequest,
};
use crate::{
    context::{HttpMetadata, RequestContext, TraceContext},
    host::Host,
    middleware::Handler,
    operation::{Envelope, HttpInput, HttpMethod, Unary, XrpcKind, XrpcOperation},
};
use axum::{
    Json, Router,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use mime::Mime;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::Semaphore;

/// A registered operation handler. [`XrpcRouter::mount`] builds one from a
/// hosted [`Handler`]; [`XrpcOperationHandler::typed`] is the escape hatch for
/// an operation that is not hosted through this crate.
#[derive(Clone)]
pub struct XrpcOperationHandler {
    operation: XrpcOperation,
    handler: Arc<dyn Fn(XrpcRequest) -> HandlerFuture + Send + Sync>,
}

type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<crate::ResponseEnvelope<Value>, XrpcHandlerError>> + Send>>;

impl std::fmt::Debug for XrpcOperationHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XrpcOperationHandler")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl XrpcOperationHandler {
    pub fn new<F, Fut, O>(operation: XrpcOperation, handler: F) -> Self
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

    async fn call(
        &self,
        request: XrpcRequest,
    ) -> Result<crate::ResponseEnvelope<Value>, XrpcHandlerError> {
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
        self.register(XrpcOperationHandler::new(P::XRPC, move |request| {
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
                envelope.transport = request.transport;
                let response = host
                    .invoke(envelope)
                    .await
                    .map_err(|error| domain_error(P::XRPC, &error))?;
                Ok(crate::ResponseEnvelope {
                    output: serde_json::to_value(response.output)
                        .expect("operation output must serialize"),
                    metadata: response.metadata,
                })
            }
        }))
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

/// Encodes a typed operation error as the standard XRPC error envelope. A
/// non-serializable error is a programming error in the error type.
fn domain_error<E>(operation: XrpcOperation, error: &E) -> XrpcHandlerError
where
    E: Serialize + std::fmt::Debug,
{
    let value = serde_json::to_value(error).expect("operation errors must serialize");
    XrpcHandlerError::Domain {
        status: DOMAIN_ERROR_STATUS,
        body: XrpcErrorEnvelope {
            error: operation.error_schema.to_owned(),
            message: Some(format!("{error:?}")),
            details: BTreeMap::from([(DOMAIN_ERROR_FIELD.to_owned(), value)]),
        },
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
    context.trace = TraceContext::from_headers(
        header_string(headers, TRACEPARENT_HEADER)?.as_deref(),
        header_string(headers, "tracestate")?.as_deref(),
    );
    if let Some(value) = header_string(headers, TIMEOUT_HEADER)? {
        let millis = value
            .parse::<u64>()
            .map_err(|_| "x-timeout-ms must be an unsigned integer".to_owned())?;
        context = context.with_time_budget(Duration::from_millis(millis));
    }
    Ok((
        context,
        HttpMetadata {
            authorization: header_string(headers, AUTHORIZATION_HEADER)?,
            idempotency_key: header_string(headers, IDEMPOTENCY_KEY_HEADER)?,
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
