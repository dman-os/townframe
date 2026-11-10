//! The XRPC envelope: how a method and its request context become an HTTP request, and how
//! an HTTP response becomes the method's output or error. A transport implementing
//! [`HttpTransport`] only moves the bytes; everything about the wire format lives here, in the
//! same shape as the host's XRPC router.

use crate::client::UnaryTransport;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header;
use method_core::HttpInput;
use method_core::HttpMethod;
use method_core::MethodKind;
use method_core::Unary;
use method_core::XrpcMethod;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use service_core_rs::ClientError;
use service_core_rs::Envelope;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use service_core_rs::ResponseMetadata;
use service_core_rs::xrpc::AUTHORIZATION_HEADER;
use service_core_rs::xrpc::DOMAIN_ERROR_FIELD;
use service_core_rs::xrpc::IDEMPOTENCY_KEY_HEADER;
use service_core_rs::xrpc::REQUEST_ID_HEADER;
use service_core_rs::xrpc::TIMEOUT_HEADER;
use service_core_rs::xrpc::TRACEPARENT_HEADER;
use service_core_rs::xrpc::XrpcErrorEnvelope;
use std::future::Future;
use thiserror::Error;

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

impl<E> From<XrpcClientError<E>> for ClientError<E> {
    fn from(error: XrpcClientError<E>) -> Self {
        match error {
            XrpcClientError::Domain { error, .. } => Self::Method(error),
            XrpcClientError::Encode(message) => Self::Transport {
                message,
                outcome_unknown: false,
            },
            XrpcClientError::Transport {
                message,
                outcome_unknown,
            }
            | XrpcClientError::Protocol {
                message,
                outcome_unknown,
            } => Self::Transport {
                message,
                outcome_unknown,
            },
            XrpcClientError::Http {
                status,
                body,
                outcome_unknown,
            } => Self::Transport {
                message: format!("HTTP {status}: {body}"),
                outcome_unknown,
            },
        }
    }
}

/// An encoded unary request. `path` is relative to the server's base URL and carries the query
/// string of a query method; the deadline travels in a header.
#[derive(Debug, Clone)]
pub struct XrpcRequest {
    pub method: Method,
    pub path: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct XrpcResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// The HTTP exchange failed before a response was read, or while reading it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct HttpTransportError {
    pub message: String,
}

/// Moves one [`XrpcRequest`] to the server and returns its [`XrpcResponse`]. A guest implements
/// this over its HTTP import, a host over its HTTP client; [`Xrpc`] turns it into a
/// [`UnaryTransport`] for every unary method. The future carries no `Send` bound for the
/// same reason [`UnaryTransport`]'s does.
pub trait HttpTransport {
    fn send(
        &self,
        request: XrpcRequest,
    ) -> impl Future<Output = Result<XrpcResponse, HttpTransportError>>;
}

/// A [`UnaryTransport`] speaking the XRPC envelope over an [`HttpTransport`].
#[derive(Debug, Clone)]
pub struct Xrpc<H>(pub H);

impl<H: HttpTransport, P: Unary> UnaryTransport<P> for Xrpc<H> {
    async fn call(
        &self,
        request: Envelope<P>,
    ) -> Result<ResponseEnvelope<P::Output>, ClientError<P::Error>> {
        let Envelope {
            context,
            metadata,
            params,
            ..
        } = request;
        let encoded = encode_request(P::XRPC, &context, &metadata, &params)
            .map_err(XrpcClientError::<P::Error>::Encode)?;
        let outcome_unknown = P::XRPC.kind == MethodKind::Procedure;
        let response =
            self.0
                .send(encoded)
                .await
                .map_err(|error| XrpcClientError::<P::Error>::Transport {
                    message: error.message,
                    outcome_unknown,
                })?;
        Ok(decode_response(
            P::XRPC,
            context.request_id,
            response.status,
            &response.headers,
            &response.body,
        )?)
    }
}

/// Writes the headers every XRPC hop carries from a call's context: `x-request-id`,
/// `traceparent`/`tracestate`, `authorization`, `idempotency-key` and the remaining budget as a
/// relative `x-timeout-ms` (like gRPC's `grpc-timeout`, so a hop re-derives it from its own
/// clock). The server side parses the same set in `service_utils_rs`'s `request_context`.
pub fn write_context_headers(
    headers: &mut HeaderMap,
    context: &RequestContext,
    metadata: &HttpMetadata,
) -> Result<(), String> {
    headers.insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&context.request_id.to_string())
            .map_err(|error| error.to_string())?,
    );
    for (name, value) in [
        (TRACEPARENT_HEADER, context.trace.traceparent()),
        ("tracestate", context.trace.tracestate()),
    ] {
        if let Some(value) = value {
            headers.insert(
                name,
                HeaderValue::from_str(value).map_err(|error| error.to_string())?,
            );
        }
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
    if let Some(remaining) = context.remaining_budget() {
        headers.insert(
            TIMEOUT_HEADER,
            HeaderValue::from_str(&remaining.as_millis().to_string())
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(())
}

pub fn encode_request<P: Serialize>(
    operation: XrpcMethod,
    context: &RequestContext,
    metadata: &HttpMetadata,
    params: &P,
) -> Result<XrpcRequest, String> {
    if operation.kind == MethodKind::Subscription {
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
    write_context_headers(&mut headers, context, metadata)?;
    if operation.input == HttpInput::JsonBody {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    Ok(XrpcRequest {
        method: match operation.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
        },
        path,
        headers,
        body,
    })
}

pub fn decode_response<O, E>(
    operation: XrpcMethod,
    request_id: uuid::Uuid,
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ResponseEnvelope<O>, XrpcClientError<E>>
where
    O: DeserializeOwned,
    E: DeserializeOwned,
{
    if let Err(message) = response_request_id(headers, request_id) {
        return Err(XrpcClientError::Protocol {
            message,
            outcome_unknown: operation.kind == MethodKind::Procedure,
        });
    }
    let value =
        serde_json::from_slice::<Value>(body).map_err(|error| XrpcClientError::Protocol {
            message: format!("invalid JSON XRPC response: {error}"),
            outcome_unknown: operation.kind == MethodKind::Procedure,
        })?;
    if status.is_success() {
        let output = serde_json::from_value(value).map_err(|error| XrpcClientError::Protocol {
            message: format!("invalid XRPC success payload: {error}"),
            outcome_unknown: operation.kind == MethodKind::Procedure,
        })?;
        let metadata = ResponseMetadata::read_headers(headers).map_err(|message| {
            XrpcClientError::Protocol {
                message,
                outcome_unknown: operation.kind == MethodKind::Procedure,
            }
        })?;
        return Ok(ResponseEnvelope { output, metadata });
    }
    let envelope = match serde_json::from_value::<XrpcErrorEnvelope>(value.clone()) {
        Ok(envelope) => envelope,
        Err(error) => {
            return Err(XrpcClientError::Protocol {
                message: format!("invalid XRPC error envelope: {error}"),
                outcome_unknown: operation.kind == MethodKind::Procedure,
            });
        }
    };
    let typed = envelope
        .details()
        .get(DOMAIN_ERROR_FIELD)
        .cloned()
        .unwrap_or_else(|| value.clone());
    match serde_json::from_value::<E>(typed) {
        Ok(error) => Err(XrpcClientError::Domain {
            status,
            error,
            outcome_unknown: operation.kind == MethodKind::Procedure && status.is_server_error(),
        }),
        Err(_) => Err(XrpcClientError::Http {
            status,
            body: value,
            outcome_unknown: operation.kind == MethodKind::Procedure,
        }),
    }
}

fn response_request_id(headers: &HeaderMap, expected: uuid::Uuid) -> Result<(), String> {
    let Some(value) = headers.get(REQUEST_ID_HEADER) else {
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
