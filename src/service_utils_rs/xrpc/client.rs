//! Client side: encode an operation to HTTP, decode the response.

use super::{
    AUTHORIZATION_HEADER, DOMAIN_ERROR_FIELD, IDEMPOTENCY_KEY_HEADER, REQUEST_ID_HEADER,
    TIMEOUT_HEADER, TRACEPARENT_HEADER, XrpcErrorEnvelope,
};
use crate::{
    context::{HttpMetadata, RequestContext},
    operation::{HttpInput, HttpMethod, XrpcKind, XrpcOperation},
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use futures::StreamExt;
use reqwest::Url;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;

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

pub(super) struct EncodedXrpcRequest {
    pub(super) path: String,
    pub(super) headers: HeaderMap,
    pub(super) body: Vec<u8>,
}

pub(super) fn encode_xrpc_request<P: Serialize>(
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
    pub(super) client: reqwest::Client,
    pub(super) base_url: Url,
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
        self.execute_with_metadata(operation, context, metadata, params)
            .await
            .map(|response| response.output)
    }

    pub async fn execute_with_metadata<P, O, E>(
        &self,
        operation: XrpcOperation,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<crate::ResponseEnvelope<O>, XrpcClientError<E>>
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
        if let Some(timeout) = context.remaining_budget() {
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
    /// Opens a WebSocket subscription. The returned channel carries the same
    /// `EventFrame` sequence the direct and IRPC transports deliver: `Open`
    /// with response metadata, `Event`s, then `End` or a typed error.
    pub async fn subscribe<P>(
        &self,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
        buffer: usize,
    ) -> Result<
        irpc::channel::mpsc::Receiver<Result<crate::EventFrame<P::Event>, P::Error>>,
        XrpcClientError<P::Error>,
    >
    where
        P: crate::Subscription,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let operation = P::XRPC;
        let query = serde_urlencoded::to_string(params)
            .map_err(|error| XrpcClientError::Encode(error.to_string()))?;
        let mut url = self.base_url.clone();
        url.set_path(&format!(
            "{}/xrpc/{}",
            url.path().trim_end_matches('/'),
            operation.nsid
        ));
        url.set_query((!query.is_empty()).then_some(query.as_str()));
        let host = url
            .host_str()
            .ok_or_else(|| XrpcClientError::Encode("subscription URL has no host".to_owned()))?
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| XrpcClientError::Encode("subscription URL has no port".to_owned()))?;
        if url.scheme() != "http" {
            return Err(XrpcClientError::Protocol {
                message: "websocket subscriptions require an http base URL (wss is not supported)"
                    .to_owned(),
                outcome_unknown: false,
            });
        }
        let mut stream = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: false,
            })?;
        let key = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            uuid::Uuid::now_v7().as_bytes(),
        );
        let path = match url.query() {
            Some(query) => format!("{}?{}", url.path(), query),
            None => url.path().to_owned(),
        };
        let mut request = format!(
            "GET {path} HTTP/1.1\r\nhost: {host}:{port}\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-key: {key}\r\nsec-websocket-version: 13\r\n{REQUEST_ID_HEADER}: {}\r\n",
            context.request_id
        );
        if let Some(value) = &metadata.authorization {
            request.push_str(&format!("{AUTHORIZATION_HEADER}: {value}\r\n"));
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: false,
            })?;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read_exact(&mut byte).await.is_err() || head.len() > 16 * 1024 {
                return Err(XrpcClientError::Transport {
                    message: "websocket handshake failed".to_owned(),
                    outcome_unknown: false,
                });
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        if !head.starts_with("HTTP/1.1 101") {
            return Err(XrpcClientError::Transport {
                message: format!(
                    "websocket handshake refused: {}",
                    head.lines().next().unwrap_or_default()
                ),
                outcome_unknown: false,
            });
        }
        let (tx, rx) = irpc::channel::mpsc::channel(buffer.max(1));
        tokio::spawn(async move {
            let mut socket = super::ws::WsIo::new(stream, true);
            loop {
                match socket.read_message().await {
                    Ok(Some(super::ws::WsMessage::Text(text))) => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else {
                            break;
                        };
                        if let Some(open) = value.get("open") {
                            let metadata: crate::ResponseMetadata = open
                                .get("metadata")
                                .cloned()
                                .and_then(|metadata| serde_json::from_value(metadata).ok())
                                .unwrap_or_default();
                            if tx
                                .send(Ok(crate::EventFrame::Open(metadata)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        } else if let Some(event) = value.get("event") {
                            match serde_json::from_value::<P::Event>(event.clone()) {
                                Ok(event) => {
                                    if tx.send(Ok(crate::EventFrame::Event(event))).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        } else if let Some(error) = value.get("error") {
                            if let Ok(error) = serde_json::from_value::<P::Error>(error.clone()) {
                                drop(tx.send(Err(error)).await);
                            }
                            break;
                        }
                    }
                    Ok(Some(super::ws::WsMessage::Close)) | Ok(None) | Err(_) => {
                        drop(tx.send(Ok(crate::EventFrame::End)).await);
                        break;
                    }
                }
            }
        });
        Ok(rx)
    }
}

pub(super) fn decode_xrpc_response<O, E>(
    operation: XrpcOperation,
    request_id: uuid::Uuid,
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<crate::ResponseEnvelope<O>, XrpcClientError<E>>
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
        let output = serde_json::from_value(value).map_err(|error| XrpcClientError::Protocol {
            message: format!("invalid XRPC success payload: {error}"),
            outcome_unknown: operation.kind == XrpcKind::Procedure,
        })?;
        let metadata = crate::ResponseMetadata::read_headers(headers).map_err(|message| {
            XrpcClientError::Protocol {
                message,
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            }
        })?;
        return Ok(crate::ResponseEnvelope { output, metadata });
    }
    let envelope = match serde_json::from_value::<XrpcErrorEnvelope>(value.clone()) {
        Ok(envelope) => envelope,
        Err(error) => {
            return Err(XrpcClientError::Protocol {
                message: format!("invalid XRPC error envelope: {error}"),
                outcome_unknown: operation.kind == XrpcKind::Procedure,
            });
        }
    };
    // A hosted operation carries its typed error in the detail field; an
    // error type that is itself shaped like the envelope decodes from the body.
    let typed = envelope
        .details
        .get(DOMAIN_ERROR_FIELD)
        .cloned()
        .unwrap_or_else(|| value.clone());
    match serde_json::from_value::<E>(typed) {
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

pub(super) fn response_request_id(headers: &HeaderMap, expected: uuid::Uuid) -> Result<(), String> {
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
