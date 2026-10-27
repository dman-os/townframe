use super::client::XrpcClientError;
use super::client::decode_xrpc_response;
use super::client::encode_xrpc_request;
use crate::HttpMetadata;
use crate::HttpMethod;
use crate::RequestContext;
use crate::XrpcKind;
use crate::XrpcOperation;
use http::HeaderValue;
use http::Method;
use http::header;
use http_body_util::BodyExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

pub const IROH_XRPC_HTTP_ALPN: &[u8] = b"townframe/xrpc-http/1";

pub struct IrohHttpStream {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

impl IrohHttpStream {
    pub fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
        Self { send, recv }
    }
}

impl tokio::io::AsyncRead for IrohHttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}
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

#[derive(Clone)]
pub struct IrohXrpcClient {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    max_response_bytes: usize,
    connection: Arc<tokio::sync::Mutex<IrohClientConnection>>,
}

struct IrohClientConnection {
    connection: Option<iroh::endpoint::Connection>,
    generation: u64,
}

impl std::fmt::Debug for IrohXrpcClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohXrpcClient")
            .field("endpoint_addr", &self.endpoint_addr)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

struct IrohConnectionTaskGuard(Option<tokio::task::JoinHandle<Result<(), hyper::Error>>>);
impl Drop for IrohConnectionTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}
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

    pub async fn active_connection(&self) -> Option<iroh::endpoint::Connection> {
        self.connection.lock().await.connection.clone()
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
        let outcome_unknown = operation.kind == XrpcKind::Procedure;
        if let Some(timeout) = context.remaining_budget() {
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
    ) -> Result<crate::ResponseEnvelope<O>, XrpcClientError<E>>
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
