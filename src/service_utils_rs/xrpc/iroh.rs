//! The same HTTP/1 XRPC wire served over Iroh bidirectional streams, so a node
//! exposes one canonical interface to peers and to local HTTP alike.

use super::{
    client::{XrpcClientError, decode_xrpc_response, encode_xrpc_request},
    serve::expected_peer_error,
};
use crate::{
    context::{HttpMetadata, RequestContext},
    operation::{HttpMethod, XrpcKind, XrpcOperation},
};
use axum::{
    Router,
    http::{HeaderValue, Method, header},
};
use http_body_util::BodyExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{pin::Pin, sync::Arc};
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
};

pub const IROH_XRPC_HTTP_ALPN: &[u8] = b"townframe/xrpc-http/1";

struct IrohHttpStream {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

impl IrohHttpStream {
    fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
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
pub struct IrohXrpcProtocolHandler {
    router: Router,
    connection_admission: Arc<Semaphore>,
    stream_admission: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
    shutdown_state: watch::Receiver<bool>,
}

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
            .half_close(true)
            .serve_connection(io, hyper_util::service::TowerToHyperService::new(router)).with_upgrades() => result,
        _ = stopped => Ok(()),
    }
}

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
        let router = self
            .router
            .clone()
            .layer(axum::Extension(crate::TransportContext::Iroh {
                peer: connection.remote_id().to_string(),
            }));
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
                    let router = router.clone();
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

#[derive(Clone)]
pub struct IrohXrpcClient {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    max_response_bytes: usize,
    pub(super) connection: Arc<tokio::sync::Mutex<IrohClientConnection>>,
}

pub(super) struct IrohClientConnection {
    pub(super) connection: Option<iroh::endpoint::Connection>,
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
            // A cancelled request cannot await its stream task. Abort only this
            // stream's Hyper future; the shared QUIC connection remains alive.
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

    pub(super) async fn execute_inner<P, O, E>(
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
