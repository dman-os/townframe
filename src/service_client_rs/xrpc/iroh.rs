use super::client::XrpcClientError;
use super::client::decode_xrpc_response;
use super::client::encode_xrpc_request;
use http::HeaderValue;
use http::Method;
use http::header;
use http_body_util::BodyExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use service_core_rs::HttpMetadata;
use service_core_rs::HttpMethod;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use service_core_rs::XrpcKind;
use service_core_rs::XrpcOperation;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

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

/// Handle to an XRPC peer reached over one retained Iroh connection.
///
/// Handles never touch the retained connection directly: a single
/// `IrohConnectionActor` task owns it and handles send it commands. The
/// actor owns the connection lifecycle (connect, reuse, replace), never the
/// request I/O: it hands each request a clone of the connection, and the
/// request opens its own bidirectional stream and drives its HTTP exchange
/// on the caller's task, so concurrent requests multiplex over the
/// connection instead of queueing behind the actor. The actor stops when the
/// last handle is dropped and closes the command channel, dropping the
/// retained connection.
#[derive(Clone)]
pub struct IrohXrpcClient {
    endpoint_addr: iroh::EndpointAddr,
    max_response_bytes: usize,
    // Unbounded so `Discard` stays synchronous and cannot be lost to a
    // cancellation at an await point. The queue stays bounded in practice:
    // a request has at most one reply-awaiting command outstanding plus one
    // `Discard`.
    connections: mpsc::UnboundedSender<IrohConnectionCommand>,
}

#[derive(Debug)]
enum IrohConnectionCommand {
    /// Reply with the retained connection, connecting first when none is
    /// retained or the retained one has closed.
    Acquire(oneshot::Sender<Result<(iroh::endpoint::Connection, u64), String>>),
    /// Reply with the retained connection as-is, without connecting.
    Inspect(oneshot::Sender<Option<iroh::endpoint::Connection>>),
    /// Forget the retained connection if it is still the given generation.
    /// Generations tell successive connections apart, so a request that saw
    /// an old connection die cannot discard its replacement.
    Discard(u64),
}

struct IrohConnectionActor {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    connection: Option<iroh::endpoint::Connection>,
    generation: u64,
}

impl IrohConnectionActor {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<IrohConnectionCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                IrohConnectionCommand::Acquire(mut reply) => {
                    // Connecting inline keeps a single connect in flight:
                    // commands that arrive meanwhile wait and then reuse its
                    // connection. The connect belongs to its requester, so a
                    // requester cancelled mid-connect (dropped future or
                    // exhausted time budget) closes its reply and abandons it.
                    let acquired = tokio::select! {
                        acquired = self.acquire() => acquired,
                        () = reply.closed() => continue,
                    };
                    // A failed send means the requester was cancelled after the
                    // connect resolved; the new connection stays retained.
                    if let Err(unclaimed) = reply.send(acquired) {
                        drop(unclaimed);
                    }
                }
                IrohConnectionCommand::Inspect(reply) => {
                    // A failed send means the requester was cancelled.
                    if let Err(unclaimed) = reply.send(self.connection.clone()) {
                        drop(unclaimed);
                    }
                }
                IrohConnectionCommand::Discard(generation) => {
                    if self.generation == generation {
                        self.connection = None;
                    }
                }
            }
        }
    }

    async fn acquire(&mut self) -> Result<(iroh::endpoint::Connection, u64), String> {
        if let Some(connection) = &self.connection {
            if connection.close_reason().is_none() {
                return Ok((connection.clone(), self.generation));
            }
            self.connection = None;
        }
        let connection = self
            .endpoint
            .connect(self.endpoint_addr.clone(), IROH_XRPC_HTTP_ALPN)
            .await
            .map_err(|error| error.to_string())?;
        self.generation = self.generation.wrapping_add(1);
        self.connection = Some(connection.clone());
        Ok((connection, self.generation))
    }
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
        let (connections, commands) = mpsc::unbounded_channel();
        // Detached on purpose: the actor's only exit is the command channel
        // closing when the last handle drops. It has no fallible paths of its
        // own (connect failures are replies), so a panic is a bug that the
        // process panic hook and the handles' `expect`s both surface.
        tokio::spawn(
            IrohConnectionActor {
                endpoint,
                endpoint_addr: endpoint_addr.clone(),
                connection: None,
                generation: 0,
            }
            .run(commands),
        );
        Ok(Self {
            endpoint_addr,
            max_response_bytes,
            connections,
        })
    }

    pub async fn active_connection(&self) -> Option<iroh::endpoint::Connection> {
        self.request(IrohConnectionCommand::Inspect).await
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> IrohConnectionCommand,
    ) -> T {
        let (reply, response) = oneshot::channel();
        self.send(command(reply));
        // Short of panicking, the actor drops a reply unanswered only once
        // its receiver is gone, which cannot happen while this awaits it.
        response
            .await
            .expect("Iroh connection actor answers every live request")
    }

    fn send(&self, command: IrohConnectionCommand) {
        // This handle keeps the command channel open, so the actor only goes
        // away by panicking.
        self.connections
            .send(command)
            .expect("Iroh connection actor outlives its client handles");
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
    ) -> Result<ResponseEnvelope<O>, XrpcClientError<E>>
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
    ) -> Result<ResponseEnvelope<O>, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        let encoded = encode_xrpc_request(operation, &context, metadata, params)
            .map_err(XrpcClientError::Encode)?;
        let (connection, generation) =
            self.request(IrohConnectionCommand::Acquire)
                .await
                .map_err(|message| XrpcClientError::Transport {
                    message,
                    outcome_unknown: operation.kind == XrpcKind::Procedure,
                })?;
        let (send, recv) = match connection.open_bi().await {
            Ok(stream) => stream,
            Err(error) => {
                self.send(IrohConnectionCommand::Discard(generation));
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
            self.send(IrohConnectionCommand::Discard(generation));
        }
        decode_xrpc_response(operation, context.request_id, status, &headers, &body)
    }
}

#[cfg(test)]
mod tests;
