//! XRPC over iroh: the typed service client's transport to a peer reached by its iroh endpoint,
//! on native hosts and in the browser (wasm32-unknown-unknown, where iroh is relay-only).
//!
//! The wire is HTTP/1, one exchange per QUIC bi-stream, on one QUIC connection per peer that
//! every call reuses. [`IrohHttp`] moves the bytes of one exchange as a
//! [`service_client_core_rs::xrpc::HttpTransport`]; the envelope codec is the core's [`Xrpc`].
//! `Client::new(IrohHttp::new(..))` is the typed client: [`IrohHttp`] is also a
//! [`UnaryTransport`], which adds the deadline to [`Xrpc`].
//!
//! The runtime is [`n0_future`]'s, which is tokio on a native host and the browser's event loop
//! in wasm: it spawns the one connection actor per peer and times the deadline. Nothing spawns
//! per call: the hyper connection of an exchange is driven on the caller's task beside it. The server
//! side of the wire is `service_utils_rs`'s iroh protocol handler, which serves
//! [`IrohHttpStream`] too.
//!
//! Unary calls only. The server already upgrades an iroh exchange to its websocket
//! subscription wire, so subscriptions need the client half: hyper's upgrade of the response
//! and the websocket framing that lives in `service_client_native_rs` today, which would move
//! here to build for the browser.

use http::HeaderValue;
use http::header;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use method_core::ContextError;
use method_core::Unary;
use service_client_core_rs::UnaryTransport;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::Xrpc;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_core_rs::ClientError;
use service_core_rs::Envelope;
use service_core_rs::ResponseEnvelope;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub const IROH_XRPC_HTTP_ALPN: &[u8] = b"townframe/xrpc-http/1";

/// One QUIC bi-stream as hyper's I/O, for either end of an exchange.
///
/// Dropping it before the response is read to its end stops the receive half, which sends the
/// peer STOP_SENDING; the server watches for that and drops the handler of the exchange.
pub struct IrohHttpStream {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

impl IrohHttpStream {
    pub fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
        Self { send, recv }
    }
}

impl hyper::rt::Read for IrohHttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        // iroh reads into tokio's `ReadBuf`, which hyper's cursor does not expose; this is the
        // bridge `hyper_util::rt::TokioIo` uses, without its tokio runtime features.
        // SAFETY: `ReadBuf::uninit` only writes initialised bytes into the cursor's spare
        // capacity and counts them in `filled`, so exactly `filled` bytes are initialised for
        // `advance`.
        let filled = unsafe {
            let mut read = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(Pin::new(&mut self.recv), cx, &mut read) {
                Poll::Ready(Ok(())) => read.filled().len(),
                other => return other,
            }
        };
        // SAFETY: see above.
        unsafe { buf.advance(filled) };
        Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for IrohHttpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
    }

    /// Finishes the send half: the peer reads the end of the stream.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}

/// XRPC exchanges with one peer over one retained iroh connection.
///
/// The first call connects; later calls reuse the connection, each on a bi-stream of its own,
/// so concurrent calls multiplex over it. A closed connection is replaced by the next call that
/// finds it closed. Clones share the connection.
///
/// Handles never touch the retained connection: one connection actor task owns it, and a call
/// asks it for the connection, then opens its stream and drives its exchange on the caller's
/// task. The actor connects inline, so concurrent calls wait for a single connect and share its
/// connection; the connect belongs to the call that asked for it, and a call dropped mid-connect
/// abandons it. The actor stops when the last handle drops, dropping the connection.
///
/// A dropped call drops its stream, which stops the server's handler (see
/// [`IrohHttpStream`]) and leaves the connection to the other calls.
#[derive(Clone)]
pub struct IrohHttp {
    // Only the peer's id: a handle is cloned per call, its full address set is not.
    peer: iroh::EndpointId,
    max_response_bytes: usize,
    // Unbounded so that sending is synchronous; a call has at most one command outstanding.
    connections: mpsc::UnboundedSender<ConnectionCommand>,
}

#[derive(Debug)]
enum ConnectionCommand {
    /// Reply with the retained connection, connecting first when none is retained or the
    /// retained one has closed.
    Acquire(oneshot::Sender<Result<iroh::endpoint::Connection, String>>),
    /// Reply with the retained connection as it is, without connecting.
    Inspect(oneshot::Sender<Option<iroh::endpoint::Connection>>),
}

struct ConnectionActor {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    connection: Option<iroh::endpoint::Connection>,
}

impl ConnectionActor {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<ConnectionCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                ConnectionCommand::Acquire(mut reply) => {
                    let acquired = tokio::select! {
                        biased;
                        () = reply.closed() => continue,
                        acquired = self.acquire() => acquired,
                    };
                    // A failed send means the call was dropped after the connect resolved; the
                    // connection stays retained for the next one.
                    if let Err(unclaimed) = reply.send(acquired) {
                        drop(unclaimed);
                    }
                }
                ConnectionCommand::Inspect(reply) => {
                    // A failed send means the caller was dropped.
                    if let Err(unclaimed) = reply.send(self.connection.clone()) {
                        drop(unclaimed);
                    }
                }
            }
        }
    }

    async fn acquire(&mut self) -> Result<iroh::endpoint::Connection, String> {
        if let Some(connection) = &self.connection
            && connection.close_reason().is_none()
        {
            return Ok(connection.clone());
        }
        let connection = self
            .endpoint
            .connect(self.endpoint_addr.clone(), IROH_XRPC_HTTP_ALPN)
            .await
            .map_err(|error| error.to_string())?;
        self.connection = Some(connection.clone());
        Ok(connection)
    }
}

impl std::fmt::Debug for IrohHttp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohHttp")
            .field("peer", &self.peer)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

impl IrohHttp {
    /// Calls the XRPC server of `endpoint_addr` through `endpoint`. A response body larger than
    /// `max_response_bytes` fails the call.
    ///
    /// Spawns the connection actor through [`n0_future::task::spawn`]: on a native host this
    /// must run inside a tokio runtime, in the browser it is a `wasm-bindgen-futures` task.
    pub fn new(
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        max_response_bytes: usize,
    ) -> Self {
        assert!(
            max_response_bytes > 0,
            "response body limit must be positive"
        );
        let peer = endpoint_addr.id;
        let (connections, commands) = mpsc::unbounded_channel();
        // Detached on purpose: the actor's only exit is the last handle dropping its sender.
        // It has no fallible path of its own (connect failures are replies), so a panic is a bug
        // that the process panic hook and the handles' `expect`s both surface.
        drop(n0_future::task::spawn(
            ConnectionActor {
                endpoint,
                endpoint_addr,
                connection: None,
            }
            .run(commands),
        ));
        Self {
            peer,
            max_response_bytes,
            connections,
        }
    }

    /// The retained connection as it is, closed or not; `None` before the first call connects.
    pub async fn active_connection(&self) -> Option<iroh::endpoint::Connection> {
        self.request(ConnectionCommand::Inspect).await
    }

    async fn request<T>(&self, command: impl FnOnce(oneshot::Sender<T>) -> ConnectionCommand) -> T {
        let (reply, response) = oneshot::channel();
        // This handle keeps the command channel open, so the actor only goes away by panicking.
        self.connections
            .send(command(reply))
            .expect("the connection actor outlives its handles");
        // Short of panicking, the actor drops a reply unanswered only once its receiver is
        // gone, which cannot happen while this awaits it.
        response
            .await
            .expect("the connection actor answers every live request")
    }

    async fn exchange(&self, request: XrpcRequest) -> Result<XrpcResponse, String> {
        let connection = self.request(ConnectionCommand::Acquire).await?;
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|error| error.to_string())?;
        let (mut sender, driver) =
            hyper::client::conn::http1::handshake(IrohHttpStream::new(send, recv))
                .await
                .map_err(|error| error.to_string())?;
        let mut http = hyper::Request::builder()
            .method(request.method)
            .uri(format!("/{}", request.path))
            .body(http_body_util::Full::new(Bytes::from(request.body)))
            .map_err(|error| error.to_string())?;
        *http.headers_mut() = request.headers;
        // One exchange per stream: the server closes the stream after its response, and so
        // does the driver below, which is what lets it finish.
        http.headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        let max_response_bytes = self.max_response_bytes;
        let exchange = async move {
            let response = sender
                .send_request(http)
                .await
                .map_err(|error| error.to_string())?;
            let (parts, body) = response.into_parts();
            let body = collect_body(body, max_response_bytes).await?;
            Ok(XrpcResponse {
                status: parts.status,
                headers: parts.headers,
                body,
            })
        };
        // hyper's connection future does the stream I/O of the exchange, so it is polled beside
        // it on this task instead of being spawned. A failure of either ends both, dropping the
        // stream. The driver's own end of an exchange (the peer closing, a cancelled or
        // incomplete message) also fails `exchange`, which reports it.
        let driver = async move {
            match driver.await {
                Ok(()) => Ok(()),
                Err(error)
                    if error.is_closed()
                        || error.is_canceled()
                        || error.is_incomplete_message()
                        || error.is_shutdown() =>
                {
                    Ok(())
                }
                Err(error) => Err(format!("HTTP connection: {error}")),
            }
        };
        let (response, ()) = futures::future::try_join(exchange, driver).await?;
        Ok(response)
    }
}

async fn collect_body(
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

impl HttpTransport for IrohHttp {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        self.exchange(request)
            .await
            .map_err(|message| HttpTransportError { message })
    }
}

/// [`Xrpc`] over this transport, bounded by the call's deadline: a spent budget answers
/// [`ContextError::DeadlineExceeded`] and drops the exchange, which stops the server's handler.
/// [`Xrpc`] alone only sends the budget along for the server to enforce.
impl<P: Unary> UnaryTransport<P> for IrohHttp {
    async fn call(
        &self,
        request: Envelope<P>,
    ) -> Result<ResponseEnvelope<P::Output>, ClientError<P::Error>> {
        let budget = request.context.remaining_budget();
        let xrpc = Xrpc(self.clone());
        let call = UnaryTransport::<P>::call(&xrpc, request);
        match budget {
            Some(budget) => n0_future::time::timeout(budget, call)
                .await
                .unwrap_or_else(|_| Err(ContextError::DeadlineExceeded.into())),
            None => call.await,
        }
    }
}

#[cfg(test)]
mod tests;
