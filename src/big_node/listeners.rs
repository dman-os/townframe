//! Listener attachments (ADR 033 §3, §5): serve the node's router over a Unix socket and/or TCP.
//! A listener is an attachment, so the node boots and stops complete without any. Which
//! The descriptor socket is a strictly one-request/one-response stream: SCM_RIGHTS must arrive
//! with the first `recvmsg` that returns frame bytes; descriptors on a later read, ancillary
//! truncation, or any new frame before the response closes the connection.
//! operations exist is dispatch's business; a listener only decides who a connection is.
//!
//! - **Unix socket.** The kernel vouches for the peer's uid when it connects. Only the uid the
//!   node runs as is admitted, and it acts as the operator: an anonymous request is the
//!   operator's identity, and a presented bearer token narrows it. Every other uid is closed
//!   before a byte is read. The socket lives in a directory only its owner can enter.
//! - **TCP.** Nothing about the connection says who it is, so only the capability each request
//!   presents counts.

use big_cabinet_core::authority::Authorization;
use big_dispatch_core::Body;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::Origin;
use service_utils_rs::ClientError;
use service_utils_rs::HttpMetadata;
use service_utils_rs::RequestContext;
use service_utils_rs::descriptor;

use big_dispatch::Anonymous;
use big_dispatch::Edge;
use big_dispatch_core::DispatchProtocol;
use service_client_native_rs::Client;
use service_utils_rs::xrpc::AcceptFuture;
use service_utils_rs::xrpc::ConnectionAcceptor;
use service_utils_rs::xrpc::PeerCredentials;
use service_utils_rs::xrpc::UnixConnectionAcceptor;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::sync::watch;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre::WrapErr;
use utils_rs::prelude::eyre::bail;

const MAX_CONNECTIONS: usize = 256;

/// Where the node listens. Both are optional and a node with neither is complete. On a booted
/// node this holds what was bound: a TCP port of `0` is the one the OS chose.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listeners {
    /// The Unix socket path. The node creates its missing parent directories with mode `0700`
    /// (and refuses an existing parent that other users can enter), removes a stale socket and
    /// removes the socket again on stop. Unix platforms only.
    pub unix_socket: Option<PathBuf>,
    /// The descriptor socket paired with `unix_socket`, when the node serves local descriptor calls.
    #[cfg(unix)]
    pub fd_socket: Option<PathBuf>,
    pub tcp: Option<SocketAddr>,
}

/// A running listener: the serving task, how to ask it to stop, and the socket file it owns.
pub(crate) struct Listener {
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    socket: Option<PathBuf>,
}

impl Listener {
    /// Closes the listener and every connection, then removes the socket file.
    pub(crate) async fn stop(self) -> Res<()> {
        // A send failure means the serving task has already ended; its result, joined below,
        // says why.
        let ended_early = self.shutdown.send(()).is_err();
        let served = self.task.await.expect("the listener task panicked");
        if let Some(socket) = &self.socket {
            match std::fs::remove_file(socket) {
                Ok(()) => {}
                // Removed behind our back: what we wanted is already so.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).wrap_err_with(|| format!("removing {}", socket.display()));
                }
            }
        }
        served.wrap_err("the listener failed")?;
        assert!(
            !ended_early,
            "a listener only ends cleanly when asked to stop"
        );
        Ok(())
    }

    /// Serves the router over TCP at `address`; returns the listener and the bound address.
    pub(crate) async fn tcp(
        address: SocketAddr,
        dispatch: Client<DispatchProtocol>,
    ) -> Res<(Self, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .wrap_err_with(|| format!("binding {address}"))?;
        let bound = listener.local_addr()?;
        let router = big_dispatch::xrpc_router(dispatch, Anonymous::Refused, Edge::Tcp);
        let (task, shutdown) = service_utils_rs::xrpc::serve_with_connection_limit(
            listener,
            router.into_router(),
            MAX_CONNECTIONS,
        )
        .await?;
        Ok((
            Self {
                shutdown,
                task,
                socket: None,
            },
            bound,
        ))
    }

    /// Serves the router over the Unix socket at `path`, admitting `operator` for any peer that
    /// runs as the node's uid.
    #[cfg(unix)]
    pub(crate) async fn unix(
        path: &Path,
        dispatch: Client<DispatchProtocol>,
        operator: Authorization,
    ) -> Res<(Self, UnixClients)> {
        use std::os::unix::fs::MetadataExt;
        let listener = bind_owner_only(path).await?;
        let (connections, open) = watch::channel(0);
        let (drain, drain_requested) = watch::channel(false);
        let (closed, listener_closed) = watch::channel(false);
        // The socket file belongs to the uid the node runs as, which is how that uid is
        // learned without a syscall of its own.
        let owner_uid = std::fs::symlink_metadata(path)?.uid();
        let router = big_dispatch::xrpc_router(dispatch, Anonymous::Acts(operator), Edge::Unix);
        let (task, shutdown) = service_utils_rs::xrpc::serve_with_acceptor(
            OwnerOnly {
                owner_uid,
                inner: Some(UnixConnectionAcceptor::new(listener)),
                socket: path.to_owned(),
                connections,
                drain: drain_requested,
                closed,
            },
            router.into_router(),
            MAX_CONNECTIONS,
        )
        .await?;
        Ok((
            Self {
                shutdown,
                task,
                socket: Some(path.to_owned()),
            },
            UnixClients {
                open,
                drain,
                listener_closed,
            },
        ))
    }

    /// Serves descriptor frames for local-only calls.  The connection is strictly one request and
    /// one response: a new frame arriving before the response is a protocol refusal.
    #[cfg(unix)]
    pub(crate) async fn fd(
        path: &Path,
        dispatch: Client<DispatchProtocol>,
        operator: Authorization,
    ) -> Res<(Self, FdClients)> {
        use std::os::unix::fs::MetadataExt;

        let listener = bind_owner_only(path).await?;
        let owner_uid = std::fs::symlink_metadata(path)?.uid();
        let (connections, open) = watch::channel(0);
        let (drain, mut drain_requested) = watch::channel(false);
        let (closed, listener_closed) = watch::channel(false);
        let (shutdown, mut shutdown_requested) = oneshot::channel();
        // Keep the drain channel alive until the listener task observes shutdown; callers may drop
        // the node before stopping its staged listener.
        let drain_liveness = drain.clone();
        let socket = path.to_owned();
        let task = tokio::spawn(async move {
            let _drain_liveness = drain_liveness;
            // `None` once the listening socket was closed on request ([`FdClients::close_listener`]):
            // like the HTTP socket's, the task then keeps its open connections and accepts no more,
            // and ends only when it is stopped.
            let mut listener = Some(listener);
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let accepted = tokio::select! {
                    completed = tasks.join_next(), if !tasks.is_empty() => {
                        completed
                            .expect("descriptor socket connection task disappeared")
                            .expect("descriptor socket connection task panicked");
                        continue;
                    }
                    accepted = async {
                        match &listener {
                            Some(listener) => listener.accept().await.map(Some),
                            None => std::future::pending().await,
                        }
                    } => accepted?,
                    _ = &mut shutdown_requested => break,
                    () = async {
                        // Nobody left to ask for a drain (the node dropped its handle) or the
                        // listener is already closed: it serves until it is stopped.
                        if listener.is_none()
                            || drain_requested.wait_for(|requested| *requested).await.is_err()
                        {
                            std::future::pending::<()>().await;
                        }
                    } => None,
                };
                let Some((stream, _)) = accepted else {
                    // Dropping the listener closes the socket, so a later connect is refused
                    // instead of being accepted and then aborted at stop.
                    listener = None;
                    match std::fs::remove_file(&socket) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                    closed.send_replace(true);
                    continue;
                };
                let peer = match stream.peer_cred() {
                    Ok(peer) => peer,
                    Err(error) => {
                        tracing::debug!(%error, "descriptor socket peer credential lookup failed");
                        drop(stream);
                        continue;
                    }
                };
                if peer.uid() != owner_uid {
                    drop(stream);
                    continue;
                }
                let credentials = PeerCredentials {
                    uid: peer.uid(),
                    gid: peer.gid(),
                    pid: peer.pid(),
                };
                let dispatch = dispatch.clone();
                let operator = operator;
                let connections = connections.clone();
                tasks.spawn(async move {
                    connections.send_modify(|open| *open += 1);
                    let result = serve_fd_connection(stream, credentials, dispatch, operator).await;
                    connections.send_modify(|open| *open -= 1);
                    if let Err(error) = result {
                        tracing::debug!(%error, "descriptor socket connection refused");
                    }
                });
            }
            // Stopped: the socket closes and its file goes (stop removes it too, if this did not),
            // then the connections still open are let finish.
            drop(listener);
            closed.send_replace(true);
            while let Some(result) = tasks.join_next().await {
                result.expect("descriptor socket connection task panicked");
            }
            Ok(())
        });
        Ok((
            Self {
                shutdown,
                task,
                socket: Some(path.to_owned()),
            },
            FdClients {
                open,
                drain,
                listener_closed,
            },
        ))
    }

    #[cfg(not(unix))]
    pub(crate) async fn unix(
        _path: &Path,
        _dispatch: Client<DispatchProtocol>,
        _operator: Authorization,
    ) -> Res<(Self, UnixClients)> {
        bail!("Unix socket listeners are only available on Unix platforms")
    }
}

/// The fd socket's lifecycle handle, parallel to [`UnixClients`] for the HTTP socket.
#[cfg(unix)]
pub struct FdClients {
    open: watch::Receiver<usize>,
    drain: watch::Sender<bool>,
    listener_closed: watch::Receiver<bool>,
}

#[cfg(unix)]
impl FdClients {
    pub async fn close_listener(&self) -> Res<()> {
        self.drain.send_replace(true);
        self.listener_closed
            .clone()
            .wait_for(|closed| *closed)
            .await
            .wrap_err(ENDED)?;
        Ok(())
    }

    pub async fn wait_until_idle(&self) -> Res<()> {
        self.open
            .clone()
            .wait_for(|open| *open == 0)
            .await
            .wrap_err(ENDED)?;
        Ok(())
    }

    pub async fn drain(&self) -> Res<()> {
        self.close_listener().await?;
        self.wait_until_idle().await
    }
}

#[cfg(unix)]
async fn serve_fd_connection(
    mut stream: tokio::net::UnixStream,
    peer: PeerCredentials,
    dispatch: Client<DispatchProtocol>,
    operator: Authorization,
) -> std::io::Result<()> {
    let frame = receive_fd_frame(&stream).await?;
    let invocation = Invoke {
        operation: frame.operation,
        parameters: frame.parameters,
        body: Body::Descriptors(frame.body),
        context: InvocationContext {
            origin: Origin::UnixPeer { uid: peer.uid },
            ..Default::default()
        },
    };
    let response = match dispatch
        .call_with(
            RequestContext::root(),
            HttpMetadata {
                authorization: Some(operator.header_value()),
                ..Default::default()
            },
            invocation,
        )
        .await
    {
        Ok(output) => match output.body {
            Body::Json(value) => {
                let count = value
                    .get("results")
                    .and_then(serde_json::Value::as_array)
                    .or_else(|| value.as_array())
                    .map_or(0, Vec::len);
                if count > 0 || value.get("results").is_some() || value.is_array() {
                    descriptor::encode_response(0, count, &value)?
                } else {
                    descriptor::encode_response(1, 0, &value)?
                }
            }
            body => descriptor::encode_response(
                1,
                0,
                &serde_json::json!({
                    "error": "InvalidOutput",
                    "message": format!("descriptor call answered a {} body", body.case()),
                }),
            )?,
        },
        Err(ClientError::Method(error)) => descriptor::encode_response(
            1,
            0,
            &serde_json::to_value(error).expect("dispatch errors serialize"),
        )?,
        Err(ClientError::Transport { message, .. }) => {
            return Err(std::io::Error::other(message));
        }
    };
    stream.write_all(&response).await
}

#[cfg(unix)]
async fn receive_fd_frame(
    stream: &tokio::net::UnixStream,
) -> std::io::Result<descriptor::DescriptorFrame> {
    use tokio::io::Interest;

    let mut frame = Vec::new();
    let mut descriptors = Vec::new();
    let mut first_read = true;
    let mut expected: Option<usize> = None;
    loop {
        let capacity = if first_read {
            1
        } else {
            expected
                .map(|length| length.saturating_sub(frame.len()))
                .unwrap_or(descriptor::MAX_HEADER_OCTETS + 4 - frame.len())
        };
        let mut buffer = vec![0; capacity.max(1)];
        let part = stream
            .async_io(Interest::READABLE, || {
                descriptor::recv_part(stream, &mut buffer)
            })
            .await?;
        if part.ancillary_truncated || part.unexpected_ancillary {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "descriptor ancillary data was truncated or unexpected",
            ));
        }
        if (!first_read && !part.descriptors.is_empty())
            || (first_read && part.bytes.is_empty() && !part.descriptors.is_empty())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "descriptors did not arrive with the first bytes of the frame",
            ));
        }
        if part.bytes.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "descriptor frame ended before its header",
            ));
        }
        frame.extend_from_slice(&part.bytes);
        if first_read {
            descriptors.extend(part.descriptors);
        }
        if expected.is_none() && frame.len() >= 4 {
            let header_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            if header_len > descriptor::MAX_HEADER_OCTETS {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "descriptor frame header is too long",
                ));
            }
            expected = Some(header_len + 4);
        }
        if let Some(length) = expected {
            if frame.len() > length {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "a second descriptor frame arrived before the response",
                ));
            }
            if frame.len() == length {
                break;
            }
        }
        first_read = false;
    }
    let mut extra = [0; 1];
    match stream.try_io(Interest::READABLE, || {
        descriptor::recv_part(stream, &mut extra)
    }) {
        Ok(part) => {
            if !part.bytes.is_empty()
                || !part.descriptors.is_empty()
                || part.ancillary_truncated
                || part.unexpected_ancillary
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "additional descriptor frame arrived before the response",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    descriptor::decode_request(&frame, descriptors)
}

/// What a node's Unix socket listener is serving, and how to wind it down without closing a
/// client that is still connected.
pub struct UnixClients {
    open: watch::Receiver<usize>,
    drain: watch::Sender<bool>,
    listener_closed: watch::Receiver<bool>,
}

impl UnixClients {
    /// Stops accepting connections and returns once the socket is closed and its file removed:
    /// a connect after this is refused, never accepted and then aborted. Connections accepted
    /// before keep being served.
    pub async fn close_listener(&self) -> Res<()> {
        self.drain.send_replace(true);
        self.listener_closed
            .clone()
            .wait_for(|closed| *closed)
            .await
            .wrap_err(ENDED)?;
        Ok(())
    }

    /// Returns once every accepted connection has ended.
    pub async fn wait_until_idle(&self) -> Res<()> {
        self.open
            .clone()
            .wait_for(|open| *open == 0)
            .await
            .wrap_err(ENDED)?;
        Ok(())
    }

    /// Closes the listener, then waits until the clients it had accepted are gone. Stopping the
    /// node after this closes no client.
    pub async fn drain(&self) -> Res<()> {
        self.close_listener().await?;
        self.wait_until_idle().await
    }
}

const ENDED: &str = "the Unix socket listener ended before it was drained";

/// Whether a peer may connect to the operator's socket: only the uid the node runs as.
fn admits(owner_uid: u32, peer: &PeerCredentials) -> bool {
    peer.uid == owner_uid
}

/// The admission policy of the Unix socket listener around the acceptor that reports peers:
/// it keeps accepting past, and closes, any connection whose uid is not the owner's.
struct OwnerOnly {
    owner_uid: u32,
    /// The listening socket; `None` once it was closed on request ([`UnixClients::drain`]).
    inner: Option<UnixConnectionAcceptor>,
    socket: PathBuf,
    drain: watch::Receiver<bool>,
    /// Set when the listening socket has been closed and its file removed.
    closed: watch::Sender<bool>,
    /// The number of admitted connections still open.
    connections: watch::Sender<usize>,
}

/// An admitted connection, counted open until it is dropped: the serving loop drops the stream
/// when the connection ends, so the count falls whichever side closed it.
struct Counted {
    inner: tokio::net::UnixStream,
    connections: watch::Sender<usize>,
}

impl Counted {
    fn new(inner: tokio::net::UnixStream, connections: watch::Sender<usize>) -> Self {
        connections.send_modify(|open| *open += 1);
        Self { inner, connections }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.connections.send_modify(|open| *open -= 1);
    }
}

impl tokio::io::AsyncRead for Counted {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for Counted {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl ConnectionAcceptor for OwnerOnly {
    type Stream = Counted;
    type Peer = PeerCredentials;

    fn accept(&mut self) -> AcceptFuture<'_, Self::Stream, Self::Peer> {
        Box::pin(async move {
            loop {
                let Some(inner) = self.inner.as_mut() else {
                    // Closed: the serving loop keeps its open connections and accepts no more.
                    return std::future::pending().await;
                };
                let accepted = tokio::select! {
                    accepted = inner.accept() => Some(accepted?),
                    () = async {
                        // Nobody left to ask for a drain (the node dropped its handle): the
                        // listener serves until it is stopped.
                        if self.drain.wait_for(|requested| *requested).await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    } => None,
                };
                let Some((stream, peer)) = accepted else {
                    // Dropping the acceptor closes the socket, so a later connect is refused
                    // instead of being accepted and then aborted at stop.
                    self.inner = None;
                    match std::fs::remove_file(&self.socket) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                    self.closed.send_replace(true);
                    continue;
                };
                if admits(self.owner_uid, &peer) {
                    return Ok((Counted::new(stream, self.connections.clone()), peer));
                }
                tracing::warn!(
                    uid = peer.uid,
                    pid = peer.pid,
                    "refused a Unix socket connection from another uid"
                );
                drop(stream);
            }
        })
    }
}

/// Binds `path` in a directory only the owner can enter, replacing a stale socket but never a
/// live one or any other kind of file.
#[cfg(unix)]
async fn bind_owner_only(path: &Path) -> Res<tokio::net::UnixListener> {
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        bail!("{} has no parent directory to hold it", path.display());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .wrap_err_with(|| format!("creating {}", parent.display()))?;
    let mode = std::fs::metadata(parent)?.mode();
    if mode & 0o077 != 0 {
        bail!(
            "{} is accessible to other users (mode {:o}); the node's socket needs a 0700 directory",
            parent.display(),
            mode & 0o7777
        );
    }
    match std::fs::symlink_metadata(path) {
        Ok(found) if found.file_type().is_socket() => {
            match tokio::net::UnixStream::connect(path).await {
                Ok(_) => bail!("another process is listening on {}", path.display()),
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path).wrap_err_with(|| {
                        format!("removing the stale socket {}", path.display())
                    })?;
                }
                Err(error) => {
                    return Err(error)
                        .wrap_err_with(|| format!("probing the socket {}", path.display()));
                }
            }
        }
        Ok(_) => bail!("{} exists and is not a socket", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).wrap_err_with(|| format!("inspecting {}", path.display())),
    }
    let listener = tokio::net::UnixListener::bind(path)
        .wrap_err_with(|| format!("binding {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_uid_the_node_runs_as_is_admitted() {
        let peer = |uid| PeerCredentials {
            uid,
            gid: 100,
            pid: Some(1),
        };
        assert!(admits(1000, &peer(1000)));
        assert!(!admits(1000, &peer(1001)));
        assert!(!admits(1000, &peer(0)));
        assert!(!admits(0, &peer(1000)));
    }
}
