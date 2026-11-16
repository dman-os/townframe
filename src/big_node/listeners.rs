//! Listener attachments (ADR 033 §3, §5): serve the node's router over a Unix socket and/or TCP.
//! A listener is an attachment, so the node boots and stops complete without any. Which
//! operations exist is dispatch's business; a listener only decides who a connection is.
//!
//! - **Unix socket.** The kernel vouches for the peer's uid when it connects. Only the uid the
//!   node runs as is admitted, and it acts as the operator: an anonymous request is the
//!   operator's identity, and a presented bearer token narrows it. Every other uid is closed
//!   before a byte is read. The socket lives in a directory only its owner can enter.
//! - **TCP.** Nothing about the connection says who it is, so only the capability each request
//!   presents counts.

use big_cabinet_core::authority::Authorization;
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

    #[cfg(not(unix))]
    pub(crate) async fn unix(
        _path: &Path,
        _dispatch: Client<DispatchProtocol>,
        _operator: Authorization,
    ) -> Res<(Self, UnixClients)> {
        bail!("Unix socket listeners are only available on Unix platforms")
    }
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
