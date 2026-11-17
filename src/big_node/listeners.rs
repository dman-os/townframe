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
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre::WrapErr;
use utils_rs::prelude::eyre::bail;

/// Bound on one request body served over a listener.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
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
        let router = big_dispatch::xrpc_router(dispatch, Anonymous::Refused, MAX_BODY_BYTES);
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
    ) -> Res<Self> {
        use std::os::unix::fs::MetadataExt;
        let listener = bind_owner_only(path).await?;
        // The socket file belongs to the uid the node runs as, which is how that uid is
        // learned without a syscall of its own.
        let owner_uid = std::fs::symlink_metadata(path)?.uid();
        let router = big_dispatch::xrpc_router(dispatch, Anonymous::Acts(operator), MAX_BODY_BYTES);
        let (task, shutdown) = service_utils_rs::xrpc::serve_with_acceptor(
            OwnerOnly {
                owner_uid,
                inner: UnixConnectionAcceptor::new(listener),
            },
            router.into_router(),
            MAX_CONNECTIONS,
        )
        .await?;
        Ok(Self {
            shutdown,
            task,
            socket: Some(path.to_owned()),
        })
    }

    #[cfg(not(unix))]
    pub(crate) async fn unix(
        _path: &Path,
        _dispatch: Client<DispatchProtocol>,
        _operator: Authorization,
    ) -> Res<Self> {
        bail!("Unix socket listeners are only available on Unix platforms")
    }
}

/// Whether a peer may connect to the operator's socket: only the uid the node runs as.
fn admits(owner_uid: u32, peer: &PeerCredentials) -> bool {
    peer.uid == owner_uid
}

/// The admission policy of the Unix socket listener around the acceptor that reports peers:
/// it keeps accepting past, and closes, any connection whose uid is not the owner's.
struct OwnerOnly {
    owner_uid: u32,
    inner: UnixConnectionAcceptor,
}

impl ConnectionAcceptor for OwnerOnly {
    type Stream = tokio::net::UnixStream;
    type Peer = PeerCredentials;

    fn accept(&mut self) -> AcceptFuture<'_, Self::Stream, Self::Peer> {
        Box::pin(async move {
            loop {
                let (stream, peer) = self.inner.accept().await?;
                if admits(self.owner_uid, &peer) {
                    return Ok((stream, peer));
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
