//! A Unix-domain-socket accept driver that reports the kernel's word on who connected.

use super::serve::AcceptFuture;
use super::serve::ConnectionAcceptor;

/// The credentials the kernel holds for the process on the other end of a Unix socket:
/// `SO_PEERCRED` on Linux, `getpeereid`/`LOCAL_PEERCRED` on macOS and the BSDs (tokio picks the
/// mechanism per platform). The peer cannot forge them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub uid: u32,
    pub gid: u32,
    /// Not every platform reports a pid.
    pub pid: Option<i32>,
}

/// Accepts connections on a Unix listener and reports each peer's [`PeerCredentials`]. It makes no
/// admission decision: whoever serves the connections decides what a uid may do.
pub struct UnixConnectionAcceptor {
    listener: tokio::net::UnixListener,
}

impl UnixConnectionAcceptor {
    pub fn new(listener: tokio::net::UnixListener) -> Self {
        Self { listener }
    }
}

impl ConnectionAcceptor for UnixConnectionAcceptor {
    type Stream = tokio::net::UnixStream;
    type Peer = PeerCredentials;

    fn accept(&mut self) -> AcceptFuture<'_, Self::Stream, Self::Peer> {
        Box::pin(async move {
            let (stream, _) = self.listener.accept().await?;
            let credentials = stream.peer_cred()?;
            Ok((
                stream,
                PeerCredentials {
                    uid: credentials.uid(),
                    gid: credentials.gid(),
                    pid: credentials.pid(),
                },
            ))
        })
    }
}
