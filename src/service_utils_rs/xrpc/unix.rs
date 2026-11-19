//! A Unix-domain-socket accept driver that reports the kernel's word on who connected.

use super::PeerCredentials;
use super::serve::AcceptFuture;
use super::serve::ConnectionAcceptor;

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
