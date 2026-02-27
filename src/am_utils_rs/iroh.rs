//! modified from https://github.com/n0-computer/iroh-examples/tree/8b40bb5557bacbfd817a4b66a931aec6af655b51/iroh-automerge-repo
//! unkown license??
//! Combines [`iroh`] with automerge's [`samod`] library, a library to create "automerge repositories"
//! in rust that speak the automerge repo protocol.

use crate::interlude::*;

use codec::Codec;
use samod::ConnDirection;
use tokio_util::codec::{FramedRead, FramedWrite};

use super::AmCtx;

mod codec;

impl AmCtx {
    pub const SYNC_ALPN: &[u8] = b"townframe/automerge-repo/1";

    pub async fn spawn_connection_iroh(
        &self,
        endpoint: &iroh::Endpoint,
        addr: impl Into<iroh::EndpointAddr>,
        // rx_from_peer: iroh::endpoint::RecvStream,
        // tx_to_peer: iroh::endpoint::SendStream,
        // direction: samod::ConnDirection,
    ) -> Res<super::RepoConnection> {
        let addr = addr.into();
        let endpoint_id = addr.id;
        let conn = endpoint.connect(addr, Self::SYNC_ALPN).await?;
        let (tx, rx) = conn.open_bi().await?;

        let repo = self.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
                ConnDirection::Outgoing,
            )
        })
        .wrap_err("failed to establish connection")?;
        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| ferr!("failed on handshake: {err:?}"))?;
        let join_handle = tokio::spawn(
            async move {
                let fin_reason = conn.finished().await;
                info!(?fin_reason, "sync server connector task finished");
            }
            .instrument(tracing::info_span!("mpsc sync server connector task")),
        );

        Ok(super::RepoConnection {
            peer_info,
            join_handle,
        })
    }
}

impl iroh::protocol::ProtocolHandler for AmCtx {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let endpoint_id = connection.remote_id();

        let (tx, rx) = connection.accept_bi().await?;

        let repo = self.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
                ConnDirection::Incoming,
            )
        })
        .map_err(iroh::protocol::AcceptError::from_err)?;

        // let peer_info = conn
        //     .handshake_complete()
        //     .await
        //     .map_err(|err| Box::from(format!("failed on handshake: {err:?}")))
        //     .map_err(iroh::protocol::AcceptError::from_boxed)?;
        // let join_handle = tokio::spawn(
        //     async move {}.instrument(tracing::info_span!("mpsc sync server connector task")),
        // );
        let fin_reason = conn.finished().await;
        info!(?fin_reason, %endpoint_id, "incoming connection finished");

        Ok(())
    }

    async fn shutdown(&self) {
        self.repo.stop().await
    }
}
