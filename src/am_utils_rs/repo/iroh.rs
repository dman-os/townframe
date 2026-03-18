//! modified from https://github.com/n0-computer/iroh-examples/tree/8b40bb5557bacbfd817a4b66a931aec6af655b51/iroh-automerge-repo
//! MIT/Apache 2.0
//! Combines [`iroh`] with automerge's [`samod`] library, a library to create "automerge repositories"
//! in rust that speak the automerge repo protocol.

use crate::interlude::*;

use codec::Codec;
use samod::{AcceptorEvent, Dialer, DialerEvent, Transport};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use crate::repo::{BigRepo, SharedBigRepo};

mod codec;

const CONN_URL_SCHEME: &str = "db+iroh+samod";

fn close_iroh_conn(conn: Option<&iroh::endpoint::Connection>, code: u32, reason: &'static [u8]) {
    if let Some(conn) = conn {
        conn.close(code.into(), reason);
    }
}

impl BigRepo {
    pub const SYNC_ALPN: &[u8] = b"townframe/automerge-repo/0";

    #[tracing::instrument(skip(self, endpoint, end_signal_tx))]
    pub async fn spawn_connection_iroh(
        &self,
        endpoint: &iroh::Endpoint,
        to_addr: iroh::EndpointAddr,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::repo::ConnFinishSignal>>,
        // direction: samod::ConnDirection,
        // rx_from_peer: iroh::endpoint::RecvStream,
        // tx_to_peer: iroh::endpoint::SendStream,
    ) -> Res<crate::repo::RepoConnection> {
        let endpoint_id = to_addr.id;
        let repo = self.samod_repo().clone();
        let maybe_conn: Arc<std::sync::Mutex<Option<Arc<iroh::endpoint::Connection>>>> = default();
        let shutdown_confirmed = Arc::new(AtomicBool::new(false));
        let dialer = IrohDialer {
            url: Url::parse(&format!(
                "{CONN_URL_SCHEME}:{}",
                utils_rs::hash::encode_base58_multibase(endpoint_id)
            ))
            .expect(ERROR_IMPOSSIBLE),
            endpoint: endpoint.clone(),
            endpoint_id,
            to_addr,
            conn: Arc::clone(&maybe_conn),
            shutdown_confirmed: Arc::clone(&shutdown_confirmed),
        };
        let handle = repo
            .dial(samod::BackoffConfig::default(), Arc::new(dialer))
            .wrap_err("error setting up dialer")?;

        let peer_info = handle
            .established()
            .await
            .wrap_err("error during handshake")?;
        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();
        let conn_id = handle.connection_id().expect(ERROR_IMPOSSIBLE);

        let cancel_token = CancellationToken::new();
        let fut = {
            let cancel_token = cancel_token.clone();
            let peer_id = Arc::<str>::clone(&peer_id);
            async move {
                let mut events = handle.events();
                let mut last_reason = None;
                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            debug!("cancel token lit");
                            shutdown_confirmed.store(true, Ordering::SeqCst);
                            handle.close();
                            let cloned = maybe_conn.lock().expect(ERROR_MUTEX).clone();
                            close_iroh_conn(cloned.as_deref(), 220, b"we are shutting down");
                            break;
                        }
                        evt = events.next() => {
                            let Some(evt) = evt else {
                                handle.close();
                                let cloned = maybe_conn.lock().expect(ERROR_MUTEX).clone();
                                close_iroh_conn(
                                    cloned.as_deref(),
                                    500,
                                    b"samod connection stream ended abruptly",
                                );
                                eyre::bail!(
                                    "connection stream ended before disconnect"
                                );
                            };
                            match evt {
                                DialerEvent::Connected { peer_info } => {
                                    if peer_info.peer_id.as_str() != &peer_id[..] {
                                        eyre::bail!(
                                            "reconnection changed peer_id {peer_id} != {:?}",
                                            peer_info.peer_id
                                        );
                                    }
                                    info!("connection established");
                                }
                                DialerEvent::Disconnected { reason } => {
                                    last_reason = Some(reason);
                                    info!("connection lost");
                                },
                                DialerEvent::Reconnecting { attempt } => {
                                    if shutdown_confirmed.load(Ordering::SeqCst) {
                                        debug!(attempt, "skipping reconnect because shutdown was confirmed");
                                        break;
                                    }
                                    info!(?attempt, "trying to reconnect to peer");
                                }
                                DialerEvent::MaxRetriesReached => {
                                    info!("max retries reached, aborting");
                                    handle.close();
                                    let cloned = maybe_conn.lock().expect(ERROR_MUTEX).clone();
                                    close_iroh_conn(
                                        cloned.as_deref(),
                                        500,
                                        b"samod connection stream ended abruptly",
                                    );
                                    if let Some(tx) = end_signal_tx {
                                        tx.send(crate::repo::ConnFinishSignal {
                                            conn_id,
                                            peer_id,
                                            // FIXME: find better reason type
                                            reason: last_reason.unwrap_or_else(|| "max re-connention attempts reached".into()),
                                        })
                                        .inspect_err(|_| warn!("connection owner closed before finish"))
                                        .ok();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                eyre::Ok(())
            }
        };
        let join_handle = tokio::spawn(
            async {
                fut.await.unwrap();
            }
            .instrument(tracing::info_span!(
                "iroh connector task",
                peer = ?peer_info,
                endpoint_id = ?endpoint_id,
            )),
        );

        Ok(crate::repo::RepoConnection {
            id: conn_id,
            peer_id,
            peer_info,
            endpoint_id: Some(endpoint_id),
            join_handle: Some(join_handle),
            cancel_token,
        })
    }
}

struct IrohDialer {
    url: Url,
    endpoint: iroh::Endpoint,
    endpoint_id: iroh::PublicKey,
    to_addr: iroh::EndpointAddr,
    conn: Arc<std::sync::Mutex<Option<Arc<iroh::endpoint::Connection>>>>,
    shutdown_confirmed: Arc<AtomicBool>,
}

// FIXME: dialer doesn't support aborting dials
impl Dialer for IrohDialer {
    fn url(&self) -> Url {
        self.url.clone()
    }

    fn connect(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Transport, Box<dyn std::error::Error + Send + Sync>>,
                > + Send,
        >,
    > {
        let endpoint = self.endpoint.clone();
        let addr = self.to_addr.clone();
        let endpoint_id = self.endpoint_id;
        let conn = Arc::clone(&self.conn);
        let shutdown_confirmed = Arc::clone(&self.shutdown_confirmed);
        Box::pin(async move {
            if shutdown_confirmed.load(Ordering::SeqCst) {
                return Err("dial aborted: shutdown confirmed".into());
            }
            let cloned = conn.lock().expect(ERROR_MUTEX).clone();
            if let Some(conn) = cloned {
                if let Some(close_reason) = conn.close_reason() {
                    error!(?close_reason, "connection was closed");
                    match close_reason {
                        iroh::endpoint::ConnectionError::ApplicationClosed(application_close) => {
                            if application_close.error_code == 220_u32.into() {
                                warn!("the peer signalled that they're shutting down");
                                shutdown_confirmed.store(true, Ordering::SeqCst);
                                return Err("dial aborted: peer is shutting down".into());
                            }
                        }
                        iroh::endpoint::ConnectionError::LocallyClosed => {
                            debug!("we're shutting down locally");
                            shutdown_confirmed.store(true, Ordering::SeqCst);
                            return Err("dial aborted: local shutdown".into());
                        }
                        _ => {}
                    }
                } else {
                    warn!("re-dialing on a still open connection??");
                    let (tx, rx) = conn.open_bi().await?;
                    return Ok(Transport::new(
                        FramedRead::new(rx, Codec::new(endpoint_id)),
                        FramedWrite::new(tx, Codec::new(endpoint_id)),
                    ));
                }
            }
            let new_conn = endpoint.connect(addr, BigRepo::SYNC_ALPN).await?;
            let new_conn = Arc::new(new_conn);
            let (tx, rx) = new_conn.open_bi().await?;
            conn.lock().expect(ERROR_MUTEX).replace(new_conn);
            Ok(Transport::new(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct IrohRepoProtocol {
    pub big_repo: SharedBigRepo,
    pub cancel_token: CancellationToken,
    pub conn_tx: tokio::sync::mpsc::UnboundedSender<crate::repo::RepoConnection>,
    pub end_signal_tx: tokio::sync::mpsc::UnboundedSender<crate::repo::ConnFinishSignal>,
}

impl iroh::protocol::ProtocolHandler for IrohRepoProtocol {
    #[tracing::instrument(skip(self, connection))]
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let endpoint_id = connection.remote_id();
        tracing::record_all!(
            tracing::Span::current(),
            endpoint_id = ?endpoint_id,
        );

        let (tx, rx) = connection.accept_bi().await?;

        let repo = self.big_repo.samod_repo().clone();
        let acceptor = repo
            .make_acceptor(
                Url::parse(&format!(
                    "{CONN_URL_SCHEME}:{}",
                    utils_rs::hash::encode_base58_multibase(endpoint_id)
                ))
                .expect(ERROR_IMPOSSIBLE),
            )
            .expect("error making acceptor");
        acceptor
            .accept(Transport::new(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
            ))
            .map_err(|err| {
                let msg = format!("failed making samod acceptor for {endpoint_id}: {err:?}");
                connection.close(500u32.into(), msg.as_bytes());
                Box::from(msg)
            })
            .map_err(iroh::protocol::AcceptError::from_boxed)?;

        let mut events = acceptor.events();
        let event = events.next().await;
        let Some(event) = event else {
            close_iroh_conn(
                Some(&connection),
                500,
                b"samod connection stream ended abruptly before connection",
            );
            return Err(iroh::protocol::AcceptError::from_boxed(Box::from(format!(
                "connection stream ended before disconnect with {endpoint_id}"
            ))));
        };
        let (conn_id, peer_info) = match event {
            AcceptorEvent::ClientConnected {
                connection_id,
                peer_info,
            } => (connection_id, peer_info),
            AcceptorEvent::ClientDisconnected { reason, .. } => {
                let msg = format!("failed on samod handshake with {endpoint_id}: {reason}");
                connection.close(500u32.into(), msg.as_bytes());
                return Err(iroh::protocol::AcceptError::from_boxed(Box::from(msg)));
            }
        };

        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();

        tracing::record_all!(
            tracing::Span::current(),
            peer = ?peer_info,
        );

        let cancel_token = self.cancel_token.child_token();
        self.conn_tx
            .send(crate::repo::RepoConnection {
                id: conn_id,
                peer_id: Arc::<str>::clone(&peer_id),
                join_handle: None,
                peer_info,
                endpoint_id: Some(endpoint_id),
                cancel_token: cancel_token.clone(),
            })
            .expect(ERROR_CHANNEL);

        tokio::select! {
            _ = cancel_token.cancelled() => {
                close_iroh_conn(Some(&connection), 220, b"we are shutting down");
                debug!("cancel token lit");
            }
            evt = events.next() => {
                let Some(evt) = evt else {
                    close_iroh_conn(Some(&connection), 500, b"samod connection stream ended abruptly");
                    return Err(iroh::protocol::AcceptError::from_boxed(Box::from(format!(
                        "connection stream ended before disconnect with {endpoint_id}"
                    ))));
                };
                match evt {
                    AcceptorEvent::ClientDisconnected {
                        reason,
                        ..
                    } => {
                        close_iroh_conn(Some(&connection), 200, b"you disconnected");
                        info!(?reason, "incoming connection finished");
                        self.end_signal_tx.send(crate::repo::ConnFinishSignal {
                            conn_id,
                            peer_id,
                            reason: format!("{reason}"),
                        })
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                    },
                    AcceptorEvent::ClientConnected {..} => {
                        unreachable!()
                    }
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        self.cancel_token.cancel();
        // BigRepo shutdown owns samod repo shutdown; protocol shutdown only tears down routing.
    }
}
