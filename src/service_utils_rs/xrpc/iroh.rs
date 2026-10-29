use super::serve::expected_peer_error;
use axum::Router;
pub use service_client_rs::xrpc::iroh::IROH_XRPC_HTTP_ALPN;
use service_client_rs::xrpc::iroh::IrohHttpStream;
pub use service_client_rs::xrpc::iroh::IrohXrpcClient;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio::task::JoinSet;

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
            .keep_alive(false).half_close(true)
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
                        Some(Ok(Err(error))) => { first_error.get_or_insert(error.to_string()); streams.abort_all(); break; }
                        Some(Err(error)) if !error.is_cancelled() => { first_error.get_or_insert(error.to_string()); streams.abort_all(); break; }
                        Some(Err(_)) => {}
                        None => unreachable!("guarded JoinSet branch cannot be empty"),
                    }
                }
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { streams.abort_all(); break; } }
                closed = connection.closed() => { drop(closed); streams.abort_all(); break; }
                accepted = connection.accept_bi() => {
                    let (send, recv) = match accepted { Ok(stream) => stream, Err(_) => { streams.abort_all(); break; } };
                    let permit = loop {
                        tokio::select! {
                            biased;
                            joined = streams.join_next(), if !streams.is_empty() => {
                                match joined {
                                    Some(Ok(Ok(()))) => {}
                                    Some(Ok(Err(error))) if expected_peer_error(&error) => {}
                                    Some(Ok(Err(error))) => { first_error.get_or_insert(error.to_string()); streams.abort_all(); break 'accept; }
                                    Some(Err(error)) if !error.is_cancelled() => { first_error.get_or_insert(error.to_string()); streams.abort_all(); break 'accept; }
                                    Some(Err(_)) => {}
                                    None => unreachable!("guarded JoinSet branch cannot be empty"),
                                }
                            }
                            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { streams.abort_all(); break 'accept; } }
                            closed = connection.closed() => { drop(closed); streams.abort_all(); break 'accept; }
                            permit = Arc::clone(&self.stream_admission).acquire_owned() => {
                                break permit.map_err(|_| iroh::protocol::AcceptError::from_err(std::io::Error::other("Iroh stream admission closed")))?;
                            }
                        }
                    };
                    let router = router.clone();
                    streams.spawn(async move { let result = serve_iroh_stream(router, send, recv).await; drop(permit); result });
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
