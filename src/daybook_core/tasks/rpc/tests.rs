use super::*;
use crate::interlude::*;

#[tokio::test]
async fn removed_native_identity_closes_surviving_task_transport() -> Res<()> {
    let (repo, _sync, repo_stop) = crate::test_support::boot_repo().await?;
    let (identities, rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
    async fn endpoint() -> Res<iroh::Endpoint> {
        Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?)
    }
    let server = endpoint().await?;
    let client_endpoint = endpoint().await?;
    let peer = PeerKey::new([91; 32]);
    identities.register_peer(client_endpoint.id(), peer.clone());
    let (requests, mut incoming) = mpsc::channel(4);
    let router = iroh::protocol::Router::builder(server.clone())
        .accept(
            TASK_COORDINATION_ALPN,
            TaskCoordinationProtocolHandler::new(identities.clone(), requests),
        )
        .spawn();
    let client = irpc_iroh::client::<TaskCoordinationRpc>(
        client_endpoint.clone(),
        server.addr(),
        TASK_COORDINATION_ALPN,
    );
    let session = SessionKey {
        node: super::super::NodePubkey::new([91; 32]),
        incarnation: super::super::NodeIncarnationId::new(1),
        session: super::super::RouterSessionId::new(1),
    };
    let request = || ExecutorReportRequest {
        pool: super::super::test_util::pool(),
        session,
        report: ExecutorReport::Closed,
    };
    let first = client.rpc(request());
    let receive = async {
        let authenticated = incoming.recv().await.unwrap();
        assert_eq!(authenticated.peer, peer);
        let TaskCoordinationRpcMessage::Report(message) = authenticated.request else {
            panic!("unexpected registration")
        };
        message.tx.send(Ok(())).await?;
        eyre::Ok(())
    };
    let (reply, received) = tokio::join!(first, receive);
    reply?.map_err(|error| eyre::eyre!(error))?;
    received?;
    identities.unregister_peer(peer);
    let rejected = client.rpc(request()).await;
    assert!(
        rejected.is_err(),
        "removed application identity must not retain scheduling access"
    );
    assert!(matches!(
        incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    client_endpoint.close().await;
    router.shutdown().await?;
    rpc_stop.stop().await?;
    repo_stop().await?;
    Ok(())
}
