use crate::interlude::*;

use daybook_core::repos::Repo;
use daybook_core::sync::IrohSyncEvent;

pub async fn run(sync_urls: Vec<String>, exit_when_synced: bool) -> Res<ExitCode> {
    let sync_repo = lazy::sync_repo().await?;
    let local_ticket_url = sync_repo.get_clone_ticket_url().await?;
    {
        use qrcode::QrCode;
        use qrcode::render::unicode;
        let code = QrCode::new(&local_ticket_url[..]).unwrap();
        let image = code
            .render::<unicode::Dense1x2>()
            .dark_color(unicode::Dense1x2::Light)
            .light_color(unicode::Dense1x2::Dark)
            .build();
        println!("Scan the following QR code to clone this repo");
        println!();
        println!("{image}");
        println!();
        println!("Or copy the following ticket:");
        println!();
        println!();
        println!("{local_ticket_url}");
        println!();
        println!();
    }

    for sync_url in &sync_urls {
        sync_repo.connect_url(sync_url).await?;
    }
    let config_repo = lazy::config_repo().await?;
    let devices = config_repo.list_known_sync_devices().await?;
    let peer_ids: Vec<_> = devices
        .into_iter()
        .map(|dev| big_sync_core::PeerId::new(*dev.endpoint_id.as_bytes()))
        .collect();

    if exit_when_synced {
        if peer_ids.is_empty() {
            error!("--exit-when-synced requires at least one sync URL");
            return Ok(ExitCode::FAILURE);
        }
        sync_repo
            // TODO: parametrize timeout
            .wait_until_peers_sync(&peer_ids, std::time::Duration::from_secs(120))
            .await?;
    } else {
        let listener = sync_repo.subscribe(daybook_core::repos::SubscribeOpts::new(512));
        sync_repo.connect_known_devices_once().await?;
        loop {
            match listener.recv_lossy_async().await {
                Ok(event) => match &*event {
                    IrohSyncEvent::IncomingConnection { peer_key } => {
                        info!(%peer_key, "incoming connection");
                    }
                    IrohSyncEvent::OutgoingConnection { peer_key } => {
                        info!(%peer_key, "outgoing connection");
                    }
                    IrohSyncEvent::ConnectionClosed { peer_key, reason } => {
                        info!(%peer_key, ?reason, "connection closed");
                    }
                    IrohSyncEvent::PeerFullySynced {
                        peer_key,
                        doc_count,
                    } => {
                        info!(%peer_key, ?doc_count, "peer fully synced");
                    }
                    IrohSyncEvent::PartitionFullySynced {
                        peer_key,
                        partition,
                    } => {
                        info!(%peer_key, ?partition, "partition fully synced");
                    }
                    IrohSyncEvent::DocSyncedWithPeer { peer_key, doc_id } => {
                        info!(%peer_key, ?doc_id, "doc synced with peer");
                    }
                    IrohSyncEvent::BlobSynced { hash } => {
                        info!(%hash, "blob synced");
                    }
                    IrohSyncEvent::BlobSyncBackoff {
                        hash,
                        delay,
                        attempt_no,
                    } => {
                        info!(%hash, ?delay, ?attempt_no, "blob sync backoff");
                    }
                    IrohSyncEvent::BlobDownloadStarted { hash } => {
                        info!(%hash, "blob download started");
                    }
                    IrohSyncEvent::BlobDownloadFinished { hash, success } => {
                        info!(%hash, ?success, "blob download finished");
                    }
                    IrohSyncEvent::StalePeer { peer_key } => {
                        warn!(%peer_key, "stale sync peer");
                    }
                },
                Err(err) => {
                    warn!(?err, "sync listener closed");
                    break;
                }
            };
        }
    }
    Ok(ExitCode::SUCCESS)
}
