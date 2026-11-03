use crate::InventoryMirrors;
use crate::interlude::*;
use big_blobs_core::BlobId;
use big_sync::SyncBackend;
use big_sync::SyncTaskRunOutcome;
use big_sync_core::ObjKey;
use big_sync_core::PeerKey;
use big_sync_core::SyncCompletionDeets;
use big_sync_core::SyncTaskCompletion;
use big_sync_core::part_store::ObjPayload;
use std::collections::BTreeMap;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct DownloadPeers {
    pub endpoint: iroh::Endpoint,
    pub lookup: iroh::address_lookup::MemoryLookup,
    pub addresses: BTreeMap<PeerKey, iroh::EndpointAddr>,
}

impl DownloadPeers {
    pub(crate) fn fetch(
        self,
        store: iroh_blobs::api::Store,
        attempt: crate::admission::Attempt,
        length: u64,
    ) -> futures::future::BoxFuture<
        'static,
        (
            crate::admission::Attempt,
            Res<Vec<iroh_blobs::api::TempTag>>,
        ),
    > {
        Box::pin(async move {
            let result = async {
                let address = self
                    .addresses
                    .get(&attempt.peer)
                    .expect("download peer was selected from registered addresses");
                let hash = iroh_blobs::Hash::from(attempt.digest.to_bytes32());
                let guard = store
                    .tags()
                    .temp_tag(iroh_blobs::HashAndFormat::raw(hash))
                    .await?;
                store
                    .downloader(&self.endpoint)
                    .download(hash, vec![address.id])
                    .await
                    .map_err(|error| eyre::eyre!("inventory byte transfer failed: {error:?}"))?;
                let reader =
                    store.blobs().sync_reader(hash).await?.ok_or_else(|| {
                        eyre::eyre!("download did not produce complete stored bytes")
                    })?;
                eyre::ensure!(
                    reader.len() == length,
                    "download length disagrees with inventory pin"
                );
                Ok(vec![guard])
            }
            .await;
            (attempt, result)
        })
    }
}

#[derive(Clone)]
pub struct BlobSyncBackend {
    mirrors: InventoryMirrors,
    peers: watch::Sender<Option<DownloadPeers>>,
}

impl BlobSyncBackend {
    pub fn new(
        mirrors: InventoryMirrors,
        endpoint: iroh::Endpoint,
        address_lookup: iroh::address_lookup::MemoryLookup,
    ) -> Self {
        let peers = mirrors.downloads.clone();
        peers.send_replace(Some(DownloadPeers {
            endpoint,
            lookup: address_lookup,
            addresses: BTreeMap::new(),
        }));
        Self { mirrors, peers }
    }

    pub fn register_peer_addr(&self, peer_id: PeerKey, addr: iroh::EndpointAddr) {
        self.peers.send_modify(|peers| {
            let peers = peers.as_mut().expect("blob transport is configured");
            peers.lookup.add_endpoint_info(addr.clone());
            peers.addresses.insert(peer_id, addr);
        });
    }

    pub fn unregister_peer_addr(&self, peer_id: PeerKey) {
        self.peers.send_modify(|peers| {
            peers
                .as_mut()
                .expect("blob transport is configured")
                .addresses
                .remove(&peer_id);
        });
    }

    pub fn active_peer_ids(&self) -> Vec<PeerKey> {
        self.peers
            .borrow()
            .as_ref()
            .expect("blob transport is configured")
            .addresses
            .keys()
            .cloned()
            .collect()
    }

    async fn observe(
        &self,
        peer: PeerKey,
        object: &ObjKey,
        parts: Vec<big_sync_core::PartKey>,
        payload: Option<&ObjPayload>,
        present: bool,
    ) -> Res<()> {
        let digest = BlobId::try_from(object)
            .wrap_err_with(|| format!("blob object key is not a blob digest: {object}"))?;
        for part in parts {
            let inventory = self
                .mirrors
                .admission
                .inventory(&part)
                .await?
                .ok_or_else(|| {
                    eyre::eyre!("blob observation names an unregistered inventory part")
                })?;
            self.mirrors
                .admission
                .observe(&inventory, digest, &peer, 0, present, payload)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SyncBackend for BlobSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<big_sync_core::PartKey>,
        remote_payload: Option<ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        self.observe(peer_id, &obj_id, parts, remote_payload.as_ref(), true)
            .await?;
        Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
            obj_id,
            deets: SyncCompletionDeets::Noop,
        }))
    }

    async fn remove_obj_from_parts(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<big_sync_core::PartKey>,
    ) -> Res<()> {
        self.observe(peer_id, &obj_id, parts, None, false).await
    }
}

#[cfg(test)]
mod access_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transfer_tests;
#[cfg(test)]
mod acceptance_tests;
