use crate::BlobService;
use crate::encrypt::TAG_CT_PREFIX;
use crate::encrypt::TAG_PT_PREFIX;
use crate::interlude::*;
use big_blobs_core::BlobId;
use big_repo::SharedPartStore;
use big_sync::SyncBackend;
use big_sync::SyncTaskRunOutcome;
use big_sync_core::ObjKey;
use big_sync_core::PartKey;
use big_sync_core::PeerKey;
use big_sync_core::SyncCompletionDeets;
use big_sync_core::SyncTaskCompletion;
use big_sync_core::part_store::ObjPayload;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_blobs::HashAndFormat;
use iroh_blobs::api::TempTag;
use iroh_blobs::store::virtual_blob::Provider as _;

#[derive(Clone)]
pub struct BlobSyncBackend {
    service: Arc<BlobService>,
    part_store: SharedPartStore,
    endpoint: iroh::Endpoint,
    address_lookup: iroh::address_lookup::MemoryLookup,
    peer_addrs: Arc<surelock::mutex::Mutex<HashMap<PeerKey, iroh::EndpointAddr>>>,
}

impl BlobSyncBackend {
    pub fn new(
        service: Arc<BlobService>,
        part_store: SharedPartStore,
        endpoint: iroh::Endpoint,
        address_lookup: iroh::address_lookup::MemoryLookup,
    ) -> Self {
        Self {
            service,
            part_store,
            endpoint,
            address_lookup,
            peer_addrs: Arc::new(surelock::mutex::Mutex::new(HashMap::new())),
        }
    }

    pub fn register_peer_addr(&self, peer_id: PeerKey, addr: iroh::EndpointAddr) {
        self.address_lookup.add_endpoint_info(addr.clone());
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.insert(peer_id, addr);
        });
    }

    pub fn unregister_peer_addr(&self, peer_id: PeerKey) {
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.remove(&peer_id);
        });
    }

    pub fn active_peer_ids(&self) -> Vec<PeerKey> {
        surelock::key::lock_scope(|key| {
            let (map, _key) = key.lock(&self.peer_addrs);
            map.keys().cloned().collect()
        })
    }

    pub async fn ensure_local_blob(&self, peer_id: PeerKey, blob_id: BlobId) -> Res<()> {
        let guards = self.retain_and_announce(peer_id, blob_id).await?;
        drop(guards);
        Ok(())
    }

    async fn retain_and_announce(
        &self,
        peer_id: PeerKey,
        blob_id: BlobId,
    ) -> Res<(TempTag, Option<TempTag>)> {
        let store = self.service.store();
        let hash = Hash::from(blob_id.to_bytes32());
        let guard = store.tags().temp_tag(HashAndFormat::raw(hash)).await?;
        let plaintext_guard = if self.service.stat(&blob_id).await?.is_some() {
            if store.blobs().sync_reader(hash).await?.is_some() {
                None
            } else {
                let ciphertext = store.tags().get(format!("{TAG_CT_PREFIX}{hash}")).await?;
                let plaintext = store.tags().get(format!("{TAG_PT_PREFIX}{hash}")).await?;
                eyre::ensure!(
                    ciphertext.is_some_and(|tag| tag.hash == hash && tag.format == BlobFormat::Raw)
                        && self.service.provider.reader_for(&hash).is_some(),
                    "virtual representation {blob_id} has no rooted cipher provider"
                );
                let plaintext = plaintext.ok_or_else(|| {
                    eyre::eyre!("virtual representation {blob_id} has no plaintext root")
                })?;
                eyre::ensure!(
                    plaintext.format == BlobFormat::Raw,
                    "virtual representation {blob_id} has a non-raw plaintext root"
                );
                let plaintext_guard = store
                    .tags()
                    .temp_tag(HashAndFormat::raw(plaintext.hash))
                    .await?;
                eyre::ensure!(
                    store.blobs().sync_reader(plaintext.hash).await?.is_some(),
                    "virtual representation {blob_id} has no complete stored plaintext"
                );
                Some(plaintext_guard)
            }
        } else {
            let provider_addr = surelock::key::lock_scope(|key| {
                let (map, _key) = key.lock(&self.peer_addrs);
                map.get(&peer_id).cloned()
            })
            .ok_or_else(|| {
                eyre::eyre!("peer {peer_id} is no longer registered for blob downloads")
            })?;
            self.address_lookup.add_endpoint_info(provider_addr.clone());
            store
                .downloader(&self.endpoint)
                .download(hash, vec![provider_addr.id])
                .await
                .map_err(|error| {
                    eyre::eyre!(
                        "failed downloading representation {blob_id} from peer {peer_id}: {error:?}"
                    )
                })?;
            eyre::ensure!(
                self.service.stat(&blob_id).await?.is_some()
                    && store.blobs().sync_reader(hash).await?.is_some(),
                "download of representation {blob_id} did not produce complete stored bytes"
            );
            None
        };
        if plaintext_guard.is_none() {
            store
                .tags()
                .set(format!("held:{hash}"), HashAndFormat::raw(hash))
                .await?;
        }
        self.service.announce(blob_id).await?;
        Ok((guard, plaintext_guard))
    }
}

#[async_trait]
impl SyncBackend for BlobSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<PartKey>,
        remote_payload: Option<ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        let blob_id = BlobId::try_from(&obj_id)
            .wrap_err_with(|| format!("blob object key is not a blob digest: {obj_id}"))?;
        let guards = self.retain_and_announce(peer_id, blob_id).await?;
        let local_payload = self.part_store.obj_payload(obj_id.clone()).await?;
        let deets = match (&local_payload, &remote_payload) {
            (_, None) => SyncCompletionDeets::Noop,
            (None, Some(_)) => SyncCompletionDeets::AddedMember,
            (Some(local), Some(remote)) if local != remote => SyncCompletionDeets::ChangedObject,
            _ => SyncCompletionDeets::Noop,
        };
        match remote_payload {
            Some(payload) if local_payload.as_ref() != Some(&payload) => {
                self.part_store
                    .set_obj_payload(obj_id.clone(), payload)
                    .await?;
            }
            None if local_payload.is_none() => {
                self.part_store
                    .set_obj_payload(obj_id.clone(), serde_json::json!({}))
                    .await?;
            }
            _ => {}
        }
        if !parts.is_empty() {
            self.part_store
                .add_obj_to_parts(obj_id.clone(), parts)
                .await?;
        }
        drop(guards);
        Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
            obj_id,
            deets,
        }))
    }

    async fn remove_obj_from_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        BlobId::try_from(&obj_id)
            .wrap_err_with(|| format!("blob object key is not a blob digest: {obj_id}"))?;
        for part in parts {
            self.part_store
                .remove_obj_from_part(obj_id.clone(), part)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
