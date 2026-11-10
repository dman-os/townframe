use super::*;
use crate::BlobService;
use crate::encrypt::TAG_CT_PREFIX;
use crate::encrypt::TAG_PT_PREFIX;
use big_repo::SharedBigRepo;
use big_repo::SharedPartStore;
use big_repo::keyhive_core::access::Access;
use big_repo::keyhive_core::principal::identifier::Identifier;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_blobs::HashAndFormat;
use iroh_blobs::api::TempTag;
use iroh_blobs::store::virtual_blob::Provider as _;
use utils_rs::prelude::eyre;

#[derive(Clone)]
pub(crate) struct Projection {
    pub service: Arc<BlobService>,
    pub parts: SharedPartStore,
    pub repo: SharedBigRepo,
}

impl Projection {
    pub async fn readable(&self, inventory: &DocId) -> Res<bool> {
        let local =
            ed25519_dalek::VerifyingKey::from_bytes(&self.repo.local_peer_id().to_bytes32()?)?;
        let document = ed25519_dalek::VerifyingKey::from_bytes(&inventory.to_bytes32()?)?;
        Ok(self
            .repo
            .keyhive()
            .agent_access_on(&Identifier::from(local), Identifier::from(document))
            .await
            .is_some_and(|access| access >= Access::Read))
    }

    pub async fn apply(&self, admission: &Admission, inventory: &DocId, digest: BlobId) -> Res<()> {
        let projection = self.clone();
        let inventory = inventory.clone();
        let target = inventory.clone();
        admission
            .publish(&inventory, digest, move |mut state, length| {
                Box::pin(async move {
                    let ownership = projection.service.ownership.clone();
                    ownership
                        .publish(move |publication| {
                            Box::pin(async move {
                                let inventory = target;
                                let guards = local_bytes(&projection.service, digest).await?;
                                let available = guards.is_some();
                                state.bytes = available;
                                let authorized = projection.readable(&inventory).await?;
                                let part = crate::workers::inventory_part(&inventory);
                                let owner = format!("inventory:{inventory}:{digest}");
                                if state.retains_bytes() {
                                    publication.claim(&owner, &[digest]).await?;
                                }
                                if state.advertisable() && authorized {
                                    let length =
                                        length.expect("an advertisable pair has a pin length");
                                    let info = projection
                                        .service
                                        .stat(&digest)
                                        .await?
                                        .expect("verified local bytes have a size");
                                    eyre::ensure!(
                                        info.length_octets == length,
                                        "inventory pin length disagrees with verified blob size"
                                    );
                                    projection.parts.ensure_part(part.clone()).await?;
                                    projection
                                        .parts
                                        .set_obj_payload(
                                            digest.into(),
                                            serde_json::json!({"lengthOctets": length}),
                                        )
                                        .await?;
                                    projection
                                        .parts
                                        .add_obj_to_parts(digest.into(), vec![part])
                                        .await?;
                                    projection.service.announce(digest).await?;
                                } else {
                                    projection
                                        .parts
                                        .remove_obj_from_part(digest.into(), part)
                                        .await?;
                                }
                                if !state.retains_bytes() {
                                    publication.release(&owner).await?;
                                }
                                drop(guards);
                                Ok(available)
                            })
                        })
                        .await
                })
            })
            .await
    }
}

pub(crate) async fn local_bytes(
    service: &BlobService,
    digest: BlobId,
) -> Res<Option<Vec<TempTag>>> {
    let store = service.store();
    let hash = Hash::from(digest.to_bytes32());
    let guard = store.tags().temp_tag(HashAndFormat::raw(hash)).await?;
    if service.stat(&digest).await?.is_none() {
        return Ok(None);
    }
    if store.blobs().sync_reader(hash).await?.is_some() {
        return Ok(Some(vec![guard]));
    }
    let Some(ciphertext) = store.tags().get(format!("{TAG_CT_PREFIX}{hash}")).await? else {
        return Ok(None);
    };
    if ciphertext.hash != hash
        || ciphertext.format != BlobFormat::Raw
        || service.provider.reader_for(&hash).is_none()
    {
        return Ok(None);
    }
    let Some(plaintext) = store.tags().get(format!("{TAG_PT_PREFIX}{hash}")).await? else {
        return Ok(None);
    };
    if plaintext.hash == hash || plaintext.format != BlobFormat::Raw {
        return Ok(None);
    }
    let plaintext_guard = store
        .tags()
        .temp_tag(HashAndFormat::raw(plaintext.hash))
        .await?;
    if service
        .stat(&BlobId::new(*plaintext.hash.as_bytes()))
        .await?
        .is_none()
        || store.blobs().sync_reader(plaintext.hash).await?.is_none()
    {
        return Ok(None);
    }
    Ok(Some(vec![guard, plaintext_guard]))
}
