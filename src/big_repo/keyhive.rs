use crate::{interlude::*, DocumentId};

use async_lock::Mutex;
use keyhive_crypto::signer::memory::MemorySigner;
use nonempty::NonEmpty;
use subduction_keyhive::runtime::SendableRuntimeKeyhive;

#[derive(Clone)]
pub struct BigKeyhiveHandle {
    keyhive: Arc<Mutex<SendableRuntimeKeyhive>>,
    contact_card: Arc<keyhive_core::contact_card::ContactCard>,
    keyhive_peer_id: subduction_keyhive::KeyhivePeerId,
}

impl BigKeyhiveHandle {
    pub async fn boot_memory() -> Res<Self> {
        Self::boot_memory_from_seed(rand::random()).await
    }

    pub async fn boot_memory_from_seed(seed: [u8; 32]) -> Res<Self> {
        let signer = MemorySigner::from(ed25519_dalek::SigningKey::from_bytes(&seed));
        let (keyhive, keyhive_peer_id, contact_card) =
            subduction_keyhive::runtime::init_sendable_keyhive(signer)
                .await
                .map_err(|err| ferr!("failed booting memory keyhive: {err}"))?;
        Ok(Self {
            keyhive: Arc::new(Mutex::new(keyhive)),
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
        })
    }

    pub(crate) async fn clone_keyhive(&self) -> SendableRuntimeKeyhive {
        self.keyhive.lock().await.clone()
    }

    pub(crate) fn contact_card(&self) -> &keyhive_core::contact_card::ContactCard {
        &self.contact_card
    }

    pub(crate) fn keyhive_peer_id(&self) -> subduction_keyhive::KeyhivePeerId {
        self.keyhive_peer_id.clone()
    }

    pub async fn create_doc(&self, initial_content_heads: NonEmpty<[u8; 32]>) -> Res<DocumentId> {
        let doc_id = self.create_doc_id_bytes(initial_content_heads).await?;
        Ok(DocumentId::new(doc_id))
    }

    /// Grant an agent access to a document.
    pub async fn grant_doc_access(
        &self,
        agent: keyhive_core::principal::agent::Agent<
            future_form::Sendable,
            keyhive_crypto::signer::memory::MemorySigner,
            Vec<u8>,
            keyhive_core::listener::no_listener::NoListener,
        >,
        doc_id: DocumentId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;
        let doc_id_bytes = doc_id.into_bytes();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
            .map_err(|_| ferr!("doc_id is not a valid Ed25519 point"))?;
        let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
            keyhive_core::principal::identifier::Identifier::from(vk),
        );
        let kh = self.keyhive.lock().await;
        let doc = kh.get_document(kh_doc_id).await.ok_or_else(|| {
            ferr!(
                "document not found in keyhive: {doc_id} (bytes={:?})",
                doc_id_bytes
            )
        })?;
        kh.add_member(agent, &Membered::Document(kh_doc_id, doc), access, &[])
            .await
            .map_err(|e| ferr!("grant failed: {e}"))?;
        Ok(())
    }

    /// Get an agent by peer ID (after contact card exchange).
    pub async fn get_agent_by_peer_id(
        &self,
        peer_id: &subduction_keyhive::KeyhivePeerId,
    ) -> Res<
        Option<
            keyhive_core::principal::agent::Agent<
                future_form::Sendable,
                keyhive_crypto::signer::memory::MemorySigner,
                Vec<u8>,
                keyhive_core::listener::no_listener::NoListener,
            >,
        >,
    > {
        let key_bytes = peer_id.verifying_key();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(key_bytes)
            .map_err(|_| ferr!("peer id is not a valid Ed25519 point"))?;
        let identifier = keyhive_core::principal::identifier::Identifier::from(vk);
        let kh = self.keyhive.lock().await;
        Ok(kh.get_agent(identifier).await)
    }

    async fn create_doc_id_bytes(
        &self,
        initial_content_heads: NonEmpty<[u8; 32]>,
    ) -> Res<[u8; 32]> {
        let initial_content_heads = NonEmpty {
            head: initial_content_heads.head.to_vec(),
            tail: initial_content_heads
                .tail
                .into_iter()
                .map(Vec::from)
                .collect(),
        };
        let keyhive = self.keyhive.lock().await;
        let doc = keyhive
            .generate_doc(vec![], initial_content_heads)
            .await
            .map_err(|err| ferr!("failed creating keyhive document: {err}"))?;
        let doc_id = doc.lock().await.doc_id().to_bytes();
        Ok(doc_id)
    }

    #[cfg(test)]
    async fn create_doc_for_test(
        &self,
        initial_content_heads: NonEmpty<[u8; 32]>,
    ) -> Res<(DocumentId, [u8; 32])> {
        let doc_id = self.create_doc_id_bytes(initial_content_heads).await?;
        Ok((DocumentId::new(doc_id), doc_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonempty::nonempty;

    #[tokio::test]
    async fn create_doc_returns_keyhive_document_id_bytes() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory().await?;
        let (doc_id, keyhive_doc_id) = keyhive.create_doc_for_test(nonempty![[1; 32]]).await?;
        assert_eq!(doc_id.into_bytes(), keyhive_doc_id);
        Ok(())
    }

    #[tokio::test]
    async fn create_doc_mints_distinct_documents() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory().await?;
        let first = keyhive.create_doc(nonempty![[1; 32]]).await?;
        let second = keyhive.create_doc(nonempty![[2; 32]]).await?;
        assert_ne!(first, second);
        Ok(())
    }

    #[tokio::test]
    async fn boot_memory_from_seed_can_create_documents() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory_from_seed([7; 32]).await?;
        let doc_id = keyhive.create_doc(nonempty![[3; 32]]).await?;
        assert_ne!(doc_id.into_bytes(), [0; 32]);
        Ok(())
    }
}
