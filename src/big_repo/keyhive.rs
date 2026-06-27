use crate::{interlude::*, DocumentId};

use async_lock::Mutex;
use keyhive_crypto::signer::memory::MemorySigner;
use nonempty::NonEmpty;
use std::fmt::{Debug, Formatter};
use subduction_keyhive::runtime::SendableRuntimeKeyhive;

pub type BigKeyhiveAgent = keyhive_core::principal::agent::Agent<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    keyhive_core::listener::no_listener::NoListener,
>;

type BigKeyhiveGroupInner = keyhive_core::principal::group::Group<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    keyhive_core::listener::no_listener::NoListener,
>;

type BigKeyhiveGroupShared = Arc<futures::lock::Mutex<BigKeyhiveGroupInner>>;

pub struct BigKeyhiveGroup {
    id: keyhive_core::principal::group::id::GroupId,
    inner: BigKeyhiveGroupShared,
}

impl Clone for BigKeyhiveGroup {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Debug for BigKeyhiveGroup {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("BigKeyhiveGroup")
            .field(&self.id())
            .finish()
    }
}

impl BigKeyhiveGroup {
    pub fn id(&self) -> keyhive_core::principal::group::id::GroupId {
        self.id
    }

    pub(crate) fn shared(&self) -> BigKeyhiveGroupShared {
        Arc::clone(&self.inner)
    }

    pub(crate) fn as_agent(&self) -> BigKeyhiveAgent {
        BigKeyhiveAgent::Group(self.id(), self.shared())
    }

    pub(crate) fn as_peer(&self) -> BigKeyhivePeer {
        BigKeyhivePeer::Group(self.id(), self.shared())
    }
}

#[derive(Debug, Clone)]
pub enum BigKeyhiveAuthority {
    Agent(BigKeyhiveAgent),
    Group(BigKeyhiveGroup),
}

impl From<BigKeyhiveAgent> for BigKeyhiveAuthority {
    fn from(agent: BigKeyhiveAgent) -> Self {
        Self::Agent(agent)
    }
}

impl From<BigKeyhiveGroup> for BigKeyhiveAuthority {
    fn from(group: BigKeyhiveGroup) -> Self {
        Self::Group(group)
    }
}

impl BigKeyhiveAuthority {
    fn into_agent(self) -> BigKeyhiveAgent {
        match self {
            Self::Agent(agent) => agent,
            Self::Group(group) => group.as_agent(),
        }
    }

    fn into_peer(self) -> Res<BigKeyhivePeer> {
        Ok(match self {
            Self::Agent(agent) => BigKeyhivePeer::try_from(agent)
                .map_err(|err| ferr!("invalid keyhive peer authority: {err}"))?,
            Self::Group(group) => group.as_peer(),
        })
    }
}

type BigKeyhivePeer = keyhive_core::principal::peer::Peer<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    keyhive_core::listener::no_listener::NoListener,
>;

#[derive(Clone)]
pub struct BigKeyhiveHandle {
    keyhive: Arc<Mutex<SendableRuntimeKeyhive>>,
    signer: MemorySigner,
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
            subduction_keyhive::runtime::init_sendable_keyhive(signer.clone())
                .await
                .map_err(|err| ferr!("failed booting memory keyhive: {err}"))?;
        Ok(Self {
            keyhive: Arc::new(Mutex::new(keyhive)),
            signer,
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
        })
    }

    pub(crate) async fn restore_from_storage_archive(
        &mut self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        let storage_id =
            subduction_keyhive::StorageHash::new(*self.keyhive_peer_id.verifying_key());
        let archives =
            subduction_keyhive::load_archives::<Vec<u8>, _, future_form::Sendable>(storage)
                .await
                .map_err(|err| ferr!("failed loading keyhive archives: {err}"))?;
        let Some((_, archive)) = archives
            .into_iter()
            .find(|(archive_storage_id, _)| *archive_storage_id == storage_id)
        else {
            return Ok(());
        };

        let restored = keyhive_core::keyhive::Keyhive::try_from_archive(
            &archive,
            self.signer.clone(),
            keyhive_core::store::ciphertext::memory::MemoryCiphertextStore::<Vec<u8>, Vec<u8>>::new(
            ),
            keyhive_core::listener::no_listener::NoListener,
            Arc::new(futures::lock::Mutex::new(rand_08::rngs::OsRng)),
        )
        .await
        .map_err(|err| ferr!("failed restoring keyhive from archive: {err:?}"))?;
        let contact_card = restored
            .contact_card()
            .await
            .map_err(|err| ferr!("failed restoring keyhive contact card: {err}"))?;
        let peer_id = subduction_keyhive::KeyhivePeerId::from_bytes(restored.id().to_bytes());
        *self.keyhive.lock().await = restored;
        self.contact_card = Arc::new(contact_card);
        self.keyhive_peer_id = peer_id;
        Ok(())
    }

    pub(crate) async fn clone_keyhive(&self) -> SendableRuntimeKeyhive {
        self.keyhive.lock().await.clone()
    }

    pub(crate) fn shared_keyhive(&self) -> Arc<Mutex<SendableRuntimeKeyhive>> {
        Arc::clone(&self.keyhive)
    }

    pub(crate) fn contact_card(&self) -> &keyhive_core::contact_card::ContactCard {
        &self.contact_card
    }

    pub(crate) fn keyhive_peer_id(&self) -> subduction_keyhive::KeyhivePeerId {
        self.keyhive_peer_id.clone()
    }

    pub async fn create_doc(&self, initial_content_heads: NonEmpty<[u8; 32]>) -> Res<DocumentId> {
        self.create_doc_with_parents(Vec::new(), initial_content_heads)
            .await
    }

    pub async fn create_doc_with_parents(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        initial_content_heads: NonEmpty<[u8; 32]>,
    ) -> Res<DocumentId> {
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?;
        let doc_id = self
            .create_doc_id_bytes(coparents, initial_content_heads)
            .await?;
        Ok(DocumentId::new(doc_id))
    }

    pub async fn create_group_with_parents(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Res<BigKeyhiveGroup> {
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?;
        let keyhive = self.keyhive.lock().await;
        let group = keyhive
            .generate_group(coparents)
            .await
            .map_err(|err| ferr!("failed creating keyhive group: {err}"))?;
        let id = group.lock().await.group_id();
        Ok(BigKeyhiveGroup { id, inner: group })
    }

    pub async fn add_member_to_group(
        &self,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;

        let member = member.into().into_agent();
        let group_id = group.id();
        let kh = self.keyhive.lock().await;
        kh.add_member(
            member,
            &Membered::Group(group_id, group.shared()),
            access,
            &[],
        )
        .await
        .map_err(|err| ferr!("group member add failed: {err}"))?;
        Ok(())
    }

    /// Grant an agent access to a document.
    pub async fn grant_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;
        let agent = principal.into().into_agent();
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
            .map_err(|err| ferr!("grant failed: {err}"))?;
        // E2EE branch: add_member fires CGKA ops to the event listener
        // automatically, so no manual receive_cgka_op loop is needed.
        Ok(())
    }

    /// Get an agent by peer ID (after contact card exchange).
    pub async fn get_agent_by_peer_id(
        &self,
        peer_id: &subduction_keyhive::KeyhivePeerId,
    ) -> Res<Option<BigKeyhiveAgent>> {
        let key_bytes = peer_id.verifying_key();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(key_bytes)
            .map_err(|_| ferr!("peer id is not a valid Ed25519 point"))?;
        let identifier = keyhive_core::principal::identifier::Identifier::from(vk);
        let kh = self.keyhive.lock().await;
        Ok(kh.get_agent(identifier).await)
    }

    async fn create_doc_id_bytes(
        &self,
        coparents: Vec<BigKeyhivePeer>,
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
            .generate_doc(coparents, initial_content_heads)
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
        let doc_id = self
            .create_doc_id_bytes(Vec::new(), initial_content_heads)
            .await?;
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
