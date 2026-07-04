use crate::interlude::*;

use crate::DocumentId;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::listener::no_listener::NoListener;
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

type BigKeyhiveDelegation = keyhive_core::principal::group::delegation::Delegation<
    future_form::Sendable,
    MemorySigner,
    Vec<u8>,
    NoListener,
>;

#[derive(Clone)]
pub struct BigKeyhiveHandle {
    keyhive: Arc<SendableRuntimeKeyhive>,
    signer: MemorySigner,
    contact_card: Arc<keyhive_core::contact_card::ContactCard>,
    keyhive_peer_id: subduction_keyhive::KeyhivePeerId,
}
// a background task. rename to new
impl BigKeyhiveHandle {
    pub(crate) async fn new(seed: [u8; 32]) -> Res<Self> {
        let signer = MemorySigner::from(ed25519_dalek::SigningKey::from_bytes(&seed));
        let (keyhive, keyhive_peer_id, contact_card) =
            subduction_keyhive::runtime::init_sendable_keyhive(signer.clone())
                .await
                .map_err(|err| ferr!("error on keyhive init: {err:?}"))?;
        Ok(Self {
            keyhive: Arc::new(keyhive),
            signer,
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
        })
    }

    pub(crate) async fn restore_from_storage_archive(
        seed: [u8; 32],
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<Option<Self>> {
        use keyhive_crypto::verifiable::Verifiable;
        let signer = MemorySigner::from(ed25519_dalek::SigningKey::from_bytes(&seed));
        let storage_id = subduction_keyhive::StorageHash::new(*signer.verifying_key().as_bytes());
        let archives =
            subduction_keyhive::load_archives::<Vec<u8>, _, future_form::Sendable>(storage)
                .await
                .map_err(|err| ferr!("error loading keyhive archives: {err:?}"))?;
        let Some((_, archive)) = archives
            .into_iter()
            .find(|(archive_storage_id, _)| *archive_storage_id == storage_id)
        else {
            return Ok(None);
        };
        let restored = keyhive_core::keyhive::Keyhive::try_from_archive(
            &archive,
            signer.clone(),
            keyhive_core::store::ciphertext::memory::MemoryCiphertextStore::<Vec<u8>, Vec<u8>>::new(
            ),
            keyhive_core::listener::no_listener::NoListener,
            Arc::new(futures::lock::Mutex::new(rand_08::rngs::OsRng)),
        )
        .await
        .map_err(|err| ferr!("error restoring keyhive from archive: {err:?}"))?;
        let contact_card = restored.get_existing_contact_card().await;
        let keyhive_peer_id =
            subduction_keyhive::KeyhivePeerId::from_bytes(restored.id().to_bytes());
        Ok(Some(Self {
            keyhive: Arc::new(restored),
            signer,
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
        }))
    }

    pub(crate) async fn ingest_from_storage(
        &self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        subduction_keyhive::ingest_from_storage(self.keyhive.as_ref(), storage)
            .await
            .map_err(|err| ferr!("error ingesting keyhive storage: {err}"))?;
        Ok(())
    }

    pub(crate) async fn import_prekey_secrets(
        &self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        let Some(bytes) = storage
            .load_prekey_secrets()
            .await
            .map_err(|err| ferr!("error loading keyhive prekey secrets: {err}"))?
        else {
            return Ok(());
        };
        self.keyhive
            .import_prekey_secrets(&bytes)
            .await
            .map_err(|err| ferr!("failed importing keyhive prekey secrets: {err}"))?;
        Ok(())
    }

    pub(crate) async fn save_prekey_secrets(
        &self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        let bytes = self
            .keyhive
            .export_prekey_secrets()
            .await
            .map_err(|err| ferr!("error exporting keyhive prekey secrets: {err}"))?;
        storage
            .save_prekey_secrets(bytes)
            .await
            .map_err(|err| ferr!("error saving keyhive prekey secrets: {err}"))?;
        Ok(())
    }

    pub(crate) fn clone_keyhive(&self) -> Arc<SendableRuntimeKeyhive> {
        Arc::clone(&self.keyhive)
    }

    pub(crate) fn contact_card(&self) -> &keyhive_core::contact_card::ContactCard {
        &self.contact_card
    }

    pub(crate) fn keyhive_peer_id(&self) -> subduction_keyhive::KeyhivePeerId {
        self.keyhive_peer_id.clone()
    }

    pub(crate) async fn create_doc(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        initial_content_heads: NonEmpty<[u8; 32]>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<DocumentId> {
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?;
        let initial_content_heads = NonEmpty {
            head: initial_content_heads.head.to_vec(),
            tail: initial_content_heads
                .tail
                .into_iter()
                .map(Vec::from)
                .collect(),
        };
        let keyhive = self.keyhive.as_ref();
        let doc = keyhive
            .generate_doc(coparents, initial_content_heads)
            .await
            .map_err(|err| ferr!("failed creating keyhive document: {err}"))?;
        let (doc_id, cgka_ops, delegations) = {
            let locked = doc.lock().await;
            (
                locked.doc_id().to_bytes(),
                locked
                    .cgka_ops()
                    .map_err(|err| ferr!("failed reading initial doc cgka ops: {err}"))?
                    .iter()
                    .flat_map(|epoch| epoch.iter().map(|op| op.as_ref().clone()))
                    .collect::<Vec<_>>(),
                locked
                    .members()
                    .values()
                    .flat_map(|delegations| delegations.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        };
        persist_cgka_update_ops(storage, cgka_ops).await?;
        for delegation in delegations {
            persist_delegation(storage, delegation).await?;
        }
        Ok(DocumentId::new(doc_id))
    }

    pub(crate) async fn create_group_with_parents(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<BigKeyhiveGroup> {
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?;
        let keyhive = self.keyhive.as_ref();
        let group = keyhive
            .generate_group(coparents)
            .await
            .map_err(|err| ferr!("error creating keyhive group: {err}"))?;
        let (id, delegations) = {
            let locked = group.lock().await;
            (
                locked.group_id(),
                locked
                    .members()
                    .values()
                    .flat_map(|delegations| delegations.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        };
        for delegation in delegations {
            persist_delegation(storage, delegation).await?;
        }
        Ok(BigKeyhiveGroup { id, inner: group })
    }

    pub(crate) async fn add_member_to_group(
        &self,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
        access: keyhive_core::access::Access,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;

        let member = member.into().into_agent();
        let group_id = group.id();
        let kh = self.keyhive.as_ref();
        let update = kh
            .add_member(
                member,
                &Membered::Group(group_id, group.shared()),
                access,
                &[],
            )
            .await
            .map_err(|err| ferr!("group member add failed: {err}"))?;
        persist_cgka_update_ops(storage, update.cgka_ops).await?;
        persist_delegation(storage, update.delegation).await?;
        Ok(())
    }

    /// Grant an agent access to a document.
    pub(crate) async fn grant_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        access: keyhive_core::access::Access,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;
        let agent = principal.into().into_agent();
        let doc_id_bytes = doc_id.into_bytes();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
            .map_err(|_| ferr!("doc_id is not a valid Ed25519 point"))?;
        let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
            keyhive_core::principal::identifier::Identifier::from(vk),
        );
        let kh = self.keyhive.as_ref();
        let doc = kh.get_document(kh_doc_id).await.ok_or_else(|| {
            ferr!("document not found in keyhive: {doc_id} (bytes={doc_id_bytes:?})",)
        })?;
        let update = kh
            .add_member(agent, &Membered::Document(kh_doc_id, doc), access, &[])
            .await
            .map_err(|err| ferr!("grant failed: {err}"))?;
        persist_cgka_update_ops(storage, update.cgka_ops).await?;
        persist_delegation(storage, update.delegation).await?;
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
        let kh = self.keyhive.as_ref();
        Ok(kh.get_agent(identifier).await)
    }
}

async fn persist_delegation(
    storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    delegation: Arc<keyhive_crypto::signed::Signed<BigKeyhiveDelegation>>,
) -> Res<()> {
    let event: StaticEvent<Vec<u8>> = keyhive_core::event::Event::<
        future_form::Sendable,
        MemorySigner,
        Vec<u8>,
        NoListener,
    >::Delegated(delegation)
    .into();
    subduction_keyhive::save_event::<Vec<u8>, _, future_form::Sendable>(storage, &event)
        .await
        .map_err(|err| ferr!("failed saving keyhive delegation event: {err}"))?;
    Ok(())
}

async fn persist_cgka_update_ops(
    storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    cgka_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
) -> Res<()> {
    for cgka_op in cgka_ops {
        let event = StaticEvent::CgkaOperation(Box::new(cgka_op));
        subduction_keyhive::save_event::<Vec<u8>, _, future_form::Sendable>(storage, &event)
            .await
            .map_err(|err| ferr!("failed saving cgka update op: {err}"))?;
    }
    Ok(())
}
