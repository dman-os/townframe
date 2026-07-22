use crate::interlude::*;

use crate::{keyhive_listener::BigRepoKeyhiveListener, DocumentId};
use keyhive_core::access::Access;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId as KhDocumentId;
use keyhive_core::principal::group::id::GroupId as KhGroupId;
use keyhive_core::principal::identifier::Identifier;
use keyhive_crypto::signer::memory::MemorySigner;
use nonempty::NonEmpty;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub type BigKeyhiveAgent = keyhive_core::principal::agent::Agent<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    BigRepoKeyhiveListener,
>;

type BigKeyhiveGroupInner = keyhive_core::principal::group::Group<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    BigRepoKeyhiveListener,
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

    fn into_identifier(self) -> Identifier {
        self.into_agent().id()
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
    BigRepoKeyhiveListener,
>;

type BigKeyhiveDelegation = keyhive_core::principal::group::delegation::Delegation<
    future_form::Sendable,
    MemorySigner,
    Vec<u8>,
    BigRepoKeyhiveListener,
>;

type BigKeyhiveRevocation = keyhive_core::principal::group::revocation::Revocation<
    future_form::Sendable,
    MemorySigner,
    Vec<u8>,
    BigRepoKeyhiveListener,
>;

/// The concrete [`Keyhive`] type with our [`BigRepoKeyhiveListener`].
type BigKeyhiveKeyhive = keyhive_core::keyhive::Keyhive<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    BigRepoKeyhiveListener,
    rand_08::rngs::OsRng,
>;

#[derive(Clone)]
pub struct BigKeyhiveHandle {
    keyhive: Arc<BigKeyhiveKeyhive>,
    signer: MemorySigner,
    contact_card: Arc<keyhive_core::contact_card::ContactCard>,
    keyhive_peer_id: subduction_keyhive::KeyhivePeerId,
}
// a background task. rename to new
impl BigKeyhiveHandle {
    pub(crate) async fn new(seed: [u8; 32], listener: BigRepoKeyhiveListener) -> Res<Self> {
        let signer = MemorySigner::from(ed25519_dalek::SigningKey::from_bytes(&seed));
        let (keyhive, keyhive_peer_id, contact_card) =
            subduction_keyhive::runtime::init_sendable_keyhive(signer.clone(), listener)
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
        listener: BigRepoKeyhiveListener,
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
            listener,
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

    pub(crate) fn clone_keyhive(&self) -> Arc<BigKeyhiveKeyhive> {
        Arc::clone(&self.keyhive)
    }

    /// All docs reachable by `agent`, with the [`Access`] level for each.
    /// O(all_docs × transitive_members) — only for boot full reindex.
    pub async fn docs_for_agent(&self, agent: &Identifier) -> BTreeMap<DocumentId, Access> {
        let keyhive = self.keyhive.as_ref();
        let mut caps = BTreeMap::new();
        let doc_ids: Vec<KhDocumentId> = {
            let docs = keyhive.documents().lock().await;
            docs.keys().copied().collect()
        };
        for kh_doc_id in doc_ids {
            if let Some(doc) = keyhive.get_document(kh_doc_id).await {
                let locked = doc.lock().await;
                if let Some((_, access)) = locked.transitive_members().await.get(agent) {
                    caps.insert(DocumentId::new(kh_doc_id.to_bytes()), *access);
                }
            }
        }
        caps
    }

    /// All agents (individuals + groups) who can reach this doc/group, with [`Access`].
    /// O(|transitive_members(target)|) — used for incremental per-target update.
    pub async fn agents_for_membered(&self, id: Identifier) -> HashMap<[u8; 32], Access> {
        let keyhive = self.keyhive.as_ref();
        // Try document first, then group
        if let Some(doc) = keyhive.get_document(KhDocumentId::from(id)).await {
            let locked = doc.lock().await;
            return locked
                .transitive_members()
                .await
                .into_iter()
                .map(|(id, (_, access))| (id.to_bytes(), access))
                .collect();
        }
        if let Some(group) = keyhive.get_group(KhGroupId::from(id)).await {
            let locked = group.lock().await;
            return locked
                .transitive_members()
                .await
                .into_iter()
                .map(|(id, (_, access))| (id.to_bytes(), access))
                .collect();
        }
        HashMap::new()
    }

    /// What [`Access`] does `agent` have on this doc/group? None if unreachable.
    pub async fn agent_access_on(
        &self,
        agent: &Identifier,
        membered_id: Identifier,
    ) -> Option<Access> {
        let keyhive = self.keyhive.as_ref();
        if let Some(doc) = keyhive.get_document(KhDocumentId::from(membered_id)).await {
            let locked = doc.lock().await;
            return locked
                .transitive_members()
                .await
                .get(agent)
                .map(|(_, access)| *access);
        }
        if let Some(group) = keyhive.get_group(KhGroupId::from(membered_id)).await {
            let locked = group.lock().await;
            return locked
                .transitive_members()
                .await
                .get(agent)
                .map(|(_, access)| *access);
        }
        None
    }

    /// All groups AND docs `agent` can reach, with [`Access`].
    pub async fn membered_for_agent(&self, agent: &Identifier) -> HashMap<[u8; 32], Access> {
        let keyhive = self.keyhive.as_ref();
        let mut caps = HashMap::new();
        // Enumerate docs
        let doc_ids: Vec<KhDocumentId> = {
            let docs = keyhive.documents().lock().await;
            docs.keys().copied().collect()
        };
        for kh_doc_id in doc_ids {
            if let Some(doc) = keyhive.get_document(kh_doc_id).await {
                let locked = doc.lock().await;
                if let Some((_, access)) = locked.transitive_members().await.get(agent) {
                    caps.insert(kh_doc_id.to_bytes(), *access);
                }
            }
        }
        // Enumerate groups
        #[allow(unused_mut)]
        let mut group_ids: Vec<KhGroupId> = {
            let groups = keyhive.groups().lock().await;
            groups.keys().copied().collect()
        };
        for kh_group_id in group_ids {
            if let Some(group) = keyhive.get_group(kh_group_id).await {
                let locked = group.lock().await;
                if let Some((_, access)) = locked.transitive_members().await.get(agent) {
                    caps.insert(kh_group_id.to_bytes(), *access);
                }
            }
        }
        caps
    }

    pub(crate) async fn document_ids(&self) -> Vec<big_sync_core::ObjId> {
        self.keyhive
            .documents()
            .lock()
            .await
            .keys()
            .map(|id| big_sync_core::ObjId::new(id.to_bytes()))
            .collect()
    }

    pub(crate) async fn group_document_ids_by_id(&self) -> HashMap<[u8; 32], BTreeSet<DocumentId>> {
        let group_ids: Vec<KhGroupId> =
            self.keyhive.groups().lock().await.keys().copied().collect();
        let mut out = HashMap::new();
        for group_id in group_ids {
            let docs = self
                .keyhive
                .document_ids_containing_group(group_id)
                .await
                .into_iter()
                .map(|id| DocumentId::new(id.to_bytes()))
                .collect();
            out.insert(group_id.to_bytes(), docs);
        }
        out
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

    pub(crate) async fn group_document_ids(&self, group: &BigKeyhiveGroup) -> BTreeSet<DocumentId> {
        self.keyhive
            .document_ids_containing_group(group.id())
            .await
            .into_iter()
            .map(|doc_id| DocumentId::new(*doc_id.as_bytes()))
            .collect()
    }

    pub(crate) async fn add_member_to_group(
        &self,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
        access: keyhive_core::access::Access,
        after_content: BTreeMap<DocumentId, Vec<Vec<u8>>>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<BTreeSet<DocumentId>> {
        use keyhive_core::principal::membered::Membered;

        let member = member.into().into_agent();
        let group_id = group.id();
        let kh = self.keyhive.as_ref();
        let after_content = after_content
            .into_iter()
            .map(|(doc_id, refs)| Ok((keyhive_doc_id(doc_id)?, refs)))
            .collect::<Res<BTreeMap<_, _>>>()?;
        let update = kh
            .add_member_with_manual_content(
                member,
                &Membered::Group(group_id, group.shared()),
                access,
                after_content,
            )
            .await
            .map_err(|err| ferr!("group member add failed: {err}"))?;
        let affected_docs = update
            .cgka_ops
            .iter()
            .map(|op| DocumentId::new(*op.payload().doc_id().as_bytes()))
            .collect();
        persist_cgka_update_ops(storage, update.cgka_ops).await?;
        persist_delegation(storage, update.delegation).await?;
        Ok(affected_docs)
    }

    /// Grant an agent access to a document.
    pub(crate) async fn grant_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        access: keyhive_core::access::Access,
        after_content: Vec<Vec<u8>>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;
        let agent = principal.into().into_agent();
        let kh_doc_id = keyhive_doc_id(doc_id)?;
        let kh = self.keyhive.as_ref();
        let doc = kh
            .get_document(kh_doc_id)
            .await
            .ok_or_else(|| ferr!("document not found in keyhive: {doc_id}"))?;
        let update = kh
            .add_member_with_manual_content(
                agent,
                &Membered::Document(kh_doc_id, doc),
                access,
                BTreeMap::from([(kh_doc_id, after_content)]),
            )
            .await
            .map_err(|err| ferr!("grant failed: {err}"))?;
        persist_cgka_update_ops(storage, update.cgka_ops).await?;
        persist_delegation(storage, update.delegation).await?;
        Ok(())
    }

    /// Revoke an authority's access to a document with an explicit content frontier.
    pub(crate) async fn revoke_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        retain_all_other_members: bool,
        after_content: Vec<Vec<u8>>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        use keyhive_core::principal::membered::Membered;

        let kh_doc_id = keyhive_doc_id(doc_id)?;
        let kh = self.keyhive.as_ref();
        let doc = kh
            .get_document(kh_doc_id)
            .await
            .ok_or_else(|| ferr!("document not found in keyhive: {doc_id}"))?;
        let update = kh
            .revoke_member_with_manual_content(
                principal.into().into_identifier(),
                retain_all_other_members,
                &Membered::Document(kh_doc_id, doc),
                BTreeMap::from([(kh_doc_id, after_content)]),
            )
            .await
            .map_err(|err| ferr!("revoke failed: {err}"))?;
        persist_cgka_update_ops(storage, update.cgka_ops().to_vec()).await?;
        for revocation in update.revocations() {
            persist_revocation(storage, Arc::clone(revocation)).await?;
        }
        for redelegation in update.redelegations() {
            persist_delegation(storage, Arc::clone(redelegation)).await?;
        }
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

fn keyhive_doc_id(doc_id: DocumentId) -> Res<keyhive_core::principal::document::id::DocumentId> {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .map_err(|_| ferr!("doc_id is not a valid Ed25519 point"))?;
    Ok(keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    ))
}

async fn persist_delegation(
    storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    delegation: Arc<keyhive_crypto::signed::Signed<BigKeyhiveDelegation>>,
) -> Res<()> {
    let event: StaticEvent<Vec<u8>> = keyhive_core::event::Event::<
        future_form::Sendable,
        MemorySigner,
        Vec<u8>,
        BigRepoKeyhiveListener,
    >::Delegated(delegation)
    .into();
    subduction_keyhive::save_event::<Vec<u8>, _, future_form::Sendable>(storage, &event)
        .await
        .map_err(|err| ferr!("failed saving keyhive delegation event: {err}"))?;
    Ok(())
}

async fn persist_revocation(
    storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    revocation: Arc<keyhive_crypto::signed::Signed<BigKeyhiveRevocation>>,
) -> Res<()> {
    let event: StaticEvent<Vec<u8>> = keyhive_core::event::Event::<
        future_form::Sendable,
        MemorySigner,
        Vec<u8>,
        BigRepoKeyhiveListener,
    >::Revoked(revocation)
    .into();
    subduction_keyhive::save_event::<Vec<u8>, _, future_form::Sendable>(storage, &event)
        .await
        .map_err(|err| ferr!("failed saving keyhive revocation event: {err}"))?;
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
