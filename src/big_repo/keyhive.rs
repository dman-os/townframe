use crate::interlude::*;

use crate::{
    DocumentId, handler::BigRepoKeyhiveProtocol, keyhive_listener::BigRepoKeyhiveListener,
};
use keyhive_core::access::Access;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId as KhDocumentId;
use keyhive_core::principal::group::id::GroupId as KhGroupId;
use keyhive_core::principal::identifier::Identifier;
use keyhive_core::principal::membered::Membered;
use keyhive_crypto::signer::memory::MemorySigner;
use nonempty::NonEmpty;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use subduction_keyhive::message::EventHash;

pub type BigKeyhiveAgent = keyhive_core::principal::agent::Agent<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    BigRepoKeyhiveListener,
>;

type BigKeyhiveMembered = keyhive_core::principal::membered::Membered<
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
    contact_card: Arc<keyhive_core::contact_card::ContactCard>,
    keyhive_peer_id: subduction_keyhive::KeyhivePeerId,
    /// Test-only: when `Some`, [`Self::prekeys`] reports this set instead of folding the
    /// local individual's current prekey ops.
    ///
    /// Exists so a test can put the *published view* into disagreement with the core's
    /// prekey state, which is the one incoherence `prekey_janitor::refill_to_floor` cannot
    /// expand its way out of. `rotate_op_count_for` exists for the same reason on the
    /// rotation side: production has no path that desynchronises the two. Compiled out
    /// entirely without `cfg(test)`, so a production build cannot set it.
    #[cfg(test)]
    pinned_prekey_view: Arc<
        std::sync::Mutex<Option<std::collections::HashSet<keyhive_crypto::share_key::ShareKey>>>,
    >,
}
/// What an admitted Keyhive event names.
///
/// The difference between "names no graph" and "names a graph this hive cannot
/// resolve *yet*" is load-bearing. The first is a property of the event: a
/// prekey op changes which peers can be reached, not who is a member of what.
/// The second is a property of this hive's ingest position and clears itself
/// once the dependency lands — Keyhive classifies exactly it as a missing
/// dependency (`ReceiveStaticDelegationError::is_missing_dependency`). A caller
/// that takes the two for one thing either fails over a delivery race or drops
/// an access change it cannot yet name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventSubject {
    /// The graph the event changes.
    Named(Identifier),
    /// The event names no graph, so it cannot change a closure.
    Unnamed,
    /// The event's proof chain is not resolvable here yet: the hive has not
    /// applied a delegation the chain names. The event is early rather than
    /// undecodable, and the caller retries once the missing link lands.
    Unresolved,
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
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
            #[cfg(test)]
            pinned_prekey_view: Arc::new(std::sync::Mutex::new(None)),
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
            contact_card: Arc::new(contact_card),
            keyhive_peer_id,
            #[cfg(test)]
            pinned_prekey_view: Arc::new(std::sync::Mutex::new(None)),
        }))
    }

    /// Restore the full prekey state (membership ops + secret halves) from the
    /// incremental sidecar. Restores published membership as well as secrets, so
    /// restarts do not require a compaction to keep the pool intact.
    pub(crate) async fn import_prekey_state(
        &self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        let Some(bytes) = storage
            .load_prekey_secrets()
            .await
            .map_err(|err| ferr!("error loading keyhive prekey state: {err}"))?
        else {
            return Ok(());
        };
        self.keyhive
            .import_prekey_state(&bytes)
            .await
            .map_err(|err| ferr!("failed importing keyhive prekey state: {err}"))?;
        Ok(())
    }

    pub(crate) async fn save_prekey_state(
        &self,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        let bytes = self
            .keyhive
            .export_prekey_state()
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

    /// The local individual's currently published prekeys.
    ///
    /// Rebuilt from the individual's prekey ops (Add publishes, Rotate
    /// retires and replaces), mirroring `PrekeyState::build` upstream —
    /// the fields themselves are `pub(crate)`.
    ///
    /// NOTE: the tombstones must be collected and applied in a *second*
    /// pass, exactly like upstream `PrekeyState::build`: applying Rotate
    /// removals inline while iterating the ops map is order-dependent and
    /// resurrects rotated-out keys when the original Add happens to iterate
    /// after its Rotate.
    ///
    /// Test-only override: once [`Self::pin_published_prekeys`] has been called this
    /// returns the pinned set, so a test can observe what a divergent view does to its
    /// readers.
    pub(crate) async fn prekeys(
        &self,
    ) -> std::collections::HashSet<keyhive_crypto::share_key::ShareKey> {
        #[cfg(test)]
        if let Some(frozen) = self
            .pinned_prekey_view
            .lock()
            .expect("pinned prekey view mutex is never poisoned")
            .clone()
        {
            return frozen;
        }
        use keyhive_core::principal::individual::op::KeyOp;
        let individual = self.keyhive.individual().await;
        let locked = individual.lock().await;
        let mut set = std::collections::HashSet::new();
        let mut tombstones = Vec::new();
        for op in locked.prekey_ops().values() {
            let op: &KeyOp = op.as_ref();
            match op {
                KeyOp::Add(add) => {
                    set.insert(add.payload().share_key);
                }
                KeyOp::Rotate(rot) => {
                    tombstones.push(rot.payload().old);
                    set.insert(rot.payload().new);
                }
            }
        }
        for tombstone in tombstones {
            // Mirror `PrekeyState::build` upstream: the published set must
            // never become empty (a stale rotation cycle would otherwise
            // tombstone every key). Skip a removal that would empty it.
            if set.len() > 1 || !set.contains(&tombstone) {
                set.remove(&tombstone);
            }
        }
        set
    }

    /// Test-only: pin the set [`Self::prekeys`] reports, so every later publication into
    /// the core is invisible to it.
    ///
    /// This is the published-view half of the divergence `refill_to_floor` guards against:
    /// the core keeps publishing prekeys while the set this caller reads does not move. A
    /// booted handle starts one key *below* the floor (the production genesis publishes 7,
    /// `Active::generate`), so the refill has exactly one slot to fill; the set is passed
    /// explicitly so a test can name the view it wants — including one reporting fewer keys
    /// than the core has published. That is the shape that used to spin the refill loop
    /// forever, silently and with the durable cursor frozen behind it, so the loop now
    /// crashes naming the divergence. Test-only for the same reason as `rotate_op_count_for`:
    /// no production path can desynchronise the two, and the `cfg(test)` field cannot be set
    /// in a production build.
    #[cfg(test)]
    pub(crate) fn pin_published_prekeys(
        &self,
        pinned: std::collections::HashSet<keyhive_crypto::share_key::ShareKey>,
    ) {
        *self
            .pinned_prekey_view
            .lock()
            .expect("pinned prekey view mutex is never poisoned") = Some(pinned);
    }

    /// The local individual id of the active keyhive agent.
    pub(crate) async fn local_individual_id(
        &self,
    ) -> keyhive_core::principal::individual::id::IndividualId {
        self.keyhive.individual().await.lock().await.id()
    }

    /// Replace a published prekey with a fresh one.
    pub(crate) async fn rotate_prekey(
        &self,
        prekey: keyhive_crypto::share_key::ShareKey,
    ) -> Res<
        Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::individual::op::rotate_key::RotateKeyOp,
            >,
        >,
    > {
        self.keyhive
            .rotate_prekey(prekey)
            .await
            .map_err(|err| ferr!("prekey rotation failed: {err}"))
    }

    /// Publish an additional prekey, growing the available pool.
    pub(crate) async fn expand_prekeys(
        &self,
    ) -> Res<
        Arc<
            keyhive_crypto::signed::Signed<
                keyhive_core::principal::individual::op::add_key::AddKeyOp,
            >,
        >,
    > {
        self.keyhive
            .expand_prekeys()
            .await
            .map_err(|err| ferr!("prekey expansion failed: {err}"))
    }

    /// Number of `RotateKeyOp`s in the local prekey op set whose `old` key is
    /// `prekey`. Test-only: the published-set precheck makes the janitor
    /// idempotent, and this counts the rotations to prove it.
    #[cfg(test)]
    pub(crate) async fn rotate_op_count_for(
        &self,
        old: keyhive_crypto::share_key::ShareKey,
    ) -> usize {
        use keyhive_core::principal::individual::op::KeyOp;
        let individual = self.keyhive.individual().await;
        let locked = individual.lock().await;
        locked
            .prekey_ops()
            .values()
            .filter(|op| match op.as_ref() {
                KeyOp::Rotate(rot) => rot.payload().old == old,
                KeyOp::Add(_) => false,
            })
            .count()
    }

    pub async fn get_group(
        &self,
        id: keyhive_core::principal::group::id::GroupId,
    ) -> Option<BigKeyhiveGroup> {
        self.keyhive
            .get_group(id)
            .await
            .map(|inner| BigKeyhiveGroup { id, inner })
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
                let members =
                    transitive_members_short_locked(Membered::Document(kh_doc_id, doc)).await;
                if let Some((_, access)) = members.get(agent) {
                    caps.insert(DocumentId::new(kh_doc_id.to_bytes()), *access);
                }
            }
        }
        caps
    }

    /// All agents (individuals + groups) who can reach this doc/group, with [`Access`].
    /// O(|transitive_members(target)|) — used for incremental per-target update.
    ///
    /// Keyed by keyhive [`Identifier`], not by its bytes: the identifier is the
    /// identity every caller either already holds or must keep, and a byte key
    /// throws that away for the callers that need it back.
    pub async fn agents_for_membered(&self, id: Identifier) -> BTreeMap<Identifier, Access> {
        let keyhive = self.keyhive.as_ref();
        // Try document first, then group
        if let Some(doc) = keyhive.get_document(KhDocumentId::from(id)).await {
            return transitive_members_short_locked(Membered::Document(
                KhDocumentId::from(id),
                doc,
            ))
            .await
            .into_iter()
            .map(|(id, (_, access))| (id, access))
            .collect();
        }
        if let Some(group) = keyhive.get_group(KhGroupId::from(id)).await {
            return transitive_members_short_locked(Membered::Group(KhGroupId::from(id), group))
                .await
                .into_iter()
                .map(|(id, (_, access))| (id, access))
                .collect();
        }
        BTreeMap::new()
    }

    /// Whether this hive holds a document node for `id`.
    ///
    /// The `Identifier`-addressed twin of [`Self::get_group`]: an id is a
    /// membered subject when it names either, and a caller that resolves the
    /// kind itself must ask in the same order `agents_for_membered` does.
    pub(crate) async fn has_document(&self, id: Identifier) -> bool {
        self.keyhive
            .get_document(KhDocumentId::from(id))
            .await
            .is_some()
    }

    /// The graph an admitted event names, or why it names none.
    ///
    /// A `CgkaOperation` names its document; a `Delegated`/`Revoked` names the
    /// graph Keyhive dispatched the operation to, which is the proof chain's
    /// root issuer ([`SignedSubjectId`], consumed at `keyhive.rs:1980,2066`) and
    /// *not* the immediate signer — the two differ whenever a non-root member
    /// re-delegates, which is the hazard the group-part worker's
    /// `delegation.issuer` proxy carries (B19).
    ///
    /// The wire form carries proof *digests* (`StaticDelegation::proof`), so
    /// the chain is resolved through this hive's own graph: there is no
    /// payload-only derivation, and the resolution here is the same one Keyhive
    /// performs when it applies the event.
    ///
    /// Prekey events change which peers can be *reached*, not who is a member
    /// of what, so they name no graph and cannot change a closure.
    ///
    /// The graph is named from the event's own proof chain and never from the
    /// *delegate*: materializing the event needs the delegate's installed `Agent`
    /// record, and a replica that only observes a graph is never sent one — measured
    /// in the private-reader topology, where the delegate stayed unknown for at least
    /// 77s and a decode that waited for it never ran. Naming the graph needs none of
    /// that ([`Keyhive::static_membership_subject`]).
    ///
    /// An unresolvable chain is reported as [`EventSubject::Unresolved`] and not as a
    /// failure: the event is early, its source row is not settled yet, and the caller
    /// retries once the missing link lands. Anything else is an error.
    pub(crate) async fn event_subject_id(&self, event: StaticEvent<Vec<u8>>) -> Res<EventSubject> {
        Ok(match &event {
            StaticEvent::PrekeysExpanded(_) | StaticEvent::PrekeyRotated(_) => {
                EventSubject::Unnamed
            }
            StaticEvent::CgkaOperation(operation) => EventSubject::Named(Identifier::from(
                ed25519_dalek::VerifyingKey::from(*operation.payload().doc_id()),
            )),
            // The proof chain's *root* issuer, not the immediate signer: the walk
            // answers the chain head's issuer, which is the graph Keyhive dispatched
            // the operation to. Using the immediate issuer is the B19 hazard and would
            // name a non-membered id whenever a non-root member re-delegates.
            StaticEvent::Delegated(_) | StaticEvent::Revoked(_) => {
                match self.keyhive.static_membership_subject(&event).await {
                    Some(subject) => EventSubject::Named(subject),
                    None => EventSubject::Unresolved,
                }
            }
        })
    }

    /// What [`Access`] does `agent` have on this doc/group? None if unreachable.
    pub async fn agent_access_on(
        &self,
        agent: &Identifier,
        membered_id: Identifier,
    ) -> Option<Access> {
        let keyhive = self.keyhive.as_ref();
        if let Some(doc) = keyhive.get_document(KhDocumentId::from(membered_id)).await {
            return transitive_members_short_locked(Membered::Document(
                KhDocumentId::from(membered_id),
                doc,
            ))
            .await
            .get(agent)
            .map(|(_, access)| *access);
        }
        if let Some(group) = keyhive.get_group(KhGroupId::from(membered_id)).await {
            return transitive_members_short_locked(Membered::Group(
                KhGroupId::from(membered_id),
                group,
            ))
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
                let members =
                    transitive_members_short_locked(Membered::Document(kh_doc_id, doc)).await;
                if let Some((_, access)) = members.get(agent) {
                    caps.insert(kh_doc_id.to_bytes(), *access);
                }
            }
        }
        // Enumerate groups
        let group_ids: Vec<KhGroupId> = {
            let groups = keyhive.groups().lock().await;
            groups.keys().copied().collect()
        };
        for kh_group_id in group_ids {
            if let Some(group) = keyhive.get_group(kh_group_id).await {
                let members =
                    transitive_members_short_locked(Membered::Group(kh_group_id, group)).await;
                if let Some((_, access)) = members.get(agent) {
                    caps.insert(kh_group_id.to_bytes(), *access);
                }
            }
        }
        caps
    }

    pub(crate) async fn document_ids(&self) -> Vec<big_sync_core::ObjKey> {
        self.keyhive
            .documents()
            .lock()
            .await
            .keys()
            .map(|id| big_sync_core::ObjKey::new(id.to_bytes()))
            .collect()
    }

    pub(crate) async fn document_content_keys(
        &self,
        doc_id: DocumentId,
    ) -> Res<Vec<(Vec<u8>, [u8; 32])>> {
        let doc = self
            .keyhive
            .get_document(keyhive_doc_id(doc_id.clone())?)
            .await
            .ok_or_else(|| ferr!("keyhive document not found: {doc_id}"))?;
        Ok(doc
            .lock()
            .await
            .known_decryption_keys()
            .iter()
            .map(|(reference, key)| (reference.clone(), (*key).into()))
            .collect())
    }

    pub(crate) async fn document_has_content(&self, doc_id: DocumentId) -> Res<bool> {
        let kh_doc_id = keyhive_doc_id(doc_id)?;
        Ok(self.keyhive.get_document(kh_doc_id).await.is_some())
    }

    pub(crate) async fn document_ids_containing_group(
        &self,
        group_id: Identifier,
    ) -> BTreeSet<DocumentId> {
        self.keyhive
            .document_ids_containing_group(KhGroupId::from(group_id))
            .await
            .into_iter()
            .map(|id| DocumentId::new(id.to_bytes()))
            .collect()
    }

    pub(crate) async fn group_ids_containing_document(
        &self,
        doc_id: DocumentId,
    ) -> Res<BTreeSet<[u8; 32]>> {
        // Non-document object ids (plain part-store payloads synced alongside
        // documents) belong to no keyhive group. Eligibility callers treat
        // them as out of scope instead of failing the worker.
        let Ok(kh_doc_id) = keyhive_doc_id(doc_id) else {
            return Ok(BTreeSet::new());
        };
        let Some(doc) = self.keyhive.get_document(kh_doc_id).await else {
            return Ok(BTreeSet::new());
        };
        let transitive = Membered::Document(kh_doc_id, doc)
            .transitive_members()
            .await;
        let group_ids: Vec<KhGroupId> =
            self.keyhive.groups().lock().await.keys().copied().collect();
        Ok(group_ids
            .into_iter()
            .filter_map(|group_id| {
                let group_identifier: Identifier = group_id.into();
                transitive
                    .contains_key(&group_identifier)
                    .then_some(group_id.to_bytes())
            })
            .collect())
    }

    /// Whether `doc_id` is shaped like a Keyhive document id (an Ed25519
    /// verifying key). The shape is not a classifier: blake3-derived object ids
    /// pass it about half the time. Only the frontier worker's test-only scope
    /// assertion uses this — the document/derived scope boundary, not id shape,
    /// is what keeps non-documents out of the frontier source.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn is_valid_keyhive_document_id(doc_id: DocumentId) -> bool {
        keyhive_doc_id(doc_id).is_ok()
    }

    pub(crate) async fn current_cgka_ops_count(&self, doc_id: DocumentId) -> Res<usize> {
        let Ok(kh_doc_id) = keyhive_doc_id(doc_id) else {
            return Ok(0);
        };
        let Some(doc) = self.keyhive.get_document(kh_doc_id).await else {
            return Ok(0);
        };
        Ok(doc.lock().await.cgka().map_or(0, |cgka| cgka.ops_count()))
    }

    pub(crate) fn contact_card(&self) -> &keyhive_core::contact_card::ContactCard {
        &self.contact_card
    }

    pub(crate) async fn receive_contact_card(
        &self,
        contact_card: &keyhive_core::contact_card::ContactCard,
    ) -> Res<BigKeyhiveAgent> {
        self.keyhive
            .receive_contact_card(contact_card)
            .await
            .map_err(|error| ferr!("failed receiving Keyhive contact card: {error}"))?;
        // `receive_contact_card` invokes our Keyhive listener, which persists
        // the prekey event before this await completes.
        self.get_agent_by_peer_id(&subduction_keyhive::KeyhivePeerId::from_bytes(
            contact_card.id().to_bytes(),
        ))
        .await?
        .ok_or_eyre("received contact card did not create a Keyhive agent")
    }

    pub(crate) fn keyhive_peer_id(&self) -> subduction_keyhive::KeyhivePeerId {
        self.keyhive_peer_id.clone()
    }

    pub(crate) async fn create_doc(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        initial_content_heads: NonEmpty<[u8; 32]>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<(DocumentId, Vec<EventHash>)> {
        // `generate_doc` addresses its coparents by id; build the peers anyway
        // so each authority is still validated on the way through.
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?
            .into_iter()
            .map(|peer| peer.id())
            .collect::<Vec<Identifier>>();
        let initial_content_heads = NonEmpty {
            head: initial_content_heads.head.to_vec(),
            tail: initial_content_heads
                .tail
                .into_iter()
                .map(Vec::from)
                .collect(),
        };
        let coparent_count = coparents.len();
        let keyhive = self.keyhive.as_ref();
        let kh_doc_id = match keyhive.generate_doc(coparents, initial_content_heads).await {
            Ok(kh_doc_id) => kh_doc_id,
            Err(err) => {
                // A coparent's prekey is published by its own hive and only reaches us through
                // sync, so this error means an individual we are about to co-sign with has no
                // published prekey here yet. Either its publication is still in flight, or we
                // never pulled it; it is logged rather than retried because a retry would hide
                // the second case. Say which of the two it is rather than leaving the caller
                // to guess.
                if let keyhive_core::principal::document::GenerateDocError::MissingPrekeys(
                    missing,
                ) = &err
                {
                    self.explain_missing_prekeys(missing, coparent_count, "create_doc")
                        .await;
                }
                return Err(ferr!("failed creating keyhive document: {err}"));
            }
        };
        let hashes = self.persist_document_events(kh_doc_id, protocol).await?;
        Ok((DocumentId::new(kh_doc_id.to_bytes()), hashes))
    }

    /// Distinguish the two ways a coparent can have no prekey here, because they have different
    /// owners. An individual that is not registered at all means its own prekey op never reached
    /// us: the identifier travels in other principals' events (a delegation names the delegate),
    /// but the node is only ever born from the individual's own op, so a member we learned about
    /// from someone else's delegation is simply absent. A registered individual holding no prekey
    /// ops is the other case, and not one the wire can produce: `Individual::new` builds the state
    /// from the op that registers it, so an empty state is something we restored or pruned.
    async fn explain_missing_prekeys(
        &self,
        missing: &keyhive_core::principal::individual::MissingPrekeys,
        coparent_count: usize,
        site: &'static str,
    ) {
        let keyhive_core::principal::individual::MissingPrekeys::NoPublishedPrekey(missing_id) =
            missing;
        let missing_id = **missing_id;
        let detail = match self.keyhive.get_individual(missing_id).await {
            None => "individual not registered locally: no op of its own was applied".to_owned(),
            Some(individual) => {
                let locked = individual.lock().await;
                // TEMP-INSTRUMENTATION(prekey-dive): `pick_prekey` reads the live set, which
                // `PrekeyState::build` provably cannot empty while the op log is non-empty
                // (it skips a tombstone that would empty the set). A zero beside a non-zero
                // op count therefore means this field was deserialized stale, or that the
                // selection used a different copy of the same individual.
                format!(
                    "individual registered: held prekey ops={}, live prekeys={}",
                    locked.prekey_ops().len(),
                    locked.prekeys().len(),
                )
            }
        };
        tracing::warn!(
            %missing_id,
            coparent_count,
            site,
            detail = %detail,
            "document creation has no published prekey for a coparent"
        );
    }

    /// TEMP-INSTRUMENTATION(prekey-dive): which copy of a coparent's individual the prekey
    /// selection walks, and how it compares with the hive registry's.
    ///
    /// A `Peer::Individual` selects from the `Individual` it carries; a group or document
    /// peer walks the individuals embedded in its members' delegation payloads. Neither is
    /// necessarily the registry's copy, and only the registry's is what
    /// [`Self::explain_missing_prekeys`] reports on.
    async fn probe_coparent_prekeys(&self, coparents: &[BigKeyhivePeer], doc_id: &DocumentId) {
        let probe_doc_id = match keyhive_doc_id(doc_id.clone()) {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!(%err, "prekey probe: cannot compute the keyhive document id");
                return;
            }
        };
        for peer in coparents {
            let selected = peer.pick_individual_prekeys(probe_doc_id).await;
            match peer {
                BigKeyhivePeer::Individual(id, indie) => {
                    let (peer_ops, peer_live) = {
                        let locked = indie.lock().await;
                        (locked.prekey_ops().len(), locked.prekeys().len())
                    };
                    let registry = self.keyhive.get_individual(*id).await;
                    let (reg_ops, reg_live, same_arc) = match registry {
                        Some(registry) => {
                            let locked = registry.lock().await;
                            (
                                locked.prekey_ops().len(),
                                locked.prekeys().len(),
                                Arc::ptr_eq(&registry, indie),
                            )
                        }
                        None => (0, 0, false),
                    };
                    tracing::warn!(
                        %id,
                        peer_ops,
                        peer_live,
                        reg_ops,
                        reg_live,
                        same_arc,
                        selection_ok = selected.is_ok(),
                        selection_err = %selected.as_ref().err().map(ToString::to_string).unwrap_or_default(),
                        "prekey probe: individual coparent"
                    );
                }
                BigKeyhivePeer::Group(id, group) => {
                    tracing::warn!(group = %id, "prekey probe: group coparent");
                    let members = group.lock().await.transitive_members().await;
                    for (member, (agent, _access)) in members {
                        if let BigKeyhiveAgent::Individual(member_id, indie) = agent {
                            let locked = indie.lock().await;
                            tracing::warn!(
                                group = %id,
                                %member,
                                %member_id,
                                member_ops = locked.prekey_ops().len(),
                                member_live = locked.prekeys().len(),
                                "prekey probe: group member's embedded individual"
                            );
                        }
                    }
                }
                BigKeyhivePeer::Document(id, _doc) => {
                    tracing::warn!(document = %id, "prekey probe: document coparent");
                }
            }
        }
    }

    pub(crate) async fn reserve_doc_id(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<DocumentId> {
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand_08::rngs::OsRng);
        let doc_id = DocumentId::new(signing_key.verifying_key().to_bytes());
        let reservation = crate::keyhive_storage::DocReservation {
            magic: crate::keyhive_storage::DOC_RESERVATION_MAGIC,
            doc_id: doc_id.to_bytes32()?,
            signing_key: signing_key.to_bytes(),
            parents: parents
                .into_iter()
                .map(|parent| parent.into_identifier().to_bytes())
                .collect(),
            initial_keys: Vec::new(),
            initial_content: None,
        };
        storage
            .save_doc_reservation(&reservation)
            .await
            .map_err(|err| ferr!("failed persisting document id reservation: {err}"))?;
        Ok(doc_id)
    }

    pub(crate) async fn stage_reserved_doc(
        &self,
        doc_id: DocumentId,
        initial_content: Vec<u8>,
        initial_keys: Vec<(Vec<u8>, [u8; 32])>,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<()> {
        if storage
            .load_doc_reservation(doc_id.to_bytes32()?)
            .await
            .map_err(|err| ferr!("failed loading document reservation: {err}"))?
            .is_none()
        {
            if self
                .keyhive
                .get_document(keyhive_doc_id(doc_id.clone())?)
                .await
                .is_some()
            {
                return Ok(());
            }
            return Err(ferr!("no reservation and no keyhive document for {doc_id}"));
        }
        storage
            .stage_doc_reservation(doc_id.to_bytes32()?, initial_content, initial_keys)
            .await
            .map_err(|err| ferr!("failed staging initial document content: {err}"))
    }

    /// Ensure the Keyhive document exists with the reserved signing key and
    /// real, non-empty content heads. Reservation cleanup is performed only by
    /// the outer lifecycle after Sedimentree persistence and pending-group
    /// cleanup have completed.
    ///
    /// Idempotent: if the reservation is already gone the document was already
    /// finalized; if the Keyhive document already exists the events were
    /// persisted and only the reservation cleanup is retried.
    pub(crate) async fn finalize_reserved_doc(
        &self,
        doc_id: DocumentId,
        content_heads: NonEmpty<[u8; 32]>,
        protocol: &BigRepoKeyhiveProtocol,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<Vec<EventHash>> {
        let kh_doc_id = keyhive_doc_id(doc_id.clone())?;
        let Some(reservation) = storage
            .load_doc_reservation(doc_id.to_bytes32()?)
            .await
            .map_err(|err| ferr!("failed loading document id reservation: {err}"))?
        else {
            // No reservation: the document was either never allocated or
            // already finalized. Only the latter is a successful completion.
            if self.keyhive.get_document(kh_doc_id).await.is_some() {
                return Ok(Vec::new());
            }
            return Err(ferr!(
                "no reservation and no keyhive document for {doc_id}; cannot finalize"
            ));
        };
        if self.keyhive.get_document(kh_doc_id).await.is_some() {
            // Events were already persisted. Leave the reservation in place;
            // the outer lifecycle still has to persist the Sedimentree and
            // remove the pending-group authority.
            return Ok(Vec::new());
        }
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&reservation.signing_key);
        if signing_key.verifying_key().to_bytes() != doc_id.to_bytes32()? {
            return Err(ferr!(
                "reserved signing key does not match document id {doc_id}"
            ));
        }
        let mut coparents = Vec::with_capacity(reservation.parents.len());
        for parent_id in reservation.parents {
            let vk = ed25519_dalek::VerifyingKey::from_bytes(&parent_id)
                .map_err(|_| ferr!("reserved parent is not a valid Ed25519 point"))?;
            let identifier = Identifier::from(vk);
            let agent =
                self.keyhive.get_agent(identifier).await.ok_or_else(|| {
                    ferr!("cannot resolve reserved parent authority {identifier:?}")
                })?;
            coparents.push(BigKeyhiveAuthority::Agent(agent).into_peer()?);
        }
        let initial_content_heads = NonEmpty {
            head: content_heads.head.to_vec(),
            tail: content_heads.tail.into_iter().map(Vec::from).collect(),
        };
        let coparent_count = coparents.len();
        // TEMP-INSTRUMENTATION(prekey-dive): the selection reads the individual each
        // coparent `Peer` carries, and a group/document peer walks the individuals embedded
        // in its delegation payloads -- neither is necessarily the hive registry's copy.
        // Keep them so the error branch can say which copy failed to produce a prekey.
        let coparents_for_probe = coparents.clone();
        let kh_doc_id = match self
            .keyhive
            .generate_doc_with_reserved_signer(signing_key, coparents, initial_content_heads)
            .await
        {
            Ok(kh_doc_id) => kh_doc_id,
            Err(err) => {
                // Same reasoning as `create_doc`: a coparent prekey that has not reached us is
                // a pull/publication question first, so record which individual is missing it.
                if let keyhive_core::principal::document::GenerateDocError::MissingPrekeys(
                    missing,
                ) = &err
                {
                    self.explain_missing_prekeys(missing, coparent_count, "reserved")
                        .await;
                }
                self.probe_coparent_prekeys(&coparents_for_probe, &doc_id)
                    .await;
                return Err(ferr!("failed creating keyhive document: {err}"));
            }
        };
        let hashes = self.persist_document_events(kh_doc_id, protocol).await?;
        Ok(hashes)
    }

    pub(crate) async fn complete_reserved_doc(
        &self,
        pending_group: &BigKeyhiveGroup,
        doc_id: DocumentId,
        after_content: Vec<Vec<u8>>,
        protocol: &BigRepoKeyhiveProtocol,
        storage: &crate::keyhive_storage::BigRepoKeyhiveStorage,
    ) -> Res<Vec<EventHash>> {
        let Some(_reservation) = storage
            .load_doc_reservation(doc_id.to_bytes32()?)
            .await
            .map_err(|err| ferr!("failed loading document reservation: {err}"))?
        else {
            if self
                .keyhive
                .get_document(keyhive_doc_id(doc_id.clone())?)
                .await
                .is_none()
            {
                return Err(ferr!("no reservation and no keyhive document for {doc_id}"));
            }
            return Ok(Vec::new());
        };
        let document_ids = self.group_document_ids(pending_group).await;
        let hashes = if document_ids.contains(&doc_id) {
            self.revoke_group_from_doc(pending_group, doc_id.clone(), after_content, protocol)
                .await?
        } else {
            Vec::new()
        };
        storage
            .delete_doc_reservation(doc_id.to_bytes32()?)
            .await
            .map_err(|err| ferr!("failed deleting document reservation: {err}"))?;
        Ok(hashes)
    }

    pub(crate) async fn revoke_group_from_doc(
        &self,
        pending_group: &BigKeyhiveGroup,
        doc_id: DocumentId,
        after_content: Vec<Vec<u8>>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<Vec<EventHash>> {
        self.revoke_doc_access(pending_group.clone(), doc_id, true, after_content, protocol)
            .await
    }

    async fn persist_document_events(
        &self,
        doc_id: KhDocumentId,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<Vec<EventHash>> {
        // Document creation answers with an id now, so resolve the shared
        // handle here to read the ops the new document starts with.
        let doc = self
            .keyhive
            .as_ref()
            .get_document(doc_id)
            .await
            .ok_or_else(|| ferr!("keyhive document missing when persisting its initial events"))?;
        let (cgka_ops, delegations) = {
            let locked = doc.lock().await;
            (
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
        let mut hashes = persist_cgka_update_ops(protocol, cgka_ops).await?;
        for delegation in delegations {
            if let Some(hash) = persist_delegation(protocol, delegation).await? {
                hashes.push(hash);
            }
        }
        Ok(hashes)
    }

    pub(crate) async fn create_group_with_parents(
        &self,
        parents: Vec<BigKeyhiveAuthority>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<(BigKeyhiveGroup, Vec<EventHash>)> {
        // As in `create_doc`: validate through the peer, hand over the id.
        let coparents = parents
            .into_iter()
            .map(BigKeyhiveAuthority::into_peer)
            .collect::<Res<Vec<_>>>()?
            .into_iter()
            .map(|peer| peer.id())
            .collect::<Vec<Identifier>>();
        let keyhive = self.keyhive.as_ref();
        let group_id = keyhive
            .generate_group(coparents)
            .await
            .map_err(|err| ferr!("error creating keyhive group: {err}"))?;
        // `generate_group` answers with the id alone; read the shared handle
        // back so the group can still be locked for its delegations.
        let inner = keyhive
            .get_group(group_id)
            .await
            .ok_or_else(|| ferr!("keyhive group missing after creating it"))?;
        let (id, delegations) = {
            let locked = inner.lock().await;
            (
                locked.group_id(),
                locked
                    .members()
                    .values()
                    .flat_map(|delegations| delegations.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        };
        let mut hashes = Vec::new();
        for delegation in delegations {
            if let Some(hash) = persist_delegation(protocol, delegation).await? {
                hashes.push(hash);
            }
        }
        Ok((BigKeyhiveGroup { id, inner }, hashes))
    }

    pub(crate) async fn group_document_ids(&self, group: &BigKeyhiveGroup) -> BTreeSet<DocumentId> {
        self.keyhive
            .document_ids_containing_group(group.id())
            .await
            .into_iter()
            .map(|doc_id| DocumentId::new(doc_id.as_bytes()))
            .collect()
    }

    pub(crate) async fn add_member_to_group(
        &self,
        member: impl Into<BigKeyhiveAuthority>,
        group: &BigKeyhiveGroup,
        access: keyhive_core::access::Access,
        after_content: BTreeMap<DocumentId, Vec<Vec<u8>>>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<(BTreeSet<DocumentId>, Vec<EventHash>)> {
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
            .map(|op| DocumentId::new(op.payload().doc_id().as_bytes()))
            .collect();
        let mut hashes = persist_cgka_update_ops(protocol, update.cgka_ops).await?;
        if let Some(hash) = persist_delegation(protocol, update.delegation).await? {
            hashes.push(hash);
        }
        Ok((affected_docs, hashes))
    }

    /// Grant an agent access to a document.
    pub(crate) async fn grant_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        access: keyhive_core::access::Access,
        after_content: Vec<Vec<u8>>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<Vec<EventHash>> {
        use keyhive_core::principal::membered::Membered;
        let agent = principal.into().into_agent();
        let kh_doc_id = keyhive_doc_id(doc_id.clone())?;
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
        let mut hashes = persist_cgka_update_ops(protocol, update.cgka_ops).await?;
        if let Some(hash) = persist_delegation(protocol, update.delegation).await? {
            hashes.push(hash);
        }
        Ok(hashes)
    }

    /// Revoke an authority's access to a document with an explicit content frontier.
    pub(crate) async fn revoke_doc_access(
        &self,
        principal: impl Into<BigKeyhiveAuthority>,
        doc_id: DocumentId,
        retain_all_other_members: bool,
        after_content: Vec<Vec<u8>>,
        protocol: &BigRepoKeyhiveProtocol,
    ) -> Res<Vec<EventHash>> {
        use keyhive_core::principal::membered::Membered;

        let kh_doc_id = keyhive_doc_id(doc_id.clone())?;
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
        let mut hashes = persist_cgka_update_ops(protocol, update.cgka_ops().to_vec()).await?;
        for revocation in update.revocations() {
            if let Some(hash) = persist_revocation(protocol, Arc::clone(revocation)).await? {
                hashes.push(hash);
            }
        }
        for redelegation in update.redelegations() {
            if let Some(hash) = persist_delegation(protocol, Arc::clone(redelegation)).await? {
                hashes.push(hash);
            }
        }
        Ok(hashes)
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
    // A document key is a BigSync object key, which ADR 012 decision 1 makes arbitrary
    // bytes: the keyhive identifier is a fixed-width consumer, so a wrong-width key is an
    // error here rather than a panicking assertion.
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32()?)
        .map_err(|_| ferr!("doc_id is not a valid Ed25519 point"))?;
    Ok(keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    ))
}

async fn persist_delegation(
    protocol: &BigRepoKeyhiveProtocol,
    delegation: Arc<keyhive_crypto::signed::Signed<BigKeyhiveDelegation>>,
) -> Res<Option<EventHash>> {
    let event: StaticEvent<Vec<u8>> = keyhive_core::event::Event::<
        future_form::Sendable,
        MemorySigner,
        Vec<u8>,
        BigRepoKeyhiveListener,
    >::Delegated(delegation)
    .into();
    Ok(protocol
        .persist_local_events(vec![event])
        .await
        .map_err(|err| ferr!("failed persisting keyhive delegation event: {err}"))?
        .into_iter()
        .next())
}

async fn persist_revocation(
    protocol: &BigRepoKeyhiveProtocol,
    revocation: Arc<keyhive_crypto::signed::Signed<BigKeyhiveRevocation>>,
) -> Res<Option<EventHash>> {
    let event: StaticEvent<Vec<u8>> = keyhive_core::event::Event::<
        future_form::Sendable,
        MemorySigner,
        Vec<u8>,
        BigRepoKeyhiveListener,
    >::Revoked(revocation)
    .into();
    Ok(protocol
        .persist_local_events(vec![event])
        .await
        .map_err(|err| ferr!("failed persisting keyhive revocation event: {err}"))?
        .into_iter()
        .next())
}

async fn persist_cgka_update_ops(
    protocol: &BigRepoKeyhiveProtocol,
    cgka_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
) -> Res<Vec<EventHash>> {
    protocol
        .persist_local_events(
            cgka_ops
                .into_iter()
                .map(|op| StaticEvent::CgkaOperation(Box::new(op)))
                .collect(),
        )
        .await
        .map_err(|err| ferr!("failed persisting cgka update ops: {err}"))
}

struct ExploreNode {
    membered: BigKeyhiveMembered,
    access: Access,
}

/// Transitive-membership walk with short per-node locks.
/// Replicates `Group::transitive_members` semantics (explore/expanded/access-min
/// with the root excluded) but never holds a doc/group lock across an await that
/// acquires another lock: every node is locked only long enough to clone its
/// direct members + capabilities, then the lock is dropped before the next node
/// is visited. A walk therefore holds at most one lock at a time, so concurrent
/// walks rooted at different docs/groups cannot ABBA-deadlock with each other
/// (or with materialization decrypts that briefly lock a single document).
async fn transitive_members_short_locked(
    root: BigKeyhiveMembered,
) -> HashMap<Identifier, (BigKeyhiveAgent, Access)> {
    let root_id: Identifier = root.agent_id().into();
    let mut caps: HashMap<Identifier, (BigKeyhiveAgent, Access)> = HashMap::new();
    let mut expanded: HashMap<Identifier, Access> = HashMap::new();
    let mut explore: Vec<ExploreNode> = Vec::new();

    // Capture the root's direct members under a short lock, then walk.
    let root_members = root.members().await;
    for member_id in root_members.keys() {
        let Some(dlg) = root.get_capability(member_id).await else {
            // Revoked concurrently between the members() snapshot and this
            // capability lookup: the member is no longer part of the group.
            // Skip rather than panic — keyhive's own walk never sees this
            // because it holds the group lock for the whole traversal, but
            // our short-locked walk deliberately drops it between awaits.
            continue;
        };
        enqueue_member(
            dlg.payload.delegate().clone(),
            dlg.payload.can(),
            &root_id,
            &mut caps,
            &mut expanded,
            &mut explore,
        );
    }

    while let Some(explored) = explore.pop() {
        let membered = explored.membered;
        let access = explored.access;
        let members = membered.members().await;
        for (mem_id, dlgs) in members.iter() {
            let Some(dlg) = membered.get_capability(mem_id).await else {
                // Same concurrent-revocation race as the root loop above.
                continue;
            };
            let member_access = access.min(dlg.payload.can());
            if mem_id != &root_id
                && caps
                    .get(mem_id)
                    .is_none_or(|(_, existing_access)| *existing_access < member_access)
            {
                caps.insert(*mem_id, (dlg.payload.delegate().clone(), member_access));
            }
            for sub_dlg in dlgs.iter() {
                enqueue_member(
                    sub_dlg.payload.delegate().clone(),
                    access.min(sub_dlg.payload.can()),
                    &root_id,
                    &mut caps,
                    &mut expanded,
                    &mut explore,
                );
            }
        }
    }

    caps
}

fn enqueue_member(
    agent: BigKeyhiveAgent,
    access: Access,
    root_id: &Identifier,
    caps: &mut HashMap<Identifier, (BigKeyhiveAgent, Access)>,
    expanded: &mut HashMap<Identifier, Access>,
    explore: &mut Vec<ExploreNode>,
) {
    let id = agent.id();
    if &id == root_id {
        return;
    }
    if caps
        .get(&id)
        .is_none_or(|(_, existing_access)| *existing_access < access)
    {
        caps.insert(id, (agent.clone(), access));
    }
    if let Some(membered) = agent.as_membered()
        && expanded
            .get(&id)
            .is_none_or(|existing_access| *existing_access < access)
    {
        expanded.insert(id, access);
        explore.push(ExploreNode { membered, access });
    }
}

#[cfg(test)]
mod tests;
