//! Keyhive access delta stream: an inert mapping from the durable Keyhive
//! admission log to per-subject access deltas (ADR 013, lane 1).
//!
//! The store owns no cursor, no event loop, no output storage, and no revision
//! assignment of its own — the shape of `DocDeltaRevisionStore`, not the shape
//! ADR 008 §5 describes. Consumers drive it with a `ConcurrentDeltaWalker` over
//! their own walker-state repo, which owns the cursor and the ack path; the
//! stream owns neither.
//!
//! Two things this mapping does that the frontier-derived precedent does not:
//!
//! 1. It reads Keyhive state — the transitive closure of the subject an
//!    admitted event names — at read time, which is what makes the design
//!    replay-safe without a durable projection: the log carries *when* to look,
//!    Keyhive carries *what is true*.
//! 2. It resolves the event's subject through that same local graph. A
//!    delegation's wire form carries only proof *digests*
//!    (`StaticDelegation::proof`), so the proof-chain root — the graph Keyhive
//!    dispatched the operation to — is not derivable from the payload alone.
//!
//! `AccessSubjectSet::All` is the shape built here: every admitted event's
//! subject produces an entry and no closure cache is consulted, because there
//! is nothing to test membership against. `Watched` — the O(k) optimisation
//! that pays for an affected test — lands with its first consumer.
//!
//! The stream is exported from `big_repo` for one reason: it needs the
//! crate-private admission store and the crate-private Keyhive helpers. The raw
//! admission log is not exposed (§1).

use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::keyhive_admission;
use crate::runtime2::tasks::TokioTimer;
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateRepo;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use keyhive_core::access::Access;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::group::id::GroupId as KhGroupId;
use keyhive_core::principal::identifier::Identifier;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;

/// A Keyhive *membered* identity that carries a membership graph, with its kind.
///
/// An individual holds capabilities but is not itself a subject, and a
/// delegation is not a subject either: it is an event whose subject is the
/// graph it modifies. The kind is part of the identity because a document and a
/// group are addressed in the same identifier space, and a consumer that names
/// a subject has to say which one it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AccessSubject {
    Document(Identifier),
    Group(Identifier),
}

/// Which subjects a consuming site mirrors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessSubjectSet {
    /// Only these subjects. The affected test is
    /// `subject(event) ∈ watch ∪ ⋃ closure(subject)` over the ids in the cached
    /// closure rows, so this is the path that pays for an O(k) test.
    Watched(BTreeSet<AccessSubject>),
    /// Every subject the log names. There is no affected test to make: every
    /// admitted event's subject produces an entry and no closure cache is
    /// consulted. Expanding a subject into the consumer's own affected set
    /// stays consumer-side.
    All,
}

/// Selection for one consuming site: the subjects it mirrors and its memory.
#[derive(Clone, Debug)]
pub struct KeyhiveAccessSelector<M> {
    pub watch: AccessSubjectSet,
    /// The site's walker-state repo, which owns the walker cursor and (under
    /// `Watched`) the per-subject closure rows. Under `All` the reader consults
    /// no closure cache, so a consumer whose sink already holds the last
    /// snapshot may settle without a sparse row at all.
    pub memory: M,
}

/// Sparse row value: the whole recomputed closure of one subject.
///
/// This is a **row value**, not a repository — it is what a consumer writes
/// through its walker-state repo (`delta_walker_key_state`), and no method
/// anywhere returns it. It is not a payload cache either: it is the index the
/// `Watched` affected test reads, and it cannot be reconstructed from any sink,
/// because no sink can answer "which groups are reachable from this subject".
///
/// The key is the `(id -> Access)` map and not the id set: a re-delegation can
/// change a member's level without changing the id set, and comparing id sets
/// would silently keep a stale level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyhiveAccessMemory {
    pub agents: BTreeMap<Identifier, Access>,
}

/// The row is a map keyed by [`Identifier`], which is not a string, so the
/// derived codec cannot carry it: a JSON object needs string keys, and this is
/// a JSON row. The wire form is the ordered `(id, level)` pair list the
/// `BTreeMap` already iterates in, so encoding is order-stable and decoding
/// rebuilds the map. A pair list round trips losslessly here because the
/// closure map holds exactly one entry per id.
impl Serialize for KeyhiveAccessMemory {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.agents
            .iter()
            .collect::<Vec<(&Identifier, &Access)>>()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for KeyhiveAccessMemory {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let pairs = Vec::<(Identifier, Access)>::deserialize(deserializer)?;
        Ok(Self {
            agents: pairs.into_iter().collect(),
        })
    }
}

/// One access delta: the resulting closure of one subject, verbatim.
///
/// `agents` is the full resulting set, not a diff, and access levels are copied
/// verbatim including `Relay` — relays are supposed to retain blob partitions
/// without reading private documents, and withholding them here would withhold
/// exactly the entries ADR 001 intends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyhiveAccessDelta {
    pub subject: AccessSubject,
    pub agents: BTreeMap<Identifier, Access>,
    /// The admission head observed just before the closure was read.
    ///
    /// Diagnostic only: per-key ordering plus the walker's supersede already
    /// exclude stale application, and the memory row is what decides a
    /// recompute. Because the head is observed before the closure reads, the
    /// closure is always at or ahead of this seq — a closure may lead its
    /// revision, never lag it.
    pub computed_at_seq: u64,
}

/// Inert mapping over the durable Keyhive admission log.
pub struct KeyhiveAccessRevisionStore<M> {
    source: keyhive_admission::Store,
    keyhive: BigKeyhiveHandle,
    _memory: PhantomData<M>,
}

impl<M> KeyhiveAccessRevisionStore<M>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'static,
{
    /// Build the stream over `repo`'s admission log and Keyhive hive.
    pub fn new(repo: &crate::BigRepo) -> Self {
        Self::over(repo.sqlite_store(), repo.keyhive().clone())
    }

    /// Build over an explicit admission store and hive.
    fn over(store: SqliteBigRepoStore, keyhive: BigKeyhiveHandle) -> Self {
        Self {
            source: keyhive_admission::Store {
                store,
                timer: Arc::new(TokioTimer),
            },
            keyhive,
            _memory: PhantomData,
        }
    }

    /// Register this consumer's retention cursor: the admission-log floor that
    /// keeps this consumer's wake-ups alive.
    ///
    /// Monotone by construction — registration keeps `MAX(seq, excluded.seq)`
    /// — so a lower cursor cannot move the floor back. The reader id is derived
    /// from the walker identity `(namespace, consumer_id)` that owns the
    /// consumer's cursor, never passed in, so one consumer cannot acquire two
    /// retention rows and two consumers cannot share one.
    ///
    /// This is *not* the walker's durable progress: it may lag it and must
    /// never lead it. The payload here is read-time state, so a wake-up pruned
    /// ahead of the consumer is not self-healing — the subject would stay stale
    /// until some later event touched it again.
    pub async fn note_retention(
        &self,
        memory: &big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo,
        cursor: u64,
    ) -> Res<()> {
        self.source
            .store
            .register_keyhive_admission_reader(&memory.retention_reader_id(), cursor)
            .await
    }

    /// The floor through which the admission log has already been pruned.
    ///
    /// A consumer whose durable cursor is below this floor cannot be woken for
    /// the gap between the two: those wake-ups are tombstoned and pruned. It
    /// must therefore rebuild its sinks from live Keyhive state and resume at
    /// the floor (ADR 013 §9) rather than replay a history it cannot be told
    /// about, and it must not register a retention cursor below the floor,
    /// which would pin every tombstone in the scope against pruning.
    pub async fn archived_through(&self) -> Res<u64> {
        self.source.store.archived_through().await
    }
}

#[async_trait::async_trait]
impl<M> RevisionedStore for KeyhiveAccessRevisionStore<M>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'static,
{
    type Revision = u64;
    type Entry = KeyhiveAccessDelta;
    type Selector = KeyhiveAccessSelector<M>;
    type Error = eyre::Report;
    type Reader<'a>
        = KeyhiveAccessReader<M>
    where
        Self: 'a;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.source.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: u64,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        match selector.watch {
            AccessSubjectSet::All => {}
            AccessSubjectSet::Watched(_) => {
                todo!("ADR 013 §6: Watched subject sets land with the first O(k) consumer")
            }
        }
        Ok(KeyhiveAccessReader {
            store: self.source.clone(),
            reader: self.source.open((), after).await?,
            keyhive: self.keyhive.clone(),
            memory: selector.memory,
        })
    }
}

/// One open reader over the admission log's subject-bearing events.
pub struct KeyhiveAccessReader<M> {
    /// The source, held only to observe the admission head for
    /// [`KeyhiveAccessDelta::computed_at_seq`].
    store: keyhive_admission::Store,
    reader: keyhive_admission::Reader,
    keyhive: BigKeyhiveHandle,
    /// Held for the `Watched` shape, which is `todo!()`: under `All` no closure
    /// cache is consulted, and that is what the spy test asserts. The `Watched`
    /// lane reads this field, and removes the `expect` below when it does.
    #[expect(
        dead_code,
        reason = "read only by the Watched path, which is a todo!()"
    )]
    memory: M,
}

impl<M> KeyhiveAccessReader<M> {
    /// The kind of membered graph `id` names in this hive, if any.
    ///
    /// Document first, then group, matching
    /// [`BigKeyhiveHandle::agents_for_membered`], so the kind and the closure
    /// come from the same resolution order. Keyhive's own dispatch probes
    /// groups first; the two can only disagree on an id that is both a document
    /// and a group, which one keypair cannot produce.
    async fn access_subject(&self, id: Identifier) -> Res<Option<AccessSubject>> {
        if self.keyhive.has_document(id).await {
            return Ok(Some(AccessSubject::Document(id)));
        }
        if self.keyhive.get_group(KhGroupId::from(id)).await.is_some() {
            return Ok(Some(AccessSubject::Group(id)));
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl<M> RevisionedStoreReader<u64, KeyhiveAccessDelta, eyre::Report> for KeyhiveAccessReader<M>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo,
{
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, KeyhiveAccessDelta>, eyre::Report> {
        // `limits` bounds the *source* rows read, not the entries emitted: a
        // page whose rows name no subject is returned as an entry-less
        // revision and the walker settles it without a task.
        let (revision, rows) = match self.reader.next(limits).await? {
            RevisionRead::ReplayComplete { through } => {
                return Ok(RevisionRead::ReplayComplete { through });
            }
            RevisionRead::Entries { revision, entries } => (revision, entries),
        };
        if rows.is_empty() {
            return Ok(RevisionRead::Entries {
                revision,
                entries: Vec::new(),
            });
        }
        // Observed before the closures are read, so every entry's closure is at
        // or ahead of the seq it carries.
        let computed_at_seq = self.store.latest_revision().await?;
        let mut entries = Vec::new();
        for row in rows {
            let event: StaticEvent<Vec<u8>> = bincode::deserialize(&row.bytes)
                .map_err(|err| ferr!("admitted Keyhive event decode failed: {err}"))?;
            let Some(id) = self.keyhive.event_subject_id(event).await? else {
                continue;
            };
            let Some(subject) = self.access_subject(id).await? else {
                continue;
            };
            entries.push(KeyhiveAccessDelta {
                subject,
                agents: self.keyhive.agents_for_membered(id).await,
                computed_at_seq,
            });
        }
        Ok(RevisionRead::Entries { revision, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyhive::BigKeyhiveAgent;
    use crate::keyhive_listener::BigRepoKeyhiveListener;
    use crate::keyhive_storage::BigRepoKeyhiveStorage;
    use big_sync::delta_walker_state::{
        SqliteDeltaWalkerStateRepo, SqliteDeltaWalkerStateTransaction,
    };
    use big_sync_core::delta_walker_state::{DeltaWalkerProgress, DeltaWalkerStateResult};
    use big_sync_core::revisioned_store::contract::{
        RevisionedStoreContractHarness, assert_revisioned_store_contract,
    };
    use keyhive_core::event::Event;
    use keyhive_core::principal::group::delegation::StaticDelegation;
    use keyhive_core::principal::membered::Membered;
    use keyhive_crypto::signed::Signed;
    use keyhive_crypto::verifiable::Verifiable;
    use sqlx_utils_rs::SqlCtx;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use subduction_keyhive::storage::StorageHash;

    const SCOPE: &str = "keyhive-access-stream";

    /// A Keyhive hive plus an empty admission log, with no runtime attached:
    /// every admission row is written by the test, so a replay is exact and no
    /// worker can react to a fixture.
    struct Harness {
        _sql: SqlCtx,
        store: SqliteBigRepoStore,
        keyhive: BigKeyhiveHandle,
        local_agent: Identifier,
        memory: SpyMemory,
        admissions: AtomicU64,
    }

    impl Harness {
        async fn new() -> Self {
            let sql = SqlCtx::memory().await.expect("create sqlite database");
            let store =
                SqliteBigRepoStore::new(sql.clone(), SCOPE, big_sync_core::BuckId::MAX_LEVEL)
                    .await
                    .expect("create sqlite big repo store");
            let (evt_tx, _evt_rx) = async_channel::unbounded();
            let listener = BigRepoKeyhiveListener {
                evt_tx,
                storage: BigRepoKeyhiveStorage::memory(),
            };
            let keyhive = BigKeyhiveHandle::new([7; 32], listener)
                .await
                .expect("boot keyhive handle");
            let local_agent: Identifier = keyhive.local_individual_id().await.into();
            let memory = SpyMemory {
                reads: Arc::new(AtomicUsize::new(0)),
                inner: state_repo(&sql, "spy").await,
            };
            Self {
                _sql: sql,
                store,
                keyhive,
                local_agent,
                memory,
                admissions: AtomicU64::new(1),
            }
        }

        fn stream(&self) -> KeyhiveAccessRevisionStore<SpyMemory> {
            KeyhiveAccessRevisionStore::over(self.store.clone(), self.keyhive.clone())
        }

        /// A group rooted at its own key, plus one delegation *on* it signed by
        /// a non-root member: the fixture whose subject is the proof chain's
        /// root and not the signer. Returns the subject and the wire bytes —
        /// the `StaticEvent` form Keyhive persists for this delegation
        /// (`persist_delegation`), built from the delegation the hive applied.
        async fn group_with_member_delegation(
            &self,
            member: &BigKeyhiveAgent,
        ) -> (Identifier, Vec<u8>) {
            let hive = self.keyhive.clone_keyhive();
            let group = hive.generate_group(vec![]).await.expect("generate group");
            let group_identifier: Identifier = group.lock().await.group_id().into();
            let update = hive
                .add_member_with_manual_content(
                    member.clone(),
                    &Membered::Group(KhGroupId::from(group_identifier), Arc::clone(&group)),
                    Access::Read,
                    BTreeMap::new(),
                )
                .await
                .expect("add member");
            let event: StaticEvent<Vec<u8>> = Event::Delegated(update.delegation).into();
            let bytes = bincode::serialize(&event).expect("serialize delegation event");
            (group_identifier, bytes)
        }

        /// A group whose agent can be added elsewhere, with its identifier.
        async fn member_group(&self) -> (Identifier, BigKeyhiveAgent) {
            let hive = self.keyhive.clone_keyhive();
            let group = hive.generate_group(vec![]).await.expect("generate group");
            let identifier: Identifier = group.lock().await.group_id().into();
            let agent = hive
                .get_agent(identifier)
                .await
                .expect("a group is its own agent");
            (identifier, agent)
        }

        /// A delegation event whose wire form names no graph this hive holds:
        /// the id names neither a document nor a group, so there is no closure
        /// to read.
        fn unknown_subject_event(&self) -> Vec<u8> {
            let stranger = Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&[9; 32]).expect("verifying key"),
            );
            let delegation = Signed::new(
                StaticDelegation::<Vec<u8>> {
                    can: Access::Read,
                    proof: None,
                    delegate: self.local_agent,
                    after_revocations: Vec::new(),
                    after_content: BTreeMap::new(),
                },
                stranger.verifying_key(),
                ed25519_dalek::Signature::from_bytes(&[0; 64]),
            );
            bincode::serialize(&StaticEvent::Delegated(delegation)).expect("serialize event")
        }

        /// Admit one event as one atomic revision, returning its seq.
        async fn admit(&self, bytes: Vec<u8>) -> u64 {
            let index = self.admissions.fetch_add(1, Ordering::SeqCst);
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&index.to_le_bytes());
            let hash = StorageHash::new(hash);
            self.store
                .save_keyhive_event(hash, bytes, None)
                .await
                .expect("save keyhive event");
            self.store
                .append_admitted_events(vec![hash], None)
                .await
                .expect("admit keyhive event")
        }
    }

    async fn state_repo(sql: &SqlCtx, consumer: &str) -> SqliteDeltaWalkerStateRepo {
        SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            "keyhive-access-test",
            consumer,
        )
        .await
        .expect("create delta walker state repo")
    }

    /// Counting spy for the consumer's walker state.
    ///
    /// `All` has no affected test, so it consults no closure cache: every read
    /// this counts is a bug. Reads are delegated rather than panicked so the
    /// assertion is a number, not an absence of panic.
    #[derive(Clone)]
    struct SpyMemory {
        reads: Arc<AtomicUsize>,
        inner: SqliteDeltaWalkerStateRepo,
    }

    impl SpyMemory {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateRepo for SpyMemory {
        type Context<'a>
            = <SqliteDeltaWalkerStateRepo as DeltaWalkerStateRepo>::Context<'a>
        where
            Self: 'a;
        type Transaction<'a>
            = SqliteDeltaWalkerStateTransaction<'a>
        where
            Self: 'a;

        async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.progress().await
        }

        async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.begin().await
        }

        async fn begin_with_context<'a>(
            &'a self,
            context: Self::Context<'a>,
        ) -> DeltaWalkerStateResult<Self::Transaction<'a>>
        where
            Self: 'a,
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.inner.begin_with_context(context))
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerSparseStateRepo for SpyMemory {
        async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key).await
        }

        async fn get_many(
            &self,
            keys: &[Vec<u8>],
        ) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_many(keys).await
        }
    }

    fn all_selector(memory: &SpyMemory) -> KeyhiveAccessSelector<SpyMemory> {
        KeyhiveAccessSelector {
            watch: AccessSubjectSet::All,
            memory: memory.clone(),
        }
    }

    /// Read one page of a reader that has already crossed its replay boundary.
    async fn page(reader: &mut KeyhiveAccessReader<SpyMemory>) -> Vec<KeyhiveAccessDelta> {
        let mut entries = Vec::new();
        loop {
            match reader
                .next(RevisionReadLimits::default())
                .await
                .expect("reader must not fail")
            {
                RevisionRead::Entries { entries: page, .. } => entries.extend(page),
                RevisionRead::ReplayComplete { .. } => return entries,
            }
        }
    }

    #[tokio::test]
    async fn prekey_events_name_no_subject() {
        let harness = Harness::new().await;
        let expanded = harness
            .keyhive
            .expand_prekeys()
            .await
            .expect("expand prekeys");
        assert_eq!(
            harness
                .keyhive
                .event_subject_id(StaticEvent::PrekeysExpanded(Box::new((*expanded).clone())))
                .await
                .expect("resolve prekey expansion"),
            None,
            "an expanded prekey changes reachability, not membership"
        );

        let rotated_key = harness
            .keyhive
            .prekeys()
            .await
            .into_iter()
            .next()
            .expect("a published prekey");
        let rotated = harness
            .keyhive
            .rotate_prekey(rotated_key)
            .await
            .expect("rotate prekey");
        assert_eq!(
            harness
                .keyhive
                .event_subject_id(StaticEvent::PrekeyRotated(Box::new((*rotated).clone())))
                .await
                .expect("resolve prekey rotation"),
            None,
            "a rotated prekey changes reachability, not membership"
        );
    }

    #[tokio::test]
    async fn delegation_subject_is_the_proof_chain_root_not_the_signer() {
        let harness = Harness::new().await;
        let (_, member) = harness.member_group().await;
        let (subject, bytes) = harness.group_with_member_delegation(&member).await;

        let event: StaticEvent<Vec<u8>> =
            bincode::deserialize(&bytes).expect("decode delegation event");
        let StaticEvent::Delegated(delegation) = &event else {
            panic!("expected a delegation event");
        };
        let signer: Identifier = delegation.issuer.into();
        assert_ne!(
            signer, subject,
            "the fixture has to re-delegate from a non-root member"
        );
        assert_eq!(
            harness
                .keyhive
                .event_subject_id(event)
                .await
                .expect("resolve delegation"),
            Some(subject),
            "the subject is the graph Keyhive dispatched to, not the signer"
        );
    }

    #[tokio::test]
    async fn cgka_operation_names_its_document() {
        let harness = Harness::new().await;
        let hive = harness.keyhive.clone_keyhive();
        let doc = hive
            .generate_doc(vec![], nonempty::NonEmpty::new(vec![0u8; 32]))
            .await
            .expect("generate document");
        let doc_identifier: Identifier = doc.lock().await.doc_id().into();

        // A real CGKA operation on that document: the share-key rotation the
        // causal-checkpoint path performs (`native.rs:471`), in the wire form
        // the admission log carries.
        let (operation, local_secret) = hive
            .force_pcs_update(doc)
            .await
            .expect("rotate the document's share key");
        hive.import_local_cgka_secret(local_secret)
            .await
            .expect("retain the rotated share secret");

        assert_eq!(
            harness
                .keyhive
                .event_subject_id(StaticEvent::CgkaOperation(Box::new(operation)))
                .await
                .expect("resolve cgka operation"),
            Some(doc_identifier),
            "a cgka operation names the document whose tree it edits"
        );
    }

    #[tokio::test]
    async fn events_naming_no_graph_produce_no_entry() {
        let harness = Harness::new().await;
        harness.admit(harness.unknown_subject_event()).await;
        let mut reader = harness
            .stream()
            .open(all_selector(&harness.memory), 0)
            .await
            .expect("open reader");

        assert!(
            page(&mut reader).await.is_empty(),
            "an id this hive holds no graph for has no closure to read"
        );
    }

    #[tokio::test]
    async fn all_reports_one_entry_per_subject_bearing_event_without_a_closure_cache() {
        let harness = Harness::new().await;
        let (member_identifier, member) = harness.member_group().await;
        let (first, first_bytes) = harness.group_with_member_delegation(&member).await;
        let (second, second_bytes) = harness.group_with_member_delegation(&member).await;

        // Two events on one subject are two entries: `All` has no affected test
        // to run, so there is nothing to collapse them against.
        harness.admit(first_bytes.clone()).await;
        harness.admit(first_bytes.clone()).await;
        harness.admit(second_bytes).await;
        harness.admit(harness.unknown_subject_event()).await;
        let head = harness.admit(first_bytes).await;

        let mut reader = harness
            .stream()
            .open(all_selector(&harness.memory), 0)
            .await
            .expect("open reader");
        let entries = page(&mut reader).await;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.subject)
                .collect::<Vec<_>>(),
            vec![
                AccessSubject::Group(first),
                AccessSubject::Group(first),
                AccessSubject::Group(second),
                AccessSubject::Group(first),
            ],
            "one entry per subject-bearing event, in admission order"
        );
        let closure = BTreeMap::from([
            (harness.local_agent, Access::Admin),
            (member_identifier, Access::Read),
        ]);
        assert!(
            entries.iter().all(|entry| entry.agents == closure),
            "each entry carries its subject's closure, read from Keyhive"
        );
        assert!(
            entries.iter().all(|entry| entry.computed_at_seq == head),
            "the admission head is observed before the closures are read"
        );
        assert_eq!(harness.memory.reads(), 0, "`All` consults no closure cache");
    }

    #[tokio::test]
    async fn latest_revision_is_the_admission_head() {
        let harness = Harness::new().await;
        let stream = harness.stream();
        assert_eq!(stream.latest_revision().await.expect("head"), 0);
        let seq = harness.admit(harness.unknown_subject_event()).await;
        assert_eq!(stream.latest_revision().await.expect("head"), seq);
    }

    #[tokio::test]
    #[should_panic(expected = "Watched subject sets land with the first O(k) consumer")]
    async fn watched_selectors_are_unimplemented() {
        let harness = Harness::new().await;
        // The panic comes from the `todo!()` inside `open`, so this call never
        // returns; `expect` is here because the result is `must_use`.
        harness
            .stream()
            .open(
                KeyhiveAccessSelector {
                    watch: AccessSubjectSet::Watched(BTreeSet::new()),
                    memory: harness.memory.clone(),
                },
                0,
            )
            .await
            .expect("a Watched selector must refuse to open");
    }

    #[test]
    fn memory_row_round_trips_and_compares_by_access_level() {
        let agent = Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&[1; 32]).expect("verifying key"),
        );
        let row = KeyhiveAccessMemory {
            agents: BTreeMap::from([(agent, Access::Relay)]),
        };
        let encoded = serde_json::to_vec(&row).expect("encode memory row");
        let decoded: KeyhiveAccessMemory =
            serde_json::from_slice(&encoded).expect("decode memory row");
        assert_eq!(decoded, row, "an unchanged closure map compares equal");
        assert_ne!(
            decoded,
            KeyhiveAccessMemory {
                agents: BTreeMap::from([(agent, Access::Read)]),
            },
            "a re-delegation that changes only the level is a different row"
        );
    }

    #[tokio::test]
    async fn retention_cursor_is_derived_and_monotone() {
        let harness = Harness::new().await;
        let stream = harness.stream();
        assert_eq!(
            harness.memory.inner.retention_reader_id(),
            "keyhive-access-test/spy",
            "the reader id is the consumer's walker identity, never a raw string"
        );
        for _ in 0..3 {
            harness.admit(harness.unknown_subject_event()).await;
        }
        for hash in harness.store.load_keyhive_events().await.expect("log") {
            harness
                .store
                .delete_keyhive_event(hash.0)
                .await
                .expect("tombstone event");
        }
        stream
            .note_retention(&harness.memory.inner, 3)
            .await
            .expect("register at the head");
        stream
            .note_retention(&harness.memory.inner, 0)
            .await
            .expect("a lower cursor is a no-op, not a regression");
        assert_eq!(
            harness
                .store
                .run_maintenance()
                .await
                .expect("prune admitted events"),
            3,
            "a cursor that moved back to zero would pin every row (watermark zero prunes nothing)"
        );
    }

    #[tokio::test]
    async fn a_stalled_registered_reader_holds_the_pruning_floor() {
        let harness = Harness::new().await;
        let stream = harness.stream();
        for _ in 0..3 {
            harness.admit(harness.unknown_subject_event()).await;
        }
        for hash in harness.store.load_keyhive_events().await.expect("log") {
            harness
                .store
                .delete_keyhive_event(hash.0)
                .await
                .expect("tombstone event");
        }
        let stalled = state_repo(&harness._sql, "stalled").await;
        let ahead = state_repo(&harness._sql, "ahead").await;
        stream
            .note_retention(&ahead, 3)
            .await
            .expect("register the caught-up reader");
        stream
            .note_retention(&stalled, 1)
            .await
            .expect("register the stalled reader");
        assert_eq!(
            harness
                .store
                .run_maintenance()
                .await
                .expect("prune admitted events"),
            1,
            "the minimum over registered readers is what prunes, and only one is registered here"
        );
        assert_eq!(
            harness
                .store
                .load_keyhive_events()
                .await
                .expect("log")
                .len(),
            2,
            "a stalled registered consumer pins every row above its cursor"
        );
        stream
            .note_retention(&stalled, 2)
            .await
            .expect("the stalled reader advances");
        assert_eq!(
            harness
                .store
                .run_maintenance()
                .await
                .expect("prune admitted events"),
            1,
            "the floor moves only when the registered cursor moves"
        );
        assert_eq!(
            harness
                .store
                .load_keyhive_events()
                .await
                .expect("log")
                .len(),
            1
        );
    }

    struct AccessContractHarness {
        harness: Harness,
        store: KeyhiveAccessRevisionStore<SpyMemory>,
        /// One distinct subject-naming event per index.
        fixtures: Vec<(AccessSubject, Vec<u8>)>,
    }

    impl AccessContractHarness {
        async fn new() -> Self {
            let harness = Harness::new().await;
            let (_, member) = harness.member_group().await;
            // Each call generates a fresh group, so the fixtures name four
            // distinct subjects, one per index.
            let mut fixtures = Vec::new();
            for _ in 0..4 {
                let (subject, bytes) = harness.group_with_member_delegation(&member).await;
                fixtures.push((AccessSubject::Group(subject), bytes));
            }
            let store = harness.stream();
            Self {
                harness,
                store,
                fixtures,
            }
        }
    }

    #[async_trait::async_trait]
    impl RevisionedStoreContractHarness for AccessContractHarness {
        type Store = KeyhiveAccessRevisionStore<SpyMemory>;

        fn store(&self) -> &Self::Store {
            &self.store
        }

        async fn commit(
            &self,
            entry: KeyhiveAccessDelta,
        ) -> Result<u64, <Self::Store as RevisionedStore>::Error> {
            let (_, bytes) = self
                .fixtures
                .iter()
                .find(|(subject, _)| *subject == entry.subject)
                .expect("a fixture per expected subject")
                .clone();
            Ok(self.harness.admit(bytes).await)
        }

        fn entry(&self, index: u64) -> KeyhiveAccessDelta {
            let (subject, _) = &self.fixtures[usize::try_from(index).expect("small index")];
            KeyhiveAccessDelta {
                subject: *subject,
                agents: BTreeMap::new(),
                computed_at_seq: 0,
            }
        }

        /// The reader computes the closure and the admission head from the hive;
        /// the harness's expected rows carry the entry's key identity only. The
        /// stream contract is about which revisions carry which keys.
        fn entries_match(
            &self,
            expected: &KeyhiveAccessDelta,
            actual: &KeyhiveAccessDelta,
        ) -> bool {
            expected.subject == actual.subject
        }

        fn all_selector(&self, _after: u64) -> KeyhiveAccessSelector<SpyMemory> {
            all_selector(&self.harness.memory)
        }
    }

    #[tokio::test]
    async fn sqlite_keyhive_access_revisioned_store_contract() {
        assert_revisioned_store_contract(&AccessContractHarness::new().await).await;
    }
}
