//! In-memory implementations of the driver's two embedder traits, for tests (here and in the
//! bindings' end-to-end tests).

use crate::interlude::*;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;

use super::ObjJob;
use super::PeerCursors;
use super::Settle;
use super::SyncBackend;
use crate::durable_outbox::Advert;
use crate::durable_outbox::HeldConfig;
use crate::durable_outbox::HeldTable;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;
use crate::part_store::ObjPayload;
use crate::part_store::PartStoreRead;
use crate::part_store::laws;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::sync::PeerCursor;

#[derive(Default)]
struct Table {
    cursors: BTreeMap<(PeerKey, String, PartKey), PeerCursor>,
    /// Every entry ever written, in the order received.
    log: Vec<(PartKey, PeerCursor)>,
    /// While set, every read or write fails (a table the driver must not outlive).
    failing: Option<Failing>,
}

/// Which calls of a [`MemoryPeerCursors`] fail.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Failing {
    Reads,
    Writes,
}

/// A [`PeerCursors`] in memory. Cloning shares the table, so a test keeps a handle to read
/// back what the driver wrote and to start a driver again over the same cursors.
#[derive(Clone, Default)]
pub struct MemoryPeerCursors {
    table: Arc<Mutex<Table>>,
}

impl MemoryPeerCursors {
    /// Every entry written, in the order the driver issued it: what law 5 is judged on. The
    /// stored cursors alone would hide a write that went backwards and came forward again.
    pub fn written(&self) -> Vec<(PartKey, PeerCursor)> {
        self.table.lock().expect("table lock poisoned").log.clone()
    }

    /// The stored cursor of `part`.
    pub fn get(&self, peer: &PeerKey, backend: &str, part: &PartKey) -> Option<PeerCursor> {
        self.table
            .lock()
            .expect("table lock poisoned")
            .cursors
            .get(&(peer.clone(), backend.to_owned(), part.clone()))
            .copied()
    }

    /// From now on every call of that kind fails, the way a broken disk would. The driver's
    /// law 9: that is a bug and crashes it.
    pub fn fail(&self, failing: Failing) {
        self.table.lock().expect("table lock poisoned").failing = Some(failing);
    }
}

impl PeerCursors for MemoryPeerCursors {
    async fn read(
        &self,
        peer: &PeerKey,
        backend: &str,
        parts: &[PartKey],
    ) -> Res<Vec<Option<PeerCursor>>> {
        if self.table.lock().expect("table lock poisoned").failing == Some(Failing::Reads) {
            eyre::bail!("the table was told to fail its reads");
        }
        Ok(parts
            .iter()
            .map(|part| self.get(peer, backend, part))
            .collect())
    }

    async fn write(
        &self,
        peer: &PeerKey,
        backend: &str,
        writes: &[(PartKey, PeerCursor)],
    ) -> Res<()> {
        let mut table = self.table.lock().expect("table lock poisoned");
        if table.failing == Some(Failing::Writes) {
            eyre::bail!("the table was told to fail its writes");
        }
        for (part, cursor) in writes {
            table.log.push((part.clone(), *cursor));
            table
                .cursors
                .insert((peer.clone(), backend.to_owned(), part.clone()), *cursor);
        }
        Ok(())
    }
}

/// The payload a [`MemoryBackend`] can merge: a last-writer-wins register. Two nodes that wrote
/// different payloads for one object each keep the one with the greater `(written_at,
/// writer_id)`, so both settle on it however the exchange interleaves.
///
/// Nothing outside the memory backend knows this shape: the core types carry any JSON as the
/// payload, and a real backend merges in its own way (automerge documents, content-addressed
/// blobs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwPayload {
    pub value: serde_json::Value,
    #[serde(rename = "writtenAt")]
    pub written_at: u64,
    #[serde(rename = "writerId")]
    pub writer_id: PeerKey,
}

impl LwwPayload {
    pub fn into_payload(self) -> ObjPayload {
        serde_json::to_value(self).expect(ERROR_JSON)
    }

    /// The stamped payload of `value`, written by one fixed writer at time `value`: a larger
    /// value is a later write, and equal stamps always carry equal values.
    pub fn at(value: u8) -> ObjPayload {
        Self {
            value: serde_json::json!(value),
            written_at: u64::from(value),
            writer_id: PeerKey::new(&[0]),
        }
        .into_payload()
    }

    /// A batch setting each `(obj, value)`'s object to [`Self::at`] `value`: what a peer's
    /// store holds for a [`MemoryBackend`] to take.
    pub fn set(payloads: &[(u8, u8)]) -> PartWrite {
        PartWrite::SetPayloads(
            payloads
                .iter()
                .map(|&(obj, value)| (laws::obj(obj), Self::at(value)))
                .collect(),
        )
    }

    /// Whether a peer's `incoming` payload replaces the `held` one.
    ///
    /// The two are ordered by `written_at`, then `writer_id`, and `incoming` wins only when
    /// greater: applying it is idempotent, and applying two payloads in either order leaves
    /// the greater, so concurrent writes converge. Equal metadata must carry equal values (one
    /// writer does not stamp two payloads alike), which is asserted. A payload that is not an
    /// [`LwwPayload`] panics: the memory backend is a test backend that merges only what it
    /// can order, and an unstamped payload would have to overwrite, which does not converge.
    pub fn wins(incoming: &ObjPayload, held: &ObjPayload) -> bool {
        let stamped = |payload| {
            Self::deserialize(payload).unwrap_or_else(|err| {
                panic!("the memory backend takes only LwwPayloads, got {payload}: {err}")
            })
        };
        let (incoming, held) = (stamped(incoming), stamped(held));
        match (incoming.written_at, &incoming.writer_id).cmp(&(held.written_at, &held.writer_id)) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => {
                assert_eq!(
                    incoming.value, held.value,
                    "equal LWW metadata must not diverge in payload value"
                );
                false
            }
        }
    }
}

/// Where a [`MemoryBackend`] gets an object's payload.
enum Payloads {
    /// From another store that stands in for the peer, on `fetch`.
    Download(MemoryPartStore),
    /// From the job's own hint, on `record`: all the bytes there are when no peer store exists
    /// (a browser page).
    Hint,
}

type HeldWants = HeldTable<MemoryOkv<Vec<u8>, Vec<u8>>>;

/// What the backends of one node share: its store, its held wants, its clock and the stores
/// that stand in for its peers.
struct Node {
    local: MemoryPartStore,
    db: MemoryDb,
    held: HeldWants,
    /// The stores a parked want is fetched from once its object is a member, by peer.
    sources: Mutex<BTreeMap<PeerKey, MemoryPartStore>>,
    /// The clock that stamps held wants, in milliseconds: moved by the test alone.
    now_ms: AtomicU64,
}

/// A [`SyncBackend`] over a local [`MemoryPartStore`], in one of two modes. A backend serves
/// one peer, the provider of what it parks.
///
/// **Membership is the backend's own** (see [`SyncBackend`]): `record` never touches it, and
/// the test stands in for the node's membership source by calling
/// [`member_added`](Self::member_added) and [`member_removed`](Self::member_removed), the
/// simulated local keyhive stream.
///
/// [`new`](Self::new) **downloads**: `record` answers `Fetch` for a job that needs a payload
/// and names a part the object is a member of, `Done` for one that needs none; a job whose
/// object is a member of none of its parts is parked in the held wants and answers `Done` (a
/// notification before membership). `fetch` copies the payload from a second store standing
/// in for the peer, and errors when that store no longer has it (the peer dropped the
/// content). [`member_added`](Self::member_added) fetches what was parked.
///
/// [`hinted`](Self::hinted) **takes the payload from the job**: `record` merges the job's
/// payload hint and answers `Done`, whether or not the object is a member (the bytes are
/// here; a payload is not membership, and a partless payload is the store's own to collect);
/// `fetch` has nothing to do, and nothing is ever parked.
///
/// Both merge a peer's payload instead of overwriting with it: see [`LwwPayload`].
///
/// Both run [`backend_laws`](super::backend_laws).
pub struct MemoryBackend {
    node: Arc<Node>,
    peer: PeerKey,
    payloads: Payloads,
    fail_next_write: AtomicBool,
}

impl MemoryBackend {
    /// A backend of the node whose store is `local`, pulling from the peer `peer`, whose store
    /// is `source`.
    pub fn new(peer: PeerKey, local: MemoryPartStore, source: MemoryPartStore) -> Self {
        let db = MemoryDb::default();
        let node = Node {
            held: HeldTable::new(db.table(), HeldConfig::default()),
            sources: Mutex::new([(peer.clone(), source.clone())].into()),
            local,
            db,
            now_ms: AtomicU64::new(0),
        };
        Self {
            node: Arc::new(node),
            peer,
            payloads: Payloads::Download(source),
            fail_next_write: AtomicBool::new(false),
        }
    }

    pub fn hinted(local: MemoryPartStore) -> Self {
        let db = MemoryDb::default();
        let node = Node {
            held: HeldTable::new(db.table(), HeldConfig::default()),
            sources: default(),
            local,
            db,
            now_ms: AtomicU64::new(0),
        };
        Self {
            node: Arc::new(node),
            peer: PeerKey::new(&[]),
            payloads: Payloads::Hint,
            fail_next_write: AtomicBool::new(false),
        }
    }

    /// Bound the held wants. Called before the backend is shared with a sibling.
    pub fn with_held_config(mut self, config: HeldConfig) -> Self {
        let node = Arc::get_mut(&mut self.node).expect("configured before it is shared");
        node.held = HeldTable::new(node.db.table(), config);
        self
    }

    /// A backend of the same node for another peer, `peer`, whose store is `source`: one
    /// store, one set of held wants and one clock, so a want advertised by either peer is
    /// released by one membership.
    pub fn sibling(&self, peer: PeerKey, source: MemoryPartStore) -> Self {
        self.node
            .sources
            .lock()
            .expect("sources lock poisoned")
            .insert(peer.clone(), source.clone());
        Self {
            node: Arc::clone(&self.node),
            peer,
            payloads: Payloads::Download(source),
            fail_next_write: AtomicBool::new(false),
        }
    }

    pub fn local(&self) -> &MemoryPartStore {
        &self.node.local
    }

    /// The store standing in for the peer; `None` for a [`hinted`](Self::hinted) backend.
    pub fn source(&self) -> Option<&MemoryPartStore> {
        match &self.payloads {
            Payloads::Download(source) => Some(source),
            Payloads::Hint => None,
        }
    }

    /// The next write fails after staging everything, before it commits: a transaction that
    /// is atomic leaves the store as it was.
    pub fn fail_next_write(&self) {
        self.fail_next_write.store(true, AtomicOrdering::SeqCst);
    }

    /// Move the clock that stamps held wants (their first-seen time, and so their TTL).
    pub fn advance_clock(&self, by: Duration) {
        let by = u64::try_from(by.as_millis()).expect("a short step");
        self.node.now_ms.fetch_add(by, AtomicOrdering::SeqCst);
    }

    /// How many wants `peer` has parked.
    pub async fn parked_by(&self, peer: &PeerKey) -> Res<usize> {
        self.node.held.held_by(peer).await
    }

    /// The membership source says `obj` is now a member of `part`; a notification that was
    /// parked for the object is fetched from the peers that advertised it (law 9).
    ///
    /// The want is released only once its payload is durable: when the peer is gone, the
    /// error reaches the source, which retries, and the want stays parked. A want no known
    /// peer advertised stays parked too.
    pub async fn member_added(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::AddMembers(vec![(
            obj.clone(),
            part.clone(),
        )])])
        .await?;
        self.fetch_parked(obj).await
    }

    /// The membership source says `obj` is no longer a member of `part`.
    pub async fn member_removed(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::RemoveMembers(vec![(
            obj.clone(),
            part.clone(),
        )])])
        .await
    }

    /// Fetch the want parked for `obj` from its providers, in order, and release it.
    async fn fetch_parked(&self, obj: &ObjKey) -> Res<()> {
        let held = &self.node.held;
        let Some(want) = held.get(&mut self.node.db.begin(), obj).await? else {
            return Ok(());
        };
        let sources = want
            .providers
            .iter()
            .filter_map(|provider| {
                self.node
                    .sources
                    .lock()
                    .expect("sources lock poisoned")
                    .get(provider)
                    .cloned()
            })
            .collect::<Vec<_>>();
        // Every provider's payload is merged, not the first that answers: the want says the
        // object changed, and a provider that is behind another must not hide the newer
        // payload the other advertised (the payloads merge, so the order is free). One that
        // fails leaves the want parked for a retry.
        for source in sources {
            self.download(&source, obj).await?;
        }
        let mut tx = self.node.db.begin();
        held.release(&mut tx, obj).await?;
        tx.commit()?;
        Ok(())
    }

    /// Whether `obj` is a member of any of `parts`, pending memberships included.
    async fn is_member(&self, obj: &ObjKey, parts: &[PartKey]) -> Res<bool> {
        Ok(self
            .node
            .local
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .is_some_and(|state| parts.iter().any(|part| state.parts.contains(part))))
    }

    /// Park a notification for an object that is not a member (law 9).
    async fn park(&self, job: &ObjJob) -> Res<()> {
        // What a parked want is for is the object's current payload, whichever peer holds it:
        // one identity for every advert of an object, so the providers union.
        let advert = Advert::new(self.peer.clone(), 0, ByteKey::new(&[]));
        let now_ms = self.node.now_ms.load(AtomicOrdering::SeqCst);
        let mut tx = self.node.db.begin();
        let held = self
            .node
            .held
            .hold(&mut tx, &job.obj, advert, now_ms)
            .await?;
        tx.commit()?;
        if !held.dropped.is_empty() {
            tracing::debug!(dropped = ?held.dropped, "held wants were dropped");
        }
        Ok(())
    }

    /// Whether a peer's payload for `obj` replaces what the backend holds: when it holds none,
    /// or the peer's wins under [`LwwPayload`]'s order.
    async fn adopts(&self, obj: &ObjKey, incoming: &ObjPayload) -> Res<bool> {
        let held = self
            .node
            .local
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .and_then(|state| state.content);
        Ok(held.is_none_or(|(held, _)| LwwPayload::wins(incoming, &held)))
    }

    async fn write(&self, writes: Vec<PartWrite>) -> Res<()> {
        let local = &self.node.local;
        let mut tx = local.begin();
        for write in writes {
            laws::stage(local, &mut tx, write).await?;
        }
        if self.fail_next_write.swap(false, AtomicOrdering::SeqCst) {
            // dropped uncommitted: rolled back
            eyre::bail!("the write was told to fail");
        }
        tx.commit();
        Ok(())
    }

    /// Copy `obj`'s payload from `source`, merging.
    async fn download(&self, source: &MemoryPartStore, obj: &ObjKey) -> Res<()> {
        let state = source
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0);
        let Some((payload, _)) = state.and_then(|state| state.content) else {
            eyre::bail!("the peer no longer holds the payload of {obj:?}");
        };
        if !self.adopts(obj, &payload).await? {
            return Ok(());
        }
        self.write(vec![PartWrite::SetPayloads(vec![(obj.clone(), payload)])])
            .await
    }
}

impl SyncBackend for MemoryBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        // The job's parts are a notification: nothing below changes membership (law 8).
        if let (Payloads::Hint, Some(payload)) = (&self.payloads, &job.payload)
            && self.adopts(&job.obj, payload).await?
        {
            self.write(vec![PartWrite::SetPayloads(vec![(
                job.obj.clone(),
                payload.clone(),
            )])])
            .await?;
        }
        if matches!(self.payloads, Payloads::Hint) {
            return Ok(Settle::Done);
        }
        if job.add.is_empty() {
            // a removal moves no payload; a job with no parts at all is content
            return Ok(if job.remove.is_empty() {
                Settle::Fetch
            } else {
                Settle::Done
            });
        }
        if self.is_member(&job.obj, &job.add).await? {
            return Ok(Settle::Fetch);
        }
        self.park(&job).await?;
        // The source may have added the object between the check and the park, and its hook
        // found nothing to release: look again, now that the want is visible to it.
        if self.is_member(&job.obj, &job.add).await? {
            self.fetch_parked(&job.obj).await?;
        }
        Ok(Settle::Done)
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        let Payloads::Download(source) = &self.payloads else {
            return Ok(());
        };
        if job.add.is_empty() && !job.remove.is_empty() {
            return Ok(());
        }
        self.download(source, &job.obj).await
    }
}
