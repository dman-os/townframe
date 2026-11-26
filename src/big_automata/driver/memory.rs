//! In-memory implementations of the driver's two embedder traits, for tests (here and in the
//! bindings' end-to-end tests).

use crate::interlude::*;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;

use super::ObjJob;
use super::PeerCursors;
use super::Settle;
use super::SyncBackend;
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

/// A [`SyncBackend`] over a local [`MemoryPartStore`], in one of two modes.
///
/// [`new`](Self::new) **downloads**: `record` applies the job's removals and adds (an add is
/// pending until the object has a payload) and answers `Done` when the job needs no payload,
/// `Fetch` otherwise; `fetch` copies the payload from a second store standing in for the peer,
/// and errors when that store no longer has it (the peer dropped the content).
///
/// [`hinted`](Self::hinted) **takes the payload from the job**: `record` applies the removals,
/// the job's payload hint and the adds in one transaction and answers `Done`; `fetch` has
/// nothing to do.
///
/// Both merge a peer's payload instead of overwriting with it: see [`LwwPayload`].
///
/// Both run [`backend_laws`](super::backend_laws).
pub struct MemoryBackend {
    local: MemoryPartStore,
    payloads: Payloads,
    fail_next_write: AtomicBool,
}

impl MemoryBackend {
    pub fn new(local: MemoryPartStore, source: MemoryPartStore) -> Self {
        Self {
            local,
            payloads: Payloads::Download(source),
            fail_next_write: AtomicBool::new(false),
        }
    }

    pub fn hinted(local: MemoryPartStore) -> Self {
        Self {
            local,
            payloads: Payloads::Hint,
            fail_next_write: AtomicBool::new(false),
        }
    }

    pub fn local(&self) -> &MemoryPartStore {
        &self.local
    }

    /// The store standing in for the peer; `None` for a [`hinted`](Self::hinted) backend.
    pub fn source(&self) -> Option<&MemoryPartStore> {
        match &self.payloads {
            Payloads::Download(source) => Some(source),
            Payloads::Hint => None,
        }
    }

    /// The membership source says `obj` is now a member of `part`.
    pub async fn member_added(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::AddMembers(vec![(obj.clone(), part.clone())])])
            .await
    }

    /// The membership source says `obj` is no longer a member of `part`.
    pub async fn member_removed(&self, obj: &ObjKey, part: &PartKey) -> Res<()> {
        self.write(vec![PartWrite::RemoveMembers(vec![(
            obj.clone(),
            part.clone(),
        )])])
        .await
    }

    /// The next write fails after staging everything, before it commits: a transaction that
    /// is atomic leaves the store as it was.
    pub fn fail_next_write(&self) {
        self.fail_next_write.store(true, AtomicOrdering::SeqCst);
    }

    /// Whether a job moves the payload: any job that adds, and a content job (no parts at
    /// all). A removal alone does not.
    fn wants_payload(job: &ObjJob) -> bool {
        !job.add.is_empty() || job.remove.is_empty()
    }

    /// Whether a peer's payload for `obj` replaces what the backend holds: when it holds none,
    /// or the peer's wins under [`LwwPayload`]'s order.
    async fn adopts(&self, obj: &ObjKey, incoming: &ObjPayload) -> Res<bool> {
        let held = self
            .local
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .and_then(|state| state.content);
        Ok(held.is_none_or(|(held, _)| LwwPayload::wins(incoming, &held)))
    }

    async fn write(&self, writes: Vec<PartWrite>) -> Res<()> {
        let mut tx = self.local.begin();
        for write in writes {
            laws::stage(&self.local, &mut tx, write).await?;
        }
        if self.fail_next_write.swap(false, AtomicOrdering::SeqCst) {
            // dropped uncommitted: rolled back
            eyre::bail!("the write was told to fail");
        }
        tx.commit();
        Ok(())
    }
}

impl SyncBackend for MemoryBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        let members = |parts: &[PartKey]| {
            parts
                .iter()
                .map(|part| (job.obj.clone(), part.clone()))
                .collect::<Vec<_>>()
        };
        let mut writes = vec![];
        if !job.remove.is_empty() {
            writes.push(PartWrite::RemoveMembers(members(&job.remove)));
        }
        if let (Payloads::Hint, Some(payload)) = (&self.payloads, &job.payload)
            && self.adopts(&job.obj, payload).await?
        {
            writes.push(PartWrite::SetPayloads(vec![(
                job.obj.clone(),
                payload.clone(),
            )]));
        }
        if !job.add.is_empty() {
            writes.push(PartWrite::AddMembers(members(&job.add)));
        }
        self.write(writes).await?;
        Ok(match self.payloads {
            Payloads::Download(_) if Self::wants_payload(&job) => Settle::Fetch,
            _ => Settle::Done,
        })
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        let Payloads::Download(source) = &self.payloads else {
            return Ok(());
        };
        if !Self::wants_payload(&job) {
            return Ok(());
        }
        let state = source
            .obj_states(std::slice::from_ref(&job.obj))
            .await?
            .remove(0);
        let Some((payload, _)) = state.and_then(|state| state.content) else {
            eyre::bail!("the peer no longer holds the payload of {:?}", job.obj);
        };
        if !self.adopts(&job.obj, &payload).await? {
            return Ok(());
        }
        self.write(vec![PartWrite::SetPayloads(vec![(job.obj, payload)])])
            .await
    }
}
