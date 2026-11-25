//! In-memory implementations of the driver's two embedder traits, for tests (here and in the
//! bindings' end-to-end tests).

use crate::interlude::*;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::ObjJob;
use super::PeerCursors;
use super::Settle;
use super::SyncBackend;
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

    /// The next write fails after staging everything, before it commits: a transaction that
    /// is atomic leaves the store as it was.
    pub fn fail_next_write(&self) {
        self.fail_next_write.store(true, Ordering::SeqCst);
    }

    /// Whether a job moves the payload: any job that adds, and a content job (no parts at
    /// all). A removal alone does not.
    fn wants_payload(job: &ObjJob) -> bool {
        !job.add.is_empty() || job.remove.is_empty()
    }

    async fn write(&self, writes: Vec<PartWrite>) -> Res<()> {
        let mut tx = self.local.begin();
        for write in writes {
            laws::stage(&self.local, &mut tx, write).await?;
        }
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
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
        if let (Payloads::Hint, Some(payload)) = (&self.payloads, &job.payload) {
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
        self.write(vec![PartWrite::SetPayloads(vec![(job.obj, payload)])])
            .await
    }
}
