//! In-memory implementations of the driver's two embedder traits, for tests (here and in the
//! bindings' end-to-end tests).

use crate::interlude::*;

use std::collections::BTreeMap;
use std::sync::Mutex;

use super::ObjJob;
use super::PeerCursors;
use super::Settle;
use super::SyncBackend;
use crate::part_store::PartStoreRead;
use crate::part_store::laws;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::rev::Rev;

#[derive(Default)]
struct Table {
    cursors: BTreeMap<(PeerKey, String, PartKey), Rev>,
    /// Every entry ever written, in the order received.
    log: Vec<(PartKey, Rev)>,
}

/// A [`PeerCursors`] in memory. Cloning shares the table, so a test keeps a handle to read
/// back what the driver wrote and to start a driver again over the same cursors.
#[derive(Clone, Default)]
pub struct MemoryPeerCursors {
    table: Arc<Mutex<Table>>,
}

impl MemoryPeerCursors {
    /// Every entry written, in the order the driver issued it: what law 5 is judged on. The
    /// table itself ignores an entry below the stored cursor, so the stored cursors alone
    /// would hide a write that went backwards.
    pub fn written(&self) -> Vec<(PartKey, Rev)> {
        self.table.lock().expect("table lock poisoned").log.clone()
    }

    /// The stored cursor of `part`.
    pub fn get(&self, peer: &PeerKey, backend: &str, part: &PartKey) -> Rev {
        self.table
            .lock()
            .expect("table lock poisoned")
            .cursors
            .get(&(peer.clone(), backend.to_owned(), part.clone()))
            .copied()
            .unwrap_or(0)
    }
}

impl PeerCursors for MemoryPeerCursors {
    async fn read(&self, peer: &PeerKey, backend: &str, parts: &[PartKey]) -> Res<Vec<Rev>> {
        Ok(parts
            .iter()
            .map(|part| self.get(peer, backend, part))
            .collect())
    }

    async fn write(&self, peer: &PeerKey, backend: &str, writes: &[(PartKey, Rev)]) -> Res<()> {
        let mut table = self.table.lock().expect("table lock poisoned");
        for (part, cursor) in writes {
            table.log.push((part.clone(), *cursor));
            let stored = table
                .cursors
                .entry((peer.clone(), backend.to_owned(), part.clone()))
                .or_default();
            *stored = (*stored).max(*cursor);
        }
        Ok(())
    }
}

/// A [`SyncBackend`] over a local [`MemoryPartStore`] that takes object payloads from another
/// one, standing in for the bytes of a download. `record` applies the job's removals and adds
/// (an add is pending until the object has a payload) and answers `Done` when the job needs no
/// payload, `Fetch` otherwise; `fetch` copies the payload.
pub struct MemoryBackend {
    local: MemoryPartStore,
    source: MemoryPartStore,
}

impl MemoryBackend {
    pub fn new(local: MemoryPartStore, source: MemoryPartStore) -> Self {
        Self { local, source }
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
        tx.commit();
        Ok(())
    }
}

impl SyncBackend for MemoryBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        let mut writes = vec![];
        if !job.remove.is_empty() {
            writes.push(PartWrite::RemoveMembers(
                job.remove
                    .iter()
                    .map(|part| (job.obj.clone(), part.clone()))
                    .collect(),
            ));
        }
        if !job.add.is_empty() {
            writes.push(PartWrite::AddMembers(
                job.add
                    .iter()
                    .map(|part| (job.obj.clone(), part.clone()))
                    .collect(),
            ));
        }
        self.write(writes).await?;
        Ok(if Self::wants_payload(&job) {
            Settle::Fetch
        } else {
            Settle::Done
        })
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        if !Self::wants_payload(&job) {
            return Ok(());
        }
        let state = self
            .source
            .obj_states(std::slice::from_ref(&job.obj))
            .await?
            .remove(0);
        let payload = state
            .and_then(|state| state.content)
            .map(|(payload, _)| payload);
        if let Some(payload) = payload {
            self.write(vec![PartWrite::SetPayloads(vec![(job.obj, payload)])])
                .await?;
        }
        Ok(())
    }
}
