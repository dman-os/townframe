//! The memory backend, in both its modes, obeys the laws every `SyncBackend` runs.

use crate::driver::backend_laws::BackendRig;
use crate::driver::backend_laws::sync_backend_laws;
use crate::driver::memory::LwwPayload;
use crate::driver::memory::MemoryBackend;
use crate::interlude::*;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::laws::stage;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::rev::Rev;

/// Memory backends share everything but where the payload comes from.
async fn observe(backend: &MemoryBackend, obj: &ObjKey) -> Res<Option<ObjState>> {
    Ok(backend
        .local()
        .obj_states(std::slice::from_ref(obj))
        .await?
        .remove(0))
}

/// Two writes of one object at the same time: the same `written_at`, told apart by the writer
/// alone, which is the order the memory backend breaks the tie by.
fn concurrent_payloads() -> (ObjPayload, ObjPayload) {
    let write = |value: u8, writer: u8| {
        LwwPayload {
            value: serde_json::json!(value),
            written_at: 5,
            writer_id: PeerKey::new(&[writer]),
        }
        .into_payload()
    };
    (write(1, 1), write(2, 2))
}

/// Downloads from a second store standing in for the peer.
struct Download;

impl BackendRig for Download {
    type Backend = MemoryBackend;
    type Stamp = Rev;
    const DOWNLOADS: bool = true;

    fn fresh(&self) -> MemoryBackend {
        MemoryBackend::new(MemoryPartStore::default(), MemoryPartStore::default())
    }

    async fn peer_holds(
        &self,
        backend: &MemoryBackend,
        obj: &ObjKey,
        payload: Option<&ObjPayload>,
    ) -> Res<()> {
        let source = backend.source().expect("a downloading backend");
        let mut tx = source.begin();
        let write = match payload {
            Some(payload) => PartWrite::SetPayloads(vec![(obj.clone(), payload.clone())]),
            None => PartWrite::DropPayloads(vec![obj.clone()]),
        };
        stage(source, &mut tx, write).await?;
        tx.commit();
        Ok(())
    }

    async fn observe(&self, backend: &MemoryBackend, obj: &ObjKey) -> Res<Option<ObjState>> {
        observe(backend, obj).await
    }

    async fn stamp(&self, backend: &MemoryBackend) -> Res<Rev> {
        backend.local().latest_rev().await
    }

    fn fail_next_write(&self, backend: &MemoryBackend) {
        backend.fail_next_write();
    }

    fn payload(value: u8) -> ObjPayload {
        LwwPayload::at(value)
    }

    fn concurrent_payloads(&self) -> (ObjPayload, ObjPayload) {
        concurrent_payloads()
    }
}

/// Takes the payload from the job: the peer holds nothing, and holds it nowhere.
struct Hinted;

impl BackendRig for Hinted {
    type Backend = MemoryBackend;
    type Stamp = Rev;
    const DOWNLOADS: bool = false;

    fn fresh(&self) -> MemoryBackend {
        MemoryBackend::hinted(MemoryPartStore::default())
    }

    async fn peer_holds(&self, _: &MemoryBackend, _: &ObjKey, _: Option<&ObjPayload>) -> Res<()> {
        Ok(())
    }

    async fn observe(&self, backend: &MemoryBackend, obj: &ObjKey) -> Res<Option<ObjState>> {
        observe(backend, obj).await
    }

    async fn stamp(&self, backend: &MemoryBackend) -> Res<Rev> {
        backend.local().latest_rev().await
    }

    fn fail_next_write(&self, backend: &MemoryBackend) {
        backend.fail_next_write();
    }

    fn payload(value: u8) -> ObjPayload {
        LwwPayload::at(value)
    }

    fn concurrent_payloads(&self) -> (ObjPayload, ObjPayload) {
        concurrent_payloads()
    }
}

#[tokio::test]
async fn the_downloading_memory_backend_obeys_the_backend_laws() -> Res<()> {
    sync_backend_laws(&Download).await
}

#[tokio::test]
async fn the_hinted_memory_backend_obeys_the_backend_laws() -> Res<()> {
    sync_backend_laws(&Hinted).await
}
