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
        MemoryBackend::new(
            PeerKey::new(&[1]),
            MemoryPartStore::default(),
            MemoryPartStore::default(),
        )
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

    async fn member_added(&self, backend: &MemoryBackend, obj: &ObjKey, part: &PartKey) -> Res<()> {
        backend.member_added(obj, part).await
    }

    async fn member_removed(
        &self,
        backend: &MemoryBackend,
        obj: &ObjKey,
        part: &PartKey,
    ) -> Res<()> {
        backend.member_removed(obj, part).await
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

    async fn member_added(&self, backend: &MemoryBackend, obj: &ObjKey, part: &PartKey) -> Res<()> {
        backend.member_added(obj, part).await
    }

    async fn member_removed(
        &self,
        backend: &MemoryBackend,
        obj: &ObjKey,
        part: &PartKey,
    ) -> Res<()> {
        backend.member_removed(obj, part).await
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

/// The held wants' bounds reach the memory backend: a peer's notifications of objects that are
/// not members are parked up to the per-peer cap, oldest dropped first, and expire with the
/// TTL; only what is still parked is fetched when the membership arrives.
#[tokio::test]
async fn the_parked_notifications_of_a_peer_are_capped_and_expire() -> Res<()> {
    use crate::driver::ObjJob;
    use crate::driver::Settle;
    use crate::driver::SyncBackend;
    use crate::durable_outbox::HeldConfig;
    use crate::part_store::laws::obj;
    use crate::part_store::laws::part;

    let source = MemoryPartStore::default();
    let peer = PeerKey::new(&[1]);
    let backend = MemoryBackend::new(peer.clone(), MemoryPartStore::default(), source.clone())
        .with_held_config(HeldConfig {
            max_per_peer: 2,
            ttl: Duration::from_secs(30),
        });
    let notify = async |n: u8| -> Res<()> {
        let mut tx = source.begin();
        stage(
            &source,
            &mut tx,
            PartWrite::SetPayloads(vec![(obj(n), LwwPayload::at(n))]),
        )
        .await?;
        tx.commit();
        let job = ObjJob {
            obj: obj(n),
            add: vec![part(1)],
            remove: vec![],
            payload: Some(LwwPayload::at(n)),
        };
        assert_eq!(
            backend.record(job).await?,
            Settle::Done,
            "a parked job is done"
        );
        backend.advance_clock(Duration::from_secs(10));
        Ok(())
    };
    let payload_of = async |n: u8| -> Res<Option<ObjPayload>> {
        Ok(observe(&backend, &obj(n))
            .await?
            .and_then(|state| state.content.map(|(p, _)| p)))
    };

    for n in 1..=3 {
        notify(n).await?;
    }
    assert_eq!(backend.parked_by(&peer).await?, 2, "the cap");
    // object 1 was the oldest and was dropped: nothing is fetched for it
    backend.member_added(&obj(1), &part(1)).await?;
    assert_eq!(payload_of(1).await?, None, "a dropped want was fetched");
    backend.member_added(&obj(2), &part(1)).await?;
    assert_eq!(payload_of(2).await?, Some(LwwPayload::at(2)));
    // object 3 was first seen at 20s and lives 30s: the notification at 50s expires it
    backend.advance_clock(Duration::from_secs(20));
    notify(4).await?;
    assert_eq!(
        backend.parked_by(&peer).await?,
        1,
        "object 3 outlived its ttl"
    );
    backend.member_added(&obj(3), &part(1)).await?;
    assert_eq!(payload_of(3).await?, None, "an expired want was fetched");
    backend.member_added(&obj(4), &part(1)).await?;
    assert_eq!(payload_of(4).await?, Some(LwwPayload::at(4)));
    Ok(())
}
