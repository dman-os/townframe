use super::*;
use crate::part_store::host_contract::SingleTargetPageStore;
use sqlite_rs::Database;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub(crate) struct Backend {
    pub store: SqlitePartStore,
    pub sql: Database,
    pub fail: AtomicBool,
    pub content_calls: AtomicUsize,
    pub id: &'static str,
}

#[async_trait]
impl ReplayReceiver for Backend {
    async fn receive_replay(
        &self,
        receipt: &ReplayReceipt,
        targets: &[ReplaySubscriptionTarget],
    ) -> Res<()> {
        assert_eq!(self.store.pending().await?, Some(receipt.clone()));
        assert_eq!(
            targets,
            receipt
                .replay
                .routes
                .iter()
                .filter(|route| route.backend == self.id)
                .map(|route| route.target.clone())
                .collect::<Vec<_>>()
        );
        for (route, predecessor, resume) in receipt.replay.bounds()? {
            assert_eq!(
                self.store
                    .progress(receipt.replay.peer_id.clone(), route.clone())
                    .await?,
                ReceiveProgress {
                    received: resume,
                    applied: predecessor
                }
            );
        }
        self.sql.execute(
            "INSERT INTO test_receive_admissions (id, replay) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
            (i64::try_from(receipt.id)?, serde_json::to_vec(receipt)?),
        ).await?;
        if self.fail.swap(false, Ordering::SeqCst) {
            eyre::bail!("injected crash after durable admission before acknowledgment");
        }
        Ok(())
    }
}

#[async_trait]
impl crate::SyncBackend for Backend {
    fn replay_receiver(&self) -> Option<&dyn ReplayReceiver> {
        Some(self)
    }

    async fn sync_obj(
        &self,
        _peer: PeerKey,
        obj_id: ObjKey,
        parts: big_sync_core::PartObservations,
        _payload: Option<crate::ObjPayload>,
    ) -> Res<crate::SyncTaskRunOutcome> {
        self.content_calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            parts.is_empty(),
            "durable backend content callbacks must not carry admission observations"
        );
        Ok(crate::SyncTaskRunOutcome::Completion(
            big_sync_core::SyncTaskCompletion {
                obj_id,
                deets: big_sync_core::SyncCompletionDeets::Noop,
            },
        ))
    }

    async fn remove_obj_from_parts(
        &self,
        _peer: PeerKey,
        _obj: ObjKey,
        _parts: big_sync_core::PartObservations,
    ) -> Res<()> {
        panic!("durable backend removals must only arrive through replay receipts")
    }
}

pub(crate) async fn backend(sql: Database, fail: bool) -> Res<Arc<Backend>> {
    sql.execute("CREATE TABLE IF NOT EXISTS test_receive_admissions (id INTEGER PRIMARY KEY, replay BLOB NOT NULL)", ()).await?;
    Ok(Arc::new(Backend {
        store: SqlitePartStore::new(sql.clone(), "receive-reopen", BuckId::MAX_LEVEL).await?,
        sql,
        fail: AtomicBool::new(fail),
        content_calls: AtomicUsize::new(0),
        id: "durable",
    }))
}

pub(crate) fn backends(
    backend: &Arc<Backend>,
) -> HashMap<crate::BackendId, Arc<dyn crate::SyncBackend>> {
    [(
        Arc::from("durable"),
        Arc::clone(backend) as Arc<dyn crate::SyncBackend>,
    )]
    .into()
}

async fn source_page(
    source: &MemoryPartStore,
    route: &ReceiveRoute,
    reader: PeerKey,
    cursor: u64,
) -> Res<ReceivedReplay> {
    let target = route.target.with_cursor(cursor);
    let page = source
        .replay_page_for_target(target.clone(), 16, reader, Duration::ZERO)
        .await?;
    Ok(ReceivedReplay {
        peer_id: PeerKey::new([3; 32]),
        requested: vec![target],
        page,
        routes: vec![route.clone()],
    })
}

#[tokio::test]
async fn receive_crash_reopen_replays_admission_and_preserves_tombstone_coverage() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("receive.sqlite");
    let source = MemoryPartStore::new();
    let part = PartKey::new([1; 32]);
    let object = ObjKey::new([2; 32]);
    let reader = crate::part_store::contract::grant_bucket_read(&source, [part.clone()]).await?;
    source
        .set_obj_payload(object.clone(), serde_json::json!("payload"))
        .await?;
    source
        .add_obj_to_parts(object.clone(), vec![part.clone()])
        .await?;
    let route = ReceiveRoute {
        backend: "durable".into(),
        target: ReplaySubscriptionTarget::Part {
            part_id: part.clone(),
        },
    };
    let first = source_page(&source, &route, reader.clone(), 0).await?;
    assert!(
        first
            .page
            .events
            .iter()
            .any(|event| matches!(event, PartEvent::Changed(changed) if changed.obj_id == object))
    );
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = backend(runtime.database(&path, &[], "migrations"), true).await?;
    let receipt = backend.store.accept(first).await?.unwrap();
    let resume = backend
        .store
        .progress(receipt.replay.peer_id.clone(), route.clone())
        .await?
        .received;
    assert!(resume > 0);
    assert!(drain(&backend.store, &backends(&backend)).await.is_err());
    assert_eq!(backend.store.pending().await?, Some(receipt.clone()));
    source
        .remove_obj_from_part(object.clone(), part.clone())
        .await?;
    let wrong = source_page(&source, &route, reader.clone(), 0).await?;
    assert!(
        !wrong
            .page
            .events
            .iter()
            .any(|event| matches!(event, PartEvent::Removed(_))),
        "negative control: replay from content-applied zero suppresses the tombstone"
    );
    drop(backend);
    drop(runtime);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = self::backend(runtime.database(path, &[], "migrations"), false).await?;
    assert_eq!(backend.store.pending().await?, Some(receipt.clone()));
    assert_eq!(
        backend
            .store
            .get_peer_part_cursor(receipt.replay.peer_id.clone(), part.clone())
            .await?,
        0
    );
    drain(&backend.store, &backends(&backend)).await?;
    assert!(backend.store.pending().await?.is_none());
    let rows = backend
        .sql
        .query("SELECT replay FROM test_receive_admissions", ())
        .await?
        .all::<(Vec<u8>,)>()?;
    assert_eq!(
        rows.len(),
        1,
        "crash redelivery must not duplicate durable admission work"
    );
    assert_eq!(
        serde_json::from_slice::<ReplayReceipt>(&rows[0].0)?,
        receipt
    );
    let next = source_page(&source, &route, reader, resume).await?;
    assert!(
        next.page
            .events
            .iter()
            .any(|event| matches!(event, PartEvent::Removed(removed) if removed.obj_id == object))
    );
    let removed = backend.store.accept(next).await?.unwrap();
    drain(&backend.store, &backends(&backend)).await?;
    assert!(removed.id > receipt.id);
    assert_eq!(
        backend
            .store
            .get_peer_part_cursor(receipt.replay.peer_id.clone(), part)
            .await?,
        0
    );
    assert_eq!(
        backend
            .store
            .progress(receipt.replay.peer_id.clone(), route)
            .await?
            .applied,
        removed.replay.bounds()?[0].2
    );
    Ok(())
}
