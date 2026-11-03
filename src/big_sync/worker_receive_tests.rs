use super::*;
use crate::HostPartStore;
use crate::receive::ReceiveJournal;
use crate::receive::ReceiveProgress;
use crate::receive::ReplayReceipt;
use crate::receive::tests::recovery;
use crate::receive::tests::replay;
use big_sync_core::rpc::ReplayRequestId;
use big_sync_core::rpc::ReplaySessionId;
use big_sync_core::rpc::ReplayTargetId;
use big_sync_core::rpc::TargetVerdict;
use big_sync_core::tasks::MachineTaskResult;
use big_sync_core::tasks::decide_peer_strat::DecidePeerStrategyTask;
use big_sync_core::tasks::replay_page::ReplayPageResult;
use big_sync_core::tasks::replay_page::ReplayPageTask;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

fn result(replay: &ReceivedReplay) -> MachineTaskMsg {
    MachineTaskMsg::MachineTaskResult(MachineTaskResult {
        task_id: 1,
        deets: TaskResultDeets::ReplayPage(ReplayPageResult {
            peer_id: replay.peer_id.clone(),
            page: replay.page.clone(),
            requested: replay.requested.clone(),
            update: None,
        }),
    })
}

fn worker(backend: &Arc<recovery::Backend>) -> BigSyncWorker {
    let (sync_tx, sync_rx) = mpsc::bounded(2, "receive-sync".into(), "receive-test".into());
    let (task_tx, task_rx) = mpsc::bounded(2, "receive-tasks".into(), "receive-test".into());
    BigSyncWorker {
        cancel_token: CancellationToken::new(),
        label: "receive-test",
        scope_key: Arc::from("receive-reopen"),
        task_set: utils_rs::AbortableJoinSet::new(),
        part_store: Arc::new(backend.store.clone()),
        sync_backends: recovery::backends(backend),
        machine: BigSyncMachine::default(),
        machine_spawn_queue: default(),
        sync_spawn_queue: default(),
        host_rx: tokio::sync::mpsc::channel(2).1,
        subscription_rx: tokio::sync::mpsc::unbounded_channel().1,
        subscriptions: default(),
        sync_tx,
        sync_rx,
        task_tx,
        task_rx,
        stats_tx: tokio::sync::broadcast::channel(2).0,
        subscription_events: tokio::sync::broadcast::channel(2).0,
        full_sync_waiters: default(),
        rpc_clients: default(),
        tasks: default(),
        sync_tasks: default(),
        zombie_tasks: default(),
        peers: [(
            PeerKey::new([3; 32]),
            PeerState {
                configured_parts: default(),
                parts: [(PartKey::new([1; 32]), Arc::from("durable"))].into(),
                configured_objects: default(),
                objects: [(ObjKey::new([2; 32]), Arc::from("durable"))].into(),
            },
        )]
        .into(),
    }
}

#[tokio::test]
async fn receive_prepare_after_reopen_ignores_bucket_and_content_high_water_marks() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("receive-prepare.sqlite");
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.database(&path, &[], "migrations"), false).await?;
    let page = replay();
    backend.store.accept(page.clone()).await?.unwrap();
    crate::receive::drain(&backend.store, &recovery::backends(&backend)).await?;
    backend
        .store
        .set_peer_part_cursor(page.peer_id.clone(), PartKey::new([1; 32]), 99_000)
        .await?;
    drop(backend);
    drop(runtime);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.database(&path, &[], "migrations"), false).await?;
    let worker = worker(&backend);
    let mut strategy = MachineTask {
        id: 1,
        deets: MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask::new(
            page.peer_id.clone(),
            [PartKey::new([1; 32])].into(),
            SyncMode::Bucket,
        )),
    };
    assert!(worker.prepare_receive_task(&mut strategy).await?.is_empty());
    let MachineTaskDeets::DecidePeerStrategy(strategy) = strategy.deets else {
        unreachable!()
    };
    assert_eq!(
        strategy.received_cursors,
        [(PartKey::new([1; 32]), 10)].into()
    );
    let mut task = MachineTask {
        id: 2,
        deets: MachineTaskDeets::ReplayPage(ReplayPageTask {
            peer_id: page.peer_id.clone(),
            session_id: ReplaySessionId(1),
            request_id: ReplayRequestId(1),
            targets: page
                .routes
                .iter()
                .enumerate()
                .map(|(index, route)| {
                    (
                        ReplayTargetId(u32::try_from(index).unwrap()),
                        route.target.with_cursor(100_000),
                    )
                })
                .collect(),
            supersede: None,
            limit: 16,
            hold_ms: 0,
            subscription: None,
        }),
    };
    assert_eq!(worker.prepare_receive_task(&mut task).await?, page.routes);
    let MachineTaskDeets::ReplayPage(task) = task.deets else {
        unreachable!()
    };
    assert_eq!(
        task.targets,
        page.routes
            .iter()
            .enumerate()
            .map(|(index, route)| {
                (
                    ReplayTargetId(u32::try_from(index).unwrap()),
                    route.target.with_cursor(10),
                )
            })
            .collect::<Vec<_>>()
    );
    for route in page.routes {
        assert_eq!(
            backend.store.progress(page.peer_id.clone(), route).await?,
            ReceiveProgress {
                received: 10,
                applied: 10
            }
        );
    }
    assert_eq!(backend.content_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn receive_gate_durably_hands_off_before_dispatch_and_rejects_overlapping_result() -> Res<()>
{
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.ephemeral(&[])?, true).await?;
    let page = replay();
    let backends = recovery::backends(&backend);
    assert!(
        gate_result(
            &backend.store,
            &backends,
            result(&page),
            page.routes.clone()
        )
        .await
        .is_err()
    );
    let receipt = backend.store.pending().await?.unwrap();
    assert_eq!(
        receipt.replay, page,
        "the gate must persist the entire raw page before admission"
    );
    let msg = gate_result(
        &backend.store,
        &backends,
        result(&page),
        page.routes.clone(),
    )
    .await?;
    assert!(matches!(
        msg,
        MachineTaskMsg::MachineTaskError(MachineTaskError {
            deets: MachineTaskErrDeets::ReplayPage(ReplayPageTaskError {
                deets: ReplayPageTaskErrorDeets::StaleReceiveBounds,
                ..
            }),
            ..
        })
    ));
    assert!(backend.store.pending().await?.is_none());
    let mut next = page.clone();
    next.requested
        .iter_mut()
        .for_each(|target| *target = ReplaySubscriptionTarget::from(&*target).with_cursor(10));
    assert!(matches!(
        gate_result(
            &backend.store,
            &backends,
            result(&next),
            next.routes.clone()
        )
        .await?,
        MachineTaskMsg::MachineTaskResult(_)
    ));
    Ok(())
}

#[tokio::test]
async fn receive_reopen_before_handoff_drains_pending_before_fresh_coverage() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("receive-before-handoff.sqlite");
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.database(&path, &[], "migrations"), false).await?;
    let page = replay();
    let receipt = backend.store.accept(page.clone()).await?.unwrap();
    assert_eq!(
        backend
            .sql
            .query("SELECT COUNT(*) FROM test_receive_admissions", ())
            .await?
            .one::<i64>()?,
        0
    );
    assert_eq!(backend.content_calls.load(Ordering::SeqCst), 0);
    drop(backend);
    drop(runtime);

    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.database(&path, &[], "migrations"), false).await?;
    assert_eq!(backend.store.pending().await?, Some(receipt.clone()));
    for route in &page.routes {
        assert_eq!(
            backend
                .store
                .progress(page.peer_id.clone(), route.clone())
                .await?,
            ReceiveProgress {
                received: 10,
                applied: 0
            }
        );
    }
    let mut fresh = page.clone();
    fresh.requested = fresh
        .routes
        .iter()
        .map(|route| route.target.with_cursor(10))
        .collect();
    fresh.page.events.clear();
    fresh.page.targets.iter_mut().for_each(|(_, verdict)| {
        *verdict = TargetVerdict::Events {
            resume: 20,
            drained: true,
        }
    });
    assert!(backend.store.accept(fresh.clone()).await.is_err());
    assert!(matches!(
        gate_result(
            &backend.store,
            &recovery::backends(&backend),
            result(&fresh),
            fresh.routes.clone()
        )
        .await?,
        MachineTaskMsg::MachineTaskResult(_)
    ));
    let rows = backend
        .sql
        .query("SELECT replay FROM test_receive_admissions ORDER BY id", ())
        .await?
        .all::<(Vec<u8>,)>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        serde_json::from_slice::<ReplayReceipt>(&rows[0].0)?,
        receipt
    );
    assert_eq!(
        serde_json::from_slice::<ReplayReceipt>(&rows[1].0)?.replay,
        fresh
    );
    assert_eq!(
        backend.content_calls.load(Ordering::SeqCst),
        0,
        "receipt handoff must not start content work"
    );
    assert!(backend.store.pending().await?.is_none());
    for route in page.routes {
        assert_eq!(
            backend.store.progress(page.peer_id.clone(), route).await?,
            ReceiveProgress {
                received: 20,
                applied: 20
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn receive_gate_uses_captured_backend_ownership_and_partitions_mixed_targets() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.ephemeral(&[])?, false).await?;
    let other = Arc::new(recovery::Backend {
        store: backend.store.clone(),
        sql: backend.sql.clone(),
        fail: AtomicBool::new(true),
        content_calls: Default::default(),
        id: "other",
    });
    let mut backends = recovery::backends(&backend);
    backends.insert(Arc::from("other"), Arc::<recovery::Backend>::clone(&other));
    let mut page = replay();
    page.routes[1].backend = "other".into();
    assert!(
        gate_result(
            &backend.store,
            &backends,
            result(&page),
            page.routes.clone()
        )
        .await
        .is_err()
    );
    let ReplayReceipt {
        replay: persisted, ..
    } = backend.store.pending().await?.unwrap();
    assert_eq!(persisted.routes, page.routes);
    backends.remove("durable");
    assert!(
        crate::receive::drain(&backend.store, &backends)
            .await
            .is_err(),
        "a rebound route must not consume the former owner's receipt"
    );
    backends.insert(
        Arc::from("durable"),
        Arc::<recovery::Backend>::clone(&backend),
    );
    crate::receive::drain(&backend.store, &backends).await?;
    assert!(backend.store.pending().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn durable_receive_content_tasks_never_supply_membership_or_sparse_tombstones() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let backend = recovery::backend(runtime.ephemeral(&[])?, false).await?;
    let page = replay();
    backend.store.accept(page.clone()).await?.unwrap();
    crate::receive::drain(&backend.store, &recovery::backends(&backend)).await?;
    backend
        .store
        .set_peer_part_cursor(page.peer_id.clone(), PartKey::new([1; 32]), 99_000)
        .await?;
    let (tx, rx) = mpsc::bounded(2, "receive-content".into(), "receive-test".into());
    for (id, kind) in [(1, SyncTaskKind::Sync), (2, SyncTaskKind::RemoveFromParts)] {
        SyncTaskWorker {
            task: SyncTask {
                id,
                kind,
                part_observations: [(PartKey::new([1; 32]), 100_000)].into(),
                deets: SyncTaskDeets {
                    peer_id: PeerKey::new([3; 32]),
                    obj_id: ObjKey::new([2; 32]),
                    remote_payload: None,
                },
            },
            worker: "receive-test",
            backend: Arc::<recovery::Backend>::clone(&backend),
            host_tx: tx.clone(),
            cancel_token: CancellationToken::new(),
        }
        .run()
        .await;
        match rx.recv().await? {
            BigSyncEvent::SyncCompleted(event) => assert_eq!(event.task_id, 1),
            BigSyncEvent::RemoveCompleted(event) => assert_eq!(event.task_id, 2),
            event => panic!("unexpected content completion: {event:?}"),
        }
    }
    assert!(backend.store.pending().await?.is_none());
    for route in page.routes {
        assert_eq!(
            backend.store.progress(page.peer_id.clone(), route).await?,
            ReceiveProgress {
                received: 10,
                applied: 10
            }
        );
    }
    assert_eq!(backend.content_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend
            .sql
            .query("SELECT COUNT(*) FROM test_receive_admissions", ())
            .await?
            .one::<i64>()?,
        1
    );
    assert_eq!(
        backend
            .store
            .get_peer_part_cursor(page.peer_id, PartKey::new([1; 32]))
            .await?,
        99_000
    );
    Ok(())
}
