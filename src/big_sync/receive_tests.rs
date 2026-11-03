use super::*;
use crate::HostPartStore;
use crate::MemoryPartStore;
use crate::SqlitePartStore;
use big_sync_core::BuckId;
use big_sync_core::ObjKey;
use big_sync_core::PartKey;
use big_sync_core::rpc::ObjChanged;
use big_sync_core::rpc::PartEvent;

pub(crate) fn replay() -> ReceivedReplay {
    let part = PartKey::new([1; 32]);
    let object = ObjKey::new([2; 32]);
    let targets = vec![
        ReplaySubscriptionTarget::Part {
            part_id: part.clone(),
        },
        ReplaySubscriptionTarget::Object {
            obj_id: object.clone(),
        },
    ];
    let requested: Vec<_> = targets.iter().map(|target| target.with_cursor(0)).collect();
    ReceivedReplay {
        peer_id: PeerKey::new([3; 32]),
        page: ReplayPage {
            events: [object, ObjKey::new([4; 32])]
                .into_iter()
                .map(|obj_id| {
                    PartEvent::Changed(ObjChanged {
                        cursor: 10,
                        part_ids: vec![part.clone()],
                        obj_id,
                        payload: serde_json::json!({"revision": 1}),
                    })
                })
                .collect(),
            targets: requested
                .iter()
                .cloned()
                .map(|target| {
                    (
                        target,
                        TargetVerdict::Events {
                            resume: 10,
                            drained: false,
                        },
                    )
                })
                .collect(),
        },
        requested,
        routes: targets
            .into_iter()
            .map(|target| ReceiveRoute {
                backend: "durable".into(),
                target,
            })
            .collect(),
    }
}

async fn assert_journal(store: &dyn HostPartStore) -> Res<()> {
    let journal = store.receive_journal().unwrap();
    let page = replay();
    let receipt = journal.accept(page.clone()).await?.unwrap();
    assert_eq!(receipt.replay, page);
    assert_eq!(journal.pending().await?, Some(receipt.clone()));
    for route in &page.routes {
        assert_eq!(
            journal
                .progress(page.peer_id.clone(), route.clone())
                .await?,
            ReceiveProgress {
                received: 10,
                applied: 0
            }
        );
    }
    assert!(journal.accept(page.clone()).await.is_err());
    assert!(journal.applied(receipt.id + 1).await.is_err());
    assert_eq!(journal.pending().await?, Some(receipt.clone()));
    journal.applied(receipt.id).await?;
    assert!(journal.pending().await?.is_none());
    for cursor in [0, 5, 11] {
        let mut stale = page.clone();
        stale.requested = stale
            .routes
            .iter()
            .map(|route| route.target.with_cursor(cursor))
            .collect();
        stale.page.targets.iter_mut().for_each(|(_, verdict)| {
            *verdict = TargetVerdict::Events {
                resume: 20,
                drained: true,
            }
        });
        assert!(journal.accept(stale).await?.is_none());
    }
    let mut partial = page.clone();
    partial.requested[0] = partial.routes[0].target.with_cursor(10);
    assert!(journal.accept(partial).await?.is_none());
    for route in &page.routes {
        assert_eq!(
            journal
                .progress(page.peer_id.clone(), route.clone())
                .await?,
            ReceiveProgress {
                received: 10,
                applied: 10
            }
        );
        let mut other_backend = route.clone();
        other_backend.backend = "another".into();
        assert_eq!(
            journal
                .progress(page.peer_id.clone(), other_backend)
                .await?,
            ReceiveProgress::default()
        );
        assert_eq!(
            journal
                .progress(PeerKey::new([9; 32]), route.clone())
                .await?,
            ReceiveProgress::default()
        );
    }
    let mut next = page.clone();
    next.requested = next
        .routes
        .iter()
        .map(|route| route.target.with_cursor(10))
        .collect();
    next.page.targets[0].1 = TargetVerdict::Events {
        resume: 30,
        drained: true,
    };
    next.page.targets[1].1 = TargetVerdict::Unauthorized;
    let second = journal.accept(next).await?.unwrap();
    assert!(second.id > receipt.id);
    journal.applied(second.id).await?;
    assert_eq!(
        journal
            .progress(page.peer_id.clone(), page.routes[0].clone())
            .await?,
        ReceiveProgress {
            received: 30,
            applied: 30
        }
    );
    assert_eq!(
        journal
            .progress(page.peer_id.clone(), page.routes[1].clone())
            .await?,
        ReceiveProgress {
            received: 10,
            applied: 10
        }
    );
    Ok(())
}

#[tokio::test]
async fn memory_receive_journal_chain_and_atomicity() -> Res<()> {
    assert_journal(&MemoryPartStore::new()).await
}

#[tokio::test]
async fn sqlite_receive_journal_chain_and_scope_isolation() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[])?;
    let store = SqlitePartStore::new(sql.clone(), "receive", BuckId::MAX_LEVEL).await?;
    assert_journal(&store).await?;
    let other = SqlitePartStore::new(sql, "other", BuckId::MAX_LEVEL).await?;
    assert!(other.pending().await?.is_none());
    let page = replay();
    for route in page.routes {
        assert_eq!(
            other.progress(page.peer_id.clone(), route).await?,
            ReceiveProgress::default()
        );
    }
    Ok(())
}

#[path = "receive_recovery_tests.rs"]
pub(crate) mod recovery;
