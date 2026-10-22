use super::*;
use clay_docs::DocumentGrant;
use clay_docs::DocumentService;
use clay_docs::FacetSelection;
use clay_docs::MockDocuments;
use clay_docs::OperationStatus;
use daybook_types::manifest::RoutineFacetAccess;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Waker;

struct Rig {
    host: Host,
    instance: Instance,
    session: DocumentSession,
    access: Access,
    incoming: mpsc::Receiver<Work>,
    node: MockDocuments,
}

impl Rig {
    fn new(limits: Limits) -> Self {
        let host = Host::new();
        let instance = host.start("guest");
        let service = DocumentService::new(32);
        let access = service
            .grant_documents(
                &host,
                &instance,
                vec![DocumentGrant {
                    document: "allowed".into(),
                    branch: "main".into(),
                    facets: vec![RoutineFacetAccess {
                        tag: "config".into(),
                        key_id: Some("count".into()),
                        read: true,
                        write: true,
                        ..Default::default()
                    }],
                }],
            )
            .unwrap();
        let (session, incoming) =
            DocumentSession::new(host.clone(), instance.clone(), limits).unwrap();
        let access = session.grant(access).unwrap();
        Self {
            host,
            instance,
            session,
            access,
            incoming,
            node: service.mock_documents(),
        }
    }

    fn route() -> FacetRoute {
        FacetRoute {
            document: "allowed".into(),
            branch: "main".into(),
            tag: "config".into(),
            key_id: "count".into(),
        }
    }

    fn selection() -> WatchSelection {
        WatchSelection {
            documents: ["allowed".into()].into(),
            branch: "main".into(),
            facets: FacetSelection::Keys([("config".into(), "count".into())].into()),
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.host.close();
    }
}

fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

#[tokio::test]
async fn pending_watch_release_closes_the_data_watch_and_cancels_delivery() {
    let mut rig = Rig::new(Limits::default());
    let watch = rig.access.watch(Rig::selection()).unwrap();
    let retained = watch.watch.clone();
    let work = rig.incoming.recv().await.unwrap();
    let mut pending = std::pin::pin!(work.wait());
    assert_pending(pending.as_mut());
    drop(watch);
    assert_eq!(retained.current(), Err(AccessError::Revoked));
    assert!(pending.await.is_none());
    assert_eq!(rig.session.handle_count(), 1);
}

#[tokio::test]
async fn queued_and_dequeued_watch_notifications_recheck_release() {
    for dequeued in [false, true] {
        let mut rig = Rig::new(Limits::default());
        let watch = rig.access.watch(Rig::selection()).unwrap();
        rig.node.external_change(Rig::route(), Some(json!(7)));
        let work = rig.incoming.recv().await.unwrap();
        let work = if dequeued {
            let (work, delivery) = work.wait().await.unwrap();
            assert_eq!(
                delivery,
                Delivery::Changed {
                    id: watch.id(),
                    snapshot: watch.current().unwrap()
                }
            );
            assert!(!work.released());
            work
        } else {
            work
        };
        drop(watch);
        assert!(work.released());
        assert!(work.wait().await.is_none());
    }
}

#[tokio::test]
async fn pending_operation_release_preserves_source_completion_without_guest_delivery() {
    let mut rig = Rig::new(Limits::default());
    let operation = rig
        .access
        .mutate(Rig::route(), FacetMutation::Write(json!(1)))
        .unwrap();
    let retained = operation.operation.clone();
    let work = rig.incoming.recv().await.unwrap();
    let mut pending = std::pin::pin!(work.wait());
    assert_pending(pending.as_mut());
    drop(operation);
    assert!(pending.await.is_none());
    rig.node.complete(retained.id(), Ok(()));
    assert_eq!(
        retained.current(),
        OperationReceipt {
            id: retained.id(),
            revision: 1,
            status: OperationStatus::Accepted,
        }
    );
    assert_eq!(rig.session.handle_count(), 1);
}

#[tokio::test]
async fn completed_receipts_recheck_release_or_session_closure_before_delivery() {
    for close_owner in [false, true] {
        let mut rig = Rig::new(Limits::default());
        let operation = rig
            .access
            .mutate(Rig::route(), FacetMutation::Write(json!(1)))
            .unwrap();
        let receipt = operation.current().unwrap();
        rig.node.complete(receipt.id, Ok(()));
        let (work, delivery) = rig.incoming.recv().await.unwrap().wait().await.unwrap();
        assert_eq!(
            delivery,
            Delivery::Completed(OperationReceipt {
                id: receipt.id,
                revision: 1,
                status: OperationStatus::Accepted,
            })
        );
        assert!(!work.released());
        if close_owner {
            rig.host.close_instance(&rig.instance).unwrap();
            assert_eq!(operation.current(), Err(AccessError::Revoked));
        } else {
            drop(operation);
        }
        assert!(work.released());
        assert!(work.wait().await.is_none());
    }
}

#[tokio::test]
async fn admission_rejects_before_mutation_and_releases_resource_and_queue_capacity() {
    let mut rig = Rig::new(Limits {
        handles: 2,
        pending_deliveries: 1,
    });
    let extra = rig.session.grant(rig.access.access.clone()).unwrap();
    assert!(matches!(
        rig.session.grant(rig.access.access.clone()),
        Err(AccessError::Denied)
    ));
    assert!(matches!(
        rig.access
            .mutate(Rig::route(), FacetMutation::Write(json!(1))),
        Err(AccessError::Denied)
    ));
    assert!(rig.node.reads().is_empty());
    drop(extra);
    let watch = rig.access.watch(Rig::selection()).unwrap();
    let reads = rig.node.reads();
    drop(watch);
    assert_eq!(rig.session.handle_count(), 1);
    assert!(matches!(
        rig.access
            .mutate(Rig::route(), FacetMutation::Write(json!(2))),
        Err(AccessError::Denied)
    ));
    assert_eq!(rig.node.reads(), reads);
    assert!(rig.incoming.recv().await.unwrap().wait().await.is_none());
    let mut denied_route = Rig::route();
    denied_route.document = "forged".into();
    assert!(matches!(
        rig.access
            .mutate(denied_route, FacetMutation::Write(json!(3))),
        Err(AccessError::Denied)
    ));
    assert_eq!(rig.session.handle_count(), 1);
    assert!(rig.incoming.try_recv().is_err());
    let operation = rig
        .access
        .mutate(Rig::route(), FacetMutation::Write(json!(4)))
        .unwrap();
    assert_eq!(operation.current().unwrap().id, 2);
    assert_eq!(rig.session.handle_count(), 2);
    drop(operation);
    assert!(rig.incoming.recv().await.unwrap().wait().await.is_none());
    assert_eq!(rig.session.handle_count(), 1);
}

#[tokio::test]
async fn selection_lag_and_repeated_snapshots_use_neutral_values() {
    let mut rig = Rig::new(Limits::default());
    let watch = rig.access.watch(Rig::selection()).unwrap();
    let work = rig.incoming.recv().await.unwrap();
    watch.select(Rig::selection()).unwrap();
    let (work, delivery) = work.wait().await.unwrap();
    assert_eq!(delivery, Delivery::Lagged(watch.id()));
    rig.node.external_change(Rig::route(), Some(json!(7)));
    let (work, delivery) = work.wait().await.unwrap();
    assert_eq!(
        delivery,
        Delivery::Changed {
            id: watch.id(),
            snapshot: watch.current().unwrap()
        }
    );
    rig.node.hold_refreshes(true);
    watch.refresh().unwrap();
    let (work, delivery) = work.wait().await.unwrap();
    assert_eq!(
        delivery,
        Delivery::Changed {
            id: watch.id(),
            snapshot: watch.current().unwrap()
        }
    );
    drop(watch);
    assert!(work.wait().await.is_none());
}

#[tokio::test]
async fn session_authority_is_independent_of_the_document_grant_owner() {
    let rig = Rig::new(Limits::default());
    let receiver = rig.host.start("receiver");
    let (session, mut incoming) =
        DocumentSession::new(rig.host.clone(), receiver.clone(), Limits::default()).unwrap();
    let access = session.grant(rig.access.access.clone()).unwrap();
    let watch = access.watch(Rig::selection()).unwrap();
    let operation = access
        .mutate(Rig::route(), FacetMutation::Write(json!(1)))
        .unwrap();
    let retained = operation.operation.clone();
    let watch_work = incoming.recv().await.unwrap();
    let operation_work = incoming.recv().await.unwrap();
    let mut pending = std::pin::pin!(operation_work.wait());
    assert_pending(pending.as_mut());
    rig.host.close_instance(&receiver).unwrap();
    assert_eq!(rig.host.check(&rig.instance), Ok(()));
    assert_eq!(session.check(), Err(AccessError::Revoked));
    assert!(matches!(
        session.grant(rig.access.access.clone()),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        access.watch(Rig::selection()),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        access.mutate(Rig::route(), FacetMutation::Write(json!(2))),
        Err(AccessError::Revoked)
    ));
    assert_eq!(watch.current(), Err(AccessError::Revoked));
    assert_eq!(watch.select(Rig::selection()), Err(AccessError::Revoked));
    assert_eq!(watch.refresh(), Err(AccessError::Revoked));
    assert_eq!(operation.current(), Err(AccessError::Revoked));
    assert!(watch_work.wait().await.is_none());
    assert!(pending.await.is_none());
    rig.node.complete(retained.id(), Ok(()));
    assert_eq!(retained.current().status, OperationStatus::Accepted);
    let foreign = Host::new();
    assert!(matches!(
        DocumentSession::new(foreign.clone(), rig.instance.clone(), Limits::default()),
        Err(AccessError::Denied)
    ));
    foreign.close();
}
