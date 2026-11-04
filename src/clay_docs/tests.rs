use super::*;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Waker;

fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

fn route(document: &str, key_id: &str) -> FacetRoute {
    FacetRoute {
        document: document.into(),
        branch: "main".into(),
        tag: "org.example.config".into(),
        key_id: key_id.into(),
    }
}
fn selection(documents: &[&str]) -> WatchSelection {
    WatchSelection {
        documents: documents.iter().map(|id| (*id).into()).collect(),
        branch: "main".into(),
        facets: FacetSelection::Tags(["org.example.config".into()].into()),
    }
}
fn grant(
    service: &DocumentService,
    host: &Host,
    instance: &Instance,
    documents: &[&str],
    key_id: Option<&str>,
) -> DocumentAccess {
    service
        .grant_documents(
            host,
            instance,
            documents
                .iter()
                .map(|document| DocumentGrant {
                    document: (*document).into(),
                    branch: "main".into(),
                    facets: vec![RoutineFacetAccess {
                        tag: "org.example.config".into(),
                        key_id: key_id.map(str::to_owned),
                        read: true,
                        write: true,
                        create: true,
                        delete: true,
                        ..Default::default()
                    }],
                })
                .collect(),
        )
        .unwrap()
}

#[test]
fn multiple_document_mirrors_filter_warm_cache_and_follow_membership() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(1));
    node.seed(route("a", "secret"), json!(2));
    node.seed(route("b", "main"), json!(3));
    node.seed(route("unrelated", "main"), json!(4));
    let privileged = host.start("privileged");
    let warm = grant(&service, &host, &privileged, &["a"], /*key_id*/ None)
        .watch(selection(&["a"]))
        .unwrap();
    assert_eq!(warm.current().unwrap().facets.len(), 2);
    let instance = host.start("restricted");
    let access = grant(&service, &host, &instance, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a", "b"])).unwrap();
    assert_eq!(
        mirror.current().unwrap().facets,
        BTreeMap::from([
            (route("a", "main"), FacetValue::Present(json!(1))),
            (route("b", "main"), FacetValue::Present(json!(3)))
        ])
    );
    let reads = node.reads();
    assert!(matches!(
        access.watch(selection(&["unrelated"])),
        Err(AccessError::Denied)
    ));
    assert_eq!(node.reads(), reads);
    let before = mirror.current().unwrap();
    node.external_change(route("a", "secret"), Some(json!(99)));
    assert_eq!(mirror.current().unwrap(), before);
    node.external_change(route("b", "main"), /*value*/ None);
    assert_eq!(
        mirror.current().unwrap().facets[&route("b", "main")],
        FacetValue::Absent
    );
    node.external_change(route("b", "main"), Some(json!(5)));
    assert_eq!(
        mirror.current().unwrap().facets[&route("b", "main")],
        FacetValue::Present(json!(5))
    );
    assert!(!node.cached_routes().contains(&route("unrelated", "main")));
    let mut updates = mirror.subscribe().unwrap();
    host.close_instance(&instance).unwrap();
    assert!(updates.borrow_and_update().revoked);
    assert!(updates.borrow().facets.is_empty());
    assert!(updates.has_changed().is_err());
    host.close();
}

#[test]
fn config_binding_resolution_and_manifest_rights_are_not_conflated() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.bind_config("alpha".into(), "a".into(), "main".into());
    node.bind_config("beta".into(), "b".into(), "main".into());
    node.seed(route("a", "main"), json!(1));
    let instance = host.start("alpha");
    let alpha = service
        .grant_config(
            &host,
            &instance,
            "alpha",
            vec![
                RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    read: true,
                    ..Default::default()
                },
                RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    write: true,
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let beta = service
        .grant_config(
            &host,
            &instance,
            "beta",
            vec![RoutineFacetAccess {
                tag: "org.example.config".into(),
                key_id: Some("new".into()),
                create: true,
                ..Default::default()
            }],
        )
        .unwrap();
    let facet = alpha
        .facet::<Value>("org.example.config".into(), "main".into())
        .unwrap();
    assert_eq!(facet.value(&facet.watch.current().unwrap()), Some(json!(1)));
    facet.write(json!(2)).unwrap();
    assert_eq!(
        alpha
            .mutate(
                "org.example.config".into(),
                "main".into(),
                FacetMutation::Delete
            )
            .unwrap_err(),
        AccessError::Denied
    );
    assert_eq!(
        alpha
            .mutate(
                "org.example.config".into(),
                "other".into(),
                FacetMutation::Write(json!(3))
            )
            .unwrap_err(),
        AccessError::Denied
    );
    assert!(matches!(
        beta.facet::<Value>("org.example.config".into(), "new".into()),
        Err(AccessError::Denied)
    ));
    beta.mutate(
        "org.example.config".into(),
        "new".into(),
        FacetMutation::Create(json!(4)),
    )
    .unwrap();
    assert_eq!(
        beta.mutate(
            "org.example.config".into(),
            "new".into(),
            FacetMutation::Write(json!(5))
        )
        .unwrap_err(),
        AccessError::Denied
    );
    node.settle(/*accept*/ true);
    assert_eq!(node.reads(), vec![route("a", "main"), route("b", "new")]);
    host.close();
}

#[test]
#[should_panic(expected = "a config ACL names no owner plug")]
fn a_config_acl_naming_an_owner_plug_is_a_programming_error() {
    let host = Host::new();
    let service = DocumentService::new(32);
    service
        .mock_documents()
        .bind_config("alpha".into(), "a".into(), "main".into());
    let instance = host.start("alpha");
    let _ = service.grant_config(
        &host,
        &instance,
        "alpha",
        vec![RoutineFacetAccess {
            owner_plug_id: Some("beta".into()),
            tag: "org.example.config".into(),
            read: true,
            ..Default::default()
        }],
    );
}

#[test]
fn held_refreshes_deduplicate_and_cannot_resurrect_removed_selections() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.hold_refreshes(/*hold*/ true);
    node.seed(route("a", "main"), json!(1));
    node.seed(route("b", "main"), json!(2));
    let first = host.start("first");
    let second = host.start("second");
    let access = grant(&service, &host, &first, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let other = grant(&service, &host, &second, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    assert_eq!(node.pending_refreshes(), 1);
    assert!(matches!(
        mirror.current().unwrap().freshness,
        Freshness::Refreshing
    ));
    mirror.select(selection(&["b"])).unwrap();
    other.close();
    node.complete_refreshes();
    assert_eq!(node.reads(), vec![route("b", "main")]);
    let snapshot = mirror.current().unwrap();
    assert!(!matches!(snapshot.freshness, Freshness::Refreshing));
    assert_eq!(
        snapshot.facets,
        BTreeMap::from([(route("b", "main"), FacetValue::Present(json!(2)))])
    );
    mirror.select(selection(&["a"])).unwrap();
    host.close_instance(&first).unwrap();
    node.complete_refreshes();
    assert_eq!(node.reads(), vec![route("b", "main")]);
    host.close();
}

#[test]
fn optimistic_overlays_are_shared_authorized_and_pinned_after_watch_closure() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(1));
    let first = host.start("writer");
    let second = host.start("reader");
    let access = grant(&service, &host, &first, &["a"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let other = grant(&service, &host, &second, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    access
        .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
        .unwrap();
    access
        .mutate(route("a", "main"), FacetMutation::Write(json!(3)))
        .unwrap();
    mirror.refresh().unwrap();
    assert_eq!(
        other.current().unwrap().facets[&route("a", "main")],
        FacetValue::Present(json!(3))
    );
    assert_eq!(other.current().unwrap().pending.len(), 2);
    host.close_instance(&first).unwrap();
    node.settle(/*accept*/ true);
    assert!(other.current().unwrap().pending.is_empty());
    assert_eq!(
        other.current().unwrap().facets[&route("a", "main")],
        FacetValue::Present(json!(3))
    );
    host.close_instance(&second).unwrap();
    assert!(node.cached_routes().contains(&route("a", "main")));
    host.close();
}

#[test]
fn only_unretained_projection_values_are_evicted() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let instance = host.start("cache-reader");
    let documents = (0..40).map(|id| format!("doc-{id}")).collect::<Vec<_>>();
    for document in &documents {
        node.seed(route(document, "main"), json!(document));
    }
    let access = grant(
        &service,
        &host,
        &instance,
        &documents.iter().map(String::as_str).collect::<Vec<_>>(),
        Some("main"),
    );
    let retained = access
        .watch(selection(
            &documents.iter().map(String::as_str).collect::<Vec<_>>(),
        ))
        .unwrap();
    assert_eq!(node.cached_routes().len(), 40);
    drop(retained);
    assert_eq!(node.cached_routes().len(), 32);
    host.close();
}

#[test]
fn a_targeted_read_hydrates_only_that_route_and_preserves_loading_states() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(1));
    node.seed(route("b", "main"), json!(2));
    let instance = host.start("reader");
    let access = grant(&service, &host, &instance, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a", "b"])).unwrap();
    assert_eq!(
        mirror.read(&route("a", "main")),
        Ok(FacetValue::Present(json!(1)))
    );
    // The read hydrated exactly one route; the unrequested route stays where it was.
    assert!(node.cached_routes().contains(&route("a", "main")));
    assert_eq!(
        mirror.read(&route("b", "main")),
        Ok(FacetValue::Present(json!(2)))
    );
    assert!(matches!(
        mirror.read(&route("a", "secret")),
        Err(AccessError::Denied)
    ));
    assert!(matches!(
        mirror.read(&route("unrelated", "main")),
        Err(AccessError::Denied)
    ));
    host.close();
}

#[test]
fn read_only_grants_reject_every_mutation_without_hydration() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(1));
    let instance = host.start("reader");
    let access = service
        .grant_documents(
            &host,
            &instance,
            vec![DocumentGrant {
                document: "a".into(),
                branch: "main".into(),
                facets: vec![RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    read: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let mirror = access.watch(selection(&["a"])).unwrap();
    let before = mirror.current().unwrap();
    let reads = node.reads();
    for mutation in [
        FacetMutation::Write(json!(2)),
        FacetMutation::Create(json!(3)),
        FacetMutation::Delete,
    ] {
        assert_eq!(
            access.mutate(route("a", "main"), mutation).unwrap_err(),
            AccessError::Denied
        );
    }
    assert_eq!(
        access
            .mutate(route("a", "secret"), FacetMutation::Write(json!(4)))
            .unwrap_err(),
        AccessError::Denied
    );
    assert_eq!(mirror.current().unwrap(), before);
    assert_eq!(node.reads(), reads);
    host.close();
}

#[test]
fn document_ref_access_preserves_the_grant_and_its_revocation() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let instance = host.start("reader");
    let foreign_host = Host::new();
    assert!(matches!(
        service.grant_documents(&foreign_host, &instance, Vec::new()),
        Err(AccessError::Denied)
    ));
    foreign_host.close();
    let access = service
        .grant_documents(
            &host,
            &instance,
            vec![DocumentGrant {
                document: "a".into(),
                branch: "approved-branch".into(),
                facets: vec![RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    read: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let document = access
        .document("a".into(), "approved-branch".into())
        .unwrap();
    assert_eq!(document.document_id(), "a");
    assert_eq!(document.branch(), "approved-branch");
    let cloned = document.access();
    assert!(Arc::ptr_eq(&cloned.authority, &access.authority));
    assert!(matches!(
        cloned.document("other".into(), document.branch().into()),
        Err(AccessError::Denied)
    ));
    assert!(matches!(
        cloned.document(document.document_id().into(), "unapproved-branch".into()),
        Err(AccessError::Denied)
    ));
    let permitted = WatchSelection {
        documents: [document.document_id().into()].into(),
        branch: document.branch().into(),
        facets: FacetSelection::Keys([("org.example.config".into(), "main".into())].into()),
    };
    let mirror = cloned.watch(permitted.clone()).unwrap();
    assert_eq!(
        mirror.current().unwrap().facets,
        BTreeMap::from([(
            FacetRoute {
                document: "a".into(),
                branch: "approved-branch".into(),
                tag: "org.example.config".into(),
                key_id: "main".into()
            },
            FacetValue::Absent,
        )])
    );
    assert_eq!(
        document
            .mutate(
                "org.example.config".into(),
                "main".into(),
                FacetMutation::Write(json!(1))
            )
            .unwrap_err(),
        AccessError::Denied
    );
    host.close_instance(&instance).unwrap();
    assert!(matches!(cloned.watch(permitted), Err(AccessError::Revoked)));
    assert!(matches!(
        document
            .access()
            .document("a".into(), "approved-branch".into()),
        Err(AccessError::Revoked)
    ));
    assert_eq!(mirror.current(), Err(AccessError::Revoked));
    host.close();
}

#[tokio::test]
async fn operation_subscription_retains_the_receipt_before_and_after_owner_closure() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], Some("main"));
    let operation = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let mut updates = operation.subscribe();
    assert_eq!(*updates.borrow_and_update(), operation.current());
    host.close_instance(&instance).unwrap();
    assert_eq!(operation.current().status, OperationStatus::LocallyApplied);
    node.complete(operation.id(), Ok(()));
    updates.changed().await.unwrap();
    let accepted = OperationReceipt {
        id: operation.id(),
        revision: 1,
        status: OperationStatus::Accepted,
    };
    assert_eq!(*updates.borrow_and_update(), accepted);
    assert!(updates.changed().await.is_err());
    let mut late = operation.subscribe();
    assert_eq!(*late.borrow_and_update(), accepted);
    assert!(late.changed().await.is_err());
    drop(operation);
    host.close();
    assert_eq!(*updates.borrow(), accepted);
    assert_eq!(*late.borrow(), accepted);
}

#[test]
fn accepting_one_mutation_never_persists_a_later_overlay() {
    for (accepted, rejected, expected) in [
        (
            FacetMutation::Write(json!(2)),
            FacetMutation::Write(json!(3)),
            FacetValue::Present(json!(2)),
        ),
        (
            FacetMutation::Create(json!(2)),
            FacetMutation::Delete,
            FacetValue::Present(json!(2)),
        ),
        (
            FacetMutation::Delete,
            FacetMutation::Create(json!(3)),
            FacetValue::Absent,
        ),
    ] {
        let host = Host::new();
        let service = DocumentService::new(32);
        let node = service.mock_documents();
        node.seed(route("a", "main"), json!(1));
        let instance = host.start("writer");
        let access = grant(&service, &host, &instance, &["a"], Some("main"));
        let mirror = access.watch(selection(&["a"])).unwrap();
        let first = access.mutate(route("a", "main"), accepted).unwrap();
        let later = access.mutate(route("a", "main"), rejected).unwrap();
        node.complete(first.id(), Ok(()));
        assert_eq!(
            first.current(),
            OperationReceipt {
                id: first.id(),
                revision: 1,
                status: OperationStatus::Accepted,
            }
        );
        assert_eq!(
            later.current(),
            OperationReceipt {
                id: later.id(),
                revision: 0,
                status: OperationStatus::LocallyApplied,
            }
        );
        let rejection = OperationError::Rejected {
            message: "later write denied".into(),
        };
        node.complete(later.id(), Err(rejection.clone()));
        assert_eq!(
            later.current(),
            OperationReceipt {
                id: later.id(),
                revision: 1,
                status: OperationStatus::RolledBack(rejection),
            }
        );
        assert_eq!(
            mirror.current().unwrap().facets,
            BTreeMap::from([(route("a", "main"), expected.clone())])
        );
        mirror.refresh().unwrap();
        assert_eq!(
            mirror.current().unwrap().facets,
            BTreeMap::from([(route("a", "main"), expected)])
        );
        assert!(mirror.current().unwrap().pending.is_empty());
        host.close();
    }
}

#[test]
fn transport_failures_preserve_uncertainty_and_restore_the_latest_source() {
    for outcome_unknown in [false, true] {
        let host = Host::new();
        let service = DocumentService::new(32);
        let node = service.mock_documents();
        node.seed(route("a", "main"), json!(1));
        let instance = host.start("writer");
        let access = grant(&service, &host, &instance, &["a"], Some("main"));
        let mirror = access.watch(selection(&["a"])).unwrap();
        let operation = access
            .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
            .unwrap();
        node.external_change(route("a", "main"), Some(json!(3)));
        assert_eq!(
            mirror.current().unwrap().facets[&route("a", "main")],
            FacetValue::Present(json!(2))
        );
        let failure = OperationError::Transport {
            message: "connection lost".into(),
            outcome_unknown,
        };
        node.complete(operation.id(), Err(failure.clone()));
        assert_eq!(
            operation.current(),
            OperationReceipt {
                id: operation.id(),
                revision: 1,
                status: OperationStatus::RolledBack(failure),
            }
        );
        assert_eq!(
            mirror.current().unwrap().facets,
            BTreeMap::from([(route("a", "main"), FacetValue::Present(json!(3)))])
        );
        assert!(mirror.current().unwrap().pending.is_empty());
        node.external_change(route("a", "main"), Some(json!(2)));
        assert_eq!(
            mirror.current().unwrap().facets[&route("a", "main")],
            FacetValue::Present(json!(2))
        );
        host.close();
    }
}

#[tokio::test]
async fn independent_receipts_survive_out_of_order_completion_and_snapshot_coalescing() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(0));
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let before = mirror.current().unwrap();
    let mut notifications = mirror.subscribe().unwrap();
    let first = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let second = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
        .unwrap();
    let third = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(3)))
        .unwrap();
    assert!(first.id() < second.id() && second.id() < third.id());
    let transport = OperationError::Transport {
        message: "lost reply".into(),
        outcome_unknown: true,
    };
    let rejection = OperationError::Rejected {
        message: "denied".into(),
    };
    node.complete(third.id(), Err(transport.clone()));
    node.complete(first.id(), Ok(()));
    node.complete(second.id(), Err(rejection.clone()));
    let current = mirror.current().unwrap();
    assert!(current.revision > before.revision + 1);
    assert_eq!(
        current.facets,
        BTreeMap::from([(route("a", "main"), FacetValue::Present(json!(1)))])
    );
    assert!(current.pending.is_empty());
    assert_eq!(*notifications.borrow_and_update(), current);
    assert_eq!(mirror.next(before.revision).await.unwrap(), current);
    for (operation, status) in [
        (&first, OperationStatus::Accepted),
        (&second, OperationStatus::RolledBack(rejection)),
        (&third, OperationStatus::RolledBack(transport)),
    ] {
        let receipt = OperationReceipt {
            id: operation.id(),
            revision: 1,
            status,
        };
        assert_eq!(operation.current(), receipt);
        assert_eq!(operation.next(0).await.unwrap(), Some(receipt));
        assert_eq!(operation.next(1).await.unwrap(), None);
    }
    let retained = first.clone();
    drop(first);
    for value in 4..40 {
        let operation = access
            .mutate(route("a", "main"), FacetMutation::Write(json!(value)))
            .unwrap();
        node.complete(operation.id(), Ok(()));
    }
    assert_eq!(
        retained.current(),
        OperationReceipt {
            id: retained.id(),
            revision: 1,
            status: OperationStatus::Accepted
        }
    );
    host.close();
}

#[test]
fn out_of_order_acceptance_and_rejection_remain_operation_local() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(0));
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let older = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let newer = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
        .unwrap();
    node.complete(newer.id(), Ok(()));
    assert_eq!(newer.current().status, OperationStatus::Accepted);
    assert_eq!(older.current().status, OperationStatus::LocallyApplied);
    let rejection = OperationError::Rejected {
        message: "older rejected".into(),
    };
    node.complete(older.id(), Err(rejection.clone()));
    assert_eq!(
        older.current().status,
        OperationStatus::RolledBack(rejection)
    );
    assert_eq!(newer.current().status, OperationStatus::Accepted);
    mirror.refresh().unwrap();
    assert_eq!(
        mirror.current().unwrap().facets,
        BTreeMap::from([(route("a", "main"), FacetValue::Present(json!(2)))])
    );
    host.close();
}

#[tokio::test]
async fn operation_next_atomically_observes_completion_after_read_or_while_waiting() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], Some("main"));
    let first = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let before = first.current();
    node.complete(first.id(), Ok(()));
    assert_eq!(
        first.next(before.revision).await.unwrap(),
        Some(first.current())
    );
    assert_eq!(first.next(2).await, Err(DocumentError::SubscriptionLagged));
    let second = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
        .unwrap();
    let mut next = std::pin::pin!(second.next(second.current().revision));
    assert_pending(next.as_mut());
    node.complete(second.id(), Ok(()));
    assert_eq!(next.await.unwrap(), Some(second.current()));
    assert_eq!(second.next(second.current().revision).await.unwrap(), None);
    host.close();
}

#[tokio::test]
async fn owner_and_watch_closure_do_not_cancel_operations_or_erase_receipts() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let writer = host.start("writer");
    let reader = host.start("reader");
    let access = grant(&service, &host, &writer, &["a"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let other = grant(&service, &host, &reader, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    let operation = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let initial = operation.current();
    let mut next = std::pin::pin!(operation.next(initial.revision));
    assert_pending(next.as_mut());
    mirror.close();
    host.close_instance(&writer).unwrap();
    assert_eq!(operation.current(), initial);
    assert_pending(next.as_mut());
    node.complete(operation.id(), Ok(()));
    assert_eq!(
        next.await.unwrap(),
        Some(OperationReceipt {
            id: operation.id(),
            revision: 1,
            status: OperationStatus::Accepted
        })
    );
    assert_eq!(
        other.current().unwrap().facets,
        BTreeMap::from([(route("a", "main"), FacetValue::Present(json!(1)))])
    );
    assert!(other.current().unwrap().pending.is_empty());
    host.close();
    assert_eq!(operation.current().status, OperationStatus::Accepted);
}

#[test]
fn dropping_an_operation_handle_does_not_cancel_source_completion() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let writer = host.start("writer");
    let access = grant(&service, &host, &writer, &["a"], Some("main"));
    let operation = access
        .mutate(route("a", "main"), FacetMutation::Write(json!(1)))
        .unwrap();
    let id = operation.id();
    drop(operation);
    host.close_instance(&writer).unwrap();
    node.complete(id, Ok(()));
    let reader = host.start("reader");
    let mirror = grant(&service, &host, &reader, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    assert_eq!(
        mirror.current().unwrap().facets,
        BTreeMap::from([(route("a", "main"), FacetValue::Present(json!(1)))])
    );
    assert!(mirror.current().unwrap().pending.is_empty());
    host.close();
}

#[test]
fn all_mutation_wrappers_use_the_same_source_completion_path() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], Some("main"));
    let document = access.document("a".into(), "main".into()).unwrap();
    let facet = document
        .facet::<Value>("org.example.config".into(), "main".into())
        .unwrap();
    let write = facet.write(json!(1)).unwrap();
    let create = document
        .mutate(
            "org.example.config".into(),
            "main".into(),
            FacetMutation::Create(json!(2)),
        )
        .unwrap();
    node.settle(true);
    assert_eq!(write.current().status, OperationStatus::Accepted);
    assert_eq!(create.current().status, OperationStatus::Accepted);
    facet.watch.refresh().unwrap();
    assert_eq!(facet.value(&facet.watch.current().unwrap()), Some(json!(2)));
    let delete = access
        .mutate(route("a", "main"), FacetMutation::Delete)
        .unwrap();
    node.settle(false);
    assert_eq!(
        delete.current().status,
        OperationStatus::RolledBack(OperationError::Rejected {
            message: "mock source rejected operation".into()
        })
    );
    assert_eq!(facet.value(&facet.watch.current().unwrap()), Some(json!(2)));
    host.close();
}

#[tokio::test]
async fn document_next_is_gap_free_and_coalesces_complete_snapshots() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    node.seed(route("a", "main"), json!(1));
    let instance = host.start("reader");
    let mirror = grant(&service, &host, &instance, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    let before = mirror.current().unwrap();
    node.external_change(route("a", "main"), Some(json!(2)));
    node.external_change(route("a", "main"), Some(json!(3)));
    let latest = mirror.current().unwrap();
    assert_eq!(mirror.next(before.revision).await.unwrap(), latest);
    assert_eq!(
        mirror.next(latest.revision + 1).await,
        Err(DocumentError::SubscriptionLagged)
    );
    let mut next = std::pin::pin!(mirror.next(latest.revision));
    assert_pending(next.as_mut());
    node.external_change(route("a", "main"), Some(json!(4)));
    assert_eq!(next.await.unwrap(), mirror.current().unwrap());
    host.close();
}

#[tokio::test]
async fn document_next_rejects_old_selection_continuations_and_allows_rereading() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let instance = host.start("reader");
    let access = grant(&service, &host, &instance, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let before = mirror.current().unwrap();
    let mut next = std::pin::pin!(mirror.next(before.revision));
    assert_pending(next.as_mut());
    mirror.select(selection(&["b"])).unwrap();
    assert_eq!(next.await, Err(DocumentError::SubscriptionLagged));
    let selected = mirror.current().unwrap();
    assert_eq!(
        selected.facets,
        BTreeMap::from([(route("b", "main"), FacetValue::Absent)])
    );
    let mut next = std::pin::pin!(mirror.next(selected.revision));
    assert_pending(next.as_mut());
    mirror.select(selection(&["b"])).unwrap();
    assert_eq!(next.await, Err(DocumentError::SubscriptionLagged));
    let selected = mirror.current().unwrap();
    let mut next = std::pin::pin!(mirror.next(selected.revision));
    assert_pending(next.as_mut());
    service
        .mock_documents()
        .external_change(route("b", "main"), Some(json!(1)));
    assert_eq!(next.await.unwrap(), mirror.current().unwrap());
    host.close();
}

#[tokio::test]
async fn document_next_rechecks_revocation_after_pending_or_ready_notifications() {
    for ready_notification in [false, true] {
        let host = Host::new();
        let service = DocumentService::new(32);
        let node = service.mock_documents();
        let instance = host.start("reader");
        let mirror = grant(&service, &host, &instance, &["a"], Some("main"))
            .watch(selection(&["a"]))
            .unwrap();
        let revision = mirror.current().unwrap().revision;
        let mut next = std::pin::pin!(mirror.next(revision));
        assert_pending(next.as_mut());
        if ready_notification {
            node.external_change(route("a", "main"), Some(json!(1)));
        }
        host.close_instance(&instance).unwrap();
        assert_eq!(next.await, Err(DocumentError::Access(AccessError::Revoked)));
        assert_eq!(
            mirror.next(revision).await,
            Err(DocumentError::Access(AccessError::Revoked))
        );
        host.close();
    }
}

/// Drops every handle of the service so its persist future sees the queue close, then drives
/// the future to completion: everything the source changed has been written afterwards.
async fn flush(service: DocumentService, persist: impl Future<Output = ()>) {
    drop(service);
    persist.await;
}

/// What the unrestricted `document` mirror observes in a service.
fn observed(service: &DocumentService, documents: &[&str]) -> BTreeMap<FacetRoute, FacetValue> {
    let host = Host::new();
    let instance = host.start("observer");
    let facets = grant(service, &host, &instance, documents, /*key_id*/ None)
        .watch(selection(documents))
        .unwrap()
        .current()
        .unwrap()
        .facets;
    host.close();
    facets
}

#[tokio::test]
async fn the_durable_source_survives_a_rebuild_and_persisted_values_beat_seeds() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (service, persist) = DocumentService::durable(Arc::clone(&store), 32)
        .await
        .unwrap();
    let node = service.mock_documents();
    node.seed(route("a", "seeded"), json!(1));
    node.seed(route("a", "written"), json!(1));
    node.seed(route("a", "removed"), json!(1));
    node.seed(route("a", "replaced"), json!(1));
    node.external_change(route("a", "removed"), None);
    node.external_change(route("a", "replaced"), Some(json!("external")));
    // An accepted mutation persists; a rejected one leaves the stored value alone.
    let host = Host::new();
    let instance = host.start("writer");
    let access = grant(&service, &host, &instance, &["a"], None);
    let accepted = access
        .mutate(route("a", "written"), FacetMutation::Write(json!(2)))
        .unwrap();
    let created = access
        .mutate(route("a", "created"), FacetMutation::Create(json!(3)))
        .unwrap();
    let rejected = access
        .mutate(route("a", "seeded"), FacetMutation::Write(json!(9)))
        .unwrap();
    node.complete(accepted.id(), Ok(()));
    node.complete(created.id(), Ok(()));
    node.complete(
        rejected.id(),
        Err(OperationError::Rejected {
            message: "denied".into(),
        }),
    );
    host.close();
    drop((access, accepted, created, rejected, node));
    flush(service, persist).await;

    let (service, persist) = DocumentService::durable(Arc::clone(&store), 32)
        .await
        .unwrap();
    // Reseeding after a restart must not overwrite what the previous boot persisted.
    let node = service.mock_documents();
    node.seed(route("a", "written"), json!("seed"));
    node.seed(route("a", "removed"), json!("seed"));
    let after = observed(&service, &["a"]);
    assert_eq!(
        after.get(&route("a", "written")),
        Some(&FacetValue::Present(json!(2)))
    );
    assert_eq!(
        after.get(&route("a", "created")),
        Some(&FacetValue::Present(json!(3)))
    );
    assert_eq!(
        after.get(&route("a", "seeded")),
        Some(&FacetValue::Present(json!(1)))
    );
    assert_eq!(
        after.get(&route("a", "replaced")),
        Some(&FacetValue::Present(json!("external")))
    );
    // The removal was persisted, so the reseed lands as a fresh insert this boot.
    assert_eq!(
        after.get(&route("a", "removed")),
        Some(&FacetValue::Present(json!("seed")))
    );
    drop(node);
    flush(service, persist).await;
}

#[tokio::test]
async fn a_persisted_removal_stays_removed_after_a_rebuild() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (service, persist) = DocumentService::durable(Arc::clone(&store), 32)
        .await
        .unwrap();
    let node = service.mock_documents();
    node.seed(route("a", "gone"), json!(1));
    node.external_change(route("a", "gone"), None);
    drop(node);
    flush(service, persist).await;
    assert!(store.scan_prefix(b"").await.unwrap().is_empty());
    let (service, _persist) = DocumentService::durable(store, 32).await.unwrap();
    assert!(observed(&service, &["a"]).is_empty());
}

#[test]
fn route_keys_round_trip_and_stay_unambiguous() {
    let a = FacetRoute {
        document: "ab".into(),
        branch: "c".into(),
        tag: "".into(),
        key_id: "\u{0}é".into(),
    };
    let b = FacetRoute {
        document: "a".into(),
        branch: "bc".into(),
        tag: "".into(),
        key_id: "\u{0}é".into(),
    };
    assert_ne!(a.encode(), b.encode());
    assert_eq!(FacetRoute::decode(&a.encode()), a);
    assert_eq!(FacetRoute::decode(&b.encode()), b);
}

#[tokio::test]
async fn closing_persistence_drains_writes_queued_just_before() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (service, persist) = DocumentService::durable(Arc::clone(&store), 32)
        .await
        .unwrap();
    service
        .mock_documents()
        .external_change(route("a", "late"), Some(json!(7)));
    // The service stays alive: closing alone must end the future after it drains.
    service.close_persistence();
    persist.await;
    assert_eq!(store.scan_prefix(b"").await.unwrap().len(), 1);
}

#[tokio::test]
#[should_panic(expected = "document source changed after the composition shut down")]
async fn a_source_write_after_closing_persistence_panics() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (service, _persist) = DocumentService::durable(store, 32).await.unwrap();
    service.close_persistence();
    service.mock_documents().seed(route("a", "x"), json!(1));
}

#[tokio::test]
async fn persisted_resolves_once_earlier_changes_reach_the_store() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (service, persist) = DocumentService::durable(Arc::clone(&store), 32)
        .await
        .unwrap();
    // Nothing queued yet: already persisted, without the persist future running.
    service.persisted().await;
    service
        .mock_documents()
        .external_change(route("a", "one"), Some(json!(1)));
    let first = service.persisted();
    service
        .mock_documents()
        .external_change(route("a", "two"), Some(json!(2)));
    let both = service.persisted();
    let mut persist = Box::pin(persist);
    let mut first = Box::pin(first);
    let mut both = Box::pin(both);
    // Not applied until the persist future runs.
    assert_pending(first.as_mut());
    assert_pending(both.as_mut());
    assert_pending(persist.as_mut());
    // One drain applied both queued changes; each acknowledgement then resolves.
    first.await;
    both.await;
    assert_eq!(store.scan_prefix(b"").await.unwrap().len(), 2);
    // Volatile services have nothing to wait for.
    DocumentService::new(4).persisted().await;
}
