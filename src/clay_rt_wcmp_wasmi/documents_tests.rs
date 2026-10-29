use clay_docs::DocumentGrant;
use clay_docs::DocumentService;
use clay_docs::OperationError;
use clay_guest_session::Delivery;
use clay_guest_session::Limits;
use clay_host::Host;
use clay_rt_wcmp::test_utils::WcmpStoreState;
use clay_rt_wcmp::test_utils::receipt_to_wire;
use clay_rt_wcmp::test_utils::wit;
use wcmp::Resource;
use wit::HostDocumentAccess;
use wit::HostDocumentWatch;
use wit::HostOperation;

fn route(document: &str) -> wit::FacetRoute {
    wit::FacetRoute {
        document: document.into(),
        branch: "main".into(),
        key: wit::FacetKey {
            tag: "config".into(),
            key_id: "count".into(),
        },
    }
}

#[tokio::test]
async fn granted_resources_validate_input_and_preserve_transport_outcomes() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let instance = host.start("wasm");
    let access = service
        .grant_documents(
            &host,
            &instance,
            vec![DocumentGrant {
                document: "allowed".into(),
                branch: "main".into(),
                facets: vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: "config".into(),
                    key_id: Some("count".into()),
                    read: true,
                    write: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let (mut state, mut incoming) =
        WcmpStoreState::new(host.clone(), instance.clone(), Limits::default()).unwrap();
    let access = state.documents.grant(access).unwrap();
    let access = state.table.push(access).unwrap().rep();
    let denied = HostDocumentAccess::mutate(
        &mut state,
        Resource::new_borrow(access),
        route("forged"),
        wit::FacetMutation::Update("1".into()),
    )
    .unwrap();
    assert!(matches!(denied, Err(wit::DocumentError::Denied)));
    let malformed = HostDocumentAccess::mutate(
        &mut state,
        Resource::new_borrow(access),
        route("allowed"),
        wit::FacetMutation::Update("{".into()),
    )
    .unwrap();
    assert!(matches!(
        malformed,
        Err(wit::DocumentError::InvalidValue(_))
    ));
    for mutation in [
        wit::FacetMutation::Create("1".into()),
        wit::FacetMutation::Delete,
    ] {
        assert!(matches!(
            HostDocumentAccess::mutate(
                &mut state,
                Resource::new_borrow(access),
                route("allowed"),
                mutation
            )
            .unwrap(),
            Err(wit::DocumentError::Denied)
        ));
    }
    assert!(incoming.try_recv().is_err());
    assert!(service.mock_documents().reads().is_empty());

    let watch = HostDocumentAccess::watch(
        &mut state,
        Resource::new_borrow(access),
        wit::WatchSelection {
            documents: vec!["allowed".into()],
            branch: "main".into(),
            facets: wit::FacetSelection::Keys(vec![wit::FacetKey {
                tag: "config".into(),
                key_id: "count".into(),
            }]),
        },
    )
    .unwrap()
    .unwrap();
    let watch_rep = watch.rep();
    let initial = HostDocumentWatch::current(&mut state, Resource::new_borrow(watch_rep))
        .unwrap()
        .unwrap();
    assert!(matches!(initial.facets[0].value, wit::FacetValue::Absent));
    let watch_work = incoming.recv().await.unwrap();
    let operation = HostDocumentAccess::mutate(
        &mut state,
        Resource::new_borrow(access),
        route("allowed"),
        wit::FacetMutation::Update("7".into()),
    )
    .unwrap()
    .unwrap();
    let operation_rep = operation.rep();
    let pending = HostOperation::current(&mut state, Resource::new_borrow(operation_rep))
        .unwrap()
        .unwrap();
    assert!(matches!(
        pending.status,
        wit::OperationStatus::LocallyApplied
    ));
    let work = incoming.recv().await.unwrap();
    service.mock_documents().complete(
        pending.id,
        Err(OperationError::Transport {
            message: "lost response".into(),
            outcome_unknown: true,
        }),
    );
    let (_, Delivery::Completed(receipt)) = work.wait().await.unwrap() else {
        panic!("operation must deliver a receipt");
    };
    assert!(matches!(receipt_to_wire(receipt), wit::OperationReceipt {
        id, revision: 1,
        status: wit::OperationStatus::RolledBack(wit::OperationFailure::Transport(wit::TransportFailure { outcome_unknown: true, .. })),
    } if id == pending.id));
    HostDocumentWatch::drop(&mut state, watch).unwrap();
    assert!(HostDocumentWatch::current(&mut state, Resource::new_borrow(watch_rep)).is_err());
    assert!(watch_work.wait().await.is_none());
    host.close_instance(&instance).unwrap();
    assert!(matches!(
        HostOperation::current(&mut state, Resource::new_borrow(operation_rep)).unwrap(),
        Err(wit::DocumentError::Revoked)
    ));
    assert!(matches!(
        HostDocumentAccess::mutate(
            &mut state,
            Resource::new_borrow(access),
            route("allowed"),
            wit::FacetMutation::Update("8".into()),
        )
        .unwrap(),
        Err(wit::DocumentError::Revoked)
    ));
    assert!(matches!(
        HostDocumentAccess::watch(
            &mut state,
            Resource::new_borrow(access),
            wit::WatchSelection {
                documents: vec!["allowed".into()],
                branch: "main".into(),
                facets: wit::FacetSelection::Tags(vec!["config".into()]),
            },
        )
        .unwrap(),
        Err(wit::DocumentError::Revoked)
    ));
    HostOperation::drop(&mut state, operation).unwrap();
    HostDocumentAccess::drop(&mut state, Resource::new_own(access)).unwrap();
    assert_eq!(state.documents.handle_count(), 0);
    host.close();
}

#[tokio::test]
async fn a_full_delivery_queue_rejects_before_creating_an_optimistic_write() {
    let host = Host::new();
    let service = DocumentService::new(32);
    let instance = host.start("bounded");
    let access = service
        .grant_documents(
            &host,
            &instance,
            vec![DocumentGrant {
                document: "allowed".into(),
                branch: "main".into(),
                facets: vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: "config".into(),
                    write: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let (mut state, mut incoming) = WcmpStoreState::new(
        host.clone(),
        instance,
        Limits {
            pending_deliveries: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let access = state.documents.grant(access).unwrap();
    let access = state.table.push(access).unwrap().rep();
    let first = HostDocumentAccess::mutate(
        &mut state,
        Resource::new_borrow(access),
        route("allowed"),
        wit::FacetMutation::Update("0".into()),
    )
    .unwrap()
    .unwrap();
    let first_receipt = HostOperation::current(&mut state, Resource::new_borrow(first.rep()))
        .unwrap()
        .unwrap();
    assert_eq!(first_receipt.id, 1);
    assert!(matches!(
        HostDocumentAccess::mutate(
            &mut state,
            Resource::new_borrow(access),
            route("allowed"),
            wit::FacetMutation::Update("1".into())
        )
        .unwrap(),
        Err(wit::DocumentError::Denied)
    ));
    assert_eq!(state.documents.handle_count(), 2);
    let first_work = incoming.recv().await.unwrap();
    HostOperation::drop(&mut state, first).unwrap();
    assert!(first_work.wait().await.is_none());
    service.mock_documents().complete(first_receipt.id, Ok(()));
    let operation = HostDocumentAccess::mutate(
        &mut state,
        Resource::new_borrow(access),
        route("allowed"),
        wit::FacetMutation::Update("2".into()),
    )
    .unwrap()
    .unwrap();
    let receipt = HostOperation::current(&mut state, Resource::new_borrow(operation.rep()))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.id, first_receipt.id + 1);
    service.mock_documents().complete(receipt.id, Ok(()));
    let (work, _) = incoming.recv().await.unwrap().wait().await.unwrap();
    assert!(!work.released());
    HostOperation::drop(&mut state, operation).unwrap();
    assert!(
        work.released(),
        "a receipt that became ready after its handle was dropped must not be delivered"
    );
    host.close();
}
