use super::ActivationError;
use super::WasmIncarnation;
use super::WasmStoreState;
use super::bindings::townframe::dayframe::documents as wit;
use clay_docs::DocumentRef;
use clay_docs::DocumentSnapshot;
use clay_docs::FacetMutation;
use clay_docs::FacetRoute;
use clay_docs::FacetSelection;
use clay_docs::FacetValue;
use clay_docs::OperationError;
use clay_docs::OperationReceipt;
use clay_docs::OperationStatus;
use clay_docs::WatchSelection;
pub use clay_guest_session::Access as WasmDocumentAccess;
use clay_guest_session::Delivery;
pub use clay_guest_session::Operation as WasmOperation;
pub use clay_guest_session::Watch as WasmDocumentWatch;
use clay_guest_session::Work;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use wasmtime::component::Resource;

pub(super) async fn drive(
    host: Host,
    instance: Instance,
    incarnation: Arc<Mutex<Option<WasmIncarnation>>>,
    mut incoming: mpsc::Receiver<Work>,
    pending_limit: usize,
) {
    let mut pending = FuturesUnordered::new();
    loop {
        let ready = tokio::select! {
            _ = host.await_close(&instance) => return,
            work = incoming.recv(), if pending.len() < pending_limit => {
                let Some(work) = work else { return };
                pending.push(work.wait());
                continue;
            }
            ready = pending.next(), if !pending.is_empty() => ready.unwrap(),
        };
        let Some((work, delivery)) = ready else {
            continue;
        };
        let repeat = !matches!(delivery, Delivery::Completed(_));
        let result = {
            let mut guard = incarnation.lock().unwrap();
            if work.released() {
                continue;
            }
            let Some(active) = guard.as_mut() else { return };
            active.refuel();
            let events = active.client.townframe_dayframe_document_events();
            let result = match delivery {
                Delivery::Changed { id, snapshot } => {
                    events.call_changed(&mut active.store, &snapshot_to_wire(id, snapshot))
                }
                Delivery::Lagged(id) => events.call_lagged(&mut active.store, id),
                Delivery::Completed(receipt) => {
                    events.call_completed(&mut active.store, &receipt_to_wire(receipt))
                }
            };
            if result.is_err() {
                guard.take();
            }
            result
        };
        if result.is_err() {
            host.close_instance(&instance).unwrap();
            return;
        }
        if repeat {
            pending.push(work.wait());
        }
    }
}

pub(super) fn activate(
    active: &mut WasmIncarnation,
    configs: BTreeMap<String, DocumentRef>,
) -> Result<(), ActivationError> {
    let mut bindings = Vec::with_capacity(configs.len());
    for (name, document) in configs {
        let access = active.store.data().documents.grant(document.access())?;
        let access = active
            .store
            .data_mut()
            .table
            .push(access)
            .map_err(|error| {
                ActivationError::Initialization(format!("granting {name}: {error}"))
            })?;
        bindings.push(wit::DocumentBinding {
            name,
            document: document.document_id().into(),
            branch: document.branch().into(),
            access,
        });
    }
    active.refuel();
    active
        .client
        .townframe_dayframe_micro_lifecycle()
        .call_activate(&mut active.store, &bindings)
        .map_err(|trap| {
            ActivationError::Initialization(format!("guest trapped during activation: {trap:#}"))
        })?
        .map_err(ActivationError::Initialization)
}

fn access_error(error: AccessError) -> wit::DocumentError {
    match error {
        AccessError::Denied => wit::DocumentError::Denied,
        AccessError::Revoked => wit::DocumentError::Revoked,
    }
}

fn selection_from_wire(selection: wit::WatchSelection) -> WatchSelection {
    WatchSelection {
        documents: selection.documents.into_iter().collect(),
        branch: selection.branch,
        facets: match selection.facets {
            wit::FacetSelection::Tags(tags) => FacetSelection::Tags(tags.into_iter().collect()),
            wit::FacetSelection::Keys(keys) => {
                FacetSelection::Keys(keys.into_iter().map(|key| (key.tag, key.key_id)).collect())
            }
        },
    }
}

fn route_from_wire(route: wit::FacetRoute) -> FacetRoute {
    FacetRoute {
        document: route.document,
        branch: route.branch,
        tag: route.key.tag,
        key_id: route.key.key_id,
    }
}

fn snapshot_to_wire(id: u64, snapshot: DocumentSnapshot) -> wit::DocumentSnapshot {
    wit::DocumentSnapshot {
        watch_id: id,
        revision: snapshot.revision,
        facets: snapshot
            .facets
            .into_iter()
            .map(|(route, value)| wit::FacetEntry {
                route: wit::FacetRoute {
                    document: route.document,
                    branch: route.branch,
                    key: wit::FacetKey {
                        tag: route.tag,
                        key_id: route.key_id,
                    },
                },
                value: match value {
                    FacetValue::Loading => wit::FacetValue::Loading,
                    FacetValue::Absent => wit::FacetValue::Absent,
                    FacetValue::Present(value) => {
                        wit::FacetValue::Present(serde_json::to_string(&value).unwrap())
                    }
                },
            })
            .collect(),
        pending: snapshot.pending.into_iter().collect(),
        refreshing: matches!(snapshot.freshness, clay_docs::Freshness::Refreshing),
        revoked: snapshot.revoked,
    }
}

fn receipt_to_wire(receipt: OperationReceipt) -> wit::OperationReceipt {
    wit::OperationReceipt {
        id: receipt.id,
        revision: receipt.revision,
        status: match receipt.status {
            OperationStatus::LocallyApplied => wit::OperationStatus::LocallyApplied,
            OperationStatus::Accepted => wit::OperationStatus::Accepted,
            OperationStatus::RolledBack(error) => wit::OperationStatus::RolledBack(match error {
                OperationError::Rejected { message } => wit::OperationFailure::Rejected(message),
                OperationError::Transport {
                    message,
                    outcome_unknown,
                } => wit::OperationFailure::Transport(wit::TransportFailure {
                    message,
                    outcome_unknown,
                }),
            }),
        },
    }
}

impl wit::Host for WasmStoreState {}

impl wit::HostDocumentAccess for WasmStoreState {
    fn watch(
        &mut self,
        access: Resource<WasmDocumentAccess>,
        selection: wit::WatchSelection,
    ) -> wasmtime::Result<Result<Resource<WasmDocumentWatch>, wit::DocumentError>> {
        let access = self.table.get(&access)?;
        let watch = match access.watch(selection_from_wire(selection)) {
            Ok(watch) => watch,
            Err(error) => return Ok(Err(access_error(error))),
        };
        Ok(Ok(self.table.push(watch)?))
    }

    fn mutate(
        &mut self,
        access: Resource<WasmDocumentAccess>,
        route: wit::FacetRoute,
        mutation: wit::FacetMutation,
    ) -> wasmtime::Result<Result<Resource<WasmOperation>, wit::DocumentError>> {
        if let Err(error) = self.documents.check() {
            return Ok(Err(access_error(error)));
        }
        let parsed = match mutation {
            wit::FacetMutation::Update(value) => {
                serde_json::from_str(&value).map(FacetMutation::Write)
            }
            wit::FacetMutation::Create(value) => {
                serde_json::from_str(&value).map(FacetMutation::Create)
            }
            wit::FacetMutation::Delete => Ok(FacetMutation::Delete),
        };
        let mutation = match parsed {
            Ok(mutation) => mutation,
            Err(error) => return Ok(Err(wit::DocumentError::InvalidValue(error.to_string()))),
        };
        let access = self.table.get(&access)?;
        let operation = match access.mutate(route_from_wire(route), mutation) {
            Ok(operation) => operation,
            Err(error) => return Ok(Err(access_error(error))),
        };
        Ok(Ok(self.table.push(operation)?))
    }

    fn drop(&mut self, access: Resource<WasmDocumentAccess>) -> wasmtime::Result<()> {
        self.table.delete(access)?;
        Ok(())
    }
}

impl wit::HostDocumentWatch for WasmStoreState {
    fn current(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
    ) -> wasmtime::Result<Result<wit::DocumentSnapshot, wit::DocumentError>> {
        let entry = self.table.get(&resource)?;
        Ok(entry
            .current()
            .map(|snapshot| snapshot_to_wire(entry.id(), snapshot))
            .map_err(access_error))
    }

    fn select(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
        selection: wit::WatchSelection,
    ) -> wasmtime::Result<Result<(), wit::DocumentError>> {
        Ok(self
            .table
            .get(&resource)?
            .select(selection_from_wire(selection))
            .map_err(access_error))
    }

    fn refresh(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
    ) -> wasmtime::Result<Result<(), wit::DocumentError>> {
        Ok(self.table.get(&resource)?.refresh().map_err(access_error))
    }

    fn drop(&mut self, resource: Resource<WasmDocumentWatch>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl wit::HostOperation for WasmStoreState {
    fn current(
        &mut self,
        resource: Resource<WasmOperation>,
    ) -> wasmtime::Result<Result<wit::OperationReceipt, wit::DocumentError>> {
        Ok(self
            .table
            .get(&resource)?
            .current()
            .map(receipt_to_wire)
            .map_err(access_error))
    }

    fn drop(&mut self, resource: Resource<WasmOperation>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_docs::DocumentGrant;
    use clay_docs::DocumentService;
    use clay_guest_session::Limits;
    use clay_host::Host;
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
            WasmStoreState::new(host.clone(), instance.clone(), Limits::default()).unwrap();
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
        let (mut state, mut incoming) = WasmStoreState::new(
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
}
