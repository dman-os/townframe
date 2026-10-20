use super::ActivationError;
use super::{WasmIncarnation, WasmStoreState, bindings::townframe::dayframe::documents as wit};
use clay_data::{
    DocumentAccess, DocumentError, DocumentRef, DocumentSnapshot, DocumentWatch, FacetMutation,
    FacetRoute, FacetSelection, FacetValue, Operation, OperationError, OperationReceipt,
    OperationStatus, WatchSelection,
};
use clay_host::{AccessError, Instance};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, watch};
use wasmtime::component::Resource;

pub struct WasmDocumentAccess(DocumentAccess);

pub struct WasmDocumentWatch {
    id: u64,
    watch: DocumentWatch,
    _cancel: watch::Sender<()>,
}

pub struct WasmOperation {
    operation: Operation,
    _cancel: watch::Sender<()>,
}

pub(super) enum Work {
    Watch {
        id: u64,
        watch: DocumentWatch,
        revision: u64,
        cancel: watch::Receiver<()>,
    },
    Operation {
        operation: Operation,
        revision: u64,
        cancel: watch::Receiver<()>,
    },
}

enum Delivery {
    Changed(wit::DocumentSnapshot),
    Lagged(u64),
    Completed(wit::OperationReceipt),
}

impl Work {
    async fn wait(mut self) -> Option<(Self, Delivery)> {
        let delivery = match &mut self {
            Self::Watch {
                id,
                watch,
                revision,
                cancel,
            } => {
                let next = tokio::select! {
                    biased;
                    _ = cancel.changed() => return None,
                    next = watch.next(*revision) => next,
                };
                match next {
                    Ok(snapshot) => {
                        *revision = snapshot.revision;
                        Delivery::Changed(snapshot_to_wire(*id, snapshot))
                    }
                    Err(DocumentError::SubscriptionLagged) => {
                        *revision = match watch.current() {
                            Ok(snapshot) => snapshot.revision,
                            Err(AccessError::Revoked) => return None,
                            Err(AccessError::Denied) => {
                                panic!("issued document watch lost its authority")
                            }
                        };
                        Delivery::Lagged(*id)
                    }
                    Err(DocumentError::Access(AccessError::Revoked)) => return None,
                    Err(DocumentError::Access(AccessError::Denied)) => {
                        panic!("issued document watch lost its authority")
                    }
                }
            }
            Self::Operation {
                operation,
                revision,
                cancel,
            } => {
                let next = tokio::select! {
                    biased;
                    _ = cancel.changed() => return None,
                    next = operation.next(*revision) => next.unwrap(),
                };
                Delivery::Completed(receipt_to_wire(next?))
            }
        };
        Some((self, delivery))
    }

    /// A guest resource handle owns the cancel sender. Dropping the handle after this work became
    /// ready but before delivery must still suppress the callback, so the guest never receives a
    /// snapshot or receipt for a handle it no longer holds.
    fn released(&self) -> bool {
        match self {
            Self::Watch { cancel, .. } | Self::Operation { cancel, .. } => {
                cancel.has_changed().is_err()
            }
        }
    }
}

pub(super) async fn drive(
    instance: Instance,
    incarnation: Arc<Mutex<Option<WasmIncarnation>>>,
    mut incoming: mpsc::Receiver<Work>,
) {
    let mut pending = FuturesUnordered::new();
    loop {
        let ready = tokio::select! {
            _ = instance.closed() => return,
            work = incoming.recv(), if pending.len() < WasmStoreState::MAX_PENDING_DELIVERIES => {
                let Some(work) = work else { return };
                pending.push(work.wait());
                continue;
            }
            ready = pending.next(), if !pending.is_empty() => ready.unwrap(),
        };
        let Some((work, delivery)) = ready else {
            continue;
        };
        if work.released() {
            continue;
        }
        let result = {
            let mut guard = incarnation.lock().unwrap();
            if instance.check().is_err() {
                return;
            }
            let Some(active) = guard.as_mut() else { return };
            active.refuel();
            let events = active.client.townframe_dayframe_document_events();
            let result = match delivery {
                Delivery::Changed(snapshot) => events.call_changed(&mut active.store, &snapshot),
                Delivery::Lagged(id) => events.call_lagged(&mut active.store, id),
                Delivery::Completed(receipt) => events.call_completed(&mut active.store, &receipt),
            };
            if result.is_err() {
                guard.take();
            }
            result
        };
        if result.is_err() {
            instance.close();
            return;
        }
        if matches!(work, Work::Watch { .. }) {
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
        let access = active
            .store
            .data_mut()
            .table
            .push(WasmDocumentAccess(document.access()))
            .map_err(|error| {
                ActivationError::Initialization(format!("granting {name}: {error}"))
            })?;
        active.store.data_mut().document_handles += 1;
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
        refreshing: matches!(snapshot.freshness, clay_data::Freshness::Refreshing),
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
        if self.document_handles >= Self::MAX_DOCUMENT_HANDLES {
            return Ok(Err(wit::DocumentError::Denied));
        }
        let permit = match self.work.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(_) => return Ok(Err(wit::DocumentError::Denied)),
        };
        let watch = match access.0.watch(selection_from_wire(selection)) {
            Ok(watch) => watch,
            Err(error) => return Ok(Err(access_error(error))),
        };
        let revision = match watch.current() {
            Ok(snapshot) => snapshot.revision,
            Err(error) => return Ok(Err(access_error(error))),
        };
        self.next_watch = self
            .next_watch
            .checked_add(1)
            .expect("document watch identity overflow");
        let id = self.next_watch;
        let (cancel, cancelled) = watch::channel(());
        let resource = self.table.push(WasmDocumentWatch {
            id,
            watch: watch.clone(),
            _cancel: cancel,
        })?;
        self.document_handles += 1;
        permit.send(Work::Watch {
            id,
            watch,
            revision,
            cancel: cancelled,
        });
        Ok(Ok(resource))
    }

    fn mutate(
        &mut self,
        access: Resource<WasmDocumentAccess>,
        route: wit::FacetRoute,
        mutation: wit::FacetMutation,
    ) -> wasmtime::Result<Result<Resource<WasmOperation>, wit::DocumentError>> {
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
        if self.document_handles >= Self::MAX_DOCUMENT_HANDLES {
            return Ok(Err(wit::DocumentError::Denied));
        }
        let permit = match self.work.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(_) => return Ok(Err(wit::DocumentError::Denied)),
        };
        let operation = match access.0.mutate(route_from_wire(route), mutation) {
            Ok(operation) => operation,
            Err(error) => return Ok(Err(access_error(error))),
        };
        let (cancel, cancelled) = watch::channel(());
        let resource = self.table.push(WasmOperation {
            operation: operation.clone(),
            _cancel: cancel,
        })?;
        self.document_handles += 1;
        permit.send(Work::Operation {
            operation,
            revision: 0,
            cancel: cancelled,
        });
        Ok(Ok(resource))
    }

    fn drop(&mut self, access: Resource<WasmDocumentAccess>) -> wasmtime::Result<()> {
        self.table.delete(access)?;
        self.document_handles -= 1;
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
            .watch
            .current()
            .map(|snapshot| snapshot_to_wire(entry.id, snapshot))
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
            .watch
            .select(selection_from_wire(selection))
            .map_err(access_error))
    }

    fn refresh(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
    ) -> wasmtime::Result<Result<(), wit::DocumentError>> {
        Ok(self
            .table
            .get(&resource)?
            .watch
            .refresh()
            .map(|_| ())
            .map_err(access_error))
    }

    fn drop(&mut self, resource: Resource<WasmDocumentWatch>) -> wasmtime::Result<()> {
        self.table.delete(resource)?.watch.close();
        self.document_handles -= 1;
        Ok(())
    }
}

impl wit::HostOperation for WasmStoreState {
    fn current(
        &mut self,
        resource: Resource<WasmOperation>,
    ) -> wasmtime::Result<Result<wit::OperationReceipt, wit::DocumentError>> {
        if let Err(error) = self.instance.check() {
            return Ok(Err(access_error(error)));
        }
        Ok(Ok(receipt_to_wire(
            self.table.get(&resource)?.operation.current(),
        )))
    }

    fn drop(&mut self, resource: Resource<WasmOperation>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        self.document_handles -= 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_data::{DocumentGrant, DocumentService};
    use clay_host::Host;
    use wit::{HostDocumentAccess, HostDocumentWatch, HostOperation};

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
        let (work, mut incoming) = mpsc::channel(1024);
        let mut state = WasmStoreState::new(instance.clone(), work);
        let access = state.table.push(WasmDocumentAccess(access)).unwrap().rep();
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
        let (_, delivery) = work.wait().await.unwrap();
        assert!(
            matches!(delivery, Delivery::Completed(wit::OperationReceipt {
            id, revision: 1,
            status: wit::OperationStatus::RolledBack(wit::OperationFailure::Transport(wit::TransportFailure { outcome_unknown: true, .. })),
        }) if id == pending.id)
        );
        HostDocumentWatch::drop(&mut state, watch).unwrap();
        assert!(watch_work.wait().await.is_none());
        instance.close();
        assert!(matches!(
            HostOperation::current(&mut state, Resource::new_borrow(operation_rep)).unwrap(),
            Err(wit::DocumentError::Revoked)
        ));
        HostOperation::drop(&mut state, operation).unwrap();
        assert_eq!(state.document_handles, 0);
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
        let (work, mut incoming) = mpsc::channel(1);
        let permit = work.clone().try_reserve_owned().unwrap();
        let mut state = WasmStoreState::new(instance, work);
        let access = state.table.push(WasmDocumentAccess(access)).unwrap().rep();
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
        assert_eq!(state.document_handles, 0);
        drop(permit);
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
        assert_eq!(receipt.id, 1);
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
