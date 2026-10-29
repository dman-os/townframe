use super::ActivationError;
use super::WcmpIncarnation;
use super::WcmpStoreState;
pub use super::bindings::townframe::dayframe::documents as wit;
use super::first_poll;
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
use clay_guest_session::DeliveryOutcome;
pub use clay_guest_session::Operation as WasmOperation;
pub use clay_guest_session::Watch as WasmDocumentWatch;
use clay_guest_session::Work;
use clay_host::AccessError;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use wcmp::Resource;

pub(super) fn deliver(
    incarnation: &Rc<RefCell<Option<WcmpIncarnation>>>,
    work: &Work,
    delivery: Delivery,
) -> DeliveryOutcome {
    if work.released() {
        return DeliveryOutcome::Skipped;
    }
    let mut guard = incarnation.borrow_mut();
    let Some(active) = guard.as_mut() else {
        return DeliveryOutcome::Stopped;
    };
    let events = active.client.townframe_dayframe_document_events();
    let result = match delivery {
        Delivery::Changed { id, snapshot } => {
            first_poll(events.call_changed(&mut active.store, &snapshot_to_wire(id, snapshot)))
        }
        Delivery::Lagged(id) => first_poll(events.call_lagged(&mut active.store, id)),
        Delivery::Completed(receipt) => {
            first_poll(events.call_completed(&mut active.store, &receipt_to_wire(receipt)))
        }
    };
    if result.is_err() {
        guard.take();
        return DeliveryOutcome::Trapped;
    }
    DeliveryOutcome::Delivered
}

pub(super) async fn activate(
    active: &mut WcmpIncarnation,
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
    active
        .client
        .townframe_dayframe_micro_lifecycle()
        .call_activate(&mut active.store, bindings)
        .await
        .map_err(|trap| {
            ActivationError::Initialization(format!("guest trapped during activation: {trap}"))
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

pub fn receipt_to_wire(receipt: OperationReceipt) -> wit::OperationReceipt {
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

impl wit::Host for WcmpStoreState {}

impl wit::HostDocumentAccess for WcmpStoreState {
    fn watch(
        &mut self,
        access: Resource<WasmDocumentAccess>,
        selection: wit::WatchSelection,
    ) -> wcmp::Result<Result<Resource<WasmDocumentWatch>, wit::DocumentError>> {
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
    ) -> wcmp::Result<Result<Resource<WasmOperation>, wit::DocumentError>> {
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

    fn drop(&mut self, access: Resource<WasmDocumentAccess>) -> wcmp::Result<()> {
        self.table.delete(access)?;
        Ok(())
    }
}

impl wit::HostDocumentWatch for WcmpStoreState {
    fn current(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
    ) -> wcmp::Result<Result<wit::DocumentSnapshot, wit::DocumentError>> {
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
    ) -> wcmp::Result<Result<(), wit::DocumentError>> {
        Ok(self
            .table
            .get(&resource)?
            .select(selection_from_wire(selection))
            .map_err(access_error))
    }

    fn refresh(
        &mut self,
        resource: Resource<WasmDocumentWatch>,
    ) -> wcmp::Result<Result<(), wit::DocumentError>> {
        Ok(self.table.get(&resource)?.refresh().map_err(access_error))
    }

    fn drop(&mut self, resource: Resource<WasmDocumentWatch>) -> wcmp::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl wit::HostOperation for WcmpStoreState {
    fn current(
        &mut self,
        resource: Resource<WasmOperation>,
    ) -> wcmp::Result<Result<wit::OperationReceipt, wit::DocumentError>> {
        Ok(self
            .table
            .get(&resource)?
            .current()
            .map(receipt_to_wire)
            .map_err(access_error))
    }

    fn drop(&mut self, resource: Resource<WasmOperation>) -> wcmp::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}
