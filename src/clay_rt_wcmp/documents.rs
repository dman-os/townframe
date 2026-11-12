use super::ActivationError;
use super::WcmpIncarnation;
use super::WcmpStoreState;
use super::bindings::exports::townframe::dayframe::micro_lifecycle as lifecycle_wit;
pub use super::bindings::townframe::dayframe::documents as wit;
use super::first_poll;
use clay_docs::DocumentSnapshot;
use clay_docs::DatumMutation;
use clay_docs::DatumRoute;
use clay_docs::DatumSelection;
use clay_docs::DatumValue;
use clay_docs::OperationError;
use clay_docs::OperationReceipt;
use clay_docs::OperationStatus;
use clay_docs::WatchSelection;
pub use clay_guest_session::Access as WasmDocumentAccess;
use clay_guest_session::Delivery;
use clay_guest_session::DeliveryOutcome;
use clay_guest_session::InitialContext;
use clay_guest_session::InitializationFailure;
pub use clay_guest_session::Operation as WasmOperation;
pub use clay_guest_session::Watch as WasmDocumentWatch;
use clay_guest_session::Work;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::capabilities::admit_publications;
use clay_host::AccessError;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use wcmp::Resource;
use wcmp::ResourceAny;

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
    let outcome = match delivery {
        Delivery::Changed { id, snapshot } => first_poll(
            events.call_changed(&mut active.store, &snapshot_to_wire(id, snapshot)),
            &active.waker,
            "document-events.changed",
        ),
        Delivery::Lagged(id) => first_poll(
            events.call_lagged(&mut active.store, id),
            &active.waker,
            "document-events.lagged",
        ),
        Delivery::Completed(receipt) => first_poll(
            events.call_completed(&mut active.store, &receipt_to_wire(receipt)),
            &active.waker,
            "document-events.completed",
        ),
    };
    // A guest trap withdraws the incarnation, and the cause is kept for the supervisor to report.
    if let Err(trap) = outcome {
        guard.take();
        return DeliveryOutcome::Trapped(format!("guest trapped in a document delivery: {trap}"));
    }
    DeliveryOutcome::Delivered
}

/// Grant `context` into the guest's resource table, run `micro-lifecycle.initialize`, and take
/// over what it answered: the guest's `instance-root` and the publication lease of every
/// returned publication, keyed by name and admitted against what the manifest declares.
pub(super) async fn initialize(
    active: &mut WcmpIncarnation,
    context: InitialContext,
) -> Result<(ResourceAny, BTreeMap<String, PublishedCapability>), ActivationError> {
    let InitialContext {
        arguments,
        configs,
        collections,
        registrations,
        requirements,
        publications: declared,
        micro,
        activator,
    } = context;
    if let Some(activator) = activator {
        active.store.data().capabilities.attach_activator(activator);
    }
    if let Some(micro) = &micro {
        active
            .store
            .data()
            .capabilities
            .attach_own(micro, &declared);
    }
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
    let mut collection_bindings = Vec::with_capacity(collections.len());
    for (name, projection) in collections {
        let reader = active.store.data().collections.grant(projection)?;
        let reader = active
            .store
            .data_mut()
            .table
            .push(reader)
            .map_err(|error| {
                ActivationError::Initialization(format!("granting {name}: {error}"))
            })?;
        collection_bindings.push(super::collections::wit::CollectionBinding { name, reader });
    }
    let mut registration_bindings = Vec::with_capacity(registrations.len());
    for (name, grant) in registrations {
        let writer = active.store.data().collections.grant_writer(grant)?;
        let writer = active
            .store
            .data_mut()
            .table
            .push(writer)
            .map_err(|error| {
                ActivationError::Initialization(format!("granting registration {name}: {error}"))
            })?;
        registration_bindings
            .push(super::collections::registration_wit::RegistrationBinding { name, writer });
    }
    let mut requirement_bindings = Vec::with_capacity(requirements.len());
    for (name, publication) in requirements {
        let capability = active
            .store
            .data()
            .capabilities
            .mint(&publication)
            .map_err(|failure| {
                ActivationError::Initialization(format!("minting requirement {name}: {failure:?}"))
            })?;
        let capability = active
            .store
            .data_mut()
            .table
            .push(capability)
            .map_err(|error| {
                ActivationError::Initialization(format!("granting requirement {name}: {error}"))
            })?;
        requirement_bindings.push(lifecycle_wit::RequirementBinding { name, capability });
    }
    let context = lifecycle_wit::InitialContext {
        arguments,
        configs: bindings,
        collections: collection_bindings,
        registrations: registration_bindings,
        requirements: requirement_bindings,
    };
    let lifecycle = active.client.townframe_dayframe_micro_lifecycle();
    let initialized = active
        .store
        .run_concurrent(async |accessor| lifecycle.call_initialize(accessor, context).await)
        .await
        .and_then(|trapped| trapped)
        .map_err(|trap| {
            ActivationError::Initialization(format!("guest trapped during initialization: {trap}"))
        })?
        .map_err(|failure| {
            ActivationError::Refused(match failure {
                lifecycle_wit::InitializationError::InvalidConfiguration(message) => {
                    InitializationFailure::InvalidConfiguration(message)
                }
                lifecycle_wit::InitializationError::Unavailable(message) => {
                    InitializationFailure::Unavailable(message)
                }
                lifecycle_wit::InitializationError::Rejected(message) => {
                    InitializationFailure::Rejected(message)
                }
            })
        })?;
    let returned = initialized
        .publications
        .into_iter()
        .map(|publication| {
            let lease = active
                .store
                .data_mut()
                .table
                .delete(publication.capability)
                .expect("a published capability must be in the host resource table");
            (publication.name, lease)
        })
        .collect::<Vec<_>>();
    let publications =
        admit_publications(&declared, returned).map_err(ActivationError::Initialization)?;
    Ok((initialized.root, publications))
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
        datums: match selection.datums {
            wit::DatumSelection::Tags(tags) => DatumSelection::Tags(tags.into_iter().collect()),
            wit::DatumSelection::Keys(keys) => {
                DatumSelection::Keys(keys.into_iter().map(|key| (key.tag, key.slot)).collect())
            }
        },
    }
}

fn route_from_wire(route: wit::DatumRoute) -> DatumRoute {
    DatumRoute {
        document: route.document,
        branch: route.branch,
        tag: route.key.tag,
        slot: route.key.slot,
    }
}

fn snapshot_to_wire(id: u64, snapshot: DocumentSnapshot) -> wit::DocumentSnapshot {
    wit::DocumentSnapshot {
        watch_id: id,
        revision: snapshot.revision,
        datums: snapshot
            .datums
            .into_iter()
            .map(|(route, value)| wit::DatumEntry {
                route: wit::DatumRoute {
                    document: route.document,
                    branch: route.branch,
                    key: wit::DatumKey {
                        tag: route.tag,
                        slot: route.slot,
                    },
                },
                value: match value {
                    DatumValue::Loading => wit::DatumValue::Loading,
                    DatumValue::Absent => wit::DatumValue::Absent,
                    DatumValue::Present(value) => {
                        wit::DatumValue::Present(serde_json::to_string(&value).unwrap())
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
        route: wit::DatumRoute,
        mutation: wit::DatumMutation,
    ) -> wcmp::Result<Result<Resource<WasmOperation>, wit::DocumentError>> {
        if let Err(error) = self.documents.check() {
            return Ok(Err(access_error(error)));
        }
        let parsed = match mutation {
            wit::DatumMutation::Update(value) => {
                serde_json::from_str(&value).map(DatumMutation::Write)
            }
            wit::DatumMutation::Create(value) => {
                serde_json::from_str(&value).map(DatumMutation::Create)
            }
            wit::DatumMutation::Delete => Ok(DatumMutation::Delete),
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
