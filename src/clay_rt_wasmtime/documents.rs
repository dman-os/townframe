use super::ActivationError;
use super::WasmIncarnation;
use super::WasmStoreState;
use super::bindings::exports::townframe::dayframe::micro_lifecycle as lifecycle_wit;
use super::bindings::townframe::dayframe::collection_registration as registration_wit;
use super::bindings::townframe::dayframe::collections as collection_wit;
use super::bindings::townframe::dayframe::documents as wit;
use super::first_poll;
use super::trap_reason;
use super::wasmtime;
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
use clay_guest_session::InitialContext;
use clay_guest_session::InitializationFailure;
pub use clay_guest_session::Operation as WasmOperation;
pub use clay_guest_session::Watch as WasmDocumentWatch;
use clay_guest_session::Work;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::capabilities::admit_publications;
use clay_host::AccessError;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use wasmtime::component::Resource;
use wasmtime::component::ResourceAny;

pub(super) fn deliver(
    incarnation: &Arc<Mutex<Option<WasmIncarnation>>>,
    work: &Work,
    delivery: Delivery,
) -> clay_guest_session::DeliveryOutcome {
    use clay_guest_session::DeliveryOutcome;
    if work.released() {
        return DeliveryOutcome::Skipped;
    }
    let mut guard = incarnation.lock().unwrap();
    let Some(active) = guard.as_mut() else {
        return DeliveryOutcome::Stopped;
    };
    active.refuel();
    let events = active.client.townframe_dayframe_document_events();
    let trapped = match delivery {
        Delivery::Changed { id, snapshot } => first_poll(
            events.call_changed(&mut active.store, &snapshot_to_wire(id, snapshot)),
            &active.waker,
        ),
        Delivery::Lagged(id) => {
            first_poll(events.call_lagged(&mut active.store, id), &active.waker)
        }
        Delivery::Completed(receipt) => first_poll(
            events.call_completed(&mut active.store, &receipt_to_wire(receipt)),
            &active.waker,
        ),
    };
    // A suspension (Suspended) or a guest trap both withdraw the incarnation: document
    // callbacks are the same class of contract violation as a presentation call that fails.
    let cause = match trapped {
        Ok(Ok(())) => None,
        Ok(Err(trap)) => Some(format!(
            "guest trapped in a document delivery: {}",
            trap_reason(&trap)
        )),
        Err(super::Suspended) => Some("a document delivery suspended on its first poll".to_owned()),
    };
    if let Some(cause) = cause {
        guard.take();
        return DeliveryOutcome::Trapped(cause);
    }
    DeliveryOutcome::Delivered
}

/// Grant `context` into the guest's resource table, run `micro-lifecycle.initialize`, and take
/// over what it answered: the guest's `instance-root` and the publication lease of every
/// returned publication, keyed by name and admitted against what the manifest declares.
pub(super) async fn initialize(
    active: &mut WasmIncarnation,
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
                ActivationError::Initialization(format!("granting collection {name}: {error}"))
            })?;
        collection_bindings.push(collection_wit::CollectionBinding { name, reader });
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
        registration_bindings.push(registration_wit::RegistrationBinding { name, writer });
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
    active.refuel();
    let lifecycle = active.client.townframe_dayframe_micro_lifecycle();
    let initialized = active
        .store
        .run_concurrent(async |accessor| lifecycle.call_initialize(accessor, context).await)
        .await
        .and_then(|trapped| trapped)
        .map_err(|trap| {
            ActivationError::Initialization(format!(
                "guest trapped during initialization: {}",
                trap_reason(&trap)
            ))
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
    let returned = initialized.publications.into_iter().map(|publication| {
        let lease = active
            .store
            .data_mut()
            .table
            .delete(publication.capability)
            .expect("a published capability must be in the host resource table");
        (publication.name, lease)
    });
    let publications = admit_publications(&declared, returned.collect::<Vec<_>>())
        .map_err(ActivationError::Initialization)?;
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
        route: wit::DatumRoute,
        mutation: wit::DatumMutation,
    ) -> wasmtime::Result<Result<Resource<WasmOperation>, wit::DocumentError>> {
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

    fn route(document: &str) -> wit::DatumRoute {
        wit::DatumRoute {
            document: document.into(),
            branch: "main".into(),
            key: wit::DatumKey {
                tag: "config".into(),
                slot: "count".into(),
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
                    datums: vec![clay_docs::DatumAccess {
                        tag: "config".into(),
                        slot: Some("count".into()),
                        read: true,
                        write: true,
                        ..Default::default()
                    }],
                }],
            )
            .unwrap();
        let (mut state, mut incoming) =
            WasmStoreState::detached(host.clone(), instance.clone(), Limits::default()).unwrap();
        let access = state.documents.grant(access).unwrap();
        let access = state.table.push(access).unwrap().rep();
        let denied = HostDocumentAccess::mutate(
            &mut state,
            Resource::new_borrow(access),
            route("forged"),
            wit::DatumMutation::Update("1".into()),
        )
        .unwrap();
        assert!(matches!(denied, Err(wit::DocumentError::Denied)));
        let malformed = HostDocumentAccess::mutate(
            &mut state,
            Resource::new_borrow(access),
            route("allowed"),
            wit::DatumMutation::Update("{".into()),
        )
        .unwrap();
        assert!(matches!(
            malformed,
            Err(wit::DocumentError::InvalidValue(_))
        ));
        for mutation in [
            wit::DatumMutation::Create("1".into()),
            wit::DatumMutation::Delete,
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
                datums: wit::DatumSelection::Keys(vec![wit::DatumKey {
                    tag: "config".into(),
                    slot: "count".into(),
                }]),
            },
        )
        .unwrap()
        .unwrap();
        let watch_rep = watch.rep();
        let initial = HostDocumentWatch::current(&mut state, Resource::new_borrow(watch_rep))
            .unwrap()
            .unwrap();
        assert!(matches!(initial.datums[0].value, wit::DatumValue::Absent));
        let watch_work = incoming.recv().await.unwrap();
        let operation = HostDocumentAccess::mutate(
            &mut state,
            Resource::new_borrow(access),
            route("allowed"),
            wit::DatumMutation::Update("7".into()),
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
                wit::DatumMutation::Update("8".into()),
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
                    datums: wit::DatumSelection::Tags(vec!["config".into()]),
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
                    datums: vec![clay_docs::DatumAccess {
                        tag: "config".into(),
                        write: true,
                        ..Default::default()
                    }],
                }],
            )
            .unwrap();
        let (mut state, mut incoming) = WasmStoreState::detached(
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
            wit::DatumMutation::Update("0".into()),
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
                wit::DatumMutation::Update("1".into())
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
            wit::DatumMutation::Update("2".into()),
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
