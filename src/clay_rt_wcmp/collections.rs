use super::IncarnationWake;
use super::WcmpComponentRef;
use super::WcmpStoreState;
pub use super::bindings::townframe::dayframe::capability_bank as cap_wit;
pub use super::bindings::townframe::dayframe::collection_registration as registration_wit;
pub use super::bindings::townframe::dayframe::collections as wit;
use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::Nsid;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::RowEvidence;
use clay_collection::Stamp;
use clay_collection::Stamped;
use clay_collection::Version;
use clay_collection::WriteBasis;
use clay_collection::WriterId;
use clay_guest_session::capabilities::CallBinding;
use clay_guest_session::capabilities::CallError;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityRef;
use clay_guest_session::capabilities::GuestCallTarget;
use clay_guest_session::capabilities::PresentationComponent;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::capabilities::RefStatus;
use clay_guest_session::collections::BytesPage;
use clay_guest_session::collections::CollectionReader;
use clay_guest_session::collections::CollectionValue;
use clay_guest_session::collections::ProjectionReader;
use clay_guest_session::collections::ReadFailure;
use clay_guest_session::collections::RegistrationWriter;
use clay_guest_session::collections::WriteFailure;
use std::sync::Arc;
use std::time::Duration;
use wcmp::Accessor;
use wcmp::HasSelf;
use wcmp::Resource;

impl wit::Host for WcmpStoreState {}

impl<T: 'static> wit::HostCollectionReaderWithStore<T> for HasSelf<WcmpStoreState> {
    async fn acquire(
        accessor: &Accessor<T, Self>,
        reader: Resource<CollectionReader>,
        selector: Vec<u8>,
        resume: Option<Vec<u8>>,
    ) -> wcmp::Result<Result<Resource<ProjectionReader>, wit::AcquireError>> {
        accessor.with(|mut access| {
            let state = access.get();
            let lease = match state
                .table
                .get(&reader)?
                .acquire(selector, resume.map(Cursor))
            {
                Ok(lease) => lease,
                Err(error) => return Ok(Err(acquire_error_to_wire(error))),
            };
            Ok(Ok(state.table.push(lease)?))
        })
    }
}

impl wit::HostCollectionReader for WcmpStoreState {
    fn drop(&mut self, reader: Resource<CollectionReader>) -> wcmp::Result<()> {
        self.table.delete(reader)?;
        Ok(())
    }
}

impl<T: 'static> wit::HostProjectionLeaseWithStore<T> for HasSelf<WcmpStoreState> {
    async fn read(
        accessor: &Accessor<T, Self>,
        lease: Resource<ProjectionReader>,
        committed: Option<Vec<u8>>,
        limits: wit::ReadLimits,
    ) -> wcmp::Result<Result<wit::Page, wit::ReadError>> {
        let (reader, max_poll_timeout, wake) = accessor.with(|mut access| {
            let state = access.get();
            Ok::<_, wcmp::Error>((
                state.table.get(&lease)?.clone(),
                state.collection_limits.max_poll_timeout,
                state.collection_wake.clone(),
            ))
        })?;
        let timeout = Duration::from_millis(limits.poll_timeout_ms.into()).min(max_poll_timeout);
        let read = reader.read(
            committed.map(Cursor),
            ReadLimits {
                max_transactions: limits.max_transactions,
                max_changes: limits.max_changes,
            },
            sleep(timeout),
        );
        let result = match wake {
            Some(wake) => wake.drive(read).await,
            None => read.await,
        };
        match result {
            Ok(page) => accessor.with(|mut access| Ok(Ok(page_to_wire(access.get(), page)?))),
            Err(ReadFailure::Read(error)) => Ok(Err(read_error_to_wire(error))),
            Err(ReadFailure::Busy) => Err(wcmp::Error::Internal {
                message: "collection projection read is already in progress".into(),
            }),
            Err(ReadFailure::PageLimit) => Err(wcmp::Error::Internal {
                message: "collection projection page exceeds the configured byte limit".into(),
            }),
        }
    }
}

impl wit::HostProjectionLease for WcmpStoreState {
    fn drop(&mut self, lease: Resource<ProjectionReader>) -> wcmp::Result<()> {
        self.table.get(&lease)?.clone().cancel();
        self.table.delete(lease)?;
        Ok(())
    }
}

impl registration_wit::Host for WcmpStoreState {}

impl registration_wit::HostRegistrationWriter for WcmpStoreState {
    fn drop(&mut self, writer: Resource<RegistrationWriter>) -> wcmp::Result<()> {
        self.table.delete(writer)?;
        Ok(())
    }
}

impl<T: 'static> registration_wit::HostRegistrationWriterWithStore<T> for HasSelf<WcmpStoreState> {
    async fn put(
        accessor: &Accessor<T, Self>,
        writer: Resource<RegistrationWriter>,
        key: Vec<u8>,
        basis: registration_wit::WriteBasis,
        value: registration_wit::WrittenValue,
    ) -> wcmp::Result<Result<wit::Stamp, registration_wit::MutationError>> {
        accessor.with(|mut access| {
            let state = access.get();
            let value = value_from_wire(state, value)?;
            let writer = state.table.get(&writer)?;
            Ok(value
                .and_then(|value| writer.put_value(key, basis_from_wire(basis), value))
                .map(stamp_to_wire)
                .map_err(write_failure_to_wire))
        })
    }

    async fn delete(
        accessor: &Accessor<T, Self>,
        writer: Resource<RegistrationWriter>,
        key: Vec<u8>,
        basis: registration_wit::WriteBasis,
    ) -> wcmp::Result<Result<wit::Stamp, registration_wit::MutationError>> {
        accessor.with(|mut access| {
            let writer = access.get().table.get(&writer)?;
            Ok(writer
                .delete(key, basis_from_wire(basis))
                .map(stamp_to_wire)
                .map_err(write_failure_to_wire))
        })
    }
}

fn write_failure_to_wire(failure: WriteFailure) -> registration_wit::MutationError {
    match failure {
        WriteFailure::Conflict { current } => {
            registration_wit::MutationError::Conflict(current.map(stamp_to_wire))
        }
        WriteFailure::Revoked => registration_wit::MutationError::Revoked,
        WriteFailure::KeyTooLarge => registration_wit::MutationError::KeyTooLarge,
        WriteFailure::ValueTooLarge => registration_wit::MutationError::ValueTooLarge,
        WriteFailure::RowLimit => registration_wit::MutationError::RowLimit,
        WriteFailure::InvalidAttachment => registration_wit::MutationError::InvalidAttachment,
        WriteFailure::CapabilityLimit => registration_wit::MutationError::CapabilityLimit,
    }
}

fn basis_from_wire(basis: registration_wit::WriteBasis) -> WriteBasis {
    match basis {
        registration_wit::WriteBasis::Concurrent => WriteBasis::Concurrent,
        registration_wit::WriteBasis::Observed(stamp) => {
            WriteBasis::Observed(stamp_from_wire(stamp))
        }
        registration_wit::WriteBasis::CompareAndSet(expected) => {
            WriteBasis::CompareAndSet(expected.map(stamp_from_wire))
        }
    }
}

fn stamp_from_wire(stamp: wit::Stamp) -> Stamp {
    Stamp {
        lamport: stamp.lamport,
        writer: WriterId::from_encoded(stamp.writer),
    }
}

fn stamp_to_wire(stamp: Stamp) -> wit::Stamp {
    wit::Stamp {
        lamport: stamp.lamport,
        writer: stamp.writer.as_bytes().to_vec(),
    }
}

fn version_to_wire(version: Version) -> wit::Version {
    wit::Version {
        incarnation: version.incarnation,
        sequence: version.seq,
    }
}

fn acquire_error_to_wire(error: AcquireError) -> wit::AcquireError {
    match error {
        AcquireError::ResetRequired => wit::AcquireError::ResetRequired,
        AcquireError::SourceUnavailable => wit::AcquireError::SourceUnavailable,
        AcquireError::Revoked => wit::AcquireError::Revoked,
        AcquireError::InvalidSelector => wit::AcquireError::InvalidSelector,
        AcquireError::UnauthorizedSelector => wit::AcquireError::UnauthorizedSelector,
    }
}

fn read_error_to_wire(error: ReadError) -> wit::ReadError {
    match error {
        ReadError::ResetRequired => wit::ReadError::ResetRequired,
        ReadError::SourceUnavailable => wit::ReadError::SourceUnavailable,
        ReadError::Revoked => wit::ReadError::Revoked,
    }
}

fn page_to_wire(state: &mut WcmpStoreState, page: BytesPage) -> wcmp::Result<wit::Page> {
    let mut transactions = Vec::with_capacity(page.transactions.len());
    for transaction in page.transactions {
        let mut changes = Vec::with_capacity(transaction.changes.len());
        for change in transaction.changes {
            changes.push(change_to_wire(state, change)?);
        }
        transactions.push(wit::Transaction { changes });
    }
    Ok(wit::Page {
        transactions,
        through: page.through.0,
        bootstrap_complete: page.bootstrap_complete,
    })
}

fn change_to_wire(
    state: &mut WcmpStoreState,
    change: Change<Vec<u8>, Stamped<Option<CollectionValue>>>,
) -> wcmp::Result<wit::Change> {
    Ok(match change {
        Change::Upsert {
            key,
            row_version,
            value,
            evidence,
            write_context,
        } => {
            let Stamped { stamp, value } = value;
            let value = match value {
                None => None,
                Some(value) => {
                    let mut capabilities = Vec::with_capacity(value.attachments.len());
                    for attachment in &value.attachments {
                        let reference =
                            state
                                .capabilities
                                .mint_attachment(attachment)
                                .map_err(|failure| wcmp::Error::Internal {
                                    message: format!("{failure:?}"),
                                })?;
                        match state.table.push(reference) {
                            Ok(resource) => capabilities.push(resource),
                            Err(error) => {
                                for resource in capabilities {
                                    state.table.delete(resource)?;
                                }
                                return Err(error.into());
                            }
                        }
                    }
                    Some(wit::EncodedValue {
                        body: value.body,
                        capabilities,
                    })
                }
            };
            wit::Change::Upsert(wit::Upsert {
                key: wit::EncodedValue {
                    body: key,
                    capabilities: Vec::new(),
                },
                row_version: version_to_wire(row_version),
                stamp: stamp_to_wire(stamp),
                value,
                evidence: match evidence {
                    RowEvidence::Authoritative => wit::RowEvidence::Authoritative,
                    RowEvidence::Pending => wit::RowEvidence::Pending,
                },
                write_context,
            })
        }
        Change::Remove { key, row_version } => wit::Change::Remove(wit::Removal {
            key: wit::EncodedValue {
                body: key,
                capabilities: Vec::new(),
            },
            row_version: version_to_wire(row_version),
        }),
    })
}

/// The value a guest wrote, its reference attachments taken out of the resource table and its
/// own-publication attachments turned into the endpoints of the micro's declared publications.
/// A name the manifest does not declare is an invalid attachment, reported to the guest as the
/// write's failure, after every reference of the value was released.
fn value_from_wire(
    state: &mut WcmpStoreState,
    value: registration_wit::WrittenValue,
) -> wcmp::Result<Result<CollectionValue, WriteFailure>> {
    let mut attachments = Vec::with_capacity(value.attachments.len());
    let mut invalid = false;
    for attachment in value.attachments {
        match attachment {
            registration_wit::RowAttachment::Reference(reference) => {
                attachments.push(state.table.get(&reference)?.attachment());
                state.table.delete(reference)?;
            }
            registration_wit::RowAttachment::OwnPublication(name) => {
                match state.capabilities.own_endpoint(&name) {
                    Some(endpoint) => attachments.push(endpoint),
                    None => invalid = true,
                }
            }
        }
    }
    Ok(if invalid {
        Err(WriteFailure::InvalidAttachment)
    } else {
        Ok(CollectionValue {
            body: value.body,
            attachments,
        })
    })
}

fn call_error(error: CallError) -> cap_wit::CallError {
    match error {
        CallError::Capability(failure) => cap_wit::CallError::Capability(capability_error(failure)),
        CallError::Provider(message) => cap_wit::CallError::Provider(message),
    }
}

fn capability_error(failure: CapabilityFailure) -> cap_wit::CapabilityError {
    match failure {
        CapabilityFailure::ContractMismatch => cap_wit::CapabilityError::ContractMismatch,
        CapabilityFailure::Revoked => cap_wit::CapabilityError::Revoked,
        CapabilityFailure::LimitExceeded => cap_wit::CapabilityError::LimitExceeded,
        CapabilityFailure::Unavailable => cap_wit::CapabilityError::Unavailable,
    }
}

impl cap_wit::Host for WcmpStoreState {
    fn publish_presentation(
        &mut self,
        name: String,
    ) -> wcmp::Result<Result<Resource<PublishedCapability>, cap_wit::CapabilityError>> {
        match self.capabilities.publish_presentation(&name) {
            Ok(lease) => Ok(Ok(self.table.push(lease)?)),
            Err(failure) => Ok(Err(capability_error(failure))),
        }
    }

    fn publish_call(
        &mut self,
        name: String,
        contract: String,
    ) -> wcmp::Result<Result<Resource<PublishedCapability>, cap_wit::CapabilityError>> {
        let target = Arc::new(GuestCallTarget::new(name.clone(), self.calls.clone()));
        // The guest is untrusted: a contract string that is not an NSID cannot be published.
        let Ok(contract) = Nsid::parse(&contract) else {
            return Ok(Err(capability_error(CapabilityFailure::ContractMismatch)));
        };
        match self.capabilities.publish_call(&name, contract, target) {
            Ok(lease) => Ok(Ok(self.table.push(lease)?)),
            Err(failure) => Ok(Err(capability_error(failure))),
        }
    }
}

impl cap_wit::HostPublishedCapability for WcmpStoreState {
    fn reference(
        &mut self,
        lease: Resource<PublishedCapability>,
    ) -> wcmp::Result<Result<Resource<CapabilityRef>, cap_wit::CapabilityError>> {
        match self.table.get(&lease)?.reference() {
            Ok(reference) => Ok(Ok(self.table.push(reference)?)),
            Err(failure) => Ok(Err(capability_error(failure))),
        }
    }

    fn drop(&mut self, lease: Resource<PublishedCapability>) -> wcmp::Result<()> {
        self.table.delete(lease)?;
        Ok(())
    }
}

impl cap_wit::HostCapabilityRef for WcmpStoreState {
    fn status(&mut self, reference: Resource<CapabilityRef>) -> wcmp::Result<cap_wit::RefStatus> {
        Ok(match self.table.get(&reference)?.status() {
            RefStatus::Live => cap_wit::RefStatus::Live,
            RefStatus::Dormant => cap_wit::RefStatus::Dormant,
            RefStatus::Disabled => cap_wit::RefStatus::Disabled,
            RefStatus::Faulted => cap_wit::RefStatus::Faulted,
            RefStatus::Unavailable => cap_wit::RefStatus::Unavailable,
            RefStatus::Revoked => cap_wit::RefStatus::Revoked,
        })
    }

    fn drop(&mut self, reference: Resource<CapabilityRef>) -> wcmp::Result<()> {
        self.table.delete(reference)?;
        Ok(())
    }
}

/// Take a second handle to the table entry `resource` names, so the host import can await a
/// redemption that may activate a provider while the store (and the guest's own handle) stay
/// free to move on. The await is wrapped in the incarnation's wake so that the activation
/// finishing re-drives the store.
fn shared<T: 'static, R: 'static>(
    accessor: &Accessor<T, HasSelf<WcmpStoreState>>,
    resource: &Resource<R>,
    share: impl FnOnce(&R) -> R,
) -> (R, Option<Arc<IncarnationWake>>) {
    accessor.with(|mut access| {
        let state = access.get();
        (
            share(state.table.get(resource).unwrap()),
            state.collection_wake.clone(),
        )
    })
}

async fn awaited<F: std::future::Future>(
    wake: Option<Arc<IncarnationWake>>,
    pending: F,
) -> F::Output {
    match wake {
        Some(wake) => wake.drive(pending).await,
        None => pending.await,
    }
}

impl<T: 'static> cap_wit::HostCapabilityRefWithStore<T> for HasSelf<WcmpStoreState> {
    async fn call(
        accessor: &Accessor<T, Self>,
        reference: Resource<CapabilityRef>,
        request: Vec<u8>,
    ) -> wcmp::Result<Result<Vec<u8>, cap_wit::CallError>> {
        let (reference, wake) = shared(accessor, &reference, CapabilityRef::share);
        Ok(awaited(wake, reference.call(request))
            .await
            .map_err(call_error))
    }

    async fn retain_call(
        accessor: &Accessor<T, Self>,
        reference: Resource<CapabilityRef>,
    ) -> wcmp::Result<Result<Resource<CallBinding>, cap_wit::CapabilityError>> {
        let (reference, wake) = shared(accessor, &reference, CapabilityRef::share);
        match awaited(wake, reference.retain_call()).await {
            Ok(binding) => accessor.with(|mut access| Ok(Ok(access.get().table.push(binding)?))),
            Err(failure) => Ok(Err(capability_error(failure))),
        }
    }

    async fn redeem_presentation(
        accessor: &Accessor<T, Self>,
        reference: Resource<CapabilityRef>,
    ) -> wcmp::Result<Result<Resource<PresentationComponent>, cap_wit::CapabilityError>> {
        let (reference, wake) = shared(accessor, &reference, CapabilityRef::share);
        match awaited(wake, reference.redeem_presentation()).await {
            Ok(component) => {
                accessor.with(|mut access| Ok(Ok(access.get().table.push(component)?)))
            }
            Err(failure) => Ok(Err(capability_error(failure))),
        }
    }
}

impl cap_wit::HostCallBinding for WcmpStoreState {
    fn drop(&mut self, binding: Resource<CallBinding>) -> wcmp::Result<()> {
        self.table.delete(binding)?;
        Ok(())
    }
}

impl<T: 'static> cap_wit::HostCallBindingWithStore<T> for HasSelf<WcmpStoreState> {
    async fn call(
        accessor: &Accessor<T, Self>,
        binding: Resource<CallBinding>,
        request: Vec<u8>,
    ) -> wcmp::Result<Result<Vec<u8>, cap_wit::CallError>> {
        let (binding, wake) = shared(accessor, &binding, CallBinding::share);
        Ok(awaited(wake, binding.call(request))
            .await
            .map_err(call_error))
    }
}

impl cap_wit::HostPresentationComponent for WcmpStoreState {
    fn component(
        &mut self,
        proxy: Resource<PresentationComponent>,
    ) -> wcmp::Result<Result<Resource<WcmpComponentRef>, cap_wit::CapabilityError>> {
        let component = match self.table.get(&proxy)?.component() {
            Ok(component) => component,
            Err(failure) => return Ok(Err(capability_error(failure))),
        };
        if self.presentation.admit_components(1).is_err() {
            return Ok(Err(cap_wit::CapabilityError::LimitExceeded));
        }
        Ok(Ok(self.table.push(WcmpComponentRef(component))?))
    }

    fn drop(&mut self, proxy: Resource<PresentationComponent>) -> wcmp::Result<()> {
        self.table.delete(proxy)?;
        Ok(())
    }
}

async fn sleep(duration: Duration) {
    #[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
    {
        let promise = js_sys::Promise::new(&mut |resolve, _reject| {
            let millis = duration.as_millis().min(i32::MAX as u128) as i32;
            match web_sys::window() {
                Some(window) => {
                    drop(
                        window.set_timeout_with_callback_and_timeout_and_arguments_0(
                            &resolve, millis,
                        ),
                    );
                }
                None => {
                    drop(resolve.call0(&resolve));
                }
            }
        });
        drop(wasm_bindgen_futures::JsFuture::from(promise).await);
    }
    #[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
    {
        tokio::time::sleep(duration).await;
    }
}
