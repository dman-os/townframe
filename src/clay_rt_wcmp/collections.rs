use super::WcmpStoreState;
pub use super::bindings::townframe::dayframe::collection_registration as registration_wit;
pub use super::bindings::townframe::dayframe::collections as wit;
use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::RowEvidence;
use clay_collection::Version;
use clay_guest_session::collections::BytesPage;
use clay_guest_session::collections::CollectionReader;
use clay_guest_session::collections::ProjectionReader;
use clay_guest_session::collections::ReadFailure;
use clay_guest_session::collections::RegistrationWriter;
use clay_guest_session::collections::WriteFailure;
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
            Ok(page) => Ok(Ok(page_to_wire(page))),
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
        local_key: Vec<u8>,
        based_on: Option<wit::Version>,
        value: wit::EncodedValue,
    ) -> wcmp::Result<Result<registration_wit::MutationAccepted, registration_wit::MutationError>>
    {
        accessor.with(|mut access| {
            let writer = access.get().table.get(&writer)?;
            Ok(writer
                .put(local_key, based_on.map(version_from_wire), value.body)
                .map(accepted)
                .map_err(write_failure_to_wire))
        })
    }

    async fn remove(
        accessor: &Accessor<T, Self>,
        writer: Resource<RegistrationWriter>,
        local_key: Vec<u8>,
        based_on: Option<wit::Version>,
    ) -> wcmp::Result<
        Result<Option<registration_wit::MutationAccepted>, registration_wit::MutationError>,
    > {
        accessor.with(|mut access| {
            let writer = access.get().table.get(&writer)?;
            Ok(writer
                .remove(local_key, based_on.map(version_from_wire))
                .map(|version| version.map(accepted))
                .map_err(write_failure_to_wire))
        })
    }
}

fn accepted(version: Version) -> registration_wit::MutationAccepted {
    registration_wit::MutationAccepted {
        row_version: version_to_wire(version),
    }
}

fn write_failure_to_wire(failure: WriteFailure) -> registration_wit::MutationError {
    match failure {
        WriteFailure::Conflict { current } => {
            registration_wit::MutationError::Conflict(current.map(version_to_wire))
        }
        WriteFailure::Revoked => registration_wit::MutationError::Revoked,
        WriteFailure::KeyTooLarge => registration_wit::MutationError::KeyTooLarge,
        WriteFailure::ValueTooLarge => registration_wit::MutationError::ValueTooLarge,
        WriteFailure::RowLimit => registration_wit::MutationError::RowLimit,
    }
}

fn version_from_wire(version: wit::Version) -> Version {
    Version::new(version.incarnation, version.sequence)
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

fn page_to_wire(page: BytesPage) -> wit::Page {
    wit::Page {
        transactions: page
            .transactions
            .into_iter()
            .map(|transaction| wit::Transaction {
                changes: transaction
                    .changes
                    .into_iter()
                    .map(change_to_wire)
                    .collect(),
            })
            .collect(),
        through: page.through.0,
        bootstrap_complete: page.bootstrap_complete,
    }
}

fn change_to_wire(change: Change<Vec<u8>, Vec<u8>>) -> wit::Change {
    match change {
        Change::Upsert {
            key,
            row_version,
            value,
            evidence,
            write_context,
        } => wit::Change::Upsert(wit::Upsert {
            key: wit::EncodedValue { body: key },
            row_version: version_to_wire(row_version),
            value: wit::EncodedValue { body: value },
            evidence: match evidence {
                RowEvidence::Authoritative => wit::RowEvidence::Authoritative,
                RowEvidence::Pending => wit::RowEvidence::Pending,
            },
            write_context,
        }),
        Change::Remove { key, row_version } => wit::Change::Remove(wit::Removal {
            key: wit::EncodedValue { body: key },
            row_version: version_to_wire(row_version),
        }),
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
