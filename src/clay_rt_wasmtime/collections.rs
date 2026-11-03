use super::WasmStoreState;
use super::bindings::townframe::dayframe::collection_registration as registration_wit;
use super::bindings::townframe::dayframe::collections as wit;
use super::wasmtime;
use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::RowEvidence;
use clay_collection::Version;
use clay_guest_session::collections::CollectionReader;
use clay_guest_session::collections::ProjectionReader;
use clay_guest_session::collections::ReadFailure;
use clay_guest_session::collections::RegistrationWriter;
use clay_guest_session::collections::WriteFailure;
use std::time::Duration;
use wasmtime::component::Accessor;
use wasmtime::component::HasSelf;
use wasmtime::component::Resource;

impl wit::Host for WasmStoreState {}

impl wit::HostCollectionReader for WasmStoreState {
    fn drop(&mut self, reader: Resource<CollectionReader>) -> wasmtime::Result<()> {
        self.table.delete(reader)?;
        Ok(())
    }
}

impl<T: 'static> wit::HostCollectionReaderWithStore<T> for HasSelf<WasmStoreState> {
    async fn acquire(
        accessor: &Accessor<T, Self>,
        reader: Resource<CollectionReader>,
        selector: Vec<u8>,
        resume: Option<Vec<u8>>,
    ) -> wasmtime::Result<Result<Resource<ProjectionReader>, wit::AcquireError>> {
        let reader = accessor.with(|mut access| access.get().table.get(&reader).unwrap().clone());
        let lease = match reader.acquire(selector, resume.map(Cursor)) {
            Ok(lease) => lease,
            Err(error) => return Ok(Err(acquire_error(error))),
        };
        accessor.with(|mut access| Ok(Ok(access.get().table.push(lease)?)))
    }
}

impl wit::HostProjectionLease for WasmStoreState {
    fn drop(&mut self, lease: Resource<ProjectionReader>) -> wasmtime::Result<()> {
        self.table.get(&lease)?.cancel();
        self.table.delete(lease)?;
        Ok(())
    }
}

impl<T: 'static> wit::HostProjectionLeaseWithStore<T> for HasSelf<WasmStoreState> {
    async fn read(
        accessor: &Accessor<T, Self>,
        lease: Resource<ProjectionReader>,
        committed: Option<Vec<u8>>,
        limits: wit::ReadLimits,
    ) -> wasmtime::Result<Result<wit::Page, wit::ReadError>> {
        let (lease, wake) = accessor.with(|mut access| {
            let state = access.get();
            (
                state.table.get(&lease).unwrap().clone(),
                state.collection_wake.clone(),
            )
        });
        let timeout = lease.poll_timeout(Duration::from_millis(limits.poll_timeout_ms.into()));
        let read = lease.read(
            committed.map(Cursor),
            ReadLimits {
                max_transactions: limits.max_transactions,
                max_changes: limits.max_changes,
            },
            tokio::time::sleep(timeout),
        );
        let result = match wake {
            Some(wake) => wake.drive(read).await,
            None => read.await,
        };
        match result {
            Ok(page) => Ok(Ok(page_to_wire(page))),
            Err(ReadFailure::Read(error)) => Ok(Err(read_error(error))),
            Err(ReadFailure::Busy) => Err(wasmtime::Error::msg(
                "projection lease already has an in-flight read",
            )),
            Err(ReadFailure::PageLimit) => Err(wasmtime::Error::msg(
                "projection page exceeds the session byte limit",
            )),
        }
    }
}

impl registration_wit::Host for WasmStoreState {}

impl registration_wit::HostRegistrationWriter for WasmStoreState {
    fn drop(&mut self, writer: Resource<RegistrationWriter>) -> wasmtime::Result<()> {
        self.table.delete(writer)?;
        Ok(())
    }
}

impl<T: 'static> registration_wit::HostRegistrationWriterWithStore<T> for HasSelf<WasmStoreState> {
    async fn put(
        accessor: &Accessor<T, Self>,
        writer: Resource<RegistrationWriter>,
        local_key: Vec<u8>,
        based_on: Option<wit::Version>,
        value: wit::EncodedValue,
    ) -> wasmtime::Result<Result<registration_wit::MutationAccepted, registration_wit::MutationError>>
    {
        accessor.with(|mut access| {
            let writer = access.get().table.get(&writer)?;
            Ok(writer
                .put(local_key, based_on.map(version_from_wire), value.body)
                .map(accepted)
                .map_err(write_failure))
        })
    }

    async fn remove(
        accessor: &Accessor<T, Self>,
        writer: Resource<RegistrationWriter>,
        local_key: Vec<u8>,
        based_on: Option<wit::Version>,
    ) -> wasmtime::Result<
        Result<Option<registration_wit::MutationAccepted>, registration_wit::MutationError>,
    > {
        accessor.with(|mut access| {
            let writer = access.get().table.get(&writer)?;
            Ok(writer
                .remove(local_key, based_on.map(version_from_wire))
                .map(|version| version.map(accepted))
                .map_err(write_failure))
        })
    }
}

fn accepted(version: Version) -> registration_wit::MutationAccepted {
    registration_wit::MutationAccepted {
        row_version: version_to_wire(version),
    }
}

fn write_failure(failure: WriteFailure) -> registration_wit::MutationError {
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

fn acquire_error(error: AcquireError) -> wit::AcquireError {
    match error {
        AcquireError::ResetRequired => wit::AcquireError::ResetRequired,
        AcquireError::SourceUnavailable => wit::AcquireError::SourceUnavailable,
        AcquireError::Revoked => wit::AcquireError::Revoked,
        AcquireError::InvalidSelector => wit::AcquireError::InvalidSelector,
        AcquireError::UnauthorizedSelector => wit::AcquireError::UnauthorizedSelector,
    }
}

fn read_error(error: ReadError) -> wit::ReadError {
    match error {
        ReadError::ResetRequired => wit::ReadError::ResetRequired,
        ReadError::SourceUnavailable => wit::ReadError::SourceUnavailable,
        ReadError::Revoked => wit::ReadError::Revoked,
    }
}

fn page_to_wire(page: clay_collection::Page<Vec<u8>, Vec<u8>>) -> wit::Page {
    wit::Page {
        transactions: page
            .transactions
            .into_iter()
            .map(|transaction| wit::Transaction {
                changes: transaction
                    .changes
                    .into_iter()
                    .map(|change| match change {
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
                    })
                    .collect(),
            })
            .collect(),
        through: page.through.0,
        bootstrap_complete: page.bootstrap_complete,
    }
}

fn version_to_wire(version: Version) -> wit::Version {
    wit::Version {
        incarnation: version.incarnation,
        sequence: version.seq,
    }
}
