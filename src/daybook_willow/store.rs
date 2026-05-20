use crate::interlude::*;

use bab_rs::generic::storage::units::{ByteCount, ByteIndex};
use ufotofu::{BulkConsumer, BulkProducer, Consumer};
use willow25::{
    prelude::*,
    storage::{
        CreateEntryError, GetPayloadSliceError, NondestructiveInsert, PersistentStoreError,
        StoreOrConsumerError,
    },
};

// the willow25::storage::Store interface is not dyn compat
pub enum WillowStore {
    Persisted(willow25::storage::PersistentStore),
    Memory(willow25::storage::MemoryStore),
}

impl willow25::storage::Store for WillowStore {
    type InternalError = PersistentStoreError;

    async fn create_entry<P>(
        &mut self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &willow25::prelude::Path,
        timestamp: willow25::prelude::Timestamp,
        payload_producer: &mut P,
        payload_length: u64,
        write_capability: &WriteCapability,
        secret: &SubspaceSecret,
    ) -> Result<Option<AuthorisedEntry>, CreateEntryError<Self::InternalError>>
    where
        P: BulkProducer<Item = u8>,
    {
        match self {
            WillowStore::Persisted(inner) => {
                inner
                    .create_entry(
                        namespace_id,
                        subspace_id,
                        path,
                        timestamp,
                        payload_producer,
                        payload_length,
                        write_capability,
                        secret,
                    )
                    .await
            }
            WillowStore::Memory(inner) => inner
                .create_entry(
                    namespace_id,
                    subspace_id,
                    path,
                    timestamp,
                    payload_producer,
                    payload_length,
                    write_capability,
                    secret,
                )
                .await
                .map_err(|err| {
                    use CreateEntryError::*;
                    match err {
                        CreateEntryError::StoreError(_) => unreachable!(),
                        CreateEntryError::AuthorisationTokenError => {
                            CreateEntryError::AuthorisationTokenError
                        }
                    }
                }),
        }
    }

    async fn create_entry_nondestructive<P>(
        &mut self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &willow25::prelude::Path,
        timestamp: willow25::prelude::Timestamp,
        payload_producer: &mut P,
        payload_length: u64,
        write_capability: &WriteCapability,
        secret: &SubspaceSecret,
    ) -> Result<NondestructiveInsert, CreateEntryError<Self::InternalError>>
    where
        P: BulkProducer<Item = u8>,
    {
        match self {
            WillowStore::Persisted(inner) => {
                inner
                    .create_entry_nondestructive(
                        namespace_id,
                        subspace_id,
                        path,
                        timestamp,
                        payload_producer,
                        payload_length,
                        write_capability,
                        secret,
                    )
                    .await
            }
            WillowStore::Memory(inner) => inner
                .create_entry_nondestructive(
                    namespace_id,
                    subspace_id,
                    path,
                    timestamp,
                    payload_producer,
                    payload_length,
                    write_capability,
                    secret,
                )
                .await
                .map_err(|err| {
                    use CreateEntryError::*;
                    match err {
                        CreateEntryError::StoreError(_) => unreachable!(),
                        CreateEntryError::AuthorisationTokenError => {
                            CreateEntryError::AuthorisationTokenError
                        }
                    }
                }),
        }
    }

    async fn insert_entry(&mut self, entry: AuthorisedEntry) -> Result<bool, Self::InternalError> {
        match self {
            WillowStore::Persisted(inner) => inner.insert_entry(entry).await,
            WillowStore::Memory(inner) => {
                Ok(inner.insert_entry(entry).await.expect(ERROR_IMPOSSIBLE))
            }
        }
    }

    async fn forget_entry<K>(
        &mut self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
    ) -> Result<bool, Self::InternalError>
    where
        K: Keylike,
    {
        match self {
            WillowStore::Persisted(inner) => {
                inner.forget_entry(namespace_id, key, expected_digest).await
            }
            WillowStore::Memory(inner) => Ok(inner
                .forget_entry(namespace_id, key, expected_digest)
                .await
                .expect(ERROR_IMPOSSIBLE)),
        }
    }

    async fn forget_area(
        &mut self,
        namespace_id: &NamespaceId,
        area: &Area,
    ) -> Result<(), Self::InternalError> {
        match self {
            WillowStore::Persisted(inner) => inner.forget_area(namespace_id, area).await,
            WillowStore::Memory(inner) => Ok(inner
                .forget_area(namespace_id, area)
                .await
                .expect(ERROR_IMPOSSIBLE)),
        }
    }

    async fn forget_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<(), Self::InternalError> {
        match self {
            WillowStore::Persisted(inner) => inner.forget_namespace(namespace_id).await,
            WillowStore::Memory(inner) => Ok(inner
                .forget_namespace(namespace_id)
                .await
                .expect(ERROR_IMPOSSIBLE)),
        }
    }

    async fn get_entry<K>(
        &mut self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
    ) -> Result<Option<AuthorisedEntry>, Self::InternalError>
    where
        K: Keylike,
    {
        match self {
            WillowStore::Persisted(inner) => {
                inner.get_entry(namespace_id, key, expected_digest).await
            }
            WillowStore::Memory(inner) => Ok(inner
                .get_entry(namespace_id, key, expected_digest)
                .await
                .expect(ERROR_IMPOSSIBLE)),
        }
    }

    async fn get_entry_and_payload_slice<K, C>(
        &mut self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
        payload_slice_start: ByteIndex,
        payload_slice_length: ByteCount,
        c: &mut C,
    ) -> Result<
        Option<(AuthorisedEntry, ByteCount)>,
        StoreOrConsumerError<Self::InternalError, C::Error>,
    >
    where
        K: Keylike,
        C: BulkConsumer<Item = u8>,
    {
        match self {
            WillowStore::Persisted(inner) => {
                inner
                    .get_entry_and_payload_slice(
                        namespace_id,
                        key,
                        expected_digest,
                        payload_slice_start,
                        payload_slice_length,
                        c,
                    )
                    .await
            }
            WillowStore::Memory(inner) => inner
                .get_entry_and_payload_slice(
                    namespace_id,
                    key,
                    expected_digest,
                    payload_slice_start,
                    payload_slice_length,
                    c,
                )
                .await
                .map_err(|err| {
                    use StoreOrConsumerError::*;
                    match err {
                        StoreError(_) => unreachable!(),
                        ConsumerError(err) => ConsumerError(err),
                    }
                }),
        }
    }

    async fn get_payload_slice<K, C>(
        &mut self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
        start: ByteIndex,
        length: ByteCount,
        c: &mut C,
    ) -> Result<ByteCount, willow25::storage::GetPayloadSliceError<Self::InternalError, C::Error>>
    where
        K: Keylike,
        C: BulkConsumer<Item = u8>,
    {
        // match self {
        //     WillowStore::Persisted(inner) => todo!(),
        //     WillowStore::Memory(inner) => todo!(),
        // }
        match self {
            WillowStore::Persisted(inner) => {
                inner
                    .get_payload_slice(namespace_id, key, expected_digest, start, length, c)
                    .await
            }
            WillowStore::Memory(inner) => inner
                .get_payload_slice(namespace_id, key, expected_digest, start, length, c)
                .await
                .map_err(|err| {
                    use GetPayloadSliceError::*;
                    match err {
                        StoreError(_) => unreachable!(),
                        ConsumerError(err) => ConsumerError(err),
                        NoSuchEntry => NoSuchEntry,
                    }
                }),
        }
    }

    async fn get_area<C>(
        &mut self,
        namespace_id: &NamespaceId,
        area: &Area,
        c: &mut C,
    ) -> Result<(), StoreOrConsumerError<Self::InternalError, C::Error>>
    where
        C: Consumer<Item = AuthorisedEntry>,
    {
        match self {
            WillowStore::Persisted(inner) => inner.get_area(namespace_id, area, c).await,
            WillowStore::Memory(inner) => {
                inner.get_area(namespace_id, area, c).await.map_err(|err| {
                    use StoreOrConsumerError::*;
                    match err {
                        StoreError(_) => unreachable!(),
                        ConsumerError(err) => ConsumerError(err),
                    }
                })
            }
        }
    }

    async fn flush(&mut self) -> Result<(), Self::InternalError> {
        match self {
            WillowStore::Persisted(inner) => inner.flush().await,
            WillowStore::Memory(inner) => Ok(inner.flush().await.expect(ERROR_IMPOSSIBLE)),
        }
    }
}
