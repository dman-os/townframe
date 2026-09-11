//! Presents a [`WillowStore`] as a `willow25::Store`.
//!
//! The upstream trait exists so the implementation can be validated against Willow's
//! specified store semantics, and so `big_willow` is a generally usable Willow store rather
//! than only a BigRepo component. Upstream's methods take `&mut self`, which is why this is a
//! newtype rather than an impl on the store itself.
//!
//! Payloads travel separately from entry metadata upstream, and a store may hold an entry
//! whose payload has not arrived. Two consequences:
//!
//! - Upstream's payload-less [`Store::insert_entry`] maps to [`WillowStore::insert_entry`],
//!   which retains the stored payload when the entry is unchanged and otherwise records it as
//!   absent. Supplying a payload goes through [`Store::create_entry`], which maps to
//!   [`WillowStore::insert_entry_with_payload`].
//! - Reading a slice of an absent payload writes zero bytes, which is exactly upstream's
//!   "writes as many bytes as available" contract. Callers that need to know whether an entry
//!   exists use [`Store::get_entry`], which is unaffected by payload arrival.
//!
//! `PayloadPrefixStore` is deliberately not implemented: it needs `bab_rs` chunked payload
//! storage, which this crate does not have.

// The implementations below mirror upstream's own parameter names, notably `c` for the
// bulk consumer. Renaming them would obscure the correspondence to `willow25::Store`.
// `big_sync`'s sqlite codec allows the same lint for the same reason.
#![allow(clippy::disallowed_names)]
use std::num::NonZeroUsize;
use std::sync::Arc;

use bab_rs::generic::storage::units::{ByteCount, ByteIndex};
use ufotofu::prelude::*;
use willow25::prelude::*;
use willow25::storage::{
    CreateEntryError, GetPayloadSliceError, NondestructiveInsert, StoreOrConsumerError,
};

use crate::store::{AreaReadLimits, EntryKey, InsertOutcome, StoreError, WillowStore, prunes};

/// A [`WillowStore`] exposed as a `willow25::Store`.
pub struct UpstreamStore {
    inner: Arc<dyn WillowStore>,
}

impl UpstreamStore {
    pub fn new(inner: Arc<dyn WillowStore>) -> Self {
        Self { inner }
    }

    /// Reads exactly `payload_length` bytes from `producer`.
    ///
    /// Upstream's `Store` contract makes a producer that emits fewer bytes than the entry's
    /// payload length a programming error, so a short or failing producer panics. `P::Final`
    /// and `P::Error` carry no `Debug` bound, so this cannot use `expect`.
    async fn read_payload<P>(producer: &mut P, payload_length: u64) -> Vec<u8>
    where
        P: BulkProducer<Item = u8>,
    {
        let mut payload = vec![0u8; payload_length as usize];
        if !payload.is_empty()
            && producer
                .bulk_overwrite_full_slice(&mut payload)
                .await
                .is_err()
        {
            panic!("payload producer must emit the entry payload length in bytes");
        }
        payload
    }

    /// The entry at `key` and its retained payload, unless `expected_digest` rules it out.
    ///
    /// The payload is `None` when it has not arrived; see [`WillowStore::get_payload`].
    async fn stored_entry<K>(
        &self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
    ) -> Result<Option<(AuthorisedEntry, Option<Vec<u8>>)>, StoreError>
    where
        K: Keylike,
    {
        let Some(entry) = self
            .inner
            .get_entry(namespace_id, key.subspace_id(), key.path())
            .await?
        else {
            return Ok(None);
        };

        if expected_digest.is_some_and(|digest| *entry.payload_digest() != digest) {
            return Ok(None);
        }

        let payload = self
            .inner
            .get_payload(namespace_id, key.subspace_id(), key.path())
            .await?;

        Ok(Some((entry, payload)))
    }

    /// Writes the `[start, start + length)` window of `payload` into `c`.
    ///
    /// An absent payload contributes no bytes. The window is clamped to what is stored:
    /// upstream leaves the behaviour unspecified when it starts past the end of the payload.
    async fn write_payload_slice<C>(
        payload: Option<&[u8]>,
        start: ByteIndex,
        length: ByteCount,
        c: &mut C,
    ) -> Result<ByteCount, C::Error>
    where
        C: BulkConsumer<Item = u8>,
    {
        let stored = payload.unwrap_or_default();
        let start = (start as usize).min(stored.len());
        let end = start.saturating_add(length as usize).min(stored.len());
        let slice = &stored[start..end];

        c.consume_full_slice(slice)
            .await
            .map_err(|err| err.reason)?;

        Ok(slice.len() as ByteCount)
    }

    /// Builds and authorises the entry described by the given fields.
    fn build_entry(
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
        timestamp: Timestamp,
        payload: &[u8],
        write_capability: &WriteCapability,
        secret: &SubspaceSecret,
    ) -> Result<AuthorisedEntry, CreateEntryError<StoreError>> {
        Entry::builder()
            .namespace_id(namespace_id.clone())
            .subspace_id(subspace_id.clone())
            .path(path.clone())
            .timestamp(timestamp)
            .payload_length(payload.len() as u64)
            .payload_digest(PayloadDigest::from_payload(payload))
            .build()
            .into_authorised_entry(write_capability, secret)
            .map_err(|_| CreateEntryError::AuthorisationTokenError)
    }
}

impl Store for UpstreamStore {
    type InternalError = StoreError;

    async fn create_entry<P>(
        &mut self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
        timestamp: Timestamp,
        payload_producer: &mut P,
        payload_length: u64,
        write_capability: &WriteCapability,
        secret: &SubspaceSecret,
    ) -> Result<Option<AuthorisedEntry>, CreateEntryError<Self::InternalError>>
    where
        P: BulkProducer<Item = u8>,
    {
        let payload = Self::read_payload(payload_producer, payload_length).await;
        let entry = Self::build_entry(
            namespace_id,
            subspace_id,
            path,
            timestamp,
            &payload,
            write_capability,
            secret,
        )?;

        let stored = entry.clone();
        match self
            .inner
            .insert_entry_with_payload(entry, &payload)
            .await
            .map_err(CreateEntryError::StoreError)?
        {
            InsertOutcome::Inserted { .. } => Ok(Some(stored)),
            InsertOutcome::Outdated => Ok(None),
        }
    }

    async fn create_entry_nondestructive<P>(
        &mut self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
        timestamp: Timestamp,
        payload_producer: &mut P,
        payload_length: u64,
        write_capability: &WriteCapability,
        secret: &SubspaceSecret,
    ) -> Result<NondestructiveInsert, CreateEntryError<Self::InternalError>>
    where
        P: BulkProducer<Item = u8>,
    {
        let payload = Self::read_payload(payload_producer, payload_length).await;
        let entry = Self::build_entry(
            namespace_id,
            subspace_id,
            path,
            timestamp,
            &payload,
            write_capability,
            secret,
        )?;

        // Would an entry already in the store prune the new one? Those entries sit at
        // prefixes of the new path, so they are point lookups.
        for prefix in entry.path().all_prefixes() {
            let existing = self
                .inner
                .get_entry(entry.namespace_id(), entry.subspace_id(), &prefix)
                .await
                .map_err(CreateEntryError::StoreError)?;
            if existing.is_some_and(|existing| existing.is_newer_than(&entry)) {
                return Ok(NondestructiveInsert::Outdated);
            }
        }

        // Would the new entry prune anything? Those entries sit inside the area rooted at the
        let area = Area::new(
            Some(entry.subspace_id().clone()),
            entry.path().clone(),
            TimeRange::full(),
        );
        let prefixed = self
            .inner
            .read_area(
                entry.namespace_id(),
                &area,
                None,
                AreaReadLimits {
                    max_entries: NonZeroUsize::MAX,
                },
            )
            .await
            .map_err(CreateEntryError::StoreError)?;
        // The new entry prunes the ones it is newer than or equal to, except a tie at its own
        // path, which the store replaces in place. If this and the store's rule diverged, the
        // adapter would report that nothing would be pruned and then prune anyway.
        if prefixed.entries.iter().any(|existing| prunes(existing, &entry)) {
            return Ok(NondestructiveInsert::Prevented);
        }

        let stored = entry.clone();
        self.inner
            .insert_entry_with_payload(entry, &payload)
            .await
            .map_err(CreateEntryError::StoreError)?;

        Ok(NondestructiveInsert::Success(stored))
    }

    async fn insert_entry(&mut self, entry: AuthorisedEntry) -> Result<bool, Self::InternalError> {
        match self.inner.insert_entry(entry).await? {
            InsertOutcome::Inserted { .. } => Ok(true),
            InsertOutcome::Outdated => Ok(false),
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
        if expected_digest.is_some()
            && self
                .stored_entry(namespace_id, key, expected_digest)
                .await?
                .is_none()
        {
            return Ok(false);
        }

        self.inner
            .forget_entry(namespace_id, key.subspace_id(), key.path())
            .await
    }

    async fn forget_area(
        &mut self,
        namespace_id: &NamespaceId,
        area: &Area,
    ) -> Result<(), Self::InternalError> {
        self.inner.forget_area(namespace_id, area).await
    }

    async fn forget_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<(), Self::InternalError> {
        self.inner.forget_namespace(namespace_id).await
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
        Ok(self
            .stored_entry(namespace_id, key, expected_digest)
            .await?
            .map(|(entry, _payload)| entry))
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
        let Some((entry, payload)) = self
            .stored_entry(namespace_id, key, expected_digest)
            .await
            .map_err(StoreOrConsumerError::StoreError)?
        else {
            return Ok(None);
        };

        let written = Self::write_payload_slice(
            payload.as_deref(),
            payload_slice_start,
            payload_slice_length,
            c,
        )
        .await
        .map_err(StoreOrConsumerError::ConsumerError)?;

        Ok(Some((entry, written)))
    }

    async fn get_payload_slice<K, C>(
        &mut self,
        namespace_id: &NamespaceId,
        key: &K,
        expected_digest: Option<PayloadDigest>,
        start: ByteIndex,
        length: ByteCount,
        c: &mut C,
    ) -> Result<ByteCount, GetPayloadSliceError<Self::InternalError, C::Error>>
    where
        K: Keylike,
        C: BulkConsumer<Item = u8>,
    {
        let Some((_entry, payload)) = self
            .stored_entry(namespace_id, key, expected_digest)
            .await
            .map_err(GetPayloadSliceError::StoreError)?
        else {
            return Err(GetPayloadSliceError::NoSuchEntry);
        };

        Self::write_payload_slice(payload.as_deref(), start, length, c)
            .await
            .map_err(GetPayloadSliceError::ConsumerError)
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
        let mut resume: Option<EntryKey> = None;

        loop {
            let page = self
                .inner
                .read_area(namespace_id, area, resume.as_ref(), AreaReadLimits::default())
                .await
                .map_err(StoreOrConsumerError::StoreError)?;

            let next = page.next;

            for entry in page.entries {
                c.consume_item(entry)
                    .await
                    .map_err(StoreOrConsumerError::ConsumerError)?;
            }

            match next {
                Some(key) => resume = Some(key),
                None => return Ok(()),
            }
        }
    }

    async fn flush(&mut self) -> Result<(), Self::InternalError> {
        self.inner.flush().await
    }
}
