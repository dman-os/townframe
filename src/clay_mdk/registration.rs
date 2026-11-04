//! The guest-side registration writer (ADR 017 §9). It wraps the host's
//! `registration-writer` and fills in each write's [`WriteBasis`] from the guest's own
//! replica of the collection, so an ordinary `put` or `delete` supersedes everything this
//! guest has seen for the key, tombstones included, and only a key the replica has never
//! held is written `concurrent`. Explicit `observed` and `compare-and-set` bases stay
//! available through the `_with` calls.

use crate::collection_registration as wire;
use crate::collections;
use clay_collection::Stamp;
use clay_collection::WriteBasis;
use clay_collection::WriterId;

impl From<Stamp> for collections::Stamp {
    fn from(stamp: Stamp) -> Self {
        Self {
            lamport: stamp.lamport,
            writer: stamp.writer.as_bytes().to_vec(),
        }
    }
}

impl From<collections::Stamp> for Stamp {
    fn from(stamp: collections::Stamp) -> Self {
        Self {
            lamport: stamp.lamport,
            writer: WriterId::new(stamp.writer),
        }
    }
}

impl From<WriteBasis> for wire::WriteBasis {
    fn from(basis: WriteBasis) -> Self {
        match basis {
            WriteBasis::Concurrent => Self::Concurrent,
            WriteBasis::Observed(stamp) => Self::Observed(stamp.into()),
            WriteBasis::CompareAndSet(expected) => Self::CompareAndSet(expected.map(Into::into)),
        }
    }
}

/// `replica_basis` reads the guest's replica of the collection for a key, typically
/// `|key| mirror.borrow().write_basis(key)` over a `clay_collection::Mirror`. It must not
/// hold a borrow past its return: writes await the host.
pub struct RegistrationWriter<F> {
    writer: wire::RegistrationWriter,
    replica_basis: F,
}

impl<F: Fn(&Vec<u8>) -> WriteBasis> RegistrationWriter<F> {
    pub fn new(writer: wire::RegistrationWriter, replica_basis: F) -> Self {
        Self {
            writer,
            replica_basis,
        }
    }

    /// Put `value` at `key`, superseding what this guest's replica holds for it.
    pub async fn put(
        &self,
        key: Vec<u8>,
        value: wire::WrittenValue,
    ) -> Result<collections::Stamp, wire::MutationError> {
        let basis = (self.replica_basis)(&key).into();
        self.put_with(key, basis, value).await
    }

    pub async fn put_with(
        &self,
        key: Vec<u8>,
        basis: wire::WriteBasis,
        value: wire::WrittenValue,
    ) -> Result<collections::Stamp, wire::MutationError> {
        self.writer.put(key, basis, value).await
    }

    /// Delete `key`, superseding what this guest's replica holds for it.
    pub async fn delete(&self, key: Vec<u8>) -> Result<collections::Stamp, wire::MutationError> {
        let basis = (self.replica_basis)(&key).into();
        self.delete_with(key, basis).await
    }

    pub async fn delete_with(
        &self,
        key: Vec<u8>,
        basis: wire::WriteBasis,
    ) -> Result<collections::Stamp, wire::MutationError> {
        self.writer.delete(key, basis).await
    }
}
