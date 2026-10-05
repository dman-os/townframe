//! Backend observations describe files without opening their bytes.

use crate::interlude::*;
use std::future::Future;

mod path;
pub use path::{PathError, RelPath};

#[cfg(unix)]
pub mod tokio_fs;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BackendId(pub String);

/// The producing backend owns both selectors; neither implies continuity across moves.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OutputVersion {
    pub output: Vec<u8>,
    pub version: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub backend: BackendId,
    pub output: OutputVersion,
}

/// Metadata only; symlink targets are opaque UTF-8 values interpreted by their backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Description {
    File { source: Source, size: Option<u64> },
    Directory,
    Symlink { target: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntry {
    pub path: RelPath,
    pub description: Description,
}

/// A complete observation ordered by `RelPath`. An unreadable path is an error, not an omitted entry.
pub trait BackendTree {
    type Error: Error;

    /// Returns the next entry, or `None` once the observation is complete.
    fn next_entry(&mut self) -> impl Future<Output = Result<Option<TreeEntry>, Self::Error>>;
}

/// Range-read futures are Send so asynchronous runtime consumers can await them.
/// Reads an opened version into caller-owned storage; it may require a lazy full render first.
pub trait ByteReader {
    type Error: Error;

    /// Returns at most `buffer.len()` bytes. An empty buffer or an offset at/past EOF returns zero.
    fn read_at(
        &mut self,
        offset: u64,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Reports its own tree and opens exact output versions, never silently substituting latest.
pub trait Producer {
    type Error: Error;
    type Tree: BackendTree<Error = Self::Error>;
    type Reader: ByteReader<Error = Self::Error>;

    fn id(&self) -> &BackendId;
    fn observe(&self) -> impl Future<Output = Result<Self::Tree, Self::Error>>;
    fn open(
        &self,
        output: &OutputVersion,
    ) -> impl Future<Output = Result<Self::Reader, Self::Error>> + Send;
}

/// Routes a source reference without exposing the producer's internal state to a consumer.
pub trait ByteAccess {
    type Error: Error;
    type Reader: ByteReader;

    fn open(
        &self,
        source: &Source,
    ) -> impl Future<Output = Result<Self::Reader, Self::Error>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum AccessError<E: Error> {
    #[error("source backend {requested:?} does not match producer {available:?}")]
    WrongBackend {
        requested: BackendId,
        available: BackendId,
    },
    #[error("producer failed to open output: {0}")]
    Producer(#[source] E),
}

/// A statically typed byte route for one producer, not a backend registry.
pub struct ProducerAccess<'a, P> {
    producer: &'a P,
}

impl<'a, P: Producer> ProducerAccess<'a, P> {
    pub fn new(producer: &'a P) -> Self {
        Self { producer }
    }
}

impl<P: Producer + Sync> ByteAccess for ProducerAccess<'_, P>
where
    P::Error: 'static,
{
    type Error = AccessError<P::Error>;
    type Reader = P::Reader;

    async fn open(&self, source: &Source) -> Result<Self::Reader, Self::Error> {
        if &source.backend != self.producer.id() {
            return Err(AccessError::WrongBackend {
                requested: source.backend.clone(),
                available: self.producer.id().clone(),
            });
        }
        self.producer
            .open(&source.output)
            .await
            .map_err(AccessError::Producer)
    }
}

#[cfg(test)]
pub(crate) mod fixture;
#[cfg(test)]
mod tests;
