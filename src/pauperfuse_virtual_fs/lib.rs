//! Read-only path views over versioned sources. This is not a WASI ABI implementation.

mod interlude {
    pub use pauperfuse::backends::{
        BackendTree, ByteAccess, ByteReader, Description, RelPath, Source,
    };
    pub use std::error::Error;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

#[derive(Debug, thiserror::Error)]
pub enum MountError<E: Error> {
    #[error("backend observation failed: {0}")]
    Observation(#[source] E),
    #[error("duplicate or out-of-order mounted path: {0}")]
    Order(RelPath),
    #[error("mounted root must be a directory")]
    RootKind,
    #[error("non-directory ancestor of mounted path: {0}")]
    NotDirectory(RelPath),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError<E: Error> {
    #[error("path is not mounted: {0}")]
    NotFound(RelPath),
    #[error("path is not a regular file: {0}")]
    NotFile(RelPath),
    #[error("cannot open source {source:?}: {cause}")]
    Access {
        source: Source,
        #[source]
        cause: E,
    },
}

#[derive(Default)]
pub struct VirtualFs {
    entries: BTreeMap<RelPath, Description>,
}

impl VirtualFs {
    /// Replaces the namespace only after the complete ordered observation succeeds.
    pub async fn mount<T: BackendTree>(
        &mut self,
        tree: &mut T,
    ) -> Result<(), MountError<T::Error>> {
        let mut entries = BTreeMap::new();
        let mut previous = None;
        while let Some(entry) = tree.next_entry().await.map_err(MountError::Observation)? {
            if previous.as_ref().is_some_and(|path| path >= &entry.path) {
                return Err(MountError::Order(entry.path));
            }
            if entry.path.is_root() && !matches!(entry.description, Description::Directory) {
                return Err(MountError::RootKind);
            }
            for depth in 0..entry.path.len() {
                let ancestor = entry.path.ancestor(depth).unwrap();
                if let Some(description) = entries.get(&ancestor)
                    && !matches!(description, Description::Directory)
                {
                    return Err(MountError::NotDirectory(ancestor));
                }
            }
            previous = Some(entry.path.clone());
            let replaced = entries.insert(entry.path, entry.description);
            assert!(replaced.is_none(), "validated paths must be distinct");
        }
        self.entries = entries;
        Ok(())
    }

    pub fn metadata(&self, path: &RelPath) -> Option<&Description> {
        self.entries.get(path)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&RelPath, &Description)> {
        self.entries.iter()
    }

    /// Resolves the mounted source exactly once. Symlinks are metadata only here.
    pub async fn open<A: ByteAccess>(
        &self,
        path: &RelPath,
        access: &A,
    ) -> Result<FileHandle<A::Reader>, OpenError<A::Error>> {
        let description = self
            .entries
            .get(path)
            .ok_or_else(|| OpenError::NotFound(path.clone()))?;
        let Description::File { source, .. } = description else {
            return Err(OpenError::NotFile(path.clone()));
        };
        let reader = access
            .open(source)
            .await
            .map_err(|cause| OpenError::Access {
                source: source.clone(),
                cause,
            })?;
        Ok(FileHandle {
            source: source.clone(),
            reader,
        })
    }
}

pub struct FileHandle<R> {
    source: Source,
    reader: R,
}

impl<R: ByteReader> FileHandle<R> {
    pub fn source(&self) -> &Source {
        &self.source
    }

    pub async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, R::Error> {
        let count = self.reader.read_at(offset, buffer).await?;
        assert!(
            count <= buffer.len(),
            "reader returned an impossible byte count"
        );
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
