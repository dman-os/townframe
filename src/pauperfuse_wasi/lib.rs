//! Isolated read-only WASI Preview 2 filesystem imports. No wash or stream integration.
#![cfg(unix)]

mod interlude {
    pub use pauperfuse::backends::{ByteAccess, ByteReader, Description, RelPath, Source};
    pub use pauperfuse_virtual_fs::{FileHandle, VirtualFs};
    pub use std::error::Error as StdError;
    pub use utils_rs::prelude::*;
}

mod filesystem;
pub use filesystem::{Filesystem, MAX_READ_BYTES, SourceFailure};

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "host",
        imports: { default: async | trappable },
        with: {
            "wasi:filesystem/types.descriptor": crate::filesystem::DescriptorToken,
            "wasi:filesystem/types.directory-entry-stream": crate::filesystem::DirectoryToken,
        },
    });
}

#[cfg(test)]
mod tests;
