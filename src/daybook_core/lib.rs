#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;

pub mod drawer;
#[allow(unused)]
pub mod gen;
pub mod repos;
pub mod stores;
pub mod tables;

#[cfg(test)]
mod tincans;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

#[cfg(feature = "uniffi")]
uniffi::custom_type!(Uuid, Vec<u8>, {
    remote,
    lower: |uuid| uuid.as_bytes().to_vec(),
    try_lift: |bytes: Vec<u8>| {
        uuid::Uuid::from_slice(&bytes)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    }
});

mod wasm {
    use crate::interlude::*;

    use wash_runtime::*;

    async fn main() -> Res<()> {
        let host = host::HostBuilder::new()
            .with_engine({
                engine::Engine::builder()
                    .build()
                    .map_err(utils_rs::anyhow_to_eyre!())?
            })
            .with_plugin({ Arc::new(plugin::wasi_config::RuntimeConfig::default()) })
            .map_err(utils_rs::anyhow_to_eyre!())?
            .build()
            .map_err(utils_rs::anyhow_to_eyre!())?;

        let host = host.start().await.map_err(utils_rs::anyhow_to_eyre!())?;

        Ok(())
    }

    mod wflows {
        use crate::interlude::*;
    }
}
