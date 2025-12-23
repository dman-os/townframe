#[allow(unused)]
mod interlude {
    pub use crate::stores::Store;

    pub use daybook_types::doc::ChangeHashSet;

    pub use api_utils_rs::prelude::*;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use samod::DocumentId;
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::am::AmCtx;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;

pub mod blobs;
pub mod config;
pub mod drawer;
pub mod plugs;
#[allow(unused)]
pub mod repos;
pub mod rt;
pub mod stores;
pub mod tables;

#[cfg(test)]
mod e2e;

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

#[cfg(feature = "uniffi")]
uniffi::custom_type!(ChangeHashSet, Vec<String>, {
    remote,
    lower: |hash| utils_rs::am::serialize_commit_heads(&hash.0),
    try_lift: |strings: Vec<String>| {
        Ok(ChangeHashSet(utils_rs::am::parse_commit_heads(&strings).to_anyhow()?))
    }
});

pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        sqlite_vec::sqlite3_vec_init();
    });
}
