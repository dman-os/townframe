#![recursion_limit = "512"]

#[allow(unused)]
mod interlude {
    pub use crate::stores::{AmStore, VersionTag, Versioned};

    pub use daybook_types::doc::ChangeHashSet;

    pub use am_utils_rs::prelude::*;
    pub use api_utils_rs::prelude::*;
    pub use automerge::ActorId;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use samod::DocumentId;
    pub use std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::{CHeapStr, DHashMap};
}

pub mod blobs;
pub mod config;
pub mod drawer;
pub mod imgtools;
pub mod index;
pub mod local_state;
pub mod plugs;
pub mod progress;
pub mod repo;
#[allow(unused)]
pub mod repos;
pub mod rt;
pub mod secrets;
pub mod stores;
pub mod sync;
pub mod tables;

#[cfg(test)]
mod e2e;

#[cfg(test)]
mod tincans;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
daybook_types::custom_type_set!();

#[cfg(feature = "uniffi")]
use crate::stores::VersionTag;

#[cfg(feature = "uniffi")]
uniffi::custom_type!(VersionTag, String, {
    remote,
    lower: |tag| format!(
        "{}_{}",
        utils_rs::hash::encode_base58_multibase(tag.actor_id.to_bytes()),
        tag.version.bs58()
    ),
    try_lift: |str| {
        let (actor_id, version)  = str.split_once('_')
            .ok_or_else(|| uniffi::deps::anyhow::anyhow!("unable to parse VersionTag from {str:?}"))?;

        let mut buf = [0_u8; 16];
        utils_rs::hash::decode_base58_multibase_onto(actor_id, &mut buf).to_anyhow()?;
        let actor_id = buf.into();

        let mut buf = [0_u8; 16];
        utils_rs::hash::decode_base58_multibase_onto(version, &mut buf).to_anyhow()?;
        let version = Uuid::from_bytes(buf);

        Ok(VersionTag{
            actor_id,
            version
        })
    }
});

pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        let entry_point: unsafe extern "C" fn(
            *mut libsqlite3_sys::sqlite3,
            *mut *mut std::ffi::c_char,
            *const libsqlite3_sys::sqlite3_api_routines,
        ) -> i32 = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        libsqlite3_sys::sqlite3_auto_extension(Some(entry_point));
    });
}

pub mod app;
