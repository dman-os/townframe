#![recursion_limit = "512"]

#[allow(unused)]
mod interlude {
    pub use crate::stores::Store;

    pub use daybook_types::doc::ChangeHashSet;

    pub use api_utils_rs::prelude::*;
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
    pub use utils_rs::am::AmCtx;
    pub use utils_rs::{CHeapStr, DHashMap};
}

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
daybook_types::custom_type_set!();

pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        sqlite_vec::sqlite3_vec_init();
    });
}

pub mod app;
