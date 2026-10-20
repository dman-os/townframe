// FIXME: investigate why this is being flagged
// by rustc
#![recursion_limit = "256"]

#[expect(unused)]
mod interlude {
    pub use autosurgeon::Hydrate;
    pub use autosurgeon::Reconcile;
    pub use regex;
    pub use std::borrow::Cow;
    pub use std::collections::HashMap;
    pub use std::path::PathBuf;
    pub use std::rc::Rc;
    pub use std::sync::Arc;
    pub use std::sync::LazyLock;
    pub use std::sync::RwLock;
    pub use tokio;
    pub use utils_rs::CHeapStr;
    pub use utils_rs::DHashMap;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

uniffi::setup_scaffolding!();

mod camera;
mod ffi;
mod listener_bridge;
mod macros;
mod repos;
mod rt;

pub use daybook_core::app::AppCtx;
pub use daybook_core::app::Database;
pub use daybook_core::repo::RepoOpenOptions;

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
