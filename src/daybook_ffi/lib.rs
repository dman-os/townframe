#![recursion_limit = "512"]

#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::PathBuf,
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;

uniffi::setup_scaffolding!();

mod camera;
mod ffi;
mod listener_bridge;
mod macros;
mod repos;
mod rt;

pub use daybook_core::app::{GlobalCtx, SqlCtx};
pub use daybook_core::repo::RepoOpenOptions;

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
