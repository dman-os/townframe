#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use interlude::*;

uniffi::setup_scaffolding!();

mod am;
mod docs;
mod ffi;
mod globals;
mod macros;
mod repos;
mod samod;
mod sql;
mod tables;

struct Ctx {
    acx: am::AmCtx,
    // rt: tokio::runtime::Handle,
    sql: sql::SqlCtx,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn new() -> Result<Arc<Self>, eyre::Report> {
        let sql = sql::SqlCtx::new().await?;
        let acx = am::AmCtx::new().await?;
        let cx = Arc::new(Self {
            acx,
            // rt: tokio::runtime::Handle::current(),
            sql,
        });
        // Initialize automerge document from globals/kv and start sync worker lazily.
        cx.acx.init_from_globals(cx.clone()).await?;
        Ok(cx)
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
