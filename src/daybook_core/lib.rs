#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use std::{
        borrow::Cow,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use interlude::*;

uniffi::setup_scaffolding!();

mod am;
mod docs;
mod ffi;
mod samod;

struct Ctx {
    acx: am::AmCtx,
    // rt: tokio::runtime::Handle,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn new() -> Result<Arc<Self>, eyre::Report> {
        let acx = am::AmCtx::new().await?;
        Ok(Arc::new(Self {
            acx,
            // rt: tokio::runtime::Handle::current(),
        }))
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}

mod sql {
    use crate::interlude::*;

    pub struct SqlCtx {
        db_pool: sqlx::SqlitePool
    }

    impl SqlCtx {
        pub async fn new() -> Res<Self> {
            let db_pool = sqlx::SqlitePool::connect("sqlite://daybook.db").await?;

            Ok(Self { db_pool })
        }
    }
}