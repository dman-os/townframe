mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use std::{
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use std::collections::HashMap;

use interlude::*;

mod am;
mod ffi;

#[derive(uniffi::Object)]
struct Ctx {
    rt: tokio::runtime::Runtime,
    acx: am::AmWorker,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    fn new() -> Result<Arc<Self>, eyre::Report> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let acx = rt.block_on(async { am::am_worker() });
        Ok(Arc::new(Self { rt, acx }))
    }
}

uniffi::setup_scaffolding!();

#[derive(Debug, Clone, autosurgeon::Reconcile, autosurgeon::Hydrate, uniffi::Record)]
struct Doc {
    #[key]
    id: Uuid,
    #[autosurgeon(with = "am::autosurgeon_date")]
    timestamp: OffsetDateTime,
}

#[derive(autosurgeon::Reconcile, autosurgeon::Hydrate)]
struct Docs {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<Uuid, Doc>,
}
