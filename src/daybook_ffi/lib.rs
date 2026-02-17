#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
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
mod macros;
mod repos;

pub use daybook_core::repo::{GlobalCtx, RepoOpenOptions, SqlCtx};

struct Ctx {
    pub repo_ctx: daybook_core::repo::RepoCtx,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn init(
        repo_root: PathBuf,
        ws_connector_url: Option<String>,
    ) -> Result<Arc<Self>, eyre::Report> {
        let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
        let repo_ctx = daybook_core::repo::RepoCtx::open(
            &global_ctx,
            &repo_root,
            RepoOpenOptions {
                ensure_initialized: true,
                peer_id: "daybook_client".to_string(),
                ws_connector_url,
            },
        )
        .await?;
        Ok(Arc::new(Self { repo_ctx }))
    }

    fn doc_app(&self) -> &samod::DocHandle {
        &self.repo_ctx.doc_app
    }

    fn doc_drawer(&self) -> &samod::DocHandle {
        &self.repo_ctx.doc_drawer
    }

    fn acx(&self) -> &utils_rs::am::AmCtx {
        &self.repo_ctx.acx
    }

    fn local_actor_id(&self) -> &automerge::ActorId {
        &self.repo_ctx.local_actor_id
    }

    fn local_user_path(&self) -> &str {
        &self.repo_ctx.local_user_path
    }

    fn blobs_root(&self) -> &std::path::Path {
        &self.repo_ctx.layout.blobs_root
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
